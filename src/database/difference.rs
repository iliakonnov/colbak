use std::borrow::Borrow;
use std::ops::{RangeInclusive};

use rusqlite::params;
use snafu::{OptionExt, ResultExt};

use crate::fileinfo::Info;
use crate::path::External;

use super::error::*;
use super::index::Database;
use super::snapshot::Snapshot;
use super::SqlName;

// It looks like bitflag, but it is not.
// Each row in database may have only one of these bits set.
// Making an bitflag allows to filter rows much more easily and efficient.
#[derive(Clone, Copy, Debug, Eq, PartialEq, num_enum::TryFromPrimitive)]
#[repr(u8)]
pub enum DiffType {
    Deleted = 0b001,
    Created = 0b010,
    Changed = 0b100,
}

impl DiffType {
    fn parse(num: u8) -> Option<DiffType> {
        use std::convert::TryInto;
        num.try_into().ok()
    }
}

#[derive(Debug, Clone)]
pub enum DiffRow {
    Deleted {
        before: Info<External>,
    },
    Created {
        after: Info<External>,
    },
    Changed {
        before: Info<External>,
        after: Info<External>,
    },
}

impl DiffRow {
    #[must_use]
    pub fn kind(&self) -> DiffType {
        match self {
            DiffRow::Deleted { .. } => DiffType::Deleted,
            DiffRow::Created { .. } => DiffType::Created,
            DiffRow::Changed { .. } => DiffType::Changed,
        }
    }
}

pub struct Diff<'a> {
    db: &'a Database,
    name: SqlName,
    before_snap: &'a SqlName,
    after_snap: &'a SqlName,
}

impl<'a> Diff<'a> {
    pub fn new(
        db: &'a Database,
        before_snap: &'a SqlName,
        after_snap: &'a SqlName,
    ) -> Result<Self, Error> {
        let name = SqlName::new(format!("diff_{}_vs_{}", before_snap, after_snap)).context(
            CantBuildDiffName {
                before: before_snap,
                after: after_snap,
            },
        )?;
        {
            db.conn
                .execute(&db.attach(&name)?, params![])
                .context(SqliteFailed)?;
            db.conn
                .execute(
                    &fmt_sql!(
                        "
                        CREATE TABLE IF NOT EXISTS {name}.diff (
                            before INTEGER,  -- REFERENCES <before>.snap(id)
                            after  INTEGER,  -- REFERENCES <after>.snap(id),
                            type   INTEGER   -- see `DiffType`
                        )
                        "
                    ),
                    params![],
                )
                .context(SqliteFailed)?;
        }
        Ok(Diff {
            db,
            name,
            before_snap,
            after_snap,
        })
    }

    pub fn fill<D1: Borrow<Database>, D2: Borrow<Database>>(
        &self,
        before: &Snapshot<D1>,
        after: &Snapshot<D2>,
    ) -> Result<(), Error> {
        let before = &before.name;
        let after = &after.name;

        {
            self.db
                .conn
                .execute_batch(&fmt_sql!(
                    "
                    CREATE INDEX IF NOT EXISTS {after}.idx_ident ON snap ( identifier );
                    CREATE INDEX IF NOT EXISTS {before}.idx_ident ON snap ( identifier );
                    CREATE INDEX IF NOT EXISTS {after}.idx_info ON snap ( info );
                    CREATE INDEX IF NOT EXISTS {before}.idx_info ON snap ( info );
                "
                ))
                .context(SqliteFailed)?;
        }

        let name = &self.name;
        let deleted = DiffType::Deleted as u8;
        let created = DiffType::Created as u8;
        let changed = DiffType::Changed as u8;
        self.db
            .conn
            .execute_batch(&fmt_sql!(
                r#"
                    DELETE FROM {name}.diff;

                    INSERT INTO {name}.diff (before, after, type)
                    SELECT
                        id,
                        NULL,
                        {deleted}
                    FROM {before}.snap
                    WHERE identifier NOT IN (SELECT identifier FROM {after}.snap);

                    INSERT INTO {name}.diff (before, after, type)
                    SELECT
                        NULL,
                        id,
                        {created}
                    FROM {after}.snap
                    WHERE identifier NOT IN (SELECT identifier FROM {before}.snap);

                    INSERT INTO {name}.diff (before, after, type)
                    SELECT
                        {before}.snap.id,
                        {after}.snap.id,
                        {changed}
                    FROM {after}.snap
                        INNER JOIN {before}.snap
                        USING (identifier)
                    WHERE {after}.snap.info != {before}.snap.info;
                "#
            ))
            .context(SqliteFailed)?;

        Ok(())
    }

    pub fn query(&'a self) -> DiffQuery<'a> {
        DiffQuery {
            diff: self,
            enabled_kinds: 0b111,
            allowed_sizes: 0..=u64::MAX
        }
    }
}

#[must_use]
pub struct DiffQuery<'a> {
    diff: &'a Diff<'a>,
    enabled_kinds: u8,
    allowed_sizes: RangeInclusive<u64>,
}

impl<'a> DiffQuery<'a> {
    fn load_info(
        &'a self,
        source: &SqlName,
        id: Option<u64>,
    ) -> Result<Option<Info<External>>, Error> {
        let id = match id {
            Some(x) => x,
            None => return Ok(None),
        };
        let json: String = self
            .diff
            .db
            .conn
            .query_row(
                &fmt_sql!("SELECT INFO FROM {source}.snap WHERE id=?"),
                params![id],
                |row| row.get(0),
            )
            .context(SqliteFailed)?;
        let info = serde_json::from_str(&json).context(JsonFailed)?;
        Ok(Some(info))
    }

    pub fn for_each<F, E>(&'a self, mut func: F) -> Result<Result<(), E>, Error>
    where
        F: FnMut(DiffRow) -> Result<(), E>,
    {
        let name = &self.diff.name;
        let type_filter = self.enabled_kinds;
        let min_size = self.allowed_sizes.start();
        let max_size = self.allowed_sizes.end();
        let mut statement = self
            .diff
            .db
            .conn
            .prepare(&fmt_sql!(
                r#"
                SELECT type, before, after
                FROM {name}.diff
                WHERE (type & {type_filter}) != 0
                AND {min_size} <= size AND size <= {max_size}
                "#
            ))
            .context(SqliteFailed)?;

        let mut rows = statement.query(params![]).context(SqliteFailed)?;
        while let Some(row) = rows.next().context(SqliteFailed)? {
            let kind: u8 = row.get(0).context(SqliteFailed)?;
            let before: Option<u64> = row.get(1).context(SqliteFailed)?;
            let after: Option<u64> = row.get(2).context(SqliteFailed)?;

            let kind = DiffType::parse(kind).context(WrongDiffType { found: kind })?;
            let before = self.load_info(self.diff.before_snap, before)?;
            let after = self.load_info(self.diff.after_snap, after)?;

            let row = match kind {
                DiffType::Deleted => DiffRow::Deleted {
                    before: before.context(InvalidDiffRow)?,
                },
                DiffType::Created => DiffRow::Created {
                    after: after.context(InvalidDiffRow)?,
                },
                DiffType::Changed => DiffRow::Changed {
                    before: before.context(InvalidDiffRow)?,
                    after: after.context(InvalidDiffRow)?,
                },
            };

            match func(row) {
                Ok(_) => {}
                res @ Err(_) => return Ok(res),
            }
        }

        Ok(Ok(()))
    }

    pub fn deny_kind(mut self, kind: DiffType) -> Self {
        self.enabled_kinds &= !(kind as u8);
        self
    }

    pub fn only_kind(mut self, kind: DiffType) -> Self {
        self.enabled_kinds = kind as u8;
        self
    }

    pub fn with_size(mut self, size: RangeInclusive<u64>) -> Self {
        self.allowed_sizes = size;
        self
    }
}

impl Drop for Diff<'_> {
    fn drop(&mut self) {
        let _unused_result = self
            .db
            .conn
            .execute(&fmt_sql!("DETACH DATABASE {0}", self.name), params![]);
    }
}
