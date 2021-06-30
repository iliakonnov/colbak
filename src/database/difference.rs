use std::borrow::Borrow;

use rusqlite::params;
use snafu::{OptionExt, ResultExt};

use crate::fileinfo::Info;
use crate::path::External;

use super::error::*;
use super::snapshot::Snapshot;
use super::SqlName;
use super::index::Database;

pub struct Diff<'a> {
    db: &'a Database,
    name: SqlName,
    before_snap: SqlName,
    after_snap: SqlName,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, num_enum::TryFromPrimitive)]
#[repr(u8)]
pub enum DiffType {
    Deleted = 0,
    Created = 1,
    Changed = 2,
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

impl<'a> Diff<'a> {
    pub fn new(db: &'a Database, before_snap: SqlName, after_snap: SqlName) -> Result<Self, Error> {
        let name = SqlName::new(format!("diff_{}_vs_{}", &before_snap, &after_snap)).context(
            CantBuildDiffName {
                before: &before_snap,
                after: &after_snap,
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
        let name = &self.name;
        let mut statement = self
            .db
            .conn
            .prepare(&fmt_sql!("SELECT type, before, after FROM {name}.diff"))
            .context(SqliteFailed)?;

        let mut rows = statement.query(params![]).context(SqliteFailed)?;
        loop {
            let row = rows.next().context(SqliteFailed)?;
            let row = match row {
                Some(x) => x,
                None => break,
            };
            let kind: u8 = row.get(0).context(SqliteFailed)?;
            let before: Option<u64> = row.get(1).context(SqliteFailed)?;
            let after: Option<u64> = row.get(2).context(SqliteFailed)?;

            let kind = DiffType::parse(kind).context(WrongDiffType { found: kind })?;
            let before = self.load_info(&self.before_snap, before)?;
            let after = self.load_info(&self.after_snap, after)?;

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

    pub fn of_kind<F, E>(&'a self, kind: DiffType, mut func: F) -> Result<Result<(), E>, Error>
    where
        F: FnMut(DiffRow) -> Result<(), E>,
    {
        // It's way easier to filter inside of Rust instead of passing `WHERE type = {kind}` to sqlite.
        // But in future it would be easy to switch to sqlite.
        // At this moment it's very inefficient since it deserializes all infos, including ignored ones.
        self.for_each(|row| {
            if row.kind() == kind {
                func(row)
            } else {
                Ok(())
            }
        })
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
