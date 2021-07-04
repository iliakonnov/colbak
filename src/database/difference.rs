use std::ops::RangeInclusive;

use rusqlite::params;
use snafu::{OptionExt, ResultExt};

use crate::fileinfo::Info;
use crate::path::{EncodedPath, External};

use super::index::Database;
use super::SqlName;
use super::{error::*, RowId};

/// Type of change that single row is describing.
///
/// It looks like bitflag, but it is not.
/// Each row in database may have only one of these bits set.
/// Making an bitflag allows to filter rows much more easily and efficient.
#[derive(Clone, Copy, Debug, Eq, PartialEq, num_enum::TryFromPrimitive)]
#[repr(u8)]
pub enum DiffType {
    /// File deleted
    Deleted = 0b001,
    /// New file created.
    ///
    /// Modifying content of file will produce two changes: `Deleted` and `Created`
    Created = 0b010,
    /// Identifier does not changed, but some parts of info did.
    ///
    /// That means that contents ([identifier]) of file are the same, only metadata is different.
    ///
    /// [identifier]: crate::fileinfo::FileIdentifier
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
        rowid: RowId,
        before: Info<External>,
        size: u64,
        path: EncodedPath<External>,
    },
    Created {
        rowid: RowId,
        after: Info<External>,
        size: u64,
        path: EncodedPath<External>,
    },
    Changed {
        rowid: RowId,
        before: Info<External>,
        after: Info<External>,
        size: u64,
        path: EncodedPath<External>,
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

/// Difference between two snapshots.
pub struct Diff<'a> {
    db: &'a Database,
    name: SqlName,
    before_snap: &'a SqlName,
    after_snap: &'a SqlName,
}

impl<'a> Diff<'a> {
    /// Computes difference between two snapshots, creating database if needed.
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
                            type   INTEGER,  -- see `DiffType`
                            size   INTEGER,  -- size of file, used by packer
                            path   TEXT      -- path to file, again for packer
                        )
                        "
                    ),
                    params![],
                )
                .context(SqliteFailed)?;
        }
        let result = Diff {
            db,
            name,
            before_snap,
            after_snap,
        };
        result.fill()?;
        Ok(result)
    }

    fn fill(&self) -> Result<(), Error> {
        let before = self.before_snap;
        let after = self.after_snap;

        let name = &self.name;
        let deleted = DiffType::Deleted as u8;
        let created = DiffType::Created as u8;
        let changed = DiffType::Changed as u8;
        self.db
            .conn
            .execute_batch(&fmt_sql!(
                r#"
                    CREATE INDEX IF NOT EXISTS {after}.idx_ident ON snap ( identifier );
                    CREATE INDEX IF NOT EXISTS {before}.idx_ident ON snap ( identifier );
                    CREATE INDEX IF NOT EXISTS {after}.idx_info ON snap ( info );
                    CREATE INDEX IF NOT EXISTS {before}.idx_info ON snap ( info );

                    DELETE FROM {name}.diff;

                    INSERT INTO {name}.diff
                        (before, after, type, size, path)
                    SELECT
                        id, NULL,  {deleted}, size, path
                    FROM {before}.snap
                    WHERE identifier NOT IN (SELECT identifier FROM {after}.snap);

                    INSERT INTO {name}.diff
                        (before, after, type, size, path)
                    SELECT
                        NULL, id, {created}, size, path
                    FROM {after}.snap
                    WHERE identifier NOT IN (SELECT identifier FROM {before}.snap);

                    INSERT INTO {name}.diff
                        (before, after, type, size, path)
                    SELECT
                        {before}.snap.id,
                        {after}.snap.id,
                        {changed},
                        size,
                        path
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
            allowed_sizes: 0..=u64::MAX,
        }
    }
}

/// Small structure that helps making efficient queries to the [`Difference`](Difference).
#[must_use]
pub struct DiffQuery<'a> {
    diff: &'a Diff<'a>,
    /// Bitmask that toggles allowed [types](DiffType) of changes
    enabled_kinds: u8,
    /// Size of files that will be returned
    allowed_sizes: RangeInclusive<u64>,
}

impl<'a> DiffQuery<'a> {
    /// Loads information about the file from snapshot named `name`.
    /// 
    /// Returns `None` iff `id` is `None`.
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

    /// Selects provided columns with correct filters.
    fn select(&'a self, select: &str) -> Result<rusqlite::Statement, Error> {
        let name = &self.diff.name;
        let type_filter = self.enabled_kinds;
        let min_size = self.allowed_sizes.start();
        let max_size = self.allowed_sizes.end();
        let statement = self
            .diff
            .db
            .conn
            .prepare(&fmt_sql!(
                r#"
                SELECT {select}
                FROM {name}.diff
                WHERE (type & {type_filter}) != 0
                AND {min_size} <= size AND size <= {max_size}
                "#
            ))
            .context(SqliteFailed)?;
        Ok(statement)
    }

    /// Returns count of matching rows
    pub fn count(&'a self) -> Result<u64, Error> {
        let mut statement = self.select("COUNT(*)")?;
        statement
            .query_row(params![], |x| x.get(0))
            .context(SqliteFailed)
    }

    /// Applies function to each matching row
    pub fn for_each<F, E>(&'a self, mut func: F) -> Result<Result<(), E>, Error>
    where
        F: FnMut(DiffRow) -> Result<(), E>,
    {
        let mut statement = self.select("type, before, after, size, path, ROWID")?;

        let mut rows = statement.query(params![]).context(SqliteFailed)?;
        while let Some(row) = rows.next().context(SqliteFailed)? {
            let kind: u8 = row.get(0).context(SqliteFailed)?;
            let before: Option<u64> = row.get(1).context(SqliteFailed)?;
            let after: Option<u64> = row.get(2).context(SqliteFailed)?;
            let size: u64 = row.get(3).context(SqliteFailed)?;
            let path: Vec<u8> = row.get(4).context(SqliteFailed)?;
            let rowid = row.get(5).context(SqliteFailed)?;

            let kind = DiffType::parse(kind).context(WrongDiffType { found: kind })?;
            let before = self.load_info(self.diff.before_snap, before)?;
            let after = self.load_info(self.diff.after_snap, after)?;
            let path = EncodedPath::from_vec(path);
            let rowid = RowId(rowid);

            let row = match kind {
                DiffType::Deleted => DiffRow::Deleted {
                    rowid,
                    path,
                    size,
                    before: before.context(InvalidDiffRow)?,
                },
                DiffType::Created => DiffRow::Created {
                    rowid,
                    path,
                    size,
                    after: after.context(InvalidDiffRow)?,
                },
                DiffType::Changed => DiffRow::Changed {
                    rowid,
                    path,
                    size,
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

    pub fn less_than(self, size: u64) -> Self {
        #[allow(clippy::range_minus_one)]
        self.with_size(0..=size - 1)
    }

    pub fn larger_or_eq(self, size: u64) -> Self {
        self.with_size(size..=u64::MAX)
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
