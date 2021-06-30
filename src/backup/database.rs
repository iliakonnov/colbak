use std::borrow::{Borrow, BorrowMut};
use std::path::{Path, PathBuf};

use crate::fileinfo::{FileIdentifier, Info};
use crate::path::EncodedPath;
use crate::path::External;

use rusqlite::{named_params, params};
use snafu::{ensure, OptionExt, ResultExt, Snafu};

macro_rules! fmt_sql {
    ($($args:tt)*) => {{
        let sql = format!($($args)*);
        log!(fmt_sql: "fmt_sql: {}", sql);
        sql
    }}
}

#[derive(Debug, Snafu)]
pub enum Error {
    SqliteFailed {
        source: rusqlite::Error,
        backtrace: snafu::Backtrace,
    },
    JsonFailed {
        source: serde_json::Error,
        backtrace: snafu::Backtrace,
    },
    CantWalkdir {
        source: walkdir::Error,
    },
    CantBuildDiffName {
        source: NotAValidSqlName,
        before: SqlName,
        after: SqlName,
    },
    CantBuildPath {
        str: std::ffi::OsString,
        backtrace: snafu::Backtrace,
    },
    TooManySnapshots,
    TooManyRows,
    WrongDiffType {
        found: u8,
    },
    InvalidDiffRow,
    #[snafu(display("It looks like you have mixed different databases: this=0x{:x}, before=0x{:x}, after=0x{:x}", this, before, after))]
    DatabasesMixed {
        backtrace: snafu::Backtrace,
        this: usize,
        before: usize,
        after: usize,
    },
}

#[derive(Debug, Snafu)]
pub struct NotAValidSqlName {
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlName(String);

impl SqlName {
    pub fn new(name: String) -> Result<SqlName, NotAValidSqlName> {
        let mut chars = name.chars();
        if matches!(chars.next(), Some('A'..='Z' | 'a'..='z'))
            && chars.all(|c| matches!(c, '0'..='9' | 'A'..='Z' | 'a'..='z' | '_'))
        {
            Ok(SqlName(name))
        } else {
            Err(NotAValidSqlName { name })
        }
    }

    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn now() -> SqlName {
        let name = time::OffsetDateTime::now_utc().format("at%Y_%m_%d_%H_%M_%S_%N");
        // PANIC: name is completely correct, so it's safe to unwrap here.
        SqlName::new(name).unwrap()
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SqlName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> From<&'a SqlName> for SqlName {
    fn from(x: &'a SqlName) -> SqlName {
        x.clone()
    }
}

fn generate_id(table_id: u64, row_id: u64) -> Result<u64, Error> {
    // Single database can have up to 2^23 snapshots.
    // That's enough for 100 years of making snapshots every 6 minutes.
    // (it should be easy to reset the counter too)
    // Each snapshot can have up to 2^(63-23) = 2^40 of files.
    // That is 256 times more than NTFS supports.
    // So it should be enough for any practical use.
    ensure!(table_id < (1 << 23), TooManySnapshots);
    ensure!(row_id < (1 << 40), TooManyRows);
    Ok((table_id << 23) | row_id)
}

pub struct Database {
    snapshot_count: usize,
    conn: rusqlite::Connection,
    root: PathBuf,
}

impl Database {
    fn attach(&self, name: &SqlName) -> Result<String, Error> {
        let mut root = self.root.clone();
        root.push(name.as_str());
        root.set_extension("db");
        let path = root
            .into_os_string()
            .into_string()
            .map_err(|str| CantBuildPath { str }.build())?;
        Ok(fmt_sql!("ATTACH DATABASE '{path}' AS {name}"))
    }

    pub fn open<P: AsRef<Path>>(root: P) -> Result<Self, Error> {
        let mut root = root.as_ref().to_owned();
        root.push("db.sqlite3");
        let db = rusqlite::Connection::open(&root).context(SqliteFailed)?;
        root.pop();

        db.execute(
            "CREATE TABLE IF NOT EXISTS snapshots (
                name TEXT NOT NULL,
                created_at DATETIME NOT NULL,
                filled_at DATETIME,
                is_uploaded BOOLEAN
            )",
            params![],
        )
        .context(SqliteFailed)?;
        let snapshot_count = db
            .query_row("SELECT COUNT(*) FROM snapshots", params![], |r| r.get(0))
            .context(SqliteFailed)?;
        Ok(Self {
            snapshot_count,
            conn: db,
            root,
        })
    }

    pub fn readonly_snapshot(&self, name: SqlName) -> Result<Snapshot<&Database>, Error> {
        self.conn
            .execute(&self.attach(&name)?, params![])
            .context(SqliteFailed)?;
        // FIXME: We should check is snapshot exists.
        Ok(Snapshot { db: self, name })
    }

    pub fn open_snapshot(&mut self, name: SqlName) -> Result<Snapshot<&mut Database>, Error> {
        // Attach database:
        self.conn
            .execute(&self.attach(&name)?, params![])
            .context(SqliteFailed)?;
        // Maybe we should create a table then.
        let is_exists = self
            .conn
            .execute(
                &fmt_sql!(
                    "SELECT name FROM {name}.sqlite_master
                    WHERE type='table' AND name='snap'",
                ),
                params![],
            )
            .context(SqliteFailed)?;
        if is_exists == 0 {
            // Ok, let's initialize it then
            let txn = self.conn.unchecked_transaction().context(SqliteFailed)?;
            let first_id = generate_id(self.snapshot_count as _, 0)?;
            txn.execute_batch(&fmt_sql!(
                "
                    CREATE TABLE {name}.snap (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        path STRING,
                        identifier BLOB,   /* binary data */
                        info TEXT          /* json */
                    );
                    INSERT INTO {name}.snap(id) VALUES ({first_id});
                    DELETE FROM {name}.snap WHERE id={first_id};
                "
            ))
            .context(SqliteFailed)?;
            txn.execute(
                "INSERT INTO snapshots(name, created_at, filled) VALUES (:name, :created_at, 0)",
                named_params![
                    ":name": name.0,
                    ":created_at": time::OffsetDateTime::now_utc().format(time::Format::Rfc3339),
                ],
            )
            .context(SqliteFailed)?;
            txn.commit().context(SqliteFailed)?;
            self.snapshot_count += 1;
        }
        Ok(Snapshot { db: self, name })
    }

    pub fn compare_snapshots<'a, D1: Borrow<Database>, D2: Borrow<Database>>(
        &'a self,
        before: &Snapshot<D1>,
        after: &Snapshot<D2>,
    ) -> Result<Diff<'a>, Error> {
        {
            let this = self as *const Self as usize;
            let before = std::borrow::Borrow::borrow(&before.db) as *const Self as usize;
            let after = std::borrow::Borrow::borrow(&after.db) as *const Self as usize;
            snafu::ensure!(
                this == before && before == after,
                DatabasesMixed {
                    this,
                    before,
                    after,
                }
            );
        }
        let diff = Diff::new(self, before.name.clone(), after.name.clone())?;
        diff.fill(before, after)?;
        Ok(diff)
    }
}

pub struct Snapshot<D: Borrow<Database>> {
    db: D,
    name: SqlName,
}

#[must_use]
pub struct SnapshotFiller<'a> {
    snap_name: &'a SqlName,
    transaction: rusqlite::Transaction<'a>,
    sql: String,
}

impl<'a> SnapshotFiller<'a> {
    pub fn new<D: BorrowMut<Database>>(snapshot: &'a mut Snapshot<D>) -> Result<Self, Error> {
        let mut txn = snapshot
            .db
            .borrow_mut()
            .conn
            .transaction()
            .context(SqliteFailed)?;
        txn.set_drop_behavior(rusqlite::DropBehavior::Rollback);
        let sql = fmt_sql!(
            "INSERT INTO {0}.snap(path, identifier, info)
            VALUES(:path, :identifier, :info)",
            snapshot.name
        );
        Ok(SnapshotFiller {
            snap_name: &snapshot.name,
            transaction: txn,
            sql,
        })
    }

    fn get_statement(&self) -> Result<rusqlite::CachedStatement, Error> {
        self.transaction
            .prepare_cached(&self.sql)
            .context(SqliteFailed)
    }

    pub fn add(&self, entry: walkdir::DirEntry) -> Result<(), Error> {
        let metadata = entry.metadata().context(CantWalkdir)?;
        let path = EncodedPath::from_path(entry.into_path());
        let info = Info::with_metadata(path, &metadata);
        self.get_statement()?.execute(named_params![
            ":path": info.path.as_bytes(),
            ":identifier": info.identifier().as_ref().map(FileIdentifier::as_bytes).unwrap_or_default(),
            ":info": serde_json::to_string(&info).context(JsonFailed)?,
        ])
        .context(SqliteFailed)?;
        Ok(())
    }

    pub fn save(self) -> Result<(), Error> {
        self.transaction
            .execute(
                "UPDATE snapshots SET filled_at=? WHERE name=?",
                params![
                    time::OffsetDateTime::now_utc().format(time::Format::Rfc3339),
                    self.snap_name.as_str()
                ],
            )
            .context(SqliteFailed)?;
        self.transaction.commit().context(SqliteFailed)?;
        Ok(())
    }

    pub fn fill(self, root: &Path) -> Result<Self, Error> {
        log!(time: "Walking over {}", root = root.to_string_lossy());
        let walk = walkdir::WalkDir::new(root).into_iter();
        for entry in walk {
            let entry = entry.context(CantWalkdir)?;
            self.add(entry)?;
        }
        log!(time: "Done walking ({})", root = root.to_string_lossy());
        Ok(self)
    }
}

impl<'a, D: BorrowMut<Database>> Snapshot<D> {
    pub fn filler(&mut self) -> Result<SnapshotFiller, Error> {
        SnapshotFiller::new(self)
    }
}

impl<'a, D: Borrow<Database>> Snapshot<D> {
    pub fn name(&self) -> &SqlName {
        &self.name
    }
}

impl<'a, D: Borrow<Database>> Drop for Snapshot<D> {
    fn drop(&mut self) {
        let db: &Database = self.db.borrow();
        let _unused_result = db
            .conn
            .execute(&fmt_sql!("DETACH DATABASE {0}", self.name), params![]);
    }
}

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
