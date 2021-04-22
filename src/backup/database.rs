use std::path::Path;

use crate::fileinfo::Info;
use crate::path::EncodedPath;
use crate::path::External;

use fallible_iterator::FallibleIterator;
use rusqlite::{named_params, params};
use snafu::IntoError;
use snafu::OptionExt;
use snafu::{ensure, ResultExt, Snafu};

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
        before: SqlName,
        after: SqlName,
    },
    TooManySnapshots,
    TooManyRows,
    DatabasesMixed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlName(String);

impl SqlName {
    pub fn new(name: String) -> Option<SqlName> {
        if name
            .chars()
            .all(|c| matches!(c, '1'..='9' | 'A'..='Z' | 'a'..='z'))
        {
            Some(SqlName(name))
        } else {
            None
        }
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
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db = rusqlite::Connection::open(path).context(SqliteFailed)?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS snapshots (
            name TEXT NOT NULL,
            created_at DATETIME NOT NULL,
            filled_at DATETIME,
            closed_at DATETIME
        ",
            params![],
        )
        .context(SqliteFailed)?;
        let snapshot_count = db
            .execute("SELECT COUNT(*) FROM snapshots", params![])
            .context(SqliteFailed)?;
        Ok(Self {
            snapshot_count,
            conn: db,
        })
    }

    pub fn open_snapshot(&mut self, name: SqlName) -> Result<Snapshot, Error> {
        // Attach database:
        self.conn
            .execute(&format!("ATTACH DATABASE {name}.db AS {name}"), params![])
            .context(SqliteFailed)?;
        // Maybe we should create a table then.
        let is_exists = self
            .conn
            .execute(
                &format!(
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
            txn.execute_batch(&format!(
                "
                    CREATE TABLE {name}.snap (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        path STRING,
                        identifier BLOB,   /* binary data */
                        info TEXT NOT NULL /* json */
                    );
                    INSERT INTO snap(id) VALUES ({first_id});
                    DELETE FROM snap WHERE id={first_id};
                "
            ))
            .context(SqliteFailed)?;
            txn.execute_named(
                "INSERT INTO snapshots(name, created_at) VALUES (:name, :created_at)",
                named_params![
                    ":name": name.0,
                    "created_at": time::OffsetDateTime::now_utc().format(time::Format::Rfc3339),
                ],
            )
            .context(SqliteFailed)?;
            txn.commit().context(SqliteFailed)?;
            self.snapshot_count += 1;
        }
        Ok(Snapshot { db: self, name })
    }

    pub fn compare_snapshots<'a>(
        &'a self,
        before: &Snapshot<'a>,
        after: &Snapshot<'a>,
    ) -> Result<Diff<'a>, Error> {
        snafu::ensure!(
            std::ptr::eq(self, before.db) && std::ptr::eq(before, after),
            DatabasesMixed
        );
        let name = SqlName::new(format!("diff_{}_vs_{}", &before.name, &after.name)).context(
            CantBuildDiffName {
                before: &before.name,
                after: &after.name,
            },
        )?;
        let diff = Diff::new(self, name)?;
        Ok(diff)
    }
}

pub struct Snapshot<'a> {
    db: &'a Database,
    name: SqlName,
}

impl Snapshot<'_> {
    pub fn fill<P: AsRef<Path>>(&self, root: P) -> Result<(), Error> {
        let walk = walkdir::WalkDir::new(root).into_iter();
        let txn = self.db.conn.unchecked_transaction().context(SqliteFailed)?;
        {
            let mut stmt = txn
                .prepare(&format!(
                    "INSERT INTO {0}.snap(id, path, identifier, info)
                    VALUES(:path, :identifier, :info)",
                    self.name
                ))
                .context(SqliteFailed)?;
            for i in walk {
                let i = i.context(CantWalkdir)?;
                let metadata = i.metadata().context(CantWalkdir)?;
                let path = EncodedPath::from_path(i.into_path());
                let info = Info::with_metadata(path, metadata);
                stmt.execute_named(named_params![
                    ":path": info.path.as_bytes(),
                    ":identifier": info.identifier().as_ref().map(|i| i.as_bytes()),
                    ":info": serde_json::to_string(&info).context(JsonFailed)?,
                ])
                .context(SqliteFailed)?;
            }
        }
        txn.commit().context(SqliteFailed)?;
        Ok(())
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        let _ = self
            .db
            .conn
            .execute(&format!("DETACH DATABASE {0}", self.name), params![]);
    }
}

pub struct Diff<'a> {
    db: &'a Database,
    name: SqlName,
}

#[repr(u8)]
pub enum DiffType {
    Deleted = 0,
    Created = 1,
    Changed = 2,
}

impl<'a> Diff<'a> {
    pub fn new(db: &'a Database, name: SqlName) -> Result<Self, Error> {
        {
            db.conn
                .execute(&format!("ATTACH DATABASE {name}.db AS {name}"), params![])
                .context(SqliteFailed)?;
            db.conn
                .execute(
                    &format!(
                        "
                        CREATE TABLE IF NOT EXISTS {name}.diff (
                            before INTEGER,  -- REFERENCES <before>.snap(id)
                            after  INTEGER,  -- REFERENCES <after>.snap(id),
                            type   INTEGER,  -- see `DiffType`
                            info   TEXT,     -- same as snap.info 
                        )
                        "
                    ),
                    params![],
                )
                .context(SqliteFailed)?;
        }
        Ok(Diff { db, name })
    }

    pub fn fill(&self, before: &Snapshot<'a>, after: &Snapshot<'a>) -> Result<(), Error> {
        let b = &before.name;
        let a = &after.name;

        {
            self.db
                .conn
                .execute_batch(&format!(
                    "
                    CREATE INDEX IF NOT EXISTS {a}.idx_ident ON {a}.snap ( identifier );
                    CREATE INDEX IF NOT EXISTS {b}.idx_ident ON {b}.snap ( identifier );
                    CREATE INDEX IF NOT EXISTS {a}.idx_info ON {a}.snap ( info );
                    CREATE INDEX IF NOT EXISTS {b}.idx_info ON {b}.snap ( info );
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
            .execute_batch(&format!(
                r#"
                    INSERT INTO {name}.diff
                    SELECT
                        id AS before,
                        NULL AS after,
                        {deleted} as type,
                        info
                    FROM {b}
                    WHERE identifier NOT IN {a};

                    INSERT INTO {name}.diff
                    SELECT
                        NULL AS before,
                        id AS after,
                        {created} as type,
                        info
                    FROM {a}
                    WHERE identifier NOT IN {b};

                    INSERT INTO {name}.diff
                    SELECT
                        {b}.snap.id AS before,
                        {a}.snap.id AS after,
                        {changed} as type,
                        {a}.snap.info as info
                    FROM {a}.snap
                        INNER JOIN {b}.snap
                        USING (identifier)
                    WHERE {a}.snap.info != {b}.snap.info;
                "#
            ))
            .context(SqliteFailed)?;

        Ok(())
    }

    pub fn for_each<F, E>(&'a self, kind: DiffType, mut func: F) -> Result<Result<(), E>, Error>
    where
        F: FnMut(Info<External>) -> Result<(), E>,
    {
        let name = &self.name;
        let kind = kind as u8;
        let mut statement = self
            .db
            .conn
            .prepare(&format!(
                "
            SELECT info
            FROM {name}.diff
            WHERE type = {kind}
            "
            ))
            .context(SqliteFailed)?;
        let mut rows = statement
            .query(params![])
            .context(SqliteFailed)?
            .map(|row| row.get::<_, Vec<u8>>(0))
            .map_err(|e| SqliteFailed.into_error(e))
            .map(|x| serde_json::from_slice(&x).context(JsonFailed));
        loop {
            let row = match rows.next()? {
                Some(x) => x,
                None => break,
            };
            match func(row) {
                Ok(_) => {}
                res @ Err(_) => return Ok(res),
            }
        }
        Ok(Ok(()))
    }
}

impl Drop for Diff<'_> {
    fn drop(&mut self) {
        let _ = self
            .db
            .conn
            .execute(&format!("DETACH DATABASE {0}", self.name), params![]);
    }
}
