use std::borrow::Borrow;
use std::path::{Path, PathBuf};

use rusqlite::{named_params, params};
use snafu::ResultExt;

use crate::database::generate_id;
use crate::utils::Utils;

use super::difference::Diff;
use super::snapshot::Snapshot;
use super::{error::*, SqlName};

/// Index of all taken snapshots
pub struct Database {
    snapshot_count: usize,
    pub(super) conn: rusqlite::Connection,
    root: PathBuf,
}

impl Database {
    /// Returns SQL string that attaches given database.
    pub(super) fn attach(&self, name: &SqlName) -> Result<String, Error> {
        let mut root = self.root.clone();
        root.push(name.as_str());
        root.set_extension("db");
        let path = root
            .into_os_string()
            .into_string()
            .map_err(|str| CantBuildPath { str }.build())?;
        Ok(fmt_sql!("ATTACH DATABASE '{path}' AS {name}"))
    }

    /// Opens database at given path.
    ///
    /// Note that path is a directory, not `.db` file.
    /// Many auxiliary databases will be stored there too.
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

    /// Opens a snapshot for reading only.
    pub fn readonly_snapshot(&self, name: SqlName) -> Result<Snapshot<&Database>, Error> {
        self.conn
            .execute(&self.attach(&name)?, params![])
            .context(SqliteFailed)?;
        if self.is_snapshot_exists(&name)? {
            Ok(Snapshot { db: self, name })
        } else {
            NoSnapshotExists { name }.fail()
        }
    }

    /// Checks is snapshot exists.
    fn is_snapshot_exists(&self, name: &SqlName) -> Result<bool, Error> {
        let rows = self
            .conn
            .query_row(
                &fmt_sql!(
                    "SELECT COUNT(*) FROM {name}.sqlite_master
                    WHERE type='table' AND name='snap'",
                ),
                params![],
                |row| row.get::<_, u64>(0),
            )
            .context(SqliteFailed)?;
        Ok(rows != 0)
    }

    /// Attaches database and creates tables if needed.
    /// Returns true when new snapshot was created.
    fn init_snapshot(&self, name: &SqlName) -> Result<bool, Error> {
        // Attach database:
        self.conn
            .execute(&self.attach(name)?, params![])
            .context(SqliteFailed)?;
        // Maybe it was already initialized
        if self.is_snapshot_exists(name)? {
            return Ok(false);
        }
        // Ok, let's initialize it then
        let txn = self.conn.unchecked_transaction().context(SqliteFailed)?;
        let first_id = generate_id(self.snapshot_count as _, 0)?;
        txn.execute_batch(&fmt_sql!(
            "
                CREATE TABLE {name}.snap (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    path STRING,
                    size INTEGER,
                    identifier BLOB,   /* binary data */
                    info TEXT          /* json */
                );
                INSERT INTO {name}.snap(id) VALUES ({first_id});
                DELETE FROM {name}.snap WHERE id={first_id};
            "
        ))
        .context(SqliteFailed)?;
        txn.execute(
            fmt_sql!(static
                "INSERT INTO snapshots(name, created_at, filled_at) VALUES (:name, :created_at, 0)"
            ),
            named_params![
                ":name": name.0,
                ":created_at": time::OffsetDateTime::now_utc().format_rfc3339(),
            ],
        )
        .context(SqliteFailed)?;
        txn.commit().context(SqliteFailed)?;
        Ok(true)
    }

    // FIXME: Refactor to return `SnapshotFiller` instead. `Snapshot` should be read only.
    /// Opens snapshot, creating new database if needed.
    ///
    /// If you are not going to write rows here, it is better to use [`readonly_snapshot`] instead.
    ///
    /// [`readonly_snapshot`]: Self::readonly_snapshot
    pub fn open_snapshot(&mut self, name: SqlName) -> Result<Snapshot<&mut Database>, Error> {
        if self.init_snapshot(&name)? {
            self.snapshot_count += 1;
        }
        Ok(Snapshot { db: self, name })
    }

    /// Returns empty snapshot. Mostly useful for debugging purposes only.
    pub fn empty_snapshot(&self) -> Result<Snapshot<&Database>, Error> {
        // UNWRAP: `empty_snap` is correct sql name.
        #[allow(clippy::unwrap_used)]
        let name = SqlName::new("empty_snap".to_string()).unwrap();
        self.init_snapshot(&name)?;
        Ok(Snapshot { db: self, name })
    }

    /// Computes a difference between two given snapshots. See [Diff] documentation for details.
    ///
    /// Returns error if snapshot do not belong to this database (`self == before.db == after.db`).
    pub fn compare_snapshots<'a, D1: Borrow<Database>, D2: Borrow<Database>>(
        &'a self,
        before: &'a Snapshot<D1>,
        after: &'a Snapshot<D2>,
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
        Diff::new(self, &before.name, &after.name)
    }
}
