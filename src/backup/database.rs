use std::path::Path;

use crate::fileinfo::Info;
use crate::path::EncodedPath;

use rusqlite::{named_params, params};
use snafu::{ResultExt, Snafu};

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
    DatabasesMixed,
}

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

    fn attach(&self) -> String {
        format!("ATTACH DATABASE {0}.db AS db_{0}", self.0)
    }

    fn detach(&self) -> String {
        format!("DETACH DATABASE db_{0}", self.0)
    }

    fn create_snapshot(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS db_{0}.snap_{0} (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    path STRING,
                    identifier BLOB,   /* bincode */
                    info TEXT NOT NULL /* json */
                )",
            self.0
        )
    }

    fn insert_snapshot(&self) -> String {
        format!(
            "INSERT INTO
                 db_{0}.snap_{0}(path, identifier, info)
                 VALUES(:path, :identifier, :info)",
            self.0
        )
    }

    fn drop_diff(&self) -> String {
        format!("DROP TABLE db_{0}.diff_{0}", self.0)
    }
}

pub struct Database {
    db: rusqlite::Connection,
}

impl Database {
    pub fn open_snapshot(&self, name: SqlName) -> Result<Snapshot, Error> {
        self.db
            .execute(&name.attach(), params![])
            .context(SqliteFailed)?;
        self.db
            .execute(&name.create_snapshot(), params![])
            .context(SqliteFailed)?;
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
        todo!()
    }
}

pub struct Snapshot<'a> {
    db: &'a Database,
    name: SqlName,
}

impl Snapshot<'_> {
    pub fn fill<P: AsRef<Path>>(&self, root: P) -> Result<(), Error> {
        let walk = walkdir::WalkDir::new(root).into_iter();
        let txn = self.db.db.unchecked_transaction().context(SqliteFailed)?;
        {
            let mut stmt = txn
                .prepare(&self.name.insert_snapshot())
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
        let _ = self.db.db.execute(&self.name.detach(), params![]);
    }
}

pub struct Diff<'a> {
    db: &'a Database,
    name: SqlName,
}

impl Drop for Diff<'_> {
    fn drop(&mut self) {
        let _ = self.db.db.execute(&self.name.drop_diff(), params![]);
        let _ = self.db.db.execute(&self.name.detach(), params![]);
    }
}
