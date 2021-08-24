use std::path::Path;

use rusqlite::params;
use snafu::{Backtrace, ResultExt, Snafu};

use crate::fileinfo::Info;
use crate::path::External;
use crate::DateTime;
use crate::utils::Utils;

use super::Key;

#[derive(Debug, Snafu)]
pub enum Error {
    SqliteFailed {
        source: rusqlite::Error,
        backtrace: Backtrace,
    },
}

/// Stores state of remote cloud provider.
pub struct State {
    db: rusqlite::Connection,
}

pub struct UploadedArchive {
    pub key: Key,
    pub files: Vec<Info<External>>,
    pub uploaded_at: DateTime,
}

impl State {
    /// Opens database file at specified path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db = rusqlite::Connection::open(path).context(SqliteFailed)?;
        db.execute_batch(
            r#"
                CREATE TABLE IF NOT EXISTS archives(
                    id PRIMARY KEY AUTOINCREMENT,
                    key TEXT,
                    uploaded_at TEXT
                );
                CREATE TABLE IF NOT EXISTS contents(
                    hash TEXT,
                    archive INTEGER FOREIGN KEY REFERENCES archives(id)
                );
            "#,
        )
        .context(SqliteFailed)?;
        Ok(State { db })
    }

    /// Puts information about uploaded archive to the database.
    pub fn set_uploaded(&mut self, archive: UploadedArchive) -> Result<(), Error> {
        let txn = self.db.transaction().context(SqliteFailed)?;
        let uploaded_at = archive.uploaded_at.format_rfc3339();
        txn.execute(
            "INSERT INTO archives(key, uploaded_at) VALUES (?, ?)",
            params![archive.key.0, uploaded_at],
        )
        .context(SqliteFailed)?;

        {
            let id = txn.last_insert_rowid();
            let mut query = txn
                .prepare_cached("INSERT INTO contents(hash, archive) VALUES (?, ?)")
                .context(SqliteFailed)?;
            for file in archive.files {
                let hash = file.hash.map(|hash| hash.to_string());
                query.execute(params![hash, id]).context(SqliteFailed)?;
            }
        }

        txn.commit().context(SqliteFailed)
    }
}
