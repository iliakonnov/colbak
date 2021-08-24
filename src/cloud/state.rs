use std::path::Path;

use rusqlite::params;
use snafu::{Backtrace, ResultExt, Snafu};

use crate::cpio::Archive;
use crate::fileinfo::Info;
use crate::path::{External, Local};
use crate::utils::Utils;
use crate::DateTime;

use super::{CloudProvider, Key};

#[derive(Snafu)]
pub enum Error<C: CloudProvider> {
    SqliteFailed {
        source: rusqlite::Error,
        backtrace: Backtrace,
    },
    CloudFailed {
        source: C::Error,
        backtrace: Backtrace,
    },
}

impl<C: CloudProvider> std::fmt::Debug for Error<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SqliteFailed { source, backtrace } => f
                .debug_struct("SqliteFailed")
                .field("source", source)
                .field("backtrace", backtrace)
                .finish(),
            Self::CloudFailed { source, backtrace } => f
                .debug_struct("CloudFailed")
                .field("source", source)
                .field("backtrace", backtrace)
                .finish(),
        }
    }
}

/// Stores state of remote cloud provider.
pub struct State<C: CloudProvider> {
    db: rusqlite::Connection,
    cloud: C,
}

pub struct UploadedArchive {
    pub key: Key,
    pub files: Vec<Info<External>>,
    pub uploaded_at: DateTime,
}

impl<C: CloudProvider> State<C> {
    pub fn fake<P: AsRef<Path>>(
        path: P,
    ) -> Result<State<super::FakeCloud>, Error<super::FakeCloud>> {
        State::open(path)
    }

    /// Opens database file at specified path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error<C>> {
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
        let cloud = C::new();
        Ok(State { db, cloud })
    }

    /// Puts information about uploaded archive to the database.
    pub fn set_uploaded(&mut self, archive: UploadedArchive) -> Result<(), Error<C>> {
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

    /// Uploads given files to the cloud.
    pub async fn upload(&mut self, files: Vec<Info<Local>>) -> Result<(), Error<C>> {
        let mut archive = Archive::new();
        for f in files.clone() {
            archive.add(f);
        }

        let reader = archive.read();
        let key = self.cloud.upload(reader).await.context(CloudFailed)?;

        let files = files.into_iter().map(Info::cast).collect();
        let uploaded = UploadedArchive {
            key,
            files,
            uploaded_at: DateTime::now_utc(),
        };
        self.set_uploaded(uploaded)?;
        Ok(())
    }
}
