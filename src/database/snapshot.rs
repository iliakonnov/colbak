use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::path::Path;

use rusqlite::named_params;
use rusqlite::params;
use snafu::ResultExt;

use crate::fileinfo::FileIdentifier;
use crate::fileinfo::Info;
use crate::path::EncodedPath;

use super::error::*;
use super::index::Database;
use super::SqlName;

/// Snapshot of filesystem at one moment
///
/// Stores the list of files with their metadata.
pub struct Snapshot<D: Borrow<Database>> {
    pub(super) db: D,
    pub(super) name: SqlName,
}

/// Simple struct that allows filling snapshot with files.
/// 
/// Note that if [`save()`](Self::save) is not called, transaction will be rolled back.
#[must_use]
pub struct SnapshotFiller<'a> {
    snap_name: &'a SqlName,
    transaction: rusqlite::Transaction<'a>,
}

impl<'a> SnapshotFiller<'a> {
    fn new<D: BorrowMut<Database>>(snapshot: &'a mut Snapshot<D>) -> Result<Self, Error> {
        let mut txn = snapshot
            .db
            .borrow_mut()
            .conn
            .transaction()
            .context(SqliteFailed)?;
        txn.set_drop_behavior(rusqlite::DropBehavior::Rollback);
        Ok(SnapshotFiller {
            snap_name: &snapshot.name,
            transaction: txn,
        })
    }

    fn get_statement(&self) -> Result<rusqlite::CachedStatement, Error> {
        let sql = fmt_sql!(
            "INSERT INTO {0}.snap(path, identifier, info, size)
            VALUES(:path, :identifier, :info, :size)",
            &self.snap_name
        );
        self.transaction.prepare_cached(&sql).context(SqliteFailed)
    }

    /// Adds new entry to snapshot directly from [`walkdir::DirEntry`](walkdir::DirEntry).
    pub fn add(&self, entry: walkdir::DirEntry) -> Result<(), Error> {
        let metadata = entry.metadata().context(CantWalkdir)?;
        let path = EncodedPath::from_path(entry.into_path());
        let info = Info::with_metadata(path, &metadata);
        self.get_statement()?.execute(named_params![
            ":path": info.path.as_bytes(),
            ":identifier": info.identifier().as_ref().map(FileIdentifier::as_bytes).unwrap_or_default(),
            ":info": serde_json::to_string(&info).context(JsonFailed)?,
            ":size": info.size()
        ])
        .context(SqliteFailed)?;
        Ok(())
    }

    /// Must be called after snapshot is filled.
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

    /// Walk given directory, putting each file into snapshot.
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
