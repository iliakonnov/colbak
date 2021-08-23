//! This module contains code related to storing snapshots and computing differences.
//! You won't find index of uploaded files here.

mod difference;
mod error;
mod index;
mod snapshot;

use error::*;

pub use {
    difference::{Diff, DiffRow, DiffType},
    error::Error,
    index::Database,
    snapshot::Snapshot,
};

use snafu::{ensure, Snafu};

/// `ROWID` from sqlite database.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct RowId(pub u64);

#[derive(Debug, Snafu)]
pub struct NotAValidSqlName {
    name: String,
}

/// Valid name that can be used in sql without additional actions.
/// Can be only created with [`SqlName::new`](SqlName::new)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlName(String);

impl SqlName {
    /// Validates name for correctness.
    ///
    /// 1. First character must be a letter
    /// 2. Following characters may letters, digits or an underscore
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

    /// Creates name with current date and time
    #[must_use]
    pub fn now() -> SqlName {
        let name = time::OffsetDateTime::now_utc().format("at%Y_%m_%d_%H_%M_%S_%N");
        // PANIC: name is completely correct, so it's safe to unwrap here.
        #[allow(clippy::unwrap_used)]
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

/// Generates combined id from `table_id` and `row_id`.
fn generate_id(table_id: u64, row_id: u64) -> Result<u64, Error> {
    // Single database can have up to 2^23 snapshots.
    // That's enough for 100 years of making snapshots every 6 minutes.
    // (it should be easy to reset the counter too)
    // Each snapshot can have up to 2^(63-23) = 2^40 of files.
    // That is 256 times more than NTFS supports.
    // So it should be enough for any practical use.
    ensure!(table_id < (1 << 23), TooManySnapshots);
    ensure!(row_id < (1 << 40), TooManyRows);
    Ok((table_id << 40) | row_id)
}
