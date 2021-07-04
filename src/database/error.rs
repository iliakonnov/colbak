use snafu::Snafu;

use super::{NotAValidSqlName, SqlName};

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(super)")]
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
