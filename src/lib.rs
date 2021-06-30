#![feature(
    min_type_alias_impl_trait,
    trait_alias,
    type_ascription,
    never_type,
    exhaustive_patterns,
    min_specialization,
    try_blocks,
    arbitrary_enum_discriminant,
    format_args_capture,
    backtrace
)]
#![cfg_attr(windows, feature(windows_by_handle))]
#![allow(dead_code, unused_macros)]
#![warn(clippy::pedantic)]
#![deny(
    // This project should never panic.
    // This single lint is much simpler to deny than unwrap_used, expect_used and few others.
    clippy::missing_panics_doc
)]
#![allow(
    // I will write docs later:
    clippy::missing_errors_doc,
    // I know better, what is readable. This project does not have any long literals really.
    clippy::unreadable_literal,
    // For me, it's better to make as many arms, as many variants in enum.
    clippy::match_same_arms,
    // Again, I do not think that this lint is needed.
    clippy::module_name_repetitions,
    // This lint is useful, but too annoying
    clippy::wildcard_imports,
)]

use snafu::Snafu;
use std::path::{Path, PathBuf};
pub use time::OffsetDateTime as DateTime;

#[macro_use]
pub mod logging;

pub mod database;
pub mod cpio;
pub mod fileext;
pub mod fileinfo;
pub mod path;
pub mod serde_b64;
pub mod types;

type CommandResult = Result<(), TopError>;

#[derive(Debug, Snafu)]
pub enum TopError {
    #[snafu(context(false))]
    TokioIo { source: tokio::io::Error },
    #[snafu(context(false))]
    CpioReadingError { source: cpio::reader::ReadingError },
    #[snafu(context(false))]
    CpioReadError { source: cpio::reader::ReadError },
    #[snafu(context(false))]
    DbOpenError { source: database::Error },
    #[snafu(context(false))]
    InvalidSnapshotName {
        source: database::NotAValidSqlName,
    },
}

pub async fn create_cpio(dst: PathBuf, files: Vec<PathBuf>) -> CommandResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut archive = cpio::Archive::new();
    for f in files {
        let info = fileinfo::Info::new(f).await?;
        archive.add(info);
    }
    let mut src = archive.read();
    let mut dst = tokio::fs::File::create(dst).await?;
    let mut buf = vec![0; 1024];
    loop {
        let len = src.read(&mut buf).await?;
        if len == 0 {
            break;
        }
        let slice = &buf[..len];
        dst.write_all(slice).await?;
    }
    Ok(())
}

pub async fn extract_cpio(src: PathBuf, _dst: PathBuf) -> CommandResult {
    use cpio::reader::NextItem;
    use tokio::fs::File;
    let archive = File::open(src).await?;
    let mut reader = cpio::Reader::new(archive);
    let files = loop {
        reader = match reader.advance().await? {
            NextItem::File(f) => {
                println!("{:#?}", f.info());
                f.skip().await?
            }
            NextItem::End(e) => break e,
        };
    };
    println!("{:#?}", files);
    Ok(())
}

pub fn create_snapshot(db: PathBuf, root: &Path) -> CommandResult {
    use database::{Database, SqlName};
    let mut db = Database::open(db)?;
    let name = SqlName::now();
    let mut snap = db.open_snapshot(name)?;
    snap.filler()?.fill(root)?.save()?;
    println!("{}", snap.name());
    Ok(())
}

pub fn diff_snapshot(db: PathBuf, before: String, after: String) -> CommandResult {
    use database::{Database, SqlName};
    let db = Database::open(db)?;

    let before = db.readonly_snapshot(SqlName::new(before)?)?;
    let after = db.readonly_snapshot(SqlName::new(after)?)?;
    let diff = db.compare_snapshots(&before, &after)?;
    let _ = diff.for_each::<_, !>(|row| {
        println!("{:?}", row);
        Ok(())
    })?;

    Ok(())
}
