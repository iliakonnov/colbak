#![feature(type_alias_impl_trait, backtrace, type_ascription, never_type, min_specialization)]
#![cfg_attr(windows, feature(windows_by_handle))]
#![allow(dead_code)]

use std::path::PathBuf;
pub use time::OffsetDateTime as DateTime;


#[macro_use]
mod logging;

pub mod cpio;
pub mod fileext;
pub mod fileinfo;
pub mod path;
pub mod serde_b64;
pub mod types;
mod backup;

type CommandResult = Result<(), Box<dyn std::error::Error>>;

pub async fn create_cpio(dest: PathBuf, files: Vec<PathBuf>) -> CommandResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut archive = cpio::Archive::new();
    for f in files {
        let info = fileinfo::Info::new(f).await?;
        archive.add(info);
    }
    let mut src = archive.read();
    let mut dst = tokio::fs::File::create(dest).await?;
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
