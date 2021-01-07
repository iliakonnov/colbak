#![feature(type_alias_impl_trait)]
#![cfg_attr(windows, feature(windows_by_handle))]

use std::path::PathBuf;
use time::OffsetDateTime as DateTime;
use async_trait::async_trait;
use rusoto_core::credential::{AwsCredentials, ProvideAwsCredentials, CredentialsError};
use serde::Deserialize;

mod fileinfo;
use fileinfo::FileInfo;

mod strings;
mod cpio;
mod collector;
mod fileext;

#[derive(Debug, Deserialize, Clone)]
struct Config {
    host: String,
    minimal_file_size: usize,
    locations: Vec<Location>,
    credentials: AwsAuth,
}

#[derive(Debug, Deserialize, Clone)]
struct Location {
    from: PathBuf,
    exclude: Vec<PathBuf>,
    bucket: String,
    alias: String,
}

#[derive(Deserialize, Clone)]
struct AwsAuth(AwsCredentials);

impl std::fmt::Debug for AwsAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AwsAuth { ... }")
    }
}

#[async_trait]
impl ProvideAwsCredentials for AwsAuth {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        Ok(self.0.clone())
    }
}

#[derive(Debug, Clone)]
pub enum FileKind {
    File {
        size: u64
    },
    Directory,
    Unknown
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Checksum(u128);

impl From<xxhrs::XXH3_128<'_>> for Checksum {
    fn from(x: xxhrs::XXH3_128) -> Checksum {
        Checksum(x.finish())
    }
}

impl std::fmt::Display for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
         f.write_fmt(format_args!("{{{:x}}}", self.0))
    }
}

struct Stored<T> {
    stored_at: DateTime,
    etag: String,
    key: String,
    data: T,
}

enum Pack {
    Single(FileInfo),
    Many(Vec<FileInfo>)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let res = collector::collect("./test")?;
    println!("{}", res);
    Ok(())
}
