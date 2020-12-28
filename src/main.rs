#![feature(type_alias_impl_trait)]

use std::path::PathBuf;
use time::OffsetDateTime as DateTime;
use async_trait::async_trait;
use rusoto_core::credential::{AwsCredentials, ProvideAwsCredentials, CredentialsError};
use serde::Deserialize;

mod fileinfo;
use fileinfo::FileInfo;

mod cpio;

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
async fn main() {
    use tokio::io::{AsyncWriteExt, AsyncReadExt};
    use tokio::fs::File;
    let mut archive = cpio::Archive::new();
    for i in std::env::args().skip(1) {
        let path = i.clone().into();
        let file = FileInfo::new(path).await.unwrap();
        archive.add(i, file);
    }
    let mut file = File::create("test.cpio").await.unwrap();
    archive.write(file).await.unwrap();
}
