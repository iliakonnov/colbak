#![feature(type_alias_impl_trait)]
#![cfg_attr(windows, feature(windows_by_handle))]

use std::path::PathBuf;
use time::OffsetDateTime as DateTime;
use async_trait::async_trait;
use rusoto_core::credential::{AwsCredentials, ProvideAwsCredentials, CredentialsError};
use serde::{Serialize, Deserialize};

mod fileinfo;
use fileinfo::Info;

mod strings;
mod cpio;
mod collector;
mod fileext;
mod database;

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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let res = collector::collect("./test", b"./test".to_vec())?;
    println!("{}", res);
    Ok(())
}
