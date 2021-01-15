#![feature(type_alias_impl_trait, backtrace, type_ascription)]
#![cfg_attr(windows, feature(windows_by_handle))]
#![allow(dead_code)]

use std::path::PathBuf;
use time::OffsetDateTime as DateTime;
use async_trait::async_trait;
use rusoto_core::credential::{AwsCredentials, ProvideAwsCredentials, CredentialsError};
use serde::{Serialize, Deserialize};

mod fileinfo;
use fileinfo::Info;
use sha2::Digest;

mod serialization;
mod strings;
mod cpio;
mod tree;
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

// 256-bit hash
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Checksum([u8; 32]);

impl<D: Digest> From<D> for Checksum {
    fn from(x: D) -> Checksum {
        let fin = x.finalize();
        let mut arr = [0; 32];
        arr.copy_from_slice(&fin[0..32]);
        Checksum(arr)
    }
}

impl std::fmt::Display for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("{")?;
        for i in &self.0 {
            f.write_fmt(format_args!("{:x}", i))?;
        }
        f.write_str("}")?;
        Ok(())
    }
}

async fn entry_point() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = entry_point().await {
        eprintln!("ERROR!");
        eprintln!("{}", e);

        if let Some(trace) = e.backtrace() {
            eprintln!("\nTRACE:");
            eprintln!("{}", trace);
        } else {
            eprintln!("\nTrace missing :(");
        }
    }
}
