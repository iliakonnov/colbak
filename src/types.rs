use async_trait::async_trait;
use rusoto_core::credential::{AwsCredentials, CredentialsError, ProvideAwsCredentials};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::path::PathBuf;
use crate::serde_b64;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    host: String,
    minimal_file_size: usize,
    locations: Vec<Location>,
    credentials: AwsAuth,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Location {
    from: PathBuf,
    exclude: Vec<PathBuf>,
    bucket: String,
    alias: String,
}

#[derive(Deserialize, Clone)]
pub struct AwsAuth(AwsCredentials);

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
pub struct Checksum(#[serde(with = "serde_b64")] pub [u8; 32]);

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
