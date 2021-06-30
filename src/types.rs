use crate::serde_b64;
use serde::{Deserialize, Serialize};
use sha2::Digest;

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
