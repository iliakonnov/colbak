use crate::serde_b64;
use digest::generic_array::{ArrayLength, GenericArray};
use serde::{Deserialize, Serialize};

const LENGTH: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Checksum(#[serde(with = "serde_b64")] pub [u8; LENGTH]);

impl<OutputSize: ArrayLength<u8>> From<GenericArray<u8, OutputSize>> for Checksum {
    fn from(fin: GenericArray<u8, OutputSize>) -> Checksum {
        let mut arr = [0; LENGTH];
        let min_length = LENGTH.min(fin.len());
        arr[..min_length].copy_from_slice(&fin[..min_length]);
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

impl std::fmt::Debug for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}
