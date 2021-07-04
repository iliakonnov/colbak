#[macro_use]
mod state_machine;
mod pending;
pub mod reader;
mod smart_read;
mod writer;

use crate::fileinfo::{Info, UnspecifiedInfo};
use crate::DateTime;
use pending::Pending;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::mem::size_of;

use crate::path::{EncodedPath, External, Local, PathKind};
pub use reader::Reader;

/// This is header of old binary format. See [`man 5 cpio`](http://man.he.net/man5/cpio) for details.
#[derive(Debug)]
#[repr(C)]
pub struct CpioHeader {
    /// Should always be equal to [`MAGIC`](MAGIC) (`0o070707`).
    magic: u16,
    /// Stores both `dev` and `ino` fields of cpio format.
    /// But we do not need to store `dev` and more important `inode` takes 4 bytes.
    /// So in this format `inode` spans over both `dev` and `ino`
    dev_ino: [u16; 2],
    mode: u16,
    uid: u16,
    gid: u16,
    /// Number of links. This archiver does not aware of hard links, so this field is mostly useless.
    /// It is set to `1` for files, to `2` for directories and `0` for everything else.
    nlink: u16,
    /// `rdev` field is used for storing higher bits of file size.
    /// This allows us to decode files up to 2^48 = 256TB, but is not supported by normal archivers.
    rdev: u16,
    mtime: [u16; 2],
    /// Length of path should not exceed 65536 bytes, and that matches maximum path length on Windows.
    namesize: u16,
    /// Unfortunately, old cpio format works bad with files larger that 4GB.
    /// They should not be archived using cpio when possible.
    filesize: [u16; 2],
}

const MAGIC: u16 = 0o070707;
const TRAILER: &[u8] = b"TRAILER!!!\0";

// FIXME: Isn't it better to just use TRAILER.len()? Will it hurt performance so much?
#[allow(clippy::cast_possible_truncation)] // Obviously TRAILER.len() fits into u16.
const TRAILER_LEN: u16 = TRAILER.len() as u16;

/// Splits single `u32` into two `u16`.
#[must_use]
pub fn convert_u32(n: u32) -> [u16; 2] {
    let lower = n & 0x0000_FFFF;
    let higher = n >> 16;
    #[allow(clippy::cast_possible_truncation)]
    [higher as u16, lower as u16]
}

/// Reverse of [`convert_u32`](convert_u32)
#[must_use]
pub fn decode_u32(x: [u16; 2]) -> u32 {
    let (higher, lower) = (u32::from(x[0]), u32::from(x[1]));
    (higher << 16) | lower
}

/// Encodes unix timestamp into two bytes.
/// Any date between 1970.01.01 and 2106.02.07 will be stored without any losses.
/// Date outside of this range will be represented as `[0, 0]` or `[0xFFFF, 0xFFFF]`
#[must_use]
pub fn encode_timestamp(x: i64) -> [u16; 2] {
    match x {
        x if x <= 0 => [0, 0],
        x if x > u32::MAX.into() => [u16::MAX, u16::MAX],
        x => {
            // We already handled these cases in other arms
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            convert_u32(x as u32)
        }
    }
}

impl CpioHeader {
    /// Generates `TRAILER!!!` entry that marks end of archive.
    /// Can be followed by any given content, most normal archivers will handle it without major issues.
    #[must_use]
    pub fn trailer(content: &[u8]) -> Vec<u8> {
        let header = CpioHeader {
            magic: MAGIC,
            dev_ino: [0, 0],
            mode: 0,
            uid: 0,
            gid: 0,
            nlink: 0,
            rdev: 0,
            mtime: [0, 0],
            namesize: TRAILER_LEN,
            filesize: [0, 0],
        };
        let header = header.into_array();

        let mut res =
            Vec::with_capacity(size_of::<CpioHeader>() + TRAILER.len() + 1 + content.len());
        res.extend_from_slice(&header);
        res.extend_from_slice(TRAILER);
        if TRAILER_LEN % 2 != 0 {
            res.push(0);
        }
        res.extend_from_slice(content);
        res
    }

    /// Returns true when current entry is an `TRAILER!!!` entry.
    #[must_use]
    pub fn is_trailer(&self, name: &[u8]) -> bool {
        matches!(
            self,
            CpioHeader {
                mode: 0,
                namesize: TRAILER_LEN,
                filesize: [0, 0],
                ..
            }
        ) && name == TRAILER
    }

    /// Creates header from given info.
    /// Note that provided name may be different form one stored in info.
    /// Name must not exceed 65535 bytes,
    #[must_use]
    fn from_info<K: PathKind>(info: &Info<K>, name: &[u8]) -> Self {
        let mode;
        let nlink;
        let filesize;
        match &info.data {
            UnspecifiedInfo::File(file) => {
                mode = 0o0100000;
                nlink = 1;
                filesize = file.size;
            }
            UnspecifiedInfo::Dir(_) => {
                mode = 0o0040000;
                nlink = 2;
                filesize = 0;
            }
            UnspecifiedInfo::Unknown(_) => {
                mode = 0o0020000;
                nlink = 0;
                filesize = 0;
            }
        };
        let mode = mode | (info.mode & (!0o0170000));

        // Name should include NUL byte, but it is not included in `name`.
        let namesize = name.len() + 1;
        debug_assert!(u16::try_from(namesize).is_ok());

        let max_normal_size = u64::from(u32::MAX);
        let rdev = filesize >> 32;
        let filesize = filesize & max_normal_size;
        // Now maximum file size is 2^(32 + 16) = 2^48 = 256 TB

        // Unfortunately, there is no much we can do.
        // cpio does not support large integers, so let's simply truncate them.
        #[allow(clippy::cast_possible_truncation)]
        CpioHeader {
            magic: MAGIC,
            dev_ino: convert_u32(info.inode as u32),
            mode: mode as u16,
            uid: 0,
            gid: 0,
            nlink,
            rdev: rdev as u16,
            mtime: encode_timestamp(info.modified_at.unix_timestamp()),
            namesize: namesize as u16,
            filesize: convert_u32(filesize as u32),
        }
    }

    /// Creates header for given info, correctly attaches filename and returns everything.
    #[must_use]
    pub fn encode<K: PathKind>(info: &Info<K>) -> Vec<u8> {
        let name = info.path.crop_name_to(u16::MAX - 1);

        let header = Self::from_info(info, &name);
        let namesize = header.namesize as usize;
        let header = header.into_array();

        let mut result = Vec::with_capacity(size_of::<CpioHeader>() + namesize + 1);
        result.extend_from_slice(&header);
        result.extend_from_slice(&name);
        // Name always ends with NUL byte, and if the namesize is odd, an additional NUL byte should be added.
        result.push(0);
        if namesize % 2 != 0 {
            result.push(0);
        }
        result
    }

    /// Decodes header from provided array, checking for correct magic.
    #[must_use]
    pub fn decode(data: [u8; size_of::<CpioHeader>()]) -> Option<Self> {
        let header: CpioHeader = unsafe { std::mem::transmute(data) };
        match header.magic {
            MAGIC => Some(header),
            _ => None,
        }
    }

    /// Encodes header into byte array.
    #[must_use]
    pub fn into_array(self) -> [u8; size_of::<Self>()] {
        unsafe { std::mem::transmute(self) }
    }

    /// Decodes full size from different fields.
    #[must_use]
    pub fn size(&self) -> u64 {
        let higher = u64::from(self.rdev);
        let lower = u64::from(decode_u32(self.filesize));
        (higher << 32) | lower
    }

    /// Extracts info from header, using provided name.
    #[must_use]
    pub fn info(&self, name: &[u8]) -> Info<External> {
        use crate::fileinfo::{DirInfo, FileInfo, UnknownInfo};

        debug_assert_eq!(self.namesize as usize - 1, name.len());

        let kind = self.mode & 0o0170000;
        let mode = self.mode & 0o0000777;
        let data = match kind {
            0o0100000 => UnspecifiedInfo::File(FileInfo { size: self.size() }),
            0o0040000 => UnspecifiedInfo::Dir(DirInfo {}),
            _ => UnspecifiedInfo::Unknown(UnknownInfo {}),
        };
        Info {
            path: EncodedPath::from_vec(name.to_vec()),
            inode: decode_u32(self.dev_ino).into(),
            mode: mode.into(),
            created_at: DateTime::from_unix_timestamp(0),
            modified_at: DateTime::from_unix_timestamp(decode_u32(self.mtime).into()),
            hash: None,
            data,
        }
    }
}

/// Pending cpio archive, waiting for be written.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Archive {
    files: Vec<Pending>,
}

impl Archive {
    #[must_use]
    pub fn new() -> Self {
        Archive { files: Vec::new() }
    }

    /// Adds file to the archive by it's path.
    pub fn add(&mut self, file: Info<Local>) {
        self.files.push(Pending::new(file));
    }

    /// Generates trailer with custom json-serialized metadata.
    #[must_use]
    pub fn trailer(&self) -> Vec<u8> {
        let mut infos = Vec::with_capacity(self.files.len());
        for pending in &self.files {
            let mut info = pending.info.clone();
            info.hash = pending.calculated.or(info.hash);
            infos.push(info);
        }
        let content = serde_json::to_vec(&infos).unwrap_or_default();
        CpioHeader::trailer(&content)
    }

    /// Returns `AsyncRead` over contents of this archive.
    pub fn read(&mut self) -> impl tokio::io::AsyncRead + '_ {
        writer::Reader::new(self)
    }
}

impl Default for Archive {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn timestamp_small() {
        assert_eq!(encode_timestamp(-42), [0, 0]);
    }

    #[test]
    fn timestamp_normal() {
        assert_eq!(encode_timestamp(10), [0, 10]);
        assert_eq!(encode_timestamp(0x60DB_8840), [0x60DB, 0x8840]);
    }

    #[test]
    fn timestamp_big() {
        assert_eq!(encode_timestamp(i64::from(u32::MAX) + 42), [0xFFFF, 0xFFFF]);
    }
}
