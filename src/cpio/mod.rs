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

#[derive(Debug)]
#[repr(C)]
struct CpioHeader {
    magic: u16,
    //dev: u16,
    //ino: u16,
    dev_ino: [u16; 2],
    mode: u16,
    uid: u16,
    gid: u16,
    nlink: u16,
    rdev: u16,
    mtime: [u16; 2],
    namesize: u16,
    filesize: [u16; 2],
}

const MAGIC: u16 = 0o070707;
const TRAILER: &[u8] = b"TRAILER!!!\0";

// FIXME: Isn't it better to just use TRAILER.len()? Will it hurt performance so much?
#[allow(clippy::cast_possible_truncation)] // Obviously TRAILER.len() fits into u16.
const TRAILER_LEN: u16 = TRAILER.len() as u16;

fn convert_u32(n: u32) -> [u16; 2] {
    let lower = n & 0x0000_FFFF;
    let higher = n >> 16;
    #[allow(clippy::cast_possible_truncation)]
    [higher as u16, lower as u16]
}

fn decode_u32(x: [u16; 2]) -> u32 {
    let (higher, lower) = (u32::from(x[0]), u32::from(x[1]));
    (higher << 16) | lower
}

fn encode_timestamp(x: i64) -> [u16; 2] {
    match x {
        x if x <= 0 => [0, 0],
        x if x > u32::MAX.into() => [u16::MAX, u16::MAX],
        x => {
            // We already handled these cases in different arms
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            convert_u32(x as u32)
        }
    }
}

impl CpioHeader {
    pub fn trailer(content: &[u8]) -> Vec<u8> {
        let header = CpioHeader {
            magic: 0o070707,
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
        let header: [u8; size_of::<CpioHeader>()] = unsafe { std::mem::transmute(header) };

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

    pub fn encode<K: PathKind>(info: &Info<K>) -> Vec<u8> {
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
        let name = info.path.crop_name_to(u16::MAX - 1);
        let namesize = name.len() + 1;
        debug_assert!(u16::try_from(namesize).is_ok());

        let max_normal_size = u64::from(u32::MAX);
        let rdev = filesize >> 32;
        let filesize = filesize & max_normal_size;
        // Now maximum file size is 2^(32 + 16) = 2^48 = 256 TB

        // Unfortunately, there is no much we can do.
        // cpio does not support large integers, so let's simply truncate them.
        #[allow(clippy::cast_possible_truncation)]
        let header = CpioHeader {
            magic: MAGIC,
            dev_ino: convert_u32(info.inode as u32),
            mode: mode as u16,
            uid: 0,
            gid: 0,
            nlink,
            rdev: rdev as u16,
            mtime: encode_timestamp(info.mtime.unix_timestamp()),
            namesize: namesize as u16,
            filesize: convert_u32(filesize as u32),
        };
        let header: [u8; size_of::<CpioHeader>()] = unsafe { std::mem::transmute(header) };

        let mut result = Vec::with_capacity(size_of::<CpioHeader>() + namesize + 1);
        result.extend_from_slice(&header);
        result.extend_from_slice(&name);
        result.push(0);
        if namesize % 2 != 0 {
            result.push(0);
        }
        result
    }

    pub fn decode(data: [u8; size_of::<CpioHeader>()]) -> Option<Self> {
        let header: CpioHeader = unsafe { std::mem::transmute(data) };
        match header.magic {
            MAGIC => Some(header),
            _ => None,
        }
    }

    pub fn size(&self) -> u64 {
        let higher = u64::from(self.rdev);
        let lower = u64::from(decode_u32(self.filesize));
        (higher << 32) | lower
    }

    pub fn info(&self, name: &[u8]) -> Info<External> {
        use crate::fileinfo::{DirInfo, FileInfo, UnknownInfo};
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
            ctime: DateTime::from_unix_timestamp(0),
            mtime: DateTime::from_unix_timestamp(decode_u32(self.mtime).into()),
            hash: None,
            data,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Archive {
    files: Vec<Pending>,
}

impl Archive {
    #[must_use]
    pub fn new() -> Self {
        Archive { files: Vec::new() }
    }

    pub fn add(&mut self, file: Info<Local>) {
        self.files.push(Pending::new(file));
    }

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

    pub fn read(&mut self) -> impl tokio::io::AsyncRead + '_ {
        writer::Reader::new(self)
    }
}

impl Default for Archive {
    fn default() -> Self {
        Self::new()
    }
}
