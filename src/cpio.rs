use thiserror::*;
use std::mem::size_of;
use tokio::io::AsyncWrite;

use crate::{FileInfo, Checksum};
use crate::strings::*;
use std::borrow::Cow;
use std::path::Path;

#[derive(Debug)]
#[repr(C)]
struct CpioHeader {
    _magic: u16,
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

#[derive(Debug)]
#[repr(C)]
struct Metadata {
    _zero: u8,
    inode: u64,
    ctime: i128,
    mtime: i128,
    hash: u128,
}

fn convert_u32(n: u32) -> [u16; 2] {
    let lower = n & 0x0000_FFFF;
    let higher = n >> 16;
    [higher as u16, lower as u16]
}

fn convert_timestamp(x: i64) -> [u16; 2] {
    if x <= 0 {
        [0, 0]
    } else if x <= std::u32::MAX as i64 {
        convert_u32(x as u32)
    } else {
        [0xFFFF, 0xFFFF]
    }
}


impl CpioHeader {
    fn trailer() -> Vec<u8> {
        let name = b"TRAILER!!!\0";
        let header = CpioHeader {
            _magic: 0o070707,
            dev_ino: [0, 0],
            mode: 0,
            uid: 0,
            gid: 0,
            nlink: 0,
            rdev: 0,
            mtime: [0, 0],
            namesize: name.len() as u16,
            filesize: [0, 0],
        };
        let header: [u8; size_of::<CpioHeader>()] = unsafe { std::mem::transmute(header) };

        let mut res = Vec::with_capacity(size_of::<CpioHeader>() + name.len() + 1);
        res.extend_from_slice(&header);
        res.extend_from_slice(name);
        res.push(0);
        res
    }

    fn encode(alias: Option<&str>, info: &FileInfo) -> Vec<u8> {
        let mode;
        let nlink;
        let filesize;
        match info.kind {
            crate::FileKind::File { size } => {
                mode = 0o0100000;
                nlink = 1;
                filesize = size;
            }
            crate::FileKind::Directory => {
                mode = 0o0040000;
                nlink = 2;
                filesize = 0;
            }
            crate::FileKind::Unknown => {
                mode = 0o0020000;
                nlink = 0;
                filesize = 0;
            }
        };
        let mode = mode | (info.mode & (!0o0170000));
        let name = alias.map(str_to_bytes)
            .unwrap_or_else(|| Cow::Borrowed(&info.path));
        let name = crop_name(name);
        let namesize = name.len() + size_of::<Metadata>();

        let header = CpioHeader {
            _magic: 0o070707,
            dev_ino: convert_u32(info.inode as _),
            mode: mode as u16,
            uid: 0,
            gid: 0,
            nlink,
            rdev: 0,
            mtime: convert_timestamp(info.mtime.unix_timestamp()),
            namesize: namesize as u16,
            filesize: convert_u32(filesize as u32),
        };
        let header: [u8; size_of::<CpioHeader>()] = unsafe { std::mem::transmute(header) };

        let metadata = Metadata {
            _zero: 0u8,
            inode: info.inode as u64,
            ctime: info.ctime.unix_timestamp_nanos(),
            mtime: info.mtime.unix_timestamp_nanos(),
            hash: info.hash.unwrap_or_default().0,
        };
        let metadata: [u8; size_of::<Metadata>()] = unsafe { std::mem::transmute(metadata) };

        let mut result = Vec::with_capacity(size_of::<CpioHeader>() + namesize + 1);
        result.extend_from_slice(&header);
        result.extend_from_slice(&name);
        result.extend_from_slice(&metadata);
        if namesize % 2 != 0 {
            result.push(0);
        }
        result
    }
}

pub struct Archive {
    files: Vec<(String, FileInfo)>,
}

#[derive(Error, Debug)]
pub enum ArchivingError {
    #[error("something went wrong when performing IO: {0}")]
    Io(#[from] tokio::io::Error),
    #[error("real hash differs from specified (expected {expected}, found {found})")]
    HashMismatch {
        expected: Checksum,
        found: Checksum,
    },
    #[error("unable to encode filename: {0}")]
    InvalidFilename(#[from] os_str_bytes::EncodingError),
}

impl Archive {
    pub fn new() -> Self {
        Archive {
            files: Vec::new()
        }
    }
    pub fn add(&mut self, alias: String, file: FileInfo) {
        self.files.push((alias, file));
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut dst: W) -> Result<(), ArchivingError> {
        use tokio::io::{AsyncWriteExt, AsyncReadExt};
        use tokio::fs::File;
        let mut buf = [0; 65536];

        for (alias, info) in &self.files {
            let header = CpioHeader::encode(Some(alias), info);
            dst.write_all(&header).await?;

            let real_path = bytes_to_osstr(&info.path)?;
            let real_path = Path::new(&real_path);
            let mut file = File::open(real_path).await?;
            let mut file_length = 0;
            let mut hash = xxhrs::XXH3_128::new();
            loop {
                let len = file.read(&mut buf).await?;
                if len == 0 {
                    break;
                }
                let buf = &buf[..len];
                dst.write_all(buf).await?;
                hash.write(buf);
                file_length += len;
            }

            let checksum = Checksum::from(hash);
            if let Some(expected) = info.hash {
                if expected != checksum {
                    return Err(ArchivingError::HashMismatch {
                        expected,
                        found: checksum,
                    });
                }
            }

            if file_length % 2 != 0 {
                dst.write_all(&[0]).await?;
            }
        }

        let trailer = CpioHeader::trailer();
        dst.write_all(&trailer).await?;

        Ok(())
    }
}
