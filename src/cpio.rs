use std::borrow::Cow;
use std::future::Future;
use std::mem::size_of;
use std::pin::Pin;
use std::task::{Poll, Waker};

use os_str_bytes::OsStrBytes;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::FileInfo;

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

fn u32_to_ascii(num: u32) -> [u8; 6] {
    // To fit into 6 bytes we need at least 41 different chars
    // For 5 bytes we need 85, but that is too much.
    let digits = b'0'..b'9'; // 10
    let upper = b'A'..b'Z'; // 25
    // 6 more chars:
    let additional = [b'-', b'+', b'!', b'=', b'_', b'#'];

    let alphabet = additional
        .iter()
        .copied()
        .chain(digits)
        .chain(upper)
        .rev()
        .collect::<Vec<u8>>();
    assert!(alphabet.len() >= 41);
    let mut result = [alphabet[0]; 6];
    let mut idx = 0;
    let mut num = num as usize;
    while num != 0 {
        let rem = num % alphabet.len();
        let div = num / alphabet.len();
        result[idx] = alphabet[rem];
        num = div;
        idx += 1;
    }
    result
}

fn crop_name(name: Cow<[u8]>) -> Cow<[u8]> {
    let max_length = (u16::MAX as usize) - size_of::<Metadata>() - 1;
    if name.len() <= max_length {
        return name;
    }

    let hash = xxhrs::XXH32::hash(&name);
    let hash = u32_to_ascii(hash);
    let ext_start = name.len() - 10;
    let dot = (&name[ext_start..])
        .iter()
        .rposition(|&x| x == b'.')
        .unwrap_or(name.len());
    let (name, extension) = name.split_at(dot);
    let space_available = max_length - extension.len() - hash.len();
    let name = &name[..space_available];
    let res = name
        .iter()
        .chain(hash.iter())
        .chain(extension.iter())
        .copied()
        .collect::<Vec<u8>>();
    Cow::Owned(res)
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
        let name = alias
            .or(info.path.to_str())
            .map(|s| Cow::Borrowed(s.as_bytes()))
            .unwrap_or_else(|| info.path.as_os_str().to_bytes());
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

impl Archive {
    pub fn new() -> Self {
        Archive {
            files: Vec::new()
        }
    }
    pub fn add(&mut self, alias: String, file: FileInfo) {
        self.files.push((alias, file));
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut dst: W) -> Result<(), tokio::io::Error> {
        use tokio::io::{AsyncWriteExt, AsyncReadExt};
        use tokio::fs::File;
        let mut buf = [0; 65536];

        for (alias, info) in &self.files {
            let header = CpioHeader::encode(Some(alias), info);
            dst.write_all(&header).await?;

            let mut file = File::open(&info.path).await?;
            let mut file_length = 0;
            loop {
                let len = file.read(&mut buf).await?;
                if len == 0 {
                    break;
                }
                dst.write_all(&buf[..len]).await?;
                file_length += len;
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
