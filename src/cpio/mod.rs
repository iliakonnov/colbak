use crate::fileinfo::{Info, UnspecifiedInfo};
use crate::strings::*;
use crate::types::Checksum;
use pending::Pending;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::borrow::Cow;
use std::mem::size_of;
mod pending;
mod write_proxy;
mod writer;

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
    pub fn trailer(content: &[u8]) -> Vec<u8> {
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
        if content.is_empty() {
            res.push(0);
        } else {
            res.extend_from_slice(content);
        }
        res
    }

    pub fn encode(alias: Option<&EncodedPath>, info: &Info) -> Vec<u8> {
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
        let name = alias.map(|x| x.as_ref()).unwrap_or(&info.path.0);
        let name = crop_name_to(Cow::Borrowed(name), u16::MAX - 1);
        let namesize = name.len() + 1;
        debug_assert!(namesize <= u16::MAX as usize);

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

        let mut result = Vec::with_capacity(size_of::<CpioHeader>() + namesize + 1);
        result.extend_from_slice(&header);
        result.extend_from_slice(&name);
        result.push(0);
        if namesize % 2 != 0 {
            result.push(0);
        }
        result
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Archive {
    files: Vec<Pending>,
}

#[derive(Snafu, Debug)]
pub enum ArchivingError {
    #[snafu(display("something went wrong when performing IO: {}", source))]
    IoFailed { source: tokio::io::Error },
    #[snafu(display(
        "real hash differs from specified (expected {}, found {})",
        expected,
        found
    ))]
    HashMismatch { expected: Checksum, found: Checksum },
    #[snafu(display("unable to encode filename: {}", source))]
    InvalidFilename { source: os_str_bytes::EncodingError },
}

impl Archive {
    pub fn new() -> Self {
        Archive { files: Vec::new() }
    }

    pub fn add(&mut self, alias: EncodedPath, file: Info) {
        self.files.push(Pending::new(file, alias));
    }

    pub fn trailer(&self) -> Vec<u8> {
        let content = serde_json::to_vec(self).unwrap_or_default();
        CpioHeader::trailer(&content)
    }

    pub fn read<'a>(&'a mut self) -> impl tokio::io::AsyncRead + 'a {
        let machine = writer::Machine::new(self);
        let engine = write_proxy::MachineBuffer::new(machine);
        engine
    }
}
