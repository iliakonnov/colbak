use std::fs::Metadata;
use std::path::Path;
use std::time::SystemTime;

use tokio::fs::File;

use crate::*;
use crate::strings::bytes_to_osstr;
use crate::fileext::FileExtensions;
use std::fmt;

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: Vec<u8>,
    pub inode: u64,
    pub mode: u32,
    pub ctime: DateTime,
    pub mtime: DateTime,
    pub kind: FileKind,
    pub hash: Option<Checksum>,
}

impl FileInfo {
    pub fn size(&self) -> Option<u64> {
        match self.kind {
            FileKind::File { size } => Some(size),
            FileKind::Directory | FileKind::Unknown => None
        }
    }
}

fn systime_to_datetime(x: Result<SystemTime, std::io::Error>) -> DateTime {
    match x {
        Ok(x) => DateTime::from(x),
        Err(e) => DateTime::from_unix_timestamp(std::i64::MIN)
    }
}

fn extract_kind(metadata: &Metadata) -> FileKind {
    if metadata.is_file() {
        FileKind::File {
            size: metadata.len()
        }
    } else if metadata.is_dir() {
        FileKind::Directory
    } else {
        FileKind::Unknown
    }
}

impl FileInfo {
    pub async fn new(path: Vec<u8>) -> Result<Self, tokio::io::Error> {
        let real_path = bytes_to_osstr(&path).unwrap();
        let real_path = Path::new(&real_path);
        let file = File::open(real_path).await?;
        let metadata = file.metadata().await?;
        let res = FileInfo::with_metadata(path, metadata);
        Ok(res)
    }

    pub fn with_metadata(path: Vec<u8>, metadata: Metadata) -> Self {
        Self {
            path,
            inode: metadata.inode(),
            mode: metadata.mode(),
            ctime: systime_to_datetime(metadata.created()),
            mtime: systime_to_datetime(metadata.modified()),
            kind: extract_kind(&metadata),
            hash: None,
        }
    }
}
