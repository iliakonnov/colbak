use std::fs::FileType;
use std::fs::Metadata;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::SystemTime;

use tokio::fs::File;

use crate::*;

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: PathBuf,
    pub inode: u64,
    pub mode: u32,
    pub ctime: DateTime,
    pub mtime: DateTime,
    pub kind: FileKind,
    pub hash: Option<Checksum>
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
    pub async fn new(path: PathBuf) -> Result<Self, tokio::io::Error> {
        let file = File::open(&path).await?;
        let metadata = file.metadata().await?;
        let res = FileInfo::with_metadata(path, metadata);
        Ok(res)
    }

    pub fn with_metadata(path: PathBuf, metadata: Metadata) -> Self {
        Self {
            path,
            inode: metadata.ino(),
            mode: metadata.mode(),
            ctime: systime_to_datetime(metadata.created()),
            mtime: systime_to_datetime(metadata.modified()),
            kind: extract_kind(&metadata),
            hash: None,
        }
    }
}
