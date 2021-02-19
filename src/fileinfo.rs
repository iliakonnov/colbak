use crate::fileext::FileExtensions;
use crate::path::{EncodedPath, Local, PathKind};
use crate::types::Checksum;
use crate::DateTime;
use serde::{Deserialize, Serialize};
use std::fs::Metadata;
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::fs::File;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Info<K: PathKind, Kind = UnspecifiedInfo> {
    pub path: EncodedPath<K>,
    pub inode: u64,
    pub mode: u32,
    pub ctime: DateTime,
    pub mtime: DateTime,
    pub hash: Option<Checksum>,
    #[serde(flatten)]
    pub data: Kind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileInfo {
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DirInfo {}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct UnknownInfo {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnspecifiedInfo {
    File(FileInfo),
    Dir(DirInfo),
    Unknown(UnknownInfo),
}

pub enum InfoKind<P: PathKind> {
    File(Info<P, FileInfo>),
    Dir(Info<P, DirInfo>),
    Unknown(Info<P, UnknownInfo>),
}

#[repr(C)]
pub struct FileIdentifier {
    inode: u64,
    ctime: i128,
    size: u64,
    mtime: i128,
}

impl FileIdentifier {
    pub fn as_bytes(&self) -> &[u8] {
        let ptr = self as *const _ as *const _;
        unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<Self>()) }
    }
}

impl<K: PathKind> Info<K, FileInfo> {
    pub fn identifier(&self) -> FileIdentifier {
        FileIdentifier {
            inode: self.inode,
            ctime: self.ctime.unix_timestamp_nanos(),
            size: self.data.size,
            mtime: self.mtime.unix_timestamp_nanos(),
        }
    }
}

impl<P: PathKind> Info<P, UnspecifiedInfo> {
    pub fn size(&self) -> Option<u64> {
        match &self.data {
            UnspecifiedInfo::File(file) => Some(file.size),
            UnspecifiedInfo::Dir(_) => None,
            UnspecifiedInfo::Unknown(_) => None,
        }
    }

    pub fn turn(self) -> InfoKind<P> {
        match &self.data {
            UnspecifiedInfo::File(_) => InfoKind::File(self.into_file().unwrap()),
            UnspecifiedInfo::Dir(_) => InfoKind::Dir(self.into_dir().unwrap()),
            UnspecifiedInfo::Unknown(_) => InfoKind::Unknown(self.into_unknown().unwrap()),
        }
    }
}

macro_rules! conversion {
    (using $i:ident ($f:ident) from $t:ty) => {
        impl<P: PathKind> From<Info<P, $t>> for Info<P, UnspecifiedInfo> {
            fn from(x: Info<P, $t>) -> Self {
                Self {
                    path: x.path,
                    inode: x.inode,
                    mode: x.mode,
                    ctime: x.ctime,
                    mtime: x.mtime,
                    hash: x.hash,
                    data: UnspecifiedInfo::$i(x.data),
                }
            }
        }

        impl<P: PathKind> Info<P, UnspecifiedInfo> {
            pub fn $f(self) -> Result<Info<P, $t>, Self> {
                match self.data {
                    UnspecifiedInfo::$i(data) => Ok(Info {
                        data,
                        path: self.path,
                        inode: self.inode,
                        mode: self.mode,
                        ctime: self.ctime,
                        mtime: self.mtime,
                        hash: self.hash,
                    }),
                    _ => Err(self),
                }
            }
        }
    };
}
conversion!(using Dir (into_dir) from DirInfo);
conversion!(using File (into_file) from FileInfo);
conversion!(using Unknown (into_unknown) from UnknownInfo);

fn systime_to_datetime(x: Result<SystemTime, std::io::Error>) -> DateTime {
    match x {
        Ok(x) => DateTime::from(x),
        Err(_) => DateTime::from_unix_timestamp(std::i64::MIN),
    }
}

fn extract_kind(metadata: &Metadata) -> UnspecifiedInfo {
    if metadata.is_file() {
        UnspecifiedInfo::File(FileInfo {
            size: metadata.len(),
        })
    } else if metadata.is_dir() {
        UnspecifiedInfo::Dir(DirInfo {})
    } else {
        UnspecifiedInfo::Unknown(UnknownInfo {})
    }
}

impl Info<Local> {
    pub async fn new(local_path: PathBuf) -> Result<Self, tokio::io::Error> {
        let file = File::open(&local_path).await?;
        let metadata = file.metadata().await?;

        let path = EncodedPath::from_path(local_path);
        Ok(Info::with_metadata(path, metadata))
    }

    pub fn with_metadata(path: EncodedPath<Local>, metadata: Metadata) -> Self {
        Self {
            path,
            inode: metadata.inode(),
            mode: metadata.mode(),
            ctime: systime_to_datetime(metadata.created()),
            mtime: systime_to_datetime(metadata.modified()),
            data: extract_kind(&metadata),
            hash: None,
        }
    }
}
