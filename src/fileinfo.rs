use crate::fileext::FileExtensions;
use crate::strings::osstr_to_bytes;
use crate::strings::EncodedPath;
use crate::types::Checksum;
use crate::DateTime;
use serde::{Deserialize, Serialize};
use std::fs::Metadata;
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::fs::File;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Info<Kind = UnspecifiedInfo> {
    pub path: EncodedPath,
    pub local_path: Option<PathBuf>,
    pub inode: u64,
    pub mode: u32,
    pub ctime: DateTime,
    pub mtime: DateTime,
    pub hash: Option<Checksum>,
    pub data: Kind,
}

impl<Kind> Info<Kind>
where
    Kind: Default,
{
    pub fn fake(path: EncodedPath) -> Self {
        Self {
            path,
            local_path: None,
            inode: 0,
            mode: 0,
            ctime: DateTime::from_unix_timestamp(0),
            mtime: DateTime::from_unix_timestamp(0),
            hash: None,
            data: Kind::default(),
        }
    }
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

pub enum InfoKind {
    File(Info<FileInfo>),
    Dir(Info<DirInfo>),
    Unknown(Info<UnknownInfo>),
}

impl Info<UnspecifiedInfo> {
    pub fn size(&self) -> Option<u64> {
        match &self.data {
            UnspecifiedInfo::File(file) => Some(file.size),
            UnspecifiedInfo::Dir(_) => None,
            UnspecifiedInfo::Unknown(_) => None,
        }
    }

    pub fn turn(self) -> InfoKind {
        match &self.data {
            UnspecifiedInfo::File(_) => InfoKind::File(self.into_file().unwrap()),
            UnspecifiedInfo::Dir(_) => InfoKind::Dir(self.into_dir().unwrap()),
            UnspecifiedInfo::Unknown(_) => InfoKind::Unknown(self.into_unknown().unwrap()),
        }
    }
}

macro_rules! conversion {
    (using $i:ident ($f:ident) from $t:ty) => {
        impl From<Info<$t>> for Info<UnspecifiedInfo> {
            fn from(x: Info<$t>) -> Self {
                Self {
                    path: x.path,
                    local_path: x.local_path,
                    inode: x.inode,
                    mode: x.mode,
                    ctime: x.ctime,
                    mtime: x.mtime,
                    hash: x.hash,
                    data: UnspecifiedInfo::$i(x.data),
                }
            }
        }

        impl Info<UnspecifiedInfo> {
            pub fn $f(self) -> Result<Info<$t>, Self> {
                match self.data {
                    UnspecifiedInfo::$i(data) => Ok(Info {
                        data,
                        path: self.path,
                        local_path: self.local_path,
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

impl Info {
    pub async fn new(local_path: PathBuf) -> Result<Self, tokio::io::Error> {
        let file = File::open(&local_path).await?;
        let metadata = file.metadata().await?;

        let path = osstr_to_bytes(local_path.as_os_str()).to_vec();
        let mut res = Info::with_metadata(path.into(), metadata);
        res.local_path = Some(local_path);
        Ok(res)
    }

    pub fn with_metadata(path: EncodedPath, metadata: Metadata) -> Self {
        Self {
            path,
            local_path: None,
            inode: metadata.inode(),
            mode: metadata.mode(),
            ctime: systime_to_datetime(metadata.created()),
            mtime: systime_to_datetime(metadata.modified()),
            data: extract_kind(&metadata),
            hash: None,
        }
    }
}
