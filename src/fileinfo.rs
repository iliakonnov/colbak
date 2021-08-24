// This module is painful to unit-test: `Info` struct is too large to construct manually.

use crate::fileext::FileExtensions;
use crate::path::{EncodedPath, Local, PathKind};
use crate::types::Checksum;
use crate::DateTime;
use serde::{Deserialize, Serialize};
use std::fs::Metadata;
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::fs::File;

/// Stores generic information about some object in filesystem: file or directory or whatever.
/// More specific information is stored in `data` field.
///
/// Available kinds:
/// - [`UnspecifiedInfo`](UnspecifiedInfo) (default)
/// - [`FileInfo`](FileInfo)
/// - [`DirInfo`](DirInfo)
/// - [`UnknownInfo`](UnknownInfo)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(serialize = "Kind: Serialize", deserialize = "Kind: Deserialize<'de>"))]
pub struct Info<P: PathKind, Kind = UnspecifiedInfo> {
    pub path: EncodedPath<P>,
    /// Somewhat unique file id. Used when computing [identifier](FileIdentifier)
    pub inode: u64,
    /// Unix-like access mode.
    pub mode: u32,
    pub user_id: u32,
    pub group_id: u32,
    pub created_at: DateTime,
    pub modified_at: DateTime,
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

/// Used when you need to store Info for different kinds under the same type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum UnspecifiedInfo {
    File(FileInfo),
    Dir(DirInfo),
    Unknown(UnknownInfo),
}

/// Stores kind of info _outside_, unlike [`UnspecifiedInfo`](UnspecifiedInfo) type that stores it's _inside_.
#[repr(u8, C)]
pub enum InfoKind<P: PathKind> {
    File(Info<P, FileInfo>) = 1,
    Dir(Info<P, DirInfo>) = 2,
    Unknown(Info<P, UnknownInfo>) = u8::MAX,
}

/// Unique file identifier, that identifies **contents** of the type, not the filename.
/// Includes fields that are extremely likely to change when content changes.
///
/// This identifier is used to find what files are really changed, it is good enough to do it reliably.
/// (at least it's not worse than looking at `modified_at`, and many popular are doing just that)
#[repr(C)]
pub struct FileIdentifier {
    inode: u64,
    ctime: i128,
    size: u64,
    mtime: i128,
}

impl FileIdentifier {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        let ptr = (self as *const Self).cast::<u8>();
        unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<Self>()) }
    }
}

impl<P: PathKind> Info<P, UnspecifiedInfo> {
    /// Creates identifier from Info, when possible.
    ///
    /// It is currently possible only if Info stores information about the file,
    /// not a directory or something else.
    #[must_use]
    pub fn identifier(&self) -> Option<FileIdentifier> {
        match self.clone().turn() {
            InfoKind::File(f) => Some(f.identifier()),
            InfoKind::Dir(_) => None,
            InfoKind::Unknown(_) => None,
        }
    }
}

impl<P: PathKind> Info<P, FileInfo> {
    /// Creates identifier from file-related Info.
    #[must_use]
    pub fn identifier(&self) -> FileIdentifier {
        FileIdentifier {
            inode: self.inode,
            ctime: self.created_at.unix_timestamp_nanos(),
            size: self.data.size,
            mtime: self.modified_at.unix_timestamp_nanos(),
        }
    }
}

impl<P: PathKind> Info<P, UnspecifiedInfo> {
    /// Returns size of file, or None when it is not a file.
    #[must_use]
    pub fn size(&self) -> Option<u64> {
        match &self.data {
            UnspecifiedInfo::File(file) => Some(file.size),
            UnspecifiedInfo::Dir(_) => None,
            UnspecifiedInfo::Unknown(_) => None,
        }
    }

    /// Turns from `Info<UnspecifiedInfo>` (enum is inside) to `InfoKind` (enum is outside).
    #[allow(clippy::unwrap_used)]
    #[must_use]
    pub fn turn(self) -> InfoKind<P> {
        // PANIC: This function does not panic, since it always converting to correct variant
        match &self.data {
            UnspecifiedInfo::File(_) => InfoKind::File(self.into_file().unwrap()),
            UnspecifiedInfo::Dir(_) => InfoKind::Dir(self.into_dir().unwrap()),
            UnspecifiedInfo::Unknown(_) => InfoKind::Unknown(self.into_unknown().unwrap()),
        }
    }
}

impl<P: PathKind, K> Info<P, K> {
    /// Changes kind of underlying path. Use with caution.
    ///
    /// See also: [`Path::cast`]
    pub fn cast<T: PathKind>(self) -> Info<T, K> {
        Info {
            path: self.path.cast(),
            inode: self.inode,
            mode: self.mode,
            user_id: self.user_id,
            group_id: self.group_id,
            created_at: self.created_at,
            modified_at: self.modified_at,
            hash: self.hash,
            data: self.data,
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
                    user_id: x.user_id,
                    group_id: x.group_id,
                    created_at: x.created_at,
                    modified_at: x.modified_at,
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
                        user_id: self.user_id,
                        group_id: self.group_id,
                        created_at: self.created_at,
                        modified_at: self.modified_at,
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

/// Converts `SystemTime` to normal `DateTime`, falling back to
/// `-9999` year when provided with Err variant (minumum supported date by `time` crate).
///
/// # Examples
///
/// `std::time::SystemTime::UNIX_EPOCH`
/// ```
/// # use colbak_lib::fileinfo::systime_to_datetime;
/// # use std::time::SystemTime;
/// # use time::{PrimitiveDateTime, macros::{date, time}};
/// let datetime = systime_to_datetime::<()>(Ok(SystemTime::UNIX_EPOCH));
/// assert_eq!(datetime, PrimitiveDateTime::new(date!(1970-01-01), time!(00:00:00)).assume_utc());
/// ```
///
/// `Err` variant:
/// ```
/// # use colbak_lib::fileinfo::systime_to_datetime;
/// # use time::{PrimitiveDateTime, macros::{date, time}};
/// let datetime = systime_to_datetime(Err("something went very wrong"));
/// assert_eq!(datetime, PrimitiveDateTime::new(date!(-9999-01-01), time!(00:00:00)).assume_utc());
/// ```
#[allow(clippy::needless_pass_by_value)] // False-positive
pub fn systime_to_datetime<E>(x: Result<SystemTime, E>) -> DateTime {
    const MIN: DateTime = time::Date::MIN.midnight().assume_utc();
    x.map_or_else(|_| MIN, DateTime::from)
}

/// Extracts specific information about the file from metadata given by OS.
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
        Ok(Info::with_metadata(path, &metadata))
    }

    #[must_use]
    pub fn with_metadata(path: EncodedPath<Local>, metadata: &Metadata) -> Self {
        Self {
            path,
            inode: metadata.inode(),
            mode: metadata.mode(),
            user_id: metadata.user_id(),
            group_id: metadata.group_id(),
            created_at: systime_to_datetime(metadata.created()),
            modified_at: systime_to_datetime(metadata.modified()),
            data: extract_kind(metadata),
            hash: None,
        }
    }
}
