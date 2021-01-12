use crate::fileinfo::{DirInfo, FileInfo, Info, UnknownInfo};
use crate::strings::EncodedPath;
use heed::types::{OwnedType, SerdeBincode};
use heed::zerocopy::{self, AsBytes, FromBytes, Unaligned};
use heed::{Database, Env};
use serde::{Deserialize, Serialize};
use snafu::Backtrace;
use snafu::{ensure, OptionExt, Snafu};
use std::path::PathBuf;

mod db;
use db::*;

mod collector;

mod json;

pub use collector::collect;

pub struct Tree {
    env: Env,
    directories: KeyedDb<DirWrap>,
    files: KeyedDb<FileWrap>,
    sizes: Database<OwnedType<BySizeKey>, SerdeBincode<Id<FileWrap>>>,
    others: KeyedDb<Info<UnknownInfo>>,
    root: Id<DirWrap>,
}

#[derive(Debug, Clone, Copy, AsBytes, FromBytes, Unaligned)]
#[repr(C)]
pub struct BySizeKey {
    size: heed::zerocopy::U64<heed::byteorder::BigEndian>,
    file_id: heed::zerocopy::U64<heed::byteorder::BigEndian>,
}

#[derive(Debug)]
pub struct File {
    id: Id<FileWrap>,
}

impl File {
    fn new(id: Id<FileWrap>) -> Self {
        Self { id }
    }
}

#[derive(Debug)]
pub struct Directory {
    id: Id<DirWrap>,
}

impl Directory {
    fn new(id: Id<DirWrap>) -> Self {
        Self { id }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DirWrap {
    parent: Option<Id<DirWrap>>,
    dirs: Vec<Id<DirWrap>>,
    files: Vec<Id<FileWrap>>,
    info: DirectoryInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DirectoryInfo {
    name: EncodedPath,
    size: u64,
    info: Info<DirInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileWrap {
    parent: Id<DirWrap>,
    info: Info<FileInfo>,
}

#[derive(Debug, Snafu)]
pub enum TreeError {
    #[snafu(display("provided node not found in the tree"))]
    NonexistentNode {},
    #[snafu(display("tree is corrupted"))]
    Corrupted {},
    #[snafu(context(false))]
    DatabaseFailed { source: heed::Error },
    #[snafu(display("looks like that file was already removed"))]
    FileAlreadyRemoved {},
    #[snafu(display("looks like that file was already added"))]
    FileAlreadyAdded {},
    #[snafu(display("invalid path provided: {:?}", path))]
    InvalidPath { path: PathBuf, backtrace: Backtrace },
    #[snafu(display("path does not exists"))]
    NoDirectoryExists {},
    #[snafu(display("can't encode path: {}", source))]
    ConversionFailed { source: os_str_bytes::EncodingError },
    #[snafu(display("error when computing recursive function ({:?}): {}", info, source))]
    RecursionFailed {
        info: Box<dyn std::fmt::Debug + 'static>,
        #[snafu(source(from(TreeError, Box::new)))]
        source: Box<TreeError>,
    },
}

impl RecursionFailed<Box<dyn std::fmt::Debug + 'static>> {
    fn new<T: std::fmt::Debug + 'static>(info: T) -> Self {
        Self {
            info: Box::new(info),
        }
    }
}
trait TreeResultExt<T>: Sized {
    fn or_corrupt(self) -> Result<T, TreeError>;

    fn or_nonexists(self) -> Result<T, TreeError>;
}

impl<T, E> TreeResultExt<T> for Result<Option<T>, E>
where
    E: Into<TreeError>,
{
    fn or_corrupt(self) -> Result<T, TreeError> {
        self.map_err(|e| e.into())
            .and_then(|opt| opt.context(Corrupted {}))
    }

    fn or_nonexists(self) -> Result<T, TreeError> {
        self.map_err(|e| e.into())
            .and_then(|opt| opt.context(NonexistentNode {}))
    }
}

impl Tree {
    pub fn remove(&mut self, file: File) -> Result<(), TreeError> {
        let mut txn = self.env.write_txn()?;
        let id = file.id;
        let file: FileWrap = self.files.get(&txn, id.as_ref()).or_nonexists()?;

        let dirkey = file.parent.as_ref();
        let mut dir = self.directories.get(&txn, dirkey).or_corrupt()?;

        let idx = dir
            .files
            .iter()
            .position(|x| *x == id)
            // Probably we can siply ignore that error
            .context(FileAlreadyRemoved {})?;
        dir.files.remove(idx);
        self.directories.put(&mut txn, dirkey, &dir)?;

        let deleted = self.sizes.delete(
            &mut txn,
            &BySizeKey {
                file_id: id.idx,
                size: heed::zerocopy::U64::new(file.info.data.size),
            },
        )?;
        // This error may be ignored too
        ensure!(deleted, Corrupted {});

        txn.commit()?;

        Ok(())
    }

    pub fn root(&self) -> Directory {
        Directory::new(self.root)
    }

    pub fn get(&self, file: &File) -> Result<Info<FileInfo>, TreeError> {
        let txn = self.env.read_txn()?;
        self.files
            .get(&txn, file.id.as_ref())
            .or_nonexists()
            .map(|x| x.info)
    }

    pub fn parent(&self, file: &File) -> Result<Directory, TreeError> {
        let txn = self.env.read_txn()?;
        self.files
            .get(&txn, file.id.as_ref())
            .or_nonexists()
            .map(|x| Directory::new(x.parent))
    }
}
