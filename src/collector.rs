use crate::fileinfo::{DirInfo, FileInfo, Info, InfoKind, UnknownInfo};
use crate::strings::EncodedPath;
use crate::strings::{bytes_to_osstr, osstr_to_bytes};
use heed::types::{OwnedType, SerdeBincode};
use heed::zerocopy::{self, AsBytes, FromBytes, Unaligned};
use heed::{Database, Env};
use serde::{Deserialize, Serialize};
use std::backtrace::Backtrace;
use std::borrow::Cow;
use std::ffi::OsStr;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

type BEu64 = heed::zerocopy::U64<heed::byteorder::BigEndian>;
type Key = BEu64;
type KeyedDb<T> = Database<OwnedType<Key>, SerdeBincode<T>>;

trait KeyedDbExt<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn alloc(&self, txn: &mut heed::RwTxn, val: &T) -> heed::Result<Id<T>>;
}

impl<T> KeyedDbExt<T> for KeyedDb<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn alloc(&self, txn: &mut heed::RwTxn, val: &T) -> heed::Result<Id<T>> {
        let idx = match self.last(txn)? {
            Some((key, _)) => key.get() + 1,
            None => 0,
        };
        let idx = Key::new(idx);
        self.put(txn, &idx, val)?;
        Ok(Id {
            idx,
            _phantom: PhantomData::default(),
        })
    }
}

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
pub struct Id<T> {
    idx: Key,
    _phantom: PhantomData<T>,
}

impl<T> Serialize for Id<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.idx.get())
    }
}

impl<'de, T> Deserialize<'de> for Id<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let idx = <u64 as Deserialize<'de>>::deserialize(deserializer)?;
        Ok(Self {
            idx: Key::new(idx),
            _phantom: PhantomData::default(),
        })
    }
}

impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        Self {
            idx: self.idx,
            _phantom: PhantomData::default(),
        }
    }
}

impl<T> Copy for Id<T> {}

impl<T> AsRef<Key> for Id<T> {
    fn as_ref(&self) -> &Key {
        &self.idx
    }
}

impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.idx == other.idx
    }
}

impl<T> Eq for Id<T> {}

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

#[derive(Debug, Error)]
pub enum TreeError {
    #[error("provided node not found in the tree")]
    NonexistentNode,
    #[error("tree is corrupted")]
    Corrupt,
    #[error("database failed: {0}")]
    Database(#[from] heed::Error),
    #[error("looks like that file was already removed")]
    FileAlreadyRemoved,
    #[error("looks like that file was already added")]
    FileAlreadyAdded,
    #[error("invalid path provided: `{path}`")]
    InvalidPath {
        path: PathBuf,
        backtrace: Backtrace
    },
    #[error("path does not exists")]
    NoDirectoryExists,
    #[error("can't encode path: {0}")]
    ConversionFailed(#[from] os_str_bytes::EncodingError),
}

trait ResultExt<T>: Sized {
    fn map_none(self, x: TreeError) -> Result<T, TreeError>;

    fn or_corrupt(self) -> Result<T, TreeError> {
        self.map_none(TreeError::Corrupt)
    }

    fn or_nonexists(self) -> Result<T, TreeError> {
        self.map_none(TreeError::NonexistentNode)
    }
}

impl<T, E> ResultExt<T> for Result<Option<T>, E>
where
    E: Into<TreeError>,
{
    fn map_none(self, x: TreeError) -> Result<T, TreeError> {
        match self {
            Ok(Some(x)) => Ok(x),
            Ok(None) => Err(x),
            Err(e) => Err(e.into()),
        }
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
            .ok_or(TreeError::FileAlreadyRemoved)?;
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
        if !deleted {
            return Err(TreeError::Corrupt);
        }

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

    pub async fn to_json<W: AsyncWrite + Unpin>(
        &self,
        dst: &mut W,
    ) -> Result<(), JsonizationError> {
        let txn = self.env.read_txn()?;
        dst.write_all(b"{\n").await?;

        dst.write_all(b"\"files\": {\n").await?;
        let mut is_first = true;
        for i in self.files.iter(&txn)? {
            if !is_first {
                dst.write_all(b",\n").await?;
            }
            let (k, v) = i?;
            let file = serde_json::to_string_pretty(&v)?;
            dst.write_all(format!("\"{}\": {}", k.get(), file).as_bytes())
                .await?;
            is_first = false;
        }

        dst.write_all(b"\n}, \"dirs\": {\n").await?;
        let mut is_first = true;
        for i in self.directories.iter(&txn)? {
            if !is_first {
                dst.write_all(b",\n").await?;
            }
            let (k, v) = i?;
            let dir = serde_json::to_string_pretty(&v)?;
            dst.write_all(format!("\"{}\": {}", k.get(), dir).as_bytes())
                .await?;
            is_first = false;
        }

        dst.write_all(b"\n}, \"others\": {\n").await?;
        let mut is_first = true;
        for i in self.others.iter(&txn)? {
            if !is_first {
                dst.write_all(b",\n").await?;
            }
            let (k, v) = i?;
            let other = serde_json::to_string_pretty(&v)?;
            dst.write_all(format!("\"{}\": {}", k.get(), other).as_bytes())
                .await?;
            is_first = false;
        }

        dst.write_all(b"\n}}").await?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum JsonizationError {
    #[error("io error: {0}")]
    Io(#[from] tokio::io::Error),
    #[error("database error: {0}")]
    Db(#[from] heed::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

impl Tree {
    fn new<P: AsRef<Path>>(store: P) -> heed::Result<Self> {
        let env = heed::EnvOpenOptions::new().max_dbs(4).open(store)?;

        let directories = env.create_database(Some("dirs"))?;
        let files = env.create_database(Some("files"))?;
        let others = env.create_database(Some("others"))?;
        let sizes = env.create_database(Some("by_size"))?;

        let root: EncodedPath = Vec::new().into();
        let root = DirWrap {
            parent: None,
            dirs: vec![],
            files: vec![],
            info: DirectoryInfo {
                name: root.clone(),
                size: 0,
                info: Info::fake(root),
            },
        };
        let mut txn = env.write_txn()?;
        let root = directories.alloc(&mut txn, &root)?;
        txn.commit()?;

        Ok(Tree {
            env,
            directories,
            files,
            others,
            sizes,
            root,
        })
    }

    fn get_directory<P: AsRef<Path>>(&self, path: P) -> Result<Id<DirWrap>, TreeError> {
        let txn = self.env.read_txn()?;
        self._get_directory(&txn, path)
    }

    fn _get_directory<P: AsRef<Path>>(
        &self,
        txn: &heed::RoTxn,
        path: P,
    ) -> Result<Id<DirWrap>, TreeError> {
        let path = path.as_ref();

        let root = self
            .directories
            .get(&txn, self.root.as_ref())
            .or_corrupt()?;
        if root.info.name.0 == osstr_to_bytes(path.as_os_str())[..] {
            return Ok(self.root);
        }

        match path.parent() {
            None => Err(TreeError::InvalidPath {
                path: path.to_owned(),
                backtrace: Backtrace::capture()
            }),
            Some(parent) => {
                let filename = path
                    .file_name()
                    .ok_or_else(|| TreeError::InvalidPath {
                        path: path.to_owned(),
                        backtrace: Backtrace::capture(),
                    })?;
                let filename = osstr_to_bytes(filename);

                let parent_id = self._get_directory(txn, parent)?;

                // Trying to find a dir.
                let parent = self
                    .directories
                    .get(&txn, parent_id.as_ref())
                    .or_corrupt()?;
                for &i in &parent.dirs {
                    let d = self.directories.get(&txn, i.as_ref()).or_corrupt()?;
                    if d.info.name.0 == &filename[..] {
                        return Ok(i);
                    }
                }

                // Can't find such dir.
                Ok(self.root)
            }
        }
    }

    fn prepare_place<'a, 'b, Kind>(
        &'a self,
        txn: &heed::RoTxn,
        info: &'b Info<Kind>,
    ) -> Result<(Cow<'b, OsStr>, Id<DirWrap>, DirWrap), TreeError> {
        let path_cow = bytes_to_osstr(&info.path.0)?;
        let path = Path::new(&path_cow);
        let parent = path
            .parent()
            .ok_or_else(|| TreeError::InvalidPath {
                path: path.to_owned(),
                backtrace: Backtrace::capture(),
            })?;
        let parent = self._get_directory(txn, parent)?;
        let directory = self
            .directories
            .get(txn, parent.as_ref())
            .map_err(|err| err.into())
            .and_then(|opt| opt.ok_or_else(|| TreeError::InvalidPath {
                path: path.to_owned(),
                backtrace: Backtrace::capture()
            }))?;

        for &i in &directory.files {
            let file = self.files.get(txn, i.as_ref()).or_corrupt()?;
            if file.info.path == info.path {
                return Err(TreeError::FileAlreadyAdded);
            }
        }

        Ok((path_cow, parent, directory))
    }

    fn put_file(&mut self, info: Info<FileInfo>) -> Result<Id<FileWrap>, TreeError> {
        let mut txn = self.env.write_txn()?;
        let (_, parent, mut directory) = self.prepare_place(&txn, &info)?;
        let wrapped = FileWrap { parent, info };
        let res = self.files.alloc(&mut txn, &wrapped)?;
        directory.files.push(res);
        self.directories
            .put(&mut txn, parent.as_ref(), &directory)?;
        self.sizes.put(
            &mut txn,
            &BySizeKey {
                file_id: res.idx,
                size: heed::zerocopy::U64::new(wrapped.info.data.size),
            },
            &res,
        )?;
        txn.commit()?;
        Ok(res)
    }

    fn put_dir(&mut self, info: Info<DirInfo>) -> Result<Id<DirWrap>, TreeError> {
        let mut txn = self.env.write_txn()?;

        let (path, parent, mut directory) = self.prepare_place(&txn, &info)?;
        let path = Path::new(&path);
        let name = path
            .file_name()
            .ok_or_else(|| TreeError::InvalidPath {
                path: path.to_owned(),
                backtrace: Backtrace::capture(),
            })?;
        let res = self.directories.alloc(
            &mut txn,
            &DirWrap {
                parent: Some(parent),
                dirs: Vec::new(),
                files: Vec::new(),
                info: DirectoryInfo {
                    name: osstr_to_bytes(name).into_owned().into(),
                    size: 0,
                    info,
                },
            },
        )?;
        directory.dirs.push(res);
        self.directories
            .put(&mut txn, parent.as_ref(), &directory)?;
        txn.commit()?;
        Ok(res)
    }

    fn put_other(&mut self, info: &Info<UnknownInfo>) -> Result<(), TreeError> {
        let mut txn = self.env.write_txn()?;
        self.others.alloc(&mut txn, info)?;
        txn.commit()?;
        Ok(())
    }

    fn _fill_sizes(&self, txn: &mut heed::RwTxn, root_id: Id<DirWrap>) -> Result<u64, TreeError> {
        let root = self
            .directories
            .get(&txn, root_id.as_ref())
            .or_nonexists()?;
        let mut size = 0;
        for &i in &root.files {
            let f = self.files.get(&txn, i.as_ref()).or_corrupt()?;
            size += f.info.data.size;
        }
        let dirs = root.dirs.to_vec();
        for i in dirs {
            size += self._fill_sizes(txn, i)?;
        }

        let mut root = self
            .directories
            .get(&txn, root_id.as_ref())
            .or_nonexists()?;
        root.info.size = size;
        self.directories.put(txn, root_id.as_ref(), &root)?;
        Ok(size)
    }

    fn fill_sizes(&self, root_id: Id<DirWrap>) -> Result<u64, TreeError> {
        let mut txn = self.env.write_txn()?;
        let res = self._fill_sizes(&mut txn, root_id)?;
        txn.commit()?;
        Ok(res)
    }
}

#[derive(Debug, Error)]
pub enum CollectionError {
    #[error("something went wrong with tree: {error}")]
    Tree {
        #[from] error: TreeError,
        backtrace: Backtrace,
    },
    #[error("something went wrong while walking: {0}")]
    Walking(#[from] walkdir::Error),
    #[error("something went wrong when performing io: {0}")]
    Io(#[from] std::io::Error),
    #[error("something went wrong when working with storage: {0}")]
    Heed(#[from] heed::Error),
    #[error("can't extract filename from given path")]
    FilenameMissing,
}

pub fn collect<P: AsRef<Path>>(root: P) -> Result<Tree, CollectionError> {
    let root = root.as_ref();
    let walk = walkdir::WalkDir::new(&root)
        .into_iter();

    let root = match root.file_name() {
        Some(f) => f,
        None => root.as_os_str(),
    };
    let storage = std::env::current_dir()?.join("tree");
    let mut tree = Tree::new(storage)?;

    for i in walk {
        let i = i?;
        let meta = i.metadata()?;
        let path = i.path();
        let path = osstr_to_bytes(path.as_os_str()).into_owned().into();
        let info = Info::with_metadata(path, meta).turn();
        match info {
            InfoKind::File(file) => {
                tree.put_file(file)?;
            }
            InfoKind::Dir(dir) => {
                tree.put_dir(dir)?;
            }
            InfoKind::Unknown(other) => {
                tree.put_other(&other)?;
            }
        }
    }
    tree.fill_sizes(tree.root)?;

    Ok(tree)
}
