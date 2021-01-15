use std::borrow::Cow;
use std::ffi::OsStr;
use std::path::Path;

use crate::fileinfo::{Info, InfoKind};
use crate::strings::{bytes_to_osstr, osstr_to_bytes};

use snafu::{Backtrace, ResultExt, Snafu};

use super::*;

impl Tree {
    fn get_directory<P: AsRef<Path>>(
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
            None => InvalidPath { path }.fail()?,
            Some(parent) => {
                let filename = path.file_name().context(InvalidPath { path })?;
                let filename = osstr_to_bytes(filename);

                let parent_id = self.get_directory(txn, parent)?;

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
                InvalidPath { path }.fail()?
            }
        }
    }

    fn prepare_place<'a, 'b, Kind>(
        &'a self,
        txn: &heed::RoTxn,
        info: &'b Info<Kind>,
    ) -> Result<(Cow<'b, OsStr>, Id<DirWrap>, DirWrap), TreeError> {
        let path_cow = bytes_to_osstr(&info.path.0).context(ConversionFailed {})?;
        let path = Path::new(&path_cow);
        let parent = path.parent().context(InvalidPath { path })?;
        let parent = self
            .get_directory(txn, parent)
            .with_context(|| RecursionFailed::new(path.to_owned()))?;
        let directory = self
            .directories
            .get(txn, parent.as_ref())
            .map_err(|e| e.into())
            .and_then(|opt| opt.context(InvalidPath { path }))?;

        for &i in &directory.files {
            let file = self.files.get(txn, i.as_ref()).or_corrupt()?;
            if file.info.path == info.path {
                return FileAlreadyAdded {}.fail();
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
        let name = path.file_name().context(InvalidPath { path })?;
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
            size += self
                ._fill_sizes(txn, i)
                .with_context(|| RecursionFailed::new(root_id))?;
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

#[derive(Debug, Snafu)]
pub enum CollectionError {
    #[snafu(context(false))]
    TreeFailed {
        source: TreeError,
        backtrace: Backtrace,
    },
    #[snafu(context(false))]
    WalkingFailed {
        source: walkdir::Error,
        backtrace: Backtrace,
    },
    #[snafu(context(false))]
    IoFailed {
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(context(false))]
    HeedFailed {
        source: heed::Error,
        backtrace: Backtrace,
    },
}

pub async fn collect<P: AsRef<Path>>(root: P) -> Result<Tree, CollectionError> {
    let root = root.as_ref();
    let walk = walkdir::WalkDir::new(&root).into_iter();

    let storage = std::env::current_dir()?.join("tree");
    let mut tree = Tree::build(storage)?.empty()?;

    for i in walk {
        let i = i?;
        let path = i.into_path();
        let info = Info::new(path).await?;
        match info.turn() {
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
