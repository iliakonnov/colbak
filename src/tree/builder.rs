use std::path::Path;

use heed::types::{OwnedType, SerdeBincode};
use heed::{Database, Env};

use crate::fileinfo::{Info, UnknownInfo};
use crate::strings::EncodedPath;

use super::db::{Id, KeyedDb, KeyedDbExt};
use super::{BySizeKey, DirWrap, DirectoryInfo, FileWrap, Tree, TreeError, TreeResultExt};

pub struct TreeBuilder {
    env: Env,
    directories: KeyedDb<DirWrap>,
    files: KeyedDb<FileWrap>,
    sizes: Database<OwnedType<BySizeKey>, SerdeBincode<Id<FileWrap>>>,
    others: KeyedDb<Info<UnknownInfo>>,
}

impl Tree {
    pub fn build<P: AsRef<Path>>(path: P) -> heed::Result<TreeBuilder> {
        let env = heed::EnvOpenOptions::new().max_dbs(4).open(path)?;

        let directories = env.create_database(Some("dirs"))?;
        let files = env.create_database(Some("files"))?;
        let others = env.create_database(Some("others"))?;
        let sizes = env.create_database(Some("by_size"))?;

        Ok(TreeBuilder {
            env,
            directories,
            files,
            others,
            sizes,
        })
    }
}

fn simple_root() -> DirWrap {
    let root: EncodedPath = vec![].into();
    DirWrap {
        parent: None,
        dirs: vec![],
        files: vec![],
        info: DirectoryInfo {
            name: root.clone(),
            size: 0,
            info: Info::fake(root),
        },
    }
}

impl TreeBuilder {
    pub fn open(self) -> Result<Tree, TreeError> {
        let txn = self.env.read_txn()?;
        let key = Id::first();
        let root: DirWrap = self.directories.get(&txn, &key.idx).or_corrupt()?;
        if simple_root() != root {
            return super::Corrupted {}.fail();
        }
        txn.commit()?;
        Ok(self.with_root(key))
    }

    pub fn empty(self) -> Result<Tree, TreeError> {
        let mut txn = self.env.write_txn()?;
        self.directories.clear(&mut txn)?;
        self.files.clear(&mut txn)?;
        self.others.clear(&mut txn)?;
        self.sizes.clear(&mut txn)?;

        let root = simple_root();
        let root = self.directories.alloc(&mut txn, &root)?;
        txn.commit()?;

        Ok(self.with_root(root))
    }

    fn with_root(self, root: Id<DirWrap>) -> Tree {
        Tree {
            env: self.env,
            directories: self.directories,
            files: self.files,
            sizes: self.sizes,
            others: self.others,
            root,
        }
    }
}
