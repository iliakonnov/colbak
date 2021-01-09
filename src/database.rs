use heed::{EnvOpenOptions, Database as Db, PolyDatabase as PolyDb};
use heed::types::*;
use time::OffsetDateTime as DateTime;
use serde::{Serialize, Deserialize};
use crate::Checksum;
use crate::fileinfo::Info;
use std::path::Path;
use crate::collector::Tree;
use std::collections::HashMap;

// 256-bit
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EncryptionKey([u8; 32]);

impl EncryptionKey {
    fn encode(&self) -> String {
        base64::encode(&self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileSpecifier {
    pub size: u64,
    pub hash: Option<Checksum>,
    pub inode: u64,
    pub ctime: DateTime,
    pub mtime: DateTime,
}

impl FileSpecifier {
    pub fn cmp_hashes(&self, other: &Self) -> Option<bool> {
        if self.size != other.size {
            return Some(false);
        }
        match (self.hash, other.hash) {
            (Some(a), Some(b)) => Some(a == b),
            _ => None
        }
    }

    pub fn cmp_same_filesystem(&self, other: &Self) -> bool {
        self.cmp_hashes(other).unwrap_or(true)
            && self.inode == other.inode
            && self.ctime == other.ctime
            && self.mtime == other.mtime
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stored {
    stored_at: DateTime,
    encryption: EncryptionKey,
    key: String,
    etag: String,
    data: Pack,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Pack {
    Single(FileSpecifier),
    Many(Vec<FileSpecifier>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeNode {
    file: Info,
    when: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KnownFile {
    Stored {
        id: u64
    },
    Located {
        path: Vec<u8>
    }
}

pub struct Database {
    env: heed::Env,
    stored: Db<OwnedType<u64>, SerdeBincode<Stored>>,
    tree: Db<OwnedType<u64>, SerdeBincode<TreeNode>>,
    known: Db<SerdeBincode<FileSpecifier>, SerdeBincode<KnownFile>>,
    meta: PolyDb,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlainDatabase {
    stored: HashMap<u64, Stored>,
    tree: HashMap<u64, TreeNode>,
    known: HashMap<FileSpecifier, KnownFile>,
    when: u64,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, heed::Error> {
        let env = EnvOpenOptions::new().open(&path)?;
        Ok(Self {
            stored: env.create_database(Some("stored"))?,
            tree: env.create_database(Some("tree"))?,
            known: env.create_database(Some("known"))?,
            meta: env.create_poly_database(Some("meta"))?,
            env,
        })
    }

    pub fn to_plain(&self) -> Result<PlainDatabase, heed::Error> {
        let txn = self.env.read_txn()?;
        let stored = self.stored.iter(&txn)?.collect::<Result<_, _>>()?;
        let tree = self.tree.iter(&txn)?.collect::<Result<_, _>>()?;
        let known = self.known.iter(&txn)?.collect::<Result<_, _>>()?;
        let when = self.get_when(&txn)?;
        Ok(PlainDatabase {
            stored,
            tree,
            known,
            when
        })
    }

    fn get_when<T>(&self, txn: &heed::RoTxn<'_, T>) -> Result<u64, heed::Error> {
        Ok(self.meta.get::<_, Str, OwnedType<u64>>(txn, "idx")?.unwrap_or_default())
    }

    fn inc_when<T>(&self, txn: &mut heed::RwTxn<'_, '_, T>, when: u64) -> Result<(), heed::Error> {
        self.meta.put::<_, Str, OwnedType<u64>>(txn, "idx", &(when+1))
    }

    pub fn store_tree(&self, tree: &Tree) -> Result<(), heed::Error> {
        let mut txn = self.env.write_txn()?;
        let when = self.get_when(&txn)?;
        let mut key = self.tree.len(&txn)? as u64;
        for file in tree.iter_all() {
            self.tree.put(&mut txn, &key, &TreeNode {
                file,
                when,
            })?;
            key += 1;
        }
        txn.commit()?;
        Ok(())
    }
}
