use std::path::Path;
use std::fmt;
use crate::fileinfo::FileInfo;
use id_arena::{Arena, Id};
use thiserror::Error;
use std::collections::VecDeque;
use crate::strings::{osstr_to_bytes, bytes_to_osstr};
use crate::FileKind;
use crate::fileext::FileExtensions;


#[derive(Debug, Clone)]
pub struct Tree {
    directories: Arena<DirWrap>,
    files: Arena<FileWrap>,
    root: Id<DirWrap>,
}

#[derive(Debug)]
pub struct File {
    id: Id<FileWrap>
}

impl File {
    fn new(id: Id<FileWrap>) -> Self {
        Self { id }
    }
}

#[derive(Debug)]
pub struct Directory {
    id: Id<DirWrap>
}

impl Directory {
    fn new(id: Id<DirWrap>) -> Self {
        Self { id }
    }
}

#[derive(Debug, Clone)]
struct DirWrap {
    parent: Option<Id<DirWrap>>,
    dirs: Vec<Id<DirWrap>>,
    files: Vec<Id<FileWrap>>,
    info: DirectoryInfo,
}

impl FileInfo {
    fn fmt(&self, f: &mut fmt::Formatter, indent: usize) -> fmt::Result {
        let indent = " ".repeat(indent);
        let path = bytes_to_osstr(&self.path)
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|e| format!("{{Err: {}}}", e));
        f.write_fmt(format_args!("{}+ {} {{\n", indent, path))?;
        f.write_fmt(format_args!("{}    size: {:?}\n", indent, self.size()))?;
        f.write_fmt(format_args!("{}    hash: {:?}\n", indent, self.hash))?;
        f.write_fmt(format_args!("{}}}\n", indent))?;
        Ok(())
    }
}

impl DirWrap {
    fn fmt(&self, tree: &Tree, f: &mut fmt::Formatter, indent: usize) -> fmt::Result {
        let indent_s = " ".repeat(indent);
        let name = bytes_to_osstr(&self.info.name)
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|e| format!("{{Err: {}}}", e));
        f.write_fmt(format_args!("{}{}/ ({}) {{\n", indent_s, name, self.info.size))?;
        for &file in &self.files {
            match tree.files.get(file) {
                None => f.write_fmt(format_args!("{}    {{Corrupted!}}", indent_s))?,
                Some(wrap) => wrap.info.fmt(f, indent + 4)?
            }
        }
        for &dir in &self.dirs {
            match tree.directories.get(dir) {
                None => f.write_fmt(format_args!("{}    {{Corrupted!}}\n", indent_s))?,
                Some(wrap) => wrap.fmt(tree, f, indent + 4)?
            }
        }
        f.write_fmt(format_args!("{}}}\n", indent_s))?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct DirectoryInfo {
    name: Vec<u8>,
    size: u64,
}

#[derive(Debug, Clone)]
struct FileWrap {
    parent: Id<DirWrap>,
    info: FileInfo,
}

#[derive(Debug, Clone, Error)]
pub enum TreeError {
    #[error("provided node not found in the tree")]
    NonexistentNode,
    #[error("tree is corrupted")]
    Corrupt,
    #[error("looks like that file was already removed")]
    FileAlreadyRemoved,
    #[error("looks like that file was already added")]
    FileAlreadyAdded,
    #[error("invalid path provided")]
    InvalidPath,
}

impl Tree {
    pub fn remove(&mut self, file: File) -> Result<(), TreeError> {
        let id = file.id;
        let file = self.files.get(id)
            .ok_or(TreeError::NonexistentNode)?;
        let dir = self.directories.get_mut(file.parent)
            .ok_or(TreeError::Corrupt)?;

        // Probably we can simply ignore that error
        let idx = dir.files
            .iter()
            .position(|x| *x == id)
            .ok_or(TreeError::FileAlreadyRemoved)?;
        dir.files.remove(idx);

        Ok(())
    }

    pub fn root(&self) -> Directory {
        Directory::new(self.root)
    }

    /// Sorted by increasing size
    pub fn by_size(&self, root: Directory) -> Result<Vec<(File, &FileInfo)>, TreeError> {
        let mut files = self.files(root)?;
        files.sort_unstable_by_key(|x| x.1.size());
        Ok(files)
    }

    /// Sorted by increasing depth
    pub fn files(&self, root: Directory) -> Result<Vec<(File, &FileInfo)>, TreeError> {
        let mut res = Vec::new();
        let mut frontier = VecDeque::new();
        frontier.push_back(root.id);

        if self.directories.get(root.id).is_none() {
            return Err(TreeError::NonexistentNode);
        }

        while let Some(current) = frontier.pop_front() {
            let dir = self.directories.get(current)
                .ok_or(TreeError::Corrupt)?;
            for &id in &dir.files {
                let file = self.files.get(id)
                    .ok_or(TreeError::Corrupt)?;
                res.push((id, &file.info));
            }
            for &i in &dir.dirs {
                frontier.push_back(i);
            }
        }

        Ok(res.into_iter()
            .map(|(x, y)| (File::new(x), y))
            .collect())
    }

    pub fn get(&self, file: &File) -> Result<&FileInfo, TreeError> {
        self.files
            .get(file.id)
            .map(|x| &x.info)
            .ok_or(TreeError::NonexistentNode)
    }

    pub fn parent(&self, file: &File) -> Result<Directory, TreeError> {
        self.files
            .get(file.id)
            .map(|x| Directory::new(x.parent))
            .ok_or(TreeError::NonexistentNode)
    }
}

impl fmt::Display for Tree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Tree {\n")?;
        match self.directories.get(self.root) {
            Some(root) => root.fmt(self, f, 4)?,
            None => f.write_fmt(format_args!("    Oops. Can't find a root.\n"))?,
        }
        f.write_str("}")?;
        Ok(())
    }
}

impl Tree {
    fn new(root: Vec<u8>) -> Self {
        let mut dirs = Arena::new();
        let root = dirs.alloc(DirWrap {
            parent: None,
            dirs: vec![],
            files: vec![],
            info: DirectoryInfo {
                name: root,
                size: 0,
            },
        });
        Tree {
            directories: dirs,
            files: Arena::new(),
            root,
        }
    }

    fn get_directory_or_create<P: AsRef<Path>>(&mut self, path: P) -> Result<Id<DirWrap>, TreeError> {
        let path = path.as_ref();

        let root = self.directories.get(self.root).ok_or(TreeError::Corrupt)?;
        if root.info.name == osstr_to_bytes(path.as_os_str())[..] {
            return Ok(self.root);
        }

        match path.parent() {
            None => Err(TreeError::InvalidPath),
            Some(parent) => {
                let filename = path.file_name().ok_or(TreeError::InvalidPath)?;
                let filename = osstr_to_bytes(filename);

                let parent_id = self.get_directory_or_create(parent)?;

                // Borrow parent immutable
                let parent = self.directories.get(parent_id).ok_or(TreeError::Corrupt)?;
                for &i in &parent.dirs {
                    let d = self.directories.get(i).ok_or(TreeError::Corrupt)?;
                    if d.info.name == &filename[..] {
                        return Ok(i);
                    }
                }

                let id = self.directories.alloc(DirWrap {
                    parent: Some(parent_id),
                    dirs: vec![],
                    files: vec![],
                    info: DirectoryInfo {
                        name: filename.into_owned(),
                        size: u64::MAX,
                    },
                });

                // Now borrow mutable
                let parent = self.directories.get_mut(parent_id).ok_or(TreeError::Corrupt)?;
                parent.dirs.push(id);

                Ok(id)
            }
        }
    }

    fn put_file(&mut self, info: FileInfo) -> Result<Id<FileWrap>, TreeError> {
        let path = bytes_to_osstr(&info.path).map_err(|_| TreeError::InvalidPath)?;
        let path = Path::new(&path);
        let parent = path.parent().ok_or(TreeError::InvalidPath)?;
        let parent = self.get_directory_or_create(parent)?;
        let directory = self.directories.get_mut(parent).ok_or(TreeError::Corrupt)?;

        for &i in &directory.files {
            let file = self.files.get(i).ok_or(TreeError::Corrupt)?;
            if file.info.path == info.path {
                return Err(TreeError::FileAlreadyAdded);
            }
        }

        let res = self.files.alloc(FileWrap {
            parent,
            info,
        });
        directory.files.push(res);
        Ok(res)
    }

    fn fill_sizes(&mut self, root_id: Id<DirWrap>) -> Result<u64, TreeError> {
        let root = self.directories.get(root_id).ok_or(TreeError::NonexistentNode)?;
        let mut size = 0;
        for &i in &root.files {
            let f = self.files.get(i).ok_or(TreeError::Corrupt)?;
            size += f.info.size().unwrap_or_default();
        }
        let dirs = root.dirs.to_vec();
        for i in dirs {
            size += self.fill_sizes(i)?;
        }

        let root = self.directories.get_mut(root_id).ok_or(TreeError::NonexistentNode)?;
        root.info.size = size;
        Ok(size)
    }
}

#[derive(Debug, Error)]
pub enum CollectionError {
    #[error("something went wrong with tree: {0}")]
    Tree(#[from] TreeError),
    #[error("something went wrong while walking: {0}")]
    Walking(#[from] walkdir::Error),
    #[error("something went wrong when performing io: {0}")]
    Io(#[from] std::io::Error),
}

pub fn collect<P: AsRef<Path>>(root: P) -> Result<Tree, CollectionError> {
    let root = root.as_ref();
    let walk = walkdir::WalkDir::new(root).into_iter();
    let root = osstr_to_bytes(root.as_os_str());
    let mut tree = Tree::new(root.to_vec());

    for i in walk {
        let i = i?;
        if i.file_type().is_file() {
            let meta = i.metadata()?;
            tree.put_file(FileInfo {
                path: osstr_to_bytes(i.path().as_os_str()).into_owned(),
                inode: meta.inode(),
                mode: meta.mode(),
                ctime: meta.created()?.into(),
                mtime: meta.modified()?.into(),
                kind: FileKind::File {
                    size: meta.len(),
                },
                hash: None,
            })?;
        }
    }
    tree.fill_sizes(tree.root)?;

    Ok(tree)
}
