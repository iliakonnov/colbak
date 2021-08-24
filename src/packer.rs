use std::cell::RefCell;
use std::collections::BTreeSet;

use bumpalo::Bump;
use radix_trie::Trie;
use smallvec::SmallVec;

use crate::database::{Diff, DiffRow, RowId};
use crate::utils::Utils;
pub struct Packed(pub Vec<SmallVec<[RowId; 4]>>);

#[derive(Debug)]
struct File<'a> {
    rowid: RowId,
    size: u64,
    directory: &'a Directory<'a>,
    #[cfg(debug_assertions)]
    is_inserted: std::sync::atomic::AtomicBool,
}

impl PartialEq for File<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.rowid.0 == other.rowid.0
    }
}

impl PartialOrd for File<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for File<'_> {}

impl Ord for File<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.size.cmp(&other.size) {
            std::cmp::Ordering::Equal => self.rowid.0.cmp(&other.rowid.0),
            x => x,
        }
    }
}

#[derive(Debug)]
struct Directory<'a> {
    parent: Option<&'a Directory<'a>>,
    files: RefCell<BTreeSet<&'a File<'a>>>,
    subdirs: RefCell<Vec<&'a Directory<'a>>>,
}

fn find_related_directories<'a>(file: &'a File<'a>) -> impl Iterator<Item = &'a Directory<'a>> {
    // FIXME: This should be implemented as iterator.
    let mut result = Vec::new();

    let mut front = vec![file.directory];
    let mut front2 = Vec::new();

    // Going up to 3 directories forward
    for _ in 0..4 {
        for dir in front.drain(..) {
            result.push(dir);
            for &child in dir.subdirs.borrow().iter() {
                front2.push(child);
            }
        }
        std::mem::swap(&mut front, &mut front2);
    }
    std::mem::drop((front, front2));

    // Then a bit backward
    if let Some(backwards) = file.directory.parent {
        result.push(backwards);
        result.extend(backwards.parent);
        // Now again moving forward
        let forward = backwards.subdirs.borrow();
        result.extend(
            forward
                .iter()
                .filter(|&&x| !std::ptr::eq(x, file.directory)),
        );
    }

    result.into_iter()
}

#[allow(clippy::missing_panics_doc)]
pub fn pack(diff: &Diff, min_size: u64) -> Result<Packed, crate::database::Error> {
    let arena = Bump::new();

    // First, we want to fill files and directories.
    let mut directories = Trie::new();
    let mut files = BTreeSet::new();
    let Ok(()) = diff.query().less_than(min_size).for_each::<_, !>(|row| {
        if let DiffRow::Changed {
            rowid, size, path, ..
        }
        | DiffRow::Created {
            rowid, size, path, ..
        } = row
        {
            let mut parent = None;
            for key in path.prefixes() {
                let dir = directories.get(key).copied().unwrap_or_else(|| {
                    let dir = arena.alloc(Directory {
                        parent,
                        files: RefCell::new(BTreeSet::new()),
                        subdirs: RefCell::new(Vec::new()),
                    });
                    if let Some(parent) = parent {
                        parent.subdirs.borrow_mut().push(dir);
                    }
                    let dir: &Directory = &*dir;
                    directories.insert(key.to_vec(), dir);
                    dir
                });
                parent = Some(dir);
            }
            #[allow(clippy::expect_used)]
            let parent = parent.expect("prefixes() always returns empty string first.");

            let file = arena.alloc(File {
                rowid,
                size,
                directory: parent,
                #[cfg(debug_assertions)]
                is_inserted: std::sync::atomic::AtomicBool::new(false),
            });
            let file = &*file;
            parent.files.borrow_mut().insert(file);
            files.insert(file);
        }
        Ok(())
    })?;
    std::mem::drop(directories);

    // Now we can really make packs
    let mut result = Vec::new();
    let mut top = Vec::new();
    for size_of_pack in 2.. {
        // Searching for top-N smallest files in related directories
        let largest = match files.pop_last() {
            Some(x) => x,
            None => break,
        };
        largest
            .directory
            .files
            .borrow_mut()
            .remove(largest)
            .debug_assert();

        let files_to_add = size_of_pack - 1;
        let mut min_size = u64::MAX;
        for dir in find_related_directories(largest) {
            for &file in dir.files.borrow().iter() {
                if top.len() < files_to_add {
                    top.push(file);
                    min_size = min_size.min(file.size);
                    continue;
                }

                debug_assert_eq!(
                    min_size,
                    top.iter().map(|x| x.size).min().unwrap_or(u64::MAX)
                );
                #[cfg(debug_assertions)]
                debug_assert!(!file.is_inserted.load(std::sync::atomic::Ordering::SeqCst));

                if file.size >= min_size {
                    continue;
                }

                let to_remove = top
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, f)| f.size > file.size);
                debug_assert!(to_remove.is_some());
                if let Some((idx, _)) = to_remove {
                    top.remove(idx);
                    min_size = file.size;
                }
                top.push(file);
            }
        }

        // Then create pack from them.
        let mut pack = SmallVec::with_capacity(top.len());
        for f in top.drain(..) {
            pack.push(f.rowid);
            files.remove(f).debug_assert();
            f.directory.files.borrow_mut().remove(f).debug_assert();
            #[cfg(debug_assertions)]
            f.is_inserted
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        pack.push(largest.rowid);
        result.push(pack);
    }
    debug_assert!(files.is_empty());
    std::mem::drop(files);
    std::mem::drop(arena);

    // Finally, we should add bigger files that were skipped earlier.
    let Ok(()) = diff
        .query()
        .larger_or_eq(min_size)
        .for_each::<_, !>(|row| {
            if let DiffRow::Changed { rowid, .. } | DiffRow::Created { rowid, .. } = row {
                result.push(smallvec::smallvec![rowid]);
            }
            Ok(())
        })?;
    Ok(Packed(result))
}
