use std::cell::RefCell;
use std::collections::BinaryHeap;

use bumpalo::Bump;
use radix_trie::Trie;
use smallvec::SmallVec;

use crate::database::{Diff, DiffRow, DiffType, RowId};
pub struct Packed(pub Vec<SmallVec<[RowId; 4]>>);

struct File<'a> {
    rowid: RowId,
    size: u64,
    directory: &'a Directory<'a>,
}

impl PartialEq for File<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.size.eq(&other.size)
    }
}

impl PartialOrd for File<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.size.partial_cmp(&other.size)
    }
}

impl Eq for File<'_> {}

impl Ord for File<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.size.cmp(&other.size)
    }
}

struct Directory<'a> {
    parent: Option<&'a Directory<'a>>,
    files: RefCell<BinaryHeap<&'a File<'a>>>,
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
        result.extend_from_slice(&backwards.subdirs.borrow());
    }

    result.into_iter()
}

#[allow(clippy::missing_panics_doc)]
pub fn pack(diff: &Diff, min_size: u64) -> Result<Packed, crate::database::Error> {
    let arena = Bump::new();

    // First, we want to fill files and directories.
    let mut directories = Trie::new();
    let mut files = BinaryHeap::new();
    let Ok(()) = diff
        .query()
        .only_kind(DiffType::Changed)
        .less_than(min_size)
        .for_each::<_, !>(|row| {
            if let DiffRow::Changed {
                rowid, size, path, ..
            } = row
            {
                let mut parent = None;
                for key in path.prefixes() {
                    let dir = directories.get(key).copied().unwrap_or_else(|| {
                        let dir = arena.alloc(Directory {
                            parent,
                            files: RefCell::new(BinaryHeap::new()),
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
                // prefixes() always returns empty string first.
                let parent = parent.unwrap();
                let file = arena.alloc(File {
                    rowid,
                    size,
                    directory: parent,
                });
                let file = &*file;
                parent.files.borrow_mut().push(file);
                files.push(file);
            }
            Ok(())
        })?;
    std::mem::drop(directories);

    // Now we can really make packs
    let mut result = Vec::new();
    let mut top = Vec::new();
    for size_of_pack in 2.. {
        // Searching for top-N smallest files in related directories
        let largest = match files.pop() {
            Some(x) => x,
            None => break,
        };
        let files_to_add = size_of_pack - 1;
        let mut min_size = u64::MAX;
        for dir in find_related_directories(largest) {
            for &file in dir.files.borrow().iter() {
                if top.len() < files_to_add {
                    top.push(file);
                    continue;
                }
                if file.size >= min_size {
                    continue;
                }
                min_size = file.size;

                let to_remove = top
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, f)| f.size > min_size);
                debug_assert!(to_remove.is_some());
                if let Some((idx, _)) = to_remove {
                    top.remove(idx);
                }
                top.push(file);
            }
        }

        // Then creating a pack from them.
        let mut pack: SmallVec<_> = top.drain(..).map(|f| f.rowid).collect();
        pack.push(largest.rowid);
        result.push(pack);
    }

    // Finally, we should add bigger files that were skippped earlier.
    let Ok(()) = diff
        .query()
        .only_kind(DiffType::Changed)
        .larger_or_eq(min_size)
        .for_each::<_, !>(|row| {
            if let DiffRow::Changed { rowid, .. } = row {
                result.push(smallvec::smallvec![rowid]);
            }
            Ok(())
        })?;
    Ok(Packed(result))
}
