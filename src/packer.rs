use std::alloc::Layout;
use std::marker::PhantomData;
use std::mem::size_of;
use std::pin::Pin;
use std::ptr::NonNull;

use bumpalo::Bump;
use radix_trie::{Trie, TrieCommon};
use slice_dst::SliceWithHeader;
use smallvec::SmallVec;
use static_assertions as sa;

use crate::database::{Diff, DiffRow, RowId};

// #region Some crazy self-referencing data structures
struct Link<'a, T> {
    pointer: Option<NonNull<T>>,
    phantom: PhantomData<&'a T>,
}

impl<'a, T> Link<'a, T> {
    fn new<R: Into<NonNull<T>>>(r: R) -> Self {
        Self {
            pointer: Some(r.into()),
            phantom: PhantomData::default(),
        }
    }

    fn none() -> Self {
        Self {
            pointer: None,
            phantom: PhantomData::default(),
        }
    }
}

struct FollowedBySlice<'a, I, S> {
    info: I,
    count: usize,
    slice: PhantomData<[Link<'a, S>]>,
}

impl<'a, I, S> FollowedBySlice<'a, I, S> {
    #[allow(clippy::mut_from_ref)]
    fn new(arena: &Bump, info: I, count: usize) -> &mut Self {
        let layout = Layout::new::<Self>();
        let array = Layout::array::<Link<'a, S>>(count).unwrap();
        let (layout, padding) = layout.extend(array).unwrap();
        assert_eq!(padding, 0);
        let res: *mut Self = arena.alloc_layout(layout).as_ptr().cast();
        unsafe {
            std::ptr::write(
                res,
                FollowedBySlice {
                    info,
                    count,
                    slice: PhantomData::default(),
                },
            );
            // Zero out array. This is safe since all zeros is a valid pattern for Link<'a, S>
            let array = res.add(1);
            std::ptr::write_bytes::<Link<'a, S>>(array.cast(), 0, count);
            &mut *res
        }
    }

    fn slice_mut(&mut self) -> &mut [Link<'a, S>] {
        unsafe {
            let this = self as *mut Self;
            let array = this.add(1).cast();
            std::slice::from_raw_parts_mut(array, self.count)
        }
    }
}

struct FileInfo<'a> {
    size: u64,
    rowid: RowId,
    directory: Link<'a, SubDirs<'a>>,
}

struct SubDirsInfo<'a> {
    parent: Link<'a, SubDirs<'a>>,
    files: Link<'a, FilesInDir<'a>>,
    //subdirs: [Option<&'a SubDirs<'a>>],
}

#[repr(transparent)]
struct SubDirs<'a>(FollowedBySlice<'a, SubDirsInfo<'a>, SubDirs<'a>>);
impl<'a> SubDirs<'a> {
    #[allow(clippy::mut_from_ref)]
    fn new(arena: &'a Bump, info: SubDirsInfo<'a>, count: usize) -> &'a mut Self {
        let inner = FollowedBySlice::new(arena, info, count);
        unsafe {
            let ptr = (inner as *mut FollowedBySlice<SubDirsInfo, SubDirs>).cast::<Self>();
            &mut *ptr
        }
    }
}

struct FilesInDirInfo<'a> {
    directory: Link<'a, SubDirs<'a>>,
}
type FilesInDir<'a> = FollowedBySlice<'a, FilesInDirInfo<'a>, FileInfo<'a>>;

struct Packer<'a> {
    files: &'a [FileInfo<'a>],
}

// #endregion

pub struct Pack {
    // I hope that most packs won't be bigger than three files.
    files: SmallVec<[RowId; 3]>,
}

pub struct Packed {
    big_files: Vec<RowId>,
    packs: Vec<Pack>,
}

enum TrieEntry<'a> {
    Directory(Link<'a, SubDirsInfo<'a>>),
    File,
}

#[allow(clippy::mut_from_ref)]
fn add_subtrie<'a>(
    arena: &'a Bump,
    subtrie: &radix_trie::SubTrieMut<Vec<u8>, TrieEntry>,
) -> &'a mut SubDirs<'a> {
    // First, we need to walk over children and count directories (skip files — they are leafs):
    let subdirectories = subtrie.children().filter(|x| !x.is_leaf()).count();

    // Then create empty (filled with Nones) SubDirInfo for them.
    let info = SubDirs::new(
        arena,
        SubDirsInfo {
            parent: Link::none(),
            files: Link::none(),
        },
        subdirectories,
    );
    let ptr: NonNull<_> = info.into();

    // Iterate over children again, now filling values
    let slice = info.0.slice_mut();
    for (idx, child) in subtrie.children().filter(|x| !x.is_leaf()).enumerate() {
        let child = subtrie.sub
        let child = add_subtrie(arena, &child);
        child.0.info.parent = Link::new(ptr);
        slice[idx] = Link::new(child);
    }

    info
}

#[allow(clippy::missing_panics_doc)]
pub fn pack(diff: &Diff, pack_size: u64) -> Result<Packed, crate::database::Error> {
    // To reduce memory usage, we should collect small packs before proceeding to larger ones.
    // This way we won't store big_files simlutaniosuly with storing pretty large Packer

    let query = diff
        .query()
        .less_than(pack_size)
        .only_kind(crate::database::DiffType::Created);
    #[allow(clippy::cast_possible_truncation)]
    let count_of_small_files = query.count()? as usize;
    let estimated_directory_count = count_of_small_files / 5;
    let estimated_capacity =
        // Array of FileInfo's:
        count_of_small_files * size_of::<FileInfo>() +
        // Each `SubDirs` contains references to all its children:
        estimated_directory_count * size_of::<SubDirs>() +
        estimated_directory_count * size_of::<&SubDirsInfo>() +
        // Same with `FilesInDir`: each directory contains references to all its children
        estimated_directory_count * size_of::<FilesInDir>() +
        count_of_small_files * size_of::<&FileInfo>();
    let arena = Bump::with_capacity(estimated_capacity);

    // First of all, we want to fill AllFiles and Trie
    let mut files = bumpalo::collections::Vec::with_capacity_in(count_of_small_files, &arena);
    let mut filenames = Trie::new();
    let Ok(()) = query.for_each::<_, !>(|row| {
        if let DiffRow::Created {
            rowid, size, path, ..
        } = row
        {
            files.push(FileInfo {
                size,
                rowid,
                directory: Link::none(), // Must be filled later (1)
            });
            for prefix in path.prefixes() {
                filenames.insert(prefix.to_vec(), TrieEntry::Directory(Link::none()));
                // Must be filled later (2)
            }
            filenames.insert(path.as_bytes().to_vec(), TrieEntry::File);
        }
        Ok(())
    })?;
    let files = files.into_bump_slice();

    // Now we want to walk over the Trie and collect all directories with their subdirs.
    for i in filenames.children() {
        add_subtrie(&arena, &i);
    }

    unimplemented!()
}

struct ReSize<T: ?Sized> {
    size: usize,
    value: T,
}
