use smallvec::SmallVec;

use crate::database::{Diff, DiffType, RowId};
pub struct Packed(pub Vec<SmallVec<[RowId; 4]>>);

#[allow(clippy::missing_panics_doc)]
pub fn pack(diff: &Diff, min_size: u64) -> Result<Packed, crate::database::Error> {
    let mut result = Vec::new();
    let mut last_pack = SmallVec::new();
    let mut pack_size = 0;
    let Ok(()) = diff
        .query()
        .deny_kind(DiffType::Deleted)
        .for_each::<_, !>(|row| {
            last_pack.push(row.rowid());
            pack_size += row.size();

            if pack_size >= min_size {
                let full_pack = std::mem::replace(&mut last_pack, SmallVec::new());
                result.push(full_pack);
                pack_size = 0;
            }
            Ok(())
        })?;
    Ok(Packed(result))
}
