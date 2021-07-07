use std::fs::Metadata;

pub(crate) trait FileExtensions {
    fn inode(&self) -> u64;
    fn mode(&self) -> u32;
    fn user_id(&self) -> u32;
    fn group_id(&self) -> u32;
}

#[cfg(unix)]
impl FileExtensions for Metadata {
    fn inode(&self) -> u64 {
        std::os::unix::fs::MetadataExt::ino(self)
    }

    fn mode(&self) -> u32 {
        std::os::unix::fs::MetadataExt::mode(self)
    }

    fn user_id(&self) -> u32 {
        std::os::unix::fs::MetadataExt::uid(self)
    }

    fn group_id(&self) -> u32 {
        std::os::unix::fs::MetadataExt::gid(self)
    }
}

#[cfg(windows)]
impl FileExtensions for Metadata {
    fn inode(&self) -> u64 {
        std::os::windows::fs::MetadataExt::file_index(self).unwrap_or_default()
    }

    fn mode(&self) -> u32 {
        u32::MAX
    }

    fn user_id(&self) -> u32 {
        u32::MAX
    }

    fn group_id(&self) -> u32 {
        u32::MAX
    }
}
