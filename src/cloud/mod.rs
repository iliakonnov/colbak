use crate::cpio::Archive;
use async_trait::async_trait;

pub mod state;

/// Key of archive in cloud.
pub struct Key(pub String);

#[async_trait]
pub trait CloudProvider {
    type Error;
    fn new() -> Self;
    async fn upload(&self, archive: &mut Archive) -> Result<Key, Self::Error>;
    async fn delete(&self, key: Key) -> Result<(), Self::Error>;
}
