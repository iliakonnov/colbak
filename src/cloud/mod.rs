use std::pin::Pin;

use futures::Future;
use tokio::io::AsyncRead;

pub mod local_fs;
pub mod state;

/// Key of archive in cloud.
pub struct Key(pub String);

pub trait CloudProvider {
    type Error: std::fmt::Debug + std::error::Error + 'static;

    fn upload<'a, A: AsyncRead + Unpin + 'a>(
        &'a self,
        archive: A,
    ) -> Pin<Box<dyn 'a + Future<Output = Result<Key, Self::Error>>>>;

    fn delete<'a>(
        &'a self,
        key: Key,
    ) -> Pin<Box<dyn 'a + Future<Output = Result<(), Self::Error>>>>;

    type DownloadReader<'a>: 'a + AsyncRead
    where
        Self: 'a;

    #[allow(clippy::type_complexity)]
    fn download<'a>(
        &'a self,
        key: Key,
    ) -> Pin<Box<dyn 'a + Future<Output = Result<Self::DownloadReader<'a>, Self::Error>>>>;
}

#[derive(Debug, Clone, Copy)]
pub struct FakeCloud;

#[derive(snafu::Snafu, Debug)]
pub struct FakeDoesNotWork;

impl CloudProvider for FakeCloud {
    type Error = FakeDoesNotWork;

    fn upload<'a, A: AsyncRead + Unpin + 'a>(
        &'a self,
        _archive: A,
    ) -> Pin<Box<dyn 'a + Future<Output = Result<Key, Self::Error>>>> {
        Box::pin(async { Err(FakeDoesNotWork) })
    }

    fn delete<'a>(
        &'a self,
        _key: Key,
    ) -> Pin<Box<dyn 'a + Future<Output = Result<(), Self::Error>>>> {
        Box::pin(async { Err(FakeDoesNotWork) })
    }

    type DownloadReader<'a> = impl 'a + AsyncRead;
    fn download<'a>(
        &'a self,
        _key: Key,
    ) -> Pin<Box<dyn 'a + Future<Output = Result<Self::DownloadReader<'a>, Self::Error>>>> {
        Box::pin(async { Result::<tokio::io::Empty, _>::Err(FakeDoesNotWork) })
    }
}
