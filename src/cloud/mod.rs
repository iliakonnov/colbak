use futures::Future;
use tokio::io::AsyncRead;

pub mod local_fs;
pub mod state;

/// Key of archive in cloud.
pub struct Key(pub String);

pub trait CloudProvider {
    type Error: std::fmt::Debug + std::error::Error + 'static;

    type UploadFuture<'a, A: 'a>: 'a + Future<Output = Result<Key, Self::Error>>;
    fn upload<'a, A: AsyncRead + Unpin + 'a>(&'a self, archive: A) -> Self::UploadFuture<'a, A>;

    type DeleteFuture<'a>: 'a + Future<Output = Result<(), Self::Error>>;
    fn delete(&self, key: Key) -> Self::DeleteFuture<'_>;

    type DownloadReader<'a>: 'a + AsyncRead;
    type DownloadFuture<'a>: 'a + Future<Output = Result<Self::DownloadReader<'a>, Self::Error>>;
    fn download(&self, key: Key) -> Self::DownloadFuture<'_>;
}

#[derive(Debug, Clone, Copy)]
pub struct FakeCloud;

#[derive(snafu::Snafu, Debug)]
pub struct FakeDoesNotWork;

impl CloudProvider for FakeCloud {
    type Error = FakeDoesNotWork;

    type UploadFuture<'a, A: 'a> = impl 'a + Future<Output = Result<Key, Self::Error>>;
    fn upload<'a, A: AsyncRead + Unpin + 'a>(&'a self, _archive: A) -> Self::UploadFuture<'a, A> {
        async { Err(FakeDoesNotWork) }
    }

    type DeleteFuture<'a> = impl 'a + Future<Output = Result<(), Self::Error>>;
    fn delete(&self, _key: Key) -> Self::DeleteFuture<'_> {
        async { Err(FakeDoesNotWork) }
    }

    type DownloadReader<'a> = impl 'a + AsyncRead;
    type DownloadFuture<'a> =
        impl 'a + Future<Output = Result<Self::DownloadReader<'a>, Self::Error>>;
    fn download(&self, _key: Key) -> Self::DownloadFuture<'_> {
        async { Result::<tokio::io::Empty, _>::Err(FakeDoesNotWork) }
    }
}
