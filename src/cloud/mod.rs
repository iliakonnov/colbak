use futures::Future;
use tokio::io::AsyncRead;

pub mod state;

/// Key of archive in cloud.
pub struct Key(pub String);

pub trait CloudProvider {
    type Error: std::fmt::Debug + std::error::Error + 'static;
    fn new() -> Self;

    type UploadFuture<A>: Future<Output = Result<Key, Self::Error>>;
    fn upload<A: AsyncRead>(&self, archive: A) -> Self::UploadFuture<A>;

    type DeleteFuture: Future<Output = Result<(), Self::Error>>;
    fn delete(&self, key: Key) -> Self::DeleteFuture;
}

#[derive(Debug, Clone, Copy)]
pub struct FakeCloud;

#[derive(snafu::Snafu, Debug)]
pub struct FakeDoesNotWork;

impl CloudProvider for FakeCloud {
    type Error = FakeDoesNotWork;

    fn new() -> Self {
        FakeCloud
    }

    type UploadFuture<A> = impl Future<Output = Result<Key, Self::Error>>;
    fn upload<A: AsyncRead>(&self, _archive: A) -> Self::UploadFuture<A> {
        async { Err(FakeDoesNotWork) }
    }

    type DeleteFuture = impl Future<Output = Result<(), Self::Error>>;
    fn delete(&self, _key: Key) -> Self::DeleteFuture {
        async { Err(FakeDoesNotWork) }
    }
}
