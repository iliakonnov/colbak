#![cfg(feature = "local-fs")]

use std::path::PathBuf;

use futures::Future;
use snafu::{ResultExt, Snafu};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use super::{CloudProvider, Key};

/// Basic cloud that stores all files in the local filesystem
pub struct LocalFs {
    root: PathBuf,
}

#[derive(Debug, Snafu)]
pub enum Error {
    IoFailed {
        source: tokio::io::Error,
        backtrace: snafu::Backtrace,
    },
}

impl CloudProvider for LocalFs {
    type Error = Error;

    type UploadFuture<'a, A: 'a> = impl 'a + Future<Output = Result<Key, Self::Error>>;

    fn upload<'a, A: AsyncRead + Unpin + 'a>(
        &'a self,
        mut archive: A,
    ) -> Self::UploadFuture<'a, A> {
        async move {
            let name = uuid::Uuid::new_v4();
            let name = name.to_hyphenated_ref().to_string();
            let path = self.root.join(&name);
            let mut file = tokio::fs::File::open(path).await.context(IoFailed)?;
            let mut buf = [0; 4096];
            loop {
                let len = archive.read_exact(&mut buf).await.context(IoFailed)?;
                if len == 0 {
                    break Ok(Key(name));
                }
                file.write_all(&buf[..len]).await.context(IoFailed)?;
            }
        }
    }

    type DeleteFuture<'a> = impl 'a + Future<Output = Result<(), Self::Error>>;

    fn delete(&self, key: Key) -> Self::DeleteFuture<'_> {
        async move {
            let path = self.root.join(&key.0);
            tokio::fs::remove_file(path).await.context(IoFailed)
        }
    }

    type DownloadReader<'a> = impl 'a + AsyncRead;
    type DownloadFuture<'a> =
        impl 'a + Future<Output = Result<Self::DownloadReader<'a>, Self::Error>>;
    fn download(&self, key: Key) -> Self::DownloadFuture<'_> {
        async move {
            let path = self.root.join(&key.0);
            let file = tokio::fs::File::open(path).await.context(IoFailed)?;
            Ok(file)
        }
    }
}
