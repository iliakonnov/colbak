#![cfg(feature = "local-fs")]

use std::path::PathBuf;
use std::pin::Pin;

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

    fn upload<'a, A: AsyncRead + Unpin + 'a>(
        &'a self,
        mut archive: A,
    ) -> Pin<Box<dyn 'a + Future<Output = Result<Key, Self::Error>>>> {
        Box::pin(async move {
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
        })
    }

    fn delete<'a>(
        &'a self,
        key: Key,
    ) -> Pin<Box<dyn 'a + Future<Output = Result<(), Self::Error>>>> {
        Box::pin(async move {
            let path = self.root.join(&key.0);
            tokio::fs::remove_file(path).await.context(IoFailed)
        })
    }

    type DownloadReader<'a> = impl 'a + AsyncRead;
    fn download<'a>(
        &'a self,
        key: Key,
    ) -> Pin<Box<dyn 'a + Future<Output = Result<Self::DownloadReader<'a>, Self::Error>>>> {
        Box::pin(async move {
            let path = self.root.join(&key.0);
            let file = tokio::fs::File::open(path).await.context(IoFailed)?;
            Ok(file)
        })
    }
}
