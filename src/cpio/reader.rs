use super::CpioHeader;
use crate::fileinfo::Info;
use crate::path::{External, Local};
use snafu::{OptionExt, ResultExt, Snafu};
use std::mem::size_of;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Extractor of cpio archive.
pub struct Reader<R> {
    reader: R,
}

impl<R> Reader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }
}

/// Represents single file being read from archive at this moment.
/// Can be optionally skipped.
pub struct ReadFile<R> {
    filename: Vec<u8>,
    reader: R,
    header: CpioHeader,
}

#[derive(Debug, Snafu)]
#[snafu(context(false))]
pub struct ReadError {
    source: tokio::io::Error,
    backtrace: snafu::Backtrace,
}

impl<R: AsyncRead + Unpin> ReadFile<R> {
    /// Writes contents of file to the provided writer.
    pub async fn drain_to<W>(self, dst: &mut W) -> Result<Reader<R>, ReadError>
    where
        W: AsyncWrite + Unpin,
    {
        let size = self.header.size();
        let mut file = self.reader.take(size);

        let mut buf = vec![0; 1024 * 1024];
        loop {
            let len = file.read(&mut buf).await?;
            if len == 0 {
                break;
            }
            dst.write_all(&buf[..len]).await?;
        }

        let mut reader = file.into_inner();
        if size % 2 != 0 {
            reader.read_u8().await?;
        }

        Ok(Reader { reader })
    }

    /// Skips this file completely
    pub async fn skip(self) -> Result<Reader<R>, ReadError> {
        let mut sink = tokio::io::sink();
        self.drain_to(&mut sink).await
    }

    /// Extracts info about this file
    pub fn info(&self) -> Info<External> {
        // We should skip NUL byte in the end.
        let name = &self.filename[..self.filename.len() - 1];
        self.header.info(name)
    }
}

/// Optional metadata that is stored in the `TRAILER!!!` entry.
#[derive(Debug, Clone)]
pub struct UnpackedArchive {
    pub files: Option<Vec<Info<Local>>>,
}

pub enum NextItem<R> {
    File(ReadFile<R>),
    End(UnpackedArchive),
}

#[derive(Debug, Snafu)]
pub enum ReadingError {
    IoFailed {
        source: tokio::io::Error,
        backtrace: snafu::Backtrace,
    },
    /// Probably header had invalid magic
    InvalidHeader,
    /// Filename does not ends with zero byte
    InvalidName,
    CantDeserializeArchive {
        source: serde_json::Error,
    },
}

impl<R: AsyncRead + Unpin> Reader<R> {
    pub async fn advance(mut self) -> Result<NextItem<R>, ReadingError> {
        let mut header = [0; size_of::<CpioHeader>()];
        self.reader
            .read_exact(&mut header)
            .await
            .context(IoFailed {})?;
        let header = CpioHeader::decode(header).context(InvalidHeader)?;

        let mut filename = vec![0; header.namesize as usize];
        self.reader
            .read_exact(&mut filename)
            .await
            .context(IoFailed {})?;
        if filename.last().copied() != Some(0) {
            return InvalidName.fail();
        }

        if header.namesize % 2 != 0 {
            self.reader.read_u8().await.context(IoFailed {})?;
        }

        if header.is_trailer(&filename) {
            // FIXME: Limit size of json.
            let mut json = Vec::new();
            self.reader.read_to_end(&mut json).await.context(IoFailed)?;
            let files = if json.iter().all(|x| *x == 0) {
                None
            } else {
                // FIXME: This error is not critical.
                Some(serde_json::from_slice(&json).context(CantDeserializeArchive {})?)
            };
            return Ok(NextItem::End(UnpackedArchive { files }));
        }

        Ok(NextItem::File(ReadFile {
            filename,
            reader: self.reader,
            header,
        }))
    }
}
