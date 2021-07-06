use super::CpioHeader;
use crate::fileinfo::Info;
use crate::path::External;
use snafu::{OptionExt, ResultExt, Snafu};
use std::io::SeekFrom;
use std::mem::size_of;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

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
        // TODO: What if it is a directory or not-a-file?

        let size = self.header.size();
        let mut file = self.reader.take(size);

        // FIXME: Checksum is not computed. I used to have a wrapper that computes checksum.
        // It should be computed by caller, not here.
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
    pub async fn skip(mut self) -> Result<Reader<R>, ReadError>
    where
        R: AsyncSeek,
    {
        let mut size = self.header.size();
        if size % 2 != 0 {
            size += 1;
        }
        #[allow(clippy::cast_possible_wrap)] // size can't be moore than 2^48
        self.reader.seek(SeekFrom::Current(size as i64)).await?;
        Ok(Reader {
            reader: self.reader,
        })
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
    pub files: Option<Vec<Info<External>>>,
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
