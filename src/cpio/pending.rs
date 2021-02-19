use super::CpioHeader;
use crate::fileinfo::{Info, UnspecifiedInfo};
use crate::path::Local;
use crate::types::Checksum;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use sha2::Sha256;
use snafu::{ResultExt, Snafu};
use std::pin::Pin;
use std::{io, task::Poll};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pending {
    pub info: Info<Local>,
    pub calculated: Option<Checksum>,
}

#[derive(Debug, Snafu)]
pub enum CantOpen {
    IoFailed { source: io::Error },
    InvalidPath { source: os_str_bytes::EncodingError },
}

pub type PendingReader<'a> = impl AsyncRead + 'a;
pub type OpeningReadFuture<'a> =
    impl std::future::Future<Output = Result<PendingReader<'a>, CantOpen>>;

impl Pending {
    pub fn new(info: Info<Local>) -> Self {
        Self {
            info,
            calculated: None,
        }
    }

    pub async fn read(&mut self) -> Result<impl AsyncRead + '_, CantOpen> {
        let path = self.info.path.to_path().context(InvalidPath)?;
        let file = std::fs::File::open(path).context(IoFailed {})?;
        file.lock_exclusive().context(IoFailed {})?;
        let mut file = File::from_std(file);
        let size = file.metadata().await.context(IoFailed {})?.len();

        let reading = if size < 10 * 1024 * 1024 {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await.context(IoFailed {})?;
            Reading::SmallBegin {
                pending: self,
                data,
            }
        } else {
            Reading::Large {
                pending: self,
                opened: Box::pin(file),
                hasher: Sha256::default(),
                length: 0,
            }
        };
        Ok(reading)
    }

    pub fn read_fut(&mut self) -> OpeningReadFuture<'_> {
        self.read()
    }

    pub fn header(&self) -> Vec<u8> {
        CpioHeader::encode(&self.info)
    }
}

enum Reading<'a> {
    SmallBegin {
        pending: &'a mut Pending,
        data: Vec<u8>,
    },
    SmallContinue {
        pending: &'a mut Pending,
        data: Vec<u8>,
        start: usize,
        checksum: Checksum,
    },
    Large {
        pending: &'a mut Pending,
        opened: Pin<Box<File>>,
        hasher: sha2::Sha256,
        length: u64,
    },
    Done {
        pending: &'a mut Pending,
        checksum: Checksum,
    },
    Mismatch(Mismatch),
    Error,
    Invalid,
}

#[derive(Debug, Clone, Snafu)]
pub enum Mismatch {
    HashMismatch {
        pending: Pending,
        expected: Checksum,
        found: Checksum,
    },
    SizeMismatch {
        pending: Pending,
        expected: u64,
        found: u64,
    },
}

#[derive(Debug, Snafu)]
struct InternalVeryBadError;

fn check_size(found: u64, pending: &Pending) -> Result<(), Mismatch> {
    if let UnspecifiedInfo::File(file) = &pending.info.data {
        if file.size != found {
            let err = Mismatch::SizeMismatch {
                found,
                expected: file.size,
                pending: pending.clone(),
            };
            return Err(err);
        }
    }
    Ok(())
}

fn check_hash(found: Checksum, pending: &Pending) -> Result<(), Mismatch> {
    if let Some(expected) = pending.info.hash {
        if found != expected {
            let err = Mismatch::HashMismatch {
                expected,
                found,
                pending: pending.clone(),
            };
            return Err(err);
        }
    }
    Ok(())
}

macro_rules! do_check {
    ($self:ident.$check:ident($arg:expr, $pending:ident)) => {
        if let Err(err) = $check($arg, $pending) {
            *$self.as_mut() = Reading::Mismatch(err.clone());
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, err)));
        };
    };
}

impl<'a> AsyncRead for Reading<'a> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut this = std::mem::replace(&mut *self, Reading::Invalid);
        if let Reading::Mismatch { .. } | Reading::Error = this {
            *self.as_mut() = this;
            return Poll::Ready(Ok(0));
        }

        if let Reading::SmallBegin { pending, data } = this {
            do_check!(self.check_size(data.len() as u64, pending));

            let mut digest = sha2::Sha256::default();
            digest.update(&data);
            let checksum = digest.into();

            do_check!(self.check_hash(checksum, pending));

            pending.calculated = Some(checksum);
            this = Reading::SmallContinue {
                pending,
                data,
                start: 0,
                checksum,
            }
            // Fallthrough
        }

        if let Reading::SmallContinue {
            pending,
            data,
            start,
            checksum,
        } = this
        {
            let slice = &data[start..];
            if slice.is_empty() {
                *self.as_mut() = Reading::Done { pending, checksum };
                return Poll::Ready(Ok(0));
            }

            let to_read = buf.len().min(slice.len());
            buf[..to_read].copy_from_slice(&slice[..to_read]);

            *self.as_mut() = Reading::SmallContinue {
                pending,
                data,
                start: start + to_read,
                checksum,
            };
            return Poll::Ready(Ok(to_read));
        }

        if let Reading::Large {
            mut opened,
            pending,
            mut hasher,
            length,
        } = this
        {
            match opened.as_mut().poll_read(cx, buf) {
                Poll::Pending => {
                    *self.as_mut() = Reading::Large {
                        opened,
                        pending,
                        hasher,
                        length,
                    };
                    return Poll::Pending;
                }
                Poll::Ready(Err(err)) => {
                    *self.as_mut() = Reading::Error;
                    return Poll::Ready(Err(err));
                }
                // EOF:
                Poll::Ready(Ok(0)) if buf.is_empty() => {
                    do_check!(self.check_size(length, pending));

                    let checksum = hasher.into();
                    do_check!(self.check_hash(checksum, pending));

                    pending.calculated = Some(checksum);
                    *self.as_mut() = Reading::Done { pending, checksum };
                    return Poll::Ready(Ok(0));
                }
                Poll::Ready(Ok(len)) => {
                    let slice = &buf[..len];
                    hasher.update(slice);

                    *self.as_mut() = Reading::Large {
                        opened,
                        pending,
                        hasher,
                        length: length + (len as u64),
                    };
                    return Poll::Ready(Ok(len));
                }
            }
        }

        // Either Invalid or something even worse
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::Other,
            InternalVeryBadError,
        )))
    }
}
