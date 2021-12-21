use crate::cpio::smart_read::{SmartBuf, SmartRead, SmartReader};
use crate::cpio::state_machine::{AdvanceResult, Advanceable};
use crate::cpio::CpioHeader;
use crate::fileinfo::{Info, UnspecifiedInfo};
use crate::path::{Local, PathKind};
use crate::types::Checksum;
use crate::DefaultDigest;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use snafu::{ResultExt, Snafu};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::AsyncRead;

/// File in archive that is not archived yet.
///
/// Note: currently only used `P` is [`Local`](Local)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "P:")]
pub struct Pending<P: PathKind> {
    pub info: Info<P>,
    /// Checksum computed when reading this file. May differ from one in info.
    pub calculated: Option<Checksum>,
}

#[derive(Debug, Snafu)]
pub enum CantOpen {
    IoFailed { source: io::Error },
    InvalidPath { source: os_str_bytes::EncodingError },
}

/// Result of [`Pending::read`](Pending::read) function.
pub type PendingReader<'a> = impl AsyncRead + 'a;

/// Future that is returned by [`Pending::read_fut`](Pending::read_fut)
pub type OpeningReadFuture<'a> =
    impl std::future::Future<Output = Result<PendingReader<'a>, CantOpen>>;

impl<P: PathKind> Pending<P> {
    #[must_use]
    pub fn new(info: Info<P>) -> Self {
        Self {
            info,
            calculated: None,
        }
    }
}

impl Pending<Local> {
    /// Opens file for reading and returns it.
    /// After file is completely read, [`self.calculated`] will be updated
    ///
    /// [`self.calculated`]: Self::calculated
    pub async fn read(&mut self) -> Result<impl AsyncRead + '_, CantOpen> {
        let path = self.info.path.to_path().context(InvalidPath)?;
        let file = std::fs::File::open(path).context(IoFailed {})?;
        file.lock_exclusive().context(IoFailed {})?;
        let file = File::from_std(file);

        let reading = Reading::File(states::File {
            pending: self,
            opened: Box::pin(file),
            hasher: DefaultDigest::default(),
            length: 0,
        });
        Ok(SmartReader::new(reading))
    }

    /// Same as [`Self::read`](Self::read), but returns named type.
    pub fn read_fut(&mut self) -> OpeningReadFuture<'_> {
        self.read()
    }

    /// Returns [cpio header](CpioHeader) for this file.
    #[must_use]
    pub fn header(&self) -> Vec<u8> {
        CpioHeader::encode(&self.info)
    }
}

/// State machine:
/// ```text
/// ↙--        ↙--↖
/// File --> Done-/
///      \-> Mismatch -> !
/// ```
pub enum Reading<'a> {
    /// Should be unreachable.
    Poisoned,
    /// File is being read.
    File(states::File<'a>),
    /// Fle successfully was read.
    Done(states::Done<'a>),
    /// Computed information differs from expected (stored in info).
    Mismatch(Mismatch<Local>),
}

impl SmartRead for Reading<'_> {
    fn amortized_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut SmartBuf<'_, '_, '_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let state = std::mem::replace(this, Reading::Poisoned);
        let (new_state, result) = match_advance! {
            match state.advance(cx, buf) {
                Reading::Poisoned => return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "State is poisoned"))),
                Reading::File => |x| x,
                Reading::Done => Reading::Done,
                Reading::Mismatch => Reading::Mismatch,
            }
        };
        *this = new_state;
        result
    }
}

#[derive(Debug, Snafu)]
pub enum Mismatch<P: PathKind> {
    HashMismatch {
        pending: Pending<P>,
        expected: Box<Checksum>,
        found: Box<Checksum>,
    },
    SizeMismatch {
        pending: Pending<P>,
        expected: u64,
        found: u64,
    },
}

/// Stores variants of [`Reading`](Reading) state machine.
mod states {
    use super::*;

    pub struct File<'a> {
        pub pending: &'a mut Pending<Local>,
        pub opened: Pin<Box<tokio::fs::File>>,
        pub hasher: DefaultDigest,
        pub length: u64,
    }

    pub struct Done<'a> {
        pub pending: &'a mut Pending<Local>,
        pub checksum: Checksum,
    }
}

impl<'a> Advanceable for states::File<'a> {
    type Next = Reading<'a>;

    fn advance(
        mut self,
        cx: &mut Context<'_>,
        buf: &mut SmartBuf<'_, '_, '_>,
    ) -> AdvanceResult<Self, Self::Next> {
        match buf.fill_using(self.opened.as_mut(), cx) {
            Poll::Pending => AdvanceResult::Pending(self),
            Poll::Ready(Err(err)) => AdvanceResult::Failed(err),
            Poll::Ready(Ok(Some(written))) => {
                self.length += written.len() as u64;
                self.hasher.update(written);
                AdvanceResult::Ready(Reading::File(self))
            }
            Poll::Ready(Ok(None)) => {
                // EOF
                buf.eof();

                if let UnspecifiedInfo::File(file) = &self.pending.info.data {
                    if file.size != self.length {
                        return AdvanceResult::Ready(Reading::Mismatch(Mismatch::SizeMismatch {
                            expected: file.size,
                            found: self.length,
                            pending: self.pending.clone(),
                        }));
                    }
                }

                let checksum = self.hasher.finalize().into();

                if let Some(expected) = self.pending.info.hash {
                    if checksum != expected {
                        return AdvanceResult::Ready(Reading::Mismatch(Mismatch::HashMismatch {
                            expected: Box::new(expected),
                            found: Box::new(checksum),
                            pending: self.pending.clone(),
                        }));
                    }
                }

                self.pending.calculated = Some(checksum);
                AdvanceResult::Ready(Reading::Done(states::Done {
                    pending: self.pending,
                    checksum,
                }))
            }
        }
    }
}

// TODO: This impl does not look good
impl Advanceable for Mismatch<Local> {
    type Next = Self;

    fn advance(
        self,
        _cx: &mut Context<'_>,
        _buf: &mut SmartBuf<'_, '_, '_>,
    ) -> AdvanceResult<Self, Self::Next> {
        AdvanceResult::Failed(io::Error::new(io::ErrorKind::InvalidData, self))
    }
}

impl Advanceable for states::Done<'_> {
    type Next = Self;

    fn advance(
        self,
        _cx: &mut Context<'_>,
        _buf: &mut SmartBuf<'_, '_, '_>,
    ) -> AdvanceResult<Self, Self::Next> {
        AdvanceResult::Ready(self)
    }
}
