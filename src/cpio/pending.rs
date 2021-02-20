use crate::cpio::smart_read::{SmartBuf, SmartRead, SmartReader};
use crate::cpio::state_machine::{AdvanceResult, Advanceable};
use crate::cpio::CpioHeader;
use crate::fileinfo::{Info, UnspecifiedInfo};
use crate::path::Local;
use crate::types::Checksum;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use snafu::{ResultExt, Snafu};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{AsyncRead};

enum State {}

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
        let file = File::from_std(file);

        let reading = Reading::File(states::File {
            pending: self,
            opened: Box::pin(file),
            hasher: Sha256::default(),
            length: 0,
        });
        Ok(SmartReader::new(reading))
    }

    pub fn read_fut(&mut self) -> OpeningReadFuture<'_> {
        self.read()
    }

    pub fn header(&self) -> Vec<u8> {
        CpioHeader::encode(&self.info)
    }
}

// ↙--        ↙--↖
// File --> Done-/
//      \-> Mismatch -> !

pub enum Reading<'a> {
    Poisoned,
    File(states::File<'a>),
    Done(states::Done<'a>),
    Mismatch(Mismatch),
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

mod states {
    use super::*;

    pub struct File<'a> {
        pub pending: &'a mut Pending,
        pub opened: Pin<Box<tokio::fs::File>>,
        pub hasher: Sha256,
        pub length: u64,
    }

    pub struct Done<'a> {
        pub pending: &'a mut Pending,
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

                let checksum = self.hasher.into();

                if let Some(expected) = self.pending.info.hash {
                    if checksum != expected {
                        return AdvanceResult::Ready(Reading::Mismatch(Mismatch::HashMismatch {
                            expected,
                            found: checksum,
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

impl Advanceable for Mismatch {
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
