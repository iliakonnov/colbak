use super::pending::{OpeningReadFuture, Pending, PendingReader};
use super::state_machine::*;
use crate::cpio::smart_read::{SmartBuf, SmartRead, SmartReader};
use crate::cpio::Archive;
use either::Either;
use pin_project_lite::pin_project;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

pin_project! {
    pub struct Reader<'a> {
        #[pin]
        inner: SmartReader<State<'a>>
    }
}

impl<'a> AsyncRead for Reader<'a> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
    }
}

impl Reader<'_> {
    pub fn new(archive: &mut Archive) -> Reader {
        Reader {
            inner: SmartReader::new(State::None(states::None {
                archive: archive as *mut _,
                phantom: Default::default(),
                position: 0,
            })),
        }
    }
}

//                                  ↙--↖
// None -> Header -> OpeningFile -> File -> None again
//     \-> Trailer -> EOF /
//                     ↖-/

enum State<'a> {
    None(states::None<'a>),
    Header(states::Header<'a>),
    OpeningFile(states::OpeningFile<'a>),
    File(states::File<'a>),
    Trailer(states::Trailer<'a>),
    Eof(states::Eof),
    Poisoned,
}

impl SmartRead for State<'_> {
    fn amortized_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut SmartBuf<'_, '_, '_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let state = std::mem::replace(this, State::Poisoned);
        let (new_state, result) = match_advance! {
            match state.advance(cx, buf) {
                State::Poisoned => return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "State is poisoned"))),
                State::None => |x| match x {
                    Either::Left(header) => State::Header(header),
                    Either::Right(trailer) => State::Trailer(trailer),
                },
                State::Header => State::OpeningFile,
                State::OpeningFile => State::File,
                State::File => |x| match x {
                    Either::Left(file) => State::File(file),
                    Either::Right(none) => State::None(none)
                },
                State::Trailer => State::Eof,
                State::Eof => State::Eof,
            }
        };
        *this = new_state;
        result
    }
}

struct StateContainer<'a> {
    state: State<'a>,
}

mod states {
    use super::*;

    pub struct None<'a> {
        pub archive: *mut Archive,
        pub phantom: std::marker::PhantomData<&'a mut Archive>,
        pub position: usize,
    }

    pub struct Header<'a> {
        pub none: None<'a>,
        pub file: &'a mut super::Pending,
    }

    pub struct OpeningFile<'a> {
        pub none: None<'a>,
        pub future: Pin<Box<super::OpeningReadFuture<'a>>>,
    }

    pub struct File<'a> {
        pub none: None<'a>,
        pub length: u64,
        pub reader: Pin<Box<PendingReader<'a>>>,
    }

    pub struct Trailer<'a> {
        pub archive: &'a mut Archive,
    }

    pub struct Eof;
}

impl<'a> Advanceable for states::None<'a> {
    type Next = Either<states::Header<'a>, states::Trailer<'a>>;
    fn advance(
        self,
        _cx: &mut Context<'_>,
        _buf: &mut SmartBuf<'_, '_, '_>,
    ) -> AdvanceResult<Self, Self::Next> {
        // Switch to the next file

        let archive = unsafe {
            // This is safe: at this state we do not have any other references to archive's content
            &mut *self.archive
        };

        let res = if self.position < archive.files.len() {
            let file = &mut archive.files[self.position];
            // None -> Header
            Either::Left(states::Header { none: self, file })
        } else {
            Either::Right(states::Trailer { archive })
        };
        AdvanceResult::Ready(res)
    }
}

impl<'a> Advanceable for states::Header<'a> {
    type Next = states::OpeningFile<'a>;
    fn advance(
        self,
        _cx: &mut Context<'_>,
        buf: &mut SmartBuf<'_, '_, '_>,
    ) -> AdvanceResult<Self, Self::Next> {
        let header = self.file.header();
        buf.put_slice(&header);

        let future = self.file.read_fut();

        let res = states::OpeningFile {
            none: self.none,
            future: Box::pin(future),
        };
        AdvanceResult::Ready(res)
    }
}

impl<'a> Advanceable for states::OpeningFile<'a> {
    type Next = states::File<'a>;
    fn advance(
        mut self,
        cx: &mut Context<'_>,
        _buf: &mut SmartBuf<'_, '_, '_>,
    ) -> AdvanceResult<Self, Self::Next> {
        match self.future.as_mut().poll(cx) {
            Poll::Pending => AdvanceResult::Pending(self),
            Poll::Ready(Err(err)) => {
                AdvanceResult::Failed(io::Error::new(io::ErrorKind::Other, err))
            }
            Poll::Ready(Ok(reader)) => {
                // OpeningFile -> File
                AdvanceResult::Ready(states::File {
                    none: self.none,
                    reader: Box::pin(reader),
                    length: 0,
                })
            }
        }
    }
}

impl<'a> Advanceable for states::File<'a> {
    type Next = Either<Self, states::None<'a>>;
    fn advance(
        mut self,
        cx: &mut Context<'_>,
        buf: &mut SmartBuf<'_, '_, '_>,
    ) -> AdvanceResult<Self, Self::Next> {
        match buf.fill_using(self.reader.as_mut(), cx) {
            Poll::Pending => AdvanceResult::Pending(self),
            Poll::Ready(Err(e)) => AdvanceResult::Failed(e),
            Poll::Ready(Ok(None)) => {
                // EOF
                if self.length % 2 != 0 {
                    buf.put_slice(&[0]);
                }
                // Switch to next file
                self.none.position += 1;
                AdvanceResult::Ready(Either::Right(self.none))
            }
            Poll::Ready(Ok(Some(written))) => {
                self.length += written.len() as u64;
                AdvanceResult::Ready(Either::Left(self))
            }
        }
    }
}

impl<'a> Advanceable for states::Trailer<'a> {
    type Next = states::Eof;
    fn advance(
        self,
        _cx: &mut Context<'_>,
        buf: &mut SmartBuf<'_, '_, '_>,
    ) -> AdvanceResult<Self, Self::Next> {
        let trailer = self.archive.trailer();
        buf.put_slice(&trailer);
        AdvanceResult::Ready(states::Eof)
    }
}

impl Advanceable for states::Eof {
    type Next = states::Eof;
    fn advance(
        self,
        _cx: &mut Context<'_>,
        buf: &mut SmartBuf<'_, '_, '_>,
    ) -> AdvanceResult<Self, Self::Next> {
        buf.eof();
        AdvanceResult::Ready(states::Eof)
    }
}