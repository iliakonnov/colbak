use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncRead;

use super::engine::*;
use super::pending::PendingReader;
use super::pending::{OpeningReadFuture, Pending};
use super::Archive;

pub struct Machine<'a> {
    _phantom: PhantomData<&'a mut Archive>,
    archive: *mut Archive,
    state: State<'a>,
    position: usize,
    waiter: Option<std::task::Waker>,
}

// None -> Header -> OpeningFile -> File -> None again
//     \-> Trailer -> EOF

enum State<'a> {
    None,
    Header {
        file: &'a mut Pending,
    },
    OpeningFile {
        future: Pin<Box<OpeningReadFuture<'a>>>,
    },
    File {
        reader: Pin<Box<PendingReader<'a>>>,
    },
    Trailer,
    Eof,
    Invalid,
}

impl<'a> Machine<'a> {
    pub fn read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut CowSlice<'_, '_>,
    ) -> io::Result<ReadResult> {
        let state = std::mem::replace(&mut self.state, State::Invalid);
        match state {
            State::Invalid => {
                // Should not be here
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Machine is in the invalid state",
                ))
            }
            State::None => {
                // Switch to the next file
                let archive = unsafe { self.archive.as_mut::<'a>() }.unwrap();
                if self.position < archive.files.len() {
                    let file = &mut archive.files[self.position];
                    // None -> Header
                    let new_state: State<'a> = State::Header { file };
                    self.state = new_state;
                    Ok(ReadResult::Good)
                } else {
                    // None -> Trailer
                    self.state = State::Trailer;
                    Ok(ReadResult::Good)
                }
            }
            State::Header { file } => {
                let header = file.header();
                buf.extend_from_slice(&header);

                // Header -> OpeningFile
                let future = file.read_fut();
                self.state = State::OpeningFile {
                    future: Box::pin(future),
                };
                Ok(ReadResult::Good)
            }
            State::OpeningFile { mut future } => {
                match future.as_mut().poll(cx) {
                    Poll::Pending => Ok(ReadResult::Pending),
                    Poll::Ready(Err(err)) => Err(io::Error::new(io::ErrorKind::Other, err)),
                    Poll::Ready(Ok(reader)) => {
                        // OpeningFile -> File
                        self.state = State::File {
                            reader: Box::pin(reader),
                        };
                        Ok(ReadResult::Good)
                    }
                }
            }
            State::File { mut reader } => {
                let slice = buf.remaining();
                let is_empty = slice.is_empty();
                match reader.as_mut().poll_read(cx, slice) {
                    Poll::Pending => Ok(ReadResult::Pending),
                    Poll::Ready(Err(e)) => Err(e),
                    Poll::Ready(Ok(0)) if !is_empty => {
                        self.position += 1;
                        // File -> None
                        self.state = State::None;
                        Ok(ReadResult::Good)
                    }
                    Poll::Ready(Ok(len)) => {
                        buf.add_used(len);
                        Ok(ReadResult::Good)
                    },
                }
            }
            State::Trailer => {
                let archive = unsafe { &mut *self.archive };
                let trailer = archive.trailer();
                buf.extend_from_slice(&trailer);

                // Trailer -> Eof
                self.state = State::Eof;
                Ok(ReadResult::Good)
            }
            State::Eof => Ok(ReadResult::Eof),
        }
    }
}
