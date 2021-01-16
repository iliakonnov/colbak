use super::pending::PendingReader;
use super::pending::{OpeningReadFuture, Pending};
use super::write_proxy::CowSlice;
use super::write_proxy::ReadResult;
use super::Archive;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

pub struct Machine<'a> {
    _phantom: PhantomData<&'a mut Archive>,
    archive: *mut Archive,
    state: State<'a>,
    position: usize,
}

impl<'a> Machine<'a> {
    pub fn new(archive: &'a mut Archive) -> Self {
        Machine {
            _phantom: PhantomData,
            archive: archive as *mut _,
            state: State::None,
            position: 0,
        }
    }
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
        length: usize,
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
                    "Machine is in the invalid state. Prev: {}",
                ))
            }
            State::None => {
                // Switch to the next file
                let archive = unsafe { self.archive.as_mut::<'a>() }.unwrap();
                if self.position < archive.files.len() {
                    let file = &mut archive.files[self.position];
                    // None -> Header
                    self.state = State::Header { file };
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
                    Poll::Pending => {
                        self.state = State::OpeningFile { future };
                        Ok(ReadResult::Pending)
                    }
                    Poll::Ready(Err(err)) => {
                        self.state = State::OpeningFile { future };
                        Err(io::Error::new(io::ErrorKind::Other, err))
                    }
                    Poll::Ready(Ok(reader)) => {
                        // OpeningFile -> File
                        self.state = State::File {
                            reader: Box::pin(reader),
                            length: 0,
                        };
                        Ok(ReadResult::Good)
                    }
                }
            }
            State::File { mut reader, length } => {
                let slice = buf.remaining();
                let is_empty = slice.is_empty();
                match reader.as_mut().poll_read(cx, slice) {
                    Poll::Pending => {
                        self.state = State::File { reader, length };
                        Ok(ReadResult::Pending)
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = State::File { reader, length };
                        Err(e)
                    }
                    Poll::Ready(Ok(0)) if !is_empty => {
                        self.position += 1;
                        if length % 2 != 0 {
                            buf.extend_from_slice(b"0");
                        }
                        // File -> None
                        self.state = State::None;
                        Ok(ReadResult::Good)
                    }
                    Poll::Ready(Ok(len)) => {
                        self.state = State::File {
                            reader,
                            length: length + len,
                        };
                        buf.add_used(len);
                        Ok(ReadResult::Good)
                    }
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
