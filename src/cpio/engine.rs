use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use pin_project::pin_project;

use tokio::io::AsyncRead;

use super::machine::Machine;

pub enum ReadResult {
    Pending,
    Eof,
    Good,
}

pub struct CowSlice<'a, 'b> {
    buffer: &'a mut [u8],
    used: usize,
    more: &'b mut Vec<u8>,
}

impl CowSlice<'_, '_> {
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        let remaining = self.remaining();

        if remaining.len() > slice.len() {
            // All good!
            remaining[..slice.len()].copy_from_slice(slice);
            self.used += slice.len();
            return;
        }

        // Beginning goes to slice, but everything after goes to the vec
        let (fits, additional) = slice.split_at(remaining.len());
        remaining.copy_from_slice(fits);
        self.used += fits.len();

        self.more.extend_from_slice(additional);
    }

    pub fn remaining(&mut self) -> &mut [u8] {
        &mut self.buffer[self.used..]
    }

    pub fn is_empty(&self) -> bool {
        self.used == 0
    }

    pub fn add_used(&mut self, used: usize) {
        self.used += used;
    }
}

#[pin_project]
pub struct MachineBuffer<'a> {
    #[pin]
    machine: Machine<'a>,
    buffer: Vec<u8>,
    start: usize,
}

impl<'a> MachineBuffer<'a> {
    pub fn new(machine: Machine<'a>) -> Self {
        Self {
            machine,
            buffer: Vec::new(),
            start: 0
        }
    }
}

impl<'a> AsyncRead for MachineBuffer<'a> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if !self.buffer.is_empty() {
            let buffer = &self.buffer[self.start..];
            if buf.len() < buffer.len() {
                // Can't fit everything
                buf.copy_from_slice(&buffer[..buf.len()]);
                self.start += buf.len();
                Poll::Ready(Ok(buf.len()))
            } else {
                // We can copy whole buffer to the buf
                let len = buffer.len();
                buf[..buffer.len()].copy_from_slice(buffer);

                self.buffer.clear();
                self.start = 0;
                Poll::Ready(Ok(len))
            }
        } else {
            let mut this = self.project();

            let buflen = buf.len();
            let mut cow = CowSlice {
                buffer: buf,
                used: 0,
                more: &mut this.buffer,
            };

            let mut machine: Pin<&mut Machine> = this.machine;
            loop {
                match machine.as_mut().read(cx, &mut cow) {
                    Err(err) => return Poll::Ready(Err(err)),
                    Ok(ReadResult::Pending) => return Poll::Pending,
                    Ok(ReadResult::Eof) => return Poll::Ready(Ok(0)),
                    Ok(ReadResult::Good) => {
                        if buflen == 0 {
                            return Poll::Ready(Ok(0))
                        } else if cow.is_empty() {
                            continue
                        } else {
                            let len = cow.used;
                            return Poll::Ready(Ok(len))
                        }
                    }
                };
            }
        }
    }
}
