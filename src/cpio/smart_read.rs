use pin_project_lite::pin_project;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

pin_project! {
    pub struct SmartReader<T> {
        #[pin]
        inner: T,
        buffer: Vec<u8>,
        start: usize,
    }
}

impl<T> SmartReader<T> {
    pub fn new(inner: T) -> Self {
        SmartReader {
            inner,
            buffer: Vec::new(),
            start: 0,
        }
    }
}

pub struct SmartBuf<'a, 'b, 'c> {
    real: &'a mut ReadBuf<'b>,
    buffer: &'c mut Vec<u8>,
    is_empty: bool,
    is_eof: bool,
}

impl<'a, 'b, 'c> SmartBuf<'a, 'b, 'c> {
    pub(crate) fn put_slice(&mut self, slice: &[u8]) {
        if slice.is_empty() {
            return;
        }

        let mid = self.real.remaining();
        if mid > slice.len() {
            // We can write everything right to the `real`.
            self.real.put_slice(slice);
        } else {
            // Some part will go to the buffer
            let (fits, other) = slice.split_at(mid);
            self.real.put_slice(fits);
            self.buffer.extend_from_slice(other);
        }
        self.is_empty = false;
    }

    pub fn eof(&mut self) {
        self.is_eof = true;
    }

    pub fn fill_using<T: AsyncRead + Unpin>(
        &mut self,
        other: Pin<&mut T>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Option<&[u8]>>> {
        let mut temp = unsafe {
            let unfilled = self.real.unfilled_mut();
            ReadBuf::uninit(unfilled)
        };
        match other.poll_read(cx, &mut temp) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {
                let capacity = temp.capacity();
                let filled = temp.filled().len();
                if filled == 0 && capacity != 0 {
                    // EOF:
                    return Poll::Ready(Ok(None));
                }
                if filled != 0 {
                    unsafe {
                        self.real.assume_init(filled);
                        self.real.advance(filled);
                        self.is_empty = false;
                    }
                }
                let buf = self.real.filled();
                let buf = &buf[buf.len() - filled..];
                Poll::Ready(Ok(Some(buf)))
            }
        }
    }
}

pub trait SmartRead {
    fn amortized_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut SmartBuf<'_, '_, '_>,
    ) -> Poll<io::Result<()>>;
}

impl<T> AsyncRead for SmartReader<T>
where
    T: SmartRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        read_buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Exit early, do nothing.
        if read_buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let this = self.project();

        // Check if we have some content buffered.
        {
            let buffered = &this.buffer[*this.start..];
            if !buffered.is_empty() {
                // If so, write it and return.
                let to_write = buffered.len().min(read_buf.remaining());
                read_buf.put_slice(&buffered[..to_write]);
                *this.start += to_write;
                return Poll::Ready(Ok(()));
            }
        }

        // At this moment there is no useful data in buffer:
        let buffer: &mut Vec<u8> = this.buffer;
        buffer.clear();
        *this.start = 0;

        // Now try to get data from inner, until something is written.
        let mut buf = SmartBuf {
            real: read_buf,
            buffer,
            is_empty: true,
            is_eof: false,
        };

        let mut inner: Pin<&mut T> = this.inner;
        loop {
            match inner.as_mut().amortized_read(cx, &mut buf) {
                Poll::Pending => return Poll::Pending,
                err @ Poll::Ready(Err(_)) => return err,
                Poll::Ready(Ok(())) => {
                    if !(buf.is_empty) {
                        // When something is written, we just return.
                        return Poll::Ready(Ok(()));
                    }
                    // Amortizer allows inner to write nothing.
                    if buf.is_eof {
                        // So it should explicitly mark that there is no more data.
                        return Poll::Ready(Ok(()));
                    } else {
                        // Otherwise we will try reading again.
                        continue;
                    }
                }
            }
        }
    }
}
