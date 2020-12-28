use std::path::PathBuf;
use std::pin::Pin;
use std::task::Poll;

use tar::Header;
use tokio::io::AsyncRead;

struct TarredFile {
    name: PathBuf,
    src: Pin<Box<dyn AsyncRead>>,
}

struct HeaderIterator {
    header: Header,
    position: usize,
    file: Pin<Box<dyn AsyncRead>>,
}

enum CurrentPosition {
    None,
    Header(HeaderIterator),
    File(Pin<Box<dyn AsyncRead>>),
}

struct AsyncTar {
    currently_reading: CurrentPosition,
    waiting: Vec<TarredFile>,
}

impl AsyncTar {
    fn new() -> Self {
        Self {
            currently_reading: CurrentPosition::None,
            waiting: Vec::new(),
        }
    }
}

impl AsyncRead for AsyncTar {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        mut buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut length = 0;
        while !buf.is_empty() {
            let currently_reading = std::mem::replace(&mut self.currently_reading, CurrentPosition::None);
            let new = match currently_reading {
                CurrentPosition::None => match self.waiting.pop() {
                    Some(f) => {
                        let mut header = Header::new_gnu();
                        header.set_path(f.name);
                        header.set_cksum();
                        CurrentPosition::Header(HeaderIterator {
                            file: f.src,
                            header,
                            position: 0,
                        })
                    }
                    None => return Poll::Ready(Ok(length)),
                },
                CurrentPosition::Header(mut h) => {
                    let slice = &h.header.as_bytes()[h.position..];
                    if slice.is_empty() {
                        // Done eith header, so let's switch to the file contents.
                        CurrentPosition::File(h.file)
                    } else {
                        let to_copy = slice.len().min(buf.len());
                        let slice = &slice[..to_copy];
                        buf.copy_from_slice(slice);
                        buf = &mut buf[to_copy..];
                        length += to_copy;
                        h.position += to_copy;

                        // Continue reading header. Probably we done
                        CurrentPosition::Header(h)
                    }
                }
                CurrentPosition::File(mut f) => {
                    let len = match AsyncRead::poll_read(f.as_mut(), cx, buf)? {
                        Poll::Ready(x) => x,
                        Poll::Pending => return Poll::Pending,
                    };
                    if len == 0 {
                        // buf is not empty (loop condition), but file is empty.
                        // So we should switch to the enxt item
                        CurrentPosition::None
                    } else {
                        length += len;
                        buf = &mut buf[len..];

                        // Continue reading
                        CurrentPosition::File(f)
                    }
                }
            };
            self.currently_reading = new;
        }
        Poll::Ready(Ok(length))
    }
}
