use crate::DefaultDigest;
use std::pin::Pin;
use std::task::Poll;

use pin_project_lite::pin_project;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};

pin_project! {
    /// Acts as wrapper around `R` that computes hash of all passing data.
    ///
    /// `R` may be [`AsyncRead`](AsyncWrite) or [`AsyncWrite`](AsyncWrite).
    pub struct StreamHash<R, D = DefaultDigest> {
        #[pin]
        inner: R,
        digest: D,
    }
}

pub fn stream_hash<R>(inner: R) -> StreamHash<R, DefaultDigest> {
    StreamHash::new(inner)
}

impl<R, D: digest::Digest> StreamHash<R, D> {
    #[must_use]
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            digest: D::new(),
        }
    }

    pub fn finalize(self) -> digest::Output<D> {
        self.digest.finalize()
    }
}

impl<R, D> AsyncRead for StreamHash<R, D>
where
    R: AsyncRead,
    D: digest::Digest,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let mut this = self.project();
        let result = this.inner.as_mut().poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = result {
            let filled = buf.filled();
            this.digest.update(filled);
        }
        result
    }
}

impl<R, D> AsyncWrite for StreamHash<R, D>
where
    R: AsyncWrite,
    D: digest::Digest,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let mut this = self.project();
        let result = this.inner.as_mut().poll_write(cx, buf);
        if let Poll::Ready(Ok(len)) = result {
            let filled = &buf[..len];
            this.digest.update(filled);
        }
        result
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}
