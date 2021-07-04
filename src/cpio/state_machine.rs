// FIXME: Write good docs for this small module

use crate::cpio::smart_read::SmartBuf;
use std::io;
use std::task::{Context, Poll};

pub enum AdvanceResult<Src, Dst> {
    Pending(Src),
    Ready(Dst),
    Failed(io::Error),
}

impl<Src, Dst> AdvanceResult<Src, Dst> {
    pub fn unpack<S>(
        self,
        pending: impl FnOnce(Src) -> S,
        ready: impl FnOnce(Dst) -> S,
        failed: impl FnOnce() -> S,
    ) -> (S, Poll<io::Result<()>>) {
        match self {
            AdvanceResult::Pending(p) => (pending(p), Poll::Pending),
            AdvanceResult::Ready(r) => (ready(r), Poll::Ready(Ok(()))),
            AdvanceResult::Failed(e) => (failed(), Poll::Ready(Err(e))),
        }
    }
}

pub trait Advanceable: Sized {
    type Next;
    fn advance(
        self,
        cx: &mut Context<'_>,
        buf: &mut SmartBuf<'_, '_, '_>,
    ) -> AdvanceResult<Self, Self::Next>;
}

macro_rules! match_advance {
    {
        match $state:ident.$advance:ident($cx:expr, $buf:expr) {
            $poisoned_path:path => $poisoned:expr,
            $(
                $variant:path => $then:expr,
            )*
        }
    } => {
        match $state {
            $poisoned_path => $poisoned,
            $(
                $variant(x) => x.$advance($cx, $buf).unpack($variant, $then, || $poisoned_path),
            )*
        }
    }
}
