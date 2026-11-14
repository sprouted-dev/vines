//! Fixtures shared by the integration tests.

// Each test binary compiles this module and uses a subset of it. Clippy's
// `allow_attributes` checks outer attributes only; an `expect` here would
// fail in the binaries that do use everything.
#![allow(dead_code, reason = "each test binary uses a subset of the fixtures")]
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "fixtures run inside tests, where a panic is the failure report"
)]

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use vines::frame::Frame;
use vines::policy::SourceDecl;
use vines::run::{Exit, RunBuilder, RunContext};
use vines::sink::{Ack, DurableSink, SinkError};

pub(crate) fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("test limits are non-zero")
}

pub(crate) const GRACE: Duration = Duration::from_secs(1);

/// A run over a source that cannot be made to wait.
pub(crate) fn builder() -> RunBuilder<u32> {
    RunBuilder::new(SourceDecl::without_backpressure(), GRACE).unwrap()
}

/// A run over a source that can be made to wait.
pub(crate) fn backpressured() -> RunBuilder<u32> {
    RunBuilder::new(SourceDecl::with_backpressure(), GRACE).unwrap()
}

/// A producer that emits `0..n`, then completes with the count.
pub(crate) fn emit_all(
    n: u32,
) -> impl FnOnce(RunContext<u32>) -> BoxFuture<'static, Exit<u64, ()>> {
    move |mut ctx| {
        Box::pin(async move {
            for item in 0..n {
                ctx.emit(item).await.unwrap();
            }
            Exit::Completed(ctx.emitted())
        })
    }
}

/// Return once every task on this runtime is idle. Needs a paused clock
/// (`start_paused = true`): Tokio advances paused time only when no task
/// can run, so the timer fires after everything else has parked.
pub(crate) async fn until_idle() {
    tokio::time::sleep(Duration::from_nanos(1)).await;
}

/// Whether `future` is still pending after `wait` of paused time, with every
/// other task idle.
pub(crate) async fn still_pending_after<F: Future>(wait: Duration, future: F) -> bool {
    tokio::time::timeout(wait, future).await.is_err()
}

/// Bound a wait so a missing event fails the test instead of hanging it.
pub(crate) async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_mins(10), future)
        .await
        .expect("no event within 10 minutes of paused time")
}

/// A sink recording every frame, optionally slow, optionally failing at one
/// frame index, optionally panicking.
#[derive(Clone)]
pub(crate) struct MemorySink<P> {
    pub(crate) frames: Arc<Mutex<Vec<Frame<P>>>>,
    pub(crate) delay: Option<Duration>,
    pub(crate) fail_at: Option<usize>,
    pub(crate) panic_at: Option<usize>,
}

impl<P> MemorySink<P> {
    pub(crate) fn new() -> Self {
        Self {
            frames: Arc::new(Mutex::new(Vec::new())),
            delay: None,
            fail_at: None,
            panic_at: None,
        }
    }

    pub(crate) fn frames(&self) -> Vec<Frame<P>>
    where
        P: Clone,
    {
        self.frames.lock().unwrap().clone()
    }
}

impl<P: Send + 'static> DurableSink<P> for MemorySink<P> {
    async fn accept(&mut self, frame: Frame<P>) -> Result<Ack, SinkError> {
        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }
        let index = self.frames.lock().unwrap().len();
        assert!(
            self.panic_at != Some(index),
            "sink panicked at frame {index}"
        );
        if self.fail_at == Some(index) {
            return Err(SinkError::new(format!("disk full at frame {index}")));
        }
        self.frames.lock().unwrap().push(frame);
        Ok(Ack::accepted())
    }
}
