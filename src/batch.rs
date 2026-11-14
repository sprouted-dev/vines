//! Batching by count, by time, by size and on completion, with an explicit
//! cancellation choice.
//!
//! A batch flushes when the first of these happens:
//!
//! - it holds `max_items` items ([`FlushReason::Count`]);
//! - its byte total reaches `max_bytes`, or the next item would push it
//!   past ([`FlushReason::Bytes`]);
//! - `max_wait` has passed since its first item ([`FlushReason::Deadline`]).
//!   **The deadline fires on its own**, with no new input. (Terra's
//!   `terra-pipeline` batch checks its window only when an input arrives,
//!   so a quiet source never flushes; this operator does not repeat that.)
//! - the input ends ([`FlushReason::Completed`]).
//!
//! An item larger than `max_bytes` on its own is emitted alone with
//! [`FlushReason::Oversize`]: never silently dropped, never merged.
//!
//! On cancellation the policy's [`OnCancel`] decides: flush what is held
//! ([`FlushReason::Cancelled`]) or discard it. Either way the stream then
//! yields one [`BatchEvent::Cancelled`] (carrying the discard count, which
//! may be zero) and ends, so a cancelled batcher is never mistaken for a
//! completed one.
//!
//! Batches are in memory. This is legitimate only where loss on process
//! death is acceptable; a batch that feeds a durable effect needs a
//! retained manifest, which is the consumer's (Terra's) job.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{Fuse, Stream, StreamExt};
use futures::FutureExt;
use pin_project_lite::pin_project;
use tokio::time::{Instant, Sleep};

use crate::cancel::{CancelCause, CancelScope};
use crate::error::PolicyError;

/// What happens to a partial batch when the scope is cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OnCancel {
    /// Emit what is held as a [`FlushReason::Cancelled`] batch.
    Flush,
    /// Drop what is held; the count is reported in
    /// [`BatchEvent::Cancelled`].
    Discard,
}

type SizeFn<T> = Arc<dyn Fn(&T) -> usize + Send + Sync>;

/// A batching policy. There is no default: `max_items` and the cancel
/// choice are required.
pub struct BatchPolicy<T> {
    max_items: NonZeroUsize,
    max_wait: Option<Duration>,
    max_bytes: Option<(NonZeroUsize, SizeFn<T>)>,
    on_cancel: OnCancel,
}

impl<T> Clone for BatchPolicy<T> {
    fn clone(&self) -> Self {
        Self {
            max_items: self.max_items,
            max_wait: self.max_wait,
            max_bytes: self.max_bytes.clone(),
            on_cancel: self.on_cancel,
        }
    }
}

impl<T> fmt::Debug for BatchPolicy<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchPolicy")
            .field("max_items", &self.max_items)
            .field("max_wait", &self.max_wait)
            .field("max_bytes", &self.max_bytes.as_ref().map(|(max, _)| *max))
            .field("on_cancel", &self.on_cancel)
            .finish()
    }
}

impl<T> BatchPolicy<T> {
    /// Flush every `max_items` items and on completion; on cancel, do
    /// `on_cancel`.
    pub fn new(max_items: NonZeroUsize, on_cancel: OnCancel) -> Self {
        Self {
            max_items,
            max_wait: None,
            max_bytes: None,
            on_cancel,
        }
    }

    /// Also flush `max_wait` after a batch's first item, whether or not
    /// more input arrives.
    ///
    /// # Errors
    ///
    /// [`PolicyError::ZeroDuration`] when `max_wait` is zero.
    pub fn max_wait(mut self, max_wait: Duration) -> Result<Self, PolicyError> {
        if max_wait.is_zero() {
            return Err(PolicyError::ZeroDuration {
                scope: "batch".to_string(),
                field: "max_wait",
            });
        }
        self.max_wait = Some(max_wait);
        Ok(self)
    }

    /// Also flush before a batch's total `size` would exceed `max_bytes`.
    #[must_use]
    pub fn max_bytes(
        mut self,
        max_bytes: NonZeroUsize,
        size: impl Fn(&T) -> usize + Send + Sync + 'static,
    ) -> Self {
        self.max_bytes = Some((max_bytes, Arc::new(size)));
        self
    }
}

/// Why a batch was emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FlushReason {
    /// It reached `max_items`.
    Count,
    /// It reached `max_bytes`, or the next item would not fit.
    Bytes,
    /// A single item larger than `max_bytes`, emitted alone.
    Oversize,
    /// `max_wait` passed since its first item.
    Deadline,
    /// The input ended.
    Completed,
    /// The scope was cancelled and the policy is [`OnCancel::Flush`].
    Cancelled,
}

/// One emitted batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch<T> {
    /// The items, in input order.
    pub items: Vec<T>,
    /// Their total size by the policy's size function (0 without one).
    pub bytes: usize,
    /// Why it was emitted.
    pub reason: FlushReason,
}

/// What a batcher discarded on cancel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Discarded {
    /// Items discarded.
    pub items: usize,
    /// Their total size (0 without a size function).
    pub bytes: usize,
}

/// What a batcher yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchEvent<T> {
    /// A batch.
    Batch(Batch<T>),
    /// The scope was cancelled. Always the last event of a cancelled
    /// batcher; `discarded` is what [`OnCancel::Discard`] dropped (zero
    /// under [`OnCancel::Flush`] or with nothing held).
    Cancelled {
        /// Why the scope was cancelled.
        cause: CancelCause,
        /// What was dropped.
        discarded: Discarded,
    },
}

impl<T> BatchEvent<T> {
    /// The batch, if this is one.
    pub fn into_batch(self) -> Option<Batch<T>> {
        match self {
            Self::Batch(batch) => Some(batch),
            Self::Cancelled { .. } => None,
        }
    }
}

pin_project! {
    /// Stream returned by [`batch`].
    #[must_use = "streams do nothing unless polled"]
    pub struct Batcher<S: Stream> {
        #[pin]
        input: Fuse<S>,
        policy: BatchPolicy<S::Item>,
        held: Vec<S::Item>,
        bytes: usize,
        deadline: Option<Pin<Box<Sleep>>>,
        cancel: Option<BoxFuture<'static, CancelCause>>,
        out: VecDeque<BatchEvent<S::Item>>,
        finished: bool,
    }
}

/// Batch `input` under `policy`, cancelled by `scope`.
///
/// Dropping the batcher is not cancellation: whatever it holds is dropped
/// with it, unreported. Cancel the scope to get the declared behaviour.
pub fn batch<S: Stream>(input: S, policy: BatchPolicy<S::Item>, scope: &CancelScope) -> Batcher<S> {
    let scope = scope.clone();
    Batcher {
        input: input.fuse(),
        policy,
        held: Vec::new(),
        bytes: 0,
        deadline: None,
        cancel: Some(async move { scope.cancelled().await }.boxed()),
        out: VecDeque::new(),
        finished: false,
    }
}

struct Held<'a, T> {
    policy: &'a BatchPolicy<T>,
    items: &'a mut Vec<T>,
    bytes: &'a mut usize,
    deadline: &'a mut Option<Pin<Box<Sleep>>>,
    out: &'a mut VecDeque<BatchEvent<T>>,
}

impl<T> Held<'_, T> {
    fn flush(&mut self, reason: FlushReason) {
        *self.deadline = None;
        if self.items.is_empty() {
            return;
        }
        self.out.push_back(BatchEvent::Batch(Batch {
            items: std::mem::take(self.items),
            bytes: std::mem::take(self.bytes),
            reason,
        }));
    }

    fn push(&mut self, item: T) {
        let size = match &self.policy.max_bytes {
            Some((max, size_of)) => {
                let size = size_of(&item);
                if size > max.get() {
                    self.flush(FlushReason::Bytes);
                    self.out.push_back(BatchEvent::Batch(Batch {
                        items: vec![item],
                        bytes: size,
                        reason: FlushReason::Oversize,
                    }));
                    return;
                }
                if *self.bytes + size > max.get() {
                    self.flush(FlushReason::Bytes);
                }
                size
            }
            None => 0,
        };
        if self.items.is_empty() {
            if let Some(wait) = self.policy.max_wait {
                *self.deadline = Some(Box::pin(tokio::time::sleep_until(Instant::now() + wait)));
            }
        }
        self.items.push(item);
        *self.bytes += size;
        if self.items.len() >= self.policy.max_items.get() {
            self.flush(FlushReason::Count);
        } else if let Some((max, _)) = &self.policy.max_bytes {
            if *self.bytes >= max.get() {
                self.flush(FlushReason::Bytes);
            }
        }
    }

    fn cancel(&mut self, cause: CancelCause) {
        let discarded = match self.policy.on_cancel {
            OnCancel::Flush => {
                self.flush(FlushReason::Cancelled);
                Discarded::default()
            }
            OnCancel::Discard => {
                *self.deadline = None;
                let discarded = Discarded {
                    items: self.items.len(),
                    bytes: *self.bytes,
                };
                self.items.clear();
                *self.bytes = 0;
                discarded
            }
        };
        self.out
            .push_back(BatchEvent::Cancelled { cause, discarded });
    }
}

impl<S: Stream> Stream for Batcher<S> {
    type Item = BatchEvent<S::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(event) = this.out.pop_front() {
                return Poll::Ready(Some(event));
            }
            if *this.finished {
                return Poll::Ready(None);
            }
            let mut held = Held {
                policy: this.policy,
                items: this.held,
                bytes: this.bytes,
                deadline: this.deadline,
                out: this.out,
            };
            // Cancellation first: once requested, no more input is taken.
            if let Some(cancel) = this.cancel.as_mut() {
                if let Poll::Ready(cause) = cancel.poll_unpin(cx) {
                    *this.cancel = None;
                    held.cancel(cause);
                    *this.finished = true;
                    continue;
                }
            }
            // The deadline fires on its own, input or not.
            if let Some(deadline) = held.deadline.as_mut() {
                if deadline.as_mut().poll(cx).is_ready() {
                    held.flush(FlushReason::Deadline);
                    continue;
                }
            }
            match this.input.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => held.push(item),
                Poll::Ready(None) => {
                    held.flush(FlushReason::Completed);
                    *this.cancel = None;
                    *this.finished = true;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
