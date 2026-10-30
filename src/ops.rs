//! Basic stream operators: map, filter, tap with an explicit failure
//! policy, and bounded parallel map with declared output order.
//!
//! These are thin on purpose. `map`, `filter` and `par_map` delegate to
//! `futures`; what vines adds is that the choices that matter (what a tap
//! failure does, whether parallel output keeps input order, how many run at
//! once) are parameters with no default.

use std::future::{ready, Future};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use futures::future::Either;
use futures::stream::{Stream, StreamExt};
use pin_project_lite::pin_project;

/// Map every item.
pub fn map<S, F, U>(stream: S, f: F) -> impl Stream<Item = U>
where
    S: Stream,
    F: FnMut(S::Item) -> U,
{
    stream.map(f)
}

/// Keep the items `predicate` accepts.
pub fn filter<S, F>(stream: S, mut predicate: F) -> impl Stream<Item = S::Item>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool,
{
    stream.filter(move |item| ready(predicate(item)))
}

/// What an isolated tap does after it fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TapIsolation {
    /// Keep calling the tap on later items. Every failure is counted.
    KeepCalling,
    /// Stop calling the tap after its first failure. Later items still
    /// pass; they are counted as skipped.
    DetachOnFirstFailure,
}

/// What an isolated tap did, shared with its stream. Clone to keep a handle.
#[derive(Debug)]
pub struct TapStats<E> {
    inner: Arc<Mutex<TapSnapshot<E>>>,
}

impl<E> Clone for TapStats<E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<E: Clone> TapStats<E> {
    /// The counts so far.
    pub fn snapshot(&self) -> TapSnapshot<E> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Counts for an isolated tap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TapSnapshot<E> {
    /// Times the tap was called.
    pub calls: u64,
    /// Times it returned an error.
    pub failures: u64,
    /// Items that passed without the tap being called (after detaching).
    pub skipped: u64,
    /// The first error, kept for the report.
    pub first_failure: Option<E>,
    /// Whether the tap is detached.
    pub detached: bool,
}

/// Call `f` on every item, passing items through unchanged. A tap failure
/// never fails the stream: it is counted in the returned [`TapStats`], and
/// `isolation` decides whether the tap keeps being called.
///
/// `f` runs inline, so a slow tap slows the stream. For a slow or
/// asynchronous observer use a fan-out observer
/// ([`crate::run::RunBuilder::observe`]), which has its own queue.
pub fn tap<S, F, E>(
    stream: S,
    isolation: TapIsolation,
    mut f: F,
) -> (impl Stream<Item = S::Item>, TapStats<E>)
where
    S: Stream,
    F: FnMut(&S::Item) -> Result<(), E>,
{
    let stats = TapStats {
        inner: Arc::new(Mutex::new(TapSnapshot {
            calls: 0,
            failures: 0,
            skipped: 0,
            first_failure: None,
            detached: false,
        })),
    };
    let shared = Arc::clone(&stats.inner);
    let tapped = stream.map(move |item| {
        let mut state = shared.lock().unwrap_or_else(PoisonError::into_inner);
        if state.detached {
            state.skipped += 1;
            return item;
        }
        state.calls += 1;
        if let Err(error) = f(&item) {
            state.failures += 1;
            if state.first_failure.is_none() {
                state.first_failure = Some(error);
            }
            if isolation == TapIsolation::DetachOnFirstFailure {
                state.detached = true;
            }
        }
        item
    });
    (tapped, stats)
}

/// A required tap failed: the stream yields this once and ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TapFailed<T, E> {
    /// 0-based index of the item whose tap failed.
    pub index: u64,
    /// That item, not passed downstream as a success.
    pub item: T,
    /// The tap's error.
    pub error: E,
}

pin_project! {
    /// Stream returned by [`tap_required`].
    #[must_use = "streams do nothing unless polled"]
    pub struct TapRequired<S, F> {
        #[pin]
        stream: S,
        f: F,
        index: u64,
        failed: bool,
    }
}

/// Call `f` on every item; a failure fails the stream. Items pass as
/// `Ok(item)`. The first failure yields `Err(TapFailed)` carrying the item,
/// and then the stream ends without pulling another item.
pub fn tap_required<S, F, E>(stream: S, f: F) -> TapRequired<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> Result<(), E>,
{
    TapRequired {
        stream,
        f,
        index: 0,
        failed: false,
    }
}

impl<S, F, E> Stream for TapRequired<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> Result<(), E>,
{
    type Item = Result<S::Item, TapFailed<S::Item, E>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.failed {
            return Poll::Ready(None);
        }
        let Some(item) = futures::ready!(this.stream.poll_next(cx)) else {
            return Poll::Ready(None);
        };
        let index = *this.index;
        *this.index += 1;
        match (this.f)(&item) {
            Ok(()) => Poll::Ready(Some(Ok(item))),
            Err(error) => {
                *this.failed = true;
                Poll::Ready(Some(Err(TapFailed { index, item, error })))
            }
        }
    }
}

/// Output order for [`par_map`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputOrder {
    /// Results come out in input order. A slow item holds back faster ones
    /// behind it (head-of-line blocking), but never more than `limit` run.
    Ordered,
    /// Results come out as they complete.
    Unordered,
}

/// Run `f` on up to `limit` items at once. Never unbounded: `limit` is a
/// [`NonZeroUsize`]. Dropping the stream drops the in-flight futures.
pub fn par_map<S, F, Fut>(
    stream: S,
    limit: NonZeroUsize,
    order: OutputOrder,
    f: F,
) -> impl Stream<Item = Fut::Output>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future,
{
    let futures = stream.map(f);
    match order {
        OutputOrder::Ordered => Either::Left(futures.buffered(limit.get())),
        OutputOrder::Unordered => Either::Right(futures.buffer_unordered(limit.get())),
    }
}
