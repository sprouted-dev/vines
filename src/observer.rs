//! The consumer side of fan-out: ephemeral observer streams and durable
//! observer handles, with their failures and counts.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};

use crate::frame::{Frame, Record};
use crate::outlet::Outlet;
use crate::policy::ObserverPolicy;
use crate::sink::{DurableSink, SinkError};

/// Why an observer failed. An observer with `answer = none` failing never
/// fails the run; one with `answer = required` gates the run's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserverFailure {
    /// `loss = none` with `overflow = spill`, and the spill quota ran out.
    SpillQuotaExhausted {
        /// The quota that ran out.
        quota: std::num::NonZeroUsize,
    },
    /// The durable sink rejected a frame.
    Sink(SinkError),
    /// The consumer dropped its stream before the end marker.
    Abandoned,
    /// The consumer reported failure with [`ObserverStream::fail`].
    Consumer(String),
    /// The durable sink panicked.
    Panicked,
    /// The durable drain was dropped before it settled (the run was never
    /// spawned, or the runtime shut down).
    Dropped,
    /// A required observer had not settled when the run's cancellation
    /// grace ran out.
    Unsettled,
}

/// Running counts for one observer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObserverCounts {
    /// Items offered to this observer.
    pub offered: u64,
    /// Items the consumer (or the durable drain) took from the queue.
    pub delivered: u64,
    /// Items this observer will never see (dropped, missed by joining late,
    /// or emitted after it failed).
    pub lost: u64,
    /// Gap markers queued.
    pub gaps: u64,
    /// Items admitted into the spill quota.
    pub spilled: u64,
    /// Items offered after the consumer stopped reading; not delivered.
    pub discarded: u64,
    /// Item frames a durable sink acknowledged.
    pub acknowledged: u64,
    /// Most items queued at once.
    pub peak_depth: usize,
}

/// A snapshot of one observer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserverReport {
    /// The observer's name.
    pub name: String,
    /// Its policy.
    pub policy: ObserverPolicy,
    /// Its counts at the time of the snapshot.
    pub counts: ObserverCounts,
    /// Its failure, if it failed.
    pub failure: Option<ObserverFailure>,
    /// Its settlement: `None` while it is still consuming.
    pub settled: Option<Result<(), ObserverFailure>>,
}

/// An ephemeral observer: a stream of [`Frame`]s ending with one
/// [`Frame::End`].
///
/// Dropping it before the end marker abandons the observer
/// ([`ObserverFailure::Abandoned`]); the run goes on. For an
/// `answer = required` observer, taking the end marker is what settles it,
/// so read it concurrently with awaiting the run's report.
pub struct ObserverStream<P> {
    outlet: Arc<Outlet<P>>,
}

impl<P> ObserverStream<P> {
    pub(crate) fn new(outlet: Arc<Outlet<P>>) -> Self {
        Self { outlet }
    }

    /// The observer's name.
    pub fn name(&self) -> &str {
        self.outlet.name()
    }

    /// The observer's counts so far.
    pub fn counts(&self) -> ObserverCounts {
        self.outlet.counts()
    }

    /// Report that this consumer failed. With `answer = required` this
    /// gates the run's answer; with `answer = none` it is only reported.
    pub fn fail(self, reason: impl Into<String>) {
        self.outlet.fail(ObserverFailure::Consumer(reason.into()));
    }

    /// Collect every frame up to and including the end marker.
    pub async fn into_record(mut self) -> Record<P> {
        let mut record = Record {
            items: Vec::new(),
            gaps: Vec::new(),
            end: None,
        };
        while let Some(frame) = self.next().await {
            match frame {
                Frame::Item { seq, item } => record.items.push((seq, item)),
                Frame::Gap(gap) => record.gaps.push(gap),
                Frame::End(end) => record.end = Some(end),
            }
        }
        record
    }
}

impl<P> Stream for ObserverStream<P> {
    type Item = Frame<P>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Frame<P>>> {
        let polled = self.outlet.poll_pop(cx);
        if let Poll::Ready(Some(Frame::End(_))) = &polled {
            self.outlet.settle_at_end();
        }
        polled
    }
}

impl<P> Drop for ObserverStream<P> {
    fn drop(&mut self) {
        self.outlet.abandon();
    }
}

/// A durable observer: vines drains its queue into the
/// [`DurableSink`] in a task of its own. With `answer = none` the drain
/// outlives the run's answer; await [`Self::finished`] for its end.
pub struct DurableObserver<P> {
    outlet: Arc<Outlet<P>>,
}

impl<P> DurableObserver<P> {
    /// The observer's name.
    pub fn name(&self) -> &str {
        self.outlet.name()
    }

    /// The observer's counts so far.
    pub fn counts(&self) -> ObserverCounts {
        self.outlet.counts()
    }

    /// Wait until the sink acknowledged the end marker or the observer
    /// failed, then report.
    pub async fn finished(self) -> ObserverReport {
        self.outlet.wait_settled().await;
        self.outlet.report()
    }
}

/// Settles the outlet if the drain ends without settling (panic, drop).
struct DrainGuard<P> {
    outlet: Arc<Outlet<P>>,
}

impl<P> Drop for DrainGuard<P> {
    fn drop(&mut self) {
        if self.outlet.settlement().is_none() {
            let failure = if std::thread::panicking() {
                ObserverFailure::Panicked
            } else {
                ObserverFailure::Dropped
            };
            self.outlet.fail(failure);
        }
    }
}

/// The handle and the drain future for a durable observer. The guard is
/// built now, so dropping the never-run future still settles.
pub(crate) fn durable<P, S>(
    outlet: Arc<Outlet<P>>,
    mut sink: S,
) -> (
    DurableObserver<P>,
    impl std::future::Future<Output = ()> + Send + 'static,
)
where
    P: Send + 'static,
    S: DurableSink<P>,
{
    let guard = DrainGuard {
        outlet: Arc::clone(&outlet),
    };
    let drain = async move {
        let guard = guard;
        let outlet = &guard.outlet;
        while let Some(frame) = outlet.pop().await {
            let is_item = matches!(frame, Frame::Item { .. });
            let is_end = matches!(frame, Frame::End(_));
            match sink.accept(frame).await {
                Ok(_ack) => {
                    if is_item {
                        outlet.acknowledged_item();
                    }
                    if is_end {
                        outlet.settle_at_end();
                        break;
                    }
                }
                Err(error) => {
                    outlet.fail(ObserverFailure::Sink(error));
                    break;
                }
            }
        }
        // A closed outlet whose consumer side was released (never reached
        // the end marker) is a drop, settled by the guard.
    };
    (DurableObserver { outlet }, drain)
}
