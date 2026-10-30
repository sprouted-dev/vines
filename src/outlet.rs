//! One observer's bounded queue, shared by the run's producer side and the
//! observer's consumer side. Internal: the public faces are
//! [`crate::observer::ObserverStream`] and
//! [`crate::observer::DurableObserver`].
//!
//! Invariants:
//!
//! - Item frames in the queue never exceed the policy's holding limit.
//! - Gap frames take no capacity; at most one sits between two items, so
//!   the queue stays bounded by twice the limit plus the end marker.
//! - Order is the order of emission. A drop never reorders what remains.
//! - Settlement (for answer gating) is decided once; the first decision
//!   stands.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use tokio::sync::{watch, Notify};

use crate::frame::{Coverage, End, Ending, Frame, Gap, GapReason};
use crate::observer::{ObserverCounts, ObserverFailure, ObserverReport};
use crate::policy::{Loss, ObserverPolicy, Overflow};

pub(crate) type Settlement = Option<Result<(), ObserverFailure>>;

pub(crate) struct Outlet<P> {
    name: String,
    policy: ObserverPolicy,
    state: Mutex<State<P>>,
    /// Woken when an item leaves the queue or the outlet closes; the
    /// producer waits on it under `stop_producer`.
    writable: Notify,
    settled: watch::Sender<Settlement>,
}

struct State<P> {
    queue: VecDeque<Frame<P>>,
    /// Item frames in `queue`.
    items: usize,
    /// A gap still growing at the tail; pushed before the next item or the
    /// end marker.
    tail_gap: Option<Gap>,
    counts: ObserverCounts,
    /// The consumer stopped reading (dropped, failed, or its sink failed).
    gone: bool,
    /// The end marker has been queued; nothing more is offered.
    ended: bool,
    /// The consumer has taken the end marker.
    end_taken: bool,
    failure: Option<ObserverFailure>,
    waker: Option<Waker>,
}

impl<P> Outlet<P> {
    /// A new outlet that joins after `missed` items were already emitted.
    pub(crate) fn new(name: String, policy: ObserverPolicy, missed: u64) -> Arc<Self> {
        let mut state = State {
            queue: VecDeque::new(),
            items: 0,
            tail_gap: None,
            counts: ObserverCounts::default(),
            gone: false,
            ended: false,
            end_taken: false,
            failure: None,
            waker: None,
        };
        if missed > 0 {
            state.queue.push_back(Frame::Gap(Gap {
                reason: GapReason::JoinedLate,
                first_seq: 0,
                last_seq: missed - 1,
            }));
            state.counts.gaps = 1;
            state.counts.lost = missed;
        }
        let (settled, _) = watch::channel(None);
        Arc::new(Self {
            name,
            policy,
            state: Mutex::new(state),
            writable: Notify::new(),
            settled,
        })
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    fn lock(&self) -> MutexGuard<'_, State<P>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn is_stop_producer(&self) -> bool {
        self.policy.overflow == Overflow::StopProducer
    }

    /// Whether a `stop_producer` producer may emit now.
    pub(crate) fn has_space(&self) -> bool {
        let state = self.lock();
        state.gone || state.ended || state.items < self.policy.queue.get()
    }

    pub(crate) fn writable(&self) -> &Notify {
        &self.writable
    }

    /// Offer one item. Never waits.
    pub(crate) fn offer(&self, seq: u64, item: P) {
        let waker = {
            let mut state = self.lock();
            state.counts.offered += 1;
            if state.gone || state.ended {
                state.counts.discarded += 1;
                return;
            }
            if state.failure.is_some() {
                state.counts.lost += 1;
                state.extend_tail_gap(GapReason::ObserverFailed, seq);
                return;
            }
            if state.items < self.policy.queue.get() {
                state.push_item(seq, item);
            } else {
                match self.policy.overflow {
                    Overflow::DropNewest => {
                        state.counts.lost += 1;
                        state.extend_tail_gap(GapReason::DroppedNewest, seq);
                    }
                    Overflow::DropOldest => {
                        state.drop_oldest_item();
                        state.push_item(seq, item);
                    }
                    // The producer waited for space before offering; this
                    // only exceeds the queue if the consumer raced it, and
                    // then by at most one item per emit.
                    Overflow::StopProducer => state.push_item(seq, item),
                    Overflow::Spill { quota } => {
                        if state.items < self.policy.holding_limit() {
                            state.counts.spilled += 1;
                            state.push_item(seq, item);
                        } else if self.policy.loss == Loss::Allowed {
                            state.counts.lost += 1;
                            state.extend_tail_gap(GapReason::SpillExhausted, seq);
                        } else {
                            let failure = ObserverFailure::SpillQuotaExhausted { quota };
                            state.failure = Some(failure.clone());
                            state.counts.lost += 1;
                            state.extend_tail_gap(GapReason::ObserverFailed, seq);
                            drop(state);
                            self.settle(Err(failure));
                            return self.wake();
                        }
                    }
                }
            }
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn wake(&self) {
        let waker = self.lock().waker.take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Queue the end marker. Idempotent.
    pub(crate) fn close(&self, ending: Ending, emitted: u64) {
        let waker = {
            let mut state = self.lock();
            if state.ended {
                return;
            }
            state.flush_tail_gap();
            let coverage = Coverage {
                emitted,
                lost: state.counts.lost,
                gaps: state.counts.gaps,
                failure: state.failure.clone(),
            };
            if !state.gone {
                state.queue.push_back(Frame::End(End { ending, coverage }));
            }
            state.ended = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        self.writable.notify_waiters();
    }

    /// Take the next frame. `Ready(None)` after the end marker was taken.
    pub(crate) fn poll_pop(&self, cx: &mut Context<'_>) -> Poll<Option<Frame<P>>> {
        let mut state = self.lock();
        if let Some(frame) = state.queue.pop_front() {
            match &frame {
                Frame::Item { .. } => {
                    state.items -= 1;
                    state.counts.delivered += 1;
                }
                Frame::End(_) => state.end_taken = true,
                Frame::Gap(_) => {}
            }
            drop(state);
            if self.is_stop_producer() {
                self.writable.notify_waiters();
            }
            return Poll::Ready(Some(frame));
        }
        if state.ended {
            return Poll::Ready(None);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    pub(crate) async fn pop(&self) -> Option<Frame<P>> {
        std::future::poll_fn(|cx| self.poll_pop(cx)).await
    }

    /// Count one item frame the sink acknowledged.
    pub(crate) fn acknowledged_item(&self) {
        self.lock().counts.acknowledged += 1;
    }

    /// The consumer took the end marker (stream) or the sink acknowledged it
    /// (durable): settle with the observer's own failure, if any.
    pub(crate) fn settle_at_end(&self) {
        let failure = self.lock().failure.clone();
        self.settle(failure.map_or(Ok(()), Err));
    }

    /// The consumer stopped reading or its sink failed: record the failure,
    /// release the queue and settle.
    pub(crate) fn fail(&self, failure: ObserverFailure) {
        {
            let mut state = self.lock();
            if state.failure.is_none() {
                state.failure = Some(failure.clone());
            }
            state.release();
        }
        self.writable.notify_waiters();
        self.settle(Err(failure));
    }

    /// The consumer went away. Abandonment only if it had not taken the end
    /// marker; afterwards, settlement already stands.
    pub(crate) fn abandon(&self) {
        let end_taken = {
            let mut state = self.lock();
            let end_taken = state.end_taken;
            state.release();
            end_taken
        };
        self.writable.notify_waiters();
        if !end_taken {
            self.settle(Err(ObserverFailure::Abandoned));
        }
    }

    pub(crate) fn settle(&self, result: Result<(), ObserverFailure>) {
        self.settled.send_if_modified(|settled| {
            if settled.is_none() {
                *settled = Some(result);
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn settlement(&self) -> Settlement {
        self.settled.borrow().clone()
    }

    pub(crate) async fn wait_settled(&self) {
        let mut settled = self.settled.subscribe();
        // The sender lives in `self`, so this cannot see a closed channel.
        let _ = settled.wait_for(Option::is_some).await;
    }

    pub(crate) fn counts(&self) -> ObserverCounts {
        self.lock().counts
    }

    pub(crate) fn report(&self) -> ObserverReport {
        let (counts, failure) = {
            let state = self.lock();
            (state.counts, state.failure.clone())
        };
        ObserverReport {
            name: self.name.clone(),
            policy: self.policy,
            counts,
            failure,
            settled: self.settlement(),
        }
    }
}

impl<P> State<P> {
    fn push_item(&mut self, seq: u64, item: P) {
        self.flush_tail_gap();
        self.queue.push_back(Frame::Item { seq, item });
        self.items += 1;
        self.counts.peak_depth = self.counts.peak_depth.max(self.items);
    }

    fn flush_tail_gap(&mut self) {
        if let Some(gap) = self.tail_gap.take() {
            self.queue.push_back(Frame::Gap(gap));
            self.counts.gaps += 1;
        }
    }

    fn extend_tail_gap(&mut self, reason: GapReason, seq: u64) {
        match &mut self.tail_gap {
            Some(gap) if gap.reason == reason && gap.last_seq + 1 == seq => gap.last_seq = seq,
            _ => {
                self.flush_tail_gap();
                self.tail_gap = Some(Gap {
                    reason,
                    first_seq: seq,
                    last_seq: seq,
                });
            }
        }
    }

    /// Drop the oldest queued item, marking its place with a gap (merged
    /// into an adjacent drop gap when contiguous).
    fn drop_oldest_item(&mut self) {
        let Some(index) = self
            .queue
            .iter()
            .position(|frame| matches!(frame, Frame::Item { .. }))
        else {
            return;
        };
        let Some(Frame::Item { seq, .. }) = self.queue.remove(index) else {
            unreachable!("position found an item");
        };
        self.items -= 1;
        self.counts.lost += 1;
        if index > 0 {
            if let Some(Frame::Gap(gap)) = self.queue.get_mut(index - 1) {
                if gap.reason == GapReason::DroppedOldest && gap.last_seq + 1 == seq {
                    gap.last_seq = seq;
                    return;
                }
            }
        }
        self.queue.insert(
            index,
            Frame::Gap(Gap {
                reason: GapReason::DroppedOldest,
                first_seq: seq,
                last_seq: seq,
            }),
        );
        self.counts.gaps += 1;
    }

    /// Nobody will read the queue again: free it and refuse more.
    fn release(&mut self) {
        self.gone = true;
        self.queue.clear();
        self.items = 0;
        self.tail_gap = None;
        self.waker = None;
    }
}
