//! Ordering: per-stream order is always preserved; order across branches
//! only where it is declared.
//!
//! Every vines stream keeps its own order: observers see items in emission
//! order, batches keep input order, [`crate::ops::par_map`] with
//! [`crate::ops::OutputOrder::Ordered`] keeps input order. When several
//! branches are joined, there is no cross-branch order unless the join
//! declares one:
//!
//! - [`merge_arrival`] declares none: items come out as branches produce
//!   them.
//! - [`merge_by_sequence`] declares source order: each branch carries the
//!   source's sequence numbers ([`Sequenced`]), and the merge emits in
//!   ascending sequence, waiting for a branch that has not yet shown its
//!   next item.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{self, Stream, StreamExt};

/// An item with the sequence number its source assigned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Sequenced<T> {
    /// The source sequence number.
    pub seq: u64,
    /// The item.
    pub item: T,
}

/// Number a stream's items from 0 in order.
pub fn sequence<S: Stream>(input: S) -> impl Stream<Item = Sequenced<S::Item>> {
    input.enumerate().map(|(seq, item)| Sequenced {
        seq: seq as u64,
        item,
    })
}

/// Join branches with no cross-branch order: items come out as they arrive.
/// Each branch's own order is kept.
pub fn merge_arrival<S>(branches: Vec<S>) -> impl Stream<Item = S::Item>
where
    S: Stream,
{
    stream::select_all(branches.into_iter().map(Box::pin))
}

/// A branch yielded a sequence number lower than one already emitted, so
/// source order cannot be honoured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("branch {branch} yielded seq {seq} after seq {after} was emitted")]
pub struct OrderViolation {
    /// Index of the offending branch.
    pub branch: usize,
    /// The out-of-order sequence number.
    pub seq: u64,
    /// The last sequence number emitted.
    pub after: u64,
}

/// Stream returned by [`merge_by_sequence`].
#[must_use = "streams do nothing unless polled"]
pub struct MergeBySequence<S: Stream> {
    branches: Vec<Pin<Box<S>>>,
    heads: Vec<Option<S::Item>>,
    ended: Vec<bool>,
    last: Option<u64>,
    failed: bool,
}

/// Join branches in ascending source sequence. Each branch must yield
/// ascending sequence numbers (vines streams do). Equal numbers from
/// different branches (one item fanned into two branches) come out in
/// branch order. A branch that goes backwards yields one
/// [`OrderViolation`] and ends the merge.
pub fn merge_by_sequence<S, T>(branches: Vec<S>) -> MergeBySequence<S>
where
    S: Stream<Item = Sequenced<T>>,
{
    let count = branches.len();
    MergeBySequence {
        branches: branches.into_iter().map(Box::pin).collect(),
        heads: (0..count).map(|_| None).collect(),
        ended: vec![false; count],
        last: None,
        failed: false,
    }
}

impl<S, T> Stream for MergeBySequence<S>
where
    S: Stream<Item = Sequenced<T>>,
{
    type Item = Result<Sequenced<T>, OrderViolation>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Only boxed streams and plain data: Unpin.
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(None);
        }
        let mut waiting = false;
        for (index, branch) in this.branches.iter_mut().enumerate() {
            if this.heads[index].is_some() || this.ended[index] {
                continue;
            }
            match branch.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => this.heads[index] = Some(item),
                Poll::Ready(None) => this.ended[index] = true,
                Poll::Pending => waiting = true,
            }
        }
        // A branch with no head yet might hold a lower number: wait for it.
        if waiting {
            return Poll::Pending;
        }
        let next = this
            .heads
            .iter()
            .enumerate()
            .filter_map(|(index, head)| head.as_ref().map(|head| (head.seq, index)))
            .min();
        let Some((seq, index)) = next else {
            return Poll::Ready(None);
        };
        let item = this.heads[index].take().expect("head chosen above");
        if let Some(after) = this.last {
            if seq < after {
                this.failed = true;
                return Poll::Ready(Some(Err(OrderViolation {
                    branch: index,
                    seq,
                    after,
                })));
            }
        }
        this.last = Some(seq);
        Poll::Ready(Some(Ok(item)))
    }
}

impl<S: Stream> Unpin for MergeBySequence<S> {}
