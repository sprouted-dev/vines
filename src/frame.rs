//! What an observer receives: items, gap markers, and one end marker.
//!
//! Every item carries the source sequence number it was emitted with, so
//! order is checkable and a [`Gap`] can name exactly which items are
//! missing. An observer's stream always ends with one [`Frame::End`] whose
//! [`Coverage`] says whether the observer saw everything. A consumer that
//! summarises a run reads coverage from there; it never infers completeness
//! from the absence of gap markers.

use crate::observer::ObserverFailure;

/// One frame delivered to an observer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame<P> {
    /// A progress item, with its source sequence number (0-based, dense,
    /// assigned by the run in emission order).
    Item {
        /// Source sequence number.
        seq: u64,
        /// The item.
        item: P,
    },
    /// Items this observer will never see, with why.
    Gap(Gap),
    /// The run has ended; nothing follows.
    End(End),
}

impl<P> Frame<P> {
    /// The item, if this is an item frame.
    pub fn item(&self) -> Option<&P> {
        match self {
            Self::Item { item, .. } => Some(item),
            _ => None,
        }
    }

    /// The item's sequence number, if this is an item frame.
    pub fn seq(&self) -> Option<u64> {
        match self {
            Self::Item { seq, .. } => Some(*seq),
            _ => None,
        }
    }
}

/// A contiguous range of source items this observer did not receive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Gap {
    /// Why they are missing.
    pub reason: GapReason,
    /// First missing sequence number.
    pub first_seq: u64,
    /// Last missing sequence number (inclusive).
    pub last_seq: u64,
}

impl Gap {
    /// How many items are missing.
    pub fn count(&self) -> u64 {
        self.last_seq - self.first_seq + 1
    }
}

/// Why a [`Gap`] exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GapReason {
    /// The queue was full and `overflow = drop_oldest` dropped these.
    DroppedOldest,
    /// The queue was full and `overflow = drop_newest` dropped these.
    DroppedNewest,
    /// The spill quota was exhausted and `loss = allowed` dropped these.
    SpillExhausted,
    /// The observer attached after these were emitted.
    JoinedLate,
    /// The observer had already failed (with `loss = none`) when these
    /// were emitted.
    ObserverFailed,
}

/// The end marker: how the run ended and what this observer saw of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct End {
    /// How the run's producer ended.
    pub ending: Ending,
    /// What this observer received.
    pub coverage: Coverage,
}

/// How a run ended, without its result value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Ending {
    /// The producer completed.
    Completed,
    /// The producer reported failure.
    Failed,
    /// Cancellation was requested.
    Cancelled,
    /// The deadline passed.
    TimedOut,
    /// The producer broke its contract (panicked, or stopped unasked).
    Crashed,
    /// The run was declared and dropped without being spawned.
    NotStarted,
}

/// What one observer received of a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Coverage {
    /// Items the source emitted during the run.
    pub emitted: u64,
    /// Items this observer did not receive (dropped, missed by joining
    /// late, or emitted after it failed).
    pub lost: u64,
    /// Gap markers delivered.
    pub gaps: u64,
    /// The observer's failure, if it failed.
    pub failure: Option<ObserverFailure>,
}

impl Coverage {
    /// Whether this observer received every emitted item and did not fail.
    pub fn is_complete(&self) -> bool {
        self.lost == 0 && self.failure.is_none()
    }

    /// Items received: `emitted - lost`.
    pub fn received(&self) -> u64 {
        self.emitted.saturating_sub(self.lost)
    }
}

/// An observer's whole stream, collected: see
/// [`crate::observer::ObserverStream::into_record`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<P> {
    /// Items in delivery order, with their sequence numbers.
    pub items: Vec<(u64, P)>,
    /// Gap markers in delivery order.
    pub gaps: Vec<Gap>,
    /// The end marker; `None` only if the stream closed without one (it
    /// was never started).
    pub end: Option<End>,
}

impl<P> Record<P> {
    /// Whether this record is a complete account of what the source
    /// emitted: it ended, lost nothing, carries no gap and its observer did
    /// not fail. A summary built from an incomplete record must say so.
    pub fn is_complete(&self) -> bool {
        self.gaps.is_empty()
            && self
                .end
                .as_ref()
                .is_some_and(|end| end.coverage.is_complete())
    }
}
