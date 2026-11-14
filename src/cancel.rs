//! Cancellation scopes: a [`CancellationToken`] plus an optional deadline,
//! with child scopes that cascade.
//!
//! A scope answers two questions honestly:
//!
//! - **Was cancellation requested, and why?** [`CancelScope::cause`] says
//!   [`CancelCause::Requested`] (someone called [`CancelScope::cancel`] on
//!   this scope or an ancestor) or [`CancelCause::TimedOut`] (this scope's
//!   effective deadline passed). The first cause in time wins, and it is
//!   decided by timestamps, not by which task happened to poll first.
//! - **Nothing else.** A scope never says that work *stopped*. Whether a
//!   producer stopped is something only the producer can confirm; see
//!   [`crate::run::Stop`].
//!
//! A child's effective deadline is the earlier of its own deadline and its
//! parent's. Cancelling a parent cancels every child; cancelling a child
//! never touches its parent.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Why a scope is cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancelCause {
    /// [`CancelScope::cancel`] was called on this scope or an ancestor.
    Requested,
    /// The scope's effective deadline (its own or an ancestor's) passed.
    TimedOut,
}

/// A cancellation scope. Cheap to clone; clones share state.
#[derive(Clone)]
pub struct CancelScope {
    inner: Arc<Inner>,
}

struct Inner {
    token: CancellationToken,
    /// Effective deadline: the earlier of this scope's own and its parent's.
    deadline: Option<Instant>,
    /// The first cause recorded on this scope, with when it happened.
    recorded: Mutex<Option<(CancelCause, Instant)>>,
    parent: Option<CancelScope>,
}

impl CancelScope {
    /// A root scope with no deadline.
    pub fn new() -> Self {
        Self::build(CancellationToken::new(), None, None)
    }

    /// A root scope that times out at `deadline`.
    pub fn with_deadline(deadline: Instant) -> Self {
        Self::build(CancellationToken::new(), Some(deadline), None)
    }

    /// A root scope that times out `timeout` from now.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::with_deadline(Instant::now() + timeout)
    }

    /// A child scope: cancelled when this scope is, with this scope's
    /// deadline.
    #[must_use]
    pub fn child(&self) -> Self {
        self.child_inner(None)
    }

    /// A child scope with its own deadline. The effective deadline is the
    /// earlier of `deadline` and this scope's.
    #[must_use]
    pub fn child_with_deadline(&self, deadline: Instant) -> Self {
        self.child_inner(Some(deadline))
    }

    /// A child scope that times out `timeout` from now (or at this scope's
    /// deadline, if that is earlier).
    #[must_use]
    pub fn child_with_timeout(&self, timeout: Duration) -> Self {
        self.child_inner(Some(Instant::now() + timeout))
    }

    fn child_inner(&self, own: Option<Instant>) -> Self {
        let deadline = match (own, self.inner.deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        Self::build(self.inner.token.child_token(), deadline, Some(self.clone()))
    }

    fn build(token: CancellationToken, deadline: Option<Instant>, parent: Option<Self>) -> Self {
        Self {
            inner: Arc::new(Inner {
                token,
                deadline,
                recorded: Mutex::new(None),
                parent,
            }),
        }
    }

    /// The effective deadline, if any.
    pub fn deadline(&self) -> Option<Instant> {
        self.inner.deadline
    }

    /// Request cancellation of this scope and every child. Idempotent: the
    /// first cause stands.
    pub fn cancel(&self) {
        self.record(CancelCause::Requested, Instant::now());
        self.inner.token.cancel();
    }

    fn record(&self, cause: CancelCause, at: Instant) {
        let mut recorded = self
            .inner
            .recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if recorded.is_none() {
            *recorded = Some((cause, at));
        }
    }

    fn recorded(&self) -> Option<(CancelCause, Instant)> {
        *self
            .inner
            .recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether cancellation has been requested or the deadline has passed.
    /// True past the deadline even if nothing has awaited
    /// [`Self::cancelled`] yet.
    pub fn is_cancelled(&self) -> bool {
        self.cause().is_some()
    }

    /// Why this scope is cancelled, or `None` if it is not. When several
    /// causes apply (a request on an ancestor, a deadline), the earliest in
    /// time wins; a tie goes to the deadline.
    pub fn cause(&self) -> Option<CancelCause> {
        let now = Instant::now();
        let mut best: Option<(CancelCause, Instant)> = None;
        if let Some(deadline) = self.inner.deadline {
            if deadline <= now {
                best = Some((CancelCause::TimedOut, deadline));
            }
        }
        let mut scope = Some(self);
        while let Some(current) = scope {
            if let Some((cause, at)) = current.recorded() {
                if best.is_none_or(|(_, best_at)| at < best_at) {
                    best = Some((cause, at));
                }
            }
            scope = current.inner.parent.as_ref();
        }
        if best.is_none() && self.inner.token.is_cancelled() {
            // Only reachable when the raw token handed out by `token()` was
            // cancelled directly: that is a request.
            best = Some((CancelCause::Requested, now));
        }
        best.map(|(cause, _)| cause)
    }

    /// Resolves when the scope is cancelled or its deadline passes, with
    /// the cause. When the deadline fires here, the scope's token is
    /// cancelled too, so children and token holders see it.
    pub async fn cancelled(&self) -> CancelCause {
        if let Some(cause) = self.cause() {
            if cause == CancelCause::TimedOut {
                self.inner.token.cancel();
            }
            return cause;
        }
        match self.inner.deadline {
            Some(deadline) => {
                tokio::select! {
                    () = self.inner.token.cancelled() => {}
                    () = tokio::time::sleep_until(deadline) => {
                        self.record(CancelCause::TimedOut, deadline);
                        self.inner.token.cancel();
                    }
                }
            }
            None => self.inner.token.cancelled().await,
        }
        self.cause().unwrap_or(CancelCause::Requested)
    }

    /// The underlying token, for code that only speaks
    /// [`CancellationToken`]. Cancelling it directly counts as a request.
    ///
    /// The deadline cancels this token only once something awaits
    /// [`Self::cancelled`] past the deadline (a [`crate::run::Run`] always
    /// does). Prefer [`Self::cancelled`] where you can.
    pub fn token(&self) -> CancellationToken {
        self.inner.token.clone()
    }
}

impl Default for CancelScope {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CancelScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancelScope")
            .field("deadline", &self.inner.deadline)
            .field("cause", &self.cause())
            .field("has_parent", &self.inner.parent.is_some())
            .finish()
    }
}
