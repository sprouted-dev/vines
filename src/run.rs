//! The operator shape: one owned producer, a live progress stream fanned out
//! to declared observers, and exactly one terminal result.
//!
//! ```text
//! RunBuilder ──observe()──────────▶ ObserverStream  (ephemeral)
//!     │     ──observe_durable()──▶ DurableObserver (sink-acknowledged)
//!     │
//!     └─spawn(scope, producer)───▶ Run ──finish()──▶ RunReport (exactly once)
//!                    │
//!            producer(RunContext) ──emit()──▶ every observer's own queue
//! ```
//!
//! Guarantees:
//!
//! - **The result never waits on progress.** The result travels on its own
//!   channel. On a source without backpressure, [`RunContext::emit`] never
//!   waits: each observer's overflow policy decides. Only an observer that
//!   declared `stop_producer`, on a source that declared it can wait, can
//!   slow the producer.
//! - **One source, one owner.** The producer runs once, when
//!   [`RunBuilder::spawn`] is called. Attaching observers (before or after
//!   the start) never runs it again.
//! - **Answer gating is explicit.** `answer = required` observers must
//!   settle before the report is sent; their failures arrive as
//!   [`GateFailure`]s and [`RunReport::answer`] refuses to hand over the
//!   outcome as if nothing happened.
//! - **Cancellation is not a stop.** When the scope is cancelled the
//!   producer gets `stop_grace` to return [`Exit::Stopped`]. If it does,
//!   the outcome is [`Stop::Confirmed`]; if it does not, the outcome is
//!   [`Stop::Unconfirmed`] and the still-running producer is handed back
//!   as a [`Straggler`]. A producer that completes despite the request
//!   completes: a confirmed success stays a success.
//! - **Dropping a [`Run`] cancels it.** The owner going away is a request
//!   to stop; it is not a confirmation.

use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures::future::{BoxFuture, Either};
use futures::FutureExt;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

use crate::cancel::{CancelCause, CancelScope};
use crate::error::PolicyError;
use crate::frame::Ending;
use crate::observer::{self, DurableObserver, ObserverFailure, ObserverReport, ObserverStream};
use crate::outlet::Outlet;
use crate::policy::{Answer, ObserverPolicy, SourceDecl};
use crate::sink::DurableSink;

/// What a producer returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exit<R, E> {
    /// The work completed with a result.
    Completed(R),
    /// The work failed.
    Failed(E),
    /// The producer confirms it stopped because cancellation was requested.
    /// Returning this without a request is a contract break
    /// ([`Crash::StoppedUnrequested`]).
    Stopped,
}

/// Whether the producer confirmed it stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stop {
    /// The producer returned [`Exit::Stopped`] within the grace.
    Confirmed,
    /// Cancellation was requested; the producer has not confirmed it
    /// stopped. Vines does not claim it did. See [`RunReport::straggler`].
    Unconfirmed,
}

/// A producer that broke its contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Crash {
    /// The producer panicked, with the panic message.
    Panicked(String),
    /// The producer's task was aborted (runtime shutdown).
    Aborted,
    /// The producer returned [`Exit::Stopped`] without being asked.
    StoppedUnrequested,
    /// The run's driver went away before deciding (runtime shutdown).
    DriverLost,
}

/// A run's terminal outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<R, E> {
    /// The producer completed.
    Completed(R),
    /// The producer failed.
    Failed(E),
    /// Cancellation was requested (on the run or an ancestor scope).
    Cancelled(Stop),
    /// The scope's deadline passed.
    TimedOut(Stop),
    /// The producer broke its contract.
    Crashed(Crash),
}

impl<R, E> Outcome<R, E> {
    /// The outcome without its value.
    pub fn ending(&self) -> Ending {
        match self {
            Self::Completed(_) => Ending::Completed,
            Self::Failed(_) => Ending::Failed,
            Self::Cancelled(_) => Ending::Cancelled,
            Self::TimedOut(_) => Ending::TimedOut,
            Self::Crashed(_) => Ending::Crashed,
        }
    }
}

/// A required observer that did not settle successfully.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateFailure {
    /// The observer's name.
    pub observer: String,
    /// Why it failed.
    pub failure: ObserverFailure,
}

/// An outcome withheld because a required observer failed. The outcome is
/// kept so the caller can see what the producer did; it is not the answer.
#[derive(Debug)]
pub struct Gated<R, E> {
    /// What the producer did.
    pub outcome: Outcome<R, E>,
    /// The required observers that failed.
    pub failures: Vec<GateFailure>,
}

/// The run's one terminal report.
pub struct RunReport<R, E> {
    /// The producer's outcome.
    pub outcome: Outcome<R, E>,
    /// Whether, and why, cancellation was requested, even if the producer
    /// completed anyway.
    pub cancel_requested: Option<CancelCause>,
    /// Required observers that failed or did not settle. Empty when the
    /// answer is not gated.
    pub gate_failures: Vec<GateFailure>,
    /// Every observer, as of the report.
    pub observers: Vec<ObserverReport>,
    /// Items the producer emitted before the run ended.
    pub emitted: u64,
    /// The producer, still running, when it did not confirm a stop.
    pub straggler: Option<Straggler<R, E>>,
}

impl<R, E> RunReport<R, E> {
    /// The outcome, if no required observer failed. Otherwise
    /// [`Gated`]: the outcome is not an answer.
    pub fn answer(self) -> Result<Outcome<R, E>, Gated<R, E>> {
        if self.gate_failures.is_empty() {
            Ok(self.outcome)
        } else {
            Err(Gated {
                outcome: self.outcome,
                failures: self.gate_failures,
            })
        }
    }

    /// Whether a required observer gated the answer.
    pub fn is_gated(&self) -> bool {
        !self.gate_failures.is_empty()
    }

    /// The report for one observer, by name.
    pub fn observer(&self, name: &str) -> Option<&ObserverReport> {
        self.observers.iter().find(|report| report.name == name)
    }
}

impl<R: fmt::Debug, E: fmt::Debug> fmt::Debug for RunReport<R, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunReport")
            .field("outcome", &self.outcome)
            .field("cancel_requested", &self.cancel_requested)
            .field("gate_failures", &self.gate_failures)
            .field("observers", &self.observers)
            .field("emitted", &self.emitted)
            .field("straggler", &self.straggler.is_some())
            .finish()
    }
}

/// A producer that had not confirmed a stop when the grace ran out.
pub struct Straggler<R, E> {
    handle: JoinHandle<Exit<R, E>>,
}

impl<R, E> Straggler<R, E> {
    /// Wait for the producer's eventual exit. It may well be
    /// [`Exit::Completed`]: a cancel request does not prove an effect did
    /// not happen.
    pub async fn exit(self) -> Result<Exit<R, E>, Crash> {
        self.handle.await.map_err(crash)
    }

    /// Whether the producer has exited.
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// Drop the producer's future. This stops in-process work only; it does
    /// not prove an external process stopped or an effect did not happen.
    pub fn abort(self) {
        self.handle.abort();
    }
}

/// Why [`RunContext::emit`] did not deliver an item. The item comes back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitError<P> {
    /// Cancellation was requested while waiting for a `stop_producer`
    /// observer to make room.
    Cancelled(P),
    /// The run has already ended (a straggler emitting after the report).
    Ended(P),
}

impl<P> EmitError<P> {
    /// The item that was not delivered.
    pub fn into_inner(self) -> P {
        match self {
            Self::Cancelled(item) | Self::Ended(item) => item,
        }
    }
}

impl<P> fmt::Display for EmitError<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled(_) => f.write_str("cancelled while waiting for observer capacity"),
            Self::Ended(_) => f.write_str("the run has already ended"),
        }
    }
}

impl<P: fmt::Debug> std::error::Error for EmitError<P> {}

// ── the hub: one source's observers ─────────────────────────────────────

struct Hub<P> {
    state: Mutex<HubState<P>>,
}

struct HubState<P> {
    outlets: Vec<Arc<Outlet<P>>>,
    next_seq: u64,
    ended: bool,
}

impl<P> Hub<P> {
    fn new() -> Self {
        Self {
            state: Mutex::new(HubState {
                outlets: Vec::new(),
                next_seq: 0,
                ended: false,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, HubState<P>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn attach(&self, name: String, policy: ObserverPolicy) -> Result<Arc<Outlet<P>>, PolicyError> {
        let mut state = self.lock();
        if state.ended {
            return Err(PolicyError::RunEnded { observer: name });
        }
        if state.outlets.iter().any(|outlet| outlet.name() == name) {
            return Err(PolicyError::DuplicateObserver { observer: name });
        }
        let outlet = Outlet::new(name, policy, state.next_seq);
        state.outlets.push(Arc::clone(&outlet));
        Ok(outlet)
    }

    /// Close every outlet with the run's ending. Returns items emitted.
    fn close(&self, ending: Ending) -> u64 {
        let mut state = self.lock();
        if state.ended {
            return state.next_seq;
        }
        state.ended = true;
        for outlet in &state.outlets {
            outlet.close(ending, state.next_seq);
        }
        state.next_seq
    }

    fn reports(&self) -> Vec<ObserverReport> {
        self.lock()
            .outlets
            .iter()
            .map(|outlet| outlet.report())
            .collect()
    }

    fn emitted(&self) -> u64 {
        self.lock().next_seq
    }
}

impl<P: Clone> Hub<P> {
    async fn emit(&self, item: P, scope: &CancelScope) -> Result<(), EmitError<P>> {
        // Wait for every stop_producer observer to have room. Only reachable
        // on a source that declared backpressure (checked at attach).
        loop {
            let blocking = {
                let state = self.lock();
                if state.ended {
                    return Err(EmitError::Ended(item));
                }
                state
                    .outlets
                    .iter()
                    .find(|outlet| outlet.is_stop_producer() && !outlet.has_space())
                    .cloned()
            };
            let Some(outlet) = blocking else { break };
            let notified = outlet.writable().notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if outlet.has_space() {
                continue;
            }
            tokio::select! {
                biased;
                () = &mut notified => {}
                _ = scope.cancelled() => return Err(EmitError::Cancelled(item)),
            }
        }
        let mut state = self.lock();
        if state.ended {
            return Err(EmitError::Ended(item));
        }
        let seq = state.next_seq;
        state.next_seq += 1;
        if let Some((last, rest)) = state.outlets.split_last() {
            for outlet in rest {
                outlet.offer(seq, item.clone());
            }
            last.offer(seq, item);
        }
        Ok(())
    }
}

impl<P> Drop for Hub<P> {
    fn drop(&mut self) {
        // Only reached for a builder dropped without spawning: a spawned
        // run's driver holds the hub until it closes it.
        self.close(Ending::NotStarted);
    }
}

// ── building ────────────────────────────────────────────────────────────

/// Declares a run's source and observers, then starts it.
pub struct RunBuilder<P> {
    source: SourceDecl,
    stop_grace: Duration,
    hub: Arc<Hub<P>>,
    required: Vec<Arc<Outlet<P>>>,
    drains: Vec<BoxFuture<'static, ()>>,
}

impl<P: Clone + Send + 'static> RunBuilder<P> {
    /// A run over a source that declares `source`. `stop_grace` bounds how
    /// long the run waits, after cancellation, for the producer to confirm
    /// a stop, and for required observers to settle. It must be non-zero.
    pub fn new(source: SourceDecl, stop_grace: Duration) -> Result<Self, PolicyError> {
        if stop_grace.is_zero() {
            return Err(PolicyError::ZeroDuration {
                scope: "run".to_string(),
                field: "stop_grace",
            });
        }
        Ok(Self {
            source,
            stop_grace,
            hub: Arc::new(Hub::new()),
            required: Vec::new(),
            drains: Vec::new(),
        })
    }

    /// Attach an ephemeral observer.
    pub fn observe(
        &mut self,
        name: impl Into<String>,
        policy: ObserverPolicy,
    ) -> Result<ObserverStream<P>, PolicyError> {
        let name = name.into();
        policy.check_attach(&name, self.source, false)?;
        let outlet = self.hub.attach(name, policy)?;
        if policy.answer == Answer::Required {
            self.required.push(Arc::clone(&outlet));
        }
        Ok(ObserverStream::new(outlet))
    }

    /// Attach a durable observer draining into `sink`. The drain starts with
    /// the run.
    pub fn observe_durable<S: DurableSink<P>>(
        &mut self,
        name: impl Into<String>,
        policy: ObserverPolicy,
        sink: S,
    ) -> Result<DurableObserver<P>, PolicyError> {
        let name = name.into();
        policy.check_attach(&name, self.source, true)?;
        let outlet = self.hub.attach(name, policy)?;
        if policy.answer == Answer::Required {
            self.required.push(Arc::clone(&outlet));
        }
        let (handle, drain) = observer::durable(outlet, sink);
        self.drains.push(drain.boxed());
        Ok(handle)
    }

    /// Start the producer, once, in a child of `scope`. Requires a Tokio
    /// runtime.
    pub fn spawn<F, Fut, R, E>(self, scope: &CancelScope, producer: F) -> Run<P, R, E>
    where
        F: FnOnce(RunContext<P>) -> Fut,
        Fut: Future<Output = Exit<R, E>> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let scope = scope.child();
        for drain in self.drains {
            tokio::spawn(drain);
        }
        let context = RunContext {
            hub: Arc::clone(&self.hub),
            scope: scope.clone(),
        };
        let producer = tokio::spawn(producer(context));
        let (report_tx, report_rx) = oneshot::channel();
        tokio::spawn(drive(
            producer,
            Arc::clone(&self.hub),
            scope.clone(),
            self.stop_grace,
            self.required,
            report_tx,
        ));
        Run {
            hub: self.hub,
            source: self.source,
            scope,
            report: Some(report_rx),
        }
    }
}

// ── running ─────────────────────────────────────────────────────────────

/// What the producer holds: the emitter and its cancellation scope.
pub struct RunContext<P> {
    hub: Arc<Hub<P>>,
    scope: CancelScope,
}

impl<P: Clone + Send + 'static> RunContext<P> {
    /// Emit one progress item to every observer, in order.
    ///
    /// On a source without backpressure this never waits: the returned
    /// future is ready at once. On a backpressure source it waits while a
    /// `stop_producer` observer is full, and returns
    /// [`EmitError::Cancelled`] if cancellation arrives first.
    pub async fn emit(&mut self, item: P) -> Result<(), EmitError<P>> {
        self.hub.emit(item, &self.scope).await
    }

    /// Items emitted so far.
    pub fn emitted(&self) -> u64 {
        self.hub.emitted()
    }

    /// The run's cancellation scope. Watch it and return [`Exit::Stopped`]
    /// to confirm a stop.
    pub fn scope(&self) -> &CancelScope {
        &self.scope
    }

    /// Whether cancellation was requested or the deadline passed.
    pub fn is_cancelled(&self) -> bool {
        self.scope.is_cancelled()
    }

    /// Resolves when cancellation is requested or the deadline passes.
    pub async fn cancelled(&self) -> CancelCause {
        self.scope.cancelled().await
    }
}

/// A started run: its owner's handle.
pub struct Run<P, R, E> {
    hub: Arc<Hub<P>>,
    source: SourceDecl,
    scope: CancelScope,
    report: Option<oneshot::Receiver<RunReport<R, E>>>,
}

impl<P: Clone + Send + 'static, R, E> Run<P, R, E> {
    /// Attach an ephemeral observer to the running run. It starts with a
    /// [`crate::frame::GapReason::JoinedLate`] gap covering what it missed.
    /// `answer = required` is refused: a required observer must be declared
    /// before the start.
    pub fn observe(
        &self,
        name: impl Into<String>,
        policy: ObserverPolicy,
    ) -> Result<ObserverStream<P>, PolicyError> {
        let name = name.into();
        policy.check_attach(&name, self.source, false)?;
        if policy.answer == Answer::Required {
            return Err(PolicyError::LateRequiredObserver { observer: name });
        }
        Ok(ObserverStream::new(self.hub.attach(name, policy)?))
    }

    /// Attach a durable observer to the running run; its drain starts now.
    /// `answer = required` is refused, as for [`Self::observe`].
    pub fn observe_durable<S: DurableSink<P>>(
        &self,
        name: impl Into<String>,
        policy: ObserverPolicy,
        sink: S,
    ) -> Result<DurableObserver<P>, PolicyError> {
        let name = name.into();
        policy.check_attach(&name, self.source, true)?;
        if policy.answer == Answer::Required {
            return Err(PolicyError::LateRequiredObserver { observer: name });
        }
        let outlet = self.hub.attach(name, policy)?;
        let (handle, drain) = observer::durable(outlet, sink);
        tokio::spawn(drain);
        Ok(handle)
    }
}

impl<P, R, E> Run<P, R, E> {
    /// Request cancellation. The report says whether the producer confirmed.
    pub fn cancel(&self) {
        self.scope.cancel();
    }

    /// The run's own scope (a child of the one it was spawned in).
    pub fn scope(&self) -> &CancelScope {
        &self.scope
    }

    /// Wait for the one terminal report.
    pub async fn finish(mut self) -> RunReport<R, E> {
        let report = match self.report.as_mut() {
            Some(report) => report.await.ok(),
            None => None,
        };
        // Taken only now, so a `finish` dropped mid-wait still cancels.
        self.report = None;
        report.unwrap_or_else(|| RunReport {
            outcome: Outcome::Crashed(Crash::DriverLost),
            cancel_requested: self.scope.cause(),
            gate_failures: Vec::new(),
            observers: self.hub.reports(),
            emitted: self.hub.emitted(),
            straggler: None,
        })
    }
}

impl<P, R, E> Drop for Run<P, R, E> {
    fn drop(&mut self) {
        // An owner that goes away without taking the report is asking the
        // run to stop.
        if self.report.is_some() {
            self.scope.cancel();
        }
    }
}

fn crash(error: JoinError) -> Crash {
    if error.is_panic() {
        let payload = error.into_panic();
        let message = payload
            .downcast_ref::<&str>()
            .map(|text| (*text).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "non-string panic payload".to_string());
        Crash::Panicked(message)
    } else {
        Crash::Aborted
    }
}

fn outcome_of<R, E>(
    joined: Result<Exit<R, E>, JoinError>,
    cause: Option<CancelCause>,
) -> Outcome<R, E> {
    match joined {
        Ok(Exit::Completed(result)) => Outcome::Completed(result),
        Ok(Exit::Failed(error)) => Outcome::Failed(error),
        Ok(Exit::Stopped) => match cause {
            Some(CancelCause::Requested) => Outcome::Cancelled(Stop::Confirmed),
            Some(CancelCause::TimedOut) => Outcome::TimedOut(Stop::Confirmed),
            None => Outcome::Crashed(Crash::StoppedUnrequested),
        },
        Err(error) => Outcome::Crashed(crash(error)),
    }
}

async fn drive<P, R, E>(
    mut producer: JoinHandle<Exit<R, E>>,
    hub: Arc<Hub<P>>,
    scope: CancelScope,
    grace: Duration,
    required: Vec<Arc<Outlet<P>>>,
    report: oneshot::Sender<RunReport<R, E>>,
) {
    let first = tokio::select! {
        biased;
        joined = &mut producer => Either::Left(joined),
        cause = scope.cancelled() => Either::Right(cause),
    };
    let (outcome, straggler) = match first {
        Either::Left(joined) => (outcome_of(joined, scope.cause()), None),
        Either::Right(cause) => match tokio::time::timeout(grace, &mut producer).await {
            Ok(joined) => (outcome_of(joined, Some(cause)), None),
            Err(_) => {
                let outcome = match cause {
                    CancelCause::Requested => Outcome::Cancelled(Stop::Unconfirmed),
                    CancelCause::TimedOut => Outcome::TimedOut(Stop::Unconfirmed),
                };
                (outcome, Some(Straggler { handle: producer }))
            }
        },
    };
    let emitted = hub.close(outcome.ending());

    // Gate on required observers; bounded by the grace once cancelled.
    let settled = async {
        for outlet in &required {
            outlet.wait_settled().await;
        }
    };
    let bound = async {
        scope.cancelled().await;
        tokio::time::sleep(grace).await;
    };
    tokio::select! {
        biased;
        () = settled => {}
        () = bound => {}
    }
    let gate_failures = required
        .iter()
        .filter_map(|outlet| {
            if outlet.settlement().is_none() {
                outlet.fail(ObserverFailure::Unsettled);
            }
            match outlet.settlement() {
                Some(Ok(())) => None,
                Some(Err(failure)) => Some(GateFailure {
                    observer: outlet.name().to_string(),
                    failure,
                }),
                None => unreachable!("settled just above"),
            }
        })
        .collect();

    let _ = report.send(RunReport {
        outcome,
        cancel_requested: scope.cause(),
        gate_failures,
        observers: hub.reports(),
        emitted,
        straggler,
    });
}
