//! Typed errors for declarations vines refuses.
//!
//! Every policy rule in [`crate::policy`] has exactly one variant here, so a
//! caller (Terra's chain validator, say) can map a refusal back to the field
//! an author wrote.

use thiserror::Error;

use crate::policy::OverflowKind;

/// A declaration vines refuses to build.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum PolicyError {
    /// `loss = none` with `overflow = drop_oldest | drop_newest`: a drop
    /// policy is loss by definition.
    #[error("observer `{observer}`: loss=none forbids overflow={overflow}; declare stop_producer or spill")]
    LossNoneForbidsDrop {
        /// The observer's name.
        observer: String,
        /// The drop policy that was declared.
        overflow: OverflowKind,
    },
    /// `durability = durable` without a [`crate::sink::DurableSink`]: a
    /// frame can only count as accepted when a sink acknowledges it.
    #[error("observer `{observer}`: durability=durable requires a DurableSink (attach it with observe_durable)")]
    DurableRequiresSink {
        /// The observer's name.
        observer: String,
    },
    /// A [`crate::sink::DurableSink`] attached to an observer declared
    /// `durability = ephemeral`: the declaration and the wiring disagree.
    #[error("observer `{observer}`: durability=ephemeral was given a DurableSink; declare durability=durable")]
    EphemeralGivenSink {
        /// The observer's name.
        observer: String,
    },
    /// `overflow = stop_producer` on a source that does not declare it can
    /// be backpressured (an agent usually cannot).
    #[error("observer `{observer}`: overflow=stop_producer requires a source that declares backpressure support, and this source does not")]
    StopProducerRequiresBackpressure {
        /// The observer's name.
        observer: String,
    },
    /// `overflow = spill` without a quota.
    #[error("observer `{observer}`: overflow=spill requires a finite quota")]
    SpillRequiresQuota {
        /// The observer's name.
        observer: String,
    },
    /// A required limit was not declared. Vines has no implicit limits.
    #[error("{scope}: `{field}` is required; vines has no implicit limits")]
    MissingLimit {
        /// The observer's name, or the operator the limit belongs to.
        scope: String,
        /// The missing field.
        field: &'static str,
    },
    /// A limit was declared as zero. Zero never means "unlimited".
    #[error("{scope}: `{field}` must be at least 1; zero never means unlimited")]
    ZeroLimit {
        /// The observer's name, or the operator the limit belongs to.
        scope: String,
        /// The zero field.
        field: &'static str,
    },
    /// A duration limit was declared as zero.
    #[error("{scope}: `{field}` must be longer than zero")]
    ZeroDuration {
        /// The operator the duration belongs to.
        scope: String,
        /// The zero field.
        field: &'static str,
    },
    /// Two observers on one run share a name.
    #[error("observer `{observer}` is already attached to this run")]
    DuplicateObserver {
        /// The repeated name.
        observer: String,
    },
    /// An `answer = required` observer attached after the run started. A
    /// required observer gates the result, so it must be declared before
    /// the producer runs.
    #[error(
        "observer `{observer}`: answer=required observers must be declared before the run starts"
    )]
    LateRequiredObserver {
        /// The observer's name.
        observer: String,
    },
    /// An observer attached after the run's terminal result was decided.
    #[error("observer `{observer}`: the run has already ended")]
    RunEnded {
        /// The observer's name.
        observer: String,
    },
}
