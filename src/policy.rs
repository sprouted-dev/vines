//! Per-observer policy and source capabilities.
//!
//! Each observer of a run declares three independent policies and one
//! resource contract:
//!
//! | Policy | Values | Question it answers |
//! |---|---|---|
//! | [`Answer`] | `required` / `none` | Can this observer's failure settle the run's answer? |
//! | [`Durability`] | `ephemeral` / `durable` | Is a frame accepted only when a [`crate::sink::DurableSink`] acknowledges it? |
//! | [`Loss`] | `allowed` / `none` | May this observer skip frames? |
//! | queue + [`Overflow`] | capacity, `drop_oldest` / `drop_newest` / `stop_producer` / `spill{quota}` | What happens when the observer falls behind? |
//!
//! The rules (each refused with a typed [`PolicyError`]):
//!
//! 1. `loss = none` forbids `drop_oldest` and `drop_newest`.
//! 2. `durability = durable` requires a [`crate::sink::DurableSink`];
//!    `ephemeral` refuses one.
//! 3. `stop_producer` requires a source that declares
//!    [`Backpressure::Supported`]. Agents usually do not.
//! 4. `spill` requires a finite, non-zero quota.
//! 5. Every capacity is a [`NonZeroUsize`]: never zero, never "unlimited".
//!
//! [`ObserverDecl`] is the loose, as-authored form (numbers may be missing
//! or zero, the way they arrive from YAML); [`ObserverDecl::validate`] turns
//! it into a typed [`ObserverPolicy`] or a [`PolicyError`].

use std::fmt;
use std::num::NonZeroUsize;

use crate::error::PolicyError;

/// Answer authority: can this observer's failure settle the run's answer?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Answer {
    /// The run's answer is gated on this observer settling successfully.
    /// Its failure fails the answer explicitly (see
    /// [`crate::run::RunReport::answer`]).
    Required,
    /// This observer's failure never fails the run.
    None,
}

/// Delivery durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Durability {
    /// Frames are delivered to an in-process stream; nothing survives the
    /// process.
    Ephemeral,
    /// Frames go to a [`crate::sink::DurableSink`]; a frame counts as
    /// accepted only when the sink acknowledges it.
    Durable,
}

/// Acceptable loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Loss {
    /// The observer may skip frames. Every skip is counted and surfaces as
    /// a [`crate::frame::Gap`].
    Allowed,
    /// The observer may not skip frames. If it cannot keep up within its
    /// declared resources it fails rather than silently losing frames.
    None,
}

/// What happens when an observer's queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Overflow {
    /// Drop the oldest queued frame to admit the new one. Lossy.
    DropOldest,
    /// Drop the new frame. Lossy.
    DropNewest,
    /// Make the producer wait for space. Requires a backpressure-capable
    /// source; the one way a slow observer can slow the producer.
    StopProducer,
    /// Hold up to `quota` frames beyond the queue capacity. When the quota
    /// is exhausted: with `loss = allowed`, the new frame is dropped (a
    /// [`crate::frame::GapReason::SpillExhausted`] gap); with
    /// `loss = none`, the observer fails
    /// ([`crate::observer::ObserverFailure::SpillQuotaExhausted`]).
    Spill {
        /// Frames held beyond the queue capacity.
        quota: NonZeroUsize,
    },
}

impl Overflow {
    /// The policy's name without its parameters.
    pub fn kind(&self) -> OverflowKind {
        match self {
            Self::DropOldest => OverflowKind::DropOldest,
            Self::DropNewest => OverflowKind::DropNewest,
            Self::StopProducer => OverflowKind::StopProducer,
            Self::Spill { .. } => OverflowKind::Spill,
        }
    }

    /// Whether this policy drops frames by design.
    pub fn is_drop(&self) -> bool {
        matches!(self, Self::DropOldest | Self::DropNewest)
    }
}

/// An [`Overflow`] without its parameters, for errors and reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OverflowKind {
    /// [`Overflow::DropOldest`].
    DropOldest,
    /// [`Overflow::DropNewest`].
    DropNewest,
    /// [`Overflow::StopProducer`].
    StopProducer,
    /// [`Overflow::Spill`].
    Spill,
}

impl fmt::Display for OverflowKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::DropOldest => "drop_oldest",
            Self::DropNewest => "drop_newest",
            Self::StopProducer => "stop_producer",
            Self::Spill => "spill",
        })
    }
}

/// One observer's validated policy.
///
/// Fields are public so a caller can build one directly; the combination
/// rules are checked when the observer is attached to a run, so a policy
/// that breaks them can be written but never runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObserverPolicy {
    /// Answer authority.
    pub answer: Answer,
    /// Delivery durability.
    pub durability: Durability,
    /// Acceptable loss.
    pub loss: Loss,
    /// Frames the observer's queue holds before [`Self::overflow`] applies.
    pub queue: NonZeroUsize,
    /// What happens when the queue is full.
    pub overflow: Overflow,
}

impl ObserverPolicy {
    /// An ephemeral, lossy observer with no answer authority: the shape of
    /// a live display. Adjust with the setters.
    pub fn new(queue: NonZeroUsize, overflow: Overflow) -> Self {
        Self {
            answer: Answer::None,
            durability: Durability::Ephemeral,
            loss: Loss::Allowed,
            queue,
            overflow,
        }
    }

    /// Set the answer authority.
    #[must_use]
    pub fn answer(mut self, answer: Answer) -> Self {
        self.answer = answer;
        self
    }

    /// Set the delivery durability.
    #[must_use]
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Set the acceptable loss.
    #[must_use]
    pub fn loss(mut self, loss: Loss) -> Self {
        self.loss = loss;
        self
    }

    /// The most frames this observer can hold at once: the queue, plus the
    /// spill quota when it spills.
    pub fn holding_limit(&self) -> usize {
        match self.overflow {
            Overflow::Spill { quota } => self.queue.get().saturating_add(quota.get()),
            _ => self.queue.get(),
        }
    }

    /// Check the rules that need only the policy itself (rule 1).
    ///
    /// # Errors
    ///
    /// [`PolicyError::LossNoneForbidsDrop`] when `loss = none` meets a drop
    /// overflow.
    pub fn check(&self, observer: &str) -> Result<(), PolicyError> {
        if self.loss == Loss::None && self.overflow.is_drop() {
            return Err(PolicyError::LossNoneForbidsDrop {
                observer: observer.to_string(),
                overflow: self.overflow.kind(),
            });
        }
        Ok(())
    }

    /// Check every rule against the source and the wiring (rules 1 to 3).
    pub(crate) fn check_attach(
        &self,
        observer: &str,
        source: SourceDecl,
        has_durable_sink: bool,
    ) -> Result<(), PolicyError> {
        self.check(observer)?;
        match (self.durability, has_durable_sink) {
            (Durability::Durable, false) => {
                return Err(PolicyError::DurableRequiresSink {
                    observer: observer.to_string(),
                })
            }
            (Durability::Ephemeral, true) => {
                return Err(PolicyError::EphemeralGivenSink {
                    observer: observer.to_string(),
                })
            }
            _ => {}
        }
        if self.overflow == Overflow::StopProducer && source.backpressure != Backpressure::Supported
        {
            return Err(PolicyError::StopProducerRequiresBackpressure {
                observer: observer.to_string(),
            });
        }
        Ok(())
    }
}

/// Whether a source can be made to wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Backpressure {
    /// The producer can wait for observers without harm (a pulled stream,
    /// a paginated read).
    Supported,
    /// The producer cannot wait (an agent's output, a webhook). Observers
    /// must drop, spill or fail; they can never stall it.
    Unsupported,
}

/// What a source declares about itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourceDecl {
    /// Whether the producer can be backpressured.
    pub backpressure: Backpressure,
}

impl SourceDecl {
    /// A source that can never be made to wait. The default stance for
    /// agents.
    pub fn without_backpressure() -> Self {
        Self {
            backpressure: Backpressure::Unsupported,
        }
    }

    /// A source that can be made to wait.
    pub fn with_backpressure() -> Self {
        Self {
            backpressure: Backpressure::Supported,
        }
    }
}

/// An overflow policy as authored, before validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OverflowDecl {
    /// `drop_oldest`.
    DropOldest,
    /// `drop_newest`.
    DropNewest,
    /// `stop_producer`.
    StopProducer,
    /// `spill { quota }`; the quota may be missing or zero as authored.
    Spill {
        /// The authored quota.
        quota: Option<usize>,
    },
}

/// An observer policy as authored (from YAML, JSON, a CLI), before
/// validation. Limits may be missing or zero here; [`Self::validate`]
/// refuses both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObserverDecl {
    /// Answer authority.
    pub answer: Answer,
    /// Delivery durability.
    pub durability: Durability,
    /// Acceptable loss.
    pub loss: Loss,
    /// Queue capacity as authored.
    pub queue: Option<usize>,
    /// Overflow as authored.
    pub overflow: OverflowDecl,
}

impl ObserverDecl {
    /// Validate into a typed policy. Checks that every limit is present
    /// and non-zero, and the rules that need only the policy (rule 1).
    /// Source and sink rules are checked when the observer is attached.
    ///
    /// # Errors
    ///
    /// [`PolicyError::MissingLimit`] or [`PolicyError::ZeroLimit`] for the
    /// queue or spill quota, [`PolicyError::SpillRequiresQuota`] for a spill
    /// without one, and [`PolicyError::LossNoneForbidsDrop`] as in
    /// [`ObserverPolicy::check`].
    pub fn validate(&self, observer: &str) -> Result<ObserverPolicy, PolicyError> {
        let queue = nonzero(observer, "queue", self.queue)?;
        let overflow = match self.overflow {
            OverflowDecl::DropOldest => Overflow::DropOldest,
            OverflowDecl::DropNewest => Overflow::DropNewest,
            OverflowDecl::StopProducer => Overflow::StopProducer,
            OverflowDecl::Spill { quota: None } => {
                return Err(PolicyError::SpillRequiresQuota {
                    observer: observer.to_string(),
                })
            }
            OverflowDecl::Spill { quota } => Overflow::Spill {
                quota: nonzero(observer, "spill.quota", quota)?,
            },
        };
        let policy = ObserverPolicy {
            answer: self.answer,
            durability: self.durability,
            loss: self.loss,
            queue,
            overflow,
        };
        policy.check(observer)?;
        Ok(policy)
    }
}

/// Validate an authored limit: present and non-zero.
///
/// # Errors
///
/// [`PolicyError::MissingLimit`] when `value` is `None`, and
/// [`PolicyError::ZeroLimit`] when it is zero; both name `scope` and
/// `field`.
pub fn nonzero(
    scope: &str,
    field: &'static str,
    value: Option<usize>,
) -> Result<NonZeroUsize, PolicyError> {
    let value = value.ok_or_else(|| PolicyError::MissingLimit {
        scope: scope.to_string(),
        field,
    })?;
    NonZeroUsize::new(value).ok_or_else(|| PolicyError::ZeroLimit {
        scope: scope.to_string(),
        field,
    })
}
