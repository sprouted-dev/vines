//! The durable-acceptance boundary.
//!
//! Vines holds no durable state. A `durability = durable` observer hands
//! every frame to a [`DurableSink`] the consumer (Terra) supplies, and a
//! frame counts as accepted only when the sink returns [`Ack`]. Vines does
//! not retry a rejected frame: retry, reconciliation and `unknown` belong
//! to the sink's owner.

use std::future::Future;

use thiserror::Error;

use crate::frame::Frame;

/// A sink's acknowledgement that it durably accepted a frame. Construct it
/// with [`Ack::accepted`] only once the frame is safe by the sink's own
/// definition (committed, fsynced, appended).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct Ack {
    _accepted: (),
}

impl Ack {
    /// The frame is durably accepted.
    pub fn accepted() -> Self {
        Self { _accepted: () }
    }
}

/// A sink's refusal of a frame. The observer fails with
/// [`crate::observer::ObserverFailure::Sink`] and receives nothing more.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Error)]
#[error("durable sink rejected a frame: {message}")]
pub struct SinkError {
    message: String,
}

impl SinkError {
    /// A rejection with a reason.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// The reason.
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Where a durable observer's frames go. Vines calls [`Self::accept`] for
/// every frame in order (items, gap markers, and the end marker last), one
/// at a time, and never calls it again after an error.
pub trait DurableSink<P>: Send + 'static {
    /// Accept one frame. Return [`Ack`] only when it is durably accepted.
    fn accept(&mut self, frame: Frame<P>) -> impl Future<Output = Result<Ack, SinkError>> + Send;
}
