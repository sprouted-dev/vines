//! An executable proof of the vine language.
//!
//! Two kinds of thing. **Operators** are Rust: the library registers core
//! ones, anyone registers more through [`Registry::register`]. **Vines** are
//! YAML documents that string operators together; [`Registry::load`] checks
//! one and returns a [`Vine`], which runs nothing until asked to.
//!
//! - Document format and control flow: [`vine`] module docs.
//! - Shapes: [`shape`] module docs.
//! - Expressions (`$in.x`, `==`, `!=`, null): [`expr`] module docs.
//! - Running, cancellation, pause and resume: [`run`] module docs.
//! - Stream vines and writing stream operators: [`stream`] module docs.
//!
//! A vine holds no state, names no other vine, and knows nothing about what
//! starts it. Its reach, [`Vine::reach`], is everything its operators touch.

pub mod error;
pub mod expr;
pub mod registry;
pub mod run;
pub mod shape;
pub mod stream;
pub mod vine;

pub use error::{LoadError, RegistryError, RunError};
pub use registry::{OpContext, OpError, OperatorSpec, Registry};
pub use run::Output;
pub use shape::{Shape, Ty};
pub use stream::{
    Checkpoint, Emit, Memory, Stop, StreamOperator, StreamOperatorSpec, StreamReport, StreamSink,
};
pub use vine::{Mode, Vine};

// Operators need these to build values and watch cancellation.
pub use serde_json::{json, Value};
pub use tokio_util::sync::CancellationToken;
// Stream operators set deadlines on tokio's clock, so paused test time works.
pub use futures::future::BoxFuture;
pub use tokio::time::Instant;
