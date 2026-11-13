//! An executable proof of the vine language.
//!
//! Two kinds of thing. **Operators** are Rust: the library registers core
//! ones, anyone registers more through [`Registry::register`]. **Vines**
//! string operators together. A vine is built in Rust with [`def`] and
//! checked by [`Registry::build`], which returns a [`Vine`] that runs
//! nothing until asked to.
//!
//! YAML is a layer on top, behind the `yaml` feature. `Registry::load`
//! reads a vine written as a document and hands the same description to
//! the same checker. With the feature off, no YAML parser is compiled in.
//!
//! The library reaches nothing outside by itself. An **act** is something
//! that does, such as `git.worktree_add`. The caller registers the Rust
//! behind each name with [`Registry::register_act`], and a vine runs it
//! with a `call` step.
//!
//! - Document format and control flow: [`vine`] module docs.
//! - Shapes: [`shape`] module docs.
//! - Expressions (`$in.x`, `==`, `!=`, null): [`expr`] module docs.
//! - Running, cancellation, pause and resume: [`run`] module docs.
//! - Stream vines and writing stream operators: [`stream`] module docs.
//! - Pipes, stream acts and `fork`: [`pipe`] module docs.
//! - Saving a pipe run and resuming it after a restart: [`journal`] module docs.
//!
//! A vine holds no state, names no other vine, and knows nothing about what
//! starts it. Its reach, [`Vine::reach`], is everything its operators touch.

pub mod def;
pub mod error;
pub mod expr;
pub mod journal;
pub mod pipe;
pub mod registry;
pub mod run;
pub mod shape;
pub mod stream;
pub mod vine;

pub use error::{LoadError, RegistryError, RunError};
pub use journal::{Entry, Event, Journal};
pub use pipe::Pipe;
pub use registry::{ActStream, OpContext, OpError, OperatorSpec, Part, Registry};
pub use run::Output;
pub use shape::{Shape, Ty};
pub use stream::{
    Checkpoint, Emit, Memory, Stop, StreamOperator, StreamOperatorSpec, StreamReport, StreamSink,
};
pub use vine::{Mode, Vine};
// `Step` and the builder functions stay under [`def`].

// Operators need these to build values and watch cancellation.
pub use serde_json::{json, Value};
pub use tokio_util::sync::CancellationToken;
// Stream operators set deadlines on tokio's clock, so paused test time works.
pub use futures::future::BoxFuture;
pub use tokio::time::Instant;
