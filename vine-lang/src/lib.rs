//! An executable proof of the vine language.
//!
//! A vine is a stream in and a stream out: items flow through a pipe of
//! stages and come out on named outputs. A vine is described in Rust with
//! the functions in [`pipe`] and checked by [`Registry::build_pipe`], which
//! returns a [`Pipe`] that runs nothing until asked to.
//!
//! YAML is a layer on top, behind the `yaml` feature. `Registry::load_pipe`
//! reads a vine written as a document and hands the same description to
//! the same checker. With the feature off, no YAML parser is compiled in.
//!
//! The library reaches nothing outside by itself. **Operators** are Rust:
//! the library registers core ones, anyone registers more through
//! [`Registry::register`]. An **act** is something that does, such as
//! `git.worktree_add`. The caller registers the Rust behind each name with
//! [`Registry::register_act`], and a vine runs it with a `call` stage.
//!
//! - Stages, stream acts and `fork`: [`pipe`] module docs.
//! - Shapes: [`shape`] module docs.
//! - Expressions (`$item.x`, `==`, `!=`, null): [`expr`] module docs.
//! - Saving a pipe run and resuming it after a restart, from a journal for a
//!   run that ends or from a saved position for an endless stream:
//!   [`journal`] module docs.
//!
//! A vine holds no state and knows nothing about what starts it. Its reach,
//! [`Pipe::reach`], is everything its operators and acts touch.

pub mod error;
pub mod expr;
pub mod journal;
pub mod pipe;
pub mod registry;
pub mod shape;

pub use error::{LoadError, RegistryError, RunError};
pub use journal::{Bookmark, Ending, Entry, Event, Journal, Line, Progress, Timing};
pub use pipe::{OutputId, Pipe, PipeOutput};
pub use registry::{ActStream, OpContext, OpError, OperatorSpec, Part, Registry};
pub use shape::{Shape, Ty};

// Operators need these to build values.
pub use serde_json::{json, Value};
