//! An executable proof of the vine language.
//!
//! A vine is a stream in and a stream out: items flow through a pipe of
//! operators and come out on named outputs. A vine is described in Rust
//! with the functions in [`pipe`] and checked by [`Registry::build_pipe`],
//! which returns a [`Pipe`] that runs nothing until asked to.
//!
//! YAML is a layer on top, behind the `yaml` feature. `Registry::load_pipe`
//! reads a vine written as a document and hands the same description to
//! the same checker. With the feature off, no YAML parser is compiled in.
//!
//! Every step of a pipe is an operator, as in RxJS. The built-in ones, such
//! as `filter`, `mergeMap` and `catchError`, ship with the library. The
//! caller registers more by name with [`Registry::register`]. A registered
//! operator declares what it touches. One that touches nothing is pure.
//! One that touches something, such as `git.worktree_add`, reaches
//! outside: a vine runs it with `call` or `tap`, and its result is saved.
//! The library reaches nothing outside by itself.
//!
//! - Operators, stream operators and `fork`: [`pipe`] module docs.
//! - Registering operators: [`registry`] module docs.
//! - Shapes: [`shape`] module docs.
//! - Expressions (`$item.x`, `==`, `!=`, null): [`expr`] module docs.
//! - Saving a pipe run and resuming it after a restart, from a journal for a
//!   run that ends or from a saved position for an endless stream:
//!   [`journal`] module docs.
//!
//! A vine holds no state and knows nothing about what starts it. Its reach,
//! [`Pipe::reach`], is everything its registered operators touch.

pub mod error;
pub mod expr;
pub mod journal;
pub mod pipe;
pub mod registry;
pub mod shape;

pub use error::{LoadError, RegistryError, RunError};
pub use journal::{Bookmark, Ending, Entry, Event, Journal, Line, Progress, Timing};
pub use pipe::{OutputId, Pipe, PipeOutput};
pub use registry::{OpContext, OpError, OperatorSpec, OperatorStream, Part, Registry};
pub use shape::{Shape, Ty};

// Operators need these to build values.
pub use serde_json::{json, Value};
