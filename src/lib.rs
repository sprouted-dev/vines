//! # vines
//!
//! A thin, in-process stream composition layer over Tokio and `futures`
//! for workflow operators: agent output, channel batching, progress
//! fan-out.
//!
//! Vines is **not** a durable workflow engine and **not** an Rx clone. It
//! owns no storage, no scheduler and no retries. What it does own:
//!
//! - **The operator shape** ([`run`]): one producer, run once, with a live
//!   progress stream and exactly one terminal result. A full progress
//!   queue never holds the result back.
//! - **Declared per-observer policy** ([`policy`]): answer authority,
//!   durability, acceptable loss, queue capacity and overflow, each chosen
//!   per observer and checked when the observer is attached.
//! - **Visible loss** ([`frame`]): every dropped item is counted and marked
//!   with a [`frame::Gap`]; every observer stream ends with a
//!   [`frame::Coverage`] that says whether it saw everything.
//! - **Honest cancellation** ([`cancel`]): a cancel scope says that
//!   cancellation was requested or timed out; only the producer can say it
//!   stopped.
//! - **Operators** ([`ops`], [`batch`], [`ordering`]): map, filter, tap with
//!   an explicit failure policy, bounded parallel map, batching that flushes
//!   on an idle deadline, and joins that keep order only where declared.
//!
//! Durability, recovery, effects, claims and answers stay with the
//! consumer. Vines talks to durable storage only through
//! [`sink::DurableSink`].
//!
//! ## Example
//!
//! ```
//! use std::num::NonZeroUsize;
//! use std::time::Duration;
//! use vines::cancel::CancelScope;
//! use vines::policy::{Overflow, ObserverPolicy, SourceDecl};
//! use vines::run::{Exit, Outcome, RunBuilder};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let mut builder =
//!     RunBuilder::<String>::new(SourceDecl::without_backpressure(), Duration::from_secs(5))
//!         .unwrap();
//! let display = builder
//!     .observe(
//!         "display",
//!         ObserverPolicy::new(NonZeroUsize::new(64).unwrap(), Overflow::DropOldest),
//!     )
//!     .unwrap();
//!
//! let run = builder.spawn(&CancelScope::new(), |mut ctx| async move {
//!     for line in ["reading", "editing", "testing"] {
//!         // Never waits: this source declared no backpressure.
//!         let _ = ctx.emit(line.to_string()).await;
//!     }
//!     Exit::<_, String>::Completed("done")
//! });
//!
//! let record = display.into_record().await;
//! assert!(record.is_complete());
//! assert_eq!(record.items.len(), 3);
//!
//! let report = run.finish().await;
//! assert_eq!(report.answer().unwrap(), Outcome::Completed("done"));
//! # }
//! ```

#![warn(missing_docs)]

pub mod batch;
pub mod cancel;
pub mod error;
pub mod frame;
pub mod observer;
pub mod ops;
pub mod ordering;
mod outlet;
pub mod policy;
pub mod run;
pub mod sink;

pub use error::PolicyError;
