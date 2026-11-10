//! Stream operators: the steps of a stream vine's pipe.
//!
//! A stream vine is a pipe. Each step takes a stream and gives a stream.
//! Three kinds of step stand in a pipe:
//!
//! - An **ordinary operator** (registered with
//!   [`Registry::register`](crate::Registry::register)) applies to each
//!   item flowing past, one at a time, in order. Its settings see the
//!   item as `$item` and the source positions it came from as `$span`.
//!   Its result is the next step's `$item`.
//! - A **stream operator** (registered with
//!   [`Registry::register_stream`](crate::Registry::register_stream))
//!   changes the flow: it may hold items, emit fewer or more than it got,
//!   emit on a timer, and emit at end of stream. `collect` is the core one.
//! - **`each`**, last in the pipe only, holds once-style steps (`guard`,
//!   `switch`, `try`, `out`) and runs them for each item.
//!
//! # Writing a stream operator
//!
//! Implement [`StreamOperator`] and register a factory that builds a fresh
//! instance from the step's settings. The factory runs once when the vine
//! loads (to check the settings: return `Err` with a readable problem) and
//! once at the start of every run. Settings are fixed when the vine loads:
//! they cannot refer to `$item`.
//!
//! # Memory
//!
//! An operator declares its [`Memory`]:
//!
//! - [`Memory::None`]: holds nothing between items.
//! - [`Memory::Rebuildable`]: holds items it can get back by replay, such as
//!   `collect`'s buffer. Items it holds are **not consumed**: the position
//!   stays before them, so a resumed run feeds them in again. It must say,
//!   through [`StreamOperator::holding`], whether it holds anything after
//!   each call.
//! - [`Memory::Kept`]: holds something replay cannot rebuild, such as a
//!   running summary. The runner asks for it with [`StreamOperator::save`],
//!   hands it to the caller in each [`Checkpoint`], and gives it back with
//!   [`StreamOperator::restore`] on resume. Whatever a kept operator has
//!   absorbed is consumed once its results have come out the end.
//!
//! # Position
//!
//! Source items are numbered from 1. The position is "consumed through
//! item N": it moves only once everything that depended on items up to N
//! has come out the end of the pipe, or was absorbed by a kept operator, or
//! was dropped by an operator holding nothing. A batch from `collect`
//! moves it by the whole batch when the batch's result comes out the end.
//!
//! Every call to an operator (`push`, `wake`, `end`) may emit any number of
//! values. They carry, as `$span`, the source positions released by that
//! call. The position moves past them when the last of them comes out the
//! end. If an earlier one came out and a later one failed, a resumed run
//! emits the earlier one again: fan-out is at-least-once.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::error::RunError;
use crate::registry::{OpContext, OpError};
use crate::run::Output;
use crate::shape::{Shape, Ty};

/// What an operator holds between items. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Memory {
    None,
    Rebuildable,
    Kept,
}

/// What a stream operator declares about itself.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamOperatorSpec {
    pub name: String,
    /// Checked when the vine loads; the factory may check more.
    pub settings: Shape,
    /// The shape of each value it emits, or `None` for any value.
    pub emits: Option<Shape>,
    pub touches: BTreeSet<String>,
    pub memory: Memory,
}

impl StreamOperatorSpec {
    /// No settings, emits anything, touches nothing, holds nothing.
    pub fn new(name: &str) -> Self {
        StreamOperatorSpec {
            name: name.to_owned(),
            settings: Shape::new(),
            emits: None,
            touches: BTreeSet::new(),
            memory: Memory::None,
        }
    }

    pub fn settings(mut self, shape: Shape) -> Self {
        self.settings = shape;
        self
    }

    pub fn emits(mut self, shape: Shape) -> Self {
        self.emits = Some(shape);
        self
    }

    pub fn touches<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.touches.extend(names.into_iter().map(Into::into));
        self
    }

    pub fn memory(mut self, memory: Memory) -> Self {
        self.memory = memory;
        self
    }
}

/// Where an operator puts what it emits during one call.
#[derive(Debug, Default)]
pub struct Emit {
    values: Vec<Value>,
}

impl Emit {
    pub fn emit(&mut self, value: Value) {
        self.values.push(value);
    }

    pub(crate) fn take(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.values)
    }
}

/// A step that takes a stream and gives a stream. One instance serves one
/// run. Calls never overlap: the runner awaits each before the next.
pub trait StreamOperator: Send {
    /// One item arrives.
    fn push<'a>(
        &'a mut self,
        item: Value,
        out: &'a mut Emit,
        cx: &'a OpContext,
    ) -> BoxFuture<'a, Result<(), OpError>>;

    /// When this operator next wants [`wake`](Self::wake) called, if ever.
    fn deadline(&self) -> Option<Instant> {
        None
    }

    /// The deadline passed. Must clear or move the deadline; a deadline
    /// still due after `wake` stops the run.
    fn wake<'a>(
        &'a mut self,
        out: &'a mut Emit,
        cx: &'a OpContext,
    ) -> BoxFuture<'a, Result<(), OpError>> {
        let _ = (out, cx);
        Box::pin(async { Ok(()) })
    }

    /// The source closed. Flush and emit any end-of-stream values. Memory
    /// is dropped afterwards.
    fn end<'a>(
        &'a mut self,
        out: &'a mut Emit,
        cx: &'a OpContext,
    ) -> BoxFuture<'a, Result<(), OpError>> {
        let _ = (out, cx);
        Box::pin(async { Ok(()) })
    }

    /// Rebuildable operators: whether anything is held after the last call.
    fn holding(&self) -> bool {
        false
    }

    /// Kept operators: the memory to save.
    fn save(&self) -> Value {
        Value::Null
    }

    /// Kept operators: take back saved memory before the first item.
    fn restore(&mut self, memory: Value) -> Result<(), String> {
        let _ = memory;
        Ok(())
    }
}

pub(crate) type Factory =
    Arc<dyn Fn(&Value) -> Result<Box<dyn StreamOperator>, String> + Send + Sync>;

#[derive(Clone)]
pub(crate) struct RegisteredStream {
    pub spec: Arc<StreamOperatorSpec>,
    pub factory: Factory,
}

/// Where a stream run is: what to save, and what to resume from.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Checkpoint {
    /// Source items consumed, counted from the first item ever. A resumed
    /// run's source starts at the item after this.
    pub position: u64,
    /// Kept memory, by the path of the step that keeps it (`steps[1]`).
    pub memory: BTreeMap<String, Value>,
}

impl Checkpoint {
    /// A fresh start: nothing consumed, no memory.
    pub fn start() -> Self {
        Self::default()
    }
}

/// Where a stream run sends what it produces. The caller saves
/// checkpoints; the crate stores nothing.
pub trait StreamSink: Send {
    /// One result came out the end of the pipe.
    fn output(&mut self, output: Output);
    /// The position or kept memory moved. Save it to resume from.
    fn checkpoint(&mut self, checkpoint: &Checkpoint);
}

/// How a stream run stopped.
#[derive(Debug, Clone, PartialEq)]
pub enum Stop {
    /// The source closed and every operator flushed. Memory was dropped.
    Ended,
    /// The caller paused (cancelled the token). Resume from the checkpoint.
    Paused,
    /// A step failed. The checkpoint is before the work that failed.
    Failed(RunError),
}

/// What a stream run did.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamReport {
    pub stop: Stop,
    /// The last checkpoint: what to save and resume from.
    pub checkpoint: Checkpoint,
}

// ---------------------------------------------------------------- collect

/// `collect: { size, or_every? }`: batches of `size` items, or what has
/// arrived when `or_every` has passed since the batch's first item,
/// whichever comes first. Emits `{ items, count }`. Flushes a partial
/// batch at end of stream. Never emits an empty batch.
pub(crate) fn collect_spec() -> StreamOperatorSpec {
    StreamOperatorSpec::new("collect")
        .settings(
            Shape::new()
                .required("size", Ty::Int)
                .optional("or_every", Ty::Text),
        )
        .emits(
            Shape::new()
                .required("items", Ty::List)
                .required("count", Ty::Int),
        )
        .memory(Memory::Rebuildable)
}

pub(crate) fn collect_factory(settings: &Value) -> Result<Box<dyn StreamOperator>, String> {
    let size = settings["size"]
        .as_u64()
        .filter(|n| *n >= 1)
        .ok_or("`size` must be at least 1")?;
    let every = match &settings["or_every"] {
        Value::Null => None,
        v => {
            let text = v.as_str().unwrap_or_default();
            let d = parse_duration(text).ok_or_else(|| {
                format!("`or_every` is a duration such as `60s`, `500ms` or `2m`, found {v}")
            })?;
            if d.is_zero() {
                return Err("`or_every` must be longer than zero".into());
            }
            Some(d)
        }
    };
    Ok(Box::new(Collect {
        size: size as usize,
        every,
        buf: Vec::new(),
        due: None,
    }))
}

/// `500ms`, `60s`, `2m`, `1h`.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let split = s.find(|c: char| !c.is_ascii_digit())?;
    let (n, unit) = s.split_at(split);
    let n: u64 = n.parse().ok()?;
    Some(match unit {
        "ms" => Duration::from_millis(n),
        "s" => Duration::from_secs(n),
        "m" => Duration::from_secs(n.checked_mul(60)?),
        "h" => Duration::from_secs(n.checked_mul(3600)?),
        _ => return None,
    })
}

struct Collect {
    size: usize,
    every: Option<Duration>,
    buf: Vec<Value>,
    due: Option<Instant>,
}

impl Collect {
    fn flush(&mut self, out: &mut Emit) {
        self.due = None;
        if self.buf.is_empty() {
            return;
        }
        let items = std::mem::take(&mut self.buf);
        let count = items.len();
        out.emit(json!({ "items": items, "count": count }));
    }
}

impl StreamOperator for Collect {
    fn push<'a>(
        &'a mut self,
        item: Value,
        out: &'a mut Emit,
        _cx: &'a OpContext,
    ) -> BoxFuture<'a, Result<(), OpError>> {
        Box::pin(async move {
            if self.buf.is_empty() {
                self.due = self.every.map(|e| Instant::now() + e);
            }
            self.buf.push(item);
            if self.buf.len() >= self.size {
                self.flush(out);
            }
            Ok(())
        })
    }

    fn deadline(&self) -> Option<Instant> {
        self.due
    }

    fn wake<'a>(
        &'a mut self,
        out: &'a mut Emit,
        _cx: &'a OpContext,
    ) -> BoxFuture<'a, Result<(), OpError>> {
        self.flush(out);
        Box::pin(async { Ok(()) })
    }

    fn end<'a>(
        &'a mut self,
        out: &'a mut Emit,
        _cx: &'a OpContext,
    ) -> BoxFuture<'a, Result<(), OpError>> {
        self.flush(out);
        Box::pin(async { Ok(()) })
    }

    fn holding(&self) -> bool {
        !self.buf.is_empty()
    }
}
