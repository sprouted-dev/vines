//! One model: a vine is a stream in and a stream out.
//!
//! Items go in, flow through a pipe of stages, and come out on the vine's
//! named outputs. Every stage takes a stream and gives a stream. There is
//! no "once" and no "stream" here: a vine fed one item is a stream of one.
//! The vine does not know what an output becomes. It emits values, and
//! whoever runs it decides what to do with them.
//!
//! Build one in Rust with the functions in this module, then check it with
//! [`Registry::build_pipe`](crate::Registry::build_pipe). The YAML layer
//! writes the same stages down as text.
//!
//! ```
//! use vine_lang::pipe::{emit_with, pipe, split};
//! use vine_lang::{json, Registry, Shape, Ty};
//!
//! let def = pipe("each_job")
//!     .input(Shape::new().required("plan", Ty::Id).required("jobs", Ty::List))
//!     .output("wanted", Shape::new().required("plan", Ty::Id).required("job", Ty::Any))
//!     .stages([
//!         split("$item.jobs").into("job"),
//!         emit_with("wanted", json!({ "plan": "$item.plan", "job": "$item.job" })),
//!     ]);
//! let vine = Registry::new().build_pipe(def).unwrap();
//! # let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! let out = rt.block_on(vine.run_one(json!({ "plan": "p-1", "jobs": ["a", "b"] }))).unwrap();
//! assert_eq!(out.len(), 2);
//! ```
//!
//! # The item
//!
//! A stage sees the item reaching it as `$item`. A stage's result replaces
//! the item, unless the stage says `into: name`. Then the item is kept and
//! the result is added to it under that name. That is how a vine carries
//! what it has learned forward: there are no variables, only the item.
//!
//! # The stages
//!
//! - [`op`] and [`call`]: run an operator or an act for each item.
//! - [`tap`]: run an act for its effect. The item passes through unchanged.
//! - [`split`]: one item becomes one item per element of a list.
//! - [`filter`]: drop the items a condition does not hold for.
//! - [`take`]: let this many items through, then end the stream.
//! - [`last`]: when the stream ends, let only its final item through.
//! - [`collect`]: gather items into batches of a size, and give what is
//!   left as one more batch when the stream ends.
//! - [`together`]: run several operators or acts side by side for one
//!   item, and go on when all of them are done.
//! - [`each`]: run a pipe for each item. [`Overlap`] says what happens
//!   when a new item arrives while the last one is still running.
//! - [`fork`]: send each item down side pipes as well as on down this one.
//! - [`use_vine`]: run another vine for each item, and say what to do
//!   with each thing it emits.
//! - [`branch`]: run the pipe whose key equals a value of the item.
//! - [`emit`]: send the item to one of the vine's named outputs.
//!
//! # Stream acts
//!
//! An act registered with
//! [`Registry::register_stream_act`](crate::Registry::register_stream_act)
//! gives a stream. `call` on it turns one item into one item per value the
//! act gives. With `into`, each is the item with the value added. The act
//! may end with an end value, such as `{ status: finished }`. The stage
//! drops it, unless the stage names a field for it with
//! [`end`](Stage::end). Then one last item comes after the others: the
//! item as it reached the stage, with the end value added under that name
//! and no `into` field. So a stream that gave nothing still gives its end.
//!
//! A failure from a stream act is an ordinary failure. The items it gave
//! before the failure have already gone on. A `catch` can handle it.
//!
//! # Fork
//!
//! [`fork`] sends each item down every side pipe and then on down the main
//! pipe, unchanged and in order. A side pipe sees the whole stream, so
//! [`collect`] and [`last`] work in it. What a side pipe emits comes out of
//! the vine. A value that reaches the end of a side pipe without being
//! emitted is dropped. For each item, the side pipes run in the order they
//! are written, each until it wants the next item, and then the item goes
//! on down the main pipe.
//!
//! A failure in a side pipe is a failure of the item that caused it, as if
//! a stage of the main pipe had failed: a `catch` after the fork handles
//! it, and the item's work stops. Without a `catch` it ends the stream.
//!
//! # Stages that cover the ones before them
//!
//! Four stages do not act on the item. They stand after the stages they
//! cover, as `catchError` and `finalize` do in RxJS, and they work one
//! item at a time:
//!
//! - [`catch`]: if a covered stage fails, run these stages instead. They
//!   get the item as it was when it failed, plus the failure as
//!   `$item.error`.
//! - [`finalize`]: run these stages afterwards, however the work ended.
//! - [`retry`]: if a covered stage fails, wait and run them again.
//! - [`timeout`]: fail the item if the covered stages take too long.
//!
//! One failing item does not stop the items after it, as long as a
//! `catch` covers the stage that failed. A failure nothing catches ends
//! the whole stream.
//!
//! `catch` and `finalize` pass on what the covered stages give as it comes.
//! `retry` and `timeout` hold it until the covered stages are done for the
//! item.
//!
//! ## The end of the stream inside a covered stage
//!
//! A covering stage runs the stages before it once for each item that
//! reaches them. So in there, "when the stream ends" means when that one
//! item's own stream ends: [`last`] gives the final line of one agent
//! run, not of every run. The checker refuses `last` and `collect` where
//! they could only ever see one item: before a covering stage, or inside
//! `each`, `branch`, `catch`, `finalize` or `use`, with no stage before
//! them that makes many items from one, such as `split` or a stream act.
//!
//! # Surviving a restart
//!
//! [`Pipe::run_saved`] runs a vine and hands the caller, besides what it
//! emits, [`Entry`] values to save. After a restart the caller runs the
//! vine again over the same input with the [`Journal`] it saved. The run
//! replays: an act with a saved result is not called again, a stream act
//! gives its saved items again and then carries on from where it was, and
//! an output the caller was already handed is not handed out again. The
//! stages rebuild what they held, such as a half full batch, because the
//! same items flow through them again. See [`crate::journal`].
//!
//! Replay needs the run to call its acts in the same order every time. So
//! a vine with `each` that merges, switches or exhausts, or with a
//! `timeout`, cannot be resumed, and `run_saved` refuses it.
//!
//! # What this module leaves out
//!
//! `finalize` does not run when the caller drops the stream partway: Rust
//! has no async drop. A run that is dropped and never resumed leaves
//! behind whatever its acts made. A journal only grows: nothing compacts
//! it.

mod book;
mod check;
mod drive;
mod flow;
mod unit;
#[cfg(feature = "yaml")]
mod yaml;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::ready;
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};
use serde_json::Value;

use crate::error::RunError;
use crate::journal::{Event, Journal};
use crate::run::Output;
use crate::shape::Shape;

use book::{Book, Cx};
use flow::{fatal, Flowing, Items, Transform};

pub(crate) use check::assemble;
#[cfg(feature = "yaml")]
pub(crate) use yaml::load;

// ------------------------------------------------------------ description

/// What happens when a new item arrives while the pipe inside an [`each`]
/// is still running for an earlier one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overlap {
    /// Run them side by side. Results come out as they finish.
    Merge,
    /// Run them one at a time, in order.
    Concat,
    /// Drop the earlier run and start the new one.
    Switch,
    /// Ignore the new item while a run is going.
    Exhaust,
}

/// What a stage's result does to the item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keep {
    /// The result is the new item.
    Replace,
    /// The item is kept, with the result added under this name.
    Into(String),
    /// The item is kept unchanged and the result is dropped.
    Tap,
}

/// One stage of a pipe, as described.
#[derive(Debug, Clone, PartialEq)]
pub enum Stage {
    Op {
        name: String,
        with: Value,
        keep: Keep,
    },
    Call {
        act: String,
        with: Value,
        keep: Keep,
        /// For a stream act: the field its end value goes under, on one
        /// last item.
        end: Option<String>,
    },
    Split {
        from: Value,
        into: Option<String>,
    },
    Filter {
        when: Value,
    },
    Take(u64),
    /// When the stream ends, let only its final item through.
    Last,
    /// Batches of `size`, and what is left when the stream ends.
    Collect {
        size: u64,
        into: Option<String>,
    },
    Together(Vec<Stage>),
    Each {
        overlap: Overlap,
        at_once: Option<usize>,
        pipe: Vec<Stage>,
    },
    /// Send each item down every side pipe, and on down this one.
    Fork(Vec<Vec<Stage>>),
    /// Covers the stages before it in its pipe: if one of them fails for
    /// an item, run these stages instead.
    Catch(Vec<Stage>),
    /// Covers the stages before it: run these afterwards, however the
    /// item's work ended.
    Finalize(Vec<Stage>),
    /// Covers the stages before it: if one fails, run them again.
    Retry {
        times: u32,
        wait: Duration,
    },
    /// Covers the stages before it: fail if they take longer than this.
    Timeout(Duration),
    /// Run another vine for each item.
    Use {
        vine: String,
        with: Option<Value>,
        keep: Keep,
        on: Vec<(String, Vec<Stage>)>,
    },
    Branch {
        on: Value,
        cases: Vec<(String, Vec<Stage>)>,
        otherwise: Option<Vec<Stage>>,
    },
    Emit {
        to: String,
        with: Option<Value>,
    },
}

impl Stage {
    /// Keep the item and add this stage's result to it under `name`.
    /// For [`split`], each new item is the old one with the element added
    /// under `name`. For [`collect`], the item kept is the last one of the
    /// batch, with the batch added under `name`.
    pub fn into(mut self, name: &str) -> Self {
        match &mut self {
            Stage::Op { keep, .. } | Stage::Call { keep, .. } | Stage::Use { keep, .. } => {
                *keep = Keep::Into(name.to_owned())
            }
            Stage::Split { into, .. } | Stage::Collect { into, .. } => {
                *into = Some(name.to_owned())
            }
            _ => {}
        }
        self
    }

    /// For [`call`] on a stream act: after the items, give one last item
    /// with the act's end value added under `name`.
    pub fn end(mut self, name: &str) -> Self {
        if let Stage::Call { end, .. } = &mut self {
            *end = Some(name.to_owned());
        }
        self
    }

    /// For [`use_vine`]: the stages to run for what the other vine sends
    /// to this output. Every output of the other vine needs one.
    pub fn on(mut self, output: &str, stages: impl IntoIterator<Item = Stage>) -> Self {
        if let Stage::Use { on, .. } = &mut self {
            on.push((output.to_owned(), stages.into_iter().collect()));
        }
        self
    }

    /// For [`each`] with [`Overlap::Merge`]: run at most this many side by
    /// side.
    pub fn at_once(mut self, limit: usize) -> Self {
        if let Stage::Each { at_once, .. } = &mut self {
            *at_once = Some(limit);
        }
        self
    }
}

/// Run a registered operator for each item.
pub fn op(name: &str, with: Value) -> Stage {
    Stage::Op {
        name: name.to_owned(),
        with,
        keep: Keep::Replace,
    }
}

/// Run a registered act, such as `git.worktree_add`, for each item. On a
/// stream act, each item becomes one item per value the act gives.
pub fn call(act: &str, with: Value) -> Stage {
    Stage::Call {
        act: act.to_owned(),
        with,
        keep: Keep::Replace,
        end: None,
    }
}

/// Run an act for its effect. The item passes through unchanged. A stream
/// act cannot be tapped: use [`call`].
pub fn tap(act: &str, with: Value) -> Stage {
    Stage::Call {
        act: act.to_owned(),
        with,
        keep: Keep::Tap,
        end: None,
    }
}

/// One item becomes one item per element of the list at `from`.
pub fn split(from: &str) -> Stage {
    Stage::Split {
        from: Value::String(from.to_owned()),
        into: None,
    }
}

/// Drop the items the condition does not hold for.
pub fn filter(when: &str) -> Stage {
    Stage::Filter {
        when: Value::String(when.to_owned()),
    }
}

/// Let this many items through, then end the stream.
pub fn take(count: u64) -> Stage {
    Stage::Take(count)
}

/// When the stream ends, let only its final item through. An empty stream
/// gives nothing.
pub fn last() -> Stage {
    Stage::Last
}

/// Gather items into batches of `size`. A batch is `{ items, count }`. When
/// the stream ends, what is left comes as one more, smaller batch. An empty
/// batch is never given. With [`into`](Stage::into), the item is the last
/// one of the batch with the batch added under that name.
pub fn collect(size: u64) -> Stage {
    Stage::Collect { size, into: None }
}

/// Run these operators or acts side by side for one item, and go on when
/// all of them are done. Each one says where its result goes with `into`.
pub fn together(stages: impl IntoIterator<Item = Stage>) -> Stage {
    Stage::Together(stages.into_iter().collect())
}

/// Run a pipe for each item.
pub fn each(overlap: Overlap, pipe: impl IntoIterator<Item = Stage>) -> Stage {
    Stage::Each {
        overlap,
        at_once: None,
        pipe: pipe.into_iter().collect(),
    }
}

/// Send each item down every one of these side pipes, and on down this
/// pipe unchanged. See the module docs.
pub fn fork<P>(pipes: impl IntoIterator<Item = P>) -> Stage
where
    P: IntoIterator<Item = Stage>,
{
    Stage::Fork(pipes.into_iter().map(|p| p.into_iter().collect()).collect())
}

/// If a stage before this one fails for an item, run these stages
/// instead. They get the item as it was when it failed, with the failure
/// added as `$item.error`: its `code`, its `message`, and `from`, the
/// operator or act that failed.
pub fn catch(stages: impl IntoIterator<Item = Stage>) -> Stage {
    Stage::Catch(stages.into_iter().collect())
}

/// Run these stages after the stages before this one, however the item's
/// work ended. They get the item as it was when it arrived. They are for
/// cleanup: what they give back is dropped, and they may not emit.
pub fn finalize(stages: impl IntoIterator<Item = Stage>) -> Stage {
    Stage::Finalize(stages.into_iter().collect())
}

/// If a stage before this one fails for an item, wait and run them again,
/// up to `times` more times. Every act they run must be safe to repeat.
pub fn retry(times: u32, wait: Duration) -> Stage {
    Stage::Retry { times, wait }
}

/// Fail an item with the code `timeout` if the stages before this one
/// take longer than `limit`.
pub fn timeout(limit: Duration) -> Stage {
    Stage::Timeout(limit)
}

/// Run another vine for each item. `with` builds its input. With no
/// `with`, the item is its input. Add an [`on`](Stage::on) for each of
/// its outputs.
pub fn use_vine(vine: &str, with: Option<Value>) -> Stage {
    Stage::Use {
        vine: vine.to_owned(),
        with,
        keep: Keep::Replace,
        on: Vec::new(),
    }
}

/// Run the pipe whose key equals the value at `on`. With no `otherwise`,
/// an item that matches no case passes through unchanged.
pub fn branch<'a, C, P>(on: &str, cases: C, otherwise: Option<Vec<Stage>>) -> Stage
where
    C: IntoIterator<Item = (&'a str, P)>,
    P: IntoIterator<Item = Stage>,
{
    Stage::Branch {
        on: Value::String(on.to_owned()),
        cases: cases
            .into_iter()
            .map(|(k, p)| (k.to_owned(), p.into_iter().collect()))
            .collect(),
        otherwise,
    }
}

/// Send the item, as it is, to one of the vine's outputs.
pub fn emit(to: &str) -> Stage {
    Stage::Emit {
        to: to.to_owned(),
        with: None,
    }
}

/// Send this value to one of the vine's outputs.
pub fn emit_with(to: &str, with: Value) -> Stage {
    Stage::Emit {
        to: to.to_owned(),
        with: Some(with),
    }
}

/// A vine as described, before it is checked.
#[derive(Debug, Clone)]
pub struct PipeDef {
    pub(crate) name: String,
    pub(crate) input: Shape,
    pub(crate) outputs: BTreeMap<String, Shape>,
    pub(crate) stages: Vec<Stage>,
}

/// Start describing a vine.
pub fn pipe(name: &str) -> PipeDef {
    PipeDef {
        name: name.to_owned(),
        input: Shape::new(),
        outputs: BTreeMap::new(),
        stages: Vec::new(),
    }
}

impl PipeDef {
    /// The shape of each item that goes in.
    pub fn input(mut self, shape: Shape) -> Self {
        self.input = shape;
        self
    }

    /// Declare one named output and the shape of what it carries.
    pub fn output(mut self, name: &str, shape: Shape) -> Self {
        self.outputs.insert(name.to_owned(), shape);
        self
    }

    /// The stages, first to last.
    pub fn stages(mut self, stages: impl IntoIterator<Item = Stage>) -> Self {
        self.stages = stages.into_iter().collect();
        self
    }
}

/// A checked vine, ready to run.
#[derive(Clone)]
pub struct Pipe {
    name: String,
    input: Shape,
    outputs: Arc<BTreeMap<String, Shape>>,
    reach: BTreeSet<String>,
    /// Why the vine cannot be resumed, if it cannot.
    unresumable: Option<String>,
    transform: Transform,
}

impl std::fmt::Debug for Pipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipe")
            .field("name", &self.name)
            .field("outputs", &self.outputs.keys().collect::<Vec<_>>())
            .field("reach", &self.reach)
            .finish_non_exhaustive()
    }
}

impl Pipe {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn input(&self) -> &Shape {
        &self.input
    }

    pub fn outputs(&self) -> &BTreeMap<String, Shape> {
        &self.outputs
    }

    /// Everything this vine's operators and acts touch, sorted.
    pub fn reach(&self) -> &BTreeSet<String> {
        &self.reach
    }

    /// Whether [`Pipe::run_saved`] can run this vine.
    pub fn resumable(&self) -> bool {
        self.unresumable.is_none()
    }

    /// Run the vine over a stream of items. What comes back is the stream
    /// of what it emits. A failure no `catch` handles is the last thing
    /// on the stream. Dropping the stream stops the run.
    pub fn run<S>(&self, input: S) -> BoxStream<'static, Result<Output, RunError>>
    where
        S: Stream<Item = Value> + Send + 'static,
    {
        self.drive(input, None)
            .filter_map(|r| {
                ready(match r {
                    Ok(Event::Output(out)) => Some(Ok(out)),
                    Ok(Event::Save(_)) => None,
                    Err(e) => Some(Err(e)),
                })
            })
            .boxed()
    }

    /// Run the vine so that it can be resumed after a restart.
    ///
    /// Besides each [`Event::Output`], the run gives [`Event::Save`]
    /// entries. Append each one to what you keep for this run, in order.
    /// After a restart, call `run_saved` again with the same input items,
    /// from the first, and the [`Journal`] of what you kept. An empty
    /// journal is a fresh start.
    ///
    /// The run hands out an output and then the [`Entry::Emitted`] that
    /// records it. If you save after every entry, no output is handed out
    /// twice. If the run stops after an output and before you saved the
    /// entry after it, that output comes again.
    ///
    /// A vine with `each` that merges, switches or exhausts, or with a
    /// `timeout`, cannot be resumed: the run gives one
    /// [`RunError::NotResumable`] and ends.
    pub fn run_saved<S>(
        &self,
        input: S,
        journal: Journal,
    ) -> BoxStream<'static, Result<Event, RunError>>
    where
        S: Stream<Item = Value> + Send + 'static,
    {
        if let Some(why) = &self.unresumable {
            let e = RunError::NotResumable {
                vine: self.name.clone(),
                why: why.clone(),
            };
            return stream::once(ready(Err(e))).boxed();
        }
        match Book::from_journal(&journal) {
            Ok(book) => self.drive(input, Some(Arc::new(Mutex::new(book)))),
            Err(problem) => stream::once(ready(Err(RunError::ReplayMisfit { problem }))).boxed(),
        }
    }

    /// Run as a stage of another vine.
    fn flow(&self, items: Items, cx: Cx) -> Items {
        let shape = self.input.clone();
        let checked = items.map(move |flow| match flow {
            Ok(Flowing::Value(v)) => match shape.check(&v) {
                Ok(()) => Ok(Flowing::Value(v)),
                Err(p) => Err(fatal(RunError::InputMisfit(p))),
            },
            other => other,
        });
        (self.transform)(checked.boxed(), cx)
    }

    /// Feed the vine one item and collect everything it emits. A vine that
    /// must give exactly one answer says so itself, with `take: 1`.
    pub async fn run_one(&self, input: Value) -> Result<Vec<Output>, RunError> {
        let mut out = Vec::new();
        let mut results = self.run(stream::once(ready(input)));
        while let Some(r) = results.next().await {
            out.push(r?);
        }
        Ok(out)
    }
}
