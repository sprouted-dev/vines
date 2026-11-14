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
//! let def = pipe("spread_jobs")
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
//! - [`buffer_count`] (`bufferCount`): gather items into batches of a
//!   size, and give what is left as one more batch when the stream ends.
//! - [`fork_join`] (`forkJoin`): run several operators or acts side by
//!   side for one item, and go on when all of them are done.
//! - [`concat_map`], [`merge_map`], [`switch_map`] and [`exhaust_map`]
//!   (`concatMap`, `mergeMap`, `switchMap`, `exhaustMap`): run a pipe for
//!   each item. Which one says what happens when a new item arrives while
//!   the last one is still running: wait for it, run beside it, drop it,
//!   or ignore the new one. [`Flatten`] names the four.
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
//! before the failure have already gone on. A `catchError` can handle it.
//!
//! # Fork
//!
//! [`fork`] sends each item down every side pipe and then on down the main
//! pipe, unchanged and in order. A side pipe sees the whole stream, so
//! [`buffer_count`] and [`last`] work in it. What a side pipe emits comes out of
//! the vine. A value that reaches the end of a side pipe without being
//! emitted is dropped. For each item, the side pipes run in the order they
//! are written, each until it wants the next item, and then the item goes
//! on down the main pipe.
//!
//! A failure in a side pipe is a failure of the item that caused it, as if
//! a stage of the main pipe had failed: a `catchError` after the fork
//! handles it, and the item's work stops. Without a `catchError` it ends
//! the stream.
//!
//! # Stages that cover the ones before them
//!
//! Four stages do not act on the item. They stand after the stages they
//! cover, as `catchError` and `finalize` do in RxJS, and they work one
//! item at a time:
//!
//! - [`catch_error`] (`catchError`): if a covered stage fails, run these stages instead. They
//!   get the item as it was when it failed, plus the failure as
//!   `$item.error`.
//! - [`finalize`]: run these stages afterwards, however the work ended.
//! - [`retry`]: if a covered stage fails, wait and run them again.
//! - [`timeout`]: fail the item if the covered stages take too long.
//!
//! One failing item does not stop the items after it, as long as a
//! `catchError` covers the stage that failed. A failure nothing catches ends
//! the whole stream.
//!
//! `catchError` and `finalize` pass on what the covered stages give as it
//! comes.
//! `retry` and `timeout` hold it until the covered stages are done for the
//! item.
//!
//! ## The end of the stream inside a covered stage
//!
//! A covering stage runs the stages before it once for each item that
//! reaches them. So in there, "when the stream ends" means when that one
//! item's own stream ends: [`last`] gives the final line of one agent
//! run, not of every run. The checker refuses `last` and `bufferCount`
//! where they could only ever see one item: before a covering stage, or
//! inside a mapping stage, `branch`, `catchError`, `finalize` or `use`,
//! with no stage before
//! them that makes many items from one, such as `split` or a stream act.
//!
//! # Surviving a restart
//!
//! [`Pipe::run_saved`] runs a vine and hands the caller, besides what it
//! emits, [`Entry`](crate::Entry) values to save. After a restart the caller runs the
//! vine again over the same input with the [`Journal`] it saved. The run
//! replays: an act with a saved result is not called again, a stream act
//! gives its saved items again and then carries on from where it was, and
//! an output the caller was already handed is not handed out again. The
//! stages rebuild what they held, such as a half full batch, because the
//! same items flow through them again. See [`crate::journal`].
//!
//! Every output carries an [`OutputId`], made from where it stands in the
//! run: the input item, the group of work, and how many outputs that
//! group made before it. An output the caller was handed and whose line
//! it had not saved comes again after a restart with the same id, so the
//! caller drops repeats by id. Two outputs of one run never share an id,
//! even with the same name and payload. See `output.rs`.
//!
//! Replay needs each act's key to name the same act every time. Keys
//! count how many times a stage ran, so a replay must reach each stage in
//! the same order the first run did. In a `mergeMap`, values run
//! side by side and finish in any order, so each value that enters it
//! starts a group of work, numbered in the order the values enter, and the
//! keys and outputs of its work name the group. Each group replays in its
//! own order. See [`crate::journal`] for the keys.
//!
//! So what a caller can rely on with a `mergeMap`:
//!
//! - Within one value's work, outputs come in order, before and after a
//!   restart.
//! - Across values, outputs come in the order the work finishes, and after
//!   a restart that order can differ. Nothing more is promised.
//! - An act is called only once the caller has been handed every line made
//!   so far, by any value. Acts already running finish in any order. After
//!   a crash, the acts that run again are those that had started and whose
//!   result the caller had not saved: at most one per value running side
//!   by side (times the widest `forkJoin`), so at most `concurrent` with
//!   a cap, plus one result handed out and not saved.
//!
//! The checker refuses, for both ways to resume, what a replay in another
//! order could not rebuild: `take` or `bufferCount` after a `mergeMap`, an
//! act on the value `last` picked after one, and a `catchError` or
//! `finalize` that covers one. `switchMap` and `exhaustMap` depend on
//! time, not on order, and are refused too. The rules are in `order.rs`.
//!
//! A `timeout` depends on time too, but what it decides is saved: each run
//! of the stages it covers is a scope of its own, and the run saves
//! [`Entry::Timed`](crate::Entry::Timed) before anything that depends on
//! the decision. A replay takes the decision and does not ask the clock.
//! With no decision saved, the wait starts again from nothing. See
//! `timer.rs`.
//!
//! # Endless streams: a saved position
//!
//! A journal suits a run that ends, such as one job. A vine over an
//! endless stream would replay from the first item ever, and its journal
//! would never shrink. [`Pipe::run_from`] runs it with a saved position
//! instead. It gives the same outputs and the same entries, each as a
//! [`Line`](crate::Line) naming the input item it belongs to, and
//! [`Progress::Through`] with `n` once every input item through `n` is
//! fully done. The caller keeps a [`Bookmark`]: the position and the lines
//! past it. After a restart it feeds the items after the position, and the
//! items that were in flight replay from their lines.
//!
//! The run knows an item is done by a tag. Each input item gets one when it
//! goes in, and every value, output and failure made from it carries a
//! clone. A batch from `bufferCount` carries the tags of its items, and ties
//! them together so the position never stops partway through a batch. An
//! output's tag is dropped when the output is handed out. When the last
//! clone of an item's tag is gone, nothing holds anything made from it.
//!
//! A stage that keeps something across items other than the items it
//! holds cannot be rebuilt by feeding the items after the position. So
//! `run_from` refuses, besides what `run_saved` refuses:
//!
//! - `take` over the whole stream: it counts, and the count would start
//!   again. A vine that ends itself has an end, so use `run_saved`.
//! - `bufferCount` over the whole stream after a stage that makes many items
//!   from one: a batch could end partway through an input item.
//!
//! [`Pipe::resumable_from_position`] says whether a vine can run this way.
//! `last` and `bufferCount` over whole items, `fork`, `catchError`,
//! `finalize`, `retry`, `timeout`, `concatMap`, `mergeMap`, `use`,
//! `branch` and stream acts all can.
//!
//! Under a `mergeMap`, an input item can be done before an
//! earlier one. The run gives [`Progress::Done`] for it, the caller's
//! bookmark keeps its number and drops its lines, and a run from that
//! bookmark skips it. The position still moves only past items in order.
//! So the bookmark holds the lines of the items in flight, at most
//! `concurrent` of them in the `mergeMap`, plus runs of numbers. Without
//! `concurrent` every item fed is in flight at once.
//!
//! # A stream act that gives its items again
//!
//! A stream act registered with
//! [`Registry::register_stream_act`](crate::Registry::register_stream_act)
//! is saved item by item: one line for each item it gives, and one for
//! each output made from them. An agent run that writes a line a second
//! for a day saves every line, twice over.
//!
//! An act registered with
//! [`Registry::register_replayable_stream_act`](crate::Registry::register_replayable_stream_act)
//! says it can give its items again from any item it gave, as a host that
//! stores what a session wrote can. Then the position idea works one level
//! down. The items a call gives are numbered from 1, and each gets a tag
//! that holds the tag of the item that started the call. When the last
//! clone of item `k`'s tag is gone, item `k` is done, and the call's own
//! position moves past it in order. A batch of the call's items ties them
//! together, as a batch of input items does. The run saves an
//! [`Entry::Through`](crate::Entry::Through): the call's position, and how
//! many outputs made from the items after it were handed out. It saves an
//! [`Entry::Stopped`](crate::Entry::Stopped) when the call ends, fails, or
//! is abandoned because a `catchError` or `finalize` took over. It saves no item
//! and no `Emitted` line for an output made from one.
//!
//! After a restart the act is asked for its items from the one after the
//! position. Stages that hold items, such as `bufferCount`, refill the way they
//! do for input items. The outputs the caller was handed are made again and
//! not handed out, by count. The keys of acts run for item `k` start with
//! the call's key and `k`, such as `pipe[1]#0@7/pipe[4]#0`, so they line up
//! whichever item a restart begins at. When the position passes `k`, the
//! caller's [`Journal::record`] or [`Bookmark::record`] drops those lines.
//! So what the caller keeps for a call is a few lines, however many items
//! it gives.
//!
//! The checker saves a call item by item, as if the act did not say it
//! gives again, where a later item could not rebuild what comes after it:
//!
//! - `take` that what the call gives reaches, in the same run of a pipe.
//!   It counts, and after a restart it would count from the position.
//! - `bufferCount` that could put the items of two calls, or of the call and of
//!   something else, in one batch. A batch must hold one call's items.
//! - `retry` over the call. It drops what a failed attempt gave and tries
//!   again, so an item it dropped would look done.
//!
//! [`Pipe::saves_every_item`] names each such call and why. A vine run by
//! `use` was checked on its own and cannot change how it saves, so a vine
//! that refuses what a used vine's call gives cannot be resumed.
//!
//! # What this module leaves out
//!
//! `finalize` does not run when the caller drops the stream partway: Rust
//! has no async drop. A run that is dropped and never resumed leaves
//! behind whatever its acts made. A journal only grows, except for the
//! lines a [`Journal::record`] drops for a stream act saved by position. A
//! bookmark shrinks as the position moves. A stream act saved item by item
//! keeps every line until its item is done.

mod book;
mod call_sites;
mod check;
mod drive;
mod flow;
mod order;
mod output;
mod stream_call;
mod tag;
mod timer;
mod unit;
#[cfg(feature = "yaml")]
mod yaml;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::ready;
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};
use serde_json::Value;

use crate::error::RunError;
use crate::journal::{Bookmark, Event, Journal, Progress};
use crate::shape::Shape;

use book::{Book, Cx};
use drive::Saving;
use flow::{fatal, Flowing, Items, Transform};
use timer::Covered;

pub(crate) use check::assemble;
pub use output::{OutputId, PipeOutput};
#[cfg(feature = "yaml")]
pub(crate) use yaml::load;

// ------------------------------------------------------------ description

/// Which of the four mapping stages a [`Stage::Mapped`] is: what happens
/// when a new item arrives while the inner pipe is still running for an
/// earlier one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flatten {
    /// `concatMap`: run them one at a time, in order.
    Concat,
    /// `mergeMap`: run them side by side. Results come out as they finish.
    Merge,
    /// `switchMap`: drop the earlier run and start the new one.
    Switch,
    /// `exhaustMap`: ignore the new item while a run is going.
    Exhaust,
}

impl Flatten {
    /// The stage's key in YAML and in paths, such as `mergeMap`.
    pub fn key(self) -> &'static str {
        match self {
            Flatten::Concat => "concatMap",
            Flatten::Merge => "mergeMap",
            Flatten::Switch => "switchMap",
            Flatten::Exhaust => "exhaustMap",
        }
    }
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
    BufferCount {
        size: u64,
        into: Option<String>,
    },
    ForkJoin(Vec<Stage>),
    /// `concatMap`, `mergeMap`, `switchMap` or `exhaustMap`, as `flatten`
    /// says: run a pipe for each item. `concurrent` caps how many run side
    /// by side, and only `mergeMap` takes it.
    Mapped {
        flatten: Flatten,
        concurrent: Option<usize>,
        pipe: Vec<Stage>,
    },
    /// Send each item down every side pipe, and on down this one.
    Fork(Vec<Vec<Stage>>),
    /// Covers the stages before it in its pipe: if one of them fails for
    /// an item, run these stages instead.
    CatchError(Vec<Stage>),
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
    /// under `name`. For [`buffer_count`], the item kept is the last one of the
    /// batch, with the batch added under `name`.
    pub fn into(mut self, name: &str) -> Self {
        match &mut self {
            Stage::Op { keep, .. } | Stage::Call { keep, .. } | Stage::Use { keep, .. } => {
                *keep = Keep::Into(name.to_owned())
            }
            Stage::Split { into, .. } | Stage::BufferCount { into, .. } => {
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

    /// For [`merge_map`]: run at most this many side by side. The checker
    /// refuses it on the other mapping stages.
    pub fn concurrent(mut self, limit: usize) -> Self {
        if let Stage::Mapped { concurrent, .. } = &mut self {
            *concurrent = Some(limit);
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
pub fn buffer_count(size: u64) -> Stage {
    Stage::BufferCount { size, into: None }
}

/// Run these operators or acts side by side for one item, and go on when
/// all of them are done. Each one says where its result goes with `into`.
pub fn fork_join(stages: impl IntoIterator<Item = Stage>) -> Stage {
    Stage::ForkJoin(stages.into_iter().collect())
}

fn mapped(flatten: Flatten, pipe: impl IntoIterator<Item = Stage>) -> Stage {
    Stage::Mapped {
        flatten,
        concurrent: None,
        pipe: pipe.into_iter().collect(),
    }
}

/// Run a pipe for each item, one item at a time, in order.
pub fn concat_map(pipe: impl IntoIterator<Item = Stage>) -> Stage {
    mapped(Flatten::Concat, pipe)
}

/// Run a pipe for each item, side by side. What it gives comes out as it
/// finishes. Cap how many run at once with [`concurrent`](Stage::concurrent).
pub fn merge_map(pipe: impl IntoIterator<Item = Stage>) -> Stage {
    mapped(Flatten::Merge, pipe)
}

/// Run a pipe for each item. A new item drops the run for the one before.
pub fn switch_map(pipe: impl IntoIterator<Item = Stage>) -> Stage {
    mapped(Flatten::Switch, pipe)
}

/// Run a pipe for each item. An item that arrives while a run is going is
/// ignored.
pub fn exhaust_map(pipe: impl IntoIterator<Item = Stage>) -> Stage {
    mapped(Flatten::Exhaust, pipe)
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
pub fn catch_error(stages: impl IntoIterator<Item = Stage>) -> Stage {
    Stage::CatchError(stages.into_iter().collect())
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
    /// The acts its `call` and `tap` stages name, here and in the vines it
    /// uses.
    acts: BTreeSet<String>,
    /// The vines it runs with `use`, and the vines those use.
    uses: BTreeSet<String>,
    /// Why the vine cannot be resumed, if it cannot.
    unresumable: Option<String>,
    /// Why the vine cannot run from a saved position, if it cannot.
    unpositioned: Option<String>,
    /// The stream act calls saved item by item, and why.
    saves_every_item: Vec<SavesEveryItem>,
    /// Some stream act call, here or in a vine this one uses, is saved by
    /// position.
    by_position: bool,
    /// It holds a `mergeMap`, here or in a vine it uses.
    merges: bool,
    /// Which acts each `timeout` covers, to refuse a journal saved before
    /// one was added.
    covered: Covered,
    transform: Transform,
}

/// A stream act call that a saved run saves item by item, so what the
/// caller keeps grows with every item it gives, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavesEveryItem {
    /// The path of the `call` stage, such as `pipe[1]`.
    pub path: String,
    /// The stream act it calls.
    pub act: String,
    /// Why it cannot be saved by position.
    pub why: String,
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

    /// Every act this vine runs with `call` or `tap`, here or in a vine it
    /// runs with `use`, sorted. Operators are not acts and are not listed.
    pub fn acts(&self) -> &BTreeSet<String> {
        &self.acts
    }

    /// Every vine this vine runs with `use`, and every vine those run with
    /// `use`, sorted by name. Empty for a vine that uses none.
    ///
    /// The library has no rule about which vine may use which. A caller
    /// with such a rule reads this to apply it.
    pub fn uses(&self) -> &BTreeSet<String> {
        &self.uses
    }

    /// Whether [`Pipe::run_saved`] can run this vine.
    pub fn resumable(&self) -> bool {
        self.unresumable.is_none()
    }

    /// Whether [`Pipe::run_from`] can run this vine.
    pub fn resumable_from_position(&self) -> bool {
        self.unresumable.is_none() && self.unpositioned.is_none()
    }

    /// The stream act calls that a saved run saves item by item, with the
    /// reason for each: the act does not say it gives its items again, or
    /// a stage after it could not be rebuilt from a later item. Calls in
    /// vines this one runs by `use` are named by the `use` stage's path,
    /// such as `pipe[2]>job_run:pipe[1]`. Every other stream act call is
    /// saved by position, and what the caller keeps for it stays bounded.
    pub fn saves_every_item(&self) -> &[SavesEveryItem] {
        &self.saves_every_item
    }

    fn refused(&self, why: &str) -> RunError {
        RunError::NotResumable {
            vine: self.name.clone(),
            why: why.to_owned(),
        }
    }

    /// Run the vine over a stream of items. What comes back is the stream
    /// of what it emits, each output with an id no other output of this run
    /// has. A run that saves nothing cannot be resumed, so its ids count
    /// the outputs of the whole run in the order they come, and do not
    /// match the ids a saved run gives. A failure no `catchError` handles is the
    /// last thing on the stream. Dropping the stream stops the run.
    pub fn run<S>(&self, input: S) -> BoxStream<'static, Result<PipeOutput, RunError>>
    where
        S: Stream<Item = Value> + Send + 'static,
    {
        self.drive(input, Saving::Not)
            .filter_map(|r| {
                ready(match r {
                    Ok(Progress::Output(out)) => Some(Ok(out)),
                    Ok(Progress::Save(_) | Progress::Through(_) | Progress::Done(_)) => None,
                    Err(e) => Some(Err(e)),
                })
            })
            .boxed()
    }

    /// Run the vine so that it can be resumed after a restart.
    ///
    /// Besides each [`Event::Output`], the run gives [`Event::Save`]
    /// entries. Pass each event to [`Journal::record`] and save the
    /// journal before you read the next. It appends the entry, and drops
    /// the entries a stream act saved by position no longer needs.
    /// After a restart, call `run_saved` again with the same input items,
    /// from the first, and the [`Journal`] of what you kept. An empty
    /// journal is a fresh start.
    ///
    /// The run hands out an output and then the [`Entry::Emitted`](crate::Entry::Emitted) that
    /// records it. If you save after every entry, no output is handed out
    /// twice. If the run stops after an output and before you saved the
    /// entry after it, that output comes again, with the same
    /// [`OutputId`]: drop it by its id. Ids are unique within this run and
    /// its restarts; join them with your own key for the run to keep the
    /// outputs of many runs apart.
    ///
    /// Under a `mergeMap`, an output whose line was not saved can
    /// come again after other values' new outputs, not only first.
    ///
    /// A vine with a `switchMap` or `exhaustMap`, or that counts or
    /// batches values in the order a `mergeMap` finishes them, cannot
    /// be resumed: the
    /// run gives one [`RunError::NotResumable`] and ends.
    /// [`Pipe::resumable`] says so before you run it.
    pub fn run_saved<S>(
        &self,
        input: S,
        journal: Journal,
    ) -> BoxStream<'static, Result<Event, RunError>>
    where
        S: Stream<Item = Value> + Send + 'static,
    {
        if let Some(why) = &self.unresumable {
            return stream::once(ready(Err(self.refused(why)))).boxed();
        }
        match self.book(Book::from_journal(&journal)) {
            Ok(book) => self
                .drive(input, Saving::Journal(book))
                .filter_map(|r| {
                    ready(match r {
                        Ok(Progress::Output(out)) => Some(Ok(Event::Output(out))),
                        Ok(Progress::Save(line)) => Some(Ok(Event::Save(line.entry))),
                        // A journal covers the whole run: there is no
                        // position to move.
                        Ok(Progress::Through(_) | Progress::Done(_)) => None,
                        Err(e) => Some(Err(e)),
                    })
                })
                .boxed(),
            Err(problem) => stream::once(ready(Err(RunError::ReplayMisfit { problem }))).boxed(),
        }
    }

    /// Run the vine over an endless stream so that it can carry on after a
    /// restart from a saved position.
    ///
    /// Feed it the input items after `bookmark`'s position: for a fresh
    /// start, [`Bookmark::new`] and every item from the first. The run
    /// numbers them on from there, and skips the items the bookmark keeps
    /// as done. Besides each [`Progress::Output`], it gives
    /// [`Progress::Save`] lines, [`Progress::Through`], and, under an
    /// `mergeMap`, [`Progress::Done`] for an item done before an
    /// earlier one. Pass each one to [`Bookmark::record`], and save the
    /// bookmark, before you read the next. After a restart, call `run_from` again with the bookmark
    /// you saved and the input items after its position.
    ///
    /// `Through(n)` says every input item through `n` is fully done: every
    /// act it caused finished, every output it caused was handed out, and
    /// no stage holds anything made from it. A stage that holds items, such
    /// as `bufferCount`, keeps the position before them. The bookmark keeps
    /// only the lines of the items past the position, so it stays as small
    /// as the work in flight.
    ///
    /// The items past the position replay from their lines, as in
    /// [`Pipe::run_saved`]: an act with a saved result is not called again,
    /// and an output already handed out is not handed out again. If the run
    /// stops after an output and before you saved the line after it, that
    /// output comes again, with the same [`OutputId`]. An id names the
    /// input item it belongs to, counted from the first item ever, so ids
    /// stay unique however many restarts the stream goes through.
    ///
    /// A vine that cannot replay in one order, or that keeps a count or a
    /// batch across items in a way a restart cannot rebuild, is refused: the
    /// run gives one [`RunError::NotResumable`] and ends.
    /// [`Pipe::resumable_from_position`] says so before you run it.
    pub fn run_from<S>(
        &self,
        input: S,
        bookmark: Bookmark,
    ) -> BoxStream<'static, Result<Progress, RunError>>
    where
        S: Stream<Item = Value> + Send + 'static,
    {
        if let Some(why) = self.unresumable.as_ref().or(self.unpositioned.as_ref()) {
            return stream::once(ready(Err(self.refused(why)))).boxed();
        }
        match self.book(Book::from_bookmark(&bookmark)) {
            Ok(book) => self.drive(
                input,
                Saving::Position {
                    book,
                    position: bookmark.position(),
                    done: bookmark.done().collect(),
                },
            ),
            Err(problem) => stream::once(ready(Err(RunError::ReplayMisfit { problem }))).boxed(),
        }
    }

    /// The book read from what the caller saved, once it is known to be
    /// saved for this vine as it is now, as far as the run can tell.
    fn book(&self, read: Result<Book, String>) -> Result<Book, String> {
        let book = read?;
        self.covered.check(book.keys())?;
        Ok(book)
    }

    /// Run as a stage of another vine.
    fn flow(&self, items: Items, cx: Cx) -> Items {
        let shape = self.input.clone();
        let checked = items.map(move |flow| match flow {
            Ok(Flowing::Value(v, tag)) => match shape.check(&v) {
                Ok(()) => Ok(Flowing::Value(v, tag)),
                Err(p) => Err(fatal(RunError::InputMisfit(p)).of(&tag)),
            },
            other => other,
        });
        (self.transform)(checked.boxed(), cx)
    }

    /// Feed the vine one item and collect everything it emits. A vine that
    /// must give exactly one answer says so itself, with `take: 1`.
    pub async fn run_one(&self, input: Value) -> Result<Vec<PipeOutput>, RunError> {
        let mut out = Vec::new();
        let mut results = self.run(stream::once(ready(input)));
        while let Some(r) = results.next().await {
            out.push(r?);
        }
        Ok(out)
    }
}
