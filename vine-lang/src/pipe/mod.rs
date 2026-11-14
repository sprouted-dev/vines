//! One model: a vine is a stream in and a stream out.
//!
//! Items go in, flow through a pipe of operators, and come out on the
//! vine's named outputs. Every operator takes a stream and gives a stream.
//! A vine fed one item is a stream of one. The vine does not know what an
//! output becomes. It emits values, and whoever runs it decides what to do
//! with them.
//!
//! Build one in Rust with the functions in this module, then check it with
//! [`Registry::build_pipe`](crate::Registry::build_pipe). The YAML layer
//! writes the same operators down as text.
//!
//! ```
//! use vine_lang::pipe::{emit_with, pipe, split};
//! use vine_lang::{json, Registry, Shape, Ty};
//!
//! let def = pipe("spread_jobs")
//!     .input(Shape::new().required("plan", Ty::Id).required("jobs", Ty::List))
//!     .output("wanted", Shape::new().required("plan", Ty::Id).required("job", Ty::Any))
//!     .operators([
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
//! An operator sees the item reaching it as `$item`. An operator's result
//! replaces the item, unless it says `into: name`. Then the item is kept
//! and the result is added to it under that name. That is how a vine
//! carries what it has learned forward: there are no variables, only the
//! item.
//!
//! # The operators
//!
//! The built-in operators:
//!
//! - [`split`]: one item becomes one item per element of a list.
//! - [`filter`]: drop the items a condition does not hold for.
//! - [`take`]: let this many items through, then end the stream.
//! - [`last`]: when the stream ends, let only its final item through.
//! - [`buffer_count`] (`bufferCount`): gather items into batches of
//!   `bufferSize`, and give what is left as one more batch when the stream
//!   ends.
//! - [`fork_join`] (`forkJoin`): run several registered operators side by
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
//! - [`catch_error`], [`finalize`], [`retry`] and [`timeout`]: see below.
//!
//! The caller registers more by name (see [`crate::registry`]):
//!
//! - [`op`]: run a pure registered operator, one that touches nothing,
//!   such as `set`. YAML writes it by its name: `set: { ... }`.
//! - [`call`]: run a registered operator that touches something outside,
//!   such as `git.worktree_add`, and save its result.
//! - [`tap`]: the same, for its effect only. The item passes through
//!   unchanged.
//!
//! # Stream operators
//!
//! An operator registered with
//! [`Registry::register_stream`](crate::Registry::register_stream) gives a
//! stream. `call` on it turns one item into one item per value it gives.
//! With `into`, each is the item with the value added. It may end with an
//! end value, such as `{ status: finished }`. The `call` drops it, unless
//! it names a field for it with [`end`](Operator::end). Then one last item
//! comes after the others: the item as it reached the `call`, with the end
//! value added under that name and no `into` field. So a stream that gave
//! nothing still gives its end.
//!
//! A failure from a stream operator is an ordinary failure. The items it
//! gave before the failure have already gone on. A `catchError` can handle
//! it.
//!
//! # Fork
//!
//! [`fork`] sends each item down every side pipe and then on down the main
//! pipe, unchanged and in order. A side pipe sees the whole stream, so
//! [`buffer_count`] and [`last`] work in it. What a side pipe emits comes
//! out of the vine. A value that reaches the end of a side pipe without
//! being emitted is dropped. For each item, the side pipes run in the order
//! they are written, each until it wants the next item, and then the item
//! goes on down the main pipe.
//!
//! A failure in a side pipe is a failure of the item that caused it, as if
//! an operator of the main pipe had failed: a `catchError` after the fork
//! handles it, and the item's work stops. Without a `catchError` it ends
//! the stream.
//!
//! # Operators that apply to the ones before them
//!
//! Four operators do not change the item. They apply to the operators
//! before them in the pipe, as `catchError` and `finalize` do in RxJS, and
//! they work one item at a time:
//!
//! - [`catch_error`] (`catchError`): if one of them fails, run these
//!   operators instead. They get the item as it was when it failed, plus
//!   the failure as `$item.error`.
//! - [`finalize`]: run these operators afterwards, however the work ended.
//! - [`retry`]: if one of them fails, wait `delay` and run them again, up
//!   to `count` more times.
//! - [`timeout`]: fail the item if they take too long.
//!
//! One failing item does not stop the items after it, as long as a
//! `catchError` applies to the operator that failed. A failure nothing
//! catches ends the whole stream.
//!
//! `catchError` and `finalize` pass on what the operators before them give
//! as it comes. `retry` and `timeout` hold it until those operators are
//! done for the item.
//!
//! ## The end of the stream before them
//!
//! These four run the operators before them once for each item that
//! reaches them. So in there, "when the stream ends" means when that one
//! item's own stream ends: [`last`] gives the final line of one agent run,
//! not of every run. The checker refuses `last` and `bufferCount` where
//! they could only ever see one item: before one of the four, or inside a
//! mapping operator, `branch`, `catchError`, `finalize` or `use`, with no
//! operator before them that makes many items from one, such as `split` or
//! a stream operator.
//!
//! # Surviving a restart
//!
//! [`Pipe::run_saved`] runs a vine and hands the caller, besides what it
//! emits, [`Entry`](crate::Entry) values to save. After a restart the
//! caller runs the vine again over the same input with the [`Journal`] it
//! saved. The run replays: a `call` with a saved result is not made again,
//! a stream operator gives its saved items again and then carries on from
//! where it was, and an output the caller was already handed is not handed
//! out again. The operators rebuild what they held, such as a half full
//! batch, because the same items flow through them again. Pure operators
//! are not saved: they run again. See [`crate::journal`].
//!
//! Every output carries an [`OutputId`], made from where it stands in the
//! run: the input item, the group of work, and how many outputs that group
//! made before it. An output the caller was handed and whose line it had
//! not saved comes again after a restart with the same id, so the caller
//! drops repeats by id. Two outputs of one run never share an id, even
//! with the same name and payload. See `output.rs`.
//!
//! Replay needs each saved call's key to name the same call every time.
//! Keys count how many times an operator ran, so a replay must reach each
//! operator in the same order the first run did. In a `mergeMap`, values
//! run side by side and finish in any order, so each value that enters it
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
//! - A `call` is made only once the caller has been handed every line made
//!   so far, by any value. Calls already running finish in any order. After
//!   a crash, the calls made again are those that had started and whose
//!   result the caller had not saved: at most one per value running side
//!   by side (times the widest `forkJoin`), so at most `concurrent` with
//!   a cap, plus one result handed out and not saved.
//!
//! The checker refuses, for both ways to resume, what a replay in another
//! order could not rebuild: `take` or `bufferCount` after a `mergeMap`, a
//! `call` or `tap` on the value `last` picked after one, and a `catchError`
//! or `finalize` that applies to one. `switchMap` and `exhaustMap` depend
//! on time, not on order, and are refused too. The rules are in
//! `order.rs`.
//!
//! A `timeout` depends on time too, but what it decides is saved: each run
//! of the operators it applies to is a scope of its own, and the run saves
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
//! clone. A batch from `bufferCount` carries the tags of its items, and
//! ties them together so the position never stops partway through a
//! batch. An output's tag is dropped when the output is handed out. When
//! the last clone of an item's tag is gone, nothing holds anything made
//! from it.
//!
//! An operator that keeps something across items other than the items it
//! holds cannot be rebuilt by feeding the items after the position. So
//! `run_from` refuses, besides what `run_saved` refuses:
//!
//! - `take` over the whole stream: it counts, and the count would start
//!   again. A vine that ends itself has an end, so use `run_saved`.
//! - `bufferCount` over the whole stream after an operator that makes many
//!   items from one: a batch could end partway through an input item.
//!
//! [`Pipe::resumable_from_position`] says whether a vine can run this way.
//! `last` and `bufferCount` over whole items, `fork`, `catchError`,
//! `finalize`, `retry`, `timeout`, `concatMap`, `mergeMap`, `use`,
//! `branch` and stream operators all can.
//!
//! Under a `mergeMap`, an input item can be done before an
//! earlier one. The run gives [`Progress::Done`] for it, the caller's
//! bookmark keeps its number and drops its lines, and a run from that
//! bookmark skips it. The position still moves only past items in order.
//! So the bookmark holds the lines of the items in flight, at most
//! `concurrent` of them in the `mergeMap`, plus runs of numbers. Without
//! `concurrent` every item fed is in flight at once.
//!
//! # A stream operator that gives its items again
//!
//! A stream operator registered with
//! [`Registry::register_stream`](crate::Registry::register_stream) is
//! saved item by item: one line for each item it gives, and one for each
//! output made from them. An agent run that writes a line a second for a
//! day saves every line, twice over.
//!
//! One registered with
//! [`Registry::register_replayable_stream`](crate::Registry::register_replayable_stream)
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
//! is abandoned because a `catchError` or `finalize` took over. It saves
//! no item and no `Emitted` line for an output made from one.
//!
//! After a restart the operator is asked for its items from the one after
//! the position. Operators that hold items, such as `bufferCount`, refill
//! the way they do for input items. The outputs the caller was handed are
//! made again and not handed out, by count. The keys of calls made for
//! item `k` start with the call's key and `k`, such as
//! `pipe[1]#0@7/pipe[4]#0`, so they line up whichever item a restart
//! begins at. When the position passes `k`, the caller's
//! [`Journal::record`] or [`Bookmark::record`] drops those lines. So what
//! the caller keeps for a call is a few lines, however many items it
//! gives.
//!
//! The checker saves a call item by item, as if the operator did not say
//! it gives again, where a later item could not rebuild what comes after
//! it:
//!
//! - `take` that what the call gives reaches, in the same run of a pipe.
//!   It counts, and after a restart it would count from the position.
//! - `bufferCount` that could put the items of two calls, or of the call
//!   and of something else, in one batch. A batch must hold one call's
//!   items.
//! - `retry` applied to the call. It drops what a failed attempt gave and
//!   tries again, so an item it dropped would look done.
//!
//! [`Pipe::saves_every_item`] names each such call and why. A vine run by
//! `use` was checked on its own and cannot change how it saves, so a vine
//! that refuses what a used vine's call gives cannot be resumed.
//!
//! # What this module leaves out
//!
//! `finalize` does not run when the caller drops the stream partway: Rust
//! has no async drop. A run that is dropped and never resumed leaves
//! behind whatever its calls made. A journal only grows, except for the
//! lines a [`Journal::record`] drops for a stream operator saved by
//! position. A bookmark shrinks as the position moves. A stream operator
//! saved item by item keeps every line until its item is done.

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

use book::{Book, Cx, ReplayError};
use drive::Saving;
use flow::{fatal, Flowing, Items, Transform};
use timer::Timeouts;

pub(crate) use check::assemble;
pub use output::{OutputId, PipeOutput};
#[cfg(feature = "yaml")]
pub(crate) use yaml::load;

// ------------------------------------------------------------ description

/// Which of the four mapping operators an [`Operator::Mapped`] is: what
/// happens when a new item arrives while the inner pipe is still running
/// for an earlier one.
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
    /// The operator's key in YAML and in paths, such as `mergeMap`.
    pub fn key(self) -> &'static str {
        match self {
            Flatten::Concat => "concatMap",
            Flatten::Merge => "mergeMap",
            Flatten::Switch => "switchMap",
            Flatten::Exhaust => "exhaustMap",
        }
    }
}

/// What an operator's result does to the item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keep {
    /// The result is the new item.
    Replace,
    /// The item is kept, with the result added under this name.
    Into(String),
    /// The item is kept unchanged and the result is dropped.
    Tap,
}

/// One operator of a pipe, as described.
#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    /// A registered pure operator, written by its name, such as `set`.
    Named {
        name: String,
        with: Value,
        keep: Keep,
    },
    /// A registered operator that touches something outside, run by
    /// `call` or `tap`, such as `git.worktree_add`.
    Call {
        name: String,
        with: Value,
        keep: Keep,
        /// For a stream operator: the field its end value goes under, on
        /// one last item.
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
    /// Batches of `buffer_size`, and what is left when the stream ends.
    BufferCount {
        buffer_size: u64,
        into: Option<String>,
    },
    ForkJoin(Vec<Operator>),
    /// `concatMap`, `mergeMap`, `switchMap` or `exhaustMap`, as `flatten`
    /// says: run a pipe for each item. `concurrent` caps how many run side
    /// by side, and only `mergeMap` takes it.
    Mapped {
        flatten: Flatten,
        concurrent: Option<usize>,
        pipe: Vec<Operator>,
    },
    /// Send each item down every side pipe, and on down this one.
    Fork(Vec<Vec<Operator>>),
    /// Applies to the operators before it in its pipe: if one of them
    /// fails for an item, run these instead.
    CatchError(Vec<Operator>),
    /// Applies to the operators before it: run these afterwards, however
    /// the item's work ended.
    Finalize(Vec<Operator>),
    /// Applies to the operators before it: if one fails, wait `delay` and
    /// run them again, up to `count` more times.
    Retry {
        count: u32,
        delay: Duration,
    },
    /// Applies to the operators before it: fail if they take longer than
    /// this.
    Timeout(Duration),
    /// Run another vine for each item.
    Use {
        vine: String,
        with: Option<Value>,
        keep: Keep,
        on: Vec<(String, Vec<Operator>)>,
    },
    Branch {
        on: Value,
        cases: Vec<(String, Vec<Operator>)>,
        otherwise: Option<Vec<Operator>>,
    },
    Emit {
        to: String,
        with: Option<Value>,
    },
}

impl Operator {
    /// Keep the item and add this operator's result to it under `name`.
    /// For [`split`], each new item is the old one with the element added
    /// under `name`. For [`buffer_count`], the item kept is the last one of
    /// the batch, with the batch added under `name`.
    pub fn into(mut self, name: &str) -> Self {
        match &mut self {
            Operator::Named { keep, .. }
            | Operator::Call { keep, .. }
            | Operator::Use { keep, .. } => *keep = Keep::Into(name.to_owned()),
            Operator::Split { into, .. } | Operator::BufferCount { into, .. } => {
                *into = Some(name.to_owned())
            }
            _ => {}
        }
        self
    }

    /// For [`call`] on a stream operator: after the items, give one last
    /// item with the operator's end value added under `name`.
    pub fn end(mut self, name: &str) -> Self {
        if let Operator::Call { end, .. } = &mut self {
            *end = Some(name.to_owned());
        }
        self
    }

    /// For [`use_vine`]: the operators to run for what the other vine
    /// sends to this output. Every output of the other vine needs one.
    pub fn on(mut self, output: &str, operators: impl IntoIterator<Item = Operator>) -> Self {
        if let Operator::Use { on, .. } = &mut self {
            on.push((output.to_owned(), operators.into_iter().collect()));
        }
        self
    }

    /// For [`merge_map`]: run at most this many side by side. The checker
    /// refuses it on the other mapping operators.
    pub fn concurrent(mut self, limit: usize) -> Self {
        if let Operator::Mapped { concurrent, .. } = &mut self {
            *concurrent = Some(limit);
        }
        self
    }
}

/// Run a registered pure operator, such as `set`, for each item.
pub fn op(name: &str, with: Value) -> Operator {
    Operator::Named {
        name: name.to_owned(),
        with,
        keep: Keep::Replace,
    }
}

/// Run a registered operator that touches something outside, such as
/// `git.worktree_add`, for each item. On a stream operator, each item
/// becomes one item per value it gives.
pub fn call(name: &str, with: Value) -> Operator {
    Operator::Call {
        name: name.to_owned(),
        with,
        keep: Keep::Replace,
        end: None,
    }
}

/// Run a registered operator that touches something outside for its
/// effect. The item passes through unchanged. A stream operator cannot be
/// tapped: use [`call`].
pub fn tap(name: &str, with: Value) -> Operator {
    Operator::Call {
        name: name.to_owned(),
        with,
        keep: Keep::Tap,
        end: None,
    }
}

/// One item becomes one item per element of the list at `from`.
pub fn split(from: &str) -> Operator {
    Operator::Split {
        from: Value::String(from.to_owned()),
        into: None,
    }
}

/// Drop the items the condition does not hold for.
pub fn filter(when: &str) -> Operator {
    Operator::Filter {
        when: Value::String(when.to_owned()),
    }
}

/// Let this many items through, then end the stream.
pub fn take(count: u64) -> Operator {
    Operator::Take(count)
}

/// When the stream ends, let only its final item through. An empty stream
/// gives nothing.
pub fn last() -> Operator {
    Operator::Last
}

/// Gather items into batches of `buffer_size`. A batch is
/// `{ items, count }`. When the stream ends, what is left comes as one
/// more, smaller batch. An empty batch is never given. With
/// [`into`](Operator::into), the item is the last one of the batch with
/// the batch added under that name.
pub fn buffer_count(buffer_size: u64) -> Operator {
    Operator::BufferCount {
        buffer_size,
        into: None,
    }
}

/// Run these registered operators side by side for one item, and go on
/// when all of them are done. Each one says where its result goes with
/// `into`.
pub fn fork_join(operators: impl IntoIterator<Item = Operator>) -> Operator {
    Operator::ForkJoin(operators.into_iter().collect())
}

fn mapped(flatten: Flatten, pipe: impl IntoIterator<Item = Operator>) -> Operator {
    Operator::Mapped {
        flatten,
        concurrent: None,
        pipe: pipe.into_iter().collect(),
    }
}

/// Run a pipe for each item, one item at a time, in order.
pub fn concat_map(pipe: impl IntoIterator<Item = Operator>) -> Operator {
    mapped(Flatten::Concat, pipe)
}

/// Run a pipe for each item, side by side. What it gives comes out as it
/// finishes. Cap how many run at once with
/// [`concurrent`](Operator::concurrent).
pub fn merge_map(pipe: impl IntoIterator<Item = Operator>) -> Operator {
    mapped(Flatten::Merge, pipe)
}

/// Run a pipe for each item. A new item drops the run for the one before.
pub fn switch_map(pipe: impl IntoIterator<Item = Operator>) -> Operator {
    mapped(Flatten::Switch, pipe)
}

/// Run a pipe for each item. An item that arrives while a run is going is
/// ignored.
pub fn exhaust_map(pipe: impl IntoIterator<Item = Operator>) -> Operator {
    mapped(Flatten::Exhaust, pipe)
}

/// Send each item down every one of these side pipes, and on down this
/// pipe unchanged. See the module docs.
pub fn fork<P>(pipes: impl IntoIterator<Item = P>) -> Operator
where
    P: IntoIterator<Item = Operator>,
{
    Operator::Fork(pipes.into_iter().map(|p| p.into_iter().collect()).collect())
}

/// If an operator before this one fails for an item, run these instead.
/// They get the item as it was when it failed, with the failure added as
/// `$item.error`: its `code`, its `message`, and `from`, the name of the
/// operator that failed.
pub fn catch_error(operators: impl IntoIterator<Item = Operator>) -> Operator {
    Operator::CatchError(operators.into_iter().collect())
}

/// Run these after the operators before this one, however the item's work
/// ended. They get the item as it was when it arrived. They are for
/// cleanup: what they give back is dropped, and they may not emit.
pub fn finalize(operators: impl IntoIterator<Item = Operator>) -> Operator {
    Operator::Finalize(operators.into_iter().collect())
}

/// If an operator before this one fails for an item, wait `delay` and run
/// them again, up to `count` more times. Every operator among them that
/// touches something outside must be safe to repeat.
pub fn retry(count: u32, delay: Duration) -> Operator {
    Operator::Retry { count, delay }
}

/// Fail an item with the code `timeout` if the operators before this one
/// take longer than `limit`.
pub fn timeout(limit: Duration) -> Operator {
    Operator::Timeout(limit)
}

/// Run another vine for each item. `with` builds its input. With no
/// `with`, the item is its input. Add an [`on`](Operator::on) for each of
/// its outputs.
pub fn use_vine(vine: &str, with: Option<Value>) -> Operator {
    Operator::Use {
        vine: vine.to_owned(),
        with,
        keep: Keep::Replace,
        on: Vec::new(),
    }
}

/// Run the pipe whose key equals the value at `on`. With no `otherwise`,
/// an item that matches no case passes through unchanged.
pub fn branch<'a, C, P>(on: &str, cases: C, otherwise: Option<Vec<Operator>>) -> Operator
where
    C: IntoIterator<Item = (&'a str, P)>,
    P: IntoIterator<Item = Operator>,
{
    Operator::Branch {
        on: Value::String(on.to_owned()),
        cases: cases
            .into_iter()
            .map(|(k, p)| (k.to_owned(), p.into_iter().collect()))
            .collect(),
        otherwise,
    }
}

/// Send the item, as it is, to one of the vine's outputs.
pub fn emit(to: &str) -> Operator {
    Operator::Emit {
        to: to.to_owned(),
        with: None,
    }
}

/// Send this value to one of the vine's outputs.
pub fn emit_with(to: &str, with: Value) -> Operator {
    Operator::Emit {
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
    pub(crate) operators: Vec<Operator>,
}

/// Start describing a vine.
pub fn pipe(name: &str) -> PipeDef {
    PipeDef {
        name: name.to_owned(),
        input: Shape::new(),
        outputs: BTreeMap::new(),
        operators: Vec::new(),
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

    /// The operators, first to last.
    pub fn operators(mut self, operators: impl IntoIterator<Item = Operator>) -> Self {
        self.operators = operators.into_iter().collect();
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
    /// The operators its `call` and `tap` name, here and in the vines it
    /// uses.
    calls: BTreeSet<String>,
    /// The vines it runs with `use`, and the vines those use.
    uses: BTreeSet<String>,
    /// Why the vine cannot be resumed, if it cannot.
    unresumable: Option<String>,
    /// Why the vine cannot run from a saved position, if it cannot.
    unpositioned: Option<String>,
    /// The stream operator calls saved item by item, and why.
    saves_every_item: Vec<SavesEveryItem>,
    /// Some stream operator call, here or in a vine this one uses, is
    /// saved by position.
    by_position: bool,
    /// It holds a `mergeMap`, here or in a vine it uses.
    merges: bool,
    /// Which saved operators each `timeout` applies to, to refuse a
    /// journal saved before one was added.
    timeouts: Timeouts,
    transform: Transform,
}

/// A stream operator call that a saved run saves item by item, so what
/// the caller keeps grows with every item it gives, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavesEveryItem {
    /// The path of the `call`, such as `pipe[1]`.
    pub path: String,
    /// The stream operator it calls.
    pub operator: String,
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

    /// Everything this vine's registered operators touch, sorted.
    pub fn reach(&self) -> &BTreeSet<String> {
        &self.reach
    }

    /// Every registered operator this vine runs with `call` or `tap`, here
    /// or in a vine it runs with `use`, sorted. These are the ones that
    /// touch something outside; pure operators are not listed.
    pub fn calls(&self) -> &BTreeSet<String> {
        &self.calls
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

    /// The stream operator calls that a saved run saves item by item, with
    /// the reason for each: the operator does not say it gives its items
    /// again, or an operator after it could not be rebuilt from a later
    /// item. Calls in vines this one runs by `use` are named by the path of
    /// the `use`, such as `pipe[2]>job_run:pipe[1]`. Every other stream
    /// operator call is saved by position, and what the caller keeps for it
    /// stays bounded.
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
    /// the entries a stream operator saved by position no longer needs.
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
            Err(misfit) => stream::once(ready(Err(RunError::ReplayMisfit {
                problem: misfit.to_string(),
            })))
            .boxed(),
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
    /// call it caused finished, every output it caused was handed out, and
    /// no operator holds anything made from it. An operator that holds
    /// items, such as `bufferCount`, keeps the position before them. The bookmark keeps
    /// only the lines of the items past the position, so it stays as small
    /// as the work in flight.
    ///
    /// The items past the position replay from their lines, as in
    /// [`Pipe::run_saved`]: a call with a saved result is not made again,
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
            Err(misfit) => stream::once(ready(Err(RunError::ReplayMisfit {
                problem: misfit.to_string(),
            })))
            .boxed(),
        }
    }

    /// The book read from what the caller saved, once it is known to be
    /// saved for this vine as it is now, as far as the run can tell.
    fn book(&self, read: Result<Book, ReplayError>) -> Result<Book, ReplayError> {
        let book = read?;
        self.timeouts.check(book.keys())?;
        Ok(book)
    }

    /// Run as an operator of another vine.
    fn flow(&self, items: Items, cx: Cx) -> Items {
        let shape = self.input.clone();
        let checked = items.map(move |flow| match flow {
            Ok(Flowing::Value(v, tag)) => match shape.check(&v) {
                Ok(()) => Ok(Flowing::Value(v, tag)),
                Err(p) => Err(fatal(RunError::InputMisfit(p.to_string())).of(&tag)),
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
