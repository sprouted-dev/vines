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
mod checked;
mod describe;
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

pub(crate) use check::assemble;
pub use checked::{Pipe, SavesEveryItem};
pub use describe::{
    branch, buffer_count, call, catch_error, concat_map, emit, emit_with, exhaust_map, filter,
    finalize, fork, fork_join, last, merge_map, op, pipe, retry, split, switch_map, take, tap,
    timeout, use_vine, Flatten, Keep, Operator, PipeDef,
};
pub use output::{OutputId, PipeOutput};
#[cfg(feature = "yaml")]
pub(crate) use yaml::load;
