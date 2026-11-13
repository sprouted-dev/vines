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

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Poll, Waker};
use std::time::Duration;

use futures::future::{join_all, ready, BoxFuture};
use futures::stream::{self, BoxStream};
use futures::{FutureExt, Stream, StreamExt};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::error::{LoadError, RunError};
use crate::expr::{case_key, is_name, Cond, Env, Ref, Template};
use crate::journal::{Entry, Event, Journal};
use crate::registry::{
    ActStream, Handler, OpContext, OpError, OperatorSpec, Part, Registry, StreamHandler,
};
use crate::run::Output;
use crate::shape::{Shape, Ty};

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

// ---------------------------------------------------------------- running

/// What flows between stages: a value still being worked on, or a value
/// already sent to an output. Later stages leave an output alone.
#[derive(Debug, Clone)]
enum Flowing {
    Value(Value),
    Out(Output),
}

enum Fault {
    /// An operator or act failed.
    Op(Box<OpFault>),
    Fatal(Box<RunError>),
}

struct OpFault {
    path: String,
    from: String,
    error: OpError,
    /// The item as it was when the stage failed.
    item: Value,
}

fn fatal(e: RunError) -> Fault {
    Fault::Fatal(Box::new(e))
}

fn misfit(problem: impl Into<String>) -> Fault {
    fatal(RunError::ReplayMisfit {
        problem: problem.into(),
    })
}

type Flow = Result<Flowing, Fault>;
type Items = BoxStream<'static, Flow>;
type Transform = Arc<dyn Fn(Items, Cx) -> Items + Send + Sync>;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// What a saved run remembers from the journal it was given, and what it
/// has yet to hand the caller.
#[derive(Default)]
struct Book {
    /// Saved results, by key. Taken out as the replay uses them.
    saved: HashMap<String, Saved>,
    /// How many times each stage has run so far in this run.
    counts: HashMap<String, u64>,
    /// New entries for the caller to save.
    fresh: VecDeque<Entry>,
    /// The outputs the caller was already handed, oldest first.
    handed: VecDeque<String>,
}

enum Saved {
    Once {
        input: Value,
        outcome: Result<Value, OpError>,
    },
    Stream {
        input: Value,
        items: VecDeque<Value>,
        end: Option<Result<Value, OpError>>,
    },
}

impl Book {
    fn from_journal(journal: &Journal) -> Result<Book, String> {
        let mut book = Book::default();
        for (i, entry) in journal.entries().iter().enumerate() {
            let problem = |what: &str| format!("entry {i}: {what}");
            match entry {
                Entry::Done { at, input, result } => {
                    if book.saved.contains_key(at) {
                        return Err(problem(&format!("`{at}` is saved twice")));
                    }
                    book.saved.insert(
                        at.clone(),
                        Saved::Once {
                            input: input.clone(),
                            outcome: Ok(result.clone()),
                        },
                    );
                }
                Entry::Failed {
                    at,
                    input,
                    code,
                    message,
                } => {
                    let failure = OpError::new(code.clone(), message.clone());
                    match book.saved.get_mut(at) {
                        Some(Saved::Stream {
                            end: end @ None, ..
                        }) => *end = Some(Err(failure)),
                        Some(_) => return Err(problem(&format!("`{at}` is saved twice"))),
                        None => {
                            book.saved.insert(
                                at.clone(),
                                Saved::Once {
                                    input: input.clone(),
                                    outcome: Err(failure),
                                },
                            );
                        }
                    }
                }
                Entry::Started { at, input } => {
                    if book.saved.contains_key(at) {
                        return Err(problem(&format!("`{at}` is saved twice")));
                    }
                    book.saved.insert(
                        at.clone(),
                        Saved::Stream {
                            input: input.clone(),
                            items: VecDeque::new(),
                            end: None,
                        },
                    );
                }
                Entry::Item { at, value } => match book.saved.get_mut(at) {
                    Some(Saved::Stream {
                        items, end: None, ..
                    }) => items.push_back(value.clone()),
                    _ => {
                        return Err(problem(&format!(
                            "an item for `{at}`, which is not a stream that is still going"
                        )))
                    }
                },
                Entry::Ended { at, value } => match book.saved.get_mut(at) {
                    Some(Saved::Stream {
                        end: end @ None, ..
                    }) => *end = Some(Ok(value.clone())),
                    _ => {
                        return Err(problem(&format!(
                            "an end for `{at}`, which is not a stream that is still going"
                        )))
                    }
                },
                Entry::Emitted { output } => book.handed.push_back(output.clone()),
            }
        }
        Ok(book)
    }
}

/// What every stage of one run shares: the book, when the run is saved,
/// and the scope its keys live in.
#[derive(Clone, Default)]
struct Cx {
    book: Option<Arc<Mutex<Book>>>,
    scope: Arc<str>,
}

impl Cx {
    /// The key for this run of the stage at `path`, when the run is saved.
    fn key(&self, path: &str) -> Option<String> {
        let book = self.book.as_ref()?;
        let mut book = lock(book);
        let at = format!("{}{path}", self.scope);
        let count = book.counts.entry(at.clone()).or_insert(0);
        let key = format!("{at}#{count}");
        *count += 1;
        Some(key)
    }

    fn take(&self, key: &str) -> Option<Saved> {
        lock(self.book.as_ref()?).saved.remove(key)
    }

    fn save(&self, entry: Entry) {
        if let Some(book) = &self.book {
            lock(book).fresh.push_back(entry);
        }
    }

    /// The scope for a vine run by `use` at `path`.
    fn within(&self, path: &str, vine: &str) -> Cx {
        Cx {
            book: self.book.clone(),
            scope: format!("{}{path}>{vine}:", self.scope).into(),
        }
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

    fn drive<S>(
        &self,
        input: S,
        book: Option<Arc<Mutex<Book>>>,
    ) -> BoxStream<'static, Result<Event, RunError>>
    where
        S: Stream<Item = Value> + Send + 'static,
    {
        let shape = self.input.clone();
        let checked = input.map(move |v| match shape.check(&v) {
            Ok(()) => Ok(Flowing::Value(v)),
            Err(p) => Err(fatal(RunError::InputMisfit(p))),
        });
        let cx = Cx {
            book: book.clone(),
            scope: "".into(),
        };
        let mut inner = (self.transform)(checked.boxed(), cx);
        let mut held: Option<Flow> = None;
        let (mut ended, mut stopped) = (false, false);
        stream::poll_fn(move |task| {
            if stopped {
                return Poll::Ready(None);
            }
            loop {
                // Hand out what to save as soon as it is known.
                if let Some(entry) = book.as_ref().and_then(|b| lock(b).fresh.pop_front()) {
                    return Poll::Ready(Some(Ok(Event::Save(entry))));
                }
                let flow = match held.take() {
                    Some(flow) => flow,
                    None if ended => {
                        let left = book.as_ref().map_or(0, |b| lock(b).handed.len());
                        stopped = true;
                        if left > 0 {
                            return Poll::Ready(Some(Err(RunError::ReplayMisfit {
                                problem: format!(
                                    "the journal says {left} more outputs were handed out, and the run ended without making them"
                                ),
                            })));
                        }
                        return Poll::Ready(None);
                    }
                    None => match inner.poll_next_unpin(task) {
                        Poll::Ready(Some(flow)) => {
                            held = Some(flow);
                            continue;
                        }
                        Poll::Ready(None) => {
                            ended = true;
                            continue;
                        }
                        Poll::Pending => {
                            if book.as_ref().is_some_and(|b| !lock(b).fresh.is_empty()) {
                                continue;
                            }
                            return Poll::Pending;
                        }
                    },
                };
                match flow {
                    Ok(Flowing::Out(out)) => {
                        if let Some(b) = &book {
                            let mut b = lock(b);
                            if let Some(name) = b.handed.pop_front() {
                                if name != out.name {
                                    stopped = true;
                                    return Poll::Ready(Some(Err(RunError::ReplayMisfit {
                                        problem: format!(
                                            "the journal says output `{name}` was handed out next, and the run made `{}`",
                                            out.name
                                        ),
                                    })));
                                }
                                // Handed out before the restart.
                                continue;
                            }
                            b.fresh.push_back(Entry::Emitted {
                                output: out.name.clone(),
                            });
                        }
                        return Poll::Ready(Some(Ok(Event::Output(out))));
                    }
                    // The checker refuses a pipe with a path that does not emit.
                    Ok(Flowing::Value(_)) => {
                        stopped = true;
                        return Poll::Ready(Some(Err(RunError::ItemMisfit {
                            path: "pipe".into(),
                            problem: "an item reached the end of the pipe without being emitted"
                                .into(),
                        })));
                    }
                    // A failure ends the stream.
                    Err(fault) => {
                        stopped = true;
                        return Poll::Ready(Some(Err(run_error(fault))));
                    }
                }
            }
        })
        .boxed()
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

fn run_error(fault: Fault) -> RunError {
    match fault {
        Fault::Fatal(e) => *e,
        Fault::Op(fault) => {
            let OpFault {
                path, from, error, ..
            } = *fault;
            if error.code == OpError::CANCELLED {
                RunError::Cancelled { path }
            } else {
                RunError::Unhandled {
                    path,
                    operator: from,
                    code: error.code,
                    message: error.message,
                }
            }
        }
    }
}

fn env_of(item: &Value) -> Env {
    let mut env = Env::new();
    env.insert("item".into(), item.clone());
    env
}

fn one(flow: Flow) -> Items {
    stream::once(ready(flow)).boxed()
}

fn chain(transforms: Vec<Transform>) -> Transform {
    Arc::new(move |items: Items, cx: Cx| {
        transforms
            .iter()
            .fold(items, |items, t| t(items, cx.clone()))
    })
}

/// How a unit runs.
#[derive(Clone)]
enum Runner {
    /// An operator. It reaches nothing outside, so it is not saved.
    Op(Handler),
    /// An act. Its result is saved.
    Act(Handler),
    /// A stream act. Its items and its end are saved.
    Stream(StreamHandler),
}

/// An operator or act, ready to run for one item.
#[derive(Clone)]
struct Unit {
    path: String,
    spec: Arc<OperatorSpec>,
    runner: Runner,
    with: Template,
    keep: Keep,
    end: Option<String>,
}

impl Unit {
    fn resolve(&self, item: &Value) -> Result<Value, Fault> {
        let resolved = self.with.resolve(&env_of(item));
        self.spec.settings.check(&resolved).map_err(|problem| {
            fatal(RunError::SettingsMisfit {
                path: self.path.clone(),
                operator: self.spec.name.clone(),
                problem,
            })
        })?;
        Ok(resolved)
    }

    fn failed(&self, error: OpError, item: &Value) -> Fault {
        Fault::Op(Box::new(OpFault {
            path: self.path.clone(),
            from: self.spec.name.clone(),
            error,
            item: item.clone(),
        }))
    }

    /// Run for one item and give back the raw result.
    fn result(&self, item: Value, cx: Cx) -> BoxFuture<'static, Result<Value, Fault>> {
        let unit = self.clone();
        async move {
            let resolved = unit.resolve(&item)?;
            let (handler, saved) = match &unit.runner {
                Runner::Op(h) => (h.clone(), false),
                Runner::Act(h) => (h.clone(), true),
                Runner::Stream(_) => unreachable!("a stream act runs through `stream`"),
            };
            let key = if saved { cx.key(&unit.path) } else { None };
            if let Some(key) = &key {
                match cx.take(key) {
                    None => {}
                    Some(Saved::Once { input, outcome }) => {
                        if input != resolved {
                            return Err(misfit(format!(
                                "`{key}` was saved for input {input}, and the run gave it {resolved}"
                            )));
                        }
                        return outcome.map_err(|e| unit.failed(e, &item));
                    }
                    Some(Saved::Stream { .. }) => {
                        return Err(misfit(format!(
                            "`{key}` was saved as a stream act, and `{}` is not one",
                            unit.spec.name
                        )))
                    }
                }
            }
            let cx_op = OpContext::new(CancellationToken::new());
            let result = match handler(resolved.clone(), cx_op).await {
                Ok(v) => v,
                Err(error) => {
                    if let Some(at) = key {
                        if error.code != OpError::CANCELLED {
                            cx.save(Entry::Failed {
                                at,
                                input: resolved,
                                code: error.code.clone(),
                                message: error.message.clone(),
                            });
                        }
                    }
                    return Err(unit.failed(error, &item));
                }
            };
            if let Some(shape) = &unit.spec.returns {
                shape.check(&result).map_err(|problem| {
                    fatal(RunError::ResultMisfit {
                        path: unit.path.clone(),
                        operator: unit.spec.name.clone(),
                        problem,
                    })
                })?;
            }
            if let Some(at) = key {
                cx.save(Entry::Done {
                    at,
                    input: resolved,
                    result: result.clone(),
                });
            }
            Ok(result)
        }
        .boxed()
    }

    /// Run for one item and give back the item that flows on.
    fn apply(&self, item: Value, cx: Cx) -> BoxFuture<'static, Result<Value, Fault>> {
        let unit = self.clone();
        async move {
            let result = unit.result(item.clone(), cx).await?;
            unit.merge(item, result)
        }
        .boxed()
    }

    fn merge(&self, item: Value, result: Value) -> Result<Value, Fault> {
        match &self.keep {
            Keep::Replace => Ok(result),
            Keep::Tap => Ok(item),
            Keep::Into(name) => add_field(&self.path, item, name, result),
        }
    }

    /// Run a stream act for one item: one item out per value it gives.
    fn stream(&self, item: Value, cx: Cx) -> Items {
        let Runner::Stream(handler) = &self.runner else {
            unreachable!("only a stream act runs as a stream")
        };
        let input = match self.resolve(&item) {
            Ok(v) => v,
            Err(fault) => return one(Err(fault)),
        };
        let key = cx.key(&self.path);
        let mut run = StreamRun {
            unit: self.clone(),
            handler: handler.clone(),
            item,
            input,
            cx: cx.clone(),
            key: key.clone(),
            replay: VecDeque::new(),
            replay_end: None,
            started: false,
            live: None,
            given: 0,
            done: false,
        };
        if let Some(key) = &key {
            match cx.take(key) {
                None => {}
                Some(Saved::Stream { input, items, end }) => {
                    if input != run.input {
                        return one(Err(misfit(format!(
                            "`{key}` was saved for input {input}, and the run gave it {}",
                            run.input
                        ))));
                    }
                    run.replay = items;
                    run.replay_end = end;
                    run.started = true;
                }
                Some(Saved::Once { .. }) => {
                    return one(Err(misfit(format!(
                    "`{key}` was saved as an act that gives one result, and `{}` is a stream act",
                    self.spec.name
                ))))
                }
            }
        }
        stream::unfold(run, StreamRun::next).boxed()
    }
}

/// One call of a stream act, as it goes: first what was saved, then live.
struct StreamRun {
    unit: Unit,
    handler: StreamHandler,
    /// The item as it reached the stage.
    item: Value,
    input: Value,
    cx: Cx,
    key: Option<String>,
    replay: VecDeque<Value>,
    replay_end: Option<Result<Value, OpError>>,
    started: bool,
    live: Option<ActStream>,
    /// How many items this call has given, saved ones included.
    given: u64,
    done: bool,
}

impl StreamRun {
    async fn next(mut self) -> Option<(Flow, StreamRun)> {
        if self.done {
            return None;
        }
        if let Some(value) = self.replay.pop_front() {
            self.given += 1;
            let flow = self.item_with(value);
            return Some((flow, self));
        }
        if let Some(end) = self.replay_end.take() {
            return self.finish(end);
        }
        if self.live.is_none() {
            if !self.started {
                if let Some(at) = &self.key {
                    self.cx.save(Entry::Started {
                        at: at.clone(),
                        input: self.input.clone(),
                    });
                }
                self.started = true;
            }
            let cx = OpContext::new(CancellationToken::new()).resuming(self.given);
            self.live = Some((self.handler)(self.input.clone(), cx));
        }
        let next = self.live.as_mut()?.next().await;
        let spec = self.unit.spec.clone();
        let path = self.unit.path.clone();
        match next {
            Some(Ok(Part::Item(value))) => {
                if let Some(shape) = &spec.returns {
                    if let Err(problem) = shape.check(&value) {
                        return self.fail_hard(RunError::ResultMisfit {
                            path,
                            operator: spec.name.clone(),
                            problem,
                        });
                    }
                }
                if let Some(at) = &self.key {
                    self.cx.save(Entry::Item {
                        at: at.clone(),
                        value: value.clone(),
                    });
                }
                self.given += 1;
                let flow = self.item_with(value);
                Some((flow, self))
            }
            Some(Ok(Part::End(value))) => {
                if let Some(shape) = &spec.ends {
                    if let Err(problem) = shape.check(&value) {
                        return self.fail_hard(RunError::ResultMisfit {
                            path,
                            operator: spec.name.clone(),
                            problem: format!("its end: {problem}"),
                        });
                    }
                }
                self.ended(value)
            }
            Some(Err(error)) => {
                if let Some(at) = &self.key {
                    if error.code != OpError::CANCELLED {
                        self.cx.save(Entry::Failed {
                            at: at.clone(),
                            input: self.input.clone(),
                            code: error.code.clone(),
                            message: error.message.clone(),
                        });
                    }
                }
                self.finish(Err(error))
            }
            None => {
                if spec.ends.is_some() {
                    return self.fail_hard(RunError::ResultMisfit {
                        path,
                        operator: spec.name.clone(),
                        problem: "its stream stopped without giving the end it declares".into(),
                    });
                }
                self.ended(Value::Null)
            }
        }
    }

    fn ended(mut self, value: Value) -> Option<(Flow, StreamRun)> {
        if let Some(at) = &self.key {
            self.cx.save(Entry::Ended {
                at: at.clone(),
                value: value.clone(),
            });
        }
        self.live = None;
        self.finish(Ok(value))
    }

    fn finish(mut self, end: Result<Value, OpError>) -> Option<(Flow, StreamRun)> {
        self.done = true;
        self.live = None;
        match end {
            Ok(value) => {
                let name = self.unit.end.clone()?;
                let flow =
                    add_field(&self.unit.path, self.item.clone(), &name, value).map(Flowing::Value);
                Some((flow, self))
            }
            Err(error) => {
                let fault = self.unit.failed(error, &self.item);
                Some((Err(fault), self))
            }
        }
    }

    fn fail_hard(mut self, e: RunError) -> Option<(Flow, StreamRun)> {
        self.done = true;
        self.live = None;
        Some((Err(fatal(e)), self))
    }

    fn item_with(&self, value: Value) -> Flow {
        match &self.unit.keep {
            Keep::Into(name) => {
                add_field(&self.unit.path, self.item.clone(), name, value).map(Flowing::Value)
            }
            _ => Ok(Flowing::Value(value)),
        }
    }
}

fn add_field(path: &str, mut item: Value, name: &str, value: Value) -> Result<Value, Fault> {
    match item.as_object_mut() {
        Some(map) => {
            map.insert(name.to_owned(), value);
            Ok(item)
        }
        None => Err(fatal(RunError::ItemMisfit {
            path: path.into(),
            problem: format!("`into: {name}` adds a field to the item, but the item is not a map"),
        })),
    }
}

/// Apply `f` to each value still being worked on. Outputs and failures
/// pass through.
fn per_item<F>(f: F) -> Transform
where
    F: Fn(Value, Cx) -> BoxFuture<'static, Result<Value, Fault>> + Send + Sync + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |items: Items, cx: Cx| {
        let f = f.clone();
        items
            .then(move |flow| {
                let (f, cx) = (f.clone(), cx.clone());
                async move {
                    match flow {
                        Ok(Flowing::Value(v)) => f(v, cx).await.map(Flowing::Value),
                        other => other,
                    }
                }
            })
            .boxed()
    })
}

/// Turn each value still being worked on into a stream, one after the
/// other. Outputs and failures pass through.
fn per_item_stream<F>(f: F) -> Transform
where
    F: Fn(Value, Cx) -> Items + Send + Sync + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |items: Items, cx: Cx| {
        let f = f.clone();
        items
            .map(move |flow| match flow {
                Ok(Flowing::Value(v)) => f(v, cx.clone()),
                other => one(other),
            })
            .flatten()
            .boxed()
    })
}

/// Each new value starts the pipe inside for itself. `Switch` drops the
/// run in flight for the new one. `Exhaust` ignores the new one while a
/// run is in flight. Outputs and failures from upstream are never dropped.
fn latest_or_first(outer: Items, inner: Transform, switch: bool, cx: Cx) -> Items {
    let mut outer = Some(outer);
    let mut current: Option<Items> = None;
    let mut passing: VecDeque<Flow> = VecDeque::new();
    stream::poll_fn(move |task| {
        if let Some(upstream) = outer.as_mut() {
            loop {
                match upstream.poll_next_unpin(task) {
                    Poll::Ready(Some(Ok(Flowing::Value(v)))) => {
                        if switch || current.is_none() {
                            current = Some(inner(one(Ok(Flowing::Value(v))), cx.clone()));
                        }
                    }
                    Poll::Ready(Some(other)) => passing.push_back(other),
                    Poll::Ready(None) => {
                        outer = None;
                        break;
                    }
                    Poll::Pending => break,
                }
            }
        }
        if let Some(flow) = passing.pop_front() {
            return Poll::Ready(Some(flow));
        }
        if let Some(run) = current.as_mut() {
            match run.poll_next_unpin(task) {
                Poll::Ready(Some(flow)) => return Poll::Ready(Some(flow)),
                Poll::Ready(None) => current = None,
                Poll::Pending => return Poll::Pending,
            }
        }
        if outer.is_none() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    })
    .boxed()
}

/// When the stream ends, give its final value.
fn last_of(mut items: Items) -> Items {
    let mut held: Option<Value> = None;
    let mut done = false;
    stream::poll_fn(move |task| loop {
        if done {
            return Poll::Ready(None);
        }
        match items.poll_next_unpin(task) {
            Poll::Ready(Some(Ok(Flowing::Value(v)))) => held = Some(v),
            Poll::Ready(Some(other)) => return Poll::Ready(Some(other)),
            Poll::Ready(None) => {
                done = true;
                return Poll::Ready(held.take().map(|v| Ok(Flowing::Value(v))));
            }
            Poll::Pending => return Poll::Pending,
        }
    })
    .boxed()
}

/// Batches of `size`, and what is left when the stream ends.
fn batches(mut items: Items, size: usize, into: Option<String>, path: String) -> Items {
    let mut buf: Vec<Value> = Vec::new();
    let mut done = false;
    let batch = move |items: Vec<Value>| -> Flow {
        let count = items.len();
        match &into {
            None => Ok(Flowing::Value(json!({ "items": items, "count": count }))),
            Some(name) => {
                let last = items.last().cloned().unwrap_or(Value::Null);
                add_field(&path, last, name, json!({ "items": items, "count": count }))
                    .map(Flowing::Value)
            }
        }
    };
    stream::poll_fn(move |task| loop {
        if done {
            return Poll::Ready(None);
        }
        match items.poll_next_unpin(task) {
            Poll::Ready(Some(Ok(Flowing::Value(v)))) => {
                buf.push(v);
                if buf.len() >= size {
                    return Poll::Ready(Some(batch(std::mem::take(&mut buf))));
                }
            }
            Poll::Ready(Some(other)) => return Poll::Ready(Some(other)),
            Poll::Ready(None) => {
                done = true;
                if buf.is_empty() {
                    return Poll::Ready(None);
                }
                return Poll::Ready(Some(batch(std::mem::take(&mut buf))));
            }
            Poll::Pending => return Poll::Pending,
        }
    })
    .boxed()
}

/// What a side pipe of a fork reads from.
#[derive(Default)]
struct SideIn {
    queue: VecDeque<Flow>,
    closed: bool,
    /// The side pipe asked for its next item and there was none.
    starved: bool,
    waker: Option<Waker>,
}

impl SideIn {
    fn wake(&mut self) {
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }
}

fn side_input(shared: Arc<Mutex<SideIn>>) -> Items {
    stream::poll_fn(move |task| {
        let mut side = lock(&shared);
        if let Some(flow) = side.queue.pop_front() {
            return Poll::Ready(Some(flow));
        }
        if side.closed {
            return Poll::Ready(None);
        }
        side.starved = true;
        side.waker = Some(task.waker().clone());
        Poll::Pending
    })
    .boxed()
}

/// Send each value down every side pipe, then on down the main pipe. For
/// each value the side pipes run in order, each until it wants the next
/// value, so what comes out does not depend on timing.
fn forked(mut upstream: Items, sides: Vec<Transform>, cx: Cx) -> Items {
    let inputs: Vec<Arc<Mutex<SideIn>>> = sides
        .iter()
        .map(|_| Arc::new(Mutex::new(SideIn::default())))
        .collect();
    let mut outs: Vec<Option<Items>> = sides
        .iter()
        .zip(&inputs)
        .map(|(side, input)| Some(side(side_input(input.clone()), cx.clone())))
        .collect();
    let mut ready_out: VecDeque<Flow> = VecDeque::new();
    let mut held: Option<Flow> = None;
    let mut draining: Option<usize> = None;
    let mut upstream_done = false;
    stream::poll_fn(move |task| loop {
        if let Some(flow) = ready_out.pop_front() {
            return Poll::Ready(Some(flow));
        }
        if let Some(i) = draining {
            if i == outs.len() {
                draining = None;
                if let Some(flow) = held.take() {
                    return Poll::Ready(Some(flow));
                }
                if upstream_done {
                    return Poll::Ready(None);
                }
                continue;
            }
            let Some(side) = outs[i].as_mut() else {
                draining = Some(i + 1);
                continue;
            };
            match side.poll_next_unpin(task) {
                // A value that reaches the end of a side pipe is dropped.
                Poll::Ready(Some(Ok(Flowing::Value(_)))) => {}
                Poll::Ready(Some(other)) => ready_out.push_back(other),
                Poll::Ready(None) => {
                    outs[i] = None;
                    draining = Some(i + 1);
                }
                Poll::Pending => {
                    let idle = {
                        let s = lock(&inputs[i]);
                        s.starved && s.queue.is_empty() && !s.closed
                    };
                    if !idle {
                        return Poll::Pending;
                    }
                    draining = Some(i + 1);
                }
            }
            continue;
        }
        if upstream_done {
            return Poll::Ready(None);
        }
        match upstream.poll_next_unpin(task) {
            Poll::Ready(Some(Ok(Flowing::Value(v)))) => {
                for (input, out) in inputs.iter().zip(&outs) {
                    if out.is_some() {
                        let mut s = lock(input);
                        s.queue.push_back(Ok(Flowing::Value(v.clone())));
                        s.starved = false;
                        s.wake();
                    }
                }
                held = Some(Ok(Flowing::Value(v)));
                draining = Some(0);
            }
            Poll::Ready(Some(other)) => return Poll::Ready(Some(other)),
            Poll::Ready(None) => {
                upstream_done = true;
                for input in &inputs {
                    let mut s = lock(input);
                    s.closed = true;
                    s.wake();
                }
                draining = Some(0);
            }
            Poll::Pending => return Poll::Pending,
        }
    })
    .boxed()
}

/// Pass on what the body gives. When it ends, or at its first failure,
/// run the cleanup, and only then pass on the failure: a caller stops
/// reading at a failure, and the cleanup must not be skipped.
fn tidied(body: Items, cleanup: Transform, item: Value, cx: Cx) -> Items {
    let mut body = Some(body);
    let mut start = Some((cleanup, item, cx));
    let mut tidy: Option<Items> = None;
    let mut after: VecDeque<Flow> = VecDeque::new();
    let mut done = false;
    stream::poll_fn(move |task| loop {
        if let Some(b) = body.as_mut() {
            match b.poll_next_unpin(task) {
                Poll::Ready(Some(Ok(flow))) => return Poll::Ready(Some(Ok(flow))),
                Poll::Ready(Some(Err(fault))) => {
                    after.push_back(Err(fault));
                    body = None;
                }
                Poll::Ready(None) => body = None,
                Poll::Pending => return Poll::Pending,
            }
            continue;
        }
        if let Some((cleanup, item, cx)) = start.take() {
            tidy = Some(cleanup(one(Ok(Flowing::Value(item))), cx));
        }
        if let Some(t) = tidy.as_mut() {
            match t.poll_next_unpin(task) {
                // What cleanup gives back is dropped. Its failure is not.
                Poll::Ready(Some(Ok(_))) => continue,
                Poll::Ready(Some(Err(fault))) => {
                    after.push_back(Err(fault));
                    tidy = None;
                }
                Poll::Ready(None) => tidy = None,
                Poll::Pending => return Poll::Pending,
            }
            continue;
        }
        if done {
            return Poll::Ready(None);
        }
        match after.pop_front() {
            Some(flow) => return Poll::Ready(Some(flow)),
            None => {
                done = true;
                return Poll::Ready(None);
            }
        }
    })
    .boxed()
}

/// Run the body; on the first failure a vine may handle, drop the body and
/// run the handler on the item as it was when it failed.
fn rescued(body: Items, handler: Transform, cx: Cx) -> Items {
    let mut body = Some(body);
    let mut rescue: Option<Items> = None;
    stream::poll_fn(move |task| loop {
        if let Some(r) = rescue.as_mut() {
            return r.poll_next_unpin(task);
        }
        let Some(b) = body.as_mut() else {
            return Poll::Ready(None);
        };
        match b.poll_next_unpin(task) {
            Poll::Ready(Some(flow)) if handleable(&flow) => {
                body = None;
                let Err(Fault::Op(fault)) = flow else {
                    unreachable!("a handleable result is a failed operator")
                };
                let OpFault {
                    from, error, item, ..
                } = *fault;
                let failure = json!({
                    "code": error.code,
                    "message": error.message,
                    "from": from,
                });
                let failed = match item {
                    Value::Object(mut map) => {
                        map.insert("error".into(), failure);
                        Value::Object(map)
                    }
                    other => json!({ "item": other, "error": failure }),
                };
                rescue = Some(handler(one(Ok(Flowing::Value(failed))), cx.clone()));
            }
            Poll::Ready(None) => {
                body = None;
                return Poll::Ready(None);
            }
            other => return other,
        }
    })
    .boxed()
}

// --------------------------------------------------------------- checking

/// What is known about the item at a point in the pipe: its fields, or
/// nothing.
type Known = Option<Shape>;

struct Checker<'a> {
    reg: &'a Registry,
    outputs: Arc<BTreeMap<String, Shape>>,
    reach: BTreeSet<String>,
    unresumable: Option<String>,
}

pub(crate) fn assemble(reg: &Registry, def: PipeDef) -> Result<Pipe, LoadError> {
    if !is_name(&def.name) {
        return Err(malformed("vine", "`vine` is the name, lower snake_case"));
    }
    if def.outputs.is_empty() {
        return Err(malformed(
            "outputs",
            "outputs is a non-empty map of output name to shape",
        ));
    }
    if let Some(bad) = def.outputs.keys().find(|k| !is_name(k)) {
        return Err(malformed(
            &format!("outputs.{bad}"),
            "output names are lower snake_case",
        ));
    }
    let mut cx = Checker {
        reg,
        outputs: Arc::new(def.outputs),
        reach: BTreeSet::new(),
        unresumable: None,
    };
    let (transform, _) = cx.pipe(&def.stages, "pipe", Some(def.input.clone()), false)?;
    if !emits(&def.stages) {
        return Err(LoadError::MissingOutput {
            path: "pipe".into(),
        });
    }
    Ok(Pipe {
        name: def.name,
        input: def.input,
        outputs: cx.outputs,
        reach: cx.reach,
        unresumable: cx.unresumable,
        transform,
    })
}

/// Whether every item that enters these stages is sure to be emitted.
fn emits(stages: &[Stage]) -> bool {
    let Some((last, before)) = stages.split_last() else {
        return false;
    };
    match last {
        // These let what was emitted before them go by.
        Stage::Take(_) | Stage::Finalize(_) | Stage::Retry { .. } | Stage::Timeout(_) => {
            emits(before)
        }
        Stage::Catch(handler) => emits(before) && emits(handler),
        Stage::Emit { .. } => true,
        Stage::Each { pipe, .. } => emits(pipe),
        Stage::Use { on, .. } => on.iter().all(|(_, p)| emits(p)),
        Stage::Branch {
            cases, otherwise, ..
        } => otherwise.as_deref().is_some_and(emits) && cases.iter().all(|(_, p)| emits(p)),
        // A fork's side pipes may emit, but the item still goes on down
        // the main pipe.
        _ => false,
    }
}

/// Whether a stage may stand after stages that already emit.
fn follows_emit(stage: &Stage) -> bool {
    matches!(
        stage,
        Stage::Take(_)
            | Stage::Catch(_)
            | Stage::Finalize(_)
            | Stage::Retry { .. }
            | Stage::Timeout(_)
    )
}

/// Whether a stage covers the stages before it, one item at a time.
fn covers(stage: &Stage) -> Option<&'static str> {
    match stage {
        Stage::Catch(_) => Some("catch"),
        Stage::Finalize(_) => Some("finalize"),
        Stage::Retry { .. } => Some("retry"),
        Stage::Timeout(_) => Some("timeout"),
        _ => None,
    }
}

/// Whether a stage works across the whole stream, not one item at a time,
/// in a way a covering stage cannot hold. `last` and `collect` are not
/// here: before a covering stage they work on one item's own stream.
fn stream_wide(stage: &Stage) -> Option<&'static str> {
    match stage {
        Stage::Take(_) => Some("take"),
        Stage::Each { overlap, .. } if *overlap != Overlap::Concat => Some("each"),
        _ => None,
    }
}

/// Whether these stages emit anywhere, at any depth.
fn emits_anywhere(stages: &[Stage]) -> bool {
    stages.iter().any(|s| match s {
        Stage::Emit { .. } => true,
        Stage::Each { pipe, .. } | Stage::Catch(pipe) | Stage::Finalize(pipe) => {
            emits_anywhere(pipe)
        }
        Stage::Fork(sides) => sides.iter().any(|p| emits_anywhere(p)),
        Stage::Use { on, .. } => on.iter().any(|(_, p)| emits_anywhere(p)),
        Stage::Branch {
            cases, otherwise, ..
        } => {
            cases.iter().any(|(_, p)| emits_anywhere(p))
                || otherwise.as_deref().is_some_and(emits_anywhere)
        }
        _ => false,
    })
}

/// Run `f` for each value still being worked on, and send on everything
/// it gives back. Outputs and failures from upstream pass through.
fn covering<F>(f: F) -> Transform
where
    F: Fn(Value, Cx) -> BoxFuture<'static, Vec<Flow>> + Send + Sync + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |items: Items, cx: Cx| {
        let f = f.clone();
        items
            .then(move |flow| {
                let (f, cx) = (f.clone(), cx.clone());
                async move {
                    match flow {
                        Ok(Flowing::Value(item)) => f(item, cx).await,
                        other => vec![other],
                    }
                }
            })
            .flat_map(stream::iter)
            .boxed()
    })
}

/// Whether a result is a failure a vine may handle. Cancellation is not.
fn handleable(flow: &Flow) -> bool {
    matches!(flow, Err(Fault::Op(f)) if f.error.code != OpError::CANCELLED)
}

impl Checker<'_> {
    /// Check a pipe and build it. `single` says the stream reaching it is
    /// one item, as inside `each`, `branch`, `catch`, `finalize` and the
    /// arms of `use`.
    fn pipe(
        &mut self,
        stages: &[Stage],
        path: &str,
        mut known: Known,
        single: bool,
    ) -> Result<(Transform, Known), LoadError> {
        if stages.is_empty() {
            return Err(malformed(path, "a pipe needs at least one stage"));
        }
        // The stages before the last covering stage run once per item.
        let last_cover = stages.iter().rposition(|s| covers(s).is_some());
        let mut one_item = last_cover.is_some() || single;
        let mut many = false;
        let mut transforms: Vec<Transform> = Vec::new();
        for (i, stage) in stages.iter().enumerate() {
            let here = format!("{path}[{i}]");
            if emits(&stages[..i]) && !follows_emit(stage) {
                return Err(LoadError::Unreachable { path: here });
            }
            if let Some(word) = covers(stage) {
                // It wraps everything before it in this pipe, per item.
                if i == 0 {
                    return Err(malformed(
                        &here,
                        &format!("`{word}` covers the stages before it, and there are none"),
                    ));
                }
                if let Some(wide) = stages[..i].iter().find_map(stream_wide) {
                    return Err(malformed(
                        &here,
                        &format!(
                            "`{word}` covers the stages before it one item at a time, but `{wide}` works across the whole stream; put `{wide}` after `{word}`"
                        ),
                    ));
                }
                let body = chain(std::mem::take(&mut transforms));
                transforms.push(self.cover(stage, &here, body)?);
                known = None;
                if Some(i) == last_cover {
                    one_item = single && !many;
                }
                continue;
            }
            if one_item {
                let word = match stage {
                    Stage::Last => Some("last"),
                    Stage::Collect { .. } => Some("collect"),
                    _ => None,
                };
                if let Some(word) = word {
                    return Err(malformed(
                        &format!("{here}.{word}"),
                        &format!(
                            "`{word}` would only ever see one item here: the stages before a covering stage, and inside `each`, `branch`, `catch`, `finalize` and `use`, run once for each item; put a stage that makes many items from one, such as `split` or a stream act, before it"
                        ),
                    ));
                }
            }
            let (t, next) = self.stage(stage, &here, known, one_item)?;
            transforms.push(t);
            known = next;
            if self.makes_many(stage) {
                many = true;
                one_item = false;
            }
        }
        Ok((chain(transforms), known))
    }

    /// Whether a stage can turn one item into many.
    fn makes_many(&self, stage: &Stage) -> bool {
        match stage {
            Stage::Split { .. } | Stage::Each { .. } | Stage::Use { .. } | Stage::Branch { .. } => {
                true
            }
            Stage::Call { act, .. } => self.reg.get_stream_act(act).is_some(),
            _ => false,
        }
    }

    fn not_resumable(&mut self, why: String) {
        if self.unresumable.is_none() {
            self.unresumable = Some(why);
        }
    }

    /// A stage that covers the stages before it, `body`, one item at a
    /// time.
    fn cover(
        &mut self,
        stage: &Stage,
        path: &str,
        body: Transform,
    ) -> Result<Transform, LoadError> {
        match stage {
            Stage::Catch(stages) => {
                // The item as it was when it failed is not known here.
                let (handler, _) = self.pipe(stages, &format!("{path}.catch"), None, true)?;
                Ok(per_item_stream(move |item, cx| {
                    rescued(
                        body(one(Ok(Flowing::Value(item))), cx.clone()),
                        handler.clone(),
                        cx,
                    )
                }))
            }
            Stage::Finalize(stages) => {
                let here = format!("{path}.finalize");
                if emits_anywhere(stages) {
                    return Err(malformed(
                        &here,
                        "`finalize` is for cleanup and may not emit",
                    ));
                }
                let (cleanup, _) = self.pipe(stages, &here, None, true)?;
                Ok(per_item_stream(move |item, cx| {
                    let work = body(one(Ok(Flowing::Value(item.clone()))), cx.clone());
                    tidied(work, cleanup.clone(), item, cx)
                }))
            }
            Stage::Retry { times, wait } => {
                let (times, wait) = (*times, *wait);
                if times == 0 {
                    return Err(malformed(
                        &format!("{path}.retry"),
                        "`retry` tries again at least once",
                    ));
                }
                Ok(covering(move |item, cx| {
                    let body = body.clone();
                    async move {
                        let mut left = times;
                        loop {
                            let out: Vec<Flow> =
                                body(one(Ok(Flowing::Value(item.clone()))), cx.clone())
                                    .collect()
                                    .await;
                            if left == 0 || !out.iter().any(handleable) {
                                return out;
                            }
                            left -= 1;
                            tokio::time::sleep(wait).await;
                        }
                    }
                    .boxed()
                }))
            }
            Stage::Timeout(limit) => {
                let limit = *limit;
                if limit.is_zero() {
                    return Err(malformed(
                        &format!("{path}.timeout"),
                        "`timeout` is longer than nothing",
                    ));
                }
                self.not_resumable(format!(
                    "{path} is a `timeout`, and a replay cannot know where the time ran out"
                ));
                let at = path.to_owned();
                Ok(covering(move |item, cx| {
                    let (body, at) = (body.clone(), at.clone());
                    async move {
                        let run =
                            body(one(Ok(Flowing::Value(item.clone()))), cx).collect::<Vec<Flow>>();
                        match tokio::time::timeout(limit, run).await {
                            Ok(out) => out,
                            Err(_) => vec![Err(Fault::Op(Box::new(OpFault {
                                path: at,
                                from: "timeout".into(),
                                error: OpError::new(
                                    "timeout",
                                    format!("took longer than {}s", limit.as_secs_f64()),
                                ),
                                item,
                            })))],
                        }
                    }
                    .boxed()
                }))
            }
            _ => unreachable!("only covering stages get here"),
        }
    }

    fn unit(
        &mut self,
        stage: &Stage,
        path: &str,
        known: &Known,
    ) -> Result<(Unit, Known), LoadError> {
        let (spec, runner, name, with, keep, end, with_path) = match stage {
            Stage::Op { name, with, keep } => {
                let Some(op) = self.reg.get(name) else {
                    return Err(LoadError::UnknownOperator {
                        path: path.into(),
                        name: name.clone(),
                    });
                };
                (
                    op.spec.clone(),
                    Runner::Op(op.handler.clone()),
                    name,
                    with,
                    keep,
                    &None,
                    format!("{path}.{name}"),
                )
            }
            Stage::Call {
                act,
                with,
                keep,
                end,
            } => {
                let (spec, runner) = if let Some(op) = self.reg.get_act(act) {
                    (op.spec.clone(), Runner::Act(op.handler.clone()))
                } else if let Some(op) = self.reg.get_stream_act(act) {
                    (op.spec.clone(), Runner::Stream(op.handler.clone()))
                } else {
                    return Err(LoadError::UnknownAct {
                        path: path.into(),
                        name: act.clone(),
                    });
                };
                (spec, runner, act, with, keep, end, format!("{path}.with"))
            }
            _ => unreachable!("only operators and acts are units"),
        };
        let is_stream = matches!(runner, Runner::Stream(_));
        if is_stream && *keep == Keep::Tap {
            return Err(malformed(
                path,
                &format!(
                    "`tap` runs an act for its effect and lets the item on once; `{name}` gives a stream, so use `call`"
                ),
            ));
        }
        if let Some(end) = end {
            if !is_stream {
                return Err(malformed(
                    &format!("{path}.end"),
                    &format!("`end` names where a stream act's end goes, and `{name}` is not a stream act"),
                ));
            }
            if !is_name(end) {
                return Err(malformed(
                    &format!("{path}.end"),
                    "`end` is a name, lower snake_case",
                ));
            }
            if *keep == Keep::Into(end.clone()) {
                return Err(LoadError::DuplicateBinding {
                    path: format!("{path}.end"),
                    name: end.clone(),
                });
            }
        }
        let template = Template::parse(with).map_err(|p| malformed(&with_path, &p))?;
        check_refs(template.refs(), known, &with_path)?;
        spec.settings
            .check_template(&template)
            .map_err(|problem| LoadError::BadSettings {
                path: with_path,
                operator: name.clone(),
                problem,
            })?;
        let next = match keep {
            Keep::Replace if end.is_some() => None,
            Keep::Replace => spec.returns.clone(),
            Keep::Tap => known.clone(),
            Keep::Into(field) => {
                if !is_name(field) {
                    return Err(malformed(
                        &format!("{path}.into"),
                        "`into` is a name, lower snake_case",
                    ));
                }
                match end {
                    // The end item has the end field and no `into` field.
                    Some(end) => known
                        .clone()
                        .map(|s| s.optional(field, Ty::Any).optional(end, Ty::Any)),
                    None => known.clone().map(|s| s.required(field, Ty::Any)),
                }
            }
        };
        self.reach.extend(spec.touches.iter().cloned());
        let unit = Unit {
            path: path.into(),
            spec,
            runner,
            with: template,
            keep: keep.clone(),
            end: end.clone(),
        };
        Ok((unit, next))
    }

    fn stage(
        &mut self,
        stage: &Stage,
        path: &str,
        known: Known,
        single: bool,
    ) -> Result<(Transform, Known), LoadError> {
        match stage {
            Stage::Op { .. } | Stage::Call { .. } => {
                let (unit, next) = self.unit(stage, path, &known)?;
                if matches!(unit.runner, Runner::Stream(_)) {
                    Ok((per_item_stream(move |item, cx| unit.stream(item, cx)), next))
                } else {
                    Ok((per_item(move |item, cx| unit.apply(item, cx)), next))
                }
            }
            Stage::Split { from, into } => {
                let here = format!("{path}.split");
                let from = Template::parse(from).map_err(|p| malformed(&here, &p))?;
                check_refs(from.refs(), &known, &here)?;
                if let Some(name) = into {
                    if !is_name(name) {
                        return Err(malformed(
                            &format!("{path}.into"),
                            "`into` is a name, lower snake_case",
                        ));
                    }
                }
                let next = match into {
                    Some(name) => known.map(|s| s.required(name, Ty::Any)),
                    None => None,
                };
                let (into, at) = (into.clone(), path.to_owned());
                let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
                    let (from, into, at) = (from.clone(), into.clone(), at.clone());
                    items
                        .flat_map(move |flow| {
                            let many: Vec<Flow> = match flow {
                                Ok(Flowing::Value(item)) => match from.resolve(&env_of(&item)) {
                                    Value::Array(parts) => parts
                                        .into_iter()
                                        .map(|part| match &into {
                                            Some(name) => add_field(&at, item.clone(), name, part)
                                                .map(Flowing::Value),
                                            None => Ok(Flowing::Value(part)),
                                        })
                                        .collect(),
                                    _ => vec![Err(fatal(RunError::ItemMisfit {
                                        path: at.clone(),
                                        problem: "`split` needs a list".into(),
                                    }))],
                                },
                                other => vec![other],
                            };
                            stream::iter(many)
                        })
                        .boxed()
                });
                Ok((t, next))
            }
            Stage::Filter { when } => {
                let here = format!("{path}.filter");
                let cond = Cond::parse(when).map_err(|p| malformed(&here, &p))?;
                check_refs(cond.refs(), &known, &here)?;
                let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
                    let cond = cond.clone();
                    items
                        .filter(move |flow| {
                            ready(match flow {
                                Ok(Flowing::Value(item)) => cond.eval(&env_of(item)),
                                _ => true,
                            })
                        })
                        .boxed()
                });
                Ok((t, known))
            }
            Stage::Take(count) => {
                if *count == 0 {
                    return Err(malformed(
                        &format!("{path}.take"),
                        "`take` lets at least one item through",
                    ));
                }
                let count = usize::try_from(*count).unwrap_or(usize::MAX);
                let t: Transform = Arc::new(move |items: Items, _cx: Cx| items.take(count).boxed());
                Ok((t, known))
            }
            Stage::Last => {
                let t: Transform = Arc::new(|items: Items, _cx: Cx| last_of(items));
                Ok((t, known))
            }
            Stage::Collect { size, into } => {
                let here = format!("{path}.collect");
                if *size == 0 {
                    return Err(malformed(&here, "`collect` needs a `size` of at least 1"));
                }
                if let Some(name) = into {
                    if !is_name(name) {
                        return Err(malformed(
                            &format!("{path}.into"),
                            "`into` is a name, lower snake_case",
                        ));
                    }
                }
                let next = match into {
                    Some(name) => known.map(|s| s.required(name, Ty::Object)),
                    None => Some(
                        Shape::new()
                            .required("items", Ty::List)
                            .required("count", Ty::Int),
                    ),
                };
                let size = usize::try_from(*size).unwrap_or(usize::MAX);
                let (into, at) = (into.clone(), path.to_owned());
                let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
                    batches(items, size, into.clone(), at.clone())
                });
                Ok((t, next))
            }
            Stage::Together(parts) => {
                let here = format!("{path}.together");
                if parts.len() < 2 {
                    return Err(malformed(
                        &here,
                        "`together` runs two or more stages side by side",
                    ));
                }
                let mut units = Vec::new();
                let mut next = known.clone();
                for (i, part) in parts.iter().enumerate() {
                    let at = format!("{here}[{i}]");
                    let field = match part {
                        Stage::Op {
                            keep: Keep::Into(f),
                            ..
                        }
                        | Stage::Call {
                            keep: Keep::Into(f),
                            ..
                        } => f.clone(),
                        _ => {
                            return Err(malformed(
                                &at,
                                "each stage inside `together` is an operator or an act with `into`, so the results do not overwrite each other",
                            ))
                        }
                    };
                    // Each one sees the item as it was before any of them ran.
                    let (unit, _) = self.unit(part, &at, &known)?;
                    if matches!(unit.runner, Runner::Stream(_)) {
                        return Err(malformed(
                            &at,
                            &format!(
                                "`{}` gives a stream, and `together` waits for one result from each stage",
                                unit.spec.name
                            ),
                        ));
                    }
                    if units.iter().any(|(f, _)| f == &field) {
                        return Err(LoadError::DuplicateBinding {
                            path: format!("{at}.into"),
                            name: field,
                        });
                    }
                    next = next.map(|s| s.required(&field, Ty::Any));
                    units.push((field, unit));
                }
                let t = per_item(move |item, cx| {
                    let units = units.clone();
                    async move {
                        let results = join_all(
                            units
                                .iter()
                                .map(|(_, u)| u.result(item.clone(), cx.clone())),
                        )
                        .await;
                        let mut item = item;
                        for ((field, unit), result) in units.iter().zip(results) {
                            item = add_field(&unit.path, item, field, result?)?;
                        }
                        Ok(item)
                    }
                    .boxed()
                });
                Ok((t, next))
            }
            Stage::Each {
                overlap,
                at_once,
                pipe,
            } => {
                let here = format!("{path}.each");
                if at_once.is_some() && *overlap != Overlap::Merge {
                    return Err(malformed(
                        &here,
                        "`at_once` limits side-by-side runs, so it goes with `overlap: merge`",
                    ));
                }
                if *at_once == Some(0) {
                    return Err(malformed(&here, "`at_once` is at least 1"));
                }
                if *overlap != Overlap::Concat {
                    self.not_resumable(format!(
                        "{here} runs items with overlap `{}`, and a replay needs one order",
                        match overlap {
                            Overlap::Merge => "merge",
                            Overlap::Switch => "switch",
                            Overlap::Exhaust => "exhaust",
                            Overlap::Concat => "concat",
                        }
                    ));
                }
                let (inner, _) = self.pipe(pipe, &format!("{here}.do"), known, true)?;
                let (overlap, at_once) = (*overlap, *at_once);
                let t: Transform = Arc::new(move |items: Items, cx: Cx| {
                    let inner = inner.clone();
                    match overlap {
                        Overlap::Switch => latest_or_first(items, inner, true, cx),
                        Overlap::Exhaust => latest_or_first(items, inner, false, cx),
                        Overlap::Concat | Overlap::Merge => {
                            let runs = items.map(move |flow| match flow {
                                Ok(Flowing::Value(v)) => {
                                    inner(one(Ok(Flowing::Value(v))), cx.clone())
                                }
                                other => one(other),
                            });
                            if overlap == Overlap::Concat {
                                runs.flatten().boxed()
                            } else {
                                runs.flatten_unordered(at_once).boxed()
                            }
                        }
                    }
                });
                Ok((t, None))
            }
            Stage::Fork(sides) => {
                let here = format!("{path}.fork");
                if sides.is_empty() {
                    return Err(malformed(&here, "`fork` needs at least one side pipe"));
                }
                let mut built = Vec::new();
                for (j, side) in sides.iter().enumerate() {
                    let (t, _) = self.pipe(side, &format!("{here}[{j}]"), known.clone(), single)?;
                    built.push(t);
                }
                let t: Transform =
                    Arc::new(move |items: Items, cx: Cx| forked(items, built.clone(), cx));
                Ok((t, known))
            }
            Stage::Catch(_) | Stage::Finalize(_) | Stage::Retry { .. } | Stage::Timeout(_) => {
                unreachable!("covering stages are handled where the pipe is put together")
            }
            Stage::Use {
                vine,
                with,
                keep,
                on,
            } => {
                let Some(other) = self.reg.get_vine(vine).cloned() else {
                    return Err(LoadError::UnknownVine {
                        path: path.into(),
                        name: vine.clone(),
                    });
                };
                let with = match with {
                    Some(w) => {
                        let at = format!("{path}.with");
                        let t = Template::parse(w).map_err(|p| malformed(&at, &p))?;
                        check_refs(t.refs(), &known, &at)?;
                        other.input().check_template(&t).map_err(|problem| {
                            LoadError::BadSettings {
                                path: at,
                                operator: vine.clone(),
                                problem,
                            }
                        })?;
                        Some(t)
                    }
                    None => {
                        if let Some(item) = &known {
                            item.fits_into(other.input()).map_err(|problem| {
                                LoadError::BadSettings {
                                    path: path.into(),
                                    operator: vine.clone(),
                                    problem,
                                }
                            })?;
                        }
                        None
                    }
                };
                if let Keep::Into(field) = keep {
                    if !is_name(field) {
                        return Err(malformed(
                            &format!("{path}.into"),
                            "`into` is a name, lower snake_case",
                        ));
                    }
                }
                let mut arms: BTreeMap<String, Transform> = BTreeMap::new();
                for (output, stages) in on {
                    let at = format!("{path}.on.{output}");
                    if !other.outputs().contains_key(output) {
                        return Err(LoadError::UndeclaredOutput {
                            path: at,
                            name: output.clone(),
                        });
                    }
                    if arms.contains_key(output) {
                        return Err(malformed(&at, "this output is listed twice"));
                    }
                    arms.insert(output.clone(), self.pipe(stages, &at, None, true)?.0);
                }
                if let Some(missing) = other.outputs().keys().find(|o| !arms.contains_key(*o)) {
                    return Err(malformed(
                        &format!("{path}.on"),
                        &format!(
                            "vine `{vine}` can send to `{missing}`, and nothing here says what to do with it"
                        ),
                    ));
                }
                if let Some(why) = &other.unresumable {
                    self.not_resumable(format!("{path} uses vine `{vine}`, and {why}"));
                }
                self.reach.extend(other.reach().iter().cloned());
                let (keep, at) = (keep.clone(), path.to_owned());
                let t: Transform = Arc::new(move |items: Items, cx: Cx| {
                    let (other, with, keep, arms, at) = (
                        other.clone(),
                        with.clone(),
                        keep.clone(),
                        arms.clone(),
                        at.clone(),
                    );
                    items
                        .map(move |flow| {
                            let item = match flow {
                                Ok(Flowing::Value(item)) => item,
                                other => return one(other),
                            };
                            let input = match &with {
                                Some(t) => t.resolve(&env_of(&item)),
                                None => item.clone(),
                            };
                            let (keep, arms, at) = (keep.clone(), arms.clone(), at.clone());
                            let inner_cx = cx.within(&at, other.name());
                            let cx = cx.clone();
                            other
                                .flow(one(Ok(Flowing::Value(input))), inner_cx)
                                .map(move |flow| match flow {
                                    Ok(Flowing::Out(out)) => {
                                        let next = match &keep {
                                            Keep::Into(field) => {
                                                add_field(&at, item.clone(), field, out.payload)
                                            }
                                            _ => Ok(out.payload),
                                        };
                                        match next {
                                            Ok(v) => arms[&out.name](
                                                one(Ok(Flowing::Value(v))),
                                                cx.clone(),
                                            ),
                                            Err(fault) => one(Err(fault)),
                                        }
                                    }
                                    other => one(other),
                                })
                                .flatten()
                                .boxed()
                        })
                        .flatten()
                        .boxed()
                });
                Ok((t, None))
            }
            Stage::Branch {
                on,
                cases,
                otherwise,
            } => {
                let here = format!("{path}.branch");
                let on = Template::parse(on).map_err(|p| malformed(&format!("{here}.on"), &p))?;
                check_refs(on.refs(), &known, &format!("{here}.on"))?;
                if cases.is_empty() {
                    return Err(malformed(&here, "`branch` needs at least one case"));
                }
                let mut arms: BTreeMap<String, Transform> = BTreeMap::new();
                for (key, pipe) in cases {
                    let at = format!("{here}.cases.{key}");
                    if arms.contains_key(key) {
                        return Err(malformed(&at, "this case is listed twice"));
                    }
                    let (arm, _) = self.pipe(pipe, &at, known.clone(), true)?;
                    arms.insert(key.clone(), arm);
                }
                let fallback = match otherwise {
                    Some(pipe) => Some(self.pipe(pipe, &format!("{here}.else"), known, true)?.0),
                    None => None,
                };
                let t: Transform = Arc::new(move |items: Items, cx: Cx| {
                    let (on, arms, fallback) = (on.clone(), arms.clone(), fallback.clone());
                    items
                        .map(move |flow| match flow {
                            Ok(Flowing::Value(item)) => {
                                let key = case_key(&on.resolve(&env_of(&item)));
                                let arm = key.and_then(|k| arms.get(&k)).or(fallback.as_ref());
                                match arm {
                                    Some(arm) => arm(one(Ok(Flowing::Value(item))), cx.clone()),
                                    None => one(Ok(Flowing::Value(item))),
                                }
                            }
                            other => one(other),
                        })
                        .flatten()
                        .boxed()
                });
                Ok((t, None))
            }
            Stage::Emit { to, with } => {
                let here = format!("{path}.emit");
                let Some(shape) = self.outputs.get(to).cloned() else {
                    return Err(LoadError::UndeclaredOutput {
                        path: here,
                        name: to.clone(),
                    });
                };
                let with = match with {
                    Some(w) => {
                        let at = format!("{here}.with");
                        let t = Template::parse(w).map_err(|p| malformed(&at, &p))?;
                        check_refs(t.refs(), &known, &at)?;
                        shape
                            .check_template(&t)
                            .map_err(|problem| LoadError::OutputMisfit {
                                path: at,
                                output: to.clone(),
                                problem,
                            })?;
                        Some(t)
                    }
                    None => {
                        if let Some(item) = &known {
                            item.fits_into(&shape)
                                .map_err(|problem| LoadError::OutputMisfit {
                                    path: here.clone(),
                                    output: to.clone(),
                                    problem,
                                })?;
                        }
                        None
                    }
                };
                let (to, at) = (to.clone(), here);
                let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
                    let (to, at, with, shape) =
                        (to.clone(), at.clone(), with.clone(), shape.clone());
                    items
                        .map(move |flow| match flow {
                            Ok(Flowing::Value(item)) => {
                                let payload = match &with {
                                    Some(t) => t.resolve(&env_of(&item)),
                                    None => item,
                                };
                                match shape.check(&payload) {
                                    Ok(()) => Ok(Flowing::Out(Output {
                                        name: to.clone(),
                                        payload,
                                    })),
                                    Err(problem) => Err(fatal(RunError::OutputMisfit {
                                        path: at.clone(),
                                        output: to.clone(),
                                        problem,
                                    })),
                                }
                            }
                            other => other,
                        })
                        .boxed()
                });
                Ok((t, None))
            }
        }
    }
}

/// `$item` is the only name in a pipe. A field is checked when the item's
/// fields are known at that point.
fn check_refs(refs: Vec<&Ref>, known: &Known, path: &str) -> Result<(), LoadError> {
    for r in refs {
        if r.root != "item" {
            return Err(LoadError::UnboundReference {
                path: path.into(),
                reference: r.to_string(),
                name: r.root.clone(),
            });
        }
        if let (Some(shape), Some(first)) = (known, r.path.first()) {
            if !shape.is_open() && shape.field(first).is_none() {
                return Err(LoadError::UnknownField {
                    path: path.into(),
                    reference: r.to_string(),
                    name: r.root.clone(),
                    field: first.clone(),
                });
            }
        }
    }
    Ok(())
}

fn malformed(path: &str, problem: &str) -> LoadError {
    LoadError::Malformed {
        path: path.into(),
        problem: problem.into(),
    }
}

// ------------------------------------------------------------------- YAML

/// The YAML layer. It reads the same stages the functions above build.
#[cfg(feature = "yaml")]
pub(crate) fn load(reg: &Registry, yaml: &str) -> Result<Pipe, LoadError> {
    let raw: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|e| LoadError::Yaml(e.to_string()))?;
    let doc: Value = serde_json::to_value(raw).map_err(|e| LoadError::Yaml(e.to_string()))?;
    let Some(map) = doc.as_object() else {
        return Err(malformed("vine", "a vine document is a map"));
    };
    if let Some(key) = map
        .keys()
        .find(|k| !["vine", "input", "outputs", "pipe"].contains(&k.as_str()))
    {
        return Err(LoadError::UnknownKey {
            path: "vine".into(),
            key: key.clone(),
        });
    }
    let name = match map.get("vine") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(malformed("vine", "missing `vine` (the vine's name)")),
    };
    let input = match map.get("input") {
        Some(v) => Shape::parse(v).map_err(|p| malformed("input", &p))?,
        None => return Err(malformed("vine", "missing `input` (the input shape)")),
    };
    let mut outputs = BTreeMap::new();
    for (k, v) in map
        .get("outputs")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let shape = Shape::parse(v).map_err(|p| malformed(&format!("outputs.{k}"), &p))?;
        outputs.insert(k.clone(), shape);
    }
    let Some(stages) = map.get("pipe") else {
        return Err(malformed("vine", "missing `pipe`"));
    };
    let stages = read_pipe(stages, "pipe")?;
    assemble(
        reg,
        PipeDef {
            name,
            input,
            outputs,
            stages,
        },
    )
}

#[cfg(feature = "yaml")]
fn read_pipe(v: &Value, path: &str) -> Result<Vec<Stage>, LoadError> {
    let Some(list) = v.as_array() else {
        return Err(malformed(path, "a pipe is a list of stages"));
    };
    list.iter()
        .enumerate()
        .map(|(i, s)| read_stage(s, &format!("{path}[{i}]")))
        .collect()
}

#[cfg(feature = "yaml")]
fn read_stage(v: &Value, path: &str) -> Result<Stage, LoadError> {
    const WORDS: &[&str] = &[
        "call", "tap", "split", "filter", "take", "last", "collect", "together", "each", "fork",
        "catch", "finalize", "retry", "timeout", "use", "branch", "emit",
    ];
    if v.as_str() == Some("last") {
        return Ok(Stage::Last);
    }
    let Some(map) = v.as_object() else {
        return Err(malformed(
            path,
            "a stage is a map with one key, or the word `last`",
        ));
    };
    let mut main: Vec<&String> = map
        .keys()
        .filter(|k| !["with", "into", "on", "end"].contains(&k.as_str()))
        .collect();
    let key = match (main.pop(), main.pop()) {
        (Some(key), None) => key.as_str(),
        (None, _) => {
            return Err(malformed(
                path,
                "a stage needs one key that says what it does",
            ))
        }
        (Some(a), Some(b)) => {
            return Err(malformed(
                path,
                &format!("a stage does one thing, found `{b}` and `{a}`"),
            ))
        }
    };
    let body = &map[key];
    let here = format!("{path}.{key}");
    let text = |v: &Value, what: &str| -> Result<String, LoadError> {
        v.as_str()
            .map(str::to_owned)
            .ok_or_else(|| malformed(&here, what))
    };
    let into = match map.get("into") {
        None => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(malformed(&format!("{path}.into"), "`into` is a name")),
    };
    let takes = |with: bool, into_ok: bool| -> Result<(), LoadError> {
        for (present, allowed, key) in [
            (map.contains_key("with"), with, "with"),
            (into.is_some(), into_ok, "into"),
            (map.contains_key("on"), key == "use", "on"),
            (map.contains_key("end"), key == "call", "end"),
        ] {
            if present && !allowed {
                return Err(LoadError::UnknownKey {
                    path: path.into(),
                    key: key.into(),
                });
            }
        }
        Ok(())
    };
    let fields = |allowed: &[&str]| -> Result<&serde_json::Map<String, Value>, LoadError> {
        let Some(m) = body.as_object() else {
            return Err(malformed(&here, &format!("`{key}` takes a map")));
        };
        match m.keys().find(|k| !allowed.contains(&k.as_str())) {
            Some(k) => Err(LoadError::UnknownKey {
                path: here.clone(),
                key: k.clone(),
            }),
            None => Ok(m),
        }
    };
    let with = || map.get("with").cloned().unwrap_or_else(|| json!({}));
    let keep = |into: Option<String>| into.map_or(Keep::Replace, Keep::Into);
    Ok(match key {
        "call" => {
            takes(true, true)?;
            let end = match map.get("end") {
                None => None,
                Some(Value::String(s)) => Some(s.clone()),
                Some(_) => return Err(malformed(&format!("{path}.end"), "`end` is a name")),
            };
            Stage::Call {
                act: text(body, "`call` names one act, such as `git.worktree_add`")?,
                with: with(),
                keep: keep(into),
                end,
            }
        }
        "tap" => {
            takes(true, false)?;
            Stage::Call {
                act: text(body, "`tap` names one act, such as `git.refresh_branch`")?,
                with: with(),
                keep: Keep::Tap,
                end: None,
            }
        }
        "split" => {
            takes(false, true)?;
            Stage::Split {
                from: body.clone(),
                into,
            }
        }
        "filter" => {
            takes(false, false)?;
            Stage::Filter { when: body.clone() }
        }
        "take" => {
            takes(false, false)?;
            Stage::Take(
                body.as_u64()
                    .ok_or_else(|| malformed(&here, "`take` is a whole number"))?,
            )
        }
        "last" => {
            takes(false, false)?;
            if !body.is_null() {
                return Err(malformed(&here, "`last` takes nothing: write `- last`"));
            }
            Stage::Last
        }
        "collect" => {
            takes(false, true)?;
            let m = fields(&["size"])?;
            let size = m
                .get("size")
                .and_then(Value::as_u64)
                .ok_or_else(|| malformed(&here, "`collect` needs `size`, a whole number"))?;
            Stage::Collect { size, into }
        }
        "fork" => {
            takes(false, false)?;
            let Some(sides) = body.as_array() else {
                return Err(malformed(&here, "`fork` is a list of side pipes"));
            };
            Stage::Fork(
                sides
                    .iter()
                    .enumerate()
                    .map(|(j, side)| read_pipe(side, &format!("{here}[{j}]")))
                    .collect::<Result<_, _>>()?,
            )
        }
        "together" => {
            takes(false, false)?;
            Stage::Together(read_pipe(body, &here)?)
        }
        "each" => {
            takes(false, false)?;
            let m = fields(&["do", "overlap", "at_once"])?;
            let overlap = match m.get("overlap").map(|v| v.as_str()) {
                None | Some(Some("concat")) => Overlap::Concat,
                Some(Some("merge")) => Overlap::Merge,
                Some(Some("switch")) => Overlap::Switch,
                Some(Some("exhaust")) => Overlap::Exhaust,
                _ => {
                    return Err(malformed(
                        &format!("{here}.overlap"),
                        "overlap is `concat`, `merge`, `switch` or `exhaust`",
                    ))
                }
            };
            let at_once = match m.get("at_once") {
                None => None,
                Some(v) => Some(
                    v.as_u64()
                        .and_then(|n| usize::try_from(n).ok())
                        .ok_or_else(|| {
                            malformed(&format!("{here}.at_once"), "`at_once` is a whole number")
                        })?,
                ),
            };
            let Some(body) = m.get("do") else {
                return Err(malformed(&here, "`each` needs `do`, the pipe to run"));
            };
            Stage::Each {
                overlap,
                at_once,
                pipe: read_pipe(body, &format!("{here}.do"))?,
            }
        }
        "catch" => {
            takes(false, false)?;
            Stage::Catch(read_pipe(body, &here)?)
        }
        "finalize" => {
            takes(false, false)?;
            Stage::Finalize(read_pipe(body, &here)?)
        }
        "retry" => {
            takes(false, false)?;
            let m = fields(&["times", "wait"])?;
            let times = m
                .get("times")
                .and_then(Value::as_u64)
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| malformed(&here, "`retry` needs `times`, a whole number"))?;
            let wait = match m.get("wait") {
                None => Duration::ZERO,
                Some(v) => v
                    .as_str()
                    .and_then(crate::stream::parse_duration)
                    .ok_or_else(|| {
                        malformed(&format!("{here}.wait"), "`wait` is a time such as `5s`")
                    })?,
            };
            Stage::Retry { times, wait }
        }
        "timeout" => {
            takes(false, false)?;
            Stage::Timeout(
                body.as_str()
                    .and_then(crate::stream::parse_duration)
                    .ok_or_else(|| malformed(&here, "`timeout` is a time such as `30s`"))?,
            )
        }
        "use" => {
            takes(true, true)?;
            let mut on = Vec::new();
            match map.get("on") {
                None => {}
                Some(Value::Object(arms)) => {
                    for (output, stages) in arms {
                        on.push((
                            output.clone(),
                            read_pipe(stages, &format!("{path}.on.{output}"))?,
                        ));
                    }
                }
                Some(_) => {
                    return Err(malformed(
                        &format!("{path}.on"),
                        "`on` is a map of output name to stages",
                    ))
                }
            }
            Stage::Use {
                vine: text(body, "`use` names one vine")?,
                with: map.get("with").cloned(),
                keep: keep(into),
                on,
            }
        }
        "branch" => {
            takes(false, false)?;
            let m = fields(&["on", "cases", "else"])?;
            let (Some(on), Some(Value::Object(cases))) = (m.get("on"), m.get("cases")) else {
                return Err(malformed(&here, "`branch` needs `on` and a map of `cases`"));
            };
            let mut arms = Vec::new();
            for (k, p) in cases {
                arms.push((k.clone(), read_pipe(p, &format!("{here}.cases.{k}"))?));
            }
            let otherwise = match m.get("else") {
                Some(p) => Some(read_pipe(p, &format!("{here}.else"))?),
                None => None,
            };
            Stage::Branch {
                on: on.clone(),
                cases: arms,
                otherwise,
            }
        }
        "emit" => {
            takes(false, false)?;
            match body {
                Value::String(to) => Stage::Emit {
                    to: to.clone(),
                    with: None,
                },
                _ => {
                    let m = fields(&["to", "with"])?;
                    let Some(Value::String(to)) = m.get("to") else {
                        return Err(malformed(&here, "`emit` names the output in `to`"));
                    };
                    Stage::Emit {
                        to: to.clone(),
                        with: m.get("with").cloned(),
                    }
                }
            }
        }
        name => {
            debug_assert!(!WORDS.contains(&name));
            takes(false, true)?;
            Stage::Op {
                name: name.to_owned(),
                with: body.clone(),
                keep: keep(into),
            }
        }
    })
}
