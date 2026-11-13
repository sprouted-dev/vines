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
//! - [`together`]: run several operators or acts side by side for one
//!   item, and go on when all of them are done.
//! - [`each`]: run a pipe for each item. [`Overlap`] says what happens
//!   when a new item arrives while the last one is still running.
//! - [`use_vine`]: run another vine for each item, and say what to do
//!   with each thing it emits.
//! - [`branch`]: run the pipe whose key equals a value of the item.
//! - [`emit`]: send the item to one of the vine's named outputs.
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
//! # What this module leaves out
//!
//! Saved positions, memory across a restart, and resuming after a crash.
//! The older stream runner in [`crate::run`] has them. This module is the
//! shape of the one model, not yet its durability. `finalize` does not
//! run when the caller drops the stream partway: Rust has no async drop.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use futures::future::{join_all, ready, BoxFuture};
use futures::stream::{self, BoxStream};
use futures::{FutureExt, Stream, StreamExt};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::error::{LoadError, RunError};
use crate::expr::{case_key, is_name, Cond, Env, Ref, Template};
use crate::registry::{OpContext, OpError, Registered, Registry};
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
    },
    Split {
        from: Value,
        into: Option<String>,
    },
    Filter {
        when: Value,
    },
    Take(u64),
    Together(Vec<Stage>),
    Each {
        overlap: Overlap,
        at_once: Option<usize>,
        pipe: Vec<Stage>,
    },
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
    /// under `name`.
    pub fn into(mut self, name: &str) -> Self {
        match &mut self {
            Stage::Op { keep, .. } | Stage::Call { keep, .. } | Stage::Use { keep, .. } => {
                *keep = Keep::Into(name.to_owned())
            }
            Stage::Split { into, .. } => *into = Some(name.to_owned()),
            _ => {}
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

/// Run a registered act, such as `git.worktree_add`, for each item.
pub fn call(act: &str, with: Value) -> Stage {
    Stage::Call {
        act: act.to_owned(),
        with,
        keep: Keep::Replace,
    }
}

/// Run an act for its effect. The item passes through unchanged.
pub fn tap(act: &str, with: Value) -> Stage {
    Stage::Call {
        act: act.to_owned(),
        with,
        keep: Keep::Tap,
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

type Flow = Result<Flowing, Fault>;
type Items = BoxStream<'static, Flow>;
type Transform = Arc<dyn Fn(Items) -> Items + Send + Sync>;

/// A checked vine, ready to run.
#[derive(Clone)]
pub struct Pipe {
    name: String,
    input: Shape,
    outputs: Arc<BTreeMap<String, Shape>>,
    reach: BTreeSet<String>,
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

    /// Run the vine over a stream of items. What comes back is the stream
    /// of what it emits. A failure no `attempt` handles is the last thing
    /// on the stream. Dropping the stream stops the run.
    pub fn run<S>(&self, input: S) -> BoxStream<'static, Result<Output, RunError>>
    where
        S: Stream<Item = Value> + Send + 'static,
    {
        let shape = self.input.clone();
        let checked = input.map(move |v| match shape.check(&v) {
            Ok(()) => Ok(Flowing::Value(v)),
            Err(p) => Err(fatal(RunError::InputMisfit(p))),
        });
        (self.transform)(checked.boxed())
            .map(|flow| match flow {
                Ok(Flowing::Out(out)) => Ok(out),
                // The checker refuses a pipe with a path that does not emit.
                Ok(Flowing::Value(_)) => Err(RunError::ItemMisfit {
                    path: "pipe".into(),
                    problem: "an item reached the end of the pipe without being emitted".into(),
                }),
                Err(Fault::Fatal(e)) => Err(*e),
                Err(Fault::Op(fault)) => {
                    let OpFault {
                        path, from, error, ..
                    } = *fault;
                    Err(if error.code == OpError::CANCELLED {
                        RunError::Cancelled { path }
                    } else {
                        RunError::Unhandled {
                            path,
                            operator: from,
                            code: error.code,
                            message: error.message,
                        }
                    })
                }
            })
            // A failure ends the stream.
            .scan(false, |done, r| {
                let next = if *done { None } else { Some(r) };
                if matches!(next, Some(Err(_))) {
                    *done = true;
                }
                ready(next)
            })
            .boxed()
    }

    /// Run as a stage of another vine.
    fn flow(&self, items: Items) -> Items {
        let shape = self.input.clone();
        let checked = items.map(move |flow| match flow {
            Ok(Flowing::Value(v)) => match shape.check(&v) {
                Ok(()) => Ok(Flowing::Value(v)),
                Err(p) => Err(fatal(RunError::InputMisfit(p))),
            },
            other => other,
        });
        (self.transform)(checked.boxed())
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

fn env_of(item: &Value) -> Env {
    let mut env = Env::new();
    env.insert("item".into(), item.clone());
    env
}

fn one(flow: Flow) -> Items {
    stream::once(ready(flow)).boxed()
}

/// An operator or act, ready to run for one item.
#[derive(Clone)]
struct Unit {
    path: String,
    op: Registered,
    with: Template,
    keep: Keep,
}

impl Unit {
    /// Run for one item and give back the raw result.
    fn result(&self, item: Value) -> BoxFuture<'static, Result<Value, Fault>> {
        let unit = self.clone();
        async move {
            let name = unit.op.spec.name.clone();
            let resolved = unit.with.resolve(&env_of(&item));
            unit.op.spec.settings.check(&resolved).map_err(|problem| {
                fatal(RunError::SettingsMisfit {
                    path: unit.path.clone(),
                    operator: name.clone(),
                    problem,
                })
            })?;
            let cx = OpContext::new(CancellationToken::new());
            let result = (unit.op.handler)(resolved, cx).await.map_err(|error| {
                Fault::Op(Box::new(OpFault {
                    path: unit.path.clone(),
                    from: name.clone(),
                    error,
                    item: item.clone(),
                }))
            })?;
            if let Some(shape) = &unit.op.spec.returns {
                shape.check(&result).map_err(|problem| {
                    fatal(RunError::ResultMisfit {
                        path: unit.path.clone(),
                        operator: name,
                        problem,
                    })
                })?;
            }
            Ok(result)
        }
        .boxed()
    }

    /// Run for one item and give back the item that flows on.
    fn apply(&self, item: Value) -> BoxFuture<'static, Result<Value, Fault>> {
        let unit = self.clone();
        async move {
            let result = unit.result(item.clone()).await?;
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
    F: Fn(Value) -> BoxFuture<'static, Result<Value, Fault>> + Send + Sync + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |items: Items| {
        let f = f.clone();
        items
            .then(move |flow| {
                let f = f.clone();
                async move {
                    match flow {
                        Ok(Flowing::Value(v)) => f(v).await.map(Flowing::Value),
                        other => other,
                    }
                }
            })
            .boxed()
    })
}

/// Each new value starts the pipe inside for itself. `Switch` drops the
/// run in flight for the new one. `Exhaust` ignores the new one while a
/// run is in flight. Outputs and failures from upstream are never dropped.
fn latest_or_first(outer: Items, inner: Transform, switch: bool) -> Items {
    let mut outer = Some(outer);
    let mut current: Option<Items> = None;
    let mut passing: VecDeque<Flow> = VecDeque::new();
    stream::poll_fn(move |cx| {
        if let Some(upstream) = outer.as_mut() {
            loop {
                match upstream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(Flowing::Value(v)))) => {
                        if switch || current.is_none() {
                            current = Some(inner(one(Ok(Flowing::Value(v)))));
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
            match run.poll_next_unpin(cx) {
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

// --------------------------------------------------------------- checking

/// What is known about the item at a point in the pipe: its fields, or
/// nothing.
type Known = Option<Shape>;

struct Checker<'a> {
    reg: &'a Registry,
    outputs: Arc<BTreeMap<String, Shape>>,
    reach: BTreeSet<String>,
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
    };
    let (transform, _) = cx.pipe(&def.stages, "pipe", Some(def.input.clone()))?;
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

/// Whether a stage works across the whole stream, not one item at a time.
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
    F: Fn(Value) -> BoxFuture<'static, Vec<Flow>> + Send + Sync + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |items: Items| {
        let f = f.clone();
        items
            .then(move |flow| {
                let f = f.clone();
                async move {
                    match flow {
                        Ok(Flowing::Value(item)) => f(item).await,
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
    fn pipe(
        &mut self,
        stages: &[Stage],
        path: &str,
        mut known: Known,
    ) -> Result<(Transform, Known), LoadError> {
        if stages.is_empty() {
            return Err(malformed(path, "a pipe needs at least one stage"));
        }
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
                let before = std::mem::take(&mut transforms);
                let body: Transform =
                    Arc::new(move |items: Items| before.iter().fold(items, |items, t| t(items)));
                transforms.push(self.cover(stage, &here, body)?);
                known = None;
                continue;
            }
            let (t, next) = self.stage(stage, &here, known)?;
            transforms.push(t);
            known = next;
        }
        let all: Transform =
            Arc::new(move |items: Items| transforms.iter().fold(items, |items, t| t(items)));
        Ok((all, known))
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
                let (handler, _) = self.pipe(stages, &format!("{path}.catch"), None)?;
                Ok(covering(move |item| {
                    let (body, handler) = (body.clone(), handler.clone());
                    async move {
                        let mut out = Vec::new();
                        let mut run = body(one(Ok(Flowing::Value(item))));
                        while let Some(flow) = run.next().await {
                            if !handleable(&flow) {
                                out.push(flow);
                                continue;
                            }
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
                            let rescue = handler(one(Ok(Flowing::Value(failed))));
                            out.extend(rescue.collect::<Vec<_>>().await);
                            break;
                        }
                        out
                    }
                    .boxed()
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
                let (cleanup, _) = self.pipe(stages, &here, None)?;
                Ok(covering(move |item| {
                    let (body, cleanup) = (body.clone(), cleanup.clone());
                    async move {
                        let mut out: Vec<Flow> =
                            body(one(Ok(Flowing::Value(item.clone())))).collect().await;
                        let mut tidy = cleanup(one(Ok(Flowing::Value(item))));
                        while let Some(flow) = tidy.next().await {
                            // What cleanup gives back is dropped. Its failure is not.
                            if flow.is_err() {
                                out.push(flow);
                                break;
                            }
                        }
                        out
                    }
                    .boxed()
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
                Ok(covering(move |item| {
                    let body = body.clone();
                    async move {
                        let mut left = times;
                        loop {
                            let out: Vec<Flow> =
                                body(one(Ok(Flowing::Value(item.clone())))).collect().await;
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
                let at = path.to_owned();
                Ok(covering(move |item| {
                    let (body, at) = (body.clone(), at.clone());
                    async move {
                        let run =
                            body(one(Ok(Flowing::Value(item.clone())))).collect::<Vec<Flow>>();
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
        let (op, name, with, keep, with_path) = match stage {
            Stage::Op { name, with, keep } => {
                let Some(op) = self.reg.get(name) else {
                    return Err(LoadError::UnknownOperator {
                        path: path.into(),
                        name: name.clone(),
                    });
                };
                (op.clone(), name, with, keep, format!("{path}.{name}"))
            }
            Stage::Call { act, with, keep } => {
                let Some(op) = self.reg.get_act(act) else {
                    return Err(LoadError::UnknownAct {
                        path: path.into(),
                        name: act.clone(),
                    });
                };
                (op.clone(), act, with, keep, format!("{path}.with"))
            }
            _ => unreachable!("only operators and acts are units"),
        };
        let template = Template::parse(with).map_err(|p| malformed(&with_path, &p))?;
        check_refs(template.refs(), known, &with_path)?;
        op.spec
            .settings
            .check_template(&template)
            .map_err(|problem| LoadError::BadSettings {
                path: with_path,
                operator: name.clone(),
                problem,
            })?;
        let next = match keep {
            Keep::Replace => op.spec.returns.clone(),
            Keep::Tap => known.clone(),
            Keep::Into(field) => {
                if !is_name(field) {
                    return Err(malformed(
                        &format!("{path}.into"),
                        "`into` is a name, lower snake_case",
                    ));
                }
                known.clone().map(|s| s.required(field, Ty::Any))
            }
        };
        self.reach.extend(op.spec.touches.iter().cloned());
        let unit = Unit {
            path: path.into(),
            op,
            with: template,
            keep: keep.clone(),
        };
        Ok((unit, next))
    }

    fn stage(
        &mut self,
        stage: &Stage,
        path: &str,
        known: Known,
    ) -> Result<(Transform, Known), LoadError> {
        match stage {
            Stage::Op { .. } | Stage::Call { .. } => {
                let (unit, next) = self.unit(stage, path, &known)?;
                Ok((per_item(move |item| unit.apply(item)), next))
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
                let t: Transform = Arc::new(move |items: Items| {
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
                let t: Transform = Arc::new(move |items: Items| {
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
                let t: Transform = Arc::new(move |items: Items| items.take(count).boxed());
                Ok((t, known))
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
                    if units.iter().any(|(f, _)| f == &field) {
                        return Err(LoadError::DuplicateBinding {
                            path: format!("{at}.into"),
                            name: field,
                        });
                    }
                    next = next.map(|s| s.required(&field, Ty::Any));
                    units.push((field, unit));
                }
                let t = per_item(move |item| {
                    let units = units.clone();
                    async move {
                        let results =
                            join_all(units.iter().map(|(_, u)| u.result(item.clone()))).await;
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
                let (inner, _) = self.pipe(pipe, &format!("{here}.do"), known)?;
                let (overlap, at_once) = (*overlap, *at_once);
                let t: Transform = Arc::new(move |items: Items| {
                    let inner = inner.clone();
                    match overlap {
                        Overlap::Switch => latest_or_first(items, inner, true),
                        Overlap::Exhaust => latest_or_first(items, inner, false),
                        Overlap::Concat | Overlap::Merge => {
                            let runs = items.map(move |flow| match flow {
                                Ok(Flowing::Value(v)) => inner(one(Ok(Flowing::Value(v)))),
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
                    arms.insert(output.clone(), self.pipe(stages, &at, None)?.0);
                }
                if let Some(missing) = other.outputs().keys().find(|o| !arms.contains_key(*o)) {
                    return Err(malformed(
                        &format!("{path}.on"),
                        &format!(
                            "vine `{vine}` can send to `{missing}`, and nothing here says what to do with it"
                        ),
                    ));
                }
                self.reach.extend(other.reach().iter().cloned());
                let (keep, at) = (keep.clone(), path.to_owned());
                let t: Transform = Arc::new(move |items: Items| {
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
                            other
                                .flow(one(Ok(Flowing::Value(input))))
                                .map(move |flow| match flow {
                                    Ok(Flowing::Out(out)) => {
                                        let next = match &keep {
                                            Keep::Into(field) => {
                                                add_field(&at, item.clone(), field, out.payload)
                                            }
                                            _ => Ok(out.payload),
                                        };
                                        match next {
                                            Ok(v) => arms[&out.name](one(Ok(Flowing::Value(v)))),
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
                    let (arm, _) = self.pipe(pipe, &at, known.clone())?;
                    arms.insert(key.clone(), arm);
                }
                let fallback = match otherwise {
                    Some(pipe) => Some(self.pipe(pipe, &format!("{here}.else"), known)?.0),
                    None => None,
                };
                let t: Transform = Arc::new(move |items: Items| {
                    let (on, arms, fallback) = (on.clone(), arms.clone(), fallback.clone());
                    items
                        .map(move |flow| match flow {
                            Ok(Flowing::Value(item)) => {
                                let key = case_key(&on.resolve(&env_of(&item)));
                                let arm = key.and_then(|k| arms.get(&k)).or(fallback.as_ref());
                                match arm {
                                    Some(arm) => arm(one(Ok(Flowing::Value(item)))),
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
                let t: Transform = Arc::new(move |items: Items| {
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
        "call", "tap", "split", "filter", "take", "together", "each", "catch", "finalize", "retry",
        "timeout", "use", "branch", "emit",
    ];
    let Some(map) = v.as_object() else {
        return Err(malformed(path, "a stage is a map with one key"));
    };
    let mut main: Vec<&String> = map
        .keys()
        .filter(|k| !["with", "into", "on"].contains(&k.as_str()))
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
            Stage::Call {
                act: text(body, "`call` names one act, such as `git.worktree_add`")?,
                with: with(),
                keep: keep(into),
            }
        }
        "tap" => {
            takes(true, false)?;
            Stage::Call {
                act: text(body, "`tap` names one act, such as `git.refresh_branch`")?,
                with: with(),
                keep: Keep::Tap,
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
