//! The runner.
//!
//! A run checks the input against the declared shape, runs the steps, and
//! ends with exactly one declared output. A step failure no `catch`
//! handles ends the run with a [`RunError`], never with an output.
//!
//! Cancellation is cooperative. The runner checks the token before each
//! step and hands it to operators, which should return
//! [`OpError::cancelled`](crate::OpError::cancelled) when it fires.
//! `finally` blocks run with a fresh token, so cleanup is not cut short by
//! the cancellation that caused it. Dropping the run future instead of
//! cancelling it skips `finally`: Rust has no async drop.
//!
//! # Stream runs
//!
//! [`Vine::run_stream`] pulls source items one at a time and pushes each
//! through the whole pipe before pulling the next; that is the only
//! backpressure there is, and it keeps order. Between items it wakes any
//! stream operator whose deadline has passed. Results go to the caller's
//! [`StreamSink`] as they come out the end, followed by a [`Checkpoint`]
//! whenever the position or kept memory moves. The caller saves those.
//!
//! - **Pause**: cancel the token. The run stops between items (or inside
//!   one, when a step sees the token) without flushing. Items held by
//!   rebuildable operators and the item in flight are not consumed.
//! - **Crash**: drop the run future. Resume from the last checkpoint the
//!   sink received.
//! - **End**: the source closed. Each stream operator, first to last,
//!   flushes and runs its end part; what it emits flows on down the pipe.
//!   The final checkpoint has no memory: it is dropped.
//! - **Failure**: a step fails. The run stops; the checkpoint stays before
//!   the item or batch that failed, and a resumed run retries it.
//!
//! A checkpoint's kept memory always matches its position: the runner
//! saves each kept operator's memory after each call, labelled with the
//! last source item it reflects, and checkpoints the newest one not past
//! the position.

use std::collections::{BTreeMap, VecDeque};

use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::error::RunError;
use crate::expr::{case_key, Env, Template};
use crate::registry::{OpContext, OpError, Registered};
use crate::shape::Shape;
use crate::stream::{
    Checkpoint, Emit, Memory, Stop, StreamOperator, StreamOperatorSpec, StreamReport, StreamSink,
};
use crate::vine::{Kind, Mode, PipeKind, Step, Vine};

/// The declared output a run ended with.
#[derive(Debug, Clone, PartialEq)]
pub struct Output {
    pub name: String,
    pub payload: Value,
}

enum Flow {
    Next,
    End(Output),
}

enum Fault {
    Op {
        path: String,
        operator: String,
        error: OpError,
    },
    Fatal(RunError),
}

type Outputs = BTreeMap<String, Shape>;

impl Vine {
    /// Run a once-mode vine.
    pub async fn run(&self, input: Value) -> Result<Output, RunError> {
        self.run_with(input, CancellationToken::new()).await
    }

    /// Run a once-mode vine that the caller may cancel.
    pub async fn run_with(
        &self,
        input: Value,
        cancel: CancellationToken,
    ) -> Result<Output, RunError> {
        if self.mode != Mode::Once {
            return Err(self.wrong_mode());
        }
        self.run_item(input, &cancel).await
    }

    fn wrong_mode(&self) -> RunError {
        RunError::WrongMode {
            vine: self.name.clone(),
            mode: self.mode.as_str(),
        }
    }

    async fn run_item(&self, input: Value, cancel: &CancellationToken) -> Result<Output, RunError> {
        self.input.check(&input).map_err(RunError::InputMisfit)?;
        let mut env = Env::new();
        env.insert("in".into(), input);
        let cx = OpContext::new(cancel.clone());
        match exec_block(&self.steps, env, &cx, &self.outputs).await {
            Ok(Flow::End(out)) => Ok(out),
            // The loader refuses steps that can finish without an output.
            Ok(Flow::Next) => unreachable!("loader guarantees every path ends in an output"),
            Err(Fault::Op {
                path,
                operator,
                error,
            }) => Err(if error.code == OpError::CANCELLED {
                RunError::Cancelled { path }
            } else {
                RunError::Unhandled {
                    path,
                    operator,
                    code: error.code,
                    message: error.message,
                }
            }),
            Err(Fault::Fatal(e)) => Err(e),
        }
    }
}

fn exec_block<'a>(
    steps: &'a [Step],
    mut env: Env,
    cx: &'a OpContext,
    outputs: &'a Outputs,
) -> BoxFuture<'a, Result<Flow, Fault>> {
    async move {
        for step in steps {
            if let Flow::End(out) = exec_step(step, &mut env, cx, outputs).await? {
                return Ok(Flow::End(out));
            }
        }
        Ok(Flow::Next)
    }
    .boxed()
}

async fn exec_step(
    step: &Step,
    env: &mut Env,
    cx: &OpContext,
    outputs: &Outputs,
) -> Result<Flow, Fault> {
    match &step.kind {
        Kind::Op { op, settings, bind } => {
            let result = call_op(&step.path, op, settings, env, cx).await?;
            if let Some(b) = bind {
                env.insert(b.clone(), result);
            }
            Ok(Flow::Next)
        }
        Kind::Guard { when, out, with } => {
            if when.eval(env) {
                end(&step.path, out, with.resolve(env), outputs)
            } else {
                Ok(Flow::Next)
            }
        }
        Kind::Out { name, with } => end(&step.path, name, with.resolve(env), outputs),
        Kind::Switch {
            on,
            cases,
            otherwise,
        } => {
            let key = case_key(&on.eval(env));
            let branch = key.and_then(|k| cases.get(&k)).or(otherwise.as_ref());
            match branch {
                Some(b) => exec_block(b, env.clone(), cx, outputs).await,
                None => Ok(Flow::Next),
            }
        }
        Kind::Try {
            body,
            catch,
            finally,
        } => {
            let mut result = exec_block(body, env.clone(), cx, outputs).await;
            if let Err(Fault::Op { error, .. }) = &result {
                if let Some(handler) = catch.get(&error.code) {
                    let mut caught = env.clone();
                    caught.insert(
                        "error".into(),
                        json!({ "code": error.code, "message": error.message }),
                    );
                    result = exec_block(handler, caught, cx, outputs).await;
                }
            }
            if !finally.is_empty() {
                // Shielded: cleanup gets a token the caller has not cancelled.
                let shielded = OpContext::new(CancellationToken::new());
                match exec_block(finally, env.clone(), &shielded, outputs).await {
                    Ok(_) => {}
                    Err(Fault::Op {
                        path,
                        operator,
                        error,
                    }) => {
                        return Err(Fault::Fatal(RunError::CleanupFailed {
                            path,
                            operator,
                            code: error.code,
                            message: error.message,
                            masked: describe(&result),
                        }))
                    }
                    Err(fatal) => return Err(fatal),
                }
            }
            result
        }
    }
}

async fn call_op(
    path: &str,
    op: &Registered,
    settings: &Template,
    env: &Env,
    cx: &OpContext,
) -> Result<Value, Fault> {
    let name = &op.spec.name;
    if cx.cancellation().is_cancelled() {
        return Err(Fault::Op {
            path: path.into(),
            operator: name.clone(),
            error: OpError::cancelled("cancelled before the step started"),
        });
    }
    let resolved = settings.resolve(env);
    op.spec.settings.check(&resolved).map_err(|problem| {
        Fault::Fatal(RunError::SettingsMisfit {
            path: path.into(),
            operator: name.clone(),
            problem,
        })
    })?;
    let result = (op.handler)(resolved, cx.clone())
        .await
        .map_err(|error| Fault::Op {
            path: path.into(),
            operator: name.clone(),
            error,
        })?;
    if let Some(shape) = &op.spec.returns {
        shape.check(&result).map_err(|problem| {
            Fault::Fatal(RunError::ResultMisfit {
                path: path.into(),
                operator: name.clone(),
                problem,
            })
        })?;
    }
    Ok(result)
}

fn end(path: &str, name: &str, payload: Value, outputs: &Outputs) -> Result<Flow, Fault> {
    outputs[name].check(&payload).map_err(|problem| {
        Fault::Fatal(RunError::OutputMisfit {
            path: path.into(),
            output: name.into(),
            problem,
        })
    })?;
    Ok(Flow::End(Output {
        name: name.into(),
        payload,
    }))
}

fn describe(r: &Result<Flow, Fault>) -> String {
    match r {
        Ok(Flow::End(o)) => format!("output `{}`", o.name),
        Ok(Flow::Next) => "the next step".into(),
        Err(Fault::Op {
            operator, error, ..
        }) => format!("error `{}` from `{operator}`", error.code),
        Err(Fault::Fatal(e)) => format!("error: {e}"),
    }
}

// ---------------------------------------------------------------- streams

/// Source positions, inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Span {
    first: u64,
    last: u64,
}

fn union(a: Option<Span>, b: Option<Span>) -> Option<Span> {
    match (a, b) {
        (Some(a), Some(b)) => Some(Span {
            first: a.first.min(b.first),
            last: a.last.max(b.last),
        }),
        (a, b) => a.or(b),
    }
}

/// A value flowing down the pipe.
struct Elem {
    value: Value,
    /// The source positions it came from: `$span`.
    lineage: Option<Span>,
    /// The positions consumed once it comes out the end.
    owes: Option<Span>,
}

enum Stage<'v> {
    Map {
        path: &'v str,
        op: &'v Registered,
        settings: &'v Template,
    },
    Each {
        steps: &'v [Step],
    },
    Flow(Box<FlowStage<'v>>),
}

struct FlowStage<'v> {
    path: &'v str,
    spec: &'v StreamOperatorSpec,
    inst: Box<dyn StreamOperator>,
    /// Positions pushed in and not yet released.
    held: Option<Span>,
    /// Kept memory: the last source item it reflects.
    key: u64,
    /// Kept memory: snapshots by key, oldest first.
    snaps: VecDeque<(u64, Value)>,
}

/// What to do at a stage, copied out so `self` is free again.
enum At<'v> {
    End,
    Map(&'v str, &'v Registered, &'v Template),
    Each(&'v [Step]),
    Flow,
}

struct PipeRun<'v> {
    vine: &'v Vine,
    stages: Vec<Stage<'v>>,
    cx: OpContext,
    pulled: u64,
    position: u64,
    released: BTreeMap<u64, u64>,
    last: Checkpoint,
}

enum Event {
    Pause,
    Wake,
    Item(Value),
    End,
}

impl Vine {
    /// Run a stream-mode vine over `source`, starting from `from`
    /// ([`Checkpoint::start`] for a fresh start). `source` yields the items
    /// after `from.position`. Results and checkpoints go to `sink`.
    /// Cancelling `cancel` pauses the run.
    pub async fn run_stream<S, K>(
        &self,
        mut source: S,
        from: Checkpoint,
        sink: &mut K,
        cancel: CancellationToken,
    ) -> StreamReport
    where
        S: Stream<Item = Value> + Unpin,
        K: StreamSink,
    {
        if self.mode != Mode::Stream {
            return StreamReport {
                stop: Stop::Failed(self.wrong_mode()),
                checkpoint: from,
            };
        }
        let mut run = match PipeRun::start(self, &from, &cancel) {
            Ok(run) => run,
            Err(e) => {
                return StreamReport {
                    stop: Stop::Failed(e),
                    checkpoint: from,
                }
            }
        };
        let sink: &mut dyn StreamSink = sink;
        loop {
            let deadline = run.deadline();
            let event = tokio::select! {
                biased;
                _ = cancel.cancelled() => Event::Pause,
                _ = sleep_until(deadline) => Event::Wake,
                next = source.next() => next.map_or(Event::End, Event::Item),
            };
            let done = match event {
                Event::Pause => return run.stop(Stop::Paused),
                Event::Item(v) => run.item(v, sink).await,
                Event::Wake => run.wake(sink).await,
                Event::End => match run.end(sink).await {
                    Ok(()) => return run.ended(sink),
                    Err(fault) => Err(fault),
                },
            };
            match done {
                Ok(()) => run.quiesce(sink),
                Err(fault) => return run.stop(stop_for(fault)),
            }
        }
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

fn stop_for(fault: Fault) -> Stop {
    match fault {
        Fault::Op { error, .. } if error.code == OpError::CANCELLED => Stop::Paused,
        Fault::Op {
            path,
            operator,
            error,
        } => Stop::Failed(RunError::Unhandled {
            path,
            operator,
            code: error.code,
            message: error.message,
        }),
        Fault::Fatal(e) => Stop::Failed(e),
    }
}

fn item_env(item: Value, lineage: Option<Span>) -> Env {
    let mut env = Env::new();
    env.insert("item".into(), item);
    env.insert(
        "span".into(),
        match lineage {
            Some(s) => json!({ "first": s.first, "last": s.last }),
            None => json!({ "first": null, "last": null }),
        },
    );
    env
}

impl<'v> PipeRun<'v> {
    fn start(
        vine: &'v Vine,
        from: &Checkpoint,
        cancel: &CancellationToken,
    ) -> Result<Self, RunError> {
        let mut stages = Vec::new();
        let mut unclaimed: Vec<&String> = from.memory.keys().collect();
        for step in &vine.pipe {
            stages.push(match &step.kind {
                PipeKind::Map { op, settings } => Stage::Map {
                    path: &step.path,
                    op,
                    settings,
                },
                PipeKind::Each { steps } => Stage::Each { steps },
                PipeKind::Flow { op, settings } => {
                    let spec: &StreamOperatorSpec = &op.spec;
                    let mut inst =
                        (op.factory)(settings).map_err(|problem| RunError::SettingsMisfit {
                            path: step.path.clone(),
                            operator: spec.name.clone(),
                            problem,
                        })?;
                    let saved = from.memory.get(&step.path);
                    unclaimed.retain(|k| **k != step.path);
                    let mut snaps = VecDeque::new();
                    match (spec.memory, saved) {
                        (Memory::Kept, Some(m)) => {
                            inst.restore(m.clone()).map_err(|p| {
                                RunError::ResumeMisfit(format!(
                                    "memory for `{}` (`{}`) does not fit: {p}",
                                    step.path, spec.name
                                ))
                            })?;
                        }
                        (Memory::Kept, None) if from.position > 0 => {
                            return Err(RunError::ResumeMisfit(format!(
                                "no saved memory for `{}` (`{}`), which must be kept; resuming at position {} without it would lose what it remembered",
                                step.path, spec.name, from.position
                            )));
                        }
                        (Memory::Kept, None) => {}
                        (_, Some(_)) => {
                            return Err(RunError::ResumeMisfit(format!(
                                "saved memory names `{}` (`{}`), which keeps no memory",
                                step.path, spec.name
                            )));
                        }
                        (_, None) => {}
                    }
                    if spec.memory == Memory::Kept {
                        snaps.push_back((from.position, inst.save()));
                    }
                    Stage::Flow(Box::new(FlowStage {
                        path: &step.path,
                        spec,
                        inst,
                        held: None,
                        key: from.position,
                        snaps,
                    }))
                }
            });
        }
        if let Some(k) = unclaimed.first() {
            return Err(RunError::ResumeMisfit(format!(
                "saved memory names `{k}`, which is no step of this vine that keeps memory"
            )));
        }
        let mut run = PipeRun {
            vine,
            stages,
            cx: OpContext::new(cancel.clone()),
            pulled: from.position,
            position: from.position,
            released: BTreeMap::new(),
            last: Checkpoint::start(),
        };
        run.last = run.checkpoint();
        Ok(run)
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        self.stages
            .iter()
            .filter_map(|s| match s {
                Stage::Flow(f) => f.inst.deadline(),
                _ => None,
            })
            .min()
    }

    async fn item(&mut self, value: Value, sink: &mut dyn StreamSink) -> Result<(), Fault> {
        self.vine
            .input
            .check(&value)
            .map_err(|e| Fault::Fatal(RunError::InputMisfit(e)))?;
        self.pulled += 1;
        let span = Some(Span {
            first: self.pulled,
            last: self.pulled,
        });
        let elem = Elem {
            value,
            lineage: span,
            owes: span,
        };
        self.feed(0, elem, sink).await
    }

    async fn wake(&mut self, sink: &mut dyn StreamSink) -> Result<(), Fault> {
        let now = tokio::time::Instant::now();
        for idx in 0..self.stages.len() {
            let cx = self.cx.clone();
            let Stage::Flow(f) = &mut self.stages[idx] else {
                continue;
            };
            if f.inst.deadline().is_none_or(|d| d > now) {
                continue;
            }
            let (path, name) = (f.path, &f.spec.name);
            let mut out = Emit::default();
            f.inst
                .wake(&mut out, &cx)
                .await
                .map_err(|error| Fault::Op {
                    path: path.into(),
                    operator: name.clone(),
                    error,
                })?;
            if f.inst.deadline().is_some_and(|d| d <= now) {
                return Err(Fault::Fatal(RunError::DeadlineNotMoved {
                    path: path.into(),
                    operator: name.clone(),
                }));
            }
            let elems = self.settle(idx, out.take(), None, false)?;
            for e in elems {
                self.feed(idx + 1, e, sink).await?;
            }
        }
        Ok(())
    }

    async fn end(&mut self, sink: &mut dyn StreamSink) -> Result<(), Fault> {
        for idx in 0..self.stages.len() {
            let cx = self.cx.clone();
            let Stage::Flow(f) = &mut self.stages[idx] else {
                continue;
            };
            let (path, name) = (f.path, &f.spec.name);
            let mut out = Emit::default();
            f.inst.end(&mut out, &cx).await.map_err(|error| Fault::Op {
                path: path.into(),
                operator: name.clone(),
                error,
            })?;
            let elems = self.settle(idx, out.take(), None, true)?;
            for e in elems {
                self.feed(idx + 1, e, sink).await?;
            }
        }
        Ok(())
    }

    /// Push one value into stage `idx` and on to the end of the pipe.
    fn feed<'a>(
        &'a mut self,
        idx: usize,
        elem: Elem,
        sink: &'a mut dyn StreamSink,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        async move {
            let vine = self.vine;
            let cx = self.cx.clone();
            let at = match self.stages.get(idx) {
                None => At::End,
                Some(Stage::Map { path, op, settings }) => At::Map(path, op, settings),
                Some(Stage::Each { steps }) => At::Each(steps),
                Some(Stage::Flow(_)) => At::Flow,
            };
            match at {
                At::End => {
                    // The pipe ends in an operator: each result is the one output.
                    let name = vine
                        .pipe_output
                        .as_deref()
                        .expect("loader names the output of a pipe not ending in `each`");
                    let path = &vine.pipe[idx - 1].path;
                    let Flow::End(out) = end(path, name, elem.value, &vine.outputs)? else {
                        unreachable!()
                    };
                    sink.output(out);
                    self.release(elem.owes);
                    Ok(())
                }
                At::Map(path, op, settings) => {
                    let env = item_env(elem.value, elem.lineage);
                    let value = call_op(path, op, settings, &env, &cx).await?;
                    let next = Elem {
                        value,
                        lineage: elem.lineage,
                        owes: elem.owes,
                    };
                    self.feed(idx + 1, next, sink).await
                }
                At::Each(steps) => {
                    let env = item_env(elem.value, elem.lineage);
                    match exec_block(steps, env, &cx, &vine.outputs).await? {
                        Flow::End(out) => {
                            sink.output(out);
                            self.release(elem.owes);
                            Ok(())
                        }
                        Flow::Next => {
                            unreachable!("loader guarantees every path in `each` ends in an output")
                        }
                    }
                }
                At::Flow => {
                    let Some(Stage::Flow(f)) = self.stages.get_mut(idx) else {
                        unreachable!()
                    };
                    f.held = union(f.held, elem.owes);
                    let (path, name) = (f.path, &f.spec.name);
                    let mut out = Emit::default();
                    f.inst
                        .push(elem.value, &mut out, &cx)
                        .await
                        .map_err(|error| Fault::Op {
                            path: path.into(),
                            operator: name.clone(),
                            error,
                        })?;
                    let elems = self.settle(idx, out.take(), elem.lineage, false)?;
                    for e in elems {
                        self.feed(idx + 1, e, sink).await?;
                    }
                    Ok(())
                }
            }
        }
        .boxed()
    }

    /// After a call on stream stage `idx`: check what it emitted, decide
    /// what it released, and snapshot kept memory.
    fn settle(
        &mut self,
        idx: usize,
        values: Vec<Value>,
        input: Option<Span>,
        ending: bool,
    ) -> Result<Vec<Elem>, Fault> {
        let Some(Stage::Flow(f)) = self.stages.get_mut(idx) else {
            unreachable!("settle is called on stream stages")
        };
        if let Some(shape) = &f.spec.emits {
            for v in &values {
                shape.check(v).map_err(|problem| {
                    Fault::Fatal(RunError::ResultMisfit {
                        path: f.path.into(),
                        operator: f.spec.name.clone(),
                        problem,
                    })
                })?;
            }
        }
        let kept = f.spec.memory == Memory::Kept;
        let released = if ending || kept || !f.inst.holding() {
            f.held.take()
        } else {
            None
        };
        if kept {
            if let Some(s) = input {
                f.key = f.key.max(s.last);
            }
            let snap = f.inst.save();
            if f.snaps.back().is_some_and(|(k, _)| *k == f.key) {
                f.snaps.pop_back();
            }
            f.snaps.push_back((f.key, snap));
        }
        let lineage = released.or(input);
        let n = values.len();
        let elems = values
            .into_iter()
            .enumerate()
            .map(|(i, value)| Elem {
                value,
                lineage,
                owes: if i + 1 == n { released } else { None },
            })
            .collect();
        if n == 0 {
            self.release(released);
        }
        Ok(elems)
    }

    fn release(&mut self, span: Option<Span>) {
        let Some(s) = span else { return };
        if s.last <= self.position {
            return;
        }
        let first = s.first.max(self.position + 1);
        let e = self.released.entry(first).or_insert(s.last);
        *e = (*e).max(s.last);
        while let Some(last) = self.released.remove(&(self.position + 1)) {
            self.position = last;
        }
    }

    /// The position and, for each kept stage, the newest memory snapshot
    /// not past it. Older snapshots are dropped.
    fn checkpoint(&mut self) -> Checkpoint {
        let position = self.position;
        let mut memory = BTreeMap::new();
        for s in &mut self.stages {
            let Stage::Flow(f) = s else { continue };
            if f.spec.memory != Memory::Kept {
                continue;
            }
            let i = f
                .snaps
                .iter()
                .rposition(|(k, _)| *k <= position)
                .expect("the starting snapshot is never past the position");
            f.snaps.drain(..i);
            memory.insert(f.path.to_owned(), f.snaps[0].1.clone());
        }
        Checkpoint { position, memory }
    }

    /// Between items, with nothing in flight: tell the sink if anything moved.
    fn quiesce(&mut self, sink: &mut dyn StreamSink) {
        let cp = self.checkpoint();
        if cp != self.last {
            sink.checkpoint(&cp);
            self.last = cp;
        }
    }

    fn stop(self, stop: Stop) -> StreamReport {
        StreamReport {
            stop,
            checkpoint: self.last,
        }
    }

    fn ended(self, sink: &mut dyn StreamSink) -> StreamReport {
        let cp = Checkpoint {
            position: self.position,
            memory: BTreeMap::new(),
        };
        if cp != self.last {
            sink.checkpoint(&cp);
        }
        StreamReport {
            stop: Stop::Ended,
            checkpoint: cp,
        }
    }
}
