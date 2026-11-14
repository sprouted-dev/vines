//! An operator or act, ready to run for one item, and one call of a
//! stream act.

use std::collections::VecDeque;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream;
use futures::{FutureExt, StreamExt};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::book::{Cx, Saved};
use super::flow::{env_of, fatal, misfit, one, Fault, Flow, Flowing, Items, OpFault};
use super::Keep;
use crate::error::RunError;
use crate::expr::Template;
use crate::journal::Entry;
use crate::registry::{ActStream, Handler, OpContext, OpError, OperatorSpec, Part, StreamHandler};

/// How a unit runs.
#[derive(Clone)]
pub(super) enum Runner {
    /// An operator. It reaches nothing outside, so it is not saved.
    Op(Handler),
    /// An act. Its result is saved.
    Act(Handler),
    /// A stream act. Its items and its end are saved.
    Stream(StreamHandler),
}

/// An operator or act, ready to run for one item.
#[derive(Clone)]
pub(super) struct Unit {
    pub(super) path: String,
    pub(super) spec: Arc<OperatorSpec>,
    pub(super) runner: Runner,
    pub(super) with: Template,
    pub(super) keep: Keep,
    pub(super) end: Option<String>,
}

impl Unit {
    pub(super) fn resolve(&self, item: &Value) -> Result<Value, Fault> {
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

    pub(super) fn failed(&self, error: OpError, item: &Value) -> Fault {
        Fault::Op(Box::new(OpFault {
            path: self.path.clone(),
            from: self.spec.name.clone(),
            error,
            item: item.clone(),
        }))
    }

    /// Run for one item and give back the raw result.
    pub(super) fn result(&self, item: Value, cx: Cx) -> BoxFuture<'static, Result<Value, Fault>> {
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
    pub(super) fn apply(&self, item: Value, cx: Cx) -> BoxFuture<'static, Result<Value, Fault>> {
        let unit = self.clone();
        async move {
            let result = unit.result(item.clone(), cx).await?;
            unit.merge(item, result)
        }
        .boxed()
    }

    pub(super) fn merge(&self, item: Value, result: Value) -> Result<Value, Fault> {
        match &self.keep {
            Keep::Replace => Ok(result),
            Keep::Tap => Ok(item),
            Keep::Into(name) => add_field(&self.path, item, name, result),
        }
    }

    /// Run a stream act for one item: one item out per value it gives.
    pub(super) fn stream(&self, item: Value, cx: Cx) -> Items {
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
pub(super) struct StreamRun {
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

    pub(super) fn ended(mut self, value: Value) -> Option<(Flow, StreamRun)> {
        if let Some(at) = &self.key {
            self.cx.save(Entry::Ended {
                at: at.clone(),
                value: value.clone(),
            });
        }
        self.live = None;
        self.finish(Ok(value))
    }

    pub(super) fn finish(mut self, end: Result<Value, OpError>) -> Option<(Flow, StreamRun)> {
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

    pub(super) fn fail_hard(mut self, e: RunError) -> Option<(Flow, StreamRun)> {
        self.done = true;
        self.live = None;
        Some((Err(fatal(e)), self))
    }

    pub(super) fn item_with(&self, value: Value) -> Flow {
        match &self.unit.keep {
            Keep::Into(name) => {
                add_field(&self.unit.path, self.item.clone(), name, value).map(Flowing::Value)
            }
            _ => Ok(Flowing::Value(value)),
        }
    }
}

pub(super) fn add_field(
    path: &str,
    mut item: Value,
    name: &str,
    value: Value,
) -> Result<Value, Fault> {
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
