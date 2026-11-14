//! One call of a stream act, as it goes: first what was saved, then live.

use std::collections::VecDeque;

use futures::stream;
use futures::StreamExt;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::book::{Cx, Key, Saved};
use super::flow::{fatal, held, misfit, one, Fault, Flow, Flowing, Items};
use super::tag::Tag;
use super::unit::{add_field, Runner, Unit};
use super::Keep;
use crate::error::RunError;
use crate::journal::Entry;
use crate::registry::{ActStream, OpContext, OpError, Part, StreamHandler};

impl Unit {
    /// Run a stream act for one item: one item out per value it gives.
    pub(super) fn stream(&self, item: Value, tag: Tag, cx: Cx) -> Items {
        let Runner::Stream(handler) = &self.runner else {
            unreachable!("only a stream act runs as a stream")
        };
        let input = match self.resolve(&item) {
            Ok(v) => v,
            Err(fault) => return one(Err(fault.of(&tag))),
        };
        let key = cx.key(&self.path, &tag);
        let mut run = StreamRun {
            unit: self.clone(),
            handler: handler.clone(),
            item,
            tag: tag.clone(),
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
                        ))
                        .of(&tag)));
                    }
                    run.replay = items;
                    run.replay_end = end;
                    run.started = true;
                }
                Some(Saved::Once { .. }) => {
                    return one(Err(misfit(format!(
                    "`{key}` was saved as an act that gives one result, and `{}` is a stream act",
                    self.spec.name
                ))
                    .of(&tag)))
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
    tag: Tag,
    input: Value,
    cx: Cx,
    key: Option<Key>,
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
                    self.cx.save(
                        at,
                        Entry::Started {
                            at: at.at.clone(),
                            input: self.input.clone(),
                        },
                    );
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
                    self.cx.save(
                        at,
                        Entry::Item {
                            at: at.at.clone(),
                            value: value.clone(),
                        },
                    );
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
                        self.cx.save(
                            at,
                            Entry::Failed {
                                at: at.at.clone(),
                                input: self.input.clone(),
                                code: error.code.clone(),
                                message: error.message.clone(),
                            },
                        );
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
            self.cx.save(
                at,
                Entry::Ended {
                    at: at.at.clone(),
                    value: value.clone(),
                },
            );
        }
        self.live = None;
        self.finish(Ok(value))
    }

    /// The call is over. What it gives last carries the item's tag; the
    /// call lets go of its own, so the item can be done as soon as that
    /// last value is, without waiting to be asked for more.
    pub(super) fn finish(mut self, end: Result<Value, OpError>) -> Option<(Flow, StreamRun)> {
        self.done = true;
        self.live = None;
        let tag = std::mem::take(&mut self.tag);
        match end {
            Ok(value) => {
                let name = self.unit.end.clone()?;
                let made = add_field(&self.unit.path, self.item.clone(), &name, value);
                Some((held(made, &tag), self))
            }
            Err(error) => {
                let fault = self.unit.failed(error, &self.item, &tag);
                Some((Err(fault), self))
            }
        }
    }

    pub(super) fn fail_hard(mut self, e: RunError) -> Option<(Flow, StreamRun)> {
        self.done = true;
        self.live = None;
        let fault = fatal(e).of(&std::mem::take(&mut self.tag));
        Some((Err(fault), self))
    }

    pub(super) fn item_with(&self, value: Value) -> Flow {
        match &self.unit.keep {
            Keep::Into(name) => {
                self.tagged(add_field(&self.unit.path, self.item.clone(), name, value))
            }
            _ => Ok(Flowing::Value(value, self.tag.clone())),
        }
    }

    /// A value made for this call's item, or its failure, held for it.
    fn tagged(&self, made: Result<Value, Fault>) -> Flow {
        held(made, &self.tag)
    }
}
