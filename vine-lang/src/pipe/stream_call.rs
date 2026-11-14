//! One call of a stream act, as it goes: first what was saved, then live.
//!
//! A call is saved in one of two ways.
//!
//! - Every item: each item the act gives is saved, and a replay gives the
//!   saved items again itself before it asks the act for the rest.
//! - By position, for an act that gives its items again: the items are
//!   numbered and tagged, and the run saves only how far the call got (an
//!   [`Entry::Through`]) and how it stopped (an [`Entry::Stopped`]). A
//!   replay asks the act for the items after the position, so the stages
//!   after it rebuild what they held the way they do for input items.
//!
//! A call that stops early because a `catch` or `finalize` took over the
//! rest of the item's work saves that it was abandoned, so a replay does
//! not read on past where the first run stopped.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use futures::stream;
use futures::StreamExt;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::book::{Cx, Key, Saved};
use super::flow::{fatal, held, lock, misfit, one, Fault, Flow, Flowing, Items};
use super::tag::{Ledger, Tag};
use super::unit::{add_field, Runner, Unit};
use super::Keep;
use crate::error::RunError;
use crate::journal::{Ending, Entry};
use crate::registry::{ActStream, OpContext, OpError, Part, StreamHandler};

/// How the checker says a stream act call is saved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ItemSaving {
    /// Every item the act gives.
    Every,
    /// How far the call got. The act gives its items again.
    Position,
}

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
            position: None,
        };
        let Some(key) = key else {
            return stream::unfold(run, StreamRun::next).boxed();
        };
        let mut saving = self.saving.get().copied().unwrap_or(ItemSaving::Every);
        let (mut through, mut skip, mut stopped) = (0, 0, None);
        match cx.take(&key) {
            None => {}
            Some(Saved::Stream { input, items, end }) => {
                if input != run.input {
                    return run.misfit_input(&key, &input);
                }
                // A call saved item by item replays that way, whatever
                // the act says now.
                if !items.is_empty() || end.is_some() {
                    saving = ItemSaving::Every;
                }
                run.replay = items;
                run.replay_end = end;
                run.started = true;
            }
            Some(Saved::Position {
                input,
                through: t,
                handed,
                stopped: s,
            }) => {
                if input != run.input {
                    return run.misfit_input(&key, &input);
                }
                if saving == ItemSaving::Every {
                    return one(Err(misfit(format!(
                        "`{key}` was saved by position, and `{}` here saves every item",
                        self.spec.name
                    ))
                    .of(&tag)));
                }
                (through, skip, stopped) = (t, handed, s);
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
        if saving == ItemSaving::Position {
            let call = Arc::new(key.clone());
            if let Some(book) = &cx.book {
                lock(book).open_call(&key, through, skip);
            }
            run.given = through;
            run.position = Some(ByPosition {
                ledger: Arc::new(Mutex::new(Ledger::of_call(
                    through,
                    call.clone(),
                    cx.moves.clone(),
                ))),
                call,
                replayed_from: through,
                stop_saved: stopped.is_some(),
                stopped,
            });
        }
        stream::unfold(run, StreamRun::next).boxed()
    }
}

/// What a call saved by position keeps besides what every call keeps.
struct ByPosition {
    /// Counts the items this call gives as they are done.
    ledger: Arc<Mutex<Ledger>>,
    call: Arc<Key>,
    /// The position the call had when this run took it up.
    replayed_from: u64,
    /// For a replay of a call that had stopped: how many items it gave,
    /// and how it stopped. Taken when the replay reaches that point.
    stopped: Option<(u64, Ending)>,
    /// The stop is saved, before the restart or by this run.
    stop_saved: bool,
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
    /// How many items this call has given, saved ones included. For a call
    /// saved by position it starts at the position.
    given: u64,
    done: bool,
    /// Set when the call is saved by position.
    position: Option<ByPosition>,
}

impl Drop for StreamRun {
    /// A call saved by position that is dropped before it stopped was
    /// abandoned: a `catch` or `finalize` took over, or the caller stopped
    /// reading. In the second case nobody reads the line.
    fn drop(&mut self) {
        let Some(position) = &self.position else {
            return;
        };
        if self.done || !self.started || position.stop_saved {
            return;
        }
        if let Some(at) = &self.key {
            self.cx.save(
                at,
                Entry::Stopped {
                    at: at.at.clone(),
                    items: self.given,
                    how: Ending::Abandoned,
                },
            );
        }
    }
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
        if self.at_saved_stop() {
            return self.give_saved_stop();
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
                if let (Some(at), None) = (&self.key, &self.position) {
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
                if let Some(short) = self.gave_fewer_again("ended") {
                    return self.fail_hard(short);
                }
                self.ended(value)
            }
            Some(Err(error)) => {
                if error.code != OpError::CANCELLED {
                    if let Some(short) = self.gave_fewer_again(&format!(
                        "failed with `{}`: {}",
                        error.code, error.message
                    )) {
                        return self.fail_hard(short);
                    }
                    self.save_failure(&error);
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
                if let Some(short) = self.gave_fewer_again("ended") {
                    return self.fail_hard(short);
                }
                self.ended(Value::Null)
            }
        }
    }

    /// Whether this is a replay of a call saved by position that stopped,
    /// and it has given again every item it gave before it stopped.
    fn at_saved_stop(&self) -> bool {
        let stop = self.position.as_ref().and_then(|p| p.stopped.as_ref());
        stop.is_some_and(|(items, _)| self.given >= *items)
    }

    /// Give what a saved stop says comes after the items: nothing when the
    /// call was abandoned or its end is already done, else its end again.
    /// The act is not read past this point.
    fn give_saved_stop(mut self) -> Option<(Flow, StreamRun)> {
        self.live = None;
        let position = self.position.as_mut()?;
        let (items, how) = position.stopped.take()?;
        let end_done = position.replayed_from > items;
        match how {
            Ending::Abandoned => self.give_nothing(),
            _ if end_done => self.give_nothing(),
            Ending::Ended(value) => self.finish(Ok(value)),
            Ending::Failed { code, message } => self.finish(Err(OpError::new(code, message))),
        }
    }

    /// The call is over and gives nothing more.
    fn give_nothing(mut self) -> Option<(Flow, StreamRun)> {
        self.done = true;
        self.live = None;
        None
    }

    /// For a replay of a call saved by position that stopped: the run
    /// error to give when the act stopped before it gave again every item
    /// it gave the first time.
    fn gave_fewer_again(&self, how: &str) -> Option<RunError> {
        let (items, _) = self.position.as_ref()?.stopped.as_ref()?;
        let key = self.key.as_ref()?;
        (self.given < *items).then(|| RunError::ReplayMisfit {
            problem: format!(
                "`{key}` gave {items} items before it stopped, and asked again it {how} after {}; an act that gives its items again must give the same ones",
                self.given
            ),
        })
    }

    fn save_failure(&mut self, error: &OpError) {
        let Some(at) = &self.key else {
            return;
        };
        let entry = match &mut self.position {
            None => Entry::Failed {
                at: at.at.clone(),
                input: self.input.clone(),
                code: error.code.clone(),
                message: error.message.clone(),
            },
            Some(position) => {
                position.stop_saved = true;
                Entry::Stopped {
                    at: at.at.clone(),
                    items: self.given,
                    how: Ending::Failed {
                        code: error.code.clone(),
                        message: error.message.clone(),
                    },
                }
            }
        };
        self.cx.save(at, entry);
    }

    pub(super) fn ended(mut self, value: Value) -> Option<(Flow, StreamRun)> {
        if let Some(at) = &self.key {
            let entry = match &mut self.position {
                None => Some(Entry::Ended {
                    at: at.at.clone(),
                    value: value.clone(),
                }),
                Some(position) if !position.stop_saved => {
                    position.stop_saved = true;
                    Some(Entry::Stopped {
                        at: at.at.clone(),
                        items: self.given,
                        how: Ending::Ended(value.clone()),
                    })
                }
                Some(_) => None,
            };
            if let Some(entry) = entry {
                self.cx.save(at, entry);
            }
        }
        self.live = None;
        self.finish(Ok(value))
    }

    /// The call is over. What it gives last carries the item's tag; the
    /// call lets go of its own, so the item can be done as soon as that
    /// last value is, without waiting to be asked for more. For a call
    /// saved by position, that last value is one more item of the call.
    pub(super) fn finish(mut self, end: Result<Value, OpError>) -> Option<(Flow, StreamRun)> {
        self.done = true;
        self.live = None;
        let up = std::mem::take(&mut self.tag);
        let tag = match &self.position {
            Some(p) => Tag::given(self.given + 1, &p.ledger, &p.call, &up),
            None => up,
        };
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

    /// The item for the value the act gave last, numbered `given`.
    pub(super) fn item_with(&self, value: Value) -> Flow {
        let tag = match &self.position {
            Some(p) => Tag::given(self.given, &p.ledger, &p.call, &self.tag),
            None => self.tag.clone(),
        };
        match &self.unit.keep {
            Keep::Into(name) => held(
                add_field(&self.unit.path, self.item.clone(), name, value),
                &tag,
            ),
            _ => Ok(Flowing::Value(value, tag)),
        }
    }

    fn misfit_input(self, key: &Key, saved: &Value) -> Items {
        let fault: Fault = misfit(format!(
            "`{key}` was saved for input {saved}, and the run gave it {}",
            self.input
        ));
        one(Err(fault.of(&self.tag)))
    }
}
