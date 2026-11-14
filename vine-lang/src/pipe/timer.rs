//! `timeout`, with what the clock decided saved, so a replay never asks
//! the clock about a decision already made.
//!
//! Each run of the operators a timeout applies to, for one value, is a
//! scope of its own (see [`Cx::timed_run`]), so the keys of the saved
//! calls made in it name the run.
//!
//! - When those operators are done before the limit, the run saves
//!   [`Timing::InTime`] and only then lets what they gave go on. Every line
//!   they made comes before it.
//! - When the limit comes first, the run saves [`Timing::TimedOut`] and
//!   only then drops them. A stream operator saved by position that is
//!   dropped saves that it was abandoned, and that line comes after the
//!   decision, never without it.
//!
//! On replay a saved `TimedOut` fails the item at once and the operators
//! the timeout applies to are not run, so nothing they did is done again
//! and nothing they were doing is started again. A saved `InTime` runs
//! them with no limit: their calls replay from their saved results, and a
//! stream operator asked to go on from its position is given all the time
//! it takes.
//!
//! With no decision saved, the wait starts again from nothing. The library
//! has no clock that runs across a restart: tokio's clock starts again in
//! each process. A crash before the decision is saved hands the caller
//! nothing that depends on it, so a replay that decides the other way is
//! one the caller cannot tell from a first run at another pace.

use std::time::Duration;

use futures::{FutureExt, StreamExt};
use serde_json::Value;

use super::book::{Cx, Key};
use super::flow::{one, per_item_held, Fault, Flow, Flowing, OpFault, Transform};
use super::tag::Tag;
use crate::journal::{Entry, Timing};
use crate::registry::OpError;

/// The `timeout` at `path`: fail an item whose run of `body` takes longer
/// than `limit`, and hold what `body` gives until it is done.
pub(super) fn timed(body: Transform, path: String, limit: Duration) -> Transform {
    per_item_held(move |item, tag, cx| {
        let (body, path) = (body.clone(), path.clone());
        async move {
            let (scoped, key) = cx.timed_run(&path, &tag);
            let saved = key.as_ref().and_then(|k| cx.take_timing(k));
            if saved == Some(Timing::TimedOut) {
                return vec![Err(timed_out(&path, limit, item, tag))];
            }
            let mut work = body(one(Ok(Flowing::Value(item.clone(), scoped))), cx.clone())
                .collect::<Vec<Flow>>();
            if saved == Some(Timing::InTime) {
                return work.await;
            }
            // Biased, so work done at the very moment of the limit is in
            // time, every run.
            let done = tokio::select! {
                biased;
                out = &mut work => Some(out),
                () = tokio::time::sleep(limit) => None,
            };
            match done {
                Some(out) => {
                    decide(&cx, key.as_ref(), Timing::InTime);
                    out
                }
                None => {
                    decide(&cx, key.as_ref(), Timing::TimedOut);
                    drop(work);
                    vec![Err(timed_out(&path, limit, item, tag))]
                }
            }
        }
        .boxed()
    })
}

/// Which saved calls each `timeout` of a vine applies to, here and in the
/// vines it runs by `use`, so a replay can refuse a journal or bookmark
/// saved before a `timeout` was added. The key of a call a `timeout`
/// applies to holds the scope of a run of that `timeout`, such as
/// `pipe[3].timeout@0/pipe[1]#0`. A saved key of such a call without one
/// was made by the vine as it was before, and this run would never ask
/// for it: the call would be made again and its outputs come with other
/// ids.
#[derive(Debug, Clone, Default)]
pub(super) struct Timeouts {
    /// The path of every `call` and `tap`, such as `pipe[1]` or
    /// `pipe[2]>job_run:pipe[0]`.
    calls: Vec<String>,
    /// A call's path, and the path of a `timeout` that applies to it.
    timed: Vec<(String, String)>,
}

impl Timeouts {
    /// How many calls are known so far. The calls after this mark are the
    /// ones checked since.
    pub(super) fn mark(&self) -> usize {
        self.calls.len()
    }

    /// The `call` or `tap` at `path` was checked.
    pub(super) fn call(&mut self, path: &str) {
        self.calls.push(path.to_owned());
    }

    /// The `timeout` at `path` applies to every call checked since `mark`.
    pub(super) fn timeout(&mut self, mark: usize, path: &str) {
        let timed = self.calls[mark..]
            .iter()
            .map(|c| (c.clone(), path.to_owned()));
        self.timed.extend(timed.collect::<Vec<_>>());
    }

    /// The `use` at `path` runs `vine`, whose calls and timeouts are these.
    pub(super) fn used(&mut self, path: &str, vine: &str, other: &Timeouts) {
        let within = |p: &String| format!("{path}>{vine}:{p}");
        self.calls.extend(other.calls.iter().map(within));
        self.timed
            .extend(other.timed.iter().map(|(c, t)| (within(c), within(t))));
    }

    /// Refuse a saved key of a call a `timeout` applies to that holds no
    /// run of that `timeout`.
    pub(super) fn check<'a>(&self, keys: impl Iterator<Item = &'a Key>) -> Result<(), String> {
        if self.timed.is_empty() {
            return Ok(());
        }
        for key in keys {
            let (scopes, last) = key.at.rsplit_once('/').unwrap_or(("", &key.at));
            let Some((call, _)) = last.rsplit_once('#') else {
                continue;
            };
            let unscoped = self
                .timed
                .iter()
                .find(|(c, t)| c == call && !scopes.split('/').any(|s| runs(s, t)));
            if let Some((_, timeout)) = unscoped {
                return Err(format!(
                    "`{key}` is a call the `timeout` at {timeout} applies to, saved with no run of that `timeout` in its key: it was saved before the `timeout` was added"
                ));
            }
        }
        Ok(())
    }
}

/// Whether one scope of a key, such as `pipe[3].timeout@0`, is a run of
/// the `timeout` at `path`.
fn runs(scope: &str, path: &str) -> bool {
    scope
        .strip_prefix(path)
        .and_then(|rest| rest.strip_prefix(".timeout@"))
        .is_some_and(|n| n.parse::<u64>().is_ok())
}

fn decide(cx: &Cx, key: Option<&Key>, timing: Timing) {
    if let Some(key) = key {
        let at = key.at.clone();
        cx.save(key, Entry::Timed { at, timing });
    }
}

/// The failure a `timeout` gives, held for the item as it reached the
/// operators the timeout applies to.
fn timed_out(path: &str, limit: Duration, item: Value, tag: Tag) -> Fault {
    Fault::Op(Box::new(OpFault {
        path: path.to_owned(),
        from: "timeout".into(),
        error: OpError::new(
            "timeout",
            format!("took longer than {}s", limit.as_secs_f64()),
        ),
        item,
        tag,
    }))
}
