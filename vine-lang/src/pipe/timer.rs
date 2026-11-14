//! `timeout`, with what the clock decided saved, so a replay never asks
//! the clock about a decision already made.
//!
//! Each run of the covered stages for one value is a scope of its own (see
//! [`Cx::timed_run`]), so the keys of the acts it covers name the run.
//!
//! - When the covered stages are done before the limit, the run saves
//!   [`Timing::InTime`] and only then lets what they gave go on. Every line
//!   the covered stages made comes before it.
//! - When the limit comes first, the run saves [`Timing::TimedOut`] and
//!   only then drops the covered stages. A stream act saved by position
//!   that is dropped saves that it was abandoned, and that line comes after
//!   the decision, never without it.
//!
//! On replay a saved `TimedOut` fails the item at once and the covered
//! stages are not run, so nothing they did is done again and nothing they
//! were doing is started again. A saved `InTime` runs them with no limit:
//! their acts replay from their saved results, and a stream act asked to
//! go on from its position is given all the time it takes.
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
use super::flow::{covering, one, Fault, Flow, Flowing, OpFault, Transform};
use super::tag::Tag;
use crate::journal::{Entry, Timing};
use crate::registry::OpError;

/// The `timeout` at `path`: fail an item whose run of `body` takes longer
/// than `limit`, and hold what `body` gives until it is done.
pub(super) fn timed(body: Transform, path: String, limit: Duration) -> Transform {
    covering(move |item, tag, cx| {
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

/// Which acts each `timeout` of a vine covers, here and in the vines it
/// runs by `use`, so a replay can refuse a journal or bookmark saved
/// before a `timeout` was added. The key of an act a `timeout` covers
/// holds the scope of a run of that `timeout`, such as
/// `pipe[3].timeout@0/pipe[1]#0`. A saved key of such an act without one
/// was made by the vine as it was before, and this run would never ask
/// for it: the act would run again and its outputs come with other ids.
#[derive(Debug, Clone, Default)]
pub(super) struct Covered {
    /// The path of every act, such as `pipe[1]` or `pipe[2]>job_run:pipe[0]`.
    acts: Vec<String>,
    /// An act's path, and the path of a `timeout` that covers it.
    timed: Vec<(String, String)>,
}

impl Covered {
    /// How many acts are known so far. The acts after this mark are the
    /// ones checked since.
    pub(super) fn mark(&self) -> usize {
        self.acts.len()
    }

    /// The act at `path` was checked.
    pub(super) fn act(&mut self, path: &str) {
        self.acts.push(path.to_owned());
    }

    /// The `timeout` at `path` covers every act checked since `mark`.
    pub(super) fn timeout(&mut self, mark: usize, path: &str) {
        let covered = self.acts[mark..]
            .iter()
            .map(|a| (a.clone(), path.to_owned()));
        self.timed.extend(covered.collect::<Vec<_>>());
    }

    /// The `use` at `path` runs `vine`, whose acts and timeouts are these.
    pub(super) fn used(&mut self, path: &str, vine: &str, other: &Covered) {
        let within = |p: &String| format!("{path}>{vine}:{p}");
        self.acts.extend(other.acts.iter().map(within));
        self.timed
            .extend(other.timed.iter().map(|(a, t)| (within(a), within(t))));
    }

    /// Refuse a saved key of an act a `timeout` covers that holds no run of
    /// that `timeout`.
    pub(super) fn check<'a>(&self, keys: impl Iterator<Item = &'a Key>) -> Result<(), String> {
        if self.timed.is_empty() {
            return Ok(());
        }
        for key in keys {
            let (scopes, last) = key.at.rsplit_once('/').unwrap_or(("", &key.at));
            let Some((act, _)) = last.rsplit_once('#') else {
                continue;
            };
            let unscoped = self
                .timed
                .iter()
                .find(|(a, t)| a == act && !scopes.split('/').any(|s| runs(s, t)));
            if let Some((_, timeout)) = unscoped {
                return Err(format!(
                    "`{key}` is an act the `timeout` at {timeout} covers, saved with no run of that `timeout` in its key: it was saved before the `timeout` was added"
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
/// covered stages.
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
