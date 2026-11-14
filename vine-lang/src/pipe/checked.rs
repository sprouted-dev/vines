//! A checked vine, [`Pipe`], and the ways to run it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::future::ready;
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};
use serde_json::Value;

use super::book::{Book, Cx, ReplayError};
use super::drive::Saving;
use super::flow::{fatal, Flowing, Items, Transform};
use super::output::PipeOutput;
use super::timer::Timeouts;
use crate::error::RunError;
use crate::journal::{Bookmark, Event, Journal, Progress};
use crate::shape::Shape;

/// A checked vine, ready to run.
#[derive(Clone)]
pub struct Pipe {
    pub(super) name: String,
    pub(super) input: Shape,
    pub(super) outputs: Arc<BTreeMap<String, Shape>>,
    pub(super) reach: BTreeSet<String>,
    /// The operators its `call` and `tap` name, here and in the vines it
    /// uses.
    pub(super) calls: BTreeSet<String>,
    /// The vines it runs with `use`, and the vines those use.
    pub(super) uses: BTreeSet<String>,
    /// Why the vine cannot be resumed, if it cannot.
    pub(super) unresumable: Option<String>,
    /// Why the vine cannot run from a saved position, if it cannot.
    pub(super) unpositioned: Option<String>,
    /// The stream operator calls saved item by item, and why.
    pub(super) saves_every_item: Vec<SavesEveryItem>,
    /// Some stream operator call, here or in a vine this one uses, is
    /// saved by position.
    pub(super) by_position: bool,
    /// It holds a `mergeMap`, here or in a vine it uses.
    pub(super) merges: bool,
    /// Which saved operators each `timeout` applies to, to refuse a
    /// journal saved before one was added.
    pub(super) timeouts: Timeouts,
    pub(super) transform: Transform,
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
    /// the entries a stream operator saved by position has moved past.
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
        match self.book(Book::from_journal(journal)) {
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
        let (position, done) = (bookmark.position(), bookmark.done().collect());
        match self.book(Book::from_bookmark(bookmark)) {
            Ok(book) => self.drive(
                input,
                Saving::Position {
                    book,
                    position,
                    done,
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
    pub(super) fn flow(&self, items: Items, cx: Cx) -> Items {
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
