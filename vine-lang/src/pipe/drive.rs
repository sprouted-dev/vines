//! The loop that runs a checked vine and hands its caller what comes out.
//!
//! One loop serves all three ways to run. It hands out, in this order:
//! the lines to save as soon as they are known, then the lines that say a
//! stream operator call moved, then the next output, and, when it holds
//! nothing, the position if it moved, and then each item done past the
//! position. So an output comes before the line that records it, and that
//! line comes before the position, or the `Done`, that passes its item.
//!
//! Each output gets its id as it is handed out. A saved run asks the book,
//! which counts outputs per input item and group of work. A run that saves
//! nothing has no groups and no input items to name, so it counts the
//! outputs of the whole run.
//!
//! A run from a bookmark skips the input items the bookmark keeps as done
//! past its position: they are numbered and dropped without running.

use std::sync::{Arc, Mutex};
use std::task::Poll;

use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};
use serde_json::Value;

use super::book::{Book, Cx, Handing};
use super::flow::{fatal, lock, run_error, Flow, Flowing};
use super::output::{OutputId, PipeOutput};
use super::tag::{Ledger, Moves, Tag};
use super::Pipe;
use crate::error::RunError;
use crate::journal::Progress;
use crate::shape::Shape;

/// How a run is saved.
pub(super) enum Saving {
    /// Not at all.
    Not,
    /// With a journal over the whole run.
    Journal(Book),
    /// With a position, lines for the items past it, and the items past
    /// it that are done already.
    Position {
        book: Book,
        position: u64,
        done: Vec<u64>,
    },
}

impl Pipe {
    pub(super) fn drive<S>(
        &self,
        input: S,
        saving: Saving,
    ) -> BoxStream<'static, Result<Progress, RunError>>
    where
        S: Stream<Item = Value> + Send + 'static,
    {
        let (book, ledger) = match saving {
            Saving::Not => (None, None),
            Saving::Journal(book) => (Some(Arc::new(Mutex::new(book))), None),
            Saving::Position {
                book,
                position,
                done,
            } => (
                Some(Arc::new(Mutex::new(book))),
                Some(Arc::new(Mutex::new(Ledger::after(position, done)))),
            ),
        };
        let first = ledger.as_ref().map_or(0, |l| lock(l).position() + 1);
        let checked = numbered(input, self.input.clone(), ledger.clone(), first);
        let cx = Cx {
            book: book.clone(),
            scope: "".into(),
            moves: Moves::default(),
        };
        let call_moves = cx.moves.clone();
        let mut inner = (self.transform)(checked, cx);
        let mut reported = first.saturating_sub(1);
        let positioned = ledger.clone();
        let passed_position = move |reported: u64| {
            positioned
                .as_ref()
                .map(|l| lock(l).position())
                .filter(|&p| p > reported)
        };
        let mut finished = move || ledger.as_ref().and_then(|l| lock(l).next_finished());
        let mut told: Option<u64> = None;
        // The outputs handed out so far, in a run that saves nothing.
        let mut unsaved: u64 = 0;
        let mut held: Option<Flow> = None;
        let (mut ended, mut stopped) = (false, false);
        stream::poll_fn(move |task| {
            if stopped {
                return Poll::Ready(None);
            }
            loop {
                // Hand out what to save as soon as it is known.
                if let Some(line) = book.as_ref().and_then(|b| lock(b).fresh.pop_front()) {
                    return Poll::Ready(Some(Ok(Progress::Save(line))));
                }
                if let Some(b) = &book {
                    let calls: Vec<_> = lock(&call_moves).drain(..).collect();
                    if !calls.is_empty() {
                        let mut b = lock(b);
                        for (call, through) in calls {
                            b.moved(&call, through);
                        }
                        continue;
                    }
                }
                if let Some(flow) = held.take() {
                    let Some((next, stops)) = hand_out(flow, book.as_ref(), &mut unsaved) else {
                        // Handed out before the restart.
                        continue;
                    };
                    stopped = stops;
                    return Poll::Ready(Some(next));
                }
                if let Some(p) = passed_position(reported) {
                    reported = p;
                    if let Some(b) = &book {
                        lock(b).passed(p);
                    }
                    return Poll::Ready(Some(Ok(Progress::Through(p))));
                }
                if let Some(n) = told.take().or_else(&mut finished) {
                    if let Some(b) = &book {
                        lock(b).finished(n);
                    }
                    return Poll::Ready(Some(Ok(Progress::Done(n))));
                }
                if ended {
                    let left = book.as_ref().map_or(0, |b| lock(b).handed_left());
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
                match inner.poll_next_unpin(task) {
                    Poll::Ready(Some(flow)) => held = Some(flow),
                    Poll::Ready(None) => ended = true,
                    Poll::Pending => {
                        let more = book.as_ref().is_some_and(|b| !lock(b).fresh.is_empty())
                            || (book.is_some() && !lock(&call_moves).is_empty());
                        told = finished();
                        if more || passed_position(reported).is_some() || told.is_some() {
                            continue;
                        }
                        return Poll::Pending;
                    }
                }
            }
        })
        .boxed()
    }
}

/// The input items, numbered from `first`, checked against `shape` and
/// tagged. An item the ledger keeps as done before a restart is dropped
/// without running.
fn numbered<S>(
    input: S,
    shape: Shape,
    ledger: Option<Arc<Mutex<Ledger>>>,
    first: u64,
) -> BoxStream<'static, Flow>
where
    S: Stream<Item = Value> + Send + 'static,
{
    let numbering = ledger.clone();
    input
        .zip(stream::iter(first..))
        .filter(move |(_, n)| {
            // An item done before the restart is not run again.
            let done = numbering.as_ref().is_some_and(|l| lock(l).is_done(*n));
            std::future::ready(!done)
        })
        .map(move |(v, n)| {
            let tag = match &ledger {
                Some(ledger) => Tag::item(n, ledger),
                None => Tag::default(),
            };
            match shape.check(&v) {
                Ok(()) => Ok(Flowing::Value(v, tag)),
                Err(p) => Err(fatal(RunError::InputMisfit(p.to_string())).of(&tag)),
            }
        })
        .boxed()
}

/// What the run hands its caller for one flow that reached the end of the
/// pipe, and whether that ends the run. `None` for an output the caller
/// was handed before the restart. `unsaved` counts the outputs of a run
/// that saves nothing.
fn hand_out(
    flow: Flow,
    book: Option<&Arc<Mutex<Book>>>,
    unsaved: &mut u64,
) -> Option<(Result<Progress, RunError>, bool)> {
    match flow {
        Ok(Flowing::Out(out, tag)) => {
            let id = if let Some(b) = book {
                let handing = lock(b).hand(&tag, &out.name);
                match handing {
                    Handing::Fresh(id) => id,
                    Handing::Before => return None,
                    Handing::Misfit(was) => {
                        let misfit = RunError::ReplayMisfit {
                            problem: format!(
                                "the journal says output `{was}` was handed out next, and the run made `{}`",
                                out.name
                            ),
                        };
                        return Some((Err(misfit), true));
                    }
                }
            } else {
                *unsaved += 1;
                OutputId::at(0, "", *unsaved - 1)
            };
            // The tag drops here: the item may now be done.
            let output = PipeOutput {
                id,
                name: out.name,
                payload: out.payload,
            };
            Some((Ok(Progress::Output(output)), false))
        }
        // The checker refuses a pipe with a path that does not emit.
        Ok(Flowing::Value(..)) => {
            let misfit = RunError::ItemMisfit {
                path: "pipe".into(),
                problem: "an item reached the end of the pipe without being emitted".into(),
            };
            Some((Err(misfit), true))
        }
        // A failure ends the stream. The position stays before the item
        // that failed.
        Err(fault) => Some((Err(run_error(fault)), true)),
    }
}
