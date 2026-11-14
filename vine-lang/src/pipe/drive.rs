//! The loop that runs a checked vine and hands its caller what comes out.
//!
//! One loop serves all three ways to run. It hands out, in this order:
//! the lines to save as soon as they are known, then the next output, and,
//! when it holds nothing, the position if it moved. So an output comes
//! before the line that records it, and that line comes before the
//! position that passes its item.

use std::sync::{Arc, Mutex};
use std::task::Poll;

use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};
use serde_json::Value;

use super::book::{Book, Cx, Handing};
use super::flow::{fatal, lock, run_error, Flow, Flowing};
use super::tag::{Ledger, Tag};
use super::Pipe;
use crate::error::RunError;
use crate::journal::Progress;

/// How a run is saved.
pub(super) enum Saving {
    /// Not at all.
    Not,
    /// With a journal over the whole run.
    Journal(Book),
    /// With a position, and lines for the items past it.
    Position(Book, u64),
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
            Saving::Position(book, position) => (
                Some(Arc::new(Mutex::new(book))),
                Some(Arc::new(Mutex::new(Ledger::after(position)))),
            ),
        };
        let shape = self.input.clone();
        let first = ledger.as_ref().map_or(0, |l| lock(l).position() + 1);
        let numbering = ledger.clone();
        let checked = input.zip(stream::iter(first..)).map(move |(v, n)| {
            let tag = match &numbering {
                Some(ledger) => Tag::item(n, ledger),
                None => Tag::default(),
            };
            match shape.check(&v) {
                Ok(()) => Ok(Flowing::Value(v, tag)),
                Err(p) => Err(fatal(RunError::InputMisfit(p)).of(&tag)),
            }
        });
        let cx = Cx {
            book: book.clone(),
            scope: "".into(),
        };
        let mut inner = (self.transform)(checked.boxed(), cx);
        let mut reported = first.saturating_sub(1);
        let moved = move |reported: u64| {
            ledger
                .as_ref()
                .map(|l| lock(l).position())
                .filter(|&p| p > reported)
        };
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
                if let Some(flow) = held.take() {
                    match flow {
                        Ok(Flowing::Out(out, tag)) => {
                            if let Some(b) = &book {
                                match lock(b).hand(tag.item_number(), &out.name) {
                                    Handing::Fresh => {}
                                    // Handed out before the restart.
                                    Handing::Before => continue,
                                    Handing::Misfit(was) => {
                                        stopped = true;
                                        return Poll::Ready(Some(Err(RunError::ReplayMisfit {
                                            problem: format!(
                                                "the journal says output `{was}` was handed out next, and the run made `{}`",
                                                out.name
                                            ),
                                        })));
                                    }
                                }
                            }
                            // The tag drops here: the item may now be done.
                            return Poll::Ready(Some(Ok(Progress::Output(out))));
                        }
                        // The checker refuses a pipe with a path that does not emit.
                        Ok(Flowing::Value(..)) => {
                            stopped = true;
                            return Poll::Ready(Some(Err(RunError::ItemMisfit {
                                path: "pipe".into(),
                                problem:
                                    "an item reached the end of the pipe without being emitted"
                                        .into(),
                            })));
                        }
                        // A failure ends the stream. The position stays
                        // before the item that failed.
                        Err(fault) => {
                            stopped = true;
                            return Poll::Ready(Some(Err(run_error(fault))));
                        }
                    }
                }
                if let Some(p) = moved(reported) {
                    reported = p;
                    return Poll::Ready(Some(Ok(Progress::Through(p))));
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
                        let more = book.as_ref().is_some_and(|b| !lock(b).fresh.is_empty());
                        if more || moved(reported).is_some() {
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
