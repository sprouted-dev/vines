//! The loop that runs a checked vine and hands its caller what comes out.

use std::sync::{Arc, Mutex};
use std::task::Poll;

use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};
use serde_json::Value;

use super::book::{Book, Cx};
use super::flow::{fatal, lock, run_error, Flow, Flowing};
use super::Pipe;
use crate::error::RunError;
use crate::journal::{Entry, Event};

impl Pipe {
    pub(super) fn drive<S>(
        &self,
        input: S,
        book: Option<Arc<Mutex<Book>>>,
    ) -> BoxStream<'static, Result<Event, RunError>>
    where
        S: Stream<Item = Value> + Send + 'static,
    {
        let shape = self.input.clone();
        let checked = input.map(move |v| match shape.check(&v) {
            Ok(()) => Ok(Flowing::Value(v)),
            Err(p) => Err(fatal(RunError::InputMisfit(p))),
        });
        let cx = Cx {
            book: book.clone(),
            scope: "".into(),
        };
        let mut inner = (self.transform)(checked.boxed(), cx);
        let mut held: Option<Flow> = None;
        let (mut ended, mut stopped) = (false, false);
        stream::poll_fn(move |task| {
            if stopped {
                return Poll::Ready(None);
            }
            loop {
                // Hand out what to save as soon as it is known.
                if let Some(entry) = book.as_ref().and_then(|b| lock(b).fresh.pop_front()) {
                    return Poll::Ready(Some(Ok(Event::Save(entry))));
                }
                let flow = match held.take() {
                    Some(flow) => flow,
                    None if ended => {
                        let left = book.as_ref().map_or(0, |b| lock(b).handed.len());
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
                    None => match inner.poll_next_unpin(task) {
                        Poll::Ready(Some(flow)) => {
                            held = Some(flow);
                            continue;
                        }
                        Poll::Ready(None) => {
                            ended = true;
                            continue;
                        }
                        Poll::Pending => {
                            if book.as_ref().is_some_and(|b| !lock(b).fresh.is_empty()) {
                                continue;
                            }
                            return Poll::Pending;
                        }
                    },
                };
                match flow {
                    Ok(Flowing::Out(out)) => {
                        if let Some(b) = &book {
                            let mut b = lock(b);
                            if let Some(name) = b.handed.pop_front() {
                                if name != out.name {
                                    stopped = true;
                                    return Poll::Ready(Some(Err(RunError::ReplayMisfit {
                                        problem: format!(
                                            "the journal says output `{name}` was handed out next, and the run made `{}`",
                                            out.name
                                        ),
                                    })));
                                }
                                // Handed out before the restart.
                                continue;
                            }
                            b.fresh.push_back(Entry::Emitted {
                                output: out.name.clone(),
                            });
                        }
                        return Poll::Ready(Some(Ok(Event::Output(out))));
                    }
                    // The checker refuses a pipe with a path that does not emit.
                    Ok(Flowing::Value(_)) => {
                        stopped = true;
                        return Poll::Ready(Some(Err(RunError::ItemMisfit {
                            path: "pipe".into(),
                            problem: "an item reached the end of the pipe without being emitted"
                                .into(),
                        })));
                    }
                    // A failure ends the stream.
                    Err(fault) => {
                        stopped = true;
                        return Poll::Ready(Some(Err(run_error(fault))));
                    }
                }
            }
        })
        .boxed()
    }
}
