//! What a saved run remembers from its journal, and the context every
//! stage of one run shares.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use super::flow::lock;
use crate::journal::{Entry, Journal};
use crate::registry::OpError;

/// What a saved run remembers from the journal it was given, and what it
/// has yet to hand the caller.
#[derive(Default)]
pub(super) struct Book {
    /// Saved results, by key. Taken out as the replay uses them.
    pub(super) saved: HashMap<String, Saved>,
    /// How many times each stage has run so far in this run.
    pub(super) counts: HashMap<String, u64>,
    /// New entries for the caller to save.
    pub(super) fresh: VecDeque<Entry>,
    /// The outputs the caller was already handed, oldest first.
    pub(super) handed: VecDeque<String>,
}

pub(super) enum Saved {
    Once {
        input: Value,
        outcome: Result<Value, OpError>,
    },
    Stream {
        input: Value,
        items: VecDeque<Value>,
        end: Option<Result<Value, OpError>>,
    },
}

impl Book {
    pub(super) fn from_journal(journal: &Journal) -> Result<Book, String> {
        let mut book = Book::default();
        for (i, entry) in journal.entries().iter().enumerate() {
            let problem = |what: &str| format!("entry {i}: {what}");
            match entry {
                Entry::Done { at, input, result } => {
                    if book.saved.contains_key(at) {
                        return Err(problem(&format!("`{at}` is saved twice")));
                    }
                    book.saved.insert(
                        at.clone(),
                        Saved::Once {
                            input: input.clone(),
                            outcome: Ok(result.clone()),
                        },
                    );
                }
                Entry::Failed {
                    at,
                    input,
                    code,
                    message,
                } => {
                    let failure = OpError::new(code.clone(), message.clone());
                    match book.saved.get_mut(at) {
                        Some(Saved::Stream {
                            end: end @ None, ..
                        }) => *end = Some(Err(failure)),
                        Some(_) => return Err(problem(&format!("`{at}` is saved twice"))),
                        None => {
                            book.saved.insert(
                                at.clone(),
                                Saved::Once {
                                    input: input.clone(),
                                    outcome: Err(failure),
                                },
                            );
                        }
                    }
                }
                Entry::Started { at, input } => {
                    if book.saved.contains_key(at) {
                        return Err(problem(&format!("`{at}` is saved twice")));
                    }
                    book.saved.insert(
                        at.clone(),
                        Saved::Stream {
                            input: input.clone(),
                            items: VecDeque::new(),
                            end: None,
                        },
                    );
                }
                Entry::Item { at, value } => match book.saved.get_mut(at) {
                    Some(Saved::Stream {
                        items, end: None, ..
                    }) => items.push_back(value.clone()),
                    _ => {
                        return Err(problem(&format!(
                            "an item for `{at}`, which is not a stream that is still going"
                        )))
                    }
                },
                Entry::Ended { at, value } => match book.saved.get_mut(at) {
                    Some(Saved::Stream {
                        end: end @ None, ..
                    }) => *end = Some(Ok(value.clone())),
                    _ => {
                        return Err(problem(&format!(
                            "an end for `{at}`, which is not a stream that is still going"
                        )))
                    }
                },
                Entry::Emitted { output } => book.handed.push_back(output.clone()),
            }
        }
        Ok(book)
    }
}

/// What every stage of one run shares: the book, when the run is saved,
/// and the scope its keys live in.
#[derive(Clone, Default)]
pub(super) struct Cx {
    pub(super) book: Option<Arc<Mutex<Book>>>,
    pub(super) scope: Arc<str>,
}

impl Cx {
    /// The key for this run of the stage at `path`, when the run is saved.
    pub(super) fn key(&self, path: &str) -> Option<String> {
        let book = self.book.as_ref()?;
        let mut book = lock(book);
        let at = format!("{}{path}", self.scope);
        let count = book.counts.entry(at.clone()).or_insert(0);
        let key = format!("{at}#{count}");
        *count += 1;
        Some(key)
    }

    pub(super) fn take(&self, key: &str) -> Option<Saved> {
        lock(self.book.as_ref()?).saved.remove(key)
    }

    pub(super) fn save(&self, entry: Entry) {
        if let Some(book) = &self.book {
            lock(book).fresh.push_back(entry);
        }
    }

    /// The scope for a vine run by `use` at `path`.
    pub(super) fn within(&self, path: &str, vine: &str) -> Cx {
        Cx {
            book: self.book.clone(),
            scope: format!("{}{path}>{vine}:", self.scope).into(),
        }
    }
}
