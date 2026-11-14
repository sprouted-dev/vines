//! What a saved run remembers from what its caller kept, and the context
//! every stage of one run shares.
//!
//! One book serves both ways to survive a restart. A journal is a book
//! whose lines all belong to item 0, the whole run. A bookmark's lines
//! belong to the input item they came from, and a run from a position
//! reads only the lines past it.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use super::flow::lock;
use super::tag::Tag;
use crate::journal::{Bookmark, Entry, Journal, Line};
use crate::registry::OpError;

/// Where one call of an act is saved: the input item it belongs to, and
/// its key within that item's work, such as `pipe[1]#0`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct Key {
    pub(super) item: u64,
    pub(super) at: String,
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.item == 0 {
            write!(f, "{}", self.at)
        } else {
            write!(f, "{} of item {}", self.at, self.item)
        }
    }
}

/// What a saved run remembers from what it was given, and what it has yet
/// to hand the caller.
#[derive(Default)]
pub(super) struct Book {
    /// Saved results. Taken out as the replay uses them.
    saved: HashMap<Key, Saved>,
    /// How many times each stage has run so far, per input item.
    counts: HashMap<(u64, String), u64>,
    /// New lines for the caller to keep.
    pub(super) fresh: VecDeque<Line>,
    /// The outputs the caller was already handed, oldest first, per input
    /// item.
    handed: HashMap<u64, VecDeque<String>>,
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

/// What the run should do with an output it made.
pub(super) enum Handing {
    /// Hand it out: the caller has not seen it.
    Fresh,
    /// The caller was handed it before the restart.
    Before,
    /// The caller was handed another output next.
    Misfit(String),
}

impl Book {
    /// Read a journal: every entry belongs to the whole run, item 0.
    pub(super) fn from_journal(journal: &Journal) -> Result<Book, String> {
        let mut book = Book::default();
        for (i, entry) in journal.entries().iter().enumerate() {
            book.read(0, entry)
                .map_err(|what| format!("entry {i}: {what}"))?;
        }
        Ok(book)
    }

    /// Read a bookmark. Lines at or before its position are done and are
    /// not read.
    pub(super) fn from_bookmark(bookmark: &Bookmark) -> Result<Book, String> {
        let mut book = Book::default();
        for (i, Line { item, entry }) in bookmark.lines().iter().enumerate() {
            if *item <= bookmark.position() {
                continue;
            }
            book.read(*item, entry)
                .map_err(|what| format!("line {i}: {what}"))?;
        }
        Ok(book)
    }

    fn read(&mut self, item: u64, entry: &Entry) -> Result<(), String> {
        let key = |at: &String| Key {
            item,
            at: at.clone(),
        };
        let twice = |k: &Key| format!("`{k}` is saved twice");
        match entry {
            Entry::Done { at, input, result } => {
                let k = key(at);
                if self.saved.contains_key(&k) {
                    return Err(twice(&k));
                }
                self.saved.insert(
                    k,
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
                let k = key(at);
                let failure = OpError::new(code.clone(), message.clone());
                match self.saved.get_mut(&k) {
                    Some(Saved::Stream {
                        end: end @ None, ..
                    }) => *end = Some(Err(failure)),
                    Some(_) => return Err(twice(&k)),
                    None => {
                        self.saved.insert(
                            k,
                            Saved::Once {
                                input: input.clone(),
                                outcome: Err(failure),
                            },
                        );
                    }
                }
            }
            Entry::Started { at, input } => {
                let k = key(at);
                if self.saved.contains_key(&k) {
                    return Err(twice(&k));
                }
                self.saved.insert(
                    k,
                    Saved::Stream {
                        input: input.clone(),
                        items: VecDeque::new(),
                        end: None,
                    },
                );
            }
            Entry::Item { at, value } => match self.saved.get_mut(&key(at)) {
                Some(Saved::Stream {
                    items, end: None, ..
                }) => items.push_back(value.clone()),
                _ => {
                    return Err(format!(
                        "an item for `{}`, which is not a stream that is still going",
                        key(at)
                    ))
                }
            },
            Entry::Ended { at, value } => match self.saved.get_mut(&key(at)) {
                Some(Saved::Stream {
                    end: end @ None, ..
                }) => *end = Some(Ok(value.clone())),
                _ => {
                    return Err(format!(
                        "an end for `{}`, which is not a stream that is still going",
                        key(at)
                    ))
                }
            },
            Entry::Emitted { output } => self
                .handed
                .entry(item)
                .or_default()
                .push_back(output.clone()),
        }
        Ok(())
    }

    /// Whether the output `name` of input item `item` is new to the caller.
    /// A new one is recorded as handed out.
    pub(super) fn hand(&mut self, item: u64, name: &str) -> Handing {
        let before = self.handed.get_mut(&item).and_then(VecDeque::pop_front);
        match before {
            Some(was) if was == name => Handing::Before,
            Some(was) => Handing::Misfit(was),
            None => {
                self.fresh.push_back(Line {
                    item,
                    entry: Entry::Emitted {
                        output: name.to_owned(),
                    },
                });
                Handing::Fresh
            }
        }
    }

    /// How many outputs the caller was handed that the run has not made
    /// again yet.
    pub(super) fn handed_left(&self) -> usize {
        self.handed.values().map(VecDeque::len).sum()
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
    /// The key for this run of the stage at `path` for a value with `tag`,
    /// when the run is saved.
    pub(super) fn key(&self, path: &str, tag: &Tag) -> Option<Key> {
        let book = self.book.as_ref()?;
        let mut book = lock(book);
        let item = tag.item_number();
        let at = format!("{}{path}", self.scope);
        let count = book.counts.entry((item, at.clone())).or_insert(0);
        let key = Key {
            item,
            at: format!("{at}#{count}"),
        };
        *count += 1;
        Some(key)
    }

    pub(super) fn take(&self, key: &Key) -> Option<Saved> {
        lock(self.book.as_ref()?).saved.remove(key)
    }

    pub(super) fn save(&self, key: &Key, entry: Entry) {
        if let Some(book) = &self.book {
            lock(book).fresh.push_back(Line {
                item: key.item,
                entry,
            });
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
