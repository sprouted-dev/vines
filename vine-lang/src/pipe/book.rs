//! What a saved run remembers from what its caller kept, and the context
//! every stage of one run shares.
//!
//! One book serves both ways to survive a restart. A journal is a book
//! whose lines all belong to item 0, the whole run. A bookmark's lines
//! belong to the input item they came from, and a run from a position
//! reads only the lines past it.
//!
//! For a stream act that gives its items again, the book keeps how far
//! each call got and how many outputs made from the items after that were
//! handed out. A replay hands those out again silently, in order, and then
//! hands out the rest.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use super::flow::lock;
use super::tag::{Moves, Tag};
use crate::journal::{item_of_stream, Bookmark, Ending, Entry, Journal, Line};
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
    /// For each stream act call that gives its items again: how far it got.
    calls: HashMap<Key, CallState>,
}

/// How far one call of a stream act that gives its items again got.
#[derive(Default)]
struct CallState {
    /// Every item through this number is fully done.
    through: u64,
    /// Outputs the caller was handed before a restart that the replay has
    /// not made again yet.
    skip: u64,
    /// For each item past `through`: how many outputs made from it were
    /// handed out.
    handed: BTreeMap<u64, u64>,
}

impl CallState {
    fn line(&self, call: &Key) -> Line {
        Line {
            item: call.item,
            entry: Entry::Through {
                at: call.at.clone(),
                items: self.through,
                handed: self.skip + self.handed.values().sum::<u64>(),
            },
        }
    }
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
    /// A stream act that gives its items again: how far it got, and how
    /// many outputs made from the items after that were handed out.
    Position {
        input: Value,
        through: u64,
        handed: u64,
        stopped: Option<(u64, Ending)>,
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
            Entry::Through { at, items, handed } => match self.position_of(key(at)) {
                Some(Saved::Position {
                    through,
                    handed: was,
                    ..
                }) => {
                    *through = *items;
                    *was = *handed;
                }
                _ => {
                    return Err(format!(
                        "a position for `{}`, which is not a stream act that gives its items again and was started",
                        key(at)
                    ))
                }
            },
            Entry::Stopped { at, items, how } => match self.position_of(key(at)) {
                Some(Saved::Position {
                    stopped: stopped @ None,
                    ..
                }) => *stopped = Some((*items, how.clone())),
                _ => {
                    return Err(format!(
                        "a stop for `{}`, which is not a stream act that gives its items again and is still going",
                        key(at)
                    ))
                }
            },
        }
        Ok(())
    }

    /// The saved position of a stream act call. A call saved as started
    /// with nothing after it yet becomes one.
    fn position_of(&mut self, key: Key) -> Option<&mut Saved> {
        let saved = self.saved.get_mut(&key)?;
        if let Saved::Stream {
            input,
            items,
            end: None,
        } = saved
        {
            if items.is_empty() {
                *saved = Saved::Position {
                    input: std::mem::take(input),
                    through: 0,
                    handed: 0,
                    stopped: None,
                };
            }
        }
        matches!(saved, Saved::Position { .. }).then_some(saved)
    }

    /// A call of a stream act that gives its items again starts or carries
    /// on: every item through `through` is done, and `skip` outputs made
    /// from the items after them were handed out before.
    pub(super) fn open_call(&mut self, call: &Key, through: u64, skip: u64) {
        self.calls.insert(
            call.clone(),
            CallState {
                through,
                skip,
                handed: BTreeMap::new(),
            },
        );
    }

    /// Every item through `through` of the stream act call `call` is fully
    /// done. Keep that, forget what the run counted for them, and give the
    /// caller a line to save.
    pub(super) fn moved(&mut self, call: &Key, through: u64) {
        let state = self.calls.entry(call.clone()).or_default();
        state.through = through;
        state.handed.retain(|&n, _| n > through);
        let line = state.line(call);
        self.counts.retain(|(item, at), _| {
            *item != call.item || item_of_stream(at, &call.at).is_none_or(|n| n > through)
        });
        self.fresh.push_back(line);
    }

    /// Whether the output `name`, made from a value with `tag`, is new to
    /// the caller. A new one is recorded as handed out.
    pub(super) fn hand(&mut self, tag: &Tag, name: &str) -> Handing {
        if let Some((call, n)) = tag.stream() {
            let state = self.calls.entry((*call).clone()).or_default();
            *state.handed.entry(n).or_default() += 1;
            if state.skip > 0 {
                state.skip -= 1;
                return Handing::Before;
            }
            let line = state.line(&call);
            self.fresh.push_back(line);
            return Handing::Fresh;
        }
        let item = tag.item_number();
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
    pub(super) fn handed_left(&self) -> u64 {
        let outputs: u64 = self.handed.values().map(|h| h.len() as u64).sum();
        outputs + self.calls.values().map(|c| c.skip).sum::<u64>()
    }
}

/// What every stage of one run shares: the book, when the run is saved,
/// and the scope its keys live in.
#[derive(Clone, Default)]
pub(super) struct Cx {
    pub(super) book: Option<Arc<Mutex<Book>>>,
    pub(super) scope: Arc<str>,
    /// Where the ledgers of stream act calls that give their items again
    /// say they moved.
    pub(super) moves: Moves,
}

impl Cx {
    /// The key for this run of the stage at `path` for a value with `tag`,
    /// when the run is saved.
    pub(super) fn key(&self, path: &str, tag: &Tag) -> Option<Key> {
        let book = self.book.as_ref()?;
        let mut book = lock(book);
        let item = tag.item_number();
        let at = format!("{}{}{path}", tag.scope(), self.scope);
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
            moves: self.moves.clone(),
        }
    }
}
