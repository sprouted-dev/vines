//! The bookmark of a run over an endless stream: the position, and the
//! lines of the input items past it.

use std::collections::BTreeMap;

use serde_json::{json, Value};
use thiserror::Error;

use super::entry::{Entry, EntryError};
use crate::pipe::PipeOutput;

/// Saved JSON that is not a [`Line`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LineError {
    /// The line is not a map.
    #[error("a line is a map")]
    NotAMap,
    /// `item` is absent or not a whole number.
    #[error("a line needs `item`, a whole number")]
    NeedsItem,
    /// `item` is 0.
    #[error("input items are counted from 1")]
    ItemZero,
    /// The rest of the line is not an entry.
    #[error(transparent)]
    Entry(#[from] EntryError),
}

/// Saved JSON that is not a [`Bookmark`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BookmarkError {
    /// The bookmark is not a map.
    #[error("a bookmark is a map")]
    NotAMap,
    /// The bookmark holds a field it does not have.
    #[error("a bookmark has no field `{field}`")]
    UnknownField {
        /// The field.
        field: String,
    },
    /// `position` is absent or not a whole number.
    #[error("a bookmark needs `position`, a whole number")]
    NeedsPosition,
    /// `lines` is absent or not a list.
    #[error("a bookmark needs `lines`, a list")]
    NeedsLines,
    /// One line cannot be read.
    #[error("line {index}: {source}")]
    Line {
        /// The line's place in `lines`, from 0.
        index: usize,
        /// Why it cannot be read.
        source: LineError,
    },
    /// `done` is not a list.
    #[error("a bookmark's `done` is a list of runs")]
    DoneNotAList,
    /// One run of `done` is not a pair of whole numbers.
    #[error("done run {index}: a run is [first, last], whole numbers")]
    BadRun {
        /// The run's place in `done`, from 0.
        index: usize,
    },
    /// One run of `done` is backwards or not past the position.
    #[error("done run {index}: a run lies past the position, first to last")]
    MisplacedRun {
        /// The run's place in `done`, from 0.
        index: usize,
    },
}

/// What a run over an endless stream gives its caller. See the module docs.
#[derive(Debug, Clone, PartialEq)]
pub enum Progress {
    /// The vine emitted this.
    Output(PipeOutput),
    /// Keep this line until the position passes its input item.
    Save(Line),
    /// Every input item through this number is fully done. Keep it as the
    /// position, and drop every line at or before it.
    Through(u64),
    /// This input item, past the position, is fully done while an item
    /// before it is not. Keep it as done, and drop its lines. Only a
    /// `mergeMap` finishes items out of order.
    Done(u64),
}

/// One entry of a run over an endless stream, with the input item it
/// belongs to.
#[derive(Debug, Clone, PartialEq)]
pub struct Line {
    /// The input item, counted from 1.
    pub item: u64,
    /// What was saved.
    pub entry: Entry,
}

impl Line {
    /// The line as plain JSON: the entry's JSON with `item` added.
    pub fn to_json(&self) -> Value {
        let mut v = self.entry.to_json();
        v["item"] = json!(self.item);
        v
    }

    /// Read a line written by [`Line::to_json`].
    ///
    /// # Errors
    ///
    /// The [`LineError`] that says what in `v` is not a line.
    pub fn from_json(v: &Value) -> Result<Line, LineError> {
        let Some(map) = v.as_object() else {
            return Err(LineError::NotAMap);
        };
        let Some(item) = map.get("item").and_then(Value::as_u64) else {
            return Err(LineError::NeedsItem);
        };
        if item == 0 {
            return Err(LineError::ItemZero);
        }
        let mut rest = map.clone();
        rest.remove("item");
        let entry = Entry::from_json(&Value::Object(rest))?;
        Ok(Line { item, entry })
    }
}

/// What the caller of [`Pipe::run_from`](crate::Pipe::run_from) keeps: the
/// position, the lines of the input items past it, and which items past
/// it are done already. It is bounded by the work in flight, not by the
/// length of the stream.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Bookmark {
    position: u64,
    lines: Vec<Line>,
    /// Items done past the position, as runs of numbers: first to last.
    done: BTreeMap<u64, u64>,
}

impl Bookmark {
    /// A fresh start: nothing done, nothing saved.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every input item through `position` is done, and nothing else is
    /// saved. A run from here does again whatever was in flight past the
    /// position: calls are made again and outputs handed out again.
    pub fn at(position: u64) -> Self {
        Bookmark {
            position,
            ..Bookmark::default()
        }
    }

    /// Every input item through this number is done. Feed input from the
    /// item after it.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// The lines kept for the input items past the position, oldest first.
    pub fn lines(&self) -> &[Line] {
        &self.lines
    }

    /// The lines, oldest first, taken out of the bookmark.
    pub(crate) fn into_lines(self) -> Vec<Line> {
        self.lines
    }

    /// The items past the position that are done already, in order. A run
    /// from this bookmark skips them.
    pub fn done(&self) -> impl Iterator<Item = u64> + '_ {
        self.done.iter().flat_map(|(&first, &last)| first..=last)
    }

    /// Whether item `n` is done: at or before the position, or past it and
    /// done already.
    pub fn is_done(&self, n: u64) -> bool {
        n <= self.position
            || self
                .done
                .range(..=n)
                .next_back()
                .is_some_and(|(_, &last)| last >= n)
    }

    /// Keep items `first` to `last` as done, joined to the runs of numbers
    /// they touch.
    fn mark_done(&mut self, first: u64, last: u64) {
        let (mut first, mut last) = (first.max(self.position + 1), last);
        if first > last {
            return;
        }
        let touching: Vec<(u64, u64)> = self
            .done
            .range(..=last.saturating_add(1))
            .filter(|(_, &l)| l.saturating_add(1) >= first)
            .map(|(&f, &l)| (f, l))
            .collect();
        for (f, l) in touching {
            self.done.remove(&f);
            first = first.min(f);
            last = last.max(l);
        }
        self.done.insert(first, last);
    }

    /// How many lines are kept.
    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// Whether no line is kept.
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// Keep what the run handed out: append a line, or move the position
    /// and drop the lines it passed. A line with an [`Entry::Through`]
    /// replaces the one before it for the same stream operator, and drops the
    /// lines of the items of that stream it passed. [`Progress::Done`]
    /// keeps the item as done and drops its lines. An output changes
    /// nothing.
    pub fn record(&mut self, progress: &Progress) {
        match progress {
            Progress::Output(_) => {}
            Progress::Done(n) => {
                if *n > self.position {
                    self.mark_done(*n, *n);
                    self.lines.retain(|line| line.item != *n);
                }
            }
            Progress::Save(line) => {
                if matches!(line.entry, Entry::Through { .. }) {
                    self.lines.retain(|kept| {
                        kept.item != line.item || !kept.entry.passed_by(&line.entry)
                    });
                }
                self.lines.push(line.clone());
            }
            Progress::Through(n) => {
                self.position = *n;
                self.lines.retain(|line| line.item > *n);
                let passed: Vec<(u64, u64)> =
                    self.done.range(..=*n).map(|(&f, &l)| (f, l)).collect();
                for (first, last) in passed {
                    self.done.remove(&first);
                    if last > *n {
                        self.done.insert(*n + 1, last);
                    }
                }
            }
        }
    }

    /// The bookmark as `{ "position": n, "lines": [..] }`, with
    /// `"done": [[first, last], ..]` when items past the position are done.
    pub fn to_json(&self) -> Value {
        let mut v = json!({
            "position": self.position,
            "lines": self.lines.iter().map(Line::to_json).collect::<Vec<_>>(),
        });
        if !self.done.is_empty() {
            v["done"] = self.done.iter().map(|(f, l)| json!([f, l])).collect();
        }
        v
    }

    /// Read a bookmark written by [`Bookmark::to_json`].
    ///
    /// # Errors
    ///
    /// The [`BookmarkError`] that says what in `v` is not a bookmark.
    pub fn from_json(v: &Value) -> Result<Bookmark, BookmarkError> {
        let Some(map) = v.as_object() else {
            return Err(BookmarkError::NotAMap);
        };
        if let Some(extra) = map
            .keys()
            .find(|k| !["position", "lines", "done"].contains(&k.as_str()))
        {
            return Err(BookmarkError::UnknownField {
                field: extra.clone(),
            });
        }
        let Some(position) = map.get("position").and_then(Value::as_u64) else {
            return Err(BookmarkError::NeedsPosition);
        };
        let Some(saved) = map.get("lines").and_then(Value::as_array) else {
            return Err(BookmarkError::NeedsLines);
        };
        let lines = saved
            .iter()
            .enumerate()
            .map(|(index, l)| {
                Line::from_json(l).map_err(|source| BookmarkError::Line { index, source })
            })
            .collect::<Result<_, _>>()?;
        let mut bookmark = Bookmark {
            position,
            lines,
            done: BTreeMap::new(),
        };
        let runs = match map.get("done") {
            None => &[][..],
            Some(v) => v.as_array().ok_or(BookmarkError::DoneNotAList)?,
        };
        for (index, run) in runs.iter().enumerate() {
            let pair = run.as_array().map(|p| (p.len(), p.first(), p.get(1)));
            let (Some(first), Some(last)) = (
                pair.and_then(|(_, f, _)| f?.as_u64()),
                pair.and_then(|(len, _, l)| if len == 2 { l?.as_u64() } else { None }),
            ) else {
                return Err(BookmarkError::BadRun { index });
            };
            if first > last || first <= position {
                return Err(BookmarkError::MisplacedRun { index });
            }
            bookmark.mark_done(first, last);
        }
        Ok(bookmark)
    }
}
