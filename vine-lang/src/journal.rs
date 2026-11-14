//! What a resumable run asks its caller to save, and gets back after a
//! restart.
//!
//! There are two ways to survive a restart, and they share one save format.
//!
//! - A run that ends, such as one job, uses
//!   [`Pipe::run_saved`](crate::Pipe::run_saved). It gives a stream of
//!   [`Event`]s. An [`Event::Output`] is something the vine emitted. An
//!   [`Event::Save`] is one [`Entry`] the caller appends to its store. To
//!   resume, the caller builds a [`Journal`] from what it stored and runs
//!   the vine again over the same input items, from the first.
//! - A run over an endless stream uses
//!   [`Pipe::run_from`](crate::Pipe::run_from). It gives [`Progress`]: the
//!   same outputs, the same entries as [`Line`]s that name the input item
//!   they belong to, and [`Progress::Through`], which says every input item
//!   through a number is fully done. The caller keeps a [`Bookmark`]: the
//!   position and the lines past it. To resume, it feeds input from the
//!   item after the position, with the bookmark.
//!
//! The library stores nothing. Both are plain data: [`Entry::to_json`],
//! [`Journal::from_json`] and [`Bookmark::from_json`] turn them into JSON
//! and back.
//!
//! # What an entry means
//!
//! Each act a run calls has a key: the path of its stage and a count of how
//! many times that stage ran before, such as `pipe[1]#0`. In a journal the
//! count runs over the whole run. In a bookmark it runs over the work of
//! one input item, so the keys of item 7 are the same whether the run
//! started at item 1 or at item 5. A replay calls the stages in the same
//! order, so the keys line up.
//!
//! - [`Entry::Done`] and [`Entry::Failed`]: an act gave this result or
//!   failed this way. On replay the act is not called. Its saved result is
//!   used.
//! - [`Entry::Started`], [`Entry::Item`] and [`Entry::Ended`]: a stream act
//!   was called, gave one more item, and ended. On replay the saved items
//!   flow again. If it had not ended, the act is called again with
//!   [`OpContext::resume_from`](crate::OpContext::resume_from) set to the
//!   number of items saved, and the run carries on from there.
//! - [`Entry::Emitted`]: the caller was handed this output. On replay the
//!   same output is made again and not handed out twice.
//!
//! # Which input item a line belongs to
//!
//! Input items are numbered from 1, counted from the first item ever. A
//! line belongs to the input item the act's value came from. A value made
//! from several items, such as a batch from `collect`, belongs to the last
//! of them. A batch ties its items together: the position never stops
//! between them, so a restart feeds the whole batch again.
//!
//! # The cut line
//!
//! [`Progress::Through`] with `n` means nothing in the run still holds
//! anything derived from items 1 to `n`: every act they caused has
//! finished, every output they caused was handed out, and no stage holds
//! them. Their lines will never be read again. [`Bookmark::record`] drops
//! them. A bookmark that still has some, because the caller saved the
//! position and crashed before it dropped them, is fine: a run ignores
//! lines at or before the position.

use serde_json::{json, Value};

use crate::run::Output;

/// What a resumable run gives its caller.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The vine emitted this.
    Output(Output),
    /// Append this to the journal you keep for this run.
    Save(Entry),
}

/// One line of a run's journal.
#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    /// The act at `at` was given `input` and gave `result`.
    Done {
        at: String,
        input: Value,
        result: Value,
    },
    /// The act at `at` was given `input` and failed. For a stream act this
    /// ends its stream.
    Failed {
        at: String,
        input: Value,
        code: String,
        message: String,
    },
    /// The stream act at `at` was called with `input`.
    Started { at: String, input: Value },
    /// The stream act at `at` gave one more item.
    Item { at: String, value: Value },
    /// The stream act at `at` ended with `value`.
    Ended { at: String, value: Value },
    /// The caller was handed one output with this name.
    Emitted { output: String },
}

impl Entry {
    /// The entry as plain JSON, such as `{ "kind": "item", "at": "pipe[1]#0", "value": .. }`.
    pub fn to_json(&self) -> Value {
        match self {
            Entry::Done { at, input, result } => {
                json!({ "kind": "done", "at": at, "input": input, "result": result })
            }
            Entry::Failed {
                at,
                input,
                code,
                message,
            } => json!({
                "kind": "failed", "at": at, "input": input, "code": code, "message": message
            }),
            Entry::Started { at, input } => json!({ "kind": "started", "at": at, "input": input }),
            Entry::Item { at, value } => json!({ "kind": "item", "at": at, "value": value }),
            Entry::Ended { at, value } => json!({ "kind": "ended", "at": at, "value": value }),
            Entry::Emitted { output } => json!({ "kind": "emitted", "output": output }),
        }
    }

    /// Read an entry written by [`Entry::to_json`].
    pub fn from_json(v: &Value) -> Result<Entry, String> {
        let Some(map) = v.as_object() else {
            return Err("an entry is a map".into());
        };
        let text = |key: &str| -> Result<String, String> {
            map.get(key)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| format!("an entry needs `{key}` as text"))
        };
        let value = |key: &str| -> Result<Value, String> {
            map.get(key)
                .cloned()
                .ok_or_else(|| format!("an entry needs `{key}`"))
        };
        let kind = text("kind")?;
        let entry = match kind.as_str() {
            "done" => Entry::Done {
                at: text("at")?,
                input: value("input")?,
                result: value("result")?,
            },
            "failed" => Entry::Failed {
                at: text("at")?,
                input: value("input")?,
                code: text("code")?,
                message: text("message")?,
            },
            "started" => Entry::Started {
                at: text("at")?,
                input: value("input")?,
            },
            "item" => Entry::Item {
                at: text("at")?,
                value: value("value")?,
            },
            "ended" => Entry::Ended {
                at: text("at")?,
                value: value("value")?,
            },
            "emitted" => Entry::Emitted {
                output: text("output")?,
            },
            other => return Err(format!("unknown entry kind `{other}`")),
        };
        let known: &[&str] = match &entry {
            Entry::Done { .. } => &["kind", "at", "input", "result"],
            Entry::Failed { .. } => &["kind", "at", "input", "code", "message"],
            Entry::Started { .. } => &["kind", "at", "input"],
            Entry::Item { .. } | Entry::Ended { .. } => &["kind", "at", "value"],
            Entry::Emitted { .. } => &["kind", "output"],
        };
        if let Some(extra) = map.keys().find(|k| !known.contains(&k.as_str())) {
            return Err(format!("a `{kind}` entry has no field `{extra}`"));
        }
        Ok(entry)
    }
}

/// Everything a caller saved for one run, oldest first.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Journal {
    entries: Vec<Entry>,
}

impl Journal {
    /// An empty journal: a fresh start.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, entry: Entry) {
        self.entries.push(entry);
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The journal as a JSON list of entries.
    pub fn to_json(&self) -> Value {
        Value::Array(self.entries.iter().map(Entry::to_json).collect())
    }

    /// Read a JSON list of entries.
    pub fn from_json(v: &Value) -> Result<Journal, String> {
        let Some(list) = v.as_array() else {
            return Err("a journal is a list of entries".into());
        };
        let entries = list
            .iter()
            .enumerate()
            .map(|(i, e)| Entry::from_json(e).map_err(|p| format!("entry {i}: {p}")))
            .collect::<Result<_, _>>()?;
        Ok(Journal { entries })
    }
}

impl From<Vec<Entry>> for Journal {
    fn from(entries: Vec<Entry>) -> Self {
        Journal { entries }
    }
}

impl FromIterator<Entry> for Journal {
    fn from_iter<I: IntoIterator<Item = Entry>>(iter: I) -> Self {
        Journal {
            entries: iter.into_iter().collect(),
        }
    }
}

/// What a run over an endless stream gives its caller. See the module docs.
#[derive(Debug, Clone, PartialEq)]
pub enum Progress {
    /// The vine emitted this.
    Output(Output),
    /// Keep this line until the position passes its input item.
    Save(Line),
    /// Every input item through this number is fully done. Keep it as the
    /// position, and drop every line at or before it.
    Through(u64),
}

/// One entry of a run over an endless stream, with the input item it
/// belongs to.
#[derive(Debug, Clone, PartialEq)]
pub struct Line {
    /// The input item, counted from 1.
    pub item: u64,
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
    pub fn from_json(v: &Value) -> Result<Line, String> {
        let Some(map) = v.as_object() else {
            return Err("a line is a map".into());
        };
        let Some(item) = map.get("item").and_then(Value::as_u64) else {
            return Err("a line needs `item`, a whole number".into());
        };
        if item == 0 {
            return Err("input items are counted from 1".into());
        }
        let mut rest = map.clone();
        rest.remove("item");
        let entry = Entry::from_json(&Value::Object(rest))?;
        Ok(Line { item, entry })
    }
}

/// What the caller of [`Pipe::run_from`](crate::Pipe::run_from) keeps: the
/// position, and the lines of the input items past it. It is bounded by
/// the work in flight, not by the length of the stream.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Bookmark {
    position: u64,
    lines: Vec<Line>,
}

impl Bookmark {
    /// A fresh start: nothing done, nothing saved.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every input item through `position` is done, and nothing else is
    /// saved. A run from here does again whatever was in flight past the
    /// position: acts are called again and outputs handed out again.
    pub fn at(position: u64) -> Self {
        Bookmark {
            position,
            lines: Vec::new(),
        }
    }

    /// Every input item through this number is done. Feed input from the
    /// item after it.
    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn lines(&self) -> &[Line] {
        &self.lines
    }

    /// How many lines are kept.
    pub fn len(&self) -> usize {
        self.lines.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// Keep what the run handed out: append a line, or move the position
    /// and drop the lines it passed. An output changes nothing.
    pub fn record(&mut self, progress: &Progress) {
        match progress {
            Progress::Output(_) => {}
            Progress::Save(line) => self.lines.push(line.clone()),
            Progress::Through(n) => {
                self.position = *n;
                self.lines.retain(|line| line.item > *n);
            }
        }
    }

    /// The bookmark as `{ "position": n, "lines": [..] }`.
    pub fn to_json(&self) -> Value {
        json!({
            "position": self.position,
            "lines": self.lines.iter().map(Line::to_json).collect::<Vec<_>>(),
        })
    }

    /// Read a bookmark written by [`Bookmark::to_json`].
    pub fn from_json(v: &Value) -> Result<Bookmark, String> {
        let Some(map) = v.as_object() else {
            return Err("a bookmark is a map".into());
        };
        if let Some(extra) = map
            .keys()
            .find(|k| !["position", "lines"].contains(&k.as_str()))
        {
            return Err(format!("a bookmark has no field `{extra}`"));
        }
        let Some(position) = map.get("position").and_then(Value::as_u64) else {
            return Err("a bookmark needs `position`, a whole number".into());
        };
        let Some(list) = map.get("lines").and_then(Value::as_array) else {
            return Err("a bookmark needs `lines`, a list".into());
        };
        let lines = list
            .iter()
            .enumerate()
            .map(|(i, l)| Line::from_json(l).map_err(|p| format!("line {i}: {p}")))
            .collect::<Result<_, _>>()?;
        Ok(Bookmark { position, lines })
    }
}
