//! What a resumable run asks its caller to save, and gets back after a
//! restart.
//!
//! [`Pipe::run_saved`](crate::Pipe::run_saved) gives a stream of
//! [`Event`]s. An [`Event::Output`] is something the vine emitted. An
//! [`Event::Save`] is one [`Entry`] the caller appends to its store. The
//! library stores nothing. To resume, the caller builds a [`Journal`] from
//! what it stored and runs the vine again over the same input items.
//!
//! The journal is plain data. [`Entry::to_json`] and [`Journal::from_json`]
//! turn it into JSON and back.
//!
//! # What an entry means
//!
//! Each act a run calls has a key: the path of its stage and a count of how
//! many times that stage ran before in the run, such as `pipe[1]#0`. A
//! replay calls the stages in the same order, so the keys line up.
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
