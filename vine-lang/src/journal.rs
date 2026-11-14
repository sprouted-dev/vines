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
//!   same output is made again, with the same [`OutputId`](crate::OutputId), and not handed
//!   out twice. The outputs of one group of work are matched in order, so
//!   an output made in a group of an `each` that merges names its group,
//!   and the groups need not finish in the same order again.
//! - [`Entry::Through`] and [`Entry::Stopped`]: for a stream act that gives
//!   its items again, how far a call got, how many outputs made from the
//!   items after that were handed out, and how it stopped. They take the
//!   place of `Item`, `Ended` and the `Emitted` lines of those outputs. On
//!   replay the act is called with `resume_from` set to the position, and
//!   the run hands out again, silently, as many outputs as were handed.
//!   If the call had stopped, the run reads the act only up to the items it
//!   gave before and then gives the saved end.
//!
//! # Output ids
//!
//! Every output a pipe hands out carries an [`OutputId`](crate::OutputId): the input item
//! it belongs to (none in a journal, which covers the whole run), the
//! group of work it was made in, and how many outputs that group made
//! before it. A replay makes the same outputs in the same order within
//! each group, so it makes each id again. No line saves an id: the lines
//! that say where outputs stand are all a replay needs, so a journal or
//! bookmark is the same size it was, and one saved before outputs had ids
//! reads and resumes as it always did, giving the ids a run of today
//! gives. An output handed out in the window between being handed out
//! and its line being saved comes again after a restart, with its id, so a
//! caller drops a repeat by id.
//!
//! # Keeping it small
//!
//! [`Journal::record`] and [`Bookmark::record`] keep what a run hands out.
//! For most entries they append. An [`Entry::Through`] replaces the one
//! before it for the same call, and drops every entry whose key belongs to
//! an item of that call it passed: the keys of item 7 of `pipe[1]#0` start
//! `pipe[1]#0@7/`. It keeps the `Emitted` line of an output `last` lifted
//! out of groups from such an item: that output is never made again, but
//! it took a place in the order of its group's outputs, and the outputs
//! after it keep their ids by it. There is one such line for each time a
//! `last` gives a value. [`Journal::push`] appends and drops nothing.
//!
//! # Groups of work
//!
//! An `each` that merges runs its values side by side, so their acts and
//! outputs can interleave in another order after a restart. In a saved
//! run each value that enters it starts a group, numbered in the order the
//! values enter, and the keys of the acts run for it start with the
//! group's scope, such as `pipe[1].each@3/pipe[1].each.do[0]#0`. Its
//! outputs are saved with `within` set to the scope, such as
//! `pipe[1].each@3/`. A replay matches each group's own acts and outputs in
//! that group's own order. A vine with no such `each` saves exactly the
//! lines it did before groups existed.
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
//!
//! Items can finish out of order under an `each` that merges: item 5 can
//! be done while item 3 is still running. [`Progress::Done`] with 5 says
//! so. The position stays at 2, and [`Bookmark::record`] keeps 5 as done
//! and drops its lines. After a restart the caller feeds items from 3 on,
//! as always, and the run skips item 5 without running it again. So what
//! a bookmark keeps is the lines of the items still in flight, plus the
//! numbers of the items done past the position, kept as runs of numbers.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::pipe::PipeOutput;

/// What a resumable run gives its caller.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The vine emitted this.
    Output(PipeOutput),
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
    /// The caller was handed one output with this name. Its id is not
    /// saved: a replay makes it again from where the output stands.
    /// `within` names the group of work it was made in, such as
    /// `pipe[1].each@3/`, when it was made in a group of an `each` that
    /// merges. A replay matches the outputs of one group in order,
    /// whatever order the groups finish in. `from` names the item of a
    /// stream act call the output was made from, such as `pipe[1]#0@7/`,
    /// when `last` picked that item out of groups: once the call's
    /// position passes it, the output is never made again. The line stays
    /// all the same, so the outputs after it in its group keep their ids.
    Emitted {
        output: String,
        within: Option<String>,
        from: Option<String>,
    },
    /// For a stream act that gives its items again: every item it gave
    /// through number `items` is fully done, and `handed` outputs made
    /// from the items after them were handed out. A later `Through` for
    /// the same `at` replaces this one.
    Through { at: String, items: u64, handed: u64 },
    /// A stream act that gives its items again stopped after giving
    /// `items` items, in the way `how` says.
    Stopped { at: String, items: u64, how: Ending },
}

/// How a stream act that gives its items again stopped.
#[derive(Debug, Clone, PartialEq)]
pub enum Ending {
    /// It ended with this value.
    Ended(Value),
    /// It failed this way.
    Failed { code: String, message: String },
    /// The run stopped reading it partway, because a `catch` or `finalize`
    /// took over the rest of the item's work.
    Abandoned,
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
            Entry::Emitted {
                output,
                within,
                from,
            } => {
                let mut v = json!({ "kind": "emitted", "output": output });
                if let Some(within) = within {
                    v["within"] = json!(within);
                }
                if let Some(from) = from {
                    v["from"] = json!(from);
                }
                v
            }
            Entry::Through { at, items, handed } => {
                json!({ "kind": "through", "at": at, "items": items, "handed": handed })
            }
            Entry::Stopped { at, items, how } => {
                let mut v = json!({ "kind": "stopped", "at": at, "items": items });
                match how {
                    Ending::Ended(value) => {
                        v["how"] = json!("ended");
                        v["value"] = value.clone();
                    }
                    Ending::Failed { code, message } => {
                        v["how"] = json!("failed");
                        v["code"] = json!(code);
                        v["message"] = json!(message);
                    }
                    Ending::Abandoned => v["how"] = json!("abandoned"),
                }
                v
            }
        }
    }

    /// Where the act this entry is about stands, such as `pipe[1]#0`. An
    /// [`Entry::Emitted`] names no act.
    pub fn at(&self) -> Option<&str> {
        match self {
            Entry::Done { at, .. }
            | Entry::Failed { at, .. }
            | Entry::Started { at, .. }
            | Entry::Item { at, .. }
            | Entry::Ended { at, .. }
            | Entry::Through { at, .. }
            | Entry::Stopped { at, .. } => Some(at),
            Entry::Emitted { .. } => None,
        }
    }

    /// Whether `through`, an [`Entry::Through`], makes this entry useless:
    /// it is an earlier `Through` for the same stream act, or it belongs to
    /// an item of that stream that is now done. An output made in a group
    /// belongs to the item its group's scope names. An output `last` lifted
    /// out of groups from such an item is never made again, but its line
    /// still holds its place among the outputs of the group it was handed
    /// out in, and is kept.
    fn passed_by(&self, through: &Entry) -> bool {
        let Entry::Through {
            at: call, items, ..
        } = through
        else {
            return false;
        };
        match self {
            Entry::Through { at, .. } if at == call => true,
            Entry::Emitted { within, .. } => within
                .as_deref()
                .and_then(|scope| item_of_stream(scope, call))
                .is_some_and(|n| n <= *items),
            _ => self
                .at()
                .and_then(|at| item_of_stream(at, call))
                .is_some_and(|n| n <= *items),
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
        let count = |key: &str| -> Result<u64, String> {
            map.get(key)
                .and_then(Value::as_u64)
                .ok_or_else(|| format!("an entry needs `{key}` as a whole number"))
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
                within: match map.get("within") {
                    None => None,
                    Some(_) => Some(text("within")?),
                },
                from: match map.get("from") {
                    None => None,
                    Some(_) => Some(text("from")?),
                },
            },
            "through" => Entry::Through {
                at: text("at")?,
                items: count("items")?,
                handed: count("handed")?,
            },
            "stopped" => Entry::Stopped {
                at: text("at")?,
                items: count("items")?,
                how: match text("how")?.as_str() {
                    "ended" => Ending::Ended(value("value")?),
                    "failed" => Ending::Failed {
                        code: text("code")?,
                        message: text("message")?,
                    },
                    "abandoned" => Ending::Abandoned,
                    other => return Err(format!("unknown way to stop `{other}`")),
                },
            },
            other => return Err(format!("unknown entry kind `{other}`")),
        };
        let known: &[&str] = match &entry {
            Entry::Done { .. } => &["kind", "at", "input", "result"],
            Entry::Failed { .. } => &["kind", "at", "input", "code", "message"],
            Entry::Started { .. } => &["kind", "at", "input"],
            Entry::Item { .. } | Entry::Ended { .. } => &["kind", "at", "value"],
            Entry::Emitted { .. } => &["kind", "output", "within", "from"],
            Entry::Through { .. } => &["kind", "at", "items", "handed"],
            Entry::Stopped { how, .. } => match how {
                Ending::Ended(_) => &["kind", "at", "items", "how", "value"],
                Ending::Failed { .. } => &["kind", "at", "items", "how", "code", "message"],
                Ending::Abandoned => &["kind", "at", "items", "how"],
            },
        };
        if let Some(extra) = map.keys().find(|k| !known.contains(&k.as_str())) {
            return Err(format!("a `{kind}` entry has no field `{extra}`"));
        }
        Ok(entry)
    }
}

/// The number of the stream item a key belongs to, when it belongs to an
/// item the stream act at `call` gave. The keys of item 7 of `pipe[1]#0`
/// start `pipe[1]#0@7/`.
pub(crate) fn item_of_stream(at: &str, call: &str) -> Option<u64> {
    let rest = at.strip_prefix(call)?.strip_prefix('@')?;
    let (number, _) = rest.split_once('/')?;
    number.parse().ok()
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

    /// Append an entry as it is, dropping nothing.
    pub fn push(&mut self, entry: Entry) {
        self.entries.push(entry);
    }

    /// Keep what the run handed out: append an entry, and drop the
    /// entries it makes useless. An [`Entry::Through`] replaces the one
    /// before it for the same stream act and drops the entries of the
    /// items of that stream it passed. An output changes nothing.
    pub fn record(&mut self, event: &Event) {
        if let Event::Save(entry) = event {
            if matches!(entry, Entry::Through { .. }) {
                self.entries.retain(|kept| !kept.passed_by(entry));
            }
            self.entries.push(entry.clone());
        }
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
    Output(PipeOutput),
    /// Keep this line until the position passes its input item.
    Save(Line),
    /// Every input item through this number is fully done. Keep it as the
    /// position, and drop every line at or before it.
    Through(u64),
    /// This input item, past the position, is fully done while an item
    /// before it is not. Keep it as done, and drop its lines. Only an
    /// `each` that merges finishes items out of order.
    Done(u64),
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
    /// position: acts are called again and outputs handed out again.
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

    pub fn lines(&self) -> &[Line] {
        &self.lines
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

    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// Keep what the run handed out: append a line, or move the position
    /// and drop the lines it passed. A line with an [`Entry::Through`]
    /// replaces the one before it for the same stream act, and drops the
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
    pub fn from_json(v: &Value) -> Result<Bookmark, String> {
        let Some(map) = v.as_object() else {
            return Err("a bookmark is a map".into());
        };
        if let Some(extra) = map
            .keys()
            .find(|k| !["position", "lines", "done"].contains(&k.as_str()))
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
        let mut bookmark = Bookmark {
            position,
            lines,
            done: BTreeMap::new(),
        };
        let runs = match map.get("done") {
            None => &[][..],
            Some(v) => v
                .as_array()
                .ok_or("a bookmark's `done` is a list of runs")?,
        };
        for (i, run) in runs.iter().enumerate() {
            let pair = run.as_array().map(|p| (p.len(), p.first(), p.get(1)));
            let (Some(first), Some(last)) = (
                pair.and_then(|(_, f, _)| f?.as_u64()),
                pair.and_then(|(len, _, l)| if len == 2 { l?.as_u64() } else { None }),
            ) else {
                return Err(format!(
                    "done run {i}: a run is [first, last], whole numbers"
                ));
            };
            if first > last || first <= position {
                return Err(format!(
                    "done run {i}: a run lies past the position, first to last"
                ));
            }
            bookmark.mark_done(first, last);
        }
        Ok(bookmark)
    }
}
