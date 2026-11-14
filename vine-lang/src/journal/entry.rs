//! One saved line of a run, [`Entry`], and its JSON form.

use serde_json::{json, Value};
use thiserror::Error;

/// Saved JSON that is not an [`Entry`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EntryError {
    /// The entry is not a map.
    #[error("an entry is a map")]
    NotAMap,
    /// A field that holds text is absent or not text.
    #[error("an entry needs `{key}` as text")]
    NeedsText {
        /// The field.
        key: &'static str,
    },
    /// A field is absent.
    #[error("an entry needs `{key}`")]
    Needs {
        /// The field.
        key: &'static str,
    },
    /// A field that holds a count is absent or not a whole number.
    #[error("an entry needs `{key}` as a whole number")]
    NeedsCount {
        /// The field.
        key: &'static str,
    },
    /// A `stopped` entry's `how` names no way to stop.
    #[error("unknown way to stop `{how}`")]
    UnknownEnding {
        /// The `how` as saved.
        how: String,
    },
    /// A `timed` entry's `timing` names no decision.
    #[error("unknown timing `{timing}`")]
    UnknownTiming {
        /// The `timing` as saved.
        timing: String,
    },
    /// The `kind` names no entry.
    #[error("unknown entry kind `{kind}`")]
    UnknownKind {
        /// The `kind` as saved.
        kind: String,
    },
    /// The entry holds a field its kind does not have.
    #[error("a `{kind}` entry has no field `{field}`")]
    UnknownField {
        /// The entry's `kind`.
        kind: String,
        /// The field it does not have.
        field: String,
    },
}

/// One line of a run's journal.
#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    /// The call at `at` was given `input` and gave `result`.
    Done {
        at: String,
        input: Value,
        result: Value,
    },
    /// The call at `at` was given `input` and failed. For a stream operator this
    /// ends its stream.
    Failed {
        at: String,
        input: Value,
        code: String,
        message: String,
    },
    /// The stream operator at `at` was called with `input`.
    Started { at: String, input: Value },
    /// The stream operator at `at` gave one more item.
    Item { at: String, value: Value },
    /// The stream operator at `at` ended with `value`.
    Ended { at: String, value: Value },
    /// The caller was handed one output with this name. Its id is not
    /// saved: a replay makes it again from where the output stands.
    /// `within` names the group of work it was made in, such as
    /// `pipe[1].mergeMap@3/`, when it was made in a group of a
    /// `mergeMap`. A replay matches the outputs of one group in order,
    /// whatever order the groups finish in. `from` names the item of a
    /// stream operator call the output was made from, such as `pipe[1]#0@7/`,
    /// when `last` picked that item out of groups: once the call's
    /// position passes it, the output is never made again. The line stays
    /// all the same, so the outputs after it in its group keep their ids.
    Emitted {
        output: String,
        within: Option<String>,
        from: Option<String>,
    },
    /// For a stream operator that gives its items again: every item it gave
    /// through number `items` is fully done, and `handed` outputs made
    /// from the items after them were handed out. A later `Through` for
    /// the same `at` replaces this one.
    Through { at: String, items: u64, handed: u64 },
    /// A stream operator that gives its items again stopped after giving
    /// `items` items, in the way `how` says.
    Stopped { at: String, items: u64, how: Ending },
    /// The `timeout` at `at` decided this for one run of the operators it
    /// applies to, such as `pipe[2].timeout@0`. A replay takes the decision
    /// from here and does not ask the clock again.
    Timed { at: String, timing: Timing },
}

/// What a `timeout` decided for one run of the operators it applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timing {
    /// The operators were done before the limit. A replay runs them again
    /// with no limit, so they end as they did even if they are slower.
    InTime,
    /// The limit came first, and the operators were dropped. A replay fails
    /// the item at once and does not run them.
    TimedOut,
}

impl Timing {
    fn as_str(self) -> &'static str {
        match self {
            Timing::InTime => "in_time",
            Timing::TimedOut => "timed_out",
        }
    }

    fn parse(text: &str) -> Option<Timing> {
        match text {
            "in_time" => Some(Timing::InTime),
            "timed_out" => Some(Timing::TimedOut),
            _ => None,
        }
    }
}

/// How a stream operator that gives its items again stopped.
#[derive(Debug, Clone, PartialEq)]
pub enum Ending {
    /// It ended with this value.
    Ended(Value),
    /// It failed this way.
    Failed { code: String, message: String },
    /// The run stopped reading it partway, because a `catchError` or `finalize`
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
            Entry::Timed { at, timing } => {
                json!({ "kind": "timed", "at": at, "timing": timing.as_str() })
            }
        }
    }

    /// Where the call this entry is about stands, such as `pipe[1]#0`. An
    /// [`Entry::Emitted`] names no call.
    pub fn at(&self) -> Option<&str> {
        match self {
            Entry::Done { at, .. }
            | Entry::Failed { at, .. }
            | Entry::Started { at, .. }
            | Entry::Item { at, .. }
            | Entry::Ended { at, .. }
            | Entry::Through { at, .. }
            | Entry::Stopped { at, .. }
            | Entry::Timed { at, .. } => Some(at),
            Entry::Emitted { .. } => None,
        }
    }

    /// Whether `through`, an [`Entry::Through`], makes this entry useless:
    /// it is an earlier `Through` for the same stream operator, or it belongs to
    /// an item of that stream that is now done. An output made in a group
    /// belongs to the item its group's scope names. An output `last` lifted
    /// out of groups from such an item is never made again, but its line
    /// still holds its place among the outputs of the group it was handed
    /// out in, and is kept.
    pub(super) fn passed_by(&self, through: &Entry) -> bool {
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

    /// The fields the JSON form of this entry holds, as
    /// [`Entry::to_json`] writes them.
    fn json_fields(&self) -> &'static [&'static str] {
        match self {
            Self::Done { .. } => &["kind", "at", "input", "result"],
            Self::Failed { .. } => &["kind", "at", "input", "code", "message"],
            Self::Started { .. } => &["kind", "at", "input"],
            Self::Item { .. } | Self::Ended { .. } => &["kind", "at", "value"],
            Self::Emitted { .. } => &["kind", "output", "within", "from"],
            Self::Through { .. } => &["kind", "at", "items", "handed"],
            Self::Stopped { how, .. } => match how {
                Ending::Ended(_) => &["kind", "at", "items", "how", "value"],
                Ending::Failed { .. } => &["kind", "at", "items", "how", "code", "message"],
                Ending::Abandoned => &["kind", "at", "items", "how"],
            },
            Self::Timed { .. } => &["kind", "at", "timing"],
        }
    }

    /// Read an entry written by [`Entry::to_json`].
    pub fn from_json(v: &Value) -> Result<Entry, EntryError> {
        let Some(map) = v.as_object() else {
            return Err(EntryError::NotAMap);
        };
        let text = |key: &'static str| -> Result<String, EntryError> {
            map.get(key)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or(EntryError::NeedsText { key })
        };
        let value = |key: &'static str| -> Result<Value, EntryError> {
            map.get(key).cloned().ok_or(EntryError::Needs { key })
        };
        let count = |key: &'static str| -> Result<u64, EntryError> {
            map.get(key)
                .and_then(Value::as_u64)
                .ok_or(EntryError::NeedsCount { key })
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
                    other => {
                        return Err(EntryError::UnknownEnding {
                            how: other.to_owned(),
                        })
                    }
                },
            },
            "timed" => {
                let timing = text("timing")?;
                Entry::Timed {
                    at: text("at")?,
                    timing: Timing::parse(&timing).ok_or_else(|| EntryError::UnknownTiming {
                        timing: timing.clone(),
                    })?,
                }
            }
            other => {
                return Err(EntryError::UnknownKind {
                    kind: other.to_owned(),
                })
            }
        };
        let known = entry.json_fields();
        if let Some(extra) = map.keys().find(|k| !known.contains(&k.as_str())) {
            return Err(EntryError::UnknownField {
                kind,
                field: extra.clone(),
            });
        }
        Ok(entry)
    }
}

/// The number of the stream item a key belongs to, when it belongs to an
/// item the stream operator at `call` gave. The keys of item 7 of `pipe[1]#0`
/// start `pipe[1]#0@7/`.
pub(crate) fn item_of_stream(at: &str, call: &str) -> Option<u64> {
    let rest = at.strip_prefix(call)?.strip_prefix('@')?;
    let (number, _) = rest.split_once('/')?;
    number.parse().ok()
}
