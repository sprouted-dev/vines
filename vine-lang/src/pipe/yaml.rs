//! The YAML layer. It reads the same operators the functions in the parent
//! module build.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{json, Value};

use super::check::{assemble, malformed};
use super::{Flatten, Keep, Operator, Pipe, PipeDef};
use crate::error::LoadError;
use crate::registry::Registry;
use crate::shape::Shape;

/// The YAML layer. It reads the same operators the functions above build.
#[cfg(feature = "yaml")]
pub(crate) fn load(reg: &Registry, yaml: &str) -> Result<Pipe, LoadError> {
    let raw: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|e| LoadError::Yaml(e.to_string()))?;
    let doc: Value = serde_json::to_value(raw).map_err(|e| LoadError::Yaml(e.to_string()))?;
    let Some(map) = doc.as_object() else {
        return Err(malformed("vine", "a vine document is a map"));
    };
    if let Some(key) = map
        .keys()
        .find(|k| !["vine", "input", "outputs", "pipe"].contains(&k.as_str()))
    {
        return Err(LoadError::UnknownKey {
            path: "vine".into(),
            key: key.clone(),
        });
    }
    let name = match map.get("vine") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(malformed("vine", "missing `vine` (the vine's name)")),
    };
    let input = match map.get("input") {
        Some(v) => Shape::parse(v).map_err(|p| malformed("input", &p.to_string()))?,
        None => return Err(malformed("vine", "missing `input` (the input shape)")),
    };
    let mut outputs = BTreeMap::new();
    for (k, v) in map
        .get("outputs")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let shape =
            Shape::parse(v).map_err(|p| malformed(&format!("outputs.{k}"), &p.to_string()))?;
        outputs.insert(k.clone(), shape);
    }
    let Some(ops) = map.get("pipe") else {
        return Err(malformed("vine", "missing `pipe`"));
    };
    let ops = read_pipe(ops, "pipe")?;
    assemble(
        reg,
        PipeDef {
            name,
            input,
            outputs,
            operators: ops,
        },
    )
}

/// The four mapping operator keys, for a refusal that names them.
const MAP_KEYS: &str = "`concatMap`, `mergeMap`, `switchMap` or `exhaustMap`";

/// Operator keys from before the operators took their `RxJS` names, and what
/// replaces each. A vine that still uses one is refused, not read as an
/// operator of that name.
const RENAMED_STAGES: &[(&str, &str)] = &[
    ("each", MAP_KEYS),
    ("collect", "`bufferCount`"),
    ("together", "`forkJoin`"),
    ("catch", "`catchError`"),
];

/// Arguments from before `retry` and `bufferCount` took their `RxJS` names.
const RENAMED_RETRY: &[(&str, &str)] = &[("times", "`count`"), ("wait", "`delay`")];
const RENAMED_BUFFER_COUNT: &[(&str, &str)] = &[("size", "`bufferSize`")];

/// Refuse an argument by its old name, naming the new one.
fn refuse_renamed(body: &Value, path: &str, names: &[(&str, &str)]) -> Result<(), LoadError> {
    let Some(m) = body.as_object() else {
        return Ok(());
    };
    match names.iter().find(|(old, _)| m.contains_key(*old)) {
        Some((old, new)) => Err(renamed(path, old, new)),
        None => Ok(()),
    }
}

fn renamed(path: &str, old: &str, new: &str) -> LoadError {
    LoadError::Renamed {
        path: path.into(),
        old: old.into(),
        new: new.into(),
    }
}

#[cfg(feature = "yaml")]
fn read_pipe(v: &Value, path: &str) -> Result<Vec<Operator>, LoadError> {
    let Some(list) = v.as_array() else {
        return Err(malformed(path, "a pipe is a list of operators"));
    };
    list.iter()
        .enumerate()
        .map(|(i, s)| read_operator(s, &format!("{path}[{i}]")))
        .collect()
}

/// Every key the vine language gives a meaning to. Any other key names a
/// registered pure operator.
const WORDS: &[&str] = &[
    "call",
    "tap",
    "split",
    "filter",
    "take",
    "last",
    "bufferCount",
    "forkJoin",
    "concatMap",
    "mergeMap",
    "switchMap",
    "exhaustMap",
    "fork",
    "catchError",
    "finalize",
    "retry",
    "timeout",
    "use",
    "branch",
    "emit",
];

#[cfg(feature = "yaml")]
fn read_operator(v: &Value, path: &str) -> Result<Operator, LoadError> {
    if v.as_str() == Some("last") {
        return Ok(Operator::Last);
    }
    let Some(map) = v.as_object() else {
        return Err(malformed(
            path,
            "an operator is a map with one key, or the word `last`",
        ));
    };
    let mut main: Vec<&String> = map
        .keys()
        .filter(|k| !["with", "into", "on", "end"].contains(&k.as_str()))
        .collect();
    let key = match (main.pop(), main.pop()) {
        (Some(key), None) => key.as_str(),
        (None, _) => {
            return Err(malformed(
                path,
                "an operator needs one key that says what it does",
            ))
        }
        (Some(a), Some(b)) => {
            return Err(malformed(
                path,
                &format!("an operator does one thing, found `{b}` and `{a}`"),
            ))
        }
    };
    if let Some((old, new)) = RENAMED_STAGES.iter().find(|(old, _)| *old == key) {
        return Err(renamed(path, old, new));
    }
    let into = match map.get("into") {
        None => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(malformed(&format!("{path}.into"), "`into` is a name")),
    };
    Written {
        map,
        key,
        body: &map[key],
        path,
        here: format!("{path}.{key}"),
        into,
    }
    .read()
}

/// One operator as written: the map it is, its key, the value under the
/// key, and where it stands.
struct Written<'a> {
    map: &'a serde_json::Map<String, Value>,
    key: &'a str,
    body: &'a Value,
    /// The operator's path, such as `pipe[1]`.
    path: &'a str,
    /// The path of its key, such as `pipe[1].retry`.
    here: String,
    into: Option<String>,
}

impl Written<'_> {
    fn read(self) -> Result<Operator, LoadError> {
        let (body, here) = (self.body, self.here.as_str());
        Ok(match self.key {
            "call" => self.call()?,
            "tap" => {
                self.takes(true, false)?;
                Operator::Call {
                    name: self.text(
                        body,
                        "`tap` names one operator, such as `git.refresh_branch`",
                    )?,
                    with: self.with(),
                    keep: Keep::Tap,
                    end: None,
                }
            }
            "split" => {
                self.takes(false, true)?;
                Operator::Split {
                    from: body.clone(),
                    into: self.into,
                }
            }
            "filter" => {
                self.takes(false, false)?;
                Operator::Filter { when: body.clone() }
            }
            "take" => {
                self.takes(false, false)?;
                Operator::Take(
                    body.as_u64()
                        .ok_or_else(|| malformed(here, "`take` is a whole number"))?,
                )
            }
            "last" => {
                self.takes(false, false)?;
                if !body.is_null() {
                    return Err(malformed(here, "`last` takes nothing: write `- last`"));
                }
                Operator::Last
            }
            "bufferCount" => self.buffer_count()?,
            "fork" => {
                self.takes(false, false)?;
                let Some(sides) = body.as_array() else {
                    return Err(malformed(here, "`fork` is a list of side pipes"));
                };
                Operator::Fork(
                    sides
                        .iter()
                        .enumerate()
                        .map(|(j, side)| read_pipe(side, &format!("{here}[{j}]")))
                        .collect::<Result<_, _>>()?,
                )
            }
            "forkJoin" => {
                self.takes(false, false)?;
                Operator::ForkJoin(read_pipe(body, here)?)
            }
            "concatMap" | "mergeMap" | "switchMap" | "exhaustMap" => self.mapped()?,
            "catchError" => {
                self.takes(false, false)?;
                Operator::CatchError(read_pipe(body, here)?)
            }
            "finalize" => {
                self.takes(false, false)?;
                Operator::Finalize(read_pipe(body, here)?)
            }
            "retry" => self.retry()?,
            "timeout" => {
                self.takes(false, false)?;
                Operator::Timeout(
                    body.as_str()
                        .and_then(parse_duration)
                        .ok_or_else(|| malformed(here, "`timeout` is a time such as `30s`"))?,
                )
            }
            "use" => self.use_vine()?,
            "branch" => self.branch()?,
            "emit" => self.emit()?,
            name => {
                debug_assert!(!WORDS.contains(&name));
                self.takes(false, true)?;
                Operator::Named {
                    name: name.to_owned(),
                    with: body.clone(),
                    keep: keep(self.into),
                }
            }
        })
    }

    /// The body as text, or a refusal that says `what` it should be.
    fn text(&self, v: &Value, what: &str) -> Result<String, LoadError> {
        v.as_str()
            .map(str::to_owned)
            .ok_or_else(|| malformed(&self.here, what))
    }

    /// Refuse `with`, `into`, `on` or `end` where this operator does not
    /// take it. `with` and `into` are taken as the two flags say.
    fn takes(&self, with: bool, into_ok: bool) -> Result<(), LoadError> {
        for (present, allowed, key) in [
            (self.map.contains_key("with"), with, "with"),
            (self.into.is_some(), into_ok, "into"),
            (self.map.contains_key("on"), self.key == "use", "on"),
            (self.map.contains_key("end"), self.key == "call", "end"),
        ] {
            if present && !allowed {
                return Err(LoadError::UnknownKey {
                    path: self.path.into(),
                    key: key.into(),
                });
            }
        }
        Ok(())
    }

    /// The body as a map that holds only `allowed` keys.
    fn fields(&self, allowed: &[&str]) -> Result<&serde_json::Map<String, Value>, LoadError> {
        let Some(m) = self.body.as_object() else {
            return Err(malformed(
                &self.here,
                &format!("`{}` takes a map", self.key),
            ));
        };
        match m.keys().find(|k| !allowed.contains(&k.as_str())) {
            Some(k) => Err(LoadError::UnknownKey {
                path: self.here.clone(),
                key: k.clone(),
            }),
            None => Ok(m),
        }
    }

    /// The operator's `with`, or an empty map.
    fn with(&self) -> Value {
        self.map.get("with").cloned().unwrap_or_else(|| json!({}))
    }

    fn call(self) -> Result<Operator, LoadError> {
        let path = self.path;
        self.takes(true, true)?;
        let end = match self.map.get("end") {
            None => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(_) => return Err(malformed(&format!("{path}.end"), "`end` is a name")),
        };
        Ok(Operator::Call {
            name: self.text(
                self.body,
                "`call` names one operator, such as `git.worktree_add`",
            )?,
            with: self.with(),
            keep: keep(self.into),
            end,
        })
    }

    fn buffer_count(self) -> Result<Operator, LoadError> {
        let here = self.here.as_str();
        self.takes(false, true)?;
        refuse_renamed(self.body, here, RENAMED_BUFFER_COUNT)?;
        let m = self.fields(&["bufferSize"])?;
        let buffer_size = m
            .get("bufferSize")
            .and_then(Value::as_u64)
            .ok_or_else(|| malformed(here, "`bufferCount` needs `bufferSize`, a whole number"))?;
        Ok(Operator::BufferCount {
            buffer_size,
            into: self.into,
        })
    }

    fn mapped(&self) -> Result<Operator, LoadError> {
        let (key, here) = (self.key, self.here.as_str());
        self.takes(false, false)?;
        let flatten = match key {
            "concatMap" => Flatten::Concat,
            "mergeMap" => Flatten::Merge,
            "switchMap" => Flatten::Switch,
            _ => Flatten::Exhaust,
        };
        let Some(m) = self.body.as_object() else {
            return Err(malformed(here, &format!("`{key}` takes a map")));
        };
        if m.contains_key("overlap") {
            return Err(renamed(
                here,
                "overlap",
                &format!("{MAP_KEYS} as the operator key"),
            ));
        }
        if m.contains_key("at_once") {
            return Err(renamed(here, "at_once", "`concurrent`"));
        }
        let m = self.fields(&["do", "concurrent"])?;
        let concurrent = match m.get("concurrent") {
            None => None,
            Some(v) => Some(
                v.as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| {
                        malformed(
                            &format!("{here}.concurrent"),
                            "`concurrent` is a whole number",
                        )
                    })?,
            ),
        };
        let Some(body) = m.get("do") else {
            return Err(malformed(
                here,
                &format!("`{key}` needs `do`, the pipe to run"),
            ));
        };
        Ok(Operator::Mapped {
            flatten,
            concurrent,
            pipe: read_pipe(body, &format!("{here}.do"))?,
        })
    }

    fn retry(&self) -> Result<Operator, LoadError> {
        let here = self.here.as_str();
        self.takes(false, false)?;
        refuse_renamed(self.body, here, RENAMED_RETRY)?;
        let m = self.fields(&["count", "delay"])?;
        let count = m
            .get("count")
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| malformed(here, "`retry` needs `count`, a whole number"))?;
        let delay = match m.get("delay") {
            None => Duration::ZERO,
            Some(v) => v.as_str().and_then(parse_duration).ok_or_else(|| {
                malformed(&format!("{here}.delay"), "`delay` is a time such as `5s`")
            })?,
        };
        Ok(Operator::Retry { count, delay })
    }

    fn use_vine(self) -> Result<Operator, LoadError> {
        let path = self.path;
        self.takes(true, true)?;
        let mut on = Vec::new();
        match self.map.get("on") {
            None => {}
            Some(Value::Object(arms)) => {
                for (output, ops) in arms {
                    on.push((
                        output.clone(),
                        read_pipe(ops, &format!("{path}.on.{output}"))?,
                    ));
                }
            }
            Some(_) => {
                return Err(malformed(
                    &format!("{path}.on"),
                    "`on` is a map of output name to operators",
                ))
            }
        }
        Ok(Operator::Use {
            vine: self.text(self.body, "`use` names one vine")?,
            with: self.map.get("with").cloned(),
            keep: keep(self.into),
            on,
        })
    }

    fn branch(&self) -> Result<Operator, LoadError> {
        let here = self.here.as_str();
        self.takes(false, false)?;
        let m = self.fields(&["on", "cases", "else"])?;
        let (Some(on), Some(Value::Object(cases))) = (m.get("on"), m.get("cases")) else {
            return Err(malformed(here, "`branch` needs `on` and a map of `cases`"));
        };
        let mut arms = Vec::new();
        for (k, p) in cases {
            arms.push((k.clone(), read_pipe(p, &format!("{here}.cases.{k}"))?));
        }
        let otherwise = match m.get("else") {
            Some(p) => Some(read_pipe(p, &format!("{here}.else"))?),
            None => None,
        };
        Ok(Operator::Branch {
            on: on.clone(),
            cases: arms,
            otherwise,
        })
    }

    fn emit(&self) -> Result<Operator, LoadError> {
        self.takes(false, false)?;
        if let Value::String(to) = self.body {
            return Ok(Operator::Emit {
                to: to.clone(),
                with: None,
            });
        }
        let m = self.fields(&["to", "with"])?;
        let Some(Value::String(to)) = m.get("to") else {
            return Err(malformed(&self.here, "`emit` names the output in `to`"));
        };
        Ok(Operator::Emit {
            to: to.clone(),
            with: m.get("with").cloned(),
        })
    }
}

/// What a result does to the item: added under `into`, or the new item.
fn keep(into: Option<String>) -> Keep {
    into.map_or(Keep::Replace, Keep::Into)
}

/// `500ms`, `60s`, `2m`, `1h`.
fn parse_duration(s: &str) -> Option<Duration> {
    let split = s.find(|c: char| !c.is_ascii_digit())?;
    let (n, unit) = s.split_at(split);
    let n: u64 = n.parse().ok()?;
    Some(match unit {
        "ms" => Duration::from_millis(n),
        "s" => Duration::from_secs(n),
        "m" => Duration::from_secs(n.checked_mul(60)?),
        "h" => Duration::from_secs(n.checked_mul(3600)?),
        _ => return None,
    })
}
