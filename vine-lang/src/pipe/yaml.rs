//! The YAML layer. It reads the same stages the functions in the parent
//! module build.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{json, Value};

use super::check::{assemble, malformed};
use super::{Flatten, Keep, Pipe, PipeDef, Stage};
use crate::error::LoadError;
use crate::registry::Registry;
use crate::shape::Shape;

/// The YAML layer. It reads the same stages the functions above build.
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
        Some(v) => Shape::parse(v).map_err(|p| malformed("input", &p))?,
        None => return Err(malformed("vine", "missing `input` (the input shape)")),
    };
    let mut outputs = BTreeMap::new();
    for (k, v) in map
        .get("outputs")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let shape = Shape::parse(v).map_err(|p| malformed(&format!("outputs.{k}"), &p))?;
        outputs.insert(k.clone(), shape);
    }
    let Some(stages) = map.get("pipe") else {
        return Err(malformed("vine", "missing `pipe`"));
    };
    let stages = read_pipe(stages, "pipe")?;
    assemble(
        reg,
        PipeDef {
            name,
            input,
            outputs,
            stages,
        },
    )
}

/// The four mapping stage keys, for a refusal that names them.
const MAP_KEYS: &str = "`concatMap`, `mergeMap`, `switchMap` or `exhaustMap`";

/// Stage keys from before the stages took their RxJS names, and what
/// replaces each. A vine that still uses one is refused, not read as an
/// operator of that name.
const RENAMED_STAGES: &[(&str, &str)] = &[
    ("each", MAP_KEYS),
    ("collect", "`bufferCount`"),
    ("together", "`forkJoin`"),
    ("catch", "`catchError`"),
];

fn renamed(path: &str, old: &str, new: &str) -> LoadError {
    LoadError::Renamed {
        path: path.into(),
        old: old.into(),
        new: new.into(),
    }
}

#[cfg(feature = "yaml")]
fn read_pipe(v: &Value, path: &str) -> Result<Vec<Stage>, LoadError> {
    let Some(list) = v.as_array() else {
        return Err(malformed(path, "a pipe is a list of stages"));
    };
    list.iter()
        .enumerate()
        .map(|(i, s)| read_stage(s, &format!("{path}[{i}]")))
        .collect()
}

#[cfg(feature = "yaml")]
fn read_stage(v: &Value, path: &str) -> Result<Stage, LoadError> {
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
    if v.as_str() == Some("last") {
        return Ok(Stage::Last);
    }
    let Some(map) = v.as_object() else {
        return Err(malformed(
            path,
            "a stage is a map with one key, or the word `last`",
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
                "a stage needs one key that says what it does",
            ))
        }
        (Some(a), Some(b)) => {
            return Err(malformed(
                path,
                &format!("a stage does one thing, found `{b}` and `{a}`"),
            ))
        }
    };
    if let Some((old, new)) = RENAMED_STAGES.iter().find(|(old, _)| *old == key) {
        return Err(renamed(path, old, new));
    }
    let body = &map[key];
    let here = format!("{path}.{key}");
    let text = |v: &Value, what: &str| -> Result<String, LoadError> {
        v.as_str()
            .map(str::to_owned)
            .ok_or_else(|| malformed(&here, what))
    };
    let into = match map.get("into") {
        None => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(malformed(&format!("{path}.into"), "`into` is a name")),
    };
    let takes = |with: bool, into_ok: bool| -> Result<(), LoadError> {
        for (present, allowed, key) in [
            (map.contains_key("with"), with, "with"),
            (into.is_some(), into_ok, "into"),
            (map.contains_key("on"), key == "use", "on"),
            (map.contains_key("end"), key == "call", "end"),
        ] {
            if present && !allowed {
                return Err(LoadError::UnknownKey {
                    path: path.into(),
                    key: key.into(),
                });
            }
        }
        Ok(())
    };
    let fields = |allowed: &[&str]| -> Result<&serde_json::Map<String, Value>, LoadError> {
        let Some(m) = body.as_object() else {
            return Err(malformed(&here, &format!("`{key}` takes a map")));
        };
        match m.keys().find(|k| !allowed.contains(&k.as_str())) {
            Some(k) => Err(LoadError::UnknownKey {
                path: here.clone(),
                key: k.clone(),
            }),
            None => Ok(m),
        }
    };
    let with = || map.get("with").cloned().unwrap_or_else(|| json!({}));
    let keep = |into: Option<String>| into.map_or(Keep::Replace, Keep::Into);
    Ok(match key {
        "call" => {
            takes(true, true)?;
            let end = match map.get("end") {
                None => None,
                Some(Value::String(s)) => Some(s.clone()),
                Some(_) => return Err(malformed(&format!("{path}.end"), "`end` is a name")),
            };
            Stage::Call {
                act: text(body, "`call` names one act, such as `git.worktree_add`")?,
                with: with(),
                keep: keep(into),
                end,
            }
        }
        "tap" => {
            takes(true, false)?;
            Stage::Call {
                act: text(body, "`tap` names one act, such as `git.refresh_branch`")?,
                with: with(),
                keep: Keep::Tap,
                end: None,
            }
        }
        "split" => {
            takes(false, true)?;
            Stage::Split {
                from: body.clone(),
                into,
            }
        }
        "filter" => {
            takes(false, false)?;
            Stage::Filter { when: body.clone() }
        }
        "take" => {
            takes(false, false)?;
            Stage::Take(
                body.as_u64()
                    .ok_or_else(|| malformed(&here, "`take` is a whole number"))?,
            )
        }
        "last" => {
            takes(false, false)?;
            if !body.is_null() {
                return Err(malformed(&here, "`last` takes nothing: write `- last`"));
            }
            Stage::Last
        }
        "bufferCount" => {
            takes(false, true)?;
            let m = fields(&["size"])?;
            let size = m
                .get("size")
                .and_then(Value::as_u64)
                .ok_or_else(|| malformed(&here, "`bufferCount` needs `size`, a whole number"))?;
            Stage::BufferCount { size, into }
        }
        "fork" => {
            takes(false, false)?;
            let Some(sides) = body.as_array() else {
                return Err(malformed(&here, "`fork` is a list of side pipes"));
            };
            Stage::Fork(
                sides
                    .iter()
                    .enumerate()
                    .map(|(j, side)| read_pipe(side, &format!("{here}[{j}]")))
                    .collect::<Result<_, _>>()?,
            )
        }
        "forkJoin" => {
            takes(false, false)?;
            Stage::ForkJoin(read_pipe(body, &here)?)
        }
        "concatMap" | "mergeMap" | "switchMap" | "exhaustMap" => {
            takes(false, false)?;
            let flatten = match key {
                "concatMap" => Flatten::Concat,
                "mergeMap" => Flatten::Merge,
                "switchMap" => Flatten::Switch,
                _ => Flatten::Exhaust,
            };
            let Some(m) = body.as_object() else {
                return Err(malformed(&here, &format!("`{key}` takes a map")));
            };
            if m.contains_key("overlap") {
                return Err(renamed(
                    &here,
                    "overlap",
                    &format!("{MAP_KEYS} as the stage key"),
                ));
            }
            if m.contains_key("at_once") {
                return Err(renamed(&here, "at_once", "`concurrent`"));
            }
            let m = fields(&["do", "concurrent"])?;
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
                    &here,
                    &format!("`{key}` needs `do`, the pipe to run"),
                ));
            };
            Stage::Mapped {
                flatten,
                concurrent,
                pipe: read_pipe(body, &format!("{here}.do"))?,
            }
        }
        "catchError" => {
            takes(false, false)?;
            Stage::CatchError(read_pipe(body, &here)?)
        }
        "finalize" => {
            takes(false, false)?;
            Stage::Finalize(read_pipe(body, &here)?)
        }
        "retry" => {
            takes(false, false)?;
            let m = fields(&["times", "wait"])?;
            let times = m
                .get("times")
                .and_then(Value::as_u64)
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| malformed(&here, "`retry` needs `times`, a whole number"))?;
            let wait = match m.get("wait") {
                None => Duration::ZERO,
                Some(v) => v
                    .as_str()
                    .and_then(crate::stream::parse_duration)
                    .ok_or_else(|| {
                        malformed(&format!("{here}.wait"), "`wait` is a time such as `5s`")
                    })?,
            };
            Stage::Retry { times, wait }
        }
        "timeout" => {
            takes(false, false)?;
            Stage::Timeout(
                body.as_str()
                    .and_then(crate::stream::parse_duration)
                    .ok_or_else(|| malformed(&here, "`timeout` is a time such as `30s`"))?,
            )
        }
        "use" => {
            takes(true, true)?;
            let mut on = Vec::new();
            match map.get("on") {
                None => {}
                Some(Value::Object(arms)) => {
                    for (output, stages) in arms {
                        on.push((
                            output.clone(),
                            read_pipe(stages, &format!("{path}.on.{output}"))?,
                        ));
                    }
                }
                Some(_) => {
                    return Err(malformed(
                        &format!("{path}.on"),
                        "`on` is a map of output name to stages",
                    ))
                }
            }
            Stage::Use {
                vine: text(body, "`use` names one vine")?,
                with: map.get("with").cloned(),
                keep: keep(into),
                on,
            }
        }
        "branch" => {
            takes(false, false)?;
            let m = fields(&["on", "cases", "else"])?;
            let (Some(on), Some(Value::Object(cases))) = (m.get("on"), m.get("cases")) else {
                return Err(malformed(&here, "`branch` needs `on` and a map of `cases`"));
            };
            let mut arms = Vec::new();
            for (k, p) in cases {
                arms.push((k.clone(), read_pipe(p, &format!("{here}.cases.{k}"))?));
            }
            let otherwise = match m.get("else") {
                Some(p) => Some(read_pipe(p, &format!("{here}.else"))?),
                None => None,
            };
            Stage::Branch {
                on: on.clone(),
                cases: arms,
                otherwise,
            }
        }
        "emit" => {
            takes(false, false)?;
            match body {
                Value::String(to) => Stage::Emit {
                    to: to.clone(),
                    with: None,
                },
                _ => {
                    let m = fields(&["to", "with"])?;
                    let Some(Value::String(to)) = m.get("to") else {
                        return Err(malformed(&here, "`emit` names the output in `to`"));
                    };
                    Stage::Emit {
                        to: to.clone(),
                        with: m.get("with").cloned(),
                    }
                }
            }
        }
        name => {
            debug_assert!(!WORDS.contains(&name));
            takes(false, true)?;
            Stage::Op {
                name: name.to_owned(),
                with: body.clone(),
                keep: keep(into),
            }
        }
    })
}
