//! The vine document and the loader that checks it before it runs.
//!
//! ```yaml
//! vine: claim                 # name, lower snake_case
//! mode: once                  # once (default) | stream
//! input: { item: id, version: int }
//! outputs:                    # at least one; each with a shape
//!   claimed: { item: id }
//!   refused: { gate: text }
//! steps:                      # a block: a list of steps
//!   - read: { artifact: $in.item }   # one operator key, its settings
//!     as: item                       # optional: bind the result
//!   - guard: { when: $item.state != "open", out: refused, with: { gate: state } }
//!   - out: { name: claimed, with: { item: $in.item } }
//! ```
//!
//! A step is a map with exactly one operator key, plus `as:` for operators
//! that return a value. Control-flow core operators:
//!
//! - `guard: { when, out, with? }` ends the run with `out` when `when` holds.
//! - `out: name` or `out: { name, with? }` ends the run with that output.
//! - `switch: { on, cases: { key: [steps] }, else?: [steps] }` runs the
//!   block whose key equals the value of `on`, else `else`, else nothing.
//! - `try: { do: [steps], catch?: { code: [steps] }, finally?: [steps] }`.
//!   A `catch` block runs when a step in `do` fails with that error code,
//!   and sees `$error.code` and `$error.message`. `finally` always runs,
//!   even when the run is cancelled, and must not end the run.
//!
//! Names bound inside a block are not visible after it. Every path through
//! the top-level steps must end in an output.
//!
//! # Stream vines
//!
//! ```yaml
//! vine: backup
//! mode: stream
//! input: { seq: int, body: text }    # the shape of each source item
//! outputs:
//!   stored: { name: text, first: int, last: int, count: int }
//! steps:                             # a pipe, top to bottom
//!   - collect: { size: 500, or_every: 60s }
//!   - encrypt: { key: backup, data: $item.items }
//!   - store: { to: provider, first: $span.first, last: $span.last, sealed: $item.sealed, count: $item.count }
//! ```
//!
//! In a stream vine `steps` is a pipe: each step takes a stream and gives
//! a stream (see [`crate::stream`]). A step is an ordinary operator,
//! applied per item; a stream operator such as `collect`; or `each`. There
//! is no `$in`: each step sees the item reaching it as `$item`, and the
//! source positions it came from as `$span` (`{ first, last }`). `as:` is
//! refused in a pipe: a step's result is simply the next step's `$item`.
//!
//! - `each: [steps]` holds once-style steps, run for each item in order,
//!   one at a time. `guard`, `switch`, `try` and `out` stand only here.
//!   Every path inside must end in `out` or `guard`, naming a declared
//!   output. `each` must be the last step of the pipe.
//! - A pipe that does not end in `each` names its output by declaring
//!   exactly one: each result of its last step is that output. The last
//!   step must declare the shape of its results, and that shape must fit
//!   the output.
//!
//! Stream operator settings are fixed when the vine loads and cannot refer
//! to `$item`. `each` and stream operators are refused in once vines and
//! inside `each`.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::error::LoadError;
use crate::expr::{is_name, Cond, Operand, Ref, Template};
use crate::registry::{construct_specs, Registered, Registry, COMPOSITION_WORDS, CONSTRUCTS, EACH};
use crate::shape::{Shape, Ty};
use crate::stream::RegisteredStream;

/// How a vine takes its input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// One input, one output.
    Once,
    /// A stream of inputs through a pipe of stream steps.
    Stream,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Once => "once",
            Mode::Stream => "stream",
        }
    }
}

/// A checked vine, ready to run.
pub struct Vine {
    pub(crate) name: String,
    pub(crate) mode: Mode,
    pub(crate) input: Shape,
    pub(crate) outputs: BTreeMap<String, Shape>,
    /// Once mode: the steps.
    pub(crate) steps: Vec<Step>,
    /// Stream mode: the pipe.
    pub(crate) pipe: Vec<PipeStep>,
    /// Stream mode, pipe not ending in `each`: the one output.
    pub(crate) pipe_output: Option<String>,
    pub(crate) reach: BTreeSet<String>,
}

impl std::fmt::Debug for Vine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vine")
            .field("name", &self.name)
            .field("mode", &self.mode)
            .field("outputs", &self.outputs.keys().collect::<Vec<_>>())
            .field("reach", &self.reach)
            .finish_non_exhaustive()
    }
}

impl Vine {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    pub fn input(&self) -> &Shape {
        &self.input
    }

    pub fn outputs(&self) -> &BTreeMap<String, Shape> {
        &self.outputs
    }

    /// Everything this vine's operators touch, sorted.
    pub fn reach(&self) -> &BTreeSet<String> {
        &self.reach
    }
}

pub(crate) struct Step {
    pub path: String,
    pub kind: Kind,
}

pub(crate) enum Kind {
    Op {
        op: Registered,
        settings: Template,
        bind: Option<String>,
    },
    Guard {
        when: Cond,
        out: String,
        with: Template,
    },
    Out {
        name: String,
        with: Template,
    },
    Switch {
        on: Operand,
        cases: BTreeMap<String, Vec<Step>>,
        otherwise: Option<Vec<Step>>,
    },
    Try {
        body: Vec<Step>,
        catch: BTreeMap<String, Vec<Step>>,
        finally: Vec<Step>,
    },
}

pub(crate) struct PipeStep {
    pub path: String,
    pub kind: PipeKind,
}

pub(crate) enum PipeKind {
    /// An ordinary operator, applied to each item.
    Map { op: Registered, settings: Template },
    /// A stream operator; `settings` are literal.
    Flow {
        op: RegisteredStream,
        settings: Value,
    },
    /// Once-style steps per item. Always last.
    Each { steps: Vec<Step> },
}

const TOP_KEYS: &[&str] = &["vine", "mode", "input", "outputs", "steps"];

/// What a name in scope is known to hold: a shape, or unknown.
type Scope = BTreeMap<String, Option<Shape>>;

pub(crate) fn load(reg: &Registry, yaml: &str) -> Result<Vine, LoadError> {
    // Parse to YAML's own value first: it refuses duplicate keys (a second
    // `steps:` must not silently replace the first) and keeps tags visible.
    let raw: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|e| LoadError::Yaml(e.to_string()))?;
    refuse_tags(&raw)?;
    let doc: Value = serde_json::to_value(raw).map_err(|e| LoadError::Yaml(e.to_string()))?;
    let Some(map) = doc.as_object() else {
        return Err(malformed("vine", "a vine document is a map"));
    };
    for key in map.keys() {
        if !TOP_KEYS.contains(&key.as_str()) {
            return Err(if COMPOSITION_WORDS.contains(&key.as_str()) {
                LoadError::Composition {
                    path: "vine".into(),
                    key: key.clone(),
                }
            } else {
                LoadError::UnknownKey {
                    path: "vine".into(),
                    key: key.clone(),
                }
            });
        }
    }

    let name = match map.get("vine") {
        Some(Value::String(s)) if is_name(s) => s.clone(),
        Some(_) => return Err(malformed("vine", "`vine` is the name, lower snake_case")),
        None => return Err(malformed("vine", "missing `vine` (the vine's name)")),
    };
    let mode = match map.get("mode") {
        None => Mode::Once,
        Some(Value::String(s)) if s == "once" => Mode::Once,
        Some(Value::String(s)) if s == "stream" => Mode::Stream,
        Some(_) => return Err(malformed("mode", "mode is `once` or `stream`")),
    };
    let input = match map.get("input") {
        Some(v) => Shape::parse(v).map_err(|p| malformed("input", &p))?,
        None => return Err(malformed("vine", "missing `input` (the input shape)")),
    };
    let outputs = match map.get("outputs") {
        Some(Value::Object(m)) if !m.is_empty() => {
            let mut outs = BTreeMap::new();
            for (k, v) in m {
                let path = format!("outputs.{k}");
                if !is_name(k) {
                    return Err(malformed(&path, "output names are lower snake_case"));
                }
                outs.insert(
                    k.clone(),
                    Shape::parse(v).map_err(|p| malformed(&path, &p))?,
                );
            }
            outs
        }
        Some(_) => {
            return Err(malformed(
                "outputs",
                "outputs is a non-empty map of output name to shape",
            ))
        }
        None => return Err(malformed("vine", "missing `outputs`")),
    };
    let Some(steps) = map.get("steps") else {
        return Err(malformed("vine", "missing `steps`"));
    };

    let mut cx = Loader {
        reg,
        outputs: &outputs,
        reach: BTreeSet::new(),
    };
    let (steps, pipe, pipe_output) = match mode {
        Mode::Once => {
            let mut scope = Scope::new();
            scope.insert("in".into(), Some(input.clone()));
            let steps = cx.block(steps, "steps", &scope, false)?;
            if !terminal(&steps) {
                return Err(LoadError::MissingOutput {
                    path: "steps".into(),
                });
            }
            (steps, Vec::new(), None)
        }
        Mode::Stream => {
            let (pipe, out) = cx.pipe(steps, &input)?;
            (Vec::new(), pipe, out)
        }
    };
    let reach = cx.reach;
    Ok(Vine {
        name,
        mode,
        input,
        outputs,
        steps,
        pipe,
        pipe_output,
        reach,
    })
}

/// What a pipe step sees: the item reaching it and its source positions.
fn pipe_scope(item: &Option<Shape>) -> Scope {
    let mut scope = Scope::new();
    scope.insert("item".into(), item.clone());
    scope.insert(
        "span".into(),
        Some(
            Shape::new()
                .required("first", Ty::Int)
                .required("last", Ty::Int),
        ),
    );
    scope
}

struct Loader<'a> {
    reg: &'a Registry,
    outputs: &'a BTreeMap<String, Shape>,
    reach: BTreeSet<String>,
}

impl Loader<'_> {
    fn block(
        &mut self,
        v: &Value,
        path: &str,
        outer: &Scope,
        in_finally: bool,
    ) -> Result<Vec<Step>, LoadError> {
        let Some(list) = v.as_array() else {
            return Err(malformed(path, "a block is a list of steps"));
        };
        if list.is_empty() {
            return Err(malformed(path, "a block needs at least one step"));
        }
        let mut scope = outer.clone();
        let mut out: Vec<Step> = Vec::new();
        for (i, sv) in list.iter().enumerate() {
            let p = format!("{path}[{i}]");
            if out.last().is_some_and(terminal_step) {
                return Err(LoadError::Unreachable { path: p });
            }
            out.push(self.step(sv, &p, &mut scope, in_finally)?);
        }
        Ok(out)
    }

    fn step(
        &mut self,
        v: &Value,
        path: &str,
        scope: &mut Scope,
        in_finally: bool,
    ) -> Result<Step, LoadError> {
        let Some(map) = v.as_object() else {
            return Err(malformed(path, "a step is a map with one operator key"));
        };
        let (op_name, bind) = self.op_key(map, path)?;
        if op_name == EACH || self.reg.get_stream(op_name).is_some() {
            return Err(LoadError::PipeOnly {
                path: path.into(),
                name: op_name.into(),
            });
        }
        let settings = &map[op_name];
        let here = format!("{path}.{op_name}");

        if CONSTRUCTS.contains(&op_name) {
            if bind.is_some() {
                return Err(malformed(
                    path,
                    &format!("`as` binds an operator's result; `{op_name}` returns nothing"),
                ));
            }
            let kind = self.construct(op_name, settings, &here, scope, in_finally)?;
            return Ok(Step {
                path: path.into(),
                kind,
            });
        }

        let Some(op) = self.reg.get(op_name) else {
            return Err(LoadError::UnknownOperator {
                path: path.into(),
                name: op_name.into(),
            });
        };
        let template = Template::parse(settings).map_err(|p| malformed(&here, &p))?;
        check_refs(template.refs(), scope, &here)?;
        op.spec
            .settings
            .check_template(&template)
            .map_err(|problem| LoadError::BadSettings {
                path: here.clone(),
                operator: op_name.into(),
                problem,
            })?;
        let bind = match bind {
            None => None,
            Some(Value::String(b)) if is_name(b) => {
                if b == "in" || b == "error" || scope.contains_key(b) {
                    return Err(LoadError::DuplicateBinding {
                        path: format!("{path}.as"),
                        name: b.clone(),
                    });
                }
                scope.insert(b.clone(), op.spec.returns.clone());
                Some(b.clone())
            }
            Some(_) => {
                return Err(malformed(
                    &format!("{path}.as"),
                    "`as` is a name, lower snake_case",
                ))
            }
        };
        self.reach.extend(op.spec.touches.iter().cloned());
        Ok(Step {
            path: path.into(),
            kind: Kind::Op {
                op: op.clone(),
                settings: template,
                bind,
            },
        })
    }

    /// The one operator key of a step, and its `as:` if any.
    fn op_key<'v>(
        &self,
        map: &'v serde_json::Map<String, Value>,
        path: &str,
    ) -> Result<(&'v str, Option<&'v Value>), LoadError> {
        let mut bind = None;
        let mut keys = Vec::new();
        for (k, val) in map {
            if k == "as" {
                bind = Some(val);
                continue;
            }
            if COMPOSITION_WORDS.contains(&k.as_str()) {
                return Err(LoadError::Composition {
                    path: path.into(),
                    key: k.clone(),
                });
            }
            keys.push(k.as_str());
        }
        match keys.as_slice() {
            [] => Err(malformed(path, "a step needs an operator key")),
            [one] => Ok((*one, bind)),
            many => {
                let known: Vec<&&str> = many.iter().filter(|k| self.reg.contains(k)).collect();
                Err(match known.as_slice() {
                    [] => LoadError::UnknownOperator {
                        path: path.into(),
                        name: many[0].into(),
                    },
                    [_] => LoadError::UnknownKey {
                        path: path.into(),
                        key: many
                            .iter()
                            .find(|k| !self.reg.contains(k))
                            .unwrap()
                            .to_string(),
                    },
                    _ => malformed(
                        path,
                        &format!(
                            "a step names one operator, found {}",
                            many.iter()
                                .map(|k| format!("`{k}`"))
                                .collect::<Vec<_>>()
                                .join(" and ")
                        ),
                    ),
                })
            }
        }
    }

    /// A stream vine's pipe, and the one output if it does not end in
    /// `each`.
    fn pipe(
        &mut self,
        v: &Value,
        input: &Shape,
    ) -> Result<(Vec<PipeStep>, Option<String>), LoadError> {
        let Some(list) = v.as_array() else {
            return Err(malformed("steps", "a stream pipe is a list of steps"));
        };
        if list.is_empty() {
            return Err(malformed("steps", "a stream pipe needs at least one step"));
        }
        let mut item: Option<Shape> = Some(input.clone());
        let mut pipe = Vec::new();
        let mut last = String::new();
        for (i, sv) in list.iter().enumerate() {
            let path = format!("steps[{i}]");
            let Some(map) = sv.as_object() else {
                return Err(malformed(&path, "a step is a map with one operator key"));
            };
            let (name, bind) = self.op_key(map, &path)?;
            let here = format!("{path}.{name}");
            if CONSTRUCTS.contains(&name) {
                return Err(LoadError::OnceOnly {
                    path,
                    name: name.into(),
                });
            }
            if bind.is_some() {
                return Err(malformed(
                    &format!("{path}.as"),
                    "`as` binds a result inside `each`; in a pipe each step's result is the next step's `$item`",
                ));
            }
            let settings = &map[name];
            let scope = pipe_scope(&item);
            if name == EACH {
                if i + 1 != list.len() {
                    return Err(LoadError::EachNotLast { path: here });
                }
                let steps = self.block(settings, &here, &scope, false)?;
                if !terminal(&steps) {
                    return Err(LoadError::MissingOutput { path: here });
                }
                pipe.push(PipeStep {
                    path,
                    kind: PipeKind::Each { steps },
                });
                return Ok((pipe, None));
            }
            if let Some(op) = self.reg.get_stream(name) {
                let bad = |problem: String| LoadError::BadSettings {
                    path: here.clone(),
                    operator: name.into(),
                    problem,
                };
                let template = Template::parse(settings).map_err(|p| malformed(&here, &p))?;
                if let Some(r) = template.refs().first() {
                    return Err(bad(format!(
                        "`{r}`: a stream operator's settings are fixed when the vine loads and cannot refer to items"
                    )));
                }
                op.spec.settings.check(settings).map_err(bad)?;
                (op.factory)(settings).map(drop).map_err(bad)?;
                self.reach.extend(op.spec.touches.iter().cloned());
                item = op.spec.emits.clone();
                pipe.push(PipeStep {
                    path,
                    kind: PipeKind::Flow {
                        op: op.clone(),
                        settings: settings.clone(),
                    },
                });
            } else if let Some(op) = self.reg.get(name) {
                let template = Template::parse(settings).map_err(|p| malformed(&here, &p))?;
                check_refs(template.refs(), &scope, &here)?;
                op.spec
                    .settings
                    .check_template(&template)
                    .map_err(|problem| LoadError::BadSettings {
                        path: here.clone(),
                        operator: name.into(),
                        problem,
                    })?;
                self.reach.extend(op.spec.touches.iter().cloned());
                item = op.spec.returns.clone();
                pipe.push(PipeStep {
                    path,
                    kind: PipeKind::Map {
                        op: op.clone(),
                        settings: template,
                    },
                });
            } else {
                return Err(LoadError::UnknownOperator {
                    path,
                    name: name.into(),
                });
            }
            last = here;
        }

        // The pipe ends in an operator: its results are the one output.
        let op = last.rsplit('.').next().unwrap_or_default().to_owned();
        let unmatched = |problem: String| LoadError::UnmatchedOutput {
            path: last.clone(),
            problem,
        };
        let (out, shape) = match self.outputs.iter().collect::<Vec<_>>().as_slice() {
            [(name, shape)] => ((*name).clone(), *shape),
            many => {
                return Err(unmatched(format!(
                    "a pipe that does not end in `each` sends each result of its last step, `{op}`, to the vine's one output; declare exactly one output (found {}: {})",
                    many.len(),
                    many.iter().map(|(n, _)| format!("`{n}`")).collect::<Vec<_>>().join(", ")
                )))
            }
        };
        let Some(item) = item else {
            return Err(unmatched(format!(
                "`{op}` does not declare the shape of its results, so they cannot be checked against output `{out}`; declare it, or end the pipe in `each`"
            )));
        };
        item.fits_into(shape)
            .map_err(|p| unmatched(format!("results of `{op}` do not fit output `{out}`: {p}")))?;
        Ok((pipe, Some(out)))
    }

    fn construct(
        &mut self,
        name: &str,
        settings: &Value,
        here: &str,
        scope: &Scope,
        in_finally: bool,
    ) -> Result<Kind, LoadError> {
        // Short form: `out: claimed`.
        if name == "out" {
            if let Value::String(s) = settings {
                return self.ending(here, s, &Template::Obj(BTreeMap::new()), in_finally);
            }
        }
        let spec = construct_specs()
            .into_iter()
            .find(|s| s.name == name)
            .expect("construct has a spec");
        spec.settings
            .check(settings)
            .map_err(|problem| LoadError::BadSettings {
                path: here.into(),
                operator: name.into(),
                problem,
            })?;
        let field = |k: &str| settings.get(k);
        match name {
            "guard" | "out" => {
                let target = field(if name == "guard" { "out" } else { "name" })
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let with = match field("with") {
                    Some(w) => {
                        Template::parse(w).map_err(|p| malformed(&format!("{here}.with"), &p))?
                    }
                    None => Template::Obj(BTreeMap::new()),
                };
                check_refs(with.refs(), scope, &format!("{here}.with"))?;
                let Kind::Out { name: out, with } = self.ending(here, target, &with, in_finally)?
                else {
                    unreachable!()
                };
                if name == "out" {
                    return Ok(Kind::Out { name: out, with });
                }
                let when = Cond::parse(&settings["when"])
                    .map_err(|p| malformed(&format!("{here}.when"), &p))?;
                check_refs(when.refs(), scope, &format!("{here}.when"))?;
                Ok(Kind::Guard { when, out, with })
            }
            "switch" => {
                let on = Operand::parse_value(&settings["on"])
                    .map_err(|p| malformed(&format!("{here}.on"), &p))?;
                check_refs(
                    on.as_ref().into_iter().collect(),
                    scope,
                    &format!("{here}.on"),
                )?;
                let mut cases = BTreeMap::new();
                for (k, b) in settings["cases"].as_object().into_iter().flatten() {
                    let block = self.block(b, &format!("{here}.cases.{k}"), scope, in_finally)?;
                    cases.insert(k.clone(), block);
                }
                let otherwise = match field("else") {
                    Some(b) => Some(self.block(b, &format!("{here}.else"), scope, in_finally)?),
                    None => None,
                };
                Ok(Kind::Switch {
                    on,
                    cases,
                    otherwise,
                })
            }
            "try" => {
                let body = self.block(&settings["do"], &format!("{here}.do"), scope, in_finally)?;
                let mut catch = BTreeMap::new();
                let mut err_scope = scope.clone();
                err_scope.insert(
                    "error".into(),
                    Some(
                        Shape::new()
                            .required("code", Ty::Text)
                            .required("message", Ty::Text),
                    ),
                );
                for (code, b) in settings
                    .get("catch")
                    .and_then(Value::as_object)
                    .into_iter()
                    .flatten()
                {
                    let p = format!("{here}.catch.{code}");
                    if code == crate::registry::OpError::CANCELLED {
                        return Err(LoadError::CatchCancelled { path: p });
                    }
                    if !is_name(code) {
                        return Err(malformed(&p, "an error code is lower snake_case"));
                    }
                    catch.insert(code.clone(), self.block(b, &p, &err_scope, in_finally)?);
                }
                let finally = match field("finally") {
                    Some(b) => self.block(b, &format!("{here}.finally"), scope, true)?,
                    None => Vec::new(),
                };
                Ok(Kind::Try {
                    body,
                    catch,
                    finally,
                })
            }
            _ => unreachable!("every construct is handled"),
        }
    }

    /// Check an ending (`out` or `guard`) against the declared outputs.
    fn ending(
        &self,
        here: &str,
        name: &str,
        with: &Template,
        in_finally: bool,
    ) -> Result<Kind, LoadError> {
        if in_finally {
            return Err(LoadError::EndsInFinally { path: here.into() });
        }
        let Some(shape) = self.outputs.get(name) else {
            return Err(LoadError::UndeclaredOutput {
                path: here.into(),
                name: name.into(),
            });
        };
        shape
            .check_template(with)
            .map_err(|problem| LoadError::OutputMisfit {
                path: format!("{here}.with"),
                output: name.into(),
                problem,
            })?;
        Ok(Kind::Out {
            name: name.into(),
            with: with.clone(),
        })
    }
}

fn check_refs(refs: Vec<&Ref>, scope: &Scope, path: &str) -> Result<(), LoadError> {
    for r in refs {
        match scope.get(&r.root) {
            None => {
                return Err(LoadError::UnboundReference {
                    path: path.into(),
                    reference: r.to_string(),
                    name: r.root.clone(),
                })
            }
            Some(Some(shape)) => {
                if let Some(first) = r.path.first() {
                    if !shape.is_open() && shape.field(first).is_none() {
                        return Err(LoadError::UnknownField {
                            path: path.into(),
                            reference: r.to_string(),
                            name: r.root.clone(),
                            field: first.clone(),
                        });
                    }
                }
            }
            Some(None) => {}
        }
    }
    Ok(())
}

fn terminal(steps: &[Step]) -> bool {
    steps.last().is_some_and(terminal_step)
}

fn terminal_step(s: &Step) -> bool {
    match &s.kind {
        Kind::Out { .. } => true,
        Kind::Guard { .. } | Kind::Op { .. } => false,
        Kind::Switch {
            cases, otherwise, ..
        } => otherwise.as_deref().is_some_and(terminal) && cases.values().all(|b| terminal(b)),
        Kind::Try { body, catch, .. } => terminal(body) && catch.values().all(|b| terminal(b)),
    }
}

/// YAML tags (`!include file.yaml`) are how YAML tools splice documents
/// together. The vine language has none, so any tag is refused.
fn refuse_tags(v: &serde_yaml::Value) -> Result<(), LoadError> {
    match v {
        serde_yaml::Value::Tagged(t) => Err(LoadError::Composition {
            path: "vine".into(),
            key: t.tag.to_string(),
        }),
        serde_yaml::Value::Sequence(items) => items.iter().try_for_each(refuse_tags),
        serde_yaml::Value::Mapping(m) => m
            .iter()
            .try_for_each(|(k, v)| refuse_tags(k).and_then(|()| refuse_tags(v))),
        _ => Ok(()),
    }
}

fn malformed(path: &str, problem: &str) -> LoadError {
    LoadError::Malformed {
        path: path.into(),
        problem: problem.into(),
    }
}
