//! The checker: it reads a described vine and builds the operators to run.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::future::{join_all, ready};
use futures::stream;
use futures::{FutureExt, StreamExt};
use serde_json::Value;

use super::book::Cx;
use super::call_sites::{CallSites, Feed};
use super::flow::{
    batches, chain, env_of, fatal, forked, handleable, held, last_of, latest_or_first, one,
    per_item, per_item_held, per_item_stream, rescued, tidied, Flow, Flowing, Items, Transform,
};
use super::order::{merges_anywhere, View};
use super::output::Output;
use super::timer::{timed, Timeouts};
use super::unit::{add_field, Runner, Unit};
use super::{Flatten, Keep, Operator, Pipe, PipeDef};
use crate::error::{LoadError, RunError};
use crate::expr::{case_key, is_name, Cond, Ref, Template};
use crate::registry::{Registry, Run};
use crate::shape::{Shape, Ty};

/// What is known about the item at a point in the pipe: its fields, or
/// nothing.
pub(super) type Known = Option<Shape>;

/// For a run from a saved position: what a pipe's operators see of the
/// input stream. An operator that keeps something between values only
/// matters when
/// those values come from more than one input item.
#[derive(Debug, Clone)]
pub(super) enum Stream {
    /// One input item's own work: inside a mapping operator, `branch`,
    /// `catchError`, `finalize` and the arms of `use`, and before any of
    /// `catchError`, `finalize`, `retry` and `timeout`. An operator's state
    /// starts again for each item.
    PerItem,
    /// The whole stream, each value still one whole input item.
    Whole,
    /// The whole stream, after the operator named here made many values from
    /// one input item.
    Pieces(String),
}

pub(super) struct Checker<'a> {
    reg: &'a Registry,
    outputs: Arc<BTreeMap<String, Shape>>,
    reach: BTreeSet<String>,
    calls: BTreeSet<String>,
    uses: BTreeSet<String>,
    unresumable: Option<String>,
    unpositioned: Option<String>,
    sites: CallSites,
    timeouts: Timeouts,
}

/// A checked pipe: what runs it, and the stream operator calls whose items
/// reach its end.
pub(super) struct Built {
    pub(super) transform: Transform,
    pub(super) feeds: Vec<Feed>,
}

pub(crate) fn assemble(reg: &Registry, def: PipeDef) -> Result<Pipe, LoadError> {
    if !is_name(&def.name) {
        return Err(malformed("vine", "`vine` is the name, lower snake_case"));
    }
    if def.outputs.is_empty() {
        return Err(malformed(
            "outputs",
            "outputs is a non-empty map of output name to shape",
        ));
    }
    if let Some(bad) = def.outputs.keys().find(|k| !is_name(k)) {
        return Err(malformed(
            &format!("outputs.{bad}"),
            "output names are lower snake_case",
        ));
    }
    let mut cx = Checker {
        reg,
        outputs: Arc::new(def.outputs),
        reach: BTreeSet::new(),
        calls: BTreeSet::new(),
        uses: BTreeSet::new(),
        unresumable: None,
        unpositioned: None,
        sites: CallSites::default(),
        timeouts: Timeouts::default(),
    };
    let built = cx.pipe(
        &def.operators,
        "pipe",
        Some(def.input.clone()),
        false,
        View::whole(),
        Vec::new(),
    )?;
    if !emits(&def.operators) {
        return Err(LoadError::MissingOutput {
            path: "pipe".into(),
        });
    }
    let settled = cx.sites.settle();
    let merges = merges_anywhere(reg, &def.operators);
    Ok(Pipe {
        merges,
        name: def.name,
        input: def.input,
        outputs: cx.outputs,
        reach: cx.reach,
        calls: cx.calls,
        uses: cx.uses,
        unresumable: cx.unresumable.or(settled.unresumable),
        unpositioned: cx.unpositioned,
        saves_every_item: settled.saves_every_item,
        by_position: settled.by_position,
        timeouts: cx.timeouts,
        transform: built.transform,
    })
}

/// Whether every item that enters these operators is sure to be emitted.
pub(super) fn emits(ops: &[Operator]) -> bool {
    let Some((last, before)) = ops.split_last() else {
        return false;
    };
    match last {
        // These let what was emitted before them go by.
        Operator::Take(_)
        | Operator::Finalize(_)
        | Operator::Retry { .. }
        | Operator::Timeout(_) => emits(before),
        Operator::CatchError(handler) => emits(before) && emits(handler),
        Operator::Emit { .. } => true,
        Operator::Mapped { pipe, .. } => emits(pipe),
        Operator::Use { on, .. } => on.iter().all(|(_, p)| emits(p)),
        Operator::Branch {
            cases, otherwise, ..
        } => otherwise.as_deref().is_some_and(emits) && cases.iter().all(|(_, p)| emits(p)),
        // A fork's side pipes may emit, but the item still goes on down
        // the main pipe.
        _ => false,
    }
}

/// Whether an operator may stand after operators that already emit.
pub(super) fn follows_emit(op: &Operator) -> bool {
    matches!(
        op,
        Operator::Take(_)
            | Operator::CatchError(_)
            | Operator::Finalize(_)
            | Operator::Retry { .. }
            | Operator::Timeout(_)
    )
}

/// Whether an operator applies to the operators before it in its pipe, one
/// item at a time, as `catchError`, `finalize`, `retry` and `timeout` do.
pub(super) fn applies_to_before(op: &Operator) -> Option<&'static str> {
    match op {
        Operator::CatchError(_) => Some("catchError"),
        Operator::Finalize(_) => Some("finalize"),
        Operator::Retry { .. } => Some("retry"),
        Operator::Timeout(_) => Some("timeout"),
        _ => None,
    }
}

/// Whether an operator works across the whole stream, not one item at a
/// time, in a way `catchError`, `finalize`, `retry` and `timeout` cannot
/// hold. `last` and `bufferCount` are not here: before those four they work
/// on one item's own stream.
pub(super) fn stream_wide(op: &Operator) -> Option<&'static str> {
    match op {
        Operator::Take(_) => Some("take"),
        Operator::Mapped { flatten, .. } if *flatten != Flatten::Concat => Some(flatten.key()),
        _ => None,
    }
}

/// Whether these operators emit anywhere, at any depth.
pub(super) fn emits_anywhere(ops: &[Operator]) -> bool {
    ops.iter().any(|s| match s {
        Operator::Emit { .. } => true,
        Operator::Mapped { pipe, .. } | Operator::CatchError(pipe) | Operator::Finalize(pipe) => {
            emits_anywhere(pipe)
        }
        Operator::Fork(sides) => sides.iter().any(|p| emits_anywhere(p)),
        Operator::Use { on, .. } => on.iter().any(|(_, p)| emits_anywhere(p)),
        Operator::Branch {
            cases, otherwise, ..
        } => {
            cases.iter().any(|(_, p)| emits_anywhere(p))
                || otherwise.as_deref().is_some_and(emits_anywhere)
        }
        _ => false,
    })
}

impl Checker<'_> {
    /// Check a pipe and build it. `single` says the stream reaching it is
    /// one item, as inside a mapping operator, `branch`, `catchError`, `finalize` and the
    /// arms of `use`. `view` says what it sees of the stream: the whole
    /// input stream or one item's work, which matters for a run from a
    /// saved position, and the order values reach it in, which matters for
    /// a replay. `feeds` are the stream operator calls whose items reach its
    /// start: a fork's side pipe sees what reaches the fork.
    fn pipe(
        &mut self,
        ops: &[Operator],
        path: &str,
        mut known: Known,
        single: bool,
        mut view: View,
        mut feeds: Vec<Feed>,
    ) -> Result<Built, LoadError> {
        if ops.is_empty() {
            return Err(malformed(path, "a pipe needs at least one operator"));
        }
        let mark = self.sites.mark();
        let saved_mark = self.timeouts.mark();
        // The operators before the last `catchError`, `finalize`, `retry`
        // or `timeout` run once per item.
        let last_applied = ops.iter().rposition(|s| applies_to_before(s).is_some());
        let mut one_item = last_applied.is_some() || single;
        let mut many = false;
        // The operator that first made many values from one input item, at
        // the scope of the whole stream.
        let mut maker = match &view.stream {
            Stream::Pieces(by) => Some(by.clone()),
            _ => None,
        };
        let mut transforms: Vec<Transform> = Vec::new();
        for (i, op) in ops.iter().enumerate() {
            let here = format!("{path}[{i}]");
            if emits(&ops[..i]) && !follows_emit(op) {
                return Err(LoadError::Unreachable { path: here });
            }
            if let Some(word) = applies_to_before(op) {
                // It wraps everything before it in this pipe, per item.
                if i == 0 {
                    return Err(malformed(
                        &here,
                        &format!("`{word}` applies to the operators before it, and there are none"),
                    ));
                }
                if let Some(wide) = ops[..i].iter().find_map(stream_wide) {
                    return Err(malformed(
                        &here,
                        &format!(
                            "`{word}` applies to the operators before it one item at a time, but `{wide}` works across the whole stream; put `{wide}` after `{word}`"
                        ),
                    ));
                }
                if matches!(op, Operator::CatchError(_) | Operator::Finalize(_))
                    && merges_anywhere(self.reg, &ops[..i])
                {
                    self.not_resumable(format!(
                        "{here} is a `{word}` over a `mergeMap`, and drops the items still running when one fails; how far they got depends on timing, so a restart could drop them elsewhere; put the `{word}` inside the `mergeMap`"
                    ));
                }
                let body = chain(std::mem::take(&mut transforms));
                let (t, handler_feeds) = self.apply_to_before(op, &here, body, &view)?;
                transforms.push(t);
                if matches!(op, Operator::Retry { .. }) {
                    self.sites.retry(&here, mark);
                }
                if matches!(op, Operator::Timeout(_)) {
                    self.timeouts.timeout(saved_mark, &here);
                }
                feeds.extend(handler_feeds.into_iter().map(Feed::out_of_nested));
                known = None;
                if Some(i) == last_applied {
                    one_item = single && !many;
                    feeds = feeds
                        .into_iter()
                        .map(|f| f.past_last_applied(single))
                        .collect();
                }
                continue;
            }
            if one_item {
                let word = match op {
                    Operator::Last => Some("last"),
                    Operator::BufferCount { .. } => Some("bufferCount"),
                    _ => None,
                };
                if let Some(word) = word {
                    return Err(malformed(
                        &format!("{here}.{word}"),
                        &format!(
                            "`{word}` would only ever see one item here: the operators before `catchError`, `finalize`, `retry` or `timeout`, and inside `concatMap`, `mergeMap`, `switchMap`, `exhaustMap`, `branch`, `catchError`, `finalize` and `use`, run once for each item; put an operator that makes many items from one, such as `split` or a stream operator, before it"
                        ),
                    ));
                }
            }
            let here_stream = match view.stream {
                Stream::PerItem => Stream::PerItem,
                _ if last_applied.is_some_and(|c| i < c) => Stream::PerItem,
                _ => match &maker {
                    Some(by) => Stream::Pieces(by.clone()),
                    None => Stream::Whole,
                },
            };
            self.check_position(op, &here, &here_stream);
            let here_view = view.with_stream(here_stream);
            if let Some(why) = here_view.refusal(self.reg, op, &here) {
                self.not_resumable(why);
            }
            let (t, next) = self.operator(op, &here, known, one_item, &here_view, &mut feeds)?;
            transforms.push(t);
            known = next;
            view.after(self.reg, op, &here);
            if self.makes_many(op) {
                many = true;
                one_item = false;
            }
            if maker.is_none() && self.makes_many_anywhere(op) {
                maker = Some(here.clone());
            }
        }
        Ok(Built {
            transform: chain(transforms),
            feeds,
        })
    }

    /// Refuse, for a run from a saved position, an operator that keeps
    /// something across input items that a restart from the position
    /// cannot rebuild.
    fn check_position(&mut self, op: &Operator, here: &str, stream: &Stream) {
        match (op, stream) {
            (Operator::Take(_), Stream::Whole | Stream::Pieces(_)) => self.not_positioned(format!(
                "{here} is a `take` over the whole stream: it counts items, and a run from a position would count from nothing again; a vine that ends itself has an end, so run it with `run_saved`"
            )),
            (Operator::BufferCount { .. }, Stream::Pieces(by)) => self.not_positioned(format!(
                "{here} gathers batches over the whole stream after {by}, which makes many items from one; a batch could end partway through an input item, and the position could not pass it"
            )),
            _ => {}
        }
    }

    /// Whether an operator can turn one item into many.
    fn makes_many(&self, op: &Operator) -> bool {
        match op {
            Operator::Split { .. }
            | Operator::Mapped { .. }
            | Operator::Use { .. }
            | Operator::Branch { .. } => true,
            Operator::Call { name, .. } => self.reg.stream_gives(name).is_some(),
            _ => false,
        }
    }

    /// Whether an operator can turn one item into many, counting the
    /// operators a `catchError` runs instead.
    fn makes_many_anywhere(&self, op: &Operator) -> bool {
        match op {
            Operator::CatchError(ops) => ops.iter().any(|s| self.makes_many(s)),
            _ => self.makes_many(op),
        }
    }

    fn not_positioned(&mut self, why: String) {
        if self.unpositioned.is_none() {
            self.unpositioned = Some(why);
        }
    }

    fn not_resumable(&mut self, why: String) {
        if self.unresumable.is_none() {
            self.unresumable = Some(why);
        }
    }

    /// An operator that applies to the operators before it, `body`, one item at a
    /// time.
    /// Gives what runs it, and the stream operator calls whose items come out
    /// of a `catchError` handler.
    fn apply_to_before(
        &mut self,
        op: &Operator,
        path: &str,
        body: Transform,
        view: &View,
    ) -> Result<(Transform, Vec<Feed>), LoadError> {
        match op {
            Operator::CatchError(ops) => {
                // The item as it was when it failed is not known here.
                let at = format!("{path}.catchError");
                let built = self.pipe(ops, &at, None, true, view.per_value(&at), Vec::new())?;
                let handler = built.transform;
                let t = per_item_stream(move |item, tag, cx| {
                    rescued(
                        body(one(Ok(Flowing::Value(item, tag))), cx.clone()),
                        handler.clone(),
                        cx,
                    )
                });
                Ok((t, built.feeds))
            }
            Operator::Finalize(ops) => {
                let here = format!("{path}.finalize");
                if emits_anywhere(ops) {
                    return Err(malformed(
                        &here,
                        "`finalize` is for cleanup and may not emit",
                    ));
                }
                // What the cleanup gives is dropped, so no feed comes out.
                let cleanup = self
                    .pipe(ops, &here, None, true, view.per_value(&here), Vec::new())?
                    .transform;
                let t = per_item_stream(move |item, tag, cx| {
                    let work = body(
                        one(Ok(Flowing::Value(item.clone(), tag.clone()))),
                        cx.clone(),
                    );
                    tidied(work, cleanup.clone(), item, tag, cx)
                });
                Ok((t, Vec::new()))
            }
            Operator::Retry { count, delay } => {
                let (count, delay) = (*count, *delay);
                if count == 0 {
                    return Err(malformed(
                        &format!("{path}.retry"),
                        "`retry` tries again at least once",
                    ));
                }
                let t = per_item_held(move |item, tag, cx| {
                    let body = body.clone();
                    async move {
                        let mut left = count;
                        loop {
                            let attempt = one(Ok(Flowing::Value(item.clone(), tag.clone())));
                            let out: Vec<Flow> = body(attempt, cx.clone()).collect().await;
                            if left == 0 || !out.iter().any(handleable) {
                                return out;
                            }
                            left -= 1;
                            tokio::time::sleep(delay).await;
                        }
                    }
                    .boxed()
                });
                Ok((t, Vec::new()))
            }
            Operator::Timeout(limit) => {
                let limit = *limit;
                if limit.is_zero() {
                    return Err(malformed(
                        &format!("{path}.timeout"),
                        "`timeout` is longer than nothing",
                    ));
                }
                Ok((timed(body, path.to_owned(), limit), Vec::new()))
            }
            _ => unreachable!("only `catchError`, `finalize`, `retry` and `timeout` get here"),
        }
    }

    fn unit(
        &mut self,
        operator: &Operator,
        path: &str,
        known: &Known,
    ) -> Result<(Unit, Known), LoadError> {
        let (name, with, keep, end, called) = match operator {
            Operator::Named { name, with, keep } => (name, with, keep, &None, false),
            Operator::Call {
                name,
                with,
                keep,
                end,
            } => (name, with, keep, end, true),
            _ => unreachable!("only registered operators are units"),
        };
        let Some(registered) = self.reg.get(name) else {
            return Err(LoadError::UnknownOperator {
                path: path.into(),
                name: name.clone(),
            });
        };
        let spec = registered.spec.clone();
        let runner = match (&registered.run, registered.reaches_outside(), called) {
            (Run::Once(h), false, false) => Runner::Pure(h.clone()),
            (Run::Once(h), true, true) => Runner::Outside(h.clone()),
            (Run::Stream { handler, .. }, true, true) => Runner::Stream(handler.clone()),
            (_, true, false) => {
                return Err(malformed(
                    path,
                    &format!(
                        "`{name}` touches something outside, so a pipe runs it with `call`, or `tap` for its effect"
                    ),
                ))
            }
            (_, false, _) => {
                return Err(malformed(
                    path,
                    &format!(
                        "`call` and `tap` run an operator that touches something outside, and `{name}` touches nothing; run it by its name"
                    ),
                ))
            }
        };
        let with_path = if called {
            self.calls.insert(name.clone());
            self.timeouts.call(path);
            format!("{path}.with")
        } else {
            format!("{path}.{name}")
        };
        let is_stream = matches!(runner, Runner::Stream(_));
        if is_stream && *keep == Keep::Tap {
            return Err(malformed(
                path,
                &format!(
                    "`tap` runs an operator for its effect and lets the item on once; `{name}` gives a stream, so use `call`"
                ),
            ));
        }
        if let Some(end) = end {
            if !is_stream {
                return Err(malformed(
                    &format!("{path}.end"),
                    &format!("`end` names where a stream operator's end goes, and `{name}` is not a stream operator"),
                ));
            }
            if !is_name(end) {
                return Err(malformed(
                    &format!("{path}.end"),
                    "`end` is a name, lower snake_case",
                ));
            }
            if *keep == Keep::Into(end.clone()) {
                return Err(LoadError::DuplicateBinding {
                    path: format!("{path}.end"),
                    name: end.clone(),
                });
            }
        }
        let template = Template::parse(with).map_err(|p| malformed(&with_path, &p))?;
        check_refs(template.refs(), known, &with_path)?;
        spec.settings
            .check_template(&template)
            .map_err(|problem| LoadError::BadSettings {
                path: with_path,
                operator: name.clone(),
                problem,
            })?;
        let next = match keep {
            Keep::Replace if end.is_some() => None,
            Keep::Replace => spec.returns.clone(),
            Keep::Tap => known.clone(),
            Keep::Into(field) => {
                if !is_name(field) {
                    return Err(malformed(
                        &format!("{path}.into"),
                        "`into` is a name, lower snake_case",
                    ));
                }
                match end {
                    // The end item has the end field and no `into` field.
                    Some(end) => known
                        .clone()
                        .map(|s| s.optional(field, Ty::Any).optional(end, Ty::Any)),
                    None => known.clone().map(|s| s.required(field, Ty::Any)),
                }
            }
        };
        self.reach.extend(spec.touches.iter().cloned());
        let unit = Unit {
            path: path.into(),
            spec,
            runner,
            with: template,
            keep: keep.clone(),
            end: end.clone(),
            saving: Arc::default(),
        };
        Ok((unit, next))
    }

    /// Check one operator and build it. `feeds` are the stream operator
    /// calls whose items reach it; an operator that starts new ones adds
    /// them.
    fn operator(
        &mut self,
        op: &Operator,
        path: &str,
        known: Known,
        single: bool,
        view: &View,
        feeds: &mut Vec<Feed>,
    ) -> Result<(Transform, Known), LoadError> {
        match op {
            Operator::Named { .. } | Operator::Call { .. } => {
                let (unit, next) = self.unit(op, path, &known)?;
                if let Some(gives) = self.reg.stream_gives(&unit.spec.name) {
                    let saving = unit.saving.clone();
                    let name = unit.spec.name.clone();
                    feeds.extend(self.sites.call(path, &name, gives, single, saving));
                }
                if matches!(unit.runner, Runner::Stream(_)) {
                    Ok((
                        per_item_stream(move |item, tag, cx| unit.stream(item, tag, cx)),
                        next,
                    ))
                } else {
                    Ok((
                        per_item(move |item, tag, cx| unit.apply(item, tag, cx)),
                        next,
                    ))
                }
            }
            Operator::Split { from, into } => {
                let here = format!("{path}.split");
                let from = Template::parse(from).map_err(|p| malformed(&here, &p))?;
                check_refs(from.refs(), &known, &here)?;
                if let Some(name) = into {
                    if !is_name(name) {
                        return Err(malformed(
                            &format!("{path}.into"),
                            "`into` is a name, lower snake_case",
                        ));
                    }
                }
                let next = match into {
                    Some(name) => known.map(|s| s.required(name, Ty::Any)),
                    None => None,
                };
                let (into, at) = (into.clone(), path.to_owned());
                let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
                    let (from, into, at) = (from.clone(), into.clone(), at.clone());
                    items
                        .flat_map(move |flow| {
                            let many: Vec<Flow> = match flow {
                                Ok(Flowing::Value(item, tag)) => {
                                    match from.resolve(&env_of(&item)) {
                                        Value::Array(parts) => parts
                                            .into_iter()
                                            .map(|part| match &into {
                                                Some(name) => held(
                                                    add_field(&at, item.clone(), name, part),
                                                    &tag,
                                                ),
                                                None => Ok(Flowing::Value(part, tag.clone())),
                                            })
                                            .collect(),
                                        _ => vec![Err(fatal(RunError::ItemMisfit {
                                            path: at.clone(),
                                            problem: "`split` needs a list".into(),
                                        })
                                        .of(&tag))],
                                    }
                                }
                                other => vec![other],
                            };
                            stream::iter(many)
                        })
                        .boxed()
                });
                Ok((t, next))
            }
            Operator::Filter { when } => {
                let here = format!("{path}.filter");
                let cond = Cond::parse(when).map_err(|p| malformed(&here, &p))?;
                check_refs(cond.refs(), &known, &here)?;
                let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
                    let cond = cond.clone();
                    items
                        .filter(move |flow| {
                            ready(match flow {
                                Ok(Flowing::Value(item, _)) => cond.eval(&env_of(item)),
                                _ => true,
                            })
                        })
                        .boxed()
                });
                Ok((t, known))
            }
            Operator::Take(count) => {
                if *count == 0 {
                    return Err(malformed(
                        &format!("{path}.take"),
                        "`take` lets at least one item through",
                    ));
                }
                self.sites.take(path, feeds);
                let count = usize::try_from(*count).unwrap_or(usize::MAX);
                let t: Transform = Arc::new(move |items: Items, _cx: Cx| items.take(count).boxed());
                Ok((t, known))
            }
            Operator::Last => {
                // After items run side by side, the value it gives is lifted
                // out of the groups made in this pipe, so what is made from
                // it is keyed the same whichever value it is.
                let root = view.mixed.as_ref().map(|_| view.root.clone());
                let t: Transform = Arc::new(move |items: Items, cx: Cx| {
                    last_of(items, root.as_ref().map(|r| cx.sites_within(r)))
                });
                Ok((t, known))
            }
            Operator::BufferCount { buffer_size, into } => {
                let here = format!("{path}.bufferCount");
                if *buffer_size == 0 {
                    return Err(malformed(
                        &here,
                        "`bufferCount` needs a `bufferSize` of at least 1",
                    ));
                }
                if let Some(name) = into {
                    if !is_name(name) {
                        return Err(malformed(
                            &format!("{path}.into"),
                            "`into` is a name, lower snake_case",
                        ));
                    }
                }
                let next = match into {
                    Some(name) => known.map(|s| s.required(name, Ty::Object)),
                    None => Some(
                        Shape::new()
                            .required("items", Ty::List)
                            .required("count", Ty::Int),
                    ),
                };
                self.sites.buffer_count(path, feeds);
                let size = usize::try_from(*buffer_size).unwrap_or(usize::MAX);
                let (into, at) = (into.clone(), path.to_owned());
                let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
                    batches(items, size, into.clone(), at.clone())
                });
                Ok((t, next))
            }
            Operator::ForkJoin(parts) => {
                let here = format!("{path}.forkJoin");
                if parts.len() < 2 {
                    return Err(malformed(
                        &here,
                        "`forkJoin` runs two or more operators side by side",
                    ));
                }
                let mut units = Vec::new();
                let mut next = known.clone();
                for (i, part) in parts.iter().enumerate() {
                    let at = format!("{here}[{i}]");
                    let field = match part {
                        Operator::Named {
                            keep: Keep::Into(f),
                            ..
                        }
                        | Operator::Call {
                            keep: Keep::Into(f),
                            ..
                        } => f.clone(),
                        _ => {
                            return Err(malformed(
                                &at,
                                "each operator inside `forkJoin` is a registered operator with `into`, so the results do not overwrite each other",
                            ))
                        }
                    };
                    // Each one sees the item as it was before any of them ran.
                    let (unit, _) = self.unit(part, &at, &known)?;
                    if matches!(unit.runner, Runner::Stream(_)) {
                        return Err(malformed(
                            &at,
                            &format!(
                                "`{}` gives a stream, and `forkJoin` waits for one result from each operator",
                                unit.spec.name
                            ),
                        ));
                    }
                    if units.iter().any(|(f, _)| f == &field) {
                        return Err(LoadError::DuplicateBinding {
                            path: format!("{at}.into"),
                            name: field,
                        });
                    }
                    next = next.map(|s| s.required(&field, Ty::Any));
                    units.push((field, unit));
                }
                let t = per_item(move |item, tag, cx| {
                    let units = units.clone();
                    async move {
                        let results = join_all(
                            units
                                .iter()
                                .map(|(_, u)| u.result(item.clone(), tag.clone(), cx.clone())),
                        )
                        .await;
                        let mut item = item;
                        for ((field, unit), result) in units.iter().zip(results) {
                            item = add_field(&unit.path, item, field, result?)?;
                        }
                        Ok(item)
                    }
                    .boxed()
                });
                Ok((t, next))
            }
            Operator::Mapped {
                flatten,
                concurrent,
                pipe,
            } => {
                let key = flatten.key();
                let here = format!("{path}.{key}");
                if concurrent.is_some() && *flatten != Flatten::Merge {
                    return Err(malformed(
                        &here,
                        "`concurrent` limits side-by-side runs, so only `mergeMap` takes it",
                    ));
                }
                if *concurrent == Some(0) {
                    return Err(malformed(&here, "`concurrent` is at least 1"));
                }
                if matches!(flatten, Flatten::Switch | Flatten::Exhaust) {
                    self.not_resumable(format!(
                        "{here} drops or skips work by when items arrive, and a replay cannot know when they did"
                    ));
                }
                let at = format!("{here}.do");
                let built = self.pipe(pipe, &at, known, true, view.per_value(&at), Vec::new())?;
                feeds.extend(built.feeds.into_iter().map(Feed::out_of_nested));
                let inner = built.transform;
                let (flatten, concurrent) = (*flatten, *concurrent);
                let t: Transform = Arc::new(move |items: Items, cx: Cx| {
                    let inner = inner.clone();
                    match flatten {
                        Flatten::Switch => latest_or_first(items, inner, true, cx),
                        Flatten::Exhaust => latest_or_first(items, inner, false, cx),
                        Flatten::Concat => items
                            .map(move |flow| match flow {
                                Ok(Flowing::Value(v, tag)) => {
                                    inner(one(Ok(Flowing::Value(v, tag))), cx.clone())
                                }
                                other => one(other),
                            })
                            .flatten()
                            .boxed(),
                        // Each value starts a group of its own, numbered in
                        // the order the values enter, so its work replays
                        // in its own order whatever order the groups end.
                        Flatten::Merge => {
                            let here = here.clone();
                            items
                                .map(move |flow| match flow {
                                    Ok(Flowing::Value(v, tag)) => {
                                        let group = cx.group(&here, &tag);
                                        inner(one(Ok(Flowing::Value(v, group))), cx.clone())
                                    }
                                    other => one(other),
                                })
                                .flatten_unordered(concurrent)
                                .boxed()
                        }
                    }
                });
                Ok((t, None))
            }
            Operator::Fork(sides) => {
                let here = format!("{path}.fork");
                if sides.is_empty() {
                    return Err(malformed(&here, "`fork` needs at least one side pipe"));
                }
                let mut built = Vec::new();
                let mut side_feeds = Vec::new();
                for (j, side) in sides.iter().enumerate() {
                    let at = format!("{here}[{j}]");
                    let side = self.pipe(
                        side,
                        &at,
                        known.clone(),
                        single,
                        view.clone(),
                        feeds.clone(),
                    )?;
                    built.push(side.transform);
                    side_feeds.extend(side.feeds.into_iter().map(Feed::out_of_side));
                }
                feeds.extend(side_feeds);
                let t: Transform =
                    Arc::new(move |items: Items, cx: Cx| forked(items, built.clone(), cx));
                Ok((t, known))
            }
            Operator::CatchError(_)
            | Operator::Finalize(_)
            | Operator::Retry { .. }
            | Operator::Timeout(_) => {
                unreachable!("`catchError`, `finalize`, `retry` and `timeout` are handled where the pipe is put together")
            }
            Operator::Use {
                vine,
                with,
                keep,
                on,
            } => {
                let Some(other) = self.reg.get_vine(vine).cloned() else {
                    return Err(LoadError::UnknownVine {
                        path: path.into(),
                        name: vine.clone(),
                    });
                };
                let with = match with {
                    Some(w) => {
                        let at = format!("{path}.with");
                        let t = Template::parse(w).map_err(|p| malformed(&at, &p))?;
                        check_refs(t.refs(), &known, &at)?;
                        other.input().check_template(&t).map_err(|problem| {
                            LoadError::BadSettings {
                                path: at,
                                operator: vine.clone(),
                                problem,
                            }
                        })?;
                        Some(t)
                    }
                    None => {
                        if let Some(item) = &known {
                            item.fits_into(other.input()).map_err(|problem| {
                                LoadError::BadSettings {
                                    path: path.into(),
                                    operator: vine.clone(),
                                    problem,
                                }
                            })?;
                        }
                        None
                    }
                };
                if let Keep::Into(field) = keep {
                    if !is_name(field) {
                        return Err(malformed(
                            &format!("{path}.into"),
                            "`into` is a name, lower snake_case",
                        ));
                    }
                }
                let mut arms: BTreeMap<String, Transform> = BTreeMap::new();
                for (output, ops) in on {
                    let at = format!("{path}.on.{output}");
                    if !other.outputs().contains_key(output) {
                        return Err(LoadError::UndeclaredOutput {
                            path: at,
                            name: output.clone(),
                        });
                    }
                    if arms.contains_key(output) {
                        return Err(malformed(&at, "this output is listed twice"));
                    }
                    let arm = self.pipe(ops, &at, None, true, view.per_value(&at), Vec::new())?;
                    feeds.extend(arm.feeds.into_iter().map(Feed::out_of_nested));
                    arms.insert(output.clone(), arm.transform);
                }
                if let Some(missing) = other.outputs().keys().find(|o| !arms.contains_key(*o)) {
                    return Err(malformed(
                        &format!("{path}.on"),
                        &format!(
                            "vine `{vine}` can send to `{missing}`, and nothing here says what to do with it"
                        ),
                    ));
                }
                if let Some(why) = &other.unresumable {
                    self.not_resumable(format!("{path} uses vine `{vine}`, and {why}"));
                }
                feeds.extend(self.sites.used(
                    path,
                    vine,
                    other.by_position,
                    &other.saves_every_item,
                ));
                self.reach.extend(other.reach().iter().cloned());
                self.calls.extend(other.calls().iter().cloned());
                self.uses.insert(vine.clone());
                self.uses.extend(other.uses().iter().cloned());
                self.timeouts.used(path, vine, &other.timeouts);
                let (keep, at) = (keep.clone(), path.to_owned());
                let t: Transform = Arc::new(move |items: Items, cx: Cx| {
                    let (other, with, keep, arms, at) = (
                        other.clone(),
                        with.clone(),
                        keep.clone(),
                        arms.clone(),
                        at.clone(),
                    );
                    items
                        .map(move |flow| {
                            let (item, tag) = match flow {
                                Ok(Flowing::Value(item, tag)) => (item, tag),
                                other => return one(other),
                            };
                            let input = match &with {
                                Some(t) => t.resolve(&env_of(&item)),
                                None => item.clone(),
                            };
                            let (keep, arms, at) = (keep.clone(), arms.clone(), at.clone());
                            let inner_cx = cx.within(&at, other.name());
                            let cx = cx.clone();
                            other
                                .flow(one(Ok(Flowing::Value(input, tag))), inner_cx)
                                .map(move |flow| match flow {
                                    Ok(Flowing::Out(out, tag)) => {
                                        let next = match &keep {
                                            Keep::Into(field) => {
                                                add_field(&at, item.clone(), field, out.payload)
                                            }
                                            _ => Ok(out.payload),
                                        };
                                        match held(next, &tag) {
                                            Ok(value) => {
                                                arms[&out.name](one(Ok(value)), cx.clone())
                                            }
                                            Err(fault) => one(Err(fault)),
                                        }
                                    }
                                    other => one(other),
                                })
                                .flatten()
                                .boxed()
                        })
                        .flatten()
                        .boxed()
                });
                Ok((t, None))
            }
            Operator::Branch {
                on,
                cases,
                otherwise,
            } => {
                let here = format!("{path}.branch");
                let on = Template::parse(on).map_err(|p| malformed(&format!("{here}.on"), &p))?;
                check_refs(on.refs(), &known, &format!("{here}.on"))?;
                if cases.is_empty() {
                    return Err(malformed(&here, "`branch` needs at least one case"));
                }
                let mut arms: BTreeMap<String, Transform> = BTreeMap::new();
                for (key, pipe) in cases {
                    let at = format!("{here}.cases.{key}");
                    if arms.contains_key(key) {
                        return Err(malformed(&at, "this case is listed twice"));
                    }
                    let arm = self.pipe(
                        pipe,
                        &at,
                        known.clone(),
                        true,
                        view.per_value(&at),
                        Vec::new(),
                    )?;
                    feeds.extend(arm.feeds.into_iter().map(Feed::out_of_nested));
                    arms.insert(key.clone(), arm.transform);
                }
                let fallback = match otherwise {
                    Some(pipe) => {
                        let at = format!("{here}.else");
                        let arm =
                            self.pipe(pipe, &at, known, true, view.per_value(&at), Vec::new())?;
                        feeds.extend(arm.feeds.into_iter().map(Feed::out_of_nested));
                        Some(arm.transform)
                    }
                    None => None,
                };
                let t: Transform = Arc::new(move |items: Items, cx: Cx| {
                    let (on, arms, fallback) = (on.clone(), arms.clone(), fallback.clone());
                    items
                        .map(move |flow| match flow {
                            Ok(Flowing::Value(item, tag)) => {
                                let key = case_key(&on.resolve(&env_of(&item)));
                                let arm = key.and_then(|k| arms.get(&k)).or(fallback.as_ref());
                                let flow = one(Ok(Flowing::Value(item, tag)));
                                match arm {
                                    Some(arm) => arm(flow, cx.clone()),
                                    None => flow,
                                }
                            }
                            other => one(other),
                        })
                        .flatten()
                        .boxed()
                });
                Ok((t, None))
            }
            Operator::Emit { to, with } => {
                let here = format!("{path}.emit");
                let Some(shape) = self.outputs.get(to).cloned() else {
                    return Err(LoadError::UndeclaredOutput {
                        path: here,
                        name: to.clone(),
                    });
                };
                let with = match with {
                    Some(w) => {
                        let at = format!("{here}.with");
                        let t = Template::parse(w).map_err(|p| malformed(&at, &p))?;
                        check_refs(t.refs(), &known, &at)?;
                        shape
                            .check_template(&t)
                            .map_err(|problem| LoadError::OutputMisfit {
                                path: at,
                                output: to.clone(),
                                problem,
                            })?;
                        Some(t)
                    }
                    None => {
                        if let Some(item) = &known {
                            item.fits_into(&shape)
                                .map_err(|problem| LoadError::OutputMisfit {
                                    path: here.clone(),
                                    output: to.clone(),
                                    problem,
                                })?;
                        }
                        None
                    }
                };
                let (to, at) = (to.clone(), here);
                let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
                    let (to, at, with, shape) =
                        (to.clone(), at.clone(), with.clone(), shape.clone());
                    items
                        .map(move |flow| match flow {
                            Ok(Flowing::Value(item, tag)) => {
                                let payload = match &with {
                                    Some(t) => t.resolve(&env_of(&item)),
                                    None => item,
                                };
                                match shape.check(&payload) {
                                    Ok(()) => Ok(Flowing::Out(
                                        Output {
                                            name: to.clone(),
                                            payload,
                                        },
                                        tag,
                                    )),
                                    Err(problem) => Err(fatal(RunError::OutputMisfit {
                                        path: at.clone(),
                                        output: to.clone(),
                                        problem,
                                    })
                                    .of(&tag)),
                                }
                            }
                            other => other,
                        })
                        .boxed()
                });
                Ok((t, None))
            }
        }
    }
}

/// `$item` is the only name in a pipe. A field is checked when the item's
/// fields are known at that point.
pub(super) fn check_refs(refs: Vec<&Ref>, known: &Known, path: &str) -> Result<(), LoadError> {
    for r in refs {
        if r.root != "item" {
            return Err(LoadError::UnboundReference {
                path: path.into(),
                reference: r.to_string(),
                name: r.root.clone(),
            });
        }
        if let (Some(shape), Some(first)) = (known, r.path.first()) {
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
    Ok(())
}

pub(super) fn malformed(path: &str, problem: &str) -> LoadError {
    LoadError::Malformed {
        path: path.into(),
        problem: problem.into(),
    }
}
