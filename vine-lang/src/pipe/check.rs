//! The checker: it reads a described vine and builds the stages to run.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::future::{join_all, ready};
use futures::stream;
use futures::{FutureExt, StreamExt};
use serde_json::Value;

use super::book::Cx;
use super::call_sites::{CallSites, Feed};
use super::flow::{
    batches, chain, covering, env_of, fatal, forked, handleable, held, last_of, latest_or_first,
    one, per_item, per_item_stream, rescued, tidied, Fault, Flow, Flowing, Items, OpFault,
    Transform,
};
use super::unit::{add_field, Runner, Unit};
use super::{Keep, Overlap, Pipe, PipeDef, Stage};
use crate::error::{LoadError, RunError};
use crate::expr::{case_key, is_name, Cond, Ref, Template};
use crate::registry::{OpError, Registry};
use crate::run::Output;
use crate::shape::{Shape, Ty};

/// What is known about the item at a point in the pipe: its fields, or
/// nothing.
pub(super) type Known = Option<Shape>;

/// For a run from a saved position: what a pipe's stages see of the input
/// stream. A stage that keeps something between values only matters when
/// those values come from more than one input item.
#[derive(Debug, Clone)]
pub(super) enum Stream {
    /// One input item's own work: inside `each`, `branch`, `catch`,
    /// `finalize` and the arms of `use`, and before a covering stage. A
    /// stage's state starts again for each item.
    PerItem,
    /// The whole stream, each value still one whole input item.
    Whole,
    /// The whole stream, after the stage named here made many values from
    /// one input item.
    Pieces(String),
}

pub(super) struct Checker<'a> {
    reg: &'a Registry,
    outputs: Arc<BTreeMap<String, Shape>>,
    reach: BTreeSet<String>,
    acts: BTreeSet<String>,
    uses: BTreeSet<String>,
    unresumable: Option<String>,
    unpositioned: Option<String>,
    sites: CallSites,
}

/// A checked pipe: what runs it, and the stream act calls whose items
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
        acts: BTreeSet::new(),
        uses: BTreeSet::new(),
        unresumable: None,
        unpositioned: None,
        sites: CallSites::default(),
    };
    let built = cx.pipe(
        &def.stages,
        "pipe",
        Some(def.input.clone()),
        false,
        Stream::Whole,
        Vec::new(),
    )?;
    if !emits(&def.stages) {
        return Err(LoadError::MissingOutput {
            path: "pipe".into(),
        });
    }
    let settled = cx.sites.settle();
    Ok(Pipe {
        name: def.name,
        input: def.input,
        outputs: cx.outputs,
        reach: cx.reach,
        acts: cx.acts,
        uses: cx.uses,
        unresumable: cx.unresumable.or(settled.unresumable),
        unpositioned: cx.unpositioned,
        saves_every_item: settled.saves_every_item,
        by_position: settled.by_position,
        transform: built.transform,
    })
}

/// Whether every item that enters these stages is sure to be emitted.
pub(super) fn emits(stages: &[Stage]) -> bool {
    let Some((last, before)) = stages.split_last() else {
        return false;
    };
    match last {
        // These let what was emitted before them go by.
        Stage::Take(_) | Stage::Finalize(_) | Stage::Retry { .. } | Stage::Timeout(_) => {
            emits(before)
        }
        Stage::Catch(handler) => emits(before) && emits(handler),
        Stage::Emit { .. } => true,
        Stage::Each { pipe, .. } => emits(pipe),
        Stage::Use { on, .. } => on.iter().all(|(_, p)| emits(p)),
        Stage::Branch {
            cases, otherwise, ..
        } => otherwise.as_deref().is_some_and(emits) && cases.iter().all(|(_, p)| emits(p)),
        // A fork's side pipes may emit, but the item still goes on down
        // the main pipe.
        _ => false,
    }
}

/// Whether a stage may stand after stages that already emit.
pub(super) fn follows_emit(stage: &Stage) -> bool {
    matches!(
        stage,
        Stage::Take(_)
            | Stage::Catch(_)
            | Stage::Finalize(_)
            | Stage::Retry { .. }
            | Stage::Timeout(_)
    )
}

/// Whether a stage covers the stages before it, one item at a time.
pub(super) fn covers(stage: &Stage) -> Option<&'static str> {
    match stage {
        Stage::Catch(_) => Some("catch"),
        Stage::Finalize(_) => Some("finalize"),
        Stage::Retry { .. } => Some("retry"),
        Stage::Timeout(_) => Some("timeout"),
        _ => None,
    }
}

/// Whether a stage works across the whole stream, not one item at a time,
/// in a way a covering stage cannot hold. `last` and `collect` are not
/// here: before a covering stage they work on one item's own stream.
pub(super) fn stream_wide(stage: &Stage) -> Option<&'static str> {
    match stage {
        Stage::Take(_) => Some("take"),
        Stage::Each { overlap, .. } if *overlap != Overlap::Concat => Some("each"),
        _ => None,
    }
}

/// Whether these stages emit anywhere, at any depth.
pub(super) fn emits_anywhere(stages: &[Stage]) -> bool {
    stages.iter().any(|s| match s {
        Stage::Emit { .. } => true,
        Stage::Each { pipe, .. } | Stage::Catch(pipe) | Stage::Finalize(pipe) => {
            emits_anywhere(pipe)
        }
        Stage::Fork(sides) => sides.iter().any(|p| emits_anywhere(p)),
        Stage::Use { on, .. } => on.iter().any(|(_, p)| emits_anywhere(p)),
        Stage::Branch {
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
    /// one item, as inside `each`, `branch`, `catch`, `finalize` and the
    /// arms of `use`. `stream` says whether it sees the whole input stream,
    /// which matters for a run from a saved position. `feeds` are the
    /// stream act calls whose items reach its start: a fork's side pipe
    /// sees what reaches the fork.
    fn pipe(
        &mut self,
        stages: &[Stage],
        path: &str,
        mut known: Known,
        single: bool,
        stream: Stream,
        mut feeds: Vec<Feed>,
    ) -> Result<Built, LoadError> {
        if stages.is_empty() {
            return Err(malformed(path, "a pipe needs at least one stage"));
        }
        let mark = self.sites.mark();
        // The stages before the last covering stage run once per item.
        let last_cover = stages.iter().rposition(|s| covers(s).is_some());
        let mut one_item = last_cover.is_some() || single;
        let mut many = false;
        // The stage that first made many values from one input item, at
        // the scope of the whole stream.
        let mut maker = match &stream {
            Stream::Pieces(by) => Some(by.clone()),
            _ => None,
        };
        let mut transforms: Vec<Transform> = Vec::new();
        for (i, stage) in stages.iter().enumerate() {
            let here = format!("{path}[{i}]");
            if emits(&stages[..i]) && !follows_emit(stage) {
                return Err(LoadError::Unreachable { path: here });
            }
            if let Some(word) = covers(stage) {
                // It wraps everything before it in this pipe, per item.
                if i == 0 {
                    return Err(malformed(
                        &here,
                        &format!("`{word}` covers the stages before it, and there are none"),
                    ));
                }
                if let Some(wide) = stages[..i].iter().find_map(stream_wide) {
                    return Err(malformed(
                        &here,
                        &format!(
                            "`{word}` covers the stages before it one item at a time, but `{wide}` works across the whole stream; put `{wide}` after `{word}`"
                        ),
                    ));
                }
                let body = chain(std::mem::take(&mut transforms));
                let (t, handler_feeds) = self.cover(stage, &here, body)?;
                transforms.push(t);
                if matches!(stage, Stage::Retry { .. }) {
                    self.sites.retry(&here, mark);
                }
                feeds.extend(handler_feeds.into_iter().map(Feed::out_of_nested));
                known = None;
                if Some(i) == last_cover {
                    one_item = single && !many;
                    feeds = feeds
                        .into_iter()
                        .map(|f| f.past_last_cover(single))
                        .collect();
                }
                continue;
            }
            if one_item {
                let word = match stage {
                    Stage::Last => Some("last"),
                    Stage::Collect { .. } => Some("collect"),
                    _ => None,
                };
                if let Some(word) = word {
                    return Err(malformed(
                        &format!("{here}.{word}"),
                        &format!(
                            "`{word}` would only ever see one item here: the stages before a covering stage, and inside `each`, `branch`, `catch`, `finalize` and `use`, run once for each item; put a stage that makes many items from one, such as `split` or a stream act, before it"
                        ),
                    ));
                }
            }
            let here_stream = match stream {
                Stream::PerItem => Stream::PerItem,
                _ if last_cover.is_some_and(|c| i < c) => Stream::PerItem,
                _ => match &maker {
                    Some(by) => Stream::Pieces(by.clone()),
                    None => Stream::Whole,
                },
            };
            self.check_position(stage, &here, &here_stream);
            let (t, next) = self.stage(stage, &here, known, one_item, here_stream, &mut feeds)?;
            transforms.push(t);
            known = next;
            if self.makes_many(stage) {
                many = true;
                one_item = false;
            }
            if maker.is_none() && self.makes_many_anywhere(stage) {
                maker = Some(here.clone());
            }
        }
        Ok(Built {
            transform: chain(transforms),
            feeds,
        })
    }

    /// Refuse, for a run from a saved position, a stage that keeps
    /// something across input items that a restart from the position
    /// cannot rebuild.
    fn check_position(&mut self, stage: &Stage, here: &str, stream: &Stream) {
        match (stage, stream) {
            (Stage::Take(_), Stream::Whole | Stream::Pieces(_)) => self.not_positioned(format!(
                "{here} is a `take` over the whole stream: it counts items, and a run from a position would count from nothing again; a vine that ends itself has an end, so run it with `run_saved`"
            )),
            (Stage::Collect { .. }, Stream::Pieces(by)) => self.not_positioned(format!(
                "{here} gathers batches over the whole stream after {by}, which makes many items from one; a batch could end partway through an input item, and the position could not pass it"
            )),
            _ => {}
        }
    }

    /// Whether a stage can turn one item into many.
    fn makes_many(&self, stage: &Stage) -> bool {
        match stage {
            Stage::Split { .. } | Stage::Each { .. } | Stage::Use { .. } | Stage::Branch { .. } => {
                true
            }
            Stage::Call { act, .. } => self.reg.get_stream_act(act).is_some(),
            _ => false,
        }
    }

    /// Whether a stage can turn one item into many, counting the stages a
    /// covering stage runs instead.
    fn makes_many_anywhere(&self, stage: &Stage) -> bool {
        match stage {
            Stage::Catch(stages) => stages.iter().any(|s| self.makes_many(s)),
            _ => self.makes_many(stage),
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

    /// A stage that covers the stages before it, `body`, one item at a
    /// time.
    /// Gives what runs it, and the stream act calls whose items come out
    /// of a `catch` handler.
    fn cover(
        &mut self,
        stage: &Stage,
        path: &str,
        body: Transform,
    ) -> Result<(Transform, Vec<Feed>), LoadError> {
        match stage {
            Stage::Catch(stages) => {
                // The item as it was when it failed is not known here.
                let built = self.pipe(
                    stages,
                    &format!("{path}.catch"),
                    None,
                    true,
                    Stream::PerItem,
                    Vec::new(),
                )?;
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
            Stage::Finalize(stages) => {
                let here = format!("{path}.finalize");
                if emits_anywhere(stages) {
                    return Err(malformed(
                        &here,
                        "`finalize` is for cleanup and may not emit",
                    ));
                }
                // What the cleanup gives is dropped, so no feed comes out.
                let cleanup = self
                    .pipe(stages, &here, None, true, Stream::PerItem, Vec::new())?
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
            Stage::Retry { times, wait } => {
                let (times, wait) = (*times, *wait);
                if times == 0 {
                    return Err(malformed(
                        &format!("{path}.retry"),
                        "`retry` tries again at least once",
                    ));
                }
                let t = covering(move |item, tag, cx| {
                    let body = body.clone();
                    async move {
                        let mut left = times;
                        loop {
                            let attempt = one(Ok(Flowing::Value(item.clone(), tag.clone())));
                            let out: Vec<Flow> = body(attempt, cx.clone()).collect().await;
                            if left == 0 || !out.iter().any(handleable) {
                                return out;
                            }
                            left -= 1;
                            tokio::time::sleep(wait).await;
                        }
                    }
                    .boxed()
                });
                Ok((t, Vec::new()))
            }
            Stage::Timeout(limit) => {
                let limit = *limit;
                if limit.is_zero() {
                    return Err(malformed(
                        &format!("{path}.timeout"),
                        "`timeout` is longer than nothing",
                    ));
                }
                self.not_resumable(format!(
                    "{path} is a `timeout`, and a replay cannot know where the time ran out"
                ));
                let at = path.to_owned();
                let t = covering(move |item, tag, cx| {
                    let (body, at) = (body.clone(), at.clone());
                    async move {
                        let run = body(one(Ok(Flowing::Value(item.clone(), tag.clone()))), cx)
                            .collect::<Vec<Flow>>();
                        match tokio::time::timeout(limit, run).await {
                            Ok(out) => out,
                            Err(_) => vec![Err(Fault::Op(Box::new(OpFault {
                                path: at,
                                from: "timeout".into(),
                                error: OpError::new(
                                    "timeout",
                                    format!("took longer than {}s", limit.as_secs_f64()),
                                ),
                                item,
                                tag,
                            })))],
                        }
                    }
                    .boxed()
                });
                Ok((t, Vec::new()))
            }
            _ => unreachable!("only covering stages get here"),
        }
    }

    fn unit(
        &mut self,
        stage: &Stage,
        path: &str,
        known: &Known,
    ) -> Result<(Unit, Known), LoadError> {
        let (spec, runner, name, with, keep, end, with_path) = match stage {
            Stage::Op { name, with, keep } => {
                let Some(op) = self.reg.get(name) else {
                    return Err(LoadError::UnknownOperator {
                        path: path.into(),
                        name: name.clone(),
                    });
                };
                (
                    op.spec.clone(),
                    Runner::Op(op.handler.clone()),
                    name,
                    with,
                    keep,
                    &None,
                    format!("{path}.{name}"),
                )
            }
            Stage::Call {
                act,
                with,
                keep,
                end,
            } => {
                let (spec, runner) = if let Some(op) = self.reg.get_act(act) {
                    (op.spec.clone(), Runner::Act(op.handler.clone()))
                } else if let Some(op) = self.reg.get_stream_act(act) {
                    (op.spec.clone(), Runner::Stream(op.handler.clone()))
                } else {
                    return Err(LoadError::UnknownAct {
                        path: path.into(),
                        name: act.clone(),
                    });
                };
                self.acts.insert(act.clone());
                (spec, runner, act, with, keep, end, format!("{path}.with"))
            }
            _ => unreachable!("only operators and acts are units"),
        };
        let is_stream = matches!(runner, Runner::Stream(_));
        if is_stream && *keep == Keep::Tap {
            return Err(malformed(
                path,
                &format!(
                    "`tap` runs an act for its effect and lets the item on once; `{name}` gives a stream, so use `call`"
                ),
            ));
        }
        if let Some(end) = end {
            if !is_stream {
                return Err(malformed(
                    &format!("{path}.end"),
                    &format!("`end` names where a stream act's end goes, and `{name}` is not a stream act"),
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

    /// Check one stage and build it. `feeds` are the stream act calls
    /// whose items reach it; a stage that starts new ones adds them.
    fn stage(
        &mut self,
        stage: &Stage,
        path: &str,
        known: Known,
        single: bool,
        stream: Stream,
        feeds: &mut Vec<Feed>,
    ) -> Result<(Transform, Known), LoadError> {
        match stage {
            Stage::Op { .. } | Stage::Call { .. } => {
                let (unit, next) = self.unit(stage, path, &known)?;
                if let Some(act) = self.reg.get_stream_act(&unit.spec.name) {
                    let gives = act.gives;
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
            Stage::Split { from, into } => {
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
            Stage::Filter { when } => {
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
            Stage::Take(count) => {
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
            Stage::Last => {
                let t: Transform = Arc::new(|items: Items, _cx: Cx| last_of(items));
                Ok((t, known))
            }
            Stage::Collect { size, into } => {
                let here = format!("{path}.collect");
                if *size == 0 {
                    return Err(malformed(&here, "`collect` needs a `size` of at least 1"));
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
                self.sites.collect(path, feeds);
                let size = usize::try_from(*size).unwrap_or(usize::MAX);
                let (into, at) = (into.clone(), path.to_owned());
                let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
                    batches(items, size, into.clone(), at.clone())
                });
                Ok((t, next))
            }
            Stage::Together(parts) => {
                let here = format!("{path}.together");
                if parts.len() < 2 {
                    return Err(malformed(
                        &here,
                        "`together` runs two or more stages side by side",
                    ));
                }
                let mut units = Vec::new();
                let mut next = known.clone();
                for (i, part) in parts.iter().enumerate() {
                    let at = format!("{here}[{i}]");
                    let field = match part {
                        Stage::Op {
                            keep: Keep::Into(f),
                            ..
                        }
                        | Stage::Call {
                            keep: Keep::Into(f),
                            ..
                        } => f.clone(),
                        _ => {
                            return Err(malformed(
                                &at,
                                "each stage inside `together` is an operator or an act with `into`, so the results do not overwrite each other",
                            ))
                        }
                    };
                    // Each one sees the item as it was before any of them ran.
                    let (unit, _) = self.unit(part, &at, &known)?;
                    if matches!(unit.runner, Runner::Stream(_)) {
                        return Err(malformed(
                            &at,
                            &format!(
                                "`{}` gives a stream, and `together` waits for one result from each stage",
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
            Stage::Each {
                overlap,
                at_once,
                pipe,
            } => {
                let here = format!("{path}.each");
                if at_once.is_some() && *overlap != Overlap::Merge {
                    return Err(malformed(
                        &here,
                        "`at_once` limits side-by-side runs, so it goes with `overlap: merge`",
                    ));
                }
                if *at_once == Some(0) {
                    return Err(malformed(&here, "`at_once` is at least 1"));
                }
                if *overlap != Overlap::Concat {
                    self.not_resumable(format!(
                        "{here} runs items with overlap `{}`, and a replay needs one order",
                        match overlap {
                            Overlap::Merge => "merge",
                            Overlap::Switch => "switch",
                            Overlap::Exhaust => "exhaust",
                            Overlap::Concat => "concat",
                        }
                    ));
                }
                let built = self.pipe(
                    pipe,
                    &format!("{here}.do"),
                    known,
                    true,
                    Stream::PerItem,
                    Vec::new(),
                )?;
                feeds.extend(built.feeds.into_iter().map(Feed::out_of_nested));
                let inner = built.transform;
                let (overlap, at_once) = (*overlap, *at_once);
                let t: Transform = Arc::new(move |items: Items, cx: Cx| {
                    let inner = inner.clone();
                    match overlap {
                        Overlap::Switch => latest_or_first(items, inner, true, cx),
                        Overlap::Exhaust => latest_or_first(items, inner, false, cx),
                        Overlap::Concat | Overlap::Merge => {
                            let runs = items.map(move |flow| match flow {
                                Ok(Flowing::Value(v, tag)) => {
                                    inner(one(Ok(Flowing::Value(v, tag))), cx.clone())
                                }
                                other => one(other),
                            });
                            if overlap == Overlap::Concat {
                                runs.flatten().boxed()
                            } else {
                                runs.flatten_unordered(at_once).boxed()
                            }
                        }
                    }
                });
                Ok((t, None))
            }
            Stage::Fork(sides) => {
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
                        stream.clone(),
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
            Stage::Catch(_) | Stage::Finalize(_) | Stage::Retry { .. } | Stage::Timeout(_) => {
                unreachable!("covering stages are handled where the pipe is put together")
            }
            Stage::Use {
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
                for (output, stages) in on {
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
                    let arm = self.pipe(stages, &at, None, true, Stream::PerItem, Vec::new())?;
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
                self.acts.extend(other.acts().iter().cloned());
                self.uses.insert(vine.clone());
                self.uses.extend(other.uses().iter().cloned());
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
            Stage::Branch {
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
                    let arm =
                        self.pipe(pipe, &at, known.clone(), true, Stream::PerItem, Vec::new())?;
                    feeds.extend(arm.feeds.into_iter().map(Feed::out_of_nested));
                    arms.insert(key.clone(), arm.transform);
                }
                let fallback = match otherwise {
                    Some(pipe) => {
                        let at = format!("{here}.else");
                        let arm = self.pipe(pipe, &at, known, true, Stream::PerItem, Vec::new())?;
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
            Stage::Emit { to, with } => {
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
