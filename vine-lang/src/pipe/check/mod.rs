//! The checker: it reads a described vine and builds the operators to run.
//!
//! [`assemble`] walks the pipe, and each pipe nested in it, operator by
//! operator. Each family of operators is checked and built in a module of
//! its own.

mod applied;
mod builtin;
mod emit;
mod nested;
mod placement;
mod registered;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::call_sites::{CallSites, Feed};
use super::flow::{chain, Transform};
use super::order::{merges_anywhere, View};
use super::timer::Timeouts;
use super::{Operator, Pipe, PipeDef};
use crate::error::LoadError;
use crate::expr::{is_name, Ref};
use crate::registry::Registry;
use crate::shape::Shape;
use placement::{applies_to_before, emits, follows_emit, stream_wide};

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

/// Where an operator stands while it is checked.
pub(super) struct Site<'v> {
    /// Its path, such as `pipe[1]`.
    path: &'v str,
    /// What is known about the item reaching it.
    known: Known,
    /// Whether the stream reaching it is one item, as inside a mapping
    /// operator.
    single: bool,
    /// What it sees of the stream.
    view: &'v View,
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
        let site = Site {
            path,
            known,
            single,
            view,
        };
        match op {
            Operator::Named { .. } | Operator::Call { .. } => self.registered(op, &site, feeds),
            Operator::Split { from, into } => self.split(site, from, into.as_ref()),
            Operator::Filter { when } => Self::filter(site, when),
            Operator::Take(count) => self.take(site, *count, feeds),
            Operator::Last => Self::last(site),
            Operator::BufferCount { buffer_size, into } => {
                self.buffer_count(site, *buffer_size, into.as_ref(), feeds)
            }
            Operator::ForkJoin(parts) => self.fork_join(&site, parts),
            Operator::Mapped {
                flatten,
                concurrent,
                pipe,
            } => self.mapped(site, *flatten, *concurrent, pipe, feeds),
            Operator::Fork(sides) => self.fork(site, sides, feeds),
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
            } => self.use_vine(&site, vine, with.as_ref(), keep, on, feeds),
            Operator::Branch {
                on,
                cases,
                otherwise,
            } => self.branch(site, on, cases, otherwise.as_ref(), feeds),
            Operator::Emit { to, with } => self.emit(&site, to, with.as_ref()),
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
