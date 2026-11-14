//! The operators that run pipes inside them: the four mapping operators,
//! `fork`, `use` and `branch`.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use serde_json::Value;

use super::{check_refs, malformed, Checker, Known, Site};
use crate::error::LoadError;
use crate::expr::{case_key, is_name, Template};
use crate::pipe::book::Cx;
use crate::pipe::call_sites::Feed;
use crate::pipe::flow::{env_of, forked, held, latest_or_first, one, Flowing, Items, Transform};
use crate::pipe::unit::add_field;
use crate::pipe::{Flatten, Keep, Operator, Pipe};

impl Checker<'_> {
    /// `concatMap`, `mergeMap`, `switchMap` or `exhaustMap`: a pipe run for
    /// each item.
    pub(super) fn mapped(
        &mut self,
        site: Site<'_>,
        flatten: Flatten,
        concurrent: Option<usize>,
        pipe: &[Operator],
        feeds: &mut Vec<Feed>,
    ) -> Result<(Transform, Known), LoadError> {
        let Site {
            path, known, view, ..
        } = site;
        let key = flatten.key();
        let here = format!("{path}.{key}");
        if concurrent.is_some() && flatten != Flatten::Merge {
            return Err(malformed(
                &here,
                "`concurrent` limits side-by-side runs, so only `mergeMap` takes it",
            ));
        }
        if concurrent == Some(0) {
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

    /// `fork`: each item goes down every side pipe and on down this one.
    pub(super) fn fork(
        &mut self,
        site: Site<'_>,
        sides: &[Vec<Operator>],
        feeds: &mut Vec<Feed>,
    ) -> Result<(Transform, Known), LoadError> {
        let Site {
            path,
            known,
            single,
            view,
        } = site;
        let here = format!("{path}.fork");
        if sides.is_empty() {
            return Err(malformed(&here, "`fork` needs at least one side pipe"));
        }
        let mut built = Vec::new();
        let mut side_feeds = Vec::new();
        for (j, side) in sides.iter().enumerate() {
            let at = format!("{here}[{j}]");
            let checked = self.pipe(
                side,
                &at,
                known.clone(),
                single,
                view.clone(),
                feeds.clone(),
            )?;
            built.push(checked.transform);
            side_feeds.extend(checked.feeds.into_iter().map(Feed::out_of_side));
        }
        feeds.extend(side_feeds);
        let t: Transform = Arc::new(move |items: Items, cx: Cx| forked(items, &built, &cx));
        Ok((t, known))
    }

    /// `use`: another vine run for each item, with operators for each of its
    /// outputs.
    pub(super) fn use_vine(
        &mut self,
        site: &Site<'_>,
        vine: &str,
        with: Option<&Value>,
        keep: &Keep,
        on: &[(String, Vec<Operator>)],
        feeds: &mut Vec<Feed>,
    ) -> Result<(Transform, Known), LoadError> {
        let path = site.path;
        let known = &site.known;
        let view = site.view;
        let Some(other) = self.reg.get_vine(vine).cloned() else {
            return Err(LoadError::UnknownVine {
                path: path.into(),
                name: vine.to_owned(),
            });
        };
        let with = if let Some(w) = with {
            let at = format!("{path}.with");
            let t = Template::parse(w).map_err(|p| malformed(&at, &p.to_string()))?;
            check_refs(t.refs(), known, &at)?;
            other
                .input()
                .check_template(&t)
                .map_err(|problem| LoadError::BadSettings {
                    path: at,
                    operator: vine.to_owned(),
                    problem: problem.to_string(),
                })?;
            Some(t)
        } else {
            if let Some(item) = &known {
                item.fits_into(other.input())
                    .map_err(|problem| LoadError::BadSettings {
                        path: path.into(),
                        operator: vine.to_owned(),
                        problem: problem.to_string(),
                    })?;
            }
            None
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
        feeds.extend(
            self.sites
                .used(path, vine, other.by_position, &other.saves_every_item),
        );
        self.reach.extend(other.reach().iter().cloned());
        self.calls.extend(other.calls().iter().cloned());
        self.uses.insert(vine.to_owned());
        self.uses.extend(other.uses().iter().cloned());
        self.timeouts.used(path, vine, &other.timeouts);
        let t = used(other, with, keep.clone(), arms, path.to_owned());
        Ok((t, None))
    }

    /// `branch`: the pipe whose key equals a value of the item.
    pub(super) fn branch(
        &mut self,
        site: Site<'_>,
        on: &Value,
        cases: &[(String, Vec<Operator>)],
        otherwise: Option<&Vec<Operator>>,
        feeds: &mut Vec<Feed>,
    ) -> Result<(Transform, Known), LoadError> {
        let Site {
            path, known, view, ..
        } = site;
        let here = format!("{path}.branch");
        let on =
            Template::parse(on).map_err(|p| malformed(&format!("{here}.on"), &p.to_string()))?;
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
                let arm = self.pipe(pipe, &at, known, true, view.per_value(&at), Vec::new())?;
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
}

/// What runs a `use` at `at`: `other` for each item, its input built by
/// `with`, and each of its outputs handed to the arm for it.
fn used(
    other: Pipe,
    with: Option<Template>,
    keep: Keep,
    arms: BTreeMap<String, Transform>,
    at: String,
) -> Transform {
    Arc::new(move |items: Items, cx: Cx| {
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
                                Ok(value) => arms[&out.name](one(Ok(value)), cx.clone()),
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
    })
}
