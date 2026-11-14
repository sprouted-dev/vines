//! The operators a caller registers: run by name, by `call` or `tap`, and
//! side by side in a `forkJoin`.

use std::sync::Arc;

use futures::future::join_all;
use futures::FutureExt;

use super::{check_refs, malformed, Checker, Known, Site};
use crate::error::LoadError;
use crate::expr::{is_name, Template};
use crate::pipe::call_sites::Feed;
use crate::pipe::flow::{per_item, per_item_stream, Transform};
use crate::pipe::unit::{add_field, Runner, Unit};
use crate::pipe::{Keep, Operator};
use crate::registry::Run;
use crate::shape::Ty;

impl Checker<'_> {
    /// A registered operator run by its name, by `call` or by `tap`.
    pub(super) fn registered(
        &mut self,
        op: &Operator,
        site: &Site<'_>,
        feeds: &mut Vec<Feed>,
    ) -> Result<(Transform, Known), LoadError> {
        let path = site.path;
        let known = &site.known;
        let single = site.single;
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

    /// `forkJoin`: registered operators run side by side for one item.
    pub(super) fn fork_join(
        &mut self,
        site: &Site<'_>,
        parts: &[Operator],
    ) -> Result<(Transform, Known), LoadError> {
        let path = site.path;
        let known = &site.known;
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
        let template = Template::parse(with).map_err(|p| malformed(&with_path, &p.to_string()))?;
        check_refs(template.refs(), known, &with_path)?;
        spec.settings
            .check_template(&template)
            .map_err(|problem| LoadError::BadSettings {
                path: with_path,
                operator: name.clone(),
                problem: problem.to_string(),
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
}
