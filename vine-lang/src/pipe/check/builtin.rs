//! The built-in operators that work on the item stream itself: `split`,
//! `filter`, `take`, `last` and `bufferCount`.

use std::sync::Arc;

use futures::future::ready;
use futures::{stream, StreamExt};
use serde_json::Value;

use super::{check_refs, malformed, Checker, Known, Site};
use crate::error::{LoadError, RunError};
use crate::expr::{is_name, Cond, Template};
use crate::pipe::book::Cx;
use crate::pipe::call_sites::Feed;
use crate::pipe::flow::{batches, env_of, fatal, held, last_of, Flow, Flowing, Items, Transform};
use crate::pipe::unit::add_field;
use crate::shape::{Shape, Ty};

impl Checker<'_> {
    /// `split`: one item becomes one item per element of a list.
    pub(super) fn split(
        site: Site<'_>,
        from: &Value,
        into: Option<&String>,
    ) -> Result<(Transform, Known), LoadError> {
        let Site { path, known, .. } = site;
        let here = format!("{path}.split");
        let from = Template::parse(from).map_err(|p| malformed(&here, &p.to_string()))?;
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
        let (into, at) = (into.cloned(), path.to_owned());
        let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
            let (from, into, at) = (from.clone(), into.clone(), at.clone());
            items
                .flat_map(move |flow| {
                    let many: Vec<Flow> = match flow {
                        Ok(Flowing::Value(item, tag)) => match from.resolve(&env_of(&item)) {
                            Value::Array(parts) => parts
                                .into_iter()
                                .map(|part| match &into {
                                    Some(name) => {
                                        held(add_field(&at, item.clone(), name, part), &tag)
                                    }
                                    None => Ok(Flowing::Value(part, tag.clone())),
                                })
                                .collect(),
                            _ => vec![Err(fatal(RunError::ItemMisfit {
                                path: at.clone(),
                                problem: "`split` needs a list".into(),
                            })
                            .of(&tag))],
                        },
                        other => vec![other],
                    };
                    stream::iter(many)
                })
                .boxed()
        });
        Ok((t, next))
    }

    /// `filter`: drop the items a condition does not hold for.
    pub(super) fn filter(site: Site<'_>, when: &Value) -> Result<(Transform, Known), LoadError> {
        let Site { path, known, .. } = site;
        let here = format!("{path}.filter");
        let cond = Cond::parse(when).map_err(|p| malformed(&here, &p.to_string()))?;
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

    /// `take`: let this many items through, then end the stream.
    pub(super) fn take(
        &mut self,
        site: Site<'_>,
        count: u64,
        feeds: &[Feed],
    ) -> Result<(Transform, Known), LoadError> {
        let Site { path, known, .. } = site;
        if count == 0 {
            return Err(malformed(
                &format!("{path}.take"),
                "`take` lets at least one item through",
            ));
        }
        self.sites.take(path, feeds);
        let count = usize::try_from(count).unwrap_or(usize::MAX);
        let t: Transform = Arc::new(move |items: Items, _cx: Cx| items.take(count).boxed());
        Ok((t, known))
    }

    /// `last`: when the stream ends, let only its final item through.
    pub(super) fn last(site: Site<'_>) -> (Transform, Known) {
        let Site { known, view, .. } = site;
        // After items run side by side, the value it gives is lifted
        // out of the groups made in this pipe, so what is made from
        // it is keyed the same whichever value it is.
        let root = view.mixed.as_ref().map(|_| view.root.clone());
        let t: Transform = Arc::new(move |items: Items, cx: Cx| {
            last_of(items, root.as_ref().map(|r| cx.sites_within(r)))
        });
        (t, known)
    }

    /// `bufferCount`: gather items into batches.
    pub(super) fn buffer_count(
        &mut self,
        site: Site<'_>,
        buffer_size: u64,
        into: Option<&String>,
        feeds: &[Feed],
    ) -> Result<(Transform, Known), LoadError> {
        let Site { path, known, .. } = site;
        let here = format!("{path}.bufferCount");
        if buffer_size == 0 {
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
        let batch_size = usize::try_from(buffer_size).unwrap_or(usize::MAX);
        let (into, at) = (into.cloned(), path.to_owned());
        let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
            batches(items, batch_size, into.clone(), at.clone())
        });
        Ok((t, next))
    }
}
