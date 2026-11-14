//! `emit`: what reaches a vine's named outputs, checked against their
//! shapes.

use std::sync::Arc;

use futures::StreamExt;
use serde_json::Value;

use super::{check_refs, malformed, Checker, Known, Site};
use crate::error::{LoadError, RunError};
use crate::expr::Template;
use crate::pipe::book::Cx;
use crate::pipe::flow::{env_of, fatal, Flowing, Items, Transform};
use crate::pipe::output::Output;

impl Checker<'_> {
    /// `emit`: send the item, or a value built from it, to a named output.
    pub(super) fn emit(
        &self,
        site: &Site<'_>,
        to: &str,
        with: Option<&Value>,
    ) -> Result<(Transform, Known), LoadError> {
        let path = site.path;
        let known = &site.known;
        let here = format!("{path}.emit");
        let Some(shape) = self.outputs.get(to).cloned() else {
            return Err(LoadError::UndeclaredOutput {
                path: here,
                name: to.to_owned(),
            });
        };
        let with = match with {
            Some(w) => {
                let at = format!("{here}.with");
                let t = Template::parse(w).map_err(|p| malformed(&at, &p.to_string()))?;
                check_refs(t.refs(), &known, &at)?;
                shape
                    .check_template(&t)
                    .map_err(|problem| LoadError::OutputMisfit {
                        path: at,
                        output: to.to_owned(),
                        problem: problem.to_string(),
                    })?;
                Some(t)
            }
            None => {
                if let Some(item) = &known {
                    item.fits_into(&shape)
                        .map_err(|problem| LoadError::OutputMisfit {
                            path: here.clone(),
                            output: to.to_owned(),
                            problem: problem.to_string(),
                        })?;
                }
                None
            }
        };
        let (to, at) = (to.to_owned(), here);
        let t: Transform = Arc::new(move |items: Items, _cx: Cx| {
            let (to, at, with, shape) = (to.clone(), at.clone(), with.clone(), shape.clone());
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
                                problem: problem.to_string(),
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
