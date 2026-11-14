//! A registered operator, ready to run for one item.

use std::sync::{Arc, OnceLock};

use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;

use super::book::{Cx, Saved};
use super::flow::{env_of, fatal, misfit, Fault, OpFault};
use super::stream_call::ItemSaving;
use super::tag::Tag;
use super::Keep;
use crate::error::RunError;
use crate::expr::Template;
use crate::journal::Entry;
use crate::registry::{Handler, OpContext, OpError, OperatorSpec, StreamHandler};

/// How a unit runs.
#[derive(Clone)]
pub(super) enum Runner {
    /// A pure operator. It reaches nothing outside, so it is not saved.
    Pure(Handler),
    /// An operator that touches something outside. Its result is saved.
    Outside(Handler),
    /// A stream operator. Its items and its end are saved.
    Stream(StreamHandler),
}

/// A registered operator, ready to run for one item.
#[derive(Clone)]
pub(super) struct Unit {
    pub(super) path: String,
    pub(super) spec: Arc<OperatorSpec>,
    pub(super) runner: Runner,
    pub(super) with: Template,
    pub(super) keep: Keep,
    pub(super) end: Option<String>,
    /// For a stream operator: how a call is saved. The checker sets it
    /// once it has seen the operators after it.
    pub(super) saving: Arc<OnceLock<ItemSaving>>,
}

impl Unit {
    pub(super) fn resolve(&self, item: &Value) -> Result<Value, Fault> {
        let resolved = self.with.resolve(&env_of(item));
        self.spec.settings.check(&resolved).map_err(|problem| {
            fatal(RunError::SettingsMisfit {
                path: self.path.clone(),
                operator: self.spec.name.clone(),
                problem,
            })
        })?;
        Ok(resolved)
    }

    pub(super) fn failed(&self, error: OpError, item: &Value, tag: &Tag) -> Fault {
        Fault::Op(Box::new(OpFault {
            path: self.path.clone(),
            from: self.spec.name.clone(),
            error,
            item: item.clone(),
            tag: tag.clone(),
        }))
    }

    /// Run for one item and give back the raw result.
    pub(super) fn result(
        &self,
        item: Value,
        tag: Tag,
        cx: Cx,
    ) -> BoxFuture<'static, Result<Value, Fault>> {
        let unit = self.clone();
        async move {
            let resolved = unit.resolve(&item)?;
            let (handler, saved) = match &unit.runner {
                Runner::Pure(h) => (h.clone(), false),
                Runner::Outside(h) => (h.clone(), true),
                Runner::Stream(_) => unreachable!("a stream operator runs through `stream`"),
            };
            let key = if saved { cx.key(&unit.path, &tag) } else { None };
            if let Some(key) = &key {
                match cx.take(key) {
                    None => {}
                    Some(Saved::Once { input, outcome }) => {
                        if input != resolved {
                            return Err(misfit(format!(
                                "`{key}` was saved for input {input}, and the run gave it {resolved}"
                            )));
                        }
                        return outcome.map_err(|e| unit.failed(e, &item, &tag));
                    }
                    Some(Saved::Stream { .. } | Saved::Position { .. }) => {
                        return Err(misfit(format!(
                            "`{key}` was saved as a stream operator, and `{}` is not one",
                            unit.spec.name
                        )))
                    }
                }
            }
            if key.is_some() {
                cx.handed_out().await;
            }
            let cx_op = OpContext::new();
            let result = match handler(resolved.clone(), cx_op).await {
                Ok(v) => v,
                Err(error) => {
                    if let Some(key) = &key {
                        if error.code != OpError::CANCELLED {
                            let entry = Entry::Failed {
                                at: key.at.clone(),
                                input: resolved,
                                code: error.code.clone(),
                                message: error.message.clone(),
                            };
                            cx.save(key, entry);
                        }
                    }
                    return Err(unit.failed(error, &item, &tag));
                }
            };
            if let Some(shape) = &unit.spec.returns {
                shape.check(&result).map_err(|problem| {
                    fatal(RunError::ResultMisfit {
                        path: unit.path.clone(),
                        operator: unit.spec.name.clone(),
                        problem,
                    })
                })?;
            }
            if let Some(key) = &key {
                let entry = Entry::Done {
                    at: key.at.clone(),
                    input: resolved,
                    result: result.clone(),
                };
                cx.save(key, entry);
            }
            Ok(result)
        }
        .boxed()
    }

    /// Run for one item and give back the item that flows on.
    pub(super) fn apply(
        &self,
        item: Value,
        tag: Tag,
        cx: Cx,
    ) -> BoxFuture<'static, Result<Value, Fault>> {
        let unit = self.clone();
        async move {
            let result = unit.result(item.clone(), tag, cx).await?;
            unit.merge(item, result)
        }
        .boxed()
    }

    pub(super) fn merge(&self, item: Value, result: Value) -> Result<Value, Fault> {
        match &self.keep {
            Keep::Replace => Ok(result),
            Keep::Tap => Ok(item),
            Keep::Into(name) => add_field(&self.path, item, name, result),
        }
    }
}

pub(super) fn add_field(
    path: &str,
    mut item: Value,
    name: &str,
    value: Value,
) -> Result<Value, Fault> {
    match item.as_object_mut() {
        Some(map) => {
            map.insert(name.to_owned(), value);
            Ok(item)
        }
        None => Err(fatal(RunError::ItemMisfit {
            path: path.into(),
            problem: format!("`into: {name}` adds a field to the item, but the item is not a map"),
        })),
    }
}
