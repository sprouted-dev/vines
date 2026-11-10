//! The runner.
//!
//! A run checks the input against the declared shape, runs the steps, and
//! ends with exactly one declared output. A step failure no `catch`
//! handles ends the run with a [`RunError`], never with an output.
//!
//! Cancellation is cooperative. The runner checks the token before each
//! step and hands it to operators, which should return
//! [`OpError::cancelled`](crate::OpError::cancelled) when it fires.
//! `finally` blocks run with a fresh token, so cleanup is not cut short by
//! the cancellation that caused it. Dropping the run future instead of
//! cancelling it skips `finally`: Rust has no async drop.

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::error::RunError;
use crate::expr::{case_key, Env};
use crate::registry::{OpContext, OpError};
use crate::shape::Shape;
use crate::vine::{Kind, Mode, Step, Vine};

/// The declared output a run ended with.
#[derive(Debug, Clone, PartialEq)]
pub struct Output {
    pub name: String,
    pub payload: Value,
}

/// What a stream run did.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamReport {
    /// One output per consumed item, in order.
    pub outputs: Vec<Output>,
    /// How many items ended with a declared output. A caller saves this as
    /// its position; the item after it was not consumed.
    pub consumed: usize,
    /// Why the stream stopped early, if it did. `None` means the input
    /// stream ended.
    pub stopped: Option<RunError>,
}

enum Flow {
    Next,
    End(Output),
}

enum Fault {
    Op {
        path: String,
        operator: String,
        error: OpError,
    },
    Fatal(RunError),
}

type Outputs = BTreeMap<String, Shape>;

impl Vine {
    /// Run a once-mode vine.
    pub async fn run(&self, input: Value) -> Result<Output, RunError> {
        self.run_with(input, CancellationToken::new()).await
    }

    /// Run a once-mode vine that the caller may cancel.
    pub async fn run_with(
        &self,
        input: Value,
        cancel: CancellationToken,
    ) -> Result<Output, RunError> {
        if self.mode != Mode::Once {
            return Err(self.wrong_mode());
        }
        self.run_item(input, &cancel).await
    }

    /// Run a stream-mode vine over `items`, one output per item. Stops at
    /// the first item that does not end with an output, or on cancel.
    pub async fn run_stream<S>(&self, mut items: S, cancel: CancellationToken) -> StreamReport
    where
        S: Stream<Item = Value> + Unpin,
    {
        let mut report = StreamReport {
            outputs: Vec::new(),
            consumed: 0,
            stopped: None,
        };
        if self.mode != Mode::Stream {
            report.stopped = Some(self.wrong_mode());
            return report;
        }
        loop {
            if cancel.is_cancelled() {
                report.stopped = Some(RunError::Cancelled {
                    path: "stream".into(),
                });
                break;
            }
            let Some(item) = items.next().await else {
                break;
            };
            match self.run_item(item, &cancel).await {
                Ok(out) => {
                    report.outputs.push(out);
                    report.consumed += 1;
                }
                Err(e) => {
                    report.stopped = Some(e);
                    break;
                }
            }
        }
        report
    }

    fn wrong_mode(&self) -> RunError {
        RunError::WrongMode {
            vine: self.name.clone(),
            mode: self.mode.as_str(),
        }
    }

    async fn run_item(&self, input: Value, cancel: &CancellationToken) -> Result<Output, RunError> {
        self.input.check(&input).map_err(RunError::InputMisfit)?;
        let mut env = Env::new();
        env.insert("in".into(), input);
        let cx = OpContext::new(cancel.clone());
        match exec_block(&self.steps, env, &cx, &self.outputs).await {
            Ok(Flow::End(out)) => Ok(out),
            // The loader refuses steps that can finish without an output.
            Ok(Flow::Next) => unreachable!("loader guarantees every path ends in an output"),
            Err(Fault::Op {
                path,
                operator,
                error,
            }) => Err(if error.code == OpError::CANCELLED {
                RunError::Cancelled { path }
            } else {
                RunError::Unhandled {
                    path,
                    operator,
                    code: error.code,
                    message: error.message,
                }
            }),
            Err(Fault::Fatal(e)) => Err(e),
        }
    }
}

fn exec_block<'a>(
    steps: &'a [Step],
    mut env: Env,
    cx: &'a OpContext,
    outputs: &'a Outputs,
) -> BoxFuture<'a, Result<Flow, Fault>> {
    async move {
        for step in steps {
            if let Flow::End(out) = exec_step(step, &mut env, cx, outputs).await? {
                return Ok(Flow::End(out));
            }
        }
        Ok(Flow::Next)
    }
    .boxed()
}

async fn exec_step(
    step: &Step,
    env: &mut Env,
    cx: &OpContext,
    outputs: &Outputs,
) -> Result<Flow, Fault> {
    match &step.kind {
        Kind::Op { op, settings, bind } => {
            let name = &op.spec.name;
            if cx.cancellation().is_cancelled() {
                return Err(Fault::Op {
                    path: step.path.clone(),
                    operator: name.clone(),
                    error: OpError::cancelled("cancelled before the step started"),
                });
            }
            let resolved = settings.resolve(env);
            op.spec.settings.check(&resolved).map_err(|problem| {
                Fault::Fatal(RunError::SettingsMisfit {
                    path: step.path.clone(),
                    operator: name.clone(),
                    problem,
                })
            })?;
            let result = (op.handler)(resolved, cx.clone())
                .await
                .map_err(|error| Fault::Op {
                    path: step.path.clone(),
                    operator: name.clone(),
                    error,
                })?;
            if let Some(shape) = &op.spec.returns {
                shape.check(&result).map_err(|problem| {
                    Fault::Fatal(RunError::ResultMisfit {
                        path: step.path.clone(),
                        operator: name.clone(),
                        problem,
                    })
                })?;
            }
            if let Some(b) = bind {
                env.insert(b.clone(), result);
            }
            Ok(Flow::Next)
        }
        Kind::Guard { when, out, with } => {
            if when.eval(env) {
                end(&step.path, out, with.resolve(env), outputs)
            } else {
                Ok(Flow::Next)
            }
        }
        Kind::Out { name, with } => end(&step.path, name, with.resolve(env), outputs),
        Kind::Switch {
            on,
            cases,
            otherwise,
        } => {
            let key = case_key(&on.eval(env));
            let branch = key.and_then(|k| cases.get(&k)).or(otherwise.as_ref());
            match branch {
                Some(b) => exec_block(b, env.clone(), cx, outputs).await,
                None => Ok(Flow::Next),
            }
        }
        Kind::Try {
            body,
            catch,
            finally,
        } => {
            let mut result = exec_block(body, env.clone(), cx, outputs).await;
            if let Err(Fault::Op { error, .. }) = &result {
                if let Some(handler) = catch.get(&error.code) {
                    let mut caught = env.clone();
                    caught.insert(
                        "error".into(),
                        json!({ "code": error.code, "message": error.message }),
                    );
                    result = exec_block(handler, caught, cx, outputs).await;
                }
            }
            if !finally.is_empty() {
                // Shielded: cleanup gets a token the caller has not cancelled.
                let shielded = OpContext::new(CancellationToken::new());
                match exec_block(finally, env.clone(), &shielded, outputs).await {
                    Ok(_) => {}
                    Err(Fault::Op {
                        path,
                        operator,
                        error,
                    }) => {
                        return Err(Fault::Fatal(RunError::CleanupFailed {
                            path,
                            operator,
                            code: error.code,
                            message: error.message,
                            masked: describe(&result),
                        }))
                    }
                    Err(fatal) => return Err(fatal),
                }
            }
            result
        }
    }
}

fn end(path: &str, name: &str, payload: Value, outputs: &Outputs) -> Result<Flow, Fault> {
    outputs[name].check(&payload).map_err(|problem| {
        Fault::Fatal(RunError::OutputMisfit {
            path: path.into(),
            output: name.into(),
            problem,
        })
    })?;
    Ok(Flow::End(Output {
        name: name.into(),
        payload,
    }))
}

fn describe(r: &Result<Flow, Fault>) -> String {
    match r {
        Ok(Flow::End(o)) => format!("output `{}`", o.name),
        Ok(Flow::Next) => "the next step".into(),
        Err(Fault::Op {
            operator, error, ..
        }) => format!("error `{}` from `{operator}`", error.code),
        Err(Fault::Fatal(e)) => format!("error: {e}"),
    }
}
