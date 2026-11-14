//! What flows between stages, and the stream shapes the stages are built
//! from.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Poll, Waker};

use futures::future::{ready, BoxFuture};
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use serde_json::{json, Value};

use super::book::Cx;
use super::tag::Tag;
use super::unit::add_field;
use crate::error::RunError;
use crate::expr::Env;
use crate::registry::OpError;
use crate::run::Output;

/// What flows between stages: a value still being worked on, or a value
/// already sent to an output. Later stages leave an output alone. Each
/// carries the [`Tag`] of the input items it came from.
#[derive(Clone)]
pub(super) enum Flowing {
    Value(Value, Tag),
    Out(Output, Tag),
}

/// A failure on its way to a `catch` or to the end of the run. It carries
/// the tag of the item that failed, so that item is not done while the
/// failure is in flight.
pub(super) enum Fault {
    /// An operator or act failed.
    Op(Box<OpFault>),
    Fatal(Box<RunError>, Tag),
}

pub(super) struct OpFault {
    pub(super) path: String,
    pub(super) from: String,
    pub(super) error: OpError,
    /// The item as it was when the stage failed.
    pub(super) item: Value,
    pub(super) tag: Tag,
}

impl Fault {
    /// The failure, held for the item with this tag.
    pub(super) fn of(self, tag: &Tag) -> Fault {
        match self {
            Fault::Op(mut f) => {
                f.tag.or(tag);
                Fault::Op(f)
            }
            Fault::Fatal(e, mut held) => {
                held.or(tag);
                Fault::Fatal(e, held)
            }
        }
    }
}

/// A value made for the item with `tag`, or its failure, held for it.
pub(super) fn held(made: Result<Value, Fault>, tag: &Tag) -> Flow {
    match made {
        Ok(v) => Ok(Flowing::Value(v, tag.clone())),
        Err(fault) => Err(fault.of(tag)),
    }
}

pub(super) fn fatal(e: RunError) -> Fault {
    Fault::Fatal(Box::new(e), Tag::default())
}

pub(super) fn misfit(problem: impl Into<String>) -> Fault {
    fatal(RunError::ReplayMisfit {
        problem: problem.into(),
    })
}

pub(super) type Flow = Result<Flowing, Fault>;
pub(super) type Items = BoxStream<'static, Flow>;
pub(super) type Transform = Arc<dyn Fn(Items, Cx) -> Items + Send + Sync>;

pub(super) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub(super) fn run_error(fault: Fault) -> RunError {
    match fault {
        Fault::Fatal(e, _) => *e,
        Fault::Op(fault) => {
            let OpFault {
                path, from, error, ..
            } = *fault;
            if error.code == OpError::CANCELLED {
                RunError::Cancelled { path }
            } else {
                RunError::Unhandled {
                    path,
                    operator: from,
                    code: error.code,
                    message: error.message,
                }
            }
        }
    }
}

pub(super) fn env_of(item: &Value) -> Env {
    let mut env = Env::new();
    env.insert("item".into(), item.clone());
    env
}

pub(super) fn one(flow: Flow) -> Items {
    stream::once(ready(flow)).boxed()
}

pub(super) fn chain(transforms: Vec<Transform>) -> Transform {
    Arc::new(move |items: Items, cx: Cx| {
        transforms
            .iter()
            .fold(items, |items, t| t(items, cx.clone()))
    })
}

/// Apply `f` to each value still being worked on. Outputs and failures
/// pass through.
pub(super) fn per_item<F>(f: F) -> Transform
where
    F: Fn(Value, Tag, Cx) -> BoxFuture<'static, Result<Value, Fault>> + Send + Sync + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |items: Items, cx: Cx| {
        let f = f.clone();
        items
            .then(move |flow| {
                let (f, cx) = (f.clone(), cx.clone());
                async move {
                    match flow {
                        Ok(Flowing::Value(v, tag)) => match f(v, tag.clone(), cx).await {
                            Ok(v) => Ok(Flowing::Value(v, tag)),
                            Err(fault) => Err(fault.of(&tag)),
                        },
                        other => other,
                    }
                }
            })
            .boxed()
    })
}

/// Turn each value still being worked on into a stream, one after the
/// other. Outputs and failures pass through.
pub(super) fn per_item_stream<F>(f: F) -> Transform
where
    F: Fn(Value, Tag, Cx) -> Items + Send + Sync + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |items: Items, cx: Cx| {
        let f = f.clone();
        items
            .map(move |flow| match flow {
                Ok(Flowing::Value(v, tag)) => f(v, tag, cx.clone()),
                other => one(other),
            })
            .flatten()
            .boxed()
    })
}

/// Each new value starts the pipe inside for itself. `Switch` drops the
/// run in flight for the new one. `Exhaust` ignores the new one while a
/// run is in flight. Outputs and failures from upstream are never dropped.
pub(super) fn latest_or_first(outer: Items, inner: Transform, switch: bool, cx: Cx) -> Items {
    let mut outer = Some(outer);
    let mut current: Option<Items> = None;
    let mut passing: VecDeque<Flow> = VecDeque::new();
    stream::poll_fn(move |task| {
        if let Some(upstream) = outer.as_mut() {
            loop {
                match upstream.poll_next_unpin(task) {
                    Poll::Ready(Some(Ok(Flowing::Value(v, tag)))) => {
                        if switch || current.is_none() {
                            current = Some(inner(one(Ok(Flowing::Value(v, tag))), cx.clone()));
                        }
                    }
                    Poll::Ready(Some(other)) => passing.push_back(other),
                    Poll::Ready(None) => {
                        outer = None;
                        break;
                    }
                    Poll::Pending => break,
                }
            }
        }
        if let Some(flow) = passing.pop_front() {
            return Poll::Ready(Some(flow));
        }
        if let Some(run) = current.as_mut() {
            match run.poll_next_unpin(task) {
                Poll::Ready(Some(flow)) => return Poll::Ready(Some(flow)),
                Poll::Ready(None) => current = None,
                Poll::Pending => return Poll::Pending,
            }
        }
        if outer.is_none() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    })
    .boxed()
}

/// When the stream ends, give its final value.
pub(super) fn last_of(mut items: Items) -> Items {
    let mut held: Option<(Value, Tag)> = None;
    let mut done = false;
    stream::poll_fn(move |task| loop {
        if done {
            return Poll::Ready(None);
        }
        match items.poll_next_unpin(task) {
            // The value before is dropped, and with it its hold on its item.
            Poll::Ready(Some(Ok(Flowing::Value(v, tag)))) => held = Some((v, tag)),
            Poll::Ready(Some(other)) => return Poll::Ready(Some(other)),
            Poll::Ready(None) => {
                done = true;
                return Poll::Ready(held.take().map(|(v, tag)| Ok(Flowing::Value(v, tag))));
            }
            Poll::Pending => return Poll::Pending,
        }
    })
    .boxed()
}

/// Batches of `size`, and what is left when the stream ends.
pub(super) fn batches(mut items: Items, size: usize, into: Option<String>, path: String) -> Items {
    let mut buf: Vec<(Value, Tag)> = Vec::new();
    let mut done = false;
    let batch = move |kept: Vec<(Value, Tag)>| -> Flow {
        let (items, tags): (Vec<Value>, Vec<Tag>) = kept.into_iter().unzip();
        let tag = Tag::batch(tags);
        let count = items.len();
        let value = match &into {
            None => Ok(json!({ "items": items, "count": count })),
            Some(name) => {
                let last = items.last().cloned().unwrap_or(Value::Null);
                add_field(&path, last, name, json!({ "items": items, "count": count }))
            }
        };
        held(value, &tag)
    };
    stream::poll_fn(move |task| loop {
        if done {
            return Poll::Ready(None);
        }
        match items.poll_next_unpin(task) {
            Poll::Ready(Some(Ok(Flowing::Value(v, tag)))) => {
                buf.push((v, tag));
                if buf.len() >= size {
                    return Poll::Ready(Some(batch(std::mem::take(&mut buf))));
                }
            }
            Poll::Ready(Some(other)) => return Poll::Ready(Some(other)),
            Poll::Ready(None) => {
                done = true;
                if buf.is_empty() {
                    return Poll::Ready(None);
                }
                return Poll::Ready(Some(batch(std::mem::take(&mut buf))));
            }
            Poll::Pending => return Poll::Pending,
        }
    })
    .boxed()
}

/// What a side pipe of a fork reads from.
#[derive(Default)]
pub(super) struct SideIn {
    queue: VecDeque<Flow>,
    closed: bool,
    /// The side pipe asked for its next item and there was none.
    starved: bool,
    waker: Option<Waker>,
}

impl SideIn {
    fn wake(&mut self) {
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }
}

pub(super) fn side_input(shared: Arc<Mutex<SideIn>>) -> Items {
    stream::poll_fn(move |task| {
        let mut side = lock(&shared);
        if let Some(flow) = side.queue.pop_front() {
            return Poll::Ready(Some(flow));
        }
        if side.closed {
            return Poll::Ready(None);
        }
        side.starved = true;
        side.waker = Some(task.waker().clone());
        Poll::Pending
    })
    .boxed()
}

/// Send each value down every side pipe, then on down the main pipe. For
/// each value the side pipes run in order, each until it wants the next
/// value, so what comes out does not depend on timing.
pub(super) fn forked(mut upstream: Items, sides: Vec<Transform>, cx: Cx) -> Items {
    let inputs: Vec<Arc<Mutex<SideIn>>> = sides
        .iter()
        .map(|_| Arc::new(Mutex::new(SideIn::default())))
        .collect();
    let mut outs: Vec<Option<Items>> = sides
        .iter()
        .zip(&inputs)
        .map(|(side, input)| Some(side(side_input(input.clone()), cx.clone())))
        .collect();
    let mut ready_out: VecDeque<Flow> = VecDeque::new();
    let mut held: Option<Flow> = None;
    let mut draining: Option<usize> = None;
    let mut upstream_done = false;
    stream::poll_fn(move |task| loop {
        if let Some(flow) = ready_out.pop_front() {
            return Poll::Ready(Some(flow));
        }
        if let Some(i) = draining {
            if i == outs.len() {
                draining = None;
                if let Some(flow) = held.take() {
                    return Poll::Ready(Some(flow));
                }
                if upstream_done {
                    return Poll::Ready(None);
                }
                continue;
            }
            let Some(side) = outs[i].as_mut() else {
                draining = Some(i + 1);
                continue;
            };
            match side.poll_next_unpin(task) {
                // A value that reaches the end of a side pipe is dropped.
                Poll::Ready(Some(Ok(Flowing::Value(..)))) => {}
                Poll::Ready(Some(other)) => ready_out.push_back(other),
                Poll::Ready(None) => {
                    outs[i] = None;
                    draining = Some(i + 1);
                }
                Poll::Pending => {
                    let idle = {
                        let s = lock(&inputs[i]);
                        s.starved && s.queue.is_empty() && !s.closed
                    };
                    if !idle {
                        return Poll::Pending;
                    }
                    draining = Some(i + 1);
                }
            }
            continue;
        }
        if upstream_done {
            return Poll::Ready(None);
        }
        match upstream.poll_next_unpin(task) {
            Poll::Ready(Some(Ok(Flowing::Value(v, tag)))) => {
                for (input, out) in inputs.iter().zip(&outs) {
                    if out.is_some() {
                        let mut s = lock(input);
                        s.queue
                            .push_back(Ok(Flowing::Value(v.clone(), tag.clone())));
                        s.starved = false;
                        s.wake();
                    }
                }
                held = Some(Ok(Flowing::Value(v, tag)));
                draining = Some(0);
            }
            Poll::Ready(Some(other)) => return Poll::Ready(Some(other)),
            Poll::Ready(None) => {
                upstream_done = true;
                for input in &inputs {
                    let mut s = lock(input);
                    s.closed = true;
                    s.wake();
                }
                draining = Some(0);
            }
            Poll::Pending => return Poll::Pending,
        }
    })
    .boxed()
}

/// Pass on what the body gives. When it ends, or at its first failure,
/// run the cleanup, and only then pass on the failure: a caller stops
/// reading at a failure, and the cleanup must not be skipped.
pub(super) fn tidied(body: Items, cleanup: Transform, item: Value, tag: Tag, cx: Cx) -> Items {
    let mut body = Some(body);
    let mut start = Some((cleanup, item, tag, cx));
    let mut tidy: Option<Items> = None;
    let mut after: VecDeque<Flow> = VecDeque::new();
    let mut done = false;
    stream::poll_fn(move |task| loop {
        if let Some(b) = body.as_mut() {
            match b.poll_next_unpin(task) {
                Poll::Ready(Some(Ok(flow))) => return Poll::Ready(Some(Ok(flow))),
                Poll::Ready(Some(Err(fault))) => {
                    after.push_back(Err(fault));
                    body = None;
                }
                Poll::Ready(None) => body = None,
                Poll::Pending => return Poll::Pending,
            }
            continue;
        }
        if let Some((cleanup, item, tag, cx)) = start.take() {
            tidy = Some(cleanup(one(Ok(Flowing::Value(item, tag))), cx));
        }
        if let Some(t) = tidy.as_mut() {
            match t.poll_next_unpin(task) {
                // What cleanup gives back is dropped. Its failure is not.
                Poll::Ready(Some(Ok(_))) => continue,
                Poll::Ready(Some(Err(fault))) => {
                    after.push_back(Err(fault));
                    tidy = None;
                }
                Poll::Ready(None) => tidy = None,
                Poll::Pending => return Poll::Pending,
            }
            continue;
        }
        if done {
            return Poll::Ready(None);
        }
        match after.pop_front() {
            Some(flow) => return Poll::Ready(Some(flow)),
            None => {
                done = true;
                return Poll::Ready(None);
            }
        }
    })
    .boxed()
}

/// Run the body; on the first failure a vine may handle, drop the body and
/// run the handler on the item as it was when it failed.
pub(super) fn rescued(body: Items, handler: Transform, cx: Cx) -> Items {
    let mut body = Some(body);
    let mut rescue: Option<Items> = None;
    stream::poll_fn(move |task| loop {
        if let Some(r) = rescue.as_mut() {
            return r.poll_next_unpin(task);
        }
        let Some(b) = body.as_mut() else {
            return Poll::Ready(None);
        };
        match b.poll_next_unpin(task) {
            Poll::Ready(Some(flow)) if handleable(&flow) => {
                body = None;
                let Err(Fault::Op(fault)) = flow else {
                    unreachable!("a handleable result is a failed operator")
                };
                let OpFault {
                    from,
                    error,
                    item,
                    tag,
                    ..
                } = *fault;
                let failure = json!({
                    "code": error.code,
                    "message": error.message,
                    "from": from,
                });
                let failed = match item {
                    Value::Object(mut map) => {
                        map.insert("error".into(), failure);
                        Value::Object(map)
                    }
                    other => json!({ "item": other, "error": failure }),
                };
                rescue = Some(handler(one(Ok(Flowing::Value(failed, tag))), cx.clone()));
            }
            Poll::Ready(None) => {
                body = None;
                return Poll::Ready(None);
            }
            other => return other,
        }
    })
    .boxed()
}

/// Run `f` for each value still being worked on, and send on everything
/// it gives back. Outputs and failures from upstream pass through.
pub(super) fn covering<F>(f: F) -> Transform
where
    F: Fn(Value, Tag, Cx) -> BoxFuture<'static, Vec<Flow>> + Send + Sync + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |items: Items, cx: Cx| {
        let f = f.clone();
        items
            .then(move |flow| {
                let (f, cx) = (f.clone(), cx.clone());
                async move {
                    match flow {
                        Ok(Flowing::Value(item, tag)) => f(item, tag, cx).await,
                        other => vec![other],
                    }
                }
            })
            .flat_map(stream::iter)
            .boxed()
    })
}

/// Whether a result is a failure a vine may handle. Cancellation is not.
pub(super) fn handleable(flow: &Flow) -> bool {
    matches!(flow, Err(Fault::Op(f)) if f.error.code != OpError::CANCELLED)
}
