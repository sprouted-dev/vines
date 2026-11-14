//! `catchError`, `finalize`, `retry` and `timeout`: the operators that
//! apply to the operators before them, one item at a time.

use futures::{FutureExt, StreamExt};

use super::placement::emits_anywhere;
use super::{malformed, Checker};
use crate::error::LoadError;
use crate::pipe::call_sites::Feed;
use crate::pipe::flow::{
    handleable, one, per_item_held, per_item_stream, rescued, tidied, Flow, Flowing, Transform,
};
use crate::pipe::order::View;
use crate::pipe::timer::timed;
use crate::pipe::Operator;

impl Checker<'_> {
    /// An operator that applies to the operators before it, `body`, one item at a
    /// time.
    /// Gives what runs it, and the stream operator calls whose items come out
    /// of a `catchError` handler.
    pub(super) fn apply_to_before(
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
}
