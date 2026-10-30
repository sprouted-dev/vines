//! Batching by count, idle deadline, bytes and completion; cancel flush vs
//! reported discard.

mod common;

use std::time::Duration;

use common::nz;
use futures::{stream, StreamExt};
use tokio::sync::mpsc;
use tokio::time::Instant;
use vines::batch::{batch, Batch, BatchEvent, BatchPolicy, Discarded, FlushReason, OnCancel};
use vines::cancel::{CancelCause, CancelScope};
use vines::PolicyError;

fn receiver_stream<T>(mut rx: mpsc::Receiver<T>) -> impl futures::Stream<Item = T> {
    stream::poll_fn(move |cx| rx.poll_recv(cx))
}

/// Bound a wait so a missing flush fails the test instead of hanging it.
async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(600), future)
        .await
        .expect("no event within 600s of paused time")
}

fn items<T: Clone>(event: &BatchEvent<T>) -> (Vec<T>, FlushReason) {
    match event {
        BatchEvent::Batch(batch) => (batch.items.clone(), batch.reason),
        BatchEvent::Cancelled { .. } => panic!("expected a batch, got the cancel marker"),
    }
}

#[tokio::test(start_paused = true)]
async fn flushes_by_count_and_on_completion() {
    let policy = BatchPolicy::new(nz(3), OnCancel::Flush);
    let out: Vec<_> = batch(stream::iter(1..=7), policy, &CancelScope::new())
        .collect()
        .await;
    assert_eq!(out.len(), 3);
    assert_eq!(items(&out[0]), (vec![1, 2, 3], FlushReason::Count));
    assert_eq!(items(&out[1]), (vec![4, 5, 6], FlushReason::Count));
    assert_eq!(items(&out[2]), (vec![7], FlushReason::Completed));
}

#[tokio::test(start_paused = true)]
async fn an_empty_input_yields_nothing() {
    let policy = BatchPolicy::<u8>::new(nz(3), OnCancel::Flush);
    let out: Vec<_> = batch(stream::empty(), policy, &CancelScope::new())
        .collect()
        .await;
    assert!(out.is_empty());
}

#[tokio::test(start_paused = true)]
async fn the_deadline_flushes_with_no_new_input() {
    let (tx, rx) = mpsc::channel(8);
    let policy = BatchPolicy::new(nz(100), OnCancel::Flush)
        .max_wait(Duration::from_secs(10))
        .unwrap();
    let mut batches = batch(receiver_stream(rx), policy, &CancelScope::new());
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    let started = Instant::now();
    let first = within(batches.next()).await.unwrap();
    assert_eq!(items(&first), (vec![1, 2], FlushReason::Deadline));
    assert_eq!(started.elapsed(), Duration::from_secs(10));
    // The input is still open: this was the timer, not the end of input.
    assert!(!tx.is_closed());
}

#[tokio::test(start_paused = true)]
async fn the_deadline_runs_from_a_batchs_first_item() {
    let (tx, rx) = mpsc::channel(8);
    let policy = BatchPolicy::new(nz(100), OnCancel::Flush)
        .max_wait(Duration::from_secs(10))
        .unwrap();
    let mut batches = batch(receiver_stream(rx), policy, &CancelScope::new());
    let start = Instant::now();
    let feeder = tokio::spawn(async move {
        tx.send(1).await.unwrap();
        tokio::time::sleep(Duration::from_secs(6)).await;
        tx.send(2).await.unwrap(); // inside the first window
        tokio::time::sleep(Duration::from_secs(6)).await;
        tx.send(3).await.unwrap(); // after it: starts a new window at t=12
        tx
    });
    let first = within(batches.next()).await.unwrap();
    assert_eq!(items(&first), (vec![1, 2], FlushReason::Deadline));
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    let second = within(batches.next()).await.unwrap();
    assert_eq!(items(&second), (vec![3], FlushReason::Deadline));
    assert_eq!(start.elapsed(), Duration::from_secs(22));
    drop(feeder.await.unwrap());
    assert!(within(batches.next()).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn a_count_flush_clears_the_deadline() {
    let (tx, rx) = mpsc::channel(8);
    let policy = BatchPolicy::new(nz(2), OnCancel::Flush)
        .max_wait(Duration::from_secs(10))
        .unwrap();
    let mut batches = batch(receiver_stream(rx), policy, &CancelScope::new());
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    assert_eq!(
        items(&batches.next().await.unwrap()),
        (vec![1, 2], FlushReason::Count)
    );
    // No empty deadline batch follows: the timer went with the batch.
    let quiet = tokio::time::timeout(Duration::from_secs(60), batches.next()).await;
    assert!(quiet.is_err(), "nothing held, nothing flushed");
    drop(tx);
    assert!(batches.next().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn a_zero_wait_is_refused() {
    let error = BatchPolicy::<u8>::new(nz(2), OnCancel::Flush)
        .max_wait(Duration::ZERO)
        .unwrap_err();
    assert!(matches!(
        error,
        PolicyError::ZeroDuration {
            field: "max_wait",
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn flushes_before_exceeding_max_bytes() {
    let policy = BatchPolicy::new(nz(100), OnCancel::Flush).max_bytes(nz(10), |s: &&str| s.len());
    let out: Vec<_> = batch(
        stream::iter(["aaaa", "bbbb", "cccc", "dd", "eeeeee"]),
        policy,
        &CancelScope::new(),
    )
    .collect()
    .await;
    let batches: Vec<Batch<&str>> = out.into_iter().filter_map(BatchEvent::into_batch).collect();
    assert_eq!(batches[0].items, vec!["aaaa", "bbbb"]);
    assert_eq!(
        (batches[0].bytes, batches[0].reason),
        (8, FlushReason::Bytes)
    );
    // 4 + 2 = 6, then 6 more would be 12 > 10: flush first.
    assert_eq!(batches[1].items, vec!["cccc", "dd"]);
    assert_eq!(batches[2].items, vec!["eeeeee"]);
    assert_eq!(batches[2].reason, FlushReason::Completed);
    assert!(batches.iter().all(|batch| batch.bytes <= 10));
}

#[tokio::test(start_paused = true)]
async fn a_batch_that_reaches_max_bytes_exactly_flushes_at_once() {
    let policy = BatchPolicy::new(nz(100), OnCancel::Flush).max_bytes(nz(4), |n: &usize| *n);
    let out: Vec<_> = batch(stream::iter([2usize, 2, 1]), policy, &CancelScope::new())
        .collect()
        .await;
    assert_eq!(items(&out[0]), (vec![2, 2], FlushReason::Bytes));
    assert_eq!(items(&out[1]), (vec![1], FlushReason::Completed));
}

#[tokio::test(start_paused = true)]
async fn an_oversize_item_is_emitted_alone_and_flagged() {
    let policy = BatchPolicy::new(nz(100), OnCancel::Flush).max_bytes(nz(4), |n: &usize| *n);
    let out: Vec<_> = batch(stream::iter([1usize, 9, 1]), policy, &CancelScope::new())
        .collect()
        .await;
    assert_eq!(items(&out[0]), (vec![1], FlushReason::Bytes));
    assert_eq!(items(&out[1]), (vec![9], FlushReason::Oversize));
    assert_eq!(items(&out[2]), (vec![1], FlushReason::Completed));
}

#[tokio::test(start_paused = true)]
async fn cancel_with_flush_emits_what_is_held_then_marks_the_cancel() {
    let (tx, rx) = mpsc::channel(8);
    let scope = CancelScope::new();
    let mut batches = batch(
        receiver_stream(rx),
        BatchPolicy::new(nz(10), OnCancel::Flush),
        &scope,
    );
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(1), batches.next())
        .await
        .is_err());
    scope.cancel();
    assert_eq!(
        items(&batches.next().await.unwrap()),
        (vec![1, 2], FlushReason::Cancelled)
    );
    assert_eq!(
        batches.next().await.unwrap(),
        BatchEvent::Cancelled {
            cause: CancelCause::Requested,
            discarded: Discarded::default()
        }
    );
    assert!(batches.next().await.is_none());
    // Input that arrives after the cancel is never taken.
    tx.send(3).await.unwrap();
    assert!(batches.next().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn cancel_with_discard_reports_what_it_dropped() {
    let (tx, rx) = mpsc::channel(8);
    let scope = CancelScope::new();
    let policy = BatchPolicy::new(nz(10), OnCancel::Discard).max_bytes(nz(100), |n: &usize| *n);
    let mut batches = batch(receiver_stream(rx), policy, &scope);
    for n in [3usize, 4, 5] {
        tx.send(n).await.unwrap();
    }
    assert!(tokio::time::timeout(Duration::from_secs(1), batches.next())
        .await
        .is_err());
    scope.cancel();
    assert_eq!(
        batches.next().await.unwrap(),
        BatchEvent::Cancelled {
            cause: CancelCause::Requested,
            discarded: Discarded {
                items: 3,
                bytes: 12
            }
        }
    );
    assert!(batches.next().await.is_none());
    drop(tx);
}

#[tokio::test(start_paused = true)]
async fn a_cancel_with_nothing_held_is_still_marked() {
    let scope = CancelScope::new();
    scope.cancel();
    let out: Vec<_> = batch(
        stream::pending::<u8>(),
        BatchPolicy::new(nz(10), OnCancel::Discard),
        &scope,
    )
    .collect()
    .await;
    assert_eq!(
        out,
        vec![BatchEvent::Cancelled {
            cause: CancelCause::Requested,
            discarded: Discarded::default()
        }]
    );
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_scope_reports_the_timeout() {
    let (tx, rx) = mpsc::channel(8);
    let scope = CancelScope::with_timeout(Duration::from_secs(5));
    let mut batches = batch(
        receiver_stream(rx),
        BatchPolicy::new(nz(10), OnCancel::Discard),
        &scope,
    );
    tx.send(1).await.unwrap();
    assert_eq!(
        within(batches.next()).await.unwrap(),
        BatchEvent::Cancelled {
            cause: CancelCause::TimedOut,
            discarded: Discarded { items: 1, bytes: 0 }
        }
    );
    drop(tx);
}

#[tokio::test(start_paused = true)]
async fn cancellation_wins_over_a_due_deadline() {
    let (tx, rx) = mpsc::channel(8);
    let scope = CancelScope::new();
    let policy = BatchPolicy::new(nz(10), OnCancel::Discard)
        .max_wait(Duration::from_secs(1))
        .unwrap();
    let mut batches = batch(receiver_stream(rx), policy, &scope);
    tx.send(1).await.unwrap();
    assert!(futures::FutureExt::now_or_never(batches.next()).is_none());
    tokio::time::advance(Duration::from_secs(2)).await;
    scope.cancel();
    // Both are due; the declared cancel behaviour applies, not a flush.
    assert!(matches!(
        batches.next().await.unwrap(),
        BatchEvent::Cancelled {
            discarded: Discarded { items: 1, .. },
            ..
        }
    ));
    drop(tx);
}
