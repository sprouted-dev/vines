//! map, filter, tap policies and bounded parallel map.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::nz;
use futures::{stream, StreamExt};
use tokio::time::Instant;
use vines::ops::{filter, map, par_map, tap, tap_required, OutputOrder, TapFailed, TapIsolation};

#[tokio::test]
async fn map_and_filter_compose() {
    let out: Vec<_> = filter(map(stream::iter(1..=6), |n| n * 10), |n| n % 20 == 0)
        .collect()
        .await;
    assert_eq!(out, vec![20, 40, 60]);
}

#[tokio::test]
async fn an_isolated_tap_failure_never_fails_the_stream() {
    let (tapped, stats) = tap(stream::iter(1..=5), TapIsolation::KeepCalling, |n: &i32| {
        if n % 2 == 0 {
            Err(format!("cannot record {n}"))
        } else {
            Ok(())
        }
    });
    let out: Vec<_> = tapped.collect().await;
    assert_eq!(out, vec![1, 2, 3, 4, 5], "every item passes");
    let snapshot = stats.snapshot();
    assert_eq!(snapshot.calls, 5);
    assert_eq!(snapshot.failures, 2);
    assert_eq!(snapshot.skipped, 0);
    assert_eq!(snapshot.first_failure.as_deref(), Some("cannot record 2"));
    assert!(!snapshot.detached);
}

#[tokio::test]
async fn a_detaching_tap_stops_being_called_after_its_first_failure() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let (tapped, stats) = tap(
        stream::iter(1..=5),
        TapIsolation::DetachOnFirstFailure,
        move |n: &i32| {
            counted.fetch_add(1, Ordering::SeqCst);
            if *n == 2 {
                Err("closed")
            } else {
                Ok(())
            }
        },
    );
    let out: Vec<_> = tapped.collect().await;
    assert_eq!(out, vec![1, 2, 3, 4, 5]);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let snapshot = stats.snapshot();
    assert_eq!(
        (snapshot.calls, snapshot.failures, snapshot.skipped),
        (2, 1, 3)
    );
    assert!(snapshot.detached);
}

#[tokio::test]
async fn a_required_tap_failure_fails_the_stream_and_stops_pulling() {
    let pulled = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&pulled);
    let source = stream::iter(1..=5).inspect(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    let out: Vec<_> = tap_required(
        source,
        |n: &i32| if *n == 3 { Err("write failed") } else { Ok(()) },
    )
    .collect()
    .await;
    assert_eq!(
        out,
        vec![
            Ok(1),
            Ok(2),
            Err(TapFailed {
                index: 2,
                item: 3,
                error: "write failed"
            })
        ]
    );
    assert_eq!(
        pulled.load(Ordering::SeqCst),
        3,
        "nothing pulled after the failure"
    );
}

#[tokio::test]
async fn a_required_tap_that_never_fails_passes_everything() {
    let out: Vec<Result<i32, TapFailed<i32, ()>>> =
        tap_required(stream::iter(1..=3), |_: &i32| Ok(()))
            .collect()
            .await;
    assert_eq!(out, vec![Ok(1), Ok(2), Ok(3)]);
}

fn timed(
    active: &Arc<AtomicUsize>,
    peak: &Arc<AtomicUsize>,
) -> impl FnMut(u64) -> futures::future::BoxFuture<'static, u64> {
    let active = Arc::clone(active);
    let peak = Arc::clone(peak);
    move |delay| {
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        Box::pin(async move {
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(delay)).await;
            active.fetch_sub(1, Ordering::SeqCst);
            delay
        })
    }
}

#[tokio::test(start_paused = true)]
async fn ordered_par_map_keeps_input_order_within_its_limit() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let out: Vec<_> = par_map(
        stream::iter([40, 10, 30, 20, 5]),
        nz(2),
        OutputOrder::Ordered,
        timed(&active, &peak),
    )
    .collect()
    .await;
    assert_eq!(out, vec![40, 10, 30, 20, 5]);
    assert_eq!(peak.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn unordered_par_map_yields_in_completion_order_within_its_limit() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let out: Vec<_> = par_map(
        stream::iter([40, 10, 20]),
        nz(2),
        OutputOrder::Unordered,
        timed(&active, &peak),
    )
    .collect()
    .await;
    assert_eq!(out, vec![10, 20, 40]);
    assert_eq!(peak.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn a_par_map_limit_of_one_is_sequential() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let out: Vec<_> = par_map(
        stream::iter([30, 10, 20]),
        nz(1),
        OutputOrder::Unordered,
        timed(&active, &peak),
    )
    .collect()
    .await;
    assert_eq!(out, vec![30, 10, 20]);
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert_eq!(start.elapsed(), Duration::from_millis(60));
}
