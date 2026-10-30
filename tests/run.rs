//! The operator shape: one producer, live progress, exactly one result;
//! cancellation requested vs confirmed.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::{nz, settle, GRACE};
use futures::{FutureExt, StreamExt};
use vines::cancel::{CancelCause, CancelScope};
use vines::frame::{Ending, Frame, GapReason};
use vines::policy::{ObserverPolicy, Overflow, SourceDecl};
use vines::run::{Crash, EmitError, Exit, Outcome, RunBuilder, Stop};
use vines::PolicyError;

fn builder() -> RunBuilder<u32> {
    RunBuilder::new(SourceDecl::without_backpressure(), GRACE).unwrap()
}

fn lossy(queue: usize) -> ObserverPolicy {
    ObserverPolicy::new(nz(queue), Overflow::DropNewest)
}

#[tokio::test(start_paused = true)]
async fn a_run_streams_progress_in_order_then_one_result() {
    let mut b = builder();
    let display = b.observe("display", lossy(16)).unwrap();
    let run = b.spawn(&CancelScope::new(), |mut ctx| async move {
        for n in 0..5 {
            ctx.emit(n).await.unwrap();
        }
        Exit::<_, ()>::Completed("ok")
    });
    let record = display.into_record().await;
    assert_eq!(
        record.items,
        vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)],
        "per-stream order is emission order"
    );
    let end = record.end.clone().unwrap();
    assert_eq!(end.ending, Ending::Completed);
    assert_eq!(end.coverage.emitted, 5);
    assert!(record.is_complete());

    let report = run.finish().await;
    assert_eq!(report.emitted, 5);
    assert_eq!(report.cancel_requested, None);
    assert!(report.straggler.is_none());
    assert_eq!(report.answer().unwrap(), Outcome::Completed("ok"));
}

#[tokio::test(start_paused = true)]
async fn a_full_progress_queue_never_holds_back_the_result() {
    let mut b = builder();
    // Never read: a queue of one, full after the first item.
    let stalled = b.observe("stalled", lossy(1)).unwrap();
    let run = b.spawn(&CancelScope::new(), |mut ctx| async move {
        for n in 0..10_000 {
            // On a source without backpressure, emit is ready at once.
            assert!(ctx.emit(n).now_or_never().is_some(), "emit waited");
        }
        Exit::<_, ()>::Completed(ctx.emitted())
    });
    let report = run.finish().await;
    assert_eq!(report.answer().unwrap(), Outcome::Completed(10_000));
    let counts = stalled.counts();
    assert_eq!(counts.offered, 10_000);
    assert_eq!(counts.lost, 9_999);
    assert_eq!(counts.peak_depth, 1);
}

#[tokio::test(start_paused = true)]
async fn adding_observers_never_relaunches_the_producer() {
    let launches = Arc::new(AtomicUsize::new(0));
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
    let mut b = builder();
    let first = b.observe("first", lossy(64)).unwrap();
    let second = b.observe("second", lossy(64)).unwrap();
    let counted = Arc::clone(&launches);
    let run = b.spawn(&CancelScope::new(), move |mut ctx| async move {
        counted.fetch_add(1, Ordering::SeqCst);
        ctx.emit(1).await.unwrap();
        ctx.emit(2).await.unwrap();
        gate_rx.await.unwrap();
        ctx.emit(3).await.unwrap();
        Exit::<(), ()>::Completed(())
    });
    settle().await;
    let late = run.observe("late", lossy(64)).unwrap();
    gate_tx.send(()).unwrap();

    let first = first.into_record().await;
    let second = second.into_record().await;
    let late = late.into_record().await;
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    assert_eq!(first.items, second.items);
    assert_eq!(first.items.len(), 3);
    // The late observer sees what it missed as a gap, not as silence.
    assert_eq!(late.items, vec![(2, 3)]);
    assert_eq!(late.gaps.len(), 1);
    assert_eq!(late.gaps[0].reason, GapReason::JoinedLate);
    assert_eq!((late.gaps[0].first_seq, late.gaps[0].last_seq), (0, 1));
    assert!(!late.is_complete());
    assert_eq!(late.end.unwrap().coverage.lost, 2);
    run.finish().await;
}

#[tokio::test(start_paused = true)]
async fn a_late_required_observer_is_refused() {
    let (hold_tx, hold_rx) = tokio::sync::oneshot::channel::<()>();
    let run = builder().spawn(&CancelScope::new(), |_ctx| async move {
        let _ = hold_rx.await;
        Exit::<(), ()>::Completed(())
    });
    let error = run
        .observe("late", lossy(4).answer(vines::policy::Answer::Required))
        .err()
        .unwrap();
    assert_eq!(
        error,
        PolicyError::LateRequiredObserver {
            observer: "late".into()
        }
    );
    drop(hold_tx);
    run.finish().await;
}

#[tokio::test(start_paused = true)]
async fn observing_an_ended_run_is_refused() {
    let run = builder().spawn(&CancelScope::new(), |_ctx| async {
        Exit::<(), ()>::Completed(())
    });
    settle().await;
    let error = run.observe("late", lossy(4)).err().unwrap();
    assert_eq!(
        error,
        PolicyError::RunEnded {
            observer: "late".into()
        }
    );
    run.finish().await;
}

#[tokio::test(start_paused = true)]
async fn duplicate_observer_names_are_refused() {
    let mut b = builder();
    b.observe("display", lossy(4)).unwrap();
    let error = b.observe("display", lossy(4)).err().unwrap();
    assert_eq!(
        error,
        PolicyError::DuplicateObserver {
            observer: "display".into()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_zero_stop_grace_is_refused() {
    let error = RunBuilder::<u32>::new(SourceDecl::without_backpressure(), Duration::ZERO)
        .err()
        .unwrap();
    assert!(matches!(
        error,
        PolicyError::ZeroDuration {
            field: "stop_grace",
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn a_failing_producer_fails_the_outcome() {
    let mut b = builder();
    let display = b.observe("display", lossy(4)).unwrap();
    let run = b.spawn(&CancelScope::new(), |_ctx| async {
        Exit::<(), _>::Failed("provider error")
    });
    let report = run.finish().await;
    assert_eq!(report.outcome, Outcome::Failed("provider error"));
    assert_eq!(
        display.into_record().await.end.unwrap().ending,
        Ending::Failed
    );
}

#[tokio::test(start_paused = true)]
async fn a_panicking_producer_still_yields_one_result() {
    let mut b = builder();
    let display = b.observe("display", lossy(4)).unwrap();
    let run = b.spawn(&CancelScope::new(), |mut ctx| async move {
        ctx.emit(1).await.unwrap();
        if ctx.emitted() == 1 {
            panic!("agent adapter bug");
        }
        Exit::<(), ()>::Completed(())
    });
    let report = run.finish().await;
    assert_eq!(
        report.outcome,
        Outcome::Crashed(Crash::Panicked("agent adapter bug".into()))
    );
    let record = display.into_record().await;
    assert_eq!(record.items, vec![(0, 1)]);
    assert_eq!(record.end.unwrap().ending, Ending::Crashed);
}

#[tokio::test(start_paused = true)]
async fn stopping_unasked_is_a_contract_break() {
    let run = builder().spawn(&CancelScope::new(), |_ctx| async {
        Exit::<(), ()>::Stopped
    });
    assert_eq!(
        run.finish().await.outcome,
        Outcome::Crashed(Crash::StoppedUnrequested)
    );
}

#[tokio::test(start_paused = true)]
async fn a_cancel_the_producer_confirms_is_a_confirmed_stop() {
    let mut b = builder();
    let display = b.observe("display", lossy(4)).unwrap();
    let run = b.spawn(&CancelScope::new(), |ctx| async move {
        ctx.cancelled().await;
        Exit::<(), ()>::Stopped
    });
    run.cancel();
    let report = run.finish().await;
    assert_eq!(report.outcome, Outcome::Cancelled(Stop::Confirmed));
    assert_eq!(report.cancel_requested, Some(CancelCause::Requested));
    assert!(report.straggler.is_none());
    assert_eq!(
        display.into_record().await.end.unwrap().ending,
        Ending::Cancelled
    );
}

#[tokio::test(start_paused = true)]
async fn a_cancel_the_producer_ignores_is_never_claimed_as_a_stop() {
    let (effect_tx, effect_rx) = tokio::sync::oneshot::channel::<()>();
    let run = builder().spawn(&CancelScope::new(), |mut ctx| async move {
        // Ignores cancellation; finishes its effect later.
        let _ = effect_rx.await;
        let late = ctx.emit(7).await;
        assert_eq!(late, Err(EmitError::Ended(7)), "the run already ended");
        Exit::<_, ()>::Completed("effect happened")
    });
    run.cancel();
    let started = tokio::time::Instant::now();
    let report = run.finish().await;
    assert_eq!(started.elapsed(), GRACE, "waited exactly the grace");
    assert_eq!(report.outcome, Outcome::Cancelled(Stop::Unconfirmed));
    let straggler = report.straggler.expect("the producer is still running");
    assert!(!straggler.is_finished());
    effect_tx.send(()).unwrap();
    // The cancel did not prevent the effect.
    assert_eq!(
        straggler.exit().await,
        Ok(Exit::Completed("effect happened"))
    );
}

#[tokio::test(start_paused = true)]
async fn a_straggler_can_be_aborted_without_claiming_a_stop() {
    let run = builder().spawn(&CancelScope::new(), |_ctx| async {
        futures::future::pending::<()>().await;
        Exit::<(), ()>::Completed(())
    });
    run.cancel();
    let report = run.finish().await;
    assert_eq!(report.outcome, Outcome::Cancelled(Stop::Unconfirmed));
    report.straggler.unwrap().abort();
}

#[tokio::test(start_paused = true)]
async fn a_deadline_the_producer_confirms_is_a_confirmed_timeout() {
    let scope = CancelScope::with_timeout(Duration::from_secs(30));
    let run = builder().spawn(&scope, |ctx| async move {
        ctx.cancelled().await;
        Exit::<(), ()>::Stopped
    });
    let report = run.finish().await;
    assert_eq!(report.outcome, Outcome::TimedOut(Stop::Confirmed));
    assert_eq!(report.cancel_requested, Some(CancelCause::TimedOut));
}

#[tokio::test(start_paused = true)]
async fn a_deadline_the_producer_ignores_is_an_unconfirmed_timeout() {
    let scope = CancelScope::with_timeout(Duration::from_secs(30));
    let run = builder().spawn(&scope, |_ctx| async {
        futures::future::pending::<()>().await;
        Exit::<(), ()>::Completed(())
    });
    let started = tokio::time::Instant::now();
    let report = run.finish().await;
    assert_eq!(started.elapsed(), Duration::from_secs(30) + GRACE);
    assert_eq!(report.outcome, Outcome::TimedOut(Stop::Unconfirmed));
    assert!(report.straggler.is_some());
}

#[tokio::test(start_paused = true)]
async fn a_success_after_a_cancel_request_stays_a_success() {
    let run = builder().spawn(&CancelScope::new(), |ctx| async move {
        ctx.cancelled().await;
        // The effect was already committed: report it.
        Exit::<_, ()>::Completed("committed")
    });
    run.cancel();
    let report = run.finish().await;
    assert_eq!(report.outcome, Outcome::Completed("committed"));
    assert_eq!(report.cancel_requested, Some(CancelCause::Requested));
}

#[tokio::test(start_paused = true)]
async fn cancelling_the_parent_scope_cancels_the_run() {
    let parent = CancelScope::new();
    let run = builder().spawn(&parent, |ctx| async move {
        ctx.cancelled().await;
        Exit::<(), ()>::Stopped
    });
    parent.cancel();
    assert_eq!(
        run.finish().await.outcome,
        Outcome::Cancelled(Stop::Confirmed)
    );
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_run_leaves_its_parent_scope_alone() {
    let parent = CancelScope::new();
    let run = builder().spawn(&parent, |ctx| async move {
        ctx.cancelled().await;
        Exit::<(), ()>::Stopped
    });
    run.cancel();
    run.finish().await;
    assert!(!parent.is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn dropping_the_run_handle_requests_cancellation() {
    let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
    let run = builder().spawn(&CancelScope::new(), |ctx| async move {
        let cause = ctx.cancelled().await;
        let _ = seen_tx.send(cause);
        Exit::<(), ()>::Stopped
    });
    drop(run);
    assert_eq!(seen_rx.await.unwrap(), CancelCause::Requested);
}

#[tokio::test(start_paused = true)]
async fn a_never_spawned_builder_closes_its_observers() {
    let mut b = builder();
    let mut display = b.observe("display", lossy(4)).unwrap();
    drop(b);
    match display.next().await {
        Some(Frame::End(end)) => assert_eq!(end.ending, Ending::NotStarted),
        other => panic!("expected an end marker, got {other:?}"),
    }
    assert!(display.next().await.is_none());
}
