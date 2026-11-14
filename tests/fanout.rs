//! Fan-out with per-observer policy: overflow, visible gaps, durable
//! acceptance, answer gating.

mod common;

use std::time::Duration;

use common::{backpressured, builder, emit_all, nz, until_idle, MemorySink, GRACE};
use futures::StreamExt;
use vines::cancel::CancelScope;
use vines::frame::{Coverage, End, Ending, Frame, Gap, GapReason};
use vines::observer::ObserverFailure;
use vines::policy::{Answer, Durability, Loss, ObserverPolicy, Overflow};
use vines::run::{EmitError, Exit, Outcome, Stop};

#[tokio::test(start_paused = true)]
async fn drop_newest_keeps_the_head_and_marks_the_tail_gap() {
    let mut b = builder();
    let obs = b
        .observe("display", ObserverPolicy::new(nz(3), Overflow::DropNewest))
        .unwrap();
    let report = b.spawn(&CancelScope::new(), emit_all(10)).finish().await;
    assert_eq!(report.answer().unwrap(), Outcome::Completed(10));
    let record = obs.into_record().await;
    assert_eq!(record.items, vec![(0, 0), (1, 1), (2, 2)]);
    assert_eq!(
        record.gaps,
        vec![Gap {
            reason: GapReason::DroppedNewest,
            first_seq: 3,
            last_seq: 9
        }]
    );
    let coverage = record.end.clone().unwrap().coverage;
    assert_eq!((coverage.emitted, coverage.lost, coverage.gaps), (10, 7, 1));
    assert_eq!(coverage.received(), 3);
    assert!(
        !record.is_complete(),
        "an incomplete record never reads as complete"
    );
}

#[tokio::test(start_paused = true)]
async fn drop_oldest_keeps_the_tail_and_marks_the_head_gap() {
    let mut b = builder();
    let obs = b
        .observe("display", ObserverPolicy::new(nz(3), Overflow::DropOldest))
        .unwrap();
    b.spawn(&CancelScope::new(), emit_all(10)).finish().await;
    assert_eq!(obs.counts().peak_depth, 3, "a drop frees the slot it fills");
    let frames: Vec<_> = obs.collect().await;
    assert_eq!(
        frames[0],
        Frame::Gap(Gap {
            reason: GapReason::DroppedOldest,
            first_seq: 0,
            last_seq: 6
        }),
        "the gap sits where the dropped items were"
    );
    assert_eq!(frames[1].seq(), Some(7));
    assert_eq!(frames[2].seq(), Some(8));
    assert_eq!(frames[3].seq(), Some(9));
    match &frames[4] {
        Frame::End(end) => assert_eq!((end.coverage.lost, end.coverage.gaps), (7, 1)),
        other => panic!("expected the end marker, got {other:?}"),
    }
    assert_eq!(frames.len(), 5);
}

#[tokio::test(start_paused = true)]
async fn a_late_drop_oldest_observer_keeps_its_drops_apart_from_what_it_missed() {
    let (emitted_tx, emitted_rx) = tokio::sync::oneshot::channel::<()>();
    let (go_tx, go_rx) = tokio::sync::oneshot::channel::<()>();
    let run = builder().spawn(&CancelScope::new(), |mut ctx| async move {
        for item in 0..2 {
            ctx.emit(item).await.unwrap();
        }
        emitted_tx.send(()).unwrap();
        go_rx.await.unwrap();
        for item in 2..5 {
            ctx.emit(item).await.unwrap(); // queue of one: 2 and 3 dropped
        }
        Exit::<(), ()>::Completed(())
    });
    emitted_rx.await.unwrap();
    let late = run
        .observe("late", ObserverPolicy::new(nz(1), Overflow::DropOldest))
        .unwrap();
    go_tx.send(()).unwrap();
    run.finish().await;
    let record = late.into_record().await;
    assert_eq!(
        record.gaps,
        vec![
            Gap {
                reason: GapReason::JoinedLate,
                first_seq: 0,
                last_seq: 1
            },
            Gap {
                reason: GapReason::DroppedOldest,
                first_seq: 2,
                last_seq: 3
            },
        ]
    );
    assert_eq!(record.items, vec![(4, 4)]);
}

#[tokio::test(start_paused = true)]
async fn interleaved_drops_and_reads_give_separate_gaps_in_place() {
    let mut b = builder();
    let mut obs = b
        .observe("display", ObserverPolicy::new(nz(2), Overflow::DropNewest))
        .unwrap();
    let (burst_tx, burst_rx) = tokio::sync::oneshot::channel::<()>();
    let (step_tx, mut step_rx) = tokio::sync::mpsc::channel::<()>(1);
    let run = b.spawn(&CancelScope::new(), move |mut ctx| async move {
        for item in 0..4 {
            ctx.emit(item).await.unwrap(); // 0, 1 queued; 2, 3 dropped
        }
        burst_tx.send(()).unwrap();
        step_rx.recv().await;
        for item in 4..7 {
            ctx.emit(item).await.unwrap(); // after two reads: 4, 5 queued; 6 dropped
        }
        Exit::<(), ()>::Completed(())
    });
    burst_rx.await.unwrap();
    assert_eq!(obs.next().await.unwrap().seq(), Some(0));
    assert_eq!(obs.next().await.unwrap().seq(), Some(1));
    step_tx.send(()).await.unwrap();
    run.finish().await;
    let rest: Vec<_> = obs.collect().await;
    assert_eq!(
        rest[0],
        Frame::Gap(Gap {
            reason: GapReason::DroppedNewest,
            first_seq: 2,
            last_seq: 3
        })
    );
    assert_eq!(rest[1].seq(), Some(4));
    assert_eq!(rest[2].seq(), Some(5));
    assert_eq!(
        rest[3],
        Frame::Gap(Gap {
            reason: GapReason::DroppedNewest,
            first_seq: 6,
            last_seq: 6
        })
    );
    assert!(
        matches!(&rest[4], Frame::End(end) if end.coverage.gaps == 2 && end.coverage.lost == 3)
    );
}

#[tokio::test(start_paused = true)]
async fn a_lossless_observer_that_keeps_up_has_complete_coverage() {
    let mut b = builder();
    let obs = b
        .observe(
            "summary",
            ObserverPolicy::new(nz(100), Overflow::Spill { quota: nz(10) }).loss(Loss::None),
        )
        .unwrap();
    b.spawn(&CancelScope::new(), emit_all(100)).finish().await;
    let record = obs.into_record().await;
    assert!(record.is_complete());
    assert_eq!(record.items.len(), 100);
    assert!(record.items.windows(2).all(|w| w[0].0 + 1 == w[1].0));
}

#[tokio::test(start_paused = true)]
async fn spill_absorbs_a_burst_beyond_the_queue_without_loss() {
    let mut b = builder();
    let obs = b
        .observe(
            "summary",
            ObserverPolicy::new(nz(4), Overflow::Spill { quota: nz(6) }).loss(Loss::None),
        )
        .unwrap();
    b.spawn(&CancelScope::new(), emit_all(10)).finish().await;
    let counts = obs.counts();
    assert_eq!(counts.spilled, 6);
    assert_eq!(counts.peak_depth, 10);
    assert_eq!(counts.delivered, 0, "nothing read yet");
    let record = obs.into_record().await;
    assert!(record.is_complete());
    assert_eq!(record.items.len(), 10);
}

#[tokio::test(start_paused = true)]
async fn an_exhausted_spill_with_loss_allowed_drops_with_a_gap() {
    let mut b = builder();
    let obs = b
        .observe(
            "display",
            ObserverPolicy::new(nz(2), Overflow::Spill { quota: nz(2) }),
        )
        .unwrap();
    let report = b.spawn(&CancelScope::new(), emit_all(7)).finish().await;
    assert!(!report.is_gated());
    assert_eq!(obs.counts().lost, 3);
    let record = obs.into_record().await;
    assert_eq!(record.items.len(), 4);
    assert_eq!(
        record.gaps,
        vec![Gap {
            reason: GapReason::SpillExhausted,
            first_seq: 4,
            last_seq: 6
        }]
    );
    assert_eq!(record.end.unwrap().coverage.failure, None);
}

#[tokio::test(start_paused = true)]
async fn an_exhausted_spill_with_loss_none_fails_the_observer_not_the_run() {
    let mut b = builder();
    let obs = b
        .observe(
            "summary",
            ObserverPolicy::new(nz(2), Overflow::Spill { quota: nz(1) }).loss(Loss::None),
        )
        .unwrap();
    let report = b.spawn(&CancelScope::new(), emit_all(6)).finish().await;
    // answer = none: the run's answer stands.
    assert_eq!(
        report.observer("summary").unwrap().failure,
        Some(ObserverFailure::SpillQuotaExhausted { quota: nz(1) })
    );
    assert_eq!(report.answer().unwrap(), Outcome::Completed(6));
    assert_eq!(
        obs.counts().lost,
        3,
        "the failing item and the two after it"
    );
    let record = obs.into_record().await;
    assert_eq!(record.items.len(), 3);
    assert_eq!(
        record.gaps,
        vec![Gap {
            reason: GapReason::ObserverFailed,
            first_seq: 3,
            last_seq: 5
        }]
    );
    let end = record.end.clone().unwrap();
    assert_eq!(
        end.coverage.failure,
        Some(ObserverFailure::SpillQuotaExhausted { quota: nz(1) })
    );
    assert!(!record.is_complete());
}

#[tokio::test(start_paused = true)]
async fn an_exhausted_spill_on_a_required_observer_gates_the_answer() {
    let mut b = builder();
    let obs = b
        .observe(
            "recorder",
            ObserverPolicy::new(nz(1), Overflow::Spill { quota: nz(1) })
                .loss(Loss::None)
                .answer(Answer::Required),
        )
        .unwrap();
    let report = b.spawn(&CancelScope::new(), emit_all(5)).finish().await;
    assert!(report.is_gated());
    let gated = report.answer().unwrap_err();
    assert_eq!(
        gated.outcome,
        Outcome::Completed(5),
        "the outcome is kept, not answered"
    );
    assert_eq!(gated.failures.len(), 1);
    assert_eq!(gated.failures[0].observer, "recorder");
    assert_eq!(
        gated.failures[0].failure,
        ObserverFailure::SpillQuotaExhausted { quota: nz(1) }
    );
    drop(obs);
}

#[tokio::test(start_paused = true)]
async fn a_required_observer_gates_the_result_until_it_takes_the_end() {
    let mut b = builder();
    let mut obs = b
        .observe(
            "recorder",
            ObserverPolicy::new(nz(8), Overflow::Spill { quota: nz(8) })
                .loss(Loss::None)
                .answer(Answer::Required),
        )
        .unwrap();
    let run = b.spawn(&CancelScope::new(), emit_all(3));
    let finish = tokio::spawn(run.finish());
    until_idle().await;
    assert!(
        !finish.is_finished(),
        "the answer waits for the required observer"
    );
    for expected in 0..3 {
        assert_eq!(obs.next().await.unwrap().seq(), Some(expected));
    }
    until_idle().await;
    assert!(!finish.is_finished(), "items alone do not settle it");
    assert!(matches!(obs.next().await, Some(Frame::End(_))));
    let report = finish.await.unwrap();
    assert_eq!(report.answer().unwrap(), Outcome::Completed(3));
}

#[tokio::test(start_paused = true)]
async fn a_required_observer_that_fails_gates_the_answer() {
    let mut b = builder();
    let mut obs = b
        .observe(
            "check",
            ObserverPolicy::new(nz(8), Overflow::DropOldest).answer(Answer::Required),
        )
        .unwrap();
    let run = b.spawn(&CancelScope::new(), emit_all(3));
    assert_eq!(obs.next().await.unwrap().seq(), Some(0));
    obs.fail("transcript write failed");
    let gated = run.finish().await.answer().unwrap_err();
    assert_eq!(
        gated.failures[0].failure,
        ObserverFailure::Consumer("transcript write failed".into())
    );
}

#[tokio::test(start_paused = true)]
async fn a_required_observer_that_walks_away_gates_the_answer() {
    let mut b = builder();
    let obs = b
        .observe(
            "check",
            ObserverPolicy::new(nz(8), Overflow::DropOldest).answer(Answer::Required),
        )
        .unwrap();
    drop(obs);
    let gated = b
        .spawn(&CancelScope::new(), emit_all(3))
        .finish()
        .await
        .answer()
        .unwrap_err();
    assert_eq!(gated.failures[0].failure, ObserverFailure::Abandoned);
}

#[tokio::test(start_paused = true)]
async fn an_optional_observer_failing_never_fails_the_run() {
    let mut b = builder();
    let failing = b
        .observe("display", ObserverPolicy::new(nz(1), Overflow::DropOldest))
        .unwrap();
    let walked = b
        .observe("activity", ObserverPolicy::new(nz(1), Overflow::DropOldest))
        .unwrap();
    failing.fail("render error");
    drop(walked);
    let report = b.spawn(&CancelScope::new(), emit_all(50)).finish().await;
    assert_eq!(
        report.observer("display").unwrap().settled,
        Some(Err(ObserverFailure::Consumer("render error".into())))
    );
    assert_eq!(
        report.observer("activity").unwrap().settled,
        Some(Err(ObserverFailure::Abandoned))
    );
    assert_eq!(report.observer("activity").unwrap().counts.discarded, 50);
    assert_eq!(report.answer().unwrap(), Outcome::Completed(50));
}

#[tokio::test(start_paused = true)]
async fn an_unsettled_required_observer_is_bounded_by_the_grace_after_cancel() {
    let mut b = builder();
    let obs = b
        .observe(
            "check",
            ObserverPolicy::new(nz(8), Overflow::DropOldest).answer(Answer::Required),
        )
        .unwrap();
    let run = b.spawn(&CancelScope::new(), |ctx| async move {
        ctx.cancelled().await;
        Exit::<(), ()>::Stopped
    });
    run.cancel();
    let started = tokio::time::Instant::now();
    let report = run.finish().await;
    assert_eq!(started.elapsed(), GRACE);
    assert_eq!(report.outcome, Outcome::Cancelled(Stop::Confirmed));
    assert_eq!(report.gate_failures[0].failure, ObserverFailure::Unsettled);
    drop(obs);
}

#[tokio::test(start_paused = true)]
async fn every_observer_sees_its_own_order() {
    let mut b = builder();
    let fast = b
        .observe("fast", ObserverPolicy::new(nz(1000), Overflow::DropNewest))
        .unwrap();
    let slow = b
        .observe("slow", ObserverPolicy::new(nz(7), Overflow::DropOldest))
        .unwrap();
    let spill = b
        .observe(
            "spill",
            ObserverPolicy::new(nz(5), Overflow::Spill { quota: nz(5) }),
        )
        .unwrap();
    b.spawn(&CancelScope::new(), emit_all(500)).finish().await;
    for obs in [fast, slow, spill] {
        let record = obs.into_record().await;
        let seqs: Vec<u64> = record.items.iter().map(|(seq, _)| *seq).collect();
        assert!(seqs.windows(2).all(|w| w[0] < w[1]), "order kept");
        let lost: u64 = record.gaps.iter().map(Gap::count).sum();
        assert_eq!(
            lost + seqs.len() as u64,
            500,
            "every item is delivered or in a gap"
        );
    }
}

// ── stop_producer ───────────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn stop_producer_makes_the_producer_wait_and_loses_nothing() {
    let mut b = backpressured();
    let mut obs = b
        .observe(
            "recorder",
            ObserverPolicy::new(nz(2), Overflow::StopProducer).loss(Loss::None),
        )
        .unwrap();
    let run = b.spawn(&CancelScope::new(), emit_all(20));
    let mut seen = Vec::new();
    while let Some(frame) = obs.next().await {
        assert!(obs.counts().peak_depth <= 2, "never past the queue");
        match frame {
            Frame::Item { seq, .. } => seen.push(seq),
            Frame::End(end) => assert!(end.coverage.is_complete()),
            Frame::Gap(gap) => panic!("no gap expected, got {gap:?}"),
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(seen, (0..20).collect::<Vec<_>>());
    assert_eq!(obs.counts().delivered, 20);
    assert_eq!(run.finish().await.answer().unwrap(), Outcome::Completed(20));
}

#[tokio::test(start_paused = true)]
async fn a_waiting_producer_is_released_by_cancellation() {
    let mut b = backpressured();
    let obs = b
        .observe(
            "recorder",
            ObserverPolicy::new(nz(1), Overflow::StopProducer).loss(Loss::None),
        )
        .unwrap();
    let run = b.spawn(&CancelScope::new(), |mut ctx| async move {
        ctx.emit(1).await.unwrap();
        // The queue is full and nobody reads: this waits until cancelled.
        match ctx.emit(2).await {
            Err(EmitError::Cancelled(item)) => assert_eq!(item, 2),
            other => panic!("expected cancellation, got {other:?}"),
        }
        Exit::<(), ()>::Stopped
    });
    // Parked in the second emit, waiting for room.
    until_idle().await;
    run.cancel();
    let report = run.finish().await;
    assert_eq!(report.outcome, Outcome::Cancelled(Stop::Confirmed));
    assert_eq!(report.emitted, 1, "the refused item was never emitted");
    drop(obs);
}

#[tokio::test(start_paused = true)]
async fn a_stop_producer_observer_that_walks_away_releases_the_producer() {
    let mut b = backpressured();
    let obs = b
        .observe(
            "recorder",
            ObserverPolicy::new(nz(1), Overflow::StopProducer)
                .loss(Loss::None)
                .answer(Answer::Required),
        )
        .unwrap();
    let run = b.spawn(&CancelScope::new(), emit_all(5));
    // Parked in the second emit: the queue of one is full.
    until_idle().await;
    drop(obs);
    let report = run.finish().await;
    assert_eq!(report.emitted, 5);
    assert_eq!(report.observer("recorder").unwrap().counts.discarded, 4);
    let gated = report.answer().unwrap_err();
    assert_eq!(gated.outcome, Outcome::Completed(5));
    assert_eq!(gated.failures[0].failure, ObserverFailure::Abandoned);
}

#[tokio::test(start_paused = true)]
async fn on_a_backpressure_source_a_lossy_observer_still_never_waits() {
    let mut b = backpressured();
    let obs = b
        .observe("display", ObserverPolicy::new(nz(1), Overflow::DropNewest))
        .unwrap();
    let report = b.spawn(&CancelScope::new(), emit_all(100)).finish().await;
    assert_eq!(report.answer().unwrap(), Outcome::Completed(100));
    assert_eq!(obs.counts().lost, 99);
}

// ── durable ─────────────────────────────────────────────────────────────

fn durable(queue: usize) -> ObserverPolicy {
    ObserverPolicy::new(nz(queue), Overflow::Spill { quota: nz(64) })
        .durability(Durability::Durable)
        .loss(Loss::None)
}

#[tokio::test(start_paused = true)]
async fn a_durable_observer_counts_a_frame_only_when_the_sink_acks() {
    let sink = MemorySink::<u32>::new();
    let mut slow = sink.clone();
    slow.delay = Some(Duration::from_secs(1));
    let mut b = builder();
    let handle = b.observe_durable("transcript", durable(8), slow).unwrap();
    assert_eq!(handle.name(), "transcript");
    let run = b.spawn(&CancelScope::new(), emit_all(3));
    // answer = none: the result does not wait for the slow sink.
    let report = run.finish().await;
    let early = report.observer("transcript").unwrap().clone();
    assert_eq!(report.answer().unwrap(), Outcome::Completed(3));
    assert_eq!(early.counts.acknowledged, 0, "nothing is acknowledged yet");
    assert_eq!(early.settled, None, "still draining after the answer");

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(handle.counts().acknowledged, 1);

    let finished = handle.finished().await;
    assert_eq!(finished.settled, Some(Ok(())));
    assert_eq!(finished.counts.acknowledged, 3);
    let frames = sink.frames();
    assert_eq!(frames.len(), 4, "three items and the end marker");
    assert!(matches!(&frames[3], Frame::End(end) if end.coverage.is_complete()));
}

#[tokio::test(start_paused = true)]
async fn a_required_durable_observer_gates_the_result_on_the_end_ack() {
    let sink = MemorySink::<u32>::new();
    let mut slow = sink.clone();
    slow.delay = Some(Duration::from_secs(1));
    let mut b = builder();
    b.observe_durable("transcript", durable(8).answer(Answer::Required), slow)
        .unwrap();
    let started = tokio::time::Instant::now();
    let report = b.spawn(&CancelScope::new(), emit_all(3)).finish().await;
    assert_eq!(
        started.elapsed(),
        Duration::from_secs(4),
        "three items and the end, acked"
    );
    assert_eq!(report.answer().unwrap(), Outcome::Completed(3));
    assert_eq!(sink.frames().len(), 4);
}

#[tokio::test(start_paused = true)]
async fn a_durable_sink_receives_gap_markers() {
    let sink = MemorySink::<u32>::new();
    let mut slow = sink.clone();
    slow.delay = Some(Duration::from_secs(1));
    let mut b = builder();
    let handle = b
        .observe_durable(
            "transcript",
            ObserverPolicy::new(nz(2), Overflow::DropNewest).durability(Durability::Durable),
            slow,
        )
        .unwrap();
    b.spawn(&CancelScope::new(), emit_all(10)).finish().await;
    handle.finished().await;
    // The producer emits all ten before the slow sink takes the first.
    assert_eq!(
        sink.frames(),
        vec![
            Frame::Item { seq: 0, item: 0 },
            Frame::Item { seq: 1, item: 1 },
            Frame::Gap(Gap {
                reason: GapReason::DroppedNewest,
                first_seq: 2,
                last_seq: 9
            }),
            Frame::End(End {
                ending: Ending::Completed,
                coverage: Coverage {
                    emitted: 10,
                    lost: 8,
                    gaps: 1,
                    failure: None
                }
            }),
        ],
        "the durable record says what it lacks"
    );
}

#[tokio::test(start_paused = true)]
async fn a_rejecting_sink_fails_an_optional_observer_without_failing_the_run() {
    let mut sink = MemorySink::<u32>::new();
    sink.fail_at = Some(1);
    let mut b = builder();
    let handle = b
        .observe_durable("transcript", durable(8), sink.clone())
        .unwrap();
    let report = b.spawn(&CancelScope::new(), emit_all(5)).finish().await;
    assert_eq!(report.answer().unwrap(), Outcome::Completed(5));
    let finished = handle.finished().await;
    assert!(matches!(
        finished.settled,
        Some(Err(ObserverFailure::Sink(ref error))) if error.message() == "disk full at frame 1"
    ));
    assert_eq!(finished.counts.acknowledged, 1);
    assert_eq!(
        sink.frames().len(),
        1,
        "nothing is offered after a rejection"
    );
}

#[tokio::test(start_paused = true)]
async fn a_rejecting_sink_on_a_required_observer_gates_the_answer() {
    let mut sink = MemorySink::<u32>::new();
    sink.fail_at = Some(0);
    let mut b = builder();
    b.observe_durable("transcript", durable(8).answer(Answer::Required), sink)
        .unwrap();
    let gated = b
        .spawn(&CancelScope::new(), emit_all(5))
        .finish()
        .await
        .answer()
        .unwrap_err();
    assert!(matches!(
        gated.failures[0].failure,
        ObserverFailure::Sink(_)
    ));
}

#[tokio::test(start_paused = true)]
async fn a_panicking_sink_settles_instead_of_hanging_the_gate() {
    let mut sink = MemorySink::<u32>::new();
    sink.panic_at = Some(0);
    let mut b = builder();
    b.observe_durable("transcript", durable(8).answer(Answer::Required), sink)
        .unwrap();
    let gated = b
        .spawn(&CancelScope::new(), emit_all(2))
        .finish()
        .await
        .answer()
        .unwrap_err();
    assert_eq!(gated.failures[0].failure, ObserverFailure::Panicked);
}

#[tokio::test(start_paused = true)]
async fn a_late_durable_observer_starts_draining_at_once() {
    let sink = MemorySink::<u32>::new();
    let (emitted_tx, emitted_rx) = tokio::sync::oneshot::channel::<()>();
    let (go_tx, go_rx) = tokio::sync::oneshot::channel::<()>();
    let run = builder().spawn(&CancelScope::new(), |mut ctx| async move {
        ctx.emit(0).await.unwrap();
        emitted_tx.send(()).unwrap();
        go_rx.await.unwrap();
        ctx.emit(1).await.unwrap();
        Exit::<(), ()>::Completed(())
    });
    emitted_rx.await.unwrap();
    let handle = run
        .observe_durable(
            "transcript",
            ObserverPolicy::new(nz(4), Overflow::DropOldest).durability(Durability::Durable),
            sink.clone(),
        )
        .unwrap();
    go_tx.send(()).unwrap();
    run.finish().await;
    assert_eq!(handle.finished().await.settled, Some(Ok(())));
    let frames = sink.frames();
    assert!(matches!(
        frames[0],
        Frame::Gap(Gap {
            reason: GapReason::JoinedLate,
            ..
        })
    ));
    assert_eq!(frames[1].seq(), Some(1));
    assert!(matches!(&frames[2], Frame::End(end) if end.ending == Ending::Completed));
}

#[tokio::test(start_paused = true)]
async fn a_durable_drain_dropped_unstarted_settles_as_dropped() {
    let mut b = builder();
    let handle = b
        .observe_durable("transcript", durable(4), MemorySink::<u32>::new())
        .unwrap();
    drop(b);
    assert_eq!(
        handle.finished().await.settled,
        Some(Err(ObserverFailure::Dropped))
    );
}
