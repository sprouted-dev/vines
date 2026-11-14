//! Running a vine over an endless stream from a saved position: three
//! real vines killed at every point and restarted from what the caller
//! saved, the failures, and the vines the checker refuses. Rust alone:
//! this file passes with YAML off.

mod acts;

use std::sync::Arc;
use std::time::Duration;

use acts::agent::{job_run_pipe, job_run_registry, Host};
use acts::feed::{
    event_backup_pipe, event_pages_pipe, event_relay_caught_pipe, event_relay_pipe, events,
    feed_registry,
};
use acts::restart::{assert_forward, events_in, run, run_with_kills, started, Caller, End};
use acts::World;
use futures::{stream, StreamExt};
use vine_lang::pipe::{
    call, collect, each, emit, emit_with, finalize, fork, last, pipe, split, take, tap, timeout,
    use_vine, Overlap, Pipe, PipeDef, Stage,
};
use vine_lang::{
    json, Bookmark, Entry, Event, Journal, Line, OperatorSpec, Progress, RunError, Shape, Ty, Value,
};

type Build = fn(&Arc<World>) -> Pipe;

fn relay(world: &Arc<World>) -> Pipe {
    feed_registry(world).build_pipe(event_relay_pipe()).unwrap()
}

fn relay_caught(world: &Arc<World>) -> Pipe {
    feed_registry(world)
        .build_pipe(event_relay_caught_pipe())
        .unwrap()
}

fn backup(world: &Arc<World>) -> Pipe {
    feed_registry(world)
        .build_pipe(event_backup_pipe())
        .unwrap()
}

fn pages(world: &Arc<World>) -> Pipe {
    feed_registry(world).build_pipe(event_pages_pipe()).unwrap()
}

/// Each event archived, and every third one backed up in a batch from a
/// side pipe: the batch ties its three events together.
fn archive_and_batch(world: &Arc<World>) -> Pipe {
    let def = pipe("archive_and_batch")
        .input(
            Shape::new()
                .required("seq", Ty::Int)
                .required("body", Ty::Text),
        )
        .output("archived", Shape::open())
        .output("stored", Shape::open())
        .stages([
            fork([vec![
                collect(3),
                call("backup.store", json!({ "items": "$item.items" })).into("stored"),
                emit_with("stored", json!({ "last": "$item.stored.last" })),
            ]]),
            call(
                "archive.put",
                json!({ "seq": "$item.seq", "body": "$item.body" }),
            )
            .into("archived"),
            emit_with("archived", json!({ "seq": "$item.seq" })),
        ]);
    feed_registry(world).build_pipe(def).unwrap()
}

/// Events for `event_pages`: each has 3, 0, 2 or 4 pages.
fn paged(first: u64, last: u64) -> Vec<Value> {
    (first..=last)
        .map(|n| {
            let pages = [3, 0, 2, 4][usize::try_from(n % 4).unwrap()];
            json!({ "seq": n, "pages": pages })
        })
        .collect()
}

/// The input item an output belongs to.
fn item_of(name: &str, payload: &Value) -> u64 {
    let field = if name == "stored" { "last" } else { "seq" };
    payload[field].as_u64().unwrap()
}

/// What an uninterrupted run gives: its outputs, the acts it called, the
/// positions it reported, and how many events it gave.
async fn uninterrupted(build: Build, input: &[Value]) -> (Caller, Vec<String>, usize) {
    let world = World::new();
    let mut caller = Caller::new();
    assert!(matches!(
        run(&build(&world), input, &mut caller, None).await,
        End::Ended
    ));
    let events = events_in(&build(&World::new()), input).await;
    (caller, world.log(), events)
}

/// Kill `build`'s vine after each count in `kills`, then run it to the
/// end, and check it did what an uninterrupted run does: each act once,
/// each output once and in order, the position only forward, and the
/// bookmark no bigger than `bound` lines.
async fn check_kills(build: Build, input: &[Value], kills: &[usize], bound: usize) {
    let (want, want_log, _) = uninterrupted(build, input).await;
    let world = World::new();
    let mut caller = Caller::new();
    let repeats = run_with_kills(|| build(&world), input, &mut caller, kills).await;
    assert_eq!(caller.outs, want.outs, "kills {kills:?}");
    assert_eq!(
        started(world.log()),
        started(want_log.clone()),
        "kills {kills:?}"
    );
    assert_forward(&caller.throughs);
    assert_eq!(caller.position(), input.len() as u64, "kills {kills:?}");
    assert!(caller.bookmark().is_empty(), "kills {kills:?}");
    assert!(
        caller.most <= bound,
        "kills {kills:?}: {} lines",
        caller.most
    );
    assert!(repeats <= kills.len());
}

/// Every single kill point, and every pair of kill points in a row.
async fn check_every_kill(build: Build, input: &[Value], bound: usize) {
    let (_, _, events) = uninterrupted(build, input).await;
    for k in 1..events {
        check_kills(build, input, &[k], bound).await;
    }
    for k1 in 1..events {
        for k2 in 1..events {
            check_kills(build, input, &[k1, k2], bound).await;
        }
    }
}

// ------------------------------------------------------------ uninterrupted

#[tokio::test]
async fn a_per_item_vine_moves_the_position_past_each_event_once_its_output_is_handed_out() {
    let input = events(1, 6);
    let (caller, log, _) = uninterrupted(relay, &input).await;
    assert_eq!(caller.throughs, [1, 2, 3, 4, 5, 6]);
    assert_eq!(caller.outs.len(), 6);
    assert_eq!(
        log,
        (1..=6)
            .map(|n| format!("archive.put {n}"))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn each_output_comes_before_its_line_and_its_line_before_the_position() {
    let world = World::new();
    let got: Vec<Progress> = relay(&world)
        .run_from(stream::iter(events(1, 1)), Bookmark::new())
        .map(Result::unwrap)
        .collect()
        .await;
    assert!(
        matches!(
            &got[..],
            [
                Progress::Save(Line {
                    item: 1,
                    entry: Entry::Done { .. }
                }),
                Progress::Output(_),
                Progress::Save(Line {
                    item: 1,
                    entry: Entry::Emitted { .. }
                }),
                Progress::Through(1),
            ]
        ),
        "{got:?}"
    );
}

#[tokio::test]
async fn collect_keeps_the_position_before_the_events_it_holds() {
    let (caller, log, _) = uninterrupted(backup, &events(1, 12)).await;
    // Two full batches, then what is left when the input ends.
    assert_eq!(caller.throughs, [5, 10, 12]);
    assert_eq!(
        log,
        [
            "backup.store 1-5",
            "backup.store 6-10",
            "backup.store 11-12"
        ]
    );
}

#[tokio::test]
async fn a_batch_in_a_side_pipe_ties_its_events_together_so_the_position_moves_by_batches() {
    let (caller, _, _) = uninterrupted(archive_and_batch, &events(1, 7)).await;
    // Each event is archived on its own, but the position stops only where
    // a batch ends, so a restart never starts partway through a batch.
    assert_eq!(caller.throughs, [3, 6, 7]);
    assert_eq!(caller.outs.len(), 7 + 3);
}

#[tokio::test]
async fn the_position_waits_for_a_stream_act_to_end_and_never_passes_an_item_still_giving() {
    let world = World::new();
    let mut position = 0;
    let mut run = pages(&world).run_from(stream::iter(paged(1, 8)), Bookmark::new());
    while let Some(progress) = run.next().await {
        match progress.unwrap() {
            Progress::Output(o) => assert!(
                item_of(&o.name, &o.payload) > position,
                "{} of item {} came after the position {position}",
                o.name,
                o.payload["seq"]
            ),
            Progress::Through(n) => position = n,
            Progress::Done(n) => {
                panic!("item {n} finished out of order with no `each` that merges")
            }
            Progress::Save(_) => {}
        }
    }
    assert_eq!(position, 8);
}

#[tokio::test]
async fn no_output_is_handed_out_for_an_item_the_position_has_passed() {
    for build in [relay as Build, backup, archive_and_batch] {
        let world = World::new();
        let mut position = 0;
        let mut run = build(&world).run_from(stream::iter(events(1, 13)), Bookmark::new());
        while let Some(progress) = run.next().await {
            match progress.unwrap() {
                Progress::Output(o) => assert!(item_of(&o.name, &o.payload) > position),
                Progress::Through(n) => position = n,
                Progress::Done(n) => panic!("item {n} finished out of order"),
                Progress::Save(_) => {}
            }
        }
        assert_eq!(position, 13);
    }
}

// -------------------------------------------------------------- kill points

#[tokio::test]
async fn a_per_item_vine_killed_anywhere_once_or_twice_acts_and_hands_out_once() {
    check_every_kill(relay, &events(1, 8), 2).await;
}

#[tokio::test]
async fn a_batching_vine_killed_anywhere_once_or_twice_stores_each_batch_once() {
    check_every_kill(backup, &events(1, 12), 2).await;
}

#[tokio::test]
async fn a_vine_with_a_side_batch_killed_anywhere_once_or_twice_acts_once() {
    // A batch of 3 has 3 archives and one store in flight at most.
    check_every_kill(archive_and_batch, &events(1, 7), 8).await;
}

#[tokio::test]
async fn a_stream_act_vine_killed_anywhere_once_or_twice_gives_each_page_once() {
    // The most pages one event has is 4: its start, 4 pages, its end, 5
    // handed outputs, and the start of the next event.
    check_every_kill(pages, &paged(1, 5), 12).await;
}

#[tokio::test]
async fn a_restart_mid_stream_asks_the_act_to_go_on_after_the_pages_it_saved() {
    // One event with 4 pages, after a position of 2.
    let input = [events(1, 2), vec![json!({ "seq": 3, "pages": 4 })]].concat();
    // Kill right after the line that records the second page as handed.
    let probe: Vec<Progress> = pages(&World::new())
        .run_from(stream::iter(input[2..].to_vec()), Bookmark::at(2))
        .map(Result::unwrap)
        .collect()
        .await;
    let emitted = |p: &Progress| {
        matches!(
            p,
            Progress::Save(Line {
                entry: Entry::Emitted { .. },
                ..
            })
        )
    };
    let kill = probe
        .iter()
        .enumerate()
        .filter(|(_, p)| emitted(p))
        .nth(1)
        .map(|(i, _)| i + 1);

    let world = World::new();
    let mut caller = Caller::from(Bookmark::at(2));
    run(&pages(&world), &input, &mut caller, kill).await;
    assert_eq!(caller.position(), 2, "the position waits for event 3");
    run(&pages(&world), &input, &mut caller, None).await;
    assert_eq!(
        world.log(),
        ["feed.pages 3 from 0", "feed.pages 3 again from 2"]
    );
    let got: Vec<u64> = caller
        .outs
        .iter()
        .filter(|(name, _)| name == "page")
        .map(|(_, v)| v["page"].as_u64().unwrap())
        .collect();
    assert_eq!(got, [1, 2, 3, 4]);
    assert_eq!(caller.throughs, [3]);
}

// ------------------------------------------------------------- long streams

#[tokio::test]
async fn what_the_caller_keeps_stays_bounded_over_thousands_of_events() {
    for (build, input, bound) in [
        (relay as Build, events(1, 5000), 2),
        (backup, events(1, 5000), 2),
        (archive_and_batch, events(1, 3000), 8),
        (pages, paged(1, 2000), 12),
    ] {
        let world = World::new();
        let mut caller = Caller::new();
        let len = input.len() as u64;
        // Kill halfway through, then run to the end.
        let halfway = events_in(&build(&World::new()), &input).await / 2;
        run(&build(&world), &input, &mut caller, Some(halfway)).await;
        let saved = caller.saved.to_string().len();
        assert!(caller.position() > len / 3, "{}", caller.position());
        run(&build(&world), &input, &mut caller, None).await;
        assert!(caller.most <= bound, "{} lines", caller.most);
        assert!(saved < 2_000, "the saved bookmark is {saved} bytes");
        assert_eq!(caller.position(), len);
        assert_forward(&caller.throughs);
    }
}

// ----------------------------------------------------------------- failures

fn with_bad(n: u64) -> Vec<Value> {
    let mut input = events(1, 8);
    input[usize::try_from(n - 1).unwrap()]["body"] = json!("bad");
    input
}

#[tokio::test]
async fn a_failure_nothing_catches_ends_the_run_with_the_position_before_the_failed_event() {
    let world = World::new();
    let input = with_bad(4);
    let mut caller = Caller::new();
    let end = run(&relay(&world), &input, &mut caller, None).await;
    assert!(
        matches!(&end, End::Failed(RunError::Unhandled { code, .. }) if code == "rejected"),
        "{end:?}"
    );
    assert_eq!(caller.throughs, [1, 2, 3]);
    assert_eq!(
        caller.bookmark().lines(),
        [Line {
            item: 4,
            entry: Entry::Failed {
                at: "pipe[0]#0".into(),
                input: json!({ "seq": 4, "body": "bad" }),
                code: "rejected".into(),
                message: "event 4 is bad".into(),
            },
        }]
    );
}

#[tokio::test]
async fn a_restart_after_an_uncaught_failure_replays_it_and_stops_at_the_same_place() {
    let world = World::new();
    let input = with_bad(4);
    let mut caller = Caller::new();
    run(&relay(&world), &input, &mut caller, None).await;
    let end = run(&relay(&world), &input, &mut caller, None).await;
    assert!(matches!(end, End::Failed(RunError::Unhandled { .. })));
    assert_eq!(caller.position(), 3);
    // The saved failure is replayed: the act is not asked again.
    assert_eq!(world.count("archive.put 4"), 1);
}

#[tokio::test]
async fn a_restart_with_a_vine_that_catches_the_failure_carries_on_past_it() {
    let world = World::new();
    let input = with_bad(4);
    let mut caller = Caller::new();
    run(&relay(&world), &input, &mut caller, None).await;
    // The vine gains a `catch`. Its act keeps its path, so the saved
    // failure fits it and is handled without asking the act again.
    let end = run(&relay_caught(&world), &input, &mut caller, None).await;
    assert!(matches!(end, End::Ended), "{end:?}");
    assert_eq!(caller.position(), 8);
    assert_eq!(world.count("archive.put 4"), 1);
    assert_eq!(
        caller.outs[3],
        (
            "refused".into(),
            json!({ "seq": 4, "reason": "event 4 is bad" })
        )
    );
    assert_eq!(caller.outs.len(), 8);
}

#[tokio::test]
async fn a_caught_failure_is_one_output_and_the_position_moves_past_it() {
    let (caller, log, _) = uninterrupted(relay_caught, &with_bad(4)).await;
    assert_eq!(caller.throughs, (1..=8).collect::<Vec<_>>());
    assert_eq!(caller.outs[3].0, "refused");
    assert_eq!(log.len(), 8);
}

#[tokio::test]
async fn a_caught_failure_survives_a_kill_at_every_point() {
    let input = with_bad(4);
    let (_, _, events) = uninterrupted(relay_caught, &input).await;
    for k in 1..events {
        check_kills(relay_caught, &input, &[k], 2).await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_failure_waiting_for_its_cleanup_keeps_the_position_before_its_item() {
    let world = World::new();
    let mut reg = feed_registry(&world);
    reg.register_act(
        OperatorSpec::new("slow.cleanup")
            .settings(Shape::open())
            .touches(["cleanup"]),
        |_v, _cx| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(json!({}))
        },
    )
    .unwrap();
    // `split` on text that is not a list is a run error. `finalize` holds
    // it back while the slow cleanup runs.
    let vine = reg
        .build_pipe(
            pipe("v")
                .input(Shape::new().required("parts", Ty::Any))
                .output("out", Shape::open())
                .stages([
                    split("$item.parts"),
                    emit("out"),
                    finalize([tap("slow.cleanup", json!({}))]),
                ]),
        )
        .unwrap();
    let input = vec![json!({ "parts": [{ "a": 1 }] }), json!({ "parts": "none" })];
    let mut caller = Caller::new();
    let end = run(&vine, &input, &mut caller, None).await;
    assert!(
        matches!(end, End::Failed(RunError::ItemMisfit { .. })),
        "{end:?}"
    );
    assert_eq!(caller.throughs, [1]);
}

// ----------------------------------------------------- what the caller lost

/// Kill `event_backup` at the first event that `stop` picks, and give the
/// caller and the world.
async fn backup_killed_at(stop: fn(&Progress) -> bool) -> (Arc<World>, Caller) {
    let input = events(1, 5);
    let mut events = backup(&World::new()).run_from(stream::iter(input.clone()), Bookmark::new());
    let mut k = 0;
    while let Some(p) = events.next().await {
        k += 1;
        if stop(&p.unwrap()) {
            break;
        }
    }
    let world = World::new();
    let mut caller = Caller::new();
    run(&backup(&world), &input, &mut caller, Some(k)).await;
    (world, caller)
}

#[tokio::test]
async fn a_restart_with_the_position_and_no_lines_stores_the_batch_in_flight_again() {
    let (world, mut caller) = backup_killed_at(|p| {
        matches!(
            p,
            Progress::Save(Line {
                entry: Entry::Done { .. },
                ..
            })
        )
    })
    .await;
    assert_eq!(caller.position(), 0);
    assert_eq!(caller.bookmark().len(), 1);
    // The store kept the position and lost the lines.
    caller.saved = Bookmark::at(caller.position()).to_json();
    run(&backup(&world), &events(1, 5), &mut caller, None).await;
    assert_eq!(world.log(), ["backup.store 1-5", "backup.store 1-5"]);
    assert_eq!(caller.outs.len(), 1);
}

#[tokio::test]
async fn a_restart_with_the_position_and_no_lines_hands_an_output_in_flight_out_again() {
    let (world, mut caller) = backup_killed_at(|p| {
        matches!(
            p,
            Progress::Save(Line {
                entry: Entry::Emitted { .. },
                ..
            })
        )
    })
    .await;
    assert_eq!(caller.position(), 0, "the position comes after the line");
    caller.saved = Bookmark::at(0).to_json();
    run(&backup(&world), &events(1, 5), &mut caller, None).await;
    assert_eq!(caller.outs.len(), 2);
    assert_eq!(caller.outs[0], caller.outs[1]);
    assert_eq!(world.count("backup.store"), 2);
}

#[tokio::test]
async fn a_restart_from_a_position_with_no_lines_skips_what_is_done() {
    let world = World::new();
    let mut caller = Caller::from(Bookmark::at(5));
    run(&backup(&world), &events(1, 12), &mut caller, None).await;
    assert_eq!(world.log(), ["backup.store 6-10", "backup.store 11-12"]);
    assert_eq!(caller.throughs, [10, 12]);
}

// ---------------------------------------------------------- what does not fit

async fn misfit(bookmark: Bookmark, input: Vec<Value>) -> String {
    let world = World::new();
    let got: Vec<_> = relay(&world)
        .run_from(stream::iter(input), bookmark)
        .collect()
        .await;
    match got.last() {
        Some(Err(RunError::ReplayMisfit { problem })) => problem.clone(),
        other => panic!("{other:?}"),
    }
}

fn kept(position: u64, lines: Vec<(u64, Entry)>) -> Bookmark {
    let lines: Vec<Value> = lines
        .into_iter()
        .map(|(item, entry)| Line { item, entry }.to_json())
        .collect();
    Bookmark::from_json(&json!({ "position": position, "lines": lines })).unwrap()
}

#[tokio::test]
async fn a_bookmark_that_does_not_fit_the_vine_or_its_input_is_a_run_error() {
    let done = |input: Value| Entry::Done {
        at: "pipe[0]#0".into(),
        input,
        result: json!({ "receipt": "r-1" }),
    };
    // The caller fed other items after the position than it fed before.
    let p = misfit(
        kept(
            0,
            vec![(1, done(json!({ "seq": 1, "body": "something else" })))],
        ),
        events(1, 1),
    )
    .await;
    assert!(
        p.contains("pipe[0]#0 of item 1` was saved for input"),
        "{p}"
    );
    // A stream was saved where the vine has an act that gives one result.
    let started = Entry::Started {
        at: "pipe[0]#0".into(),
        input: json!({}),
    };
    let p = misfit(kept(0, vec![(1, started)]), events(1, 1)).await;
    assert!(p.contains("saved as a stream act"), "{p}");
    // Another output was handed out for this item.
    let other = Entry::Emitted {
        output: "other".into(),
        within: None,
    };
    let p = misfit(kept(0, vec![(1, other)]), events(1, 1)).await;
    assert!(p.contains("output `other`"), "{p}");
    // More outputs were handed out than the run makes before its input
    // ends.
    let handed = || Entry::Emitted {
        output: "archived".into(),
        within: None,
    };
    let p = misfit(kept(0, vec![(1, handed()), (1, handed())]), events(1, 1)).await;
    assert!(p.contains("1 more outputs"), "{p}");
    // An item for a stream that was never started.
    let item = Entry::Item {
        at: "pipe[0]#0".into(),
        value: json!({}),
    };
    let p = misfit(kept(0, vec![(1, item)]), events(1, 1)).await;
    assert!(p.starts_with("line 0:"), "{p}");
}

#[tokio::test]
async fn lines_at_or_before_the_position_are_not_read() {
    // The caller saved the position and crashed before dropping the lines
    // it passed. They would not fit, and they are not read.
    let stale = Entry::Started {
        at: "pipe[0]#0".into(),
        input: json!({}),
    };
    let world = World::new();
    let mut caller = Caller::from(kept(3, vec![(2, stale.clone()), (3, stale)]));
    let end = run(&relay(&world), &events(1, 5), &mut caller, None).await;
    assert!(matches!(end, End::Ended), "{end:?}");
    assert_eq!(caller.throughs, [4, 5]);
}

#[test]
fn a_bookmark_survives_json_and_one_that_is_not_one_is_refused() {
    let bookmark = kept(
        7,
        vec![(
            8,
            Entry::Emitted {
                output: "out".into(),
                within: None,
            },
        )],
    );
    let text = serde_json::to_string(&bookmark.to_json()).unwrap();
    assert_eq!(
        Bookmark::from_json(&serde_json::from_str(&text).unwrap()).unwrap(),
        bookmark
    );
    let emitted = json!({ "kind": "emitted", "output": "out" });
    for (bad, says) in [
        (json!([]), "a bookmark is a map"),
        (json!({ "lines": [] }), "`position`"),
        (json!({ "position": 1 }), "`lines`"),
        (
            json!({ "position": 1, "lines": [], "at": 2 }),
            "no field `at`",
        ),
        (
            json!({ "position": 1, "lines": [emitted] }),
            "line 0: a line needs `item`",
        ),
        (
            json!({ "position": 1, "lines": [{ "item": 0, "kind": "emitted", "output": "o" }] }),
            "counted from 1",
        ),
        (
            json!({ "position": 1, "lines": [{ "item": 2, "kind": "guess" }] }),
            "unknown entry kind",
        ),
    ] {
        let err = Bookmark::from_json(&bad).unwrap_err();
        assert!(err.contains(says), "{bad}: {err}");
    }
}

#[test]
fn recording_the_position_drops_the_lines_it_passed() {
    let line = |item| {
        Progress::Save(Line {
            item,
            entry: Entry::Emitted {
                output: "out".into(),
                within: None,
            },
        })
    };
    let mut bookmark = Bookmark::new();
    for p in [line(1), line(2), line(3), Progress::Through(2)] {
        bookmark.record(&p);
    }
    assert_eq!(bookmark.position(), 2);
    assert_eq!(
        bookmark.lines().iter().map(|l| l.item).collect::<Vec<_>>(),
        [3]
    );
}

#[tokio::test]
async fn input_that_does_not_fit_the_shape_stops_the_run_before_that_item() {
    let world = World::new();
    let mut input = events(1, 5);
    input[2] = json!({ "seq": 3 });
    let mut caller = Caller::new();
    for _ in 0..2 {
        let end = run(&relay(&world), &input, &mut caller, None).await;
        assert!(
            matches!(end, End::Failed(RunError::InputMisfit(_))),
            "{end:?}"
        );
        assert_eq!(caller.position(), 2);
    }
    assert_eq!(world.log().len(), 2);
}

// ------------------------------------------------------------------ checker

fn events_vine(stages: impl IntoIterator<Item = Stage>) -> PipeDef {
    pipe("v")
        .input(
            Shape::new()
                .required("seq", Ty::Int)
                .required("body", Ty::Text),
        )
        .output("out", Shape::open())
        .stages(stages)
}

fn archive() -> Stage {
    call(
        "archive.put",
        json!({ "seq": "$item.seq", "body": "$item.body" }),
    )
}

#[tokio::test]
async fn the_checker_refuses_a_vine_a_restart_from_a_position_cannot_rebuild() {
    let world = World::new();
    let mut reg = feed_registry(&world);
    let switching = reg
        .build_pipe(
            pipe("switching")
                .input(Shape::open())
                .output("out", Shape::open())
                .stages([each(Overlap::Switch, [emit("out")])]),
        )
        .unwrap();
    reg.add_vine(switching).unwrap();
    let cases: Vec<(PipeDef, &str)> = vec![
        (
            events_vine([take(3), emit("out")]),
            "pipe[0] is a `take` over the whole stream",
        ),
        (
            events_vine([fork([vec![take(1), emit("out")]]), emit("out")]),
            "pipe[0].fork[0][0] is a `take` over the whole stream",
        ),
        (
            events_vine([split("$item.body").into("part"), collect(2), emit("out")]),
            "pipe[1] gathers batches over the whole stream after pipe[0]",
        ),
        (
            events_vine([
                call("feed.pages", json!({ "seq": "$item.seq", "pages": 2 })),
                fork([vec![collect(2), emit("out")]]),
                emit("out"),
            ]),
            "pipe[1].fork[0][0] gathers batches over the whole stream after pipe[0]",
        ),
        (
            events_vine([each(Overlap::Switch, [emit("out")])]),
            "overlap `switch`",
        ),
        (
            events_vine([each(Overlap::Exhaust, [emit("out")])]),
            "overlap `exhaust`",
        ),
        (
            events_vine([archive(), timeout(Duration::from_secs(1)), emit("out")]),
            "is a `timeout`",
        ),
        (
            events_vine([use_vine("switching", None).on("out", [emit("out")])]),
            "uses vine `switching`",
        ),
    ];
    for (def, says) in cases {
        let vine = reg.build_pipe(def).unwrap();
        assert!(!vine.resumable_from_position(), "{says}");
        let got: Vec<_> = vine
            .run_from(stream::iter(events(1, 1)), Bookmark::new())
            .collect()
            .await;
        assert!(
            matches!(&got[..], [Err(RunError::NotResumable { why, .. })] if why.contains(says)),
            "{says}: {got:?}"
        );
    }
}

#[test]
fn stages_that_start_again_for_each_item_can_run_from_a_position() {
    let world = World::new();
    let reg = feed_registry(&world);
    for def in [
        // `take` inside `each` counts one item's own stream.
        events_vine([each(
            Overlap::Concat,
            [
                call("feed.pages", json!({ "seq": "$item.seq", "pages": 3 })),
                take(1),
                emit("out"),
            ],
        )]),
        // `last` holds the newest item, and a restart feeds it again.
        events_vine([last(), emit("out")]),
        // `collect` over whole items, in the main pipe or a side pipe.
        events_vine([collect(2), emit("out")]),
        events_vine([fork([vec![collect(2), emit("out")]]), emit("out")]),
        // `collect` after a stream act, inside one item's work.
        events_vine([each(
            Overlap::Concat,
            [
                call("feed.pages", json!({ "seq": "$item.seq", "pages": 3 })),
                collect(2),
                emit("out"),
            ],
        )]),
    ] {
        let vine = reg.build_pipe(def).unwrap();
        assert!(vine.resumable_from_position(), "{vine:?}");
    }
    let (world, host) = (World::new(), Host::new());
    let job_run = job_run_registry(&world, &host)
        .build_pipe(job_run_pipe())
        .unwrap();
    assert!(job_run.resumable_from_position());
}

#[test]
fn a_vine_that_ends_itself_stays_resumable_with_the_full_journal() {
    let world = World::new();
    let vine = feed_registry(&world)
        .build_pipe(events_vine([archive(), take(1), emit("out")]))
        .unwrap();
    assert!(vine.resumable());
    assert!(!vine.resumable_from_position());
}

// ------------------------------------------------------- one save format

#[tokio::test]
async fn run_saved_and_run_from_save_the_same_entries_for_one_item() {
    let world = World::new();
    let saved: Vec<Entry> = relay(&world)
        .run_saved(stream::iter(events(1, 1)), Journal::new())
        .filter_map(|e| async move {
            match e.unwrap() {
                Event::Save(entry) => Some(entry),
                Event::Output(_) => None,
            }
        })
        .collect()
        .await;
    let lines: Vec<Entry> = relay(&world)
        .run_from(stream::iter(events(1, 1)), Bookmark::new())
        .filter_map(|p| async move {
            match p.unwrap() {
                Progress::Save(line) => Some(line.entry),
                _ => None,
            }
        })
        .collect()
        .await;
    assert_eq!(saved, lines);
}

// ------------------------------------------------------------------ job_run

#[tokio::test(start_paused = true)]
async fn job_run_over_a_stream_of_jobs_restarts_mid_job_without_a_second_working_copy() {
    let jobs: Vec<Value> = (1..=3)
        .map(|n| json!({ "job": format!("job-{n}"), "plan_branch": "plan/p-1", "brief": "go" }))
        .collect();
    let (world, host) = (World::new(), Host::new());
    let vine = || {
        job_run_registry(&world, &host)
            .build_pipe(job_run_pipe())
            .unwrap()
    };
    let mut caller = Caller::new();
    // Each job gives 120 lines, 3 summaries and a commit. Kill partway
    // through the second job.
    let end = run(&vine(), &jobs, &mut caller, Some(600)).await;
    assert!(matches!(end, End::Killed { .. }));
    assert_eq!(
        caller.position(),
        1,
        "the first job is done, the second is not"
    );
    let end = run(&vine(), &jobs, &mut caller, None).await;
    assert!(matches!(end, End::Ended), "{end:?}");
    assert_eq!(caller.position(), 3);
    assert_eq!(world.count("git.worktree_add job-2"), 1);
    assert_eq!(world.count("agent.start job-2"), 1);
    assert_eq!(world.count("agent.attach job-2"), 1);
    assert_eq!(world.count("git.commit"), 3);
    let implemented = caller
        .outs
        .iter()
        .filter(|(n, _)| n == "implemented")
        .count();
    assert_eq!(implemented, 3);
}
