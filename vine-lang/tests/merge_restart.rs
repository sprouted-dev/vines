//! A `mergeMap` survives a restart, in `run_saved` and in
//! `run_from`. Each value that enters it starts a group of work keyed by
//! the order values enter, so a replay matches each group's operators and
//! outputs in that group's own order, whatever order the groups finish in.
//! The operators take time on tokio's paused clock, so which item finishes when
//! is exact and the same every run. Nothing here uses YAML, and this file
//! passes with the feature off:
//!
//!     cargo test --no-default-features --test merge_restart

mod fake;

use std::collections::BTreeMap;
use std::sync::Arc;

use fake::desk::{
    desk_registry, job_event, job_stream_vine, job_work, job_work_caught, jobs, jobs_vine,
    merge_jobs, step, Desk,
};
use fake::restart::{
    events_in, events_saved, run_saved_with_kills_where, run_with_kills_where, Caller, Keeper,
    Repeat,
};
use futures::{stream, StreamExt};
use tokio::time::Instant;
use vine_lang::pipe::{
    call, concat_map, emit_with, fork, fork_join, last, use_vine, Pipe, PipeDef,
};
use vine_lang::{json, Bookmark, Entry, Event, Journal, Progress, RunError, Value};

fn built(desk: &Arc<Desk>, def: PipeDef) -> Pipe {
    desk_registry(desk).build_pipe(def).unwrap()
}

/// Three jobs: `a` takes five seconds, `b` one and `c` three, so they
/// finish `b`, `c`, `a`: the first to go in finishes last.
fn three() -> Vec<Value> {
    vec![jobs(&[("a", 5), ("b", 1), ("c", 3)])]
}

fn merging(concurrent: Option<usize>) -> PipeDef {
    jobs_vine([merge_jobs(concurrent, job_work())])
}

type Outs = Vec<(String, Value)>;

async fn outputs_saved(vine: &Pipe, input: &[Value]) -> Outs {
    vine.run_saved(stream::iter(input.to_vec()), Journal::new())
        .filter_map(|e| async move {
            match e.unwrap() {
                Event::Output(o) => Some((o.name, o.payload)),
                Event::Save(_) => None,
            }
        })
        .collect()
        .await
}

fn names(outs: &Outs) -> Vec<String> {
    outs.iter()
        .map(|(name, p)| format!("{name} {}", p["id"].as_str().unwrap_or_default()))
        .collect()
}

/// Each job's own outputs, in the order they came.
fn per_job(outs: &Outs) -> BTreeMap<String, Vec<Value>> {
    let mut by = BTreeMap::<String, Vec<Value>>::new();
    for (name, payload) in outs {
        let id = payload["id"].as_str().unwrap_or("-").to_owned();
        by.entry(id).or_default().push(json!([name, payload]));
    }
    by
}

fn sorted(outs: &Outs) -> Vec<String> {
    let mut all: Vec<String> = outs.iter().map(|o| format!("{o:?}")).collect();
    all.sort();
    all
}

/// How many more times steps were called than `clean` called them.
fn calls_again(desk: &Desk, clean: &[String]) -> usize {
    desk.calls().len() - clean.len()
}

// ------------------------------------------------------------- one run

#[tokio::test(start_paused = true)]
async fn items_side_by_side_finish_out_of_order_and_each_keeps_its_own_order() {
    let desk = Desk::new();
    let start = Instant::now();
    let outs = outputs_saved(&built(&desk, merging(None)), &three()).await;
    assert_eq!(
        names(&outs),
        [
            "stepped b",
            "done b",
            "stepped c",
            "done c",
            "stepped a",
            "done a"
        ]
    );
    assert_eq!(start.elapsed().as_secs(), 6);
    assert_eq!(desk.most_at_once(), 3);
}

#[tokio::test(start_paused = true)]
async fn the_cap_holds_the_third_job_back_until_a_slot_is_free() {
    let desk = Desk::new();
    let start = Instant::now();
    let outs = outputs_saved(&built(&desk, merging(Some(2))), &three()).await;
    assert_eq!(desk.most_at_once(), 2);
    // `b` is done at 2, so `c` starts then; `a` and `c` finish at 6.
    assert!(
        desk.log().contains(&"start c.one @2".to_owned()),
        "{:?}",
        desk.log()
    );
    assert_eq!(start.elapsed().as_secs(), 6);
    assert_eq!(outs.len(), 6);
}

#[tokio::test(start_paused = true)]
async fn each_groups_calls_and_outputs_are_keyed_by_the_order_jobs_go_in() {
    let desk = Desk::new();
    let vine = built(&desk, merging(None));
    let mut journal = Journal::new();
    let mut run = vine.run_saved(stream::iter(three()), journal.clone());
    while let Some(event) = run.next().await {
        journal.record(&event.unwrap());
    }
    let keys: Vec<(String, String)> = journal
        .entries()
        .iter()
        .filter_map(|e| match e {
            Entry::Done { at, result, .. } => Some((at.clone(), result.to_string())),
            Entry::Emitted {
                output,
                within: Some(within),
                ..
            } => Some((within.clone(), output.clone())),
            _ => None,
        })
        .collect();
    // `b` went in second, so it is group 1, though it finished first.
    let b = "pipe[1].mergeMap@1/";
    assert_eq!(
        keys[..4],
        [
            (
                format!("{b}pipe[1].mergeMap.do[0]#0"),
                json!({ "id": "b", "step": "one" }).to_string()
            ),
            (b.into(), "stepped".into()),
            (
                format!("{b}pipe[1].mergeMap.do[2]#0"),
                json!({ "id": "b", "step": "two" }).to_string()
            ),
            (b.into(), "done".into()),
        ]
    );
    // Through JSON and back, the full journal replays: no step runs and
    // nothing is handed out again.
    let journal = Journal::from_json(&journal.to_json()).unwrap();
    let calls = desk.calls().len();
    let again: Vec<_> = vine
        .run_saved(stream::iter(three()), journal)
        .collect()
        .await;
    assert!(again.is_empty(), "{again:?}");
    assert_eq!(desk.calls().len(), calls);
}

#[tokio::test(start_paused = true)]
async fn with_items_side_by_side_no_call_starts_before_every_result_so_far_was_handed_out() {
    // Steps finish at the same moments, so results pile up unless the run
    // waits: a step may start only once the caller holds every result made
    // before it. Steps already running finish in any order.
    for secs in [0, 1] {
        let desk = Desk::new();
        let input = vec![jobs(&[("a", secs), ("b", secs), ("c", secs), ("d", secs)])];
        // No output between the two steps, so nothing else stops a job's
        // second step from starting as soon as its first is done.
        let def = jobs_vine([merge_jobs(
            Some(3),
            vec![
                step("one", json!("$item.job.secs")),
                step("two", json!("$item.job.secs")),
                emit_with("done", json!({ "id": "$item.job.id" })),
            ],
        )]);
        let vine = built(&desk, def);
        let mut run = vine.run_saved(stream::iter(input), Journal::new());
        while let Some(event) = run.next().await {
            if let Event::Save(Entry::Done { .. }) = event.unwrap() {
                desk.handed_a_result();
            }
        }
        let starts = desk.at_each_start();
        assert_eq!(starts.len(), 8);
        for (finished, handed) in &starts {
            assert_eq!(finished, handed, "{secs}s steps: {starts:?}");
        }
        if secs > 0 {
            assert_eq!(desk.most_at_once(), 3, "the steps ran side by side");
        }
    }
}

// --------------------------------------------------------- kill sweeps

/// What a sweep over every kill point found.
#[derive(Debug, PartialEq)]
struct Sweep {
    points: usize,
    /// The most steps one kill made run again.
    most_again: usize,
    /// How many kill points fell right after an output, so it came again.
    repeats: usize,
}

/// How many steps a run killed after `k` events had started whose result
/// the caller had not saved: the ones a restart must run again.
async fn unsaved_at(def: &impl Fn() -> PipeDef, input: &[Value], k: usize) -> usize {
    let desk = Desk::new();
    let mut keeper = Keeper::new();
    fake::restart::run_saved(&built(&desk, def()), input, &mut keeper, Some(k)).await;
    desk.calls().len() - keeper.results
}

/// Kill a saved run after each count of events in turn, restart it from
/// its journal through JSON, and check it ends as an uninterrupted run:
/// the same outputs, each job's in the same order, and every step done.
/// The steps that run again are exactly the ones that had started and
/// whose result the caller had not saved, and there are at most `bound`
/// of them.
async fn sweep_saved(def: impl Fn() -> PipeDef, input: &[Value], bound: usize) -> Sweep {
    let desk = Desk::new();
    let clean_vine = built(&desk, def());
    let clean = outputs_saved(&clean_vine, input).await;
    let clean_calls = desk.calls();
    let points = events_saved(&clean_vine, input).await;
    let (mut most_again, mut repeats) = (0, 0);
    for k in 1..=points {
        let desk = Desk::new();
        let mut keeper = Keeper::new();
        repeats += run_saved_with_kills_where(
            || built(&desk, def()),
            input,
            &mut keeper,
            &[k],
            Repeat::Later,
        )
        .await;
        assert_eq!(sorted(&keeper.outs), sorted(&clean), "kill at {k}");
        assert_eq!(per_job(&keeper.outs), per_job(&clean), "kill at {k}");
        let again = calls_again(&desk, &clean_calls);
        assert!(again >= desk.cut_off(), "kill at {k}: {:?}", desk.log());
        assert_eq!(
            again,
            unsaved_at(&def, input, k).await,
            "kill at {k}: {:?}",
            desk.log()
        );
        assert!(
            again <= bound,
            "kill at {k}: {again} steps again: {:?}",
            desk.log()
        );
        most_again = most_again.max(again);
    }
    Sweep {
        points,
        most_again,
        repeats,
    }
}

fn four() -> Vec<Value> {
    vec![jobs(&[("a", 5), ("b", 1), ("c", 3), ("d", 4)])]
}

#[tokio::test(start_paused = true)]
async fn a_kill_at_every_point_of_a_saved_run_then_a_restart_ends_as_an_uninterrupted_run() {
    // This caller saves each event before the kill, so the job whose event
    // came last has no step running: with two at once, one step at most
    // is cut off.
    let sweep = sweep_saved(|| merging(Some(2)), &four(), 2).await;
    // Four jobs, each two results, two outputs and two lines for them.
    assert_eq!(sweep.points, 24, "{sweep:?}");
    assert_eq!(sweep.most_again, 1, "{sweep:?}");
    assert!(sweep.repeats > 0, "{sweep:?}");
    let sweep = sweep_saved(|| merging(Some(3)), &four(), 3).await;
    assert_eq!(sweep.most_again, 2, "{sweep:?}");
}

#[tokio::test(start_paused = true)]
async fn with_no_cap_a_kill_runs_again_at_most_one_step_per_job_running() {
    let sweep = sweep_saved(|| merging(None), &four(), 4).await;
    assert_eq!(sweep.most_again, 3, "{sweep:?}");
}

#[tokio::test(start_paused = true)]
async fn the_kill_sweep_gives_the_same_result_every_time() {
    let first = sweep_saved(|| merging(Some(2)), &four(), 2).await;
    for _ in 0..3 {
        assert_eq!(sweep_saved(|| merging(Some(2)), &four(), 2).await, first);
    }
}

#[tokio::test(start_paused = true)]
async fn two_kills_on_a_grid_then_a_restart_end_as_an_uninterrupted_run() {
    let desk = Desk::new();
    let input = three();
    let clean = outputs_saved(&built(&desk, merging(Some(2))), &input).await;
    let points = events_saved(&built(&desk, merging(Some(2))), &input).await;
    for first in (1..points).step_by(3) {
        for second in (1..points).step_by(4) {
            let desk = Desk::new();
            let mut keeper = Keeper::new();
            run_saved_with_kills_where(
                || built(&desk, merging(Some(2))),
                &input,
                &mut keeper,
                &[first, second],
                Repeat::Later,
            )
            .await;
            let at = format!("kills at {first} then {second}");
            assert_eq!(sorted(&keeper.outs), sorted(&clean), "{at}");
            assert_eq!(per_job(&keeper.outs), per_job(&clean), "{at}");
        }
    }
}

// ------------------------------------------------------------ failures

#[tokio::test(start_paused = true)]
async fn a_failure_nothing_catches_ends_the_run_and_a_restart_stops_at_it_again() {
    let desk = Desk::new();
    desk.fail("b", "two");
    let vine = built(&desk, merging(Some(2)));
    let mut keeper = Keeper::new();
    let end = fake::restart::run_saved(&vine, &three(), &mut keeper, None).await;
    let fake::restart::End::Failed(first) = end else {
        panic!("{end:?}")
    };
    assert!(
        matches!(&first, RunError::Unhandled { code, .. } if code == "broke"),
        "{first}"
    );
    assert_eq!(names(&keeper.outs), ["stepped b"]);
    // `a` was running when `b` broke; the run stopped and dropped it.
    let calls = desk.calls();
    assert_eq!(calls, ["a.one", "b.one", "b.two"]);

    let end = fake::restart::run_saved(&vine, &three(), &mut keeper, None).await;
    let fake::restart::End::Failed(again) = end else {
        panic!("{end:?}")
    };
    assert_eq!(again.to_string(), first.to_string());
    // Nothing is handed out twice, `b` runs no step again, and only the
    // step that was dropped mid-way starts again.
    assert_eq!(names(&keeper.outs), ["stepped b"]);
    assert_eq!(desk.calls(), ["a.one", "b.one", "b.two", "a.one"]);
}

#[tokio::test(start_paused = true)]
async fn a_catch_error_inside_the_merge_map_keeps_the_others_going_through_every_kill() {
    let def = || jobs_vine([merge_jobs(Some(2), job_work_caught())]);
    // `a` fails at 6, after `b` and `c` are done.
    let input = vec![jobs(&[("a", 6), ("b", 1), ("c", 2)])];
    let desk = Desk::new();
    desk.fail("a", "one");
    let outs = outputs_saved(&built(&desk, def()), &input).await;
    assert_eq!(
        names(&outs),
        ["stepped b", "done b", "stepped c", "done c", "failed a"]
    );
    // The same over every kill point, with `a` failing every time.
    let clean_calls = desk.calls();
    let failing = Desk::new();
    failing.fail("a", "one");
    let points = events_saved(&built(&failing, def()), &input).await;
    for k in 1..=points {
        let desk = Desk::new();
        desk.fail("a", "one");
        let mut keeper = Keeper::new();
        run_saved_with_kills_where(
            || built(&desk, def()),
            &input,
            &mut keeper,
            &[k],
            Repeat::Later,
        )
        .await;
        assert_eq!(sorted(&keeper.outs), sorted(&outs), "kill at {k}");
        assert_eq!(per_job(&keeper.outs), per_job(&outs), "kill at {k}");
        assert!(calls_again(&desk, &clean_calls) <= 2, "kill at {k}");
    }
}

// --------------------------------------------- stream operators inside a merge

fn lines_inside(concurrent: usize) -> PipeDef {
    jobs_vine([merge_jobs(
        Some(concurrent),
        vec![
            call(
                "work.lines",
                json!({ "id": "$item.job.id", "lines": "$item.job.secs" }),
            )
            .into("line"),
            emit_with(
                "line",
                json!({ "id": "$item.job.id", "line": "$item.line.line" }),
            ),
        ],
    )])
}

#[tokio::test(start_paused = true)]
async fn a_stream_operator_inside_a_merge_is_saved_by_position_and_stays_small() {
    let desk = Desk::new();
    let vine = built(&desk, lines_inside(2));
    assert!(
        vine.saves_every_item().is_empty(),
        "{:?}",
        vine.saves_every_item()
    );
    let input = vec![jobs(&[("a", 40), ("b", 30), ("c", 20)])];
    let mut keeper = Keeper::new();
    fake::restart::run_saved(&vine, &input, &mut keeper, None).await;
    assert_eq!(keeper.outs.len(), 90);
    let journal = keeper.journal();
    assert!(
        journal
            .entries()
            .iter()
            .all(|e| !matches!(e, Entry::Item { .. } | Entry::Emitted { .. })),
        "{:?}",
        journal.entries()
    );
    // Each call keeps when it started, its position and how it stopped,
    // however many lines it gives: three lines a call, not ninety.
    assert!(keeper.most <= 9, "{} entries", keeper.most);
}

#[tokio::test(start_paused = true)]
async fn a_stream_operator_inside_a_merge_resumes_from_its_position_at_every_kill() {
    let input = vec![jobs(&[("a", 4), ("b", 2), ("c", 3)])];
    let desk = Desk::new();
    let clean = outputs_saved(&built(&desk, lines_inside(2)), &input).await;
    let points = events_saved(&built(&desk, lines_inside(2)), &input).await;
    let mut resumed = 0;
    for k in 1..=points {
        let desk = Desk::new();
        let mut keeper = Keeper::new();
        run_saved_with_kills_where(
            || built(&desk, lines_inside(2)),
            &input,
            &mut keeper,
            &[k],
            Repeat::Later,
        )
        .await;
        assert_eq!(sorted(&keeper.outs), sorted(&clean), "kill at {k}");
        assert_eq!(per_job(&keeper.outs), per_job(&clean), "kill at {k}");
        resumed += desk
            .log()
            .iter()
            .filter(|l| l.starts_with("lines ") && !l.contains("from 0"))
            .count();
    }
    assert!(resumed > 0, "no call was asked to go on from a position");
}

#[tokio::test(start_paused = true)]
async fn a_merge_after_a_stream_operator_saved_by_position_stays_small_and_resumes() {
    // The lines of one call go side by side into the `mergeMap`. Each output
    // names its group, and the position drops the outputs of lines it
    // passed.
    let def = || {
        jobs_vine([concat_map(vec![
            call(
                "work.lines",
                json!({ "id": "$item.job.id", "lines": "$item.job.secs" }),
            )
            .into("line"),
            merge_jobs(
                Some(2),
                vec![emit_with(
                    "line",
                    json!({ "id": "$item.job.id", "line": "$item.line.line" }),
                )],
            ),
        ])])
    };
    let input = vec![jobs(&[("a", 30), ("b", 20)])];
    let desk = Desk::new();
    let vine = built(&desk, def());
    assert!(vine.saves_every_item().is_empty());
    let mut keeper = Keeper::new();
    fake::restart::run_saved(&vine, &input, &mut keeper, None).await;
    assert_eq!(keeper.outs.len(), 50);
    assert!(keeper.most <= 8, "{} entries", keeper.most);

    let small = vec![jobs(&[("a", 3), ("b", 2)])];
    let clean = outputs_saved(&built(&desk, def()), &small).await;
    let points = events_saved(&built(&desk, def()), &small).await;
    for k in 1..=points {
        let desk = Desk::new();
        let mut keeper = Keeper::new();
        run_saved_with_kills_where(
            || built(&desk, def()),
            &small,
            &mut keeper,
            &[k],
            Repeat::Later,
        )
        .await;
        assert_eq!(sorted(&keeper.outs), sorted(&clean), "kill at {k}");
    }
}

// ------------------------------------------------------- the end of all

fn summed() -> PipeDef {
    jobs_vine([
        merge_jobs(
            Some(2),
            vec![
                step("one", json!("$item.job.secs")),
                fork([[emit_with(
                    "stepped",
                    json!({ "id": "$item.job.id", "step": "one" }),
                )]]),
            ],
        ),
        last(),
        emit_with("all", json!({ "jobs": "$item.jobs" })),
    ])
}

#[tokio::test(start_paused = true)]
async fn last_after_a_merge_gives_one_summary_after_every_job_at_every_kill() {
    let desk = Desk::new();
    let clean = outputs_saved(&built(&desk, summed()), &three()).await;
    assert_eq!(names(&clean).last().map(String::as_str), Some("all "));
    let points = events_saved(&built(&desk, summed()), &three()).await;
    for k in 1..=points {
        let desk = Desk::new();
        let mut keeper = Keeper::new();
        run_saved_with_kills_where(
            || built(&desk, summed()),
            &three(),
            &mut keeper,
            &[k],
            Repeat::Later,
        )
        .await;
        assert_eq!(sorted(&keeper.outs), sorted(&clean), "kill at {k}");
        assert_eq!(keeper.outs.last(), clean.last(), "kill at {k}");
    }
}

#[tokio::test(start_paused = true)]
async fn last_after_a_merge_of_stream_operators_saved_by_position_gives_one_summary_at_every_kill()
{
    // `last` picks an end item of a call saved by position. Once the
    // call's position passes it, that item is never given again, so the
    // record of the summary made from it must go with it.
    let def = || {
        jobs_vine([
            merge_jobs(
                Some(2),
                vec![
                    call(
                        "work.lines",
                        json!({ "id": "$item.job.id", "lines": "$item.job.secs" }),
                    )
                    .into("line")
                    .end("ended"),
                    fork([[
                        vine_lang::pipe::filter("$item.line"),
                        emit_with(
                            "line",
                            json!({ "id": "$item.job.id", "line": "$item.line.line" }),
                        ),
                    ]]),
                    vine_lang::pipe::filter("$item.ended"),
                ],
            ),
            last(),
            emit_with("all", json!({ "jobs": "$item.jobs" })),
        ])
    };
    let desk = Desk::new();
    let vine = built(&desk, def());
    assert!(
        vine.saves_every_item().is_empty(),
        "{:?}",
        vine.saves_every_item()
    );
    let input = vec![jobs(&[("a", 3), ("b", 1), ("c", 2)])];
    let clean = outputs_saved(&vine, &input).await;
    assert_eq!(names(&clean).last().map(String::as_str), Some("all "));
    let points = events_saved(&vine, &input).await;
    for k in 1..=points {
        let desk = Desk::new();
        let mut keeper = Keeper::new();
        run_saved_with_kills_where(
            || built(&desk, def()),
            &input,
            &mut keeper,
            &[k],
            Repeat::Later,
        )
        .await;
        assert_eq!(sorted(&keeper.outs), sorted(&clean), "kill at {k}");
        assert_eq!(keeper.outs.last(), clean.last(), "kill at {k}");
    }
}

// ------------------------------------------------- a saved position

fn stream_vine(concurrent: usize) -> PipeDef {
    job_stream_vine([merge_jobs(Some(concurrent), job_work())])
}

/// Job events `1..`: the first takes `slow` seconds, the rest one each.
fn events(slow: u64, quick: usize) -> Vec<Value> {
    std::iter::once(job_event("e1", slow))
        .chain((2..=quick + 1).map(|n| job_event(&format!("e{n}"), 1)))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn the_position_waits_for_an_early_item_that_finishes_last_and_never_goes_back() {
    let desk = Desk::new();
    let vine = built(&desk, stream_vine(2));
    let mut run = vine.run_from(stream::iter(events(6, 3)), Bookmark::new());
    let mut seen = Vec::new();
    let mut done_out = std::collections::BTreeSet::new();
    while let Some(p) = run.next().await {
        match p.unwrap() {
            Progress::Output(o) if o.name == "done" => {
                done_out.insert(o.payload["id"].as_str().unwrap().to_owned());
            }
            Progress::Through(n) => {
                // The position never passes an item whose work is not done.
                for k in 1..=n {
                    assert!(done_out.contains(&format!("e{k}")), "through {n}");
                }
                seen.push(format!("through {n}"));
            }
            Progress::Done(n) => {
                assert!(done_out.contains(&format!("e{n}")), "done {n}");
                seen.push(format!("done {n}"));
            }
            _ => {}
        }
    }
    // e1 takes seven seconds; e2, e3 and e4 finish before it.
    assert_eq!(seen, ["done 2", "done 3", "done 4", "through 4"]);
}

#[tokio::test(start_paused = true)]
async fn a_bookmark_stays_bounded_by_the_cap_while_an_early_item_is_slow() {
    let desk = Desk::new();
    let vine = built(&desk, stream_vine(2));
    let mut caller = Caller::new();
    fake::restart::run(&vine, &events(100, 40), &mut caller, None).await;
    assert_eq!(caller.position(), 41);
    assert_eq!(caller.dones.len(), 40);
    // At most two items are in flight, each with a few lines; the forty
    // that finished past the slow one are kept as one run of numbers.
    assert!(caller.most <= 4, "{} lines", caller.most);
    assert!(caller.largest < 600, "{} bytes", caller.largest);
}

#[tokio::test(start_paused = true)]
async fn a_run_from_a_bookmark_skips_items_done_past_its_position() {
    let desk = Desk::new();
    let vine = built(&desk, stream_vine(2));
    let input = events(6, 3);
    let mut caller = Caller::new();
    // Stop once e2 to e4 are done and e1 is still running.
    let points = events_in(&vine, &input).await;
    let mut kill = None;
    for k in 1..=points {
        let mut probe = Caller::new();
        fake::restart::run(
            &built(&Desk::new(), stream_vine(2)),
            &input,
            &mut probe,
            Some(k),
        )
        .await;
        if probe.dones.len() == 3 {
            kill = Some(k);
            break;
        }
    }
    fake::restart::run(&vine, &input, &mut caller, kill).await;
    let bookmark = caller.bookmark();
    assert_eq!(bookmark.position(), 0);
    assert_eq!(bookmark.done().collect::<Vec<_>>(), [2, 3, 4]);
    assert!(bookmark.lines().iter().all(|l| l.item == 1));
    let before = desk.calls();
    fake::restart::run(&vine, &input, &mut caller, None).await;
    // Only e1 runs again, from its own lines.
    let after: Vec<String> = desk.calls()[before.len()..].to_vec();
    assert!(after.iter().all(|c| c.starts_with("e1.")), "{after:?}");
    assert_eq!(caller.position(), 4);
}

#[tokio::test(start_paused = true)]
async fn a_kill_at_every_point_of_a_run_from_a_position_ends_as_an_uninterrupted_run() {
    let input = events(6, 3);
    let desk = Desk::new();
    let mut clean = Caller::new();
    fake::restart::run(&built(&desk, stream_vine(2)), &input, &mut clean, None).await;
    let clean_calls = desk.calls();
    let points = events_in(&built(&desk, stream_vine(2)), &input).await;
    for k in 1..=points {
        for second in [None, Some(3)] {
            let desk = Desk::new();
            let mut caller = Caller::new();
            let kills: Vec<usize> = [Some(k), second].into_iter().flatten().collect();
            run_with_kills_where(
                || built(&desk, stream_vine(2)),
                &input,
                &mut caller,
                &kills,
                Repeat::Later,
            )
            .await;
            let at = format!("kills {kills:?}");
            assert_eq!(sorted(&caller.outs), sorted(&clean.outs), "{at}");
            assert_eq!(per_job(&caller.outs), per_job(&clean.outs), "{at}");
            assert_eq!(caller.position(), 4, "{at}");
            fake::restart::assert_forward(&caller.throughs);
            let again = calls_again(&desk, &clean_calls);
            assert!(
                again <= 2 * kills.len(),
                "{at}: {again} again: {:?}",
                desk.log()
            );
        }
    }
}

// ------------------------------------------ what runs inside the merge

#[tokio::test(start_paused = true)]
async fn a_used_vine_inside_the_merge_replays_at_every_kill() {
    let def = || {
        jobs_vine([merge_jobs(
            Some(2),
            vec![use_vine("job_steps", Some(json!({ "job": "$item.job" })))
                .on("done", [emit_with("done", json!({ "id": "$item.id" }))])],
        )])
    };
    let sweep = sweep_saved(def, &four(), 2).await;
    assert_eq!(sweep.most_again, 1, "{sweep:?}");
}

#[tokio::test(start_paused = true)]
async fn fork_join_inside_the_merge_map_replays_at_every_kill() {
    let def = || {
        jobs_vine([merge_jobs(
            Some(2),
            vec![
                fork_join([
                    call(
                        "work.step",
                        json!({ "id": "$item.job.id", "step": "x", "secs": "$item.job.secs" }),
                    )
                    .into("x"),
                    call(
                        "work.step",
                        json!({ "id": "$item.job.id", "step": "y", "secs": 2 }),
                    )
                    .into("y"),
                ]),
                emit_with("done", json!({ "id": "$item.job.id" })),
            ],
        )])
    };
    // Two jobs at once, each with two steps side by side.
    let sweep = sweep_saved(def, &four(), 4).await;
    assert!(sweep.most_again >= 2, "{sweep:?}");
}

#[tokio::test(start_paused = true)]
async fn a_merge_inside_a_merge_replays_at_every_kill() {
    let def = || {
        jobs_vine([merge_jobs(
            Some(2),
            vec![
                call(
                    "work.lines",
                    json!({ "id": "$item.job.id", "lines": "$item.job.secs" }),
                )
                .into("line"),
                merge_jobs(
                    Some(2),
                    vec![
                        step("one", json!(1)),
                        emit_with(
                            "line",
                            json!({ "id": "$item.job.id", "line": "$item.line.line" }),
                        ),
                    ],
                ),
            ],
        )])
    };
    let input = vec![jobs(&[("a", 3), ("b", 2), ("c", 2)])];
    let sweep = sweep_saved(def, &input, 4).await;
    assert!(sweep.points > 20, "{sweep:?}");
}
