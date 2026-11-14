//! Every output a pipe hands out has an id. The id is the same when the
//! same output is handed out again after a restart, at any kill, from a
//! journal or from a bookmark, inside a group of an `each` that merges and
//! inside a vine run by `use`. Two outputs of one run never share an id,
//! even with the same name and payload.
//!
//! The vines are built in Rust, so this file passes with the YAML feature
//! off. The acts take time on tokio's paused clock, so which item finishes
//! when is exact and the same every run.

mod acts;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use acts::desk::{desk_registry, job, job_event, jobs, step, Desk};
use futures::{stream, StreamExt};
use vine_lang::pipe::{
    call, each, emit_with, filter, fork, last, pipe, split, use_vine, Overlap, Pipe, PipeDef, Stage,
};
use vine_lang::{
    json, Bookmark, Event, Journal, OutputId, PipeOutput, Progress, Registry, Shape, Ty, Value,
};

// ------------------------------------------------------------- the vines

fn id_shape() -> Shape {
    Shape::new().required("id", Ty::Text)
}

/// One job's work: its first step, a note, its second step, the same
/// note again. So each job gives `noted { id }` twice, with the same
/// payload.
fn twice_work() -> Vec<Stage> {
    let noted = || emit_with("noted", json!({ "id": "$item.job.id" }));
    vec![
        step("one", json!("$item.job.secs")),
        fork([[noted()]]),
        step("two", json!(1)),
        noted(),
    ]
}

fn merging(work: Vec<Stage>) -> Stage {
    each(Overlap::Merge, work).at_once(2)
}

/// One item that lists jobs; the jobs run side by side.
fn twice_over_jobs() -> PipeDef {
    pipe("twice")
        .input(Shape::new().required("jobs", Ty::List))
        .output("noted", id_shape())
        .stages([split("$item.jobs").into("job"), merging(twice_work())])
}

/// A stream of jobs, one per input item; the items run side by side.
fn twice_over_a_stream() -> PipeDef {
    pipe("twice_stream")
        .input(Shape::new().required("job", Ty::Object))
        .output("noted", id_shape())
        .stages([merging(twice_work())])
}

/// `twice_over_jobs` run by `use`, its notes passed on.
fn using_twice() -> PipeDef {
    pipe("outer")
        .input(Shape::new().required("jobs", Ty::List))
        .output("passed", id_shape())
        .stages([
            use_vine("twice", None).on("noted", [emit_with("passed", json!({ "id": "$item.id" }))])
        ])
}

/// Each job gives as many lines as its `secs` from a stream act saved by
/// position, and each line is noted twice.
fn lines_twice() -> PipeDef {
    let line = || {
        emit_with(
            "line",
            json!({ "id": "$item.job.id", "line": "$item.line.line" }),
        )
    };
    pipe("lines")
        .input(Shape::new().required("jobs", Ty::List))
        .output("line", id_shape().required("line", Ty::Int))
        .stages([
            split("$item.jobs").into("job"),
            merging(vec![
                call(
                    "work.lines",
                    json!({ "id": "$item.job.id", "lines": "$item.job.secs" }),
                )
                .into("line"),
                fork([[line()]]),
                line(),
            ]),
        ])
}

/// For each group of jobs in turn: the jobs side by side, each a stream
/// act saved by position, then `last` and one summary of the group. The
/// summary is made from the end item of the call `last` picked; once that
/// call's position passes it, the line that recorded the summary is
/// dropped. The next group's summary must keep its id all the same.
fn summaries() -> PipeDef {
    pipe("summaries")
        .input(Shape::new().required("groups", Ty::List))
        .output("all", Shape::new().required("group", Ty::List))
        .stages([
            split("$item.groups").into("group"),
            each(
                Overlap::Concat,
                [
                    split("$item.group").into("job"),
                    merging(vec![
                        call(
                            "work.lines",
                            json!({ "id": "$item.job.id", "lines": "$item.job.secs" }),
                        )
                        .into("line")
                        .end("ended"),
                        filter("$item.ended"),
                    ]),
                    last(),
                    emit_with("all", json!({ "group": "$item.group" })),
                ],
            ),
        ])
}

/// The jobs side by side, then `last`, and the id of the job it picked.
fn last_job() -> PipeDef {
    pipe("last_job")
        .input(Shape::new().required("jobs", Ty::List))
        .output("finished", id_shape())
        .stages([
            split("$item.jobs").into("job"),
            merging(vec![step("one", json!("$item.job.secs"))]),
            last(),
            emit_with("finished", json!({ "id": "$item.job.id" })),
        ])
}

fn registry(desk: &Arc<Desk>) -> Registry {
    let mut reg = desk_registry(desk);
    let twice = reg.build_pipe(twice_over_jobs()).unwrap();
    reg.add_vine(twice).unwrap();
    reg
}

fn built(def: PipeDef) -> Pipe {
    registry(&Desk::new()).build_pipe(def).unwrap()
}

/// Three jobs, two of them `a`, so two give equal payloads. `a` takes two
/// seconds and `b` one: they do not finish in the order they go in.
fn three_jobs() -> Vec<Value> {
    vec![jobs(&[("a", 2), ("b", 1), ("a", 1)])]
}

fn three_job_events() -> Vec<Value> {
    vec![job_event("a", 2), job_event("b", 1), job_event("a", 1)]
}

// ------------------------------------------------------------ the caller

#[derive(Debug, Clone, Copy)]
enum Mode {
    Journal,
    Position,
}

/// What a caller keeps between runs, as a store would hold it.
fn fresh(mode: Mode) -> Value {
    match mode {
        Mode::Journal => Journal::new().to_json(),
        Mode::Position => Bookmark::new().to_json(),
    }
}

struct Run {
    outs: Vec<PipeOutput>,
    /// For each output, how many events the run had given with it.
    out_at: Vec<usize>,
    events: usize,
    ended: bool,
}

/// One run, as a caller makes it: read what it kept back from JSON, feed
/// the input (after the position, for a bookmark), record and save after
/// every event, and stop after `kill` events.
async fn run(
    mode: Mode,
    vine: &Pipe,
    input: &[Value],
    kept: &mut Value,
    kill: Option<usize>,
) -> Run {
    let mut got = Run {
        outs: Vec::new(),
        out_at: Vec::new(),
        events: 0,
        ended: false,
    };
    match mode {
        Mode::Journal => {
            let mut journal = Journal::from_json(kept).unwrap();
            let mut events = vine.run_saved(stream::iter(input.to_vec()), journal.clone());
            while let Some(event) = events.next().await {
                let event = event.unwrap();
                got.events += 1;
                if let Event::Output(out) = &event {
                    got.outs.push(out.clone());
                    got.out_at.push(got.events);
                }
                journal.record(&event);
                *kept = journal.to_json();
                if Some(got.events) == kill {
                    return got;
                }
            }
        }
        Mode::Position => {
            let mut bookmark = Bookmark::from_json(kept).unwrap();
            let from = usize::try_from(bookmark.position())
                .unwrap()
                .min(input.len());
            let mut events = vine.run_from(stream::iter(input[from..].to_vec()), bookmark.clone());
            while let Some(progress) = events.next().await {
                let progress = progress.unwrap();
                got.events += 1;
                if let Progress::Output(out) = &progress {
                    got.outs.push(out.clone());
                    got.out_at.push(got.events);
                }
                bookmark.record(&progress);
                *kept = bookmark.to_json();
                if Some(got.events) == kill {
                    return got;
                }
            }
        }
    }
    got.ended = true;
    got
}

fn ids(outs: &[PipeOutput]) -> Vec<OutputId> {
    outs.iter().map(|o| o.id.clone()).collect()
}

fn assert_unique(outs: &[PipeOutput], why: &str) {
    let set: BTreeSet<OutputId> = ids(outs).into_iter().collect();
    assert_eq!(
        set.len(),
        outs.len(),
        "{why}: ids repeat in {:?}",
        ids(outs)
    );
}

/// Each id with the output it names.
fn by_id(outs: &[PipeOutput]) -> BTreeMap<OutputId, (String, Value)> {
    outs.iter()
        .map(|o| (o.id.clone(), (o.name.clone(), o.payload.clone())))
        .collect()
}

/// Kill a run after every count of events an uninterrupted run gives, and
/// resume it. Across the two runs, every id names the output it named in
/// the uninterrupted run, every output of that run is handed out under
/// its id, and at most one output, the one handed out right before the
/// kill, comes again. Gives how many kills brought one again.
async fn ids_hold_at_every_kill(mode: Mode, vine: impl Fn() -> Pipe, input: &[Value]) -> usize {
    let clean = run(mode, &vine(), input, &mut fresh(mode), None).await;
    assert!(clean.ended);
    assert_unique(&clean.outs, "uninterrupted");
    let want = by_id(&clean.outs);
    let mut again = 0;
    for k in 1..clean.events {
        let mut kept = fresh(mode);
        let before = run(mode, &vine(), input, &mut kept, Some(k)).await;
        let after = run(mode, &vine(), input, &mut kept, None).await;
        assert!(after.ended, "{mode:?}, kill at {k}");
        assert_unique(&before.outs, "before the kill");
        assert_unique(&after.outs, "after the kill");
        let both: Vec<OutputId> = ids(&before.outs)
            .into_iter()
            .filter(|id| ids(&after.outs).contains(id))
            .collect();
        assert!(
            both.len() <= 1,
            "{mode:?}, kill at {k}: {both:?} came again"
        );
        again += both.len();
        let mut seen = by_id(&before.outs);
        for (id, out) in by_id(&after.outs) {
            if let Some(was) = seen.insert(id.clone(), out.clone()) {
                assert_eq!(
                    was, out,
                    "{mode:?}, kill at {k}: {id} came again as another output"
                );
            }
        }
        assert_eq!(seen, want, "{mode:?}, kill at {k}");
    }
    again
}

// ---------------------------------------------------------------- tests

#[tokio::test(start_paused = true)]
async fn a_plain_run_gives_each_output_its_own_id_even_with_the_same_payload() {
    let outs = built(twice_over_jobs())
        .run_one(jobs(&[("a", 1), ("a", 1)]))
        .await
        .unwrap();
    assert_eq!(outs.len(), 4);
    assert!(outs.iter().all(|o| o.payload == json!({ "id": "a" })));
    assert_unique(&outs, "plain run");
}

#[tokio::test(start_paused = true)]
async fn both_ways_to_resume_give_each_output_its_own_id() {
    for mode in [Mode::Journal, Mode::Position] {
        let got = run(
            mode,
            &built(twice_over_jobs()),
            &three_jobs(),
            &mut fresh(mode),
            None,
        )
        .await;
        assert_eq!(got.outs.len(), 6, "{mode:?}");
        assert_unique(&got.outs, "saved run");
        let got = run(
            mode,
            &built(twice_over_a_stream()),
            &three_job_events(),
            &mut fresh(mode),
            None,
        )
        .await;
        assert_eq!(got.outs.len(), 6, "{mode:?}");
        assert_unique(&got.outs, "saved run over a stream");
    }
}

#[tokio::test(start_paused = true)]
async fn an_id_is_the_same_after_a_kill_inside_groups_of_a_merge() {
    for mode in [Mode::Journal, Mode::Position] {
        let again = ids_hold_at_every_kill(mode, || built(twice_over_jobs()), &three_jobs()).await;
        assert!(
            again > 0,
            "{mode:?}: no kill fell between an output and its line"
        );
        let again =
            ids_hold_at_every_kill(mode, || built(twice_over_a_stream()), &three_job_events())
                .await;
        assert!(again > 0, "{mode:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn an_id_is_the_same_after_a_kill_inside_a_vine_run_by_use() {
    for mode in [Mode::Journal, Mode::Position] {
        let again = ids_hold_at_every_kill(mode, || built(using_twice()), &three_jobs()).await;
        assert!(again > 0, "{mode:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn an_id_is_the_same_after_a_kill_for_outputs_saved_by_position() {
    let vine = built(lines_twice());
    assert!(
        vine.saves_every_item().is_empty(),
        "{:?}",
        vine.saves_every_item()
    );
    let input = vec![jobs(&[("a", 3), ("b", 2), ("a", 2)])];
    for mode in [Mode::Journal, Mode::Position] {
        let again = ids_hold_at_every_kill(mode, || built(lines_twice()), &input).await;
        assert!(again > 0, "{mode:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_summary_whose_line_was_dropped_does_not_shift_the_ids_after_it() {
    let input = vec![json!({ "groups": [
        [job("a", 2), job("b", 1)],
        [job("c", 1), job("d", 2)],
    ] })];
    for mode in [Mode::Journal, Mode::Position] {
        ids_hold_at_every_kill(mode, || built(summaries()), &input).await;
    }
}

#[tokio::test(start_paused = true)]
async fn last_after_a_merge_keeps_its_id_when_a_restart_picks_another_item() {
    // `b` finishes first and `a` last, so `finished` names `a`. Kill right
    // after it is handed out, before its line is saved: the replay runs
    // from saved results, the jobs end in the order they went in, and
    // `last` picks `b`. The output comes again under the same id, with
    // another payload. A caller that drops repeats by id keeps `a`.
    let input = vec![jobs(&[("a", 3), ("b", 1)])];
    for mode in [Mode::Journal, Mode::Position] {
        let clean = run(mode, &built(last_job()), &input, &mut fresh(mode), None).await;
        let [at] = clean.out_at[..] else {
            panic!("{mode:?}: {:?}", clean.outs)
        };
        let mut kept = fresh(mode);
        let before = run(mode, &built(last_job()), &input, &mut kept, Some(at)).await;
        let after = run(mode, &built(last_job()), &input, &mut kept, None).await;
        let [first] = &before.outs[..] else {
            panic!("{mode:?}: {:?}", before.outs)
        };
        let [again] = &after.outs[..] else {
            panic!("{mode:?}: {:?}", after.outs)
        };
        assert_eq!(first.payload, json!({ "id": "a" }), "{mode:?}");
        assert_eq!(again.payload, json!({ "id": "b" }), "{mode:?}");
        assert_eq!(again.id, first.id, "{mode:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_journal_saved_before_outputs_had_ids_resumes_with_the_ids_of_a_fresh_run() {
    // Written as the library wrote journals before ids: the first job's
    // first step and its note. Nothing in it names an id.
    let old = json!([
        { "kind": "done", "at": "pipe[1].each@0/pipe[1].each.do[0]#0",
          "input": { "id": "a", "step": "one", "secs": 1 },
          "result": { "id": "a", "step": "one" } },
        { "kind": "emitted", "output": "noted", "within": "pipe[1].each@0/" },
    ]);
    let input = vec![jobs(&[("a", 1)])];
    let clean = run(
        Mode::Journal,
        &built(twice_over_jobs()),
        &input,
        &mut fresh(Mode::Journal),
        None,
    )
    .await;
    let mut kept = old;
    let resumed = run(
        Mode::Journal,
        &built(twice_over_jobs()),
        &input,
        &mut kept,
        None,
    )
    .await;
    assert!(resumed.ended);
    assert_eq!(resumed.outs, clean.outs[1..]);
}

#[cfg(feature = "yaml")]
#[tokio::test(start_paused = true)]
async fn the_same_vine_as_yaml_gives_the_same_ids() {
    let text = "vine: twice\n\
        input: { jobs: list }\n\
        outputs: { noted: { id: text } }\n\
        pipe:\n  - split: $item.jobs\n    into: job\n  \
        - each:\n      overlap: merge\n      at_once: 2\n      do:\n        \
        - call: work.step\n          with: { id: $item.job.id, step: one, secs: $item.job.secs }\n          into: one\n        \
        - fork:\n            - - emit: { to: noted, with: { id: $item.job.id } }\n        \
        - call: work.step\n          with: { id: $item.job.id, step: two, secs: 1 }\n          into: two\n        \
        - emit: { to: noted, with: { id: $item.job.id } }\n";
    let from_yaml = || registry(&Desk::new()).load_pipe(text).unwrap();
    for mode in [Mode::Journal, Mode::Position] {
        let rust = run(
            mode,
            &built(twice_over_jobs()),
            &three_jobs(),
            &mut fresh(mode),
            None,
        )
        .await;
        let yaml = run(mode, &from_yaml(), &three_jobs(), &mut fresh(mode), None).await;
        assert_eq!(yaml.outs, rust.outs, "{mode:?}");
        ids_hold_at_every_kill(mode, from_yaml, &three_jobs()).await;
    }
    let rust = built(twice_over_jobs())
        .run_one(three_jobs()[0].clone())
        .await
        .unwrap();
    let yaml = from_yaml().run_one(three_jobs()[0].clone()).await.unwrap();
    assert_eq!(yaml, rust);
}
