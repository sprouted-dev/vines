//! A `timeout` survives a restart, in `run_saved` and in `run_from`. What
//! the clock decided for each run of the operators a timeout applies to is saved, so a
//! replay never asks the clock about a decision already made:
//!
//! - a timeout that fired fires again at once, and the work it applied to is
//!   not run again;
//! - a timeout whose work was done in time is replayed as done, however
//!   slow the replay;
//! - with no decision saved, the wait starts again from nothing, and work
//!   whose results were saved does not count against it.
//!
//! The operators take time on tokio's paused clock, so which step finishes
//! when, and when a timeout fires, is exact and the same every run.
//! Nothing here uses YAML, and this file passes with the feature off:
//!
//!     cargo test --no-default-features --test timeout_restart

mod fake;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fake::desk::{desk_registry, jobs_vine, merge_jobs, step, Desk};
use futures::{stream, StreamExt};
use tokio::time::Instant;
use vine_lang::pipe::{
    branch, call, catch_error, emit_with, fork, pipe, retry, timeout, use_vine, Operator, Pipe,
    PipeDef,
};
use vine_lang::{
    json, Bookmark, Entry, Event, Journal, OutputId, PipeOutput, Progress, Registry, RunError,
    Shape, Timing, Ty, Value,
};

const LIMIT: Duration = Duration::from_secs(3);

// ------------------------------------------------------------- the vines

/// A job: its first step takes `secs`, its second `two`.
fn job(id: &str, secs: u64, two: u64) -> Value {
    json!({ "id": id, "secs": secs, "two": two })
}

/// `a` times out while its first step runs, `b` is done in time, and `c`
/// times out while its second step runs, after its first was done.
fn three() -> [Value; 3] {
    [job("a", 5, 1), job("b", 1, 1), job("c", 1, 5)]
}

fn jobs_input(list: &[Value]) -> Vec<Value> {
    vec![json!({ "jobs": list })]
}

fn job_events(list: &[Value]) -> Vec<Value> {
    list.iter().map(|j| json!({ "job": j })).collect()
}

fn done() -> Operator {
    emit_with("done", json!({ "id": "$item.job.id" }))
}

fn failed() -> Operator {
    emit_with(
        "failed",
        json!({ "id": "$item.job.id", "code": "$item.error.code" }),
    )
}

/// One job's work: step `one`, a `stepped` note, step `two`, all under a
/// three second timeout, then `done`. A `catchError` turns a timeout into
/// `failed`. The note is made inside the timeout, so it is held until the
/// work is done, and dropped if the timeout fires.
fn timed_work() -> Vec<Operator> {
    vec![
        step("one", json!("$item.job.secs")),
        fork([[emit_with(
            "stepped",
            json!({ "id": "$item.job.id", "step": "one" }),
        )]]),
        step("two", json!("$item.job.two")),
        timeout(LIMIT),
        done(),
        catch_error([failed()]),
    ]
}

/// The jobs side by side, two at once, over one input item that lists them.
fn timed_jobs() -> PipeDef {
    jobs_vine([merge_jobs(Some(2), timed_work())])
}

/// The same work over an endless stream of jobs, two at once.
fn timed_stream() -> PipeDef {
    over_a_stream(vec![merge_jobs(Some(2), timed_work())])
}

/// A vine over a stream of jobs, one per input item `{ job }`, with the
/// outputs `jobs_vine` has.
fn over_a_stream(operators: Vec<Operator>) -> PipeDef {
    let id = || Shape::new().required("id", Ty::Text);
    pipe("timed_stream")
        .input(Shape::new().required("job", Ty::Object))
        .output("stepped", id().required("step", Ty::Text))
        .output("done", id())
        .output("failed", id().required("code", Ty::Text))
        .output("line", id().required("line", Ty::Int))
        .operators(operators)
}

fn built(desk: &Arc<Desk>, def: PipeDef) -> Pipe {
    registry(desk).build_pipe(def).unwrap()
}

/// The desk's operators, and `timed_job`: one job's timed work as a vine of
/// its own, for `use`.
fn registry(desk: &Arc<Desk>) -> Registry {
    let mut reg = desk_registry(desk);
    let id = || Shape::new().required("id", Ty::Text);
    let inner = pipe("timed_job")
        .input(Shape::new().required("job", Ty::Object))
        .output("stepped", id().required("step", Ty::Text))
        .output("done", id())
        .output("failed", id().required("code", Ty::Text))
        .operators(timed_work());
    let inner = reg.build_pipe(inner).unwrap();
    reg.add_vine(inner).unwrap();
    reg
}

// ------------------------------------------------------------- a caller

/// Which way the caller resumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Journal,
    Position,
}

/// Everything a caller saw over one or more runs, and what it kept.
struct Caller {
    mode: Mode,
    /// The journal or bookmark, as a store would hold it.
    saved: Value,
    /// Every output it was handed, repeats included.
    handed: Vec<PipeOutput>,
    /// The most lines it kept at once.
    most: usize,
}

impl Caller {
    fn new(mode: Mode) -> Caller {
        let saved = match mode {
            Mode::Journal => Journal::new().to_json(),
            Mode::Position => Bookmark::new().to_json(),
        };
        Caller {
            mode,
            saved,
            handed: Vec::new(),
            most: 0,
        }
    }

    fn bookmark(&self) -> Bookmark {
        Bookmark::from_json(&self.saved).unwrap()
    }

    fn journal(&self) -> Journal {
        Journal::from_json(&self.saved).unwrap()
    }

    /// Run once as a caller would, saving after every event, and stop
    /// after `kill` events. Gives the events of this run, or the error it
    /// ended with.
    async fn run(
        &mut self,
        vine: &Pipe,
        input: &[Value],
        kill: Option<usize>,
    ) -> Result<Vec<Seen>, RunError> {
        let mut seen = Vec::new();
        match self.mode {
            Mode::Journal => {
                let mut journal = self.journal();
                let mut run = vine.run_saved(stream::iter(input.to_vec()), journal.clone());
                while let Some(event) = run.next().await {
                    let event = event?;
                    seen.push(match &event {
                        Event::Output(o) => {
                            self.handed.push(o.clone());
                            Seen::Output(o.clone())
                        }
                        Event::Save(entry) => Seen::Entry(entry.clone()),
                    });
                    journal.record(&event);
                    self.saved = journal.to_json();
                    self.most = self.most.max(journal.len());
                    if Some(seen.len()) == kill {
                        break;
                    }
                }
            }
            Mode::Position => {
                let mut bookmark = self.bookmark();
                let from = usize::try_from(bookmark.position()).unwrap();
                let feed = input[from.min(input.len())..].to_vec();
                let mut run = vine.run_from(stream::iter(feed), bookmark.clone());
                while let Some(progress) = run.next().await {
                    let progress = progress?;
                    seen.push(match &progress {
                        Progress::Output(o) => {
                            self.handed.push(o.clone());
                            Seen::Output(o.clone())
                        }
                        Progress::Save(line) => Seen::Entry(line.entry.clone()),
                        Progress::Through(_) | Progress::Done(_) => Seen::Moved,
                    });
                    bookmark.record(&progress);
                    self.saved = bookmark.to_json();
                    self.most = self.most.max(bookmark.len());
                    if Some(seen.len()) == kill {
                        break;
                    }
                }
            }
        }
        Ok(seen)
    }
}

/// One event a run gave.
#[derive(Debug, Clone, PartialEq)]
enum Seen {
    Output(PipeOutput),
    Entry(Entry),
    /// The position moved, or an item was done past it.
    Moved,
}

/// Each output by its id. An id handed out twice carries the same output
/// both times.
fn by_id(handed: &[PipeOutput], at: &str) -> BTreeMap<OutputId, (String, Value)> {
    let mut by = BTreeMap::new();
    for o in handed {
        let now = (o.name.clone(), o.payload.clone());
        if let Some(was) = by.insert(o.id.clone(), now.clone()) {
            assert_eq!(was, now, "{at}: id {} named two outputs", o.id);
        }
    }
    by
}

/// How many more steps ran than in the clean run. Every step of the clean
/// run ran, and no step ran that the clean run did not run.
fn extra_calls(calls: &[String], clean: &[String], at: &str) -> usize {
    let mut left = calls.to_vec();
    for c in clean {
        let i = left.iter().position(|l| l == c);
        left.remove(i.unwrap_or_else(|| panic!("{at}: `{c}` never ran: {calls:?}")));
    }
    assert!(
        left.iter().all(|c| clean.contains(c)),
        "{at}: steps the clean run never ran: {left:?}"
    );
    left.len()
}

/// What one sweep runs: the vine, its input, how to set up each desk, and
/// how many steps at most one kill may run again.
struct Sweep<'a> {
    def: &'a dyn Fn() -> PipeDef,
    input: &'a [Value],
    mode: Mode,
    setup: &'a dyn Fn(&Desk),
    again: usize,
}

impl Sweep<'_> {
    fn desk(&self) -> Arc<Desk> {
        let desk = Desk::new();
        (self.setup)(&desk);
        desk
    }

    /// A run with no kill: its outputs by id, the steps it called, and how
    /// many events it gave.
    async fn clean(&self) -> (BTreeMap<OutputId, (String, Value)>, Vec<String>, usize) {
        let desk = self.desk();
        let mut caller = Caller::new(self.mode);
        let seen = caller
            .run(&built(&desk, (self.def)()), self.input, None)
            .await
            .unwrap();
        (by_id(&caller.handed, "clean"), desk.calls(), seen.len())
    }

    /// Kill after each count of events in `kills` in turn, restart from
    /// what the caller saved, then run to the end, and check the end is
    /// the clean run's.
    async fn kills(
        &self,
        kills: &[usize],
        clean: &(BTreeMap<OutputId, (String, Value)>, Vec<String>),
    ) {
        let desk = self.desk();
        let vine = || built(&desk, (self.def)());
        let mut caller = Caller::new(self.mode);
        let at = format!("{:?}, kills {kills:?}", self.mode);
        for &k in kills {
            let seen = caller.run(&vine(), self.input, Some(k)).await;
            let seen = seen.unwrap_or_else(|e| panic!("{at}: {e}"));
            if seen.len() < k {
                break;
            }
        }
        let end = caller.run(&vine(), self.input, None).await;
        end.unwrap_or_else(|e| panic!("{at}: {e}"));
        let outs = by_id(&caller.handed, &at);
        assert_eq!(outs, clean.0, "{at}");
        let repeats = caller.handed.len() - outs.len();
        assert!(repeats <= kills.len(), "{at}: {repeats} repeats");
        let extra = extra_calls(&desk.calls(), &clean.1, &at);
        assert!(
            extra <= self.again * kills.len(),
            "{at}: {extra} steps again: {:?}",
            desk.log()
        );
        if self.mode == Mode::Position {
            let bookmark = caller.bookmark();
            assert_eq!(bookmark.position(), self.input.len() as u64, "{at}");
            assert!(bookmark.is_empty(), "{at}: {:?}", bookmark.lines());
        }
    }

    /// Kill at every event, and with `pairs`, at every pair of events.
    /// Gives how many runs it made.
    async fn run(&self, pairs: bool) -> usize {
        let (outs, calls, points) = self.clean().await;
        let clean = (outs, calls);
        let mut runs = 0;
        for first in 1..=points {
            self.kills(&[first], &clean).await;
            runs += 1;
            if pairs {
                for second in 1..=points {
                    self.kills(&[first, second], &clean).await;
                    runs += 1;
                }
            }
        }
        runs
    }
}

fn plain(_: &Desk) {}

// ---------------------------------------------------- what is saved, once

#[tokio::test(start_paused = true)]
async fn a_clean_run_saves_each_decision_before_what_depends_on_it() {
    let desk = Desk::new();
    let mut caller = Caller::new(Mode::Journal);
    let seen = caller
        .run(&built(&desk, timed_jobs()), &jobs_input(&three()), None)
        .await
        .unwrap();
    let names: Vec<String> = caller
        .handed
        .iter()
        .map(|o| format!("{} {}", o.name, o.payload["id"].as_str().unwrap()))
        .collect();
    // `a` and `c` time out. `stepped c` was made inside the timeout and
    // dropped with the work it waited on.
    assert_eq!(names, ["stepped b", "done b", "failed a", "failed c"]);
    let timings: Vec<(String, Timing)> = seen
        .iter()
        .filter_map(|s| match s {
            Seen::Entry(Entry::Timed { at, timing }) => Some((at.clone(), *timing)),
            _ => None,
        })
        .collect();
    let scope = |n: u32| format!("pipe[1].mergeMap@{n}/pipe[1].mergeMap.do[3].timeout@0");
    assert_eq!(
        timings,
        [
            (scope(1), Timing::InTime),
            (scope(0), Timing::TimedOut),
            (scope(2), Timing::TimedOut),
        ]
    );
    // Each decision comes before the output that follows from it.
    let place = |want: &dyn Fn(&Seen) -> bool| seen.iter().position(want).unwrap();
    let decided_b =
        place(&|s| matches!(s, Seen::Entry(Entry::Timed { at, .. }) if *at == scope(1)));
    let done_b = place(&|s| matches!(s, Seen::Output(o) if o.name == "done"));
    let stepped_b = place(&|s| matches!(s, Seen::Output(o) if o.name == "stepped"));
    assert!(decided_b < stepped_b && stepped_b < done_b, "{seen:?}");
    // The operators a timeout applies to are keyed by its run.
    assert!(seen
        .iter()
        .any(|s| matches!(s, Seen::Entry(Entry::Done { at, .. })
        if *at == format!("{}/pipe[1].mergeMap.do[0]#0", scope(1)))));
}

#[tokio::test(start_paused = true)]
async fn a_timeout_that_fired_fires_again_at_once_and_runs_none_of_its_work() {
    let desk = Desk::new();
    let input = jobs_input(&[job("c", 1, 5)]);
    let vine = || built(&desk, timed_jobs());
    let mut caller = Caller::new(Mode::Journal);
    // Stop right after the decision was saved, before `failed` comes.
    let seen = caller.run(&vine(), &input, None).await.unwrap();
    let decided = seen
        .iter()
        .position(|s| matches!(s, Seen::Entry(Entry::Timed { .. })))
        .unwrap();
    let desk = Desk::new();
    let vine = || built(&desk, timed_jobs());
    let mut caller = Caller::new(Mode::Journal);
    caller
        .run(&vine(), &input, Some(decided + 1))
        .await
        .unwrap();
    assert!(caller.handed.is_empty());
    let calls = desk.calls();
    assert_eq!(calls, ["c.one", "c.two"]);
    let restart = Instant::now();
    caller.run(&vine(), &input, None).await.unwrap();
    assert_eq!(restart.elapsed(), Duration::ZERO, "it did not wait");
    assert_eq!(desk.calls(), calls, "no step ran again");
    assert_eq!(caller.handed.len(), 1);
    assert_eq!(caller.handed[0].name, "failed");
    assert_eq!(caller.handed[0].payload["code"], "timeout");
}

// ------------------------------------------------ the rule before deciding

/// `[one, two, timeout(3s), done, catchError failed]` over one job, saved: the
/// events, the time each came, and the steps called.
fn one_job(secs: u64, two: u64) -> (impl Fn(&Arc<Desk>) -> Pipe, Vec<Value>) {
    let vine = |desk: &Arc<Desk>| built(desk, timed_jobs());
    (vine, jobs_input(&[job("x", secs, two)]))
}

/// Run once and stop right after the first step's result was saved.
async fn killed_after_step_one(
    desk: &Arc<Desk>,
    vine: &impl Fn(&Arc<Desk>) -> Pipe,
    input: &[Value],
) -> Caller {
    let mut probe = Caller::new(Mode::Journal);
    let seen = probe.run(&vine(&Desk::new()), input, None).await.unwrap();
    let one = seen
        .iter()
        .position(
            |s| matches!(s, Seen::Entry(Entry::Done { result, .. }) if result["step"] == "one"),
        )
        .unwrap();
    let mut caller = Caller::new(Mode::Journal);
    caller.run(&vine(desk), input, Some(one + 1)).await.unwrap();
    caller
}

#[tokio::test(start_paused = true)]
async fn a_restart_before_the_timeout_decided_starts_the_wait_again() {
    // `two` takes ten seconds, far past the limit, so it times out either
    // way. Killed one second in, after `one` was saved, the restart waits
    // the whole three seconds again for `two`, not the two left.
    let (vine, input) = one_job(1, 10);
    let desk = Desk::new();
    let mut caller = killed_after_step_one(&desk, &vine, &input).await;
    let restart = Instant::now();
    let seen = caller.run(&vine(&desk), &input, None).await.unwrap();
    assert!(matches!(
        seen.first(),
        Some(Seen::Entry(Entry::Timed {
            timing: Timing::TimedOut,
            ..
        }))
    ));
    assert_eq!(restart.elapsed(), LIMIT);
    // `one` was saved and not run again. `two` had not started at the
    // kill: an operator starts only once the caller holds the line before it.
    assert_eq!(desk.calls(), ["x.one", "x.two"]);
    assert_eq!(caller.handed[0].name, "failed");
}

#[tokio::test(start_paused = true)]
async fn work_saved_before_a_restart_does_not_count_against_the_wait() {
    // Two seconds each: the first run would time out at three, during
    // `two`. Killed after `one` was saved, the restart gives `two` the
    // whole limit, and it is done in time. Nothing the first run handed
    // out depended on a decision, so this is a run at another pace.
    let (vine, input) = one_job(2, 2);
    let clean = {
        let mut c = Caller::new(Mode::Journal);
        c.run(&vine(&Desk::new()), &input, None).await.unwrap();
        c.handed
    };
    assert_eq!(clean[0].name, "failed");
    let desk = Desk::new();
    let mut caller = killed_after_step_one(&desk, &vine, &input).await;
    caller.run(&vine(&desk), &input, None).await.unwrap();
    let names: Vec<&str> = caller.handed.iter().map(|o| o.name.as_str()).collect();
    assert_eq!(names, ["stepped", "done"]);
}

// -------------------------------------- done in time, replayed slower

/// Each job's lines from a stream operator saved by position, under the
/// timeout, two jobs at once. A job that times out is `failed`.
fn timed_lines_operators() -> Vec<Operator> {
    vec![merge_jobs(
        Some(2),
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
            timeout(LIMIT),
            catch_error([failed()]),
        ],
    )]
}

fn timed_lines() -> PipeDef {
    jobs_vine(timed_lines_operators())
}

/// Kill a run of two lines right before or right after its decision was
/// saved, make the lines ten times slower, and restart.
async fn lines_slower_after(kill_after_decision: bool) -> Vec<PipeOutput> {
    let input = jobs_input(&[job("x", 2, 0)]);
    let mut probe = Caller::new(Mode::Journal);
    let seen = probe
        .run(&built(&Desk::new(), timed_lines()), &input, None)
        .await
        .unwrap();
    let decided = seen
        .iter()
        .position(|s| {
            matches!(
                s,
                Seen::Entry(Entry::Timed {
                    timing: Timing::InTime,
                    ..
                })
            )
        })
        .unwrap();
    let kill = if kill_after_decision {
        decided + 1
    } else {
        decided
    };
    let desk = Desk::new();
    let mut caller = Caller::new(Mode::Journal);
    caller
        .run(&built(&desk, timed_lines()), &input, Some(kill))
        .await
        .unwrap();
    assert!(caller.handed.is_empty(), "the lines are held until decided");
    desk.pace_lines(10);
    let restart = Instant::now();
    caller
        .run(&built(&desk, timed_lines()), &input, None)
        .await
        .unwrap();
    if kill_after_decision {
        assert!(restart.elapsed() >= Duration::from_secs(20));
    }
    caller.handed
}

#[tokio::test(start_paused = true)]
async fn work_done_in_time_is_replayed_as_done_even_when_the_replay_is_slower() {
    let names = |outs: &[PipeOutput]| -> Vec<String> {
        outs.iter()
            .map(|o| format!("{} {}", o.name, o.payload["line"]))
            .collect()
    };
    let replayed = lines_slower_after(true).await;
    assert_eq!(names(&replayed), ["line 1", "line 2"]);
    // Killed before the decision was saved, the slower replay decides
    // again, and times out.
    let decided_again = lines_slower_after(false).await;
    assert_eq!(names(&decided_again), ["failed null"]);
}

#[tokio::test(start_paused = true)]
async fn a_timeout_is_saved_before_the_stream_operator_it_drops_says_it_was_abandoned() {
    let input = jobs_input(&[job("x", 10, 0)]);
    let mut caller = Caller::new(Mode::Journal);
    let seen = caller
        .run(&built(&Desk::new(), timed_lines()), &input, None)
        .await
        .unwrap();
    let place = |want: &dyn Fn(&Entry) -> bool| {
        seen.iter()
            .position(|s| matches!(s, Seen::Entry(e) if want(e)))
            .unwrap()
    };
    let decided = place(&|e| {
        matches!(
            e,
            Entry::Timed {
                timing: Timing::TimedOut,
                ..
            }
        )
    });
    let abandoned = place(&|e| {
        matches!(
            e,
            Entry::Stopped {
                how: vine_lang::Ending::Abandoned,
                ..
            }
        )
    });
    assert!(decided < abandoned, "{seen:?}");
}

// ----------------------------------------------------------- kill sweeps

#[tokio::test(start_paused = true)]
async fn a_run_that_ends_survives_a_kill_at_every_event_and_every_pair() {
    let input = jobs_input(&three());
    let sweep = Sweep {
        def: &timed_jobs,
        input: &input,
        mode: Mode::Journal,
        setup: &plain,
        again: 2,
    };
    let runs = sweep.run(true).await;
    assert!(runs > 200, "{runs} runs");
}

#[tokio::test(start_paused = true)]
async fn an_endless_stream_survives_a_kill_at_every_event_and_every_pair() {
    let input = job_events(&three());
    let sweep = Sweep {
        def: &timed_stream,
        input: &input,
        mode: Mode::Position,
        setup: &plain,
        again: 2,
    };
    let runs = sweep.run(true).await;
    assert!(runs > 200, "{runs} runs");
}

#[tokio::test(start_paused = true)]
async fn a_stream_operator_under_a_timeout_survives_a_kill_at_every_event() {
    // `x` gives two lines in time; `y` gives ten, and times out after
    // three while its call is still going.
    let input = jobs_input(&[job("x", 2, 0), job("y", 10, 0)]);
    for mode in [Mode::Journal, Mode::Position] {
        let stream_input = job_events(&[job("x", 2, 0), job("y", 10, 0)]);
        let (def, input): (&dyn Fn() -> PipeDef, &[Value]) = match mode {
            Mode::Journal => (&timed_lines, &input),
            Mode::Position => (&timed_lines_stream, &stream_input),
        };
        let sweep = Sweep {
            def,
            input,
            mode,
            setup: &plain,
            again: 2,
        };
        sweep.run(false).await;
    }
}

fn timed_lines_stream() -> PipeDef {
    over_a_stream(timed_lines_operators())
}

// ------------------------------------------- with `use`, `catchError`, `retry`

/// `timed_job` run by `use` for each job, two at once.
fn used_operators() -> Vec<Operator> {
    let id = json!({ "id": "$item.id" });
    vec![merge_jobs(
        Some(2),
        vec![use_vine("timed_job", Some(json!({ "job": "$item.job" })))
            .on(
                "stepped",
                [emit_with(
                    "stepped",
                    json!({ "id": "$item.id", "step": "$item.step" }),
                )],
            )
            .on("done", [emit_with("done", id.clone())])
            .on(
                "failed",
                [emit_with(
                    "failed",
                    json!({ "id": "$item.id", "code": "$item.code" }),
                )],
            )],
    )]
}

#[tokio::test(start_paused = true)]
async fn a_timeout_inside_a_used_vine_survives_a_kill_at_every_event() {
    let jobs = jobs_input(&three());
    let events = job_events(&three());
    let over_jobs = || jobs_vine(used_operators());
    let over_events = || over_a_stream(used_operators());
    for (def, input, mode) in [
        (&over_jobs as &dyn Fn() -> PipeDef, &jobs, Mode::Journal),
        (&over_events, &events, Mode::Position),
    ] {
        let sweep = Sweep {
            def,
            input,
            mode,
            setup: &plain,
            again: 2,
        };
        sweep.run(false).await;
    }
}

/// A `catchError` after the timeout that branches on what failed: a timeout
/// runs an undo step first.
fn caught_by_branch() -> PipeDef {
    jobs_vine([merge_jobs(
        Some(2),
        vec![
            step("one", json!("$item.job.secs")),
            step("two", json!("$item.job.two")),
            timeout(LIMIT),
            done(),
            catch_error([
                branch(
                    "$item.error.from",
                    [("timeout", [step("undo", json!(1))])],
                    None,
                ),
                failed(),
            ]),
        ],
    )])
}

#[tokio::test(start_paused = true)]
async fn a_caught_timeout_replays_down_the_same_branch_at_every_kill() {
    let input = jobs_input(&three());
    let sweep = Sweep {
        def: &caught_by_branch,
        input: &input,
        mode: Mode::Journal,
        setup: &plain,
        again: 2,
    };
    let (outs, calls, _) = sweep.clean().await;
    let undone: Vec<&String> = calls.iter().filter(|c| c.ends_with(".undo")).collect();
    assert_eq!(undone, ["a.undo", "c.undo"]);
    assert_eq!(outs.len(), 3);
    // `extra_calls` holds every kill to the steps the clean run took.
    sweep.run(true).await;
}

/// `retry` applies to the timeout: each try gets its own three seconds.
fn retried_timeout() -> PipeDef {
    jobs_vine([merge_jobs(
        Some(2),
        vec![
            step("one", json!("$item.job.secs")),
            timeout(LIMIT),
            retry(1, Duration::from_secs(1)),
            done(),
            catch_error([failed()]),
        ],
    )])
}

#[tokio::test(start_paused = true)]
async fn a_retry_over_a_timeout_survives_a_kill_at_every_event() {
    let input = jobs_input(&[job("a", 5, 0), job("b", 1, 0)]);
    let sweep = Sweep {
        def: &retried_timeout,
        input: &input,
        mode: Mode::Journal,
        setup: &plain,
        again: 2,
    };
    let (_, calls, _) = sweep.clean().await;
    assert_eq!(calls, ["a.one", "b.one", "a.one"], "`a` timed out twice");
    sweep.run(true).await;
}

/// The timeout applies to a `retry` of a step that always fails, and a
/// `catchError` that mends a failed step slowly.
fn timeout_over_retry_and_catch_error() -> PipeDef {
    jobs_vine([merge_jobs(
        Some(2),
        vec![
            step("one", json!("$item.job.secs")),
            retry(2, Duration::from_secs(1)),
            catch_error([step("mend", json!("$item.job.two"))]),
            timeout(Duration::from_secs(2)),
            done(),
            catch_error([failed()]),
        ],
    )])
}

#[tokio::test(start_paused = true)]
async fn a_timeout_over_a_retry_and_a_catch_error_survives_a_kill_at_every_event() {
    // `x`'s step fails a second in, and each try after it waits a second
    // first, so its two seconds run out during the tries however a
    // restart cuts them, and its mend is never reached. `z` is fine at
    // once.
    let input = jobs_input(&[job("x", 1, 10), job("z", 1, 0)]);
    let setup = |desk: &Desk| desk.fail("x", "one");
    let sweep = Sweep {
        def: &timeout_over_retry_and_catch_error,
        input: &input,
        mode: Mode::Journal,
        setup: &setup,
        again: 2,
    };
    let (outs, _, _) = sweep.clean().await;
    let names: Vec<String> = outs
        .values()
        .map(|(name, p)| format!("{name} {}", p["id"].as_str().unwrap()))
        .collect();
    assert_eq!(names, ["failed x", "done z"]);
    sweep.run(true).await;
}

// ---------------------------------------------------- what stays small

#[tokio::test(start_paused = true)]
async fn a_bookmark_keeps_one_line_more_per_item_in_flight_and_drops_it_as_the_position_moves() {
    // Forty jobs, every third times out. With the timeout the bookmark
    // keeps, for each of the two items in flight, the decision besides
    // what the same work keeps without a timeout.
    let list: Vec<Value> = (0..40)
        .map(|n| match n % 3 {
            0 => job(&format!("j{n}"), 5, 1),
            _ => job(&format!("j{n}"), 1, 1),
        })
        .collect();
    let events = job_events(&list);
    let untimed = || {
        let work: Vec<Operator> = timed_work()
            .into_iter()
            .filter(|s| !matches!(s, Operator::Timeout(_) | Operator::CatchError(_)))
            .collect();
        over_a_stream(vec![merge_jobs(Some(2), work)])
    };
    let mut without = Caller::new(Mode::Position);
    without
        .run(&built(&Desk::new(), untimed()), &events, None)
        .await
        .unwrap();
    let mut with = Caller::new(Mode::Position);
    with.run(&built(&Desk::new(), timed_stream()), &events, None)
        .await
        .unwrap();
    assert_eq!(with.bookmark().position(), 40);
    assert!(with.bookmark().is_empty(), "{:?}", with.bookmark().lines());
    assert!(
        with.most <= without.most + 2,
        "{} lines with the timeout, {} without",
        with.most,
        without.most
    );
}

// ------------------------------------------------------- old saved data

#[tokio::test(start_paused = true)]
async fn a_journal_saved_before_timeouts_were_saved_resumes_as_it_did() {
    // A vine with no timeout keys its operators as it always did. This journal
    // is in the form saved before decisions were: `b`'s first step and
    // its note.
    let untimed = || {
        let work: Vec<Operator> = timed_work()
            .into_iter()
            .filter(|s| !matches!(s, Operator::Timeout(_) | Operator::CatchError(_)))
            .collect();
        jobs_vine([merge_jobs(Some(2), work)])
    };
    let saved = json!([
        { "kind": "done", "at": "pipe[1].mergeMap@0/pipe[1].mergeMap.do[0]#0",
          "input": { "id": "b", "step": "one", "secs": 1 },
          "result": { "id": "b", "step": "one" } },
        { "kind": "emitted", "output": "stepped", "within": "pipe[1].mergeMap@0/" },
    ]);
    let input = jobs_input(&[job("b", 1, 1)]);
    let mut clean = Caller::new(Mode::Journal);
    clean
        .run(&built(&Desk::new(), untimed()), &input, None)
        .await
        .unwrap();
    let desk = Desk::new();
    let mut caller = Caller::new(Mode::Journal);
    caller.saved = saved;
    caller
        .run(&built(&desk, untimed()), &input, None)
        .await
        .unwrap();
    assert_eq!(desk.calls(), ["b.two"]);
    assert_eq!(caller.handed, clean.handed[1..]);
}

#[tokio::test(start_paused = true)]
async fn a_journal_or_bookmark_saved_before_a_timeout_was_added_is_refused_before_any_operator() {
    let untimed_work = || -> Vec<Operator> {
        timed_work()
            .into_iter()
            .filter(|s| !matches!(s, Operator::Timeout(_) | Operator::CatchError(_)))
            .collect()
    };
    // Saved partway through by the vine without its timeout.
    let jobs = jobs_input(&three());
    let mut journal = Caller::new(Mode::Journal);
    let before = jobs_vine([merge_jobs(Some(2), untimed_work())]);
    journal
        .run(&built(&Desk::new(), before), &jobs, Some(3))
        .await
        .unwrap();
    let events = job_events(&three());
    let mut bookmark = Caller::new(Mode::Position);
    let before = over_a_stream(vec![merge_jobs(Some(2), untimed_work())]);
    bookmark
        .run(&built(&Desk::new(), before), &events, Some(3))
        .await
        .unwrap();
    for (mut caller, def, input) in [
        (journal, timed_jobs(), &jobs),
        (bookmark, timed_stream(), &events),
    ] {
        let desk = Desk::new();
        let got = caller.run(&built(&desk, def), input, None).await;
        let Err(RunError::ReplayMisfit { problem }) = got else {
            panic!("{:?}: {got:?}", caller.mode)
        };
        assert!(
            problem.contains("before the `timeout` was added"),
            "{problem}"
        );
        assert!(desk.calls().is_empty(), "{:?}", desk.calls());
    }
}

#[tokio::test(start_paused = true)]
async fn a_decision_reads_back_from_json_and_a_broken_one_is_refused() {
    let timed = Entry::Timed {
        at: "pipe[1].timeout@0".into(),
        timing: Timing::TimedOut,
    };
    let back = Journal::from_json(&Journal::from(vec![timed.clone()]).to_json()).unwrap();
    assert_eq!(back.entries(), std::slice::from_ref(&timed));
    for (bad, says) in [
        (
            json!([{ "kind": "timed", "at": "x", "timing": "late" }]),
            "unknown timing `late`",
        ),
        (json!([{ "kind": "timed", "at": "x" }]), "`timing` as text"),
        (
            json!([{ "kind": "timed", "at": "x", "timing": "in_time", "when": 3 }]),
            "no field `when`",
        ),
    ] {
        let err = Journal::from_json(&bad).unwrap_err();
        assert!(err.to_string().contains(says), "{bad}: {err}");
    }
    // Saved twice for one run of the timeout, it is a misfit, not a guess.
    let twice = Journal::from(vec![timed.clone(), timed]);
    let vine = built(&Desk::new(), timed_jobs());
    let got: Vec<_> = vine
        .run_saved(stream::iter(jobs_input(&three())), twice)
        .collect()
        .await;
    assert!(
        matches!(&got[..], [Err(RunError::ReplayMisfit { problem })] if problem.contains("saved twice")),
        "{got:?}"
    );
}
