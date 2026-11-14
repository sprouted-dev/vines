//! Acts that take time on the paused clock, for vines that run items side
//! by side, and those vines built in Rust.
//!
//! - `work.step { id, step, secs }` waits `secs` seconds, then gives
//!   `{ id, step }`. It logs `start a.one @0` when it is called and
//!   `done a.one @5` when it is done. A step set to fail fails with the
//!   code `broke` instead of giving a result.
//! - `work.lines { id, lines }` is a stream act that gives its items again:
//!   `{ id, line }` for each line after `resume_from`, one a second unless
//!   [`Desk::pace_lines`] says otherwise, and ends with `{ lines }`. It logs `lines a from 0` each time it is
//!   called.
//!
//! The clock is tokio's, so a test that starts it paused gets exact times.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use tokio::time::{sleep, Instant};
use vine_lang::pipe::{call, catch, each, emit_with, fork, pipe, split, Overlap, PipeDef, Stage};
use vine_lang::{json, OpError, OperatorSpec, Part, Registry, Shape, Ty, Value};

/// What the timed acts did.
pub struct Desk {
    start: Instant,
    log: Mutex<Vec<String>>,
    fail: Mutex<BTreeSet<String>>,
    /// How many steps are running now, and the most there ever were.
    running: Mutex<(usize, usize)>,
    /// How many steps were dropped before they finished.
    cut: Mutex<usize>,
    /// How many results the caller says it was handed.
    handed: Mutex<usize>,
    /// For each step called: how many steps had finished, and how many
    /// results the caller had been handed, when it was called.
    at_start: Mutex<Vec<(usize, usize)>>,
    /// How many seconds each line of `work.lines` takes.
    line_secs: Mutex<u64>,
}

impl Desk {
    pub fn new() -> Arc<Desk> {
        Arc::new(Desk {
            start: Instant::now(),
            log: Mutex::new(Vec::new()),
            fail: Mutex::new(BTreeSet::new()),
            running: Mutex::new((0, 0)),
            cut: Mutex::new(0),
            handed: Mutex::new(0),
            at_start: Mutex::new(Vec::new()),
            line_secs: Mutex::new(1),
        })
    }

    /// How many steps were dropped while running, as a kill drops them.
    pub fn cut_off(&self) -> usize {
        *self.cut.lock().unwrap()
    }

    /// The caller was handed one more result.
    pub fn handed_a_result(&self) {
        *self.handed.lock().unwrap() += 1;
    }

    /// For each step called, in order: how many steps had finished, and
    /// how many results the caller had been handed, when it was called.
    pub fn at_each_start(&self) -> Vec<(usize, usize)> {
        self.at_start.lock().unwrap().clone()
    }

    /// Every line, in order, such as `start a.one @0`.
    pub fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    /// The steps that were called, such as `a.one`, in the order they were
    /// called, a step called twice listed twice.
    pub fn calls(&self) -> Vec<String> {
        self.log()
            .iter()
            .filter_map(|l| l.strip_prefix("start "))
            .map(|l| l.split(" @").next().unwrap_or_default().to_owned())
            .collect()
    }

    /// The most steps that were running at one time.
    pub fn most_at_once(&self) -> usize {
        self.running.lock().unwrap().1
    }

    /// Make each line of `work.lines` take `secs` seconds from now on, as
    /// a host that has slowed down would.
    pub fn pace_lines(&self, secs: u64) {
        *self.line_secs.lock().unwrap() = secs;
    }

    /// Make `id.step` fail every time it is called.
    pub fn fail(&self, id: &str, step: &str) {
        self.fail.lock().unwrap().insert(format!("{id}.{step}"));
    }

    fn note(&self, line: String) {
        let at = self.start.elapsed().as_secs();
        self.log.lock().unwrap().push(format!("{line} @{at}"));
    }

    fn enter(&self) {
        let finished = self
            .log
            .lock()
            .unwrap()
            .iter()
            .filter(|l| l.starts_with("done ") || l.starts_with("broke "))
            .count();
        let handed = *self.handed.lock().unwrap();
        self.at_start.lock().unwrap().push((finished, handed));
        let mut r = self.running.lock().unwrap();
        r.0 += 1;
        r.1 = r.1.max(r.0);
    }

    fn leave(&self) {
        self.running.lock().unwrap().0 -= 1;
    }
}

/// Counts a step as running until it finishes or is dropped.
struct Running {
    desk: Arc<Desk>,
    finished: bool,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.desk.leave();
        if !self.finished {
            *self.desk.cut.lock().unwrap() += 1;
        }
    }
}

fn text(v: &Value, key: &str) -> String {
    v[key].as_str().unwrap_or_default().to_owned()
}

/// A registry with `work.step` and `work.lines` on `desk`, and the vine
/// `job_steps`: steps `one` and `two` of the job at `$item.job`, then
/// `done`.
pub fn desk_registry(desk: &Arc<Desk>) -> Registry {
    let mut reg = desk_acts(desk);
    let id = || Shape::new().required("id", Ty::Text);
    let steps = pipe("job_steps")
        .input(Shape::new().required("job", Ty::Any))
        .output("done", id())
        .stages([
            step("one", json!("$item.job.secs")),
            step("two", json!(1)),
            emit_with("done", json!({ "id": "$item.job.id" })),
        ]);
    let steps = reg.build_pipe(steps).unwrap();
    reg.add_vine(steps).unwrap();
    reg
}

fn desk_acts(desk: &Arc<Desk>) -> Registry {
    let mut reg = Registry::new();
    let d = desk.clone();
    reg.register_act(
        OperatorSpec::new("work.step")
            .settings(
                Shape::new()
                    .required("id", Ty::Text)
                    .required("step", Ty::Text)
                    .required("secs", Ty::Int),
            )
            .returns(
                Shape::new()
                    .required("id", Ty::Text)
                    .required("step", Ty::Text),
            )
            .touches(["work"]),
        move |v, _cx| {
            let d = d.clone();
            async move {
                let name = format!("{}.{}", text(&v, "id"), text(&v, "step"));
                d.note(format!("start {name}"));
                d.enter();
                let mut running = Running {
                    desk: d.clone(),
                    finished: false,
                };
                sleep(Duration::from_secs(v["secs"].as_u64().unwrap_or(0))).await;
                running.finished = true;
                drop(running);
                if d.fail.lock().unwrap().contains(&name) {
                    d.note(format!("broke {name}"));
                    return Err(OpError::new("broke", format!("{name} broke")));
                }
                d.note(format!("done {name}"));
                Ok(json!({ "id": v["id"], "step": v["step"] }))
            }
        },
    )
    .unwrap();
    let d = desk.clone();
    reg.register_replayable_stream_act(
        OperatorSpec::new("work.lines")
            .settings(
                Shape::new()
                    .required("id", Ty::Text)
                    .required("lines", Ty::Int),
            )
            .returns(
                Shape::new()
                    .required("id", Ty::Text)
                    .required("line", Ty::Int),
            )
            .ends(Shape::new().required("lines", Ty::Int))
            .touches(["work"]),
        move |v, cx| {
            let (id, lines) = (text(&v, "id"), v["lines"].as_u64().unwrap_or(0));
            let from = cx.resume_from();
            d.note(format!("lines {id} from {from}"));
            let pace = *d.line_secs.lock().unwrap();
            let items = (from + 1..=lines).map(move |n| (n, id.clone()));
            futures::stream::iter(items)
                .then(move |(n, id)| async move {
                    sleep(Duration::from_secs(pace)).await;
                    Ok(Part::Item(json!({ "id": id, "line": n })))
                })
                .chain(futures::stream::once(async move {
                    Ok(Part::End(json!({ "lines": lines })))
                }))
        },
    )
    .unwrap();
    reg
}

/// One job, as the vines here take it: an id and how long its first step
/// takes.
pub fn job(id: &str, secs: u64) -> Value {
    json!({ "id": id, "secs": secs })
}

/// The input of a vine over `jobs`: one item that lists them.
pub fn jobs(list: &[(&str, u64)]) -> Value {
    json!({ "jobs": list.iter().map(|(id, s)| job(id, *s)).collect::<Vec<_>>() })
}

/// `work.step` on the job at `$item.job`, adding its result under `into`.
pub fn step(name: &str, secs: Value) -> Stage {
    call(
        "work.step",
        json!({ "id": "$item.job.id", "step": name, "secs": secs }),
    )
    .into(name)
}

/// The work for one job: step `one` takes the job's own time and emits
/// `stepped`; step `two` takes a second and emits `done`. So each job
/// gives two outputs, `stepped` before `done`.
pub fn job_work() -> Vec<Stage> {
    vec![
        step("one", json!("$item.job.secs")),
        fork([[emit_with(
            "stepped",
            json!({ "id": "$item.job.id", "step": "one" }),
        )]]),
        step("two", json!(1)),
        emit_with("done", json!({ "id": "$item.job.id" })),
    ]
}

/// The same work, with a `catch` inside: a job whose step fails emits
/// `failed`, and the other jobs go on.
pub fn job_work_caught() -> Vec<Stage> {
    let mut work = job_work();
    work.push(catch([emit_with(
        "failed",
        json!({ "id": "$item.job.id", "code": "$item.error.code" }),
    )]));
    work
}

/// An `each` with `overlap`, capped at `at_once`, over `work`.
pub fn each_job(overlap: Overlap, at_once: Option<usize>, work: Vec<Stage>) -> Stage {
    let stage = each(overlap, work);
    match at_once {
        Some(n) => stage.at_once(n),
        None => stage,
    }
}

/// A vine over one item that lists jobs: split them, then `stages`.
pub fn jobs_vine(stages: impl IntoIterator<Item = Stage>) -> PipeDef {
    let id = || Shape::new().required("id", Ty::Text);
    pipe("jobs")
        .input(Shape::new().required("jobs", Ty::List))
        .output("stepped", id().required("step", Ty::Text))
        .output("done", id())
        .output("failed", id().required("code", Ty::Text))
        .output("all", Shape::open())
        .output("line", id().required("line", Ty::Int))
        .stages([split("$item.jobs").into("job")].into_iter().chain(stages))
}

/// One input item of a vine over a stream of jobs.
pub fn job_event(id: &str, secs: u64) -> Value {
    json!({ "job": job(id, secs) })
}

/// A vine over a stream of jobs, one per input item `{ job }`: `stages`
/// alone.
pub fn job_stream_vine(stages: impl IntoIterator<Item = Stage>) -> PipeDef {
    let id = || Shape::new().required("id", Ty::Text);
    pipe("job_stream")
        .input(Shape::new().required("job", Ty::Object))
        .output("stepped", id().required("step", Ty::Text))
        .output("done", id())
        .output("failed", id().required("code", Ty::Text))
        .stages(stages)
}
