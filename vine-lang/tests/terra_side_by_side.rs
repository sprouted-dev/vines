//! `plan_launch_all` launches its jobs two at a time and survives a crash
//! at any point. Terra runs it with `run_saved`; each job's launch is a
//! nested press of `launch`, so two presses run side by side and their
//! events interleave.
//!
//! What "the same as an uninterrupted run" means here, now that two jobs
//! interleave:
//!
//! - the same events, as a multiset;
//! - for each job, its own events in the same order, and the plan's
//!   summary event last of all;
//! - one `launch` request per job, and the same working copies and agents.
//!
//! The order in which two jobs' events interleave is not promised: after a
//! restart it can differ from the first run.
#![cfg(feature = "yaml")]

mod acts;
mod terra;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use acts::{registry, World};
use terra::moves::{
    activate_move, launch_all_move, launch_move, undo_abandoned_launch, undo_launch_move, yaml,
};
use terra::{Answer, Event, Terra, Trellis, VineSource};
use tokio::time::Instant;
use vine_lang::{json, Value};

const MIXED: [(&str, &str); 3] = [("j-1", "ready"), ("j-2", "done"), ("j-3", "ready")];
const READY: [(&str, &str); 3] = [("j-1", "ready"), ("j-2", "ready"), ("j-3", "ready")];

fn plan_launch_all(at_once: &str) -> String {
    let text = match yaml("plan_launch_all") {
        VineSource::Yaml(text) => text,
        VineSource::Def(_) => unreachable!("read as YAML"),
    };
    assert!(text.contains("at_once: 2"), "the vine launches two at once");
    text.replace("at_once: 2", &format!("at_once: {at_once}"))
}

fn trellis(at_once: &str) -> Trellis {
    let mut vines: Vec<VineSource> = [
        "job_teardown",
        "job_launch_undo",
        "job_launch",
        "plan_activate",
    ]
    .into_iter()
    .map(yaml)
    .collect();
    vines.push(VineSource::Yaml(plan_launch_all(at_once)));
    Trellis {
        vines,
        moves: vec![
            launch_move(),
            activate_move(),
            launch_all_move(),
            undo_launch_move(),
        ],
        reactions: vec![undo_abandoned_launch()],
    }
}

/// A plan with these jobs. Starting an agent takes five seconds.
fn setup(jobs: &[(&str, &str)], at_once: &str) -> (Arc<World>, Terra) {
    let world = World::new();
    world.slow("agent.start", Duration::from_secs(5));
    let terra = Terra::new(registry(&world));
    let ids: Vec<&str> = jobs.iter().map(|(id, _)| *id).collect();
    terra.put(
        "p-1",
        json!({ "state": "active", "branch": "plan/p-1", "jobs": ids }),
    );
    for (id, state) in jobs {
        terra.put(
            id,
            json!({ "state": state, "plan": "p-1", "waiting_on": 0, "brief": "work" }),
        );
    }
    terra.install(trellis(at_once)).unwrap();
    (world, terra)
}

fn launch_all() -> Value {
    json!({ "plan": "p-1" })
}

/// The job an event is about, or `plan` for the summary.
fn about(event: &Event) -> String {
    if event.artifact.starts_with("j-") {
        return event.artifact.clone();
    }
    match event.payload["job"].as_str() {
        Some(job) => job.to_owned(),
        None => "plan".into(),
    }
}

fn per_job(events: &[Event]) -> BTreeMap<String, Vec<Event>> {
    let mut by = BTreeMap::<String, Vec<Event>>::new();
    for e in events {
        by.entry(about(e)).or_default().push(e.clone());
    }
    by
}

fn sorted<T: std::fmt::Debug>(all: &[T]) -> Vec<String> {
    let mut all: Vec<String> = all.iter().map(|e| format!("{e:?}")).collect();
    all.sort();
    all
}

/// What an uninterrupted run leaves.
struct Clean {
    answer: Answer,
    events: Vec<Event>,
    log: Vec<String>,
    trees: Vec<String>,
    agents: Vec<String>,
    launches: Vec<String>,
    points: u64,
}

async fn clean(jobs: &[(&str, &str)]) -> Clean {
    let (world, terra) = setup(jobs, "2");
    let answer = terra
        .press_as("r-1", "launch_all", launch_all())
        .await
        .unwrap();
    Clean {
        answer,
        events: terra.events(),
        log: world.log(),
        trees: world.trees(),
        agents: world.agents(),
        launches: terra.requests_of("launch"),
        points: terra.ticks(),
    }
}

/// The answer's landings, in any order, and how it ended.
fn answered(answer: &Answer) -> (Vec<String>, String) {
    match answer {
        Answer::Ran { landings, end, .. } => (sorted(landings), format!("{end:?}")),
        other => (Vec::new(), format!("{other:?}")),
    }
}

/// The run after crashes ends as `clean` did, in the sense the module docs
/// give, and ran again at most `again` acts.
fn assert_same(
    clean: &Clean,
    answer: &Answer,
    world: &World,
    terra: &Terra,
    again: usize,
    at: &str,
) {
    let events = terra.events();
    assert_eq!(answered(answer), answered(&clean.answer), "{at}");
    assert_eq!(sorted(&events), sorted(&clean.events), "{at}");
    assert_eq!(per_job(&events), per_job(&clean.events), "{at}");
    assert_eq!(
        events.last().map(|e| e.kind.as_str()),
        Some("core.plan.launched_all"),
        "{at}"
    );
    assert_eq!(terra.requests_of("launch"), clean.launches, "{at}");
    assert_eq!(world.trees(), clean.trees, "{at}");
    assert_eq!(world.agents(), clean.agents, "{at}");
    // Acts whose results were lost run again; each is safe to run twice.
    let log = world.log();
    let extra = log.len() - clean.log.len();
    assert!(extra <= again, "{at}: {extra} acts again: {log:?}");
    let mut left = log.clone();
    for line in &clean.log {
        let i = left.iter().position(|l| l == line);
        left.remove(i.unwrap_or_else(|| panic!("{at}: `{line}` never ran: {log:?}")));
    }
    assert!(left.iter().all(|l| clean.log.contains(l)), "{at}: {left:?}");
}

#[tokio::test(start_paused = true)]
async fn three_five_second_launches_take_ten_seconds_two_at_once_and_fifteen_one_at_a_time() {
    let mut took = Vec::new();
    for at_once in ["2", "1"] {
        let (_world, terra) = setup(&READY, at_once);
        let start = Instant::now();
        terra
            .press_as("r-1", "launch_all", launch_all())
            .await
            .unwrap();
        took.push(start.elapsed().as_secs());
        assert_eq!(terra.requests_of("launch").len(), 3);
    }
    assert_eq!(took, [10, 15]);
}

#[tokio::test(start_paused = true)]
async fn each_job_is_gated_on_its_own_and_a_refused_one_does_not_stop_the_others() {
    let clean = clean(&MIXED).await;
    let kinds: Vec<(String, &str)> = clean
        .events
        .iter()
        .map(|e| (about(e), e.kind.as_str()))
        .collect();
    let of = |job: &str| -> Vec<&str> {
        kinds
            .iter()
            .filter(|(j, _)| j == job)
            .map(|(_, k)| *k)
            .collect()
    };
    assert_eq!(of("j-1"), ["core.job.started", "core.plan.job_launched"]);
    assert_eq!(of("j-2"), ["core.plan.launch_refused"]);
    assert_eq!(of("j-3"), ["core.job.started", "core.plan.job_launched"]);
    assert_eq!(
        kinds.last().map(|(_, k)| *k),
        Some("core.plan.launched_all")
    );
    assert_eq!(kinds.len(), 6);
    let refused = clean
        .events
        .iter()
        .find(|e| e.kind == "core.plan.launch_refused")
        .unwrap();
    assert_eq!(refused.payload["gate"], "state");
    // j-1 and j-3 were launched side by side: j-3's agent started before
    // j-1's launch had landed its event.
    let started: Vec<&str> = clean
        .log
        .iter()
        .filter(|l| l.starts_with("agent.start"))
        .map(String::as_str)
        .collect();
    assert_eq!(started.len(), 2);
    assert_eq!(clean.agents.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_crash_at_any_point_then_a_restart_ends_as_an_uninterrupted_run() {
    for jobs in [&MIXED, &READY] {
        let clean = clean(jobs).await;
        assert!(clean.points > 30, "{} points", clean.points);
        let mut most = 0;
        for k in 1..=clean.points {
            let (world, terra) = setup(jobs, "2");
            terra.crash_at(k);
            let first = terra.press_as("r-1", "launch_all", launch_all()).await;
            assert!(first.is_none(), "crash {k} did not stop the run");
            let point = terra.crash_point().unwrap();
            terra.restart();
            let answer = terra
                .press_as("r-1", "launch_all", launch_all())
                .await
                .unwrap();
            assert_same(
                &clean,
                &answer,
                &world,
                &terra,
                2,
                &format!("crash at {k}: {point}"),
            );
            most = most.max(world.log().len() - clean.log.len());
        }
        // Two launches side by side, even with j-2 refused, since its slot
        // frees at once: a crash can cost an act of each, and does.
        assert_eq!(most, 2);
    }
}

#[tokio::test(start_paused = true)]
async fn two_crashes_on_a_grid_then_a_restart_end_as_an_uninterrupted_run() {
    let clean = clean(&READY).await;
    for k in (1..=clean.points).step_by(3) {
        for m in (1..=clean.points).step_by(5) {
            let (world, terra) = setup(&READY, "2");
            terra.crash_at(k);
            assert!(terra
                .press_as("r-1", "launch_all", launch_all())
                .await
                .is_none());
            let first = terra.crash_point().unwrap();
            terra.restart();
            terra.crash_at(m);
            let answer = match terra.press_as("r-1", "launch_all", launch_all()).await {
                Some(answer) => answer,
                None => {
                    terra.restart();
                    terra
                        .press_as("r-1", "launch_all", launch_all())
                        .await
                        .unwrap()
                }
            };
            let at = format!("crash at {k} ({first}) then {m}");
            assert_same(&clean, &answer, &world, &terra, 4, &at);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn crashing_every_few_points_still_ends_as_an_uninterrupted_run() {
    let clean = clean(&READY).await;
    for every in [2, 3, 5, 7] {
        let (world, terra) = setup(&READY, "2");
        let mut crashes = 0;
        let answer = loop {
            terra.crash_at(every);
            if let Some(answer) = terra.press_as("r-1", "launch_all", launch_all()).await {
                break answer;
            }
            crashes += 1;
            assert!(crashes < 1000, "no progress crashing every {every}");
            terra.restart();
        };
        assert!(
            crashes > 5,
            "crashing every {every} crashed {crashes} times"
        );
        assert_same(
            &clean,
            &answer,
            &world,
            &terra,
            2 * crashes,
            &format!("every {every}"),
        );
    }
}
