//! `job_launch` times out a launch whose agent hangs, and Terra lands the
//! launch's `failed` event, at any crash. Terra runs every vine with
//! `run_saved`, so the timeout's decision is in the press's journal: after
//! a crash that came after it, the launch fails again at once and no
//! second agent is started; after one that came before it, the wait
//! starts again. `plan_launch_all` still launches two jobs at once, and a
//! hung one holds its slot only until its timeout.
#![cfg(feature = "yaml")]

mod acts;
mod terra;

use std::sync::Arc;
use std::time::Duration;

use acts::{registry, World};
use terra::moves::jobs_trellis;
use terra::{Answer, Event, Terra};
use tokio::time::Instant;
use vine_lang::{json, Value};

const JOBS: [&str; 3] = ["j-1", "j-2", "j-3"];

/// A plan of three ready jobs. Starting an agent takes five seconds, and
/// in `hung` it never ends.
fn setup(hung: &[&str]) -> (Arc<World>, Terra) {
    let world = World::new();
    world.slow("agent.start", Duration::from_secs(5));
    for job in hung {
        world.slow_for(
            "agent.start",
            &format!("/trees/{job}"),
            Duration::from_secs(24 * 3600),
        );
    }
    let terra = Terra::new(registry(&world));
    terra.put(
        "p-1",
        json!({ "state": "active", "branch": "plan/p-1", "jobs": JOBS }),
    );
    for id in JOBS {
        terra.put(
            id,
            json!({ "state": "ready", "plan": "p-1", "waiting_on": 0, "brief": "work" }),
        );
    }
    terra.install(jobs_trellis()).unwrap();
    (world, terra)
}

fn kinds(events: &[Event]) -> Vec<(String, String)> {
    events
        .iter()
        .map(|e| (e.artifact.clone(), e.kind.clone()))
        .collect()
}

fn sorted<T: std::fmt::Debug>(all: &[T]) -> Vec<String> {
    let mut all: Vec<String> = all.iter().map(|e| format!("{e:?}")).collect();
    all.sort();
    all
}

#[tokio::test(start_paused = true)]
async fn a_launch_whose_agent_hangs_times_out_and_lands_launch_failed() {
    let (world, terra) = setup(&["j-1"]);
    let start = Instant::now();
    let answer = terra.press("launch", json!({ "job": "j-1" })).await;
    assert_eq!(start.elapsed(), Duration::from_secs(60));
    let landed = answer.landed();
    assert_eq!(
        kinds(&landed),
        [("j-1".to_owned(), "core.job.launch_failed".to_owned())]
    );
    assert_eq!(landed[0].payload["reason"], "took longer than 60s");
    // The working copy is removed by name, and no agent ever started.
    assert!(world.trees().is_empty(), "{:?}", world.trees());
    assert!(world.agents().is_empty());
    assert_eq!(terra.fact("j-1", "state"), json!("ready"));
}

/// What an uninterrupted press leaves.
struct Clean {
    answer: Answer,
    events: Vec<Event>,
    log: Vec<String>,
    trees: Vec<String>,
    agents: Vec<String>,
    points: u64,
}

async fn clean(hung: &[&str], name: &str, payload: &Value) -> Clean {
    let (world, terra) = setup(hung);
    let answer = terra.press_as("r-1", name, payload.clone()).await.unwrap();
    Clean {
        answer,
        events: terra.events(),
        log: world.log(),
        trees: world.trees(),
        agents: world.agents(),
        points: terra.ticks(),
    }
}

/// Crash at every point of a press of `name`, restart, and check it ends
/// as the uninterrupted press did, with at most `again` acts run again.
async fn crash_everywhere(hung: &[&str], name: &str, payload: Value, again: usize) -> (u64, u64) {
    let clean = clean(hung, name, &payload).await;
    let (mut late, mut at_once) = (0, 0);
    for k in 1..=clean.points {
        let (world, terra) = setup(hung);
        terra.crash_at(k);
        assert!(terra.press_as("r-1", name, payload.clone()).await.is_none());
        let point = terra.crash_point().unwrap();
        terra.restart();
        let restart = Instant::now();
        let answer = terra.press_as("r-1", name, payload.clone()).await.unwrap();
        let at = format!("crash at {k}: {point}");
        match (&answer, &clean.answer) {
            (
                Answer::Ran { landings, end, .. },
                Answer::Ran {
                    landings: want,
                    end: want_end,
                    ..
                },
            ) => {
                assert_eq!(sorted(landings), sorted(want), "{at}");
                assert_eq!(end, want_end, "{at}");
            }
            other => panic!("{at}: {other:?}"),
        }
        let events = terra.events();
        assert_eq!(sorted(&events), sorted(&clean.events), "{at}");
        // No hung job's working copy is left, and no agent is started
        // twice.
        assert_eq!(world.trees(), clean.trees, "{at}");
        assert_eq!(world.agents(), clean.agents, "{at}");
        let log = world.log();
        let extra = log.len() - clean.log.len();
        assert!(extra <= again, "{at}: {extra} acts again: {log:?}");
        for line in &clean.log {
            assert!(log.contains(line), "{at}: `{line}` never ran");
        }
        // A hung launch's wait starts again after a crash that came before
        // its decision was kept; after one, it does not wait at all.
        if restart.elapsed() >= Duration::from_secs(60) {
            late += 1;
        } else if restart.elapsed() == Duration::ZERO {
            at_once += 1;
        }
    }
    (late, at_once)
}

#[tokio::test(start_paused = true)]
async fn a_hung_launch_survives_a_crash_at_every_point() {
    let (late, at_once) = crash_everywhere(&["j-1"], "launch", json!({ "job": "j-1" }), 1).await;
    assert!(late > 0, "no crash fell during the wait");
    assert!(at_once > 0, "no crash fell after the decision was kept");
}

#[tokio::test(start_paused = true)]
async fn launching_a_plan_with_a_hung_job_takes_its_timeout_and_still_runs_two_at_once() {
    let (world, terra) = setup(&["j-2"]);
    let start = Instant::now();
    let answer = terra
        .press_as("r-1", "launch_all", json!({ "plan": "p-1" }))
        .await
        .unwrap();
    // j-1 and j-2 start together; j-1 is done at 5 and j-3 takes its slot,
    // done at 10. j-2's timeout ends the plan's launch at 60.
    assert_eq!(start.elapsed(), Duration::from_secs(60));
    let landed = kinds(&answer.landed());
    assert!(landed.contains(&("p-1".to_owned(), "core.plan.job_not_launched".to_owned())));
    assert_eq!(
        landed.last(),
        Some(&("p-1".to_owned(), "core.plan.launched_all".to_owned()))
    );
    let not = answer
        .landed()
        .into_iter()
        .find(|e| e.kind == "core.plan.job_not_launched")
        .unwrap();
    assert_eq!(not.payload["job"], "j-2");
    assert_eq!(not.payload["reason"], "took longer than 60s");
    assert_eq!(world.agents().len(), 2);
    assert_eq!(world.trees(), ["/trees/j-1", "/trees/j-3"]);
}

#[tokio::test(start_paused = true)]
async fn launching_a_plan_with_a_hung_job_survives_a_crash_at_every_point() {
    crash_everywhere(&["j-2"], "launch_all", json!({ "plan": "p-1" }), 2).await;
}
