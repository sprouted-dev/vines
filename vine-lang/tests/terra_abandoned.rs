//! An output Terra refuses because a fact changed mid-run lands as an
//! `abandoned` event that carries it. A reaction presses `undo_launch`,
//! whose vine `job_launch_undo` stops the agent and removes the working
//! copy the abandoned launch made.
#![cfg(feature = "yaml")]

mod fake;
mod terra;

use std::sync::Arc;

use fake::{registry, World};
use terra::moves::jobs_trellis;
use terra::{Answer, Event, Refusal, Terra, ABANDONED};
use vine_lang::{json, Value};

fn setup(world: &Arc<World>, jobs: &[&str]) -> Arc<Terra> {
    let terra = Terra::new(registry(world));
    terra.put(
        "p-1",
        json!({ "state": "active", "branch": "plan/p-1", "jobs": jobs }),
    );
    for id in jobs {
        terra.put(
            id,
            json!({ "state": "ready", "plan": "p-1", "waiting_on": 0, "brief": "work" }),
        );
    }
    terra.install(jobs_trellis()).unwrap();
    Arc::new(terra)
}

/// Pause the plan the first time `operator` runs.
fn pause_during(world: &World, terra: &Arc<Terra>, operator: &str) {
    let t = terra.clone();
    world.when(operator, move || t.set("p-1", "state", json!("paused")));
}

fn kinds(events: &[Event]) -> Vec<(&str, &str)> {
    events
        .iter()
        .map(|e| (e.kind.as_str(), e.artifact.as_str()))
        .collect()
}

fn launch(job: &str) -> Value {
    json!({ "job": job })
}

#[tokio::test]
async fn the_abandoned_event_carries_the_output_and_what_changed() {
    let world = World::new();
    let terra = setup(&world, &["j-1"]);
    pause_during(&world, &terra, "git.worktree_add");
    let answer = terra.press("launch", launch("j-1")).await;
    let Answer::Ran { request, .. } = &answer else {
        panic!("{answer:?}")
    };
    assert_eq!(
        answer.abandoned(),
        [Event {
            artifact: "j-1".into(),
            kind: ABANDONED.into(),
            payload: json!({
                "move": "launch",
                "request": request,
                "output": "started",
                "payload": { "job": "j-1", "session": "session-1", "tree": "/trees/j-1" },
                "changed": "p-1",
            }),
        }]
    );
}

#[tokio::test]
async fn a_launch_abandoned_by_a_pause_leaves_no_working_copy_and_no_agent_after_cleanup() {
    let world = World::new();
    let terra = setup(&world, &["j-1"]);
    pause_during(&world, &terra, "git.worktree_add");
    terra.press("launch", launch("j-1")).await;
    // Before the cleanup, the abandoned launch's work is still there.
    assert_eq!(world.trees(), ["/trees/j-1"]);
    assert_eq!(world.agents(), ["session-1"]);

    terra.react().await;
    assert!(world.trees().is_empty());
    assert!(world.agents().is_empty());
    assert_eq!(
        kinds(&terra.events()),
        [(ABANDONED, "j-1"), ("core.job.launch_undone", "j-1")]
    );
}

#[tokio::test]
async fn a_nested_launch_abandoned_by_a_pause_is_cleaned_up_the_same_way() {
    let world = World::new();
    let terra = setup(&world, &["j-1", "j-2"]);
    pause_during(&world, &terra, "git.worktree_add");
    terra.press("launch_all", json!({ "plan": "p-1" })).await;
    let answers = terra.react().await;
    // One cleanup, for j-1. The outer vine's abandoned outputs belong to
    // `launch_all`, and nothing reacts to them.
    assert_eq!(answers.len(), 1);
    assert!(world.trees().is_empty());
    assert!(world.agents().is_empty());
    assert_eq!(world.count("git.worktree_add j-2"), 0);
}

#[tokio::test]
async fn a_cleanup_that_fails_lands_its_failure_and_keeps_the_working_copy() {
    let world = World::new();
    let terra = setup(&world, &["j-1"]);
    pause_during(&world, &terra, "git.worktree_add");
    terra.press("launch", launch("j-1")).await;
    world.fail("agent.stop", "unavailable", "no agent host");
    let answers = terra.react().await;
    let [failed] = &answers[0].landed()[..] else {
        panic!("{answers:?}")
    };
    assert_eq!(failed.kind, "core.job.undo_failed");
    assert_eq!(
        failed.payload,
        json!({ "job": "j-1", "reason": "no agent host" })
    );
    // The agent could still be running, so its working copy stays.
    assert_eq!(world.trees(), ["/trees/j-1"]);
    assert_eq!(world.agents(), ["session-1"]);

    // Pressed again once the host is back, it finishes the cleanup.
    let abandoned = &terra.events()[0];
    let retry = json!({
        "job": "j-1",
        "output": abandoned.payload["output"],
        "work": abandoned.payload["payload"],
    });
    let again = terra.press("undo_launch", retry).await;
    assert_eq!(again.landed()[0].kind, "core.job.launch_undone");
    assert!(world.trees().is_empty());
    assert!(world.agents().is_empty());
}

#[tokio::test]
async fn a_cleanup_delivered_twice_runs_once() {
    let world = World::new();
    let terra = setup(&world, &["j-1"]);
    pause_during(&world, &terra, "git.worktree_add");
    terra.press("launch", launch("j-1")).await;
    let first = terra.react().await;
    let (events, log) = (terra.events(), world.log());
    // The queue delivers the abandoned event again: the press has the same
    // request id and attaches to the first.
    let again = terra.redeliver(0).await;
    assert_eq!(again, first);
    assert_eq!(terra.events(), events);
    assert_eq!(world.log(), log);
}

#[tokio::test]
async fn a_cleanup_pressed_twice_under_two_ids_changes_nothing_the_second_time() {
    let world = World::new();
    let terra = setup(&world, &["j-1"]);
    pause_during(&world, &terra, "git.worktree_add");
    terra.press("launch", launch("j-1")).await;
    terra.react().await;
    let abandoned = &terra.events()[0];
    let undo = json!({
        "job": "j-1",
        "output": "started",
        "work": abandoned.payload["payload"],
    });
    // Both operators are safe to run twice: the second run stops an agent that
    // is not running and removes a working copy that is not there.
    let second = terra.press("undo_launch", undo).await;
    assert_eq!(second.landed()[0].kind, "core.job.launch_undone");
    assert!(world.trees().is_empty());
    assert!(world.agents().is_empty());
    assert_eq!(world.count("agent.stop session-1"), 2);
}

#[tokio::test]
async fn an_abandoned_output_whose_work_never_started_has_nothing_to_undo() {
    let world = World::new();
    let terra = setup(&world, &["j-1"]);
    // The plan is paused while the branch is refreshed, and then the
    // working copy cannot be made: `job_launch` fails, having made
    // nothing, and its `failed` output is abandoned.
    pause_during(&world, &terra, "git.refresh_branch");
    world.fail("git.worktree_add", "disk", "disk full");
    let answer = terra.press("launch", launch("j-1")).await;
    assert_eq!(answer.abandoned()[0].payload["output"], "failed");
    let operators = world.log().len();

    let answers = terra.react().await;
    assert_eq!(answers[0].landed()[0].kind, "core.job.nothing_to_undo");
    assert_eq!(world.log().len(), operators, "the cleanup ran an operator");
}

#[tokio::test]
async fn a_cleanup_after_a_later_launch_landed_is_refused_and_leaves_that_launch_alone() {
    let world = World::new();
    let terra = setup(&world, &["j-1"]);
    let t = terra.clone();
    world.when("git.worktree_add", move || {
        // Pause once, during the first launch only.
        if t.fact("p-1", "state") == "active" && t.events().is_empty() {
            t.set("p-1", "state", json!("paused"));
        }
    });
    terra.press("launch", launch("j-1")).await;
    terra.set("p-1", "state", json!("active"));
    let relaunch = terra.press("launch", launch("j-1")).await;
    assert_eq!(relaunch.landed()[0].kind, "core.job.started");

    let answers = terra.react().await;
    assert_eq!(answers, [Answer::Refused(Refusal::Gate("state".into()))]);
    // The landed launch's working copy and agent stay.
    assert_eq!(world.trees(), ["/trees/j-1"]);
    assert_eq!(world.agents(), ["session-1"]);
}
