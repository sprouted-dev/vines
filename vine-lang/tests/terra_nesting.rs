//! One vine nests another by pressing its move. `plan_launch_all` calls
//! `move.launch` for each job: Terra checks the job's gates, builds
//! `job_launch`'s input, and lands each of its outputs before the outer
//! vine hears of it. `use` is kept for helpers no move starts, and the
//! stand-in refuses a trellis that `use`s a vine a move starts.
#![cfg(feature = "yaml")]

mod acts;
mod terra;

use std::sync::Arc;

use acts::{registry, World};
use terra::moves::{jobs_trellis, yaml};
use terra::{Answer, End, Event, InstallError, Lands, Move, Need, Terra, Trellis, VineSource};
use vine_lang::{json, LoadError, Ty, Value};

/// Terra with an active plan of these jobs, and the jobs trellis.
fn plan_of(world: &Arc<World>, jobs: &[(&str, &str)]) -> Arc<Terra> {
    let terra = Terra::new(registry(world));
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
    terra.install(jobs_trellis()).unwrap();
    Arc::new(terra)
}

fn launch_all() -> Value {
    json!({ "plan": "p-1" })
}

fn kinds(events: &[Event]) -> Vec<(&str, &str)> {
    events
        .iter()
        .map(|e| (e.kind.as_str(), e.artifact.as_str()))
        .collect()
}

fn on_plan(kind: &str, payload: Value) -> Event {
    Event {
        artifact: "p-1".into(),
        kind: kind.into(),
        payload,
    }
}

// --------------------------------------------- each job's gates are checked

#[tokio::test]
async fn a_job_that_is_not_ready_is_refused_by_name_and_the_others_go_on() {
    let world = World::new();
    let terra = plan_of(
        &world,
        &[("j-1", "ready"), ("j-2", "done"), ("j-3", "ready")],
    );
    let answer = terra.press("launch_all", launch_all()).await;
    assert!(
        matches!(answer, Answer::Ran { end: End::Done, .. }),
        "{answer:?}"
    );
    assert_eq!(world.trees(), ["/trees/j-1", "/trees/j-3"]);
    assert_eq!(world.count("git.worktree_add j-2"), 0);
    let refusal = on_plan(
        "core.plan.launch_refused",
        json!({ "plan": "p-1", "job": "j-2", "gate": "state" }),
    );
    assert!(answer.landed().contains(&refusal), "{answer:?}");
}

#[tokio::test]
async fn every_inner_event_lands_on_the_log_before_the_outer_output_made_from_it() {
    let world = World::new();
    let terra = plan_of(
        &world,
        &[("j-1", "ready"), ("j-2", "done"), ("j-3", "ready")],
    );
    terra.press("launch_all", launch_all()).await;
    // Jobs go one at a time, in the plan's order. For each, the inner
    // event lands first, then the outer vine's output about it. The
    // summary lands last.
    assert_eq!(
        kinds(&terra.events()),
        [
            ("core.job.started", "j-1"),
            ("core.plan.job_launched", "p-1"),
            ("core.plan.launch_refused", "p-1"),
            ("core.job.started", "j-3"),
            ("core.plan.job_launched", "p-1"),
            ("core.plan.launched_all", "p-1"),
        ]
    );
    assert_eq!(
        terra.events()[1].payload,
        json!({ "plan": "p-1", "job": "j-1", "session": "session-1" })
    );
    // The inner events set their facts, as an outside launch would.
    assert_eq!(terra.fact("j-1", "state"), "in_progress");
    assert_eq!(terra.fact("j-2", "state"), "done");
}

#[tokio::test]
async fn the_outer_vine_emits_its_summary_when_every_job_is_done() {
    let world = World::new();
    let terra = plan_of(&world, &[("j-1", "ready"), ("j-2", "ready")]);
    let answer = terra.press("launch_all", launch_all()).await;
    let last = answer.landed().pop().unwrap();
    assert_eq!(
        last,
        on_plan(
            "core.plan.launched_all",
            json!({ "plan": "p-1", "jobs": ["j-1", "j-2"] })
        )
    );
}

#[tokio::test]
async fn a_job_whose_plan_was_paused_meanwhile_is_refused_and_gets_no_working_copy() {
    let world = World::new();
    let terra = plan_of(&world, &[("j-1", "ready"), ("j-2", "ready")]);
    // j-1's launch is partway through when someone pauses the plan.
    let t = terra.clone();
    world.when("git.worktree_add", move || {
        t.set("p-1", "state", json!("paused"))
    });
    terra.press("launch_all", launch_all()).await;
    // j-2 is refused by the plan gate, so nothing is made for it.
    assert_eq!(world.count("git.worktree_add j-2"), 0);
    assert_eq!(terra.requests_of("launch").len(), 2);
    let events = terra.events();
    // j-1's `started` is abandoned: its plan changed while it ran. The
    // outer vine's own outputs read the plan too, so they are abandoned
    // as well, the refusal of j-2 among them.
    assert_eq!(
        kinds(&events),
        [
            ("abandoned", "j-1"),
            ("abandoned", "p-1"),
            ("abandoned", "p-1")
        ]
    );
    assert_eq!(events[0].payload["output"], "started");
    assert_eq!(events[1].payload["output"], "refused");
    assert_eq!(events[1].payload["payload"]["gate"], "plan");
    assert_eq!(events[2].payload["output"], "done");
}

#[tokio::test]
async fn a_job_whose_launch_fails_is_reported_and_the_rest_launch() {
    let world = World::new();
    let terra = plan_of(&world, &[("j-1", "ready"), ("j-2", "ready")]);
    world.fail("agent.start", "unavailable", "no agent host");
    let answer = terra.press("launch_all", launch_all()).await;
    let not_launched = on_plan(
        "core.plan.job_not_launched",
        json!({ "plan": "p-1", "job": "j-1", "reason": "no agent host" }),
    );
    assert!(answer.landed().contains(&not_launched), "{answer:?}");
    // The failed job's own vine removed its working copy.
    assert_eq!(world.trees(), ["/trees/j-2"]);
    assert_eq!(kinds(&terra.events())[0], ("core.job.launch_failed", "j-1"));
}

// ---------------------------------------------- Terra builds the inner input

#[tokio::test]
async fn the_outer_vine_passes_only_the_job_and_terra_builds_the_rest() {
    let world = World::new();
    let terra = plan_of(&world, &[("j-1", "ready")]);
    terra.set("p-1", "branch", json!("plan/renamed"));
    terra.press("launch_all", launch_all()).await;
    // The branch came from the job's plan, read by Terra for the press.
    assert_eq!(world.log()[0], "git.refresh_branch plan/renamed");
    // The press's id is built from the outer press's, the move and the
    // payload the outer vine sent: only the job.
    assert_eq!(terra.requests_of("launch"), [r#"r-1/launch{"job":"j-1"}"#]);
}

#[tokio::test]
async fn a_job_missing_a_fact_its_vine_needs_is_refused_by_that_fact() {
    let world = World::new();
    let terra = plan_of(&world, &[("j-1", "ready")]);
    terra.put(
        "j-1",
        json!({ "state": "ready", "plan": "p-1", "waiting_on": 0 }),
    );
    let answer = terra.press("launch_all", launch_all()).await;
    let refusal = on_plan(
        "core.plan.launch_refused",
        json!({ "plan": "p-1", "job": "j-1", "gate": "missing:job.brief" }),
    );
    assert!(answer.landed().contains(&refusal), "{answer:?}");
    assert!(world.log().is_empty());
}

#[tokio::test]
async fn a_plan_with_no_jobs_presses_nothing_and_has_no_summary() {
    let world = World::new();
    let terra = plan_of(&world, &[]);
    let answer = terra.press("launch_all", launch_all()).await;
    assert!(answer.landed().is_empty());
    assert!(terra.requests_of("launch").is_empty());
}

/// A trellis with `job_launch`, its move, and one more vine and move.
fn with_extra(doc: &str, extra: Move) -> Trellis {
    Trellis {
        vines: vec![yaml("job_launch"), VineSource::Yaml(doc.into())],
        moves: vec![terra::moves::launch_move(), extra],
        reactions: vec![],
    }
}

fn press_once(vine: &'static str) -> Move {
    Move {
        name: vine,
        target: "job",
        payload: vec![("job", Ty::Id)],
        gates: vec![],
        needs: vec![Need::request()],
        vine: Some(vine),
        events: vec![Lands::new("ended", "x.ended")],
    }
}

#[tokio::test]
async fn a_nested_press_under_a_request_that_is_not_running_fails_the_vine() {
    let world = World::new();
    let terra = Terra::new(registry(&world));
    terra.put("j-1", json!({ "state": "ready" }));
    let doc = "vine: forged\ninput: { job: id, request: text }\noutputs: { ended: { status: text } }\n\
        pipe:\n  - call: move.launch\n    with: { request: nobody, job: $item.job }\n    end: ended\n  \
        - filter: $item.ended\n  - emit: { to: ended, with: { status: $item.ended.status } }\n";
    terra
        .install(with_extra(doc, press_once("forged")))
        .unwrap();
    let answer = terra.press("forged", json!({ "job": "j-1" })).await;
    let Answer::Ran {
        end: End::Failed(why),
        ..
    } = answer
    else {
        panic!("expected the vine to fail, got {answer:?}")
    };
    assert!(why.contains("no_request"), "{why}");
    assert!(world.log().is_empty());
}

// ------------------------------------------------- `use` is for helpers only

#[test]
fn install_allows_a_vine_that_uses_a_helper_no_move_starts() {
    let world = World::new();
    // `job_launch_undo` uses `job_teardown`, which no move starts.
    Terra::new(registry(&world))
        .install(jobs_trellis())
        .unwrap();
}

#[test]
fn install_refuses_a_vine_that_uses_a_vine_a_move_starts() {
    let world = World::new();
    let doc = "vine: sneaky\ninput: { job: id, request: text }\noutputs: { ended: { status: text } }\n\
        pipe:\n  - use: job_launch\n    with: { job: $item.job, plan_branch: main, brief: b }\n    on:\n      \
        started: [ { emit: { to: ended, with: { status: started } } } ]\n      \
        failed: [ { emit: { to: ended, with: { status: failed } } } ]\n";
    let err = Terra::new(registry(&world))
        .install(with_extra(doc, press_once("sneaky")))
        .unwrap_err();
    assert_eq!(
        err,
        InstallError::UsesStartedVine {
            vine: "sneaky".into(),
            uses: "job_launch".into(),
            started_by: "launch".into(),
        }
    );
}

#[test]
fn install_refuses_a_vine_that_uses_a_started_vine_through_a_helper() {
    let world = World::new();
    let helper = "vine: wrapper\ninput: { job: id }\noutputs: { ended: { status: text } }\n\
        pipe:\n  - use: job_launch\n    with: { job: $item.job, plan_branch: main, brief: b }\n    on:\n      \
        started: [ { emit: { to: ended, with: { status: started } } } ]\n      \
        failed: [ { emit: { to: ended, with: { status: failed } } } ]\n";
    let trellis = Trellis {
        vines: vec![yaml("job_launch"), VineSource::Yaml(helper.into())],
        moves: vec![terra::moves::launch_move()],
        reactions: vec![],
    };
    let err = Terra::new(registry(&world)).install(trellis).unwrap_err();
    // The helper itself is the vine that uses it; no move starts the
    // helper, and still it is refused.
    assert!(
        matches!(err, InstallError::UsesStartedVine { ref vine, .. } if vine == "wrapper"),
        "{err:?}"
    );
}

#[test]
fn a_vine_of_one_trellis_can_neither_use_nor_press_into_another() {
    let world = World::new();
    let terra = Terra::new(registry(&world));
    terra.install(jobs_trellis()).unwrap();
    let uses = "vine: other\ninput: { session: text, tree: text }\noutputs: { ok: {} }\n\
        pipe:\n  - use: job_teardown\n    on:\n      torn_down: [ { emit: ok } ]\n      failed: [ { emit: ok } ]\n";
    let presses = "vine: other\ninput: { job: id, request: text }\noutputs: { ok: {} }\n\
        pipe:\n  - call: move.launch\n    with: { request: $item.request, job: $item.job }\n  - emit: ok\n";
    for doc in [uses, presses] {
        let trellis = Trellis {
            vines: vec![VineSource::Yaml(doc.into())],
            moves: vec![],
            reactions: vec![],
        };
        let err = terra.install(trellis).unwrap_err();
        assert!(
            matches!(
                err,
                InstallError::Load {
                    error: LoadError::UnknownVine { .. } | LoadError::UnknownAct { .. },
                    ..
                }
            ),
            "{err:?}"
        );
    }
}

#[test]
fn the_press_act_checks_the_payload_the_outer_vine_sends_at_install() {
    let world = World::new();
    // `launch` takes a job id; this vine sends a number.
    let doc = "vine: wrong\ninput: { job: id, request: text }\noutputs: { ended: {} }\n\
        pipe:\n  - call: move.launch\n    with: { request: $item.request, job: 7 }\n  - emit: ended\n";
    let mut extra = press_once("wrong");
    extra.events = vec![Lands::new("ended", "x.ended")];
    let err = Terra::new(registry(&world))
        .install(with_extra(doc, extra))
        .unwrap_err();
    assert!(
        matches!(
            err,
            InstallError::Load {
                error: LoadError::BadSettings { .. },
                ..
            }
        ),
        "{err:?}"
    );
}
