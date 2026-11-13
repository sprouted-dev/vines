//! Gates on the move, work in the vine. A stand-in for Terra checks the
//! move's gates, builds the vine's input, runs the vine, and turns its
//! output into an event only if nothing it read has changed.
#![cfg(feature = "yaml")]

mod acts;
mod terra;

use std::sync::Arc;

use acts::{registry, World};
use terra::{activate_move, claim_move, launch_move, Event, Gate, Move, Need, Outcome, Terra};
use vine_lang::{json, Ty, Value, Vine};

fn vine(world: &Arc<World>, name: &str) -> Vine {
    let path = format!("{}/vines/core/{name}.yaml", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    registry(world).load(&text).unwrap()
}

/// Terra with a ready job on an active plan, and `launch` installed.
fn setup(world: &Arc<World>) -> Terra {
    let mut terra = Terra::new();
    terra.put(
        "p-1",
        json!({ "state": "active", "branch": "plan/p-1", "base": "origin/main" }),
    );
    terra.put(
        "job-1",
        json!({ "state": "ready", "plan": "p-1", "waiting_on": 0, "brief": "do the work" }),
    );
    terra
        .install(launch_move(), Some(vine(world, "job_launch")))
        .unwrap();
    terra
}

fn launch() -> Value {
    json!({ "job": "job-1" })
}

// ------------------------------------------------------------------ gates

#[tokio::test]
async fn each_gate_refuses_by_name_and_the_vine_never_runs() {
    let changes: [(&str, &str, Value, &str); 4] = [
        ("job-1", "state", json!("in_progress"), "state"),
        ("p-1", "state", json!("paused"), "plan"),
        ("job-1", "waiting_on", json!(2), "dependencies"),
        ("job-1", "live_session", json!("session-0"), "session"),
    ];
    for (artifact, field, value, gate) in changes {
        let world = World::new();
        let terra = setup(&world);
        terra.set(artifact, field, value);
        let outcome = terra.request("launch", launch()).await;
        assert_eq!(outcome, Outcome::Refused { gate: gate.into() });
        assert!(world.log().is_empty(), "gate {gate} ran {:?}", world.log());
        assert!(terra.events().is_empty());
    }
}

#[tokio::test]
async fn gates_are_checked_in_the_order_the_move_declares_them() {
    let world = World::new();
    let terra = setup(&world);
    terra.set("job-1", "state", json!("done"));
    terra.set("job-1", "waiting_on", json!(2));
    let outcome = terra.request("launch", launch()).await;
    assert_eq!(
        outcome,
        Outcome::Refused {
            gate: "state".into()
        }
    );
}

#[tokio::test]
async fn a_payload_that_does_not_fit_the_move_is_refused() {
    let world = World::new();
    let terra = setup(&world);
    for payload in [
        json!({}),
        json!({ "job": 7 }),
        json!({ "job": "job-1", "extra": 1 }),
    ] {
        let outcome = terra.request("launch", payload).await;
        assert_eq!(
            outcome,
            Outcome::Refused {
                gate: "payload".into()
            }
        );
    }
    assert!(world.log().is_empty());
}

#[tokio::test]
async fn a_move_on_an_artifact_that_does_not_exist_is_refused() {
    let world = World::new();
    let terra = setup(&world);
    let outcome = terra.request("launch", json!({ "job": "job-404" })).await;
    assert_eq!(
        outcome,
        Outcome::Refused {
            gate: "state".into()
        }
    );
    assert!(world.log().is_empty());
}

#[tokio::test]
async fn an_unknown_move_is_refused() {
    let world = World::new();
    let terra = setup(&world);
    let outcome = terra.request("teleport", launch()).await;
    assert_eq!(
        outcome,
        Outcome::Refused {
            gate: "unknown_move".into()
        }
    );
}

// ------------------------------------------------------------- the input

#[tokio::test]
async fn the_vine_gets_the_payload_plus_the_facts_the_move_names() {
    let world = World::new();
    let terra = setup(&world);
    let outcome = terra.request("launch", launch()).await;
    // The caller sent only the job. The branch came from the job's plan
    // and the brief from the job.
    assert_eq!(
        world.log(),
        [
            "git.refresh_branch plan/p-1",
            "git.worktree_add job-1",
            "agent.start /trees/job-1"
        ]
    );
    let event = Event {
        artifact: "job-1".into(),
        kind: "core.job.started".into(),
        payload: json!({ "job": "job-1", "session": "session-1", "tree": "/trees/job-1" }),
    };
    assert_eq!(outcome, Outcome::Landed(event.clone()));
    assert_eq!(terra.events(), [event]);
}

#[tokio::test]
async fn a_fact_the_vine_needs_that_is_missing_refuses_the_move() {
    let world = World::new();
    let terra = setup(&world);
    terra.put(
        "job-1",
        json!({ "state": "ready", "plan": "p-1", "waiting_on": 0 }),
    );
    let outcome = terra.request("launch", launch()).await;
    assert_eq!(
        outcome,
        Outcome::Refused {
            gate: "missing:job.brief".into()
        }
    );
    assert!(world.log().is_empty());
}

#[tokio::test]
async fn a_failed_output_lands_as_its_own_event() {
    let world = World::new();
    let terra = setup(&world);
    world.fail("git.refresh_branch", "conflict", "merge conflict");
    let outcome = terra.request("launch", launch()).await;
    let Outcome::Landed(event) = outcome else {
        panic!("expected an event, got {outcome:?}")
    };
    assert_eq!(event.kind, "core.job.launch_failed");
    assert_eq!(
        event.payload,
        json!({ "job": "job-1", "reason": "merge conflict" })
    );
}

// -------------------------------------------- only if nothing changed

#[tokio::test]
async fn an_output_does_not_land_when_a_fact_the_vine_was_given_changed_meanwhile() {
    let world = World::new();
    let terra = Arc::new(setup(&world));
    // While the vine is making the working copy, someone pauses the plan.
    let t = terra.clone();
    world.when("git.worktree_add", move || {
        t.set("p-1", "state", json!("paused"))
    });
    let outcome = terra.request("launch", launch()).await;
    assert_eq!(
        outcome,
        Outcome::Stale {
            artifact: "p-1".into(),
            output: "started".into()
        }
    );
    assert!(terra.events().is_empty());
}

#[tokio::test]
async fn a_second_request_sees_the_first_ones_event_as_a_change() {
    let world = World::new();
    let terra = Arc::new(setup(&world));
    // A second launch of the same job lands while the first is running.
    let t = terra.clone();
    world.when("agent.start", move || {
        t.set("job-1", "live_session", json!("session-0"))
    });
    let outcome = terra.request("launch", launch()).await;
    assert_eq!(
        outcome,
        Outcome::Stale {
            artifact: "job-1".into(),
            output: "started".into()
        }
    );
}

#[tokio::test]
async fn a_change_to_an_artifact_the_move_never_read_does_not_block_the_write() {
    let world = World::new();
    let terra = Arc::new(setup(&world));
    terra.put("job-2", json!({ "state": "ready" }));
    let t = terra.clone();
    world.when("git.worktree_add", move || {
        t.set("job-2", "state", json!("done"))
    });
    assert!(matches!(
        terra.request("launch", launch()).await,
        Outcome::Landed(_)
    ));
}

// ----------------------------------------------------- a move of only gates

#[tokio::test]
async fn a_move_that_is_all_gates_needs_no_vine() {
    let mut terra = Terra::new();
    terra.install(claim_move(), None).unwrap();
    terra.put("an-1", json!({ "state": "open" }));
    let claim = json!({ "item": "an-1", "claimant": "ana" });
    let outcome = terra.request("claim", claim.clone()).await;
    let event = Event {
        artifact: "an-1".into(),
        kind: "core.item.claimed".into(),
        payload: claim,
    };
    assert_eq!(outcome, Outcome::Landed(event));

    terra.set("an-1", "holder", json!("ana"));
    let again = json!({ "item": "an-1", "claimant": "bo" });
    assert_eq!(
        terra.request("claim", again).await,
        Outcome::Refused {
            gate: "held".into()
        }
    );
    terra.set("an-1", "holder", Value::Null);
    terra.set("an-1", "state", json!("closed"));
    let closed = json!({ "item": "an-1", "claimant": "bo" });
    assert_eq!(
        terra.request("claim", closed).await,
        Outcome::Refused {
            gate: "state".into()
        }
    );
}

// ------------------------------------------------------------ activate

#[tokio::test]
async fn activate_gates_on_the_plans_state_and_hands_the_vine_its_base() {
    let world = World::new();
    let mut terra = Terra::new();
    terra
        .install(activate_move(), Some(vine(&world, "plan_activate")))
        .unwrap();
    terra.put("p-1", json!({ "state": "draft", "base": "origin/main" }));
    let activate = json!({ "plan": "p-1" });
    assert_eq!(
        terra.request("activate", activate.clone()).await,
        Outcome::Refused {
            gate: "state".into()
        }
    );
    assert!(world.log().is_empty());

    terra.set("p-1", "state", json!("ready"));
    let Outcome::Landed(event) = terra.request("activate", activate).await else {
        panic!("expected an event")
    };
    assert_eq!(event.kind, "core.plan.activated");
    assert_eq!(
        event.payload,
        json!({ "plan": "p-1", "branch": "plan/p-1", "lane": "/trees/p-1" })
    );
}

// ------------------------------------------------- checked at install

fn broken(change: impl FnOnce(&mut Move)) -> String {
    let world = World::new();
    let mut decl = launch_move();
    change(&mut decl);
    Terra::new()
        .install(decl, Some(vine(&world, "job_launch")))
        .unwrap_err()
}

#[test]
fn install_refuses_a_move_that_does_not_give_the_vine_a_field_it_needs() {
    let err = broken(|m| m.needs.retain(|n| n.name != "brief"));
    assert!(err.contains("missing required field `brief`"), "{err}");
}

#[test]
fn install_refuses_a_move_that_gives_the_vine_a_field_of_the_wrong_type() {
    let err = broken(|m| {
        m.needs[0] = Need {
            name: "plan_branch",
            ty: Ty::Int,
            path: "job.plan.branch",
        }
    });
    assert!(err.contains("`plan_branch` is int"), "{err}");
}

#[test]
fn install_refuses_a_move_that_gives_the_vine_a_field_it_did_not_declare() {
    let err = broken(|m| {
        m.needs.push(Need {
            name: "state",
            ty: Ty::Text,
            path: "job.state",
        })
    });
    assert!(err.contains("unexpected field `state`"), "{err}");
}

#[test]
fn install_refuses_a_move_with_no_event_for_one_of_the_vines_outputs() {
    let err = broken(|m| m.events.retain(|(o, _)| *o != "failed"));
    assert!(err.contains("no event for output `failed`"), "{err}");
}

#[test]
fn install_refuses_a_move_with_no_vine_and_no_event() {
    let mut decl = claim_move();
    decl.events.clear();
    let err = Terra::new().install(decl, None).unwrap_err();
    assert!(err.contains("no vine and no `done` event"), "{err}");
}

#[test]
fn a_gate_is_plain_data_the_move_declares() {
    // The gates live in the move's declaration, not in any vine.
    let gates: Vec<&str> = launch_move().gates.iter().map(|g: &Gate| g.gate).collect();
    assert_eq!(gates, ["state", "plan", "dependencies", "session"]);
    let world = World::new();
    let launch = vine(&world, "job_launch");
    assert!(launch.outputs().keys().all(|o| o != "refused"));
}
