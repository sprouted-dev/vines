//! Gates on the move, work in the vine. A stand-in for Terra checks the
//! move's gates, builds the vine's input, runs the vine as a stream fed
//! one item, and turns each output into an event only if nothing the
//! press read has changed.
#![cfg(feature = "yaml")]

mod acts;
mod terra;

use std::sync::Arc;

use acts::{registry, World};
use terra::moves::{items_trellis, jobs_trellis, launch_move, yaml};
use terra::{
    Answer, End, Event, InstallError, Lands, Move, Need, Refusal, Terra, Trellis, VineSource,
    ABANDONED,
};
use vine_lang::{json, LoadError, Ty, Value};

/// Terra with a ready job on an active plan, and the jobs trellis.
fn setup(world: &Arc<World>) -> Arc<Terra> {
    let terra = Terra::new(registry(world));
    terra.put(
        "p-1",
        json!({ "state": "active", "branch": "plan/p-1", "base": "origin/main" }),
    );
    terra.put(
        "job-1",
        json!({ "state": "ready", "plan": "p-1", "waiting_on": 0, "brief": "do the work" }),
    );
    terra.install(jobs_trellis()).unwrap();
    Arc::new(terra)
}

fn launch() -> Value {
    json!({ "job": "job-1" })
}

fn refused(gate: &str) -> Answer {
    Answer::Refused(Refusal::Gate(gate.into()))
}

fn started(tree: &str, session: &str) -> Event {
    Event {
        artifact: "job-1".into(),
        kind: "core.job.started".into(),
        payload: json!({ "job": "job-1", "session": session, "tree": tree }),
    }
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
        assert_eq!(terra.press("launch", launch()).await, refused(gate));
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
    assert_eq!(terra.press("launch", launch()).await, refused("state"));
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
        let answer = terra.press("launch", payload).await;
        assert!(
            matches!(answer, Answer::Refused(Refusal::Payload(_))),
            "{answer:?}"
        );
    }
    assert!(world.log().is_empty());
}

#[tokio::test]
async fn a_move_on_an_artifact_that_does_not_exist_is_refused() {
    let world = World::new();
    let terra = setup(&world);
    let answer = terra.press("launch", json!({ "job": "job-404" })).await;
    assert_eq!(answer, refused("state"));
    assert!(world.log().is_empty());
}

#[tokio::test]
async fn an_unknown_move_is_refused() {
    let world = World::new();
    let terra = setup(&world);
    assert_eq!(
        terra.press("teleport", launch()).await,
        Answer::Refused(Refusal::UnknownMove)
    );
}

// ------------------------------------------------------------- the input

#[tokio::test]
async fn the_vine_gets_the_payload_plus_the_facts_the_move_names() {
    let world = World::new();
    let terra = setup(&world);
    let answer = terra.press("launch", launch()).await;
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
    let event = started("/trees/job-1", "session-1");
    assert_eq!(answer.landed(), terra.events());
    assert_eq!(terra.events(), [event]);
}

#[tokio::test]
async fn a_landed_event_sets_the_facts_its_move_declares() {
    let world = World::new();
    let terra = setup(&world);
    terra.press("launch", launch()).await;
    assert_eq!(terra.fact("job-1", "state"), "in_progress");
    // So the same launch again is refused by the job's own gate.
    assert_eq!(terra.press("launch", launch()).await, refused("state"));
    assert_eq!(world.count("git.worktree_add"), 1);
}

#[tokio::test]
async fn a_fact_the_vine_needs_that_is_missing_refuses_the_move() {
    let world = World::new();
    let terra = setup(&world);
    terra.put(
        "job-1",
        json!({ "state": "ready", "plan": "p-1", "waiting_on": 0 }),
    );
    assert_eq!(
        terra.press("launch", launch()).await,
        Answer::Refused(Refusal::Missing("job.brief".into()))
    );
    assert!(world.log().is_empty());
}

#[tokio::test]
async fn a_failed_output_lands_as_its_own_event() {
    let world = World::new();
    let terra = setup(&world);
    world.fail("git.refresh_branch", "conflict", "merge conflict");
    let answer = terra.press("launch", launch()).await;
    let [event] = &answer.landed()[..] else {
        panic!("expected one event, got {answer:?}")
    };
    assert_eq!(event.kind, "core.job.launch_failed");
    assert_eq!(
        event.payload,
        json!({ "job": "job-1", "reason": "merge conflict" })
    );
    // A failure sets no fact: the job can be launched again.
    assert_eq!(terra.fact("job-1", "state"), "ready");
}

// -------------------------------------------------- one press, many outputs

/// A trellis whose one move starts a vine that emits once per note, and
/// the notes it read off its artifact.
fn notes_trellis(sets: Option<(&'static str, Value)>) -> Trellis {
    let doc = "vine: notes\n\
        input: { note: id, lines: list }\n\
        outputs:\n  line: { note: id, text: text }\n\
        pipe:\n  - split: $item.lines\n    into: line\n  \
        - emit: { to: line, with: { note: $item.note, text: $item.line } }\n";
    let mut lands = Lands::new("line", "notes.line");
    if let Some((field, value)) = sets {
        lands = lands.sets(field, value);
    }
    Trellis {
        vines: vec![VineSource::Yaml(doc.into())],
        moves: vec![Move {
            name: "read_note",
            target: "note",
            payload: vec![("note", Ty::Id)],
            gates: vec![],
            needs: vec![Need::fact("lines", Ty::List, "note.lines")],
            vine: Some("notes"),
            events: vec![lands],
        }],
        reactions: vec![],
    }
}

#[tokio::test]
async fn every_output_of_one_press_lands_as_its_own_event_in_order() {
    let terra = Terra::new(vine_lang::Registry::new());
    terra.install(notes_trellis(None)).unwrap();
    terra.put("n-1", json!({ "lines": ["a", "b", "c"] }));
    let answer = terra.press("read_note", json!({ "note": "n-1" })).await;
    let texts: Vec<Value> = answer
        .landed()
        .iter()
        .map(|e| e.payload["text"].clone())
        .collect();
    assert_eq!(texts, [json!("a"), json!("b"), json!("c")]);
    assert_eq!(terra.events(), answer.landed());
}

#[tokio::test]
async fn a_press_that_emits_nothing_lands_nothing_and_ends_done() {
    let terra = Terra::new(vine_lang::Registry::new());
    terra.install(notes_trellis(None)).unwrap();
    terra.put("n-1", json!({ "lines": [] }));
    let answer = terra.press("read_note", json!({ "note": "n-1" })).await;
    let Answer::Ran { landings, end, .. } = answer else {
        panic!("expected a run")
    };
    assert!(landings.is_empty());
    assert_eq!(end, End::Done);
}

#[tokio::test]
async fn a_press_is_not_made_stale_by_its_own_events() {
    // Each line sets a fact of the note the press read. The press's own
    // writes move what it saw, so its next line still lands.
    let terra = Terra::new(vine_lang::Registry::new());
    terra
        .install(notes_trellis(Some(("read", json!(true)))))
        .unwrap();
    terra.put("n-1", json!({ "lines": ["a", "b"] }));
    let answer = terra.press("read_note", json!({ "note": "n-1" })).await;
    assert_eq!(answer.landed().len(), 2);
    assert!(answer.abandoned().is_empty());
}

#[tokio::test]
async fn a_vine_that_fails_with_nothing_to_catch_it_ends_the_press_failed() {
    let world = World::new();
    let terra = Terra::new(registry(&world));
    let doc = "vine: bare\ninput: { job: id }\noutputs: { made: { path: text } }\n\
        pipe:\n  - call: git.worktree_add\n    with: { name: $item.job, from: main }\n    into: tree\n  \
        - emit: { to: made, with: { path: $item.tree.path } }\n";
    terra
        .install(Trellis {
            vines: vec![VineSource::Yaml(doc.into())],
            moves: vec![Move {
                name: "make",
                target: "job",
                payload: vec![("job", Ty::Id)],
                gates: vec![],
                needs: vec![],
                vine: Some("bare"),
                events: vec![Lands::new("made", "x.made")],
            }],
            reactions: vec![],
        })
        .unwrap();
    world.fail("git.worktree_add", "disk", "disk full");
    let answer = terra.press("make", json!({ "job": "j" })).await;
    let Answer::Ran { landings, end, .. } = answer else {
        panic!("expected a run")
    };
    assert!(landings.is_empty());
    assert!(
        matches!(end, End::Failed(ref why) if why.contains("disk full")),
        "{end:?}"
    );
}

// -------------------------------------------- only if nothing changed

#[tokio::test]
async fn an_output_does_not_land_when_a_fact_the_vine_was_given_changed_meanwhile() {
    let world = World::new();
    let terra = setup(&world);
    // While the vine is making the working copy, someone pauses the plan.
    let t = terra.clone();
    world.when("git.worktree_add", move || {
        t.set("p-1", "state", json!("paused"))
    });
    let answer = terra.press("launch", launch()).await;
    assert!(answer.landed().is_empty());
    let [abandoned] = &answer.abandoned()[..] else {
        panic!("expected one abandoned event, got {answer:?}")
    };
    assert_eq!(abandoned.kind, ABANDONED);
    assert_eq!(abandoned.payload["output"], "started");
    assert_eq!(abandoned.payload["changed"], "p-1");
    // The job did not move on: the abandoned event sets no fact.
    assert_eq!(terra.fact("job-1", "state"), "ready");
}

#[tokio::test]
async fn a_second_request_sees_the_first_ones_event_as_a_change() {
    let world = World::new();
    let terra = setup(&world);
    // A second launch of the same job lands while the first is running.
    let t = terra.clone();
    world.when("agent.start", move || {
        t.set("job-1", "live_session", json!("session-0"))
    });
    let answer = terra.press("launch", launch()).await;
    assert_eq!(answer.abandoned()[0].payload["changed"], "job-1");
    assert!(answer.landed().is_empty());
}

#[tokio::test]
async fn a_change_to_an_artifact_the_move_never_read_does_not_block_the_write() {
    let world = World::new();
    let terra = setup(&world);
    terra.put("job-2", json!({ "state": "ready" }));
    let t = terra.clone();
    world.when("git.worktree_add", move || {
        t.set("job-2", "state", json!("done"))
    });
    let answer = terra.press("launch", launch()).await;
    assert_eq!(answer.landed().len(), 1);
}

// ----------------------------------------------------- a move of only gates

#[tokio::test]
async fn a_move_that_is_all_gates_needs_no_vine() {
    let terra = Terra::new(vine_lang::Registry::new());
    terra.install(items_trellis()).unwrap();
    terra.put("an-1", json!({ "state": "open" }));
    let claim = json!({ "item": "an-1", "claimant": "ana" });
    let answer = terra.press("claim", claim.clone()).await;
    let event = Event {
        artifact: "an-1".into(),
        kind: "core.item.claimed".into(),
        payload: claim,
    };
    assert_eq!(answer.landed(), [event]);

    terra.set("an-1", "holder", json!("ana"));
    let again = json!({ "item": "an-1", "claimant": "bo" });
    assert_eq!(terra.press("claim", again).await, refused("held"));
    terra.set("an-1", "holder", Value::Null);
    terra.set("an-1", "state", json!("closed"));
    let closed = json!({ "item": "an-1", "claimant": "bo" });
    assert_eq!(terra.press("claim", closed).await, refused("state"));
}

// ------------------------------------------------------------ activate

#[tokio::test]
async fn activate_gates_on_the_plans_state_and_hands_the_vine_its_base() {
    let world = World::new();
    let terra = setup(&world);
    terra.put("p-2", json!({ "state": "draft", "base": "origin/main" }));
    let activate = json!({ "plan": "p-2" });
    assert_eq!(
        terra.press("activate", activate.clone()).await,
        refused("state")
    );
    assert!(world.log().is_empty());

    terra.set("p-2", "state", json!("ready"));
    let answer = terra.press("activate", activate).await;
    let [event] = &answer.landed()[..] else {
        panic!("expected one event, got {answer:?}")
    };
    assert_eq!(event.kind, "core.plan.activated");
    assert_eq!(
        event.payload,
        json!({ "plan": "p-2", "branch": "plan/p-2", "lane": "/trees/p-2" })
    );
}

// ------------------------------------------------- checked at install

fn broken(change: impl FnOnce(&mut Trellis)) -> InstallError {
    let world = World::new();
    let mut trellis = jobs_trellis();
    change(&mut trellis);
    Terra::new(registry(&world)).install(trellis).unwrap_err()
}

fn launch_of(trellis: &mut Trellis) -> &mut Move {
    trellis
        .moves
        .iter_mut()
        .find(|m| m.name == "launch")
        .unwrap()
}

fn misfit(err: &InstallError) -> &str {
    match err {
        InstallError::InputMisfit {
            move_name, problem, ..
        } if move_name == "launch" => problem,
        other => panic!("expected the launch input to misfit, got {other:?}"),
    }
}

#[test]
fn install_refuses_a_move_that_does_not_give_the_vine_a_field_it_needs() {
    let err = broken(|t| launch_of(t).needs.retain(|n| n.name != "brief"));
    assert!(
        misfit(&err).contains("missing required field `brief`"),
        "{err:?}"
    );
}

#[test]
fn install_refuses_a_move_that_gives_the_vine_a_field_of_the_wrong_type() {
    let err =
        broken(|t| launch_of(t).needs[0] = Need::fact("plan_branch", Ty::Int, "job.plan.branch"));
    assert!(misfit(&err).contains("`plan_branch` is int"), "{err:?}");
}

#[test]
fn install_refuses_a_move_that_gives_the_vine_a_field_it_did_not_declare() {
    let err = broken(|t| {
        launch_of(t)
            .needs
            .push(Need::fact("state", Ty::Text, "job.state"))
    });
    assert!(misfit(&err).contains("unexpected field `state`"), "{err:?}");
}

#[test]
fn install_refuses_a_move_with_no_event_for_one_of_the_vines_outputs() {
    let err = broken(|t| launch_of(t).events.retain(|l| l.output != "failed"));
    assert_eq!(
        err,
        InstallError::NoEvent {
            move_name: "launch".into(),
            output: "failed".into()
        }
    );
}

#[test]
fn install_refuses_a_move_with_an_event_for_an_output_its_vine_does_not_have() {
    let err = broken(|t| {
        launch_of(t)
            .events
            .push(Lands::new("finished", "core.job.finished"))
    });
    assert_eq!(
        err,
        InstallError::NoSuchOutput {
            move_name: "launch".into(),
            output: "finished".into()
        }
    );
}

#[test]
fn install_refuses_a_move_with_no_vine_and_no_event() {
    let mut trellis = items_trellis();
    trellis.moves[0].events.clear();
    let err = Terra::new(vine_lang::Registry::new())
        .install(trellis)
        .unwrap_err();
    assert_eq!(err, InstallError::NoVineNoDone("claim".into()));
}

#[test]
fn install_refuses_a_move_that_starts_a_vine_the_trellis_does_not_have() {
    let err = broken(|t| t.vines.retain(|_| false));
    // With no vines at all the first move to check names a missing one.
    assert!(matches!(err, InstallError::UnknownVine { .. }), "{err:?}");
}

#[test]
fn install_refuses_a_move_whose_name_is_taken() {
    let world = World::new();
    let terra = Terra::new(registry(&world));
    terra.install(jobs_trellis()).unwrap();
    let again = Trellis {
        vines: vec![yaml("job_launch")],
        moves: vec![launch_move()],
        reactions: vec![],
    };
    assert_eq!(
        terra.install(again).unwrap_err(),
        InstallError::DuplicateMove("launch".into())
    );
}

#[test]
fn install_refuses_a_payload_field_named_request() {
    let err = broken(|t| launch_of(t).payload.push(("request", Ty::Text)));
    assert_eq!(
        err,
        InstallError::ReservedField {
            move_name: "launch".into(),
            field: "request".into()
        }
    );
}

#[test]
fn install_refuses_a_vine_that_cannot_be_resumed() {
    // Terra keeps every press's journal, so it runs every vine with
    // `run_saved`. An `each` that switches drops work by when items
    // arrive, which a replay cannot know.
    let doc = "vine: wide\ninput: { job: id, items: list }\noutputs: { one: { job: id } }\n\
        pipe:\n  - split: $item.items\n    into: it\n  - each:\n      overlap: switch\n      do:\n        \
        - emit: { to: one, with: { job: $item.job } }\n";
    let err = broken(|t| {
        t.vines.push(VineSource::Yaml(doc.into()));
        t.moves.push(Move {
            name: "wide",
            target: "job",
            payload: vec![("job", Ty::Id)],
            gates: vec![],
            needs: vec![Need::fact("items", Ty::List, "job.items")],
            vine: Some("wide"),
            events: vec![Lands::new("one", "x.one")],
        });
    });
    assert_eq!(
        err,
        InstallError::NotResumable {
            vine: "wide".into()
        }
    );
}

#[test]
fn install_refuses_a_vine_that_does_not_build_and_names_its_place() {
    let err = broken(|t| t.vines.insert(0, VineSource::Yaml("vine: x\n".into())));
    assert!(
        matches!(
            err,
            InstallError::Load {
                at: 0,
                error: LoadError::Malformed { .. }
            }
        ),
        "{err:?}"
    );
}

#[test]
fn a_gate_is_plain_data_the_move_declares_and_the_vine_has_none() {
    let gates: Vec<&str> = launch_move().gates.iter().map(|g| g.gate).collect();
    assert_eq!(gates, ["state", "plan", "dependencies", "session"]);
    let world = World::new();
    let text = std::fs::read_to_string(format!(
        "{}/vines/pipe/job_launch.yaml",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let vine = registry(&world).load_pipe(&text).unwrap();
    assert!(vine.outputs().keys().all(|o| o != "refused"));
}
