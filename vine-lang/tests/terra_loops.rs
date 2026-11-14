//! The loop check at install. A trellis is refused when its moves start
//! each other in a circle that nothing stops, and allowed when the moves
//! only chain, or when a gate on the circle is sure to refuse the second
//! time round. Nothing here uses YAML, and this file passes with the
//! feature off:
//!
//!     cargo test --no-default-features --test terra_loops
#![cfg(test)]

mod fake;
mod terra;

use fake::{job_launch_pipe, registry, World};
use terra::moves::launch_move;
use terra::{
    Answer, Gate, InstallError, Lands, LoopWhy, Move, Pick, Reaction, Refusal, Terra, Trellis,
    Trigger, VineSource,
};
use vine_lang::pipe::{call, emit_with, pipe, PipeDef};
use vine_lang::{json, Registry, Shape, Ty, Value};

/// A vine over `{ thing }` that emits `done` once.
fn echo() -> PipeDef {
    let thing = || Shape::new().required("thing", Ty::Id);
    pipe("echo")
        .input(thing())
        .output("done", thing())
        .operators([emit_with("done", json!({ "thing": "$item.thing" }))])
}

/// A move on a thing that starts `echo` and lands `kind`, setting `sets`.
fn step(name: &'static str, kind: &'static str, sets: &[(&'static str, Value)]) -> Move {
    let mut lands = Lands::new("done", kind);
    for (field, value) in sets {
        lands = lands.sets(field, value.clone());
    }
    Move {
        name,
        target: "thing",
        payload: vec![("thing", Ty::Id)],
        gates: vec![],
        needs: vec![],
        vine: Some("echo"),
        events: vec![lands],
    }
}

fn gated(mut m: Move, path: &'static str, equals: Value) -> Move {
    m.gates.push(Gate {
        path,
        equals,
        name: "gate",
    });
    m
}

/// When `kind` lands, press `press` on the same thing.
fn on(kind: &'static str, press: &'static str) -> Reaction {
    Reaction {
        on: Trigger::Event(kind),
        press,
        payload: vec![("thing", Pick::Artifact)],
    }
}

fn trellis(moves: Vec<Move>, reactions: Vec<Reaction>) -> Trellis {
    Trellis {
        vines: vec![VineSource::Def(echo())],
        moves,
        reactions,
    }
}

fn install(t: Trellis) -> Result<(), InstallError> {
    Terra::new(Registry::new()).install(t)
}

fn refused_loop(moves: &[&str], why: LoopWhy) -> Result<(), InstallError> {
    Err(InstallError::Loop {
        moves: moves.iter().map(|m| (*m).into()).collect(),
        why,
    })
}

// ------------------------------------------------------------- refused

#[test]
fn a_move_whose_event_presses_it_again_with_no_gate_is_refused() {
    let t = trellis(
        vec![step("ping", "x.pinged", &[])],
        vec![on("x.pinged", "ping")],
    );
    assert_eq!(install(t), refused_loop(&["ping"], LoopWhy::NoGate));
}

#[test]
fn two_moves_that_press_each_other_with_no_gate_are_refused() {
    let t = trellis(
        vec![step("ping", "x.pinged", &[]), step("pong", "x.ponged", &[])],
        vec![on("x.pinged", "pong"), on("x.ponged", "ping")],
    );
    assert_eq!(install(t), refused_loop(&["ping", "pong"], LoopWhy::NoGate));
}

#[test]
fn a_loop_through_an_abandoned_output_is_refused() {
    let t = trellis(
        vec![step("ping", "x.pinged", &[])],
        vec![Reaction {
            on: Trigger::Abandoned { by: "ping" },
            press: "ping",
            payload: vec![("thing", Pick::Artifact)],
        }],
    );
    assert_eq!(install(t), refused_loop(&["ping"], LoopWhy::NoGate));
}

#[test]
fn a_loop_whose_gate_reads_a_fact_the_loop_never_sets_is_refused() {
    // Relaunching a job when its launch fails: the failure sets nothing,
    // so the `state` gate passes every time round.
    let world = World::new();
    let t = Trellis {
        vines: vec![VineSource::Def(job_launch_pipe())],
        moves: vec![launch_move()],
        reactions: vec![Reaction {
            on: Trigger::Event("core.job.launch_failed"),
            press: "launch",
            payload: vec![("job", Pick::Artifact)],
        }],
    };
    let err = Terra::new(registry(&world)).install(t);
    assert_eq!(err, refused_loop(&["launch"], LoopWhy::GatesHold));
}

#[test]
fn a_loop_that_sets_the_gated_fact_back_before_the_gate_is_refused() {
    // `busy` sets the state away from the gate's value, and `rest` sets it
    // back before `busy` is pressed again.
    let t = trellis(
        vec![
            gated(
                step("busy", "x.busy", &[("state", json!("busy"))]),
                "thing.state",
                json!("idle"),
            ),
            step("rest", "x.rested", &[("state", json!("idle"))]),
        ],
        vec![on("x.busy", "rest"), on("x.rested", "busy")],
    );
    assert_eq!(
        install(t),
        refused_loop(&["busy", "rest"], LoopWhy::GatesHold)
    );
}

#[test]
fn a_gate_on_another_artifact_is_not_trusted_to_stop_a_loop() {
    let t = trellis(
        vec![gated(
            step("ping", "x.pinged", &[("state", json!("done"))]),
            "thing.plan.state",
            json!("active"),
        )],
        vec![on("x.pinged", "ping")],
    );
    assert_eq!(install(t), refused_loop(&["ping"], LoopWhy::GatesHold));
}

#[test]
fn a_reaction_that_takes_its_target_from_the_payload_breaks_the_gate_s_promise() {
    let mut reaction = on("x.pinged", "ping");
    reaction.payload = vec![("thing", Pick::Field("thing"))];
    let t = trellis(
        vec![gated(
            step("ping", "x.pinged", &[("state", json!("done"))]),
            "thing.state",
            json!("ready"),
        )],
        vec![reaction],
    );
    assert_eq!(
        install(t),
        refused_loop(&["ping"], LoopWhy::ArtifactMayChange)
    );
}

#[test]
fn a_vine_that_presses_its_own_move_is_refused_even_with_a_gate() {
    let thing = || Shape::new().required("thing", Ty::Id);
    let again = pipe("again")
        .input(thing().required("request", Ty::Text))
        .output("done", thing())
        .operators([
            call(
                "move.again",
                json!({ "request": "$item.request", "thing": "$item.thing" }),
            )
            .end("ended"),
            vine_lang::pipe::filter("$item.ended"),
            emit_with("done", json!({ "thing": "$item.thing" })),
        ]);
    let mut m = gated(
        step("again", "x.again", &[("state", json!("done"))]),
        "thing.state",
        json!("ready"),
    );
    m.vine = Some("again");
    m.needs = vec![terra::Need::request()];
    let t = Trellis {
        vines: vec![VineSource::Def(again)],
        moves: vec![m],
        reactions: vec![],
    };
    assert_eq!(
        install(t),
        refused_loop(&["again"], LoopWhy::ArtifactMayChange)
    );
}

#[test]
fn a_refused_loop_installs_nothing() {
    let terra = Terra::new(Registry::new());
    let t = trellis(
        vec![step("ping", "x.pinged", &[])],
        vec![on("x.pinged", "ping")],
    );
    assert!(terra.install(t).is_err());
    // `ping` was not installed, so a fixed trellis can take the name.
    let fixed = trellis(vec![step("ping", "x.pinged", &[])], vec![]);
    terra.install(fixed).unwrap();
}

// ------------------------------------------------------------- allowed

#[test]
fn moves_that_only_chain_are_allowed() {
    let t = trellis(
        vec![
            step("first", "x.first", &[]),
            step("second", "x.second", &[]),
            step("third", "x.third", &[]),
        ],
        vec![
            on("x.first", "second"),
            on("x.second", "third"),
            on("x.first", "third"),
        ],
    );
    assert_eq!(install(t), Ok(()));
}

#[tokio::test]
async fn a_chain_runs_each_move_once() {
    let terra = Terra::new(Registry::new());
    terra
        .install(trellis(
            vec![
                step("first", "x.first", &[]),
                step("second", "x.second", &[]),
            ],
            vec![on("x.first", "second")],
        ))
        .unwrap();
    terra.put("t-1", json!({}));
    terra.press("first", json!({ "thing": "t-1" })).await;
    let answers = terra.react().await;
    assert_eq!(answers.len(), 1);
    let kinds: Vec<String> = terra.events().into_iter().map(|e| e.kind).collect();
    assert_eq!(kinds, ["x.first", "x.second"]);
}

#[test]
fn a_loop_whose_own_event_makes_its_gate_refuse_is_allowed() {
    // A started job is pressed to launch again. `started` sets the job's
    // state to `in_progress`, so the `state` gate refuses the second time.
    let world = World::new();
    let t = Trellis {
        vines: vec![VineSource::Def(job_launch_pipe())],
        moves: vec![launch_move()],
        reactions: vec![Reaction {
            on: Trigger::Event("core.job.started"),
            press: "launch",
            payload: vec![("job", Pick::Artifact)],
        }],
    };
    assert_eq!(Terra::new(registry(&world)).install(t), Ok(()));
}

#[tokio::test]
async fn a_loop_its_gate_breaks_goes_round_once_and_is_refused_by_name() {
    let t = trellis(
        vec![gated(
            step("ping", "x.pinged", &[("state", json!("done"))]),
            "thing.state",
            json!("ready"),
        )],
        vec![on("x.pinged", "ping")],
    );
    let terra = Terra::new(Registry::new());
    terra.install(t).unwrap();
    terra.put("t-1", json!({ "state": "ready" }));
    terra.press("ping", json!({ "thing": "t-1" })).await;
    let answers = terra.react().await;
    assert_eq!(answers, [Answer::Refused(Refusal::Gate("gate".into()))]);
    assert_eq!(terra.events().len(), 1);
}
