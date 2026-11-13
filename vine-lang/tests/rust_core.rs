//! The Rust core alone. Nothing here uses YAML, and this file builds and
//! passes with the `yaml` feature off:
//!
//!     cargo test --no-default-features --test rust_core

mod acts;

use acts::{claim_def, job_launch_def, ready_job, registry, with, World};
use vine_lang::def::{attempt, call, each, guard, once, op, out, stream, switch};
use vine_lang::{
    json, LoadError, OpError, OperatorSpec, Registry, RegistryError, RunError, Shape, Ty,
};

fn gate() -> Shape {
    Shape::new().required("gate", Ty::Text)
}

#[tokio::test]
async fn a_vine_built_in_rust_runs() {
    let vine = Registry::new().build(claim_def()).unwrap();
    let out = vine
        .run(json!({ "item": "an-1", "claimant": "ana", "state": "open" }))
        .await
        .unwrap();
    assert_eq!(out.name, "claimed");
    assert_eq!(out.payload, json!({ "item": "an-1", "claimant": "ana" }));
}

#[tokio::test]
async fn a_vine_built_in_rust_ends_with_its_refusals() {
    let vine = Registry::new().build(claim_def()).unwrap();
    let closed = json!({ "item": "an-1", "claimant": "ana", "state": "closed" });
    let out = vine.run(closed).await.unwrap();
    assert_eq!(
        (out.name.as_str(), &out.payload),
        ("refused", &json!({ "gate": "state" }))
    );

    let held = json!({ "item": "an-1", "claimant": "ana", "state": "open", "holder": "bo" });
    let out = vine.run(held).await.unwrap();
    assert_eq!(
        (out.name.as_str(), &out.payload),
        ("refused", &json!({ "gate": "held" }))
    );
}

#[tokio::test]
async fn a_vine_that_only_reads_its_input_reaches_nothing() {
    let vine = Registry::new().build(claim_def()).unwrap();
    assert!(vine.reach().is_empty());
}

#[tokio::test]
async fn input_that_does_not_fit_the_shape_is_refused_before_any_step() {
    let vine = Registry::new().build(claim_def()).unwrap();
    let err = vine.run(json!({ "item": "an-1" })).await.unwrap_err();
    assert!(matches!(err, RunError::InputMisfit(_)), "{err}");
}

// ------------------------------------------------------- the same checks

#[test]
fn the_checker_refuses_a_rust_vine_with_an_undeclared_output() {
    let def = once("v")
        .input(Shape::new())
        .output("done", Shape::new())
        .steps([out("finished", json!({}))]);
    let err = Registry::new().build(def).unwrap_err();
    assert_eq!(
        err,
        LoadError::UndeclaredOutput {
            path: "steps[0].out".into(),
            name: "finished".into()
        }
    );
}

#[test]
fn the_checker_refuses_a_rust_vine_that_can_end_without_an_output() {
    let def = once("v")
        .input(Shape::new().required("x", Ty::Text))
        .output("no", gate())
        .steps([guard("$in.x == \"a\"", "no", json!({ "gate": "a" }))]);
    let err = Registry::new().build(def).unwrap_err();
    assert_eq!(
        err,
        LoadError::MissingOutput {
            path: "steps".into()
        }
    );
}

#[test]
fn the_checker_refuses_a_rust_vine_that_reads_a_field_its_input_never_has() {
    let def = once("v")
        .input(Shape::new().required("x", Ty::Text))
        .output("done", Shape::new().required("y", Ty::Text))
        .steps([out("done", json!({ "y": "$in.nope" }))]);
    let err = Registry::new().build(def).unwrap_err();
    assert!(
        matches!(err, LoadError::UnknownField { ref field, .. } if field == "nope"),
        "{err}"
    );
}

#[test]
fn the_checker_refuses_a_rust_vine_with_no_outputs_or_a_bad_name() {
    let none = once("v").input(Shape::new()).steps([]);
    assert!(matches!(
        Registry::new().build(none).unwrap_err(),
        LoadError::Malformed { .. }
    ));
    let bad = once("Not A Name")
        .input(Shape::new())
        .output("done", Shape::new())
        .steps([out("done", json!({}))]);
    assert!(matches!(
        Registry::new().build(bad).unwrap_err(),
        LoadError::Malformed { .. }
    ));
}

#[test]
fn the_checker_refuses_an_unregistered_operator_by_name() {
    let def = once("v")
        .input(Shape::new())
        .output("done", Shape::new())
        .steps([op("launch_job", json!({})), out("done", json!({}))]);
    let err = Registry::new().build(def).unwrap_err();
    assert_eq!(
        err,
        LoadError::UnknownOperator {
            path: "steps[0]".into(),
            name: "launch_job".into()
        }
    );
}

// ------------------------------------------------------------------ acts

#[test]
fn the_library_ships_no_acts() {
    assert!(Registry::new().act_specs().is_empty());
}

#[test]
fn a_call_to_an_unregistered_act_is_refused_by_name() {
    let def = once("v")
        .input(Shape::new())
        .output("done", Shape::new())
        .steps([call("git.push", json!({})), out("done", json!({}))]);
    let err = Registry::new().build(def).unwrap_err();
    assert_eq!(
        err,
        LoadError::UnknownAct {
            path: "steps[0]".into(),
            name: "git.push".into()
        }
    );
}

#[test]
fn a_call_whose_input_does_not_fit_the_act_is_refused_before_it_runs() {
    let world = World::new();
    let def = once("v")
        .input(Shape::new())
        .output("done", Shape::new())
        .steps([
            call("git.worktree_remove", json!({ "dir": "/x" })),
            out("done", json!({})),
        ]);
    let err = registry(&world).build(def).unwrap_err();
    assert!(
        matches!(err, LoadError::BadSettings { ref path, ref operator, .. }
            if path == "steps[0].with" && operator == "git.worktree_remove"),
        "{err}"
    );
    assert!(world.log().is_empty());
}

#[test]
fn an_act_must_say_what_it_touches_and_have_a_well_formed_unique_name() {
    let mut reg = Registry::new();
    let ok = |_v, _cx| async { Ok(json!({})) };
    assert_eq!(
        reg.register_act(OperatorSpec::new("git.push"), ok),
        Err(RegistryError::ActTouchesNothing("git.push".into()))
    );
    assert_eq!(
        reg.register_act(OperatorSpec::new("Git Push").touches(["git"]), ok),
        Err(RegistryError::BadActName("Git Push".into()))
    );
    assert_eq!(
        reg.register_act(OperatorSpec::new("git.push").touches(["Not Good"]), ok),
        Err(RegistryError::BadTouch {
            operator: "git.push".into(),
            touch: "Not Good".into()
        })
    );
    reg.register_act(OperatorSpec::new("git.push").touches(["git"]), ok)
        .unwrap();
    assert_eq!(
        reg.register_act(OperatorSpec::new("git.push").touches(["git"]), ok),
        Err(RegistryError::Duplicate("git.push".into()))
    );
}

#[test]
fn call_is_a_reserved_word_no_operator_can_take() {
    let mut reg = Registry::new();
    let err = reg
        .register(OperatorSpec::new("call"), |_v, _cx| async { Ok(json!({})) })
        .unwrap_err();
    assert_eq!(err, RegistryError::Reserved("call".into()));
}

#[test]
fn a_vines_reach_is_what_its_acts_touch() {
    let world = World::new();
    let vine = registry(&world).build(job_launch_def()).unwrap();
    let reach: Vec<&str> = vine.reach().iter().map(String::as_str).collect();
    assert_eq!(reach, ["agent", "git"]);
}

#[tokio::test]
async fn an_act_result_that_breaks_its_declared_shape_is_a_run_error() {
    let mut reg = Registry::new();
    reg.register_act(
        OperatorSpec::new("git.head")
            .returns(Shape::new().required("sha", Ty::Text))
            .touches(["git"]),
        |_v, _cx| async { Ok(json!({ "sha": 7 })) },
    )
    .unwrap();
    let def = once("v")
        .input(Shape::new())
        .output("done", Shape::new().required("sha", Ty::Text))
        .steps([
            call("git.head", json!({})).bind("head"),
            out("done", json!({ "sha": "$head.sha" })),
        ]);
    let err = reg.build(def).unwrap().run(json!({})).await.unwrap_err();
    assert!(matches!(err, RunError::ResultMisfit { .. }), "{err}");
}

#[tokio::test]
async fn a_call_works_as_a_step_of_a_stream_pipe() {
    let world = World::new();
    let def = stream("commits")
        .input(Shape::new().required("dir", Ty::Text))
        .output("committed", Shape::new().required("sha", Ty::Text))
        .steps([call(
            "git.commit",
            json!({ "dir": "$item.dir", "message": "save" }),
        )]);
    let vine = registry(&world).build(def).unwrap();
    assert_eq!(vine.reach().iter().collect::<Vec<_>>(), ["git"]);
}

#[test]
fn each_holds_once_style_steps_at_the_end_of_a_pipe() {
    let def = stream("sort")
        .input(Shape::new().required("kind", Ty::Text))
        .output("kept", Shape::new())
        .output("dropped", Shape::new())
        .steps([each([switch(
            "$item.kind",
            [("keep", [out("kept", json!({}))])],
        )
        .otherwise([out("dropped", json!({}))])
        .end()])]);
    Registry::new().build(def).unwrap();
}

// -------------------------------------------------------------- on_error

fn failing(code: &'static str) -> Registry {
    let mut reg = Registry::new();
    reg.register_act(
        OperatorSpec::new("store.put").touches(["store"]),
        move |_v, _cx| async move { Err::<vine_lang::Value, _>(OpError::new(code, "no room")) },
    )
    .unwrap();
    reg
}

fn handled() -> Shape {
    Shape::new()
        .required("code", Ty::Text)
        .required("message", Ty::Text)
        .required("from", Ty::Text)
}

#[tokio::test]
async fn on_error_hands_the_vine_any_failure_as_data() {
    let def = once("v")
        .input(Shape::new())
        .output("stored", Shape::new())
        .output("handled", handled())
        .steps([attempt([call("store.put", json!({})), out("stored", json!({}))])
            .on_error([out(
                "handled",
                json!({ "code": "$error.code", "message": "$error.message", "from": "$error.from" }),
            )])
            .end()]);
    // The act names a code the vine has never heard of. It is still handled.
    let out = failing("quota_exceeded")
        .build(def)
        .unwrap()
        .run(json!({}))
        .await
        .unwrap();
    assert_eq!(out.name, "handled");
    assert_eq!(
        out.payload,
        json!({ "code": "quota_exceeded", "message": "no room", "from": "store.put" })
    );
}

#[tokio::test]
async fn a_vine_that_cares_about_one_code_switches_on_it_and_must_cover_the_rest() {
    let build = |code| {
        let def = once("v")
            .input(Shape::new())
            .output("stored", Shape::new())
            .output("full", Shape::new())
            .output("failed", Shape::new().required("code", Ty::Text))
            .steps([
                attempt([call("store.put", json!({})), out("stored", json!({}))])
                    .on_error([switch("$error.code", [("full", [out("full", json!({}))])])
                        .otherwise([out("failed", json!({ "code": "$error.code" }))])
                        .end()])
                    .end(),
            ]);
        failing(code).build(def).unwrap()
    };
    assert_eq!(build("full").run(json!({})).await.unwrap().name, "full");
    let other = build("timeout").run(json!({})).await.unwrap();
    assert_eq!(other.name, "failed");
    assert_eq!(other.payload, json!({ "code": "timeout" }));
}

#[test]
fn an_on_error_block_that_does_not_cover_every_failure_is_refused() {
    // No `otherwise`: a failure with any other code would fall through.
    let def = once("v")
        .input(Shape::new())
        .output("stored", Shape::new())
        .output("full", Shape::new())
        .steps([
            attempt([call("store.put", json!({})), out("stored", json!({}))])
                .on_error([switch("$error.code", [("full", [out("full", json!({}))])]).end()])
                .end(),
        ]);
    let err = failing("full").build(def).unwrap_err();
    assert_eq!(
        err,
        LoadError::MissingOutput {
            path: "steps".into()
        }
    );
}

#[test]
fn a_try_takes_catch_or_on_error_and_not_both() {
    let def = once("v")
        .input(Shape::new())
        .output("stored", Shape::new())
        .output("failed", Shape::new())
        .steps([
            attempt([call("store.put", json!({})), out("stored", json!({}))])
                .catch("full", [out("failed", json!({}))])
                .on_error([out("failed", json!({}))])
                .end(),
        ]);
    let err = failing("full").build(def).unwrap_err();
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "steps[0].try"),
        "{err}"
    );
}

#[tokio::test]
async fn on_error_does_not_swallow_cancellation() {
    let mut reg = Registry::new();
    reg.register_act(
        OperatorSpec::new("store.put").touches(["store"]),
        |_v, _cx| async { Err::<vine_lang::Value, _>(OpError::cancelled("stopped")) },
    )
    .unwrap();
    let def = once("v")
        .input(Shape::new())
        .output("stored", Shape::new())
        .output("failed", Shape::new())
        .steps([
            attempt([call("store.put", json!({})), out("stored", json!({}))])
                .on_error([out("failed", json!({}))])
                .end(),
        ]);
    let err = reg.build(def).unwrap().run(json!({})).await.unwrap_err();
    assert!(matches!(err, RunError::Cancelled { .. }), "{err}");
}

#[tokio::test]
async fn finally_still_runs_after_on_error() {
    let world = World::new();
    world.add_tree("/trees/job-1");
    world.fail("git.commit", "dirty", "nothing to commit");
    let def = once("v")
        .input(Shape::new())
        .output("done", Shape::new())
        .output("failed", Shape::new())
        .steps([attempt([
            call(
                "git.commit",
                json!({ "dir": "/trees/job-1", "message": "m" }),
            ),
            out("done", json!({})),
        ])
        .on_error([out("failed", json!({}))])
        .finally([call(
            "git.worktree_remove",
            json!({ "path": "/trees/job-1" }),
        )])
        .end()]);
    let out = registry(&world)
        .build(def)
        .unwrap()
        .run(json!({}))
        .await
        .unwrap();
    assert_eq!(out.name, "failed");
    assert!(world.trees().is_empty());
}

// -------------------------------------------- job_launch, built in Rust

#[tokio::test]
async fn job_launch_built_in_rust_starts_the_agent_and_ends() {
    let world = World::new();
    let vine = registry(&world).build(job_launch_def()).unwrap();
    let out = vine.run(ready_job()).await.unwrap();
    assert_eq!(out.name, "started");
    assert_eq!(
        out.payload,
        json!({ "job": "job-1", "session": "session-1", "tree": "/trees/job-1" })
    );
    assert_eq!(
        world.log(),
        [
            "git.refresh_branch plan/p-1",
            "git.worktree_add job-1",
            "agent.start /trees/job-1"
        ]
    );
}

#[tokio::test]
async fn job_launch_built_in_rust_refuses_before_it_touches_anything() {
    let world = World::new();
    let vine = registry(&world).build(job_launch_def()).unwrap();
    let out = vine
        .run(with(ready_job(), json!({ "waiting_on": 2 })))
        .await
        .unwrap();
    assert_eq!(out.payload, json!({ "gate": "dependencies" }));
    assert!(world.log().is_empty());
}
