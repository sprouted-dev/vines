//! The registry from the outside: what it refuses to register, what it
//! ships, and what it checks about a registered operator or act once a
//! pipe runs it. Nothing here uses YAML, and this file passes with the
//! feature off:
//!
//!     cargo test --no-default-features --test registry

use futures::future::{ready, Ready};
use vine_lang::pipe::{call, emit, emit_with, op, pipe};
use vine_lang::{
    json, LoadError, OpContext, OpError, OperatorSpec, Registry, RegistryError, RunError, Shape,
    Ty, Value,
};

fn noop(_v: Value, _cx: OpContext) -> Ready<Result<Value, OpError>> {
    ready(Ok(json!({})))
}

#[test]
fn an_operator_name_must_be_lower_snake_case() {
    let mut reg = Registry::new();
    assert_eq!(
        reg.register(OperatorSpec::new("Read"), noop),
        Err(RegistryError::BadName("Read".into()))
    );
}

#[test]
fn no_operator_can_take_call_or_a_word_that_would_pull_in_another_vine() {
    for word in ["call", "include", "import", "vine"] {
        let mut reg = Registry::new();
        assert_eq!(
            reg.register(OperatorSpec::new(word), noop),
            Err(RegistryError::Reserved(word.into())),
            "for {word}"
        );
    }
}

#[test]
fn an_operator_name_is_registered_once_and_the_core_names_are_taken() {
    let mut reg = Registry::new();
    assert_eq!(
        reg.register(OperatorSpec::new("set"), noop),
        Err(RegistryError::Duplicate("set".into()))
    );
    reg.register(OperatorSpec::new("ping"), noop).unwrap();
    assert_eq!(
        reg.register(OperatorSpec::new("ping"), noop),
        Err(RegistryError::Duplicate("ping".into()))
    );
}

#[test]
fn an_operator_that_says_what_it_touches_must_name_it_well() {
    let mut reg = Registry::new();
    assert_eq!(
        reg.register(OperatorSpec::new("pong").touches(["Facts.Read"]), noop),
        Err(RegistryError::BadTouch {
            operator: "pong".into(),
            touch: "Facts.Read".into()
        })
    );
}

#[test]
fn the_core_operators_are_set_and_select_and_touch_nothing() {
    let specs = Registry::new().specs();
    let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["select", "set"]);
    assert!(specs.iter().all(|s| s.touches.is_empty()));
}

#[test]
fn the_library_ships_no_acts() {
    let reg = Registry::new();
    assert!(reg.act_specs().is_empty());
    assert!(reg.stream_act_specs().is_empty());
}

#[test]
fn an_act_must_say_what_it_touches_and_have_a_well_formed_unique_name() {
    let mut reg = Registry::new();
    assert_eq!(
        reg.register_act(OperatorSpec::new("git.push"), noop),
        Err(RegistryError::ActTouchesNothing("git.push".into()))
    );
    assert_eq!(
        reg.register_act(OperatorSpec::new("Git Push").touches(["git"]), noop),
        Err(RegistryError::BadActName("Git Push".into()))
    );
    assert_eq!(
        reg.register_act(OperatorSpec::new("git.push").touches(["Not Good"]), noop),
        Err(RegistryError::BadTouch {
            operator: "git.push".into(),
            touch: "Not Good".into()
        })
    );
    reg.register_act(OperatorSpec::new("git.push").touches(["git"]), noop)
        .unwrap();
    assert_eq!(
        reg.register_act(OperatorSpec::new("git.push").touches(["git"]), noop),
        Err(RegistryError::Duplicate("git.push".into()))
    );
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
    let def = pipe("v")
        .input(Shape::new())
        .output("done", Shape::new().required("sha", Ty::Text))
        .stages([
            call("git.head", json!({})).into("head"),
            emit_with("done", json!({ "sha": "$item.head.sha" })),
        ]);
    let err = reg
        .build_pipe(def)
        .unwrap()
        .run_one(json!({}))
        .await
        .unwrap_err();
    assert!(
        matches!(err, RunError::ResultMisfit { ref operator, .. } if operator == "git.head"),
        "{err}"
    );
}

#[tokio::test]
async fn a_vine_of_only_core_operators_reaches_nothing() {
    let def = pipe("echo")
        .input(Shape::new().required("x", Ty::Int))
        .output("done", Shape::open())
        .stages([op("set", json!({ "x": "$item.x" })), emit("done")]);
    let vine = Registry::new().build_pipe(def).unwrap();
    assert!(vine.reach().is_empty());
    assert!(vine.acts().is_empty());
    let out = vine.run_one(json!({ "x": 4 })).await.unwrap();
    assert_eq!(out[0].payload, json!({ "x": 4 }));
}

#[test]
fn settings_that_do_not_fit_a_core_operator_are_refused_before_it_runs() {
    let def = pipe("v")
        .input(Shape::new().required("jobs", Ty::List))
        .output("done", Shape::open())
        .stages([op("select", json!({ "from": "$item.jobs" })), emit("done")]);
    assert_eq!(
        Registry::new().build_pipe(def).unwrap_err(),
        LoadError::BadSettings {
            path: "pipe[0].select".into(),
            operator: "select".into(),
            problem: "missing required field `where` (object)".into()
        }
    );
}

#[tokio::test]
async fn settings_that_only_misfit_once_resolved_are_a_run_error() {
    let mut reg = Registry::new();
    reg.register(
        OperatorSpec::new("count").settings(Shape::new().required("n", Ty::Int)),
        |s, _cx| async move { Ok(s) },
    )
    .unwrap();
    let def = pipe("v")
        .input(Shape::new().required("n", Ty::Any))
        .output("done", Shape::open())
        .stages([op("count", json!({ "n": "$item.n" })), emit("done")]);
    let err = reg
        .build_pipe(def)
        .unwrap()
        .run_one(json!({ "n": "an-1" }))
        .await
        .unwrap_err();
    assert_eq!(
        err,
        RunError::SettingsMisfit {
            path: "pipe[0]".into(),
            operator: "count".into(),
            problem: "field `n` should be int, found text \"an-1\"".into()
        }
    );
}
