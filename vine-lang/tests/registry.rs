//! The registry from the outside: what it refuses to register, what it
//! ships, and what it checks about a registered operator once a pipe runs
//! it. Nothing here uses YAML, and this file passes with the
//! feature off:
//!
//!     cargo test --no-default-features --test registry
#![cfg(test)]

use futures::future::{ready, Ready};
use futures::{stream, StreamExt};
use vine_lang::pipe::{call, emit, emit_with, op, pipe, tap};
use vine_lang::{
    json, LoadError, OpContext, OpError, OperatorSpec, Part, Registry, RegistryError, RunError,
    Shape, Ty, Value,
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
fn the_library_ships_no_operator_that_reaches_outside() {
    let reg = Registry::new();
    assert!(reg.specs().iter().all(|s| s.touches.is_empty()));
    assert!(reg.stream_specs().is_empty());
}

#[test]
fn an_operator_with_a_dotted_name_must_say_what_it_touches_and_be_named_once() {
    let mut reg = Registry::new();
    assert_eq!(
        reg.register(OperatorSpec::new("git.push"), noop),
        Err(RegistryError::TouchesNothing("git.push".into()))
    );
    assert_eq!(
        reg.register(OperatorSpec::new("Git Push").touches(["git"]), noop),
        Err(RegistryError::BadName("Git Push".into()))
    );
    assert_eq!(
        reg.register(OperatorSpec::new("git.push").touches(["Not Good"]), noop),
        Err(RegistryError::BadTouch {
            operator: "git.push".into(),
            touch: "Not Good".into()
        })
    );
    reg.register(OperatorSpec::new("git.push").touches(["git"]), noop)
        .unwrap();
    assert_eq!(
        reg.register(OperatorSpec::new("git.push").touches(["git"]), noop),
        Err(RegistryError::Duplicate("git.push".into()))
    );
}

#[tokio::test]
async fn a_result_that_breaks_its_declared_shape_is_a_run_error() {
    let mut reg = Registry::new();
    reg.register(
        OperatorSpec::new("git.head")
            .returns(Shape::new().required("sha", Ty::Text))
            .touches(["git"]),
        |_v, _cx| async { Ok(json!({ "sha": 7 })) },
    )
    .unwrap();
    let def = pipe("v")
        .input(Shape::new())
        .output("done", Shape::new().required("sha", Ty::Text))
        .operators([
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
        .operators([op("set", json!({ "x": "$item.x" })), emit("done")]);
    let vine = Registry::new().build_pipe(def).unwrap();
    assert!(vine.reach().is_empty());
    assert!(vine.calls().is_empty());
    let out = vine.run_one(json!({ "x": 4 })).await.unwrap();
    assert_eq!(out[0].payload, json!({ "x": 4 }));
}

#[test]
fn settings_that_do_not_fit_a_core_operator_are_refused_before_it_runs() {
    let def = pipe("v")
        .input(Shape::new().required("jobs", Ty::List))
        .output("done", Shape::open())
        .operators([op("select", json!({ "from": "$item.jobs" })), emit("done")]);
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
        .operators([op("count", json!({ "n": "$item.n" })), emit("done")]);
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

fn empty(_v: Value, _cx: OpContext) -> stream::Empty<Result<Part, OpError>> {
    stream::empty()
}

#[test]
fn a_stream_operator_must_say_what_it_touches_even_with_a_plain_name() {
    let mut reg = Registry::new();
    assert_eq!(
        reg.register_stream(OperatorSpec::new("count"), empty),
        Err(RegistryError::TouchesNothing("count".into()))
    );
    assert_eq!(
        reg.register_replayable_stream(OperatorSpec::new("count"), empty),
        Err(RegistryError::TouchesNothing("count".into()))
    );
}

#[test]
fn operators_that_give_one_result_and_stream_operators_share_one_namespace() {
    let mut reg = Registry::new();
    reg.register(OperatorSpec::new("ping"), noop).unwrap();
    assert_eq!(
        reg.register_stream(OperatorSpec::new("ping").touches(["net"]), empty),
        Err(RegistryError::Duplicate("ping".into()))
    );
    reg.register_stream(OperatorSpec::new("feed.pages").touches(["feed"]), empty)
        .unwrap();
    assert_eq!(
        reg.register(OperatorSpec::new("feed.pages").touches(["feed"]), noop),
        Err(RegistryError::Duplicate("feed.pages".into()))
    );
    let names: Vec<String> = reg.specs().into_iter().map(|s| s.name).collect();
    assert_eq!(names, ["feed.pages", "ping", "select", "set"]);
    let streams: Vec<String> = reg.stream_specs().into_iter().map(|s| s.name).collect();
    assert_eq!(streams, ["feed.pages"]);
}

#[test]
fn a_word_of_the_pipe_is_refused_only_for_a_pure_operator() {
    // An operator that reaches outside is written after `call`, never as a
    // key, so it cannot be mistaken for one.
    let mut reg = Registry::new();
    assert_eq!(
        reg.register(OperatorSpec::new("emit"), noop),
        Err(RegistryError::Reserved("emit".into()))
    );
    reg.register(OperatorSpec::new("emit").touches(["mail"]), noop)
        .unwrap();
}

fn with_git_push() -> Registry {
    let mut reg = Registry::new();
    reg.register(OperatorSpec::new("git.push").touches(["git"]), noop)
        .unwrap();
    reg
}

fn empty_in() -> vine_lang::pipe::PipeDef {
    pipe("v").input(Shape::new()).output("done", Shape::open())
}

#[test]
fn call_and_tap_refuse_a_pure_operator() {
    for operator in [call("set", json!({})), tap("set", json!({}))] {
        let err = with_git_push()
            .build_pipe(empty_in().operators([operator, emit("done")]))
            .unwrap_err();
        assert!(
            matches!(err, LoadError::Malformed { ref path, ref problem }
                if path == "pipe[0]" && problem.contains("`set` touches nothing")),
            "{err:?}"
        );
    }
}

#[test]
fn an_operator_that_reaches_outside_is_refused_by_its_name_alone() {
    let err = with_git_push()
        .build_pipe(empty_in().operators([op("git.push", json!({})), emit("done")]))
        .unwrap_err();
    assert!(
        matches!(err, LoadError::Malformed { ref path, ref problem }
            if path == "pipe[0]" && problem.contains("runs it with `call`")),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_pure_operator_runs_again_on_replay_and_one_that_reaches_outside_does_not() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use vine_lang::{Event, Journal};

    let (pure, outside) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let mut reg = Registry::new();
    let p = pure.clone();
    reg.register(OperatorSpec::new("count"), move |_v, _cx| {
        p.fetch_add(1, Ordering::SeqCst);
        ready(Ok(json!({})))
    })
    .unwrap();
    let o = outside.clone();
    reg.register(
        OperatorSpec::new("git.push").touches(["git"]),
        move |_v, _cx| {
            o.fetch_add(1, Ordering::SeqCst);
            ready(Ok(json!({})))
        },
    )
    .unwrap();
    let vine = reg
        .build_pipe(empty_in().operators([
            op("count", json!({})),
            tap("git.push", json!({})),
            emit("done"),
        ]))
        .unwrap();
    let mut journal = Journal::new();
    let events: Vec<_> = vine
        .run_saved(stream::iter([json!({})]), Journal::new())
        .collect()
        .await;
    for e in events {
        journal.record(&e.unwrap());
    }
    let again: Vec<_> = vine
        .run_saved(stream::iter([json!({})]), journal)
        .collect()
        .await;
    assert!(again.iter().all(Result::is_ok));
    assert!(!again.iter().any(|e| matches!(e, Ok(Event::Output(_)))));
    assert_eq!(pure.load(Ordering::SeqCst), 2);
    assert_eq!(outside.load(Ordering::SeqCst), 1);
}
