//! Stream operators, `fork`, `last`, `bufferCount`, and saved runs, in Rust alone.
//! This file passes with the YAML feature off:
//!
//!     cargo test --no-default-features --test pipe_streams
#![cfg(test)]

mod fake;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use fake::{registry, World};
use futures::{stream, StreamExt};
use tokio::time::{sleep, Instant};
use vine_lang::pipe::{
    branch, buffer_count, call, catch_error, concat_map, emit, emit_with, exhaust_map, finalize,
    fork, fork_join, last, merge_map, op, pipe, split, switch_map, take, tap, timeout, use_vine,
    Operator, Pipe, PipeDef,
};
use vine_lang::{
    json, Entry, Event, Journal, LoadError, OpError, OperatorSpec, Part, Registry, RegistryError,
    RunError, Shape, Ty, Value,
};

type Outs = Vec<(String, Value)>;

async fn all(vine: &Pipe, items: Vec<Value>) -> (Outs, Option<RunError>) {
    let mut outs = Vec::new();
    let mut run = vine.run(stream::iter(items));
    while let Some(r) = run.next().await {
        match r {
            Ok(o) => outs.push((o.name, o.payload)),
            Err(e) => return (outs, Some(e)),
        }
    }
    (outs, None)
}

fn nums(ns: &[i64]) -> Vec<Value> {
    ns.iter().map(|n| json!({ "n": n })).collect()
}

fn values(outs: &Outs) -> Vec<Value> {
    outs.iter().map(|(_, v)| v.clone()).collect()
}

/// The calls `count.up` got: its input and where it was told to resume.
type Calls = Arc<Mutex<Vec<(Value, u64)>>>;

/// A registry with two stream operators.
///
/// `count.up { to, fail_at?, bad_at?, bad_end?, no_end? }` gives `{ n }`
/// for n from 1 to `to`, starting after `resume_from`, then ends with
/// `{ total }`. It fails before item `fail_at`, gives a misfit item at
/// `bad_at`, a misfit end with `bad_end`, and stops without its end with
/// `no_end`.
///
/// `count.bare { to }` gives `{ n }` and declares no end.
fn counting() -> (Calls, Registry) {
    let calls: Calls = Arc::default();
    let mut reg = Registry::new();
    let c = calls.clone();
    reg.register_stream(
        OperatorSpec::new("count.up")
            .settings(
                Shape::new()
                    .required("to", Ty::Int)
                    .optional("fail_at", Ty::Int)
                    .optional("bad_at", Ty::Int)
                    .optional("bad_end", Ty::Bool)
                    .optional("no_end", Ty::Bool),
            )
            .returns(Shape::new().required("n", Ty::Int))
            .ends(Shape::new().required("total", Ty::Int))
            .touches(["count"]),
        move |v, cx| {
            c.lock().unwrap().push((v.clone(), cx.resume_from()));
            let to = v["to"].as_u64().unwrap();
            let mut parts: Vec<Result<Part, OpError>> = Vec::new();
            for n in cx.resume_from() + 1..=to {
                if v["fail_at"] == n {
                    parts.push(Err(OpError::new("broken", format!("broke at {n}"))));
                    break;
                }
                if v["bad_at"] == n {
                    parts.push(Ok(Part::Item(json!({ "n": "bad" }))));
                } else {
                    parts.push(Ok(Part::Item(json!({ "n": n }))));
                }
            }
            if v["no_end"] != true {
                let total = if v["bad_end"] == true {
                    json!("many")
                } else {
                    json!(to)
                };
                parts.push(Ok(Part::End(json!({ "total": total }))));
            }
            stream::iter(parts)
        },
    )
    .unwrap();
    reg.register_stream(
        OperatorSpec::new("count.bare")
            .settings(Shape::new().required("to", Ty::Int))
            .returns(Shape::new().required("n", Ty::Int))
            .touches(["count"]),
        |v, _cx| {
            let to = v["to"].as_u64().unwrap();
            stream::iter((1..=to).map(|n| Ok(Part::Item(json!({ "n": n })))))
        },
    )
    .unwrap();
    // `store.put` fails for the item whose `n` is 2.
    reg.register(
        OperatorSpec::new("store.put")
            .settings(Shape::new().required("n", Ty::Int))
            .touches(["store"]),
        |v, _cx| async move {
            if v["n"] == 2 {
                Err(OpError::new("full", "no room"))
            } else {
                Ok(v)
            }
        },
    )
    .unwrap();
    (calls, reg)
}

/// A vine over `{ n: int }` with open outputs `out`, `a`, `b` and `failed`.
fn numbers(operators: impl IntoIterator<Item = Operator>) -> PipeDef {
    pipe("numbers")
        .input(Shape::new().required("n", Ty::Int))
        .output("out", Shape::open())
        .output("a", Shape::open())
        .output("b", Shape::open())
        .output("failed", Shape::open())
        .operators(operators)
}

fn built(operators: impl IntoIterator<Item = Operator>) -> Pipe {
    counting().1.build_pipe(numbers(operators)).unwrap()
}

fn refused(operators: impl IntoIterator<Item = Operator>) -> LoadError {
    counting().1.build_pipe(numbers(operators)).unwrap_err()
}

fn count_to(to: &str) -> Operator {
    call("count.up", json!({ "to": to }))
}

// ------------------------------------------------------------ stream operators

#[tokio::test]
async fn a_stream_operator_turns_one_item_into_one_item_per_value_it_gives() {
    let vine = built([count_to("$item.n"), emit("out")]);
    let (outs, err) = all(&vine, nums(&[2, 3])).await;
    assert!(err.is_none());
    assert_eq!(values(&outs), nums(&[1, 2, 1, 2, 3]));
}

#[tokio::test]
async fn a_stream_operator_with_into_keeps_the_item_and_adds_each_value() {
    let vine = built([count_to("$item.n").into("count"), emit("out")]);
    let (outs, _) = all(&vine, nums(&[2])).await;
    assert_eq!(
        values(&outs),
        [
            json!({ "n": 2, "count": { "n": 1 } }),
            json!({ "n": 2, "count": { "n": 2 } })
        ]
    );
}

#[tokio::test]
async fn a_stream_operator_that_gives_nothing_lets_no_item_through_unless_its_end_is_named() {
    let vine = built([count_to("$item.n"), emit("out")]);
    assert_eq!(all(&vine, nums(&[0])).await, (vec![], None));

    let vine = built([count_to("$item.n").into("count").end("done"), emit("out")]);
    let (outs, _) = all(&vine, nums(&[0])).await;
    assert_eq!(values(&outs), [json!({ "n": 0, "done": { "total": 0 } })]);
}

#[tokio::test]
async fn the_end_comes_last_with_the_end_value_and_without_the_into_field() {
    let vine = built([count_to("$item.n").into("count").end("done"), emit("out")]);
    let (outs, _) = all(&vine, nums(&[2])).await;
    assert_eq!(
        values(&outs),
        [
            json!({ "n": 2, "count": { "n": 1 } }),
            json!({ "n": 2, "count": { "n": 2 } }),
            json!({ "n": 2, "done": { "total": 2 } }),
        ]
    );
    // With no `into`, each value is the item, and the end item is the item
    // as it reached the operator with the end added.
    let vine = built([count_to("$item.n").end("done"), emit("out")]);
    let (outs, _) = all(&vine, nums(&[1])).await;
    assert_eq!(
        values(&outs),
        [json!({ "n": 1 }), json!({ "n": 1, "done": { "total": 1 } })]
    );
}

#[tokio::test]
async fn a_stream_operator_that_declares_no_end_may_just_stop_and_its_end_is_null() {
    let vine = built([
        call("count.bare", json!({ "to": "$item.n" }))
            .into("count")
            .end("done"),
        emit("out"),
    ]);
    let (outs, err) = all(&vine, nums(&[1])).await;
    assert!(err.is_none());
    assert_eq!(
        values(&outs),
        [
            json!({ "n": 1, "count": { "n": 1 } }),
            json!({ "n": 1, "done": null })
        ]
    );
}

#[tokio::test]
async fn a_stream_operator_that_fails_partway_has_given_the_items_before_and_catch_error_handles_it(
) {
    let vine = built([
        call("count.up", json!({ "to": 5, "fail_at": 3 })).into("count"),
        emit_with("out", json!({ "n": "$item.count.n" })),
        catch_error([emit_with(
            "failed",
            json!({ "n": "$item.n", "error": "$item.error" }),
        )]),
    ]);
    let (outs, err) = all(&vine, nums(&[7, 8])).await;
    assert!(err.is_none());
    let expect = |n: i64| {
        vec![
            ("out".to_owned(), json!({ "n": 1 })),
            ("out".to_owned(), json!({ "n": 2 })),
            (
                "failed".to_owned(),
                json!({ "n": n, "error": { "code": "broken", "message": "broke at 3", "from": "count.up" } }),
            ),
        ]
    };
    // The failure gets the item as it reached the stream operator. The next
    // item runs as usual.
    assert_eq!(outs, [expect(7), expect(8)].concat());
}

#[tokio::test]
async fn a_stream_operator_failure_nothing_catches_ends_the_stream_after_the_items_before_it() {
    let vine = built([
        call("count.up", json!({ "to": 5, "fail_at": 3 })),
        emit("out"),
    ]);
    let (outs, err) = all(&vine, nums(&[1, 1])).await;
    assert_eq!(values(&outs), nums(&[1, 2]));
    assert!(
        matches!(err, Some(RunError::Unhandled { ref operator, ref code, .. })
            if operator == "count.up" && code == "broken"),
        "{err:?}"
    );
}

#[test]
fn input_that_does_not_fit_a_stream_operator_is_refused_before_it_runs() {
    let err = refused([call("count.up", json!({ "to": "many" })), emit("out")]);
    assert!(
        matches!(err, LoadError::BadSettings { ref path, ref operator, .. }
            if path == "pipe[0].with" && operator == "count.up"),
        "{err}"
    );
    let err = refused([call("count.up", json!({ "upto": 3 })), emit("out")]);
    assert!(matches!(err, LoadError::BadSettings { .. }), "{err}");
}

#[tokio::test]
async fn input_that_does_not_fit_a_stream_operator_at_run_time_is_a_run_error() {
    let vine = counting()
        .1
        .build_pipe(
            pipe("v")
                .input(Shape::new().required("to", Ty::Any))
                .output("out", Shape::open())
                .operators([call("count.up", json!({ "to": "$item.to" })), emit("out")]),
        )
        .unwrap();
    let err = vine.run_one(json!({ "to": "x" })).await.unwrap_err();
    assert!(matches!(err, RunError::SettingsMisfit { .. }), "{err}");
}

#[tokio::test]
async fn an_item_that_breaks_the_declared_shape_is_a_run_error_after_the_items_before_it() {
    let vine = built([
        call("count.up", json!({ "to": 3, "bad_at": 2 })),
        emit("out"),
        catch_error([emit("failed")]),
    ]);
    let (outs, err) = all(&vine, nums(&[1])).await;
    assert_eq!(values(&outs), nums(&[1]));
    assert!(
        matches!(err, Some(RunError::ResultMisfit { ref operator, .. }) if operator == "count.up"),
        "{err:?}"
    );
}

#[tokio::test]
async fn an_end_that_breaks_its_declared_shape_or_never_comes_is_a_run_error() {
    for with in [
        json!({ "to": 1, "bad_end": true }),
        json!({ "to": 1, "no_end": true }),
    ] {
        let vine = built([call("count.up", with.clone()).end("done"), emit("out")]);
        let (outs, err) = all(&vine, nums(&[1])).await;
        assert_eq!(values(&outs), nums(&[1]), "{with}");
        assert!(
            matches!(err, Some(RunError::ResultMisfit { .. })),
            "{with}: {err:?}"
        );
    }
}

#[test]
fn a_stream_operator_cannot_be_tapped_or_run_in_fork_join_and_end_needs_a_stream_operator() {
    let err = refused([tap("count.up", json!({ "to": 1 })), emit("out")]);
    assert!(
        matches!(err, LoadError::Malformed { ref path, ref problem }
            if path == "pipe[0]" && problem.contains("use `call`")),
        "{err}"
    );
    let err = refused([
        fork_join([
            count_to("$item.n").into("a"),
            call("store.put", json!({ "n": 1 })).into("b"),
        ]),
        emit("out"),
    ]);
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].forkJoin[0]"),
        "{err}"
    );
    let err = refused([
        call("store.put", json!({ "n": 1 })).end("done"),
        emit("out"),
    ]);
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].end"),
        "{err}"
    );
    let err = refused([count_to("$item.n").into("x").end("x"), emit("out")]);
    assert_eq!(
        err,
        LoadError::DuplicateBinding {
            path: "pipe[0].end".into(),
            name: "x".into()
        }
    );
    let err = refused([count_to("$item.n").end("Done"), emit("out")]);
    assert!(matches!(err, LoadError::Malformed { .. }), "{err}");
}

#[test]
fn the_checker_knows_the_fields_a_stream_operator_adds() {
    let err = refused([
        count_to("$item.n").into("count").end("done"),
        emit_with("out", json!({ "x": "$item.nope" })),
    ]);
    assert!(
        matches!(err, LoadError::UnknownField { ref field, .. } if field == "nope"),
        "{err}"
    );
    // Without an end, every item has the `into` field and the checker can
    // match the output's shape.
    let reg = counting().1;
    let def = pipe("v")
        .input(Shape::new().required("n", Ty::Int))
        .output(
            "out",
            Shape::new()
                .required("n", Ty::Int)
                .required("count", Ty::Any),
        )
        .operators([count_to("$item.n").into("count"), emit("out")]);
    reg.build_pipe(def).unwrap();
}

#[test]
fn a_stream_operator_is_registered_under_the_rules_for_operators_that_reach_outside() {
    let mut reg = counting().1;
    let spec = |name: &str| OperatorSpec::new(name).touches(["count"]);
    let empty = |_v: Value, _cx| stream::empty::<Result<Part, OpError>>();
    assert_eq!(
        reg.register_stream(spec("Count"), empty),
        Err(RegistryError::BadName("Count".into()))
    );
    assert_eq!(
        reg.register_stream(OperatorSpec::new("count.down"), empty),
        Err(RegistryError::TouchesNothing("count.down".into()))
    );
    // Operators and stream operators share one namespace, both ways.
    assert_eq!(
        reg.register_stream(spec("store.put"), empty),
        Err(RegistryError::Duplicate("store.put".into()))
    );
    assert_eq!(
        reg.register(spec("count.up"), |_v, _cx| async { Ok(json!({})) }),
        Err(RegistryError::Duplicate("count.up".into()))
    );
    let names: Vec<String> = reg.stream_specs().into_iter().map(|s| s.name).collect();
    assert_eq!(names, ["count.bare", "count.up"]);
}

// ------------------------------------------------------------------- fork

#[tokio::test]
async fn fork_sends_every_item_down_every_side_pipe_and_on_down_the_main_pipe() {
    let vine = built([
        fork([
            vec![emit_with("a", json!({ "n": "$item.n" }))],
            vec![emit_with("b", json!({ "n": "$item.n" }))],
        ]),
        emit("out"),
    ]);
    let (outs, err) = all(&vine, nums(&[1, 2])).await;
    assert!(err.is_none());
    let names: Vec<(&str, &Value)> = outs.iter().map(|(n, v)| (n.as_str(), &v["n"])).collect();
    assert_eq!(
        names,
        [
            ("a", &json!(1)),
            ("b", &json!(1)),
            ("out", &json!(1)),
            ("a", &json!(2)),
            ("b", &json!(2)),
            ("out", &json!(2)),
        ]
    );
}

/// `work.slow { secs }` waits that long on tokio's clock.
fn slow() -> Registry {
    let mut reg = counting().1;
    reg.register(
        OperatorSpec::new("work.slow")
            .settings(Shape::new().required("secs", Ty::Int))
            .touches(["work"]),
        |v, _cx| async move {
            sleep(Duration::from_secs(v["secs"].as_u64().unwrap())).await;
            Ok(json!({}))
        },
    )
    .unwrap();
    reg
}

#[tokio::test(start_paused = true)]
async fn fork_keeps_the_main_pipe_in_order_when_a_side_pipe_takes_time() {
    let start = Instant::now();
    let vine = slow()
        .build_pipe(numbers([
            fork([vec![
                tap("work.slow", json!({ "secs": "$item.n" })),
                emit("a"),
            ]]),
            emit("out"),
        ]))
        .unwrap();
    let (outs, _) = all(&vine, nums(&[3, 1, 2])).await;
    let got: Vec<(&str, &Value)> = outs.iter().map(|(n, v)| (n.as_str(), &v["n"])).collect();
    assert_eq!(
        got,
        [
            ("a", &json!(3)),
            ("out", &json!(3)),
            ("a", &json!(1)),
            ("out", &json!(1)),
            ("a", &json!(2)),
            ("out", &json!(2)),
        ]
    );
    // Each item waits for its side pipe before it goes on.
    assert_eq!(start.elapsed().as_secs(), 6);
}

#[tokio::test]
async fn a_side_pipe_sees_the_whole_stream_so_buffer_count_and_last_work_in_it() {
    let vine = built([
        fork([
            vec![buffer_count(2), emit("a")],
            vec![last(), emit_with("b", json!({ "last": "$item.n" }))],
        ]),
        emit_with("out", json!({ "n": "$item.n" })),
    ]);
    let (outs, err) = all(&vine, nums(&[1, 2, 3, 4, 5])).await;
    assert!(err.is_none());
    let batch = |ns: &[i64]| json!({ "items": nums(ns), "count": ns.len() });
    assert_eq!(
        outs,
        [
            ("out".to_owned(), json!({ "n": 1 })),
            ("a".to_owned(), batch(&[1, 2])),
            ("out".to_owned(), json!({ "n": 2 })),
            ("out".to_owned(), json!({ "n": 3 })),
            ("a".to_owned(), batch(&[3, 4])),
            ("out".to_owned(), json!({ "n": 4 })),
            ("out".to_owned(), json!({ "n": 5 })),
            // When the stream ends, the side pipes finish, in order.
            ("a".to_owned(), batch(&[5])),
            ("b".to_owned(), json!({ "last": 5 })),
        ]
    );
}

#[tokio::test]
async fn a_value_that_reaches_the_end_of_a_side_pipe_without_being_emitted_is_dropped() {
    let vine = built([fork([vec![op("set", json!({ "x": 1 }))]]), emit("out")]);
    let (outs, err) = all(&vine, nums(&[1, 2])).await;
    assert!(err.is_none());
    assert_eq!(values(&outs), nums(&[1, 2]));
}

#[tokio::test]
async fn a_side_pipe_that_ends_early_does_not_stop_the_main_pipe() {
    let vine = built([fork([vec![take(1), emit("a")]]), emit("out")]);
    let (outs, _) = all(&vine, nums(&[1, 2, 3])).await;
    let names: Vec<&str> = outs.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["a", "out", "out", "out"]);
}

#[tokio::test]
async fn a_failure_in_a_side_pipe_fails_its_item_and_a_catch_error_after_the_fork_handles_it() {
    let vine = built([
        fork([vec![tap("store.put", json!({ "n": "$item.n" }))]]),
        emit("out"),
        catch_error([emit_with(
            "failed",
            json!({ "n": "$item.n", "from": "$item.error.from" }),
        )]),
    ]);
    let (outs, err) = all(&vine, nums(&[1, 2, 3])).await;
    assert!(err.is_none());
    // Item 2 failed in the side pipe, so it does not go on down the main
    // pipe. Items 1 and 3 are not affected.
    assert_eq!(
        outs,
        [
            ("out".to_owned(), json!({ "n": 1 })),
            ("failed".to_owned(), json!({ "n": 2, "from": "store.put" })),
            ("out".to_owned(), json!({ "n": 3 })),
        ]
    );
}

#[tokio::test]
async fn a_failure_in_a_side_pipe_that_nothing_catches_ends_the_stream() {
    let vine = built([
        fork([vec![tap("store.put", json!({ "n": "$item.n" }))]]),
        emit("out"),
    ]);
    let (outs, err) = all(&vine, nums(&[1, 2, 3])).await;
    assert_eq!(values(&outs), nums(&[1]));
    assert!(
        matches!(err, Some(RunError::Unhandled { ref path, .. }) if path == "pipe[0].fork[0][0]"),
        "{err:?}"
    );
}

#[test]
fn fork_needs_at_least_one_side_pipe_and_each_side_pipe_a_operator() {
    let err = refused([fork(Vec::<Vec<Operator>>::new()), emit("out")]);
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].fork"),
        "{err}"
    );
    let err = refused([fork([Vec::new()]), emit("out")]);
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].fork[0]"),
        "{err}"
    );
}

#[test]
fn an_emit_inside_a_side_pipe_is_checked_like_any_other() {
    let err = refused([fork([vec![emit("nowhere")]]), emit("out")]);
    assert_eq!(
        err,
        LoadError::UndeclaredOutput {
            path: "pipe[0].fork[0][0].emit".into(),
            name: "nowhere".into()
        }
    );
    let def = pipe("v")
        .input(Shape::new().required("n", Ty::Int))
        .output("out", Shape::open())
        .output("job", Shape::new().required("job", Ty::Id))
        .operators([
            fork([vec![emit_with("job", json!({ "job": 7 }))]]),
            emit("out"),
        ]);
    let err = counting().1.build_pipe(def).unwrap_err();
    assert!(
        matches!(err, LoadError::OutputMisfit { ref path, .. } if path == "pipe[0].fork[0][0].emit.with"),
        "{err}"
    );
    // An operator after an emit in a side pipe can never run.
    let err = refused([fork([vec![emit("a"), op("set", json!({}))]]), emit("out")]);
    assert_eq!(
        err,
        LoadError::Unreachable {
            path: "pipe[0].fork[0][1]".into()
        }
    );
}

#[test]
fn an_emit_in_a_side_pipe_does_not_count_as_the_main_pipe_emitting() {
    assert_eq!(
        refused([fork([vec![emit("a")]])]),
        LoadError::MissingOutput {
            path: "pipe".into()
        }
    );
    // A fork after the main pipe emits can never get an item.
    assert_eq!(
        refused([emit("out"), fork([vec![emit("a")]])]),
        LoadError::Unreachable {
            path: "pipe[1]".into()
        }
    );
}

#[test]
fn finalize_may_not_hold_a_fork_that_emits() {
    let err = refused([
        emit("out"),
        finalize([fork([vec![emit("a")]]), op("set", json!({}))]),
    ]);
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[1].finalize"),
        "{err}"
    );
}

// --------------------------------------------------------- last, collect

#[tokio::test]
async fn last_lets_only_the_final_item_through_when_the_stream_ends() {
    let vine = built([last(), emit("out")]);
    assert_eq!(values(&all(&vine, nums(&[1, 2, 3])).await.0), nums(&[3]));
}

#[tokio::test]
async fn last_of_an_empty_stream_gives_nothing() {
    let vine = built([last(), emit("out")]);
    assert_eq!(all(&vine, vec![]).await, (vec![], None));
}

#[tokio::test]
async fn buffer_count_gives_batches_and_what_is_left_when_the_stream_ends() {
    let vine = built([buffer_count(2), emit("out")]);
    let (outs, _) = all(&vine, nums(&[1, 2, 3, 4, 5])).await;
    assert_eq!(
        values(&outs),
        [
            json!({ "items": nums(&[1, 2]), "count": 2 }),
            json!({ "items": nums(&[3, 4]), "count": 2 }),
            json!({ "items": nums(&[5]), "count": 1 }),
        ]
    );
}

#[tokio::test]
async fn buffer_count_never_gives_an_empty_batch() {
    let vine = built([buffer_count(2), emit("out")]);
    assert_eq!(all(&vine, nums(&[1, 2, 3, 4])).await.0.len(), 2);
    assert_eq!(all(&vine, vec![]).await, (vec![], None));
}

#[tokio::test]
async fn buffer_count_with_into_keeps_the_last_item_of_the_batch() {
    let vine = built([buffer_count(2).into("batch"), emit("out")]);
    let (outs, _) = all(&vine, nums(&[1, 2, 3])).await;
    assert_eq!(
        values(&outs),
        [
            json!({ "n": 2, "batch": { "items": nums(&[1, 2]), "count": 2 } }),
            json!({ "n": 3, "batch": { "items": nums(&[3]), "count": 1 } }),
        ]
    );
}

#[test]
fn buffer_count_of_size_zero_and_a_bad_into_are_refused() {
    let err = refused([buffer_count(0), emit("out")]);
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].bufferCount"),
        "{err}"
    );
    let err = refused([buffer_count(2).into("Batch"), emit("out")]);
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].into"),
        "{err}"
    );
    // The checker knows a batch's fields.
    let err = refused([buffer_count(2), emit_with("out", json!({ "x": "$item.n" }))]);
    assert!(
        matches!(err, LoadError::UnknownField { ref field, .. } if field == "n"),
        "{err}"
    );
}

#[tokio::test]
async fn before_catch_error_or_retry_last_and_buffer_count_see_the_end_of_each_items_own_stream() {
    let vine = built([
        count_to("$item.n").into("count"),
        last(),
        emit_with("out", json!({ "n": "$item.n", "last": "$item.count.n" })),
        catch_error([emit("failed")]),
    ]);
    let (outs, _) = all(&vine, nums(&[2, 3])).await;
    assert_eq!(
        values(&outs),
        [json!({ "n": 2, "last": 2 }), json!({ "n": 3, "last": 3 })]
    );

    // `split` makes many items from one, and `bufferCount` sees each item's
    // own list.
    let reg = counting().1;
    let vine = reg
        .build_pipe(
            pipe("v")
                .input(Shape::new().required("n", Ty::Int).required("ns", Ty::List))
                .output("out", Shape::open())
                .operators([
                    split("$item.ns"),
                    buffer_count(2),
                    emit_with("out", json!({ "count": "$item.count" })),
                    finalize([op("set", json!({}))]),
                ]),
        )
        .unwrap();
    let items = vec![
        json!({ "n": 1, "ns": [1, 2, 3] }),
        json!({ "n": 2, "ns": [4] }),
    ];
    let (outs, _) = all(&vine, items).await;
    assert_eq!(
        values(&outs),
        [
            json!({ "count": 2 }),
            json!({ "count": 1 }),
            json!({ "count": 1 })
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_side_pipe_that_starts_with_a_merge_map_gets_every_item() {
    let vine = Registry::new()
        .build_pipe(numbers([
            fork([vec![merge_map([emit_with("a", json!({ "n": "$item.n" }))])]]),
            emit("out"),
        ]))
        .unwrap();
    let items: Vec<Value> = (1..=3).map(|n| json!({ "n": n })).collect();
    let (outs, err) = tokio::time::timeout(Duration::from_mins(1), all(&vine, items))
        .await
        .expect("the side pipe stalled");
    assert!(err.is_none(), "{err:?}");
    let side: Vec<&Value> = outs
        .iter()
        .filter(|(n, _)| n == "a")
        .map(|(_, v)| v)
        .collect();
    assert_eq!(
        side,
        [&json!({ "n": 1 }), &json!({ "n": 2 }), &json!({ "n": 3 })]
    );
    assert_eq!(outs.len(), 6);
}

#[test]
fn last_and_buffer_count_where_they_could_only_see_one_item_are_refused() {
    let cases: Vec<(Vec<Operator>, &str)> = vec![
        (
            vec![last(), emit("out"), catch_error([emit("failed")])],
            "pipe[0].last",
        ),
        (
            vec![
                op("set", json!({ "n": "$item.n" })),
                buffer_count(2),
                emit("out"),
                finalize([op("set", json!({}))]),
            ],
            "pipe[1].bufferCount",
        ),
        (
            vec![concat_map([last(), emit("out")])],
            "pipe[0].concatMap.do[0].last",
        ),
        // Past a `catchError` inside one item's work, still one item.
        (
            vec![concat_map([
                op("set", json!({ "n": "$item.n" })),
                catch_error([op("set", json!({ "n": 0 }))]),
                last(),
                emit("out"),
            ])],
            "pipe[0].concatMap.do[2].last",
        ),
        (
            vec![
                count_to("$item.n"),
                emit("out"),
                catch_error([buffer_count(2), emit("failed")]),
            ],
            "pipe[2].catchError[0].bufferCount",
        ),
        (
            vec![branch(
                "$item.n",
                [("1", [last(), emit("a")])],
                Some(vec![emit("b")]),
            )],
            "pipe[0].branch.cases.1[0].last",
        ),
        // A side pipe of a fork before a `catchError` sees the item's
        // own stream, so it needs the stream operator too.
        (
            vec![
                fork([vec![buffer_count(2), emit("a")]]),
                emit("out"),
                catch_error([emit("failed")]),
            ],
            "pipe[0].fork[0][0].bufferCount",
        ),
    ];
    for (operators, path) in cases {
        let err = refused(operators);
        assert!(
            matches!(err, LoadError::Malformed { path: ref p, ref problem }
                if p == path && problem.contains("one item")),
            "{path}: {err}"
        );
    }
    // After an operator that makes many items from one, they are fine.
    counting()
        .1
        .build_pipe(numbers([
            count_to("$item.n").into("count"),
            fork([vec![buffer_count(2), emit("a")]]),
            last(),
            emit("out"),
            catch_error([emit("failed")]),
        ]))
        .unwrap();
}

#[test]
fn last_and_buffer_count_after_an_emit_can_never_run() {
    for operator in [last(), buffer_count(2)] {
        assert_eq!(
            refused([emit("out"), operator]),
            LoadError::Unreachable {
                path: "pipe[1]".into()
            }
        );
    }
}

#[test]
fn no_operator_can_take_a_new_word_of_the_pipe() {
    for word in ["fork", "last", "end"] {
        let mut reg = Registry::new();
        let err = reg
            .register(OperatorSpec::new(word), |_v, _cx| async { Ok(json!({})) })
            .unwrap_err();
        assert_eq!(err, RegistryError::Reserved(word.into()));
    }
    // `each` and `collect`, the old names of the mapping operators and of
    // `bufferCount`, stay reserved.
    for word in ["each", "collect"] {
        let mut reg = Registry::new();
        let err = reg
            .register(OperatorSpec::new(word), |_v, _cx| async { Ok(json!({})) })
            .unwrap_err();
        assert_eq!(err, RegistryError::Reserved(word.into()));
    }
}

// ------------------------------------------------------------- the journal

/// Run with `run_saved`, keeping every entry. Stop after `stop` outputs,
/// once the entry after the last of them is saved.
async fn saved_run(
    vine: &Pipe,
    items: Vec<Value>,
    saved: &mut Vec<Entry>,
    stop: Option<usize>,
) -> (Outs, Option<RunError>) {
    let mut outs = Vec::new();
    let mut run = vine.run_saved(stream::iter(items), Journal::from(saved.clone()));
    while let Some(event) = run.next().await {
        match event {
            Ok(Event::Output(o)) => outs.push((o.name, o.payload)),
            Ok(Event::Save(e)) => {
                let emitted = matches!(e, Entry::Emitted { .. });
                saved.push(e);
                if emitted && Some(outs.len()) == stop {
                    return (outs, None);
                }
            }
            Err(e) => return (outs, Some(e)),
        }
    }
    (outs, None)
}

#[test]
fn every_kind_of_entry_survives_a_trip_through_json() {
    let journal = Journal::from(vec![
        Entry::Done {
            at: "pipe[0]#0".into(),
            input: json!({ "a": 1 }),
            result: json!({ "b": [2] }),
        },
        Entry::Failed {
            at: "pipe[1]#0".into(),
            input: json!({}),
            code: "full".into(),
            message: "no room".into(),
        },
        Entry::Started {
            at: "pipe[2]#0".into(),
            input: json!({ "to": 3 }),
        },
        Entry::Item {
            at: "pipe[2]#0".into(),
            value: json!({ "n": 1 }),
        },
        Entry::Ended {
            at: "pipe[2]#0".into(),
            value: Value::Null,
        },
        Entry::Emitted {
            output: "out".into(),
            within: None,
            from: None,
        },
    ]);
    let text = serde_json::to_string(&journal.to_json()).unwrap();
    let back = Journal::from_json(&serde_json::from_str(&text).unwrap()).unwrap();
    assert_eq!(back, journal);
}

#[test]
fn a_journal_that_is_not_a_list_of_known_entries_is_refused() {
    for (bad, says) in [
        (json!({}), "a list"),
        (json!([1]), "entry 0: an entry is a map"),
        (json!([{ "kind": "guess" }]), "unknown entry kind"),
        (json!([{ "kind": "item", "at": "x" }]), "`value`"),
        (
            json!([{ "kind": "emitted", "output": 3 }]),
            "`output` as text",
        ),
        (
            json!([{ "kind": "emitted", "output": "out", "when": 1 }]),
            "no field `when`",
        ),
    ] {
        let err = Journal::from_json(&bad).unwrap_err();
        assert!(err.to_string().contains(says), "{bad}: {err}");
    }
}

#[tokio::test]
async fn run_saved_refuses_a_vine_that_cannot_replay_in_one_order() {
    let mut reg = counting().1;
    let exhausted = reg
        .build_pipe(numbers([exhaust_map([emit("out")])]))
        .unwrap();
    let inner = reg
        .build_pipe(
            pipe("inner")
                .input(Shape::new().required("n", Ty::Int))
                .output("out", Shape::open())
                .operators([switch_map([emit("out")])]),
        )
        .unwrap();
    reg.add_vine(inner).unwrap();
    let using = reg
        .build_pipe(numbers([use_vine("inner", None).on("out", [emit("out")])]))
        .unwrap();
    for (vine, says) in [
        (exhausted, "pipe[0].exhaustMap drops or skips work"),
        (using, "uses vine `inner`"),
    ] {
        assert!(!vine.resumable());
        let got: Vec<_> = vine
            .run_saved(stream::iter(nums(&[1])), Journal::new())
            .collect()
            .await;
        assert!(
            matches!(&got[..], [Err(RunError::NotResumable { why, .. })] if why.contains(says)),
            "{got:?}"
        );
    }
    assert!(built([emit("out")]).resumable());
    // What the clock decides is saved, so a `timeout` replays.
    assert!(built([
        op("set", json!({})),
        timeout(Duration::from_secs(1)),
        emit("out")
    ])
    .resumable());
}

#[tokio::test]
async fn a_call_with_a_saved_result_is_not_made_again() {
    let world = World::new();
    let vine = registry(&world)
        .build_pipe(
            pipe("v")
                .input(Shape::new().required("branch", Ty::Text))
                .output("out", Shape::open())
                .operators([
                    tap("git.refresh_branch", json!({ "branch": "$item.branch" })),
                    call(
                        "git.worktree_add",
                        json!({ "name": "$item.branch", "from": "main" }),
                    )
                    .into("tree"),
                    emit("out"),
                ]),
        )
        .unwrap();
    let items = vec![json!({ "branch": "a" }), json!({ "branch": "b" })];
    let mut saved = Vec::new();
    let (first, _) = saved_run(&vine, items.clone(), &mut saved, Some(1)).await;
    assert_eq!(first.len(), 1);
    let (rest, err) = saved_run(&vine, items, &mut saved, None).await;
    assert!(err.is_none());
    assert_eq!(
        rest,
        [(
            "out".to_owned(),
            json!({ "branch": "b", "tree": { "path": "/trees/b" } })
        )]
    );
    assert_eq!(
        world.log(),
        [
            "git.refresh_branch a",
            "git.worktree_add a",
            "git.refresh_branch b",
            "git.worktree_add b"
        ]
    );
}

#[tokio::test]
async fn a_saved_failure_is_replayed_as_a_failure() {
    let world = World::new();
    world.fail("git.worktree_add", "exists", "already there");
    let vine = registry(&world)
        .build_pipe(
            pipe("v")
                .input(Shape::new().required("branch", Ty::Text))
                .output("out", Shape::open())
                .output("failed", Shape::open())
                .operators([
                    call(
                        "git.worktree_add",
                        json!({ "name": "$item.branch", "from": "main" }),
                    ),
                    emit("out"),
                    catch_error([emit_with("failed", json!({ "why": "$item.error.code" }))]),
                ]),
        )
        .unwrap();
    let items = vec![json!({ "branch": "a" }), json!({ "branch": "b" })];
    let mut saved = Vec::new();
    let (first, _) = saved_run(&vine, items.clone(), &mut saved, Some(1)).await;
    assert_eq!(first, [("failed".to_owned(), json!({ "why": "exists" }))]);
    // The operator would work now. The replay still gives the saved failure, so
    // the run does not change its mind about what it already handed out.
    let mut replayed = saved.clone();
    replayed.retain(|e| !matches!(e, Entry::Emitted { .. }));
    let (again, err) = saved_run(&vine, items, &mut replayed, None).await;
    assert!(err.is_none());
    assert_eq!(again[0], ("failed".to_owned(), json!({ "why": "exists" })));
    assert_eq!(world.count("git.worktree_add"), 1);
}

#[tokio::test]
async fn a_stream_operator_is_told_where_to_resume_and_its_saved_items_flow_again() {
    let (calls, reg) = counting();
    let vine = reg
        .build_pipe(numbers([
            count_to("$item.n").into("count"),
            fork([vec![
                buffer_count(3),
                emit_with("a", json!({ "count": "$item.count" })),
            ]]),
            emit_with("out", json!({ "n": "$item.count.n" })),
        ]))
        .unwrap();
    let mut saved = Vec::new();
    let (first, _) = saved_run(&vine, nums(&[5]), &mut saved, Some(2)).await;
    assert_eq!(values(&first), nums(&[1, 2]));
    let (rest, err) = saved_run(&vine, nums(&[5]), &mut saved, None).await;
    assert!(err.is_none());
    // The batch of 1 to 3 was half full at the restart. It is rebuilt from
    // the saved items.
    assert_eq!(
        rest,
        [
            ("a".to_owned(), json!({ "count": 3 })),
            ("out".to_owned(), json!({ "n": 3 })),
            ("out".to_owned(), json!({ "n": 4 })),
            ("out".to_owned(), json!({ "n": 5 })),
            ("a".to_owned(), json!({ "count": 2 })),
        ]
    );
    let resumed: Vec<u64> = calls
        .lock()
        .unwrap()
        .iter()
        .map(|(_, from)| *from)
        .collect();
    // The first run started from nothing. The second was told two items
    // were already given, and gave only the three after them.
    assert_eq!(resumed, [0, 2]);
    let items_saved = saved
        .iter()
        .filter(|e| matches!(e, Entry::Item { .. }))
        .count();
    assert_eq!(items_saved, 5);
}

#[tokio::test]
async fn a_finished_stream_operator_is_not_called_again_on_replay() {
    let (calls, reg) = counting();
    let vine = reg
        .build_pipe(numbers([count_to("$item.n").end("done"), emit("out")]))
        .unwrap();
    let mut saved = Vec::new();
    let (first, _) = saved_run(&vine, nums(&[2]), &mut saved, None).await;
    assert_eq!(first.len(), 3);
    let (again, err) = saved_run(&vine, nums(&[2]), &mut saved, None).await;
    assert!(err.is_none());
    assert!(again.is_empty());
    assert_eq!(calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_used_vine_saves_its_calls_apart_from_the_outer_vines() {
    let world = World::new();
    let mut reg = registry(&world);
    let inner = reg
        .build_pipe(
            pipe("refresh")
                .input(Shape::new().required("branch", Ty::Text))
                .output("done", Shape::open())
                .operators([
                    tap("git.refresh_branch", json!({ "branch": "inner" })),
                    emit("done"),
                ]),
        )
        .unwrap();
    reg.add_vine(inner).unwrap();
    let outer = reg
        .build_pipe(
            pipe("v")
                .input(Shape::new().required("branch", Ty::Text))
                .output("out", Shape::open())
                .operators([
                    tap("git.refresh_branch", json!({ "branch": "outer" })),
                    use_vine("refresh", None).on("done", [emit("out")]),
                ]),
        )
        .unwrap();
    let mut saved = Vec::new();
    saved_run(&outer, vec![json!({ "branch": "a" })], &mut saved, None).await;
    let keys: Vec<&str> = saved
        .iter()
        .filter_map(|e| match e {
            Entry::Done { at, .. } => Some(at.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(keys, ["pipe[0]#0", "pipe[1]>refresh:pipe[0]#0"]);
    let (again, err) = saved_run(&outer, vec![json!({ "branch": "a" })], &mut saved, None).await;
    assert_eq!((again, err), (vec![], None));
    assert_eq!(world.log().len(), 2);
}

#[tokio::test]
async fn a_journal_that_does_not_fit_the_run_is_a_run_error() {
    let world = World::new();
    let vine = registry(&world)
        .build_pipe(
            pipe("v")
                .input(Shape::new().required("branch", Ty::Text))
                .output("out", Shape::open())
                .operators([
                    tap("git.refresh_branch", json!({ "branch": "$item.branch" })),
                    emit("out"),
                ]),
        )
        .unwrap();
    let a = || vec![json!({ "branch": "a" })];
    let misfit = |problem: &str, journal: Vec<Entry>, items: Vec<Value>| {
        let vine = vine.clone();
        let problem = problem.to_owned();
        async move {
            let mut saved = journal;
            let (_, err) = saved_run(&vine, items, &mut saved, None).await;
            assert!(
                matches!(&err, Some(RunError::ReplayMisfit { problem: p }) if p.contains(&problem)),
                "{problem}: {err:?}"
            );
        }
    };
    // Saved for other input: the vine or its input changed.
    misfit(
        "was saved for input",
        vec![Entry::Done {
            at: "pipe[0]#0".into(),
            input: json!({ "branch": "b" }),
            result: json!({}),
        }],
        a(),
    )
    .await;
    // Saved as a stream where the vine has an operator that gives one result.
    misfit(
        "saved as a stream operator",
        vec![Entry::Started {
            at: "pipe[0]#0".into(),
            input: json!({ "branch": "a" }),
        }],
        a(),
    )
    .await;
    // An item with no stream started.
    misfit(
        "not a stream",
        vec![Entry::Item {
            at: "pipe[0]#0".into(),
            value: json!({}),
        }],
        a(),
    )
    .await;
    // The journal says another output was handed out.
    misfit(
        "output `other`",
        vec![Entry::Emitted {
            output: "other".into(),
            within: None,
            from: None,
        }],
        a(),
    )
    .await;
    // The journal says more outputs were handed out than the run makes.
    misfit(
        "2 more outputs",
        vec![
            Entry::Emitted {
                output: "out".into(),
                within: None,
                from: None,
            },
            Entry::Emitted {
                output: "out".into(),
                within: None,
                from: None,
            },
            Entry::Emitted {
                output: "out".into(),
                within: None,
                from: None,
            },
        ],
        a(),
    )
    .await;
    assert!(world.log().len() <= 3);
}
