//! The one model, in Rust alone: a vine is a stream in and a stream out.
//! Nothing here uses YAML, and this file passes with the feature off:
//!
//!     cargo test --no-default-features --test pipe

mod acts;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use acts::{job_launch_pipe, ready_job, registry, World};
use futures::{stream, StreamExt};
use tokio::time::{sleep, Instant};
use vine_lang::pipe::{
    attempt, branch, call, each, emit, emit_with, filter, op, pipe, split, take, tap, together,
    Overlap, Pipe, PipeDef, Stage,
};
use vine_lang::{json, LoadError, OpError, OperatorSpec, Registry, RunError, Shape, Ty, Value};

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

fn n(shape: Shape) -> Shape {
    shape.required("n", Ty::Int)
}

/// A vine over items `{ n: int }` with one open output, `out`.
fn numbers(stages: impl IntoIterator<Item = Stage>) -> PipeDef {
    pipe("numbers")
        .input(n(Shape::new()))
        .output("out", Shape::open())
        .stages(stages)
}

fn nums(ns: &[i64]) -> Vec<Value> {
    ns.iter().map(|n| json!({ "n": n })).collect()
}

fn build(def: PipeDef) -> Pipe {
    Registry::new().build_pipe(def).unwrap()
}

fn refused(def: PipeDef) -> LoadError {
    Registry::new().build_pipe(def).unwrap_err()
}

// ---------------------------------------------------------- stream in, out

#[tokio::test]
async fn one_item_in_gives_what_the_vine_emits() {
    let vine = build(numbers([emit("out")]));
    let out = vine.run_one(json!({ "n": 1 })).await.unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(
        (out[0].name.as_str(), &out[0].payload),
        ("out", &json!({ "n": 1 }))
    );
}

#[tokio::test]
async fn many_items_in_come_out_in_order() {
    let vine = build(numbers([emit("out")]));
    let (outs, err) = all(&vine, nums(&[1, 2, 3])).await;
    assert!(err.is_none());
    let got: Vec<&Value> = outs.iter().map(|(_, v)| &v["n"]).collect();
    assert_eq!(got, [&json!(1), &json!(2), &json!(3)]);
}

#[tokio::test]
async fn no_items_in_gives_nothing_out() {
    let vine = build(numbers([emit("out")]));
    assert_eq!(all(&vine, vec![]).await.0, vec![]);
}

#[tokio::test]
async fn an_item_that_does_not_fit_the_input_ends_the_stream_after_what_came_before() {
    let vine = build(numbers([emit("out")]));
    let items = vec![json!({ "n": 1 }), json!({ "n": "two" }), json!({ "n": 3 })];
    let (outs, err) = all(&vine, items).await;
    assert_eq!(outs.len(), 1);
    assert!(matches!(err, Some(RunError::InputMisfit(_))), "{err:?}");
}

// ----------------------------------------------------- what the item keeps

fn with_double() -> Registry {
    let mut reg = Registry::new();
    reg.register(
        OperatorSpec::new("double")
            .settings(n(Shape::new()))
            .returns(Shape::new().required("twice", Ty::Int)),
        |v, _cx| async move { Ok(json!({ "twice": v["n"].as_i64().unwrap() * 2 })) },
    )
    .unwrap();
    reg
}

#[tokio::test]
async fn a_result_replaces_the_item_unless_the_stage_says_into() {
    let reg = with_double();
    let replace = reg
        .build_pipe(numbers([
            op("double", json!({ "n": "$item.n" })),
            emit("out"),
        ]))
        .unwrap();
    let out = replace.run_one(json!({ "n": 4 })).await.unwrap();
    assert_eq!(out[0].payload, json!({ "twice": 8 }));

    let keep = reg
        .build_pipe(numbers([
            op("double", json!({ "n": "$item.n" })).into("doubled"),
            emit("out"),
        ]))
        .unwrap();
    let out = keep.run_one(json!({ "n": 4 })).await.unwrap();
    assert_eq!(out[0].payload, json!({ "n": 4, "doubled": { "twice": 8 } }));
}

#[tokio::test]
async fn tap_runs_an_act_and_leaves_the_item_alone() {
    let world = World::new();
    let def = pipe("v")
        .input(Shape::new().required("branch", Ty::Text))
        .output("out", Shape::open())
        .stages([
            tap("git.refresh_branch", json!({ "branch": "$item.branch" })),
            emit("out"),
        ]);
    let vine = registry(&world).build_pipe(def).unwrap();
    let out = vine.run_one(json!({ "branch": "main" })).await.unwrap();
    assert_eq!(out[0].payload, json!({ "branch": "main" }));
    assert_eq!(world.log(), ["git.refresh_branch main"]);
}

#[tokio::test]
async fn into_on_an_item_that_is_not_a_map_is_a_run_error() {
    let reg = with_double();
    let def = pipe("v")
        .input(Shape::new().required("ns", Ty::List))
        .output("out", Shape::open())
        .stages([
            split("$item.ns"),
            op("double", json!({ "n": 1 })).into("doubled"),
            emit("out"),
        ]);
    let err = reg
        .build_pipe(def)
        .unwrap()
        .run_one(json!({ "ns": [1] }))
        .await
        .unwrap_err();
    assert!(matches!(err, RunError::ItemMisfit { .. }), "{err}");
}

// ------------------------------------------------------- split, filter, take

fn listed(stages: impl IntoIterator<Item = Stage>) -> Pipe {
    build(
        pipe("v")
            .input(
                Shape::new()
                    .required("plan", Ty::Id)
                    .required("jobs", Ty::Any),
            )
            .output("out", Shape::open())
            .stages(stages),
    )
}

#[tokio::test]
async fn split_turns_one_item_into_one_per_element() {
    // With no `into`, the element is the new item.
    let vine = listed([
        split("$item.jobs"),
        emit_with("out", json!({ "job": "$item" })),
    ]);
    let out = vine
        .run_one(json!({ "plan": "p", "jobs": ["a", "b", "c"] }))
        .await
        .unwrap();
    let got: Vec<&Value> = out.iter().map(|o| &o.payload["job"]).collect();
    assert_eq!(got, [&json!("a"), &json!("b"), &json!("c")]);
}

#[tokio::test]
async fn split_with_into_keeps_the_item_and_adds_the_element() {
    let vine = listed([
        split("$item.jobs").into("job"),
        emit_with("out", json!({ "plan": "$item.plan", "job": "$item.job" })),
    ]);
    let out = vine
        .run_one(json!({ "plan": "p", "jobs": ["a", "b"] }))
        .await
        .unwrap();
    let got: Vec<&Value> = out.iter().map(|o| &o.payload).collect();
    assert_eq!(
        got,
        [
            &json!({ "plan": "p", "job": "a" }),
            &json!({ "plan": "p", "job": "b" })
        ]
    );
}

#[tokio::test]
async fn split_of_an_empty_list_emits_nothing() {
    let vine = listed([split("$item.jobs"), emit("out")]);
    assert!(vine
        .run_one(json!({ "plan": "p", "jobs": [] }))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn split_of_something_that_is_not_a_list_is_a_run_error() {
    let vine = listed([split("$item.jobs"), emit("out")]);
    let err = vine
        .run_one(json!({ "plan": "p", "jobs": "a" }))
        .await
        .unwrap_err();
    assert!(matches!(err, RunError::ItemMisfit { .. }), "{err}");
}

#[tokio::test]
async fn filter_drops_the_items_its_condition_does_not_hold_for() {
    let vine = build(numbers([filter("$item.n != 2"), emit("out")]));
    let (outs, _) = all(&vine, nums(&[1, 2, 3])).await;
    assert_eq!(outs.len(), 2);
    assert!(outs.iter().all(|(_, v)| v["n"] != 2));
}

#[tokio::test]
async fn take_one_makes_a_vine_give_exactly_one_answer() {
    let vine = build(numbers([emit("out"), take(1)]));
    let (outs, err) = all(&vine, nums(&[1, 2, 3])).await;
    assert!(err.is_none());
    assert_eq!(outs, [("out".to_owned(), json!({ "n": 1 }))]);
}

#[tokio::test]
async fn take_stops_the_work_for_the_items_it_does_not_let_through() {
    let world = World::new();
    let def = pipe("v")
        .input(Shape::new().required("branch", Ty::Text))
        .output("out", Shape::open())
        .stages([
            tap("git.refresh_branch", json!({ "branch": "$item.branch" })),
            take(1),
            emit("out"),
        ]);
    let vine = registry(&world).build_pipe(def).unwrap();
    let items = vec![json!({ "branch": "a" }), json!({ "branch": "b" })];
    assert_eq!(all(&vine, items).await.0.len(), 1);
    assert_eq!(world.log(), ["git.refresh_branch a"]);
}

// ------------------------------------------------------------------ timing

/// Acts that take time, on tokio's paused clock. `work.slow` waits the
/// seconds it is told and records when it started and finished.
#[derive(Default)]
struct Clock {
    log: Mutex<Vec<String>>,
}

impl Clock {
    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
}

fn timed(start: Instant) -> (Arc<Clock>, Registry) {
    let clock = Arc::new(Clock::default());
    let mut reg = Registry::new();
    let c = clock.clone();
    reg.register_act(
        OperatorSpec::new("work.slow")
            .settings(
                Shape::new()
                    .required("id", Ty::Any)
                    .required("secs", Ty::Int)
                    .optional("fail", Ty::Bool),
            )
            .returns(Shape::new().required("id", Ty::Any))
            .touches(["work"]),
        move |v, _cx| {
            let c = c.clone();
            async move {
                let at = |c: &Clock, what: &str, v: &Value| {
                    let t = start.elapsed().as_secs();
                    c.log
                        .lock()
                        .unwrap()
                        .push(format!("{what} {} @{t}", v["id"]));
                };
                at(&c, "start", &v);
                sleep(Duration::from_secs(v["secs"].as_u64().unwrap())).await;
                if v["fail"] == true {
                    return Err(OpError::new("broken", "it broke"));
                }
                at(&c, "done", &v);
                Ok(json!({ "id": v["id"] }))
            }
        },
    )
    .unwrap();
    (clock, reg)
}

#[tokio::test(start_paused = true)]
async fn together_runs_side_by_side_and_goes_on_when_all_are_done() {
    let start = Instant::now();
    let (clock, reg) = timed(start);
    let def = numbers([
        together([
            call("work.slow", json!({ "id": "a", "secs": 10 })).into("a"),
            call("work.slow", json!({ "id": "b", "secs": 4 })).into("b"),
            call("work.slow", json!({ "id": "c", "secs": 7 })).into("c"),
        ]),
        emit("out"),
    ]);
    let out = reg
        .build_pipe(def)
        .unwrap()
        .run_one(json!({ "n": 1 }))
        .await
        .unwrap();
    // Ten seconds, not twenty-one: the longest of the three.
    assert_eq!(start.elapsed().as_secs(), 10);
    assert_eq!(
        out[0].payload,
        json!({ "n": 1, "a": { "id": "a" }, "b": { "id": "b" }, "c": { "id": "c" } })
    );
    assert_eq!(
        &clock.log()[..3],
        ["start \"a\" @0", "start \"b\" @0", "start \"c\" @0"]
    );
}

#[tokio::test(start_paused = true)]
async fn together_fails_when_one_of_them_fails_and_attempt_can_handle_it() {
    let (_clock, reg) = timed(Instant::now());
    let both = || {
        together([
            call("work.slow", json!({ "id": "a", "secs": 1 })).into("a"),
            call("work.slow", json!({ "id": "b", "secs": 1, "fail": true })).into("b"),
        ])
    };
    let bare = reg.build_pipe(numbers([both(), emit("out")])).unwrap();
    let err = bare.run_one(json!({ "n": 1 })).await.unwrap_err();
    assert!(
        matches!(err, RunError::Unhandled { ref code, .. } if code == "broken"),
        "{err}"
    );

    let handled = reg
        .build_pipe(numbers([attempt(
            [both(), emit("out")],
            [emit_with("out", json!({ "failed": "$item.error.from" }))],
        )]))
        .unwrap();
    let out = handled.run_one(json!({ "n": 1 })).await.unwrap();
    assert_eq!(out[0].payload, json!({ "failed": "work.slow" }));
}

#[test]
fn together_needs_two_or_more_stages_each_with_its_own_into() {
    let (_clock, reg) = timed(Instant::now());
    let slow = |id: &str| call("work.slow", json!({ "id": id, "secs": 1 }));
    let one = numbers([together([slow("a").into("a")]), emit("out")]);
    assert!(matches!(
        reg.build_pipe(one).unwrap_err(),
        LoadError::Malformed { .. }
    ));

    let no_into = numbers([together([slow("a").into("a"), slow("b")]), emit("out")]);
    let err = reg.build_pipe(no_into).unwrap_err();
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].together[1]"),
        "{err}"
    );

    let same = numbers([
        together([slow("a").into("x"), slow("b").into("x")]),
        emit("out"),
    ]);
    let err = reg.build_pipe(same).unwrap_err();
    assert!(
        matches!(err, LoadError::DuplicateBinding { ref name, .. } if name == "x"),
        "{err}"
    );

    let not_a_unit = numbers([together([slow("a").into("a"), take(1)]), emit("out")]);
    assert!(matches!(
        reg.build_pipe(not_a_unit).unwrap_err(),
        LoadError::Malformed { .. }
    ));
}

/// Three items, arriving at 0, 1 and 2 seconds.
fn arriving() -> impl futures::Stream<Item = Value> + Send {
    stream::iter([1u64, 2, 3]).then(|n| async move {
        if n > 1 {
            sleep(Duration::from_secs(1)).await;
        }
        json!({ "n": n })
    })
}

/// Each item takes five seconds of work, under the given overlap rule.
async fn overlapping(overlap: Overlap, at_once: Option<usize>) -> (Vec<Value>, Vec<String>, u64) {
    let start = Instant::now();
    let (clock, reg) = timed(start);
    let mut stage = each(
        overlap,
        [
            call("work.slow", json!({ "id": "$item.n", "secs": 5 })),
            emit("out"),
        ],
    );
    if let Some(limit) = at_once {
        stage = stage.at_once(limit);
    }
    let vine = reg.build_pipe(numbers([stage])).unwrap();
    let outs: Vec<Value> = vine
        .run(arriving())
        .map(|r| r.unwrap().payload["id"].clone())
        .collect()
        .await;
    (outs, clock.log(), start.elapsed().as_secs())
}

#[tokio::test(start_paused = true)]
async fn concat_runs_one_at_a_time_in_order() {
    let (outs, log, secs) = overlapping(Overlap::Concat, None).await;
    assert_eq!(outs, [json!(1), json!(2), json!(3)]);
    assert_eq!(
        log,
        [
            "start 1 @0",
            "done 1 @5",
            "start 2 @6",
            "done 2 @11",
            "start 3 @12",
            "done 3 @17"
        ]
    );
    assert_eq!(secs, 17);
}

#[tokio::test(start_paused = true)]
async fn merge_runs_side_by_side() {
    let (outs, log, secs) = overlapping(Overlap::Merge, None).await;
    assert_eq!(outs, [json!(1), json!(2), json!(3)]);
    assert_eq!(&log[..3], ["start 1 @0", "start 2 @1", "start 3 @2"]);
    assert_eq!(secs, 7);
}

#[tokio::test(start_paused = true)]
async fn merge_with_at_once_holds_the_rest_back() {
    let (outs, log, secs) = overlapping(Overlap::Merge, Some(2)).await;
    assert_eq!(outs, [json!(1), json!(2), json!(3)]);
    // With two running, the third item is not even asked for. It is asked
    // for when the first is done at 5, arrives a second later, and starts.
    assert_eq!(
        log,
        [
            "start 1 @0",
            "start 2 @1",
            "done 1 @5",
            "done 2 @6",
            "start 3 @6",
            "done 3 @11"
        ]
    );
    assert_eq!(secs, 11);
}

#[tokio::test(start_paused = true)]
async fn switch_drops_the_run_in_flight_for_the_new_item() {
    let (outs, log, secs) = overlapping(Overlap::Switch, None).await;
    assert_eq!(outs, [json!(3)]);
    assert_eq!(log, ["start 1 @0", "start 2 @1", "start 3 @2", "done 3 @7"]);
    assert_eq!(secs, 7);
}

#[tokio::test(start_paused = true)]
async fn exhaust_ignores_new_items_while_a_run_is_going() {
    let (outs, log, secs) = overlapping(Overlap::Exhaust, None).await;
    assert_eq!(outs, [json!(1)]);
    assert_eq!(log, ["start 1 @0", "done 1 @5"]);
    assert_eq!(secs, 5);
}

#[test]
fn at_once_goes_with_merge_only_and_is_at_least_one() {
    let inner = || [emit("out")];
    let err = refused(numbers([each(Overlap::Concat, inner()).at_once(2)]));
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].each"),
        "{err}"
    );
    let err = refused(numbers([each(Overlap::Merge, inner()).at_once(0)]));
    assert!(matches!(err, LoadError::Malformed { .. }), "{err}");
}

// ----------------------------------------------------------------- attempt

fn failing(code: &'static str) -> Registry {
    let mut reg = Registry::new();
    reg.register_act(
        OperatorSpec::new("store.put").touches(["store"]),
        move |_v, _cx| async move {
            Err::<Value, _>(if code == "cancelled" {
                OpError::cancelled("stopped")
            } else {
                OpError::new(code, "no room")
            })
        },
    )
    .unwrap();
    reg
}

#[tokio::test]
async fn attempt_hands_the_failure_to_on_error_as_part_of_the_item() {
    let def = numbers([attempt(
        [call("store.put", json!({})), emit("out")],
        [emit_with(
            "out",
            json!({ "n": "$item.n", "error": "$item.error" }),
        )],
    )]);
    let out = failing("full")
        .build_pipe(def)
        .unwrap()
        .run_one(json!({ "n": 1 }))
        .await
        .unwrap();
    assert_eq!(
        out[0].payload,
        json!({ "n": 1, "error": { "code": "full", "message": "no room", "from": "store.put" } })
    );
}

#[tokio::test]
async fn on_error_sees_the_item_as_it_was_when_the_stage_failed() {
    let reg = {
        let mut reg = failing("full");
        reg.register(
            OperatorSpec::new("mark").settings(Shape::open()),
            |v, _cx| async move { Ok(v) },
        )
        .unwrap();
        reg
    };
    let def = numbers([attempt(
        [
            op("mark", json!({ "step": "one" })).into("progress"),
            call("store.put", json!({})),
            emit("out"),
        ],
        [emit_with("out", json!({ "got_to": "$item.progress.step" }))],
    )]);
    let out = reg
        .build_pipe(def)
        .unwrap()
        .run_one(json!({ "n": 1 }))
        .await
        .unwrap();
    assert_eq!(out[0].payload, json!({ "got_to": "one" }));
}

#[tokio::test]
async fn one_item_failing_does_not_stop_the_items_after_it() {
    let mut reg = Registry::new();
    reg.register_act(
        OperatorSpec::new("store.put")
            .settings(n(Shape::new()))
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
    let def = numbers([attempt(
        [call("store.put", json!({ "n": "$item.n" })), emit("out")],
        [emit_with("out", json!({ "skipped": "$item.n" }))],
    )]);
    let (outs, err) = all(&reg.build_pipe(def).unwrap(), nums(&[1, 2, 3])).await;
    assert!(err.is_none());
    let got: Vec<&Value> = outs.iter().map(|(_, v)| v).collect();
    assert_eq!(
        got,
        [
            &json!({ "n": 1 }),
            &json!({ "skipped": 2 }),
            &json!({ "n": 3 })
        ]
    );
}

#[tokio::test]
async fn a_failure_nothing_handles_ends_the_stream_with_a_run_error() {
    let def = numbers([call("store.put", json!({})), emit("out")]);
    let (outs, err) = all(&failing("full").build_pipe(def).unwrap(), nums(&[1, 2])).await;
    assert!(outs.is_empty());
    assert!(
        matches!(err, Some(RunError::Unhandled { ref operator, .. }) if operator == "store.put"),
        "{err:?}"
    );
}

#[tokio::test]
async fn attempt_does_not_swallow_cancellation() {
    let def = numbers([attempt(
        [call("store.put", json!({})), emit("out")],
        [emit("out")],
    )]);
    let err = failing("cancelled")
        .build_pipe(def)
        .unwrap()
        .run_one(json!({ "n": 1 }))
        .await
        .unwrap_err();
    assert!(matches!(err, RunError::Cancelled { .. }), "{err}");
}

#[tokio::test]
async fn a_failure_inside_on_error_is_a_run_error() {
    let def = numbers([attempt(
        [call("store.put", json!({})), emit("out")],
        [call("store.put", json!({})), emit("out")],
    )]);
    let err = failing("full")
        .build_pipe(def)
        .unwrap()
        .run_one(json!({ "n": 1 }))
        .await
        .unwrap_err();
    assert!(
        matches!(err, RunError::Unhandled { ref path, .. } if path.contains("on_error")),
        "{err}"
    );
}

// ------------------------------------------------------------------ branch

fn kinds(otherwise: Option<Vec<Stage>>) -> Pipe {
    build(
        pipe("v")
            .input(Shape::new().required("kind", Ty::Text))
            .output("kept", Shape::open())
            .output("other", Shape::open())
            .stages([
                branch("$item.kind", [("keep", [emit("kept")])], otherwise),
                emit("other"),
            ]),
    )
}

#[tokio::test]
async fn branch_runs_the_case_that_matches_and_passes_the_rest_through() {
    let vine = kinds(None);
    let (outs, _) = all(
        &vine,
        vec![json!({ "kind": "keep" }), json!({ "kind": "drop" })],
    )
    .await;
    let names: Vec<&str> = outs.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["kept", "other"]);
}

#[test]
fn a_stage_after_a_branch_that_always_emits_can_never_run() {
    let def = pipe("v")
        .input(Shape::new().required("kind", Ty::Text))
        .output("kept", Shape::open())
        .output("other", Shape::open())
        .stages([
            branch(
                "$item.kind",
                [("keep", [emit("kept")])],
                Some(vec![emit("other")]),
            ),
            emit("other"),
        ]);
    assert_eq!(
        refused(def),
        LoadError::Unreachable {
            path: "pipe[1]".into()
        }
    );
}

// ------------------------------------------------------------- the checker

#[test]
fn a_pipe_that_may_not_emit_is_refused() {
    assert_eq!(
        refused(numbers([filter("$item.n == 1")])),
        LoadError::MissingOutput {
            path: "pipe".into()
        }
    );
    let def = numbers([branch("$item.n", [("1", [emit("out")])], None)]);
    assert_eq!(
        refused(def),
        LoadError::MissingOutput {
            path: "pipe".into()
        }
    );
}

#[test]
fn an_empty_pipe_and_a_take_of_zero_are_refused() {
    assert!(
        matches!(refused(numbers([])), LoadError::Malformed { ref path, .. } if path == "pipe")
    );
    assert!(matches!(
        refused(numbers([take(0), emit("out")])),
        LoadError::Malformed { ref path, .. } if path == "pipe[0].take"
    ));
}

#[test]
fn an_output_nobody_declared_is_refused() {
    assert_eq!(
        refused(numbers([emit("done")])),
        LoadError::UndeclaredOutput {
            path: "pipe[0].emit".into(),
            name: "done".into()
        }
    );
}

#[test]
fn a_payload_that_cannot_fit_its_output_is_refused_before_it_runs() {
    let def = pipe("v")
        .input(n(Shape::new()))
        .output("out", Shape::new().required("job", Ty::Id))
        .stages([emit_with("out", json!({ "job": 7 }))]);
    assert!(matches!(refused(def), LoadError::OutputMisfit { .. }));
    // Emitting the item as it is: the item's shape must fit the output.
    let def = pipe("v")
        .input(n(Shape::new()))
        .output("out", Shape::new().required("job", Ty::Id))
        .stages([emit("out")]);
    assert!(matches!(refused(def), LoadError::OutputMisfit { .. }));
}

#[tokio::test]
async fn a_payload_that_does_not_fit_its_output_at_run_time_is_a_run_error() {
    let def = pipe("v")
        .input(Shape::new().required("job", Ty::Any))
        .output("out", Shape::new().required("job", Ty::Id))
        .stages([emit_with("out", json!({ "job": "$item.job" }))]);
    let err = build(def).run_one(json!({ "job": 7 })).await.unwrap_err();
    assert!(matches!(err, RunError::OutputMisfit { .. }), "{err}");
}

#[test]
fn item_is_the_only_name_and_a_field_it_never_has_is_refused() {
    let err = refused(numbers([emit_with("out", json!({ "x": "$in.n" }))]));
    assert!(
        matches!(err, LoadError::UnboundReference { ref name, .. } if name == "in"),
        "{err}"
    );
    let err = refused(numbers([filter("$item.nope == 1"), emit("out")]));
    assert!(
        matches!(err, LoadError::UnknownField { ref field, .. } if field == "nope"),
        "{err}"
    );
}

#[test]
fn an_unregistered_operator_or_act_is_refused_by_name() {
    let err = refused(numbers([op("launch_job", json!({})), emit("out")]));
    assert_eq!(
        err,
        LoadError::UnknownOperator {
            path: "pipe[0]".into(),
            name: "launch_job".into()
        }
    );
    let err = refused(numbers([call("git.push", json!({})), emit("out")]));
    assert_eq!(
        err,
        LoadError::UnknownAct {
            path: "pipe[0]".into(),
            name: "git.push".into()
        }
    );
}

#[test]
fn an_act_handed_input_that_does_not_fit_is_refused_before_it_runs() {
    let world = World::new();
    let def = numbers([
        call("git.worktree_remove", json!({ "dir": "/x" })),
        emit("out"),
    ]);
    let err = registry(&world).build_pipe(def).unwrap_err();
    assert!(
        matches!(err, LoadError::BadSettings { ref path, .. } if path == "pipe[0].with"),
        "{err}"
    );
}

#[test]
fn no_operator_can_take_a_word_of_the_pipe() {
    for word in [
        "tap", "split", "filter", "take", "together", "attempt", "branch", "emit", "into",
    ] {
        let mut reg = Registry::new();
        let err = reg
            .register(OperatorSpec::new(word), |_v, _cx| async { Ok(json!({})) })
            .unwrap_err();
        assert_eq!(err, vine_lang::RegistryError::Reserved(word.into()));
    }
}

// --------------------------------------------------- job_launch as a pipe

#[tokio::test]
async fn job_launch_as_a_pipe_starts_the_agent_and_emits_started() {
    let world = World::new();
    let vine = registry(&world).build_pipe(job_launch_pipe()).unwrap();
    assert_eq!(
        vine.reach().iter().map(String::as_str).collect::<Vec<_>>(),
        ["agent", "git"]
    );
    let out = vine.run_one(ready_job()).await.unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].name, "started");
    assert_eq!(
        out[0].payload,
        json!({ "job": "job-1", "session": "session-1", "tree": "/trees/job-1" })
    );
}

#[tokio::test]
async fn job_launch_as_a_pipe_launches_a_stream_of_jobs() {
    let world = World::new();
    let vine = registry(&world).build_pipe(job_launch_pipe()).unwrap();
    let jobs: Vec<Value> = ["job-1", "job-2", "job-3"]
        .iter()
        .map(|j| json!({ "job": j, "plan_branch": "plan/p-1", "brief": "work" }))
        .collect();
    let (outs, err) = all(&vine, jobs).await;
    assert!(err.is_none());
    assert_eq!(outs.len(), 3);
    assert_eq!(
        world.trees(),
        ["/trees/job-1", "/trees/job-2", "/trees/job-3"]
    );
}
