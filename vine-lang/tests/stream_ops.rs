#![cfg(feature = "yaml")]

//! Stream operators written outside the library, and how the runner moves
//! the position and kept memory around them.
mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use support::{items, lines, register_summarise, Saver};
use vine_lang::{
    json, BoxFuture, CancellationToken, Checkpoint, Emit, Instant, Memory, OpContext, OpError,
    OperatorSpec, Registry, RunError, Shape, Stop, StreamOperator, StreamOperatorSpec,
    StreamReport, Ty, Value, Vine,
};

async fn run(vine: &Vine, from: Checkpoint, values: Vec<Value>) -> (StreamReport, Saver) {
    let mut saver = Saver::default();
    let report = vine
        .run_stream(items(values), from, &mut saver, CancellationToken::new())
        .await;
    (report, saver)
}

fn words(saver: &Saver) -> Vec<String> {
    saver
        .payloads()
        .iter()
        .map(|p| p["word"].as_str().unwrap().to_owned())
        .collect()
}

/// A small stream operator: everything is written by hand, which is the
/// cost of a stream operator over a plain one.
struct Op<F: FnMut(Value, &mut Emit) -> Result<(), OpError> + Send> {
    push: F,
}

impl<F: FnMut(Value, &mut Emit) -> Result<(), OpError> + Send> StreamOperator for Op<F> {
    fn push<'a>(
        &'a mut self,
        item: Value,
        out: &'a mut Emit,
        _cx: &'a OpContext,
    ) -> BoxFuture<'a, Result<(), OpError>> {
        let r = (self.push)(item, out);
        Box::pin(async move { r })
    }
}

fn registry() -> Registry {
    let mut reg = Registry::new();
    // Drops blank lines, holds nothing.
    reg.register_stream(
        StreamOperatorSpec::new("drop_blank").emits(Shape::new().required("line", Ty::Text)),
        |_| {
            Ok(Box::new(Op {
                push: |item: Value, out: &mut Emit| {
                    if item["line"] != json!("") {
                        out.emit(item);
                    }
                    Ok(())
                },
            }))
        },
    )
    .unwrap();
    // One item per word: fan-out.
    reg.register_stream(
        StreamOperatorSpec::new("split_words").emits(Shape::new().required("word", Ty::Text)),
        |_| {
            Ok(Box::new(Op {
                push: |item: Value, out: &mut Emit| {
                    for w in item["line"].as_str().unwrap_or_default().split_whitespace() {
                        out.emit(json!({ "word": w }));
                    }
                    Ok(())
                },
            }))
        },
    )
    .unwrap();
    // Says it emits ints, emits text.
    reg.register_stream(
        StreamOperatorSpec::new("liar").emits(Shape::new().required("n", Ty::Int)),
        |_| {
            Ok(Box::new(Op {
                push: |_item: Value, out: &mut Emit| {
                    out.emit(json!({ "n": "seven" }));
                    Ok(())
                },
            }))
        },
    )
    .unwrap();
    // Fails on the line "bad".
    reg.register_stream(
        StreamOperatorSpec::new("picky").emits(Shape::new().required("line", Ty::Text)),
        |_| {
            Ok(Box::new(Op {
                push: |item: Value, out: &mut Emit| {
                    if item["line"] == json!("bad") {
                        return Err(OpError::new("picky", "will not pass `bad`"));
                    }
                    out.emit(item);
                    Ok(())
                },
            }))
        },
    )
    .unwrap();
    // Rebuildable memory that always holds the last item: pairs of
    // neighbours. Holding pins the position.
    reg.register_stream(
        StreamOperatorSpec::new("pairs")
            .emits(Shape::new().required("a", Ty::Text).required("b", Ty::Text))
            .memory(Memory::Rebuildable),
        |_| Ok(Box::new(Pairs { prev: None })),
    )
    .unwrap();
    // A deadline that `wake` never moves.
    reg.register_stream(StreamOperatorSpec::new("stuck"), |_| {
        Ok(Box::new(Stuck { due: None }))
    })
    .unwrap();
    // Settings checked by the factory.
    reg.register_stream(
        StreamOperatorSpec::new("every_nth").settings(Shape::new().required("n", Ty::Int)),
        |s| {
            if s["n"].as_i64().unwrap_or(0) < 2 {
                return Err("`n` must be 2 or more".into());
            }
            Ok(Box::new(Op {
                push: |item: Value, out: &mut Emit| {
                    out.emit(item);
                    Ok(())
                },
            }))
        },
    )
    .unwrap();
    register_summarise(&mut reg);
    reg
}

struct Pairs {
    prev: Option<Value>,
}

impl StreamOperator for Pairs {
    fn push<'a>(
        &'a mut self,
        item: Value,
        out: &'a mut Emit,
        _cx: &'a OpContext,
    ) -> BoxFuture<'a, Result<(), OpError>> {
        Box::pin(async move {
            if let Some(prev) = self.prev.take() {
                out.emit(json!({ "a": prev["line"], "b": item["line"] }));
            }
            self.prev = Some(item);
            Ok(())
        })
    }
    fn holding(&self) -> bool {
        self.prev.is_some()
    }
}

struct Stuck {
    due: Option<Instant>,
}

impl StreamOperator for Stuck {
    fn push<'a>(
        &'a mut self,
        _item: Value,
        _out: &'a mut Emit,
        _cx: &'a OpContext,
    ) -> BoxFuture<'a, Result<(), OpError>> {
        self.due = Some(Instant::now());
        Box::pin(async { Ok(()) })
    }
    fn deadline(&self) -> Option<Instant> {
        self.due
    }
}

fn line(s: &str) -> Value {
    json!({ "line": s })
}

const EACH_WORD: &str = "  - each:\n      - out: { name: word, with: { word: $item.word } }\n";

fn stream_vine(reg: &Registry, steps: &str) -> Vine {
    reg.load(&format!(
        "vine: t\nmode: stream\ninput: {{ line: text }}\noutputs:\n  word: {{ word: text }}\n  kept: {{ line: text }}\nsteps:\n{steps}"
    ))
    .unwrap_or_else(|e| panic!("{e}"))
}

#[tokio::test]
async fn a_dropped_item_is_consumed() {
    let reg = registry();
    let vine = stream_vine(
        &reg,
        "  - drop_blank: {}\n  - each:\n      - out: { name: kept, with: { line: $item.line } }\n",
    );
    let (report, saver) = run(
        &vine,
        Checkpoint::start(),
        vec![line("a"), line(""), line("b")],
    )
    .await;
    assert_eq!(report.stop, Stop::Ended);
    assert_eq!(
        saver.payloads(),
        vec![json!({ "line": "a" }), json!({ "line": "b" })]
    );
    assert_eq!(saver.positions(), vec![1, 2, 3]);
}

#[tokio::test]
async fn fan_out_moves_the_position_when_the_last_piece_comes_out_and_repeats_earlier_pieces_on_resume(
) {
    let mut reg = registry();
    let boom = Arc::new(AtomicUsize::new(1));
    let b = boom.clone();
    reg.register(
        OperatorSpec::new("check")
            .settings(Shape::new().required("word", Ty::Text))
            .returns(Shape::new().required("word", Ty::Text)),
        move |s, _| {
            let b = b.clone();
            async move {
                if s["word"] == json!("boom") && b.load(Ordering::SeqCst) > 0 {
                    b.fetch_sub(1, Ordering::SeqCst);
                    return Err(OpError::new("boom", "it blew"));
                }
                Ok(s)
            }
        },
    )
    .unwrap();
    let vine = stream_vine(
        &reg,
        &format!("  - split_words: {{}}\n  - check: {{ word: $item.word }}\n{EACH_WORD}"),
    );
    let all = vec![line("a b"), line("c boom d"), line("e")];
    let (report, saver) = run(&vine, Checkpoint::start(), all.clone()).await;
    assert!(
        matches!(report.stop, Stop::Failed(RunError::Unhandled { ref path, .. }) if path == "steps[1]")
    );
    // "c" came out, but item 2 is not consumed until "d" does.
    assert_eq!(words(&saver), ["a", "b", "c"]);
    assert_eq!(saver.positions(), vec![1]);

    let (resumed, saver) = run(&vine, report.checkpoint, all[1..].to_vec()).await;
    assert_eq!(resumed.stop, Stop::Ended);
    // At-least-once: "c" comes out again.
    assert_eq!(words(&saver), ["c", "boom", "d", "e"]);
    assert_eq!(saver.positions(), vec![2, 3]);
}

#[tokio::test]
async fn a_failure_after_a_kept_operator_leaves_its_memory_at_the_last_checkpoint() {
    let mut reg = registry();
    let fails = Arc::new(AtomicUsize::new(0));
    let f = fails.clone();
    reg.register(
        OperatorSpec::new("publish")
            .settings(Shape::new().required("lines", Ty::Int))
            .returns(Shape::new().required("lines", Ty::Int)),
        move |s, _| {
            let f = f.clone();
            async move {
                if s["lines"] == json!(4) && f.load(Ordering::SeqCst) > 0 {
                    f.fetch_sub(1, Ordering::SeqCst);
                    return Err(OpError::new("unavailable", "cannot publish"));
                }
                Ok(s)
            }
        },
    )
    .unwrap();
    let vine = reg
        .load(
            "vine: t\nmode: stream\ninput: { line: text }\noutputs:\n  published: { lines: int }\nsteps:\n  - collect: { size: 2 }\n  - summarise: { field: line }\n  - publish: { lines: $item.lines }\n",
        )
        .unwrap();
    let (whole, whole_saver) = run(&vine, Checkpoint::start(), lines("s", 1, 6)).await;
    assert_eq!(whole.stop, Stop::Ended);

    // Publishing the summary of lines 3-4 fails once. The summariser had
    // already absorbed them, but the checkpoint keeps its memory as of 2.
    fails.store(1, Ordering::SeqCst);
    let (report, saver) = run(&vine, Checkpoint::start(), lines("s", 1, 6)).await;
    assert!(
        matches!(report.stop, Stop::Failed(RunError::Unhandled { ref code, .. }) if code == "unavailable")
    );
    assert_eq!(report.checkpoint.position, 2);
    assert_eq!(report.checkpoint.memory["steps[1]"]["lines"], json!(2));

    let (resumed, saver2) = run(&vine, report.checkpoint, lines("s", 3, 6)).await;
    assert_eq!(resumed.stop, Stop::Ended);
    let mut joined = saver.outputs();
    joined.extend(saver2.outputs());
    assert_eq!(joined, whole_saver.outputs());
}

#[tokio::test]
async fn a_rebuildable_operator_that_always_holds_pins_the_position_until_the_end() {
    let reg = registry();
    let vine = reg
        .load("vine: t\nmode: stream\ninput: { line: text }\noutputs:\n  pair: { a: text, b: text }\nsteps:\n  - pairs: {}\n")
        .unwrap();
    let (report, saver) = run(
        &vine,
        Checkpoint::start(),
        vec![line("x"), line("y"), line("z")],
    )
    .await;
    assert_eq!(report.stop, Stop::Ended);
    assert_eq!(
        saver.payloads(),
        vec![json!({ "a": "x", "b": "y" }), json!({ "a": "y", "b": "z" })]
    );
    // It always holds its last item, so nothing counted as consumed until
    // the end released it. Finding: a sliding window wants kept memory.
    assert_eq!(saver.positions(), vec![3]);
}

#[tokio::test]
async fn a_provided_stream_operator_failure_stops_the_run_before_the_item() {
    let reg = registry();
    let vine = stream_vine(
        &reg,
        "  - picky: {}\n  - each:\n      - out: { name: kept, with: { line: $item.line } }\n",
    );
    let (report, saver) = run(
        &vine,
        Checkpoint::start(),
        vec![line("a"), line("bad"), line("c")],
    )
    .await;
    assert_eq!(
        report.stop,
        Stop::Failed(RunError::Unhandled {
            path: "steps[0]".into(),
            operator: "picky".into(),
            code: "picky".into(),
            message: "will not pass `bad`".into(),
        })
    );
    assert_eq!(report.checkpoint.position, 1);
    assert_eq!(saver.payloads(), vec![json!({ "line": "a" })]);
}

#[tokio::test]
async fn emitting_a_value_that_breaks_the_declared_shape_stops_the_run() {
    let reg = registry();
    let vine = reg
        .load("vine: t\nmode: stream\ninput: { line: text }\noutputs:\n  n: { n: int }\nsteps:\n  - liar: {}\n")
        .unwrap();
    let (report, _) = run(&vine, Checkpoint::start(), vec![line("a")]).await;
    assert_eq!(
        report.stop,
        Stop::Failed(RunError::ResultMisfit {
            path: "steps[0]".into(),
            operator: "liar".into(),
            problem: "field `n` should be int, found text \"seven\"".into(),
        })
    );
}

#[tokio::test(start_paused = true)]
async fn a_deadline_that_wake_does_not_move_stops_the_run_instead_of_spinning() {
    let reg = registry();
    let vine = reg
        .load("vine: t\nmode: stream\ninput: { line: text }\noutputs:\n  out: { line: text }\nsteps:\n  - stuck: {}\n  - each:\n      - out: { name: out, with: { line: $item.line } }\n")
        .unwrap();
    let (tx, source) = support::channel();
    tx.send(line("a")).unwrap();
    let mut saver = Saver::default();
    let report = vine
        .run_stream(
            source,
            Checkpoint::start(),
            &mut saver,
            CancellationToken::new(),
        )
        .await;
    assert_eq!(
        report.stop,
        Stop::Failed(RunError::DeadlineNotMoved {
            path: "steps[0]".into(),
            operator: "stuck".into(),
        })
    );
}

#[tokio::test]
async fn try_inside_each_catches_a_failure_and_the_stream_goes_on() {
    let mut reg = registry();
    reg.register(
        OperatorSpec::new("deliver")
            .settings(Shape::new().required("line", Ty::Text))
            .touches(["mail"]),
        |s, _| async move {
            if s["line"] == json!("bounce") {
                return Err(OpError::new("bounced", "no such mailbox"));
            }
            Ok(Value::Null)
        },
    )
    .unwrap();
    let vine = reg
        .load(
            "
vine: mail
mode: stream
input: { line: text }
outputs:
  sent: { line: text }
  bounced: { line: text, why: text }
steps:
  - each:
      - try:
          do:
            - deliver: { line: $item.line }
            - out: { name: sent, with: { line: $item.line } }
          catch:
            bounced:
              - out: { name: bounced, with: { line: $item.line, why: $error.message } }
",
        )
        .unwrap();
    let (report, saver) = run(
        &vine,
        Checkpoint::start(),
        vec![line("a"), line("bounce"), line("c")],
    )
    .await;
    assert_eq!(report.stop, Stop::Ended);
    let names: Vec<String> = saver.outputs().into_iter().map(|o| o.name).collect();
    assert_eq!(names, ["sent", "bounced", "sent"]);
    assert_eq!(saver.positions(), vec![1, 2, 3]);
    assert_eq!(vine.reach().iter().collect::<Vec<_>>(), vec!["mail"]);
}

#[tokio::test]
async fn kept_memory_matches_the_position_when_a_buffer_after_it_holds_its_results() {
    let mut reg = registry();
    let fails = Arc::new(AtomicUsize::new(0));
    let f = fails.clone();
    reg.register(
        OperatorSpec::new("publish")
            .settings(Shape::new().required("batch", Ty::List))
            .returns(Shape::new().required("batch", Ty::List)),
        move |s, _| {
            let f = f.clone();
            async move {
                if s["batch"][0]["lines"] == json!(6) && f.load(Ordering::SeqCst) > 0 {
                    f.fetch_sub(1, Ordering::SeqCst);
                    return Err(OpError::new("unavailable", "cannot publish"));
                }
                Ok(s)
            }
        },
    )
    .unwrap();
    // Summaries of 2 lines each, published 2 summaries at a time.
    let vine = reg
        .load(
            "vine: t\nmode: stream\ninput: { line: text }\noutputs:\n  published: { batch: list }\nsteps:\n  - collect: { size: 2 }\n  - summarise: { field: line }\n  - collect: { size: 2 }\n  - publish: { batch: $item.items }\n",
        )
        .unwrap();
    let (whole, whole_saver) = run(&vine, Checkpoint::start(), lines("s", 1, 8)).await;
    assert_eq!(whole.stop, Stop::Ended);

    fails.store(1, Ordering::SeqCst);
    let (report, saver) = run(&vine, Checkpoint::start(), lines("s", 1, 8)).await;
    assert!(
        matches!(report.stop, Stop::Failed(RunError::Unhandled { ref code, .. }) if code == "unavailable")
    );
    // When it failed, the summariser had absorbed lines 1-8, and its
    // summary of 1-6 sat in the second buffer before that. The last
    // checkpoint is at 4, so its memory is the summary of exactly 1-4.
    assert_eq!(report.checkpoint.position, 4);
    assert_eq!(report.checkpoint.memory["steps[1]"]["lines"], json!(4));
    // Checkpoints the sink saw: memory always matching the position.
    for cp in saver.checkpoints() {
        assert_eq!(cp.memory["steps[1]"]["lines"], json!(cp.position));
    }

    let (resumed, saver2) = run(&vine, report.checkpoint, lines("s", 5, 8)).await;
    assert_eq!(resumed.stop, Stop::Ended);
    let mut joined = saver.outputs();
    joined.extend(saver2.outputs());
    assert_eq!(joined, whole_saver.outputs());
}
