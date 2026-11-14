//! A vine with a `timeout` written in YAML saves and replays exactly as
//! its Rust twin: the same entries, the same decisions, the same outputs
//! with the same ids, clean and after a kill at every event.
#![cfg(feature = "yaml")]

mod acts;

use std::sync::Arc;
use std::time::Duration;

use acts::desk::{desk_registry, merge_jobs, step, Desk};
use futures::{stream, StreamExt};
use vine_lang::pipe::{catch_error, emit_with, fork, pipe, split, timeout, Pipe, PipeDef};
use vine_lang::{json, Event, Journal, Shape, Ty, Value};

const YAML: &str = r#"
vine: timed
input: { jobs: list }
outputs:
  stepped: { id: text, step: text }
  done: { id: text }
  failed: { id: text, code: text }
pipe:
  - split: $item.jobs
    into: job
  - mergeMap:
      concurrent: 2
      do:
        - call: work.step
          with: { id: $item.job.id, step: one, secs: $item.job.secs }
          into: one
        - fork:
            - - emit: { to: stepped, with: { id: $item.job.id, step: one } }
        - call: work.step
          with: { id: $item.job.id, step: two, secs: $item.job.two }
          into: two
        - timeout: 3s
        - emit: { to: done, with: { id: $item.job.id } }
        - catchError:
            - emit: { to: failed, with: { id: $item.job.id, code: $item.error.code } }
"#;

fn rust() -> PipeDef {
    let id = || Shape::new().required("id", Ty::Text);
    pipe("timed")
        .input(Shape::new().required("jobs", Ty::List))
        .output("stepped", id().required("step", Ty::Text))
        .output("done", id())
        .output("failed", id().required("code", Ty::Text))
        .stages([
            split("$item.jobs").into("job"),
            merge_jobs(
                Some(2),
                vec![
                    step("one", json!("$item.job.secs")),
                    fork([[emit_with(
                        "stepped",
                        json!({ "id": "$item.job.id", "step": "one" }),
                    )]]),
                    step("two", json!("$item.job.two")),
                    timeout(Duration::from_secs(3)),
                    emit_with("done", json!({ "id": "$item.job.id" })),
                    catch_error([emit_with(
                        "failed",
                        json!({ "id": "$item.job.id", "code": "$item.error.code" }),
                    )]),
                ],
            ),
        ])
}

fn both(desk: &Arc<Desk>) -> [Pipe; 2] {
    let reg = desk_registry(desk);
    [
        reg.build_pipe(rust()).unwrap(),
        reg.load_pipe(YAML).unwrap(),
    ]
}

fn input() -> Vec<Value> {
    let job = |id: &str, secs: u64, two: u64| json!({ "id": id, "secs": secs, "two": two });
    vec![json!({ "jobs": [job("a", 5, 1), job("b", 1, 1), job("c", 1, 5)] })]
}

/// Run to the end from `journal`, or stop after `kill` events. Gives the
/// events and the journal kept.
async fn run(vine: &Pipe, mut journal: Journal, kill: Option<usize>) -> (Vec<Event>, Journal) {
    let mut run = vine.run_saved(stream::iter(input()), journal.clone());
    let mut events = Vec::new();
    while let Some(event) = run.next().await {
        let event = event.unwrap();
        journal.record(&event);
        events.push(event);
        if Some(events.len()) == kill {
            break;
        }
    }
    (events, journal)
}

#[tokio::test(start_paused = true)]
async fn the_yaml_and_rust_timeouts_save_and_replay_the_same_at_every_kill() {
    let mut clean = Vec::new();
    for i in 0..2 {
        let vine = both(&Desk::new())[i].clone();
        clean.push(run(&vine, Journal::new(), None).await.0);
    }
    assert_eq!(clean[0], clean[1]);
    let decided = clean[0]
        .iter()
        .filter(|e| matches!(e, Event::Save(vine_lang::Entry::Timed { .. })))
        .count();
    assert_eq!(decided, 3);
    for k in 1..=clean[0].len() {
        let mut after = Vec::new();
        for i in 0..2 {
            let desk = Desk::new();
            let (first, journal) = run(&both(&desk)[i], Journal::new(), Some(k)).await;
            let (rest, _) = run(&both(&desk)[i], journal, None).await;
            after.push((first, rest, desk.calls()));
        }
        assert_eq!(after[0], after[1], "kill at {k}");
    }
}
