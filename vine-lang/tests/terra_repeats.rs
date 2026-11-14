//! Terra drops an output a resumed run gives again by its output id, not
//! by its name and payload. A vine may rightly emit the same name and
//! payload twice: those are two events, before and after any crash. An
//! output given again after a restart is one event, whatever its payload.
//!
//! The vines are built in Rust, so this file passes with the YAML feature
//! off; the same vine as YAML is checked against it when the feature is on.
//! The operators take time on tokio's paused clock, so every run interleaves
//! the same way.

mod fake;
mod terra;

use fake::desk::{desk_registry, step, Desk};
use terra::{Answer, Event, Lands, Move, Terra, Trellis, VineSource};
use vine_lang::pipe::{
    concat_map, emit_with, fork, last, merge_map, pipe, split, Flatten, Operator, PipeDef,
};
use vine_lang::{json, Shape, Ty, Value};

/// Note the same thing twice for each job: after its first step, and
/// again after its second. Every output is `noted { note }`, the same name
/// and payload.
fn twice_work() -> Vec<Operator> {
    let noted = || emit_with("noted", json!({ "note": "$item.note" }));
    vec![
        step("one", json!("$item.job.secs")),
        fork([[noted()]]),
        step("two", json!(1)),
        noted(),
    ]
}

/// The jobs one after another, or side by side, two at once.
fn twice_vine(flatten: Flatten) -> PipeDef {
    let work = match flatten {
        Flatten::Merge => merge_map(twice_work()).concurrent(2),
        Flatten::Concat => concat_map(twice_work()),
        other => unreachable!("only concatMap and mergeMap resume, not {other:?}"),
    };
    pipe("note_twice")
        .input(
            Shape::new()
                .required("note", Ty::Id)
                .required("jobs", Ty::List),
        )
        .output("noted", Shape::new().required("note", Ty::Id))
        .operators([split("$item.jobs").into("job"), work])
}

/// The jobs side by side, and when all are done, `finished` with the id of
/// the job `last` picked. Which job that is can differ after a restart.
fn last_vine() -> PipeDef {
    pipe("note_last")
        .input(
            Shape::new()
                .required("note", Ty::Id)
                .required("jobs", Ty::List),
        )
        .output("finished", Shape::new().required("job", Ty::Text))
        .operators([
            split("$item.jobs").into("job"),
            merge_map([step("one", json!("$item.job.secs"))]).concurrent(2),
            last(),
            emit_with("finished", json!({ "job": "$item.job.id" })),
        ])
}

fn note_move(vine: &'static str, lands: Lands) -> Move {
    Move {
        name: "note",
        target: "note",
        payload: vec![("note", Ty::Id), ("jobs", Ty::List)],
        gates: vec![],
        needs: vec![],
        vine: Some(vine),
        events: vec![lands],
    }
}

fn setup(vine: VineSource, name: &'static str, lands: Lands) -> Terra {
    let desk = Desk::new();
    let terra = Terra::new(desk_registry(&desk));
    terra.put("n-1", json!({}));
    terra
        .install(Trellis {
            vines: vec![vine],
            moves: vec![note_move(name, lands)],
            reactions: vec![],
        })
        .unwrap();
    terra
}

fn twice(flatten: Flatten) -> Terra {
    setup(
        VineSource::Def(twice_vine(flatten)),
        "note_twice",
        Lands::new("noted", "note.noted"),
    )
}

/// Two jobs that take as long, so their notes come in pairs.
fn payload() -> Value {
    json!({ "note": "n-1", "jobs": [{ "id": "a", "secs": 2 }, { "id": "b", "secs": 2 }] })
}

fn noted(times: usize) -> Vec<Event> {
    let event = Event {
        artifact: "n-1".into(),
        kind: "note.noted".into(),
        payload: json!({ "note": "n-1" }),
    };
    vec![event; times]
}

/// Press under `r-1` with a crash at the `k`th point, then again after a
/// restart. Gives the answer and where the crash fell.
async fn crash_then_press(terra: &Terra, k: u64) -> (Option<Answer>, String) {
    terra.crash_at(k);
    let first = terra.press_as("r-1", "note", payload()).await;
    assert!(first.is_none(), "crash {k} did not stop the press");
    let point = terra.crash_point().unwrap();
    terra.restart();
    (terra.press_as("r-1", "note", payload()).await, point)
}

#[tokio::test(start_paused = true)]
async fn the_same_output_emitted_twice_lands_as_two_events() {
    for flatten in [Flatten::Concat, Flatten::Merge] {
        let terra = twice(flatten);
        terra.press_as("r-1", "note", payload()).await.unwrap();
        assert_eq!(terra.events(), noted(4), "{flatten:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_crash_between_two_equal_outputs_still_ends_with_both() {
    for flatten in [Flatten::Concat, Flatten::Merge] {
        let clean = twice(flatten);
        let want = clean.press_as("r-1", "note", payload()).await;
        let points = clean.ticks();
        assert!(points > 10, "{points} points");
        for k in 1..=points {
            let terra = twice(flatten);
            let (got, point) = crash_then_press(&terra, k).await;
            assert_eq!(terra.events(), noted(4), "{flatten:?}, crash at {point}");
            assert_eq!(got, want, "{flatten:?}, crash at {point}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn many_crashes_between_equal_outputs_still_end_with_each_once() {
    for every in [2, 3, 5] {
        let terra = twice(Flatten::Merge);
        let mut crashes = 0;
        loop {
            terra.crash_at(every);
            if terra.press_as("r-1", "note", payload()).await.is_some() {
                break;
            }
            crashes += 1;
            assert!(crashes < 1000, "no progress crashing every {every}");
            terra.restart();
        }
        assert!(
            crashes > 3,
            "crashing every {every} crashed {crashes} times"
        );
        assert_eq!(terra.events(), noted(4), "every {every}");
    }
}

#[tokio::test(start_paused = true)]
async fn an_output_given_again_with_another_payload_still_lands_once() {
    // `last` after a merge gives the job that finished last. After a
    // restart the jobs replay from what was kept and can finish in another
    // order, so `finished` can come again naming another job. It is the
    // same output, with the same id, and it lands once.
    let make = || {
        setup(
            VineSource::Def(last_vine()),
            "note_last",
            Lands::new("finished", "note.finished"),
        )
    };
    let jobs =
        json!({ "note": "n-1", "jobs": [{ "id": "a", "secs": 3 }, { "id": "b", "secs": 1 }] });
    let clean = make();
    clean.press_as("r-1", "note", jobs.clone()).await.unwrap();
    let mut landed = std::collections::BTreeMap::new();
    for k in 1..=clean.ticks() {
        let terra = make();
        terra.crash_at(k);
        assert!(terra.press_as("r-1", "note", jobs.clone()).await.is_none());
        let point = terra.crash_point().unwrap();
        terra.restart();
        terra.press_as("r-1", "note", jobs.clone()).await.unwrap();
        let events = terra.events();
        assert_eq!(events.len(), 1, "crash at {point}: {events:?}");
        landed.insert(point, events[0].payload["job"].clone());
    }
    // Without the crash, `b` finishes first and `a` last. A crash before
    // `finished` landed shows the replay picks `b`; a crash after it
    // landed gives it again naming `b`, and Terra keeps the `a` it landed.
    assert_eq!(clean.events()[0].payload, json!({ "job": "a" }));
    assert_eq!(landed["r-1 before output finished"], json!("b"));
    assert_eq!(landed["r-1 after output finished"], json!("a"));
}

#[cfg(feature = "yaml")]
#[tokio::test(start_paused = true)]
async fn the_same_vine_as_yaml_lands_the_same_events_at_every_crash() {
    let text = "vine: note_twice\n\
        input: { note: id, jobs: list }\n\
        outputs: { noted: { note: id } }\n\
        pipe:\n  - split: $item.jobs\n    into: job\n  \
        - mergeMap:\n      concurrent: 2\n      do:\n        \
        - call: work.step\n          with: { id: $item.job.id, step: one, secs: $item.job.secs }\n          into: one\n        \
        - fork:\n            - - emit: { to: noted, with: { note: $item.note } }\n        \
        - call: work.step\n          with: { id: $item.job.id, step: two, secs: 1 }\n          into: two\n        \
        - emit: { to: noted, with: { note: $item.note } }\n";
    let make = || {
        setup(
            VineSource::Yaml(text.into()),
            "note_twice",
            Lands::new("noted", "note.noted"),
        )
    };
    let rust = twice(Flatten::Merge);
    let want = rust.press_as("r-1", "note", payload()).await;
    let yaml = make();
    assert_eq!(yaml.press_as("r-1", "note", payload()).await, want);
    for k in 1..=rust.ticks() {
        let terra = make();
        let (got, point) = crash_then_press(&terra, k).await;
        assert_eq!(terra.events(), noted(4), "crash at {point}");
        assert_eq!(got, want, "crash at {point}");
    }
}
