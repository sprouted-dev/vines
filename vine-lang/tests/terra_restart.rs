//! A nested press survives a crash. Terra runs `plan_launch_all` with
//! `run_saved`, and the vine presses `launch` for each job. A crash can
//! fall before or after Terra keeps anything a run gives, at any depth.
//! After it, pressing the outer request again finishes the work: no job
//! is pressed twice, no event is lost or repeated, and no second working
//! copy or agent is made.
#![cfg(feature = "yaml")]

mod acts;
mod terra;

use std::sync::Arc;

use acts::{registry, World};
use terra::moves::jobs_trellis;
use terra::{Answer, Event, Refusal, Terra};
use vine_lang::{json, Value};

const JOBS: [(&str, &str); 3] = [("j-1", "ready"), ("j-2", "done"), ("j-3", "ready")];

fn setup() -> (Arc<World>, Terra) {
    let world = World::new();
    let terra = Terra::new(registry(&world));
    let ids: Vec<&str> = JOBS.iter().map(|(id, _)| *id).collect();
    terra.put(
        "p-1",
        json!({ "state": "active", "branch": "plan/p-1", "jobs": ids }),
    );
    for (id, state) in JOBS {
        terra.put(
            id,
            json!({ "state": state, "plan": "p-1", "waiting_on": 0, "brief": "work" }),
        );
    }
    terra.install(jobs_trellis()).unwrap();
    (world, terra)
}

fn launch_all() -> Value {
    json!({ "plan": "p-1" })
}

/// What an uninterrupted run leaves, and how many crash points it has.
struct Clean {
    answer: Answer,
    events: Vec<Event>,
    log: Vec<String>,
    trees: Vec<String>,
    agents: Vec<String>,
    launches: Vec<String>,
    points: u64,
}

async fn clean() -> Clean {
    let (world, terra) = setup();
    let answer = terra
        .press_as("r-1", "launch_all", launch_all())
        .await
        .unwrap();
    Clean {
        answer,
        events: terra.events(),
        log: world.log(),
        trees: world.trees(),
        agents: world.agents(),
        launches: terra.requests_of("launch"),
        points: terra.ticks(),
    }
}

/// The world's log after a crash at a point where an act had run and
/// its result was not yet kept: the same log with that act's line twice.
fn with_one_act_again(log: &[String], got: &[String]) -> bool {
    got.len() == log.len() + 1
        && (0..got.len()).any(|i| {
            let mut without = got.to_vec();
            let line = without.remove(i);
            without == log && log.contains(&line)
        })
}

#[tokio::test]
async fn an_uninterrupted_run_has_crash_points_inside_the_nested_presses() {
    let clean = clean().await;
    assert!(clean.points > 20, "{} points", clean.points);
    assert_eq!(clean.launches.len(), 3);
}

#[tokio::test]
async fn a_crash_at_any_point_then_a_restart_ends_as_an_uninterrupted_run() {
    let clean = clean().await;
    let mut inner_points = 0;
    let mut acts_again = 0;
    for k in 1..=clean.points {
        let (world, terra) = setup();
        terra.crash_at(k);
        let first = terra.press_as("r-1", "launch_all", launch_all()).await;
        assert!(first.is_none(), "crash {k} did not stop the run");
        let point = terra.crash_point().unwrap();
        inner_points += usize::from(point.contains("/launch"));

        terra.restart();
        let answer = terra.press_as("r-1", "launch_all", launch_all()).await;
        assert_eq!(answer.as_ref(), Some(&clean.answer), "crash at {point}");
        assert_eq!(terra.events(), clean.events, "crash at {point}");
        assert_eq!(
            terra.requests_of("launch"),
            clean.launches,
            "crash at {point}"
        );
        assert_eq!(world.trees(), clean.trees, "crash at {point}");
        assert_eq!(world.agents(), clean.agents, "crash at {point}");
        // An act whose result was lost in the crash runs again. Each act
        // here is safe to run twice, so the world ends the same.
        if point.ends_with(" before done") {
            acts_again += 1;
            assert!(
                with_one_act_again(&clean.log, &world.log()),
                "crash at {point}: {:?}",
                world.log()
            );
        } else {
            assert_eq!(world.log(), clean.log, "crash at {point}");
        }
    }
    assert!(inner_points > 0 && acts_again > 0);
}

#[tokio::test]
async fn many_crashes_in_one_run_still_end_as_an_uninterrupted_run() {
    let clean = clean().await;
    for every in [2, 3, 5, 7] {
        let (world, terra) = setup();
        let (mut crashes, mut acts_again) = (0, 0);
        let answer = loop {
            terra.crash_at(every);
            if let Some(answer) = terra.press_as("r-1", "launch_all", launch_all()).await {
                break answer;
            }
            crashes += 1;
            let point = terra.crash_point().unwrap();
            acts_again += usize::from(point.ends_with(" before done"));
            assert!(crashes < 1000, "no progress crashing every {every}");
            terra.restart();
        };
        assert!(
            crashes > 5,
            "crashing every {every} crashed {crashes} times"
        );
        assert_eq!(answer, clean.answer, "every {every}");
        assert_eq!(terra.events(), clean.events, "every {every}");
        assert_eq!(terra.requests_of("launch"), clean.launches);
        assert_eq!(world.trees(), clean.trees);
        assert_eq!(world.agents(), clean.agents);
        // Each crash that lost an act's result ran that one act again.
        assert_eq!(world.log().len(), clean.log.len() + acts_again);
    }
}

#[tokio::test]
async fn pressing_a_request_that_ended_again_gives_the_same_answer_and_does_nothing() {
    let (world, terra) = setup();
    let first = terra.press_as("r-1", "launch_all", launch_all()).await;
    let (events, log) = (terra.events(), world.log());
    let again = terra.press_as("r-1", "launch_all", launch_all()).await;
    assert_eq!(again, first);
    assert_eq!(terra.events(), events);
    assert_eq!(world.log(), log);
}

#[tokio::test]
async fn a_refused_press_stays_refused_under_its_id_whatever_changed() {
    let (_world, terra) = setup();
    terra.set("p-1", "state", json!("paused"));
    let first = terra.press_as("r-1", "launch_all", launch_all()).await;
    terra.set("p-1", "state", json!("active"));
    let again = terra.press_as("r-1", "launch_all", launch_all()).await;
    assert_eq!(again, first);
    assert!(terra.events().is_empty());
}

#[tokio::test]
async fn a_request_id_used_again_for_another_payload_is_refused() {
    let (world, terra) = setup();
    terra.put(
        "p-2",
        json!({ "state": "active", "branch": "b", "jobs": [] }),
    );
    let first = terra.press_as("r-1", "launch_all", launch_all()).await;
    let log = world.log();
    let other = terra
        .press_as("r-1", "launch_all", json!({ "plan": "p-2" }))
        .await;
    assert_eq!(other, Some(Answer::Refused(Refusal::IdTaken)));
    assert_eq!(world.log(), log);
    assert_eq!(
        terra.press_as("r-1", "launch_all", launch_all()).await,
        first
    );
}

/// A move whose vine gives one output per line of a note, and an outer
/// move whose vine presses it and passes on each line. The inner press
/// gives several items, so a restart can resume it partway.
fn notes_trellis() -> terra::Trellis {
    use terra::{Lands, Move, Need, VineSource};
    use vine_lang::Ty;
    let inner = "vine: note_lines\ninput: { note: id, lines: list }\n\
        outputs: { line: { note: id, text: text } }\n\
        pipe:\n  - split: $item.lines\n    into: line\n  \
        - emit: { to: line, with: { note: $item.note, text: $item.line } }\n";
    let outer = "vine: note_relay\ninput: { note: id, request: text }\n\
        outputs: { relayed: { text: text }, finished: { status: text } }\n\
        pipe:\n  - call: move.read_note\n    with: { request: $item.request, note: $item.note }\n    \
        into: pressed\n    end: ended\n  \
        - fork:\n      - - filter: $item.pressed\n        - emit: { to: relayed, with: { text: $item.pressed.payload.text } }\n  \
        - filter: $item.ended\n  - emit: { to: finished, with: { status: $item.ended.status } }\n";
    let note_move = |name: &'static str, vine: &'static str, needs, events| Move {
        name,
        target: "note",
        payload: vec![("note", Ty::Id)],
        gates: vec![],
        needs,
        vine: Some(vine),
        events,
    };
    terra::Trellis {
        vines: vec![
            VineSource::Yaml(inner.into()),
            VineSource::Yaml(outer.into()),
        ],
        moves: vec![
            note_move(
                "read_note",
                "note_lines",
                vec![Need::fact("lines", Ty::List, "note.lines")],
                vec![Lands::new("line", "note.line")],
            ),
            note_move(
                "relay_note",
                "note_relay",
                vec![Need::request()],
                vec![
                    Lands::new("relayed", "note.relayed"),
                    Lands::new("finished", "note.finished"),
                ],
            ),
        ],
        reactions: vec![],
    }
}

fn notes() -> Terra {
    let terra = Terra::new(vine_lang::Registry::new());
    terra.put("n-1", json!({ "lines": ["a", "b", "c", "d"] }));
    terra.install(notes_trellis()).unwrap();
    terra
}

#[tokio::test]
async fn a_nested_press_that_gives_many_items_resumes_partway_without_repeats() {
    let relay = json!({ "note": "n-1" });
    let clean = notes();
    let want = clean.press_as("r-1", "relay_note", relay.clone()).await;
    let points = clean.ticks();
    for k in 1..=points {
        let terra = notes();
        terra.crash_at(k);
        assert!(terra
            .press_as("r-1", "relay_note", relay.clone())
            .await
            .is_none());
        let point = terra.crash_point().unwrap();
        terra.restart();
        let got = terra.press_as("r-1", "relay_note", relay.clone()).await;
        assert_eq!(got, want, "crash at {point}");
        assert_eq!(terra.events(), clean.events(), "crash at {point}");
    }
}
