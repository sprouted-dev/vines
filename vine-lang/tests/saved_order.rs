//! A saved run hands out an operator's entry before it calls the next operator. So
//! a caller that saves each entry before it reads the next never loses
//! the result of an operator that a later operator already depends on: a crash can
//! lose at most the one operator whose entry it had not saved. Nothing here
//! uses YAML, and this file passes with the feature off:
//!
//!     cargo test --no-default-features --test saved_order
#![cfg(test)]

mod fake;

use fake::agent::{job, job_run_pipe, job_run_registry, Host};
use fake::{job_launch_pipe, ready_job, registry, World};
use futures::{stream, StreamExt};
use vine_lang::{Bookmark, Entry, Event, Journal, Progress};

#[tokio::test]
async fn a_saved_run_hands_out_each_acts_entry_before_it_calls_the_next_operator() {
    let world = World::new();
    let vine = registry(&world).build_pipe(job_launch_pipe()).unwrap();
    let mut run = vine.run_saved(stream::iter([ready_job()]), Journal::new());
    let mut calls_at_each_entry = Vec::new();
    while let Some(event) = run.next().await {
        if let Event::Save(Entry::Done { .. }) = event.unwrap() {
            calls_at_each_entry.push(world.log().len());
        }
    }
    assert_eq!(calls_at_each_entry, [1, 2, 3]);
}

#[tokio::test]
async fn a_run_from_a_position_hands_out_each_acts_entry_before_the_next_operator() {
    let world = World::new();
    let vine = registry(&world).build_pipe(job_launch_pipe()).unwrap();
    let mut run = vine.run_from(stream::iter([ready_job()]), Bookmark::new());
    let mut calls_at_each_entry = Vec::new();
    while let Some(progress) = run.next().await {
        if let Progress::Save(line) = progress.unwrap() {
            if matches!(line.entry, Entry::Done { .. }) {
                calls_at_each_entry.push(world.log().len());
            }
        }
    }
    assert_eq!(calls_at_each_entry, [1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn a_saved_run_hands_out_the_entry_before_it_starts_a_stream_operator() {
    let world = World::new();
    let host = Host::new();
    let vine = job_run_registry(&world, &host)
        .build_pipe(job_run_pipe())
        .unwrap();
    let mut run = vine.run_saved(stream::iter([job()]), Journal::new());
    let first = run.next().await.unwrap().unwrap();
    assert!(
        matches!(first, Event::Save(Entry::Done { .. })),
        "{first:?}"
    );
    // The working copy is made, and the agent is not started yet.
    assert_eq!(world.log(), ["git.worktree_add job-1"]);
    assert!(host.session("job-1").is_none());
}
