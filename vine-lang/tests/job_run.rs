//! `job_run`: one vine that makes a working copy, runs an agent, passes on
//! every line, commits, and cleans up. And what happens when the run is
//! dropped partway, as a daemon restart would drop it, and started again
//! from what the caller saved. Rust alone: this file passes with YAML off.

mod acts;

use std::sync::Arc;
use std::time::Duration;

use acts::agent::{expected, job, job_run_pipe, job_run_registry, Host, Script};
use acts::World;
use futures::{stream, StreamExt};
use vine_lang::{json, Entry, Event, Journal, Pipe, PipeOutput, RunError, Value};

fn vine(world: &Arc<World>, host: &Arc<Host>) -> Pipe {
    job_run_registry(world, host)
        .build_pipe(job_run_pipe())
        .unwrap()
}

/// When the caller drops the run.
#[derive(Clone, Copy)]
enum Drop {
    Never,
    /// Right after saving the entry that records this many log lines.
    AfterSaving(usize),
    /// Right after being handed this many log lines, before saving the
    /// entry that records the last one.
    BeforeSaving(usize),
}

fn log_lines(outs: &[PipeOutput]) -> usize {
    outs.iter().filter(|o| o.name == "log_line").count()
}

/// Run `job_run` as a caller would: hand on each output, save each entry.
/// Gives whether the run ended by itself.
async fn run(
    vine: &Pipe,
    saved: &mut Vec<Value>,
    outs: &mut Vec<PipeOutput>,
    drop: Drop,
) -> Result<bool, RunError> {
    // Read the journal back from plain JSON, as a store would give it.
    let journal = Journal::from_json(&Value::Array(saved.clone())).unwrap();
    let mut run = vine.run_saved(stream::iter([job()]), journal);
    while let Some(event) = run.next().await {
        match event? {
            Event::Output(out) => {
                let is_log = out.name == "log_line";
                outs.push(out);
                if let Drop::BeforeSaving(k) = drop {
                    if is_log && log_lines(outs) == k {
                        return Ok(false);
                    }
                }
            }
            Event::Save(entry) => {
                let logged =
                    matches!(&entry, Entry::Emitted { output, .. } if output == "log_line");
                saved.push(entry.to_json());
                if let Drop::AfterSaving(k) = drop {
                    if logged && log_lines(outs) == k {
                        return Ok(false);
                    }
                }
            }
        }
    }
    Ok(true)
}

fn pairs(outs: &[PipeOutput]) -> Vec<(String, Value)> {
    outs.iter()
        .map(|o| (o.name.clone(), o.payload.clone()))
        .collect()
}

const DONE: [&str; 4] = [
    "git.worktree_add job-1",
    "agent.start job-1",
    "git.commit /trees/job-1",
    "git.worktree_remove /trees/job-1",
];

// --------------------------------------------------------- uninterrupted

#[tokio::test(start_paused = true)]
async fn job_run_makes_one_working_copy_passes_on_every_line_and_commits_once() {
    let (world, host) = (World::new(), Host::new());
    let vine = vine(&world, &host);
    assert_eq!(
        vine.reach().iter().map(String::as_str).collect::<Vec<_>>(),
        ["agent", "git"]
    );
    let (mut saved, mut outs) = (Vec::new(), Vec::new());
    assert!(run(&vine, &mut saved, &mut outs, Drop::Never)
        .await
        .unwrap());
    assert_eq!(pairs(&outs), expected(120, "finished"));
    assert_eq!(world.log(), DONE);
    assert!(world.trees().is_empty());
}

#[tokio::test(start_paused = true)]
async fn job_run_gives_the_same_outputs_when_run_without_saving() {
    let (world, host) = (World::new(), Host::new());
    let outs = vine(&world, &host).run_one(job()).await.unwrap();
    assert_eq!(pairs(&outs), expected(120, "finished"));
    assert_eq!(world.log(), DONE);
}

#[tokio::test(start_paused = true)]
async fn job_run_emits_stopped_and_does_not_commit_when_the_agent_fails_or_is_cancelled() {
    for status in ["failed", "cancelled"] {
        let (world, host) = (World::new(), Host::new());
        host.script("job-1", Script::new(70, status));
        let outs = vine(&world, &host).run_one(job()).await.unwrap();
        assert_eq!(pairs(&outs), expected(70, status), "{status}");
        assert_eq!(
            world.log(),
            [
                "git.worktree_add job-1",
                "agent.start job-1",
                "git.worktree_remove /trees/job-1"
            ]
        );
        assert!(world.trees().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn job_run_emits_failed_and_removes_the_working_copy_when_the_commit_fails() {
    let (world, host) = (World::new(), Host::new());
    host.script("job-1", Script::new(3, "finished"));
    world.fail("git.commit", "empty", "nothing to commit");
    let outs = vine(&world, &host).run_one(job()).await.unwrap();
    let mut want = expected(3, "finished");
    want.pop();
    want.push((
        "failed".into(),
        json!({ "job": "job-1", "reason": "nothing to commit" }),
    ));
    assert_eq!(pairs(&outs), want);
    assert!(world.trees().is_empty());
    assert_eq!(world.count("git.worktree_remove"), 1);
}

#[tokio::test(start_paused = true)]
async fn job_run_emits_failed_and_never_starts_the_agent_when_the_working_copy_cannot_be_made() {
    let (world, host) = (World::new(), Host::new());
    world.fail("git.worktree_add", "exists", "already checked out");
    let outs = vine(&world, &host).run_one(job()).await.unwrap();
    assert_eq!(
        pairs(&outs),
        [(
            "failed".into(),
            json!({ "job": "job-1", "reason": "already checked out" })
        )]
    );
    assert!(host.session("job-1").is_none());
    // The cleanup still runs. Removing a working copy that is not there is
    // not a failure.
    assert_eq!(world.log(), ["git.worktree_remove /trees/job-1"]);
}

#[tokio::test(start_paused = true)]
async fn job_run_with_an_agent_that_writes_nothing_still_commits() {
    let (world, host) = (World::new(), Host::new());
    host.script("job-1", Script::new(0, "finished"));
    let outs = vine(&world, &host).run_one(job()).await.unwrap();
    assert_eq!(
        pairs(&outs),
        [(
            "implemented".into(),
            json!({ "job": "job-1", "commit": "abc123" })
        )]
    );
    assert_eq!(world.log(), DONE);
}

#[tokio::test(start_paused = true)]
async fn job_run_with_a_round_hundred_lines_gives_two_summaries_and_no_empty_one() {
    let (world, host) = (World::new(), Host::new());
    host.script("job-1", Script::new(100, "finished"));
    let outs = vine(&world, &host).run_one(job()).await.unwrap();
    assert_eq!(pairs(&outs), expected(100, "finished"));
    assert_eq!(outs.iter().filter(|o| o.name == "summary").count(), 2);
}

#[tokio::test(start_paused = true)]
async fn job_run_emits_failed_when_the_agent_host_is_lost_partway() {
    let (world, host) = (World::new(), Host::new());
    host.script(
        "job-1",
        Script {
            lost_after: Some(70),
            ..Script::new(120, "finished")
        },
    );
    let outs = vine(&world, &host).run_one(job()).await.unwrap();
    // Lines 1 to 70 and the summary at 50 came out before the failure.
    // The half full batch is dropped with the rest of the item's work.
    let mut want: Vec<(String, Value)> = expected(70, "finished")
        .into_iter()
        .filter(|(name, v)| name == "log_line" || v["lines"] == 50)
        .collect();
    want.push((
        "failed".into(),
        json!({ "job": "job-1", "reason": "lost the agent host" }),
    ));
    assert_eq!(pairs(&outs), want);
    assert_eq!(world.count("git.commit"), 0);
    assert!(world.trees().is_empty());
}

// --------------------------------------------------------------- restart

#[tokio::test(start_paused = true)]
async fn a_restart_mid_batch_makes_no_second_working_copy_and_attaches_to_the_same_agent() {
    let (world, host) = (World::new(), Host::new());
    let (mut saved, mut outs) = (Vec::new(), Vec::new());

    let first = vine(&world, &host);
    let ended = run(&first, &mut saved, &mut outs, Drop::AfterSaving(70))
        .await
        .unwrap();
    assert!(!ended);
    drop(first);
    assert_eq!(log_lines(&outs), 70);
    // The lines came out as the agent wrote them: it is still running.
    assert_eq!(host.session("job-1").unwrap().status, None);

    // A new vine, as after a restart, with what the caller saved.
    let second = vine(&world, &host);
    assert!(run(&second, &mut saved, &mut outs, Drop::Never)
        .await
        .unwrap());

    // No line lost, none twice, and the summaries are the ones an
    // uninterrupted run gives: the batch of 51 to 100 spans the restart.
    assert_eq!(pairs(&outs), expected(120, "finished"));
    assert_eq!(
        world.log(),
        [
            "git.worktree_add job-1",
            "agent.start job-1",
            "agent.attach job-1 from 70",
            "git.commit /trees/job-1",
            "git.worktree_remove /trees/job-1",
        ]
    );
    assert!(world.trees().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_restart_after_any_line_gives_exactly_what_an_uninterrupted_run_gives() {
    for k in [1, 49, 50, 51, 70, 119, 120] {
        let (world, host) = (World::new(), Host::new());
        let (mut saved, mut outs) = (Vec::new(), Vec::new());
        let ended = run(
            &vine(&world, &host),
            &mut saved,
            &mut outs,
            Drop::AfterSaving(k),
        )
        .await
        .unwrap();
        assert!(!ended, "{k}");
        assert!(
            run(&vine(&world, &host), &mut saved, &mut outs, Drop::Never)
                .await
                .unwrap()
        );
        assert_eq!(pairs(&outs), expected(120, "finished"), "dropped after {k}");
        assert_eq!(world.count("git.worktree_add"), 1, "{k}");
        assert_eq!(world.count("agent.start"), 1, "{k}");
        assert_eq!(world.count("agent.attach"), 1, "{k}");
        assert_eq!(world.count("git.commit"), 1, "{k}");
        assert_eq!(world.count("git.worktree_remove"), 1, "{k}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_line_handed_out_whose_save_did_not_happen_comes_again_once() {
    let (world, host) = (World::new(), Host::new());
    let (mut saved, mut outs) = (Vec::new(), Vec::new());
    run(
        &vine(&world, &host),
        &mut saved,
        &mut outs,
        Drop::BeforeSaving(70),
    )
    .await
    .unwrap();
    run(&vine(&world, &host), &mut saved, &mut outs, Drop::Never)
        .await
        .unwrap();
    let seventy: Vec<&PipeOutput> = outs
        .iter()
        .filter(|o| o.name == "log_line" && o.payload["seq"] == 70)
        .collect();
    assert_eq!(seventy.len(), 2);
    // Take the repeat out and the rest is an uninterrupted run.
    let at = outs
        .iter()
        .rposition(|o| o.name == "log_line" && o.payload["seq"] == 70)
        .unwrap();
    outs.remove(at);
    assert_eq!(pairs(&outs), expected(120, "finished"));
}

#[tokio::test(start_paused = true)]
async fn a_restart_after_the_agent_finished_unattended_reads_the_rest_and_completes() {
    let (world, host) = (World::new(), Host::new());
    let (mut saved, mut outs) = (Vec::new(), Vec::new());
    run(
        &vine(&world, &host),
        &mut saved,
        &mut outs,
        Drop::AfterSaving(30),
    )
    .await
    .unwrap();
    // Nothing is attached while the agent writes its other 90 lines.
    tokio::time::sleep(Duration::from_secs(600)).await;
    let session = host.session("job-1").unwrap();
    assert_eq!(session.status.as_deref(), Some("finished"));
    assert_eq!(session.lines.len(), 120);

    run(&vine(&world, &host), &mut saved, &mut outs, Drop::Never)
        .await
        .unwrap();
    assert_eq!(pairs(&outs), expected(120, "finished"));
    assert_eq!(
        world.log(),
        [
            "git.worktree_add job-1",
            "agent.start job-1",
            "agent.attach job-1 from 30",
            "git.commit /trees/job-1",
            "git.worktree_remove /trees/job-1",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_vine_restarted_twice_gives_what_an_uninterrupted_run_gives() {
    let (world, host) = (World::new(), Host::new());
    let (mut saved, mut outs) = (Vec::new(), Vec::new());
    for k in [30, 70] {
        let ended = run(
            &vine(&world, &host),
            &mut saved,
            &mut outs,
            Drop::AfterSaving(k),
        )
        .await
        .unwrap();
        assert!(!ended);
    }
    run(&vine(&world, &host), &mut saved, &mut outs, Drop::Never)
        .await
        .unwrap();
    assert_eq!(pairs(&outs), expected(120, "finished"));
    assert_eq!(
        world.log(),
        [
            "git.worktree_add job-1",
            "agent.start job-1",
            "agent.attach job-1 from 30",
            "agent.attach job-1 from 70",
            "git.commit /trees/job-1",
            "git.worktree_remove /trees/job-1",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_restart_with_nothing_saved_starts_again_and_attaches_to_the_running_agent() {
    let (world, host) = (World::new(), Host::new());
    let mut outs = Vec::new();
    // The first run hands out one line and is dropped before the caller
    // saved anything at all.
    let mut lost = Vec::new();
    run(
        &vine(&world, &host),
        &mut lost,
        &mut outs,
        Drop::BeforeSaving(1),
    )
    .await
    .unwrap();
    let mut saved = Vec::new();
    run(&vine(&world, &host), &mut saved, &mut outs, Drop::Never)
        .await
        .unwrap();
    // A fresh start: the line handed out before comes again, and the
    // working copy act runs again. It is safe to run twice, so there is
    // still one working copy. The agent is started once: the second run
    // attaches from the first line.
    assert_eq!(outs[0], outs[1]);
    assert_eq!(pairs(&outs[1..]), expected(120, "finished"));
    assert_eq!(
        world.log(),
        [
            "git.worktree_add job-1",
            "agent.start job-1",
            "git.worktree_add job-1",
            "agent.attach job-1 from 0",
            "git.commit /trees/job-1",
            "git.worktree_remove /trees/job-1",
        ]
    );
    assert!(world.trees().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_run_dropped_and_never_restarted_leaves_its_working_copy_and_its_agent_behind() {
    let (world, host) = (World::new(), Host::new());
    let (mut saved, mut outs) = (Vec::new(), Vec::new());
    run(
        &vine(&world, &host),
        &mut saved,
        &mut outs,
        Drop::AfterSaving(70),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(600)).await;
    // Dropping a run does not run `finalize`. The working copy is still
    // there, the session ran on to its end with nobody reading, and
    // nothing was committed.
    assert_eq!(world.trees(), ["/trees/job-1"]);
    let session = host.session("job-1").unwrap();
    assert_eq!(session.status.as_deref(), Some("finished"));
    assert_eq!(session.lines.len(), 120);
    assert_eq!(world.log(), ["git.worktree_add job-1", "agent.start job-1"]);
    // Only the journal knows what to do next. It is plain data the caller
    // still holds.
    assert!(!saved.is_empty());
}

#[tokio::test(start_paused = true)]
async fn the_journal_of_a_finished_run_replays_to_no_new_outputs_and_no_acts() {
    let (world, host) = (World::new(), Host::new());
    let (mut saved, mut outs) = (Vec::new(), Vec::new());
    run(&vine(&world, &host), &mut saved, &mut outs, Drop::Never)
        .await
        .unwrap();
    let log = world.log();
    let mut again = Vec::new();
    run(&vine(&world, &host), &mut saved, &mut again, Drop::Never)
        .await
        .unwrap();
    assert!(again.is_empty());
    assert_eq!(world.log(), log);
}
