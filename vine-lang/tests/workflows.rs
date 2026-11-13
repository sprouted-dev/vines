//! Real workflow steps written as vines: launching a job, picking up when
//! its agent session ends, activating a plan, and naming the jobs a plan
//! can launch. Each vine reads only its input and reaches outside only
//! through acts the test registers.
#![cfg(feature = "yaml")]

mod acts;

use acts::{ready_job, registry, World};
use vine_lang::{json, LoadError, Value, Vine};

fn load(world: &std::sync::Arc<World>, name: &str) -> Vine {
    let path = format!("{}/vines/core/{name}.yaml", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    registry(world)
        .load(&text)
        .unwrap_or_else(|e| panic!("{name} loads: {e}"))
}

async fn run(vine: &Vine, input: Value) -> (String, Value) {
    let out = vine.run(input).await.unwrap();
    (out.name, out.payload)
}

// ------------------------------------------------------------ job_launch

#[tokio::test]
async fn job_launch_starts_the_agent_and_ends_without_waiting_for_it() {
    let world = World::new();
    let vine = load(&world, "job_launch");
    let (name, payload) = run(&vine, ready_job()).await;
    assert_eq!(name, "started");
    assert_eq!(
        payload,
        json!({ "job": "job-1", "session": "session-1", "tree": "/trees/job-1" })
    );
    // The working copy is still there: the agent is using it.
    assert_eq!(world.trees(), ["/trees/job-1"]);
    assert_eq!(
        world.log(),
        [
            "git.refresh_branch plan/p-1",
            "git.worktree_add job-1",
            "agent.start /trees/job-1"
        ]
    );
}

#[tokio::test]
async fn job_launch_fails_cleanly_when_the_branch_cannot_be_refreshed() {
    let world = World::new();
    world.fail("git.refresh_branch", "conflict", "merge conflict");
    let vine = load(&world, "job_launch");
    let (name, payload) = run(&vine, ready_job()).await;
    assert_eq!(name, "failed");
    assert_eq!(
        payload,
        json!({ "job": "job-1", "reason": "merge conflict" })
    );
    assert!(world.trees().is_empty());
    assert!(world.log().is_empty());
}

#[tokio::test]
async fn job_launch_fails_cleanly_when_the_working_copy_cannot_be_made() {
    let world = World::new();
    world.fail("git.worktree_add", "exists", "already checked out");
    let vine = load(&world, "job_launch");
    let (name, payload) = run(&vine, ready_job()).await;
    assert_eq!(name, "failed");
    assert_eq!(
        payload,
        json!({ "job": "job-1", "reason": "already checked out" })
    );
    // Nothing was made, so nothing is removed and no agent starts.
    assert_eq!(world.log(), ["git.refresh_branch plan/p-1"]);
}

#[tokio::test]
async fn job_launch_removes_the_working_copy_when_the_agent_never_starts() {
    let world = World::new();
    world.fail("agent.start", "unavailable", "no agent host");
    let vine = load(&world, "job_launch");
    let (name, payload) = run(&vine, ready_job()).await;
    assert_eq!(name, "failed");
    assert_eq!(
        payload,
        json!({ "job": "job-1", "reason": "no agent host" })
    );
    assert!(world.trees().is_empty());
    assert_eq!(
        world.log(),
        [
            "git.refresh_branch plan/p-1",
            "git.worktree_add job-1",
            "git.worktree_remove /trees/job-1"
        ]
    );
}

#[tokio::test]
async fn job_launch_still_ends_with_an_output_when_the_cleanup_fails_too() {
    let world = World::new();
    world.fail("agent.start", "unavailable", "no agent host");
    world.fail("git.worktree_remove", "locked", "working copy is locked");
    let vine = load(&world, "job_launch");
    let (name, payload) = run(&vine, ready_job()).await;
    assert_eq!(name, "failed");
    // The reason is the cleanup's. The agent's failure is lost, because
    // the outer `on_error` sees only the newest failure.
    assert_eq!(
        payload,
        json!({ "job": "job-1", "reason": "working copy is locked" })
    );
    assert_eq!(world.trees(), ["/trees/job-1"]);
}

// ----------------------------------------------------- job_session_ended

fn ended(status: &str) -> Value {
    json!({ "job": "job-1", "tree": "/trees/job-1", "status": status })
}

#[tokio::test]
async fn a_finished_session_is_committed_and_its_working_copy_removed() {
    let world = World::new();
    world.add_tree("/trees/job-1");
    let vine = load(&world, "job_session_ended");
    let (name, payload) = run(&vine, ended("finished")).await;
    assert_eq!(name, "implemented");
    assert_eq!(payload, json!({ "job": "job-1", "commit": "abc123" }));
    assert!(world.trees().is_empty());
    assert_eq!(
        world.log(),
        [
            "git.commit /trees/job-1",
            "git.worktree_remove /trees/job-1"
        ]
    );
}

#[tokio::test]
async fn a_session_that_did_not_finish_is_stopped_with_its_status_and_no_commit() {
    for status in ["failed", "cancelled", "timed_out"] {
        let world = World::new();
        world.add_tree("/trees/job-1");
        let vine = load(&world, "job_session_ended");
        let (name, payload) = run(&vine, ended(status)).await;
        assert_eq!(name, "stopped");
        assert_eq!(payload, json!({ "job": "job-1", "reason": status }));
        assert!(world.trees().is_empty());
        assert_eq!(world.log(), ["git.worktree_remove /trees/job-1"]);
    }
}

#[tokio::test]
async fn a_commit_that_fails_is_output_failed_and_the_working_copy_is_still_removed() {
    let world = World::new();
    world.add_tree("/trees/job-1");
    world.fail("git.commit", "empty", "nothing to commit");
    let vine = load(&world, "job_session_ended");
    let (name, payload) = run(&vine, ended("finished")).await;
    assert_eq!(name, "failed");
    assert_eq!(
        payload,
        json!({ "job": "job-1", "reason": "nothing to commit" })
    );
    assert!(world.trees().is_empty());
}

#[tokio::test]
async fn a_cleanup_that_fails_is_a_run_error_that_names_what_it_masked() {
    let world = World::new();
    world.add_tree("/trees/job-1");
    world.fail("git.worktree_remove", "locked", "working copy is locked");
    let vine = load(&world, "job_session_ended");
    let err = vine.run(ended("finished")).await.unwrap_err().to_string();
    assert!(err.contains("git.worktree_remove"), "{err}");
    assert!(err.contains("output `implemented`"), "{err}");
}

// --------------------------------------------------------- plan_activate

#[tokio::test]
async fn plan_activate_cuts_the_branch_and_makes_the_plans_working_copy() {
    let world = World::new();
    let vine = load(&world, "plan_activate");
    let input = json!({ "plan": "p-1", "base": "origin/main" });
    let (name, payload) = run(&vine, input).await;
    assert_eq!(name, "activated");
    assert_eq!(
        payload,
        json!({ "plan": "p-1", "branch": "plan/p-1", "lane": "/trees/p-1" })
    );
    assert_eq!(world.log(), ["git.branch_cut p-1", "git.worktree_add p-1"]);
}

#[tokio::test]
async fn plan_activate_is_output_failed_when_either_git_act_fails() {
    for act in ["git.branch_cut", "git.worktree_add"] {
        let world = World::new();
        world.fail(act, "broken", "it broke");
        let vine = load(&world, "plan_activate");
        let input = json!({ "plan": "p-1", "base": "origin/main" });
        let (name, payload) = run(&vine, input).await;
        assert_eq!(name, "failed", "{act}");
        assert_eq!(payload, json!({ "plan": "p-1", "reason": "it broke" }));
    }
}

// ----------------------------------------------------- plan_launch_ready

fn job(id: &str, state: &str, waiting_on: u64, asked: bool) -> Value {
    json!({ "id": id, "state": state, "waiting_on": waiting_on, "asked": asked })
}

#[tokio::test]
async fn plan_launch_ready_names_the_jobs_that_can_start_now() {
    let world = World::new();
    let vine = load(&world, "plan_launch_ready");
    let jobs = json!([
        job("j-1", "ready", 0, false),
        job("j-2", "ready", 1, false), // still waiting on a dependency
        job("j-3", "ready", 0, true),  // already asked for
        job("j-4", "done", 0, false),  // not ready
        job("j-5", "ready", 0, false),
    ]);
    let input = json!({ "plan": "p-1", "jobs": jobs });
    let (name, payload) = run(&vine, input).await;
    assert_eq!(name, "ready");
    assert_eq!(
        payload,
        json!({ "plan": "p-1", "jobs": [job("j-1", "ready", 0, false), job("j-5", "ready", 0, false)] })
    );
    // It names them. It launches nothing and reaches nothing.
    assert!(vine.reach().is_empty());
    assert!(world.log().is_empty());
}

#[tokio::test]
async fn plan_launch_ready_says_so_when_nothing_can_start() {
    let world = World::new();
    let vine = load(&world, "plan_launch_ready");
    for jobs in [json!([]), json!([job("j-2", "ready", 1, false)])] {
        let input = json!({ "plan": "p-1", "jobs": jobs });
        let (name, payload) = run(&vine, input).await;
        assert_eq!(name, "nothing_ready");
        assert_eq!(payload, json!({ "plan": "p-1" }));
    }
}

// ------------------------------------------- `call` and `on_error`, YAML

fn refused(world: &std::sync::Arc<World>, steps: &str) -> LoadError {
    let text = format!(
        "vine: v\ninput: {{ dir: text }}\noutputs: {{ done: {{}}, failed: {{}} }}\nsteps:\n{steps}"
    );
    registry(world).load(&text).unwrap_err()
}

#[test]
fn with_belongs_to_a_call_step_and_to_no_other() {
    let world = World::new();
    let err = refused(
        &world,
        "  - set: { x: 1 }\n    with: { y: 2 }\n  - out: done\n",
    );
    assert_eq!(
        err,
        LoadError::UnknownKey {
            path: "steps[0]".into(),
            key: "with".into()
        }
    );
}

#[test]
fn call_names_one_act_as_text() {
    let world = World::new();
    let err = refused(&world, "  - call: { act: git.commit }\n  - out: done\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "steps[0].call"),
        "{err}"
    );
}

#[test]
fn a_call_cannot_refer_to_something_no_earlier_step_produced() {
    let world = World::new();
    let err = refused(
        &world,
        "  - call: git.worktree_remove\n    with: { path: $tree.path }\n  - out: done\n",
    );
    assert!(
        matches!(err, LoadError::UnboundReference { ref path, .. } if path == "steps[0].with"),
        "{err}"
    );
}

#[test]
fn on_error_must_be_a_block_of_steps() {
    let world = World::new();
    let err = refused(
        &world,
        "  - try:\n      do:\n        - out: done\n      on_error: failed\n",
    );
    assert!(matches!(err, LoadError::BadSettings { .. }), "{err}");
}

#[test]
fn error_is_not_in_scope_outside_on_error() {
    let world = World::new();
    let err = refused(
        &world,
        "  - call: git.worktree_remove\n    with: { path: $error.message }\n  - out: done\n",
    );
    assert!(
        matches!(err, LoadError::UnboundReference { ref name, .. } if name == "error"),
        "{err}"
    );
}
