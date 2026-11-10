mod support;

use std::sync::Arc;
use std::time::Duration;

use support::{
    register_agent, register_facts, register_git, vine_source, Agent, Facts, Git, Script,
};
use vine_lang::{json, CancellationToken, Registry, RunError, Vine};

struct World {
    facts: Arc<Facts>,
    git: Arc<Git>,
    agent: Arc<Agent>,
    vine: Vine,
}

fn world(script: Script) -> World {
    let facts = Facts::new();
    let git = Git::new();
    let agent = Agent::new(script, &git);
    let mut reg = Registry::new();
    register_facts(&mut reg, &facts);
    register_git(&mut reg, &git);
    register_agent(&mut reg, &agent);
    let vine = reg.load(&vine_source("launch")).expect("launch loads");
    facts.put(
        "j-1",
        json!({ "state": "ready", "branch": "feat", "brief": "do it" }),
    );
    World {
        facts,
        git,
        agent,
        vine,
    }
}

fn job() -> vine_lang::Value {
    json!({ "job": "j-1" })
}

/// Cleanup ran exactly once, on the tree that was made, and nothing leaked.
fn assert_cleaned_up(w: &World) {
    assert_eq!(w.git.created(), 1, "one worktree was made");
    assert_eq!(w.git.released(), vec!["/trees/feat-1".to_string()]);
    assert!(w.git.live().is_empty(), "no worktree leaked");
}

#[tokio::test(start_paused = true)]
async fn launches_and_releases_the_worktree() {
    let w = world(Script::Complete);
    let out = w.vine.run(job()).await.unwrap();
    assert_eq!(out.name, "launched");
    assert_eq!(out.payload, json!({ "job": "j-1", "session": "sess-1" }));
    assert_eq!(w.agent.started(), vec!["/trees/feat-1".to_string()]);
    assert_cleaned_up(&w);
}

#[tokio::test(start_paused = true)]
async fn refuses_a_job_that_is_not_ready_and_makes_no_worktree() {
    let w = world(Script::Complete);
    w.facts.put(
        "j-1",
        json!({ "state": "running", "branch": "feat", "brief": "x" }),
    );
    let out = w.vine.run(job()).await.unwrap();
    assert_eq!(
        (out.name.as_str(), out.payload),
        ("refused", json!({ "gate": "state" }))
    );
    assert_eq!(w.git.created(), 0);
    assert!(w.agent.started().is_empty());
}

#[tokio::test(start_paused = true)]
async fn refuses_a_missing_job() {
    let w = world(Script::Complete);
    let out = w.vine.run(json!({ "job": "j-404" })).await.unwrap();
    assert_eq!(
        (out.name.as_str(), out.payload),
        ("refused", json!({ "gate": "missing" }))
    );
    assert_eq!(w.git.created(), 0);
}

#[tokio::test(start_paused = true)]
async fn an_agent_that_fails_is_output_failed_and_cleaned_up() {
    let w = world(Script::Fail("tests did not pass"));
    let out = w.vine.run(job()).await.unwrap();
    assert_eq!(out.name, "failed");
    assert_eq!(
        out.payload,
        json!({ "job": "j-1", "reason": "tests did not pass" })
    );
    assert_cleaned_up(&w);
}

#[tokio::test(start_paused = true)]
async fn an_agent_session_that_reports_cancelled_is_failed_and_cleaned_up() {
    let w = world(Script::ReportsCancelled);
    let out = w.vine.run(job()).await.unwrap();
    assert_eq!(
        (out.name.as_str(), out.payload),
        (
            "failed",
            json!({ "job": "j-1", "reason": "session cancelled" })
        )
    );
    assert_cleaned_up(&w);
}

#[tokio::test(start_paused = true)]
async fn an_agent_error_is_caught_as_failed_and_cleaned_up() {
    let w = world(Script::Error("model unavailable"));
    let out = w.vine.run(job()).await.unwrap();
    assert_eq!(
        (out.name.as_str(), out.payload),
        (
            "failed",
            json!({ "job": "j-1", "reason": "model unavailable" })
        )
    );
    assert_cleaned_up(&w);
}

#[tokio::test(start_paused = true)]
async fn caller_cancellation_mid_run_is_a_run_error_and_still_cleans_up() {
    let w = world(Script::Hang);
    let cancel = CancellationToken::new();
    let run = w.vine.run_with(job(), cancel.clone());
    let canceller = async {
        // Let the agent start and sit for a while, then cancel.
        tokio::time::sleep(Duration::from_secs(600)).await;
        assert_eq!(w.agent.started().len(), 1, "agent is running");
        assert_eq!(w.git.live().len(), 1, "worktree is live while it runs");
        cancel.cancel();
    };
    let (result, ()) = tokio::join!(run, canceller);
    assert_eq!(
        result.unwrap_err(),
        RunError::Cancelled {
            path: "steps[4].try.do[0]".into()
        }
    );
    assert_cleaned_up(&w);
}

#[tokio::test(start_paused = true)]
async fn cancellation_before_the_run_starts_touches_nothing() {
    let w = world(Script::Complete);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let err = w.vine.run_with(job(), cancel).await.unwrap_err();
    assert_eq!(
        err,
        RunError::Cancelled {
            path: "steps[0]".into()
        }
    );
    assert_eq!(w.git.created(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_worktree_that_cannot_be_made_is_a_run_error_with_nothing_to_release() {
    let w = world(Script::Complete);
    w.git.fail_create();
    let err = w.vine.run(job()).await.unwrap_err();
    assert_eq!(
        err,
        RunError::Unhandled {
            path: "steps[3]".into(),
            operator: "worktree".into(),
            code: "git".into(),
            message: "cannot create worktree: disk full".into(),
        }
    );
    assert!(w.git.released().is_empty());
    assert!(w.agent.started().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_cleanup_failure_after_success_is_a_run_error_naming_what_it_masked() {
    let w = world(Script::Complete);
    w.git.fail_release();
    let err = w.vine.run(job()).await.unwrap_err();
    assert_eq!(
        err,
        RunError::CleanupFailed {
            path: "steps[4].try.finally[0]".into(),
            operator: "worktree_release".into(),
            code: "git".into(),
            message: "cannot remove /trees/feat-1: busy".into(),
            masked: "output `launched`".into(),
        }
    );
    assert_eq!(
        w.git.live().len(),
        1,
        "the tree leaked, and the error says so"
    );
}

#[tokio::test(start_paused = true)]
async fn a_cleanup_failure_after_cancellation_names_the_cancellation() {
    let w = world(Script::Hang);
    w.git.fail_release();
    let cancel = CancellationToken::new();
    let run = w.vine.run_with(job(), cancel.clone());
    let canceller = async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        cancel.cancel();
    };
    let (result, ()) = tokio::join!(run, canceller);
    match result.unwrap_err() {
        RunError::CleanupFailed { masked, .. } => {
            assert_eq!(masked, "error `cancelled` from `agent_run`")
        }
        other => panic!("expected CleanupFailed, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn an_agent_result_that_breaks_its_shape_is_a_run_error_and_still_cleans_up() {
    let w = world(Script::Garbage);
    match w.vine.run(job()).await.unwrap_err() {
        RunError::ResultMisfit {
            path,
            operator,
            problem,
        } => {
            assert_eq!(path, "steps[4].try.do[0]");
            assert_eq!(operator, "agent_run");
            assert_eq!(problem, "field `status` should be text, found number 7");
        }
        other => panic!("expected ResultMisfit, got {other:?}"),
    }
    assert_cleaned_up(&w);
}

#[tokio::test(start_paused = true)]
async fn a_failed_output_without_a_reason_is_refused_at_run_time_and_cleaned_up() {
    // `reason` is optional in what agent_run returns but required in the
    // `failed` output, so only the run can tell.
    let w = world(Script::FailWithoutReason);
    assert_eq!(
        w.vine.run(job()).await.unwrap_err(),
        RunError::OutputMisfit {
            path: "steps[4].try.do[1].switch.else[0]".into(),
            output: "failed".into(),
            problem: "missing required field `reason` (text)".into(),
        }
    );
    assert_cleaned_up(&w);
}

#[tokio::test(start_paused = true)]
async fn a_read_error_is_a_run_error() {
    let w = world(Script::Complete);
    w.facts.fail_reads();
    assert!(matches!(
        w.vine.run(job()).await.unwrap_err(),
        RunError::Unhandled { ref operator, .. } if operator == "read"
    ));
    assert_eq!(w.git.created(), 0);
}

#[tokio::test(start_paused = true)]
async fn refuses_input_that_does_not_fit() {
    let w = world(Script::Complete);
    assert_eq!(
        w.vine.run(json!({ "job": 12 })).await.unwrap_err(),
        RunError::InputMisfit("field `job` should be id, found number 12".into())
    );
    assert_eq!(w.git.created(), 0);
}

#[tokio::test(start_paused = true)]
async fn reach_is_facts_read_git_and_agent() {
    let w = world(Script::Complete);
    let reach: Vec<&str> = w.vine.reach().iter().map(String::as_str).collect();
    assert_eq!(reach, ["agent", "facts.read", "git"]);
}
