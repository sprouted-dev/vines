//! The four workflow vines written as pipes in YAML, and the proof that a
//! pipe built in Rust and the same pipe loaded from YAML are one vine.
#![cfg(feature = "yaml")]

mod acts;

use std::sync::Arc;

use acts::{job_launch_pipe, ready_job, registry, World};
use futures::{stream, StreamExt};
use vine_lang::pipe::{call, emit, emit_with, pipe, split, Pipe};
use vine_lang::{json, LoadError, RunError, Shape, Ty, Value};

fn text(name: &str) -> String {
    let path = format!("{}/vines/pipe/{name}.yaml", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn load(world: &Arc<World>, name: &str) -> Pipe {
    registry(world)
        .load_pipe(&text(name))
        .unwrap_or_else(|e| panic!("{name} loads: {e}"))
}

/// Feed one item and expect exactly one output.
async fn one(vine: &Pipe, input: Value) -> (String, Value) {
    let mut out = vine.run_one(input).await.unwrap();
    assert_eq!(out.len(), 1, "expected one output, got {out:?}");
    let o = out.remove(0);
    (o.name, o.payload)
}

// ------------------------------------------------------------ job_launch

#[tokio::test]
async fn job_launch_starts_the_agent_and_does_not_wait_for_it() {
    let world = World::new();
    let vine = load(&world, "job_launch");
    let (name, payload) = one(&vine, ready_job()).await;
    assert_eq!(name, "started");
    assert_eq!(
        payload,
        json!({ "job": "job-1", "session": "session-1", "tree": "/trees/job-1" })
    );
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
async fn job_launch_emits_failed_when_the_branch_cannot_be_refreshed() {
    let world = World::new();
    world.fail("git.refresh_branch", "conflict", "merge conflict");
    let (name, payload) = one(&load(&world, "job_launch"), ready_job()).await;
    assert_eq!(name, "failed");
    assert_eq!(
        payload,
        json!({ "job": "job-1", "reason": "merge conflict" })
    );
    assert!(world.log().is_empty());
}

#[tokio::test]
async fn job_launch_emits_failed_when_the_working_copy_cannot_be_made() {
    let world = World::new();
    world.fail("git.worktree_add", "exists", "already checked out");
    let (name, payload) = one(&load(&world, "job_launch"), ready_job()).await;
    assert_eq!(name, "failed");
    assert_eq!(
        payload,
        json!({ "job": "job-1", "reason": "already checked out" })
    );
    // Nothing was made, so nothing is removed.
    assert_eq!(world.log(), ["git.refresh_branch plan/p-1"]);
}

#[tokio::test]
async fn job_launch_removes_the_working_copy_when_the_agent_never_starts() {
    let world = World::new();
    world.fail("agent.start", "unavailable", "no agent host");
    let (name, payload) = one(&load(&world, "job_launch"), ready_job()).await;
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
async fn job_launch_is_a_run_error_when_the_cleanup_fails_too() {
    let world = World::new();
    world.fail("agent.start", "unavailable", "no agent host");
    world.fail("git.worktree_remove", "locked", "working copy is locked");
    let err = load(&world, "job_launch")
        .run_one(ready_job())
        .await
        .unwrap_err();
    assert!(
        matches!(err, RunError::Unhandled { ref operator, .. } if operator == "git.worktree_remove"),
        "{err}"
    );
}

#[tokio::test]
async fn job_launch_given_a_stream_of_jobs_emits_one_answer_for_each() {
    let world = World::new();
    world.fail("git.worktree_add", "exists", "already checked out");
    let vine = load(&world, "job_launch");
    let jobs = ["job-1", "job-2", "job-3"]
        .map(|j| json!({ "job": j, "plan_branch": "plan/p-1", "brief": "work" }));
    let names: Vec<String> = vine
        .run(stream::iter(jobs))
        .map(|r| r.unwrap().name)
        .collect()
        .await;
    // The first job fails. The two after it are not affected.
    assert_eq!(names, ["failed", "started", "started"]);
}

// ----------------------------------------------------- job_session_ended

fn ended(status: &str) -> Value {
    json!({ "job": "job-1", "tree": "/trees/job-1", "status": status })
}

fn with_tree() -> Arc<World> {
    let world = World::new();
    world.add_tree("/trees/job-1");
    world
}

#[tokio::test]
async fn a_finished_session_is_committed_and_its_working_copy_removed() {
    let world = with_tree();
    let (name, payload) = one(&load(&world, "job_session_ended"), ended("finished")).await;
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
async fn a_session_that_did_not_finish_is_stopped_with_no_commit() {
    for status in ["failed", "cancelled", "timed_out"] {
        let world = with_tree();
        let (name, payload) = one(&load(&world, "job_session_ended"), ended(status)).await;
        assert_eq!(name, "stopped");
        assert_eq!(payload, json!({ "job": "job-1", "reason": status }));
        assert!(world.trees().is_empty());
        assert_eq!(world.log(), ["git.worktree_remove /trees/job-1"]);
    }
}

#[tokio::test]
async fn a_commit_that_fails_emits_failed_and_still_removes_the_working_copy() {
    let world = with_tree();
    world.fail("git.commit", "empty", "nothing to commit");
    let (name, payload) = one(&load(&world, "job_session_ended"), ended("finished")).await;
    assert_eq!(name, "failed");
    assert_eq!(
        payload,
        json!({ "job": "job-1", "reason": "nothing to commit" })
    );
    assert!(world.trees().is_empty());
}

#[tokio::test]
async fn a_removal_that_fails_emits_failed_and_is_not_tried_again() {
    for status in ["finished", "cancelled"] {
        let world = with_tree();
        world.fail("git.worktree_remove", "locked", "working copy is locked");
        let (name, payload) = one(&load(&world, "job_session_ended"), ended(status)).await;
        assert_eq!(name, "failed");
        assert_eq!(
            payload,
            json!({ "job": "job-1", "reason": "working copy is locked" })
        );
        assert_eq!(world.trees(), ["/trees/job-1"]);
        assert!(!world
            .log()
            .iter()
            .any(|l| l.starts_with("git.worktree_remove")));
    }
}

// --------------------------------------------------------- plan_activate

#[tokio::test]
async fn plan_activate_cuts_the_branch_and_makes_the_working_copy() {
    let world = World::new();
    let input = json!({ "plan": "p-1", "base": "origin/main" });
    let (name, payload) = one(&load(&world, "plan_activate"), input).await;
    assert_eq!(name, "activated");
    assert_eq!(
        payload,
        json!({ "plan": "p-1", "branch": "plan/p-1", "lane": "/trees/p-1" })
    );
    assert_eq!(world.log(), ["git.branch_cut p-1", "git.worktree_add p-1"]);
}

#[tokio::test]
async fn plan_activate_emits_failed_when_either_git_act_fails() {
    for act in ["git.branch_cut", "git.worktree_add"] {
        let world = World::new();
        world.fail(act, "broken", "it broke");
        let input = json!({ "plan": "p-1", "base": "origin/main" });
        let (name, payload) = one(&load(&world, "plan_activate"), input).await;
        assert_eq!(name, "failed", "{act}");
        assert_eq!(payload, json!({ "plan": "p-1", "reason": "it broke" }));
    }
}

// ----------------------------------------------------- plan_launch_ready

fn job(id: &str, state: &str, waiting_on: u64, asked: bool) -> Value {
    json!({ "id": id, "state": state, "waiting_on": waiting_on, "asked": asked })
}

#[tokio::test]
async fn plan_launch_ready_emits_one_launch_wanted_for_each_job_that_can_start() {
    let world = World::new();
    let vine = load(&world, "plan_launch_ready");
    let jobs = json!([
        job("j-1", "ready", 0, false),
        job("j-2", "ready", 1, false), // still waiting on a dependency
        job("j-3", "ready", 0, true),  // already asked for
        job("j-4", "done", 0, false),  // not ready
        job("j-5", "ready", 0, false),
    ]);
    let out = vine
        .run_one(json!({ "plan": "p-1", "jobs": jobs }))
        .await
        .unwrap();
    let got: Vec<(&str, &Value)> = out.iter().map(|o| (o.name.as_str(), &o.payload)).collect();
    assert_eq!(
        got,
        [
            ("launch_wanted", &json!({ "plan": "p-1", "job": "j-1" })),
            ("launch_wanted", &json!({ "plan": "p-1", "job": "j-5" })),
        ]
    );
    assert!(vine.reach().is_empty());
}

#[tokio::test]
async fn plan_launch_ready_emits_nothing_when_nothing_can_start() {
    let world = World::new();
    let vine = load(&world, "plan_launch_ready");
    for jobs in [json!([]), json!([job("j-2", "ready", 1, false)])] {
        let out = vine
            .run_one(json!({ "plan": "p-1", "jobs": jobs }))
            .await
            .unwrap();
        assert!(out.is_empty());
    }
}

#[tokio::test]
async fn what_one_vine_emits_can_feed_the_next() {
    // `plan_launch_ready` names the jobs. Whoever runs the vines turns
    // each `launch_wanted` into the input of `job_launch`. Neither vine
    // knows about the other.
    let world = World::new();
    let ready = load(&world, "plan_launch_ready");
    let launch = load(&world, "job_launch");
    let jobs = json!([job("j-1", "ready", 0, false), job("j-2", "ready", 0, false)]);
    let wanted = ready
        .run(stream::iter([json!({ "plan": "p-1", "jobs": jobs })]))
        .map(|r| r.unwrap().payload)
        .map(|w| json!({ "job": w["job"], "plan_branch": "plan/p-1", "brief": "work" }));
    let started: Vec<String> = launch.run(wanted).map(|r| r.unwrap().name).collect().await;
    assert_eq!(started, ["started", "started"]);
    assert_eq!(world.trees(), ["/trees/j-1", "/trees/j-2"]);
}

// ------------------------------------------- one vine, two ways to write it

#[tokio::test]
async fn job_launch_is_the_same_vine_in_rust_and_in_yaml() {
    let inputs = [
        ready_job(),
        json!({ "job": "job-1" }),
        json!({ "job": "", "plan_branch": "b", "brief": "x" }),
    ];
    let failures = [
        None,
        Some("git.refresh_branch"),
        Some("git.worktree_add"),
        Some("agent.start"),
    ];
    for input in &inputs {
        for failing in failures {
            let (rust_world, yaml_world) = (World::new(), World::new());
            let from_rust = registry(&rust_world).build_pipe(job_launch_pipe()).unwrap();
            let from_yaml = load(&yaml_world, "job_launch");
            assert_eq!(from_rust.name(), from_yaml.name());
            assert_eq!(from_rust.input(), from_yaml.input());
            assert_eq!(from_rust.outputs(), from_yaml.outputs());
            assert_eq!(from_rust.reach(), from_yaml.reach());
            for world in [&rust_world, &yaml_world] {
                if let Some(act) = failing {
                    world.fail(act, "broken", "it broke");
                }
            }
            let a = from_rust.run_one(input.clone()).await;
            let b = from_yaml.run_one(input.clone()).await;
            assert_eq!(a, b, "input {input}, failing {failing:?}");
            assert_eq!(rust_world.log(), yaml_world.log());
            assert_eq!(rust_world.trees(), yaml_world.trees());
        }
    }
}

#[test]
fn the_same_mistake_is_refused_with_the_same_error() {
    let world = World::new();
    let reg = registry(&world);
    let head = "vine: v\ninput: { jobs: list }\noutputs: { out: { job: any } }\npipe:\n";
    let def = || {
        pipe("v")
            .input(Shape::new().required("jobs", Ty::List))
            .output("out", Shape::new().required("job", Ty::Any))
    };

    let rust = def().stages([emit("done")]);
    let yaml = format!("{head}  - emit: done\n");
    assert_eq!(
        reg.build_pipe(rust).unwrap_err(),
        reg.load_pipe(&yaml).unwrap_err()
    );

    let rust = def().stages([call("git.push", json!({})), emit("out")]);
    let yaml = format!("{head}  - call: git.push\n  - emit: out\n");
    assert_eq!(
        reg.build_pipe(rust).unwrap_err(),
        reg.load_pipe(&yaml).unwrap_err()
    );

    let rust = def().stages([
        split("$item.nope"),
        emit_with("out", json!({ "job": "$item" })),
    ]);
    let yaml =
        format!("{head}  - split: $item.nope\n  - emit: {{ to: out, with: {{ job: $item }} }}\n");
    assert_eq!(
        reg.build_pipe(rust).unwrap_err(),
        reg.load_pipe(&yaml).unwrap_err()
    );

    let rust = def().stages([split("$item.jobs").into("job")]);
    let yaml = format!("{head}  - split: $item.jobs\n    into: job\n");
    assert_eq!(
        reg.build_pipe(rust).unwrap_err(),
        reg.load_pipe(&yaml).unwrap_err()
    );
}

// --------------------------------------------------------- the YAML layer

fn bad(pipe: &str) -> LoadError {
    let world = World::new();
    let doc =
        format!("vine: v\ninput: {{ n: int }}\noutputs: {{ out: {{ n: int }} }}\npipe:\n{pipe}");
    registry(&world).load_pipe(&doc).unwrap_err()
}

#[test]
fn a_stage_does_one_thing() {
    let err = bad("  - split: $item.n\n    filter: $item.n == 1\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0]"),
        "{err}"
    );
    let err = bad("  - into: x\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0]"),
        "{err}"
    );
}

#[test]
fn with_and_into_go_only_where_they_mean_something() {
    let err = bad("  - filter: $item.n == 1\n    into: x\n  - emit: out\n");
    assert_eq!(
        err,
        LoadError::UnknownKey {
            path: "pipe[0]".into(),
            key: "into".into()
        }
    );
    let err = bad("  - take: 1\n    with: { n: 1 }\n  - emit: out\n");
    assert_eq!(
        err,
        LoadError::UnknownKey {
            path: "pipe[0]".into(),
            key: "with".into()
        }
    );
    let err = bad(
        "  - tap: git.refresh_branch\n    with: { branch: main }\n    into: x\n  - emit: out\n",
    );
    assert_eq!(
        err,
        LoadError::UnknownKey {
            path: "pipe[0]".into(),
            key: "into".into()
        }
    );
}

#[test]
fn each_reads_its_overlap_rule_and_refuses_one_it_does_not_know() {
    let err = bad("  - each: { overlap: sideways, do: [ { emit: out } ] }\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].each.overlap"),
        "{err}"
    );
    let err = bad("  - each: { overlap: merge }\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].each"),
        "{err}"
    );
    let err = bad("  - each: { do: [ { emit: out } ], limit: 2 }\n");
    assert_eq!(
        err,
        LoadError::UnknownKey {
            path: "pipe[0].each".into(),
            key: "limit".into()
        }
    );
    for overlap in ["concat", "merge", "switch", "exhaust"] {
        let world = World::new();
        let doc = format!(
            "vine: v\ninput: {{ n: int }}\noutputs: {{ out: {{ n: int }} }}\npipe:\n  - each: {{ overlap: {overlap}, do: [ {{ emit: out }} ] }}\n"
        );
        registry(&world).load_pipe(&doc).unwrap();
    }
}

#[test]
fn attempt_needs_both_pipes_and_branch_needs_its_cases() {
    let err = bad("  - attempt: { do: [ { emit: out } ] }\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].attempt"),
        "{err}"
    );
    let err = bad("  - branch: { on: $item.n }\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].branch"),
        "{err}"
    );
}

#[test]
fn a_document_with_a_key_the_pipe_form_does_not_have_is_refused() {
    let world = World::new();
    let doc = "vine: v\nmode: once\ninput: {}\noutputs: { out: {} }\npipe:\n  - emit: out\n";
    assert_eq!(
        registry(&world).load_pipe(doc).unwrap_err(),
        LoadError::UnknownKey {
            path: "vine".into(),
            key: "mode".into()
        }
    );
    let no_pipe = "vine: v\ninput: {}\noutputs: { out: {} }\n";
    assert!(matches!(
        registry(&world).load_pipe(no_pipe).unwrap_err(),
        LoadError::Malformed { .. }
    ));
}
