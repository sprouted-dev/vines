//! The four workflow vines written as pipes in YAML, and the proof that a
//! pipe built in Rust and the same pipe loaded from YAML are one vine.
#![cfg(feature = "yaml")]

mod fake;

use std::sync::Arc;

use fake::{job_launch_pipe, ready_job, registry, World};
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
async fn a_removal_that_fails_is_a_run_error_after_the_output_and_is_tried_once() {
    for (status, output) in [("finished", "implemented"), ("cancelled", "stopped")] {
        let world = with_tree();
        world.fail("git.worktree_remove", "locked", "working copy is locked");
        let vine = load(&world, "job_session_ended");
        let results: Vec<_> = vine.run(stream::iter([ended(status)])).collect().await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap().name, output);
        assert!(
            matches!(results[1], Err(RunError::Unhandled { ref operator, .. })
                if operator == "git.worktree_remove"),
            "{:?}",
            results[1]
        );
        assert_eq!(world.trees(), ["/trees/job-1"]);
    }
}

#[tokio::test]
async fn a_stream_of_ended_sessions_cleans_up_after_each_one() {
    let world = World::new();
    let vine = load(&world, "job_session_ended");
    let sessions: Vec<Value> = [
        ("job-1", "finished"),
        ("job-2", "failed"),
        ("job-3", "finished"),
    ]
    .iter()
    .map(|(job, status)| {
        let tree = format!("/trees/{job}");
        world.add_tree(&tree);
        json!({ "job": job, "tree": tree, "status": status })
    })
    .collect();
    let names: Vec<String> = vine
        .run(stream::iter(sessions))
        .map(|r| r.unwrap().name)
        .collect()
        .await;
    assert_eq!(names, ["implemented", "stopped", "implemented"]);
    assert!(world.trees().is_empty());
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
async fn plan_activate_emits_failed_when_either_git_operator_fails() {
    for operator in ["git.branch_cut", "git.worktree_add"] {
        let world = World::new();
        world.fail(operator, "broken", "it broke");
        let input = json!({ "plan": "p-1", "base": "origin/main" });
        let (name, payload) = one(&load(&world, "plan_activate"), input).await;
        assert_eq!(name, "failed", "{operator}");
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
                if let Some(operator) = failing {
                    world.fail(operator, "broken", "it broke");
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

    let rust = def().operators([emit("done")]);
    let yaml = format!("{head}  - emit: done\n");
    assert_eq!(
        reg.build_pipe(rust).unwrap_err(),
        reg.load_pipe(&yaml).unwrap_err()
    );

    let rust = def().operators([call("git.push", json!({})), emit("out")]);
    let yaml = format!("{head}  - call: git.push\n  - emit: out\n");
    assert_eq!(
        reg.build_pipe(rust).unwrap_err(),
        reg.load_pipe(&yaml).unwrap_err()
    );

    let rust = def().operators([
        split("$item.nope"),
        emit_with("out", json!({ "job": "$item" })),
    ]);
    let yaml =
        format!("{head}  - split: $item.nope\n  - emit: {{ to: out, with: {{ job: $item }} }}\n");
    assert_eq!(
        reg.build_pipe(rust).unwrap_err(),
        reg.load_pipe(&yaml).unwrap_err()
    );

    let rust = def().operators([split("$item.jobs").into("job")]);
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
fn a_operator_does_one_thing() {
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
fn the_four_map_stages_need_do_and_refuse_keys_they_do_not_take() {
    const MAPS: [&str; 4] = ["concatMap", "mergeMap", "switchMap", "exhaustMap"];
    for key in MAPS {
        let err = bad(&format!("  - {key}: {{}}\n"));
        assert!(
            matches!(err, LoadError::Malformed { ref path, .. } if *path == format!("pipe[0].{key}")),
            "{key}: {err}"
        );
        let err = bad(&format!(
            "  - {key}: {{ do: [ {{ emit: out }} ], limit: 2 }}\n"
        ));
        assert_eq!(
            err,
            LoadError::UnknownKey {
                path: format!("pipe[0].{key}"),
                key: "limit".into()
            }
        );
    }
    for key in MAPS {
        let world = World::new();
        let doc = format!(
            "vine: v\ninput: {{ n: int }}\noutputs: {{ out: {{ n: int }} }}\npipe:\n  - {key}: {{ do: [ {{ emit: out }} ] }}\n"
        );
        registry(&world).load_pipe(&doc).unwrap();
    }
}

#[test]
fn concurrent_is_taken_by_merge_map_and_refused_on_the_other_three() {
    let world = World::new();
    let doc = "vine: v\ninput: { n: int }\noutputs: { out: { n: int } }\npipe:\n  - mergeMap: { concurrent: 2, do: [ { emit: out } ] }\n";
    registry(&world).load_pipe(doc).unwrap();
    for key in ["concatMap", "switchMap", "exhaustMap"] {
        let err = bad(&format!(
            "  - {key}: {{ concurrent: 2, do: [ {{ emit: out }} ] }}\n"
        ));
        assert!(
            matches!(err, LoadError::Malformed { ref path, ref problem }
                if *path == format!("pipe[0].{key}") && problem.contains("only `mergeMap`")),
            "{key}: {err}"
        );
    }
    let err = bad("  - mergeMap: { concurrent: two, do: [ { emit: out } ] }\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].mergeMap.concurrent"),
        "{err}"
    );
    let err = bad("  - mergeMap: { concurrent: 0, do: [ { emit: out } ] }\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].mergeMap"),
        "{err}"
    );
}

// --------------------------------------------- keys from before the RxJS names

fn renamed(path: &str, old: &str, new: &str) -> LoadError {
    LoadError::Renamed {
        path: path.into(),
        old: old.into(),
        new: new.into(),
    }
}

const MAP_KEYS: &str = "`concatMap`, `mergeMap`, `switchMap` or `exhaustMap`";

#[test]
fn each_is_refused_and_the_error_names_the_four_map_operators() {
    let err = bad("  - each: { overlap: merge, do: [ { emit: out } ] }\n");
    assert_eq!(err, renamed("pipe[0]", "each", MAP_KEYS));
}

#[test]
fn collect_is_refused_and_the_error_names_buffer_count() {
    let err = bad("  - collect: { size: 2 }\n  - emit: out\n");
    assert_eq!(err, renamed("pipe[0]", "collect", "`bufferCount`"));
}

#[test]
fn retry_refuses_times_and_the_error_names_count() {
    let err = bad("  - set: { n: 1 }\n  - retry: { times: 2 }\n  - emit: out\n");
    assert_eq!(err, renamed("pipe[1].retry", "times", "`count`"));
}

#[test]
fn retry_refuses_wait_and_the_error_names_delay() {
    let err = bad("  - set: { n: 1 }\n  - retry: { count: 2, wait: 5s }\n  - emit: out\n");
    assert_eq!(err, renamed("pipe[1].retry", "wait", "`delay`"));
}

#[test]
fn buffer_count_refuses_size_and_the_error_names_buffer_size() {
    let err = bad("  - bufferCount: { size: 2 }\n  - emit: out\n");
    assert_eq!(err, renamed("pipe[0].bufferCount", "size", "`bufferSize`"));
}

#[test]
fn together_is_refused_and_the_error_names_fork_join() {
    let err = bad("  - together: [ { call: git.commit, into: c } ]\n  - emit: out\n");
    assert_eq!(err, renamed("pipe[0]", "together", "`forkJoin`"));
}

#[test]
fn catch_is_refused_and_the_error_names_catch_error() {
    let err = bad("  - emit: out\n  - catch: [ { emit: out } ]\n");
    assert_eq!(err, renamed("pipe[1]", "catch", "`catchError`"));
}

#[test]
fn overlap_is_refused_on_every_map_operator_and_the_error_names_the_operator_keys() {
    for key in ["concatMap", "mergeMap", "switchMap", "exhaustMap"] {
        let err = bad(&format!(
            "  - {key}: {{ overlap: merge, do: [ {{ emit: out }} ] }}\n"
        ));
        assert_eq!(
            err,
            renamed(
                &format!("pipe[0].{key}"),
                "overlap",
                &format!("{MAP_KEYS} as the operator key")
            )
        );
    }
}

#[test]
fn at_once_is_refused_and_the_error_names_concurrent() {
    let err = bad("  - mergeMap: { at_once: 2, do: [ { emit: out } ] }\n");
    assert_eq!(err, renamed("pipe[0].mergeMap", "at_once", "`concurrent`"));
}

#[test]
fn an_old_key_is_refused_where_it_stands_inside_another_operator() {
    let err =
        bad("  - fork:\n      - - collect: { size: 2 }\n        - emit: out\n  - emit: out\n");
    assert_eq!(
        err,
        renamed("pipe[0].fork[0][0]", "collect", "`bufferCount`")
    );
    let err =
        bad("  - concatMap:\n      do:\n        - emit: out\n        - catch: [ { emit: out } ]\n");
    assert_eq!(
        err,
        renamed("pipe[0].concatMap.do[1]", "catch", "`catchError`")
    );
}

#[test]
fn the_refusal_of_an_old_key_says_which_key_to_use() {
    let err = bad("  - collect: { size: 2 }\n  - emit: out\n");
    assert_eq!(
        err.to_string(),
        "pipe[0]: `collect` is not a key any more; use `bufferCount`"
    );
}

#[test]
fn branch_needs_its_cases_and_catch_error_and_finalize_take_a_list() {
    let err = bad("  - branch: { on: $item.n }\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].branch"),
        "{err}"
    );
    let err = bad("  - emit: out\n  - catchError: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[1].catchError"),
        "{err}"
    );
    let err = bad("  - emit: out\n  - finalize: { tap: git.commit }\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[1].finalize"),
        "{err}"
    );
}

#[test]
fn retry_and_timeout_read_their_times() {
    let err = bad("  - set: { n: 1 }\n  - retry: { delay: 5s }\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[1].retry"),
        "{err}"
    );
    let err = bad("  - set: { n: 1 }\n  - retry: { count: 2, delay: soon }\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[1].retry.delay"),
        "{err}"
    );
    let err = bad("  - set: { n: 1 }\n  - retry: { count: 2, pause: 5s }\n  - emit: out\n");
    assert_eq!(
        err,
        LoadError::UnknownKey {
            path: "pipe[1].retry".into(),
            key: "pause".into()
        }
    );
    let err = bad("  - set: { n: 1 }\n  - timeout: 30\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[1].timeout"),
        "{err}"
    );
    let world = World::new();
    let doc = "vine: v\ninput: { n: int }\noutputs: { out: { n: int } }\npipe:\n  - set: { n: $item.n }\n  - retry: { count: 2, delay: 5s }\n  - timeout: 30s\n  - emit: out\n";
    registry(&world).load_pipe(doc).unwrap();
}

#[test]
fn on_goes_only_with_use_and_is_a_map_of_outputs() {
    let err = bad("  - emit: out\n    on: { out: [] }\n");
    assert_eq!(
        err,
        LoadError::UnknownKey {
            path: "pipe[0]".into(),
            key: "on".into()
        }
    );
    let err = bad("  - use: job_launch\n    on: started\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].on"),
        "{err}"
    );
    let err = bad("  - use: nobody\n    on: { done: [ { emit: out } ] }\n");
    assert_eq!(
        err,
        LoadError::UnknownVine {
            path: "pipe[0]".into(),
            name: "nobody".into()
        }
    );
}

// ------------------------------------------------- one vine runs another

/// A registry that holds the helper `job_teardown`, and `job_launch_undo`
/// built on it.
fn undo(world: &Arc<World>) -> Pipe {
    let mut reg = registry(world);
    let teardown = reg.load_pipe(&text("job_teardown")).unwrap();
    reg.add_vine(teardown).unwrap();
    reg.load_pipe(&text("job_launch_undo")).unwrap()
}

fn abandoned(output: &str) -> Value {
    json!({
        "job": "j-1",
        "output": output,
        "work": { "job": "j-1", "session": "session-9", "tree": "/trees/j-1" },
    })
}

#[tokio::test]
async fn job_launch_undo_runs_its_helper_to_stop_the_agent_and_remove_the_tree() {
    let world = World::new();
    world.add_tree("/trees/j-1");
    let vine = undo(&world);
    let out = one(&vine, abandoned("started")).await;
    assert_eq!(
        out,
        (
            "undone".into(),
            json!({ "job": "j-1", "tree": "/trees/j-1" })
        )
    );
    assert_eq!(
        world.log(),
        ["agent.stop session-9", "git.worktree_remove /trees/j-1"]
    );
    assert!(world.trees().is_empty());
    // It reaches what `job_teardown` reaches.
    assert_eq!(
        vine.reach().iter().map(String::as_str).collect::<Vec<_>>(),
        ["agent", "git"]
    );
}

#[tokio::test]
async fn job_launch_undo_reports_a_helper_that_failed_and_keeps_the_tree() {
    let world = World::new();
    world.add_tree("/trees/j-1");
    world.fail("agent.stop", "unavailable", "no agent host");
    let out = one(&undo(&world), abandoned("started")).await;
    assert_eq!(
        out,
        (
            "failed".into(),
            json!({ "job": "j-1", "reason": "no agent host" })
        )
    );
    assert_eq!(world.trees(), ["/trees/j-1"]);
}

#[tokio::test]
async fn job_launch_undo_has_nothing_to_undo_for_an_output_that_made_nothing() {
    let world = World::new();
    let out = one(&undo(&world), abandoned("failed")).await;
    assert_eq!(
        out,
        (
            "nothing_to_undo".into(),
            json!({ "job": "j-1", "output": "failed" })
        )
    );
    assert!(world.log().is_empty());
}

#[test]
fn job_launch_undo_cannot_load_before_the_vine_it_uses() {
    let world = World::new();
    let err = registry(&world)
        .load_pipe(&text("job_launch_undo"))
        .unwrap_err();
    assert!(
        matches!(err, LoadError::UnknownVine { ref name, .. } if name == "job_teardown"),
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
