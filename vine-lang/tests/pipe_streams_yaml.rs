//! `job_run` written in YAML is the same vine as `job_run` built in Rust,
//! and the YAML forms of `fork`, `last`, `collect` and `end`.
#![cfg(feature = "yaml")]

mod acts;

use std::sync::Arc;

use acts::agent::{job, job_run_pipe, job_run_registry, Host, Script};
use acts::World;
use futures::{stream, StreamExt};
use vine_lang::pipe::{call, catch, collect, emit, fork, last, pipe, tap, Pipe, PipeDef, Stage};
use vine_lang::{json, Entry, Event, Journal, LoadError, PipeOutput, RunError, Shape, Ty, Value};

fn text() -> String {
    let path = format!("{}/vines/pipe/job_run.yaml", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn from_yaml(world: &Arc<World>, host: &Arc<Host>) -> Pipe {
    job_run_registry(world, host)
        .load_pipe(&text())
        .unwrap_or_else(|e| panic!("job_run loads: {e}"))
}

fn from_rust(world: &Arc<World>, host: &Arc<Host>) -> Pipe {
    job_run_registry(world, host)
        .build_pipe(job_run_pipe())
        .unwrap()
}

/// Run as a caller would, saving every entry. Drop the run once the entry
/// for log line `drop_after` is saved.
async fn run(
    vine: &Pipe,
    saved: &mut Vec<Value>,
    drop_after: Option<usize>,
) -> Result<Vec<PipeOutput>, RunError> {
    let journal = Journal::from_json(&Value::Array(saved.clone())).unwrap();
    let mut outs: Vec<PipeOutput> = Vec::new();
    let mut run = vine.run_saved(stream::iter([job()]), journal);
    while let Some(event) = run.next().await {
        match event? {
            Event::Output(out) => outs.push(out),
            Event::Save(entry) => {
                let logged =
                    matches!(&entry, Entry::Emitted { output, .. } if output == "log_line");
                saved.push(entry.to_json());
                let lines = outs.iter().filter(|o| o.name == "log_line").count();
                if logged && Some(lines) == drop_after {
                    break;
                }
            }
        }
    }
    Ok(outs)
}

#[test]
fn job_run_has_the_same_input_outputs_and_reach_in_rust_and_in_yaml() {
    let (world, host) = (World::new(), Host::new());
    let (a, b) = (from_rust(&world, &host), from_yaml(&world, &host));
    assert_eq!(a.name(), b.name());
    assert_eq!(a.input(), b.input());
    assert_eq!(a.outputs(), b.outputs());
    assert_eq!(a.reach(), b.reach());
    assert!(a.resumable() && b.resumable());
}

/// How each world is set up before a run.
/// Something done to a world before a run.
type Setup = fn(&World, &Host);

fn setups() -> Vec<(&'static str, Setup)> {
    vec![
        ("finished", |_, _| {}),
        ("agent failed", |_, h| {
            h.script("job-1", Script::new(70, "failed"))
        }),
        ("agent cancelled", |_, h| {
            h.script("job-1", Script::new(70, "cancelled"))
        }),
        ("no lines", |_, h| {
            h.script("job-1", Script::new(0, "finished"))
        }),
        ("commit fails", |w, _| {
            w.fail("git.commit", "empty", "nothing to commit")
        }),
        ("working copy fails", |w, _| {
            w.fail("git.worktree_add", "exists", "already checked out")
        }),
        ("host lost", |_, h| {
            h.script(
                "job-1",
                Script {
                    lost_after: Some(70),
                    ..Script::new(120, "finished")
                },
            )
        }),
    ]
}

#[tokio::test(start_paused = true)]
async fn job_run_gives_the_same_outputs_and_effects_in_rust_and_in_yaml() {
    for (case, setup) in setups() {
        let (rust_world, rust_host) = (World::new(), Host::new());
        let (yaml_world, yaml_host) = (World::new(), Host::new());
        setup(&rust_world, &rust_host);
        setup(&yaml_world, &yaml_host);
        let a = from_rust(&rust_world, &rust_host).run_one(job()).await;
        let b = from_yaml(&yaml_world, &yaml_host).run_one(job()).await;
        assert!(a.as_ref().is_ok_and(|o| !o.is_empty()), "{case}: {a:?}");
        assert_eq!(a, b, "{case}");
        assert_eq!(rust_world.log(), yaml_world.log(), "{case}");
        assert_eq!(rust_world.trees(), yaml_world.trees(), "{case}");
    }
}

#[tokio::test(start_paused = true)]
async fn job_run_resumes_the_same_way_in_rust_and_in_yaml() {
    let mut results = Vec::new();
    for build in [from_rust, from_yaml] {
        let (world, host) = (World::new(), Host::new());
        let mut saved = Vec::new();
        let mut outs = run(&build(&world, &host), &mut saved, Some(70))
            .await
            .unwrap();
        outs.extend(run(&build(&world, &host), &mut saved, None).await.unwrap());
        results.push((outs, world.log(), world.trees(), saved));
    }
    let (rust, yaml) = (&results[0], &results[1]);
    assert_eq!(rust.0, yaml.0);
    assert_eq!(rust.1, yaml.1);
    assert_eq!(rust.2, yaml.2);
    // They even save the same journal.
    assert_eq!(rust.3, yaml.3);
    assert_eq!(rust.0.len(), 120 + 3 + 1);
}

#[tokio::test(start_paused = true)]
async fn a_journal_saved_by_the_rust_vine_resumes_in_the_yaml_vine() {
    let (world, host) = (World::new(), Host::new());
    let mut saved = Vec::new();
    let mut outs = run(&from_rust(&world, &host), &mut saved, Some(70))
        .await
        .unwrap();
    outs.extend(
        run(&from_yaml(&world, &host), &mut saved, None)
            .await
            .unwrap(),
    );
    let (straight_world, straight_host) = (World::new(), Host::new());
    let straight = from_rust(&straight_world, &straight_host)
        .run_one(job())
        .await
        .unwrap();
    assert_eq!(outs, straight);
    assert_eq!(world.count("agent.start"), 1);
    assert_eq!(world.count("git.worktree_add"), 1);
}

// ---------------------------------------------------- the same mistakes

fn head() -> &'static str {
    "vine: v\ninput: { n: int }\noutputs: { out: { n: int }, failed: {} }\npipe:\n"
}

fn def(stages: impl IntoIterator<Item = Stage>) -> PipeDef {
    pipe("v")
        .input(Shape::new().required("n", Ty::Int))
        .output("out", Shape::new().required("n", Ty::Int))
        .output("failed", Shape::new())
        .stages(stages)
}

#[test]
fn the_same_mistake_with_a_new_stage_is_refused_with_the_same_error() {
    let (world, host) = (World::new(), Host::new());
    let reg = job_run_registry(&world, &host);
    let agent = || json!({ "key": "k", "dir": "/d", "brief": "b" });
    let cases: Vec<(Vec<Stage>, String)> = vec![
        (
            vec![fork(Vec::<Vec<Stage>>::new()), emit("out")],
            "  - fork: []\n  - emit: out\n".into(),
        ),
        (
            vec![collect(0), emit("out")],
            "  - collect: { size: 0 }\n  - emit: out\n".into(),
        ),
        (
            vec![last(), emit("out"), catch([emit("out")])],
            "  - last\n  - emit: out\n  - catch: [ { emit: out } ]\n".into(),
        ),
        (
            vec![fork([vec![emit("nowhere")]]), emit("out")],
            "  - fork: [ [ { emit: nowhere } ] ]\n  - emit: out\n".into(),
        ),
        (
            vec![tap("agent.run", agent()), emit("out")],
            "  - tap: agent.run\n    with: { key: k, dir: /d, brief: b }\n  - emit: out\n".into(),
        ),
        (
            vec![
                call("git.commit", json!({ "dir": "/d", "message": "m" })).end("done"),
                emit("out"),
            ],
            "  - call: git.commit\n    with: { dir: /d, message: m }\n    end: done\n  - emit: out\n"
                .into(),
        ),
        (
            vec![call("agent.run", json!({ "key": 1 })), emit("out")],
            "  - call: agent.run\n    with: { key: 1 }\n  - emit: out\n".into(),
        ),
    ];
    for (stages, yaml) in cases {
        let from_rust = reg.build_pipe(def(stages)).unwrap_err();
        let from_yaml = reg.load_pipe(&format!("{}{yaml}", head())).unwrap_err();
        assert_eq!(from_rust, from_yaml, "{yaml}");
    }
}

// --------------------------------------------------------- the YAML layer

fn bad(pipe: &str) -> LoadError {
    let (world, host) = (World::new(), Host::new());
    job_run_registry(&world, &host)
        .load_pipe(&format!("{}{pipe}", head()))
        .unwrap_err()
}

#[test]
fn last_is_one_word_and_takes_nothing() {
    let err = bad("  - last: 3\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].last"),
        "{err}"
    );
    let err = bad("  - first\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, ref problem }
            if path == "pipe[0]" && problem.contains("`last`")),
        "{err}"
    );
    let (world, host) = (World::new(), Host::new());
    let reg = job_run_registry(&world, &host);
    reg.load_pipe(&format!("{}  - last\n  - emit: out\n", head()))
        .unwrap();
    reg.load_pipe(&format!("{}  - last:\n  - emit: out\n", head()))
        .unwrap();
}

#[test]
fn collect_takes_a_size_and_an_into() {
    let err = bad("  - collect: 5\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].collect"),
        "{err}"
    );
    let err = bad("  - collect: {}\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].collect"),
        "{err}"
    );
    let err = bad("  - collect: { size: 2, every: 5s }\n  - emit: out\n");
    assert_eq!(
        err,
        LoadError::UnknownKey {
            path: "pipe[0].collect".into(),
            key: "every".into()
        }
    );
    let err = bad("  - collect: { size: 2 }\n    with: {}\n  - emit: out\n");
    assert_eq!(
        err,
        LoadError::UnknownKey {
            path: "pipe[0]".into(),
            key: "with".into()
        }
    );
}

#[test]
fn fork_is_a_list_of_pipes() {
    let err = bad("  - fork: { a: [] }\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].fork"),
        "{err}"
    );
    let err = bad("  - fork: [ { emit: out } ]\n  - emit: out\n");
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].fork[0]"),
        "{err}"
    );
    let err = bad("  - fork: [ [ { emit: out } ] ]\n    into: x\n  - emit: out\n");
    assert_eq!(
        err,
        LoadError::UnknownKey {
            path: "pipe[0]".into(),
            key: "into".into()
        }
    );
}

#[test]
fn end_goes_only_with_call_and_is_a_name() {
    let err = bad("  - emit: out\n    end: done\n");
    assert_eq!(
        err,
        LoadError::UnknownKey {
            path: "pipe[0]".into(),
            key: "end".into()
        }
    );
    let err = bad(
        "  - call: agent.run\n    with: { key: k, dir: /d, brief: b }\n    end: [x]\n  - emit: out\n",
    );
    assert!(
        matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].end"),
        "{err}"
    );
}
