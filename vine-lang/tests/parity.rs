//! One vine, two ways to write it. The same vine built in Rust and loaded
//! from YAML has the same shapes and reach, ends the same way for the same
//! input, does the same things outside, and is refused for the same
//! mistake with the same error.
#![cfg(feature = "yaml")]

mod acts;

use acts::{job_launch_def, job_session_ended_def, ready_job, registry, World};
use vine_lang::def::{call, once, out};
use vine_lang::{json, Shape, Ty, Value, Vine};

fn yaml(name: &str) -> String {
    let path = format!("{}/vines/core/{name}.yaml", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn same_description(a: &Vine, b: &Vine) {
    assert_eq!(a.name(), b.name());
    assert_eq!(a.mode(), b.mode());
    assert_eq!(a.input(), b.input());
    assert_eq!(a.outputs(), b.outputs());
    assert_eq!(a.reach(), b.reach());
}

#[tokio::test]
async fn job_session_ended_is_the_same_vine_in_rust_and_in_yaml() {
    let ended = |status: &str| json!({ "job": "job-1", "tree": "/trees/job-1", "status": status });
    let cases: Vec<(Value, Option<&str>)> = vec![
        (ended("finished"), None),
        (ended("failed"), None),
        (ended("cancelled"), None),
        (ended("finished"), Some("git.commit")),
        // Does not fit the input shape: both must refuse it the same way.
        (json!({ "job": "job-1" }), None),
        (
            json!({ "job": "", "tree": "/trees/job-1", "status": "finished" }),
            None,
        ),
    ];
    for (input, failing) in cases {
        let (rust_world, yaml_world) = (World::new(), World::new());
        let from_rust = registry(&rust_world)
            .build(job_session_ended_def())
            .unwrap();
        let from_yaml = registry(&yaml_world)
            .load(&yaml("job_session_ended"))
            .unwrap();
        same_description(&from_rust, &from_yaml);
        for world in [&rust_world, &yaml_world] {
            world.add_tree("/trees/job-1");
            if let Some(act) = failing {
                world.fail(act, "broken", "it broke");
            }
        }
        let a = from_rust.run(input.clone()).await;
        let b = from_yaml.run(input.clone()).await;
        assert_eq!(a, b, "input {input}, failing {failing:?}");
        assert_eq!(rust_world.log(), yaml_world.log(), "input {input}");
        assert_eq!(rust_world.trees(), yaml_world.trees(), "input {input}");
    }
}

#[tokio::test]
async fn a_cleanup_failure_is_the_same_run_error_in_rust_and_in_yaml() {
    let (rust_world, yaml_world) = (World::new(), World::new());
    let from_rust = registry(&rust_world)
        .build(job_session_ended_def())
        .unwrap();
    let from_yaml = registry(&yaml_world)
        .load(&yaml("job_session_ended"))
        .unwrap();
    for world in [&rust_world, &yaml_world] {
        world.fail("git.worktree_remove", "locked", "working copy is locked");
    }
    let input = json!({ "job": "job-1", "tree": "/trees/job-1", "status": "finished" });
    let a = from_rust.run(input.clone()).await.unwrap_err();
    let b = from_yaml.run(input).await.unwrap_err();
    assert_eq!(a, b);
}

/// Run `job_launch` once in a fresh world, with one act set to fail.
async fn launch(
    vine: &Vine,
    world: &World,
    input: Value,
) -> (String, Value, Vec<String>, Vec<String>) {
    let out = vine.run(input).await.unwrap();
    (out.name, out.payload, world.log(), world.trees())
}

#[tokio::test]
async fn job_launch_is_the_same_vine_in_rust_and_in_yaml() {
    let cases: Vec<(Value, Option<&str>)> = vec![
        (ready_job(), None),
        (ready_job(), Some("git.refresh_branch")),
        (ready_job(), Some("git.worktree_add")),
        (ready_job(), Some("agent.start")),
    ];
    for (input, failing) in cases {
        let (rust_world, yaml_world) = (World::new(), World::new());
        let from_rust = registry(&rust_world).build(job_launch_def()).unwrap();
        let from_yaml = registry(&yaml_world).load(&yaml("job_launch")).unwrap();
        same_description(&from_rust, &from_yaml);
        if let Some(act) = failing {
            rust_world.fail(act, "broken", "it broke");
            yaml_world.fail(act, "broken", "it broke");
        }
        let a = launch(&from_rust, &rust_world, input.clone()).await;
        let b = launch(&from_yaml, &yaml_world, input.clone()).await;
        assert_eq!(a, b, "input {input}, failing {failing:?}");
    }
}

#[test]
fn the_same_mistake_is_refused_with_the_same_error() {
    let world = World::new();
    let reg = registry(&world);

    // An output nobody declared.
    let rust = once("v")
        .input(Shape::new())
        .output("done", Shape::new())
        .steps([out("finished", json!({}))]);
    let text = "vine: v\ninput: {}\noutputs: { done: {} }\nsteps:\n  - out: { name: finished, with: {} }\n";
    assert_eq!(reg.build(rust).unwrap_err(), reg.load(text).unwrap_err());

    // An act nobody registered.
    let rust = once("v")
        .input(Shape::new())
        .output("done", Shape::new())
        .steps([call("git.push", json!({})), out("done", json!({}))]);
    let text = "vine: v\ninput: {}\noutputs: { done: {} }\nsteps:\n  - call: git.push\n  - out: { name: done, with: {} }\n";
    assert_eq!(reg.build(rust).unwrap_err(), reg.load(text).unwrap_err());

    // An act handed input that does not fit it.
    let rust = once("v")
        .input(Shape::new().required("dir", Ty::Text))
        .output("done", Shape::new())
        .steps([
            call("git.worktree_remove", json!({ "dir": "$in.dir" })),
            out("done", json!({})),
        ]);
    let text = "vine: v\ninput: { dir: text }\noutputs: { done: {} }\nsteps:\n  - call: git.worktree_remove\n    with: { dir: $in.dir }\n  - out: { name: done, with: {} }\n";
    assert_eq!(reg.build(rust).unwrap_err(), reg.load(text).unwrap_err());

    // A reference to something no earlier step produced.
    let rust = once("v")
        .input(Shape::new())
        .output("done", Shape::new().required("x", Ty::Text))
        .steps([out("done", json!({ "x": "$tree.path" }))]);
    let text = "vine: v\ninput: {}\noutputs: { done: { x: text } }\nsteps:\n  - out: { name: done, with: { x: $tree.path } }\n";
    assert_eq!(reg.build(rust).unwrap_err(), reg.load(text).unwrap_err());
}
