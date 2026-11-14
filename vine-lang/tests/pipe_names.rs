//! What a checked vine names: the acts it runs and the vines it uses. A
//! caller with its own rules about vines reads these. Nothing here uses
//! YAML, and this file passes with the feature off:
//!
//!     cargo test --no-default-features --test pipe_names

mod acts;

use acts::{job_launch_pipe, registry, World};
use vine_lang::pipe::{
    branch, call, catch_error, emit, emit_with, finalize, fork, fork_join, op, pipe, tap, use_vine,
    PipeDef,
};
use vine_lang::{json, OperatorSpec, Registry, Shape, Ty};

fn names(set: &std::collections::BTreeSet<String>) -> Vec<&str> {
    set.iter().map(String::as_str).collect()
}

/// A registry with the fake acts, a `tidy` operator, and `job_launch`.
fn with_job_launch() -> Registry {
    let world = World::new();
    let mut reg = registry(&world);
    reg.register(
        OperatorSpec::new("tidy")
            .settings(Shape::open())
            .returns(Shape::new()),
        |_v, _cx| async move { Ok(json!({})) },
    )
    .unwrap();
    let launch = reg.build_pipe(job_launch_pipe()).unwrap();
    reg.add_vine(launch).unwrap();
    reg
}

/// A vine over `{ job }` that runs `job_launch` for it.
fn launcher(name: &str) -> PipeDef {
    pipe(name)
        .input(Shape::new().required("job", Ty::Id))
        .output("done", Shape::open())
        .stages([use_vine(
            "job_launch",
            Some(json!({ "job": "$item.job", "plan_branch": "plan/p-1", "brief": "b" })),
        )
        .on("started", [emit("done")])
        .on("failed", [emit("done")])])
}

#[test]
fn a_vine_lists_the_acts_of_every_stage_that_runs_one_and_no_operator() {
    let reg = with_job_launch();
    let def = pipe("spread")
        .input(Shape::new().required("dir", Ty::Text))
        .output("done", Shape::open())
        .output("failed", Shape::open())
        .stages([
            fork_join([
                call("git.commit", json!({ "dir": "$item.dir", "message": "m" })).into("c"),
                op("tidy", json!({})).into("t"),
            ]),
            fork([vec![tap("git.refresh_branch", json!({ "branch": "b" }))]]),
            branch(
                "$item.dir",
                [(
                    "/x",
                    vec![tap(
                        "agent.start",
                        json!({ "dir": "$item.dir", "brief": "b" }),
                    )],
                )],
                None,
            ),
            emit("done"),
            catch_error([emit_with("failed", json!({}))]),
            finalize([tap("git.worktree_remove", json!({ "path": "$item.dir" }))]),
        ]);
    let vine = reg.build_pipe(def).unwrap();
    assert_eq!(
        names(vine.acts()),
        [
            "agent.start",
            "git.commit",
            "git.refresh_branch",
            "git.worktree_remove"
        ]
    );
    assert!(vine.uses().is_empty());
}

#[test]
fn a_vine_lists_the_vines_it_uses_and_the_acts_they_run() {
    let reg = with_job_launch();
    let vine = reg.build_pipe(launcher("launch_one")).unwrap();
    assert_eq!(names(vine.uses()), ["job_launch"]);
    assert_eq!(
        names(vine.acts()),
        [
            "agent.start",
            "git.refresh_branch",
            "git.worktree_add",
            "git.worktree_remove"
        ]
    );
}

#[test]
fn a_vine_lists_the_vines_its_used_vines_use() {
    let mut reg = with_job_launch();
    let one = reg.build_pipe(launcher("launch_one")).unwrap();
    reg.add_vine(one).unwrap();
    let outer = pipe("launch_outer")
        .input(Shape::new().required("job", Ty::Id))
        .output("done", Shape::open())
        .stages([use_vine("launch_one", None).on("done", [emit("done")])]);
    let vine = reg.build_pipe(outer).unwrap();
    assert_eq!(names(vine.uses()), ["job_launch", "launch_one"]);
}

#[test]
fn a_vine_that_uses_a_vine_only_in_an_arm_still_lists_it() {
    let mut reg = with_job_launch();
    let one = reg.build_pipe(launcher("launch_one")).unwrap();
    reg.add_vine(one).unwrap();
    let def = pipe("maybe")
        .input(Shape::new().required("job", Ty::Id))
        .output("done", Shape::open())
        .stages([branch(
            "$item.job",
            [(
                "j-1",
                vec![use_vine("launch_one", None).on("done", [emit("done")])],
            )],
            Some(vec![emit("done")]),
        )]);
    let vine = reg.build_pipe(def).unwrap();
    assert_eq!(names(vine.uses()), ["job_launch", "launch_one"]);
}
