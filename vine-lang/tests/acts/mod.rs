//! Fake outside acts for the tests, and the example vines built in Rust.
//!
//! The library ships no acts. A caller registers the Rust behind each
//! name. Here the "outside" is a `World` in memory.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use vine_lang::def::{attempt, call, once, out, switch, VineDef};
use vine_lang::{json, OpError, OperatorSpec, Registry, Shape, Ty, Value};

/// What the fake acts did, and which act should fail next.
#[derive(Default)]
pub struct World {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    log: Vec<String>,
    trees: BTreeSet<String>,
    fail: BTreeMap<String, OpError>,
    hooks: BTreeMap<String, Hook>,
}

type Hook = Arc<dyn Fn() + Send + Sync>;

impl World {
    pub fn new() -> Arc<World> {
        Arc::new(World::default())
    }

    /// Make the named act fail the next time it is called.
    pub fn fail(&self, act: &str, code: &str, message: &str) {
        let mut s = self.state.lock().unwrap();
        s.fail.insert(act.into(), OpError::new(code, message));
    }

    /// Every act that ran, in order, such as `git.worktree_add job-1`.
    pub fn log(&self) -> Vec<String> {
        self.state.lock().unwrap().log.clone()
    }

    /// The working copies that exist now.
    pub fn trees(&self) -> Vec<String> {
        self.state.lock().unwrap().trees.iter().cloned().collect()
    }

    pub fn add_tree(&self, path: &str) {
        self.state.lock().unwrap().trees.insert(path.into());
    }

    /// Run `hook` each time the named act runs. A test uses this to change
    /// something else while a vine is partway through.
    pub fn when(&self, act: &str, hook: impl Fn() + Send + Sync + 'static) {
        let mut s = self.state.lock().unwrap();
        s.hooks.insert(act.into(), Arc::new(hook));
    }

    fn run(&self, act: &str, detail: &str) -> Result<(), OpError> {
        let hook = {
            let mut s = self.state.lock().unwrap();
            if let Some(e) = s.fail.remove(act) {
                return Err(e);
            }
            s.log.push(format!("{act} {detail}"));
            s.hooks.get(act).cloned()
        };
        if let Some(hook) = hook {
            hook();
        }
        Ok(())
    }
}

fn text(v: &Value, key: &str) -> String {
    v[key].as_str().unwrap_or_default().to_owned()
}

/// A registry with the fake `git` and `agent` acts.
pub fn registry(world: &Arc<World>) -> Registry {
    let mut reg = Registry::new();

    let w = world.clone();
    reg.register_act(
        OperatorSpec::new("git.refresh_branch")
            .settings(Shape::new().required("branch", Ty::Text))
            .touches(["git"]),
        move |v, _cx| {
            let w = w.clone();
            async move {
                w.run("git.refresh_branch", &text(&v, "branch"))?;
                Ok(json!({}))
            }
        },
    )
    .unwrap();

    let w = world.clone();
    reg.register_act(
        OperatorSpec::new("git.branch_cut")
            .settings(
                Shape::new()
                    .required("name", Ty::Text)
                    .required("from", Ty::Text),
            )
            .returns(Shape::new().required("name", Ty::Text))
            .touches(["git"]),
        move |v, _cx| {
            let w = w.clone();
            async move {
                let name = text(&v, "name");
                w.run("git.branch_cut", &name)?;
                Ok(json!({ "name": format!("plan/{name}") }))
            }
        },
    )
    .unwrap();

    let w = world.clone();
    reg.register_act(
        OperatorSpec::new("git.worktree_add")
            .settings(
                Shape::new()
                    .required("name", Ty::Text)
                    .required("from", Ty::Text),
            )
            .returns(Shape::new().required("path", Ty::Text))
            .touches(["git"]),
        move |v, _cx| {
            let w = w.clone();
            async move {
                let name = text(&v, "name");
                w.run("git.worktree_add", &name)?;
                let path = format!("/trees/{name}");
                w.add_tree(&path);
                Ok(json!({ "path": path }))
            }
        },
    )
    .unwrap();

    let w = world.clone();
    reg.register_act(
        OperatorSpec::new("git.worktree_remove")
            .settings(Shape::new().required("path", Ty::Text))
            .touches(["git"]),
        move |v, _cx| {
            let w = w.clone();
            async move {
                let path = text(&v, "path");
                w.run("git.worktree_remove", &path)?;
                w.state.lock().unwrap().trees.remove(&path);
                Ok(json!({}))
            }
        },
    )
    .unwrap();

    let w = world.clone();
    reg.register_act(
        OperatorSpec::new("git.commit")
            .settings(
                Shape::new()
                    .required("dir", Ty::Text)
                    .required("message", Ty::Text),
            )
            .returns(Shape::new().required("sha", Ty::Text))
            .touches(["git"]),
        move |v, _cx| {
            let w = w.clone();
            async move {
                w.run("git.commit", &text(&v, "dir"))?;
                Ok(json!({ "sha": "abc123" }))
            }
        },
    )
    .unwrap();

    let w = world.clone();
    reg.register_act(
        OperatorSpec::new("agent.start")
            .settings(
                Shape::new()
                    .required("dir", Ty::Text)
                    .required("brief", Ty::Text),
            )
            .returns(Shape::new().required("id", Ty::Text))
            .touches(["agent"]),
        move |v, _cx| {
            let w = w.clone();
            async move {
                w.run("agent.start", &text(&v, "dir"))?;
                Ok(json!({ "id": "session-1" }))
            }
        },
    )
    .unwrap();

    reg
}

/// `vines/core/job_session_ended.yaml`, built in Rust.
pub fn job_session_ended_def() -> VineDef {
    let job = || Shape::new().required("job", Ty::Id);
    once("job_session_ended")
        .input(
            job()
                .required("tree", Ty::Text)
                .required("status", Ty::Text),
        )
        .output("implemented", job().required("commit", Ty::Text))
        .output("stopped", job().required("reason", Ty::Text))
        .output("failed", job().required("reason", Ty::Text))
        .steps([attempt([switch(
            "$in.status",
            [(
                "finished",
                [
                    call(
                        "git.commit",
                        json!({ "dir": "$in.tree", "message": "$in.job" }),
                    )
                    .bind("commit"),
                    out(
                        "implemented",
                        json!({ "job": "$in.job", "commit": "$commit.sha" }),
                    ),
                ],
            )],
        )
        .otherwise([out(
            "stopped",
            json!({ "job": "$in.job", "reason": "$in.status" }),
        )])
        .end()])
        .on_error([out(
            "failed",
            json!({ "job": "$in.job", "reason": "$error.message" }),
        )])
        .finally([call("git.worktree_remove", json!({ "path": "$in.tree" }))])
        .end()])
}

/// `vines/core/job_launch.yaml`, built in Rust.
pub fn job_launch_def() -> VineDef {
    let failed = || {
        out(
            "failed",
            json!({ "job": "$in.job", "reason": "$error.message" }),
        )
    };
    once("job_launch")
        .input(
            Shape::new()
                .required("job", Ty::Id)
                .required("plan_branch", Ty::Text)
                .required("brief", Ty::Text),
        )
        .output(
            "started",
            Shape::new()
                .required("job", Ty::Id)
                .required("session", Ty::Text)
                .required("tree", Ty::Text),
        )
        .output(
            "failed",
            Shape::new()
                .required("job", Ty::Id)
                .required("reason", Ty::Text),
        )
        .steps([attempt([
            call("git.refresh_branch", json!({ "branch": "$in.plan_branch" })),
            call(
                "git.worktree_add",
                json!({ "name": "$in.job", "from": "$in.plan_branch" }),
            )
            .bind("tree"),
            attempt([
                call(
                    "agent.start",
                    json!({ "dir": "$tree.path", "brief": "$in.brief" }),
                )
                .bind("session"),
                out(
                    "started",
                    json!({ "job": "$in.job", "session": "$session.id", "tree": "$tree.path" }),
                ),
            ])
            .on_error([
                call("git.worktree_remove", json!({ "path": "$tree.path" })),
                failed(),
            ])
            .end(),
        ])
        .on_error([failed()])
        .end()])
}

/// The input `job_launch` takes.
pub fn ready_job() -> Value {
    json!({
        "job": "job-1",
        "plan_branch": "plan/p-1",
        "brief": "do the work",
    })
}

/// `base` with `fields` laid over it.
pub fn with(mut base: Value, fields: Value) -> Value {
    for (k, v) in fields.as_object().unwrap() {
        base[k] = v.clone();
    }
    base
}

/// `vines/pipe/job_launch.yaml`, built in Rust.
pub fn job_launch_pipe() -> vine_lang::pipe::PipeDef {
    use vine_lang::pipe::{attempt, branch, call, emit_with, pipe, tap};
    let job = || Shape::new().required("job", Ty::Id);
    pipe("job_launch")
        .input(
            job()
                .required("plan_branch", Ty::Text)
                .required("brief", Ty::Text),
        )
        .output(
            "started",
            job()
                .required("session", Ty::Text)
                .required("tree", Ty::Text),
        )
        .output("failed", job().required("reason", Ty::Text))
        .stages([attempt(
            [
                tap(
                    "git.refresh_branch",
                    json!({ "branch": "$item.plan_branch" }),
                ),
                call(
                    "git.worktree_add",
                    json!({ "name": "$item.job", "from": "$item.plan_branch" }),
                )
                .into("tree"),
                call(
                    "agent.start",
                    json!({ "dir": "$item.tree.path", "brief": "$item.brief" }),
                )
                .into("session"),
                emit_with(
                    "started",
                    json!({ "job": "$item.job", "session": "$item.session.id", "tree": "$item.tree.path" }),
                ),
            ],
            [
                branch(
                    "$item.error.from",
                    [(
                        "agent.start",
                        [tap(
                            "git.worktree_remove",
                            json!({ "path": "$item.tree.path" }),
                        )],
                    )],
                    None,
                ),
                emit_with(
                    "failed",
                    json!({ "job": "$item.job", "reason": "$item.error.message" }),
                ),
            ],
        )])
}
