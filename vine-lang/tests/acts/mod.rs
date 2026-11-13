//! Fake outside acts for the tests, and the example vines built in Rust.
//!
//! The library ships no acts. A caller registers the Rust behind each
//! name. Here the "outside" is a `World` in memory.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use vine_lang::def::{attempt, call, guard, once, out, VineDef};
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
}

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

    fn run(&self, act: &str, detail: &str) -> Result<(), OpError> {
        let mut s = self.state.lock().unwrap();
        if let Some(e) = s.fail.remove(act) {
            return Err(e);
        }
        s.log.push(format!("{act} {detail}"));
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

/// `vines/core/claim.yaml`, built in Rust.
pub fn claim_def() -> VineDef {
    once("claim")
        .input(
            Shape::new()
                .required("item", Ty::Id)
                .required("claimant", Ty::Text)
                .required("state", Ty::Text)
                .optional("holder", Ty::Text),
        )
        .output(
            "claimed",
            Shape::new()
                .required("item", Ty::Id)
                .required("claimant", Ty::Text),
        )
        .output("refused", Shape::new().required("gate", Ty::Text))
        .steps([
            guard(
                r#"$in.state != "open""#,
                "refused",
                json!({ "gate": "state" }),
            ),
            guard("$in.holder != null", "refused", json!({ "gate": "held" })),
            out(
                "claimed",
                json!({ "item": "$in.item", "claimant": "$in.claimant" }),
            ),
        ])
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
                .required("state", Ty::Text)
                .required("plan_state", Ty::Text)
                .required("waiting_on", Ty::Int)
                .optional("live_session", Ty::Text)
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
        .output("refused", Shape::new().required("gate", Ty::Text))
        .output(
            "failed",
            Shape::new()
                .required("job", Ty::Id)
                .required("reason", Ty::Text),
        )
        .steps([
            guard(
                r#"$in.state != "ready""#,
                "refused",
                json!({ "gate": "state" }),
            ),
            guard(
                r#"$in.plan_state != "active""#,
                "refused",
                json!({ "gate": "plan" }),
            ),
            guard(
                "$in.waiting_on != 0",
                "refused",
                json!({ "gate": "dependencies" }),
            ),
            guard(
                "$in.live_session != null",
                "refused",
                json!({ "gate": "session" }),
            ),
            attempt([
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
            .end(),
        ])
}

/// A job that passes every gate of `job_launch`.
pub fn ready_job() -> Value {
    json!({
        "job": "job-1",
        "state": "ready",
        "plan_state": "active",
        "waiting_on": 0,
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
