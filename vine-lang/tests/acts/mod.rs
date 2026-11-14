//! Fake outside acts for the tests, and the example vines built in Rust.
//!
//! The library ships no acts. A caller registers the Rust behind each
//! name. Here the "outside" is a `World` in memory.
#![allow(dead_code)]

pub mod agent;
pub mod desk;
pub mod feed;
pub mod restart;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

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
    /// Running agents: session id to the directory it runs in.
    agents: BTreeMap<String, String>,
    /// How many agents were ever started, for the next session id.
    started: u64,
    fail: BTreeMap<String, OpError>,
    hooks: BTreeMap<String, Hook>,
    /// How long each act named here takes, on tokio's clock.
    slow: BTreeMap<String, std::time::Duration>,
}

type Hook = Arc<dyn Fn() + Send + Sync>;

/// How a fake stream act is registered: as one that gives only the items
/// after those it gave, or as one that gives its items again from any
/// point, the way a host that stores every item can.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gives {
    OnlyNew,
    Again,
}

/// Register a stream act the way `gives` says.
pub fn register_stream_act<F, S>(reg: &mut Registry, gives: Gives, spec: OperatorSpec, f: F)
where
    F: Fn(Value, vine_lang::OpContext) -> S + Send + Sync + 'static,
    S: futures::Stream<Item = Result<vine_lang::Part, OpError>> + Send + 'static,
{
    match gives {
        Gives::OnlyNew => reg.register_stream_act(spec, f),
        Gives::Again => reg.register_replayable_stream_act(spec, f),
    }
    .unwrap();
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

    /// The sessions of the agents running now.
    pub fn agents(&self) -> Vec<String> {
        self.state.lock().unwrap().agents.keys().cloned().collect()
    }

    /// Start an agent in `dir`, or give the session of the one running
    /// there, so starting twice in one directory starts one agent.
    fn start_agent(&self, dir: &str) -> String {
        let mut s = self.state.lock().unwrap();
        if let Some((id, _)) = s.agents.iter().find(|(_, d)| *d == dir) {
            return id.clone();
        }
        s.started += 1;
        let id = format!("session-{}", s.started);
        s.agents.insert(id.clone(), dir.to_owned());
        id
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

    /// Make the named act take this long each time, on tokio's clock. A
    /// test that starts the clock paused gets exact times.
    pub fn slow(&self, act: &str, takes: std::time::Duration) {
        self.state.lock().unwrap().slow.insert(act.into(), takes);
    }

    /// Make the named act take this long when it acts on `detail`, such
    /// as `agent.start` in `/trees/j-2`, whatever [`World::slow`] says.
    pub fn slow_for(&self, act: &str, detail: &str, takes: std::time::Duration) {
        let key = format!("{act} {detail}");
        self.state.lock().unwrap().slow.insert(key, takes);
    }

    /// Wait as long as the named act takes on `detail`.
    async fn take_time(&self, act: &str, detail: &str) {
        let takes = {
            let s = self.state.lock().unwrap();
            s.slow
                .get(&format!("{act} {detail}"))
                .or_else(|| s.slow.get(act))
                .copied()
        };
        if let Some(takes) = takes {
            tokio::time::sleep(takes).await;
        }
    }

    /// Add a line to the log without running an act.
    pub fn note(&self, line: String) {
        self.state.lock().unwrap().log.push(line);
    }

    /// How many lines of the log start with `prefix`.
    pub fn count(&self, prefix: &str) -> usize {
        self.log().iter().filter(|l| l.starts_with(prefix)).count()
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

    // Remove a working copy by its path, or by the name it was made with.
    // Removing one that is not there is not a failure, so it is safe to
    // run twice.
    let w = world.clone();
    reg.register_act(
        OperatorSpec::new("git.worktree_remove")
            .settings(
                Shape::new()
                    .optional("path", Ty::Text)
                    .optional("name", Ty::Text),
            )
            .touches(["git"]),
        move |v, _cx| {
            let w = w.clone();
            async move {
                let path = match (v["path"].as_str(), v["name"].as_str()) {
                    (Some(path), _) => path.to_owned(),
                    (None, Some(name)) => format!("/trees/{name}"),
                    (None, None) => return Err(OpError::new("bad_input", "give `path` or `name`")),
                };
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
                let dir = text(&v, "dir");
                w.take_time("agent.start", &dir).await;
                w.run("agent.start", &dir)?;
                Ok(json!({ "id": w.start_agent(&dir) }))
            }
        },
    )
    .unwrap();

    // Stop an agent by its session. Stopping one that is not running is
    // not a failure, so it is safe to run twice.
    let w = world.clone();
    reg.register_act(
        OperatorSpec::new("agent.stop")
            .settings(Shape::new().required("session", Ty::Text))
            .touches(["agent"]),
        move |v, _cx| {
            let w = w.clone();
            async move {
                let session = text(&v, "session");
                w.run("agent.stop", &session)?;
                w.state.lock().unwrap().agents.remove(&session);
                Ok(json!({}))
            }
        },
    )
    .unwrap();

    reg
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
    use vine_lang::pipe::{branch, call, catch_error, emit_with, pipe, tap, timeout};
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
        .stages([
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
            timeout(std::time::Duration::from_secs(60)),
            emit_with(
                "started",
                json!({ "job": "$item.job", "session": "$item.session.id", "tree": "$item.tree.path" }),
            ),
            catch_error([
                branch(
                    "$item.error.from",
                    [
                        (
                            "agent.start",
                            [tap(
                                "git.worktree_remove",
                                json!({ "path": "$item.tree.path" }),
                            )],
                        ),
                        (
                            "timeout",
                            [tap(
                                "git.worktree_remove",
                                json!({ "name": "$item.job" }),
                            )],
                        ),
                    ],
                    None,
                ),
                emit_with(
                    "failed",
                    json!({ "job": "$item.job", "reason": "$item.error.message" }),
                ),
            ]),
        ])
}
