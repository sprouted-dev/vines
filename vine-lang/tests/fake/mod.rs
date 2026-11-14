//! Fake outside operators for the tests, and the example vines built in Rust.
//!
//! The library ships no operators. A caller registers the Rust behind each
//! name. Here the "outside" is a `World` in memory.
// Each test binary compiles this module and uses a subset of it. An
// `expect` would go unfulfilled in the binaries that use all of it, and
// clippy's `allow_attributes` checks outer attributes only.
#![allow(dead_code, reason = "each test binary uses a subset of the fixtures")]

pub(crate) mod agent;
pub(crate) mod desk;
pub(crate) mod feed;
pub(crate) mod restart;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use vine_lang::{json, OpError, OperatorSpec, Registry, Shape, Ty, Value};

/// What the fake operators did, and which operator should fail next.
#[derive(Default)]
pub(crate) struct World {
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
    /// How long each operator named here takes, on tokio's clock.
    slow: BTreeMap<String, std::time::Duration>,
}

type Hook = Arc<dyn Fn() + Send + Sync>;

/// How a fake stream operator is registered: as one that gives only the items
/// after those it gave, or as one that gives its items again from any
/// point, the way a host that stores every item can.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Gives {
    OnlyNew,
    Again,
}

/// Register a stream operator the way `gives` says.
pub(crate) fn register_stream<F, S>(reg: &mut Registry, gives: Gives, spec: OperatorSpec, f: F)
where
    F: Fn(Value, vine_lang::OpContext) -> S + Send + Sync + 'static,
    S: futures::Stream<Item = Result<vine_lang::Part, OpError>> + Send + 'static,
{
    match gives {
        Gives::OnlyNew => reg.register_stream(spec, f),
        Gives::Again => reg.register_replayable_stream(spec, f),
    }
    .unwrap();
}

impl World {
    pub(crate) fn new() -> Arc<World> {
        Arc::new(World::default())
    }

    /// Make the named operator fail the next time it is called.
    pub(crate) fn fail(&self, operator: &str, code: &str, message: &str) {
        let mut s = self.state.lock().unwrap();
        s.fail.insert(operator.into(), OpError::new(code, message));
    }

    /// Every operator that ran, in order, such as `git.worktree_add job-1`.
    pub(crate) fn log(&self) -> Vec<String> {
        self.state.lock().unwrap().log.clone()
    }

    /// The working copies that exist now.
    pub(crate) fn trees(&self) -> Vec<String> {
        self.state.lock().unwrap().trees.iter().cloned().collect()
    }

    /// The sessions of the agents running now.
    pub(crate) fn agents(&self) -> Vec<String> {
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

    pub(crate) fn add_tree(&self, path: &str) {
        self.state.lock().unwrap().trees.insert(path.into());
    }

    /// Run `hook` each time the named operator runs. A test uses this to change
    /// something else while a vine is partway through.
    pub(crate) fn when(&self, operator: &str, hook: impl Fn() + Send + Sync + 'static) {
        let mut s = self.state.lock().unwrap();
        s.hooks.insert(operator.into(), Arc::new(hook));
    }

    /// Make the named operator take this long each time, on tokio's clock. A
    /// test that starts the clock paused gets exact times.
    pub(crate) fn slow(&self, operator: &str, takes: std::time::Duration) {
        self.state
            .lock()
            .unwrap()
            .slow
            .insert(operator.into(), takes);
    }

    /// Make the named operator take this long when it works on `detail`, such
    /// as `agent.start` in `/trees/j-2`, whatever [`World::slow`] says.
    pub(crate) fn slow_for(&self, operator: &str, detail: &str, takes: std::time::Duration) {
        let key = format!("{operator} {detail}");
        self.state.lock().unwrap().slow.insert(key, takes);
    }

    /// Wait as long as the named operator takes on `detail`.
    async fn take_time(&self, operator: &str, detail: &str) {
        let takes = {
            let s = self.state.lock().unwrap();
            s.slow
                .get(&format!("{operator} {detail}"))
                .or_else(|| s.slow.get(operator))
                .copied()
        };
        if let Some(takes) = takes {
            tokio::time::sleep(takes).await;
        }
    }

    /// Add a line to the log without running an operator.
    pub(crate) fn note(&self, line: String) {
        self.state.lock().unwrap().log.push(line);
    }

    /// How many lines of the log start with `prefix`.
    pub(crate) fn count(&self, prefix: &str) -> usize {
        self.log().iter().filter(|l| l.starts_with(prefix)).count()
    }

    fn run(&self, operator: &str, detail: &str) -> Result<(), OpError> {
        let hook = {
            let mut s = self.state.lock().unwrap();
            if let Some(e) = s.fail.remove(operator) {
                return Err(e);
            }
            s.log.push(format!("{operator} {detail}"));
            s.hooks.get(operator).cloned()
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

/// A registry with the fake `git` and `agent` operators.
pub(crate) fn registry(world: &Arc<World>) -> Registry {
    let mut reg = Registry::new();
    register_git(&mut reg, world);
    register_agent(&mut reg, world);
    reg
}

/// The fake `git` operators: branches, working copies and commits.
fn register_git(reg: &mut Registry, world: &Arc<World>) {
    let w = world.clone();
    reg.register(
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
    reg.register(
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
    reg.register(
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
    reg.register(
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
    reg.register(
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
}

/// The fake `agent` operators: start and stop an agent session.
fn register_agent(reg: &mut Registry, world: &Arc<World>) {
    let w = world.clone();
    reg.register(
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
    reg.register(
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
}

/// The input `job_launch` takes.
pub(crate) fn ready_job() -> Value {
    json!({
        "job": "job-1",
        "plan_branch": "plan/p-1",
        "brief": "do the work",
    })
}

/// `vines/pipe/job_launch.yaml`, built in Rust.
pub(crate) fn job_launch_pipe() -> vine_lang::pipe::PipeDef {
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
        .operators([
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
            timeout(std::time::Duration::from_mins(1)),
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
