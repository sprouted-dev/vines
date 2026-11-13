//! A fake agent host, and `job_run` built in Rust.
//!
//! The host owns its sessions, keyed by `key`. A session makes lines on
//! tokio's clock in a task of its own, whether or not a vine reads them,
//! and then ends with a status. `agent.run` starts a session when there is
//! none for the key and attaches to the one there is when there is. It
//! gives the lines from the position the vine asks for, and then the end.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream;
use tokio::sync::watch;
use vine_lang::pipe::{
    branch, call, catch, collect, emit_with, filter, finalize, fork, last, op, pipe, tap, PipeDef,
};
use vine_lang::{json, OpError, OperatorSpec, Part, Registry, Shape, Ty, Value};

use super::{registry, World};

/// How a session the host starts will go.
#[derive(Debug, Clone)]
pub struct Script {
    /// How many lines it makes.
    pub lines: u64,
    /// The time between lines.
    pub every: Duration,
    /// The status it ends with: `finished`, `failed`, `cancelled`.
    pub end: String,
    /// Lose the connection after this many lines are read: the read fails.
    pub lost_after: Option<u64>,
}

impl Script {
    pub fn new(lines: u64, end: &str) -> Script {
        Script {
            lines,
            every: Duration::from_secs(1),
            end: end.into(),
            lost_after: None,
        }
    }
}

/// What a session has made so far.
#[derive(Debug, Clone, Default)]
pub struct Session {
    pub lines: Vec<Value>,
    pub status: Option<String>,
}

#[derive(Default)]
pub struct Host {
    scripts: Mutex<BTreeMap<String, Script>>,
    sessions: Mutex<BTreeMap<String, Arc<watch::Sender<Session>>>>,
}

impl Host {
    pub fn new() -> Arc<Host> {
        Arc::new(Host::default())
    }

    /// How the session for `key` will go when it starts. With no script a
    /// session makes 120 lines, one a second, and finishes.
    pub fn script(&self, key: &str, script: Script) {
        self.scripts.lock().unwrap().insert(key.into(), script);
    }

    /// The session for `key`, as it is now.
    pub fn session(&self, key: &str) -> Option<Session> {
        let sessions = self.sessions.lock().unwrap();
        sessions.get(key).map(|tx| tx.borrow().clone())
    }

    /// Start the session for `key`, or attach to the one there is.
    fn open(&self, world: &World, key: &str, from: u64) -> (watch::Receiver<Session>, Script) {
        let script = self
            .scripts
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_else(|| Script::new(120, "finished"));
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(tx) = sessions.get(key) {
            world.note(format!("agent.attach {key} from {from}"));
            return (tx.subscribe(), script);
        }
        world.note(format!("agent.start {key}"));
        let tx = Arc::new(watch::Sender::new(Session::default()));
        sessions.insert(key.into(), tx.clone());
        let rx = tx.subscribe();
        let make = script.clone();
        // The session runs on its own. Dropping the vine does not stop it.
        tokio::spawn(async move {
            for seq in 1..=make.lines {
                tokio::time::sleep(make.every).await;
                tx.send_modify(|s| {
                    s.lines
                        .push(json!({ "seq": seq, "text": format!("line {seq}") }))
                });
            }
            tx.send_modify(|s| s.status = Some(make.end.clone()));
        });
        (rx, script)
    }
}

/// `job_run`'s acts: the `git` fakes of [`registry`], `agent.run` on
/// `host`, and the `summarize` operator.
pub fn job_run_registry(world: &Arc<World>, host: &Arc<Host>) -> Registry {
    let mut reg = registry(world);
    let (w, h) = (world.clone(), host.clone());
    reg.register_stream_act(
        OperatorSpec::new("agent.run")
            .settings(
                Shape::new()
                    .required("key", Ty::Id)
                    .required("dir", Ty::Text)
                    .required("brief", Ty::Text),
            )
            .returns(
                Shape::new()
                    .required("seq", Ty::Int)
                    .required("text", Ty::Text),
            )
            .ends(
                Shape::new()
                    .required("status", Ty::Text)
                    .required("lines", Ty::Int),
            )
            .touches(["agent"]),
        move |v, cx| {
            let key = v["key"].as_str().unwrap_or_default().to_owned();
            let from = cx.resume_from();
            let (rx, script) = h.open(&w, &key, from);
            let start = usize::try_from(from).unwrap();
            stream::unfold((rx, start, false), move |(mut rx, at, done)| {
                let lost_after = script.lost_after;
                async move {
                    if done {
                        return None;
                    }
                    loop {
                        if lost_after.is_some_and(|n| at as u64 >= n) {
                            let e = OpError::new("host_lost", "lost the agent host");
                            return Some((Err(e), (rx, at, true)));
                        }
                        let next = {
                            let s = rx.borrow_and_update();
                            if let Some(line) = s.lines.get(at) {
                                Some(Part::Item(line.clone()))
                            } else {
                                s.status.as_ref().map(|status| {
                                    Part::End(json!({ "status": status, "lines": s.lines.len() }))
                                })
                            }
                        };
                        match next {
                            Some(Part::Item(line)) => {
                                return Some((Ok(Part::Item(line)), (rx, at + 1, false)))
                            }
                            Some(end) => return Some((Ok(end), (rx, at, true))),
                            None => {
                                if rx.changed().await.is_err() {
                                    return None;
                                }
                            }
                        }
                    }
                }
            })
        },
    )
    .unwrap();
    reg.register(
        OperatorSpec::new("summarize")
            .settings(Shape::new().required("items", Ty::List))
            .returns(Shape::new().required("text", Ty::Text)),
        |v, _cx| async move {
            let items = v["items"].as_array().cloned().unwrap_or_default();
            let last = items
                .last()
                .and_then(|i| i["line"]["text"].as_str())
                .unwrap_or_default()
                .to_owned();
            Ok(json!({ "text": format!("{} lines, last: {last}", items.len()) }))
        },
    )
    .unwrap();
    reg
}

/// `vines/pipe/job_run.yaml`, built in Rust.
pub fn job_run_pipe() -> PipeDef {
    let job = || Shape::new().required("job", Ty::Id);
    pipe("job_run")
        .input(
            job()
                .required("plan_branch", Ty::Text)
                .required("brief", Ty::Text),
        )
        .output(
            "log_line",
            job().required("seq", Ty::Int).required("text", Ty::Text),
        )
        .output(
            "summary",
            job().required("lines", Ty::Int).required("text", Ty::Text),
        )
        .output("implemented", job().required("commit", Ty::Text))
        .output("stopped", job().required("reason", Ty::Text))
        .output("failed", job().required("reason", Ty::Text))
        .stages([
            call(
                "git.worktree_add",
                json!({ "name": "$item.job", "from": "$item.plan_branch" }),
            )
            .into("tree"),
            call(
                "agent.run",
                json!({ "key": "$item.job", "dir": "$item.tree.path", "brief": "$item.brief" }),
            )
            .into("line")
            .end("ended"),
            fork([
                vec![
                    filter("$item.line"),
                    emit_with(
                        "log_line",
                        json!({ "job": "$item.job", "seq": "$item.line.seq", "text": "$item.line.text" }),
                    ),
                ],
                vec![
                    filter("$item.line"),
                    collect(50).into("batch"),
                    op("summarize", json!({ "items": "$item.batch.items" })).into("summary"),
                    emit_with(
                        "summary",
                        json!({ "job": "$item.job", "lines": "$item.line.seq", "text": "$item.summary.text" }),
                    ),
                ],
            ]),
            last(),
            branch(
                "$item.ended.status",
                [(
                    "finished",
                    vec![
                        call(
                            "git.commit",
                            json!({ "dir": "$item.tree.path", "message": "$item.job" }),
                        )
                        .into("commit"),
                        emit_with(
                            "implemented",
                            json!({ "job": "$item.job", "commit": "$item.commit.sha" }),
                        ),
                    ],
                )],
                Some(vec![emit_with(
                    "stopped",
                    json!({ "job": "$item.job", "reason": "$item.ended.status" }),
                )]),
            ),
            catch([emit_with(
                "failed",
                json!({ "job": "$item.job", "reason": "$item.error.message" }),
            )]),
            finalize([tap("git.worktree_remove", json!({ "name": "$item.job" }))]),
        ])
}

/// The input `job_run` takes.
pub fn job() -> Value {
    json!({ "job": "job-1", "plan_branch": "plan/p-1", "brief": "do the work" })
}
