//! Fakes standing in for a host. Everything here uses the public API only:
//! these provided operators are registered from outside the library.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use vine_lang::{json, OpError, OperatorSpec, Registry, Shape, Ty, Value};

pub fn vine_source(name: &str) -> String {
    let path = format!("{}/vines/{name}.yaml", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

// ---------------------------------------------------------------- facts

/// A fake fact store: per-artifact records, a folded view, a version.
#[derive(Default)]
pub struct Facts {
    inner: Mutex<FactsInner>,
}

#[derive(Default)]
struct FactsInner {
    views: BTreeMap<String, Value>,
    records: BTreeMap<String, Vec<Value>>,
    fail_reads: bool,
    /// Writes left before every write fails. `None` means never fail.
    writes_before_failure: Option<usize>,
}

impl Facts {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// Put an artifact with these fields at version 1.
    pub fn put(&self, artifact: &str, fields: Value) {
        let mut view = fields;
        view["artifact"] = json!(artifact);
        view["version"] = json!(1);
        self.inner
            .lock()
            .unwrap()
            .views
            .insert(artifact.into(), view);
    }

    pub fn view(&self, artifact: &str) -> Option<Value> {
        self.inner.lock().unwrap().views.get(artifact).cloned()
    }

    pub fn records(&self, artifact: &str) -> Vec<Value> {
        self.inner
            .lock()
            .unwrap()
            .records
            .get(artifact)
            .cloned()
            .unwrap_or_default()
    }

    pub fn fail_reads(&self) {
        self.inner.lock().unwrap().fail_reads = true;
    }

    pub fn fail_writes_after(&self, n: usize) {
        self.inner.lock().unwrap().writes_before_failure = Some(n);
    }

    pub fn heal(&self) {
        let mut i = self.inner.lock().unwrap();
        i.fail_reads = false;
        i.writes_before_failure = None;
    }

    fn read(&self, artifact: &str) -> Result<Value, OpError> {
        let i = self.inner.lock().unwrap();
        if i.fail_reads {
            return Err(OpError::new("unavailable", "fact store is down"));
        }
        Ok(i.views.get(artifact).cloned().unwrap_or(Value::Null))
    }

    fn write(
        &self,
        artifact: &str,
        expect: Option<i64>,
        records: &[Value],
    ) -> Result<Value, OpError> {
        let mut guard = self.inner.lock().unwrap();
        let i = &mut *guard;
        if let Some(left) = i.writes_before_failure.as_mut() {
            if *left == 0 {
                return Err(OpError::new("unavailable", "fact store is down"));
            }
            *left -= 1;
        }
        let current = i
            .views
            .get(artifact)
            .and_then(|v| v["version"].as_i64())
            .unwrap_or(0);
        if let Some(expect) = expect {
            if expect != current {
                return Err(OpError::new(
                    "conflict",
                    format!("expected version {expect}, store is at {current}"),
                ));
            }
        }
        if let Some(bad) = records.iter().find(|r| !r.is_object()) {
            return Err(OpError::new(
                "bad_record",
                format!("record is not a map: {bad}"),
            ));
        }
        let view = i
            .views
            .entry(artifact.into())
            .or_insert_with(|| json!({ "artifact": artifact }));
        for r in records {
            if r["type"] == "claimed" {
                view["holder"] = r["claimant"].clone();
            }
        }
        view["version"] = json!(current + 1);
        i.records
            .entry(artifact.into())
            .or_default()
            .extend(records.iter().cloned());
        Ok(json!({ "version": current + 1 }))
    }
}

/// Register `read` and `write` over a fake fact store.
pub fn register_facts(reg: &mut Registry, facts: &Arc<Facts>) {
    let f = facts.clone();
    reg.register(
        OperatorSpec::new("read")
            .settings(Shape::new().required("artifact", Ty::Id))
            .touches(["facts.read"]),
        move |s, _cx| {
            let f = f.clone();
            async move { f.read(s["artifact"].as_str().unwrap()) }
        },
    )
    .unwrap();

    let f = facts.clone();
    reg.register(
        OperatorSpec::new("write")
            .settings(
                Shape::new()
                    .required("artifact", Ty::Id)
                    .optional("expect_version", Ty::Int)
                    .required("records", Ty::List),
            )
            .returns(Shape::new().required("version", Ty::Int))
            .touches(["facts.write"]),
        move |s, _cx| {
            let f = f.clone();
            async move {
                f.write(
                    s["artifact"].as_str().unwrap(),
                    s["expect_version"].as_i64(),
                    s["records"].as_array().unwrap(),
                )
            }
        },
    )
    .unwrap();
}

// ---------------------------------------------------------------- git

#[derive(Default)]
pub struct Git {
    inner: Mutex<GitInner>,
}

#[derive(Default)]
struct GitInner {
    live: BTreeSet<String>,
    created: usize,
    released: Vec<String>,
    fail_create: bool,
    fail_release: bool,
}

impl Git {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }
    pub fn live(&self) -> BTreeSet<String> {
        self.inner.lock().unwrap().live.clone()
    }
    pub fn is_live(&self, path: &str) -> bool {
        self.inner.lock().unwrap().live.contains(path)
    }
    pub fn created(&self) -> usize {
        self.inner.lock().unwrap().created
    }
    pub fn released(&self) -> Vec<String> {
        self.inner.lock().unwrap().released.clone()
    }
    pub fn fail_create(&self) {
        self.inner.lock().unwrap().fail_create = true;
    }
    pub fn fail_release(&self) {
        self.inner.lock().unwrap().fail_release = true;
    }
}

pub fn register_git(reg: &mut Registry, git: &Arc<Git>) {
    let g = git.clone();
    reg.register(
        OperatorSpec::new("worktree")
            .settings(Shape::new().required("branch", Ty::Text))
            .returns(Shape::new().required("path", Ty::Text))
            .touches(["git"]),
        move |s, _cx| {
            let g = g.clone();
            async move {
                let mut i = g.inner.lock().unwrap();
                if i.fail_create {
                    return Err(OpError::new("git", "cannot create worktree: disk full"));
                }
                i.created += 1;
                let path = format!("/trees/{}-{}", s["branch"].as_str().unwrap(), i.created);
                i.live.insert(path.clone());
                Ok(json!({ "path": path }))
            }
        },
    )
    .unwrap();

    let g = git.clone();
    reg.register(
        OperatorSpec::new("worktree_release")
            .settings(Shape::new().required("path", Ty::Text))
            .touches(["git"]),
        move |s, _cx| {
            let g = g.clone();
            async move {
                let mut i = g.inner.lock().unwrap();
                let path = s["path"].as_str().unwrap().to_owned();
                if i.fail_release {
                    return Err(OpError::new("git", format!("cannot remove {path}: busy")));
                }
                if !i.live.remove(&path) {
                    return Err(OpError::new("git", format!("no worktree at {path}")));
                }
                i.released.push(path);
                Ok(Value::Null)
            }
        },
    )
    .unwrap();
}

// ---------------------------------------------------------------- agent

#[derive(Debug, Clone)]
pub enum Script {
    /// Finishes after the delay with status `completed`.
    Complete,
    /// Finishes with status `failed` and this reason.
    Fail(&'static str),
    /// The agent session itself reports it was cancelled (not the caller).
    ReportsCancelled,
    /// Finishes with status `failed` and no reason.
    FailWithoutReason,
    /// The operator errors with code `agent`.
    Error(&'static str),
    /// Returns a value that breaks the declared return shape.
    Garbage,
    /// Never finishes on its own; only cancellation ends it.
    Hang,
}

pub struct Agent {
    script: Script,
    delay: Duration,
    git: Arc<Git>,
    started: Mutex<Vec<String>>,
}

impl Agent {
    pub fn new(script: Script, git: &Arc<Git>) -> Arc<Self> {
        Arc::new(Agent {
            script,
            delay: Duration::from_secs(3600),
            git: git.clone(),
            started: Mutex::default(),
        })
    }
    pub fn started(&self) -> Vec<String> {
        self.started.lock().unwrap().clone()
    }
}

pub fn register_agent(reg: &mut Registry, agent: &Arc<Agent>) {
    let a = agent.clone();
    reg.register(
        OperatorSpec::new("agent_run")
            .settings(
                Shape::new()
                    .required("dir", Ty::Text)
                    .required("brief", Ty::Text),
            )
            .returns(
                Shape::new()
                    .required("status", Ty::Text)
                    .required("session", Ty::Text)
                    .optional("reason", Ty::Text),
            )
            .touches(["agent"]),
        move |s, cx| {
            let a = a.clone();
            async move {
                let dir = s["dir"].as_str().unwrap().to_owned();
                if !a.git.is_live(&dir) {
                    return Err(OpError::new("agent", format!("{dir} is not a worktree")));
                }
                a.started.lock().unwrap().push(dir);
                let wait = async {
                    match a.script {
                        Script::Hang => std::future::pending::<()>().await,
                        _ => tokio::time::sleep(a.delay).await,
                    }
                };
                tokio::select! {
                    _ = cx.cancellation().cancelled() => {
                        return Err(OpError::cancelled("agent run cancelled by caller"));
                    }
                    _ = wait => {}
                }
                let session = "sess-1";
                match a.script {
                    Script::Complete => Ok(json!({ "status": "completed", "session": session })),
                    Script::Fail(reason) => {
                        Ok(json!({ "status": "failed", "session": session, "reason": reason }))
                    }
                    Script::ReportsCancelled => Ok(json!({
                        "status": "cancelled", "session": session, "reason": "session cancelled"
                    })),
                    Script::FailWithoutReason => {
                        Ok(json!({ "status": "failed", "session": session }))
                    }
                    Script::Error(msg) => Err(OpError::new("agent", msg)),
                    Script::Garbage => Ok(json!({ "status": 7, "session": session })),
                    Script::Hang => unreachable!(),
                }
            }
        },
    )
    .unwrap();
}

// ---------------------------------------------------------------- keys

/// Trusted signers. Stands in for a keyring: verification needs keys that
/// live outside the vine, so the operator touches `keys.read`.
pub struct Keys {
    trusted: BTreeSet<String>,
}

impl Keys {
    pub fn new(trusted: &[&str]) -> Arc<Self> {
        Arc::new(Keys {
            trusted: trusted.iter().map(|s| s.to_string()).collect(),
        })
    }
}

/// A correctly signed envelope.
pub fn envelope(signer: &str, id: &str, artifact: &str) -> Value {
    json!({
        "signer": signer,
        "signature": format!("{signer}/{id}"),
        "event": { "id": id, "artifact": artifact, "type": "note" },
    })
}

pub fn register_keys(reg: &mut Registry, keys: &Arc<Keys>) {
    let k = keys.clone();
    reg.register(
        OperatorSpec::new("verify_envelope")
            .settings(Shape::new().required("envelope", Ty::Object))
            .returns(
                Shape::new()
                    .required("ok", Ty::Bool)
                    .optional("reason", Ty::Text)
                    .optional("event", Ty::Object),
            )
            .touches(["keys.read"]),
        move |s, _cx| {
            let k = k.clone();
            async move {
                let env = &s["envelope"];
                let reject = |reason: &str| Ok(json!({ "ok": false, "reason": reason }));
                let (Some(signer), Some(sig), Some(id), Some(_artifact)) = (
                    env["signer"].as_str(),
                    env["signature"].as_str(),
                    env["event"]["id"].as_str(),
                    env["event"]["artifact"].as_str(),
                ) else {
                    return reject("malformed");
                };
                if !k.trusted.contains(signer) {
                    return reject("unknown_signer");
                }
                if sig != format!("{signer}/{id}") {
                    return reject("bad_signature");
                }
                Ok(json!({ "ok": true, "event": env["event"] }))
            }
        },
    )
    .unwrap();
}
