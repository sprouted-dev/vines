//! Fakes standing in for a host. Everything here uses the public API only:
//! these provided operators are registered from outside the library.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::{BoxStream, StreamExt};
use vine_lang::{
    json, BoxFuture, Checkpoint, Emit, Memory, OpContext, OpError, OperatorSpec, Output, Registry,
    Shape, StreamOperator, StreamOperatorSpec, StreamSink, Ty, Value,
};

/// A log several fakes write to, to show how calls interleave.
pub type Journal = Arc<Mutex<Vec<String>>>;

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
    journal: Option<Journal>,
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

    pub fn set_journal(&self, journal: &Journal) {
        self.inner.lock().unwrap().journal = Some(journal.clone());
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
        if let Some(j) = &i.journal {
            let ids: Vec<String> = records.iter().map(|r| r["id"].to_string()).collect();
            j.lock().unwrap().push(format!("write {}", ids.join(",")));
        }
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
    journal: Option<Journal>,
    in_flight: Mutex<(usize, usize)>,
}

impl Keys {
    pub fn new(trusted: &[&str]) -> Arc<Self> {
        Arc::new(Keys {
            trusted: trusted.iter().map(|s| s.to_string()).collect(),
            journal: None,
            in_flight: Mutex::default(),
        })
    }

    /// Logs each verification, and yields mid-call so overlapping calls
    /// would show in [`Keys::most_at_once`].
    pub fn with_journal(trusted: &[&str], journal: &Journal) -> Arc<Self> {
        Arc::new(Keys {
            trusted: trusted.iter().map(|s| s.to_string()).collect(),
            journal: Some(journal.clone()),
            in_flight: Mutex::default(),
        })
    }

    /// The most verifications ever running at once.
    pub fn most_at_once(&self) -> usize {
        self.in_flight.lock().unwrap().1
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
                if let Some(j) = &k.journal {
                    j.lock()
                        .unwrap()
                        .push(format!("verify {}", env["event"]["id"]));
                    {
                        let mut f = k.in_flight.lock().unwrap();
                        f.0 += 1;
                        f.1 = f.1.max(f.0);
                    }
                    tokio::task::yield_now().await;
                    k.in_flight.lock().unwrap().0 -= 1;
                }
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

// ---------------------------------------------------------------- streams

/// Stands in for whatever durably saves a stream run's results and
/// checkpoints. Shared, so a test can look while the run is going.
#[derive(Clone, Default)]
pub struct Saver(Arc<Mutex<Saved>>);

#[derive(Default)]
pub struct Saved {
    pub outputs: Vec<Output>,
    pub checkpoints: Vec<Checkpoint>,
}

impl StreamSink for Saver {
    fn output(&mut self, output: Output) {
        self.0.lock().unwrap().outputs.push(output);
    }
    fn checkpoint(&mut self, checkpoint: &Checkpoint) {
        self.0.lock().unwrap().checkpoints.push(checkpoint.clone());
    }
}

impl Saver {
    pub fn outputs(&self) -> Vec<Output> {
        self.0.lock().unwrap().outputs.clone()
    }
    pub fn payloads(&self) -> Vec<Value> {
        self.outputs().into_iter().map(|o| o.payload).collect()
    }
    pub fn checkpoints(&self) -> Vec<Checkpoint> {
        self.0.lock().unwrap().checkpoints.clone()
    }
    pub fn positions(&self) -> Vec<u64> {
        self.checkpoints().iter().map(|c| c.position).collect()
    }
    /// What a resume reads back: the last saved checkpoint.
    pub fn last(&self) -> Checkpoint {
        self.checkpoints().last().cloned().unwrap_or_default()
    }
}

/// A source the test feeds by hand. Stays open until the sender drops.
pub fn channel() -> (
    tokio::sync::mpsc::UnboundedSender<Value>,
    BoxStream<'static, Value>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let source = futures::stream::poll_fn(move |cx| rx.poll_recv(cx)).boxed();
    (tx, source)
}

/// A finished source: these items, then the end.
pub fn items(values: Vec<Value>) -> BoxStream<'static, Value> {
    futures::stream::iter(values).boxed()
}

// ---------------------------------------------------------------- backup

/// A key store `encrypt` reads from.
pub struct KeyStore {
    keys: BTreeSet<String>,
}

impl KeyStore {
    pub fn new(keys: &[&str]) -> Arc<Self> {
        Arc::new(KeyStore {
            keys: keys.iter().map(|s| s.to_string()).collect(),
        })
    }
}

/// Reverse the fake encryption: the batch's items.
pub fn unseal(sealed: &str) -> Vec<Value> {
    let body = sealed.split_once("]:").expect("sealed text").1;
    serde_json::from_str(body).expect("sealed json")
}

/// An outside backup provider. Batches are stored by name, so a store
/// under a name already there replaces it.
#[derive(Default)]
pub struct Provider {
    inner: Mutex<ProviderInner>,
}

#[derive(Default)]
struct ProviderInner {
    stored: BTreeMap<String, String>,
    puts: Vec<String>,
    fail_next: usize,
    lose_ack_next: usize,
}

impl Provider {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }
    /// Batch name to sealed contents.
    pub fn stored(&self) -> BTreeMap<String, String> {
        self.inner.lock().unwrap().stored.clone()
    }
    pub fn names(&self) -> Vec<String> {
        self.stored().keys().cloned().collect()
    }
    /// Every store call that reached the provider, by name, in order.
    pub fn puts(&self) -> Vec<String> {
        self.inner.lock().unwrap().puts.clone()
    }
    /// Every `seq` across all stored batches, in name order.
    pub fn seqs(&self) -> Vec<i64> {
        let mut out = Vec::new();
        for sealed in self.stored().values() {
            out.extend(unseal(sealed).iter().map(|v| v["seq"].as_i64().unwrap()));
        }
        out
    }
    /// The next `n` stores fail before writing anything.
    pub fn fail_next(&self, n: usize) {
        self.inner.lock().unwrap().fail_next = n;
    }
    /// The next `n` stores write, then report a timeout: the ack is lost.
    pub fn lose_ack_next(&self, n: usize) {
        self.inner.lock().unwrap().lose_ack_next = n;
    }
}

/// Register `encrypt` (touches `keys.read`) and `store` (touches
/// `backup.write`). Both are ordinary operators: item in, item out.
pub fn register_backup(reg: &mut Registry, keys: &Arc<KeyStore>, provider: &Arc<Provider>) {
    let k = keys.clone();
    reg.register(
        OperatorSpec::new("encrypt")
            .settings(
                Shape::new()
                    .required("key", Ty::Text)
                    .required("data", Ty::List),
            )
            .returns(
                Shape::new()
                    .required("sealed", Ty::Text)
                    .required("count", Ty::Int),
            )
            .touches(["keys.read"]),
        move |s, _cx| {
            let k = k.clone();
            async move {
                let key = s["key"].as_str().unwrap();
                if !k.keys.contains(key) {
                    return Err(OpError::new("no_key", format!("no key named {key}")));
                }
                let data = &s["data"];
                Ok(json!({
                    "sealed": format!("sealed[{key}]:{data}"),
                    "count": data.as_array().unwrap().len(),
                }))
            }
        },
    )
    .unwrap();

    let p = provider.clone();
    reg.register(
        OperatorSpec::new("store")
            .settings(
                Shape::new()
                    .required("to", Ty::Text)
                    .required("first", Ty::Int)
                    .required("last", Ty::Int)
                    .required("sealed", Ty::Text)
                    .required("count", Ty::Int),
            )
            .returns(
                Shape::new()
                    .required("name", Ty::Text)
                    .required("first", Ty::Int)
                    .required("last", Ty::Int)
                    .required("count", Ty::Int),
            )
            .touches(["backup.write"]),
        move |s, _cx| {
            let p = p.clone();
            async move {
                let (first, last) = (s["first"].as_i64().unwrap(), s["last"].as_i64().unwrap());
                let name = format!("{}/{first:06}-{last:06}", s["to"].as_str().unwrap());
                let mut i = p.inner.lock().unwrap();
                if i.fail_next > 0 {
                    i.fail_next -= 1;
                    return Err(OpError::new("unavailable", "provider is down"));
                }
                i.puts.push(name.clone());
                i.stored
                    .insert(name.clone(), s["sealed"].as_str().unwrap().to_owned());
                if i.lose_ack_next > 0 {
                    i.lose_ack_next -= 1;
                    return Err(OpError::new("timeout", "no answer from provider"));
                }
                Ok(json!({ "name": name, "first": first, "last": last, "count": s["count"] }))
            }
        },
    )
    .unwrap();
}

/// `seq`-numbered backup records `from..=to`.
pub fn records(from: i64, to: i64) -> Vec<Value> {
    (from..=to)
        .map(|n| json!({ "seq": n, "body": format!("record {n}") }))
        .collect()
}

// ---------------------------------------------------------------- summary

/// A provided stream operator with kept memory: the summary so far.
/// Takes batches `{ items: [{ <field>: text }] }`, emits an updated summary
/// per batch and a final one at end of stream. The digest is a running
/// hash of every line in order, so a lost or repeated line changes it.
struct Summarise {
    field: String,
    lines: u64,
    digest: u64,
    last: String,
}

impl Summarise {
    fn summary(&self, final_: bool) -> Value {
        json!({
            "text": format!("{} lines, last {:?}, digest {:016x}", self.lines, self.last, self.digest),
            "lines": self.lines,
            "final": final_,
        })
    }
}

impl StreamOperator for Summarise {
    fn push<'a>(
        &'a mut self,
        item: Value,
        out: &'a mut Emit,
        _cx: &'a OpContext,
    ) -> BoxFuture<'a, Result<(), OpError>> {
        Box::pin(async move {
            for it in item["items"].as_array().into_iter().flatten() {
                let Some(line) = it[&self.field].as_str() else {
                    return Err(OpError::new(
                        "bad_line",
                        format!("item has no text `{}`: {it}", self.field),
                    ));
                };
                for b in line.bytes().chain([b'\n']) {
                    self.digest = (self.digest ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
                }
                self.lines += 1;
                self.last = line.to_owned();
            }
            out.emit(self.summary(false));
            Ok(())
        })
    }

    fn end<'a>(
        &'a mut self,
        out: &'a mut Emit,
        _cx: &'a OpContext,
    ) -> BoxFuture<'a, Result<(), OpError>> {
        out.emit(self.summary(true));
        Box::pin(async { Ok(()) })
    }

    fn save(&self) -> Value {
        json!({ "lines": self.lines, "digest": self.digest, "last": self.last })
    }

    fn restore(&mut self, memory: Value) -> Result<(), String> {
        let (Some(lines), Some(digest), Some(last)) = (
            memory["lines"].as_u64(),
            memory["digest"].as_u64(),
            memory["last"].as_str(),
        ) else {
            return Err(format!(
                "expected {{ lines, digest, last }}, found {memory}"
            ));
        };
        self.lines = lines;
        self.digest = digest;
        self.last = last.to_owned();
        Ok(())
    }
}

pub fn register_summarise(reg: &mut Registry) {
    reg.register_stream(
        StreamOperatorSpec::new("summarise")
            .settings(Shape::new().required("field", Ty::Text))
            .emits(
                Shape::new()
                    .required("text", Ty::Text)
                    .required("lines", Ty::Int)
                    .required("final", Ty::Bool),
            )
            .touches(["model"])
            .memory(Memory::Kept),
        |settings| {
            let field = settings["field"].as_str().unwrap_or_default();
            if field.is_empty() {
                return Err("`field` must name the text field of each line".into());
            }
            Ok(Box::new(Summarise {
                field: field.to_owned(),
                lines: 0,
                digest: 0xcbf2_9ce4_8422_2325,
                last: String::new(),
            }))
        },
    )
    .unwrap();
}

/// Lines `from..=to` of one session.
pub fn lines(session: &str, from: u64, to: u64) -> Vec<Value> {
    (from..=to)
        .map(|n| json!({ "line": format!("{session} line {n}") }))
        .collect()
}
