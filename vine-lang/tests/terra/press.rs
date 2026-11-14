//! One press, as a stream: the items it gave before, then the vine run
//! live, each output landed before it is given.
//!
//! The same stream answers an outside press and a vine's `move.<name>`
//! call. After a crash it gives again, from the store, everything it gave,
//! and then resumes the vine from the journal it kept. The resumed run can
//! give again an output that landed before the crash; the store knows it
//! by its id and lands nothing.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use futures::stream::{self, BoxStream};
use futures::StreamExt;
use tokio::sync::Notify;
use vine_lang::{json, Event as RunEvent, Journal, OpError, Part, PipeOutput, RunError, Value};

use super::store::Admission;
use super::{End, Fate, Inner, Installed, Landing};

/// The stream of a press of `name` under request `id`.
pub(super) fn parts(
    inner: Arc<Inner>,
    id: String,
    name: String,
    payload: Value,
) -> BoxStream<'static, Result<Part, OpError>> {
    let press = Press {
        inner,
        id,
        name,
        payload,
        started: false,
        queue: VecDeque::new(),
        live: None,
        finished: false,
        _busy: None,
    };
    stream::unfold(press, |mut p| async move {
        p.next().await.map(|part| (part, p))
    })
    .boxed()
}

/// Read a press to its end. Gives false when Terra crashed partway: the
/// press is dropped where it stood, as a crash would leave it.
pub(super) async fn drain(
    mut parts: BoxStream<'static, Result<Part, OpError>>,
    crashed: &Notify,
) -> bool {
    let crash = crashed.notified();
    tokio::pin!(crash);
    loop {
        tokio::select! {
            biased;
            () = &mut crash => return false,
            part = parts.next() => if part.is_none() { return true },
        }
    }
}

/// What a press gives the calling vine for one output.
pub(super) fn item(landing: &Landing) -> Value {
    json!({
        "output": landing.output,
        "event": landing.event.kind,
        "landed": landing.fate == Fate::Landed,
        "payload": landing.payload,
    })
}

fn end(end: &End) -> Value {
    match end {
        End::Done => json!({ "status": "done" }),
        End::Failed(reason) => json!({ "status": "failed", "reason": reason }),
    }
}

/// The presses being read now, so two readers never run one vine twice
/// at once. It lives in memory: a crash empties it.
#[derive(Default)]
pub(super) struct Live(Mutex<BTreeSet<String>>);

struct Busy {
    inner: Arc<Inner>,
    id: String,
}

impl Drop for Busy {
    fn drop(&mut self) {
        self.inner.live.0.lock().unwrap().remove(&self.id);
    }
}

/// Wait for ever when Terra has crashed. Whoever reads the press drops it.
async fn tick(inner: &Inner, point: impl FnOnce() -> String) {
    if inner.fault.tick(point) {
        futures::future::pending::<()>().await;
    }
}

struct Press {
    inner: Arc<Inner>,
    id: String,
    name: String,
    payload: Value,
    started: bool,
    queue: VecDeque<Result<Part, OpError>>,
    live: Option<Running>,
    finished: bool,
    _busy: Option<Busy>,
}

/// The vine of an accepted press, running.
struct Running {
    installed: Arc<Installed>,
    run: BoxStream<'static, Result<RunEvent, RunError>>,
    journal: Journal,
}

impl Press {
    async fn next(&mut self) -> Option<Result<Part, OpError>> {
        loop {
            if let Some(part) = self.queue.pop_front() {
                return Some(part);
            }
            if self.finished {
                return None;
            }
            if !self.started {
                self.started = true;
                self.start();
                continue;
            }
            self.step().await;
        }
    }

    fn give(&mut self, part: Result<Part, OpError>) {
        self.queue.push_back(part);
    }

    fn finish(&mut self, part: Result<Part, OpError>) {
        self.give(part);
        self.finished = true;
    }

    fn start(&mut self) {
        let installed = self
            .inner
            .installed(&self.name)
            .expect("only installed moves are pressed");
        if !self.inner.live.0.lock().unwrap().insert(self.id.clone()) {
            let e = OpError::new("busy", format!("request {} is being read", self.id));
            return self.finish(Err(e));
        }
        self._busy = Some(Busy {
            inner: self.inner.clone(),
            id: self.id.clone(),
        });
        let mut store = self.inner.store.lock().unwrap();
        let (landings, ended) = match store.admit(&self.id, &installed.decl, &self.payload) {
            Admission::Refused(refusal) => {
                drop(store);
                let gate = json!({ "status": "refused", "gate": refusal.gate() });
                return self.finish(Ok(Part::End(gate)));
            }
            Admission::Accepted { landings, end } => (landings, end),
        };
        let Some(vine) = installed.vine.clone() else {
            // A move of only gates lands its one event as it is accepted.
            let given = match ended {
                Some(_) => landings,
                None => {
                    let landing =
                        store.land(&self.id, &installed.decl, "done", self.payload.clone());
                    store.end(&self.id, End::Done);
                    vec![landing]
                }
            };
            drop(store);
            for landing in &given {
                self.give(Ok(Part::Item(item(landing))));
            }
            return self.finish(Ok(Part::End(end(&End::Done))));
        };
        let accepted = store.accepted(&self.id);
        let journal =
            Journal::from_json(&accepted.journal).expect("the store keeps journals whole");
        let input = accepted.input.clone();
        drop(store);
        for landing in &landings {
            self.give(Ok(Part::Item(item(landing))));
        }
        if let Some(done) = ended {
            return self.finish(Ok(Part::End(end(&done))));
        }
        self.live = Some(Running {
            installed,
            run: vine.run_saved(stream::iter([input]), journal.clone()),
            journal,
        });
    }

    async fn step(&mut self) {
        let live = self
            .live
            .as_mut()
            .expect("a started press with no end is live");
        match live.run.next().await {
            None => self.close(End::Done).await,
            Some(Err(e)) => self.close(End::Failed(e.to_string())).await,
            Some(Ok(event @ RunEvent::Save(_))) => self.keep(event).await,
            Some(Ok(RunEvent::Output(out))) => self.output(out).await,
        }
    }

    /// Keep one more entry of the vine's journal.
    async fn keep(&mut self, event: RunEvent) {
        let id = self.id.clone();
        let RunEvent::Save(entry) = &event else {
            unreachable!("only entries are kept")
        };
        let kind = entry.to_json()["kind"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        tick(&self.inner, || format!("{id} before {kind}")).await;
        let live = self.live.as_mut().expect("still live");
        live.journal.record(&event);
        let saved = live.journal.to_json();
        self.inner.store.lock().unwrap().keep_journal(&id, saved);
        tick(&self.inner, || format!("{id} after {kind}")).await;
    }

    /// Land one output and give it to the caller, unless it landed before
    /// a crash: the caller was given it then.
    async fn output(&mut self, out: PipeOutput) {
        let id = self.id.clone();
        if self.inner.store.lock().unwrap().has_landed(&id, &out.id) {
            return;
        }
        let live = self.live.as_mut().expect("still live");
        let installed = live.installed.clone();
        tick(&self.inner, || format!("{id} before output {}", out.name)).await;
        let landing = self
            .inner
            .store
            .lock()
            .unwrap()
            .land_once(&id, &installed.decl, &out.id, &out.name, out.payload)
            .expect("nothing else lands this request's outputs while it is read");
        tick(&self.inner, || {
            format!("{id} after output {}", landing.output)
        })
        .await;
        self.give(Ok(Part::Item(item(&landing))));
    }

    async fn close(&mut self, how: End) {
        let id = self.id.clone();
        tick(&self.inner, || format!("{id} before end")).await;
        self.inner.store.lock().unwrap().end(&id, how.clone());
        tick(&self.inner, || format!("{id} after end")).await;
        self.live = None;
        self.finish(Ok(Part::End(end(&how))));
    }
}
