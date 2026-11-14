//! What the stand-in keeps, as a database would: the facts with their
//! versions, the event log, and every request. A crash loses nothing
//! here. Each method does its whole change under one lock, the way a
//! database would in one transaction.

use std::collections::BTreeMap;

use vine_lang::{json, Shape, Value};

use super::{Answer, End, Event, Fate, Landing, Move, Read, Refusal, ABANDONED};

#[derive(Default)]
pub(super) struct Store {
    facts: BTreeMap<String, (u64, Value)>,
    pub(super) events: Vec<Event>,
    requests: BTreeMap<String, Request>,
    next_id: u64,
    /// How many events of the log the reactions have seen.
    pub(super) reacted: usize,
}

pub(super) enum Request {
    Refused {
        move_name: String,
        payload: Value,
        refusal: Refusal,
    },
    Accepted(Accepted),
}

/// A press whose gates held.
pub(super) struct Accepted {
    pub(super) move_name: String,
    pub(super) payload: Value,
    pub(super) target: String,
    /// The version of every artifact the press read, moved on by the
    /// press's own events.
    pub(super) seen: BTreeMap<String, u64>,
    pub(super) input: Value,
    /// The vine's journal, as JSON.
    pub(super) journal: Value,
    pub(super) landings: Vec<Landing>,
    /// The last output landed and the entry after it was not kept, so a
    /// resumed run gives that output again first.
    pub(super) repeat_pending: bool,
    pub(super) end: Option<End>,
}

impl Request {
    fn move_name(&self) -> &str {
        match self {
            Request::Refused { move_name, .. } => move_name,
            Request::Accepted(a) => &a.move_name,
        }
    }

    fn payload(&self) -> &Value {
        match self {
            Request::Refused { payload, .. } => payload,
            Request::Accepted(a) => &a.payload,
        }
    }
}

/// What the store found for a press.
pub(super) enum Admission {
    Refused(Refusal),
    /// The request was accepted, now or before. Give its landings so far.
    /// `end` is set when it has ended.
    Accepted {
        landings: Vec<Landing>,
        end: Option<End>,
    },
}

impl Store {
    pub(super) fn put(&mut self, id: &str, facts: Value) {
        let version = self.facts.get(id).map_or(1, |(v, _)| v + 1);
        self.facts.insert(id.into(), (version, facts));
    }

    pub(super) fn set(&mut self, id: &str, field: &str, value: Value) {
        let (version, facts) = self.facts.get_mut(id).expect("artifact exists");
        facts[field] = value;
        *version += 1;
    }

    pub(super) fn fact(&self, id: &str, field: &str) -> Value {
        self.facts
            .get(id)
            .and_then(|(_, f)| f.get(field).cloned())
            .unwrap_or(Value::Null)
    }

    pub(super) fn new_request_id(&mut self) -> String {
        self.next_id += 1;
        format!("r-{}", self.next_id)
    }

    pub(super) fn requests_of(&self, move_name: &str) -> Vec<String> {
        self.requests
            .iter()
            .filter(|(_, r)| r.move_name() == move_name)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Whether `id` names a press of another move or payload.
    pub(super) fn clashes(&self, id: &str, move_name: &str, payload: &Value) -> bool {
        self.requests
            .get(id)
            .is_some_and(|r| r.move_name() != move_name || r.payload() != payload)
    }

    /// Whether `id` names a press that has not ended.
    pub(super) fn is_running(&self, id: &str) -> bool {
        matches!(self.requests.get(id), Some(Request::Accepted(a)) if a.end.is_none())
    }

    pub(super) fn accepted(&self, id: &str) -> &Accepted {
        match self.requests.get(id) {
            Some(Request::Accepted(a)) => a,
            _ => panic!("request {id} was accepted"),
        }
    }

    fn accepted_mut(&mut self, id: &str) -> &mut Accepted {
        match self.requests.get_mut(id) {
            Some(Request::Accepted(a)) => a,
            _ => panic!("request {id} was accepted"),
        }
    }

    pub(super) fn answer(&self, id: &str) -> Answer {
        match &self.requests[id] {
            Request::Refused { refusal, .. } => Answer::Refused(refusal.clone()),
            Request::Accepted(a) => Answer::Ran {
                request: id.into(),
                landings: a.landings.clone(),
                end: a.end.clone().expect("the press ended"),
            },
        }
    }

    /// Read `path`, starting from the payload. Records the version of
    /// every artifact it reads.
    fn read(&self, path: &str, payload: &Value, seen: &mut BTreeMap<String, u64>) -> Option<Value> {
        let mut parts = path.split('.');
        let mut value = payload.get(parts.next()?)?.clone();
        for part in parts {
            // A text value that names an artifact is followed to it.
            let id = value.as_str()?.to_owned();
            let (version, facts) = self.facts.get(&id)?;
            seen.insert(id, *version);
            value = facts.get(part).cloned().unwrap_or(Value::Null);
        }
        Some(value)
    }

    /// Find the request `id`, or check the gates and keep a new one. A
    /// refusal is kept too, so the same id is refused the same way after
    /// a restart, whatever the facts are by then.
    pub(super) fn admit(&mut self, id: &str, decl: &Move, payload: &Value) -> Admission {
        if self.clashes(id, decl.name, payload) {
            return Admission::Refused(Refusal::IdTaken);
        }
        if let Some(known) = self.requests.get(id) {
            return match known {
                Request::Refused { refusal, .. } => Admission::Refused(refusal.clone()),
                Request::Accepted(a) => Admission::Accepted {
                    landings: a.landings.clone(),
                    end: a.end.clone(),
                },
            };
        }
        match self.check(id, decl, payload) {
            Ok(accepted) => {
                self.requests.insert(id.into(), Request::Accepted(accepted));
                Admission::Accepted {
                    landings: Vec::new(),
                    end: None,
                }
            }
            Err(refusal) => {
                self.requests.insert(
                    id.into(),
                    Request::Refused {
                        move_name: decl.name.into(),
                        payload: payload.clone(),
                        refusal: refusal.clone(),
                    },
                );
                Admission::Refused(refusal)
            }
        }
    }

    /// The payload, the gates in the order the move declares them, and
    /// the needs, against the facts as they are now.
    fn check(&self, id: &str, decl: &Move, payload: &Value) -> Result<Accepted, Refusal> {
        let mut shape = Shape::new();
        for (field, ty) in &decl.payload {
            shape = shape.required(field, *ty);
        }
        shape.check(payload).map_err(Refusal::Payload)?;
        let mut seen = BTreeMap::new();
        for gate in &decl.gates {
            if self.read(gate.path, payload, &mut seen) != Some(gate.equals.clone()) {
                return Err(Refusal::Gate(gate.gate.into()));
            }
        }
        let mut input = payload.clone();
        for need in &decl.needs {
            input[need.name] = match &need.from {
                Read::Request => json!(id),
                Read::Fact(path) => match self.read(path, payload, &mut seen) {
                    Some(v) if !v.is_null() => v,
                    _ => return Err(Refusal::Missing((*path).into())),
                },
            };
        }
        Ok(Accepted {
            move_name: decl.name.into(),
            payload: payload.clone(),
            target: payload[decl.target].as_str().unwrap_or_default().into(),
            seen,
            input,
            journal: json!([]),
            landings: Vec::new(),
            repeat_pending: false,
            end: None,
        })
    }

    /// Keep the journal of request `id` after one more entry. The output
    /// landed last is safe from repeating once an entry after it is kept,
    /// unless the run has not yet given it again (`pending`).
    pub(super) fn keep_journal(&mut self, id: &str, journal: Value, pending: bool) {
        let a = self.accepted_mut(id);
        a.journal = journal;
        a.repeat_pending = pending;
    }

    pub(super) fn end(&mut self, id: &str, end: End) {
        self.accepted_mut(id).end = Some(end);
    }

    /// Turn one output of request `id`'s vine into its event, or into an
    /// `abandoned` event when an artifact the press read has changed.
    pub(super) fn land(&mut self, id: &str, decl: &Move, output: &str, payload: Value) -> Landing {
        let a = self.accepted(id);
        let target = a.target.clone();
        let changed = a
            .seen
            .iter()
            .find(|(art, v)| self.facts.get(*art).map(|(now, _)| now) != Some(*v))
            .map(|(art, _)| art.clone());
        let landing = match changed {
            None => {
                let lands = decl
                    .events
                    .iter()
                    .find(|l| l.output == output)
                    .expect("install checked every output has an event");
                if let Some((version, facts)) = self.facts.get_mut(&target) {
                    if !lands.sets.is_empty() {
                        for (field, value) in &lands.sets {
                            facts[*field] = value.clone();
                        }
                        *version += 1;
                        let now = *version;
                        if let Some(seen) = self.accepted_mut(id).seen.get_mut(&target) {
                            *seen = now;
                        }
                    }
                }
                Landing {
                    output: output.into(),
                    event: Event {
                        artifact: target,
                        kind: lands.kind.into(),
                        payload: payload.clone(),
                    },
                    payload,
                    fate: Fate::Landed,
                }
            }
            Some(changed) => Landing {
                output: output.into(),
                event: Event {
                    artifact: target,
                    kind: ABANDONED.into(),
                    payload: json!({
                        "move": decl.name,
                        "request": id,
                        "output": output,
                        "payload": payload,
                        "changed": changed,
                    }),
                },
                payload,
                fate: Fate::Abandoned,
            },
        };
        self.events.push(landing.event.clone());
        let a = self.accepted_mut(id);
        a.landings.push(landing.clone());
        a.repeat_pending = true;
        landing
    }
}
