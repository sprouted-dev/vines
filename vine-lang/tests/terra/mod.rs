//! A small stand-in for Terra, for the tests. It is not part of the
//! library, and the library knows nothing about it.
//!
//! It does the four things the vines leave to whoever runs them:
//!
//! 1. Checks a move's gates against the facts.
//! 2. Builds the vine's input from the caller's payload and the facts.
//! 3. Runs the vine.
//! 4. Turns the vine's output into an event, but only if the facts it
//!    read have not changed since.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Mutex;

use vine_lang::{json, Shape, Ty, Value, Vine};

/// A gate: the fact at `path` must equal `equals`, or the move is refused
/// with `gate`.
pub struct Gate {
    pub path: &'static str,
    pub equals: Value,
    pub gate: &'static str,
}

/// A fact the vine needs: read `path` and hand it over as `name`.
pub struct Need {
    pub name: &'static str,
    pub ty: Ty,
    pub path: &'static str,
}

/// A move as declared. `target` is the payload field that names the
/// artifact the move acts on. Paths start there: `job.state`,
/// `job.plan.branch`.
pub struct Move {
    pub name: &'static str,
    pub target: &'static str,
    pub payload: Vec<(&'static str, Ty)>,
    pub gates: Vec<Gate>,
    pub needs: Vec<Need>,
    /// Output name to event type. A move with no vine names its one event
    /// under `done`.
    pub events: Vec<(&'static str, &'static str)>,
}

/// An event that landed on an artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub artifact: String,
    pub kind: String,
    pub payload: Value,
}

/// How a request ended.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// A gate refused it. The vine did not run.
    Refused { gate: String },
    /// The vine ran and its output became this event.
    Landed(Event),
    /// The vine ran, but a fact it was given changed before the write.
    /// Nothing landed.
    Stale { artifact: String, output: String },
    /// The vine ended with no output.
    Failed(String),
}

struct Installed {
    decl: Move,
    vine: Option<Vine>,
}

#[derive(Default)]
pub struct Terra {
    facts: Mutex<BTreeMap<String, (u64, Value)>>,
    events: Mutex<Vec<Event>>,
    moves: BTreeMap<&'static str, Installed>,
}

impl Terra {
    pub fn new() -> Self {
        Terra::default()
    }

    /// Put an artifact's facts in place.
    pub fn put(&self, id: &str, facts: Value) {
        let mut all = self.facts.lock().unwrap();
        let version = all.get(id).map_or(1, |(v, _)| v + 1);
        all.insert(id.into(), (version, facts));
    }

    /// Change one fact of an artifact. Its version moves.
    pub fn set(&self, id: &str, field: &str, value: Value) {
        let mut all = self.facts.lock().unwrap();
        let (version, facts) = all.get_mut(id).expect("artifact exists");
        facts[field] = value;
        *version += 1;
    }

    pub fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    /// Install a move and the vine that answers it. Refused when the vine
    /// would not get the input it declares, or could end with an output
    /// the move has no event for. This is the early check: nothing waits
    /// until the move is requested to fail.
    pub fn install(&mut self, decl: Move, vine: Option<Vine>) -> Result<(), String> {
        if let Some(vine) = &vine {
            let mut supplied = Shape::new();
            for (name, ty) in &decl.payload {
                supplied = supplied.required(name, *ty);
            }
            for need in &decl.needs {
                supplied = supplied.required(need.name, need.ty);
            }
            supplied.fits_into(vine.input()).map_err(|p| {
                format!(
                    "move `{}` does not give vine `{}` its input: {p}",
                    decl.name,
                    vine.name()
                )
            })?;
            for output in vine.outputs().keys() {
                if !decl.events.iter().any(|(o, _)| o == output) {
                    return Err(format!(
                        "move `{}` has no event for output `{output}` of vine `{}`",
                        decl.name,
                        vine.name()
                    ));
                }
            }
        } else if !decl.events.iter().any(|(o, _)| *o == "done") {
            return Err(format!(
                "move `{}` has no vine and no `done` event",
                decl.name
            ));
        }
        self.moves.insert(decl.name, Installed { decl, vine });
        Ok(())
    }

    /// Read `path`, starting from the payload. Records the version of
    /// every artifact it reads.
    fn read(&self, path: &str, payload: &Value, seen: &mut BTreeMap<String, u64>) -> Option<Value> {
        let all = self.facts.lock().unwrap();
        let mut parts = path.split('.');
        let mut value = payload.get(parts.next()?)?.clone();
        for part in parts {
            // A text value that names an artifact is followed to it.
            let id = value.as_str()?.to_owned();
            let (version, facts) = all.get(&id)?;
            seen.insert(id, *version);
            value = facts.get(part).cloned().unwrap_or(Value::Null);
        }
        Some(value)
    }

    /// Request a move.
    pub async fn request(&self, name: &str, payload: Value) -> Outcome {
        let Some(Installed { decl, vine }) = self.moves.get(name) else {
            return Outcome::Refused {
                gate: "unknown_move".into(),
            };
        };
        let mut shape = Shape::new();
        for (field, ty) in &decl.payload {
            shape = shape.required(field, *ty);
        }
        if shape.check(&payload).is_err() {
            return Outcome::Refused {
                gate: "payload".into(),
            };
        }
        let target = payload[decl.target].as_str().unwrap_or_default().to_owned();

        // 1. Gates, against the facts as they are now.
        let mut seen = BTreeMap::new();
        for gate in &decl.gates {
            if self.read(gate.path, &payload, &mut seen) != Some(gate.equals.clone()) {
                return Outcome::Refused {
                    gate: gate.gate.into(),
                };
            }
        }

        // 2. The vine's input: the payload, plus the facts it needs.
        let mut input = payload.clone();
        for need in &decl.needs {
            match self.read(need.path, &payload, &mut seen) {
                Some(v) if !v.is_null() => input[need.name] = v,
                _ => {
                    return Outcome::Refused {
                        gate: format!("missing:{}", need.path),
                    }
                }
            }
        }

        // 3. The vine. A move with no vine is done once its gates pass.
        let (output, body) = match vine {
            Some(vine) => match vine.run(input).await {
                Ok(out) => (out.name, out.payload),
                Err(e) => return Outcome::Failed(e.to_string()),
            },
            None => ("done".to_owned(), payload.clone()),
        };

        // 4. The write, only if nothing the vine was given has changed.
        let mut all = self.facts.lock().unwrap();
        for (id, version) in &seen {
            if all.get(id).map(|(v, _)| v) != Some(version) {
                return Outcome::Stale {
                    artifact: id.clone(),
                    output,
                };
            }
        }
        let kind = decl
            .events
            .iter()
            .find(|(o, _)| *o == output)
            .map(|(_, e)| *e)
            .expect("install checked every output has an event");
        let event = Event {
            artifact: target.clone(),
            kind: kind.into(),
            payload: body,
        };
        if let Some((version, _)) = all.get_mut(&target) {
            *version += 1;
        }
        self.events.lock().unwrap().push(event.clone());
        Outcome::Landed(event)
    }
}

/// The `launch` move on a job, as Terra would declare it.
pub fn launch_move() -> Move {
    Move {
        name: "launch",
        target: "job",
        payload: vec![("job", Ty::Id)],
        gates: vec![
            Gate {
                path: "job.state",
                equals: json!("ready"),
                gate: "state",
            },
            Gate {
                path: "job.plan.state",
                equals: json!("active"),
                gate: "plan",
            },
            Gate {
                path: "job.waiting_on",
                equals: json!(0),
                gate: "dependencies",
            },
            Gate {
                path: "job.live_session",
                equals: Value::Null,
                gate: "session",
            },
        ],
        needs: vec![
            Need {
                name: "plan_branch",
                ty: Ty::Text,
                path: "job.plan.branch",
            },
            Need {
                name: "brief",
                ty: Ty::Text,
                path: "job.brief",
            },
        ],
        events: vec![
            ("started", "core.job.started"),
            ("failed", "core.job.launch_failed"),
        ],
    }
}

/// The `claim` move on an item. It is all gates, so it has no vine.
pub fn claim_move() -> Move {
    Move {
        name: "claim",
        target: "item",
        payload: vec![("item", Ty::Id), ("claimant", Ty::Text)],
        gates: vec![
            Gate {
                path: "item.state",
                equals: json!("open"),
                gate: "state",
            },
            Gate {
                path: "item.holder",
                equals: Value::Null,
                gate: "held",
            },
        ],
        needs: vec![],
        events: vec![("done", "core.item.claimed")],
    }
}

/// The `activate` move on a plan.
pub fn activate_move() -> Move {
    Move {
        name: "activate",
        target: "plan",
        payload: vec![("plan", Ty::Id)],
        gates: vec![Gate {
            path: "plan.state",
            equals: json!("ready"),
            gate: "state",
        }],
        needs: vec![Need {
            name: "base",
            ty: Ty::Text,
            path: "plan.base",
        }],
        events: vec![
            ("activated", "core.plan.activated"),
            ("failed", "core.plan.activate_failed"),
        ],
    }
}
