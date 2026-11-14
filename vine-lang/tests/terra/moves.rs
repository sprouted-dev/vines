//! The moves Terra declares in these tests, and the trellis that holds
//! them with the vines of `vines/pipe`.

use vine_lang::{json, Ty, Value};

use super::{Gate, Lands, Move, Need, Pick, Reaction, Trellis, Trigger};

/// The `launch` move on a job. Its `started` event moves the job on, so
/// a second launch of it is refused by `state`.
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
            Need::fact("plan_branch", Ty::Text, "job.plan.branch"),
            Need::fact("brief", Ty::Text, "job.brief"),
        ],
        vine: Some("job_launch"),
        events: vec![
            Lands::new("started", "core.job.started").sets("state", json!("in_progress")),
            Lands::new("failed", "core.job.launch_failed"),
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
        vine: None,
        events: vec![Lands::new("done", "core.item.claimed")],
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
        needs: vec![Need::fact("base", Ty::Text, "plan.base")],
        vine: Some("plan_activate"),
        events: vec![
            Lands::new("activated", "core.plan.activated").sets("state", json!("active")),
            Lands::new("failed", "core.plan.activate_failed"),
        ],
    }
}

/// The `launch_all` move on a plan. Its vine presses `launch` for each
/// job, so it needs its own request id and the plan's job ids, nothing
/// about any job.
pub fn launch_all_move() -> Move {
    Move {
        name: "launch_all",
        target: "plan",
        payload: vec![("plan", Ty::Id)],
        gates: vec![Gate {
            path: "plan.state",
            equals: json!("active"),
            gate: "state",
        }],
        needs: vec![Need::request(), Need::fact("jobs", Ty::List, "plan.jobs")],
        vine: Some("plan_launch_all"),
        events: vec![
            Lands::new("launched", "core.plan.job_launched"),
            Lands::new("not_launched", "core.plan.job_not_launched"),
            Lands::new("refused", "core.plan.launch_refused"),
            Lands::new("done", "core.plan.launched_all"),
        ],
    }
}

/// The `undo_launch` move on a job: undo a launch Terra abandoned. Its
/// gate keeps it from tearing down a later launch of the same job that
/// did land: that one moved the job out of `ready`.
pub fn undo_launch_move() -> Move {
    Move {
        name: "undo_launch",
        target: "job",
        payload: vec![("job", Ty::Id), ("output", Ty::Text), ("work", Ty::Object)],
        gates: vec![Gate {
            path: "job.state",
            equals: json!("ready"),
            gate: "state",
        }],
        needs: vec![],
        vine: Some("job_launch_undo"),
        events: vec![
            Lands::new("undone", "core.job.launch_undone"),
            Lands::new("nothing_to_undo", "core.job.nothing_to_undo"),
            Lands::new("failed", "core.job.undo_failed"),
        ],
    }
}

/// When a launch is abandoned, press `undo_launch` with what it made.
pub fn undo_abandoned_launch() -> Reaction {
    Reaction {
        on: Trigger::Abandoned { by: "launch" },
        press: "undo_launch",
        payload: vec![
            ("job", Pick::Artifact),
            ("output", Pick::Field("output")),
            ("work", Pick::Field("payload")),
        ],
    }
}

/// A vine of `vines/pipe`, as YAML.
#[cfg(feature = "yaml")]
pub fn yaml(name: &str) -> super::VineSource {
    let path = format!("{}/vines/pipe/{name}.yaml", env!("CARGO_MANIFEST_DIR"));
    super::VineSource::Yaml(
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}")),
    )
}

/// The jobs trellis: launching jobs one by one or a plan's at once,
/// activating plans, and undoing abandoned launches. `job_teardown` is a
/// helper that `job_launch_undo` uses; no move starts it.
#[cfg(feature = "yaml")]
pub fn jobs_trellis() -> Trellis {
    Trellis {
        vines: [
            "job_teardown",
            "job_launch_undo",
            "job_launch",
            "plan_activate",
            "plan_launch_all",
        ]
        .into_iter()
        .map(yaml)
        .collect(),
        moves: vec![
            launch_move(),
            activate_move(),
            launch_all_move(),
            undo_launch_move(),
        ],
        reactions: vec![undo_abandoned_launch()],
    }
}

/// A trellis of one move of only gates.
pub fn items_trellis() -> Trellis {
    Trellis {
        vines: vec![],
        moves: vec![claim_move()],
        reactions: vec![],
    }
}
