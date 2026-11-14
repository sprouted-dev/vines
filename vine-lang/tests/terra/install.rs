//! Installing a trellis: one registry for it, a `move.<name>` stream act
//! for each of its moves, its vines built in order, and every check that
//! can be made before anyone presses anything.
//!
//! Which vine a move starts is Terra's knowledge, so the rule "a vine a
//! move starts is never a stage of another vine" is checked here. The
//! library gives what the check needs: [`Pipe::uses`] lists the vines a
//! vine runs with `use`, and [`Pipe::acts`] the acts it calls, which shows
//! the moves it presses.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::future::ready;
use futures::stream;
use futures::StreamExt;
use vine_lang::pipe::Pipe;
use vine_lang::{OpError, OperatorSpec, Registry, Shape, Ty, Value};

use super::{
    loops, press, Event, Inner, InstallError, Installed, Move, Pick, Reaction, Read, Trellis,
    Trigger, VineSource, ABANDONED,
};

/// The act a vine calls to press `move_name`.
pub(super) fn press_act(move_name: &str) -> String {
    format!("move.{move_name}")
}

pub(super) fn install(inner: &Arc<Inner>, trellis: Trellis) -> Result<(), InstallError> {
    let Trellis {
        vines,
        moves,
        reactions,
    } = trellis;
    check_names(inner, &moves)?;
    let mut reg = inner.base.clone();
    for decl in &moves {
        register_press(&mut reg, inner, decl);
    }
    let pipes = build_vines(&mut reg, vines)?;
    let by_name: BTreeMap<&str, &Pipe> = pipes.iter().map(|p| (p.name(), p)).collect();
    for decl in &moves {
        check_move(decl, &by_name)?;
    }
    check_uses(&moves, &pipes)?;
    for reaction in &reactions {
        check_reaction(reaction, &moves)?;
    }
    loops::check(&moves, &by_name, &reactions)?;

    let mut installed = inner.moves.lock().unwrap();
    for decl in moves {
        let vine = decl.vine.map(|v| by_name[v].clone());
        installed.insert(decl.name.into(), Arc::new(Installed { decl, vine }));
    }
    inner
        .reactions
        .lock()
        .unwrap()
        .extend(reactions.into_iter().map(Arc::new));
    Ok(())
}

fn check_names(inner: &Inner, moves: &[Move]) -> Result<(), InstallError> {
    let installed = inner.moves.lock().unwrap();
    let mut names = BTreeSet::new();
    for decl in moves {
        if installed.contains_key(decl.name) || !names.insert(decl.name) {
            return Err(InstallError::DuplicateMove(decl.name.into()));
        }
        if decl.payload.iter().any(|(f, _)| *f == "request") {
            return Err(InstallError::ReservedField {
                move_name: decl.name.into(),
                field: "request".into(),
            });
        }
    }
    Ok(())
}

/// Register the stream act a vine calls to press `decl`. Its settings are
/// the move's payload plus `request`, the press the calling vine runs
/// under. The press it makes has an id built from that request, the move
/// and the payload, so the same call after a restart attaches to it.
fn register_press(reg: &mut Registry, inner: &Arc<Inner>, decl: &Move) {
    let mut settings = Shape::new().required("request", Ty::Text);
    for (field, ty) in &decl.payload {
        settings = settings.required(field, *ty);
    }
    let spec = OperatorSpec::new(&press_act(decl.name))
        .settings(settings)
        .returns(
            Shape::new()
                .required("output", Ty::Text)
                .required("event", Ty::Text)
                .required("landed", Ty::Bool)
                .required("payload", Ty::Object),
        )
        .ends(
            Shape::new()
                .required("status", Ty::Text)
                .optional("gate", Ty::Text)
                .optional("reason", Ty::Text),
        )
        .touches(["terra"]);
    let weak = Arc::downgrade(inner);
    let name = decl.name.to_owned();
    reg.register_replayable_stream_act(spec, move |mut v, cx| {
        let inner = weak
            .upgrade()
            .expect("Terra outlives the runs of its vines");
        let parent = v["request"].as_str().unwrap_or_default().to_owned();
        if let Some(fields) = v.as_object_mut() {
            fields.remove("request");
        }
        if !inner.store.lock().unwrap().is_running(&parent) {
            let e = OpError::new(
                "no_request",
                format!("`request` must be the running press `{parent}` the vine runs under"),
            );
            return stream::once(ready(Err(e))).boxed();
        }
        let id = format!("{parent}/{name}{v}");
        let from = usize::try_from(cx.resume_from()).expect("a count of items fits in usize");
        press::parts(inner, id, name.clone(), v).skip(from).boxed()
    })
    .expect("move names are checked before their acts are registered");
}

fn build_vines(reg: &mut Registry, vines: Vec<VineSource>) -> Result<Vec<Pipe>, InstallError> {
    let mut pipes = Vec::new();
    for (at, source) in vines.into_iter().enumerate() {
        let built = match source {
            VineSource::Def(def) => reg.build_pipe(def),
            #[cfg(feature = "yaml")]
            VineSource::Yaml(text) => reg.load_pipe(&text),
        };
        let pipe = built.map_err(|error| InstallError::Load { at, error })?;
        reg.add_vine(pipe.clone())
            .map_err(|_| InstallError::DuplicateVine(pipe.name().into()))?;
        pipes.push(pipe);
    }
    Ok(pipes)
}

fn check_move(decl: &Move, vines: &BTreeMap<&str, &Pipe>) -> Result<(), InstallError> {
    let move_name = || decl.name.to_owned();
    let Some(vine_name) = decl.vine else {
        if decl.events.iter().any(|l| l.output == "done") {
            return Ok(());
        }
        return Err(InstallError::NoVineNoDone(move_name()));
    };
    let Some(vine) = vines.get(vine_name) else {
        return Err(InstallError::UnknownVine {
            move_name: move_name(),
            vine: vine_name.into(),
        });
    };
    let mut supplied = Shape::new();
    for (name, ty) in &decl.payload {
        supplied = supplied.required(name, *ty);
    }
    for need in &decl.needs {
        let ty = match need.from {
            Read::Request => Ty::Text,
            Read::Fact(_) => need.ty,
        };
        supplied = supplied.required(need.name, ty);
    }
    supplied
        .fits_into(vine.input())
        .map_err(|problem| InstallError::InputMisfit {
            move_name: move_name(),
            vine: vine_name.into(),
            problem,
        })?;
    if let Some(output) = vine
        .outputs()
        .keys()
        .find(|o| !decl.events.iter().any(|l| l.output == o.as_str()))
    {
        return Err(InstallError::NoEvent {
            move_name: move_name(),
            output: output.clone(),
        });
    }
    if let Some(lands) = decl
        .events
        .iter()
        .find(|l| !vine.outputs().contains_key(l.output))
    {
        return Err(InstallError::NoSuchOutput {
            move_name: move_name(),
            output: lands.output.into(),
        });
    }
    if !vine.resumable() {
        return Err(InstallError::NotResumable {
            vine: vine_name.into(),
        });
    }
    Ok(())
}

/// A vine a move starts runs only when the move is pressed, so its gates
/// are checked and Terra builds its input. A vine that ran it with `use`
/// would skip both.
fn check_uses(moves: &[Move], pipes: &[Pipe]) -> Result<(), InstallError> {
    let started: BTreeMap<&str, &str> = moves
        .iter()
        .filter_map(|m| m.vine.map(|v| (v, m.name)))
        .collect();
    for pipe in pipes {
        if let Some((uses, by)) = pipe
            .uses()
            .iter()
            .find_map(|u| started.get(u.as_str()).map(|by| (u, by)))
        {
            return Err(InstallError::UsesStartedVine {
                vine: pipe.name().into(),
                uses: uses.clone(),
                started_by: (*by).into(),
            });
        }
    }
    Ok(())
}

fn check_reaction(reaction: &Reaction, moves: &[Move]) -> Result<(), InstallError> {
    let find = |name: &str| moves.iter().find(|m| m.name == name);
    if let Trigger::Abandoned { by } = reaction.on {
        if find(by).and_then(|m| m.vine).is_none() {
            return Err(InstallError::UnknownReactionMove(by.into()));
        }
    }
    let Some(decl) = find(reaction.press) else {
        return Err(InstallError::UnknownReactionMove(reaction.press.into()));
    };
    let takes: BTreeSet<&str> = decl.payload.iter().map(|(f, _)| *f).collect();
    let gives: BTreeSet<&str> = reaction.payload.iter().map(|(f, _)| *f).collect();
    if takes != gives {
        return Err(InstallError::ReactionPayload {
            move_name: decl.name.into(),
            problem: format!("the move takes {takes:?} and the reaction gives {gives:?}"),
        });
    }
    Ok(())
}

/// The payload `reaction` presses its move with for `event`, or nothing
/// when the reaction is not for this event.
pub(super) fn reaction_payload(reaction: &Reaction, event: &Event) -> Option<Value> {
    let fires = match reaction.on {
        Trigger::Event(kind) => event.kind == kind,
        Trigger::Abandoned { by } => event.kind == ABANDONED && event.payload["move"] == by,
    };
    if !fires {
        return None;
    }
    let mut payload = Value::Object(Default::default());
    for (field, pick) in &reaction.payload {
        payload[*field] = match pick {
            Pick::Artifact => Value::String(event.artifact.clone()),
            Pick::Field(path) => path
                .split('.')
                .fold(&event.payload, |v, part| &v[part])
                .clone(),
        };
    }
    Some(payload)
}
