//! Fake acts for vines over an endless stream of events, and those vines
//! built in Rust.
//!
//! - `archive.put { seq, body }` archives one event. It refuses an event
//!   whose body is `bad`.
//! - `backup.store { items }` stores a batch of events under a name made
//!   from the first and last `seq`, so storing the same batch again
//!   replaces it.
//! - `feed.pages { seq, pages }` is a stream act: it gives `{ page }` for
//!   each page of one event, from the page after `resume_from`, and ends
//!   with `{ pages }`. Every page can be fetched again, so it can be
//!   registered as a stream act that gives its items again.
//!
//! Every call is a line in the world's log, such as `archive.put 3`,
//! `backup.store 1-5`, `feed.pages 3 from 0` or, after a restart,
//! `feed.pages 3 again from 2`.

use std::sync::Arc;

use futures::stream;
use vine_lang::pipe::{
    buffer_count, call, catch_error, emit_with, filter, fork, pipe, PipeDef, Stage,
};
use vine_lang::{json, OpError, OperatorSpec, Part, Registry, Shape, Ty, Value};

use super::{register_stream_act, Gives, World};

fn seq(v: &Value) -> u64 {
    v["seq"].as_u64().unwrap_or_default()
}

/// A registry with the three feed acts on `world`. `feed.pages` gives
/// only the pages after those it gave, so a saved run saves every page.
pub fn feed_registry(world: &Arc<World>) -> Registry {
    feed_registry_giving(world, Gives::OnlyNew)
}

/// The feed acts, with `feed.pages` registered as a stream act that gives
/// its pages again from any point, so a saved run saves how far it got.
pub fn feed_registry_again(world: &Arc<World>) -> Registry {
    feed_registry_giving(world, Gives::Again)
}

pub fn feed_registry_giving(world: &Arc<World>, gives: Gives) -> Registry {
    let mut reg = Registry::new();

    let w = world.clone();
    reg.register_act(
        OperatorSpec::new("archive.put")
            .settings(
                Shape::new()
                    .required("seq", Ty::Int)
                    .required("body", Ty::Text),
            )
            .returns(Shape::new().required("receipt", Ty::Text))
            .touches(["archive"]),
        move |v, _cx| {
            let w = w.clone();
            async move {
                let n = seq(&v);
                if v["body"] == "bad" {
                    w.note(format!("archive.put {n} refused"));
                    return Err(OpError::new("rejected", format!("event {n} is bad")));
                }
                w.note(format!("archive.put {n}"));
                Ok(json!({ "receipt": format!("r-{n}") }))
            }
        },
    )
    .unwrap();

    let w = world.clone();
    reg.register_act(
        OperatorSpec::new("backup.store")
            .settings(Shape::new().required("items", Ty::List))
            .returns(
                Shape::new()
                    .required("name", Ty::Text)
                    .required("first", Ty::Int)
                    .required("last", Ty::Int),
            )
            .touches(["backup"]),
        move |v, _cx| {
            let w = w.clone();
            async move {
                let items = v["items"].as_array().cloned().unwrap_or_default();
                let first = items.first().map(seq).unwrap_or_default();
                let last = items.last().map(seq).unwrap_or_default();
                w.note(format!("backup.store {first}-{last}"));
                Ok(json!({
                    "name": format!("backup/{first:06}-{last:06}"),
                    "first": first,
                    "last": last,
                }))
            }
        },
    )
    .unwrap();

    let w = world.clone();
    register_stream_act(
        &mut reg,
        gives,
        OperatorSpec::new("feed.pages")
            .settings(
                Shape::new()
                    .required("seq", Ty::Int)
                    .required("pages", Ty::Int),
            )
            .returns(Shape::new().required("page", Ty::Int))
            .ends(Shape::new().required("pages", Ty::Int))
            .touches(["feed"]),
        move |v, cx| {
            let (n, pages, from) = (seq(&v), v["pages"].as_u64().unwrap(), cx.resume_from());
            // The first call for an event starts its feed. A later one,
            // after a restart, goes on from `resume_from`.
            let again = if w.count(&format!("feed.pages {n} ")) == 0 {
                ""
            } else {
                "again "
            };
            w.note(format!("feed.pages {n} {again}from {from}"));
            let mut parts: Vec<Result<Part, OpError>> = (from + 1..=pages)
                .map(|page| Ok(Part::Item(json!({ "page": page }))))
                .collect();
            parts.push(Ok(Part::End(json!({ "pages": pages }))));
            stream::iter(parts)
        },
    );

    reg
}

fn event() -> Shape {
    Shape::new()
        .required("seq", Ty::Int)
        .required("body", Ty::Text)
}

fn archived() -> Shape {
    Shape::new()
        .required("seq", Ty::Int)
        .required("receipt", Ty::Text)
}

fn relay_stages() -> Vec<Stage> {
    vec![
        call(
            "archive.put",
            json!({ "seq": "$item.seq", "body": "$item.body" }),
        )
        .into("archived"),
        emit_with(
            "archived",
            json!({ "seq": "$item.seq", "receipt": "$item.archived.receipt" }),
        ),
    ]
}

/// `vines/pipe/event_relay.yaml`, built in Rust.
pub fn event_relay_pipe() -> PipeDef {
    pipe("event_relay")
        .input(event())
        .output("archived", archived())
        .stages(relay_stages())
}

/// `event_relay` with a `catchError`: an event the archive refuses becomes one
/// `refused` output, and the stream goes on. The act keeps its path,
/// `pipe[0]`, so a bookmark saved by `event_relay` fits it.
pub fn event_relay_caught_pipe() -> PipeDef {
    let mut stages = relay_stages();
    stages.push(catch_error([emit_with(
        "refused",
        json!({ "seq": "$item.seq", "reason": "$item.error.message" }),
    )]));
    pipe("event_relay")
        .input(event())
        .output("archived", archived())
        .output(
            "refused",
            Shape::new()
                .required("seq", Ty::Int)
                .required("reason", Ty::Text),
        )
        .stages(stages)
}

/// `vines/pipe/event_backup.yaml`, built in Rust.
pub fn event_backup_pipe() -> PipeDef {
    pipe("event_backup")
        .input(event())
        .output(
            "stored",
            Shape::new()
                .required("name", Ty::Text)
                .required("first", Ty::Int)
                .required("last", Ty::Int)
                .required("count", Ty::Int),
        )
        .stages([
            buffer_count(5),
            call("backup.store", json!({ "items": "$item.items" })).into("stored"),
            emit_with(
                "stored",
                json!({
                    "name": "$item.stored.name",
                    "first": "$item.stored.first",
                    "last": "$item.stored.last",
                    "count": "$item.count",
                }),
            ),
        ])
}

/// `vines/pipe/event_pages.yaml`, built in Rust.
pub fn event_pages_pipe() -> PipeDef {
    pipe("event_pages")
        .input(
            Shape::new()
                .required("seq", Ty::Int)
                .required("pages", Ty::Int),
        )
        .output(
            "page",
            Shape::new()
                .required("seq", Ty::Int)
                .required("page", Ty::Int),
        )
        .output(
            "fetched",
            Shape::new()
                .required("seq", Ty::Int)
                .required("pages", Ty::Int),
        )
        .stages([
            call(
                "feed.pages",
                json!({ "seq": "$item.seq", "pages": "$item.pages" }),
            )
            .into("page")
            .end("ended"),
            fork([vec![
                filter("$item.page"),
                emit_with(
                    "page",
                    json!({ "seq": "$item.seq", "page": "$item.page.page" }),
                ),
            ]]),
            filter("$item.ended"),
            emit_with(
                "fetched",
                json!({ "seq": "$item.seq", "pages": "$item.ended.pages" }),
            ),
        ])
}

/// Events `{ seq, body }` numbered from `first` to `last`.
pub fn events(first: u64, last: u64) -> Vec<Value> {
    (first..=last)
        .map(|n| json!({ "seq": n, "body": format!("event {n}") }))
        .collect()
}
