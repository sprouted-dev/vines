//! The endless vines written in YAML are the same vines as their Rust
//! twins, and run from a saved position the same way.
#![cfg(test)]
#![cfg(feature = "yaml")]

mod fake;

use std::sync::Arc;

use fake::feed::{event_backup_pipe, event_pages_pipe, event_relay_pipe, events, feed_registry};
use fake::restart::{assert_forward, events_in, run, run_with_kills, started, Caller, End};
use fake::World;
use futures::{stream, StreamExt};
use vine_lang::pipe::{Pipe, PipeDef};
use vine_lang::{json, Bookmark, RunError, Value};

fn text(name: &str) -> String {
    let path = format!("{}/vines/pipe/{name}.yaml", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn from_yaml(world: &Arc<World>, name: &str) -> Pipe {
    feed_registry(world)
        .load_pipe(&text(name))
        .unwrap_or_else(|e| panic!("{name} loads: {e}"))
}

/// A vine's name, its Rust twin, and input to run it on.
type Twin = (&'static str, fn() -> PipeDef, Vec<Value>);

fn twins() -> Vec<Twin> {
    let paged = (1..=4)
        .map(|n| json!({ "seq": n, "pages": n % 3 }))
        .collect();
    vec![
        ("event_relay", event_relay_pipe, events(1, 6)),
        ("event_backup", event_backup_pipe, events(1, 12)),
        ("event_pages", event_pages_pipe, paged),
    ]
}

#[test]
fn each_endless_vine_is_the_same_in_rust_and_in_yaml() {
    let world = World::new();
    for (name, def, _) in twins() {
        let (a, b) = (
            feed_registry(&world).build_pipe(def()).unwrap(),
            from_yaml(&world, name),
        );
        assert_eq!(a.name(), b.name());
        assert_eq!(a.input(), b.input());
        assert_eq!(a.outputs(), b.outputs());
        assert_eq!(a.reach(), b.reach());
        assert!(
            a.resumable_from_position() && b.resumable_from_position(),
            "{name}"
        );
    }
}

#[tokio::test]
async fn each_endless_vine_from_yaml_killed_anywhere_gives_what_its_rust_twin_gives() {
    for (name, def, input) in twins() {
        let world = World::new();
        let mut want = Caller::new();
        let twin = feed_registry(&world).build_pipe(def()).unwrap();
        assert!(matches!(
            run(&twin, &input, &mut want, None).await,
            End::Ended
        ));
        let want_log = world.log();
        let events = events_in(&from_yaml(&World::new(), name), &input).await;
        for k in 1..events {
            let world = World::new();
            let mut caller = Caller::new();
            run_with_kills(|| from_yaml(&world, name), &input, &mut caller, &[k]).await;
            assert_eq!(caller.outs, want.outs, "{name} killed after {k}");
            assert_eq!(started(world.log()), want_log, "{name} killed after {k}");
            assert_forward(&caller.throughs);
            assert_eq!(caller.position(), input.len() as u64);
        }
    }
}

#[tokio::test]
async fn a_yaml_vine_that_counts_the_whole_stream_is_refused_from_a_position() {
    let world = World::new();
    let vine = feed_registry(&world)
        .load_pipe(
            "vine: first_three
input: { seq: int, body: text }
outputs:
  out: { seq: int }
pipe:
  - take: 3
  - emit: { to: out, with: { seq: $item.seq } }
",
        )
        .unwrap();
    assert!(vine.resumable());
    assert!(!vine.resumable_from_position());
    let got: Vec<_> = vine
        .run_from(stream::iter(events(1, 1)), Bookmark::new())
        .collect()
        .await;
    assert!(
        matches!(&got[..], [Err(RunError::NotResumable { why, .. })] if why.starts_with("pipe[0] is a `take`")),
        "{got:?}"
    );
}
