//! `job_run` and `event_pages` written in YAML are saved by position the
//! same way as their Rust twins, when their stream act gives its items
//! again.
#![cfg(feature = "yaml")]

mod acts;

use std::sync::Arc;

use acts::agent::{expected, job, job_run_registry_again, Host};
use acts::feed::{event_pages_pipe, feed_registry_again};
use acts::restart::{
    events_saved, run, run_saved, run_saved_with_kills, run_with_kills, Caller, End, Keeper,
};
use acts::World;
use vine_lang::{json, Pipe, Value};

fn text(name: &str) -> String {
    let path = format!("{}/vines/pipe/{name}.yaml", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn job_run(world: &Arc<World>, host: &Arc<Host>) -> Pipe {
    job_run_registry_again(world, host)
        .load_pipe(&text("job_run"))
        .unwrap()
}

fn pages(world: &Arc<World>) -> Pipe {
    feed_registry_again(world)
        .load_pipe(&text("event_pages"))
        .unwrap()
}

#[test]
fn the_yaml_vines_save_their_stream_act_by_position() {
    let (world, host) = (World::new(), Host::new());
    assert_eq!(job_run(&world, &host).saves_every_item(), []);
    assert_eq!(pages(&world).saves_every_item(), []);
    let rust = feed_registry_again(&world)
        .build_pipe(event_pages_pipe())
        .unwrap();
    assert_eq!(rust.saves_every_item(), pages(&world).saves_every_item());
}

#[tokio::test(start_paused = true)]
async fn job_run_from_yaml_killed_at_many_points_gives_what_an_uninterrupted_run_gives() {
    let events = events_saved(&job_run(&World::new(), &Host::new()), &[job()]).await;
    for k in (1..=events).step_by(7) {
        let (world, host) = (World::new(), Host::new());
        let mut keeper = Keeper::new();
        run_saved_with_kills(
            || job_run(&world, &host),
            &[job()],
            &mut keeper,
            &[k, k / 2 + 1],
        )
        .await;
        assert_eq!(keeper.outs, expected(120, "finished"), "kill {k}");
        assert_eq!(world.count("agent.start"), 1);
        assert_eq!(world.count("git.worktree_add"), 1);
        assert_eq!(world.count("git.worktree_remove"), 1);
        assert!(
            keeper.largest <= 1_500,
            "kill {k}: {} bytes",
            keeper.largest
        );
    }
    let mut keeper = Keeper::new();
    let end = run_saved(
        &job_run(&World::new(), &Host::new()),
        &[job()],
        &mut keeper,
        None,
    )
    .await;
    assert!(matches!(end, End::Ended));
}

#[tokio::test]
async fn event_pages_from_yaml_with_a_long_event_keeps_a_small_bookmark() {
    let input: Vec<Value> = vec![
        json!({ "seq": 1, "pages": 3 }),
        json!({ "seq": 2, "pages": 2000 }),
    ];
    let want = {
        let mut caller = Caller::new();
        run(&pages(&World::new()), &input, &mut caller, None).await;
        caller.outs
    };
    let world = World::new();
    let mut caller = Caller::new();
    run_with_kills(|| pages(&world), &input, &mut caller, &[500, 1500, 3000]).await;
    assert_eq!(caller.outs, want);
    assert_eq!(world.count("feed.pages 2 from 0"), 1);
    assert!(caller.largest <= 600, "{} bytes", caller.largest);
}
