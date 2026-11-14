//! A stream operator that gives its items again is saved by position: what the
//! caller keeps for one item in flight stays small however many items the
//! operator gives, and a restart at any point gives what an uninterrupted run
//! gives. Rust alone: this file passes with YAML off.

mod fake;

use std::sync::Arc;

use fake::agent::{
    expected, job, job_run_pipe, job_run_registry, job_run_registry_again, Host, Script,
};
use fake::feed::{event_pages_pipe, feed_registry, feed_registry_again};
use fake::restart::{
    events_in, events_saved, run, run_saved, run_saved_with_kills, run_with_kills, Caller, End,
    Keeper,
};
use fake::{register_stream, Gives, World};
use futures::{stream, StreamExt};
use vine_lang::pipe::{
    buffer_count, call, catch_error, concat_map, emit, emit_with, filter, finalize, fork, last,
    pipe, retry, take, tap, use_vine, Operator, PipeDef,
};
use vine_lang::{
    json, Bookmark, Ending, Entry, Event, Journal, Line, OpError, OperatorSpec, Part, Pipe,
    Progress, Registry, RunError, Shape, Ty, Value,
};

type Build = fn(&Arc<World>, &Arc<Host>) -> Pipe;

fn job_run(world: &Arc<World>, host: &Arc<Host>) -> Pipe {
    job_run_registry_again(world, host)
        .build_pipe(job_run_pipe())
        .unwrap()
}

fn job_run_every_item(world: &Arc<World>, host: &Arc<Host>) -> Pipe {
    job_run_registry(world, host)
        .build_pipe(job_run_pipe())
        .unwrap()
}

/// The operators in the world's log, without the agent attaching again after a
/// restart: what is left is what was done.
fn calls_done(log: Vec<String>) -> Vec<String> {
    log.into_iter()
        .filter(|l| !l.starts_with("agent.attach"))
        .collect()
}

/// An uninterrupted `run_saved` of one job: its outputs, its operators, and how
/// many events it gave.
async fn uninterrupted(
    build: Build,
    script: &Script,
) -> (Vec<(String, Value)>, Vec<String>, usize) {
    let (world, host) = (World::new(), Host::new());
    host.script("job-1", script.clone());
    let mut keeper = Keeper::new();
    let end = run_saved(&build(&world, &host), &[job()], &mut keeper, None).await;
    assert!(matches!(end, End::Ended), "{end:?}");
    let probe = Host::new();
    probe.script("job-1", script.clone());
    let events = events_saved(&build(&World::new(), &probe), &[job()]).await;
    (keeper.outs, world.log(), events)
}

/// Kill one job's `run_saved` after each count of events in `kills`, then
/// run it to the end, and check it did what an uninterrupted run does.
/// Gives what the caller kept.
async fn check_kills(build: Build, script: &Script, kills: &[usize]) -> Keeper {
    let (want, want_log, _) = uninterrupted(build, script).await;
    let (world, host) = (World::new(), Host::new());
    host.script("job-1", script.clone());
    let mut keeper = Keeper::new();
    let repeats = run_saved_with_kills(|| build(&world, &host), &[job()], &mut keeper, kills).await;
    assert_eq!(keeper.outs, want, "kills {kills:?}");
    assert_eq!(
        calls_done(world.log()),
        calls_done(want_log),
        "kills {kills:?}"
    );
    assert_eq!(world.count("agent.start"), 1, "kills {kills:?}");
    assert!(world.trees().is_empty(), "kills {kills:?}");
    assert!(repeats <= kills.len());
    keeper
}

/// Every single kill point, and pairs of kill points in a row on a grid.
async fn check_every_kill(build: Build, script: &Script, bound: usize) {
    let (_, _, events) = uninterrupted(build, script).await;
    for k in 1..=events {
        let keeper = check_kills(build, script, &[k]).await;
        assert!(
            keeper.largest <= bound,
            "kill {k}: {} bytes",
            keeper.largest
        );
    }
    for k1 in (1..events).step_by(13) {
        for k2 in (1..events).step_by(17) {
            let keeper = check_kills(build, script, &[k1, k2]).await;
            assert!(
                keeper.largest <= bound,
                "kills {k1}, {k2}: {} bytes",
                keeper.largest
            );
        }
    }
}

/// The most bytes of JSON the journal of one job ever holds for a run of
/// `job_run` by position, killed at ten points.
const JOB_RUN_BOUND: usize = 1_500;

// --------------------------------------------------------------- job_run

#[test]
fn job_run_with_an_agent_that_gives_its_lines_again_saves_no_call_item_by_item() {
    let (world, host) = (World::new(), Host::new());
    assert_eq!(job_run(&world, &host).saves_every_item(), []);
    let every = job_run_every_item(&world, &host);
    let [only] = every.saves_every_item() else {
        panic!("{:?}", every.saves_every_item());
    };
    assert_eq!(
        (only.path.as_str(), only.operator.as_str()),
        ("pipe[1]", "agent.run")
    );
    assert!(
        only.why
            .contains("does not say it can give its items again"),
        "{}",
        only.why
    );
}

#[tokio::test(start_paused = true)]
async fn job_run_by_position_saves_how_far_the_agent_got_and_not_its_lines() {
    let (world, host) = (World::new(), Host::new());
    let mut keeper = Keeper::new();
    run_saved(&job_run(&world, &host), &[job()], &mut keeper, None).await;
    assert_eq!(keeper.outs, expected(120, "finished"));
    let entries = keeper.journal().entries().to_vec();
    assert!(
        !entries
            .iter()
            .any(|e| matches!(e, Entry::Item { .. } | Entry::Emitted { .. })),
        "{entries:?}"
    );
    let stopped = Entry::Stopped {
        at: "pipe[1]#0".into(),
        items: 120,
        how: Ending::Ended(json!({ "status": "finished", "lines": 120 })),
    };
    let through = Entry::Through {
        at: "pipe[1]#0".into(),
        items: 121,
        handed: 0,
    };
    assert!(
        entries.contains(&stopped) && entries.contains(&through),
        "{entries:?}"
    );
    assert_eq!(
        entries.len(),
        5,
        "add, started, stopped, through, remove: {entries:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn job_run_by_position_killed_anywhere_once_or_twice_gives_what_an_uninterrupted_run_gives() {
    check_every_kill(job_run, &Script::new(120, "finished"), JOB_RUN_BOUND).await;
}

#[tokio::test(start_paused = true)]
async fn job_run_by_position_killed_anywhere_after_the_agent_host_is_lost_still_fails_once() {
    let script = Script {
        lost_after: Some(70),
        ..Script::new(120, "finished")
    };
    let (want, _, _) = uninterrupted(job_run, &script).await;
    assert_eq!(want.last().unwrap().0, "failed");
    check_every_kill(job_run, &script, JOB_RUN_BOUND).await;
}

#[tokio::test(start_paused = true)]
async fn job_run_by_position_killed_anywhere_with_a_commit_that_fails_removes_the_copy_once() {
    // A failure of an operator run for the end item, not of the agent.
    async fn failing_commit(kills: &[usize]) -> (Arc<World>, Keeper) {
        let (world, host) = (World::new(), Host::new());
        host.script("job-1", Script::new(60, "finished"));
        world.fail("git.commit", "empty", "nothing to commit");
        let mut keeper = Keeper::new();
        run_saved_with_kills(|| job_run(&world, &host), &[job()], &mut keeper, kills).await;
        (world, keeper)
    }
    let (world, keeper) = failing_commit(&[]).await;
    let want = keeper.outs;
    assert_eq!(
        want.last().unwrap(),
        &(
            "failed".into(),
            json!({ "job": "job-1", "reason": "nothing to commit" })
        )
    );
    let events = {
        let host = Host::new();
        host.script("job-1", Script::new(60, "finished"));
        let w = World::new();
        w.fail("git.commit", "empty", "nothing to commit");
        events_saved(&job_run(&w, &host), &[job()]).await
    };
    assert_eq!(world.count("git.worktree_remove"), 1);
    for k in 1..=events {
        let (world, keeper) = failing_commit(&[k]).await;
        assert_eq!(keeper.outs, want, "kill {k}");
        assert_eq!(world.count("git.worktree_remove"), 1, "kill {k}");
        assert!(world.trees().is_empty(), "kill {k}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_restart_mid_batch_attaches_from_the_last_whole_batch_and_rebuilds_the_summary() {
    let (world, host) = (World::new(), Host::new());
    let mut keeper = Keeper::new();
    // Kill right after the caller saved that log line 70 was handed out.
    let kill = {
        let probe = job_run(&World::new(), &Host::new());
        let mut run = probe.run_saved(stream::iter([job()]), Journal::new());
        let mut k = 0;
        let mut seventy = false;
        while let Some(event) = run.next().await {
            k += 1;
            match event.unwrap() {
                Event::Output(o) if o.payload["seq"] == 70 => seventy = true,
                Event::Save(Entry::Through { .. }) if seventy => break,
                _ => {}
            }
        }
        k
    };
    run_saved(&job_run(&world, &host), &[job()], &mut keeper, Some(kill)).await;
    let through = keeper.journal().entries().iter().find_map(|e| match e {
        Entry::Through { items, handed, .. } => Some((*items, *handed)),
        _ => None,
    });
    // Lines 51 to 70 wait in the half full batch, and their 20 log lines
    // were handed out.
    assert_eq!(through, Some((50, 20)));
    run_saved(&job_run(&world, &host), &[job()], &mut keeper, None).await;
    assert_eq!(keeper.outs, expected(120, "finished"));
    assert_eq!(
        world.log(),
        [
            "git.worktree_add job-1",
            "agent.start job-1",
            "agent.attach job-1 from 50",
            "git.commit /trees/job-1",
            "git.worktree_remove /trees/job-1",
        ]
    );
}

/// The largest journal of one job of `lines` lines killed at ten points,
/// saved by position, and the size of the journal of the same job saved
/// item by item when it ends.
async fn journal_sizes(lines: u64) -> (usize, usize) {
    let script = Script {
        every: std::time::Duration::from_millis(10),
        ..Script::new(lines, "finished")
    };
    let probe = Host::new();
    probe.script("job-1", script.clone());
    let events = events_saved(&job_run(&World::new(), &probe), &[job()]).await;
    let kills: Vec<usize> = (1..=10).map(|i| i * events / 11).collect();
    let (world, host) = (World::new(), Host::new());
    host.script("job-1", script.clone());
    let mut keeper = Keeper::new();
    run_saved_with_kills(|| job_run(&world, &host), &[job()], &mut keeper, &kills).await;
    assert_eq!(keeper.outs, expected(lines, "finished"), "{lines} lines");
    assert_eq!(world.count("agent.start"), 1);

    let host = Host::new();
    host.script("job-1", script);
    let mut journal = Journal::new();
    let mut run =
        job_run_every_item(&World::new(), &host).run_saved(stream::iter([job()]), Journal::new());
    while let Some(event) = run.next().await {
        journal.record(&event.unwrap());
    }
    (keeper.largest, journal.to_json().to_string().len())
}

#[tokio::test(start_paused = true)]
async fn what_the_caller_keeps_for_one_job_does_not_grow_with_the_lines_its_agent_writes() {
    let mut largest = Vec::new();
    for lines in [100, 1_000, 10_000] {
        let (by_position, every_item) = journal_sizes(lines).await;
        eprintln!("{lines} lines: by position at most {by_position} bytes, item by item {every_item} bytes");
        assert!(
            by_position <= JOB_RUN_BOUND,
            "{lines} lines: {by_position} bytes"
        );
        largest.push((by_position, every_item));
    }
    // Item by item grows with the lines; by position it does not.
    assert!(largest[2].1 > 50 * largest[0].1);
    assert!(largest[2].0 < largest[0].0 + 50, "{largest:?}");
}

// ------------------------------------------------- job_run from a position

fn jobs(n: u64) -> Vec<Value> {
    (1..=n)
        .map(|i| json!({ "job": format!("job-{i}"), "plan_branch": "plan/p-1", "brief": "go" }))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn job_run_by_position_over_a_stream_of_jobs_keeps_a_small_bookmark_at_every_kill() {
    let input = jobs(2);
    let script_all = |host: &Arc<Host>| {
        for j in ["job-1", "job-2"] {
            host.script(j, Script::new(60, "finished"));
        }
    };
    let (want, want_log, events) = {
        let (world, host) = (World::new(), Host::new());
        script_all(&host);
        let mut caller = Caller::new();
        run(&job_run(&world, &host), &input, &mut caller, None).await;
        let probe = Host::new();
        script_all(&probe);
        let events = events_in(&job_run(&World::new(), &probe), &input).await;
        (caller.outs, world.log(), events)
    };
    for k in (1..events).step_by(3) {
        let (world, host) = (World::new(), Host::new());
        script_all(&host);
        let mut caller = Caller::new();
        run_with_kills(
            || job_run(&world, &host),
            &input,
            &mut caller,
            &[k, k / 2 + 1],
        )
        .await;
        assert_eq!(caller.outs, want, "kill {k}");
        assert_eq!(
            calls_done(world.log()),
            calls_done(want_log.clone()),
            "kill {k}"
        );
        assert_eq!(caller.position(), 2);
        assert!(caller.bookmark().is_empty());
        assert!(
            caller.largest <= JOB_RUN_BOUND,
            "kill {k}: {} bytes",
            caller.largest
        );
    }
}

// ----------------------------------------------------------- event_pages

fn pages(world: &Arc<World>) -> Pipe {
    feed_registry_again(world)
        .build_pipe(event_pages_pipe())
        .unwrap()
}

fn pages_every_item(world: &Arc<World>) -> Pipe {
    feed_registry(world).build_pipe(event_pages_pipe()).unwrap()
}

/// Run `event_pages` killed after each count in `kills`, and check it gave
/// what an uninterrupted run gives, fetched each event's pages once, and
/// kept a bookmark of at most `bound` bytes. Gives the largest bookmark.
async fn check_pages(build: fn(&Arc<World>) -> Pipe, input: &[Value], kills: &[usize]) -> usize {
    let want = {
        let mut caller = Caller::new();
        run(&build(&World::new()), input, &mut caller, None).await;
        caller.outs
    };
    let world = World::new();
    let mut caller = Caller::new();
    run_with_kills(|| build(&world), input, &mut caller, kills).await;
    assert_eq!(caller.outs, want, "kills {kills:?}");
    for event in input {
        let first = format!("feed.pages {} from 0", event["seq"]);
        assert_eq!(world.count(&first), 1, "kills {kills:?}");
    }
    assert_eq!(caller.position(), input.len() as u64);
    caller.largest
}

#[tokio::test]
async fn event_pages_by_position_killed_anywhere_once_or_twice_gives_each_page_once() {
    let input: Vec<Value> = (1..=4)
        .zip([3, 0, 7, 2])
        .map(|(n, pages)| json!({ "seq": n, "pages": pages }))
        .collect();
    let events = events_in(&pages(&World::new()), &input).await;
    for k in 1..=events {
        assert!(check_pages(pages, &input, &[k]).await <= 600, "kill {k}");
    }
    for k1 in 1..events {
        for k2 in (1..events).step_by(3) {
            check_pages(pages, &input, &[k1, k2]).await;
        }
    }
}

/// The largest bookmark an uninterrupted run keeps, in bytes of JSON. It
/// measures only when the bookmark grew, so a bookmark that only grows
/// is not turned into text for every event.
async fn largest_bookmark(vine: &Pipe, input: &[Value]) -> usize {
    let mut bookmark = Bookmark::new();
    let (mut most, mut largest) = (0, 0);
    let mut run = vine.run_from(stream::iter(input.to_vec()), Bookmark::new());
    while let Some(progress) = run.next().await {
        bookmark.record(&progress.unwrap());
        if bookmark.len() > most {
            most = bookmark.len();
            if most.is_power_of_two() {
                largest = largest.max(bookmark.to_json().to_string().len());
            }
        }
    }
    largest
}

#[tokio::test]
async fn event_pages_by_position_keeps_a_bookmark_that_does_not_grow_with_the_pages_of_an_event() {
    let mut sizes = Vec::new();
    for count in [100, 1_000, 10_000] {
        let input = vec![
            json!({ "seq": 1, "pages": 2 }),
            json!({ "seq": 2, "pages": count }),
            json!({ "seq": 3, "pages": 1 }),
        ];
        let events = events_in(&pages(&World::new()), &input).await;
        let kills: Vec<usize> = (1..=8).map(|i| i * events / 9).collect();
        let by_position = check_pages(pages, &input, &kills).await;
        let every_item = largest_bookmark(&pages_every_item(&World::new()), &input).await;
        eprintln!("{count} pages: by position at most {by_position} bytes, item by item {every_item} bytes");
        assert!(by_position <= 600, "{count} pages: {by_position} bytes");
        sizes.push((by_position, every_item));
    }
    assert!(sizes[2].1 > 50 * sizes[0].1, "{sizes:?}");
}

// ----------------------------------- a call that a catchError takes over

/// Like `job_run`, with an operator run for each line that refuses line 70: the
/// `catchError` takes over, and the agent's stream is abandoned partway.
fn checked_run(world: &Arc<World>, host: &Arc<Host>) -> Pipe {
    let mut reg = job_run_registry_again(world, host);
    let w = world.clone();
    reg.register(
        OperatorSpec::new("check.line")
            .settings(Shape::new().required("seq", Ty::Int))
            .touches(["check"]),
        move |v, _cx| {
            let w = w.clone();
            async move {
                let seq = v["seq"].as_u64().unwrap();
                w.note(format!("check.line {seq}"));
                if seq == 70 {
                    return Err(OpError::new("bad", "line 70 is bad"));
                }
                Ok(json!({}))
            }
        },
    )
    .unwrap();
    let job = || Shape::new().required("job", Ty::Id);
    let def = pipe("checked_run")
        .input(
            job()
                .required("plan_branch", Ty::Text)
                .required("brief", Ty::Text),
        )
        .output("log_line", job().required("seq", Ty::Int))
        .output("done", job())
        .output("failed", job().required("reason", Ty::Text))
        .operators([
            call(
                "agent.run",
                json!({ "key": "$item.job", "dir": "/trees/x", "brief": "$item.brief" }),
            )
            .into("line"),
            fork([vec![emit_with(
                "log_line",
                json!({ "job": "$item.job", "seq": "$item.line.seq" }),
            )]]),
            tap("check.line", json!({ "seq": "$item.line.seq" })),
            last(),
            emit_with("done", json!({ "job": "$item.job" })),
            catch_error([emit_with(
                "failed",
                json!({ "job": "$item.job", "reason": "$item.error.message" }),
            )]),
        ]);
    reg.build_pipe(def).unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_call_a_catch_error_took_over_is_not_read_past_where_the_first_run_stopped() {
    let script = Script::new(120, "finished");
    let (want, want_log, events) = uninterrupted(checked_run, &script).await;
    assert_eq!(want.len(), 71, "70 lines and one failure: {want:?}");
    assert_eq!(want[70].0, "failed");
    for k in 1..=events {
        let (world, host) = (World::new(), Host::new());
        host.script("job-1", script.clone());
        let mut keeper = Keeper::new();
        run_saved_with_kills(|| checked_run(&world, &host), &[job()], &mut keeper, &[k]).await;
        assert_eq!(keeper.outs, want, "kill {k}");
        // Each line was checked once, so every operator ran once.
        assert_eq!(
            calls_done(world.log()),
            calls_done(want_log.clone()),
            "kill {k}"
        );
        assert!(
            keeper.largest <= JOB_RUN_BOUND,
            "kill {k}: {} bytes",
            keeper.largest
        );
    }
    // Once the failure is handled, the journal says the call was abandoned.
    let (world, host) = (World::new(), Host::new());
    let mut keeper = Keeper::new();
    run_saved(&checked_run(&world, &host), &[job()], &mut keeper, None).await;
    assert!(keeper.journal().entries().contains(&Entry::Stopped {
        at: "pipe[0]#0".into(),
        items: 70,
        how: Ending::Abandoned,
    }));
    // The finished run's journal replays to nothing more.
    let log = world.log();
    let mut again = Keeper {
        saved: keeper.saved.clone(),
        ..Keeper::new()
    };
    run_saved(&checked_run(&world, &host), &[job()], &mut again, None).await;
    assert!(again.outs.is_empty(), "{:?}", again.outs);
    assert_eq!(world.log(), log);
}

// ------------------------------------- an operator that does not keep its word

#[tokio::test(start_paused = true)]
async fn an_operator_that_gives_fewer_items_again_than_it_gave_before_it_stopped_is_a_run_error() {
    let script = Script {
        forgets_after: Some(110),
        ..Script::new(120, "finished")
    };
    let (world, host) = (World::new(), Host::new());
    host.script("job-1", script);
    // Kill right after the caller saved that the agent's stream stopped.
    // The last 20 lines still wait in the batch, so the position is 100.
    let kill = {
        let probe = job_run(&World::new(), &Host::new());
        let mut run = probe.run_saved(stream::iter([job()]), Journal::new());
        let mut k = 0;
        while let Some(event) = run.next().await {
            k += 1;
            if matches!(event.unwrap(), Event::Save(Entry::Stopped { .. })) {
                break;
            }
        }
        k
    };
    let mut keeper = Keeper::new();
    run_saved(&job_run(&world, &host), &[job()], &mut keeper, Some(kill)).await;
    let end = run_saved(&job_run(&world, &host), &[job()], &mut keeper, None).await;
    match end {
        End::Failed(RunError::ReplayMisfit { problem }) => {
            assert!(
                problem.contains("gave 120 items before it stopped"),
                "{problem}"
            );
            assert!(problem.contains("ended after 110"), "{problem}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(world.count("git.commit"), 0);
}

// ------------------------------------------------ two stream operators in a row

/// `feed.words { seq, page }` gives `{ word }` 1 to 3 for each page, from
/// the word after `resume_from`, and can give them again.
fn feed_with_words(world: &Arc<World>) -> Registry {
    let mut reg = feed_registry_again(world);
    let w = world.clone();
    register_stream(
        &mut reg,
        Gives::Again,
        OperatorSpec::new("feed.words")
            .settings(
                Shape::new()
                    .required("seq", Ty::Int)
                    .required("page", Ty::Int),
            )
            .returns(Shape::new().required("word", Ty::Int))
            .touches(["feed"]),
        move |v, cx| {
            let from = cx.resume_from();
            w.note(format!("feed.words {} {} from {from}", v["seq"], v["page"]));
            stream::iter((from + 1..=3).map(|word| Ok(Part::Item(json!({ "word": word })))))
        },
    );
    reg
}

fn words(world: &Arc<World>) -> Pipe {
    let def = pipe("words")
        .input(
            Shape::new()
                .required("seq", Ty::Int)
                .required("pages", Ty::Int),
        )
        .output("word", Shape::open())
        .operators([
            call(
                "feed.pages",
                json!({ "seq": "$item.seq", "pages": "$item.pages" }),
            )
            .into("page"),
            call(
                "feed.words",
                json!({ "seq": "$item.seq", "page": "$item.page.page" }),
            )
            .into("word"),
            emit_with(
                "word",
                json!({ "seq": "$item.seq", "page": "$item.page.page", "word": "$item.word.word" }),
            ),
        ]);
    feed_with_words(world).build_pipe(def).unwrap()
}

#[tokio::test]
async fn two_stream_operators_in_a_row_are_both_saved_by_position_and_survive_any_kill() {
    let world = World::new();
    assert_eq!(words(&world).saves_every_item(), []);
    let input: Vec<Value> = (1..=3)
        .map(|n| json!({ "seq": n, "pages": n + 1 }))
        .collect();
    let events = events_in(&words(&World::new()), &input).await;
    let want = {
        let mut caller = Caller::new();
        run(&words(&World::new()), &input, &mut caller, None).await;
        caller.outs
    };
    assert_eq!(want.len(), 3 * (2 + 3 + 4));
    for k1 in 1..events {
        for k2 in [1, events / 2, events - 1] {
            let world = World::new();
            let mut caller = Caller::new();
            run_with_kills(|| words(&world), &input, &mut caller, &[k1, k2]).await;
            assert_eq!(caller.outs, want, "kills {k1}, {k2}");
            for n in 1..=3 {
                assert_eq!(world.count(&format!("feed.pages {n} from 0")), 1);
            }
            assert!(
                caller.largest <= 700,
                "kills {k1}, {k2}: {} bytes",
                caller.largest
            );
        }
    }
}

// ------------------------------------------------------------ the checker

fn events_vine(operators: impl IntoIterator<Item = Operator>) -> PipeDef {
    pipe("v")
        .input(
            Shape::new()
                .required("seq", Ty::Int)
                .required("pages", Ty::Int),
        )
        .output("out", Shape::open())
        .operators(operators)
}

fn fetch() -> Operator {
    call(
        "feed.pages",
        json!({ "seq": "$item.seq", "pages": "$item.pages" }),
    )
    .into("page")
}

/// The paths of the calls `def` saves item by item, each with its reason.
fn saved_item_by_item(reg: &Registry, def: PipeDef) -> Vec<(String, String)> {
    reg.build_pipe(def)
        .unwrap()
        .saves_every_item()
        .iter()
        .map(|s| (s.path.clone(), s.why.clone()))
        .collect()
}

#[test]
fn the_checker_saves_a_call_item_by_item_where_a_later_item_could_not_rebuild_the_operators() {
    let reg = feed_registry_again(&World::new());
    let cases: Vec<(PipeDef, &str, &str)> = vec![
        (
            events_vine([fetch(), take(3), emit("out")]),
            "pipe[0]",
            "pipe[1] is a `take` that counts what `feed.pages` at pipe[0] gives",
        ),
        (
            events_vine([concat_map([fetch()]), take(3), emit("out")]),
            "pipe[0].concatMap.do[0]",
            "pipe[1] is a `take`",
        ),
        (
            // Outputs made in a side pipe reach a `take` in the main one.
            events_vine([fork([vec![fetch(), emit("out")]]), take(3), emit("out")]),
            "pipe[0].fork[0][0]",
            "pipe[1] is a `take`",
        ),
        (
            // Over the whole stream, a batch holds the pages of many events.
            events_vine([fetch(), fork([vec![buffer_count(2), emit("out")]]), emit("out")]),
            "pipe[0]",
            "pipe[1].fork[0][0] is a `bufferCount` that could put what `feed.pages` at pipe[0] gives in one batch",
        ),
        (
            // Out of `concatMap`, a batch may hold the pages of many events.
            events_vine([
                concat_map([fetch()]),
                buffer_count(2),
                emit("out"),
                finalize([tap("archive.put", json!({ "seq": 1, "body": "x" }))]),
            ]),
            "pipe[0].concatMap.do[0]",
            "pipe[1] is a `bufferCount`",
        ),
        (
            events_vine([fetch(), retry(2, std::time::Duration::from_secs(1)), emit("out")]),
            "pipe[0]",
            "pipe[1] is a `retry` over `feed.pages` at pipe[0]",
        ),
        (
            events_vine([
                fork([vec![fetch(), emit("out")]]),
                retry(2, std::time::Duration::from_secs(1)),
                emit("out"),
            ]),
            "pipe[0].fork[0][0]",
            "pipe[1] is a `retry`",
        ),
        (
            // A batch after a `catchError` that runs the operator holds many items' calls.
            events_vine([
                call("archive.put", json!({ "seq": "$item.seq", "body": "x" })),
                catch_error([fetch()]),
                buffer_count(2),
                emit("out"),
            ]),
            "pipe[1].catchError[0]",
            "pipe[2] is a `bufferCount`",
        ),
    ];
    for (def, path, says) in cases {
        let got = saved_item_by_item(&reg, def);
        assert!(
            got.iter().any(|(p, why)| p == path && why.contains(says)),
            "{path}: {says}: {got:?}"
        );
    }
}

#[test]
fn the_checker_saves_a_call_by_position_where_every_operator_after_it_can_be_rebuilt() {
    let reg = feed_registry_again(&World::new());
    let cleanup = || finalize([tap("archive.put", json!({ "seq": 1, "body": "x" }))]);
    for def in [
        // `last` holds one item, and a restart gives it again.
        events_vine([fetch(), last(), emit("out")]),
        // `take` inside `concatMap` counts what one item of the call gives.
        events_vine([fetch(), concat_map([take(1), emit("out")])]),
        // A batch inside one item's own work holds one call's items.
        events_vine([fetch(), buffer_count(2), emit("out"), cleanup()]),
        events_vine([
            fetch(),
            fork([vec![buffer_count(2), emit("out")]]),
            emit("out"),
            cleanup(),
        ]),
        events_vine([concat_map([fetch(), buffer_count(2), emit("out")])]),
        // `catchError` and `finalize` over the call.
        events_vine([fetch(), emit("out"), catch_error([emit("out")]), cleanup()]),
        // `retry` over operators after the call, not over the call.
        events_vine([
            fetch(),
            concat_map([
                call("archive.put", json!({ "seq": 1, "body": "x" })),
                retry(2, std::time::Duration::from_secs(1)),
                emit("out"),
            ]),
        ]),
        // A batch over the whole stream of events, before the call.
        events_vine([buffer_count(2), filter("$item.count"), emit("out")]),
    ] {
        let vine = reg.build_pipe(def).unwrap();
        assert_eq!(vine.saves_every_item(), [], "{vine:?}");
    }
}

#[test]
fn a_second_stream_operator_in_a_row_feeds_a_batch_with_many_calls_and_only_it_saves_every_item() {
    let reg = feed_with_words(&World::new());
    let def = events_vine([
        fetch(),
        call(
            "feed.words",
            json!({ "seq": "$item.seq", "page": "$item.page.page" }),
        )
        .into("word"),
        buffer_count(2),
        emit("out"),
        finalize([tap("archive.put", json!({ "seq": 1, "body": "x" }))]),
    ]);
    let got = saved_item_by_item(&reg, def);
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0].0, "pipe[1]");
}

#[tokio::test]
async fn a_vine_that_uses_one_saved_by_position_and_counts_what_it_emits_cannot_be_resumed() {
    let world = World::new();
    let mut reg = feed_registry_again(&world);
    let inner = reg
        .build_pipe(
            pipe("paged")
                .input(
                    Shape::new()
                        .required("seq", Ty::Int)
                        .required("pages", Ty::Int),
                )
                .output("out", Shape::open())
                .operators([fetch(), emit_with("out", json!({ "page": "$item.page" }))]),
        )
        .unwrap();
    assert_eq!(inner.saves_every_item(), []);
    reg.add_vine(inner).unwrap();
    let used = |more: Vec<Operator>| {
        let mut operators = vec![use_vine("paged", None).on("out", [emit("out")])];
        operators.extend(more);
        reg.build_pipe(events_vine(operators)).unwrap()
    };
    assert!(used(vec![]).resumable());
    let counting = used(vec![take(2)]);
    assert!(!counting.resumable());
    let got: Vec<_> = counting
        .run_saved(
            stream::iter([json!({ "seq": 1, "pages": 3 })]),
            Journal::new(),
        )
        .collect()
        .await;
    assert!(
        matches!(&got[..], [Err(RunError::NotResumable { why, .. })]
            if why.contains("pipe[0] uses vine `paged`, which saves a stream operator by position, and pipe[1] is a `take`")),
        "{got:?}"
    );
}

#[test]
fn a_used_vine_that_saves_a_call_item_by_item_is_named_from_the_vine_that_uses_it() {
    let mut reg = feed_registry(&World::new());
    let inner = reg
        .build_pipe(
            pipe("paged")
                .input(
                    Shape::new()
                        .required("seq", Ty::Int)
                        .required("pages", Ty::Int),
                )
                .output("out", Shape::open())
                .operators([fetch(), emit_with("out", json!({ "page": "$item.page" }))]),
        )
        .unwrap();
    reg.add_vine(inner).unwrap();
    let outer = reg
        .build_pipe(events_vine([
            use_vine("paged", None).on("out", [emit("out")])
        ]))
        .unwrap();
    let paths: Vec<&str> = outer
        .saves_every_item()
        .iter()
        .map(|s| s.path.as_str())
        .collect();
    assert_eq!(paths, ["pipe[0]>paged:pipe[0]"]);
}

// ------------------------------------------------ what is saved, and read

#[test]
fn through_and_stopped_entries_survive_json_and_bad_ones_are_refused() {
    let entries = [
        Entry::Through {
            at: "pipe[1]#0".into(),
            items: 50,
            handed: 20,
        },
        Entry::Stopped {
            at: "pipe[1]#0".into(),
            items: 120,
            how: Ending::Ended(json!({ "status": "finished" })),
        },
        Entry::Stopped {
            at: "pipe[1]#0".into(),
            items: 7,
            how: Ending::Failed {
                code: "host_lost".into(),
                message: "lost".into(),
            },
        },
        Entry::Stopped {
            at: "pipe[1]#0".into(),
            items: 70,
            how: Ending::Abandoned,
        },
    ];
    for entry in entries {
        let text = entry.to_json().to_string();
        let back = Entry::from_json(&serde_json::from_str(&text).unwrap()).unwrap();
        assert_eq!(back, entry);
    }
    for (bad, says) in [
        (
            json!({ "kind": "through", "at": "a#0", "items": 1 }),
            "`handed`",
        ),
        (
            json!({ "kind": "through", "at": "a#0", "items": -1, "handed": 0 }),
            "`items` as a whole number",
        ),
        (
            json!({ "kind": "stopped", "at": "a#0", "items": 1, "how": "lost" }),
            "unknown way to stop `lost`",
        ),
        (
            json!({ "kind": "stopped", "at": "a#0", "items": 1, "how": "ended" }),
            "`value`",
        ),
        (
            json!({ "kind": "stopped", "at": "a#0", "items": 1, "how": "abandoned", "value": 1 }),
            "no field `value`",
        ),
    ] {
        let err = Entry::from_json(&bad).unwrap_err();
        assert!(err.to_string().contains(says), "{bad}: {err}");
    }
}

fn done(at: &str) -> Entry {
    Entry::Done {
        at: at.into(),
        input: json!({}),
        result: json!({}),
    }
}

fn through(at: &str, items: u64) -> Entry {
    Entry::Through {
        at: at.into(),
        items,
        handed: 0,
    }
}

#[test]
fn recording_a_through_replaces_the_last_one_and_drops_the_lines_of_the_items_it_passed() {
    let mut journal = Journal::new();
    for entry in [
        done("pipe[0]#0"),
        through("pipe[1]#0", 2),
        done("pipe[1]#0@2/pipe[3]#0"),
        done("pipe[1]#0@3/pipe[3]#0"),
        // A second stream operator inside item 3 of the first.
        done("pipe[1]#0@3/pipe[2]#0@1/pipe[3]#0"),
        // The same operator path under another call is not passed.
        done("pipe[1]#1@2/pipe[3]#0"),
        through("pipe[1]#0", 3),
    ] {
        journal.record(&Event::Save(entry));
    }
    assert_eq!(
        journal.entries(),
        [
            done("pipe[0]#0"),
            done("pipe[1]#1@2/pipe[3]#0"),
            through("pipe[1]#0", 3),
        ]
    );

    let line = |item, entry| Progress::Save(Line { item, entry });
    let mut bookmark = Bookmark::new();
    for p in [
        line(1, done("pipe[1]#0@1/pipe[3]#0")),
        line(2, done("pipe[1]#0@1/pipe[3]#0")),
        line(1, through("pipe[1]#0", 1)),
    ] {
        bookmark.record(&p);
    }
    // Only the lines of the same input item are passed.
    assert_eq!(
        bookmark.lines(),
        [
            Line {
                item: 2,
                entry: done("pipe[1]#0@1/pipe[3]#0")
            },
            Line {
                item: 1,
                entry: through("pipe[1]#0", 1)
            },
        ]
    );
}

async fn replay(reg: &Registry, journal: Vec<Entry>) -> Vec<Result<Event, RunError>> {
    reg.build_pipe(event_pages_pipe())
        .unwrap()
        .run_saved(
            stream::iter([json!({ "seq": 1, "pages": 2 })]),
            journal.into(),
        )
        .collect()
        .await
}

fn started() -> Entry {
    Entry::Started {
        at: "pipe[0]#0".into(),
        input: json!({ "seq": 1, "pages": 2 }),
    }
}

#[tokio::test]
async fn a_journal_saved_by_position_that_does_not_fit_the_run_is_a_run_error() {
    let world = World::new();
    let again = feed_registry_again(&world);
    let misfit = |got: &[Result<Event, RunError>]| match got.last() {
        Some(Err(RunError::ReplayMisfit { problem })) => problem.clone(),
        other => panic!("{other:?}"),
    };
    // A position for a call never started.
    let p = misfit(&replay(&again, vec![through("pipe[0]#0", 1)]).await);
    assert!(p.contains("a position for `pipe[0]#0`"), "{p}");
    // An item saved for a call saved by position.
    let item = Entry::Item {
        at: "pipe[0]#0".into(),
        value: json!({ "page": 1 }),
    };
    let p = misfit(&replay(&again, vec![started(), through("pipe[0]#0", 1), item]).await);
    assert!(p.contains("an item for `pipe[0]#0`"), "{p}");
    // A second stop.
    let stop = Entry::Stopped {
        at: "pipe[0]#0".into(),
        items: 2,
        how: Ending::Abandoned,
    };
    let p = misfit(&replay(&again, vec![started(), stop.clone(), stop]).await);
    assert!(p.contains("a stop for `pipe[0]#0`"), "{p}");
    // A call saved by position, and an operator that now saves every item.
    let p = misfit(
        &replay(
            &feed_registry(&world),
            vec![started(), through("pipe[0]#0", 1)],
        )
        .await,
    );
    assert!(p.contains("was saved by position"), "{p}");
}

#[tokio::test]
async fn a_journal_saved_item_by_item_replays_that_way_after_the_operator_says_it_gives_again() {
    // The first run saved every page; then the operator was registered as one
    // that gives its pages again.
    let input = [json!({ "seq": 1, "pages": 3 })];
    let mut first = Journal::new();
    let mut outs = Vec::new();
    let mut run = feed_registry(&World::new())
        .build_pipe(event_pages_pipe())
        .unwrap()
        .run_saved(stream::iter(input.clone()), Journal::new());
    // Stop after the second page was handed out and saved.
    while let Some(event) = run.next().await {
        let event = event.unwrap();
        if let Event::Output(o) = &event {
            outs.push(o.clone());
        }
        first.record(&event);
        if outs.len() == 2 && matches!(event, Event::Save(Entry::Emitted { .. })) {
            break;
        }
    }
    drop(run);
    let world = World::new();
    let rest: Vec<Event> = feed_registry_again(&world)
        .build_pipe(event_pages_pipe())
        .unwrap()
        .run_saved(stream::iter(input), first)
        .map(Result::unwrap)
        .collect()
        .await;
    for event in &rest {
        if let Event::Output(o) = event {
            outs.push(o.clone());
        }
    }
    let pages: Vec<Value> = outs.iter().map(|o| o.payload.clone()).collect();
    assert_eq!(
        pages,
        [
            json!({ "seq": 1, "page": 1 }),
            json!({ "seq": 1, "page": 2 }),
            json!({ "seq": 1, "page": 3 }),
            json!({ "seq": 1, "pages": 3 }),
        ]
    );
    // It went on after the saved pages, and saved the third item by item.
    assert_eq!(world.log(), ["feed.pages 1 from 2"]);
    assert!(rest
        .iter()
        .any(|e| matches!(e, Event::Save(Entry::Item { .. }))));
}
