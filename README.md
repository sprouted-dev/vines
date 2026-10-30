# terra-vines

`vines` is a thin, in-process stream composition layer over Tokio and
`futures` for workflow operators: agent output, channel batching, progress
fan-out.

It is **not** a durable workflow engine, **not** an Rx clone, and **not** a
scheduler. It owns no storage and no retries. It lets a consumer *declare*
how each observer of a stream behaves, enforces those declarations at build
time, and reports loss and cancellation honestly. Terra depends on vines;
vines never depends on Terra.

```toml
[dependencies]
terra-vines = { path = "../vines" }   # library name: `vines`
```

## Boundary

| Concern | vines | Terra (the consumer) |
|---|---|---|
| Producer ownership: one source, one launch | owns | uses |
| Live progress fan-out, per-observer bounded queues | owns | declares observers |
| Observer policy (answer, durability, loss, queue, overflow) | validates and enforces | authors |
| Counted loss, gap markers, coverage | owns | shows to readers |
| Answer gating on required observers | owns (in-process) | decides what an answer means |
| Durable acceptance of a frame | calls `DurableSink`, counts acks | implements the sink (storage, fsync, append) |
| Retry, reconciliation, `unknown` results | no | owns |
| Cancellation request, deadline, cascade | owns (`CancelScope`) | wires to moves and sessions |
| "Did the process stop?" | never claims it; reports `Unconfirmed` and hands back a `Straggler` | confirms through its adapters |
| Batching in memory (count, bytes, idle deadline) | owns | |
| Durable batch manifests, cursors, receipts | no | owns |
| Graph, events, moves, claims, receipts, execution record | no | owns |

## The operator shape

A run is one producer with a live progress stream and exactly one terminal
result:

```rust
use std::num::NonZeroUsize;
use std::time::Duration;
use vines::cancel::CancelScope;
use vines::policy::{Answer, Durability, Loss, ObserverPolicy, Overflow, SourceDecl};
use vines::run::{Exit, RunBuilder};

async fn example(sink: impl vines::sink::DurableSink<String>) {
let n = |v| NonZeroUsize::new(v).unwrap();

// An agent cannot be made to wait.
let mut run = RunBuilder::<String>::new(SourceDecl::without_backpressure(), Duration::from_secs(5))
    .unwrap();

// A live display: lossy, no answer authority.
let display = run
    .observe("display", ObserverPolicy::new(n(64), Overflow::DropOldest))
    .unwrap();

// A transcript: durable, lossless within a spill quota, gates the answer.
let transcript = run
    .observe_durable(
        "transcript",
        ObserverPolicy::new(n(256), Overflow::Spill { quota: n(4096) })
            .durability(Durability::Durable)
            .loss(Loss::None)
            .answer(Answer::Required),
        sink,
    )
    .unwrap();

let scope = CancelScope::with_timeout(Duration::from_secs(600));
let handle = run.spawn(&scope, |mut ctx| async move {
    let _ = ctx.emit("thinking".to_string()).await; // never waits here
    if ctx.is_cancelled() {
        return Exit::Stopped; // the producer confirms the stop
    }
    Exit::<_, String>::Completed("patch ready")
});

let report = handle.finish().await;
match report.answer() {
    Ok(outcome) => { /* Completed / Failed / Cancelled(Stop) / TimedOut(Stop) / Crashed */ }
    Err(gated) => { /* a required observer failed: gated.failures says which */ }
}
let _ = (display, transcript);
}
```

- **The result never waits on progress.** It travels on its own channel. On
  a source without backpressure `emit` is ready at once; each observer's
  overflow policy absorbs the difference.
- **One source, one owner.** The producer runs once. Attaching observers,
  before the start or after it, never runs it again. A late observer gets a
  `JoinedLate` gap for what it missed.
- **Dropping the `Run` handle requests cancellation.**

## Observer policy

Each observer declares three independent policies and a resource contract:

| Field | Values |
|---|---|
| `answer` | `required`: the run's answer is gated on it; `none`: its failure never fails the run |
| `durability` | `ephemeral`: an in-process `ObserverStream`; `durable`: a `DurableSink`, a frame counts only when acknowledged |
| `loss` | `allowed`: may skip, every skip counted and marked; `none`: fails rather than skips |
| `queue` | `NonZeroUsize` capacity |
| `overflow` | `drop_oldest`, `drop_newest`, `stop_producer`, `spill { quota }` |

Rules, each refused with a typed `PolicyError`:

| Rule | Error |
|---|---|
| `loss = none` forbids `drop_oldest` / `drop_newest` | `LossNoneForbidsDrop` |
| `durable` requires a `DurableSink` | `DurableRequiresSink` |
| `ephemeral` refuses a `DurableSink` | `EphemeralGivenSink` |
| `stop_producer` requires a source declaring `Backpressure::Supported` | `StopProducerRequiresBackpressure` |
| `spill` requires a quota | `SpillRequiresQuota` |
| every limit present and non-zero (zero never means unlimited) | `MissingLimit`, `ZeroLimit`, `ZeroDuration` |
| observer names are unique per run | `DuplicateObserver` |
| `required` observers are declared before the start | `LateRequiredObserver` |
| no observers after the run ended | `RunEnded` |

`ObserverDecl` is the as-authored form (limits as `Option<usize>`, the way
YAML delivers them); `ObserverDecl::validate` turns it into the typed
`ObserverPolicy`.

What overflow does when the queue is full:

- `drop_oldest` / `drop_newest`: drop, count, and put a `Gap` frame exactly
  where the missing items were.
- `stop_producer`: the producer's `emit` waits for room. Cancellation releases
  it with `EmitError::Cancelled(item)`; an observer that walks away releases
  it too.
- `spill { quota }`: hold up to `quota` more. When the quota runs out:
  `loss = allowed` drops with a `SpillExhausted` gap; `loss = none` fails the
  observer (`SpillQuotaExhausted`) and marks the rest `ObserverFailed`.

Every observer stream ends with `Frame::End { ending, coverage }`. `Coverage`
says how many items were emitted and lost and whether the observer failed;
`Record::is_complete()` is false whenever anything is missing, so a summary
cannot present an incomplete record as complete.

Observer failure: `answer = none` never fails the run (it shows in that
observer's report). `answer = required` gates: `RunReport::answer()` returns
`Err(Gated { outcome, failures })`, keeping what the producer did without
calling it an answer. A required observer settles when it takes the end
marker (stream) or its sink acknowledges it (durable). After cancellation the
gate waits at most `stop_grace`, then marks stragglers `Unsettled`.

## Cancellation

`CancelScope` wraps a `CancellationToken` with an optional deadline. Children
cascade from parents, take the earlier deadline, and never cancel their
parent. The cause is `Requested` or `TimedOut`, decided by timestamps: the
first in time wins.

A run's outcome separates the request from the stop:

| Outcome | Meaning |
|---|---|
| `Completed(r)` | the producer completed, even if a cancel was requested (`cancel_requested` says so) |
| `Failed(e)` | the producer failed |
| `Cancelled(Stop::Confirmed)` / `TimedOut(Stop::Confirmed)` | the producer returned `Exit::Stopped` within `stop_grace` |
| `Cancelled(Stop::Unconfirmed)` / `TimedOut(Stop::Unconfirmed)` | requested, not confirmed; the producer is handed back in `report.straggler` |
| `Crashed(_)` | panic, abort, or `Exit::Stopped` without a request |

`Straggler::exit()` waits for the producer's real exit, which may be
`Completed`: a cancel request never proves an effect did not happen.

## Operators

- `ops::map`, `ops::filter`: thin over `futures`.
- `ops::tap(stream, TapIsolation, f)`: a tap failure never fails the stream;
  `KeepCalling` or `DetachOnFirstFailure`, counted in `TapStats`.
- `ops::tap_required(stream, f)`: a tap failure fails the stream, yielding
  `Err(TapFailed { index, item, error })` once and pulling nothing more.
- `ops::par_map(stream, limit, OutputOrder, f)`: at most `limit` at once;
  `Ordered` (input order) or `Unordered` (completion order).
- `batch::batch(stream, BatchPolicy, &scope)`: flush by count, by
  `max_bytes` through a size function, by `max_wait` from a batch's first
  item (the timer fires with no new input), and on completion. An item over
  `max_bytes` goes alone as `Oversize`. On cancel, `OnCancel::Flush` or
  `OnCancel::Discard` (no default), then a terminal
  `BatchEvent::Cancelled { cause, discarded }`, so a discard is always
  reported.
- `ordering::merge_arrival`: join branches, no cross-branch order.
  `ordering::merge_by_sequence`: join in source sequence order, waiting for
  a quiet branch and reporting an `OrderViolation` if a branch goes
  backwards. Per-stream order is always kept.

## Compared with Terra today

- **Agent taps** (`sunlight-execution/.../taps/mod.rs`): bounded per-tap
  queues, drop-and-count, a tap failure never fails the run. Vines keeps that
  as the `answer = none` + `loss = allowed` case, and also lets an observer
  declare otherwise (lossless with spill, durable, required) explicitly
  rather than by registry position. The taps' "ordered drain" with
  bounded end/finish phases is not reproduced: vines gates only on required
  observers and lets the rest drain on their own.
- **`terra-pipeline` batch**: its time window is checked only when input
  arrives. Vines' batcher flushes on the deadline itself.

## Not in this slice

- Durable anything: spill is in-memory, batches are in-memory, there is no
  manifest, cursor or recovery. Durability lives behind `DurableSink`.
- A spill-to-storage trait (spill quota is counted in items, held in memory).
- Byte budgets for observer queues (only items); bytes exist only in batching.
- Throttle, debounce, dedup, retry, windowing beyond batching.
- Joins with partial-failure policy (all/any/first); only the two merges.
- Catching panics inside `tap` closures (a panic propagates).
- The interpreter, YAML parsing, CEL, typed ports, the execution record.
- Terra integration: no adapter from `agent_run` or the tap registry yet.
- Metrics/tracing hooks; counts are available through reports only.
- Upper ceilings on limits: limits are non-zero but not capped.

## Verification

```sh
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Time-dependent tests run under `tokio::time::pause` and are deterministic.

## License

MIT OR Apache-2.0.
