# Writing vines

A vine is a stream in and a stream out. Items go in, flow through a pipe of stages, and come out on named outputs. This guide is for whoever writes one, person or agent. It says what works and what goes wrong.

The stages are in `src/pipe.rs`. Worked examples are in `vines/pipe/`.

## The model in five lines

- Every stage takes a stream and gives a stream.
- `$item` is the item reaching a stage. It is the only name there is.
- A stage's result replaces the item. With `into: name`, the item is kept and the result is added under that name.
- A vine emits values to its outputs. It does not know what an output becomes.
- A vine fed one item is a stream of one. There is no second kind of vine.

## Do

**Carry what you learn in the item.** There are no variables. If a later stage needs the working copy's path, the stage that made it says `into: tree`, and later stages read `$item.tree.path`.

**Use `tap` for an act you run only for its effect.** The item passes through unchanged. Without `tap` or `into`, an act's result replaces the item and everything you were carrying is gone.

**Catch a failure in the pipe that can fail.** Put `catch` after the stages it covers. One item failing then becomes one `failed` output, and the items after it still run. A failure nothing catches ends the whole stream. This is the oldest trap in RxJS and in redux-observable: an uncaught error kills the listener for good.

**Undo in `catch`, using the item as it was.** `catch` gets the item as it was when the stage failed, plus the failure as `$item.error`. Branch on `$item.error.from` to see which stage failed, and undo only what had been done by then. See `vines/pipe/job_launch.yaml`.

**Clean up in `finalize` when the cleanup always happens.** `finalize` runs after the stages it covers, whether they worked, failed or were caught. It gets the item as it arrived. Put it last, after `catch`. See `vines/pipe/job_session_ended.yaml`.

**Order the covering stages by what each should see.** `catch`, `finalize`, `retry` and `timeout` each cover everything before them in their pipe. So `retry` then `catch` means "try three times, then handle the failure". `catch` then `retry` means the retry never sees a failure, because the catch already handled it.

**Give `retry` a wait, and a `timeout` to anything that can hang.** A timeout fails the item with the code `timeout`, and a `catch` after it can handle that like any other failure.

**Run another vine with `use` when the two are one piece of work.** `use: job_launch` runs that vine as a stage, and `on:` says what to do with each thing it emits. The checker makes you handle every output. See `vines/pipe/plan_launch_all.yaml`.

**Say `take: 1` when a vine must give exactly one answer.** A vine may emit none, one or many. If the caller needs one, the vine says so itself.

**Treat long work that reports as it goes as a stream.** An agent run takes hours and writes lines as it works. Register it as a stream act. `call` on it gives one item per line, and `end: ended` adds one last item that says how it ended. `job_run` makes the working copy, runs the agent, and commits, all in one vine. See `vines/pipe/job_run.yaml`.

**Run a vine that holds long work with `run_saved`, and save every entry it hands you.** Save each entry in order, before you read the next event. After a restart, run the vine again with the same input and the journal you saved. Acts that finished are not run again. A stream act is asked to go on from where it was, so it must start the work the first time and attach to it after that. Outputs you were already handed are not handed out again, as long as you saved the entry that came after each one.

**Start long work and move on when it does not report as it goes.** If the work gives no stream to attach to, start it with one act, emit `started`, and let a second vine pick up when it ends. `job_launch` and `job_session_ended` are that pair.

**Use `fork` to do more than one thing with each item.** Each item goes down every side pipe, then on down the main pipe. A side pipe sees the whole stream, so `collect` and `last` work in it. Write `filter: $item.line` first in a side pipe that should skip a stream act's end item.

**Use `last` to wait for the end of a stream, and `collect` to batch it.** `collect: { size: 50 }` gives `{ items, count }`, and what is left when the stream ends. With `into: batch` the item is the last one of the batch, so you keep its fields.

**Use `together` for quick acts that do not depend on each other.** They run side by side and the pipe goes on when all are done. Each one needs its own `into`.

**Pick the overlap rule on purpose.** `each` asks what to do when a new item arrives while the last one is still running:

| Rule | What it does | Use it when |
|---|---|---|
| `concat` | One at a time, in order | Order matters, or the work must not overlap |
| `merge` | Side by side | The items are independent. Add `at_once` to cap it |
| `switch` | Drops the run in flight for the new item | Only the newest matters, such as a refresh |
| `exhaust` | Ignores new items while busy | A second request for the same work is noise |

`concat` is the default because it is the one that cannot surprise you.

**Make every act safe to run twice.** A result can repeat after a crash. An act that creates or writes should give the same result the second time.

## Don't

**Don't put a gate in a vine.** "Is this job ready" and "is the plan active" are gates. They are declared on the move, and whoever runs the vine checks them before it starts. A vine does the work.

**Don't read from outside.** A vine works on the input it was handed. If it needs a fact, the caller puts it in the input.

**Don't write a workflow step as an operator or an act.** `git.worktree_add` is an act. `prepare_job_workspace` is a workflow step, and a workflow step is a vine. The test: could a different workflow use it unchanged?

**Don't wait on long work that is not a stream act.** No other stage should hold an item for hours. `together`, `each`, `retry` and `timeout` are for quick acts. `retry` and `timeout` hold what the stages they cover give until those stages are done.

**Don't use `switch` or `exhaust` around work that must finish.** `switch` drops a run partway through. If that run had made a working copy, nothing removes it. Use them for work that is safe to abandon.

**Don't use `merge` when the acts touch the same thing.** Two side-by-side runs that write the same branch will collide. Use `concat`, or split the work so each run has its own.

**Don't emit the item as it is unless its shape is the output's shape.** Outputs are closed shapes. Use `emit` with `with:` and name the fields.

**Don't rely on `else` to hide a mistake.** A `branch` with no matching case passes the item through unchanged. If every item must be handled, give the branch an `else` and the checker will hold you to it.

**Don't put `take`, or an `each` that merges, switches or exhausts, before a covering stage.** Those work across the whole stream, and a covering stage works one item at a time. The checker refuses it. Put the stream-wide stage after.

**Know what "the end" means before a covering stage.** A covering stage runs the stages before it once for each item. So there, `last` and `collect` see the end of that one item's own stream, such as one agent run. That is what `job_run` wants. They need a stage before them that makes many items from one, such as `split` or a stream act. Without one they could only ever see one item, and the checker refuses them.

**Don't read what a covered stage added from `finalize`.** `finalize` gets the item as it arrived. `job_run` makes the working copy inside, so its `finalize` removes it by the job's name, not by `$item.tree.path`.

**Don't expect `finalize` to run when the caller drops a run.** A dropped run leaves its working copy, and its agent goes on with nobody reading. Resume it from the journal, or clean up from outside.

**Don't use `run_saved` on a vine that cannot replay in one order.** A replay calls the acts in the same order as the first run. `each` that merges, switches or exhausts, and `timeout`, break that, so `run_saved` refuses such a vine. Resume the same vine with the same input: the journal is keyed by where each act stands in the pipe.

**Don't emit from `finalize`.** It is for cleanup. The checker refuses it.

**Don't `retry` an act that is not safe to repeat.** A retry runs every covered stage again from the item as it arrived.

**Don't `use` a vine when others need to hear what happens between the two.** A used vine runs inside the outer vine's run. Nothing it emits is seen from outside unless the outer vine emits it again. When a step should be visible, be something others can react to, or take hours, emit an output and let a separate vine pick it up.

**Don't `use` a vine from another trellis.** A trellis has one version, so the vines in it change together. A vine used across trellises could change under you.

## What the checker catches before a vine runs

- A stage that names an operator or act nobody registered.
- Input to an act that does not fit what the act declares.
- A reference to anything but `$item`, or to a field the item is known not to have.
- A vine used before it exists, an output of a used vine that nothing handles, or input that cannot fit it.
- A covering stage with nothing before it, or with a stream-wide stage before it.
- An output nobody declared, or a payload that cannot fit it.
- A path through the pipe that could end without emitting.
- A stage that can never run because everything before it already emitted.
- `fork` with no side pipe, an emit in a side pipe to an output nobody declared, and a `finalize` that holds a fork that emits. An emit in a side pipe does not count as the main pipe emitting.
- `collect` with a size of 0.
- `last` or `collect` where they could only ever see one item.
- `tap` on a stream act, a stream act inside `together`, and `end` on an act that is not a stream act.

## What it does not catch

- A field read from an item whose shape is not known at that point. After `split` with no `into`, inside `catch` and `finalize`, and after `each`, `use`, `branch` or any covering stage, the checker no longer knows the item's fields. A misspelt field reads as nothing.
- A `branch` case that is misspelt. It never matches.
- An act that lies about what it touches.
- A stream act that does not go on from `resume_from`. A replay trusts it to.
- A journal saved for a different vine. A saved act whose input no longer matches is a run error, and so is an output the replay does not make again. Anything else is not checked.

## Not built yet

- Resuming a vine with `each` that merges, switches or exhausts, or with `timeout`.
- Making a journal smaller. It only grows: every line an agent writes is in it.
- Telling a stream act that the run stopped reading. Dropping a run, or a `catch` that drops the rest of an item's work, leaves the stream act's work going.
- `retry` waits again on replay, even when the attempts it waits for are saved.
- Waiting stages such as `debounce`, and `take_until` to stop work when a cancel arrives.
- `finalize` when the caller drops the stream partway.
- A name for the reach each stage uses, such as "GitHub as reviewer".
