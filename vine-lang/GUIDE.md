# Writing vines

A vine is a stream in and a stream out. Items go in, flow through a pipe of stages, and come out on named outputs. This guide is for whoever writes one, person or agent. It says what works and what goes wrong.

The stages are in `src/pipe/`. Worked examples are in `vines/pipe/`.

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

**Give `retry` a wait, and a `timeout` to anything that can hang.** A timeout fails the item with the code `timeout`, and a `catch` after it can handle that like any other failure. The `catch` gets the item as it reached the stages the timeout covers, not as it was partway through them, so undo by what the input names: `job_launch` removes the working copy by the job's name. See `vines/pipe/job_launch.yaml`.

**Resume a vine with a `timeout` as any other.** A saved run saves what the clock decided for each run of the stages a timeout covers, before anything that depends on it, so a replay never asks the clock again. What you can rely on, with `run_saved` and with `run_from`, inside an `each` that merges, inside a vine run by `use`, and with `catch` or `retry` before or after the timeout:

- A timeout that fired before the restart fires again at once. The stages it covered are not run again, so no act they had done is done again, and none they were doing is started again. A `catch` after it goes down the same branch.
- A timeout whose stages were done in time before the restart is replayed as done, with no limit, even when the replay is slower, as a stream act asked to go on can be.
- What the covered stages emit is held until they are done, and dropped if the timeout fires. Outputs keep their ids across a restart.
- Each run of the covered stages keeps one line for its decision. With `run_from` it is dropped with the item's other lines when the position passes the item.

**Know the rule for a restart before the timeout decided.** If the run stopped while the covered stages were still going, the wait starts again from nothing when the run resumes, and the stages get the whole limit again. Work whose results were saved replays at once and does not count against it. The library has no clock that runs across a restart, so it does not try to honour the time already spent. So after such a restart, the covered stages can finish in time where the first run would have timed out, or reach acts the first run never reached. Nothing you were handed depended on the decision, so this is a run at another pace, not a contradiction. If the work must not get more time than the limit in total, put the limit in the act itself.

**Expect an act in flight when a timeout fires to be dropped, not stopped.** The run stops waiting for it and never saves its result, and a replay does not call it again. But the library does not tell the act. An act that already reached outside, such as a request to start an agent, can still have its effect, and a stream act's work goes on with nobody reading. Make the `catch` after the timeout undo what such an act may have done, and make the act safe to have half run.

**Nest a vine that a move starts by pressing its move.** `plan_launch_all` launches each job with `call: move.launch`, not `use: job_launch`. The host checks the job's gates, builds `job_launch`'s input from the job's facts, and lands each output as an event before your vine hears of it. So you pass only what the move's payload takes, and the work stays visible: another vine can react to every event. The act gives one item per inner output, `{ output, event, landed, payload }`, and ends with `{ status }`: `done`, `failed` with a `reason`, or `refused` with the `gate` that refused it. A refusal is data to branch on, not a failure, so the other jobs go on. `plan_launch_all` launches two jobs at once, with `each` and `overlap: merge, at_once: 2`. Pass your own `request` through: the host builds each inner press's id from it, so after a restart the same press attaches to the first. See `vines/pipe/plan_launch_all.yaml`.

**Run a helper with `use` when the two are one piece of work and no move starts the helper.** `use: job_teardown` runs that vine as a stage, and `on:` says what to do with each thing it emits. The checker makes you handle every output. See `vines/pipe/job_launch_undo.yaml`.

**Say `take: 1` when a vine wants no more than its first answer.** A vine may emit none, one or many. `take: 1` lets at most one item through and ends the stream. It does not promise an output: the stream can be empty, and the stages after `take` decide what is emitted.

**Treat long work that reports as it goes as a stream.** An agent run takes hours and writes lines as it works. Register it as a stream act. `call` on it gives one item per line, and `end: ended` adds one last item that says how it ended. `job_run` makes the working copy, runs the agent, and commits, all in one vine. See `vines/pipe/job_run.yaml`.

**Run a vine that holds long work with `run_saved`, and keep the journal it builds.** Pass every event to `Journal::record` and save the journal before you read the next event. After a restart, run the vine again with the same input and the journal you saved. Acts whose entry you saved are not run again. A saved run hands out each act's entry before it calls the next act, so a crash can lose at most the one act whose entry you had not saved yet, and that act runs again. A stream act is asked to go on from where it was, so it must start the work the first time and attach to it after that. Outputs you were already handed are not handed out again, as long as you saved the entry that came after each one. The one you were handed last, if you had not saved the entry after it, comes again with the same id.

**Drop a repeated output by its id, never by its name and payload.** Every output a run hands out has an `id`, an `OutputId`. It says where in the run the output comes from: the input item, the group of work, and how many outputs that group made before it. It is never made from the payload, a clock or a random number. An output that comes again after a restart has the id it had, on both ways to resume, inside an `each` that merges and inside a vine run by `use`. Two outputs of one run never share an id, so a vine that emits the same name and payload twice gives two outputs, and you keep both. Ids are unique within one run and its restarts, not across runs: if you keep the outputs of many runs in one place, join the id with your own key for the run, as Terra joins it with the request id. A run that saves nothing gives ids too, unique within that run, but they count the whole run in the order outputs come and do not match a saved run's. No saved line holds an id: a replay makes each id again from where the output stands, so what you keep is no bigger, and a journal or bookmark saved before outputs had ids resumes as it did and gives the ids a new run gives.

**Run a vine over an endless stream with `run_from`, and keep the bookmark it builds.** A vine that reads events for months must not replay from the first event after a restart. `run_from` tells you when every event through a number is fully done: its acts finished, its outputs were handed out, and no stage holds it. Pass every event it gives you to `Bookmark::record` and save the bookmark before you read the next. After a restart, feed the events after `bookmark.position()` with the bookmark you saved. The bookmark holds only the lines of events still in flight, so it stays small. See `vines/pipe/event_relay.yaml`, `event_backup.yaml` and `event_pages.yaml`.

**Register a stream act that can give its items again as one.** If the host behind an act stores what it gave, as an agent host stores the lines a session wrote, register it with `register_replayable_stream_act`. Then a saved run keeps how far each call got instead of every item: a job whose agent writes ten thousand lines keeps about 500 bytes, not a megabyte. After a restart the act is asked for its items from the one after the position, and `collect` refills its batch from them. The act must give the same items again from any point it gave. Check `Pipe::saves_every_item()`: it lists every call that is still saved item by item, and why.

**Run independent items side by side with `each` and `merge`, and still resume.** Each item that enters the `each` gets its own group of saved lines, numbered in the order the items went in, not the order they finish. After a restart each item's work replays in its own order, so the items can finish in another order and nothing is lost or run twice. What you can rely on:

- Within one item's work, outputs come in order, before and after a restart. Each keeps its id whatever order the items finish in.
- Across items, outputs come in whatever order the work finishes. After a restart that order can differ from the first run. Do not depend on it.
- Each act still runs once, apart from the acts whose results you had not saved when the crash came. A run starts an act only once you hold every line made so far, so those are the acts that were running, one per item running side by side (more if the item runs `together`), plus at most one result you were handed and had not saved. With `at_once: 2` that is at most two acts.
- With `run_from`, an item can finish before an earlier one. The run says so with `Progress::Done(n)`, and `Bookmark::record` keeps the number and drops its lines. The position waits for the earlier item and never goes back. After a restart, feed the items after the position as always; the run skips the ones already done. So the bookmark holds the lines of at most `at_once` items in flight, plus runs of numbers. Without `at_once`, every item you feed is in flight at once, and the bookmark grows with how many arrive while one is slow. Over an endless stream, set `at_once`.

**Pick the mode by whether the run ends.** One job, one plan launch: `run_saved` and a journal. A stream of events with no end: `run_from` and a bookmark. They save the same entries. A bookmark's lines also say which input item each belongs to, and the position is where the caller may cut.

**Expect `collect` to hold the position back.** The events in a half full batch are not done, so the position stays before them, and after a restart they are fed again. A batch ties its events together: the position moves by whole batches, even when a `fork` sends each event on down the main pipe too.

**Start long work and move on when it does not report as it goes.** If the work gives no stream to attach to, start it with one act, emit `started`, and let a second vine pick up when it ends. `job_launch` and `job_session_ended` are that pair.

**Use `fork` to do more than one thing with each item.** Each item goes down every side pipe, then on down the main pipe. A side pipe sees the whole stream, so `collect` and `last` work in it. Write `filter: $item.line` first in a side pipe that should skip a stream act's end item.

**Use `last` to wait for the end of a stream, and `collect` to batch it.** `collect: { size: 50 }` gives `{ items, count }`, and what is left when the stream ends. With `into: batch` the item is the last one of the batch, so you keep its fields.

**Use `together` for quick acts that do not depend on each other.** They run side by side and the pipe goes on when all are done. Each one needs its own `into`.

**Pick the overlap rule on purpose.** `each` asks what to do when a new item arrives while the last one is still running:

| Rule | What it does | Use it when |
|---|---|---|
| `concat` | One at a time, in order | Order matters, or the work must not overlap |
| `merge` | Side by side | The items are independent. Add `at_once` to cap it. Resumes |
| `switch` | Drops the run in flight for the new item | Only the newest matters, such as a refresh |
| `exhaust` | Ignores new items while busy | A second request for the same work is noise |

`concat` is the default because it is the one that cannot surprise you. `switch` and `exhaust` cannot be resumed.

**Make every act safe to run twice.** A result can repeat after a crash, and an act whose entry was not saved runs again. An act that creates or writes should give the same result the second time: `git.worktree_add` for a name that has a working copy gives that copy, and `agent.start` in a directory with an agent running gives that agent.

**Undo abandoned work in a vine of its own.** When the host refuses an output because a fact the press read changed, it lands an `abandoned` event that carries the output. A reaction to that event presses a cleanup move, whose vine undoes the work from what the output says. Give the cleanup move a gate that refuses when a later run has taken the work over, as `undo_launch` refuses a job that is no longer `ready`. See `vines/pipe/job_launch_undo.yaml`.

## Don't

**Don't put a gate in a vine.** "Is this job ready" and "is the plan active" are gates. They are declared on the move, and whoever runs the vine checks them before it starts. A vine does the work.

**Don't read from outside.** A vine works on the input it was handed. If it needs a fact, the caller puts it in the input.

**Don't write a workflow step as an operator or an act.** `git.worktree_add` is an act. `prepare_job_workspace` is a workflow step, and a workflow step is a vine. The test: could a different workflow use it unchanged?

**Don't wait on long work that is not a stream act.** No other stage should hold an item for hours. `together`, `each`, `retry` and `timeout` are for quick acts. `retry` and `timeout` hold what the stages they cover give until those stages are done.

**Don't use `switch` or `exhaust` around work that must finish.** `switch` drops a run partway through. If that run had made a working copy, nothing removes it. Use them for work that is safe to abandon.

**Don't use `merge` when the acts touch the same thing.** Two side-by-side runs that write the same branch will collide. Use `concat`, or split the work so each run has its own.

**Don't emit the item as it is unless its shape is the output's shape.** Outputs are closed shapes. Use `emit` with `with:` and name the fields.

**Don't rely on `else` to hide a mistake.** A `branch` with no matching case passes the item through unchanged. If every item must be handled, give the branch an `else` and the checker will hold you to it.

**Don't count or batch what comes out of a `merge`, or act on the item `last` picks from it, in a vine you resume.** After a restart the items can finish in another order. `take` would let other items through and `collect` would batch other items, so the checker refuses both after a merging `each`. `last` is allowed: it gives the final item to finish, and after a restart that can be another item, as it could be in a run at another pace. So an act on it is refused, and what you emit from it should use only what every item shares, as `plan_launch_all`'s `done` does. The output keeps its id whichever item `last` picked: if it comes again after a restart with another item's fields, it is still the output you were handed, and a caller that drops repeats by id keeps the first. Count or batch inside the `each`, where one item's work stays in order.

**Don't cover an `each` that merges with `catch` or `finalize`, in a vine you resume.** At the first failure they drop the items still running, and how far those got depends on timing. Put the `catch` inside the `each`: then one item's failure becomes its own output and the others go on. A failure nothing catches ends the run, and the items still running stop where they are; after a restart the run stops at the same failure again, and the acts those items were running start again.

**Don't put `take`, or an `each` that merges, switches or exhausts, before a covering stage.** Those work across the whole stream, and a covering stage works one item at a time. The checker refuses it. Put the stream-wide stage after.

**Know what "the end" means before a covering stage.** A covering stage runs the stages before it once for each item. So there, `last` and `collect` see the end of that one item's own stream, such as one agent run. That is what `job_run` wants. They need a stage before them that makes many items from one, such as `split` or a stream act. Without one they could only ever see one item, and the checker refuses them.

**Don't read what a covered stage added from `finalize`.** `finalize` gets the item as it arrived. `job_run` makes the working copy inside, so its `finalize` removes it by the job's name, not by `$item.tree.path`.

**Don't expect `finalize` to run when the caller drops a run.** A dropped run leaves its working copy, and its agent goes on with nobody reading. Resume it from the journal, or clean up from outside.

**Don't use `run_saved` on a vine with an `each` that switches or exhausts.** It drops or skips work by when items arrive, and a replay cannot know when they did, so `run_saved` and `run_from` refuse such a vine. Resume the same vine with the same input: the journal is keyed by where each act stands in the pipe, inside a merging `each` by which item it was, and under a `timeout` by which run of its stages it was.

**Don't resume a journal saved before you added a `timeout`.** The acts a timeout covers are keyed by its runs, so a journal saved by the vine without it names them in a way the vine no longer asks for. `run_saved` and `run_from` refuse it with `ReplayMisfit` before any act runs. Let such a run finish on the old vine, or start it again.

**Don't use `run_from` on a vine that counts the whole stream.** A `take` over the whole stream would count from nothing again after a restart. A vine that ends itself has an end: run it with `run_saved`. `take` inside `each`, or before a covering stage, counts one item's own stream and is fine.

**Don't batch pieces of events across the whole stream with `run_from`.** A `collect` after `split`, a stream act, `each`, `use` or `branch` at the top of the pipe could end a batch partway through one event, and the position could never pass it. Batch whole events, or batch inside `each` where the batch belongs to one event.

**Don't expect a restart to get past a failure nothing catches.** The position stays before the event that failed, and its saved failure replays: the run stops at the same place every time. Add a `catch` to the vine and run it again from the same bookmark. The act keeps its path, so the saved failure is handled without asking the act again. Dropping the event's lines asks the act again instead.

**Don't feed `run_from` anything but the events after the position, in the order you fed them before.** A saved act whose input does not match is a run error. A bookmark whose keys happen to fit another vine is not caught.

**Don't let one event's stream act run for months under `run_from` unless it gives its items again.** A stream act saved item by item keeps its lines in the bookmark until the event is done. One saved by position keeps a few lines however long it runs.

**Don't put `take` after a stream act that gives its items again, or `retry` over it.** `take` counts what the act gives, and after a restart it would count from the position. `retry` drops what a failed attempt gave, so the run cannot tell what is done. The call is then saved item by item. Put `take` inside `each`, where it counts one item's own stream, and leave `retry` to quick acts after the stream act.

**Batch a stream act's items where the batch holds one call's items.** `collect` after the act, in the same pipe or a side pipe, before a covering stage, as `job_run` does, holds one call's lines. A `collect` that could mix two calls, such as one over the whole stream of events or after an `each` that runs the act, makes the call saved item by item.

**Don't emit from `finalize`.** It is for cleanup. The checker refuses it.

**Don't `retry` an act that is not safe to repeat.** A retry runs every covered stage again from the item as it arrived.

**Don't `use` a vine that a move starts.** It would skip the move's gates, and nobody would build its input. Press the move instead. The host refuses a package of vines in which one does, even through a helper: the library lists what a vine uses with `Pipe::uses`, and the host knows which vines its moves start.

**Don't `use` a vine when others need to hear what happens between the two.** A used vine runs inside the outer vine's run. Nothing it emits is seen from outside unless the outer vine emits it again. When a step should be visible, be something others can react to, or take hours, press its move, or emit an output and let a separate vine pick it up.

**Don't let a move's event press the same move again unless a gate stops it.** The host refuses such a loop at install. It allows one only when a gate on the loop reads a fact of the move's own target that the loop's own events set to a value the gate refuses, as `core.job.started` sets the job's `state` that `launch` gates on. A gate on another artifact, a loop through a nested press, or a reaction that takes its target from the event's payload does not count.

**Don't `use` a vine from another trellis.** A trellis has one version, so the vines in it change together. A vine used across trellises could change under you.

## What the checker catches before a vine runs

- A stage that names an operator or act nobody registered.
- Input to an act that does not fit what the act declares.
- A reference to anything but `$item`, or to a field the item is known not to have.
- A vine used before it exists, an output of a used vine that nothing handles, or input that cannot fit it.
- A covering stage with nothing before it, or with a stream-wide stage before it.
- An output nobody declared, or a payload that cannot fit it.
- A pipe, or a case of a `branch`, a `catch` or a `use`, that does not end at an `emit`. A `branch` with no `else` counts. A `filter` or `take` before the `emit` can still drop an item, so a run can give no output.
- A stage that can never run because everything before it already emitted.
- `fork` with no side pipe, an emit in a side pipe to an output nobody declared, and a `finalize` that holds a fork that emits. An emit in a side pipe does not count as the main pipe emitting.
- `collect` with a size of 0.
- `last` or `collect` where they could only ever see one item.
- `tap` on a stream act, a stream act inside `together`, and `end` on an act that is not a stream act.
- A stream act that gives its items again, where a later item could not rebuild what comes after it: a `take` that its items reach, a `collect` that could mix two calls, or a `retry` over it. The call is saved item by item, and `Pipe::saves_every_item` says where and why. A vine that refuses this for a vine it runs by `use` cannot be resumed.
- For both ways to resume: `each` that switches or exhausts, `take` or `collect` after a merging `each` (in a side pipe too), an act on the item `last` picked after one, and a `catch` or `finalize` that covers one at any depth. `Pipe::resumable` says so, and the run gives `NotResumable` with the reason and the stage's path. A `timeout` is not refused.
- A journal or bookmark that holds an act a `timeout` covers, saved with no run of that timeout in its key: it was saved before the timeout was added. The run gives `ReplayMisfit` before any act runs.
- For `run_from`, besides what `run_saved` refuses: `take` over the whole stream, and `collect` over the whole stream after a stage that makes many items from one. `Pipe::resumable_from_position` says so, and `run_from` gives `NotResumable` with the reason and the stage's path.

## What it does not catch

- A field read from an item whose shape is not known at that point. After `split` with no `into`, inside `catch` and `finalize`, and after `each`, `use`, `branch` or any covering stage, the checker no longer knows the item's fields. A misspelt field reads as nothing.
- A `branch` case that is misspelt. It never matches.
- An act that lies about what it touches.
- A stream act that does not go on from `resume_from`. A replay trusts it to.
- A stream act that says it gives its items again and gives other items. A run sees only an act that stops after fewer items than it gave before it stopped. Outputs handed out before a restart are skipped by count, so an act that gives other items can hand out a different output in place of one already handed.
- Whether an id you hold was made by this run. Ids say where an output stands, not which run or vine made it: two runs give the same ids. Keep them apart with your own key for the run.
- A timeout's saved decision used for another item. The decision is not checked against the item, so a journal saved for other input can fail an item at once that would have been in time.
- A journal saved for a different vine. A saved act whose input no longer matches is a run error, and so is an output the replay does not make again. Anything else is not checked.
- An output emitted from the item `last` picked after a `merge`, when the item matters. After a restart it can be another item's. An output in the window between being handed out and its line being saved comes again, and from such a `last` it can come with another item's fields, under the same id.
- A `use` of a vine that a move starts, or moves that start each other in a loop. The library knows no moves. The host checks both when it installs a package of vines, from `Pipe::uses` and `Pipe::acts`.

## Not built yet

- Resuming a vine with `each` that switches or exhausts, in either mode. They would need the run to save what time decided: which item a switch dropped and how far it got, and which items an exhaust ignored.
- A timeout that honours, after a restart, the time its stages had already taken. It would need a clock that runs across a restart, which the library does not have.
- Telling an act that a timeout stopped waiting for it.
- Making a journal smaller for a stream act that cannot give its items again. Every item it gives is saved until its input item is done.
- A position for a stage that keeps a count or a running total across events. Such state needs to be saved itself, as the older runner's `Memory::Kept` did.
- Telling a stream act that the run stopped reading. Dropping a run, or a `catch` that drops the rest of an item's work, leaves the stream act's work going.
- `retry` waits again on replay, even when the attempts it waits for are saved.
- Waiting stages such as `debounce`, and `take_until` to stop work when a cancel arrives.
- `finalize` when the caller drops the stream partway.
- A name for the reach each stage uses, such as "GitHub as reviewer".
