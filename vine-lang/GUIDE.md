# Writing vines

A vine is a stream in and a stream out. Items go in, flow through a pipe of operators, and come out on named outputs. This guide is for whoever writes one, person or agent. It says what works and what goes wrong.

Everything in a pipe is an operator, as in RxJS. The built-in operators are in `src/pipe/`. Worked examples are in `vines/pipe/`.

## The operators and their RxJS names

| Operator | RxJS | Notes |
|---|---|---|
| `filter` | `filter` | |
| `take` | `take` | |
| `last` | `last` | Written as the bare word `- last` |
| `bufferCount` | `bufferCount` | `bufferCount: { bufferSize: 50 }` |
| `forkJoin` | `forkJoin` | In RxJS a creation function; here it runs registered operators side by side for one item |
| `concatMap` | `concatMap` | |
| `mergeMap` | `mergeMap` | `concurrent` caps it, as RxJS's second argument does |
| `switchMap` | `switchMap` | |
| `exhaustMap` | `exhaustMap` | |
| `catchError` | `catchError` | |
| `finalize` | `finalize` | |
| `retry` | `retry` | `retry: { count: 3, delay: 5s }` |
| `timeout` | `timeout` | `timeout: 30s` |
| `call` | none | Runs a registered operator that touches something outside |
| `tap` on a named operator | none | Runs such an operator for its effect. RxJS `tap` takes a function, not a name |
| `split` | none | What `mergeMap(item => from(item.list))` does, in one operator |
| `fork` | none | Close to `share` with several subscribers |
| `branch` | none | |
| `emit` | none | Sends the item to one of the vine's named outputs |
| `use` | none | Runs another vine |

The caller registers more operators by name, such as `git.worktree_add`. The library registers two pure ones, `set` (RxJS `map` to a fixed shape) and `select` (a filter over a list inside the item).

## The model in five lines

- Every operator takes a stream and gives a stream.
- `$item` is the item reaching an operator. It is the only name there is.
- An operator's result replaces the item. With `into: name`, the item is kept and the result is added under that name.
- A vine emits values to its outputs. It does not know what an output becomes.
- A vine fed one item is a stream of one. There is no second kind of vine.

## Registered operators

A registered operator declares what it touches. That decides how a vine treats it:

- One that touches nothing is pure. A vine writes it by its name, `set: { ... }`, and a saved run does not save it: a replay runs it again.
- One that touches something reaches outside. It must declare what it touches. A vine runs it with `call` or `tap`. Its result is saved, so a replay does not run it again. What it touches counts toward the vine's reach. It must be safe to run twice.
- A stream operator gives many items and then an end. It always reaches outside, so it follows the same rules. One that can give its items again from any point is replayable.

`Registry::register` registers both kinds of operator that give one result. `Registry::register_stream` and `Registry::register_replayable_stream` register stream operators.

## Do

**Carry what you learn in the item.** There are no variables. If a later operator needs the working copy's path, the operator that made it says `into: tree`, and later operators read `$item.tree.path`.

**Use `tap` for an operator you run only for its effect.** The item passes through unchanged. Without `tap` or `into`, the result replaces the item and everything you were carrying is gone.

**Catch a failure in the pipe that can fail.** Put `catchError` after the operators it applies to. One item failing then becomes one `failed` output, and the items after it still run. A failure nothing catches ends the whole stream. This is the oldest trap in RxJS and in redux-observable: an uncaught error kills the listener for good.

**Undo in `catchError`, using the item as it was.** `catchError` gets the item as it was when the operator failed, plus the failure as `$item.error`. Branch on `$item.error.from` to see which operator failed, and undo only what had been done by then. See `vines/pipe/job_launch.yaml`.

**Clean up in `finalize` when the cleanup always happens.** `finalize` runs after the operators before it, whether they worked, failed or were caught. It gets the item as it arrived. Put it last, after `catchError`. See `vines/pipe/job_session_ended.yaml`.

**Order `catchError`, `finalize`, `retry` and `timeout` by what each should see.** Each applies to everything before it in its pipe. So `retry` then `catchError` means "try three times, then handle the failure". `catchError` then `retry` means the retry never sees a failure, because the `catchError` already handled it.

**Give `retry` a `delay`, and a `timeout` to anything that can hang.** Write `retry: { count: 3, delay: 5s }`. A timeout fails the item with the code `timeout`, and a `catchError` after it can handle that like any other failure. The `catchError` gets the item as it reached the operators the timeout applies to, not as it was partway through them, so undo by what the input names: `job_launch` removes the working copy by the job's name. See `vines/pipe/job_launch.yaml`.

**Resume a vine with a `timeout` as any other.** A saved run saves what the clock decided for each run of the operators a timeout applies to, before anything that depends on it, so a replay never asks the clock again. What you can rely on, with `run_saved` and with `run_from`, inside a `mergeMap`, inside a vine run by `use`, and with `catchError` or `retry` before or after the timeout:

- A timeout that fired before the restart fires again at once. The operators it applied to are not run again, so nothing they had done outside is done again, and nothing they were doing is started again. A `catchError` after it goes down the same branch.
- A timeout whose operators were done in time before the restart is replayed as done, with no limit, even when the replay is slower, as a stream operator asked to go on can be.
- What those operators emit is held until they are done, and dropped if the timeout fires. Outputs keep their ids across a restart.
- Each run of those operators keeps one line for its decision. With `run_from` it is dropped with the item's other lines when the position passes the item.

**Know the rule for a restart before the timeout decided.** If the run stopped while the operators before the timeout were still going, the wait starts again from nothing when the run resumes, and they get the whole limit again. Work whose results were saved replays at once and does not count against it. The library has no clock that runs across a restart, so it does not try to honour the time already spent. So after such a restart, the operators can finish in time where the first run would have timed out, or reach calls the first run never reached. Nothing you were handed depended on the decision, so this is a run at another pace, not a contradiction. If the work must not get more time than the limit in total, put the limit in the operator itself.

**Expect a call in flight when a timeout fires to be dropped, not stopped.** The run stops waiting for it and never saves its result, and a replay does not make it again. But the library does not tell the operator. One that already reached outside, such as a request to start an agent, can still have its effect, and a stream operator's work goes on with nobody reading. Make the `catchError` after the timeout undo what such a call may have done, and make the operator safe to have half run.

**Nest a vine that a move starts by pressing its move.** `plan_launch_all` launches each job with `call: move.launch`, not `use: job_launch`. The host checks the job's gates, builds `job_launch`'s input from the job's facts, and lands each output as an event before your vine hears of it. So you pass only what the move's payload takes, and the work stays visible: another vine can react to every event. `move.launch` is a stream operator. It gives one item per inner output, `{ output, event, landed, payload }`, and ends with `{ status }`: `done`, `failed` with a `reason`, or `refused` with the `gate` that refused it. A refusal is data to branch on, not a failure, so the other jobs go on. `plan_launch_all` launches two jobs at once, with `mergeMap` and `concurrent: 2`. Pass your own `request` through: the host builds each inner press's id from it, so after a restart the same press attaches to the first. See `vines/pipe/plan_launch_all.yaml`.

**Run a helper with `use` when the two are one piece of work and no move starts the helper.** `use: job_teardown` runs that vine as an operator, and `on:` says what to do with each thing it emits. The checker makes you handle every output. See `vines/pipe/job_launch_undo.yaml`.

**Say `take: 1` when a vine wants no more than its first answer.** A vine may emit none, one or many. `take: 1` lets at most one item through and ends the stream. It does not promise an output: the stream can be empty, and the operators after `take` decide what is emitted.

**Treat long work that reports as it goes as a stream.** An agent run takes hours and writes lines as it works. Register it as a stream operator. `call` on it gives one item per line, and `end: ended` adds one last item that says how it ended. `job_run` makes the working copy, runs the agent, and commits, all in one vine. See `vines/pipe/job_run.yaml`.

**Run a vine that holds long work with `run_saved`, and keep the journal it builds.** Pass every event to `Journal::record` and save the journal before you read the next event. After a restart, run the vine again with the same input and the journal you saved. Calls whose entry you saved are not made again. A saved run hands out each call's entry before it makes the next call, so a crash can lose at most the one call whose entry you had not saved yet, and that call is made again. A stream operator is asked to go on from where it was, so it must start the work the first time and attach to it after that. Outputs you were already handed are not handed out again, as long as you saved the entry that came after each one. The one you were handed last, if you had not saved the entry after it, comes again with the same id.

**Drop a repeated output by its id, never by its name and payload.** Every output a run hands out has an `id`, an `OutputId`. It says where in the run the output comes from: the input item, the group of work, and how many outputs that group made before it. It is never made from the payload, a clock or a random number. An output that comes again after a restart has the id it had, on both ways to resume, inside a `mergeMap` and inside a vine run by `use`. Two outputs of one run never share an id, so a vine that emits the same name and payload twice gives two outputs, and you keep both. Ids are unique within one run and its restarts, not across runs: if you keep the outputs of many runs in one place, join the id with your own key for the run, as Terra joins it with the request id. A run that saves nothing gives ids too, unique within that run, but they count the whole run in the order outputs come and do not match a saved run's. No saved line holds an id: a replay makes each id again from where the output stands, so what you keep is no bigger, and a journal or bookmark saved before outputs had ids resumes as it did and gives the ids a new run gives.

**Run a vine over an endless stream with `run_from`, and keep the bookmark it builds.** A vine that reads events for months must not replay from the first event after a restart. `run_from` tells you when every event through a number is fully done: its calls finished, its outputs were handed out, and no operator holds it. Pass every event it gives you to `Bookmark::record` and save the bookmark before you read the next. After a restart, feed the events after `bookmark.position()` with the bookmark you saved. The bookmark holds only the lines of events still in flight, so it stays small. See `vines/pipe/event_relay.yaml`, `event_backup.yaml` and `event_pages.yaml`.

**Register a stream operator that can give its items again as replayable.** If the host behind it stores what it gave, as an agent host stores the lines a session wrote, register it with `register_replayable_stream`. Then a saved run keeps how far each call got instead of every item: a job whose agent writes ten thousand lines keeps about 500 bytes, not a megabyte. After a restart the operator is asked for its items from the one after the position, and `bufferCount` refills its batch from them. It must give the same items again from any point it gave. Check `Pipe::saves_every_item()`: it lists every call that is still saved item by item, and why.

**Run independent items side by side with `mergeMap`, and still resume.** Each item that enters the `mergeMap` gets its own group of saved lines, numbered in the order the items went in, not the order they finish. After a restart each item's work replays in its own order, so the items can finish in another order and nothing is lost or run twice. What you can rely on:

- Within one item's work, outputs come in order, before and after a restart. Each keeps its id whatever order the items finish in.
- Across items, outputs come in whatever order the work finishes. After a restart that order can differ from the first run. Do not depend on it.
- Each call is still made once, apart from the calls whose results you had not saved when the crash came. A run makes a call only once you hold every line made so far, so those are the calls that were running, one per item running side by side (more if the item runs `forkJoin`), plus at most one result you were handed and had not saved. With `concurrent: 2` that is at most two calls.
- With `run_from`, an item can finish before an earlier one. The run says so with `Progress::Done(n)`, and `Bookmark::record` keeps the number and drops its lines. The position waits for the earlier item and never goes back. After a restart, feed the items after the position as always; the run skips the ones already done. So the bookmark holds the lines of at most `concurrent` items in flight, plus runs of numbers. Without `concurrent`, every item you feed is in flight at once, and the bookmark grows with how many arrive while one is slow. Over an endless stream, set `concurrent`.

**Pick the mode by whether the run ends.** One job, one plan launch: `run_saved` and a journal. A stream of events with no end: `run_from` and a bookmark. They save the same entries. A bookmark's lines also say which input item each belongs to, and the position is where the caller may cut.

**Expect `bufferCount` to hold the position back.** The events in a half full batch are not done, so the position stays before them, and after a restart they are fed again. A batch ties its events together: the position moves by whole batches, even when a `fork` sends each event on down the main pipe too.

**Start long work and move on when it does not report as it goes.** If the work gives no stream to attach to, start it with one call, emit `started`, and let a second vine pick up when it ends. `job_launch` and `job_session_ended` are that pair.

**Use `fork` to do more than one thing with each item.** Each item goes down every side pipe, then on down the main pipe. A side pipe sees the whole stream, so `bufferCount` and `last` work in it. Write `filter: $item.line` first in a side pipe that should skip a stream operator's end item.

**Use `last` to wait for the end of a stream, and `bufferCount` to batch it.** `bufferCount: { bufferSize: 50 }` gives `{ items, count }`, and what is left when the stream ends. With `into: batch` the item is the last one of the batch, so you keep its fields.

**Use `forkJoin` for quick operators that do not depend on each other.** They run side by side and the pipe goes on when all are done. Each one needs its own `into`.

**Pick the mapping operator on purpose.** Four operators run a pipe, under `do:`, for each item. They differ in what they do when a new item arrives while the last one is still running:

| Operator | What it does | Use it when |
|---|---|---|
| `concatMap` | One at a time, in order | Order matters, or the work must not overlap |
| `mergeMap` | Side by side | The items are independent. Add `concurrent` to cap it. Resumes |
| `switchMap` | Drops the run in flight for the new item | Only the newest matters, such as a refresh |
| `exhaustMap` | Ignores new items while busy | A second request for the same work is noise |

Reach for `concatMap` first: it is the one that cannot surprise you. Only `mergeMap` takes `concurrent`. `switchMap` and `exhaustMap` cannot be resumed.

**Write the RxJS name.** The operators use the RxJS names where RxJS has one. A vine that still says `each`, `overlap`, `at_once`, `collect`, `together` or `catch` is refused when it loads, and the error names the key to write instead. So is `retry` with `times` or `wait` (now `count` and `delay`) and `bufferCount` with `size` (now `bufferSize`).

**Make every operator that reaches outside safe to run twice.** A result can repeat after a crash, and a call whose entry was not saved is made again. One that creates or writes should give the same result the second time: `git.worktree_add` for a name that has a working copy gives that copy, and `agent.start` in a directory with an agent running gives that agent.

**Undo abandoned work in a vine of its own.** When the host refuses an output because a fact the press read changed, it lands an `abandoned` event that carries the output. A reaction to that event presses a cleanup move, whose vine undoes the work from what the output says. Give the cleanup move a gate that refuses when a later run has taken the work over, as `undo_launch` refuses a job that is no longer `ready`. See `vines/pipe/job_launch_undo.yaml`.

## Don't

**Don't put a gate in a vine.** "Is this job ready" and "is the plan active" are gates. They are declared on the move, and whoever runs the vine checks them before it starts. A vine does the work.

**Don't read from outside.** A vine works on the input it was handed. If it needs a fact, the caller puts it in the input.

**Don't write a workflow step as a registered operator.** `git.worktree_add` is an operator. `prepare_job_workspace` is a workflow step, and a workflow step is a vine. The test: could a different workflow use it unchanged?

**Don't wait on long work that is not a stream operator.** No other operator should hold an item for hours. `forkJoin`, the four mapping operators, `retry` and `timeout` are for quick work. `retry` and `timeout` hold what the operators before them give until those operators are done.

**Don't use `switchMap` or `exhaustMap` around work that must finish.** `switchMap` drops a run partway through. If that run had made a working copy, nothing removes it. Use them for work that is safe to abandon.

**Don't use `mergeMap` when the operators touch the same thing.** Two side-by-side runs that write the same branch will collide. Use `concatMap`, or split the work so each run has its own.

**Don't emit the item as it is unless its shape is the output's shape.** Outputs are closed shapes. Use `emit` with `with:` and name the fields.

**Don't rely on `else` to hide a mistake.** A `branch` with no matching case passes the item through unchanged. If every item must be handled, give the branch an `else` and the checker will hold you to it.

**Don't count or batch what comes out of a `mergeMap`, or call an operator on the item `last` picks from it, in a vine you resume.** After a restart the items can finish in another order. `take` would let other items through and `bufferCount` would batch other items, so the checker refuses both after a `mergeMap`. `last` is allowed: it gives the final item to finish, and after a restart that can be another item, as it could be in a run at another pace. So a `call` or `tap` on it is refused, and what you emit from it should use only what every item shares, as `plan_launch_all`'s `done` does. The output keeps its id whichever item `last` picked: if it comes again after a restart with another item's fields, it is still the output you were handed, and a caller that drops repeats by id keeps the first. Count or batch inside the `mergeMap`, where one item's work stays in order.

**Don't put a `catchError` or `finalize` after a `mergeMap` it would apply to, in a vine you resume.** At the first failure they drop the items still running, and how far those got depends on timing. Put the `catchError` inside the `mergeMap`: then one item's failure becomes its own output and the others go on. A failure nothing catches ends the run, and the items still running stop where they are; after a restart the run stops at the same failure again, and the calls those items were making start again.

**Don't put `take`, `mergeMap`, `switchMap` or `exhaustMap` before `catchError`, `finalize`, `retry` or `timeout`.** Those work across the whole stream, and these four apply to the operators before them one item at a time. The checker refuses it. Put the stream-wide operator after.

**Know what "the end" means before `catchError`, `finalize`, `retry` or `timeout`.** Each runs the operators before it once for each item. So there, `last` and `bufferCount` see the end of that one item's own stream, such as one agent run. That is what `job_run` wants. They need an operator before them that makes many items from one, such as `split` or a stream operator. Without one they could only ever see one item, and the checker refuses them.

**Don't read from `finalize` what an operator before it added.** `finalize` gets the item as it arrived. `job_run` makes the working copy before it, so its `finalize` removes it by the job's name, not by `$item.tree.path`.

**Don't expect `finalize` to run when the caller drops a run.** A dropped run leaves its working copy, and its agent goes on with nobody reading. Resume it from the journal, or clean up from outside.

**Don't use `run_saved` on a vine with a `switchMap` or `exhaustMap`.** It drops or skips work by when items arrive, and a replay cannot know when they did, so `run_saved` and `run_from` refuse such a vine. Resume the same vine with the same input: the journal is keyed by where each call stands in the pipe, inside a `mergeMap` by which item it was, and under a `timeout` by which run of the timeout it was.

**Don't resume a journal saved before you added a `timeout`.** The calls a timeout applies to are keyed by its runs, so a journal saved by the vine without it names them in a way the vine no longer asks for. `run_saved` and `run_from` refuse it with `ReplayMisfit` before any call is made. Let such a run finish on the old vine, or start it again.

**Don't use `run_from` on a vine that counts the whole stream.** A `take` over the whole stream would count from nothing again after a restart. A vine that ends itself has an end: run it with `run_saved`. `take` inside a mapping operator, or before `catchError`, `finalize`, `retry` or `timeout`, counts one item's own stream and is fine.

**Don't batch pieces of events across the whole stream with `run_from`.** A `bufferCount` after `split`, a stream operator, a mapping operator, `use` or `branch` at the top of the pipe could end a batch partway through one event, and the position could never pass it. Batch whole events, or batch inside a mapping operator where the batch belongs to one event.

**Don't expect a restart to get past a failure nothing catches.** The position stays before the event that failed, and its saved failure replays: the run stops at the same place every time. Add a `catchError` to the vine and run it again from the same bookmark. The failed call keeps its path, so the saved failure is handled without asking the operator again. Dropping the event's lines asks the operator again instead.

**Don't feed `run_from` anything but the events after the position, in the order you fed them before.** A saved call whose input does not match is a run error. A bookmark whose keys happen to fit another vine is not caught.

**Don't let one event's stream operator run for months under `run_from` unless it is replayable.** A stream operator saved item by item keeps its lines in the bookmark until the event is done. One saved by position keeps a few lines however long it runs.

**Don't put `take` after a replayable stream operator, or `retry` after it.** `take` counts what the operator gives, and after a restart it would count from the position. `retry` drops what a failed attempt gave, so the run cannot tell what is done. The call is then saved item by item. Put `take` inside a mapping operator, where it counts one item's own stream, and leave `retry` to quick operators after the stream operator.

**Batch a stream operator's items where the batch holds one call's items.** `bufferCount` after the `call`, in the same pipe or a side pipe, before `catchError`, `finalize`, `retry` or `timeout`, as `job_run` does, holds one call's lines. A `bufferCount` that could mix two calls, such as one over the whole stream of events or after a mapping operator that makes the call, makes the call saved item by item.

**Don't emit from `finalize`.** It is for cleanup. The checker refuses it.

**Don't `retry` an operator that is not safe to repeat.** A retry runs every operator before it again from the item as it arrived.

**Don't `use` a vine that a move starts.** It would skip the move's gates, and nobody would build its input. Press the move instead. The host refuses a package of vines in which one does, even through a helper: the library lists what a vine uses with `Pipe::uses`, and the host knows which vines its moves start.

**Don't `use` a vine when others need to hear what happens between the two.** A used vine runs inside the outer vine's run. Nothing it emits is seen from outside unless the outer vine emits it again. When a step should be visible, be something others can react to, or take hours, press its move, or emit an output and let a separate vine pick it up.

**Don't let a move's event press the same move again unless a gate stops it.** The host refuses such a loop at install. It allows one only when a gate on the loop reads a fact of the move's own target that the loop's own events set to a value the gate refuses, as `core.job.started` sets the job's `state` that `launch` gates on. A gate on another artifact, a loop through a nested press, or a reaction that takes its target from the event's payload does not count.

**Don't `use` a vine from another trellis.** A trellis has one version, so the vines in it change together. A vine used across trellises could change under you.

## What the checker catches before a vine runs

- An operator name nobody registered.
- `call` or `tap` on a pure operator, and an operator that touches something outside written by its name instead of with `call` or `tap`.
- Input to a registered operator that does not fit what it declares.
- A reference to anything but `$item`, or to a field the item is known not to have.
- A vine used before it exists, an output of a used vine that nothing handles, or input that cannot fit it.
- `catchError`, `finalize`, `retry` or `timeout` with nothing before it, or with a stream-wide operator before it.
- An output nobody declared, or a payload that cannot fit it.
- A pipe, or a case of a `branch`, a `catchError` or a `use`, that does not end at an `emit`. A `branch` with no `else` counts. A `filter` or `take` before the `emit` can still drop an item, so a run can give no output.
- An operator that can never run because everything before it already emitted.
- `fork` with no side pipe, an emit in a side pipe to an output nobody declared, and a `finalize` that holds a fork that emits. An emit in a side pipe does not count as the main pipe emitting.
- `bufferCount` with a `bufferSize` of 0.
- `last` or `bufferCount` where they could only ever see one item.
- `concurrent` on any mapping operator but `mergeMap`, or a `concurrent` of 0.
- An old key: `each`, `overlap`, `at_once`, `collect`, `together` or `catch`, or an old argument: `times` or `wait` on `retry`, `size` on `bufferCount`. The error names the key that replaced it.
- `tap` on a stream operator, a stream operator inside `forkJoin`, and `end` on an operator that is not a stream operator.
- A replayable stream operator where a later item could not rebuild what comes after it: a `take` that its items reach, a `bufferCount` that could mix two calls, or a `retry` after it. The call is saved item by item, and `Pipe::saves_every_item` says where and why. A vine that refuses this for a vine it runs by `use` cannot be resumed.
- For both ways to resume: `switchMap` and `exhaustMap`, `take` or `bufferCount` after a `mergeMap` (in a side pipe too), a `call` or `tap` on the item `last` picked after one, and a `catchError` or `finalize` that applies to one at any depth. `Pipe::resumable` says so, and the run gives `NotResumable` with the reason and the operator's path. A `timeout` is not refused.
- A journal or bookmark that holds a call a `timeout` applies to, saved with no run of that timeout in its key: it was saved before the timeout was added. The run gives `ReplayMisfit` before any call is made.
- For `run_from`, besides what `run_saved` refuses: `take` over the whole stream, and `bufferCount` over the whole stream after an operator that makes many items from one. `Pipe::resumable_from_position` says so, and `run_from` gives `NotResumable` with the reason and the operator's path.

## What it does not catch

- A field read from an item whose shape is not known at that point. After `split` with no `into`, inside `catchError` and `finalize`, and after a mapping operator, `use`, `branch`, `catchError`, `finalize`, `retry` or `timeout`, the checker no longer knows the item's fields. A misspelt field reads as nothing.
- A `branch` case that is misspelt. It never matches.
- An operator that lies about what it touches.
- A stream operator that does not go on from `resume_from`. A replay trusts it to.
- A stream operator that says it is replayable and gives other items. A run sees only one that stops after fewer items than it gave before it stopped. Outputs handed out before a restart are skipped by count, so one that gives other items can hand out a different output in place of one already handed.
- Whether an id you hold was made by this run. Ids say where an output stands, not which run or vine made it: two runs give the same ids. Keep them apart with your own key for the run.
- A timeout's saved decision used for another item. The decision is not checked against the item, so a journal saved for other input can fail an item at once that would have been in time.
- A journal saved for a different vine. A saved call whose input no longer matches is a run error, and so is an output the replay does not make again. Anything else is not checked.
- An output emitted from the item `last` picked after a `mergeMap`, when the item matters. After a restart it can be another item's. An output in the window between being handed out and its line being saved comes again, and from such a `last` it can come with another item's fields, under the same id.
- A `use` of a vine that a move starts, or moves that start each other in a loop. The library knows no moves. The host checks both when it installs a package of vines, from `Pipe::uses` and `Pipe::calls`.

## Not built yet

- Resuming a vine with `switchMap` or `exhaustMap`, in either mode. They would need the run to save what time decided: which item a switch dropped and how far it got, and which items an exhaust ignored.
- A timeout that honours, after a restart, the time its operators had already taken. It would need a clock that runs across a restart, which the library does not have.
- Telling an operator that a timeout stopped waiting for it.
- Making a journal smaller for a stream operator that is not replayable. Every item it gives is saved until its input item is done.
- A position for an operator that keeps a count or a running total across events. Such state would need to be saved itself.
- Telling a stream operator that the run stopped reading. Dropping a run, or a `catchError` that drops the rest of an item's work, leaves the stream operator's work going.
- `retry` waits again on replay, even when the attempts it waits for are saved.
- Waiting operators such as `debounceTime`, and `takeUntil` to stop work when a cancel arrives.
- `finalize` when the caller drops the stream partway.
- A name for the reach each operator uses, such as "GitHub as reviewer".
