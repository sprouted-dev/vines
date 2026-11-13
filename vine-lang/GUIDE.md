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

**Handle a failure inside the pipe that can fail.** Wrap the stages in `attempt`. One item failing then becomes one `failed` output, and the items after it still run. A failure nothing handles ends the whole stream.

**Clean up in `on_error`, using the item as it was.** `on_error` gets the item as it was when the stage failed, plus the failure as `$item.error`. Branch on `$item.error.from` to see which stage failed, and undo only what had been done by then. See `vines/pipe/job_launch.yaml`.

**Say `take: 1` when a vine must give exactly one answer.** A vine may emit none, one or many. If the caller needs one, the vine says so itself.

**Start long work and move on.** An agent run takes hours. Start it with one act, emit `started`, and let a second vine pick up when the session ends. `job_launch` and `job_session_ended` are that pair.

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

**Don't wait on long work.** No stage should hold an item for hours. `together` and `each` are for quick acts.

**Don't use `switch` or `exhaust` around work that must finish.** `switch` drops a run partway through. If that run had made a working copy, nothing removes it. Use them for work that is safe to abandon.

**Don't use `merge` when the acts touch the same thing.** Two side-by-side runs that write the same branch will collide. Use `concat`, or split the work so each run has its own.

**Don't emit the item as it is unless its shape is the output's shape.** Outputs are closed shapes. Use `emit` with `with:` and name the fields.

**Don't rely on `else` to hide a mistake.** A `branch` with no matching case passes the item through unchanged. If every item must be handled, give the branch an `else` and the checker will hold you to it.

**Don't repeat cleanup you cannot keep in step.** There is no `finally` yet. `job_session_ended` removes the working copy in three places. If you add a fourth path, add the cleanup too.

## What the checker catches before a vine runs

- A stage that names an operator or act nobody registered.
- Input to an act that does not fit what the act declares.
- A reference to anything but `$item`, or to a field the item is known not to have.
- An output nobody declared, or a payload that cannot fit it.
- A path through the pipe that could end without emitting.
- A stage that can never run because everything before it already emitted.

## What it does not catch

- A field read from an item whose shape is not known at that point. After `split` with no `into`, inside `on_error`, and after `each`, `attempt` or `branch`, the checker no longer knows the item's fields. A misspelt field reads as nothing.
- A `branch` case that is misspelt. It never matches.
- An act that lies about what it touches.

## Not built yet

- Saved positions, memory across a restart, and resuming after a crash.
- `finally`, timeouts, retries and waiting (`debounce`).
- A name for the reach each stage uses, such as "GitHub as reviewer".
