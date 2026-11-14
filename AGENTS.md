# vines

`terra-vines` (lib `vines`) is in-process stream composition for workflow operators. Read README.md for what it owns and what it leaves to Terra.

## Standards

Before writing, reviewing or deleting Rust or tests, load both skills and follow them:

```bash
sprout skill show clean-code
sprout skill show test-quality
```

The gates enforce what they can. Everything starts at zero, with no baseline and no grandfathered file. Fix the code, never the limit.

- Silence one lint site with `#[expect(lint, reason = "...")]`. Plain `#[allow]` fails clippy.
- Tests run on a paused Tokio clock (`start_paused = true`). Wait on a channel, an end marker or `common::until_idle()`.
- nextest never retries. A flaky test is a bug: hunt it with `just flaky-hunt 50`, then fix the cause.
- A missed mutant needs a test that fails for it. An exclusion in `.cargo/mutants.toml` needs a reason beside it.

## Gates

`just --list` names them. While working, run the targeted ones for what you touched (`just clippy`, `just test <filter>`, `just shape`, `just mutants-touched`). Before every push, run `just preflight` on the exact head you push and paste its result.

`just install-hooks` adds a pre-commit hook (fmt-check and clippy, a few seconds). It sets `core.hooksPath`, which every worktree of this repo shares. Commit with `--no-verify` only to skip a hook that is slow for a reason you can name.

## Worktrees

Work in a lane, locked at creation so cleanup sweeps leave it alone:

```bash
git worktree add .trees/feature/<name> -b feature/<name> origin/main && git worktree lock --reason "in use: <session>" .trees/feature/<name>
```

A fresh lane has a cold `target/`. Run mutation testing from a lane: `scripts/mutants.sh` refuses a checkout with lanes under `.trees/`. Leave locks you did not set alone. Unlock and remove your lane once it is merged.

The remote is `entire://`, not GitHub. Push your branch; track follow-ups in Terra (`sprout`).
