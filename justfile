# Gates for vines. `just preflight` runs them all; run it before pushing.
# While working, run the one you need (`just clippy`, `just test`, ...).

set shell := ["bash", "-euo", "pipefail", "-c"]

# List the recipes.
default:
    @just --list

# Format the workspace.
fmt:
    cargo fmt --all

# Fail on unformatted code.
fmt-check:
    cargo fmt --all --check

# Clippy over every target, warnings denied (lints: Cargo.toml).
clippy:
    cargo clippy --workspace --all-targets --locked -- -D warnings

# The test suite under nextest (no retries), then the doctests nextest skips.
test *args:
    cargo nextest run --workspace --locked {{args}}
    cargo test --workspace --doc --locked

# Build the docs with warnings denied.
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked

# Code-shape limits: 800 non-test lines a file, no Result<_, String>, no
# history comments (scripts/code-shape.py).
[doc('Code-shape limits: 800 non-test lines a file, no Result<_, String>, no history comments')]
shape:
    python3 scripts/code-shape.py

# Tests for the gate scripts themselves.
script-test:
    python3 -m unittest discover -s scripts/tests -p 'test_*.py'
    bash scripts/tests/test_mutants.sh

# Mutation-test the whole workspace; any missed mutant fails.
mutants *flags:
    bash scripts/mutants.sh --all {{flags}}

# Mutation-test only the lines changed against origin/main; any missed
# mutant fails. `--warn` reports instead; paths narrow the diff.
[doc('Mutation-test the lines changed against origin/main; any missed mutant fails')]
mutants-touched *args:
    bash scripts/mutants.sh {{args}}

# Run the whole suite <runs> times (nextest stress mode, no retries) and
# list every test that failed in any run. Extra args go to nextest, e.g.
# `just flaky-hunt 50 --test-threads 32` or `-E 'test(batch)'`.
[doc('Run the suite <runs> times with no retries; list every test that failed in any run')]
flaky-hunt runs *args:
    cargo nextest run --workspace --locked --profile flaky-hunt --stress-count {{runs}} {{args}}

# Every gate, cheapest first. Must pass on the head you push.
preflight: fmt-check clippy doc shape script-test test mutants

# Run fmt-check and clippy before each commit (.githooks/pre-commit).
# core.hooksPath is shared by every worktree of this repo; a lane whose
# branch has no .githooks/ runs no hook.
[doc('Run fmt-check and clippy before each commit (.githooks/pre-commit)')]
install-hooks:
    git config core.hooksPath .githooks
    @echo "core.hooksPath = .githooks; bypass one commit with 'git commit --no-verify'"
