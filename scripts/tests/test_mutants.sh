#!/usr/bin/env bash
# Tests for scripts/mutants.sh against a throwaway git repo (a root package,
# a member crate and a nested crate outside the workspace) with a `cargo`
# stub on PATH (metadata, nextest, and the mutants subcommand): nothing
# touched, tools missing, the diff and crate scope it hands cargo-mutants,
# the whole-workspace scope, missed mutants in strict and warn mode, every
# tool error passed through, both caps, and bad arguments.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$ROOT/scripts/mutants.sh"
# Resolved, because git (and so the script) reports the physical path.
TEST_TMP=$(cd "$(mktemp -d)" && pwd -P)
trap 'rm -rf "$TEST_TMP"' EXIT
fail() { echo "ASSERTION FAILED: $*" >&2; exit 1; }

REPO="$TEST_TMP/repo"
BIN="$TEST_TMP/bin"; LOG="$TEST_TMP/cargo.log"
mkdir -p "$BIN"
# The stub's behavior comes from STUB_* variables:
#   STUB_NO_MUTANTS=1   `cargo mutants` is not installed
#   STUB_NO_NEXTEST=1   `cargo nextest` is not installed
#   STUB_METADATA_RC=N  `cargo metadata` exits N
#   STUB_LIST=N         `--list` prints N mutants (default 2)
#   STUB_LIST_RC=N      `--list` exits N
#   STUB_CAUGHT/STUB_MISSED/STUB_TIMEOUT/STUB_UNVIABLE=N  outcomes of a run
#   STUB_RUN_RC=N       a run exits N (default: 2 when any missed, else 0)
#   STUB_RUN_SLEEP=N    a run records one caught mutant, then sleeps N
#                       seconds; SIGTERM ends it with exit 70 (cargo-mutants'
#                       "interrupted")
cat > "$BIN/cargo" <<'SH'
#!/usr/bin/env bash
printf 'cargo %s\n' "$*" >> "$LOG"
case "${1:-}" in
  metadata)
    [ -z "${STUB_METADATA_RC:-}" ] || { echo "error: stub metadata failure" >&2; exit "$STUB_METADATA_RC"; }
    printf '{"workspace_root":"%s","target_directory":"%s/target","packages":[' "$PWD" "$PWD"
    printf '{"name":"alpha","manifest_path":"%s/Cargo.toml"},' "$PWD"
    printf '{"name":"beta","manifest_path":"%s/beta/Cargo.toml"}]}\n' "$PWD"
    exit 0 ;;
  nextest) [ -z "${STUB_NO_NEXTEST:-}" ] || exit 101; echo "cargo-nextest 0.9.0"; exit 0 ;;
  mutants) ;;
  *) echo "unexpected cargo call: $*" >&2; exit 9 ;;
esac
if [ -n "${STUB_NO_MUTANTS:-}" ]; then echo "error: no such command: \`mutants\`" >&2; exit 101; fi
shift
[ "${1:-}" != --version ] || { echo "cargo-mutants 27.1.0"; exit 0; }
if [ "${1:-}" = --list ]; then
  [ -z "${STUB_LIST_RC:-}" ] || { echo "error: stub list failure" >&2; exit "$STUB_LIST_RC"; }
  i=1; while [ "$i" -le "${STUB_LIST:-2}" ]; do echo "src/lib.rs:$i:5: replace add -> i32 with $i"; i=$((i + 1)); done
  exit 0
fi
out=""; prev=""
for a in "$@"; do [ "$prev" != --output ] || out="$a"; prev="$a"; done
[ -n "$out" ] || { echo "stub: no --output" >&2; exit 9; }
r="$out/mutants.out"; mkdir -p "$r"
: > "$r/caught.txt"; : > "$r/missed.txt"; : > "$r/timeout.txt"; : > "$r/unviable.txt"
if [ -n "${STUB_RUN_SLEEP:-}" ]; then
  echo "src/lib.rs:1:5: replace add -> i32 with 1" > "$r/caught.txt"
  # Like cargo-mutants, stop the child too: an orphaned sleep would hold the
  # test's output pipe open until it finished.
  sleep "$STUB_RUN_SLEEP" & nap=$!
  trap 'kill "$nap"; exit 70' TERM
  wait "$nap"
  exit 0
fi
# A counted loop, not `seq`: BSD `seq 1 0` counts down and prints two lines.
emit() { local i=1; while [ "$i" -le "$2" ]; do echo "src/lib.rs:$((i + 10)):9: replace + with - in add ($1 $i)"; i=$((i + 1)); done >> "$r/$1.txt"; }
emit caught "${STUB_CAUGHT:-0}"; emit missed "${STUB_MISSED:-0}"
emit timeout "${STUB_TIMEOUT:-0}"; emit unviable "${STUB_UNVIABLE:-0}"
default_rc=0; [ "${STUB_MISSED:-0}" = 0 ] || default_rc=2
exit "${STUB_RUN_RC:-$default_rc}"
SH
chmod +x "$BIN/cargo"

# A root package (alpha), a member (beta) and a nested crate with its own
# `[workspace]` (nested), which cargo metadata does not list.
reset() {
  rm -rf "$REPO"; mkdir -p "$REPO/src" "$REPO/beta/src" "$REPO/nested/src" "$REPO/scripts"
  git -C "$REPO" init -q
  printf '[package]\nname = "alpha"\n' > "$REPO/Cargo.toml"
  printf '[package]\nname = "beta"\n' > "$REPO/beta/Cargo.toml"
  printf '[package]\nname = "nested"\n[workspace]\n' > "$REPO/nested/Cargo.toml"
  printf 'pub fn add(a: i32, b: i32) -> i32 {\n    a + b\n}\n' > "$REPO/src/lib.rs"
  printf 'pub fn one() -> i32 {\n    1\n}\n' > "$REPO/beta/src/lib.rs"
  printf 'pub fn two() -> i32 {\n    2\n}\n' > "$REPO/nested/src/lib.rs"
  printf 'notes\n' > "$REPO/README.md"
  printf 'target/\n' > "$REPO/.gitignore"
  cp "$SCRIPT" "$REPO/scripts/mutants.sh"
  git -C "$REPO" add -A
  git -C "$REPO" -c user.name=t -c user.email=t@t commit -qm init
  git -C "$REPO" update-ref refs/remotes/origin/main HEAD
  : > "$LOG"
}
touch_alpha() { printf 'pub fn add(a: i32, b: i32) -> i32 {\n    a + b + 0\n}\n' > "$REPO/src/lib.rs"; }
# run [ENV=VALUE...] [-- script args]: sets $out and $rc.
run() {
  local envs=() args=()
  while [ $# -gt 0 ]; do
    if [ "$1" = -- ]; then shift; args=("$@"); break; fi
    envs+=("$1"); shift
  done
  set +e
  out=$(cd "$REPO" && env PATH="$BIN:$PATH" LOG="$LOG" ${envs[@]+"${envs[@]}"} bash scripts/mutants.sh ${args[@]+"${args[@]}"} 2>&1)
  rc=$?
  set -e
}
ran_mutants() { grep -Eq '^cargo mutants (--in-diff|-p)' "$LOG"; }
patch="$REPO/target/mutants/in-diff.patch"

# 1. Nothing touched: a non-Rust change passes by saying so, without
#    asking whether cargo-mutants is installed or running it.
reset; printf 'more\n' >> "$REPO/README.md"
run
[[ "$rc" == 0 && "$out" == *"no Rust file changed against the base; nothing to mutate"* ]] || fail "nothing touched: rc=$rc $out"
grep -q 'cargo mutants' "$LOG" && fail "nothing touched must not call cargo mutants: $(cat "$LOG")"

# 2. Tools missing: strict by default (exit 3), --warn passes with one
#    install line; neither runs.
reset; touch_alpha
run STUB_NO_MUTANTS=1
[[ "$rc" == 3 && "$out" == *"FAIL (strict): cargo-mutants is not installed"* ]] || fail "missing tool is strict by default: rc=$rc $out"
run STUB_NO_MUTANTS=1 -- --warn
[[ "$rc" == 0 ]] || fail "missing tool should pass with --warn, got $rc: $out"
[[ "$out" == *"mutants: warning: cargo-mutants is not installed, so mutation testing was skipped; install it with: cargo install --locked cargo-mutants"* ]] || fail "install hint: $out"
[ "$(printf '%s\n' "$out" | grep -c 'cargo install')" = 1 ] || fail "install hint should be one line: $out"
run STUB_NO_NEXTEST=1
[[ "$rc" == 3 && "$out" == *"cargo-nextest is not installed"* ]] || fail "missing nextest: rc=$rc $out"
ran_mutants && fail "missing tools must not run"

# 3. Missed mutants fail by default: exit 2, the counts, and each missed
#    mutant as file:line: description. The run is scoped to the touched
#    crate and the diff, and writes under target/.
reset; touch_alpha
run STUB_CAUGHT=3 STUB_MISSED=2 STUB_TIMEOUT=1 STUB_UNVIABLE=4
[[ "$rc" == 2 && "$out" == *"FAIL (strict): 2 missed mutant(s)"* ]] || fail "missed mutants fail by default: rc=$rc $out"
for pair in "caught   3" "missed   2" "timeout  1" "unviable 4"; do
  [[ "$out" == *"  $pair"* ]] || fail "summary should show '$pair': $out"
done
[[ "$out" == *"  src/lib.rs:11:9: replace + with - in add (missed 1)"* ]] || fail "first missed mutant: $out"
[[ "$out" == *"  src/lib.rs:12:9: replace + with - in add (missed 2)"* ]] || fail "second missed mutant: $out"
run_line=$(grep '^cargo mutants --in-diff' "$LOG")
[[ "$run_line" == "cargo mutants --in-diff $patch -p alpha --output $REPO/target/mutants --copy-target=true --jobs 4 --colors never --timeout-multiplier 2 --minimum-test-timeout 30" ]] || fail "run arguments: $run_line"
grep -q "^cargo mutants --list --in-diff $patch -p alpha --colors never\$" "$LOG" || fail "list arguments: $(cat "$LOG")"
grep -q '^+++ b/src/lib.rs$' "$patch" || fail "patch should carry src/lib.rs: $(cat "$patch")"
grep -q '^+    a + b + 0$' "$patch" || fail "patch should carry the changed line: $(cat "$patch")"
grep -q 'beta' "$patch" && fail "patch must not carry untouched beta"

# 4. --warn (or MUTANTS_STRICT=0) reports missed mutants and passes; all
#    caught passes without a warning; timeouts alone are not missed.
run STUB_CAUGHT=1 STUB_MISSED=1 -- --warn
[[ "$rc" == 0 && "$out" == *"mutants: warning: 1 missed mutant(s)."*"(--warn; strict mode fails on this)"* ]] || fail "--warn: rc=$rc $out"
run STUB_CAUGHT=1 STUB_MISSED=1 MUTANTS_STRICT=0
[[ "$rc" == 0 ]] || fail "MUTANTS_STRICT=0: rc=$rc $out"
run STUB_CAUGHT=1 STUB_MISSED=1 MUTANTS_STRICT=0 -- --strict
[[ "$rc" == 2 ]] || fail "--strict overrides MUTANTS_STRICT=0: rc=$rc $out"
run STUB_CAUGHT=2
[[ "$rc" == 0 && "$out" == *"  caught   2"* ]] || fail "all caught: rc=$rc $out"
[[ "$out" == *warning* ]] && fail "all caught must not warn: $out"
run STUB_CAUGHT=1 STUB_TIMEOUT=1 STUB_RUN_RC=3
[[ "$rc" == 0 && "$out" == *"  timeout  1"* ]] || fail "timeouts alone: rc=$rc $out"

# 5. Tool errors are never a warning: bad arguments (1), a failing
#    unmutated build or test run (4) and an internal error (70) pass through,
#    with --warn too, and so do a failing --list and cargo metadata.
for code in 1 4 70; do
  run STUB_RUN_RC="$code" -- --warn
  [[ "$rc" == "$code" && "$out" == *"mutants: FAIL"* ]] || fail "run exit $code should pass through: rc=$rc $out"
  [[ "$out" == *"warning"* ]] && fail "run exit $code must not be reported as a warning: $out"
done
run STUB_RUN_RC=4
[[ "$out" == *"the unmutated build or tests fail"* ]] || fail "baseline failure message: $out"
: > "$LOG"
run STUB_LIST_RC=1
[[ "$rc" == 1 && "$out" == *"error: stub list failure"* && "$out" == *"cargo mutants --list failed (exit 1)"* ]] || fail "list failure: rc=$rc $out"
ran_mutants && fail "a failed --list must not run mutants"
run STUB_METADATA_RC=101
[[ "$rc" == 101 && "$out" == *"cargo metadata failed (exit 101)"* ]] || fail "metadata failure: rc=$rc $out"

# 6. Mutant-count cap: refused before any build, with the count and the cap;
#    strict exits 3, --warn passes.
: > "$LOG"
run STUB_LIST=5 MUTANTS_MAX_MUTANTS=3
[[ "$rc" == 3 && "$out" == *"FAIL (strict): cap hit: 5 mutants exceeds MUTANTS_MAX_MUTANTS=3"* ]] || fail "count cap: rc=$rc $out"
ran_mutants && fail "count cap must not run mutants"
run STUB_LIST=5 MUTANTS_MAX_MUTANTS=3 -- --warn
[[ "$rc" == 0 && "$out" == *"warning: cap hit: 5 mutants"* ]] || fail "count cap --warn: rc=$rc $out"
run STUB_LIST=3 MUTANTS_MAX_MUTANTS=3 STUB_CAUGHT=3
[[ "$rc" == 0 && "$out" == *"  caught   3"* ]] || fail "at the cap should run: rc=$rc $out"
run STUB_LIST=0
[[ "$rc" == 0 && "$out" == *"no mutants to test"* ]] || fail "no mutants listed: rc=$rc $out"

# 7. Time cap: the watchdog stops the run, the outcomes written so far are
#    reported, and the cap is named; strict exits 3, --warn passes.
start=$(date +%s)
run STUB_RUN_SLEEP=60 MUTANTS_MAX_SECONDS=1
[ $(( $(date +%s) - start )) -lt 30 ] || fail "time cap should stop the run promptly"
[[ "$rc" == 3 && "$out" == *"FAIL (strict): cap hit: stopped after MUTANTS_MAX_SECONDS=1s with 1 of 2 mutants tested"* ]] || fail "time cap: rc=$rc $out"
[[ "$out" == *"  caught   1"* ]] || fail "time cap should report partial outcomes: $out"
run STUB_RUN_SLEEP=60 MUTANTS_MAX_SECONDS=1 -- --warn
[[ "$rc" == 0 && "$out" == *"warning: cap hit: stopped after"* ]] || fail "time cap --warn: rc=$rc $out"

# 8. Scope details: an untracked new file is in the diff, a file in a crate
#    outside the workspace is named and skipped, only such files means
#    nothing to mutate, MUTANTS_TIMEOUT and MUTANTS_JOBS reach the run, and
#    an earlier run's results never leak into this one.
reset
mkdir -p "$REPO/beta/src/new"; printf 'pub fn three() -> i32 {\n    3\n}\n' > "$REPO/beta/src/new/mod.rs"
printf 'pub fn two() -> i32 {\n    2 + 0\n}\n' > "$REPO/nested/src/lib.rs"
run STUB_CAUGHT=1 MUTANTS_TIMEOUT=90 MUTANTS_JOBS=2
[[ "$rc" == 0 ]] || fail "untracked file run: rc=$rc $out"
grep -q '^+++ b/beta/src/new/mod.rs$' "$patch" || fail "untracked file should be in the diff: $(cat "$patch")"
[[ "$out" == *"not in a workspace member, not mutated: nested/src/lib.rs"* ]] || fail "nested-crate file should be named: $out"
run_line=$(grep '^cargo mutants --in-diff' "$LOG" | tail -1)
[[ "$run_line" == *" -p beta "* && "$run_line" != *"-p alpha"* && "$run_line" == *"--jobs 2 "* && "$run_line" == *"--timeout 90" ]] || fail "scoped run arguments: $run_line"
reset; printf 'pub fn two() -> i32 {\n    2 + 0\n}\n' > "$REPO/nested/src/lib.rs"
run
[[ "$rc" == 0 && "$out" == *"no changed Rust file belongs to a workspace member"* ]] || fail "nested only: rc=$rc $out"
ran_mutants && fail "nested only must not run mutants"
reset; touch_alpha
run STUB_MISSED=1
run STUB_RUN_SLEEP=60 MUTANTS_MAX_SECONDS=1
[[ "$out" != *"missed   1"* ]] || fail "a stopped run must not report the previous run's missed mutants: $out"
[ -f "$REPO/target/mutants/mutants.out.old/missed.txt" ] || fail "the previous results should move to mutants.out.old"

# 8b. Paths narrow the diff: only changed .rs files under them count.
reset; touch_alpha; printf 'pub fn one() -> i32 {\n    1 + 0\n}\n' > "$REPO/beta/src/lib.rs"
run STUB_CAUGHT=1 -- beta/
[[ "$rc" == 0 && "$out" == *"mutants: crates beta"* ]] || fail "path filter: rc=$rc $out"
grep -q '^+++ b/beta/src/lib.rs$' "$patch" || fail "path filter should carry beta: $(cat "$patch")"
grep -q '^+++ b/src/lib.rs$' "$patch" && fail "path filter must leave alpha out: $(cat "$patch")"
run -- README.md
[[ "$rc" == 0 && "$out" == *"no Rust file changed"* ]] || fail "path with no Rust change: rc=$rc $out"

# 9. --all: every member, no diff, even with nothing changed; it takes no
#    paths.
reset; : > "$LOG"
run STUB_CAUGHT=2 -- --all
[[ "$rc" == 0 && "$out" == *"mutants: whole workspace"* && "$out" == *"mutants: crates alpha beta"* ]] || fail "--all: rc=$rc $out"
run_line=$(grep '^cargo mutants -p' "$LOG")
[[ "$run_line" == "cargo mutants -p alpha -p beta --output $REPO/target/mutants --copy-target=true --jobs 4 --colors never --timeout-multiplier 2 --minimum-test-timeout 30" ]] || fail "--all run arguments: $run_line"
grep -q -- '--in-diff' "$LOG" && fail "--all must not pass a diff: $(cat "$LOG")"
run STUB_MISSED=1 -- --all
[[ "$rc" == 2 ]] || fail "--all is strict too: rc=$rc $out"
run -- --all src/
[[ "$rc" == 64 && "$out" == *"--all mutates the whole workspace; it takes no paths"* ]] || fail "--all with paths: rc=$rc $out"

# 10. A checkout with lanes under .trees/ (a main checkout, whose copies
#     would hold every lane) is refused as unchecked; an empty .trees/ is not.
reset; touch_alpha
mkdir -p "$REPO/.trees"
run STUB_CAUGHT=1
[[ "$rc" == 0 && "$out" == *"  caught   1"* ]] || fail "an empty .trees/ should run: rc=$rc $out"
mkdir -p "$REPO/.trees/feature"; : > "$LOG"
run
[[ "$rc" == 3 && "$out" == *"FAIL (strict): this checkout has lanes under .trees/"*"run from a lane"* ]] || fail ".trees refusal: rc=$rc $out"
ran_mutants && fail ".trees refusal must not run mutants"
run -- --warn
[[ "$rc" == 0 && "$out" == *"warning: this checkout has lanes under .trees/"* ]] || fail ".trees refusal --warn: rc=$rc $out"

# 11. Bad knobs and arguments are usage errors (64), not runs.
reset; touch_alpha
run MUTANTS_MAX_SECONDS=soon
[[ "$rc" == 64 && "$out" == *"MUTANTS_MAX_SECONDS must be a positive whole number"* ]] || fail "bad knob: rc=$rc $out"
run MUTANTS_JOBS=0
[[ "$rc" == 64 && "$out" == *"MUTANTS_JOBS must be a positive whole number, got '0'"* ]] || fail "zero jobs: rc=$rc $out"
run MUTANTS_STRICT=yes
[[ "$rc" == 64 && "$out" == *"MUTANTS_STRICT must be 0 or 1, got 'yes'"* ]] || fail "bad strict: rc=$rc $out"
run -- --bogus
[[ "$rc" == 64 && "$out" == *"unknown argument --bogus"* ]] || fail "bad argument: rc=$rc $out"
run MUTANTS_BASE=no-such-rev
[[ "$rc" == 64 && "$out" == *"MUTANTS_BASE=no-such-rev is not a commit"* ]] || fail "bad base: rc=$rc $out"
run -- --help
[[ "$rc" == 0 && "$out" == *"mutation-test"* && "$out" != *"set -uo"* ]] || fail "--help: rc=$rc $out"
set +e
out=$(cd "$TEST_TMP" && env PATH="$BIN:$PATH" LOG="$LOG" bash "$SCRIPT" 2>&1); rc=$?
set -e
[[ "$rc" == 1 && "$out" == *"not inside a git checkout"* ]] || fail "outside a checkout: rc=$rc $out"

echo "mutants: all script tests passed"
