#!/usr/bin/env bash
# scripts/mutants.sh [--all] [--warn|--strict] [<path>...] — mutation-test
# the workspace, or only the Rust lines this lane changed. Run via
# `just mutants` (--all) or `just mutants-touched`.
#
# cargo-mutants changes one expression at a time (a `>` to `>=`, a function
# body to `Default::default()`) and re-runs the tests. A mutant the tests
# still pass on ("missed") marks a line no test guards.
#
#   scripts/mutants.sh                 # lines changed since merge-base(HEAD, origin/main)
#   scripts/mutants.sh src/batch.rs    # only changes under these paths (git pathspecs)
#   scripts/mutants.sh --all           # every mutant in the workspace's crates
#   scripts/mutants.sh --warn          # report missed mutants, exit 0
#
# The diff scope covers committed, staged, unstaged and untracked .rs files,
# and tests only the crates that own them.
#
# Strict by default: a missed mutant fails (exit 2), and so does a run that
# did not check everything (exit 3: a cap hit, cargo-mutants missing, a main
# checkout). --warn (or MUTANTS_STRICT=0) reports both and exits 0.
#
# Knobs (environment):
#   MUTANTS_STRICT=0|1       as --warn / --strict. Default 1.
#   MUTANTS_MAX_MUTANTS=N    refuse to start above N mutants (default 400;
#                            the whole crate has about 380). The message
#                            names the count; narrow the diff or raise N.
#   MUTANTS_MAX_SECONDS=N    stop the run after N seconds (default 1800) and
#                            report what finished.
#   MUTANTS_JOBS=N           mutants built and tested at once (default 4).
#   MUTANTS_TIMEOUT=N        per-mutant test timeout in seconds. Default:
#                            twice the unmutated test run, at least 30s. The
#                            nextest `mutants` profile already kills a hung
#                            test at 5s, so this is a backstop.
#   MUTANTS_BASE=<rev>       diff against this instead of the merge-base.
#
# Exit codes: 0 pass (or --warn); 2 missed mutants; 3 the run did not check
# everything; 64 bad argument; anything else is a real failure passed through
# from git, cargo metadata or cargo-mutants (1 bad arguments, 4 the unmutated
# build or tests fail, 70 internal error). Those are never a warning.
#
# Builds run in copies of the checkout, target/ included (--copy-target), so
# the lane's own sources and target/ are never mutated and the first build is
# warm. A main checkout (one with lanes under .trees/) is refused: the copies
# would include every lane. Results land in <target>/mutants/mutants.out
# (caught.txt, missed.txt, timeout.txt, unviable.txt, log/), the previous
# run's in mutants.out.old. Exclusions live in .cargo/mutants.toml.
set -uo pipefail

EXIT_MISSED=2
EXIT_UNCHECKED=3
EXIT_USAGE=64

strict="${MUTANTS_STRICT:-1}"
scope=diff
paths=()
for arg in "$@"; do
  case "$arg" in
    --all) scope=all ;;
    --strict) strict=1 ;;
    --warn) strict=0 ;;
    -h|--help) sed -n '2,/^set -uo/p' "${BASH_SOURCE[0]}" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
    -*) echo "mutants: unknown argument $arg (usage: mutants.sh [--all] [--warn|--strict] [<path>...])" >&2; exit "$EXIT_USAGE" ;;
    *) paths+=("$arg") ;;
  esac
done
case "$strict" in
  0|1) ;;
  *) echo "mutants: MUTANTS_STRICT must be 0 or 1, got '$strict'" >&2; exit "$EXIT_USAGE" ;;
esac
if [ "$scope" = all ] && [ "${#paths[@]}" != 0 ]; then
  echo "mutants: --all mutates the whole workspace; it takes no paths" >&2
  exit "$EXIT_USAGE"
fi
[ "${#paths[@]}" != 0 ] || paths=(.)
max_mutants="${MUTANTS_MAX_MUTANTS:-400}"
max_seconds="${MUTANTS_MAX_SECONDS:-1800}"
jobs="${MUTANTS_JOBS:-4}"
for knob in "MUTANTS_MAX_MUTANTS=$max_mutants" "MUTANTS_MAX_SECONDS=$max_seconds" "MUTANTS_JOBS=$jobs" "MUTANTS_TIMEOUT=${MUTANTS_TIMEOUT:-1}"; do
  case "${knob#*=}" in
    ''|*[!0-9]*|0) echo "mutants: ${knob%%=*} must be a positive whole number, got '${knob#*=}'" >&2; exit "$EXIT_USAGE" ;;
  esac
done

# A check that could not run fails in strict mode and passes with --warn.
unchecked() {
  if [ "$strict" = 1 ]; then echo "mutants: FAIL (strict): $1" >&2; exit "$EXIT_UNCHECKED"; fi
  echo "mutants: warning: $1"
  exit 0
}

root=$(git rev-parse --show-toplevel) || { echo "mutants: not inside a git checkout" >&2; exit 1; }
cd "$root" || exit 1
rust_only() { grep '\.rs$' || true; }
tracked=""; untracked=""
if [ "$scope" = diff ]; then
  if [ -n "${MUTANTS_BASE:-}" ]; then
    base=$(git rev-parse --verify --quiet "$MUTANTS_BASE^{commit}") || { echo "mutants: MUTANTS_BASE=$MUTANTS_BASE is not a commit" >&2; exit "$EXIT_USAGE"; }
  else
    base=$(git merge-base HEAD origin/main 2>/dev/null || git rev-parse HEAD~1) || { echo "mutants: cannot find a base commit (no origin/main, no HEAD~1)" >&2; exit 1; }
  fi
  tracked=$(git diff --name-only --no-renames --diff-filter=d "$base" -- "${paths[@]}") || exit $?
  tracked=$(rust_only <<<"$tracked")
  untracked=$(git ls-files --others --exclude-standard -- "${paths[@]}") || exit $?
  untracked=$(rust_only <<<"$untracked")
  files=$(printf '%s\n%s\n' "$tracked" "$untracked" | sed '/^$/d' | sort -u)
  echo "mutants: base $base"
  if [ -z "$files" ]; then
    echo "mutants: no Rust file changed against the base; nothing to mutate"
    exit 0
  fi
else
  files=""
  echo "mutants: whole workspace"
fi

if ! cargo mutants --version >/dev/null 2>&1; then
  unchecked "cargo-mutants is not installed, so mutation testing was skipped; install it with: cargo install --locked cargo-mutants"
fi
if ! cargo nextest --version >/dev/null 2>&1; then
  unchecked "cargo-nextest is not installed (.cargo/mutants.toml tests with it); install it with: cargo install --locked cargo-nextest"
fi

meta=$(cargo metadata --no-deps --format-version 1 --locked --offline 2>/dev/null || cargo metadata --no-deps --format-version 1 --locked) || {
  rc=$?; echo "mutants: cargo metadata failed (exit $rc)" >&2; exit "$rc"; }
# Line 1: the target directory. Line 2: the crates to mutate (every member
# for --all, else those owning a changed file). Then one line per changed
# file no workspace member owns: a file belongs to the package whose
# Cargo.toml is nearest above it, so a nested crate with its own
# `[workspace]` is never mistaken for the root package.
mapping=$(python3 - "$scope" 3<<<"$meta" 4<<<"$files" <<'PY'
import json, os, sys
scope = sys.argv[1]
m = json.load(os.fdopen(3)); root = m["workspace_root"]
members = {os.path.normpath(os.path.relpath(os.path.dirname(p["manifest_path"]), root)): p["name"] for p in m["packages"]}
if scope == "all":
    print(m["target_directory"]); print(" ".join(sorted(members.values()))); sys.exit(0)
crates, outside = set(), []
for f in os.fdopen(4).read().splitlines():
    d = os.path.dirname(f)
    while d and not os.path.exists(os.path.join(root, d, "Cargo.toml")):
        d = os.path.dirname(d)
    owner = members.get(os.path.normpath(d) if d else ".")
    if owner is None:
        outside.append(f)
    else:
        crates.add(owner)
print(m["target_directory"]); print(" ".join(sorted(crates)))
print("\n".join(outside))
PY
) || { rc=$?; echo "mutants: mapping changed files to crates failed (exit $rc)" >&2; exit "$rc"; }
target_dir=$(sed -n 1p <<<"$mapping")
crates=$(sed -n 2p <<<"$mapping")
outside=$(sed -n '3,$p' <<<"$mapping" | sed '/^$/d')
[ -z "$outside" ] || printf 'mutants: not in a workspace member, not mutated: %s\n' "$outside" | sed '2,$s/^/  /'
if [ -z "$crates" ]; then
  echo "mutants: no changed Rust file belongs to a workspace member; nothing to mutate"
  exit 0
fi
echo "mutants: crates $crates"
pkg_args=(); for c in $crates; do pkg_args+=(-p "$c"); done

# cargo-mutants copies the whole checkout, gitignored files included. In a
# main checkout that copy would hold every lane under .trees/.
if [ -n "$(ls -A .trees 2>/dev/null)" ]; then
  unchecked "this checkout has lanes under .trees/, and cargo-mutants would copy all of them into its build copies; run from a lane"
fi

out="$target_dir/mutants"
mkdir -p "$out" || exit 1
scope_args=()
if [ "$scope" = diff ]; then
  patch="$out/in-diff.patch"
  write_patch() {
    if [ -n "$tracked" ]; then
      # shellcheck disable=SC2086 # one pathspec per changed file
      git diff --no-renames --no-color --no-ext-diff "$base" -- $tracked || return
    fi
    # `git diff --no-index` exits 1 when the files differ, as a new file does.
    local f
    for f in $untracked; do
      git diff --no-index --no-color --no-ext-diff -- /dev/null "$f"
      [ $? -le 1 ] || return
    done
  }
  write_patch > "$patch" || { rc=$?; echo "mutants: writing $patch failed (exit $rc)" >&2; exit "$rc"; }
  scope_args=(--in-diff "$patch")
fi

listing=$(cargo mutants --list ${scope_args[@]+"${scope_args[@]}"} "${pkg_args[@]}" --colors never 2>&1) || {
  rc=$?; printf '%s\n' "$listing" >&2; echo "mutants: cargo mutants --list failed (exit $rc)" >&2; exit "$rc"; }
count=$(printf '%s\n' "$listing" | grep -c '\.rs:[0-9]*:[0-9]*: ' || true)
if [ "$count" = 0 ]; then
  echo "mutants: no mutants to test (only tests, declarations or excluded code changed)"
  exit 0
fi
if [ "$count" -gt "$max_mutants" ]; then
  unchecked "cap hit: $count mutants exceeds MUTANTS_MAX_MUTANTS=$max_mutants; mutation testing was skipped. Raise the cap or narrow the diff (paths: mutants.sh <path>..., or MUTANTS_BASE=<rev>)."
fi

run_args=(${scope_args[@]+"${scope_args[@]}"} "${pkg_args[@]}" --output "$out" --copy-target=true --jobs "$jobs" --colors never)
if [ -n "${MUTANTS_TIMEOUT:-}" ]; then
  run_args+=(--timeout "$MUTANTS_TIMEOUT")
else
  run_args+=(--timeout-multiplier 2 --minimum-test-timeout 30)
fi
echo "mutants: testing $count mutant(s), $jobs at a time, capped at ${max_seconds}s"
echo "+ cargo mutants ${run_args[*]}"

# The time cap: a watchdog sends SIGTERM, which cargo-mutants handles by
# stopping its child builds and keeping the outcomes it has already written.
# SIGKILL follows if it has not exited 30s later.
cap_marker="$out/.time-cap-hit"
results="$out/mutants.out"
rm -f "$cap_marker"
# A run stopped before cargo-mutants writes anything must not report the
# previous run's results, so the old ones move aside first.
if [ -d "$results" ]; then rm -rf "$results.old" && mv "$results" "$results.old" || exit 1; fi
cargo mutants "${run_args[@]}" &
pid=$!
# Stopping this script stops the run too, rather than orphaning it.
trap 'kill -TERM "$pid" 2>/dev/null; kill "${watchdog:-}" 2>/dev/null; exit 143' TERM
trap 'kill -TERM "$pid" 2>/dev/null; kill "${watchdog:-}" 2>/dev/null; exit 130' INT
# It writes nothing, so it never holds the caller's output open, and on its
# own TERM it stops whichever sleep it is in (`jobs -p`, since the TERM can
# land between starting a sleep and recording its pid).
(
  trap 'kill $(jobs -p) 2>/dev/null; exit 0' TERM
  sleep "$max_seconds" & wait $!
  : > "$cap_marker"
  kill -TERM "$pid" 2>/dev/null
  sleep 30 & wait $!
  kill -KILL "$pid" 2>/dev/null
) >/dev/null 2>&1 &
watchdog=$!
wait "$pid"; rc=$?
kill "$watchdog" 2>/dev/null; wait "$watchdog" 2>/dev/null
trap - TERM INT

time_capped=0; [ ! -e "$cap_marker" ] || time_capped=1
rm -f "$cap_marker"
# cargo-mutants: 0 all caught, 2 some missed, 3 some timed out. Anything else
# is a tool failure unless the watchdog caused it.
if [ "$time_capped" = 0 ] && [ "$rc" != 0 ] && [ "$rc" != 2 ] && [ "$rc" != 3 ]; then
  case "$rc" in
    4) echo "mutants: FAIL: the unmutated build or tests fail, so no mutant was tested (cargo mutants exit 4); see $results/log" >&2 ;;
    *) echo "mutants: FAIL: cargo mutants exited $rc; see $results/debug.log" >&2 ;;
  esac
  exit "$rc"
fi

tally() { if [ -f "$results/$1.txt" ]; then grep -c . "$results/$1.txt"; else echo 0; fi; }
caught=$(tally caught); missed=$(tally missed); timeouts=$(tally timeout); unviable=$(tally unviable)
echo
echo "mutants: summary for $crates ($count mutants)"
printf '  caught   %s\n  missed   %s\n  timeout  %s\n  unviable %s\n' "$caught" "$missed" "$timeouts" "$unviable"
if [ "$missed" != 0 ]; then
  echo "missed mutants (the tests pass with this change applied, so no test guards the line):"
  sed 's/^/  /' "$results/missed.txt"
fi
echo "results: $results"
if [ "$missed" != 0 ]; then
  if [ "$strict" = 1 ]; then
    echo "mutants: FAIL (strict): $missed missed mutant(s). Add a test that fails for each, or exclude it in .cargo/mutants.toml with a reason." >&2
    exit "$EXIT_MISSED"
  fi
  echo "mutants: warning: $missed missed mutant(s). Add a test that fails for each, or exclude it in .cargo/mutants.toml with a reason. (--warn; strict mode fails on this)"
fi
if [ "$time_capped" = 1 ]; then
  tested=$((caught + missed + timeouts + unviable))
  unchecked "cap hit: stopped after MUTANTS_MAX_SECONDS=${max_seconds}s with $tested of $count mutants tested. Raise the cap or narrow the diff (paths: mutants.sh <path>..., or MUTANTS_BASE=<rev>)."
fi
exit 0
