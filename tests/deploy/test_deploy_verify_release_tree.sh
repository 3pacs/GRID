#!/usr/bin/env bash
# Failure-injection test for scripts/deploy_verify_release_tree.sh, against
# the REAL script (not a re-implementation of its logic) -- same philosophy
# as tests/deploy/test_deploy_release_swap.sh in this directory.
#
# Reproduces the exact shape of the 2026-09-22 incident this script exists to
# fix: run 35693333707's "Verify running API" step compared a process's raw
# /proc/<pid>/cwd reading against the LITERAL $DEPLOY_PATH string, which can
# never match once $DEPLOY_PATH is a symlink -- it failed on a genuinely
# healthy grid-api running the correct new code.
#
# Proves, against real processes and a real git repository (not mocks):
#   1. A process whose cwd resolves to the CURRENT live-release symlink
#      target, checked against that target's own actual HEAD commit
#      (exact match), passes.
#   1b. Exact equality, not ancestry: the SAME process/target, checked
#       against an OLDER commit that is a genuine ancestor of the live
#       target's HEAD -- i.e. the live target is a DESCENDANT of the
#       expected SHA, not that exact commit -- still FAILS. A release-
#       identity check that accepted "at least this commit" would have let
#       this incident-class bug (the wrong exact commit, but a real
#       ancestor) through silently.
#   2. A process whose cwd resolves to an OLD release directory -- not what
#      the live symlink currently points at -- fails the resolved-cwd
#      check, even though that old directory is itself a perfectly valid
#      git checkout.
#   3. A process correctly running from the live target still fails when the
#      expected SHA is unrelated entirely (not an ancestor, not a
#      descendant) -- the independent SHA check this script adds beyond the
#      cwd check alone.
#
# Runs entirely inside a temp sandbox with real git repositories and real
# background processes -- never touches any real GRID path, any real
# database, or any production host. Real Linux only (see the git-bash-on-
# Windows note in tests/deploy/test_deploy_release_swap.sh -- readlink -f and
# /proc/<pid>/cwd semantics this script depends on don't exist there either).
#
# Usage: bash tests/deploy/test_deploy_verify_release_tree.sh

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
VERIFY_SCRIPT="${REPO_ROOT}/scripts/deploy_verify_release_tree.sh"

SANDBOX="$(mktemp -d)"
BG_PIDS=()
cleanup() {
  for p in "${BG_PIDS[@]:-}"; do
    kill "$p" >/dev/null 2>&1 || true
  done
  rm -rf "$SANDBOX"
}
trap cleanup EXIT

pass_count=0
fail_count=0

assert_eq() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "PASS: $desc"
    pass_count=$((pass_count + 1))
  else
    echo "FAIL: $desc -- expected [$expected], got [$actual]"
    fail_count=$((fail_count + 1))
  fi
}

assert_true() {
  local desc="$1" cond="$2"
  if [ "$cond" = "true" ]; then
    echo "PASS: $desc"
    pass_count=$((pass_count + 1))
  else
    echo "FAIL: $desc"
    fail_count=$((fail_count + 1))
  fi
}

# Starts `sleep 100` with its cwd pinned to $1, sets $LAST_PID to its real
# pid. `exec` replaces the backgrounded subshell's own process image, so $!
# is the real sleep pid, already running from the right directory -- matches
# how a real systemd service's cwd is fixed at process start. Deliberately
# NOT invoked via `$(...)`: capturing a function's output through command
# substitution runs the whole function body in a throwaway subshell, so (a)
# appending to BG_PIDS would silently vanish with that subshell instead of
# reaching the outer trap, and (b) the backgrounded job inherits that
# subshell's own stdout, which keeps the surrounding `$(...)` pipe open
# until the 100s sleep itself exits -- observed directly (a full 100s+ hang)
# while first writing this test. Called as a plain statement instead, with
# the background job's stdio explicitly redirected away for the same reason.
start_pinned_process() {
  local dir="$1"
  ( cd "$dir" && exec sleep 100 ) >/dev/null 2>&1 &
  LAST_PID=$!
  BG_PIDS+=("$LAST_PID")
}

git_commit() {
  local dir="$1" msg="$2"
  git -C "$dir" \
    -c user.name="test" -c user.email="test@example.invalid" \
    commit -q -m "$msg"
}

# ── Shared fixture: one real git repo, two real commits, checked out into
#    two separate release directories mirroring deploy_release_swap.sh's
#    <live>.releases/<label> layout -- $live currently points at $rel_new.

live="${SANDBOX}/grid_release"
releases="${live}.releases"
mkdir -p "$releases"

seed="${SANDBOX}/seed_repo"
mkdir -p "$seed"
git -C "$seed" init -q -b main
echo "v1" > "$seed/marker.txt"
git -C "$seed" add marker.txt
git_commit "$seed" "commit A (old release)"
sha_a="$(git -C "$seed" rev-parse HEAD)"

echo "v2" > "$seed/marker.txt"
git -C "$seed" add marker.txt
git_commit "$seed" "commit B (current live release)"
sha_b="$(git -C "$seed" rev-parse HEAD)"

# Unrelated commit (different root, shares no history with $seed) -- used as
# a SHA that is not just "older" but genuinely not an ancestor of anything
# in $seed at all.
unrelated="${SANDBOX}/unrelated_repo"
mkdir -p "$unrelated"
git -C "$unrelated" init -q -b main
echo "x" > "$unrelated/f.txt"
git -C "$unrelated" add f.txt
git_commit "$unrelated" "unrelated history"
sha_unrelated="$(git -C "$unrelated" rev-parse HEAD)"

rel_old="${releases}/${sha_a}"
rel_new="${releases}/${sha_b}"
git clone -q "$seed" "$rel_old" >/dev/null 2>&1
git -C "$rel_old" checkout -q "$sha_a"
git clone -q "$seed" "$rel_new" >/dev/null 2>&1
git -C "$rel_new" checkout -q "$sha_b"

ln -sfn "$rel_new" "$live"

# ── Test 1: correct symlink target, exact HEAD SHA -- passes ───────────────

start_pinned_process "$rel_new"
pid_current="$LAST_PID"
set +e
out1="$(bash "$VERIFY_SCRIPT" "$live" "$pid_current" "$sha_b" 2>&1)"
exit1=$?
set -e
if [ "$exit1" -ne 0 ]; then echo "--- test1 output ---"; echo "$out1"; echo "--- end ---"; fi
assert_eq "correct target + exact HEAD sha: exits 0" "0" "$exit1"
assert_true "correct target + exact HEAD sha: reports the resolved target and commit" \
  "$(echo "$out1" | grep -qF "$rel_new" && echo "$out1" | grep -qF "$sha_b" && echo true || echo false)"

# ── Test 1b: correct symlink target, but the live target's HEAD is a
#    DESCENDANT of the expected SHA (sha_a is a real ancestor of sha_b, the
#    live target's actual HEAD) -- must FAIL. Exact equality, not ancestry
#    -- see the script's own header. Same cwd, same process as test 1;
#    only the expected SHA changes, isolating this to the SHA check alone.

set +e
out1b="$(bash "$VERIFY_SCRIPT" "$live" "$pid_current" "$sha_a" 2>&1)"
exit1b=$?
set -e
assert_eq "live target is a DESCENDANT of expected sha: exits non-zero (exact equality required)" "1" "$exit1b"
assert_true "descendant case: error names it as a commit mismatch (not the expected deployed commit)" \
  "$(echo "$out1b" | grep -qF "not the expected deployed commit" && echo true || echo false)"
assert_true "descendant case: does NOT claim a cwd mismatch (the cwd check genuinely passed here)" \
  "$(echo "$out1b" | grep -qF "does not resolve to the live release target" && echo false || echo true)"

# ── Test 2: process cwd resolves to the OLD release directory, not what the
#    live symlink currently points at -- fails the resolved-cwd check ──────

start_pinned_process "$rel_old"
pid_old="$LAST_PID"
set +e
out2="$(bash "$VERIFY_SCRIPT" "$live" "$pid_old" "$sha_b" 2>&1)"
exit2=$?
set -e
assert_eq "process running from an OLD release directory: exits non-zero" "1" "$exit2"
assert_true "old release directory: error names the mismatch (does not resolve to the live target)" \
  "$(echo "$out2" | grep -qF "does not resolve to the live release target" && echo true || echo false)"

# ── Test 3: process correctly running from the live target, but the
#    expected SHA is entirely unrelated (not an ancestor, not a descendant)
#    -- fails the independent SHA check even though the cwd check alone
#    would pass ─────────────────────────────────────────────────────────

set +e
out3="$(bash "$VERIFY_SCRIPT" "$live" "$pid_current" "$sha_unrelated" 2>&1)"
exit3=$?
set -e
assert_eq "correct cwd target, wrong/unrelated expected sha: exits non-zero" "1" "$exit3"
assert_true "wrong sha: error names it as a commit mismatch, not a cwd mismatch" \
  "$(echo "$out3" | grep -qF "not the expected deployed commit" && echo true || echo false)"
assert_true "wrong sha: does NOT claim a cwd mismatch (the cwd check genuinely passed here)" \
  "$(echo "$out3" | grep -qF "does not resolve to the live release target" && echo false || echo true)"

# ── Usage error: exits 2 ────────────────────────────────────────────────────

set +e
bash "$VERIFY_SCRIPT" "$live" "$pid_current" > "${SANDBOX}/usage.log" 2>&1
exit_usage=$?
set -e
assert_eq "missing argument: exits with the usage-error code" "2" "$exit_usage"

echo
echo "=== ${pass_count} passed, ${fail_count} failed ==="
if [ "$fail_count" -gt 0 ]; then
  exit 1
fi
