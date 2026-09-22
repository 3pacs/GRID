#!/usr/bin/env bash
# Failure-injection test for scripts/deploy_release_swap.sh.
#
# Proves, against the real script (not a re-implementation of its logic):
#   1. A failing build_hook (simulating a migration failure, as in #596)
#      leaves the live path completely untouched and still serving its
#      previous, working content.
#   2. A succeeding build_hook promotes the candidate atomically.
#   3. The first-ever run against a plain directory (today's production
#      shape) migrates it to the symlink pattern without changing what
#      the live path resolves to.
#
# Runs entirely inside a temp sandbox -- never touches any real GRID path,
# any real database, or any production host. Safe to run anywhere.
#
# Usage: bash tests/deploy/test_deploy_release_swap.sh

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
SWAP_SCRIPT="${REPO_ROOT}/scripts/deploy_release_swap.sh"

SANDBOX="$(mktemp -d)"
trap 'rm -rf "$SANDBOX"' EXIT

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

# ── Test 1: first-run migration from a plain directory ─────────────────────
# Simulates today's actual production shape: $LIVE_PATH is a real directory
# with real content, not yet a symlink.

t1_live="${SANDBOX}/t1/grid_release"
mkdir -p "$(dirname "$t1_live")"
mkdir -p "$t1_live"
echo "original-marker-v1" > "$t1_live/marker.txt"

t1_hook="${SANDBOX}/t1/build_ok.sh"
cat > "$t1_hook" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
candidate="$1"
echo "candidate-v2" > "$candidate/marker.txt"
exit 0
HOOK
chmod +x "$t1_hook"

set +e
bash "$SWAP_SCRIPT" "$t1_live" "v2" "$t1_hook" > "${SANDBOX}/t1/swap.log" 2>&1
t1_exit=$?
set -e
if [ "$t1_exit" -ne 0 ]; then
  echo "--- t1 swap.log ---"; cat "${SANDBOX}/t1/swap.log"; echo "--- end ---"
fi

assert_eq "first-run migration: swap script exits 0 on success" "0" "$t1_exit"
assert_true "first-run migration: live path is now a symlink" "$([ -L "$t1_live" ] && echo true || echo false)"
assert_eq "first-run migration: live path resolves to new marker content" \
  "candidate-v2" "$(cat "$t1_live/marker.txt")"

# ── Test 2: a failing build_hook (the #596 scenario) leaves the live path
#    completely untouched ─────────────────────────────────────────────────

t2_live="${SANDBOX}/t2/grid_release"
mkdir -p "$(dirname "$t2_live")"
mkdir -p "$t2_live"
echo "working-release-v1" > "$t2_live/marker.txt"
echo "some other file that must survive" > "$t2_live/other.txt"

t2_hook="${SANDBOX}/t2/build_fail.sh"
cat > "$t2_hook" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
candidate="$1"
# Simulate: fetch and PWA build succeed and write into the candidate, then
# the migration step fails -- exactly #596's sequence (ALTER TABLE succeeded,
# the backfill UPDATE did not).
echo "new-source-v2" > "$candidate/marker.txt"
echo "canceling statement due to statement timeout" >&2
exit 1
HOOK
chmod +x "$t2_hook"

set +e
bash "$SWAP_SCRIPT" "$t2_live" "v2-failing" "$t2_hook" > "${SANDBOX}/t2/swap.log" 2>&1
t2_exit=$?
set -e

assert_eq "failed build: swap script exits non-zero" "1" "$t2_exit"
# t2_live started as a PLAIN directory (this is its very first run through the
# script). The one-time plain-directory-to-symlink migration happens before
# the build hook runs at all, so it still occurs even on a run that goes on
# to fail -- that is fine: the migration only changes HOW $LIVE_PATH is
# reached (through a symlink instead of directly), never WHAT content it
# resolves to. The safety contract under test is content, not filesystem
# representation, so we assert on content, not on whether $LIVE_PATH is a
# symlink here. Test 3 below covers the steady-state case (a run against an
# already-migrated symlink) and confirms the symlink itself is untouched.
assert_eq "failed build: live path's marker is UNCHANGED (old release still serving)" \
  "working-release-v1" "$(cat "$t2_live/marker.txt")"
assert_eq "failed build: unrelated file in the old release also survives untouched" \
  "some other file that must survive" "$(cat "$t2_live/other.txt")"
assert_true "failed build: the failed candidate is preserved for inspection, not silently deleted" \
  "$([ -f "${t2_live}.releases/v2-failing/marker.txt" ] && echo true || echo false)"
assert_eq "failed build: the failed candidate does carry the new (bad) source, isolated from the live path" \
  "new-source-v2" "$(cat "${t2_live}.releases/v2-failing/marker.txt")"

# ── Test 3: after a symlink is established, a SECOND failing run leaves the
#    symlink pointed at the still-good previous release ────────────────────

t3_live="${SANDBOX}/t3/grid_release"
mkdir -p "$(dirname "$t3_live")"
mkdir -p "$t3_live"
echo "release-a" > "$t3_live/marker.txt"

t3_hook_ok="${SANDBOX}/t3/build_ok.sh"
cat > "$t3_hook_ok" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
echo "release-b" > "$1/marker.txt"
exit 0
HOOK
chmod +x "$t3_hook_ok"
bash "$SWAP_SCRIPT" "$t3_live" "release-b" "$t3_hook_ok" > "${SANDBOX}/t3/swap1.log" 2>&1
release_b_target="$(readlink -f "$t3_live")"

t3_hook_fail="${SANDBOX}/t3/build_fail.sh"
cat > "$t3_hook_fail" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
echo "release-c-broken" > "$1/marker.txt"
exit 1
HOOK
chmod +x "$t3_hook_fail"
set +e
bash "$SWAP_SCRIPT" "$t3_live" "release-c" "$t3_hook_fail" > "${SANDBOX}/t3/swap2.log" 2>&1
t3_exit=$?
set -e

assert_eq "second run, failing: swap script exits non-zero" "1" "$t3_exit"
assert_eq "second run, failing: symlink still points at release-b, not release-c" \
  "$release_b_target" "$(readlink -f "$t3_live")"
assert_eq "second run, failing: live content is still release-b's, deploy stays fully usable" \
  "release-b" "$(cat "$t3_live/marker.txt")"

echo
echo "=== ${pass_count} passed, ${fail_count} failed ==="
if [ "$fail_count" -gt 0 ]; then
  exit 1
fi
