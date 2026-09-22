#!/usr/bin/env bash
# Failure-injection test for scripts/deploy_release_swap.sh and
# scripts/deploy_release_rollback.sh.
#
# Proves, against the real scripts (not a re-implementation of their logic):
#   1-3. The original three: a failing build_hook leaves the live path
#        completely untouched; a succeeding one promotes atomically; the
#        first-ever run against a plain directory (today's production
#        shape) migrates it to the symlink pattern transparently.
#   4. A retry with a label that is ALREADY the live release (e.g.
#      `gh run rerun --failed` landing on a commit that already fully
#      deployed) is a safe no-op, not a `rm -rf` of the live target.
#   5. Two concurrent runs against the same live path, different labels,
#      are genuinely serialized by the lock -- not just "happen to not
#      collide."
#   6. A crash between the two filesystem operations of the one-time
#      plain-directory-to-symlink migration self-heals on the next run
#      instead of silently discarding the previous release.
#   7. A swap that fails AFTER the build_hook (including any migration)
#      already succeeded is reported with a distinct, unambiguous exit
#      code and message -- never conflated with an ordinary build failure.
#   8. Untracked/writable state (.env, uploads) survives a build_hook that
#      rewrites tracked files, exactly like a real `git reset --hard`.
#   9. A process that already had the live directory as its cwd before the
#      first-conversion migration keeps reading through it with zero
#      failures -- proven empirically (Linux pins cwd to the inode, not
#      the path), not just asserted.
#   10. An absolute-path reader AND a freshly-started process per attempt --
#       unlike test 9's already-open cwd -- DO see real, measured failures
#       during the first-conversion gap, including across a genuine SIGKILL
#       interruption between the rename and the symlink creation; both stop
#       failing the instant the gap closes (immediately in the normal case,
#       or once the next run's self-healing recovery executes after a real
#       crash). States the remaining availability gap with numbers.
#   11. scripts/deploy_release_rollback.sh: refuses cleanly with nothing to
#       roll back to, rolls back correctly, is idempotent, refuses to race
#       an in-flight deploy build/swap holding the lock, REQUIRES an
#       explicit --schema-compatible acknowledgment (reverting files does
#       not revert a committed migration) and refuses without it, and
#       refuses to race a still-in-flight post-swap activation window (a
#       fresh .activation-in-progress marker) while still proceeding, with
#       a loud warning, past a stale one left by a job that crashed outright.
#
# Runs entirely inside a temp sandbox -- never touches any real GRID path,
# any real database, or any production host. Safe to run anywhere with a
# real Linux `mv`/`ln`/`flock` (see the git-bash-on-Windows note in this
# repo's PR history: MSYS `mv -T` semantics differ from GNU coreutils and
# will produce false results here -- run this on real Linux).
#
# Usage: bash tests/deploy/test_deploy_release_swap.sh

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
SWAP_SCRIPT="${REPO_ROOT}/scripts/deploy_release_swap.sh"
ROLLBACK_SCRIPT="${REPO_ROOT}/scripts/deploy_release_rollback.sh"

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

# ── Test 4: a retry with a label that is ALREADY live is a safe no-op ──────
# Real trigger: `gh run rerun --failed` (the established one retry mechanism
# for this pipeline) landing on a commit whose swap already succeeded, but a
# LATER step (health check) flaked. Before this fix, the stale-candidate
# cleanup would `rm -rf` the directory the live symlink currently resolves
# to, then abort trying to `cp -a` from the now-deleted source -- breaking a
# working release for no reason.

t4_live="${SANDBOX}/t4/grid_release"
mkdir -p "$(dirname "$t4_live")"
mkdir -p "$t4_live"
echo "seed" > "$t4_live/marker.txt"

t4_hook_first="${SANDBOX}/t4/build_first.sh"
cat > "$t4_hook_first" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
echo "release-v1" > "$1/marker.txt"
HOOK
chmod +x "$t4_hook_first"
bash "$SWAP_SCRIPT" "$t4_live" "v1" "$t4_hook_first" > "${SANDBOX}/t4/first.log" 2>&1
v1_target="$(readlink -f "$t4_live")"

# This hook proves whether it ran at all -- it must NOT be invoked below.
t4_hook_rerun="${SANDBOX}/t4/build_rerun.sh"
cat > "$t4_hook_rerun" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
echo "release-v1-REBUILT-SHOULD-NOT-HAPPEN" > "$1/marker.txt"
HOOK
chmod +x "$t4_hook_rerun"

set +e
bash "$SWAP_SCRIPT" "$t4_live" "v1" "$t4_hook_rerun" > "${SANDBOX}/t4/rerun.log" 2>&1
t4_exit=$?
set -e

assert_eq "same-label retry (already live): exits 0" "0" "$t4_exit"
assert_eq "same-label retry: live target is unchanged" "$v1_target" "$(readlink -f "$t4_live")"
assert_eq "same-label retry: live content is untouched -- rebuild hook was never invoked" \
  "release-v1" "$(cat "$t4_live/marker.txt")"
assert_true "same-label retry: the live target directory itself was not deleted/corrupted" \
  "$([ -f "${v1_target}/marker.txt" ] && echo true || echo false)"

# ── Test 5: two concurrent runs, different labels, are genuinely serialized
#    by the lock -- proven by non-overlapping execution windows, not by
#    absence of an observed collision ───────────────────────────────────────

t5_live="${SANDBOX}/t5/grid_release"
mkdir -p "$(dirname "$t5_live")"
mkdir -p "$t5_live"
echo "release-a" > "$t5_live/marker.txt"

t5_hook_a="${SANDBOX}/t5/build_a.sh"
cat > "$t5_hook_a" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
echo "release-a-established" > "$1/marker.txt"
HOOK
chmod +x "$t5_hook_a"
bash "$SWAP_SCRIPT" "$t5_live" "a" "$t5_hook_a" > "${SANDBOX}/t5/setup.log" 2>&1

t5_times="${SANDBOX}/t5/times"
mkdir -p "$t5_times"

make_slow_hook() {
  local out="$1" label="$2"
  cat > "$out" << HOOK
#!/usr/bin/env bash
set -euo pipefail
date +%s%N > "${t5_times}/${label}.start"
sleep 1
echo "release-${label}" > "\$1/marker.txt"
date +%s%N > "${t5_times}/${label}.end"
HOOK
  chmod +x "$out"
}
make_slow_hook "${SANDBOX}/t5/build_b.sh" "b"
make_slow_hook "${SANDBOX}/t5/build_c.sh" "c"

set +e
bash "$SWAP_SCRIPT" "$t5_live" "b" "${SANDBOX}/t5/build_b.sh" > "${SANDBOX}/t5/swap_b.log" 2>&1 &
pid_b=$!
bash "$SWAP_SCRIPT" "$t5_live" "c" "${SANDBOX}/t5/build_c.sh" > "${SANDBOX}/t5/swap_c.log" 2>&1 &
pid_c=$!
wait "$pid_b"; exit_b=$?
wait "$pid_c"; exit_c=$?
set -e

assert_eq "concurrent runs: b exits 0" "0" "$exit_b"
assert_eq "concurrent runs: c exits 0" "0" "$exit_c"

start_b="$(cat "${t5_times}/b.start")"; end_b="$(cat "${t5_times}/b.end")"
start_c="$(cat "${t5_times}/c.start")"; end_c="$(cat "${t5_times}/c.end")"
if [ "$start_c" -ge "$end_b" ] || [ "$start_b" -ge "$end_c" ]; then
  no_overlap="true"
else
  no_overlap="false"
fi
assert_true "concurrent runs: build_hook execution windows do not overlap (the lock actually serializes them)" "$no_overlap"

t5_final="$(cat "$t5_live/marker.txt")"
assert_true "concurrent runs: live content is exactly one candidate's, not a corrupted mix" \
  "$([ "$t5_final" = "release-b" ] || [ "$t5_final" = "release-c" ] && echo true || echo false)"

# ── Test 6: a crash between the two filesystem operations of the one-time
#    migration self-heals on the next run ──────────────────────────────────

t6_live="${SANDBOX}/t6/grid_release"
mkdir -p "$(dirname "$t6_live")"
mkdir -p "$t6_live"
echo "release-before-crash" > "$t6_live/marker.txt"

t6_releases="${t6_live}.releases"
mkdir -p "$t6_releases"
t6_pre_label="pre-atomic-20260101T000000Z"
mv -T "$t6_live" "${t6_releases}/${t6_pre_label}"
# Deliberately do NOT create the symlink -- this is the interrupted state.
assert_true "crash setup: live path does not exist at all (simulated kill mid-conversion)" \
  "$([ ! -e "$t6_live" ] && [ ! -L "$t6_live" ] && echo true || echo false)"

t6_hook="${SANDBOX}/t6/build_ok.sh"
cat > "$t6_hook" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
if [ ! -f "$1/marker.txt" ] || [ "$(cat "$1/marker.txt")" != "release-before-crash" ]; then
  echo "candidate was NOT seeded from the recovered pre-crash content" >&2
  exit 1
fi
echo "release-after-recovery" > "$1/marker.txt"
HOOK
chmod +x "$t6_hook"

set +e
bash "$SWAP_SCRIPT" "$t6_live" "post-crash-v1" "$t6_hook" > "${SANDBOX}/t6/recover.log" 2>&1
t6_exit=$?
set -e
if [ "$t6_exit" -ne 0 ]; then
  echo "--- t6 recover.log ---"; cat "${SANDBOX}/t6/recover.log"; echo "--- end ---"
fi

assert_eq "crash recovery: swap script exits 0" "0" "$t6_exit"
assert_true "crash recovery: recovery message was logged" \
  "$(grep -q 'recovering interrupted first-install conversion' "${SANDBOX}/t6/recover.log" && echo true || echo false)"
assert_eq "crash recovery: live content reflects the seeded-then-rebuilt release, not an empty start" \
  "release-after-recovery" "$(cat "$t6_live/marker.txt")"
assert_true "crash recovery: live path is a symlink again" \
  "$([ -L "$t6_live" ] && echo true || echo false)"

# ── Test 7: a swap that fails AFTER a successful build is loud and
#    distinguishable (exit 4), never conflated with a build failure (exit 1) ─

t7_live="${SANDBOX}/t7/grid_release"
mkdir -p "$(dirname "$t7_live")"
mkdir -p "$t7_live"
echo "release-good" > "$t7_live/marker.txt"
t7_parent="$(dirname "$t7_live")"

# The lock-method marker MUST live outside $t7_parent: the hook is about to
# remove write access from $t7_parent, and a marker written after that,
# inside it, would fail for the same reason the swap is supposed to --
# masking the real result behind a test-harness bug instead of exercising
# deploy_release_swap.sh at all.
t7_lock_method_file="${SANDBOX}/t7-lock-method"
t7_hook="${SANDBOX}/t7/build_then_lock_parent.sh"
cat > "$t7_hook" << HOOK
#!/usr/bin/env bash
set -euo pipefail
echo "release-migrated" > "\$1/marker.txt"
# Simulate an external condition (fs went read-only, ran out of inodes...)
# making the swap ITSELF fail after the build already succeeded. Prefer the
# immutable attribute over a plain chmod since this may run as root, which
# otherwise bypasses directory permission bits entirely.
if command -v chattr >/dev/null 2>&1 && chattr +i "${t7_parent}" 2>/dev/null; then
  echo chattr > "${t7_lock_method_file}"
else
  chmod 555 "${t7_parent}"
  echo chmod > "${t7_lock_method_file}"
fi
HOOK
chmod +x "$t7_hook"

set +e
bash "$SWAP_SCRIPT" "$t7_live" "v-migrated" "$t7_hook" > "${SANDBOX}/t7/swap.log" 2>&1
t7_exit=$?
set -e

t7_method="$(cat "$t7_lock_method_file" 2>/dev/null || echo none)"
if [ "$t7_method" = "chattr" ]; then
  chattr -i "$t7_parent" 2>/dev/null || true
fi
chmod 755 "$t7_parent" 2>/dev/null || true

if [ "$t7_method" = "none" ]; then
  echo "SKIP: swap-phase-failure test -- could neither chattr +i nor chmod 555 the parent (unexpected environment)"
else
  assert_eq "swap-phase failure after successful build: distinct exit code 4" "4" "$t7_exit"
  assert_true "swap-phase failure: loud, distinguishable error message on stderr" \
    "$(grep -q 'SWAP FAILED AFTER A SUCCESSFUL BUILD' "${SANDBOX}/t7/swap.log" && echo true || echo false)"
  assert_eq "swap-phase failure: live path is untouched (unchanged from before this run)" \
    "release-good" "$(cat "$t7_live/marker.txt")"
  assert_true "swap-phase failure: the migrated candidate still exists with the correct new content, for manual recovery" \
    "$([ -f "${t7_live}.releases/v-migrated/marker.txt" ] && [ "$(cat "${t7_live}.releases/v-migrated/marker.txt")" = "release-migrated" ] && echo true || echo false)"
fi

# ── Test 8: untracked/writable state survives a build_hook that rewrites
#    tracked files, exactly like a real `git fetch` + `git reset --hard` ───

t8_live="${SANDBOX}/t8/grid_release"
mkdir -p "$t8_live/uploads"
echo "tracked-content-v1" > "$t8_live/tracked.py"
echo "SECRET=v1" > "$t8_live/.env"
echo "user-uploaded-binary-content" > "$t8_live/uploads/file.bin"

t8_hook="${SANDBOX}/t8/build_reset_like.sh"
cat > "$t8_hook" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
# `git reset --hard` only ever rewrites TRACKED files -- never touches an
# untracked .env or an untracked uploads/ directory. Simulated here by only
# touching the "tracked" file.
echo "tracked-content-v2" > "$1/tracked.py"
HOOK
chmod +x "$t8_hook"

bash "$SWAP_SCRIPT" "$t8_live" "v2-reset" "$t8_hook" > "${SANDBOX}/t8/swap.log" 2>&1

assert_eq "writable-state preservation: tracked file reflects the new build" \
  "tracked-content-v2" "$(cat "$t8_live/tracked.py")"
assert_eq "writable-state preservation: .env survives the cp -a seed, untouched by the hook" \
  "SECRET=v1" "$(cat "$t8_live/.env")"
assert_eq "writable-state preservation: an untracked upload survives untouched" \
  "user-uploaded-binary-content" "$(cat "$t8_live/uploads/file.bin")"

# ── Test 9: a process that already had the live directory as its cwd is
#    unaffected by the first-conversion migration -- proven empirically ────

t9_live="${SANDBOX}/t9/grid_release"
mkdir -p "$(dirname "$t9_live")"
mkdir -p "$t9_live"
echo "content-v1" > "$t9_live/marker.txt"

t9_reader_log="${SANDBOX}/t9/reader.log"
t9_reader_err="${SANDBOX}/t9/reader.err"
: > "$t9_reader_log"
: > "$t9_reader_err"
(
  cd "$t9_live"
  for i in $(seq 1 200); do
    if cat marker.txt >> "$t9_reader_log" 2>>"$t9_reader_err"; then
      echo "ok" >> "$t9_reader_log"
    else
      echo "FAIL at iteration $i" >> "$t9_reader_log"
    fi
  done
) &
t9_reader_pid=$!

t9_hook="${SANDBOX}/t9/build_ok.sh"
cat > "$t9_hook" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
echo "content-v2" > "$1/marker.txt"
HOOK
chmod +x "$t9_hook"
bash "$SWAP_SCRIPT" "$t9_live" "v2" "$t9_hook" > "${SANDBOX}/t9/swap.log" 2>&1

wait "$t9_reader_pid"

assert_true "already-open cwd: background reader saw zero failures across the rename" \
  "$(grep -q 'FAIL' "$t9_reader_log" && echo false || echo true)"
assert_true "already-open cwd: background reader's stderr is empty (no ENOENT etc.)" \
  "$([ ! -s "$t9_reader_err" ] && echo true || echo false)"

# ── Test 10: an ABSOLUTE-PATH reader, and a freshly-started process per
#    attempt, during the first-install conversion -- including a GENUINE
#    interruption (a real SIGKILL, not a simulated one) between the rename
#    and the symlink creation. This states the remaining availability gap
#    with measured numbers instead of just reasoning about it, and contrasts
#    directly with Test 9's already-open-cwd case (zero failures throughout
#    the same kind of window). Uses DEPLOY_TEST_MIGRATION_DELAY, a no-op in
#    every real run, to widen the otherwise sub-millisecond gap so it can be
#    observed deterministically rather than caught by chance. ─────────────

# 10a: the gap completes normally (no crash) -- confirms real, non-zero
# failures occur for a fresh absolute-path lookup during the gap, and that
# they stop the instant the symlink is created, with no lasting outage.

t10a_live="${SANDBOX}/t10a/grid_release"
mkdir -p "$(dirname "$t10a_live")"
mkdir -p "$t10a_live"
echo "content-v1" > "$t10a_live/marker.txt"

t10a_reader_log="${SANDBOX}/t10a/reader.log"
: > "$t10a_reader_log"
t10a_stop="${SANDBOX}/t10a/stop"
(
  while [ ! -f "$t10a_stop" ]; do
    # A brand-new `cat` process per attempt, absolute path -- NOT a pinned
    # cwd. This is what a fresh health check or a systemd unit starting
    # mid-conversion actually does, unlike Test 9's already-open-cwd reader.
    if cat "${t10a_live}/marker.txt" > /dev/null 2>/dev/null; then
      echo ok >> "$t10a_reader_log"
    else
      echo FAIL >> "$t10a_reader_log"
    fi
  done
) &
t10a_reader_pid=$!

t10a_hook="${SANDBOX}/t10a/build_ok.sh"
cat > "$t10a_hook" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
echo "content-v2" > "$1/marker.txt"
HOOK
chmod +x "$t10a_hook"

DEPLOY_TEST_MIGRATION_DELAY=2 bash "$SWAP_SCRIPT" "$t10a_live" "v2" "$t10a_hook" > "${SANDBOX}/t10a/swap.log" 2>&1
touch "$t10a_stop"
wait "$t10a_reader_pid"

t10a_fail_count="$(grep -c 'FAIL' "$t10a_reader_log" || true)"
t10a_ok_count="$(grep -c '^ok$' "$t10a_reader_log" || true)"
echo "INFO: absolute-path/new-process reader during a 2s widened conversion gap: ${t10a_fail_count} failed, ${t10a_ok_count} ok attempts"
assert_true "absolute-path reader: the gap is real, not just theoretical -- at least one attempt failed during it" \
  "$([ "$t10a_fail_count" -gt 0 ] && echo true || echo false)"
assert_eq "absolute-path reader: content is readable and correct again immediately once the swap completes" \
  "content-v2" "$(cat "$t10a_live/marker.txt")"

# 10b: a GENUINE interruption -- SIGKILL the swap script while it is asleep
# inside the induced gap (after the mv, before the ln) -- then confirm the
# reader sees real, continuous failures through the crashed window, and that
# a subsequent run's self-healing recovery (Test 6) restores it while the
# SAME reader is still watching.

t10b_live="${SANDBOX}/t10b/grid_release"
mkdir -p "$(dirname "$t10b_live")"
mkdir -p "$t10b_live"
echo "release-before-kill" > "$t10b_live/marker.txt"

t10b_reader_log="${SANDBOX}/t10b/reader.log"
: > "$t10b_reader_log"
t10b_stop="${SANDBOX}/t10b/stop"
(
  while [ ! -f "$t10b_stop" ]; do
    if cat "${t10b_live}/marker.txt" > /dev/null 2>/dev/null; then
      echo ok >> "$t10b_reader_log"
    else
      echo FAIL >> "$t10b_reader_log"
    fi
  done
) &
t10b_reader_pid=$!

t10b_hook="${SANDBOX}/t10b/build_ok.sh"
cat > "$t10b_hook" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
echo "release-after-recovery" > "$1/marker.txt"
HOOK
chmod +x "$t10b_hook"

DEPLOY_TEST_MIGRATION_DELAY=5 bash "$SWAP_SCRIPT" "$t10b_live" "v2" "$t10b_hook" > "${SANDBOX}/t10b/swap1.log" 2>&1 &
t10b_swap_pid=$!
sleep 1.5
kill -9 "$t10b_swap_pid" 2>/dev/null || true
wait "$t10b_swap_pid" 2>/dev/null || true

assert_true "genuine interruption: live path does not exist at all right after the kill" \
  "$([ ! -e "$t10b_live" ] && [ ! -L "$t10b_live" ] && echo true || echo false)"

sleep 1
t10b_crashed_fail_count="$(grep -c 'FAIL' "$t10b_reader_log" || true)"
assert_true "genuine interruption: the absolute-path reader observed real failures during the crashed window" \
  "$([ "$t10b_crashed_fail_count" -gt 0 ] && echo true || echo false)"

set +e
bash "$SWAP_SCRIPT" "$t10b_live" "v2-recovered" "$t10b_hook" > "${SANDBOX}/t10b/swap2.log" 2>&1
t10b_recover_exit=$?
set -e

touch "$t10b_stop"
wait "$t10b_reader_pid"

assert_eq "genuine interruption + recovery: the self-healing recovery run exits 0" "0" "$t10b_recover_exit"
assert_eq "genuine interruption + recovery: live content reflects the recovered release" \
  "release-after-recovery" "$(cat "$t10b_live/marker.txt")"
t10b_last_line="$(tail -1 "$t10b_reader_log")"
assert_true "genuine interruption + recovery: the reader is succeeding again by the time recovery has run" \
  "$([ "$t10b_last_line" = "ok" ] && echo true || echo false)"
echo "INFO: absolute-path reader across a real SIGKILL-interrupted conversion: ${t10b_crashed_fail_count} failures observed before recovery ran"

# Remaining availability gap, stated plainly: a fresh absolute-path lookup
# (a new process, a health check, `ls`) issued during the gap between the
# `mv` and the `ln` -- or during the window after a genuine crash there,
# until the next run's self-healing recovery executes -- WILL fail with
# ENOENT, as measured above. An already-open cwd (Test 9) is completely
# unaffected throughout the same kind of window, proven the same way. This
# gap cannot be closed further without a Linux-specific atomic
# directory<->symlink exchange syscall not exposed by coreutils and not
# judged worth a custom wrapper for a window that is sub-millisecond in
# every real run and occurs at most once, ever, per <live_path>. What IS
# closed: the window never lasts longer than "until the next run of this
# script," because that run self-heals unconditionally (Test 6) -- it does
# not require a human to notice and intervene.

# ── Test 11: deploy_release_rollback.sh ─────────────────────────────────────
# Every invocation below passes --schema-compatible, now a required,
# enforced acknowledgment (not just documented) that reverting FILES does
# not revert a committed migration -- see the dedicated sub-test below that
# confirms omitting it is refused. After each successful swap this test
# clears .activation-in-progress itself, standing in for deploy.yml's own
# end-of-job cleanup step, so the rollback assertions below exercise
# rollback's core swap-target logic; the activation-marker gate itself gets
# its own dedicated sub-test further down.

t11_live="${SANDBOX}/t11/grid_release"
mkdir -p "$(dirname "$t11_live")"
mkdir -p "$t11_live"
echo "release-1" > "$t11_live/marker.txt"

set +e
bash "$ROLLBACK_SCRIPT" "$t11_live" --schema-compatible > "${SANDBOX}/t11/rollback_none.log" 2>&1
t11_exit_none=$?
set -e
assert_eq "rollback with no prior successful swap: exits 1 (nothing to roll back to)" "1" "$t11_exit_none"

t11_hook1="${SANDBOX}/t11/build1.sh"
cat > "$t11_hook1" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
echo "release-2" > "$1/marker.txt"
HOOK
chmod +x "$t11_hook1"
bash "$SWAP_SCRIPT" "$t11_live" "r2" "$t11_hook1" > "${SANDBOX}/t11/swap1.log" 2>&1
rm -f "${t11_live}.releases/.activation-in-progress"

t11_hook2="${SANDBOX}/t11/build2.sh"
cat > "$t11_hook2" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
echo "release-3" > "$1/marker.txt"
HOOK
chmod +x "$t11_hook2"
bash "$SWAP_SCRIPT" "$t11_live" "r3" "$t11_hook2" > "${SANDBOX}/t11/swap2.log" 2>&1
rm -f "${t11_live}.releases/.activation-in-progress"
assert_eq "rollback setup: two swaps landed release-3 live" "release-3" "$(cat "$t11_live/marker.txt")"

bash "$ROLLBACK_SCRIPT" "$t11_live" --schema-compatible > "${SANDBOX}/t11/rollback.log" 2>&1
assert_eq "rollback: live content is release-2 again (the one before the last swap)" \
  "release-2" "$(cat "$t11_live/marker.txt")"

set +e
bash "$ROLLBACK_SCRIPT" "$t11_live" --schema-compatible > "${SANDBOX}/t11/rollback2.log" 2>&1
t11_exit_again=$?
set -e
assert_eq "rollback: rolling back again when already there exits 0 (no-op)" "0" "$t11_exit_again"
assert_eq "rollback: content still release-2 after the no-op rollback" \
  "release-2" "$(cat "$t11_live/marker.txt")"

# Schema-compatibility acknowledgment is REQUIRED, not advisory.
set +e
bash "$ROLLBACK_SCRIPT" "$t11_live" > "${SANDBOX}/t11/rollback_no_ack.log" 2>&1
t11_exit_no_ack=$?
set -e
assert_eq "rollback without --schema-compatible: refuses with its own distinct exit code" "5" "$t11_exit_no_ack"
assert_true "rollback without --schema-compatible: message explains reverting files does not revert a migration" \
  "$(grep -q 'does not reverse a committed migration' "${SANDBOX}/t11/rollback_no_ack.log" && echo true || echo false)"
assert_eq "rollback without --schema-compatible: touched nothing -- content still release-2" \
  "release-2" "$(cat "$t11_live/marker.txt")"

# Concurrency: an in-flight deploy build/swap (holding .lock) still blocks
# rollback outright, as before.
t11_slow_hook="${SANDBOX}/t11/build_slow.sh"
cat > "$t11_slow_hook" << 'HOOK'
#!/usr/bin/env bash
set -euo pipefail
sleep 3
echo "release-4" > "$1/marker.txt"
HOOK
chmod +x "$t11_slow_hook"
bash "$SWAP_SCRIPT" "$t11_live" "r4" "$t11_slow_hook" > "${SANDBOX}/t11/swap_slow.log" 2>&1 &
t11_slow_pid=$!
sleep 1
set +e
DEPLOY_LOCK_WAIT_SECS=1 bash "$ROLLBACK_SCRIPT" "$t11_live" --schema-compatible > "${SANDBOX}/t11/rollback_contended.log" 2>&1
t11_exit_contended=$?
set -e
wait "$t11_slow_pid"
rm -f "${t11_live}.releases/.activation-in-progress"

assert_eq "rollback during an in-flight deploy build/swap: fails fast with the lock-contention exit code" "3" "$t11_exit_contended"

# Activation-in-progress marker: written by deploy_release_swap.sh on every
# successful swap, normally cleared by deploy.yml's own last step. A FRESH
# marker means that clearing step (and everything before it -- restart,
# health check) may not have run yet -- rollback must refuse, not race it.
bash "$SWAP_SCRIPT" "$t11_live" "r5" "$t11_hook1" > "${SANDBOX}/t11/swap5.log" 2>&1
assert_true "activation marker: a successful swap leaves .activation-in-progress behind" \
  "$([ -f "${t11_live}.releases/.activation-in-progress" ] && echo true || echo false)"

set +e
bash "$ROLLBACK_SCRIPT" "$t11_live" --schema-compatible > "${SANDBOX}/t11/rollback_fresh_marker.log" 2>&1
t11_exit_fresh_marker=$?
set -e
assert_eq "rollback with a FRESH activation-in-progress marker: refuses with its own distinct exit code" "6" "$t11_exit_fresh_marker"
assert_eq "rollback with a FRESH activation-in-progress marker: touched nothing -- content still release-2" \
  "release-2" "$(cat "$t11_live/marker.txt")"

# A STALE marker (older than the threshold -- simulating a deploy.yml job
# that crashed outright before its own cleanup step could run) is a
# warning, not a refusal. Backdate the marker's mtime directly rather than
# racing a real clock against a short threshold.
touch -d '-30 minutes' "${t11_live}.releases/.activation-in-progress"
set +e
bash "$ROLLBACK_SCRIPT" "$t11_live" --schema-compatible > "${SANDBOX}/t11/rollback_stale_marker.log" 2>&1
t11_exit_stale=$?
set -e
assert_eq "rollback with a STALE activation-in-progress marker: proceeds (exit 0), does not refuse" "0" "$t11_exit_stale"
assert_true "rollback with a STALE marker: logs a loud warning rather than proceeding silently" \
  "$(grep -q 'STALE activation-in-progress marker' "${SANDBOX}/t11/rollback_stale_marker.log" && echo true || echo false)"
# .previous-release is overwritten on every successful swap to whatever was
# JUST live going into it -- by the time swap r5 ran, the concurrency
# sub-test's slow r4 swap had already completed in the background and
# become live, so r5's recorded previous release is release-4 (r2's own
# directory, not merely r2's text, which r5 happens to also write).
assert_eq "rollback with a STALE marker: content rolled back to whichever release was live before swap r5 (release-4, from the earlier concurrency sub-test's slow swap completing in the background)" \
  "release-4" "$(cat "$t11_live/marker.txt")"

echo
echo "=== ${pass_count} passed, ${fail_count} failed ==="
if [ "$fail_count" -gt 0 ]; then
  exit 1
fi
