#!/usr/bin/env bash
# Atomic release-directory swap for deploy.yml's "Build release tree" step.
#
# Why this exists: #596's deploy (2026-09-22) fetched, reset, and built the
# PWA directly on top of the LIVE release directory, then failed during
# `alembic upgrade head`. The new source and freshly-built PWA bundle were
# already sitting in the live path when the migration failed -- grid-api and
# grid-hermes kept running their old in-memory code, but the ON-DISK tree
# either process would load on any restart (planned, or an unplanned crash --
# both units are Restart=always) no longer matched the database schema.
# Recovering required a coordinated pause of every writer touching the
# affected tables and an isolated revert PR (#597) before either service
# could safely restart. This script makes that specific failure mode
# structurally impossible: nothing is written to the live path until a
# candidate build has fully succeeded, migration included.
#
# Design: <live_path> is a symlink pointing at a versioned directory under
# <live_path>.releases/<label>/. A run that finds <live_path> is still a
# plain directory (the current production shape) migrates it transparently
# on its first use -- moves it to <live_path>.releases/<label>/ and replaces
# <live_path> with a symlink to that same content, so every existing
# consumer of <live_path> (a systemd WorkingDirectory, a hardcoded path
# reference, anything) keeps resolving to exactly the same files, before,
# during, and after that migration. Nothing downstream of this script needs
# to change to benefit from it.
#
# Each run then does its fetch/build/migrate in a FRESH candidate directory,
# never touching the live symlink's current target. Only if the entire
# build_hook succeeds does this script repoint the symlink -- a single
# `ln -sfn` + `mv -T`, which is atomic on a POSIX filesystem: a reader
# resolving <live_path> at any instant sees either the complete old target or
# the complete new one, never a partial or missing directory. On any failure,
# the candidate is left in place for inspection and the live symlink is not
# touched at all.
#
# One previous release is kept (<live_path>.releases/<previous label>) for
# manual inspection or a manual rollback; older ones are pruned. After every
# successful swap the release that WAS live is recorded at
# <live_path>.releases/.previous-release (one absolute path, plain text) so
# scripts/deploy_release_rollback.sh can repoint <live_path> back at it
# quickly and atomically -- e.g. if a restart following a successful swap
# fails its own health check. This script does not invoke that rollback
# itself: deciding to roll back is an operator call (see that script's own
# header for why), not something to do unattended the moment a restart
# hiccups.
#
# What the lock above does NOT cover: this script exits (and releases the
# lock) the moment the FILE swap succeeds. deploy.yml's restart and
# health-verification steps for that release run AFTER this script has
# already exited, as separate steps (separate shell processes -- a flock
# held via an open file descriptor cannot span them). To keep a manual
# rollback from racing that still-in-flight activation window, this script
# also writes <live_path>.releases/.activation-in-progress on every
# successful swap; deploy.yml clears it (via
# scripts/deploy_clear_activation_marker.sh, label-scoped so it only ever
# clears its OWN deploy's marker) as its own last step (`if: always()`,
# after this run's restarts/verifications). deploy_release_rollback.sh
# refuses outright while this marker is present -- there is no
# staleness-based auto-proceed; only an operator passing
# --override-stuck-activation, a deliberate acknowledgment that they have
# positively confirmed no deploy is actually running, gets past it. See
# that script's own header.
#
# Concurrency: this script holds an exclusive lock
# (<live_path>.releases/.lock) for its ENTIRE run, from the crash-recovery
# check below through the final prune. Two runs targeting the same
# <live_path> -- two pushes racing, or a workflow_dispatch overlapping a
# push -- are fully serialized rather than both `rm -rf`ing the same stale
# candidate, both running `alembic upgrade head` concurrently against the
# same DB, or one run's prune step deleting a release the other run still
# needs. deploy.yml also sets a `concurrency:` group as the primary defense
# (a queued run never even occupies a runner); this lock is the backstop for
# anything that group doesn't cover (a manual invocation outside CI, a
# future workflow change). A run that cannot acquire it within
# DEPLOY_LOCK_WAIT_SECS (default 600) fails loudly (exit 3) instead of
# hanging a runner indefinitely.
#
# Crash recovery for the one-time migration above: it is two separate
# filesystem operations (move the directory aside, then create the symlink)
# -- POSIX has no single syscall that turns an existing non-empty directory
# into a symlink elsewhere. If this script is killed between those two
# operations, <live_path> is left NOT EXISTING AT ALL until the next run.
# Uncorrected, that next run's own migration check
# (`[ -d "$LIVE_PATH" ] && [ ! -L "$LIVE_PATH" ]`) sees neither a directory
# nor a symlink, silently treats this as "nothing to build from," and starts
# an empty candidate -- discarding the perfectly-good previous release into
# an orphaned pre-atomic-* directory nothing ever links back to. This script
# detects exactly that state and re-links <live_path> to the most recent
# pre-atomic-* directory before doing anything else, so an interrupted first
# conversion self-heals on the very next run instead of losing the release.
#
# What that recovery does NOT protect against is a brand-new path lookup of
# <live_path> issued in the narrow window between the two syscalls itself
# (a fresh process start, a health check, `ls`) -- that would see ENOENT.
# Already-running processes are unaffected regardless of the crash timing:
# Linux resolves a relative path against a process's cwd via the pinned
# directory inode, not the name, so a process that had already chdir()'d
# into <live_path> keeps reading exactly the same files straight through
# this whole conversion -- proven empirically (with real, measured failure
# counts for the contrasting cases, not just asserted) in
# tests/deploy/test_deploy_release_swap.sh. The ENOENT window itself has no
# fully atomic fix on POSIX without a filesystem-specific syscall (Linux's
# renameat2(RENAME_EXCHANGE), not exposed by coreutils' mv or worth a custom
# syscall wrapper for a one-time, sub-millisecond-in-practice window).
# Accepted as a bounded, self-healing residual risk, not a silent one, and
# it can only ever occur on whichever single future deploy is the first to
# run this script against a target that is still a plain directory today.
#
# "Self-heals on the next run" describes what THIS SCRIPT does once it
# executes again -- it is not a claim that recovery happens on its own with
# no operator involved. Nothing on grid-svr retries a crashed deploy
# automatically; this script only ever runs as part of a deploy.yml job,
# and nothing starts a new one by itself. If the very first conversion
# (today, against production's still-plain-directory $DEPLOY_PATH) is
# interrupted, <live_path> stays missing -- and grid-api/grid-hermes cannot
# be (re)started onto it, though already-running processes are unaffected
# per the paragraph above -- until an operator deliberately starts a new
# deploy.yml run: either a normal push to main, or, for the specific case
# of the triggering run itself having failed/been interrupted,
# `gh run rerun <run_id> --failed --repo 3pacs/GRID` (this session's own
# established retry mechanism). That new run's "Build release tree" step
# invokes this script again, and recovery then happens as its first action,
# automatically, within that run -- but getting to that point is a human
# decision, not a background process.
#
# A build_hook that succeeds -- including a migration that COMMITS schema
# changes -- does not guarantee the swap that follows it also succeeds (the
# filesystem could go read-only or fill up in between). If that happens, the
# database may already be ahead of the code <live_path> still serves, and
# this script cannot undo a committed migration. It does not try to; instead
# it fails with a distinctly different exit code (4) and an unambiguous, loud
# message calling this out specifically -- deliberately NOT reusing exit 1
# ("build_hook failed"), which means the opposite: nothing was touched. This
# is also why every migration in this codebase is expected to be additive
# and backward-compatible with the currently-deployed code by construction
# (the established SET LOCAL lock_timeout/statement_timeout discipline
# already assumes grid-api stays up mid-migration): this script can surface
# an incompatible-migration failure loudly, it cannot prevent one from being
# written.
#
# Usage:
#   deploy_release_swap.sh <live_path> <label> <build_hook> [build_hook_args...]
#
#   <live_path>   e.g. /data/grid_v4/grid_release. Promoted to a symlink on
#                 first use if it is still a plain directory.
#   <label>       a unique name for this run's candidate release directory,
#                 e.g. a short git SHA. Must be a valid single path segment.
#   <build_hook>  an executable invoked with the candidate directory as both
#                 its cwd and as $1. It must exit non-zero on ANY failure
#                 (fetch, install, PWA build, or migration) -- this script
#                 treats a non-zero exit as "leave the live path alone",
#                 unconditionally, with no partial-success carve-out.
#
# Exit status:
#   0  candidate build succeeded and the swap completed
#   1  build_hook failed -- <live_path> untouched, candidate kept for inspection
#   2  usage error
#   3  could not acquire the per-<live_path> lock within DEPLOY_LOCK_WAIT_SECS
#   4  build_hook succeeded (possibly including a committed migration) but the
#      swap itself failed -- <live_path> may now be running code that is
#      stale relative to the database. Needs immediate manual attention; see
#      the message printed to stderr for the exact candidate to repoint at.

set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "usage: $0 <live_path> <label> <build_hook> [build_hook_args...]" >&2
  exit 2
fi

LIVE_PATH="$1"
LABEL="$2"
BUILD_HOOK="$3"
shift 3
if [[ ! "$LABEL" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || [ "$LABEL" = ".." ]; then
  echo "release label must be one safe path segment" >&2
  exit 2
fi

RELEASES_DIR="${LIVE_PATH}.releases"
CANDIDATE_DIR="${RELEASES_DIR}/${LABEL}"
KEEP_RELEASES=2
LOCK_WAIT_SECS="${DEPLOY_LOCK_WAIT_SECS:-600}"

mkdir -p "$RELEASES_DIR"

# Serialize every run targeting this <live_path> -- see header. Held for the
# remainder of the script, released automatically on any exit.
LOCK_FILE="${RELEASES_DIR}/.lock"
exec {lock_fd}>"$LOCK_FILE"
if ! flock -w "$LOCK_WAIT_SECS" "$lock_fd"; then
  echo "could not acquire deploy lock on $LOCK_FILE within ${LOCK_WAIT_SECS}s -- another deploy against $LIVE_PATH is running long, or stuck" >&2
  exit 3
fi

# This record is created by the release controller BEFORE the first swap.
# It deliberately lives outside the mutable checkout and is never overwritten
# by a later deployment. Both paths must name real, immutable release folders.
# An old scheduler process can keep its cwd after a rename, but pruning that
# folder makes its next import (or an incidental restart) unsafe.
PRESERVATION_FILE="${RELEASES_DIR}/.runtime-preservation"
if [ -n "${GRID_DEPLOY_TEST_SANDBOX:-}" ]; then
  # Legacy filesystem failure-injection tests predate this production gate.
  # The escape hatch is confined to their temporary sandbox, never the live
  # GRID path, and new preservation tests exercise the real gate below.
  test_root="$(realpath -e -- "$GRID_DEPLOY_TEST_SANDBOX")"
  case "$test_root" in
    /tmp/tmp.*) ;;
    *) echo "test sandbox must be a mktemp directory under /tmp" >&2; exit 2 ;;
  esac
  case "$(realpath -m -- "$LIVE_PATH")" in
    "$test_root"/*) test_only_skip_preservation=1 ;;
    *) echo "test sandbox does not contain live path" >&2; exit 2 ;;
  esac
fi
if [ "${test_only_skip_preservation:-0}" != 1 ]; then
  if [ ! -f "$PRESERVATION_FILE" ] || [ -L "$PRESERVATION_FILE" ]; then
    echo "runtime preservation record missing or linked: $PRESERVATION_FILE; bootstrap before any swap" >&2
    exit 5
  fi
  mapfile -t preservation_lines < "$PRESERVATION_FILE"
  if [ "${#preservation_lines[@]}" -ne 6 ] ||
     [[ "${preservation_lines[0]}" != scheduler=* ]] ||
     [[ "${preservation_lines[1]}" != recovery=* ]] ||
     [[ "${preservation_lines[2]}" != scheduler_sha=* ]] ||
     [[ "${preservation_lines[3]}" != scheduler_tree=* ]] ||
     [[ "${preservation_lines[4]}" != recovery_sha=* ]] ||
     [[ "${preservation_lines[5]}" != recovery_tree=* ]]; then
    echo "invalid runtime preservation record: expected two paths and their SHA/tree identities" >&2
    exit 5
  fi
  scheduler_dir="${preservation_lines[0]#scheduler=}"
  recovery_dir="${preservation_lines[1]#recovery=}"
  scheduler_sha="${preservation_lines[2]#scheduler_sha=}"
  scheduler_tree="${preservation_lines[3]#scheduler_tree=}"
  recovery_sha="${preservation_lines[4]#recovery_sha=}"
  recovery_tree="${preservation_lines[5]#recovery_tree=}"
  releases_root="$(realpath -e -- "$RELEASES_DIR")"
  if [ "$releases_root" != "$RELEASES_DIR" ]; then
    echo "release root must be an absolute canonical directory: $RELEASES_DIR" >&2
    exit 5
  fi
  for protected_dir in "$scheduler_dir" "$recovery_dir"; do
    if [ ! -d "$protected_dir" ] || [ -L "$protected_dir" ] ||
       [ "$(realpath -e -- "$protected_dir")" != "$protected_dir" ] ||
       [ "$(dirname -- "$protected_dir")" != "$releases_root" ]; then
      echo "runtime preservation target is missing, linked, outside or noncanonical: $protected_dir" >&2
      exit 5
    fi
  done
  verify_preserved_checkout() {
    local name="$1" dir="$2" expected_sha="$3" expected_tree="$4"
    if [[ ! "$expected_sha" =~ ^[0-9a-f]{40}$ ]] ||
       [[ ! "$expected_tree" =~ ^[0-9a-f]{40}$ ]] ||
       [ "$(git -C "$dir" rev-parse --show-toplevel 2>/dev/null || true)" != "$dir" ] ||
       [ "$(git -C "$dir" rev-parse HEAD 2>/dev/null || true)" != "$expected_sha" ] ||
       [ "$(git -C "$dir" rev-parse 'HEAD^{tree}' 2>/dev/null || true)" != "$expected_tree" ] ||
       ! git -C "$dir" diff --quiet HEAD --; then
      echo "$name preservation Git HEAD/tree or tracked files do not match approved identity" >&2
      exit 5
    fi
  }
  verify_preserved_checkout scheduler "$scheduler_dir" "$scheduler_sha" "$scheduler_tree"
  verify_preserved_checkout recovery "$recovery_dir" "$recovery_sha" "$recovery_tree"
  scheduler_pid="$(systemctl show -p MainPID --value grid-scheduler 2>/dev/null || true)"
  scheduler_workdir="$(systemctl show -p WorkingDirectory --value grid-scheduler 2>/dev/null || true)"
  if [[ ! "$scheduler_pid" =~ ^[1-9][0-9]*$ ]] ||
     [ ! -d "/proc/${scheduler_pid}/cwd" ] ||
     [ "$(readlink -f -- "/proc/${scheduler_pid}/cwd" 2>/dev/null || true)" != "$scheduler_dir" ] ||
     [ "$scheduler_workdir" != "$scheduler_dir" ]; then
    echo "scheduler PID/cwd/effective WorkingDirectory is not pinned to $scheduler_dir" >&2
    exit 5
  fi
fi

# Preserve every installed/loaded GRID service's current and restart directory,
# not just the scheduler recorded above. Activation is intentionally independent
# of a release swap (notably for realtime). Keep the union across the swap so a
# mutable WorkingDirectory symlink cannot erase its former target from this set.
# Service repoints must share release coordination; refresh before each deletion
# as well, failing closed on an unreadable/deleted process cwd or systemd query.
declare -A runtime_release_dirs=()
CGROUP_ROOT=/sys/fs/cgroup
PROC_CGROUP_ROOT=/proc
runtime_pid_start() {
  local raw rest
  local -a fields
  raw="$(cat -- "/proc/$1/stat")" || return 1
  rest="${raw##*) }"
  read -r -a fields <<< "$rest"
  [ "${#fields[@]}" -ge 20 ] && [[ "${fields[19]}" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "${fields[19]}"
}
runtime_pid_cgroup() {
  local raw
  raw="$(cat -- "$PROC_CGROUP_ROOT/$1/cgroup")" || return 1
  # Only a single unified-v2 entry is understood; never guess at v1/hybrid.
  [[ "$raw" == 0::/* && "$raw" != *$'\n'* ]] || return 1
  printf '%s\n' "${raw#0::}"
}
protect_runtime_pid() {
  local pid="$1" owner="$2" cwd start end
  if [[ ! "$pid" =~ ^[1-9][0-9]*$ ]] ||
     ! start="$(runtime_pid_start "$pid")" ||
     ! cwd="$(readlink -- "/proc/$pid/cwd")" || [[ "$cwd" == *' (deleted)' ]]; then
    echo "missing/deleted runtime cwd for $owner PID $pid" >&2
    exit 5
  fi
  protect_runtime_path "$cwd" "$owner PID $pid"
  if ! end="$(runtime_pid_start "$pid")" || [ "$start" != "$end" ]; then
    echo "runtime PID identity changed for $owner PID $pid" >&2
    exit 5
  fi
}
protect_runtime_path() {
  local path="$1" owner="$2" resolved relative release
  if [[ "$path" != /* ]] || ! resolved="$(realpath -e -- "$path")" || [ ! -d "$resolved" ]; then
    echo "cannot resolve $owner runtime directory: $path" >&2
    exit 5
  fi
  # Also retain a configured alias inside the releases directory: deleting the
  # alias would break a future restart even though its resolved target survives.
  case "$path" in
    "$RELEASES_DIR"/*)
      relative="${path#"$RELEASES_DIR"/}"
      runtime_release_dirs["${RELEASES_DIR}/${relative%%/*}"]=1
      ;;
  esac
  case "$resolved" in
    "$RELEASES_DIR"/*)
      relative="${resolved#"$RELEASES_DIR"/}"
      release="${RELEASES_DIR}/${relative%%/*}"
      runtime_release_dirs["$release"]=1
      ;;
  esac
}
protect_runtime_cgroup() {
  local cgroup="$1" owner="$2" main_pid="$3" cgroup_dir files file members member
  if [ ! -f "$CGROUP_ROOT/cgroup.controllers" ] || [[ "$cgroup" != /* ]] ||
     [ "$cgroup" = / ] || ! cgroup_dir="$(realpath -e -- "$CGROUP_ROOT$cgroup")" ||
     [ "$cgroup_dir" != "$CGROUP_ROOT$cgroup" ] ||
     [ "$(realpath -e -- "$CGROUP_ROOT")" != "$CGROUP_ROOT" ] || [ ! -d "$cgroup_dir" ]; then
    echo "unsupported or unresolved cgroup for $owner: $cgroup" >&2
    exit 5
  fi
  if ! files="$(find "$cgroup_dir" -type f -name cgroup.procs -print)" ||
     [ ! -f "$cgroup_dir/cgroup.procs" ] || [ -z "$files" ]; then
    echo "cannot completely inventory cgroup for $owner" >&2
    exit 5
  fi
  while IFS= read -r file; do
    if ! members="$(cat -- "$file")"; then
      echo "cannot read cgroup membership for $owner" >&2
      exit 5
    fi
    while IFS= read -r member; do
      [ -n "$member" ] || continue
      protect_runtime_pid "$member" "$owner cgroup"
      runtime_member_count=$((runtime_member_count + 1))
      [ "$member" != "$main_pid" ] || runtime_main_seen=1
    done <<< "$members"
  done <<< "$files"
}
refresh_runtime_release_dirs() {
  [ "${test_only_skip_preservation:-0}" != 1 ] || return 0
  local installed loaded units unit details key value load pid workdir workdir_seen
  local state substate type remain exec_pid cgroup cgroup_seen actual_cgroup start end
  local runtime_member_count runtime_main_seen
  if ! installed="$(systemctl list-unit-files --no-legend --no-pager 'grid-*.service')" ||
     ! loaded="$(systemctl list-units --all --plain --no-legend --no-pager 'grid-*.service')"; then
    echo 'cannot inventory GRID service runtimes; refusing release deletion' >&2
    exit 5
  fi
  units="$(printf '%s\n%s\n' "$installed" "$loaded" | awk 'NF {print $1}' | sort -u)"
  if [ -z "$units" ]; then
    echo 'empty GRID service inventory; refusing release deletion' >&2
    exit 5
  fi
  while IFS= read -r unit; do
    [[ "$unit" == grid-*.service ]] || { echo "invalid runtime unit: $unit" >&2; exit 5; }
    # A template cannot run without an instance; loaded instances are included
    # by list-units above and must still be inspected.
    [[ "$unit" != *@.service ]] || continue
    if ! details="$(systemctl show --property=LoadState,MainPID,WorkingDirectory,ActiveState,ControlGroup,SubState,Type,RemainAfterExit,ExecMainPID -- "$unit")"; then
      echo "cannot inspect runtime unit $unit" >&2
      exit 5
    fi
    load= pid= workdir= workdir_seen=0 state= cgroup= cgroup_seen=0
    substate= type= remain= exec_pid=
    while IFS='=' read -r key value; do
      case "$key" in
        LoadState) load="$value" ;;
        MainPID) pid="$value" ;;
        WorkingDirectory) workdir="$value"; workdir_seen=1 ;;
        ActiveState) state="$value" ;;
        ControlGroup) cgroup="$value"; cgroup_seen=1 ;;
        SubState) substate="$value" ;;
        Type) type="$value" ;;
        RemainAfterExit) remain="$value" ;;
        ExecMainPID) exec_pid="$value" ;;
      esac
    done <<< "$details"
    if [ "$load" != loaded ] || [[ ! "$pid" =~ ^[0-9]+$ ]] ||
       [ "$workdir_seen" != 1 ] || [ "$cgroup_seen" != 1 ] || [ -z "$state" ] ||
       [ -z "$substate" ] || [ -z "$type" ] || [[ ! "$remain" =~ ^(yes|no)$ ]] ||
       [[ ! "$exec_pid" =~ ^[0-9]+$ ]]; then
      echo "ambiguous runtime identity for $unit" >&2
      exit 5
    fi
    # Empty WorkingDirectory means systemd's default /, not an unknown value.
    protect_runtime_path "${workdir:-/}" "$unit configured"
    # MainPID=0 does not prove an empty unit: forked workers can remain in its
    # cgroup during failure/stopping. The only active empty-group exception is
    # an explicitly exited RemainAfterExit oneshot, which has no running task.
    if [ "$pid" = 0 ] && [ -z "$cgroup" ] && [ "$type" = oneshot ] &&
       [ "$remain" = yes ] && [ "$state" = active ] && [ "$substate" = exited ]; then
      continue
    fi
    runtime_member_count=0 runtime_main_seen=0 actual_cgroup=
    if [ "$pid" != 0 ]; then
      if [ "$pid" != "$exec_pid" ] || ! start="$(runtime_pid_start "$pid")" ||
         ! actual_cgroup="$(runtime_pid_cgroup "$pid")"; then
        echo "unverifiable main process identity for $unit" >&2
        exit 5
      fi
      protect_runtime_pid "$pid" "$unit"
      protect_runtime_cgroup "$actual_cgroup" "$unit actual" "$pid"
      if [ "$runtime_main_seen" != 1 ]; then
        echo "main PID missing from actual cgroup for $unit" >&2
        exit 5
      fi
    fi
    if [ -n "$cgroup" ] && [ "$cgroup" != "$actual_cgroup" ]; then
      protect_runtime_cgroup "$cgroup" "$unit declared" "$pid"
    fi
    if [ "$runtime_member_count" = 0 ] && [ "$state" != inactive ] && [ "$state" != failed ]; then
      echo "ambiguous empty/inconsistent cgroup for $unit" >&2
      exit 5
    fi
    if [ "$pid" != 0 ]; then
      if ! end="$(runtime_pid_start "$pid")" || [ "$start" != "$end" ] ||
         [ "$(runtime_pid_cgroup "$pid")" != "$actual_cgroup" ]; then
        echo "runtime identity changed during inventory for $unit" >&2
        exit 5
      fi
    fi
  done <<< "$units"
}
refresh_runtime_release_dirs

# Crash recovery: see header. Only fires when <live_path> does not exist as
# anything at all (a prior run was killed mid-conversion).
if [ ! -e "$LIVE_PATH" ] && [ ! -L "$LIVE_PATH" ]; then
  recovered_target="$(find "$RELEASES_DIR" -mindepth 1 -maxdepth 1 -type d -name 'pre-atomic-*' -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2- || true)"
  if [ -n "$recovered_target" ]; then
    echo "recovering interrupted first-install conversion: $LIVE_PATH was missing entirely, relinking -> $recovered_target" >&2
    ln -sfn "$recovered_target" "$LIVE_PATH"
  fi
fi

# One-time, transparent migration: a plain directory at LIVE_PATH becomes a
# versioned release plus a symlink pointing at it. Skipped entirely once
# LIVE_PATH is already a symlink (every run after the first, and any run
# after the crash-recovery step above has already re-established one).
if [ -d "$LIVE_PATH" ] && [ ! -L "$LIVE_PATH" ]; then
  existing_label="pre-atomic-$(date -u +%Y%m%dT%H%M%SZ)"
  echo "migrating existing plain directory at $LIVE_PATH -> ${RELEASES_DIR}/${existing_label}" >&2
  mv -T "$LIVE_PATH" "${RELEASES_DIR}/${existing_label}"
  # Test-only hook, a no-op in every real run (default 0): widens the gap
  # between the two syscalls above and below so a test can deterministically
  # observe what a concurrent reader sees during it, instead of relying on
  # the sub-millisecond natural window to be caught by chance. Never set
  # outside tests/deploy/test_deploy_release_swap.sh.
  if [ "${DEPLOY_TEST_MIGRATION_DELAY:-0}" != "0" ]; then
    sleep "$DEPLOY_TEST_MIGRATION_DELAY"
  fi
  ln -sfn "${RELEASES_DIR}/${existing_label}" "$LIVE_PATH"
fi

# A run with a label that is ALREADY the live target (a retry of a commit
# that already fully deployed -- e.g. `gh run rerun --failed` landing on a
# push whose swap already succeeded but a later step, like the health check,
# flaked) must never fall into the stale-candidate cleanup below: that would
# `rm -rf` the directory $LIVE_PATH is CURRENTLY pointing at, leave the
# symlink dangling, and abort on the very next line (`cp -a` from a
# now-deleted source) -- breaking a working production release for no
# reason, exactly the class of self-inflicted outage this whole script
# exists to prevent. Detected and short-circuited here, unconditionally: no
# rebuild-in-place is ever attempted against a live target, matching the
# rest of this script's rule that nothing is ever mutated while it's live.
if [ -L "$LIVE_PATH" ]; then
  live_resolved="$(readlink -f "$LIVE_PATH")"
  candidate_resolved="$(readlink -f "$CANDIDATE_DIR" 2>/dev/null || true)"
  if [ -n "$candidate_resolved" ] && [ "$live_resolved" = "$candidate_resolved" ]; then
    echo "label '$LABEL' is already the live release at $LIVE_PATH -- nothing to do" >&2
    exit 0
  fi
fi

if [ -e "$CANDIDATE_DIR" ]; then
  refresh_runtime_release_dirs
  if [ "${test_only_skip_preservation:-0}" != 1 ] &&
     { [ "$CANDIDATE_DIR" = "$scheduler_dir" ] || [ "$CANDIDATE_DIR" = "$recovery_dir" ] ||
       [ "${runtime_release_dirs[$CANDIDATE_DIR]:-0}" = 1 ]; }; then
    echo "candidate label names a protected runtime directory" >&2
    exit 5
  fi
  echo "removing stale candidate at $CANDIDATE_DIR from a prior interrupted run" >&2
  rm -rf "$CANDIDATE_DIR"
fi

# Seed the candidate from the current live content (a real git checkout, not
# a fresh clone) so the build hook's own `git fetch`/`git reset --hard` stays
# a fast incremental update, exactly like the pre-existing single-directory
# approach -- this script only changes WHEN and WHERE that lands, not how
# expensive it is to produce.
if [ -L "$LIVE_PATH" ]; then
  cp -a "$(readlink -f "$LIVE_PATH")" "$CANDIDATE_DIR"
else
  mkdir -p "$CANDIDATE_DIR"
fi

if ! "$BUILD_HOOK" "$CANDIDATE_DIR" "$@"; then
  echo "build_hook failed -- $LIVE_PATH left untouched, candidate preserved at $CANDIDATE_DIR" >&2
  exit 1
fi

previous_target=""
if [ -L "$LIVE_PATH" ]; then
  previous_target="$(readlink -f "$LIVE_PATH")"
fi

# Atomic swap: build the new symlink at a temp path, then rename it over the
# live one in a single syscall. A reader never observes a missing or
# half-updated $LIVE_PATH. A failure here (not the build_hook's fault -- it
# already succeeded) is the one case where the database and the live code
# can disagree; see header. Distinct exit code (4), loud message, on purpose.
TMP_LINK="${LIVE_PATH}.new-$$"
if ! { ln -sfn "$CANDIDATE_DIR" "$TMP_LINK" && mv -T "$TMP_LINK" "$LIVE_PATH"; }; then
  rm -f "$TMP_LINK" 2>/dev/null || true
  echo "::error::SWAP FAILED AFTER A SUCCESSFUL BUILD -- the build_hook (including any migration) already completed against $CANDIDATE_DIR, but $LIVE_PATH could not be repointed at it. If a migration ran, the database may now be ahead of the code $LIVE_PATH still serves. $CANDIDATE_DIR has the code matching the current schema -- repoint $LIVE_PATH at it manually (readlink -f $LIVE_PATH first, to record what it was) as soon as possible, or rerun this script (the migration step is idempotent)." >&2
  exit 4
fi
echo "swapped $LIVE_PATH -> $CANDIDATE_DIR" >&2

if [ -n "$previous_target" ]; then
  echo "$previous_target" > "${RELEASES_DIR}/.previous-release"
fi

# The FILE swap is done, but deploy.yml's restart/health-verification steps
# for THIS release haven't run yet -- they're separate steps (separate shell
# processes) that start after this script has already exited and released
# its lock above. Until the caller confirms those steps finished (deploy.yml
# clears this marker as its own last step, `if: always()`), a concurrent
# deploy_release_rollback.sh must not swap the live path out from under a
# restart or health check still in flight against it. Written unconditionally
# on every successful swap; a stale one left behind by a job that crashed
# outright (not just failed a step) before reaching its clearing step is
# handled by deploy_release_rollback.sh's own staleness check, not here.
echo "label=$LABEL swapped_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "${RELEASES_DIR}/.activation-in-progress"

# Prune old releases, keeping the one just replaced (for manual rollback)
# plus the new one -- never the candidate we just failed to promote, since a
# failed run exits above before reaching this point.
# Cleanup is best-effort AFTER the pointer/activation marker are committed.
# A pruning failure must not turn a successful swap into a failed build step,
# which would prevent deploy.yml from restarting API/Hermes onto the new tree.
# An asynchronous subshell preserves errexit (unlike `if function ...`) and the
# inherited protected-path union, while containing helper `exit` and rm errors.
if [ -n "$previous_target" ]; then
  (
  set -euo pipefail
  # Process substitution hides find/sort errors from mapfile and set -e. Never
  # act on a partial directory inventory even if it contains plausible paths.
  if ! release_inventory="$(find "$RELEASES_DIR" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' | sort -rn | cut -d' ' -f2-)" ||
     [ -z "$release_inventory" ]; then
    echo 'cannot completely inventory releases; refusing prune' >&2
    exit 5
  fi
  mapfile -t all_releases <<< "$release_inventory"
  kept=0
  for rel in "${all_releases[@]}"; do
    refresh_runtime_release_dirs
    if [ "$rel" = "$CANDIDATE_DIR" ] || [ "$rel" = "$previous_target" ] ||
       { [ "${test_only_skip_preservation:-0}" != 1 ] &&
         { [ "$rel" = "$scheduler_dir" ] || [ "$rel" = "$recovery_dir" ] ||
           [ "${runtime_release_dirs[$rel]:-0}" = 1 ]; }; }; then
      kept=$((kept + 1))
      continue
    fi
    if [ "$kept" -ge "$KEEP_RELEASES" ]; then
      echo "pruning old release $rel" >&2
      rm -rf "$rel"
    else
      kept=$((kept + 1))
    fi
  done
  ) &
  prune_pid=$!
  if wait "$prune_pid"; then
    :
  else
    prune_status=$?
    echo "::warning::Release swap succeeded; pruning stopped (status $prune_status). Remaining old releases are retained; continue activation of $CANDIDATE_DIR." >&2
  fi
fi

exit 0
