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
# this whole conversion -- proven empirically, not just asserted, in
# tests/deploy/test_deploy_release_swap.sh. The ENOENT window itself has no
# fully atomic fix on POSIX without a filesystem-specific syscall (Linux's
# renameat2(RENAME_EXCHANGE), not exposed by coreutils' mv or worth a custom
# syscall wrapper for a one-time, sub-millisecond-in-practice window).
# Accepted as a bounded, self-healing residual risk, not a silent one, and
# it can only ever occur on whichever single future deploy is the first to
# run this script against a target that is still a plain directory today.
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

# Prune old releases, keeping the one just replaced (for manual rollback)
# plus the new one -- never the candidate we just failed to promote, since a
# failed run exits above before reaching this point.
if [ -n "$previous_target" ]; then
  mapfile -t all_releases < <(find "$RELEASES_DIR" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' | sort -rn | cut -d' ' -f2-)
  kept=0
  for rel in "${all_releases[@]}"; do
    if [ "$rel" = "$CANDIDATE_DIR" ] || [ "$rel" = "$previous_target" ]; then
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
fi

exit 0
