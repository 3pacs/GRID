#!/usr/bin/env bash
# Manual, atomic rollback of <live_path> to the release that was live
# immediately before the most recent successful deploy_release_swap.sh run.
#
# Why a separate script, not something deploy_release_swap.sh does itself:
# a restart or health-check failure AFTER a successful swap can have many
# different causes, some of which rolling back this same-generation release
# wouldn't fix at all (a bad systemd unit file, an unrelated host problem) --
# deciding to roll back is an operator call. This script exists so that once
# that call is made, executing it is fast and safe: it reuses the exact same
# atomic ln+mv swap deploy_release_swap.sh uses (a reader of <live_path>
# never observes a missing or half-updated directory).
#
# PREREQUISITE, enforced, not just documented: schema compatibility. This
# script only repoints a symlink -- it CANNOT undo a committed database
# migration, and does not check whether the release you're rolling back TO
# can run correctly against whatever the database schema CURRENTLY is. If
# the deploy you're rolling back away from ran a migration that already
# committed, and that migration is not backward-compatible with the older
# code, rolling back the FILES can break things exactly as badly as the
# failure you're trying to escape -- reverting files does not reverse a
# committed migration. Because this is exactly the kind of check that gets
# skipped under incident pressure, this script refuses to run at all unless
# the operator passes `--schema-compatible`, an explicit acknowledgment that
# this has actually been checked (see the refusal message below for how).
#
# Concurrency, and what it does NOT cover: this script takes the same
# per-<live_path> lock deploy_release_swap.sh does, so it can never run
# WHILE that script's own build+swap is in progress. But that lock is
# released the moment the swap script exits -- deploy.yml's subsequent
# restart and health-verification steps for that release run AFTER the swap
# script has already exited, as separate steps holding no lock. To avoid
# racing THAT window -- rolling back while grid-api/grid-hermes are still
# being restarted or health-checked against the release this script is about
# to swap away from -- this script also checks
# <live_path>.releases/.activation-in-progress, written by
# deploy_release_swap.sh on every successful swap and cleared by deploy.yml
# as its own last step (`if: always()`). A fresh marker refuses the
# rollback outright (exit 6): wait for the workflow run to finish, or
# investigate it, rather than race it. A STALE marker (older than
# DEPLOY_ACTIVATION_STALE_SECS, default 1200s -- comfortably above this
# pipeline's realistic multi-minute restart+health-check window) is treated
# as evidence the workflow job crashed outright rather than merely failing a
# step (an `if: always()` cleanup step can't run if the runner/job itself is
# killed) -- proceeding past it is logged loudly, not silent.
#
# Usage: deploy_release_rollback.sh <live_path> --schema-compatible
#
# Reads <live_path>.releases/.previous-release (written by
# deploy_release_swap.sh after every successful swap) and repoints
# <live_path> at it. Exits non-zero, touching nothing, if that file is
# missing or no longer points at a directory that still exists (e.g.
# already pruned) -- there is no fallback guess.
#
# Exit status:
#   0  <live_path> now points at the recorded previous release (or already did)
#   1  nothing to roll back to (see the message on stderr for which case)
#   2  usage error
#   3  could not acquire the per-<live_path> lock -- a deploy build/swap is in progress
#   5  refused: --schema-compatible was not passed
#   6  refused: a deploy's post-swap activation looks still in progress
#      (fresh .activation-in-progress marker) -- see message for how to
#      proceed if you've confirmed the job actually crashed instead

set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <live_path> --schema-compatible" >&2
  exit 2
fi

LIVE_PATH="$1"

if [ "${2:-}" != "--schema-compatible" ]; then
  cat >&2 << 'MSG'
refusing: --schema-compatible was not passed.

This script only repoints a symlink -- it CANNOT undo a committed database
migration, and does not check whether the code you are rolling back TO can
run correctly against whatever the CURRENT schema is. If the deploy you are
rolling back away from ran a migration that already committed, and that
migration is not backward-compatible with the OLDER code, rolling back the
files can break things exactly as badly as the failure you are trying to
escape from -- reverting files does not reverse a committed migration.

Before passing --schema-compatible, confirm this yourself, e.g.:
  - check whether the deploy you're rolling back from ran any migration at all
  - if it did, check that migration's compatibility with the OLDER release's
    code (e.g. `alembic current` against what the older release expects)
  - when in doubt, ask before assuming a plain code rollback is safe

usage: deploy_release_rollback.sh <live_path> --schema-compatible
MSG
  exit 5
fi

RELEASES_DIR="${LIVE_PATH}.releases"
MARKER="${RELEASES_DIR}/.previous-release"
INPROGRESS_MARKER="${RELEASES_DIR}/.activation-in-progress"
LOCK_FILE="${RELEASES_DIR}/.lock"
LOCK_WAIT_SECS="${DEPLOY_LOCK_WAIT_SECS:-600}"
ACTIVATION_STALE_SECS="${DEPLOY_ACTIVATION_STALE_SECS:-1200}"

if [ ! -f "$MARKER" ]; then
  echo "no $MARKER -- nothing recorded to roll back to (has a successful deploy_release_swap.sh run ever completed against $LIVE_PATH?)" >&2
  exit 1
fi

target="$(cat "$MARKER")"
if [ ! -d "$target" ]; then
  echo "recorded previous release '$target' no longer exists (already pruned?) -- refusing to roll back to it" >&2
  exit 1
fi

if [ -f "$INPROGRESS_MARKER" ]; then
  marker_epoch="$(stat -c %Y "$INPROGRESS_MARKER" 2>/dev/null || echo 0)"
  now_epoch="$(date +%s)"
  age=$(( now_epoch - marker_epoch ))
  if [ "$age" -lt "$ACTIVATION_STALE_SECS" ]; then
    echo "refusing: a deploy's post-swap activation (restart/health verification) may still be in progress -- $INPROGRESS_MARKER is ${age}s old (threshold ${ACTIVATION_STALE_SECS}s): $(cat "$INPROGRESS_MARKER" 2>/dev/null). Check the deploy.yml run before rolling back. If it actually crashed (not just failed a step), either wait for this marker to age past the threshold or remove it manually once you've confirmed no deploy is actually running." >&2
    exit 6
  fi
  echo "warning: found a STALE activation-in-progress marker (${age}s old, past the ${ACTIVATION_STALE_SECS}s threshold: $(cat "$INPROGRESS_MARKER" 2>/dev/null)) -- treating as a deploy job that crashed outright rather than merely failing a step, and proceeding" >&2
fi

mkdir -p "$RELEASES_DIR"
exec {lock_fd}>"$LOCK_FILE"
if ! flock -w "$LOCK_WAIT_SECS" "$lock_fd"; then
  echo "could not acquire deploy lock on $LOCK_FILE within ${LOCK_WAIT_SECS}s -- a deploy build/swap is in progress against $LIVE_PATH; wait for it to finish rather than rolling back mid-flight" >&2
  exit 3
fi

current_target=""
if [ -L "$LIVE_PATH" ]; then
  current_target="$(readlink -f "$LIVE_PATH")"
fi

if [ "$current_target" = "$target" ]; then
  echo "$LIVE_PATH already points at $target -- nothing to do" >&2
  exit 0
fi

TMP_LINK="${LIVE_PATH}.rollback-$$"
ln -sfn "$target" "$TMP_LINK"
mv -T "$TMP_LINK" "$LIVE_PATH"
echo "rolled back: $LIVE_PATH -> $target (was $current_target)" >&2
