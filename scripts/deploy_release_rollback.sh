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
# PREREQUISITE, enforced, not just documented: schema compatibility.
# --schema-compatible is an OPERATOR ACKNOWLEDGMENT that requires actual
# evidence gathered by a human -- it is NOT an automated compatibility
# check, and this script performs no such check itself (a symlink swap has
# no way to inspect schema compatibility). This script only repoints a
# symlink -- it CANNOT undo a committed database migration. If the deploy
# you're rolling back away from ran a migration that already committed, and
# that migration is not backward-compatible with the OLDER code, rolling
# back the files can break things exactly as badly as the failure you're
# trying to escape -- reverting files does not reverse a committed
# migration. Passing --schema-compatible is your claim that you checked
# this; the script trusts the flag, it does not verify the claim.
#
# Concurrency, and what it does NOT cover: this script takes the same
# per-<live_path> lock deploy_release_swap.sh does, so it can never run
# WHILE that script's own build+swap is in progress -- and every
# time-sensitive check below (does a rollback target still exist, is an
# activation still in progress) is performed AFTER acquiring that lock, not
# before, so nothing can change out from under the decision in the gap
# between checking and acting (an unlocked check could otherwise see a
# stale answer if a concurrent swap wrote a new marker or pruned the exact
# directory being rolled back to, in between).
#
# But the lock is released the moment deploy_release_swap.sh exits --
# deploy.yml's subsequent restart and health-verification steps for that
# release run AFTER the swap script has already exited, as separate steps
# holding no lock at all. To avoid racing THAT window -- rolling back while
# grid-api/grid-hermes are still being restarted or health-checked against
# the release this script is about to swap away from -- this script also
# checks <live_path>.releases/.activation-in-progress, written by
# deploy_release_swap.sh on every successful swap and cleared by
# deploy.yml's own last step (scripts/deploy_clear_activation_marker.sh,
# `if: always()`) once THAT deploy's activation is done.
#
# If that marker is present, this script REFUSES, period -- there is no
# staleness-based auto-proceed. Its age is reported for context, but age
# alone never decides anything: a marker being old is not proof the job
# actually crashed, only a guess. The only way past this refusal is
# --override-stuck-activation: a deliberate, SEPARATE acknowledgment from
# an operator who has positively confirmed (e.g. checked the Actions tab)
# that no deploy is actually running -- never inferred from elapsed time.
#
# Usage: deploy_release_rollback.sh <live_path> --schema-compatible [--override-stuck-activation]
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
#   6  refused: <live_path>.releases/.activation-in-progress is present and
#      --override-stuck-activation was not passed

set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <live_path> --schema-compatible [--override-stuck-activation]" >&2
  exit 2
fi

LIVE_PATH="$1"
shift

SCHEMA_ACK="false"
OVERRIDE_STUCK="false"
for arg in "$@"; do
  case "$arg" in
    --schema-compatible) SCHEMA_ACK="true" ;;
    --override-stuck-activation) OVERRIDE_STUCK="true" ;;
    *)
      echo "usage: $0 <live_path> --schema-compatible [--override-stuck-activation]" >&2
      echo "unrecognized argument: $arg" >&2
      exit 2
      ;;
  esac
done

if [ "$SCHEMA_ACK" != "true" ]; then
  cat >&2 << 'MSG'
refusing: --schema-compatible was not passed.

This flag is an OPERATOR ACKNOWLEDGMENT requiring actual evidence -- it is
NOT an automated compatibility check, and this script performs no such
check itself (there is no way to verify schema compatibility from a
symlink swap alone). This script only repoints a symlink; it CANNOT undo a
committed database migration. If the deploy you are rolling back away from
ran a migration that already committed, and that migration is not
backward-compatible with the OLDER code, rolling back the files can break
things exactly as badly as the failure you are trying to escape from --
reverting files does not reverse a committed migration.

Before passing --schema-compatible, gather actual evidence, e.g.:
  - check whether the deploy you're rolling back from ran any migration at all
  - if it did, check that migration's compatibility with the OLDER release's
    code (e.g. `alembic current` against what the older release expects)
  - when in doubt, ask before assuming a plain code rollback is safe

Passing this flag is your claim that you did this, not a system-verified
fact -- this script trusts it, it does not check it.

usage: deploy_release_rollback.sh <live_path> --schema-compatible [--override-stuck-activation]
MSG
  exit 5
fi

RELEASES_DIR="${LIVE_PATH}.releases"
MARKER="${RELEASES_DIR}/.previous-release"
INPROGRESS_MARKER="${RELEASES_DIR}/.activation-in-progress"
LOCK_FILE="${RELEASES_DIR}/.lock"
LOCK_WAIT_SECS="${DEPLOY_LOCK_WAIT_SECS:-600}"

mkdir -p "$RELEASES_DIR"
exec {lock_fd}>"$LOCK_FILE"
if ! flock -w "$LOCK_WAIT_SECS" "$lock_fd"; then
  echo "could not acquire deploy lock on $LOCK_FILE within ${LOCK_WAIT_SECS}s -- a deploy build/swap is in progress against $LIVE_PATH; wait for it to finish rather than rolling back mid-flight" >&2
  exit 3
fi

# Everything below is checked while HOLDING the lock, not before -- see
# header for why (closes the gap between an unlocked check and the action
# it justified).

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
  marker_contents="$(cat "$INPROGRESS_MARKER" 2>/dev/null || true)"
  if [ "$OVERRIDE_STUCK" != "true" ]; then
    cat >&2 << MSG
refusing: $INPROGRESS_MARKER is present (${age}s old: ${marker_contents}).

This means deploy.yml's restart/health-verification steps for that release
may not have finished -- its own cleanup step, which removes this marker
once THAT deploy is done, is the only thing that clears it under normal
operation. Rolling back now could race that still-in-flight activation.

Age alone is never a reason to proceed -- an old marker is not proof the
job crashed, only a guess. If you have POSITIVELY CONFIRMED no deploy is
actually running against $LIVE_PATH right now (checked the Actions tab; or
the job crashed outright and its cleanup step never ran), you may override
this refusal with --override-stuck-activation. That is a deliberate,
separate acknowledgment, not something this script infers for you.
MSG
    exit 6
  fi
  echo "proceeding past $INPROGRESS_MARKER (${age}s old) -- --override-stuck-activation was passed" >&2
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
