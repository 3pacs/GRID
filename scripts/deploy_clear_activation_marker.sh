#!/usr/bin/env bash
# Clears <live_path>.releases/.activation-in-progress, but ONLY if it
# belongs to THIS deploy (its `label=` matches the given label exactly).
#
# Why the label check matters: with deploy.yml's deploy job now serialized
# by a fixed `concurrency:` group across every triggering ref (not scoped
# per-ref, since $DEPLOY_PATH is the same single resource regardless of
# which ref triggered a deploy), overlapping runs against the same
# <live_path> should not happen in the first place -- but a blind,
# unconditional `rm -f` here would still be wrong defense-in-depth: if a
# LATER deploy's swap has since overwritten the marker with a different
# label (a manual workflow_dispatch racing ahead of an earlier run's own
# cleanup step, for instance), that LATER deploy now owns the marker's
# lifecycle. This job blindly clearing it would incorrectly signal
# "activation done" for a release this job never touched, letting a manual
# rollback race that OTHER, still-in-flight deploy's restart/health-check
# steps -- exactly what the marker exists to prevent.
#
# Takes the same per-<live_path> lock as deploy_release_swap.sh /
# deploy_release_rollback.sh so this read-then-maybe-delete can't race a
# concurrent write to the same marker either.
#
# Usage: deploy_clear_activation_marker.sh <live_path> <label>
#
# Always exits 0 on a normal outcome (missing marker, or one belonging to a
# different label, are not errors -- there is nothing wrong to report in
# either case). Exits non-zero only on usage error or lock timeout.
#
# Exit status:
#   0  cleared (or nothing to clear, or it belonged to someone else)
#   2  usage error
#   3  could not acquire the per-<live_path> lock

set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <live_path> <label>" >&2
  exit 2
fi

LIVE_PATH="$1"
LABEL="$2"
RELEASES_DIR="${LIVE_PATH}.releases"
MARKER="${RELEASES_DIR}/.activation-in-progress"
LOCK_FILE="${RELEASES_DIR}/.lock"
LOCK_WAIT_SECS="${DEPLOY_LOCK_WAIT_SECS:-600}"

mkdir -p "$RELEASES_DIR"
exec {lock_fd}>"$LOCK_FILE"
if ! flock -w "$LOCK_WAIT_SECS" "$lock_fd"; then
  echo "could not acquire deploy lock on $LOCK_FILE within ${LOCK_WAIT_SECS}s -- leaving $MARKER untouched" >&2
  exit 3
fi

if [ ! -f "$MARKER" ]; then
  echo "$MARKER does not exist -- nothing to clear" >&2
  exit 0
fi

if grep -q "^label=${LABEL} " "$MARKER" 2>/dev/null; then
  rm -f "$MARKER"
  echo "cleared $MARKER (label=$LABEL)" >&2
else
  echo "not clearing $MARKER -- it belongs to a different deploy (this one is label=$LABEL): $(cat "$MARKER" 2>/dev/null)" >&2
fi
