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
# never observes a missing or half-updated directory), and takes the same
# per-<live_path> lock so it can never race an in-flight deploy.
#
# What this does NOT do: touch the database. An atomic file-path swap cannot
# undo a committed migration. Rolling back the code without also considering
# schema compatibility can be exactly as dangerous as rolling forward
# incompatibly -- this script does the file-path half only, deliberately.
#
# Usage: deploy_release_rollback.sh <live_path>
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
#   3  could not acquire the per-<live_path> lock -- a deploy is in progress

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <live_path>" >&2
  exit 2
fi

LIVE_PATH="$1"
RELEASES_DIR="${LIVE_PATH}.releases"
MARKER="${RELEASES_DIR}/.previous-release"
LOCK_FILE="${RELEASES_DIR}/.lock"
LOCK_WAIT_SECS="${DEPLOY_LOCK_WAIT_SECS:-600}"

if [ ! -f "$MARKER" ]; then
  echo "no $MARKER -- nothing recorded to roll back to (has a successful deploy_release_swap.sh run ever completed against $LIVE_PATH?)" >&2
  exit 1
fi

target="$(cat "$MARKER")"
if [ ! -d "$target" ]; then
  echo "recorded previous release '$target' no longer exists (already pruned?) -- refusing to roll back to it" >&2
  exit 1
fi

mkdir -p "$RELEASES_DIR"
exec {lock_fd}>"$LOCK_FILE"
if ! flock -w "$LOCK_WAIT_SECS" "$lock_fd"; then
  echo "could not acquire deploy lock on $LOCK_FILE within ${LOCK_WAIT_SECS}s -- a deploy is in progress against $LIVE_PATH; wait for it to finish rather than rolling back mid-flight" >&2
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
