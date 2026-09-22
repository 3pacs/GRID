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
# manual inspection or a manual rollback (repoint the symlink yourself);
# older ones are pruned. This script does not automate rollback -- that is a
# separate, explicit decision, not something a script should do unattended.
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
# Exit status: 0 if the candidate build succeeded and the swap completed: 1
# if the build_hook failed (live path untouched, candidate left for
# inspection at the path printed on stderr).

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

mkdir -p "$RELEASES_DIR"

# One-time, transparent migration: a plain directory at LIVE_PATH becomes a
# versioned release plus a symlink pointing at it. Skipped entirely once
# LIVE_PATH is already a symlink (every run after the first).
if [ -d "$LIVE_PATH" ] && [ ! -L "$LIVE_PATH" ]; then
  existing_label="pre-atomic-$(date -u +%Y%m%dT%H%M%SZ)"
  echo "migrating existing plain directory at $LIVE_PATH -> ${RELEASES_DIR}/${existing_label}" >&2
  mv -T "$LIVE_PATH" "${RELEASES_DIR}/${existing_label}"
  ln -sfn "${RELEASES_DIR}/${existing_label}" "$LIVE_PATH"
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
# half-updated $LIVE_PATH.
TMP_LINK="${LIVE_PATH}.new-$$"
ln -sfn "$CANDIDATE_DIR" "$TMP_LINK"
mv -T "$TMP_LINK" "$LIVE_PATH"
echo "swapped $LIVE_PATH -> $CANDIDATE_DIR" >&2

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
