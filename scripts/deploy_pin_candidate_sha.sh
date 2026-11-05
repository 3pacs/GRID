#!/usr/bin/env bash
# Pins a build candidate directory to the EXACT deployment SHA, regardless
# of how far `main` has advanced by the time this runs.
#
# Found 2026-09-22, alongside #599's release-identity fix
# (scripts/deploy_verify_release_tree.sh): the original "Build release tree"
# logic fetched the floating `main` ref (`git fetch $SRC_URL main; git reset
# --hard FETCH_HEAD`) and only checked `merge-base --is-ancestor
# $EXPECTED_SHA HEAD` -- a deliberate tolerance for a later push landing on
# `main` between this run's own checkout and this fetch. That tolerance
# meant the CANDIDATE actually built (and, if the build succeeded, actually
# swapped live and restarted onto -- see deploy_release_swap.sh) could be a
# later commit than the one this specific run was supposed to deploy.
# Harmless on its own, but inconsistent with deploy_verify_release_tree.sh's
# exact-SHA-match requirement: that race would now correctly FAIL the
# post-deploy verify step instead of silently succeeding on the wrong
# commit -- a real, if rare, source of deploy failures this closes instead
# of just documenting.
#
# Fetching the exact SHA directly (not a branch ref) and resetting to it
# closes this STRUCTURALLY, not probabilistically: `main` advancing at any
# point -- before, during, or after this fetch -- cannot change what commit
# gets fetched, because the fetch itself names the commit, not a moving
# pointer. There is no timing window to lose a race in, unlike the old
# fetch-a-branch approach. Proven in
# tests/deploy/test_deploy_pin_candidate_sha.sh by advancing a real origin's
# `main` to a later real commit BEFORE invoking this script, then confirming
# the candidate lands on the exact original commit regardless -- both in
# isolation and plugged into the real deploy_release_swap.sh, proving the
# eventual ACTIVATED (live-swapped) content is unaffected too, not just the
# raw candidate directory.
#
# Deliberately extracted into its own script (not left inline in
# deploy_build_hook.sh) so this specific, isolated property -- what commit
# does the candidate end up at -- can be tested directly, without needing a
# working pip/npm/alembic toolchain just to exercise a git fetch.
#
# Usage: deploy_pin_candidate_sha.sh <candidate_dir> <src_url> <expected_sha>
#
# Exit status:
#   0  candidate_dir's HEAD is exactly expected_sha
#   1  fetch/reset failed, or the resulting HEAD is not exactly expected_sha
#   2  usage error

set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <candidate_dir> <src_url> <expected_sha>" >&2
  exit 2
fi

CANDIDATE_DIR="$1"
SRC_URL="$2"
EXPECTED_SHA="$3"

cd "$CANDIDATE_DIR"
echo "before: $(git log --oneline -1 2>/dev/null || echo '<no commit yet>')"
git fetch "$SRC_URL" "$EXPECTED_SHA"
git reset --hard "$EXPECTED_SHA"
echo "after:  $(git log --oneline -1)"

actual_sha="$(git rev-parse HEAD)"
[ "$actual_sha" = "$EXPECTED_SHA" ] || {
  echo "::error::candidate release tree is at $actual_sha, not the exact expected commit $EXPECTED_SHA"
  exit 1
}

echo "pin: candidate is exactly $EXPECTED_SHA"
