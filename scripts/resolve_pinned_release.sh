#!/usr/bin/env bash
# Resolve the mutable release link to the exact release directory a unit
# should be pinned to, and refuse unless it is the reviewed commit.
#
# Used by .github/workflows/deploy.yml before writing a systemd drop-in, so a
# later Restart=always recovery cannot land on newer code that nobody chose.
# Tested in tests/test_resolve_pinned_release.py.
#
# Usage: resolve_pinned_release.sh <deploy_path> <expected_full_sha>
#   Prints the resolved release directory on stdout and exits 0, or prints a
#   reason on stderr and exits non-zero. Reads only; changes nothing.
set -euo pipefail

DEPLOY_PATH="${1:?usage: resolve_pinned_release.sh <deploy_path> <expected_full_sha>}"
EXPECTED_SHA="${2:?usage: resolve_pinned_release.sh <deploy_path> <expected_full_sha>}"

if ! [[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "REFUSED: expected sha must be a full 40-character lowercase hex sha" >&2
  exit 2
fi

RELEASE="$(readlink -f -- "$DEPLOY_PATH")" || { echo "REFUSED: cannot resolve $DEPLOY_PATH" >&2; exit 3; }
RELEASES_ROOT="$(readlink -f -- "${DEPLOY_PATH}.releases")" || { echo "REFUSED: cannot resolve ${DEPLOY_PATH}.releases" >&2; exit 3; }

if [ ! -d "$RELEASE" ]; then
  echo "REFUSED: $RELEASE is not a directory" >&2
  exit 4
fi
if [ ! -d "$RELEASES_ROOT" ] || [ "$(dirname -- "$RELEASE")" != "$RELEASES_ROOT" ]; then
  echo "REFUSED: $RELEASE is not directly under $RELEASES_ROOT" >&2
  exit 5
fi
HEAD_SHA="$(git -C "$RELEASE" rev-parse HEAD 2>/dev/null)" || { echo "REFUSED: $RELEASE is not a git checkout" >&2; exit 6; }
if [ "$HEAD_SHA" != "$EXPECTED_SHA" ]; then
  echo "REFUSED: $RELEASE is at $HEAD_SHA, expected $EXPECTED_SHA" >&2
  exit 7
fi

printf '%s\n' "$RELEASE"
