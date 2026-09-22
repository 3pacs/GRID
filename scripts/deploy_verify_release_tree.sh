#!/usr/bin/env bash
# Verifies that a running process is genuinely serving GRID's deployed
# release -- not merely alive, and not merely pointed at SOME release
# directory.
#
# Found 2026-09-22 (#598's first real activation of deploy_release_swap.sh's
# symlink pattern, run 35693333707): the grid-api verify step compared a
# process's raw /proc/<pid>/cwd reading against the LITERAL, unresolved
# $DEPLOY_PATH string. /proc/<pid>/cwd always reports a process's cwd as the
# fully-resolved real path -- Linux never reports it as a symlink -- so once
# $DEPLOY_PATH became a symlink (permanent from that first swap onward), that
# comparison could never succeed again. It failed on a genuinely healthy
# grid-api running the correct new code; every step after it (grid-hermes
# restart included) was skipped by deploy.yml's own control flow, not
# retried. Not a flake -- structural, and permanent until fixed.
#
# Two independent checks here, both required, neither sufficient alone:
#
#   1. The process's cwd, resolved, equals deploy_path's resolved target.
#      Comparing resolved paths on both sides (not the literal deploy_path
#      string) is what the grid-api step was missing; grid-hermes/-realtime/
#      -scheduler/-intelligence's own verify steps already did this
#      correctly. This alone only proves the process is running FROM the
#      live release target -- it says nothing about WHICH commit that
#      target actually is. A symlink quietly left pointing at a stale or
#      otherwise-wrong release directory would satisfy it just as happily.
#
#   2. The resolved target's own git history, read directly and
#      independently right now -- not inferred from
#      deploy_build_hook.sh's in-build ancestry check, which only proved
#      this BEFORE the swap, the restart, and this fresh process existed --
#      has expected_sha as an ancestor of its HEAD. Ancestor, not exact
#      equality: deliberately mirrors deploy_build_hook.sh's own semantics
#      (git_merge-base --is-ancestor "$EXPECTED_SHA" HEAD after a fresh
#      `git fetch main`), since a legitimate later push landing on main
#      between this run's checkout and deploy_release_swap.sh's own fetch
#      can legitimately carry the deployed candidate PAST expected_sha, not
#      merely to it -- that is not a failure this check should raise.
#
# Usage: deploy_verify_release_tree.sh <deploy_path> <pid> <expected_sha>
#
# Exit status:
#   0  pid's cwd resolves to deploy_path's resolved target, and that
#      target's HEAD has expected_sha as an ancestor
#   1  verification failed -- see the message on stderr for which check
#   2  usage error

set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <deploy_path> <pid> <expected_sha>" >&2
  exit 2
fi

DEPLOY_PATH="$1"
PID="$2"
EXPECTED_SHA="$3"

fail() { echo "::error::deploy_verify_release_tree: $*" >&2; exit 1; }

cwd="$(sudo -n readlink "/proc/${PID}/cwd" 2>/dev/null || readlink "/proc/${PID}/cwd" 2>/dev/null || true)"
echo "pid=$PID cwd=$cwd"
[ -n "$cwd" ] || fail "could not read cwd for pid $PID (process gone, or /proc/$PID/cwd unreadable)"

want="$(readlink -f "$DEPLOY_PATH")"
[ -n "$want" ] || fail "could not resolve $DEPLOY_PATH"

cwd_resolved="$(readlink -f "$cwd")"
[ "$cwd_resolved" = "$want" ] || fail "cwd '$cwd' (resolved: $cwd_resolved) does not resolve to the live release target $want -- process is running a different tree"

deployed_sha="$(git -C "$want" rev-parse HEAD 2>/dev/null)" || fail "could not read a git HEAD commit inside $want -- is it a real checkout?"
echo "resolved live target: $want (commit $deployed_sha)"

git -C "$want" merge-base --is-ancestor "$EXPECTED_SHA" HEAD 2>/dev/null \
  || fail "live release target $want is at commit $deployed_sha, which does not have expected commit $EXPECTED_SHA as an ancestor"

echo "verify: pid=$PID is running the deployed tree at $want (commit $deployed_sha, includes expected $EXPECTED_SHA)"
