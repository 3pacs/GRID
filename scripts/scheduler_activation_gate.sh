#!/usr/bin/env bash
# Decides whether grid-scheduler activation may proceed, once
# activate_scheduler=true is already established (deploy.yml only invokes
# this from a step gated on that input).
#
# There is no automated "safe to restart" check here. An earlier version of
# deploy.yml tried to infer current idleness from the scheduler's own log
# tail (treating the most recent log line matching a known completion
# marker as a "verified boundary"), but a log can only show what has
# ALREADY happened -- it cannot prove nothing has started since that line
# was written. So this simply requires an explicit human acknowledgment of
# the interruption risk instead of claiming an automated guarantee it can't
# actually make.
#
# Usage: scheduler_activation_gate.sh <acknowledge_scheduler_interruption>
#   Exit 0 = proceed (acknowledgment given).
#   Exit 1 = refused (acknowledgment missing or not exactly "true").
set -euo pipefail

acknowledge="${1:-false}"

if [ "$acknowledge" != "true" ]; then
  echo "REFUSED: activating grid-scheduler requires acknowledge_scheduler_interruption=true." >&2
  echo "There is no automated safe-to-restart check -- a log tail cannot prove current idleness." >&2
  echo "If you have checked yourself (e.g. journalctl -u grid-scheduler) and accept that this" >&2
  echo "may interrupt an in-progress puller call or scheduled job, re-run with" >&2
  echo "acknowledge_scheduler_interruption=true." >&2
  exit 1
fi

echo "PROCEED: acknowledge_scheduler_interruption=true -- restarting grid-scheduler now, which will interrupt whatever is currently running."
exit 0
