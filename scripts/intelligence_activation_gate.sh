#!/usr/bin/env bash
# Decides whether grid-intelligence activation may proceed, once
# activate_intelligence=true is already established (deploy.yml only
# invokes this from a step gated on that input).
#
# Mirrors scripts/scheduler_activation_gate.sh exactly, for the same
# reason: there is no automated "safe to restart" check. grid-intelligence
# runs ~25 schedule-library jobs (intelligence/scheduler.py) ranging from
# every 15 minutes to weekly. A log tail can only show what has ALREADY
# happened -- it cannot prove nothing is mid-run right now, and it cannot
# reveal that the schedule library itself has no catch-up mechanism: a
# restart landing near a daily or weekly job's exact trigger computes its
# next run from the restart moment, silently skipping that occurrence
# until the job's next natural recurrence (up to a week later for weekly
# jobs) rather than deferring it. So this requires an explicit human
# acknowledgment instead of claiming an automated guarantee it can't make.
#
# Usage: intelligence_activation_gate.sh <acknowledge_intelligence_interruption>
#   Exit 0 = proceed (acknowledgment given).
#   Exit 1 = refused (acknowledgment missing or not exactly "true").
set -euo pipefail

acknowledge="${1:-false}"

if [ "$acknowledge" != "true" ]; then
  echo "REFUSED: activating grid-intelligence requires acknowledge_intelligence_interruption=true." >&2
  echo "There is no automated safe-to-restart check -- a log tail cannot prove current idleness," >&2
  echo "and the schedule library has no catch-up mechanism: a restart near a daily/weekly job's" >&2
  echo "exact trigger time silently skips that occurrence until its next natural recurrence" >&2
  echo "rather than deferring it. If you have checked yourself (e.g. journalctl -u grid-intelligence)" >&2
  echo "and accept that risk, re-run with acknowledge_intelligence_interruption=true." >&2
  exit 1
fi

echo "PROCEED: acknowledge_intelligence_interruption=true -- restarting grid-intelligence now, which may skip whatever job is currently running or due imminently."
exit 0
