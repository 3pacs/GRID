#!/usr/bin/env bash
# Decides whether grid-realtime activation (restart onto the release path)
# may proceed, once activate_realtime=true is already established (deploy.yml
# only invokes this from a step gated on that input).
#
# grid-realtime's restart itself is cheap and well-understood (SIGTERM ->
# graceful shutdown flush -> systemd restart, the same bounded event the
# daemon already tolerates on every unplanned crash). The reason this gate
# exists is narrower and different from the restart mechanics: this is the
# FIRST TIME this daemon will ever be restarted by this pipeline (confirmed
# 2026-09-16: its process had been running since 2026-07-29 with zero
# restarts), and its EXISTING persistence semantics -- unchanged by this
# deployment repair -- are `INSERT ... ON CONFLICT (symbol, interval, ts)
# DO NOTHING` (ingestion/realtime/flusher.py). "Idempotent" (no duplicate or
# corrupt row) is true of that; "the most complete candle wins" is NOT --
# whichever row lands FIRST at a given bucket's primary key wins permanently,
# and it is provably the truncated one, since the old process's shutdown
# flush always completes (successfully or not) strictly before a new
# process's candle for the same bucket even starts. Proven against a real
# Postgres, not merely asserted, in
# tests/test_realtime_shutdown_semantics.py::
# test_truncated_candle_blocks_a_later_complete_candle_for_the_same_bucket.
#
# This repair does NOT change that persistence semantics -- a source-aware
# merge was explored and then reverted out of the same PR after review found
# real correctness gaps in it (see docs/TODO-REALTIME-CANDLE-CORRECTNESS.md
# and docs/realtime_candle_merge_proposal_tests.py for that draft and what a
# real fix requires). So this restart carries the SAME truncation risk any
# unplanned crash-and-restart already carries today -- not a new risk this
# PR introduces, but a real, bounded one (one bucket per symbol per restart)
# that this PR does not resolve either. Combined with this being the first
# ever automated restart of a daemon that has otherwise run untouched for
# months, that calls for the same explicit human acknowledgment
# grid-scheduler's gate requires, rather than treating restart as
# risk-free just because Restart=always already makes crashes routine.
#
# Usage: realtime_activation_gate.sh <acknowledge_realtime_interruption>
#   Exit 0 = proceed (acknowledgment given).
#   Exit 1 = refused (acknowledgment missing or not exactly "true").
set -euo pipefail

acknowledge="${1:-false}"

if [ "$acknowledge" != "true" ]; then
  echo "REFUSED: activating grid-realtime requires acknowledge_realtime_interruption=true." >&2
  echo "grid-realtime's candle persistence is ON CONFLICT DO NOTHING (unchanged by this repair)" >&2
  echo "-- whichever candle for a given bucket lands first wins permanently, and a restart" >&2
  echo "always makes the OLD, truncated one land first (see docs/TODO-REALTIME-CANDLE-CORRECTNESS.md" >&2
  echo "and tests/test_realtime_shutdown_semantics.py for the proof against real Postgres)." >&2
  echo "If you accept that the bucket straddling this restart may end up with a truncated" >&2
  echo "candle for the affected symbol(s), re-run with acknowledge_realtime_interruption=true." >&2
  exit 1
fi

echo "PROCEED: acknowledge_realtime_interruption=true -- restarting grid-realtime now, accepting the known DO-NOTHING candle-truncation risk at the restart boundary."
exit 0
