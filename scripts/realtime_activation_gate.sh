#!/usr/bin/env bash
# Decides whether grid-realtime activation (restart onto the release path)
# may proceed, once activate_realtime=true is already established (deploy.yml
# only invokes this from a step gated on that input).
#
# grid-realtime's restart itself is cheap and well-understood (SIGTERM ->
# graceful shutdown flush -> systemd restart, the same bounded event the
# daemon already tolerates on every unplanned crash). The reason this gate
# exists is narrower and different from the restart mechanics: the candle
# merge SQL (INSERT_SQL, ingestion/realtime/flusher.py) that reconciles a
# truncated pre-restart candle with the post-restart continuation has known,
# accepted correctness gaps, traced and tested against a real Postgres in
# tests/test_realtime_shutdown_semantics.py rather than merely asserted:
#
#   - close is resolved by "last write wins" (whichever side's row lands at
#     the DB second). That is correct for the only reachable production
#     ordering (a restart's old-segment flush provably completes, or fails,
#     strictly before the new process's first write is even possible) -- but
#     Binance's parser never persists trade IDs or per-trade timestamps past
#     aggregation (confirmed by reading feeds/binance.py and
#     candle_builder.py, not assumed), so there is no way to verify true
#     chronological order independent of write order, or to correctly
#     resolve a genuinely out-of-order delivery if one ever occurred.
#   - Yahoo's volume/trade_count merge uses GREATEST, which never inflates
#     but also never reconstructs a bucket whose pre- and post-restart
#     segments cover genuinely different, non-overlapping minutes -- it
#     silently keeps only the larger of the two sides' own totals, not their
#     true union.
#
# See docs/TODO-REALTIME-CANDLE-CORRECTNESS.md for the full analysis and the
# larger change (capturing trade-level identity/order, or per-minute keys
# for Yahoo) that would actually close these gaps. Until that lands, a
# restart during active trading can produce one candle -- the bucket
# straddling the restart, for the affected symbol(s) -- with an incorrect
# close price or an under-reported volume/trade_count. That is a narrow,
# bounded blast radius (one bucket per symbol per restart), not a reason to
# never restart, but it is real and not yet fixed, so this requires the same
# explicit human acknowledgment grid-scheduler's gate does, rather than
# treating restart safety as fully established.
#
# Usage: realtime_activation_gate.sh <acknowledge_realtime_interruption>
#   Exit 0 = proceed (acknowledgment given).
#   Exit 1 = refused (acknowledgment missing or not exactly "true").
set -euo pipefail

acknowledge="${1:-false}"

if [ "$acknowledge" != "true" ]; then
  echo "REFUSED: activating grid-realtime requires acknowledge_realtime_interruption=true." >&2
  echo "The candle merge across a restart has known, tested-but-unresolved correctness gaps" >&2
  echo "(see docs/TODO-REALTIME-CANDLE-CORRECTNESS.md) -- close-selection and Yahoo volume" >&2
  echo "reconstruction across the restart boundary are not proven correct in every case." >&2
  echo "If you accept that the bucket straddling this restart may end up with an incorrect" >&2
  echo "close or an under-reported volume/trade_count for the affected symbol(s), re-run with" >&2
  echo "acknowledge_realtime_interruption=true." >&2
  exit 1
fi

echo "PROCEED: acknowledge_realtime_interruption=true -- restarting grid-realtime now, accepting the known candle-merge correctness gaps at the restart boundary."
exit 0
