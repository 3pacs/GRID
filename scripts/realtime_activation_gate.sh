#!/usr/bin/env bash
# Decides whether grid-realtime activation may proceed, once
# activate_realtime=true is already established (deploy.yml only invokes
# this from a step gated on that input).
#
# Same reasoning as scripts/scheduler_activation_gate.sh: there is no
# automated "safe to restart" check. A log tail can only show what has
# ALREADY happened -- it cannot prove no candle is mid-interval or that the
# Binance WebSocket session isn't mid-message right now. So this simply
# requires an explicit human acknowledgment of the interruption risk instead
# of claiming an automated guarantee it can't actually make.
#
# Usage: realtime_activation_gate.sh <acknowledge_realtime_interruption>
#   Exit 0 = proceed (acknowledgment given).
#   Exit 1 = refused (acknowledgment missing or not exactly "true").
set -euo pipefail

acknowledge="${1:-false}"

if [ "$acknowledge" != "true" ]; then
  echo "REFUSED: activating grid-realtime requires acknowledge_realtime_interruption=true." >&2
  echo "There is no automated safe-to-restart check -- a log tail cannot prove no candle is" >&2
  echo "mid-interval or that the Binance WS session isn't mid-message right now." >&2
  echo "If you have checked yourself and accept that this drops the live Binance WS session" >&2
  echo "(it auto-reconnects) and force-closes every symbol's in-progress candle bar early" >&2
  echo "(written truncated via flush_all(), not lost), re-run with" >&2
  echo "acknowledge_realtime_interruption=true." >&2
  exit 1
fi

echo "PROCEED: acknowledge_realtime_interruption=true -- restarting grid-realtime now, which will drop the live Binance WS session and truncate every symbol's in-progress candle."
exit 0
