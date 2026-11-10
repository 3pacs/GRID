"""SPY native one-minute volume for the GEX page, from Schwab ``/pricehistory``.

The GEX page withholds VWAP, relative volume and volume spikes because its
Yahoo one-minute series carries cumulative-sized "minutes" (15-25M shares in
one bar against a ~100k norm), and the median replacement it used is an
estimate, not trades. gex-watch already validates a private snapshot file for
this (``grid_native_volume.read_private_snapshot``, gex repo d39561e, off
unless ``GEX_NATIVE_VOLUME_SNAPSHOT`` names the file); nothing produced it.
This module computes that payload from Schwab's own one-minute bars.

Payload (``grid.native-volume.v1``), regular session only, completed bars only:

* ``cumulative_shares``: sum of today's completed 09:30-16:00 ET bar volumes.
* ``bar_vwap_estimate``: sum(HLC3 x volume) / sum(volume) over those bars. A
  bar estimate, never exact trade VWAP (``exact_trade_vwap`` is False).
* ``rvol``: today's cumulative through the last completed bar over the median
  cumulative through the same minute of the previous ``BASELINE_SESSIONS``
  sessions in the same response (sessions are the dates that have regular
  bars; ``calendar_version`` says so).

Reconciliation, the gate for ``status: ready``. Schwab's quote ``totalVolume``
is an independent day counter. Today's bars (all of them, extended hours
included, since the counter covers the whole day) must agree with it within
``RECONCILE_TOLERANCE``. Otherwise the payload is ``unreconciled`` and
gex-watch keeps withholding the numbers. Outside the regular session, with too
few baseline sessions, or with no completed bar the status is not ``ready``
either. Signals never read this (``signal_confirmation`` is False).
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import date, datetime, time, timezone
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo

SCHEMA = "grid.native-volume.v1"
ENVELOPE_SCHEMA = "grid.native-volume.snapshot.v1"
SYMBOL = "SPY"
SOURCE = "schwab_pricehistory"
CALENDAR_VERSION = "schwab-regular-bars-observed-v1"
BASELINE_SESSIONS = 10
HISTORY_CALENDAR_DAYS = 21          # >= 10 prior sessions plus holidays
RECONCILE_TOLERANCE = 0.02          # |bars - quote totalVolume| / totalVolume
BAR_SECONDS = 60
ET = ZoneInfo("America/New_York")
OPEN, CLOSE = time(9, 30), time(16, 0)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def _bars(body: Mapping[str, Any]) -> list[tuple[float, float, float, float, int]]:
    """(start_ts, high, low, close, volume) for well-formed candles, oldest first."""
    out = []
    for c in (body or {}).get("candles") or []:
        if not isinstance(c, Mapping):
            continue
        try:
            t = float(c["datetime"]) / 1000.0
            h, lo, cl = float(c["high"]), float(c["low"]), float(c["close"])
            v = c["volume"]
        except (KeyError, TypeError, ValueError):
            continue
        if type(v) is not int or v < 0 or not all(math.isfinite(x) for x in (t, h, lo, cl)):
            continue
        out.append((t, h, lo, cl, v))
    out.sort()
    return out


def _regular(ts: float) -> bool:
    d = datetime.fromtimestamp(ts, ET)
    return OPEN <= d.time() < CLOSE


def _session(ts: float) -> date:
    return datetime.fromtimestamp(ts, ET).date()


def _minute_of_session(ts: float) -> int:
    d = datetime.fromtimestamp(ts, ET)
    return (d.hour * 60 + d.minute) - (OPEN.hour * 60 + OPEN.minute)


def _base(now: float, status: str, reason: str | None, **extra: Any) -> dict[str, Any]:
    return {"schema": SCHEMA, "symbol": SYMBOL, "source": SOURCE, "status": status, "reason": reason,
            "historical_request": False, "signal_confirmation": False, "exact_trade_vwap": False,
            "price_basis": "provider_unspecified", "calendar_version": CALENDAR_VERSION,
            "served_at": _iso(now), **extra}


def compute(history: Mapping[str, Any], quote_total_volume: int | None, *, now: float,
            receipt_at: float) -> dict[str, Any]:
    """Pure: Schwab pricehistory body + quote ``totalVolume`` -> payload."""
    bars = _bars(history)
    today = _session(now)
    if not _regular(now):
        return _base(now, "closed", "outside_regular_session")
    completed = [b for b in bars if b[0] + BAR_SECONDS <= now]
    today_all = [b for b in completed if _session(b[0]) == today]
    today_reg = [b for b in today_all if _regular(b[0])]
    if not today_reg:
        return _base(now, "unavailable", "no_completed_regular_bar")
    last = today_reg[-1][0]
    through = _minute_of_session(last)
    cum = sum(b[4] for b in today_reg)
    if cum <= 0:
        return _base(now, "unavailable", "zero_volume")
    vwap = sum((b[1] + b[2] + b[3]) / 3.0 * b[4] for b in today_reg) / cum

    prior: dict[date, int] = {}
    full: set[date] = set()
    for b in bars:
        s = _session(b[0])
        if s >= today or not _regular(b[0]):
            continue
        m = _minute_of_session(b[0])
        if m <= through:
            prior[s] = prior.get(s, 0) + b[4]
        if m >= through:
            full.add(s)   # the session reached this minute, so its cumulative is comparable
    sessions = sorted(s for s in prior if s in full)[-BASELINE_SESSIONS:]
    extra = {"as_of": _iso(last + BAR_SECONDS), "end_of_completed_window": _iso(last + BAR_SECONDS),
             "latest_receipt_at": _iso(receipt_at), "cumulative_shares": cum,
             "bar_vwap_estimate": round(vwap, 4), "baseline_sessions": [s.isoformat() for s in sessions]}
    if len(sessions) < BASELINE_SESSIONS:
        return _base(now, "unavailable", "insufficient_baseline_sessions", **extra)
    base = median(prior[s] for s in sessions)
    extra["rvol"] = round(cum / base, 4) if base > 0 else 0.0

    day_bars = sum(b[4] for b in today_all)
    extra["reconciliation"] = {"quote_total_volume": quote_total_volume, "bars_total_volume": day_bars,
                               "tolerance": RECONCILE_TOLERANCE}
    if type(quote_total_volume) is not int or quote_total_volume <= 0:
        return _base(now, "unreconciled", "quote_total_volume_unavailable", **extra)
    diff = abs(quote_total_volume - day_bars) / quote_total_volume
    extra["reconciliation"]["relative_difference"] = round(diff, 6)
    if diff > RECONCILE_TOLERANCE:
        return _base(now, "unreconciled", "bars_disagree_with_quote_total", **extra)
    return _base(now, "ready", None, **extra)


def envelope(payload: Mapping[str, Any], *, now: float) -> dict[str, Any]:
    """The file gex-watch reads: payload plus its canonical sha256."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return {"schema": ENVELOPE_SCHEMA, "generated_at": _iso(now), "payload": dict(payload),
            "payload_sha256": hashlib.sha256(canonical.encode()).hexdigest()}


def history_params(now: float) -> dict[str, Any]:
    """Query for Schwab /pricehistory: explicit range so 10 prior sessions fit."""
    end = int(now * 1000)
    start = int((now - HISTORY_CALENDAR_DAYS * 86400) * 1000)
    return {"frequencyType": "minute", "frequency": 1, "startDate": start, "endDate": end,
            "needExtendedHoursData": "true"}
