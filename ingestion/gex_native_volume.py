"""SPY native one-minute volume for the GEX page, from Schwab ``/pricehistory``.

The GEX page withholds VWAP, relative volume and volume spikes because its
Yahoo one-minute series carries cumulative-sized "minutes" (15-25M shares in
one bar against a ~100k norm), and the median replacement it used is an
estimate, not trades. gex-watch's private reader
(``grid_native_volume.read_private_snapshot``, gex repo, hardened 2026-11-11
in 897741a) accepts an exact ``grid.native-volume.snapshot.v1`` file and
nothing else. This module produces that file's payload from Schwab's own
one-minute bars; the field set, fixed basis strings and clock rules below are
the reader's.

Payload (``grid.native-volume.v1``), regular session only, completed bars only:

* ``cumulative_shares``: sum of today's completed 09:30-16:00 ET bar volumes.
  A bar counts as completed once ``RECEIPT_GUARD_S`` seconds past its end at
  receipt (the reader requires ``latest_receipt_at >= end + 5``).
* ``bar_vwap_estimate``: sum(HLC3 x volume) / sum(volume) over those bars, never
  exact trade VWAP (``exact_trade_vwap`` False).
* ``rvol``: ``cumulative_shares / baseline_median_shares`` exactly, where the
  baseline is the median cumulative through the same minute of the previous
  ``BASELINE_SESSIONS`` sessions in the same response (``calendar_version``
  says sessions are the dates that have regular bars).
* ``as_of`` = ``served_at`` = the envelope's ``generated_at``; the receipt and
  event ages are derived from those strings, as the reader checks them.

Scope gate, ours, kept out of the payload (whose field set is fixed). Schwab's
quote ``totalVolume`` is an independent day counter, and the bars do NOT sum to
it: they carry a stable ~81-88% of it (2026-11-10: 12 sessions, 6,156
regular-hours ToS RTD readings, median gap 12.6-19.2% per session; live
2026-11-11 regular hours: 16.6-17.0%). ``cross_source_reconciled`` is
therefore False, and the payload is ``ready`` only when the gap is inside
``SCOPE_GAP_MIN``..``SCOPE_GAP_MAX``: bars at or above the counter mean
phantom or duplicated minutes (Yahoo's failure), far below it missing minutes.
The gap is returned beside the payload for the operator log. Outside the
regular session, without a full baseline, with no completed bar, or with bars
older than the reader's 120 s limit, the status is not ``ready`` either.
Validation and coverage come first: the response must be SPY's with no
duplicate minute, and every regular minute 09:30..last completed must be present
today and in each of the exact prior ten sessions (else ``failed`` /
``coverage_failed``; the scope gate cannot see a single missing or doubled
minute). Sessions are the dates that have regular bars, not an exchange
calendar: a wholly missing session is not detected as such, and the afternoons
after an early close fail coverage until it leaves the ten. Signals never read
this (``signal_confirmation`` False).
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
# Must match gex-watch grid_native_volume (VWAP_BASIS, RVOL_BASIS, INTEGRITY_SCOPE,
# and this producer's entry in its accepted producer set).
PRODUCER = "GRID Schwab pricehistory snapshot"
VWAP_BASIS = "Volume-weighted HLC3 of native one-minute OHLCV; not trade VWAP"
RVOL_BASIS = "Cumulative shares / median same-elapsed shares of exact prior ten sessions"
INTEGRITY_SCOPE = "content integrity only; trust requires verified local producer and owner-only custody"
BASELINE_SESSIONS = 10
HISTORY_CALENDAR_DAYS = 21          # >= 10 prior sessions plus holidays
SCOPE_GAP_MIN = 0.05                # (quote totalVolume - bars) / totalVolume, see the module docstring
SCOPE_GAP_MAX = 0.30
BAR_SECONDS = 60
RECEIPT_GUARD_S = 5                 # reader: latest_receipt_at >= end_of_completed_window + 5
MAX_AGE_S = 120                     # reader: receipt and event ages within 120 s of as_of
ET = ZoneInfo("America/New_York")
OPEN, CLOSE = time(9, 30), time(16, 0)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def _micro(ts: float) -> float:
    """Round to the microsecond, so ages computed here equal the reader's from the ISO strings."""
    return datetime.fromisoformat(_iso(ts)).timestamp()


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
    return OPEN <= datetime.fromtimestamp(ts, ET).time() < CLOSE


def _session(ts: float) -> date:
    return datetime.fromtimestamp(ts, ET).date()


def _minute_of_session(ts: float) -> int:
    d = datetime.fromtimestamp(ts, ET)
    return (d.hour * 60 + d.minute) - (OPEN.hour * 60 + OPEN.minute)


def _payload(now: float, status: str, reason: str | None, **fields: Any) -> dict[str, Any]:
    """The reader's exact field set; unknown values are None outside ``ready``."""
    base = {k: None for k in (
        "end_of_completed_window", "latest_receipt_at", "receipt_age_seconds", "event_age_seconds",
        "completed_minutes", "cumulative_shares", "bar_vwap_estimate", "rvol", "baseline_median_shares")}
    stamp = _iso(now)
    base.update(
        schema=SCHEMA, symbol=SYMBOL, source=SOURCE, price_basis="provider_unspecified",
        as_of=stamp, served_at=stamp, session=_session(now).isoformat(), calendar_version=CALENDAR_VERSION,
        status=status, reason=reason, baseline_sessions=[], baseline_errors={}, source_revisions=[],
        exact_trade_vwap=False, signal_confirmation=False, cross_source_reconciled=False, live=False,
        historical_request=False, vwap_basis=VWAP_BASIS, rvol_basis=RVOL_BASIS)
    base.update(fields)
    return base


def compute(history: Mapping[str, Any], quote_total_volume: int | None, *, now: float,
            receipt_at: float) -> tuple[dict[str, Any], dict[str, Any]]:
    """Pure: Schwab pricehistory body + quote ``totalVolume`` -> (payload, info).

    ``info`` carries the scope-gap reconciliation and the bars consulted (for
    the envelope's provenance); neither is part of the payload.
    """
    now, receipt_at = _micro(now), _micro(receipt_at)
    bars = _bars(history)
    info: dict[str, Any] = {"bars": bars, "reconciliation": None}
    today = _session(now)
    if not _regular(now):
        return _payload(now, "closed", "outside_regular_session"), info
    # Validation: the response is SPY's, and no minute appears twice (a duplicate
    # would be counted twice; the scope gate cannot see one extra minute).
    if str((history or {}).get("symbol") or "").strip().upper() != SYMBOL:
        return _payload(now, "failed", "wrong_symbol"), info
    if len({b[0] for b in bars}) != len(bars):
        return _payload(now, "failed", "duplicate_minute"), info
    completed = [b for b in bars if b[0] + BAR_SECONDS + RECEIPT_GUARD_S <= receipt_at]
    today_all = [b for b in completed if _session(b[0]) == today]
    today_reg = [b for b in today_all if _regular(b[0])]
    if not today_reg:
        return _payload(now, "unavailable", "no_completed_regular_bar"), info
    end = today_reg[-1][0] + BAR_SECONDS
    through = _minute_of_session(today_reg[-1][0])
    # Coverage: every regular minute 09:30..last completed is present today, and
    # in each of the exact prior ten sessions. A missing minute undercounts (or
    # skews the baseline) without moving the scope gap out of its band.
    if [_minute_of_session(b[0]) for b in today_reg] != list(range(through + 1)):
        return _payload(now, "coverage_failed", "missing_current_minute"), info
    cum = sum(b[4] for b in today_reg)
    if cum <= 0:
        return _payload(now, "unavailable", "zero_volume"), info
    vwap = sum((b[1] + b[2] + b[3]) / 3.0 * b[4] for b in today_reg) / cum

    minutes: dict[date, set[int]] = {}
    prior: dict[date, int] = {}
    for b in bars:
        s = _session(b[0])
        if s >= today or not _regular(b[0]):
            continue
        m = _minute_of_session(b[0])
        minutes.setdefault(s, set()).add(m)
        if m <= through:
            prior[s] = prior.get(s, 0) + b[4]
    # The exact prior ten sessions (dates with regular bars), never an older
    # substitute for one that is incomplete: RVOL_BASIS promises exactly these.
    sessions = sorted(minutes)[-BASELINE_SESSIONS:]
    need = set(range(through + 1))
    if len(sessions) == BASELINE_SESSIONS and any(not need <= minutes[s] for s in sessions):
        return _payload(now, "coverage_failed", "missing_baseline_minute"), info
    fields = {
        "end_of_completed_window": _iso(end), "latest_receipt_at": _iso(receipt_at),
        "receipt_age_seconds": now - receipt_at, "event_age_seconds": now - end,
        "completed_minutes": len(today_reg), "cumulative_shares": cum,
        "bar_vwap_estimate": round(vwap, 4), "baseline_sessions": [s.isoformat() for s in sessions],
        "source_revisions": [f"{today.isoformat()}:{_minute_of_session(b[0])}" for b in today_reg],
    }
    if len(sessions) < BASELINE_SESSIONS:
        return _payload(now, "unavailable", "insufficient_baseline_sessions", **fields), info
    base = float(median(prior[s] for s in sessions))
    if base <= 0:
        return _payload(now, "unavailable", "zero_baseline", **fields), info
    fields.update(baseline_median_shares=base, rvol=cum / base)
    if now - end > MAX_AGE_S or now - receipt_at > MAX_AGE_S:
        return _payload(now, "unavailable", "bars_older_than_reader_limit", **fields), info

    day_bars = sum(b[4] for b in today_all)
    recon = {"quote_total_volume": quote_total_volume, "bars_total_volume": day_bars,
             "scope_gap_band": [SCOPE_GAP_MIN, SCOPE_GAP_MAX], "relative_difference": None}
    info["reconciliation"] = recon
    if type(quote_total_volume) is not int or quote_total_volume <= 0:
        return _payload(now, "unreconciled", "quote_total_volume_unavailable", **fields), info
    gap = (quote_total_volume - day_bars) / quote_total_volume
    recon["relative_difference"] = round(gap, 6)
    if gap < SCOPE_GAP_MIN:
        return _payload(now, "unreconciled", "bars_at_or_above_quote_total", **fields), info
    if gap > SCOPE_GAP_MAX:
        return _payload(now, "unreconciled", "bars_missing_volume", **fields), info
    return _payload(now, "ready", None, **fields), info


def provenance(bars: list[tuple[float, float, float, float, int]]) -> dict[str, Any]:
    """The bars this snapshot consulted: count and a canonical sha256."""
    canonical = json.dumps([[b[0], b[1], b[2], b[3], b[4]] for b in bars], separators=(",", ":"))
    return {"producer": PRODUCER, "retained_rows_consulted": len(bars),
            "retained_rows_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
            "integrity_scope": INTEGRITY_SCOPE}


def envelope(payload: Mapping[str, Any], prov: Mapping[str, Any]) -> dict[str, Any]:
    """The file gex-watch reads; ``generated_at`` is the payload's ``as_of``."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return {"schema": ENVELOPE_SCHEMA, "generated_at": payload["as_of"], "payload": dict(payload),
            "payload_sha256": hashlib.sha256(canonical.encode()).hexdigest(), "provenance": dict(prov)}


def history_params(now: float) -> dict[str, Any]:
    """Query for Schwab /pricehistory: explicit range so 10 prior sessions fit."""
    end = int(now * 1000)
    start = int((now - HISTORY_CALENDAR_DAYS * 86400) * 1000)
    return {"frequencyType": "minute", "frequency": 1, "startDate": start, "endDate": end,
            "needExtendedHoursData": "true"}
