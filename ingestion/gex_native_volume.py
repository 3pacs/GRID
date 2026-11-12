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

* ``cumulative_shares``: sum of today's completed exchange regular-session bar volumes.
  A bar counts as completed once ``RECEIPT_GUARD_S`` seconds past its end at
  receipt (the reader requires ``latest_receipt_at >= end + 5``).
* ``bar_vwap_estimate``: sum(HLC3 x volume) / sum(volume) over those bars, never
  exact trade VWAP (``exact_trade_vwap`` False).
* ``rvol``: ``cumulative_shares / baseline_median_shares`` exactly, where the
  baseline is the median cumulative through the same minute of the previous
  ``BASELINE_SESSIONS`` expected exchange sessions in the same response.
  A missing session is never replaced by an older observed date.
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
minute). The pinned offline XNYS manifest defines sessions, holidays, DST and
early closes. Unknown calendar coverage fails closed. An early-close baseline
that cannot supply the current elapsed window is unavailable, never rescaled
or replaced. Signals never read this (``signal_confirmation`` False).
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo

from ingestion.market_calendar import ManifestCalendar, MarketSession

SCHEMA = "grid.native-volume.v1"
ENVELOPE_SCHEMA = "grid.native-volume.snapshot.v1"
SYMBOL = "SPY"
SOURCE = "schwab_pricehistory"
CALENDAR_PATH = (Path(__file__).resolve().parents[1] / "docs/reference/calendar-manifests/"
                 "xnys-rth-20261001-20271231.json")
CALENDAR_SHA256 = "efa8897e2fd94afd4f9f9b29951f142b97aece83812aca7403026df1ec4eb8ad"
CALENDAR_VERSION = (f"manifest:{CALENDAR_SHA256}:exchange_calendars:4.11.2:pandas:2.3.3:"
                    "numpy:2.5.4:tzdata:2026.6:XNYS:2026-01-01:2027-12-31")
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


def _calendar() -> ManifestCalendar:
    # Revalidate each invocation; a missing/corrupt/replaced file must not leave
    # a previously cached schedule usable. The digest is pinned in source.
    calendar = ManifestCalendar(CALENDAR_PATH, CALENDAR_SHA256)
    if calendar.version != CALENDAR_VERSION:
        raise ValueError("calendar provenance mismatch")
    return calendar


def calendar_window(now: float) -> tuple[ManifestCalendar, MarketSession | None, tuple[MarketSession, ...]]:
    """Current open session + exact ten prior sessions, or closed/unknown.

    None current means known closed. Unknown coverage or insufficient preceding
    calendar history raises; neither may fall back to weekdays or observed bars.
    The runner calls this before making any provider request.
    """
    calendar = _calendar()
    current = calendar.for_date(_session(now))
    if current is None or not current.opens.timestamp() <= now < current.closes.timestamp():
        return calendar, None, ()
    return calendar, current, calendar.prior_sessions(current.key, BASELINE_SESSIONS)


def _session(ts: float) -> date:
    return datetime.fromtimestamp(ts, ET).date()


def _minute_of_session(ts: float, session: MarketSession) -> int:
    return int((ts - session.opens.timestamp()) // BAR_SECONDS)


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
    """Schwab body + quote + verified local calendar -> (payload, info).

    ``info`` carries the scope-gap reconciliation and the bars consulted (for
    the envelope's provenance); neither is part of the payload.
    """
    now, receipt_at = _micro(now), _micro(receipt_at)
    bars = _bars(history)
    info: dict[str, Any] = {"bars": bars, "reconciliation": None}
    today = _session(now)
    try:
        _, current, expected = calendar_window(now)
    except (OSError, ValueError):
        return _payload(now, "unavailable", "calendar_unavailable"), info
    if current is None:
        return _payload(now, "closed", "outside_regular_session"), info
    # Validation: the response is SPY's, and no minute appears twice (a duplicate
    # would be counted twice; the scope gate cannot see one extra minute).
    if str((history or {}).get("symbol") or "").strip().upper() != SYMBOL:
        return _payload(now, "failed", "wrong_symbol"), info
    if len({b[0] for b in bars}) != len(bars):
        return _payload(now, "failed", "duplicate_minute"), info
    completed = [b for b in bars if b[0] + BAR_SECONDS + RECEIPT_GUARD_S <= receipt_at]
    today_all = [b for b in completed if _session(b[0]) == today]
    today_reg = [b for b in today_all
                 if current.opens.timestamp() <= b[0] < current.closes.timestamp()]
    if not today_reg:
        return _payload(now, "unavailable", "no_completed_regular_bar"), info
    end = today_reg[-1][0] + BAR_SECONDS
    through = _minute_of_session(today_reg[-1][0], current)
    # Coverage: every regular minute 09:30..last completed is present today, and
    # in each of the exact prior ten sessions. A missing minute undercounts (or
    # skews the baseline) without moving the scope gap out of its band.
    if [b[0] for b in today_reg] != [current.opens.timestamp() + BAR_SECONDS * m for m in range(through + 1)]:
        return _payload(now, "coverage_failed", "missing_current_minute"), info
    cum = sum(b[4] for b in today_reg)
    if cum <= 0:
        return _payload(now, "unavailable", "zero_volume"), info
    vwap = sum((b[1] + b[2] + b[3]) / 3.0 * b[4] for b in today_reg) / cum

    # Calendar-selected sessions, never the newest ten nonempty data dates.
    # An early close is comparable only while every exact prior session can
    # supply the same elapsed minute count. No scaling or older substitute.
    sessions = [_session(s.opens.timestamp()) for s in expected]
    short = {d.isoformat(): "early_close_insufficient_elapsed_minutes"
             for d, s in zip(sessions, expected)
             if (s.closes - s.opens).total_seconds() < (through + 1) * BAR_SECONDS}
    if short:
        return _payload(now, "unavailable", "incomparable_baseline_session", baseline_errors=short), info
    by_day = dict(zip(sessions, expected))
    minutes: dict[date, list[float]] = {}
    prior: dict[date, int] = {}
    for b in bars:
        d = _session(b[0])
        session = by_day.get(d)
        if session is None or not session.opens.timestamp() <= b[0] < session.closes.timestamp():
            continue
        minutes.setdefault(d, [])
        m = _minute_of_session(b[0], session)
        if m <= through:
            minutes[d].append(b[0])
            prior[d] = prior.get(d, 0) + b[4]
    missing = {d.isoformat(): "missing_baseline_session" for d in sessions if d not in minutes}
    if missing:
        return _payload(now, "coverage_failed", "missing_baseline_session", baseline_errors=missing), info
    gaps = {d.isoformat(): "missing_baseline_minute" for d, s in by_day.items()
            if minutes[d] != [s.opens.timestamp() + BAR_SECONDS * m for m in range(through + 1)]}
    if gaps:
        return _payload(now, "coverage_failed", "missing_baseline_minute", baseline_errors=gaps), info
    fields = {
        "end_of_completed_window": _iso(end), "latest_receipt_at": _iso(receipt_at),
        "receipt_age_seconds": now - receipt_at, "event_age_seconds": now - end,
        "completed_minutes": len(today_reg), "cumulative_shares": cum,
        "bar_vwap_estimate": round(vwap, 4), "baseline_sessions": [s.isoformat() for s in sessions],
        "source_revisions": [f"{today.isoformat()}:{_minute_of_session(b[0], current)}" for b in today_reg],
    }
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
