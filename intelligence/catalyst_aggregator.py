"""ALPHA-4 — Unified catalyst calendar + catalyst-aware scoring.

Aggregates the per-domain calendar tables that already exist in GRID into
a single ranked stream the oracle can score against:

  * ``earnings_calendar``   — quarterly earnings dates per ticker
  * ``catalyst_calendar``   — clinical trial readouts (trial_signals path)
  * Hard-coded FOMC dates   — Fed meeting calendar for 2026
  * Computed OPEX dates     — third Friday of each month + quarterly OPEX

The Tier A shortlist (#102) puts this at ~1.5% Brier lift on the ~30%
of trades that fall inside a known catalyst window. The lift comes from
two places:

  1. **Catalyst proximity score** — every prediction gets a 0..1 multiplier
     based on distance to the nearest catalyst within its horizon. Closer
     to a catalyst → wider confidence interval → smaller Kelly fraction.
  2. **Catalyst-typed feature** — the oracle reads a ``catalyst_type`` enum
     so a model trained on earnings-window behaviour can be routed
     differently from one trained on FOMC-window behaviour.

This module is the AGGREGATOR only — the oracle wiring lands in a follow-up
edit (waiting on ALPHA-3 task #106 to release `oracle/engine.py`). Until
then `proximity_score()` and `events_for_window()` can be called directly
by `discovery/options_scanner.py` and `trading/options_recommender.py`.

The aggregator is read-only at the DB layer — it never writes back. Calendar
ingestion is owned by the per-domain pullers (earnings_intel, trial_ingestor,
manual FOMC seed). All catalyst dedupe + ranking happens in memory.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Catalyst types ─────────────────────────────────────────────────────────

CATALYST_EARNINGS = "earnings"
CATALYST_CLINICAL = "clinical_trial"
CATALYST_FOMC = "fomc"
CATALYST_OPEX_MONTHLY = "opex_monthly"
CATALYST_OPEX_QUARTERLY = "opex_quarterly"

# Order matters: lower index = higher impact. Used by `nearest_catalyst`
# to break ties when two events fall on the same day.
_CATALYST_PRIORITY: dict[str, int] = {
    CATALYST_FOMC: 0,
    CATALYST_EARNINGS: 1,
    CATALYST_OPEX_QUARTERLY: 2,
    CATALYST_CLINICAL: 3,
    CATALYST_OPEX_MONTHLY: 4,
}

# Per-type baseline impact (0..1). Multiplied by horizon-aware proximity
# to produce the final score.
_CATALYST_IMPACT: dict[str, float] = {
    CATALYST_FOMC: 1.00,
    CATALYST_EARNINGS: 0.85,
    CATALYST_OPEX_QUARTERLY: 0.70,
    CATALYST_CLINICAL: 0.65,
    CATALYST_OPEX_MONTHLY: 0.40,
}


# ── Hard-coded FOMC calendar ───────────────────────────────────────────────
# 2026 FOMC dates published by the Fed. Dates are the SECOND day of each
# two-day meeting (the day the rate decision + dot plot drops). Update
# annually when the Fed publishes the next year's calendar.

_FOMC_DATES_2026: list[date] = [
    date(2026, 1, 28),
    date(2026, 3, 18),
    date(2026, 4, 29),
    date(2026, 6, 17),
    date(2026, 7, 29),
    date(2026, 9, 16),
    date(2026, 11, 4),
    date(2026, 12, 16),
]


# ── Data class ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CatalystEvent:
    """One catalyst on the unified calendar.

    Frozen so consumers can hash/dedupe and sort without copying.
    """

    ticker: str | None       # None for market-wide events (FOMC, OPEX)
    event_type: str
    event_date: date
    confidence_window_days: int = 1   # how many days around the date count
    source: str = ""
    notes: str = ""
    impact: float = 0.0       # 0..1 baseline impact

    @property
    def is_market_wide(self) -> bool:
        return self.ticker is None


# ── OPEX computation ───────────────────────────────────────────────────────


def _third_friday(year: int, month: int) -> date:
    """Return the third Friday of (year, month). Monthly OPEX falls here."""
    first = date(year, month, 1)
    # weekday(): Mon=0, ... Fri=4
    first_friday = first + timedelta(days=(4 - first.weekday()) % 7)
    return first_friday + timedelta(days=14)


def _opex_dates_for_year(year: int) -> list[tuple[date, str]]:
    """Return all monthly OPEX dates for the given year, with type tags.

    Quarterly OPEX (March, June, September, December) is tagged separately
    because it has a much larger gamma unwind than the other 8 months.
    """
    out: list[tuple[date, str]] = []
    for month in range(1, 13):
        d = _third_friday(year, month)
        kind = (
            CATALYST_OPEX_QUARTERLY
            if month in (3, 6, 9, 12)
            else CATALYST_OPEX_MONTHLY
        )
        out.append((d, kind))
    return out


# ── DB readers ─────────────────────────────────────────────────────────────


def _read_earnings_events(
    engine: Engine, *, start: date, end: date,
) -> list[CatalystEvent]:
    """Read earnings dates from earnings_calendar within [start, end]."""
    events: list[CatalystEvent] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT ticker, earnings_date, fiscal_quarter
                    FROM earnings_calendar
                    WHERE earnings_date BETWEEN :s AND :e
                    """
                ).bindparams(s=start, e=end),
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("earnings_calendar read failed: {e}", e=str(exc))
        return events

    for r in rows:
        events.append(CatalystEvent(
            ticker=str(r[0]).upper() if r[0] else None,
            event_type=CATALYST_EARNINGS,
            event_date=r[1],
            confidence_window_days=1,
            source="earnings_calendar",
            notes=f"FQ {r[2]}" if r[2] else "",
            impact=_CATALYST_IMPACT[CATALYST_EARNINGS],
        ))
    return events


def _read_clinical_events(
    engine: Engine, *, start: date, end: date,
) -> list[CatalystEvent]:
    """Read trial readouts from catalyst_calendar within [start, end]."""
    events: list[CatalystEvent] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT ticker, expected_date, event_type,
                           confidence_window_days, source, notes
                    FROM catalyst_calendar
                    WHERE is_active = TRUE
                      AND expected_date BETWEEN :s AND :e
                    """
                ).bindparams(s=start, e=end),
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("catalyst_calendar read failed: {e}", e=str(exc))
        return events

    for r in rows:
        events.append(CatalystEvent(
            ticker=str(r[0]).upper() if r[0] else None,
            event_type=CATALYST_CLINICAL,
            event_date=r[1],
            confidence_window_days=int(r[3]) if r[3] else 30,
            source=str(r[4] or "catalyst_calendar"),
            notes=str(r[5] or r[2] or ""),
            impact=_CATALYST_IMPACT[CATALYST_CLINICAL],
        ))
    return events


def _seeded_market_events(*, start: date, end: date) -> list[CatalystEvent]:
    """Return FOMC + OPEX events within [start, end]. No DB I/O."""
    events: list[CatalystEvent] = []

    # FOMC
    for d in _FOMC_DATES_2026:
        if start <= d <= end:
            events.append(CatalystEvent(
                ticker=None,
                event_type=CATALYST_FOMC,
                event_date=d,
                confidence_window_days=2,
                source="fomc_seed_2026",
                notes="FOMC rate decision + dot plot",
                impact=_CATALYST_IMPACT[CATALYST_FOMC],
            ))

    # OPEX — generate for any year that intersects the window
    years = {start.year, end.year}
    for y in years:
        for d, kind in _opex_dates_for_year(y):
            if start <= d <= end:
                events.append(CatalystEvent(
                    ticker=None,
                    event_type=kind,
                    event_date=d,
                    confidence_window_days=1,
                    source="opex_calendar",
                    notes=("Quarterly OPEX (gamma unwind)"
                           if kind == CATALYST_OPEX_QUARTERLY
                           else "Monthly OPEX"),
                    impact=_CATALYST_IMPACT[kind],
                ))
    return events


# ── Public API ─────────────────────────────────────────────────────────────


def events_for_window(
    engine: Engine,
    *,
    start: date | None = None,
    end: date | None = None,
    ticker: str | None = None,
    event_types: list[str] | None = None,
) -> list[CatalystEvent]:
    """Return every catalyst inside [start, end], optionally filtered.

    Parameters
    ----------
    engine:
        SQLAlchemy engine for the per-ticker reads.
    start, end:
        Window. Defaults to today and today+90 days.
    ticker:
        Optional case-insensitive ticker filter. Market-wide events
        (FOMC, OPEX) are always included regardless of ticker.
    event_types:
        Optional whitelist (any of the ``CATALYST_*`` constants).

    Returns
    -------
    A list of :class:`CatalystEvent`, sorted by event_date then by
    catalyst priority (FOMC first within a tie).
    """
    if start is None:
        start = date.today()
    if end is None:
        end = start + timedelta(days=90)

    events: list[CatalystEvent] = []
    events.extend(_read_earnings_events(engine, start=start, end=end))
    events.extend(_read_clinical_events(engine, start=start, end=end))
    events.extend(_seeded_market_events(start=start, end=end))

    if ticker is not None:
        upper = ticker.upper()
        events = [
            e for e in events
            if e.ticker is None or e.ticker == upper
        ]

    if event_types is not None:
        whitelist = set(event_types)
        events = [e for e in events if e.event_type in whitelist]

    events.sort(key=lambda e: (
        e.event_date,
        _CATALYST_PRIORITY.get(e.event_type, 99),
    ))
    return events


def nearest_catalyst(
    engine: Engine,
    ticker: str,
    *,
    as_of: date | None = None,
    horizon_days: int = 30,
) -> CatalystEvent | None:
    """Return the catalyst closest to ``as_of`` within ``[as_of, as_of + horizon_days]``.

    Used by the proximity score below — separated so callers that want
    just the next event for display don't pay the proximity math cost.
    """
    if as_of is None:
        as_of = date.today()
    end = as_of + timedelta(days=horizon_days)
    events = events_for_window(
        engine, start=as_of, end=end, ticker=ticker,
    )
    if not events:
        return None
    return events[0]


def proximity_score(
    engine: Engine,
    ticker: str,
    *,
    as_of: date | None = None,
    horizon_days: int = 30,
) -> dict[str, Any]:
    """Catalyst-aware multiplier for ``ticker`` over a forward window.

    Returns a dict with:

    - ``score``: 0..1 multiplier — 1.0 means a high-impact catalyst is
      hours away, 0.0 means no catalyst inside the window
    - ``nearest``: the closest catalyst event (or None)
    - ``days_to_event``: integer days to the nearest event (or None)
    - ``catalyst_type``: enum tag for downstream feature engineering
    - ``window_density``: count of catalysts inside the window

    The score uses a half-life decay:

        score = impact × exp(-days_to_event / 5)

    so a top-impact catalyst (FOMC) at d=0 gives 1.0, at d=5 gives ~0.37,
    at d=14 gives ~0.06, at d=30 gives ~0.002. Window density adds a
    small additive bump (capped) so a week packed with three earnings +
    an FOMC scores higher than the same FOMC in isolation.
    """
    import math

    if as_of is None:
        as_of = date.today()
    end = as_of + timedelta(days=horizon_days)
    events = events_for_window(
        engine, start=as_of, end=end, ticker=ticker,
    )

    if not events:
        return {
            "score": 0.0,
            "nearest": None,
            "days_to_event": None,
            "catalyst_type": None,
            "window_density": 0,
        }

    nearest = events[0]
    days_to = (nearest.event_date - as_of).days
    decay = math.exp(-max(days_to, 0) / 5.0)
    base = float(nearest.impact) * decay

    # Density bump: each additional catalyst in the window adds 0.05,
    # capped at +0.20.
    density_bump = min(0.20, max(0, len(events) - 1) * 0.05)
    score = min(1.0, base + density_bump)

    return {
        "score": round(score, 4),
        "nearest": nearest,
        "days_to_event": days_to,
        "catalyst_type": nearest.event_type,
        "window_density": len(events),
    }


def upcoming_catalysts_summary(
    engine: Engine,
    *,
    as_of: date | None = None,
    horizon_days: int = 14,
) -> dict[str, Any]:
    """Quick rollup of the next two weeks of catalysts.

    Used by the daily briefing + the dashboard "what's coming" tile.
    """
    if as_of is None:
        as_of = date.today()
    events = events_for_window(
        engine, start=as_of, end=as_of + timedelta(days=horizon_days),
    )
    by_type: dict[str, int] = {}
    for e in events:
        by_type[e.event_type] = by_type.get(e.event_type, 0) + 1
    return {
        "as_of": as_of.isoformat(),
        "horizon_days": horizon_days,
        "total": len(events),
        "by_type": by_type,
        "next": events[0] if events else None,
    }
