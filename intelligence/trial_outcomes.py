"""Score what actually happened at each trial readout.

Why this module exists
----------------------
``trial_signals`` has carried ``fwd_return_30d``, ``eval_score`` and
``evaluated_at`` since the table was created, with a schema comment saying
they are "filled by test.py post-hoc". No such writer exists anywhere in
the tree. Measured on 2026-09-10: **135 signal rows, 0 scored, 0
evaluated.** GRID has never observed the outcome of a single readout it
flagged.

That is the reason ``intelligence/catalyst_ev.py`` can only offer a
borrowed industry base rate for P(success). You cannot calibrate a
probability against events you never recorded. This module records them.

Two different measurements, deliberately kept apart
---------------------------------------------------
* ``fwd_return_30d`` — 30 calendar days from ``created_at``, the signal's
  own date. This answers *"was flagging this a good call?"* and is what
  the pre-existing ``trial_signal_performance`` view aggregates.
* ``readout_return_30d`` — 30 calendar days from
  ``primary_completion_date``, the readout itself. This answers *"what did
  the event do?"* and is the one
  ``catalyst_ev.empirical_phase_outcomes`` fits a phase base rate against.

Conflating them would quietly measure signal timing and call it trial
success. A signal raised eight months before a readout can be a fine call
and still show a flat 30-day return, and a readout can double the stock
while the signal that flagged it a year earlier looks poor.

Honesty rules
-------------
* Prices are read through ``intelligence.long_plays._load_adj_close``, the
  PIT-correct reader this repo already uses for the same universe
  (``resolved_series`` via ``PITStore``, else ``raw_series`` bounded on
  ``obs_date`` both sides and on ``pull_timestamp <= as_of``). Reusing it
  keeps one price path rather than a second, subtly different one.
* A window is scored only when there is a real observation on both ends
  within a tolerance. A missing quote produces ``None``, never a
  carried-forward or interpolated number — a fabricated outcome would
  poison the base rate this exists to measure.
* Nothing here writes ``decision_journal`` and nothing is ever updated
  twice: a scored row keeps its first measurement.

Public API
----------
``ensure_outcome_columns(engine)``, ``score_trial_outcomes(engine, ...)``,
``load_scored_outcomes(engine, ...)``, ``realized_return(...)``
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Sequence

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Forward window, in calendar days, for both measurements.
FORWARD_WINDOW_DAYS: int = 30

# How far either end of a window may be from its target date before the
# observation is refused. Markets close at weekends and holidays; 5 calendar
# days covers a long weekend without letting a "30-day" return silently
# become a 60-day one.
PRICE_TOLERANCE_DAYS: int = 5

# Years of adjusted-close history to request per scoring pass.
HISTORY_YEARS: int = 3

DEFAULT_BATCH_LIMIT: int = 500


_ENSURE_COLUMNS_SQL: tuple[str, ...] = (
    """
    ALTER TABLE trial_signals
    ADD COLUMN IF NOT EXISTS readout_return_30d NUMERIC(8,4)
    """,
    """
    ALTER TABLE trial_signals
    ADD COLUMN IF NOT EXISTS readout_scored_at TIMESTAMPTZ
    """,
)

# Rows whose readout has had time to land and which have not been scored yet.
_UNSCORED_READOUTS_SQL = text(
    """
    SELECT id, ticker, trial_phase, primary_indication, signal_type,
           primary_completion_date, created_at
    FROM trial_signals
    WHERE primary_completion_date IS NOT NULL
      AND primary_completion_date >= :earliest
      AND primary_completion_date <= :latest
      AND readout_scored_at IS NULL
      AND ticker ~ '^[A-Z.-]{1,6}$'
    ORDER BY primary_completion_date DESC
    LIMIT :limit
    """
)

_UNSCORED_SIGNALS_SQL = text(
    """
    SELECT id, ticker, created_at
    FROM trial_signals
    WHERE created_at >= :earliest
      AND created_at <= :latest
      AND fwd_return_30d IS NULL
      AND ticker ~ '^[A-Z.-]{1,6}$'
    ORDER BY created_at DESC
    LIMIT :limit
    """
)

_WRITE_READOUT_SQL = text(
    """
    UPDATE trial_signals
    SET readout_return_30d = :ret,
        readout_scored_at = :scored_at
    WHERE id = :id
      AND readout_scored_at IS NULL
    """
)

_WRITE_SIGNAL_SQL = text(
    """
    UPDATE trial_signals
    SET fwd_return_30d = :ret,
        evaluated_at = :scored_at
    WHERE id = :id
      AND fwd_return_30d IS NULL
    """
)

_SCORED_OUTCOMES_SQL = text(
    """
    SELECT trial_phase, primary_indication, signal_type,
           readout_return_30d, fwd_return_30d, primary_completion_date
    FROM trial_signals
    WHERE readout_scored_at IS NOT NULL
      AND readout_return_30d IS NOT NULL
      AND primary_completion_date <= :as_of
    ORDER BY primary_completion_date DESC
    """
)


def ensure_outcome_columns(engine: Engine) -> None:
    """Add the readout-outcome columns if missing. Idempotent; never raises."""
    try:
        with engine.begin() as conn:
            for ddl in _ENSURE_COLUMNS_SQL:
                conn.execute(text(ddl))
    except Exception as exc:  # noqa: BLE001
        log.warning("trial_outcomes: ensure_outcome_columns failed: {e}", e=str(exc))


def _as_date(value: Any) -> date | None:
    """Coerce a date/datetime/ISO string to ``date``; ``None`` when unusable."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _price_on(
    points: Sequence[tuple[date, float]],
    target: date,
    *,
    tolerance_days: int = PRICE_TOLERANCE_DAYS,
) -> float | None:
    """Closest observed close to ``target``, or ``None`` beyond tolerance.

    Takes the nearest real observation rather than carrying a stale price
    forward: an unscored window is honest, an invented one is not.
    """
    best: tuple[int, float] | None = None
    for obs_date, value in points:
        if value is None or value <= 0:
            continue
        gap = abs((obs_date - target).days)
        if gap > tolerance_days:
            continue
        if best is None or gap < best[0]:
            best = (gap, float(value))
    return best[1] if best is not None else None


def realized_return(
    points: Sequence[tuple[date, float]],
    anchor: date,
    *,
    window_days: int = FORWARD_WINDOW_DAYS,
    tolerance_days: int = PRICE_TOLERANCE_DAYS,
) -> float | None:
    """Fractional return from ``anchor`` to ``anchor + window_days``.

    ``0.42`` means +42 %. Returns ``None`` unless both ends have a real
    observation inside ``tolerance_days`` — a half-measured window is not a
    measurement.
    """
    start = _price_on(points, anchor, tolerance_days=tolerance_days)
    if start is None or start <= 0:
        return None
    end = _price_on(points, anchor + timedelta(days=window_days), tolerance_days=tolerance_days)
    if end is None or end <= 0:
        return None
    return (end - start) / start


def score_trial_outcomes(
    engine: Engine,
    *,
    as_of: date | None = None,
    limit: int = DEFAULT_BATCH_LIMIT,
    window_days: int = FORWARD_WINDOW_DAYS,
    history_years: int = HISTORY_YEARS,
) -> dict[str, Any]:
    """Measure and persist realized outcomes for readouts and signals.

    Only windows that have fully elapsed as of ``as_of`` are considered, so
    the function can never read a price from the future relative to the day
    it claims to be scoring.

    Returns a summary dict. Never raises: a failing price read or a missing
    column degrades the batch, it does not break the caller.
    """
    as_of = as_of or date.today()
    cutoff = as_of - timedelta(days=window_days)
    earliest = as_of - timedelta(days=int(history_years * 365.25))

    ensure_outcome_columns(engine)
    summary: dict[str, Any] = {
        "as_of": as_of.isoformat(),
        "readouts_considered": 0,
        "readouts_scored": 0,
        "signals_considered": 0,
        "signals_scored": 0,
        "unpriced": 0,
        "notes": [],
    }

    try:
        with engine.connect() as conn:
            readouts = conn.execute(
                _UNSCORED_READOUTS_SQL,
                {"earliest": earliest, "latest": cutoff, "limit": int(limit)},
            ).fetchall()
            signals = conn.execute(
                _UNSCORED_SIGNALS_SQL,
                {
                    "earliest": datetime.combine(earliest, datetime.min.time()).replace(tzinfo=timezone.utc),
                    "latest": datetime.combine(cutoff, datetime.max.time()).replace(tzinfo=timezone.utc),
                    "limit": int(limit),
                },
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.warning("trial_outcomes: read failed: {e}", e=str(exc))
        summary["notes"].append(f"read failed ({type(exc).__name__}); nothing scored")
        return summary

    summary["readouts_considered"] = len(readouts)
    summary["signals_considered"] = len(signals)
    if not readouts and not signals:
        return summary

    tickers = sorted(
        {str(r[1]).strip().upper() for r in readouts if r[1]}
        | {str(r[1]).strip().upper() for r in signals if r[1]}
    )

    # Reuse the repo's PIT-correct price reader rather than opening a second,
    # subtly different path to the same series.
    try:
        from intelligence.long_plays import _load_adj_close

        history = _load_adj_close(engine, tickers, history_years, as_of)
    except Exception as exc:  # noqa: BLE001
        log.warning("trial_outcomes: price history unavailable: {e}", e=str(exc))
        summary["notes"].append(f"price history unavailable ({type(exc).__name__}); nothing scored")
        return summary

    scored_at = datetime.now(timezone.utc)
    readout_writes: list[dict[str, Any]] = []
    signal_writes: list[dict[str, Any]] = []

    for row in readouts:
        ticker = str(row[1]).strip().upper()
        anchor = _as_date(row[5])
        points = history.get(ticker)
        if anchor is None or not points:
            summary["unpriced"] += 1
            continue
        ret = realized_return(points, anchor, window_days=window_days)
        if ret is None:
            summary["unpriced"] += 1
            continue
        readout_writes.append({"id": int(row[0]), "ret": round(ret, 4), "scored_at": scored_at})

    for row in signals:
        ticker = str(row[1]).strip().upper()
        anchor = _as_date(row[2])
        points = history.get(ticker)
        if anchor is None or not points:
            continue
        ret = realized_return(points, anchor, window_days=window_days)
        if ret is None:
            continue
        signal_writes.append({"id": int(row[0]), "ret": round(ret, 4), "scored_at": scored_at})

    try:
        with engine.begin() as conn:
            for params in readout_writes:
                conn.execute(_WRITE_READOUT_SQL, params)
            for params in signal_writes:
                conn.execute(_WRITE_SIGNAL_SQL, params)
    except Exception as exc:  # noqa: BLE001
        log.warning("trial_outcomes: write failed: {e}", e=str(exc))
        summary["notes"].append(f"write failed ({type(exc).__name__}); nothing persisted")
        return summary

    summary["readouts_scored"] = len(readout_writes)
    summary["signals_scored"] = len(signal_writes)
    log.info(
        "trial outcomes: scored {r}/{rc} readouts and {s}/{sc} signals ({u} unpriced)",
        r=len(readout_writes), rc=len(readouts),
        s=len(signal_writes), sc=len(signals), u=summary["unpriced"],
    )
    return summary


def load_scored_outcomes(engine: Engine, *, as_of: date | None = None) -> list[dict[str, Any]]:
    """Scored readouts, newest first — the input to ``empirical_phase_outcomes``.

    Rows carry ``fwd_return_30d`` aliased from ``readout_return_30d`` so the
    fitter reads the **event** outcome, not the signal's own 30 days. Returns
    ``[]`` when nothing is scored yet or the columns do not exist.
    """
    as_of = as_of or date.today()
    try:
        with engine.connect() as conn:
            rows = conn.execute(_SCORED_OUTCOMES_SQL, {"as_of": as_of}).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("trial_outcomes: load_scored_outcomes failed: {e}", e=str(exc))
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        readout = row[3]
        out.append(
            {
                "trial_phase": row[0],
                "primary_indication": row[1],
                "signal_type": row[2],
                # empirical_phase_outcomes() fits on this key; it is the
                # readout-anchored move, never the signal-anchored one.
                "fwd_return_30d": float(readout) if readout is not None else None,
                "readout_return_30d": float(readout) if readout is not None else None,
                "signal_return_30d": float(row[4]) if row[4] is not None else None,
                "primary_completion_date": (
                    row[5].isoformat() if hasattr(row[5], "isoformat") else row[5]
                ),
            }
        )
    return out
