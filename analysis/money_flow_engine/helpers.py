"""GRID -- Money Flow Engine Helpers.

Shared DB query helpers extracted from analysis/money_flow.py.
Pure query functions with no business logic. All SQL uses sqlalchemy.text().

Performance: raw_series has 268M+ rows. All queries use UNION ALL instead of
OR to ensure each branch hits the (series_id, obs_date DESC) composite index.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


# ---------------------------------------------------------------------------
# Arithmetic helpers
# ---------------------------------------------------------------------------

def _safe_pct_change(
    current: float | None, previous: float | None,
) -> float | None:
    """Percentage change, returning None if either value is missing/zero."""
    if current is None or previous is None or previous == 0:
        return None
    return round((current - previous) / abs(previous), 6)


def compute_z_score(
    values: list[float], current: float,
) -> float | None:
    """Z-score of *current* relative to the provided history.

    Returns None when the history is too short (<2) or has zero variance.
    """
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    if variance == 0:
        return None
    std = variance ** 0.5
    return round((current - mean) / std, 4)


def dominant_confidence(nodes: list[Any]) -> str:
    """Return the most common ``confidence`` value from a list of nodes."""
    if not nodes:
        return "estimated"
    levels: list[str] = []
    for n in nodes:
        if hasattr(n, "confidence"):
            levels.append(n.confidence)
        elif isinstance(n, dict) and "confidence" in n:
            levels.append(n["confidence"])
    if not levels:
        return "estimated"
    counter = Counter(levels)
    return counter.most_common(1)[0][0]


# ---------------------------------------------------------------------------
# Core UNION ALL query: raw_series with case-insensitive fallback
# ---------------------------------------------------------------------------

_RAW_LATEST_SQL = text("""
    SELECT value FROM (
        (SELECT value, obs_date FROM raw_series
         WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS'
         ORDER BY obs_date DESC LIMIT 1)
        UNION ALL
        (SELECT value, obs_date FROM raw_series
         WHERE series_id = :sid_lower AND obs_date <= :d AND pull_status = 'SUCCESS'
         ORDER BY obs_date DESC LIMIT 1)
    ) sub
    ORDER BY obs_date DESC LIMIT 1
""")

_RAW_HISTORY_SQL = text("""
    SELECT value, obs_date FROM (
        (SELECT value, obs_date FROM raw_series
         WHERE series_id = :sid
         AND obs_date BETWEEN :start AND :end
         AND pull_status = 'SUCCESS')
        UNION ALL
        (SELECT value, obs_date FROM raw_series
         WHERE series_id = :sid_lower
         AND obs_date BETWEEN :start AND :end
         AND pull_status = 'SUCCESS')
    ) sub
    ORDER BY obs_date
""")

_RESOLVED_LATEST_SQL = text("""
    SELECT rs.value
    FROM resolved_series rs
    JOIN feature_registry fr ON rs.feature_id = fr.id
    WHERE fr.name = :name AND rs.obs_date <= :d
    ORDER BY rs.obs_date DESC LIMIT 1
""")


# ---------------------------------------------------------------------------
# Series / DB query helpers
# ---------------------------------------------------------------------------

def _get_series_latest(
    engine: Engine, series_id: str | None, as_of: date | None = None,
) -> float | None:
    """Try resolved_series first, then raw_series (UNION ALL for case fallback)."""
    if not series_id:
        return None
    if as_of is None:
        as_of = date.today()

    sid_lower = series_id.lower()

    with engine.connect() as conn:
        # resolved_series (canonical feature name, lowercase)
        row = conn.execute(_RESOLVED_LATEST_SQL, {"name": sid_lower, "d": as_of}).fetchone()
        if row:
            return float(row[0])

        # raw_series — UNION ALL: exact match + lowercase in one query
        row = conn.execute(_RAW_LATEST_SQL, {
            "sid": series_id, "sid_lower": sid_lower, "d": as_of,
        }).fetchone()
        if row:
            return float(row[0])
    return None


def _get_series_value_at(
    engine: Engine, series_id: str | None, target_date: date,
) -> float | None:
    """Fetch any series value at or near a target date."""
    if not series_id:
        return None

    sid_lower = series_id.lower()

    with engine.connect() as conn:
        # raw_series — UNION ALL
        row = conn.execute(_RAW_LATEST_SQL, {
            "sid": series_id, "sid_lower": sid_lower, "d": target_date,
        }).fetchone()
        if row:
            return float(row[0])

        # resolved_series fallback
        row = conn.execute(_RESOLVED_LATEST_SQL, {
            "name": sid_lower, "d": target_date,
        }).fetchone()
        if row:
            return float(row[0])
    return None


# ---------------------------------------------------------------------------
# Price helpers
# ---------------------------------------------------------------------------

_PRICE_SQL = text("""
    SELECT value FROM raw_series
    WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS'
    ORDER BY obs_date DESC LIMIT 1
""")

_PRICE_RESOLVED_SQL = text("""
    SELECT rs.value
    FROM resolved_series rs
    JOIN feature_registry fr ON rs.feature_id = fr.id
    WHERE fr.name IN (:n1, :n2)
    AND rs.obs_date <= :d
    ORDER BY rs.obs_date DESC LIMIT 1
""")


def _get_price(
    engine: Engine, ticker: str, as_of: date | None = None,
) -> float | None:
    """Get the latest close price for a ticker."""
    if as_of is None:
        as_of = date.today()
    with engine.connect() as conn:
        row = conn.execute(_PRICE_SQL, {"sid": f"YF:{ticker}:close", "d": as_of}).fetchone()
        if row:
            return float(row[0])

        row = conn.execute(_PRICE_RESOLVED_SQL, {
            "n1": f"{ticker.lower()}_close",
            "n2": ticker.lower(),
            "d": as_of,
        }).fetchone()
        if row:
            return float(row[0])
    return None


def _get_price_change(
    engine: Engine, ticker: str, days: int, as_of: date | None = None,
) -> float | None:
    """Compute price change percentage over a period."""
    if as_of is None:
        as_of = date.today()
    current = _get_price(engine, ticker, as_of)
    past = _get_price(engine, ticker, as_of - timedelta(days=days))
    return _safe_pct_change(current, past)


# ---------------------------------------------------------------------------
# History helpers
# ---------------------------------------------------------------------------

def _get_series_history(
    engine: Engine,
    series_id: str | None,
    as_of: date | None = None,
    lookback_days: int = 730,
) -> list[float]:
    """Fetch up to *lookback_days* of historical values for z-score computation.

    Uses UNION ALL to handle case-insensitive series IDs in one query.
    Deduplicates by obs_date (prefers the first branch = exact match).
    """
    if not series_id:
        return []
    if as_of is None:
        as_of = date.today()
    start = as_of - timedelta(days=lookback_days)

    with engine.connect() as conn:
        rows = conn.execute(_RAW_HISTORY_SQL, {
            "sid": series_id,
            "sid_lower": series_id.lower(),
            "start": start,
            "end": as_of,
        }).fetchall()

    # Deduplicate by obs_date (UNION ALL may return both cases for same date)
    seen: set[date] = set()
    values: list[float] = []
    for r in rows:
        d = r[1]
        if d not in seen:
            seen.add(d)
            values.append(float(r[0]))
    return values


# ---------------------------------------------------------------------------
# Composite helpers
# ---------------------------------------------------------------------------

# Batched query: get values at multiple dates in one round-trip
_BATCH_VALUES_SQL = text("""
    SELECT tag, value FROM (
        (SELECT 'current' AS tag, value, obs_date FROM raw_series
         WHERE series_id = :sid AND obs_date <= :d0 AND pull_status = 'SUCCESS'
         ORDER BY obs_date DESC LIMIT 1)
        UNION ALL
        (SELECT 'current' AS tag, value, obs_date FROM raw_series
         WHERE series_id = :sid_lower AND obs_date <= :d0 AND pull_status = 'SUCCESS'
         ORDER BY obs_date DESC LIMIT 1)
        UNION ALL
        (SELECT '1d' AS tag, value, obs_date FROM raw_series
         WHERE series_id = :sid AND obs_date <= :d1 AND pull_status = 'SUCCESS'
         ORDER BY obs_date DESC LIMIT 1)
        UNION ALL
        (SELECT '1d' AS tag, value, obs_date FROM raw_series
         WHERE series_id = :sid_lower AND obs_date <= :d1 AND pull_status = 'SUCCESS'
         ORDER BY obs_date DESC LIMIT 1)
        UNION ALL
        (SELECT '1w' AS tag, value, obs_date FROM raw_series
         WHERE series_id = :sid AND obs_date <= :d7 AND pull_status = 'SUCCESS'
         ORDER BY obs_date DESC LIMIT 1)
        UNION ALL
        (SELECT '1w' AS tag, value, obs_date FROM raw_series
         WHERE series_id = :sid_lower AND obs_date <= :d7 AND pull_status = 'SUCCESS'
         ORDER BY obs_date DESC LIMIT 1)
        UNION ALL
        (SELECT '1m' AS tag, value, obs_date FROM raw_series
         WHERE series_id = :sid AND obs_date <= :d30 AND pull_status = 'SUCCESS'
         ORDER BY obs_date DESC LIMIT 1)
        UNION ALL
        (SELECT '1m' AS tag, value, obs_date FROM raw_series
         WHERE series_id = :sid_lower AND obs_date <= :d30 AND pull_status = 'SUCCESS'
         ORDER BY obs_date DESC LIMIT 1)
    ) sub
""")


def compute_changes(
    engine: Engine, series_id: str, as_of: date | None = None,
) -> dict[str, float | None]:
    """Return ``{change_1d, change_1w, change_1m}`` for a series.

    Single batched query fetches current + 3 historical values (8 UNION ALL
    branches — exact + lowercase for each date). ~4ms on 268M rows.
    """
    if as_of is None:
        as_of = date.today()

    sid_lower = series_id.lower()

    with engine.connect() as conn:
        rows = conn.execute(_BATCH_VALUES_SQL, {
            "sid": series_id, "sid_lower": sid_lower,
            "d0": as_of,
            "d1": as_of - timedelta(days=1),
            "d7": as_of - timedelta(days=7),
            "d30": as_of - timedelta(days=30),
        }).fetchall()

    # Collect first value per tag (exact match wins by UNION ALL order)
    vals: dict[str, float] = {}
    for tag, value in rows:
        if tag not in vals:
            vals[tag] = float(value)

    current = vals.get("current")
    return {
        "change_1d": _safe_pct_change(current, vals.get("1d")),
        "change_1w": _safe_pct_change(current, vals.get("1w")),
        "change_1m": _safe_pct_change(current, vals.get("1m")),
    }
