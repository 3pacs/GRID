"""FX rate lookup utilities.

Reads the canonical ``FX:{CCY}:close`` series from ``raw_series`` and
exposes three helpers used throughout the system:

* ``get_usd_rate(engine, ccy, as_of)`` — spot rate lookup
* ``convert_to_usd(engine, amount, ccy, as_of)`` — convenience wrapper
* ``get_fx_matrix(engine, ccys, start, end)`` — batch DataFrame build

Every series value represents **1 unit of CCY in USD**, so converting
a local amount to USD is simply ``amount * rate``.  USD itself is a
pass-through (rate = 1.0) and is handled without a database round-trip.

All lookups go through a small TTL cache (10 min, 256 entries) keyed by
``(ccy, as_of_iso)`` to avoid hammering the DB when large payloads
enumerate the same periods repeatedly.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text

from utils.ttl_cache import TTLCache


# Module-level cache. 10 minute TTL is long enough for a typical page
# load without masking fresh data from nightly pulls.
_FX_CACHE: TTLCache = TTLCache(ttl=600.0, max_size=256)


def _cache_key(ccy: str, as_of: date) -> str:
    return f"{ccy.upper()}|{as_of.isoformat()}"


def _coerce_as_of(as_of: date | Any) -> date | None:
    """Accept ``date`` or anything with ``.date()`` (datetime, Timestamp)."""
    if as_of is None:
        return None
    if isinstance(as_of, date):
        return as_of
    if hasattr(as_of, "date") and callable(as_of.date):
        try:
            return as_of.date()
        except Exception:
            return None
    return None


# ── Core lookups ─────────────────────────────────────────────────────


def get_usd_rate(engine: Any, ccy: str, as_of: date) -> float | None:
    """Return the value of 1 unit of ``ccy`` in USD as of ``as_of``.

    Uses the most recent ``FX:{CCY}:close`` row with
    ``obs_date <= as_of``.  Returns ``1.0`` for USD, ``None`` when no
    rate is available on or before the requested date.

    Parameters:
        engine: SQLAlchemy engine.
        ccy: 3-letter ISO currency code (e.g. 'EUR', 'JPY').
        as_of: Point-in-time date for the rate lookup.

    Returns:
        The CCY→USD rate, or None if no data available.
    """
    if not ccy:
        return None
    ccy_u = ccy.strip().upper()
    if ccy_u == "USD":
        return 1.0

    as_of_d = _coerce_as_of(as_of)
    if as_of_d is None:
        return None

    key = _cache_key(ccy_u, as_of_d)
    cached = _FX_CACHE.get(key)
    if cached is not None:
        # Distinguish cached-None from cache-miss with a sentinel tuple.
        return cached[0] if isinstance(cached, tuple) else cached

    series_id = f"FX:{ccy_u}:close"
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT value FROM raw_series "
                    "WHERE series_id = :sid "
                    "  AND obs_date <= :aod "
                    "  AND pull_status = 'SUCCESS' "
                    "ORDER BY obs_date DESC LIMIT 1"
                ).bindparams(sid=series_id, aod=as_of_d),
            ).fetchone()
    except Exception as exc:
        log.debug("fx.get_usd_rate({c},{d}) failed: {e}", c=ccy_u, d=as_of_d, e=str(exc))
        return None

    if row is None or row[0] is None:
        _FX_CACHE.set(key, (None,))  # sentinel-tuple for cached-miss
        return None

    try:
        rate = float(row[0])
    except (TypeError, ValueError):
        return None

    _FX_CACHE.set(key, rate)
    return rate


def convert_to_usd(
    engine: Any,
    amount: float | None,
    ccy: str,
    as_of: date,
) -> float | None:
    """Convert ``amount`` in ``ccy`` to USD using the ``as_of`` spot rate.

    Returns ``None`` if either the amount or the rate is unavailable.
    ``amount * 1.0`` is returned immediately for USD without any lookup.

    Parameters:
        engine: SQLAlchemy engine.
        amount: Amount in local currency.
        ccy: Source currency code.
        as_of: Point-in-time date for the rate lookup.

    Returns:
        USD-denominated amount, or None if conversion failed.
    """
    if amount is None:
        return None
    try:
        amt = float(amount)
    except (TypeError, ValueError):
        return None

    if not ccy or ccy.strip().upper() == "USD":
        return amt

    rate = get_usd_rate(engine, ccy, as_of)
    if rate is None:
        return None
    return amt * rate


def get_fx_matrix(
    engine: Any,
    ccys: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """Return a daily FX matrix for ``ccys`` over ``[start, end]``.

    Columns are ISO currency codes, index is ``obs_date``. USD is
    always present as a constant 1.0 column if requested. Missing days
    are forward-filled within the requested range so callers can do
    clean vector math.

    Parameters:
        engine: SQLAlchemy engine.
        ccys: List of 3-letter ISO currency codes.
        start: Start date (inclusive).
        end: End date (inclusive).

    Returns:
        DataFrame of CCY→USD rates.  Empty DataFrame on failure.
    """
    if not ccys:
        return pd.DataFrame()

    clean = []
    for c in ccys:
        if not c:
            continue
        cu = c.strip().upper()
        if cu not in clean:
            clean.append(cu)
    if not clean:
        return pd.DataFrame()

    # USD is identity — handle out-of-band so we never hit the DB for it.
    needs_usd = "USD" in clean
    to_query = [c for c in clean if c != "USD"]
    series_ids = [f"FX:{c}:close" for c in to_query]

    rows: list[tuple[str, date, float]] = []
    if series_ids:
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT series_id, obs_date, value FROM raw_series "
                        "WHERE series_id = ANY(:sids) "
                        "  AND obs_date BETWEEN :s AND :e "
                        "  AND pull_status = 'SUCCESS'"
                    ).bindparams(sids=series_ids, s=start, e=end),
                ).fetchall()
                rows = [(r[0], r[1], float(r[2])) for r in result if r[2] is not None]
        except Exception as exc:
            log.debug("fx.get_fx_matrix failed: {e}", e=str(exc))
            return pd.DataFrame()

    if not rows and not needs_usd:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["series_id", "obs_date", "value"])
    if not df.empty:
        df["ccy"] = df["series_id"].str.extract(r"^FX:([A-Z]+):close$")
        df = df.drop(columns=["series_id"])
        matrix = df.pivot_table(
            index="obs_date", columns="ccy", values="value", aggfunc="last",
        )
    else:
        matrix = pd.DataFrame()

    # Reindex to full business-day range and forward-fill.
    idx = pd.date_range(start=start, end=end, freq="D").date
    matrix = matrix.reindex(idx).sort_index()
    matrix = matrix.ffill()

    if needs_usd:
        matrix["USD"] = 1.0

    matrix.index.name = "obs_date"
    return matrix


def clear_cache() -> None:
    """Clear the internal FX cache. Useful in tests and after fresh pulls."""
    _FX_CACHE.clear()
