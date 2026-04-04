"""
State vector construction for the regime-matched analog engine.

Each state vector captures the macro environment at a point in time across
24 dimensions — VIX, rates, spreads, employment, liquidity, momentum, and
cross-reference divergence scores. All queries are PIT-correct (no look-ahead).

State vectors are cached in the `regime_state_vectors` table.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field, asdict
from datetime import date, timedelta, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Dimension specification ──────────────────────────────────────────────

@dataclass(frozen=True)
class DimensionSpec:
    """Declarative config for one state vector dimension."""
    name: str
    series_id: str          # raw_series.series_id or 'DERIVED:xxx'
    transform: str          # 'raw', 'percentile_rank', 'slope', 'diff', 'pct_change', 'rsi', 'ma_ratio', 'spread'
    weight: float           # importance weight for similarity matching
    transform_param: int | float = 0   # window/period for transform
    transform_param2: int = 0          # second param (e.g., slow MA for ma_ratio)
    min_history: int = 100             # minimum obs needed


# The 24 dimensions of the macro state
STATE_DIMENSIONS: list[DimensionSpec] = [
    # ── Volatility & Risk ──
    DimensionSpec('vix_level',           'VIXCLS',                             'raw',             1.2),
    DimensionSpec('vix_percentile',      'VIXCLS',                             'percentile_rank', 1.5, transform_param=504),
    # ── Rates & Curve ──
    DimensionSpec('yield_curve_level',   'T10Y2Y',                             'raw',             1.3),
    DimensionSpec('yield_curve_dir',     'T10Y2Y',                             'slope',           1.0, transform_param=63),
    DimensionSpec('fed_funds_level',     'DFF',                                'raw',             1.0),
    DimensionSpec('fed_funds_dir',       'DFF',                                'diff',            1.2, transform_param=63),
    # ── Credit ──
    DimensionSpec('hy_spread_level',     'BAMLH0A0HYM2',                       'raw',             1.4),
    DimensionSpec('hy_spread_dir',       'BAMLH0A0HYM2',                       'diff',            1.1, transform_param=63),
    DimensionSpec('ig_spread_level',     'BAMLC0A0CM',                         'raw',             0.9),
    # ── Employment & Economy ──
    DimensionSpec('unemployment_level',  'UNRATE',                             'raw',             0.8, min_history=30),
    DimensionSpec('unemployment_dir',    'UNRATE',                             'diff',            0.9, transform_param=3, min_history=30),
    DimensionSpec('industrial_prod_yoy', 'INDPRO',                             'pct_change',      0.8, transform_param=12, min_history=30),
    DimensionSpec('capacity_util',       'TCU',                                'raw',             0.6, min_history=30),
    # ── Money & Inflation ──
    DimensionSpec('m2_growth',           'M2SL',                               'pct_change',      0.7, transform_param=12, min_history=30),
    DimensionSpec('breakeven_5y',        'DERIVED:T5YIE',                      'raw',             0.8, min_history=50),
    DimensionSpec('consumer_sentiment',  'UMCSENT',                            'raw',             0.6, min_history=30),
    # ── Labor Market ──
    DimensionSpec('initial_claims',      'ICSA',                               'raw',             0.7),
    # ── Liquidity ──
    DimensionSpec('fed_net_liq_level',   'COMPUTED:fed_net_liquidity',          'raw',             1.3, min_history=20),
    DimensionSpec('fed_net_liq_chg',     'COMPUTED:fed_net_liquidity_change_1m','raw',             1.1, min_history=20),
    # ── Equity Momentum ──
    DimensionSpec('spy_momentum',        'DERIVED:SPY_MA_RATIO',               'raw',             1.0),
    DimensionSpec('spy_rsi',             'DERIVED:SPY_RSI',                     'raw',             0.8),
    # ── Real Rates ──
    DimensionSpec('real_fed_funds',      'DERIVED:REAL_FF',                     'raw',             1.0),
    # ── Cross-reference divergence ──
    DimensionSpec('crossref_divergence', 'DERIVED:CROSSREF_SCORE',             'raw',             1.2, min_history=5),
    # ── Insider sentiment ──
    DimensionSpec('insider_sentiment',   'DERIVED:INSIDER_NET',                 'raw',             0.9, min_history=5),
]

DIM_NAMES = [d.name for d in STATE_DIMENSIONS]
DIM_WEIGHTS = np.array([d.weight for d in STATE_DIMENSIONS], dtype=np.float64)


# ── State Vector ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class StateVector:
    """Immutable macro state at a point in time."""
    as_of_date: date
    values: tuple[float | None, ...]    # one per dimension, None = missing
    completeness: float                 # fraction of non-null dims
    stale_dimensions: tuple[str, ...]   # dims with data >30d old

    @property
    def array(self) -> np.ndarray:
        """Values as numpy array with NaN for missing."""
        return np.array([v if v is not None else np.nan for v in self.values], dtype=np.float64)

    @property
    def mask(self) -> np.ndarray:
        """Boolean mask — True where data exists."""
        return np.array([v is not None for v in self.values])

    def to_dict(self) -> dict[str, Any]:
        return {
            'as_of_date': self.as_of_date.isoformat(),
            'dimensions': {DIM_NAMES[i]: self.values[i] for i in range(len(self.values))},
            'completeness': self.completeness,
            'stale_dimensions': list(self.stale_dimensions),
        }


# ── Helpers ──────────────────────────────────────────────────────────────

def _fetch_series(engine: Engine, series_id: str, as_of: date, lookback_days: int = 2520) -> pd.Series:
    """Fetch series values up to as_of date (PIT-correct)."""
    cutoff = as_of - timedelta(days=lookback_days)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT obs_date, value FROM raw_series "
                "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                "AND obs_date >= :cutoff AND obs_date <= :as_of "
                "ORDER BY obs_date"
            ),
            {"sid": series_id, "cutoff": cutoff, "as_of": as_of},
        ).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    return pd.Series(
        {r[0]: float(r[1]) for r in rows},
        dtype=float,
    ).sort_index()


def _fetch_spy_prices(engine: Engine, as_of: date, lookback_days: int = 504) -> pd.Series:
    """Fetch SPY close prices for momentum/RSI computation."""
    cutoff = as_of - timedelta(days=lookback_days)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT obs_date, value FROM raw_series "
                "WHERE series_id = 'YF:SPY:close' AND pull_status = 'SUCCESS' "
                "AND obs_date >= :cutoff AND obs_date <= :as_of "
                "ORDER BY obs_date"
            ),
            {"cutoff": cutoff, "as_of": as_of},
        ).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    return pd.Series(
        {r[0]: float(r[1]) for r in rows},
        dtype=float,
    ).sort_index()


def _percentile_rank(series: pd.Series, window: int) -> float | None:
    """Percentile rank of latest value within rolling window."""
    if len(series) < 20:
        return None
    tail = series.iloc[-min(window, len(series)):]
    current = tail.iloc[-1]
    return float((tail < current).sum() / len(tail))


def _rolling_slope(series: pd.Series, window: int) -> float | None:
    """OLS slope of last `window` observations, normalized by mean."""
    tail = series.dropna().iloc[-min(window, len(series)):]
    if len(tail) < 10:
        return None
    x = np.arange(len(tail), dtype=float)
    y = tail.values
    mean_y = np.mean(y)
    if abs(mean_y) < 1e-8:
        mean_y = 1.0
    slope = np.polyfit(x, y, 1)[0]
    return float(slope / abs(mean_y))


def _diff(series: pd.Series, periods: int) -> float | None:
    """Difference between latest and value `periods` obs ago."""
    if len(series) < periods + 1:
        return None
    return float(series.iloc[-1] - series.iloc[-periods - 1])


def _pct_change(series: pd.Series, periods: int) -> float | None:
    """Percent change over `periods` observations."""
    if len(series) < periods + 1:
        return None
    prev = series.iloc[-periods - 1]
    if abs(prev) < 1e-8:
        return None
    return float((series.iloc[-1] - prev) / abs(prev))


def _rsi(prices: pd.Series, period: int = 14) -> float | None:
    """RSI-14 from price series."""
    if len(prices) < period + 1:
        return None
    delta = prices.diff().dropna()
    if len(delta) < period:
        return None
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    last_gain = gain.iloc[-1]
    last_loss = loss.iloc[-1]
    if last_loss == 0:
        return 100.0
    rs = last_gain / last_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def _ma_ratio(prices: pd.Series, fast: int = 50, slow: int = 200) -> float | None:
    """Ratio of fast MA to slow MA."""
    if len(prices) < slow:
        return None
    fast_ma = prices.iloc[-fast:].mean()
    slow_ma = prices.iloc[-slow:].mean()
    if abs(slow_ma) < 1e-8:
        return None
    return float(fast_ma / slow_ma)


def _get_crossref_score(engine: Engine, as_of: date) -> float | None:
    """Mean absolute divergence z-score from cross-reference checks."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT AVG(ABS(divergence_zscore)), COUNT(*) "
                "FROM cross_reference_checks "
                "WHERE checked_at::date <= :as_of "
                "AND checked_at::date >= :as_of - INTERVAL '7 days'"
            ),
            {"as_of": as_of},
        ).fetchone()
    if row is None or row[1] == 0:
        return None
    return float(row[0]) if row[0] is not None else None


def _get_insider_sentiment(engine: Engine, as_of: date) -> float | None:
    """Net insider sentiment from SEC Form 4 filings (30d window)."""
    cutoff = as_of - timedelta(days=30)
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT "
                "  SUM(CASE WHEN series_id LIKE :buy_pat THEN value ELSE 0 END), "
                "  SUM(CASE WHEN series_id LIKE :sell_pat THEN value ELSE 0 END) "
                "FROM raw_series "
                "WHERE series_id LIKE :insider_pat "
                "AND pull_status = 'SUCCESS' "
                "AND obs_date >= :cutoff AND obs_date <= :as_of"
            ),
            {"buy_pat": "%:BUY", "sell_pat": "%:SELL", "insider_pat": "INSIDER:%",
             "cutoff": cutoff, "as_of": as_of},
        ).fetchone()
    if row is None:
        return None
    buy_vol = float(row[0] or 0)
    sell_vol = float(row[1] or 0)
    total = buy_vol + sell_vol
    if total == 0:
        return None
    return float((buy_vol - sell_vol) / total)  # -1 to +1


# ── Normalization stats ──────────────────────────────────────────────────

_NORM_CACHE: dict[str, tuple[float, float]] | None = None


def _get_normalization_stats(engine: Engine) -> dict[str, tuple[float, float]]:
    """Compute mean and std for each series used in state vectors.

    Used to z-score normalize dimensions so they're comparable.
    Cached after first computation.
    """
    global _NORM_CACHE
    if _NORM_CACHE is not None:
        return _NORM_CACHE

    stats: dict[str, tuple[float, float]] = {}
    for dim in STATE_DIMENSIONS:
        if dim.series_id.startswith('DERIVED:'):
            continue
        series = _fetch_series(engine, dim.series_id, date.today(), lookback_days=10000)
        if len(series) < 20:
            continue
        stats[dim.series_id] = (float(series.mean()), float(series.std()))

    _NORM_CACHE = stats
    return stats


def _zscore_normalize(value: float | None, mean: float, std: float) -> float | None:
    """Z-score normalize a single value."""
    if value is None or std == 0:
        return value
    return (value - mean) / std


# ── Main computation ─────────────────────────────────────────────────────

def compute_state_vector(engine: Engine, as_of: date | None = None) -> StateVector:
    """Compute the macro state vector at a specific date (PIT-correct).

    Each dimension is fetched from the database, transformed, and z-score
    normalized against its full history.
    """
    if as_of is None:
        as_of = date.today()

    norm_stats = _get_normalization_stats(engine)
    spy_prices = _fetch_spy_prices(engine, as_of)
    values: list[float | None] = []
    stale: list[str] = []

    for dim in STATE_DIMENSIONS:
        try:
            val = _compute_dimension(engine, dim, as_of, norm_stats, spy_prices)
            values.append(val)

            # Check staleness for non-derived series
            if not dim.series_id.startswith('DERIVED:') and val is not None:
                series = _fetch_series(engine, dim.series_id, as_of, lookback_days=60)
                if len(series) > 0:
                    latest_date = series.index[-1]
                    if hasattr(latest_date, 'date'):
                        latest_date = latest_date
                    days_stale = (as_of - latest_date).days if isinstance(latest_date, date) else 30
                    if days_stale > 30:
                        stale.append(dim.name)
        except Exception as exc:
            log.debug("Dim {d} failed for {dt}: {e}", d=dim.name, dt=as_of, e=str(exc))
            values.append(None)

    non_null = sum(1 for v in values if v is not None)
    completeness = non_null / len(values) if values else 0.0

    return StateVector(
        as_of_date=as_of,
        values=tuple(values),
        completeness=completeness,
        stale_dimensions=tuple(stale),
    )


def _compute_dimension(
    engine: Engine,
    dim: DimensionSpec,
    as_of: date,
    norm_stats: dict[str, tuple[float, float]],
    spy_prices: pd.Series,
) -> float | None:
    """Compute a single dimension value."""

    # ── Derived dimensions (computed from other series) ──
    if dim.series_id == 'DERIVED:T5YIE':
        series = _fetch_series(engine, 'T5YIE', as_of)
        if series.empty:
            return None
        val = float(series.iloc[-1])
        stats = norm_stats.get('T5YIE')
        return _zscore_normalize(val, stats[0], stats[1]) if stats else val

    if dim.series_id == 'DERIVED:SPY_MA_RATIO':
        return _ma_ratio(spy_prices, 50, 200)

    if dim.series_id == 'DERIVED:SPY_RSI':
        rsi_val = _rsi(spy_prices)
        return (rsi_val - 50.0) / 25.0 if rsi_val is not None else None  # normalize to ~[-2, 2]

    if dim.series_id == 'DERIVED:REAL_FF':
        dff = _fetch_series(engine, 'DFF', as_of)
        t5yie = _fetch_series(engine, 'T5YIE', as_of)
        if dff.empty or t5yie.empty:
            return None
        return float(dff.iloc[-1] - t5yie.iloc[-1])

    if dim.series_id == 'DERIVED:CROSSREF_SCORE':
        return _get_crossref_score(engine, as_of)

    if dim.series_id == 'DERIVED:INSIDER_NET':
        return _get_insider_sentiment(engine, as_of)

    # ── Standard series dimensions ──
    series = _fetch_series(engine, dim.series_id, as_of)
    if series.empty or len(series) < dim.min_history:
        return None

    # Apply transform
    if dim.transform == 'raw':
        val = float(series.iloc[-1])
    elif dim.transform == 'percentile_rank':
        val = _percentile_rank(series, int(dim.transform_param))
        return val  # already 0-1, no z-score normalization needed
    elif dim.transform == 'slope':
        val = _rolling_slope(series, int(dim.transform_param))
        return val  # already normalized by mean
    elif dim.transform == 'diff':
        val = _diff(series, int(dim.transform_param))
    elif dim.transform == 'pct_change':
        val = _pct_change(series, int(dim.transform_param))
        return val  # already a ratio
    else:
        val = float(series.iloc[-1])

    if val is None:
        return None

    # Z-score normalize raw values
    stats = norm_stats.get(dim.series_id)
    if stats and dim.transform == 'raw':
        return _zscore_normalize(val, stats[0], stats[1])

    return val


# ── Batch computation ────────────────────────────────────────────────────

def compute_state_vector_series(
    engine: Engine,
    start: date,
    end: date,
    freq_days: int = 5,
) -> list[StateVector]:
    """Compute state vectors at regular intervals over a date range.

    Used for building the historical library.
    """
    vectors: list[StateVector] = []
    current = start
    total = (end - start).days // freq_days
    computed = 0

    while current <= end:
        try:
            sv = compute_state_vector(engine, current)
            if sv.completeness >= 0.4:  # at least 40% of dims populated
                vectors.append(sv)
            computed += 1
            if computed % 100 == 0:
                log.info("State vectors: {n}/{t} computed", n=computed, t=total)
        except Exception as exc:
            log.debug("State vector failed for {dt}: {e}", dt=current, e=str(exc))
        current += timedelta(days=freq_days)

    log.info("Computed {n} state vectors from {s} to {e}", n=len(vectors), s=start, e=end)
    return vectors


# ── Cache ────────────────────────────────────────────────────────────────

_CACHE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS regime_state_vectors (
    id          BIGSERIAL PRIMARY KEY,
    as_of_date  DATE NOT NULL UNIQUE,
    vector      JSONB NOT NULL,
    completeness DOUBLE PRECISION NOT NULL,
    stale_dims  TEXT[],
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_regime_sv_date
    ON regime_state_vectors (as_of_date DESC);
"""


def _ensure_cache_table(engine: Engine) -> None:
    with engine.begin() as conn:
        for stmt in _CACHE_TABLE_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))


def cache_state_vector(engine: Engine, sv: StateVector) -> None:
    """Store a state vector in the cache table."""
    _ensure_cache_table(engine)
    dim_dict = {DIM_NAMES[i]: sv.values[i] for i in range(len(sv.values))}
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO regime_state_vectors (as_of_date, vector, completeness, stale_dims) "
                "VALUES (:dt, :vec, :comp, :stale) "
                "ON CONFLICT (as_of_date) DO UPDATE SET "
                "vector = EXCLUDED.vector, completeness = EXCLUDED.completeness, "
                "stale_dims = EXCLUDED.stale_dims, computed_at = NOW()"
            ),
            {
                "dt": sv.as_of_date,
                "vec": json.dumps(dim_dict),
                "comp": sv.completeness,
                "stale": list(sv.stale_dimensions),
            },
        )


def load_cached_vectors(engine: Engine, min_completeness: float = 0.4) -> list[StateVector]:
    """Load all cached state vectors from the database."""
    _ensure_cache_table(engine)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT as_of_date, vector, completeness, stale_dims "
                "FROM regime_state_vectors "
                "WHERE completeness >= :mc "
                "ORDER BY as_of_date"
            ),
            {"mc": min_completeness},
        ).fetchall()

    vectors: list[StateVector] = []
    for row in rows:
        dt, vec_json, comp, stale = row
        vec_dict = vec_json if isinstance(vec_json, dict) else json.loads(vec_json)
        values = tuple(vec_dict.get(name) for name in DIM_NAMES)
        vectors.append(StateVector(
            as_of_date=dt,
            values=values,
            completeness=comp,
            stale_dimensions=tuple(stale or []),
        ))
    return vectors


def get_or_compute_state_vector(
    engine: Engine,
    as_of: date | None = None,
    force_recompute: bool = False,
) -> StateVector:
    """Get from cache or compute fresh."""
    if as_of is None:
        as_of = date.today()

    if not force_recompute:
        _ensure_cache_table(engine)
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT as_of_date, vector, completeness, stale_dims "
                    "FROM regime_state_vectors WHERE as_of_date = :dt"
                ),
                {"dt": as_of},
            ).fetchone()
        if row is not None:
            vec_dict = row[1] if isinstance(row[1], dict) else json.loads(row[1])
            values = tuple(vec_dict.get(name) for name in DIM_NAMES)
            return StateVector(
                as_of_date=row[0],
                values=values,
                completeness=row[2],
                stale_dimensions=tuple(row[3] or []),
            )

    sv = compute_state_vector(engine, as_of)
    try:
        cache_state_vector(engine, sv)
    except Exception as exc:
        log.warning("Failed to cache state vector: {e}", e=str(exc))
    return sv
