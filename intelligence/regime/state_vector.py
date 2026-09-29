"""
State vector construction for the regime-matched analog engine.

Each state vector captures the macro environment at a point in time across
24 dimensions — VIX, rates, spreads, employment, liquidity, momentum, and
cross-reference divergence scores. All queries are PIT-correct (no look-ahead).

State vectors are cached in the `regime_state_vectors` table.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.observations import MixedSourceError, read_window


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


# ── Cadence-aware staleness (GRID-STALE-SOURCES-AUDIT-20260929.md §4) ────
#
# The old rule flagged a dimension stale when its latest obs was >30 days
# old, for every dimension regardless of publication cadence. FRED's
# monthly macro series carry obs_date = the 1st of the reference month but
# are published 4-8 weeks later, so a monthly dimension is *always* >30
# days old, even the same day GRID picks up its newest release (the audit
# measured a 4-minute-to-~51-hour GRID pickup lag across all five monthly
# series it checked -- the flag was 100% a false positive, never a real
# gap). These are the only monthly series among STATE_DIMENSIONS today;
# QUARTERLY_FRED_SERIES starts empty and is ready for one, same pattern.
MONTHLY_FRED_SERIES: frozenset[str] = frozenset({
    "UNRATE", "INDPRO", "TCU", "M2SL", "UMCSENT",
})
QUARTERLY_FRED_SERIES: frozenset[str] = frozenset()

DEFAULT_STALE_DAYS = 30
# ~70 days safely covers a monthly series' worst-case release lag (4-8
# weeks) plus GRID's own pickup delay, while still catching a release
# that's genuinely been missed (the next one is always <45 days away) --
# this is the audit's own suggested fix (§4: "monthly = stale only if obs
# is more than ~70 days old").
MONTHLY_STALE_DAYS = 70
# Same logic one tier out: a quarterly series can be published 1-3 months
# after quarter-end, so give it a proportionally larger window.
QUARTERLY_STALE_DAYS = 160


def _stale_threshold_days(series_id: str) -> int:
    """Cadence-aware staleness threshold, in days, for one series_id."""
    if series_id in MONTHLY_FRED_SERIES:
        return MONTHLY_STALE_DAYS
    if series_id in QUARTERLY_FRED_SERIES:
        return QUARTERLY_STALE_DAYS
    return DEFAULT_STALE_DAYS


# ── State Vector ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class StateVector:
    """Immutable macro state at a point in time."""
    as_of_date: date
    values: tuple[float | None, ...]    # one per dimension, None = missing
    completeness: float                 # fraction of non-null dims
    stale_dimensions: tuple[str, ...]   # dims with data >30d old
    # Which SPY price series fed the momentum/RSI dimensions: "spy_full"
    # (the resolved, post re-resolve feature) or the raw "YF:SPY:close"
    # fallback, or None when neither was available (see _fetch_spy_prices).
    price_basis: str | None = None
    # True when this vector was served from regime_state_vectors rather
    # than freshly computed. GET routes (persist=False) return cached=False
    # for an in-memory computation that was never written.
    cached: bool = False

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
            'price_basis': self.price_basis,
            'cached': self.cached,
        }


# ── Helpers ──────────────────────────────────────────────────────────────

def _fetch_series(engine: Engine, series_id: str, as_of: date, lookback_days: int = 2520) -> pd.Series:
    """Fetch series values up to as_of date (PIT-correct).

    Goes through ``store.observations.read_window``: SUCCESS-only, one row
    per ``obs_date`` (latest vintage wins), bounded by ``as_of`` — replacing
    the direct, un-collapsed ``raw_series`` read this module used to do (a
    revision day or a FAILED zero marker could otherwise leak in). A
    series_id whose rows span more than one source (see
    ``store.observations.MixedSourceError``) degrades to an empty series
    rather than silently mixing sources; the caller already treats a short
    or empty series as "dimension unavailable".
    """
    cutoff = as_of - timedelta(days=lookback_days)
    try:
        with engine.connect() as conn:
            obs = read_window(conn, series_id, start=cutoff, as_of=as_of)
    except MixedSourceError as exc:
        log.warning("state_vector: {sid} is mixed-source, skipping: {e}", sid=series_id, e=str(exc))
        return pd.Series(dtype=float)
    if not obs:
        return pd.Series(dtype=float)
    return pd.Series(
        {o.obs_date: o.value for o in obs},
        dtype=float,
    ).sort_index()


def _fetch_resolved_spy_full(engine: Engine, as_of: date, cutoff: date) -> pd.Series | None:
    """Best-effort PIT read of the resolved ``spy_full`` feature.

    Mirrors ``alpha_research.realized_alpha.resolve_spy_feature`` +
    ``load_price_path`` (the re-resolve's own PIT reader, through
    ``store.pit.PITStore`` — ``LATEST_AS_OF``, retraction-aware). Returns
    ``None`` — never raises — when ``feature_registry``/``resolved_series``
    aren't there yet (the re-resolve hasn't landed) or the query fails for
    any other reason, so the caller can degrade to the raw YF series
    instead of crashing or assuming infra state this module can't verify.
    """
    try:
        from alpha_research.realized_alpha import load_price_path, resolve_spy_feature

        feature_id, _name = resolve_spy_feature(engine)
        series = load_price_path(engine, feature_id, cutoff, as_of, as_of)
    except Exception as exc:
        log.debug("state_vector: resolved spy_full unavailable ({e}); falling back to raw YF series", e=str(exc))
        return None
    if series is None or series.empty:
        return None
    idx = [d.date() if hasattr(d, "date") else d for d in series.index]
    return pd.Series(series.to_numpy(dtype=float), index=idx, dtype=float).sort_index()


def _fetch_spy_prices(engine: Engine, as_of: date, lookback_days: int = 504) -> tuple[pd.Series, str | None]:
    """SPY close series for momentum/RSI computation, PIT-correct.

    Prefers the resolved ``spy_full`` feature (the post re-resolve price
    basis, with retractions honoured) so momentum/RSI agree with the rest
    of the platform. Falls back to the raw ``YF:SPY:close`` observation
    series (SUCCESS-only, vintage-collapsed, ``source="yfinance"`` per
    ``store.observations``'s mixed-source rule) when the resolved feature
    isn't available yet. Returns ``(empty series, None)`` — an honest
    "unavailable", not a crash or a silent stale read — when neither path
    has data.

    Returns ``(prices, price_basis)`` where ``price_basis`` is
    ``"spy_full"``, ``"YF:SPY:close"``, or ``None``.
    """
    cutoff = as_of - timedelta(days=lookback_days)

    resolved = _fetch_resolved_spy_full(engine, as_of, cutoff)
    if resolved is not None and not resolved.empty:
        return resolved, "spy_full"

    try:
        with engine.connect() as conn:
            obs = read_window(conn, "YF:SPY:close", source="yfinance", start=cutoff, as_of=as_of)
    except Exception as exc:
        log.debug("state_vector: raw SPY:close fallback failed: {e}", e=str(exc))
        return pd.Series(dtype=float), None
    if not obs:
        return pd.Series(dtype=float), None
    series = pd.Series({o.obs_date: o.value for o in obs}, dtype=float).sort_index()
    return series, "YF:SPY:close"


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


# One batched, vintage-collapsed read of every INSIDER:* series in the
# window, replacing a per-series_id loop over store.observations.read_window
# (PR #713 review: ~1,548 distinct INSIDER:* series active in a 30-day
# window meant ~1,500 round trips per uncached GET). Semantics preserved
# exactly: pull_status='SUCCESS' only, bounded by [cutoff, as_of], one row
# per (series_id, obs_date) — the latest pull_timestamp wins via
# ROW_NUMBER() (portable to SQLite and Postgres, unlike DISTINCT ON) — and
# a series_id whose accepted rows span more than one source_id is excluded
# entirely (the same fail-closed rule store.observations.MixedSourceError
# enforces one series at a time), not silently mixed into the sum.
_INSIDER_SENTIMENT_SQL = text(
    "WITH candidates AS ("
    "  SELECT series_id, obs_date, value, pull_timestamp, source_id"
    "  FROM raw_series"
    "  WHERE series_id LIKE :insider_pat AND pull_status = 'SUCCESS'"
    "    AND obs_date >= :cutoff AND obs_date <= :as_of"
    "),"
    "mixed_source AS ("
    "  SELECT series_id FROM candidates"
    "  GROUP BY series_id HAVING COUNT(DISTINCT source_id) > 1"
    "),"
    "ranked AS ("
    "  SELECT series_id, value,"
    "         ROW_NUMBER() OVER ("
    "             PARTITION BY series_id, obs_date ORDER BY pull_timestamp DESC"
    "         ) AS rn"
    "  FROM candidates"
    "  WHERE series_id NOT IN (SELECT series_id FROM mixed_source)"
    ")"
    "SELECT"
    "  SUM(CASE WHEN series_id LIKE :buy_pat THEN value ELSE 0 END),"
    "  SUM(CASE WHEN series_id LIKE :sell_pat THEN value ELSE 0 END)"
    "FROM ranked WHERE rn = 1"
)


def _get_insider_sentiment(engine: Engine, as_of: date) -> float | None:
    """Net insider sentiment from SEC Form 4 filings (30d window), PIT.

    A single query (``_INSIDER_SENTIMENT_SQL``) reads every
    ``INSIDER:{ticker}:{insider_name}:{BUY|SELL}`` series in the window at
    once: SUCCESS-only, one row per ``(series_id, obs_date)`` (latest
    ``pull_timestamp`` wins), a mixed-source series_id excluded rather than
    mixed in, then summed by the ``:BUY``/``:SELL`` suffix. This is the
    batched equivalent of calling ``store.observations.read_window`` once
    per series_id and summing the results — same filters, same vintage
    rule, one round trip instead of one per series.
    """
    cutoff = as_of - timedelta(days=30)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                _INSIDER_SENTIMENT_SQL,
                {
                    "insider_pat": "INSIDER:%", "buy_pat": "%:BUY", "sell_pat": "%:SELL",
                    "cutoff": cutoff, "as_of": as_of,
                },
            ).fetchone()
    except Exception as exc:
        log.debug("insider sentiment: batched read failed: {e}", e=str(exc))
        return None

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
    spy_prices, price_basis = _fetch_spy_prices(engine, as_of)
    values: list[float | None] = []
    stale: list[str] = []

    for dim in STATE_DIMENSIONS:
        try:
            val = _compute_dimension(engine, dim, as_of, norm_stats, spy_prices)
            values.append(val)

            # Check staleness for non-derived series. The lookback here must
            # reach back at least as far as this series' own stale
            # threshold (70d monthly / 160d quarterly can both exceed a
            # fixed 60d window) -- otherwise a series stale by MORE than the
            # lookback silently returns an empty read here and is never
            # flagged at all, regardless of the threshold comparison below.
            # +60 is a buffer past the threshold itself so a series that's
            # freshly crossed into "stale" is still found, not just one
            # sitting exactly at the edge.
            stale_lookback_days = max(60, _stale_threshold_days(dim.series_id) + 60)
            if not dim.series_id.startswith('DERIVED:') and val is not None:
                series = _fetch_series(engine, dim.series_id, as_of, lookback_days=stale_lookback_days)
                if len(series) > 0:
                    latest_date = series.index[-1]
                    if hasattr(latest_date, 'date'):
                        latest_date = latest_date
                    days_stale = (as_of - latest_date).days if isinstance(latest_date, date) else 30
                    if days_stale > _stale_threshold_days(dim.series_id):
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
        price_basis=price_basis,
        cached=False,
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
            if sv.completeness >= MIN_CACHE_COMPLETENESS:
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
    """Store a state vector in the cache table.

    The only callers of this function are the nightly job
    (``scripts/run_regime_state_vectors.py``) and, indirectly,
    ``get_or_compute_state_vector(..., persist=True)`` — never a GET route.
    ``price_basis`` rides along inside the existing ``vector`` JSONB column
    under a reserved key rather than a new column, so no migration is
    needed and the 1,927 pre-existing rows (computed before this field
    existed) are read back with ``price_basis=None``, unchanged.
    """
    _ensure_cache_table(engine)
    dim_dict = {DIM_NAMES[i]: sv.values[i] for i in range(len(sv.values))}
    if sv.price_basis is not None:
        dim_dict["__price_basis__"] = sv.price_basis
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


# Minimum completeness for a vector to be cached, and for a cached row to be
# served instead of recomputed. Same floor load_cached_vectors() and
# compute_state_vector_series() already apply when building the library.
MIN_CACHE_COMPLETENESS = 0.4


def _row_to_state_vector(row: tuple, *, cached: bool) -> StateVector:
    dt, vec_json, comp, stale = row
    vec_dict = vec_json if isinstance(vec_json, dict) else json.loads(vec_json)
    values = tuple(vec_dict.get(name) for name in DIM_NAMES)
    return StateVector(
        as_of_date=dt,
        values=values,
        completeness=comp,
        stale_dimensions=tuple(stale or []),
        price_basis=vec_dict.get("__price_basis__"),
        cached=cached,
    )


def load_cached_vectors(engine: Engine, min_completeness: float = MIN_CACHE_COMPLETENESS) -> list[StateVector]:
    """Load all cached state vectors from the database. Read-only.

    Deliberately does not call ``_ensure_cache_table`` — this is a GET-path
    reader (``/regime/history`` and the analog library build). A missing
    table means "no vectors yet", not "create it now"; it degrades to an
    empty list rather than issuing DDL. Excludes any future-dated row
    defensively, even if one exists (it never should — see
    ``get_or_compute_state_vector``).
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT as_of_date, vector, completeness, stale_dims "
                    "FROM regime_state_vectors "
                    "WHERE completeness >= :mc AND as_of_date <= :today "
                    "ORDER BY as_of_date"
                ),
                {"mc": min_completeness, "today": date.today()},
            ).fetchall()
    except Exception as exc:
        log.debug("load_cached_vectors: cache table unavailable: {e}", e=str(exc))
        return []

    return [_row_to_state_vector(row, cached=True) for row in rows]


def _read_cached_row(engine: Engine, as_of: date, today: date) -> tuple | None:
    """Best-effort read of one cached row for ``as_of``, or ``None``.

    Never issues DDL: a missing table (or any other read failure) is
    treated as "no cached row", not created. ``as_of_date <= :today`` is a
    defensive belt-and-suspenders filter — the caller already refuses to
    cache or serve a future ``as_of`` — so a future-dated row can never be
    served even if one somehow ended up in the table.
    """
    try:
        with engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT as_of_date, vector, completeness, stale_dims "
                    "FROM regime_state_vectors "
                    "WHERE as_of_date = :dt AND as_of_date <= :today"
                ),
                {"dt": as_of, "today": today},
            ).fetchone()
    except Exception as exc:
        log.debug("state_vector: cache read unavailable: {e}", e=str(exc))
        return None


def get_or_compute_state_vector(
    engine: Engine,
    as_of: date | None = None,
    force_recompute: bool = False,
    persist: bool = True,
) -> StateVector:
    """Get from cache, or compute fresh.

    persist: True (default) is the nightly job's contract
        (``scripts/run_regime_state_vectors.py``) — it may ensure the cache
        table exists and, if the computed vector clears
        ``MIN_CACHE_COMPLETENESS``, write it. GET-triggered callers (the
        ``/regime`` and ``/regime/analogs`` routes) MUST pass
        ``persist=False``: with that, this function issues no DDL and calls
        ``cache_state_vector`` under no circumstance. A cache hit is still
        served (``cached=True``); a miss is computed in memory and returned
        with ``cached=False`` — never written.

    A future ``as_of`` (no observations exist yet) is always computed in
    memory only, regardless of ``persist`` — it is never read from or
    written to the cache.
    """
    today = date.today()
    if as_of is None:
        as_of = today

    if as_of > today:
        log.warning(
            "state_vector: as_of {d} is in the future — computing in memory only, never cached",
            d=as_of,
        )
        return compute_state_vector(engine, as_of)

    if not force_recompute:
        if persist:
            _ensure_cache_table(engine)
        row = _read_cached_row(engine, as_of, today)
        if row is not None and row[2] is not None and row[2] >= MIN_CACHE_COMPLETENESS:
            return _row_to_state_vector(row, cached=True)

    sv = compute_state_vector(engine, as_of)
    # Never pin a mostly-empty vector (e.g. every dimension query timed out)
    # as the day's cached state: it would be served for the rest of the day
    # instead of being recomputed, and load_cached_vectors() filters it out
    # of the analog library anyway.
    if sv.completeness < MIN_CACHE_COMPLETENESS:
        log.warning(
            "State vector for {d} only {c:.0%} complete — not cached",
            d=as_of, c=sv.completeness,
        )
        return sv
    if not persist:
        return sv
    try:
        cache_state_vector(engine, sv)
    except Exception as exc:
        log.warning("Failed to cache state vector: {e}", e=str(exc))
    return sv
