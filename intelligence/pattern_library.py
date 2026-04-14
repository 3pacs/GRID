"""
GRID Pattern Library — Independent Conviction Layer via Historical Analog Matching.

Encodes the current market state as a 12-dim vector, finds the K=50 nearest
historical analogs by cosine similarity, returns the empirical distribution of
forward outcomes. The "this setup looks like 47 prior setups, played out
positively 68% of the time" read.

This complements the oracle's vote-weighted prediction by asking the
base-rate question directly: given today's exact constellation of macro,
liquidity, sentiment, and flow signals, when have we seen this before, and
what happened next? Base-rate reasoning is the cleanest, most interpretable
confidence layer — robust to model overfitting because the historical
analogs reflect true empirical frequencies, not learned weights.

Pipeline:
    build_state_vector(engine, today)
        -> query_historical_states(engine, lookback_days=1825)
        -> find_nearest_analogs(query_vec, history, k=50)
        -> read_forward_returns(engine, ticker, dates)
        -> compute_base_rate(returns) for each horizon
        -> PatternMatchReport with analogs + per-horizon base rates
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ─────────────────────────────────────────────────────────────

STATE_VECTOR_DIM: int = 12
DEFAULT_K_NEAREST: int = 50
MIN_ANALOGS_FOR_BASE_RATE: int = 20
LOOKBACK_DAYS_DEFAULT: int = 1825  # ~5 years
FORWARD_HORIZONS_DAYS: tuple[int, ...] = (1, 7, 30, 90)

# Win/loss/flat classification threshold (in pct points). A forward return
# whose absolute value is below this threshold is classified as "flat".
DEFAULT_OUTCOME_THRESHOLD_PCT: float = 0.5

# Confidence dampening: linear ramp from MIN_ANALOGS_FOR_BASE_RATE to this
# saturation count. Below MIN, signal is 0; at SATURATION, dampening = 1.
CONFIDENCE_SATURATION_COUNT: int = 100

# Z-score lookback for normalization (matches cross_reference.py)
ZSCORE_LOOKBACK_DAYS: int = 504
MIN_OBSERVATIONS_FOR_ZSCORE: int = 20

# Liquidity state ordering (ALPHA-5 contract)
LIQUIDITY_STATE_ORDERING: dict[str, int] = {
    "CRISIS": -2,
    "TIGHTENING": -1,
    "NEUTRAL": 0,
    "EXPANSION": 1,
    "EXPANSION_STRONG": 2,
}

# State vector feature specifications. Each spec tells the builder which
# series_id to read and how to normalize. The order is load-bearing — it
# defines the dimensions of every MarketStateVector and historical state.
STATE_VECTOR_FEATURES: tuple[dict[str, Any], ...] = (
    {
        "name": "fci_score_clamped",
        "series_id": "GRID_FCI_COMPOSITE",
        "kind": "clamp_div",
        "lo": -3.0,
        "hi": 3.0,
        "scale": 3.0,
    },
    {
        "name": "liquidity_state_ordinal",
        "series_id": "ALPHA5_LIQUIDITY_STATE",
        "kind": "ordinal",
        "ordering": LIQUIDITY_STATE_ORDERING,
        "scale": 2,
    },
    {
        "name": "yield_curve_slope_pct",
        "series_id": "T10Y2Y",
        "kind": "clamp_div",
        "lo": -2.0,
        "hi": 2.0,
        "scale": 2.0,
    },
    {
        "name": "vix_level_z",
        "series_id": "VIXCLS",
        "kind": "zscore",
        "clamp": 3.0,
    },
    {
        "name": "credit_spread_bbb_z",
        "series_id": "BAMLC0A4CBBB",
        "kind": "zscore",
        "clamp": 3.0,
    },
    {
        "name": "dxy_z",
        "series_id": "DTWEXBGS",
        "kind": "zscore",
        "clamp": 3.0,
    },
    {
        "name": "sector_breadth",
        "series_id": "GRID_SECTOR_BREADTH_200D",
        "kind": "minmax",
        "lo": 0.0,
        "hi": 1.0,
    },
    {
        "name": "put_call_ratio_z",
        "series_id": "CBOE_EQUITY_PUT_CALL",
        "kind": "zscore",
        "clamp": 3.0,
    },
    {
        "name": "insider_buy_sell_z",
        "series_id": "GRID_INSIDER_NET_FLOW",
        "kind": "zscore",
        "clamp": 3.0,
    },
    {
        "name": "congress_buy_sell_z",
        "series_id": "GRID_CONGRESS_NET_FLOW",
        "kind": "zscore",
        "clamp": 3.0,
    },
    {
        "name": "retail_options_pulse_z",
        "series_id": "GRID_REDDIT_OPTIONS_PULSE",
        "kind": "zscore",
        "clamp": 3.0,
    },
    {
        "name": "shipping_fudge_alert_count",
        "series_id": "GRID_SHIPPING_FUDGE_ALERTS",
        "kind": "clamp_div",
        "lo": 0.0,
        "hi": 5.0,
        "scale": 5.0,
    },
)

assert len(STATE_VECTOR_FEATURES) == STATE_VECTOR_DIM, (
    "STATE_VECTOR_FEATURES length must equal STATE_VECTOR_DIM"
)


# ── Data Classes ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class MarketStateVector:
    """Snapshot of market state encoded as a fixed-length normalized vector."""

    as_of: date
    vector: tuple[float, ...]
    feature_names: tuple[str, ...]
    coverage: float  # fraction of features that successfully loaded

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "vector": list(self.vector),
            "feature_names": list(self.feature_names),
            "coverage": self.coverage,
        }


@dataclass(frozen=True)
class HistoricalAnalog:
    """A single past date that resembles the current state."""

    date: date
    similarity: float  # cosine similarity, in [-1, 1]
    forward_returns: dict[int, float]  # horizon_days -> forward return (pct)
    forward_outcomes: dict[int, str]  # horizon_days -> 'win' / 'loss' / 'flat'

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date.isoformat(),
            "similarity": self.similarity,
            "forward_returns": {str(k): v for k, v in self.forward_returns.items()},
            "forward_outcomes": {str(k): v for k, v in self.forward_outcomes.items()},
        }


@dataclass(frozen=True)
class BaseRateDistribution:
    """Empirical distribution of forward outcomes among matched analogs."""

    horizon_days: int
    n_analogs: int
    win_pct: float
    loss_pct: float
    flat_pct: float
    median_return_pct: float
    p05_return_pct: float
    p95_return_pct: float
    mean_return_pct: float
    std_return_pct: float
    sufficient_sample: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PatternMatchReport:
    """Full analog-matching report for a query state."""

    ticker: str | None
    query_date: date
    query_state: MarketStateVector
    analog_count: int
    top_k_analogs: tuple[HistoricalAnalog, ...]
    base_rates: dict[int, BaseRateDistribution]
    confidence_signal: float  # in [0, 1]
    generated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "query_date": self.query_date.isoformat(),
            "query_state": self.query_state.to_dict(),
            "analog_count": self.analog_count,
            "top_k_analogs": [a.to_dict() for a in self.top_k_analogs],
            "base_rates": {str(k): v.to_dict() for k, v in self.base_rates.items()},
            "confidence_signal": self.confidence_signal,
            "generated_at": self.generated_at,
        }


# ── Pure helpers (math + normalization) ───────────────────────────────────

def cosine_similarity(a: tuple[float, ...], b: tuple[float, ...]) -> float:
    """Cosine similarity between two equal-length vectors.

    Formula: dot(a, b) / (||a|| * ||b||).
    Returns 0.0 when either vector has zero norm or the vectors differ in
    length. Result is in [-1.0, 1.0] for non-degenerate inputs.
    """
    if len(a) != len(b) or not a:
        return 0.0
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for x, y in zip(a, b):
        dot += x * y
        norm_a += x * x
        norm_b += y * y
    if norm_a <= 0.0 or norm_b <= 0.0:
        return 0.0
    denom = math.sqrt(norm_a) * math.sqrt(norm_b)
    if denom <= 0.0:
        return 0.0
    return dot / denom


def normalize_zscore(
    value: float | None,
    mean: float,
    std: float,
    clamp: float = 3.0,
) -> float:
    """Convert a raw value to a clamped z-score, divided by clamp.

    Returns 0.0 if value is None or std is non-positive.
    Output is in [-1.0, 1.0].
    """
    if value is None or std is None or std <= 0.0:
        return 0.0
    z = (value - mean) / std
    if z > clamp:
        z = clamp
    elif z < -clamp:
        z = -clamp
    return z / clamp if clamp > 0 else 0.0


def normalize_minmax(
    value: float | None,
    lo: float,
    hi: float,
) -> float:
    """Map a value into [0, 1] via min-max scaling.

    Returns 0.5 if value is None (neutral default).
    Values below lo clamp to 0.0; values above hi clamp to 1.0.
    """
    if value is None:
        return 0.5
    if hi <= lo:
        return 0.5
    if value <= lo:
        return 0.0
    if value >= hi:
        return 1.0
    return (value - lo) / (hi - lo)


def normalize_clamp_div(
    value: float | None,
    lo: float,
    hi: float,
    scale: float,
) -> float:
    """Clamp a value into [lo, hi] then divide by scale.

    Returns 0.0 if value is None or scale is zero.
    """
    if value is None or scale == 0.0:
        return 0.0
    if value > hi:
        value = hi
    elif value < lo:
        value = lo
    return value / scale


def normalize_ordinal(
    value: str | None,
    ordering: dict[str, int],
    scale: int,
) -> float:
    """Map a categorical label to its ordinal rank divided by scale.

    Returns 0.0 if value is None, missing from ordering, or scale is zero.
    """
    if value is None or scale == 0:
        return 0.0
    if value not in ordering:
        return 0.0
    return ordering[value] / scale


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Linear-interpolation percentile of a pre-sorted list."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    if pct <= 0.0:
        return sorted_values[0]
    if pct >= 1.0:
        return sorted_values[-1]
    rank = pct * (len(sorted_values) - 1)
    lo_idx = int(math.floor(rank))
    hi_idx = int(math.ceil(rank))
    if lo_idx == hi_idx:
        return sorted_values[lo_idx]
    frac = rank - lo_idx
    return sorted_values[lo_idx] * (1.0 - frac) + sorted_values[hi_idx] * frac


def compute_base_rate(
    returns: list[float],
    threshold_pct: float = DEFAULT_OUTCOME_THRESHOLD_PCT,
    horizon_days: int = 0,
) -> BaseRateDistribution:
    """Compute win/loss/flat distribution and percentile stats.

    A return r is classified as:
        win   if r >  +threshold_pct
        loss  if r <  -threshold_pct
        flat  otherwise (|r| <= threshold_pct, or NaN)

    NaN returns are excluded from percentile/mean/std stats but counted as
    flat for the win/loss/flat tally to avoid dropping data silently.
    """
    if not returns:
        return BaseRateDistribution(
            horizon_days=horizon_days,
            n_analogs=0,
            win_pct=0.0,
            loss_pct=0.0,
            flat_pct=0.0,
            median_return_pct=0.0,
            p05_return_pct=0.0,
            p95_return_pct=0.0,
            mean_return_pct=0.0,
            std_return_pct=0.0,
            sufficient_sample=False,
        )

    valid = [r for r in returns if r is not None and not math.isnan(r)]
    n_total = len(returns)

    wins = sum(1 for r in valid if r > threshold_pct)
    losses = sum(1 for r in valid if r < -threshold_pct)
    flats = n_total - wins - losses

    if valid:
        sorted_vals = sorted(valid)
        median = _percentile(sorted_vals, 0.5)
        p05 = _percentile(sorted_vals, 0.05)
        p95 = _percentile(sorted_vals, 0.95)
        mean = sum(valid) / len(valid)
        if len(valid) > 1:
            var = sum((x - mean) ** 2 for x in valid) / (len(valid) - 1)
            std = math.sqrt(var)
        else:
            std = 0.0
    else:
        median = p05 = p95 = mean = std = 0.0

    return BaseRateDistribution(
        horizon_days=horizon_days,
        n_analogs=n_total,
        win_pct=wins / n_total if n_total > 0 else 0.0,
        loss_pct=losses / n_total if n_total > 0 else 0.0,
        flat_pct=flats / n_total if n_total > 0 else 0.0,
        median_return_pct=median,
        p05_return_pct=p05,
        p95_return_pct=p95,
        mean_return_pct=mean,
        std_return_pct=std,
        sufficient_sample=n_total >= MIN_ANALOGS_FOR_BASE_RATE,
    )


def find_nearest_analogs(
    query_vector: tuple[float, ...],
    historical_states: list[tuple[date, tuple[float, ...]]],
    k: int = DEFAULT_K_NEAREST,
) -> list[tuple[date, float]]:
    """Pure: return top-k (date, similarity) pairs sorted by similarity desc."""
    if not historical_states or k <= 0:
        return []
    scored = [
        (d, cosine_similarity(query_vector, vec))
        for d, vec in historical_states
        if len(vec) == len(query_vector)
    ]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:k]


def confidence_signal_from_base_rates(
    base_rates: dict[int, BaseRateDistribution],
    horizon: int = 7,
) -> float:
    """Convert a horizon's win_pct into a [0, 1] conviction signal.

    Dampening curve (linear ramp on sample size):
        n < MIN_ANALOGS_FOR_BASE_RATE             -> 0.0
        MIN_ANALOGS <= n <= CONFIDENCE_SATURATION -> linear ramp 0..1
        n > CONFIDENCE_SATURATION                 -> full strength

    Final signal = win_pct * dampening_factor.
    """
    dist = base_rates.get(horizon)
    if dist is None or dist.n_analogs < MIN_ANALOGS_FOR_BASE_RATE:
        return 0.0
    span = max(1, CONFIDENCE_SATURATION_COUNT - MIN_ANALOGS_FOR_BASE_RATE)
    raw_factor = (dist.n_analogs - MIN_ANALOGS_FOR_BASE_RATE) / span
    dampening = max(0.0, min(1.0, raw_factor))
    return max(0.0, min(1.0, dist.win_pct * dampening))


# ── DB-touching helpers ───────────────────────────────────────────────────

def _safe_get_history(
    engine: Engine,
    series_id: str,
    lookback_days: int = ZSCORE_LOOKBACK_DAYS,
    as_of: date | None = None,
) -> pd.Series:
    """Read a raw_series time series, never raising on failure."""
    if engine is None:
        return pd.Series(dtype=float)
    try:
        end_date = as_of if as_of is not None else date.today()
        cutoff = end_date - timedelta(days=lookback_days)
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT obs_date, value FROM raw_series "
                    "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                    "AND obs_date >= :cutoff AND obs_date <= :asof "
                    "ORDER BY obs_date"
                ),
                {"sid": series_id, "cutoff": cutoff, "asof": end_date},
            ).fetchall()
        if not rows:
            return pd.Series(dtype=float)
        return pd.Series({r[0]: r[1] for r in rows}).sort_index()
    except Exception as exc:  # noqa: BLE001
        log.debug(f"pattern_library: history read failed for {series_id}: {exc}")
        return pd.Series(dtype=float)


def _normalize_feature(spec: dict[str, Any], series: pd.Series) -> tuple[float, bool]:
    """Apply the spec's normalization to the most recent value of a series.

    Returns (normalized_value, loaded_flag). loaded_flag is False when the
    series is empty so we can compute coverage.
    """
    if series is None or series.empty:
        kind = spec.get("kind")
        if kind == "minmax":
            return 0.5, False
        return 0.0, False

    kind = spec.get("kind")

    if kind == "zscore":
        # Numeric values only
        numeric = pd.to_numeric(series, errors="coerce").dropna()
        if numeric.empty:
            return 0.0, False
        latest_val = float(numeric.iloc[-1])
        if len(numeric) < MIN_OBSERVATIONS_FOR_ZSCORE:
            return 0.0, True  # loaded but undefined z; treat as neutral
        mean = float(numeric.mean())
        std = float(numeric.std(ddof=0))
        return normalize_zscore(latest_val, mean, std, clamp=spec.get("clamp", 3.0)), True

    if kind == "minmax":
        numeric = pd.to_numeric(series, errors="coerce").dropna()
        if numeric.empty:
            return 0.5, False
        latest_val = float(numeric.iloc[-1])
        return normalize_minmax(latest_val, spec["lo"], spec["hi"]), True

    if kind == "clamp_div":
        numeric = pd.to_numeric(series, errors="coerce").dropna()
        if numeric.empty:
            return 0.0, False
        latest_val = float(numeric.iloc[-1])
        return normalize_clamp_div(
            latest_val,
            spec["lo"],
            spec["hi"],
            spec["scale"],
        ), True

    if kind == "ordinal":
        # Series may contain string labels; pick the latest non-null value
        non_null = series.dropna()
        if non_null.empty:
            return 0.0, False
        latest_label = str(non_null.iloc[-1])
        return normalize_ordinal(
            latest_label,
            spec["ordering"],
            spec["scale"],
        ), True

    return 0.0, False


def build_state_vector(engine: Engine, as_of: date) -> MarketStateVector:
    """Read every feature from raw_series and assemble a normalized vector.

    Never raises — degrades to all-zero defaults on engine failure.
    """
    values: list[float] = []
    names: list[str] = []
    loaded_flags: list[bool] = []

    for spec in STATE_VECTOR_FEATURES:
        names.append(spec["name"])
        try:
            series = _safe_get_history(
                engine,
                spec["series_id"],
                lookback_days=ZSCORE_LOOKBACK_DAYS,
                as_of=as_of,
            )
            value, loaded = _normalize_feature(spec, series)
        except Exception as exc:  # noqa: BLE001
            log.debug(f"pattern_library: feature {spec['name']} failed: {exc}")
            value, loaded = 0.0, False
        values.append(value)
        loaded_flags.append(loaded)

    coverage = sum(1 for f in loaded_flags if f) / max(1, len(loaded_flags))

    return MarketStateVector(
        as_of=as_of,
        vector=tuple(values),
        feature_names=tuple(names),
        coverage=coverage,
    )


def query_historical_states(
    engine: Engine,
    lookback_days: int = LOOKBACK_DAYS_DEFAULT,
    as_of: date | None = None,
) -> list[tuple[date, tuple[float, ...]]]:
    """Build a daily history of state vectors over the lookback window.

    Reads each feature once, aligns by date, then assembles one vector per
    business day. Idempotent — pure read, no writes.
    """
    if engine is None:
        return []

    end_date = as_of if as_of is not None else date.today()
    start_date = end_date - timedelta(days=lookback_days)

    feature_series: list[pd.Series] = []
    for spec in STATE_VECTOR_FEATURES:
        try:
            series = _safe_get_history(
                engine,
                spec["series_id"],
                lookback_days=lookback_days,
                as_of=end_date,
            )
        except Exception:  # noqa: BLE001
            series = pd.Series(dtype=float)
        feature_series.append(series)

    # Union of all dates seen across features within the window
    all_dates: set[date] = set()
    for s in feature_series:
        for d in s.index:
            if isinstance(d, date) and start_date <= d <= end_date:
                all_dates.add(d)

    if not all_dates:
        return []

    sorted_dates = sorted(all_dates)

    # For each date, take the most-recent-on-or-before value per feature and
    # normalize via the spec. This gives a forward-fill style state vector.
    history: list[tuple[date, tuple[float, ...]]] = []
    for d in sorted_dates:
        vec: list[float] = []
        for spec, series in zip(STATE_VECTOR_FEATURES, feature_series):
            try:
                truncated = series[series.index <= d]
                if truncated.empty:
                    val, _ = _normalize_feature(spec, truncated)
                else:
                    # For zscore features we want full-window stats not just
                    # the truncation, so pass truncated; this avoids
                    # lookahead by only seeing data up to date d.
                    val, _ = _normalize_feature(spec, truncated)
            except Exception:  # noqa: BLE001
                val = 0.0
            vec.append(val)
        history.append((d, tuple(vec)))

    return history


def read_forward_returns(
    engine: Engine,
    ticker: str | None,
    dates: list[date],
    horizons: tuple[int, ...] = FORWARD_HORIZONS_DAYS,
) -> dict[date, dict[int, float]]:
    """Read forward returns at canonical horizons for a list of base dates.

    ticker=None falls back to SPY. Never raises — empty dict on failure.
    """
    if not dates:
        return {}
    symbol = (ticker or "SPY").upper()

    if engine is None:
        return {d: {h: float("nan") for h in horizons} for d in dates}

    out: dict[date, dict[int, float]] = {}
    try:
        with engine.connect() as conn:
            for d in dates:
                row_returns: dict[int, float] = {}
                for h in horizons:
                    target = d + timedelta(days=h)
                    try:
                        result = conn.execute(
                            text(
                                "SELECT base.close, fwd.close "
                                "FROM ohlcv base "
                                "JOIN ohlcv fwd ON fwd.ticker = base.ticker "
                                "WHERE base.ticker = :tkr AND base.obs_date = :base_d "
                                "AND fwd.obs_date = ("
                                "  SELECT MIN(obs_date) FROM ohlcv "
                                "  WHERE ticker = :tkr AND obs_date >= :target_d"
                                ") "
                                "LIMIT 1"
                            ),
                            {"tkr": symbol, "base_d": d, "target_d": target},
                        ).fetchone()
                        if result and result[0] and result[1]:
                            base_px = float(result[0])
                            fwd_px = float(result[1])
                            if base_px > 0:
                                row_returns[h] = (fwd_px - base_px) / base_px * 100.0
                            else:
                                row_returns[h] = float("nan")
                        else:
                            row_returns[h] = float("nan")
                    except Exception:  # noqa: BLE001
                        row_returns[h] = float("nan")
                out[d] = row_returns
    except Exception as exc:  # noqa: BLE001
        log.debug(f"pattern_library: forward-return read failed: {exc}")
        return {d: {h: float("nan") for h in horizons} for d in dates}

    return out


def _classify_outcome(ret_pct: float, threshold_pct: float) -> str:
    """Classify a single return as win/loss/flat (NaN -> flat)."""
    if ret_pct is None or math.isnan(ret_pct):
        return "flat"
    if ret_pct > threshold_pct:
        return "win"
    if ret_pct < -threshold_pct:
        return "loss"
    return "flat"


def build_pattern_match_report(
    engine: Engine,
    ticker: str | None = None,
    as_of: date | None = None,
    k: int = DEFAULT_K_NEAREST,
    threshold_pct: float = DEFAULT_OUTCOME_THRESHOLD_PCT,
) -> PatternMatchReport:
    """End-to-end: state vector -> nearest analogs -> base rates -> report.

    Never raises. On any failure path returns a degraded report with
    confidence_signal=0.0 so the oracle can fall back gracefully.
    """
    query_date = as_of if as_of is not None else date.today()
    generated_at = datetime.now(timezone.utc).isoformat()

    # 1. Encode current state
    try:
        query_state = build_state_vector(engine, query_date)
    except Exception as exc:  # noqa: BLE001
        log.warning(f"pattern_library: build_state_vector failed: {exc}")
        query_state = MarketStateVector(
            as_of=query_date,
            vector=tuple([0.0] * STATE_VECTOR_DIM),
            feature_names=tuple(s["name"] for s in STATE_VECTOR_FEATURES),
            coverage=0.0,
        )

    # 2. Build historical library
    try:
        history = query_historical_states(
            engine,
            lookback_days=LOOKBACK_DAYS_DEFAULT,
            as_of=query_date,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(f"pattern_library: query_historical_states failed: {exc}")
        history = []

    # Exclude the query date itself so we don't pick "today" as its own analog
    history = [(d, v) for d, v in history if d < query_date]

    # 3. Nearest analogs by cosine similarity
    nearest = find_nearest_analogs(query_state.vector, history, k=k)

    # 4. Forward returns for those analog dates
    analog_dates = [d for d, _ in nearest]
    forward_lookup = read_forward_returns(engine, ticker, analog_dates)

    # 5. Build per-horizon return lists and HistoricalAnalog records
    horizon_returns: dict[int, list[float]] = {h: [] for h in FORWARD_HORIZONS_DAYS}
    analog_records: list[HistoricalAnalog] = []

    for d, sim in nearest:
        per_horizon = forward_lookup.get(d, {h: float("nan") for h in FORWARD_HORIZONS_DAYS})
        outcomes: dict[int, str] = {}
        for h in FORWARD_HORIZONS_DAYS:
            r = per_horizon.get(h, float("nan"))
            horizon_returns[h].append(r)
            outcomes[h] = _classify_outcome(r, threshold_pct)
        analog_records.append(
            HistoricalAnalog(
                date=d,
                similarity=sim,
                forward_returns=dict(per_horizon),
                forward_outcomes=outcomes,
            )
        )

    # 6. Compute per-horizon base rates (filter NaN out of the percentile
    #    stats but keep total count for the win/loss/flat tally)
    base_rates: dict[int, BaseRateDistribution] = {}
    for h in FORWARD_HORIZONS_DAYS:
        base_rates[h] = compute_base_rate(
            horizon_returns[h],
            threshold_pct=threshold_pct,
            horizon_days=h,
        )

    # 7. Confidence signal off the 7d horizon
    confidence = confidence_signal_from_base_rates(base_rates, horizon=7)

    return PatternMatchReport(
        ticker=ticker,
        query_date=query_date,
        query_state=query_state,
        analog_count=len(analog_records),
        top_k_analogs=tuple(analog_records),
        base_rates=base_rates,
        confidence_signal=confidence,
        generated_at=generated_at,
    )


__all__ = [
    "STATE_VECTOR_DIM",
    "DEFAULT_K_NEAREST",
    "MIN_ANALOGS_FOR_BASE_RATE",
    "LOOKBACK_DAYS_DEFAULT",
    "FORWARD_HORIZONS_DAYS",
    "DEFAULT_OUTCOME_THRESHOLD_PCT",
    "CONFIDENCE_SATURATION_COUNT",
    "LIQUIDITY_STATE_ORDERING",
    "STATE_VECTOR_FEATURES",
    "MarketStateVector",
    "HistoricalAnalog",
    "BaseRateDistribution",
    "PatternMatchReport",
    "cosine_similarity",
    "normalize_zscore",
    "normalize_minmax",
    "normalize_clamp_div",
    "normalize_ordinal",
    "compute_base_rate",
    "find_nearest_analogs",
    "confidence_signal_from_base_rates",
    "build_state_vector",
    "query_historical_states",
    "read_forward_returns",
    "build_pattern_match_report",
]
