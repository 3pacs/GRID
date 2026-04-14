"""
GRID Historical Scenario Library — macro-FEATURE-space analog matcher.

Companion to ``intelligence/pattern_library.py`` (which works in
signal-contribution space with a 12-dim fixed state vector). This module
works in **macro-feature space**: for a given query date, build a 10-dim
z-scored feature vector from the environment at that ``as_of`` date and
find historically similar prior predictions. Return the empirical base
rate of what happened next (hit vs miss) and a conviction multiplier
suitable for stacking on top of other adjusters.

The two libraries are orthogonal:
    - ``pattern_library``     → "what did forward returns do when our
                                 12-dim signal mix looked like this?"
    - ``historical_scenario`` → "what did oracle predictions do when the
                                 macro backdrop (vix, fci, move, dxy,
                                 realized vol, dollar, credit, duration,
                                 equity momentum, commodity) matched?"

Every historical feature lookup MUST go through ``PITStore.get_pit`` to
preserve point-in-time correctness; we never hit ``resolved_series``
directly.  The 252-day normalization window is strictly ``< as_of`` so
no lookahead sneaks in via the z-score statistics themselves.

Public entry points:
    - ``find_analogs`` — full report with top analogs + base rates.
    - ``scenario_conviction_multiplier`` — 1-call live-path float that
      never raises (returns 1.0 on any failure).

CAT-176 — amplifies conviction when the current setup matches historical
winners and degrades it when the current setup looks like a historical
loser.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore


# ── Constants ─────────────────────────────────────────────────────────────

# Mirrors ``features.per_signal_brier.CANONICAL_HORIZONS`` — kept as a
# local constant so this module has no hard import dependency on the
# Brier tracker (which may not be shipped in every branch).
CANONICAL_HORIZONS: tuple[int, ...] = (1, 7, 30, 90)

# The 10 feature lanes that make up a macro snapshot. Each entry is the
# primary ``feature_registry.name`` followed by any fallback names. The
# transform tells us how to reduce the raw time series for that lane
# into a scalar at ``as_of``.
#
# kinds:
#   - ``level``      → take the most-recent value strictly before as_of
#   - ``pct_change`` → (last / value_n_days_ago) - 1, where n = ``window``
#   - ``realized_vol`` → stdev of log returns over ``window`` days
FEATURE_SPEC: tuple[dict[str, Any], ...] = (
    {
        "name": "vix_close",
        "primary": "vix_close",
        "fallbacks": (),
        "kind": "level",
    },
    {
        "name": "move_index_close",
        "primary": "move_index_close",
        "fallbacks": (),
        "kind": "level",
    },
    {
        "name": "fci_composite",
        "primary": "fci_composite",
        "fallbacks": ("fci_chicago",),
        "kind": "level",
    },
    {
        "name": "spy_mom_20d",
        "primary": "spy_close",
        "fallbacks": (),
        "kind": "pct_change",
        "window": 20,
    },
    {
        "name": "spy_mom_60d",
        "primary": "spy_close",
        "fallbacks": (),
        "kind": "pct_change",
        "window": 60,
    },
    {
        "name": "tlt_mom_20d",
        "primary": "tlt_full",
        "fallbacks": (),
        "kind": "pct_change",
        "window": 20,
    },
    {
        "name": "hyg_mom_20d",
        "primary": "hyg_full",
        "fallbacks": (),
        "kind": "pct_change",
        "window": 20,
    },
    {
        "name": "dxy_mom_20d",
        "primary": "dxy_spot",
        "fallbacks": ("uup_etf_close",),
        "kind": "pct_change",
        "window": 20,
    },
    {
        "name": "realized_vol_30d",
        "primary": "sp500_full",
        "fallbacks": (),
        "kind": "realized_vol",
        "window": 30,
    },
    {
        "name": "wti_mom_20d",
        "primary": "wti_crude_full",
        "fallbacks": (),
        "kind": "pct_change",
        "window": 20,
    },
)

FEATURE_DIM: int = 10
assert len(FEATURE_SPEC) == FEATURE_DIM

# Z-score normalization window (strictly prior to as_of).
NORM_WINDOW_DAYS: int = 252
MIN_NORM_OBS: int = 30
ZSCORE_CLAMP: float = 4.0

# Analog matcher knobs.
DEFAULT_K: int = 50
MIN_ANALOG_SIMILARITY: float = 0.6
MIN_MATCHES_FOR_SIGNAL: int = 15
TOP_ANALOG_DISPLAY: int = 10

# Multiplier thresholds (CAT-176 spec).
HIT_RATE_HIGH_AMPLIFY: float = 0.70
HIT_RATE_NEUTRAL_HIGH: float = 0.55
HIT_RATE_NEUTRAL_LOW: float = 0.45
HIT_RATE_DEGRADE: float = 0.30

MULT_AMPLIFY: float = 1.10
MULT_NEUTRAL: float = 1.00
MULT_MILD_DEGRADE: float = 0.95
MULT_DEGRADE: float = 0.85
MULT_STRONG_DEGRADE: float = 0.70

# Directional sugar.
BULLISH = "bullish"
BEARISH = "bearish"
BULLISH_ALIASES = frozenset({"bullish", "long", "call", "up", "buy"})
BEARISH_ALIASES = frozenset({"bearish", "short", "put", "down", "sell"})

NEUTRAL_SENTINEL: float = 0.0


# ── Data classes ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ScenarioAnalog:
    """One historically similar prediction used as an analog."""

    prediction_id: str
    ticker: str
    created_at: str  # ISO format
    similarity: float
    direction: str
    verdict: str
    confidence: float
    horizon_days: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ScenarioLibraryReport:
    """Full analog-matching report for a live query setup."""

    query_as_of: str
    query_horizon_days: int
    query_feature_vector: list[float]
    n_candidates_scanned: int
    n_matches: int
    hit_rate: float
    hit_rate_long: float
    hit_rate_short: float
    mean_confidence: float
    mean_realized_return: float | None
    conviction_multiplier: float
    advisory: str
    top_analogs: list[ScenarioAnalog]
    missing_feature_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_as_of": self.query_as_of,
            "query_horizon_days": self.query_horizon_days,
            "query_feature_vector": list(self.query_feature_vector),
            "n_candidates_scanned": self.n_candidates_scanned,
            "n_matches": self.n_matches,
            "hit_rate": self.hit_rate,
            "hit_rate_long": self.hit_rate_long,
            "hit_rate_short": self.hit_rate_short,
            "mean_confidence": self.mean_confidence,
            "mean_realized_return": self.mean_realized_return,
            "conviction_multiplier": self.conviction_multiplier,
            "advisory": self.advisory,
            "top_analogs": [a.to_dict() for a in self.top_analogs],
            "missing_feature_count": self.missing_feature_count,
        }


def _neutral_report(
    *,
    as_of: date,
    horizon_days: int,
    query_vec: list[float] | None = None,
    missing: int = 0,
    advisory: str = "neutral",
    n_candidates: int = 0,
) -> ScenarioLibraryReport:
    """Safe fallback when we don't have enough data to say anything."""
    return ScenarioLibraryReport(
        query_as_of=as_of.isoformat(),
        query_horizon_days=int(horizon_days),
        query_feature_vector=list(query_vec or [NEUTRAL_SENTINEL] * FEATURE_DIM),
        n_candidates_scanned=int(n_candidates),
        n_matches=0,
        hit_rate=0.0,
        hit_rate_long=0.0,
        hit_rate_short=0.0,
        mean_confidence=0.0,
        mean_realized_return=None,
        conviction_multiplier=MULT_NEUTRAL,
        advisory=advisory,
        top_analogs=[],
        missing_feature_count=int(missing),
    )


# ── Direction helpers ─────────────────────────────────────────────────────


def _canonical_direction(raw: Any) -> str:
    """Map an arbitrary direction blob into ``bullish`` or ``bearish``.

    ``oracle_predictions.signals`` is a JSONB and prior pipelines have
    encoded direction in several ways (``direction`` field, ``prediction_type``
    like CALL/PUT, etc). Defaults to ``bullish`` on anything unknown —
    callers can still filter via the explicit ``direction`` kwarg.
    """
    if raw is None:
        return BULLISH
    if isinstance(raw, dict):
        for key in ("direction", "bias", "side", "prediction_type"):
            if key in raw:
                return _canonical_direction(raw[key])
        return BULLISH
    s = str(raw).strip().lower()
    if s in BULLISH_ALIASES:
        return BULLISH
    if s in BEARISH_ALIASES:
        return BEARISH
    return BULLISH


def _verdict_score(verdict: str) -> float:
    """Map verdict → [0, 1] hit score.

    Mirrors ``scripts.bootstrap_per_signal_brier.verdict_to_outcome``:
    hit=1.0, partial=0.5, miss=0.0 (and everything else).
    """
    v = str(verdict or "").strip().lower()
    if v == "hit":
        return 1.0
    if v == "partial":
        return 0.5
    return 0.0


# ── PIT-correct feature loading with caching ─────────────────────────────


def _resolve_feature_id(engine: Engine, name: str) -> int | None:
    """Look up a ``feature_registry`` row by exact name. Returns None if
    the name is not registered or the DB call fails."""
    if not name:
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = :n LIMIT 1"),
                {"n": name},
            ).fetchone()
        if not row:
            return None
        return int(row[0])
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "historical_scenario_library: feature_id lookup failed for {n}: {e}",
            n=name, e=str(exc),
        )
        return None


def _load_pit_series(
    pit_store: PITStore,
    feature_id: int,
    as_of: date,
    lookback_days: int,
) -> pd.Series:
    """Fetch a PIT-correct time series for one feature_id up to ``as_of``.

    Returns an empty series on failure. Never raises.
    """
    try:
        df = pit_store.get_pit(
            feature_ids=[int(feature_id)],
            as_of_date=as_of,
            vintage_policy="LATEST_AS_OF",
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "historical_scenario_library: PITStore.get_pit failed for fid={fid}: {e}",
            fid=feature_id, e=str(exc),
        )
        return pd.Series(dtype=float)

    if df is None or df.empty:
        return pd.Series(dtype=float)

    try:
        df = df.copy()
        df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce")
        df = df.dropna(subset=["obs_date"])
        df = df.sort_values("obs_date")
        series = pd.Series(
            df["value"].astype(float).values,
            index=df["obs_date"].values,
            dtype=float,
        )
        # Trim to window
        cutoff = pd.Timestamp(as_of) - pd.Timedelta(days=int(lookback_days))
        series = series[series.index >= cutoff]
        return series
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "historical_scenario_library: series shaping failed for fid={fid}: {e}",
            fid=feature_id, e=str(exc),
        )
        return pd.Series(dtype=float)


def _pick_series_for_spec(
    pit_store: PITStore,
    engine: Engine,
    spec: dict[str, Any],
    as_of: date,
    lookback_days: int,
    feature_id_cache: dict[str, int | None],
    series_cache: dict[tuple[str, date], pd.Series],
) -> pd.Series:
    """Walk primary → fallbacks for one spec and return the first
    non-empty PIT series. Empty series if nothing is found.
    """
    candidates: list[str] = [spec["primary"], *list(spec.get("fallbacks", ()))]
    for name in candidates:
        cache_key = (name, as_of)
        if cache_key in series_cache:
            series = series_cache[cache_key]
        else:
            if name not in feature_id_cache:
                feature_id_cache[name] = _resolve_feature_id(engine, name)
            fid = feature_id_cache[name]
            if fid is None:
                series_cache[cache_key] = pd.Series(dtype=float)
                continue
            series = _load_pit_series(pit_store, fid, as_of, lookback_days)
            series_cache[cache_key] = series
        if not series.empty:
            return series
    return pd.Series(dtype=float)


# ── Feature reduction ────────────────────────────────────────────────────


def _reduce_level(series: pd.Series, as_of: date) -> float | None:
    """Return the most recent value strictly before ``as_of``."""
    if series is None or series.empty:
        return None
    cutoff = pd.Timestamp(as_of)
    trimmed = series[series.index < cutoff]
    if trimmed.empty:
        return None
    val = trimmed.iloc[-1]
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _reduce_pct_change(series: pd.Series, as_of: date, window: int) -> float | None:
    """(last / ref) - 1 where ``last`` is the most recent value strictly
    before ``as_of`` and ``ref`` is the value ``window`` sessions prior.
    """
    if series is None or series.empty:
        return None
    cutoff = pd.Timestamp(as_of)
    trimmed = series[series.index < cutoff].dropna()
    if len(trimmed) < window + 1:
        return None
    last = float(trimmed.iloc[-1])
    ref = float(trimmed.iloc[-(window + 1)])
    if ref == 0.0 or math.isnan(last) or math.isnan(ref):
        return None
    return (last / ref) - 1.0


def _reduce_realized_vol(
    series: pd.Series,
    as_of: date,
    window: int,
) -> float | None:
    """Stdev of log returns over the last ``window`` observations strictly
    before ``as_of``. Returns None if insufficient or degenerate.
    """
    if series is None or series.empty:
        return None
    cutoff = pd.Timestamp(as_of)
    trimmed = series[series.index < cutoff].dropna()
    if len(trimmed) < window + 1:
        return None
    tail = trimmed.iloc[-(window + 1):].astype(float).values
    if np.any(tail <= 0):
        return None
    log_rets = np.diff(np.log(tail))
    if log_rets.size == 0:
        return None
    sigma = float(np.std(log_rets, ddof=1)) if log_rets.size > 1 else 0.0
    if math.isnan(sigma) or math.isinf(sigma):
        return None
    return sigma


def _reduce_series(spec: dict[str, Any], series: pd.Series, as_of: date) -> float | None:
    kind = spec.get("kind", "level")
    if kind == "level":
        return _reduce_level(series, as_of)
    if kind == "pct_change":
        return _reduce_pct_change(series, as_of, int(spec.get("window", 20)))
    if kind == "realized_vol":
        return _reduce_realized_vol(series, as_of, int(spec.get("window", 30)))
    return None


# ── Normalization window (strict < as_of) ────────────────────────────────


def _build_rolling_samples(
    spec: dict[str, Any],
    series: pd.Series,
    as_of: date,
) -> list[float]:
    """Produce the rolling sample of the reduced feature over the last
    ``NORM_WINDOW_DAYS`` sessions strictly before ``as_of``.

    For kind ``level`` this is simply the daily series. For the derived
    kinds we roll the same reduction across recent sessions so the
    normalization is apples-to-apples with the live query reduction.
    """
    if series is None or series.empty:
        return []

    cutoff = pd.Timestamp(as_of)
    hist = series[series.index < cutoff].dropna().astype(float)
    if hist.empty:
        return []

    # Trim to the last ~2x NORM_WINDOW_DAYS calendar days so we have enough
    # room to produce NORM_WINDOW_DAYS reduced samples without blowing
    # past the available history.
    earliest = cutoff - pd.Timedelta(days=int(NORM_WINDOW_DAYS * 2 + 90))
    hist = hist[hist.index >= earliest]
    if hist.empty:
        return []

    kind = spec.get("kind", "level")
    samples: list[float] = []

    if kind == "level":
        tail_values = hist.values[-NORM_WINDOW_DAYS:]
        for v in tail_values:
            if v is None:
                continue
            fv = float(v)
            if math.isnan(fv) or math.isinf(fv):
                continue
            samples.append(fv)
        return samples

    window = int(spec.get("window", 20))
    arr = hist.values
    n = len(arr)

    if kind == "pct_change":
        start = max(window + 1, 1)
        for i in range(start, n + 1):
            last = arr[i - 1]
            ref = arr[i - 1 - window]
            if ref == 0 or last is None or ref is None:
                continue
            try:
                pct = (float(last) / float(ref)) - 1.0
            except (TypeError, ValueError, ZeroDivisionError):
                continue
            if math.isnan(pct) or math.isinf(pct):
                continue
            samples.append(pct)

    elif kind == "realized_vol":
        for i in range(window + 1, n + 1):
            tail = arr[i - window - 1:i]
            if np.any(np.asarray(tail) <= 0):
                continue
            log_rets = np.diff(np.log(np.asarray(tail, dtype=float)))
            if log_rets.size < 2:
                continue
            sigma = float(np.std(log_rets, ddof=1))
            if math.isnan(sigma) or math.isinf(sigma):
                continue
            samples.append(sigma)

    # Keep the most recent NORM_WINDOW_DAYS samples to match the contract.
    return samples[-NORM_WINDOW_DAYS:]


def _zscore(value: float, samples: list[float]) -> float:
    """Clamp z-score against the sample window; neutral on degeneracy."""
    if value is None or math.isnan(value) or math.isinf(value):
        return NEUTRAL_SENTINEL
    if len(samples) < MIN_NORM_OBS:
        return NEUTRAL_SENTINEL
    arr = np.asarray(samples, dtype=float)
    mean = float(np.mean(arr))
    std = float(np.std(arr, ddof=1)) if arr.size > 1 else 0.0
    if std <= 0 or math.isnan(std):
        return NEUTRAL_SENTINEL
    z = (float(value) - mean) / std
    if math.isnan(z) or math.isinf(z):
        return NEUTRAL_SENTINEL
    if z > ZSCORE_CLAMP:
        z = ZSCORE_CLAMP
    elif z < -ZSCORE_CLAMP:
        z = -ZSCORE_CLAMP
    return float(z)


# ── Vector builder (PIT-safe) ────────────────────────────────────────────


def _build_feature_vector(
    engine: Engine,
    pit_store: PITStore,
    as_of: date,
    *,
    feature_id_cache: dict[str, int | None],
    series_cache: dict[tuple[str, date], pd.Series],
) -> tuple[np.ndarray, int]:
    """Return ``(z_vector, missing_feature_count)`` for ``as_of``.

    Never raises — any per-feature failure falls through to the neutral
    sentinel 0.0 and bumps the missing counter so callers can degrade
    confidence accordingly.
    """
    vec: list[float] = []
    missing = 0

    # We need enough raw history to support both the reduction (windows
    # up to 60 sessions) and the NORM_WINDOW_DAYS rolling samples.
    lookback = NORM_WINDOW_DAYS * 2 + 120

    for spec in FEATURE_SPEC:
        try:
            series = _pick_series_for_spec(
                pit_store, engine, spec, as_of, lookback,
                feature_id_cache, series_cache,
            )
            reduced = _reduce_series(spec, series, as_of)
            if reduced is None:
                vec.append(NEUTRAL_SENTINEL)
                missing += 1
                continue
            samples = _build_rolling_samples(spec, series, as_of)
            z = _zscore(reduced, samples)
            if z == NEUTRAL_SENTINEL and (reduced is None or len(samples) < MIN_NORM_OBS):
                missing += 1
            vec.append(z)
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "historical_scenario_library: feature {n} failed at {d}: {e}",
                n=spec.get("name"), d=as_of, e=str(exc),
            )
            vec.append(NEUTRAL_SENTINEL)
            missing += 1

    return np.asarray(vec, dtype=float), missing


# ── Cosine similarity ────────────────────────────────────────────────────


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    """Plain cosine similarity with defensive zero-norm handling."""
    if a.size != b.size or a.size == 0:
        return 0.0
    dot = float(np.dot(a, b))
    na = float(np.linalg.norm(a))
    nb = float(np.linalg.norm(b))
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    sim = dot / (na * nb)
    if math.isnan(sim) or math.isinf(sim):
        return 0.0
    return max(-1.0, min(1.0, sim))


# ── Candidate loading ────────────────────────────────────────────────────


def _load_candidate_predictions(
    engine: Engine,
    *,
    as_of: date,
    horizon_days: int,
    ticker: str | None,
    direction: str | None,
) -> list[dict[str, Any]]:
    """Pull historical scored predictions matching (horizon, optional
    ticker) from ``oracle_predictions``. The ``direction`` param is
    applied in Python because it lives inside the ``signals`` JSONB.

    Uses the same expiry-minus-created_at cascade as
    ``scripts/bootstrap_per_signal_brier.py``.
    """
    try:
        sql = (
            "SELECT id, ticker, created_at, expiry, confidence, verdict, "
            "signals, realized_return "
            "FROM oracle_predictions "
            "WHERE verdict IN ('hit', 'miss', 'partial') "
            "  AND created_at IS NOT NULL "
            "  AND expiry IS NOT NULL "
        )
        params: dict[str, Any] = {}
        if ticker:
            sql += " AND ticker = :tkr"
            params["tkr"] = ticker
        sql += " ORDER BY created_at DESC LIMIT 5000"
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
    except Exception as exc:  # noqa: BLE001
        # Fall back without realized_return (column may not exist yet).
        log.debug(
            "historical_scenario_library: candidate load with realized_return failed: {e}",
            e=str(exc),
        )
        try:
            sql = (
                "SELECT id, ticker, created_at, expiry, confidence, verdict, signals "
                "FROM oracle_predictions "
                "WHERE verdict IN ('hit', 'miss', 'partial') "
                "  AND created_at IS NOT NULL "
                "  AND expiry IS NOT NULL "
            )
            params = {}
            if ticker:
                sql += " AND ticker = :tkr"
                params["tkr"] = ticker
            sql += " ORDER BY created_at DESC LIMIT 5000"
            with engine.connect() as conn:
                rows = conn.execute(text(sql), params).fetchall()
            rows = [(*r, None) for r in rows]
        except Exception as exc2:  # noqa: BLE001
            log.warning(
                "historical_scenario_library: oracle_predictions read failed: {e}",
                e=str(exc2),
            )
            return []

    candidates: list[dict[str, Any]] = []
    want_dir = None
    if direction:
        want_dir = _canonical_direction(direction)

    for r in rows:
        try:
            pid, tkr, created, expiry, conf, verdict, signals, realized = r
        except (TypeError, ValueError):
            continue

        h = _row_horizon_days(created, expiry)
        if h is None or h != int(horizon_days):
            continue

        cand_dir = _canonical_direction(signals)
        if want_dir is not None and cand_dir != want_dir:
            continue

        created_date = _coerce_date(created)
        if created_date is None:
            continue

        candidates.append({
            "id": str(pid),
            "ticker": str(tkr or ""),
            "created_at": created,
            "created_date": created_date,
            "confidence": _coerce_float(conf) or 0.0,
            "verdict": str(verdict or "").lower(),
            "direction": cand_dir,
            "horizon_days": int(h),
            "realized_return": _coerce_float(realized),
        })

    return candidates


def _row_horizon_days(created: Any, expiry: Any) -> int | None:
    """Mirror ``bootstrap_per_signal_brier._coerce_horizon_days`` minus
    the 7-day default fallback (callers filter on the exact horizon)."""
    if created is None or expiry is None:
        return None
    try:
        cd = created.date() if isinstance(created, datetime) else created
        ed = expiry.date() if isinstance(expiry, datetime) else expiry
        delta = (ed - cd).days
    except Exception:  # noqa: BLE001
        return None
    if delta <= 0:
        return None
    return int(delta)


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return pd.to_datetime(value).date()
    except Exception:  # noqa: BLE001
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


# ── Overlap filter (anti-leak) ───────────────────────────────────────────


def _within_overlap_window(
    candidate_date: date,
    query_date: date,
    horizon_days: int,
) -> bool:
    """True if the candidate's window overlaps the query window enough to
    risk leakage. We exclude any candidate whose ``created_at`` is within
    ``2 * horizon_days`` of the query date. This prevents picking the
    "yesterday" prediction that is still forward-overlapping the trade
    we are trying to size today.
    """
    days = abs((candidate_date - query_date).days)
    return days < int(2 * horizon_days)


# ── Base rate scoring ────────────────────────────────────────────────────


def _classify_multiplier(n_matches: int, hit_rate: float) -> tuple[float, str]:
    """Apply the CAT-176 multiplier table."""
    if n_matches < MIN_MATCHES_FOR_SIGNAL:
        return MULT_NEUTRAL, "insufficient analogs"
    if hit_rate >= HIT_RATE_HIGH_AMPLIFY:
        return MULT_AMPLIFY, "historical setups in this condition worked"
    if hit_rate >= HIT_RATE_NEUTRAL_HIGH:
        return MULT_NEUTRAL, "mixed historical analogs"
    if hit_rate >= HIT_RATE_NEUTRAL_LOW:
        return MULT_MILD_DEGRADE, "mildly adverse historical precedent"
    if hit_rate >= HIT_RATE_DEGRADE:
        return MULT_DEGRADE, "adverse historical precedent"
    return MULT_STRONG_DEGRADE, "historical setups failed"


def _hit_rate(analogs: Iterable[tuple[float, dict[str, Any]]]) -> float:
    scores: list[float] = []
    for _sim, row in analogs:
        scores.append(_verdict_score(row["verdict"]))
    if not scores:
        return 0.0
    return sum(scores) / len(scores)


# ── Main entry points ────────────────────────────────────────────────────


def find_analogs(
    engine: Engine,
    *,
    as_of: date,
    horizon_days: int,
    direction: str | None = None,
    ticker: str | None = None,
    k: int = DEFAULT_K,
) -> ScenarioLibraryReport:
    """Find historical analogs and return a full base-rate report.

    Never raises. Any DB failure, empty history, or shape mismatch
    returns a neutral report with ``conviction_multiplier == 1.0`` and an
    explanatory advisory.
    """
    try:
        pit_store = PITStore(engine)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "historical_scenario_library: PITStore init failed: {e}",
            e=str(exc),
        )
        return _neutral_report(
            as_of=as_of,
            horizon_days=horizon_days,
            advisory="pit store unavailable",
        )

    feature_id_cache: dict[str, int | None] = {}
    series_cache: dict[tuple[str, date], pd.Series] = {}

    # 1. Build the query vector.
    try:
        query_vec, query_missing = _build_feature_vector(
            engine, pit_store, as_of,
            feature_id_cache=feature_id_cache,
            series_cache=series_cache,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "historical_scenario_library: query vector build failed: {e}",
            e=str(exc),
        )
        return _neutral_report(
            as_of=as_of,
            horizon_days=horizon_days,
            advisory="query vector build failed",
        )

    # 2. Load candidate predictions.
    candidates = _load_candidate_predictions(
        engine,
        as_of=as_of,
        horizon_days=horizon_days,
        ticker=ticker,
        direction=direction,
    )
    n_scanned = len(candidates)
    if n_scanned == 0:
        return _neutral_report(
            as_of=as_of,
            horizon_days=horizon_days,
            query_vec=query_vec.tolist(),
            missing=query_missing,
            advisory="no historical predictions",
        )

    # 3. Score each candidate.
    scored: list[tuple[float, dict[str, Any]]] = []
    for cand in candidates:
        cand_date = cand["created_date"]
        if _within_overlap_window(cand_date, as_of, horizon_days):
            continue
        try:
            cand_vec, _cand_missing = _build_feature_vector(
                engine, pit_store, cand_date,
                feature_id_cache=feature_id_cache,
                series_cache=series_cache,
            )
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "historical_scenario_library: candidate vector failed ({pid}): {e}",
                pid=cand["id"], e=str(exc),
            )
            continue
        sim = _cosine(query_vec, cand_vec)
        scored.append((sim, cand))

    if not scored:
        return _neutral_report(
            as_of=as_of,
            horizon_days=horizon_days,
            query_vec=query_vec.tolist(),
            missing=query_missing,
            n_candidates=n_scanned,
            advisory="all candidates filtered by overlap window",
        )

    # 4. Nearest-K pass, threshold filter.
    scored.sort(key=lambda t: t[0], reverse=True)
    top_k = scored[: max(1, int(k))]
    qualified = [(s, r) for (s, r) in top_k if s >= MIN_ANALOG_SIMILARITY]

    n_matches = len(qualified)
    if n_matches == 0:
        mult, advisory = _classify_multiplier(0, 0.0)
        return ScenarioLibraryReport(
            query_as_of=as_of.isoformat(),
            query_horizon_days=int(horizon_days),
            query_feature_vector=query_vec.tolist(),
            n_candidates_scanned=n_scanned,
            n_matches=0,
            hit_rate=0.0,
            hit_rate_long=0.0,
            hit_rate_short=0.0,
            mean_confidence=0.0,
            mean_realized_return=None,
            conviction_multiplier=mult,
            advisory=advisory,
            top_analogs=[],
            missing_feature_count=query_missing,
        )

    # 5. Aggregate base rates.
    long_matches = [(s, r) for (s, r) in qualified if r["direction"] == BULLISH]
    short_matches = [(s, r) for (s, r) in qualified if r["direction"] == BEARISH]

    hit_rate = _hit_rate(qualified)
    hit_rate_long = _hit_rate(long_matches) if long_matches else 0.0
    hit_rate_short = _hit_rate(short_matches) if short_matches else 0.0
    mean_confidence = sum(r["confidence"] for _, r in qualified) / n_matches

    realized = [r["realized_return"] for _, r in qualified if r["realized_return"] is not None]
    mean_realized_return = (sum(realized) / len(realized)) if realized else None

    mult, advisory = _classify_multiplier(n_matches, hit_rate)

    top_analogs = [
        ScenarioAnalog(
            prediction_id=r["id"],
            ticker=r["ticker"],
            created_at=_format_ts(r["created_at"]),
            similarity=float(max(0.0, min(1.0, (s + 1.0) / 2.0))),
            direction=r["direction"],
            verdict=r["verdict"],
            confidence=float(r["confidence"]),
            horizon_days=int(r["horizon_days"]),
        )
        for (s, r) in qualified[:TOP_ANALOG_DISPLAY]
    ]

    return ScenarioLibraryReport(
        query_as_of=as_of.isoformat(),
        query_horizon_days=int(horizon_days),
        query_feature_vector=query_vec.tolist(),
        n_candidates_scanned=n_scanned,
        n_matches=n_matches,
        hit_rate=float(hit_rate),
        hit_rate_long=float(hit_rate_long),
        hit_rate_short=float(hit_rate_short),
        mean_confidence=float(mean_confidence),
        mean_realized_return=mean_realized_return,
        conviction_multiplier=float(mult),
        advisory=advisory,
        top_analogs=top_analogs,
        missing_feature_count=int(query_missing),
    )


def scenario_conviction_multiplier(
    engine: Engine,
    *,
    as_of: date,
    horizon_days: int,
    direction: str | None = None,
) -> float:
    """Live-path convenience returning just the multiplier.

    Returns 1.0 on any failure — never raises.
    """
    try:
        report = find_analogs(
            engine,
            as_of=as_of,
            horizon_days=horizon_days,
            direction=direction,
        )
        return float(report.conviction_multiplier)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "historical_scenario_library: conviction multiplier failed: {e}",
            e=str(exc),
        )
        return MULT_NEUTRAL


def _format_ts(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


__all__ = [
    "CANONICAL_HORIZONS",
    "FEATURE_SPEC",
    "FEATURE_DIM",
    "NORM_WINDOW_DAYS",
    "MIN_NORM_OBS",
    "ZSCORE_CLAMP",
    "DEFAULT_K",
    "MIN_ANALOG_SIMILARITY",
    "MIN_MATCHES_FOR_SIGNAL",
    "TOP_ANALOG_DISPLAY",
    "HIT_RATE_HIGH_AMPLIFY",
    "HIT_RATE_NEUTRAL_HIGH",
    "HIT_RATE_NEUTRAL_LOW",
    "HIT_RATE_DEGRADE",
    "MULT_AMPLIFY",
    "MULT_NEUTRAL",
    "MULT_MILD_DEGRADE",
    "MULT_DEGRADE",
    "MULT_STRONG_DEGRADE",
    "ScenarioAnalog",
    "ScenarioLibraryReport",
    "find_analogs",
    "scenario_conviction_multiplier",
]
