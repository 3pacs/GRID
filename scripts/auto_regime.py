#!/usr/bin/env python3
"""GRID auto-regime: continuous stress index with derivative-based transition detection.

Instead of GMM clustering (which suffers from label switching and has no
economic model), we compute a composite **Stress Index** from z-scores of
regime-relevant features, weighted by economic meaning.

The stress index S(t) is a continuous signal from ~-3 (extreme calm) to +3
(extreme stress). Regimes are derived from S(t) and its first derivative
dS/dt (momentum):

    GROWTH:  S < -0.3  and  dS/dt <= 0   (low stress, improving or stable)
    NEUTRAL: -0.3 <= S <= 0.6             (mixed signals)
    FRAGILE: S > 0.6   or  dS/dt > 0.15  (elevated stress or deteriorating fast)
    CRISIS:  S > 1.5   and  dS/dt > 0    (high stress, still worsening)

The derivative catches transitions early — a low-stress market that's
deteriorating quickly gets flagged as FRAGILE before S itself crosses
the threshold.

Confidence is derived from how cleanly the reading falls into one regime
vs straddling boundaries.
"""

import argparse
import json
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
from loguru import logger as log
from sqlalchemy import text

if TYPE_CHECKING:
    import pandas as pd

from db import get_engine
from store.pit import PITStore

# Path for user weight overrides (written by API, read here)
WEIGHTS_OVERRIDE_PATH = Path(__file__).resolve().parent.parent / "outputs" / "regime_weights.json"

# ── Feature weights ─────────────────────────────────────────────
#
# Positive weight = feature INCREASES with stress (VIX, spreads)
# Negative weight = feature DECREASES with stress (equities, copper)
# Magnitude = how much this feature matters for regime classification
#
# These are the knobs the user will eventually control via sliders.

DEFAULT_FEATURE_WEIGHTS: dict[str, float] = {
    # Volatility (most direct stress measures)
    "vix":               +0.20,
    "move_index":        +0.08,
    "vxn":               +0.05,

    # Credit (transmission mechanism: stress → wider spreads)
    "hy_spread":         +0.15,
    "chicago_fed":       +0.06,

    # Rates (curve inversion = recession signal)
    "yield_curve_10y2y": -0.10,   # inverted curve = stress
    "treasury_10y":      +0.03,   # higher rates = tighter conditions
    "breakeven_10y":     -0.03,   # falling breakevens = deflation fear

    # Risk assets (fall during stress)
    "sp500":             -0.10,
    "copper":            -0.05,   # Dr. Copper — growth proxy
    "crude_oil":         -0.03,

    # Safe havens (rise during stress)
    "gold":              +0.05,
    "dollar_index":      +0.05,

    # Sentiment
    "spy_rsi":           -0.04,   # low RSI = oversold = stress
    "put_call_ratio":    +0.03,   # high PCR = fear
}


# ── Concept → fed feature ───────────────────────────────────────
#
# The weights above are keyed by *concept*. Those are the knobs: the API
# sliders send them, ``outputs/regime_weights.json`` overrides them, and the
# stress index is normalized by them. They are NOT feature_registry rows.
#
# Several of the rows that carry those names are dead ends. The live raw series
# are mapped elsewhere by ``normalization/entity_map.py``: ``VIXCLS`` and
# ``YF:^VIX:close`` resolve to ``vix_spot``, ``BAMLH0A0HYM2`` to
# ``hy_oas_spread``, ``YF:^GSPC:close`` to ``sp500_full``, ``DGS10`` to
# ``yc_10y``. Scoring the same-named registry rows scored whatever a one-off
# historical load had left in them.
#
# So each concept lists the features that could back it, best first, and
# ``_resolve_regime_bindings`` takes the first one that is model-eligible and
# actually carries resolved history. Renaming registry rows or editing
# entity_map would ripple through every other consumer; an alias layer here
# does not.
# Chosen from the live registry on 2026-09-11 (ops-exec run 34549188717, run
# #142): for each concept, every registry row whose name matches it, with its
# resolved coverage and the raw freshness of every entity_map seed pointing at
# it. Candidates are ordered best-first; the resolver picks the first that is
# model-eligible and has the history. An empty list is a deliberate statement:
# nothing feeds this concept, so its weight drops to zero rather than scoring a
# frozen column.
REGIME_FEATURE_SOURCES: dict[str, list[str]] = {
    # vix_spot (id 11) is the feature — 18,303 observations back to 1990, fed by
    # both VIXCLS and YF:^VIX:close, raw fresh to 2026-09-10 — and it is
    # model_eligible = FALSE, so the governance gate below skips it and this
    # concept scores nothing until that flag is flipped. It is the only
    # candidate on purpose. vvix (id 18094) is eligible and fed, but VVIX is
    # the volatility *of* VIX: a different series with different dynamics that
    # can spike while VIX is quiet, and it would carry the index's largest
    # weight (+0.20) under a slider labelled "vix". `vix` (id 105) is eligible
    # with 505 observations and no seed at all — listing it as a fallback would
    # bind this concept straight back to one of the frozen rows that produced
    # the hollow backfill. An honest zero beats either.
    "vix":               ["vix_spot"],

    # hy_oas_spread (id 18150): 7,636 observations from 1996, fed by
    # BAMLH0A0HYM2, raw fresh to 2026-09-09. hyg_full resolves further (to
    # 2026-07-10) but it is the HY *ETF price*, which falls as spreads widen —
    # scoring it under a +0.15 stress weight would invert the credit signal, so
    # it is deliberately not a candidate.
    "hy_spread":         ["hy_oas_spread", "hy_spread"],

    # sp500_full (id 686): 8,351 observations from 1993, YF:^GSPC:close fresh.
    "sp500":             ["sp500_full", "sp500"],

    # gold_full (id 660): 5,443 observations from 2004, YF:GC=F:close fresh.
    "gold":              ["gold_full", "gold"],

    # copper (id 126) is one of the two concepts whose own name is fed —
    # YF:HG=F:close, raw fresh to 2026-09-10.
    "copper":            ["copper"],

    # dxy_index (id 14) is the real dollar index and is fed, but
    # model_eligible = FALSE. uup_etf_close (id 40213) is the eligible fed
    # substitute: the UUP ETF tracks the same basket in the same direction,
    # 4,804 observations from 2007, YF:UUP:close raw fresh to 2026-09-10. A
    # z-score does not care about the price level (~$27 vs ~104).
    "dollar_index":      ["dxy_index", "uup_etf_close", "dollar_index"],

    # yc_breakeven_10y (id 18113): 5,814 observations from 2003, fed by T10YIE,
    # raw fresh to 2026-09-10.
    "breakeven_10y":     ["yc_breakeven_10y", "breakeven_10y"],

    # yld_curve_2s10s (id 1) is fed by T10Y2Y (raw fresh to 2026-09-10) with
    # 9,066 observations from 1990 — and is model_eligible = FALSE. Nothing
    # else carries the curve, so this concept scores zero until that flag is
    # flipped. Deliberately not falling back to yield_curve_10y2y (id 102),
    # which no seed feeds.
    "yield_curve_10y2y": ["yld_curve_2s10s"],

    # entity_map maps DGS10 → yc_10y, but no registry row by that name exists,
    # so the mapping is dangling and the 10y has no fed feature at all.
    # treasury_10y (id 106) stopped at 2026-03-19 with no seed behind it.
    "treasury_10y":      ["yc_10y"],

    # spy_pcr (id 1430) is genuinely fresh — the options writer has been
    # updating it daily through 2026-09-10 — but it only starts 2026-03-25,
    # 153 observations, short of the 252 the rolling z-score needs. It will be
    # picked up automatically once it has the history.
    "put_call_ratio":    ["spy_pcr"],

    # ── No fed feature exists for these ──────────────────────────────
    # Every candidate is a registry row with no entity_map seed behind it, so
    # the resolver cannot refresh them whatever happens to it; all of them stop
    # between 2026-03-19 and 2026-04-04. An empty list drops the weight to zero
    # and logs the concept, which is the honest reading — scoring a column
    # frozen in April is what made regime_history report a five-month-old
    # market as today's.
    "move_index":        [],   # move_index id 174, ice_bofa_move id 184
    "vxn":               [],   # vxn id 173
    "chicago_fed":       [],   # chicago_fed id 194
    "crude_oil":         [],   # crude_oil id 125, eia_crude_price id 804
    "spy_rsi":           [],   # spy_rsi id 324, qqq_rsi id 326
}

# A candidate must carry at least this much resolved history before it can back
# a concept: the stress index is a 252-day rolling z-score, and a column with
# less than that contributes mostly its own warm-up.
MIN_CANDIDATE_OBSERVATIONS = 252

# Above this, run() logs a warning: the label describes the data's date, not
# today's, and dad's surfaces should say so.
MAX_FRESH_DATA_AGE_DAYS = 5


def _load_effective_weights(overrides: dict[str, float] | None = None) -> dict[str, float]:
    """Return effective weights: defaults merged with file overrides and/or explicit overrides."""
    weights = dict(DEFAULT_FEATURE_WEIGHTS)
    # Load from persisted override file
    if WEIGHTS_OVERRIDE_PATH.exists():
        try:
            with open(WEIGHTS_OVERRIDE_PATH) as f:
                file_overrides = json.load(f)
            weights.update(file_overrides)
            log.info("Loaded {n} weight overrides from {p}", n=len(file_overrides), p=WEIGHTS_OVERRIDE_PATH)
        except Exception as exc:
            log.warning("Failed to load weight overrides: {e}", e=str(exc))
    # Apply explicit overrides (from API call)
    if overrides:
        weights.update(overrides)
    return weights


# Active weights (loaded at import, refreshed per-run)
FEATURE_WEIGHTS: dict[str, float] = _load_effective_weights()

# Regime thresholds on the stress index
REGIME_THRESHOLDS = {
    "CRISIS":  {"s_min": 1.5,  "ds_min": 0.0},
    "FRAGILE": {"s_min": 0.6,  "ds_min": None},
    "NEUTRAL": {"s_min": -0.3, "s_max": 0.6},
    "GROWTH":  {"s_max": -0.3},
}

POSTURE_MAP = {
    "GROWTH": "AGGRESSIVE",
    "NEUTRAL": "BALANCED",
    "FRAGILE": "DEFENSIVE",
    "CRISIS": "CAPITAL_PRESERVATION",
}

# How many days to use for the derivative (smoothing window)
DERIVATIVE_WINDOW = 5

# Trailing window fed to the rolling z-scores. 756 trading-ish days ~= 3 years,
# which is what the 252-day rolling window needs to warm up plus headroom.
LOOKBACK_DAYS = 756

# ── regime_history vocabulary ───────────────────────────────────
#
# ``regime_history`` is the cross-lane table read by the chat regime context,
# api/routers/intel.py, the oracle prediction context, AstroGrid and the trial
# signal. Its ``regime`` column carries this module's own state names.
#
# That is what the rows already on griddb contain — the 2026-03 load wrote
# ``decision_journal.inferred_state`` verbatim with ``source='decision_journal'``
# — and it is what the readers that act on the value expect:
#
#   grid/signals/trial_signal.py gates BUY on {"GROWTH", "NEUTRAL"} and stores
#   the label into a CHECK-constrained column accepting only these four plus
#   UNKNOWN. oracle/prediction_context.py::canonical_regime folds all four into
#   its five-state bucket. api/routers/intel.py passes the label straight to the
#   briefing.
#
# The "Canonical Historical Regime Contract" in .coordination.md describes a
# different four-label vocabulary (risk_on / risk_off / neutral / transition).
# No writer has ever produced it — the 12 loaded rows are all 'NEUTRAL', which
# happens to read as valid under both — and adopting it here would collapse
# CRISIS into FRAGILE and silently turn every trial-signal BUY into a WATCHLIST.
# store/astrogrid.py::_normalize_regime_label is the one reader written against
# that document; it now folds these states into its own labels instead.
REGIME_HISTORY_LABELS = ("GROWTH", "NEUTRAL", "FRAGILE", "CRISIS")

# Provenance recorded in regime_history.source, distinguishing rows this writer
# produced from the 2026-03 one-off load that used source='decision_journal'.
REGIME_HISTORY_SOURCE = "auto_regime"


def _compute_stress_index(
    feature_matrix: "pd.DataFrame",
    feature_names: dict[int, str],
    weights: dict[str, float] | None = None,
) -> "np.ndarray":
    """Compute the weighted stress index for each row in the feature matrix.

    For each feature:
      1. Compute rolling z-score (252-day window)
      2. Multiply by the feature's weight
      3. Sum across all features

    Returns:
        1D array of stress index values, one per row.
    """

    active_weights = weights if weights is not None else FEATURE_WEIGHTS
    n_rows = len(feature_matrix)
    stress = np.zeros(n_rows)
    total_weight = 0.0
    contributions = {}

    for col in feature_matrix.columns:
        name = feature_names.get(col, "")
        weight = active_weights.get(name)
        if weight is None:
            continue

        series = feature_matrix[col].astype(float)
        # Rolling z-score: (value - rolling_mean) / rolling_std
        roll_mean = series.rolling(window=252, min_periods=30).mean()
        roll_std = series.rolling(window=252, min_periods=30).std().replace(0, 1)
        z = ((series - roll_mean) / roll_std).fillna(0).values

        stress += z * weight
        total_weight += abs(weight)
        # Track latest contribution for debugging
        if len(z) > 0:
            contributions[name] = {
                "z": round(float(z[-1]), 3),
                "weight": weight,
                "contribution": round(float(z[-1] * weight), 4),
            }

    # Normalize so the index is in z-score-like units
    if total_weight > 0:
        stress /= total_weight

    return stress, contributions


def _classify_regime(s: float, ds: float) -> tuple[str, float]:
    """Classify regime from stress index S and its derivative dS/dt.

    Returns (regime_name, confidence).
    """
    # CRISIS: high stress AND still worsening
    if s > 1.5 and ds >= 0:
        # Confidence: how far above 1.5
        conf = min(1.0, 0.7 + (s - 1.5) * 0.3)
        return "CRISIS", conf

    # FRAGILE: elevated stress OR deteriorating fast
    if s > 0.6 or ds > 0.15:
        # If both conditions met, higher confidence
        conf = 0.5
        if s > 0.6:
            conf += min(0.25, (s - 0.6) * 0.25)
        if ds > 0.15:
            conf += min(0.25, (ds - 0.15) * 1.0)
        return "FRAGILE", min(1.0, conf)

    # GROWTH: low stress and stable/improving
    if s < -0.3 and ds <= 0.05:
        conf = min(1.0, 0.6 + abs(s + 0.3) * 0.3)
        return "GROWTH", conf

    # NEUTRAL: everything else
    # Confidence is higher when squarely in the middle
    dist_to_edge = min(abs(s - 0.6), abs(s + 0.3))
    conf = min(1.0, 0.4 + dist_to_edge * 0.5)
    return "NEUTRAL", conf


def _pit_feature_frame(
    pit: PITStore,
    feature_ids: list[int],
    as_of: date,
    lookback_days: int = LOOKBACK_DAYS,
) -> "pd.DataFrame":
    """Return a PIT-correct wide feature matrix as known on ``as_of``.

    Goes through ``store.pit`` with the FIRST_RELEASE vintage policy so the
    frame contains only rows whose ``release_date`` is on or before ``as_of``,
    and asserts that explicitly before the caller can act on it. Recomputing a
    historical day with today's revised vintages would be lookahead, which is
    exactly what the backfill must not do.

    Parameters:
        pit: The PIT store.
        feature_ids: feature_registry IDs to load.
        as_of: Decision date. Nothing released after this date is included.
        lookback_days: Trailing window of observations to keep.

    Returns:
        pd.DataFrame indexed by obs_date, one column per feature_id. Empty if
        the PIT query returned nothing.
    """
    import pandas as pd

    raw = pit.get_pit(feature_ids, as_of, vintage_policy="FIRST_RELEASE")
    # get_pit already runs this as a safety net; call it again explicitly so the
    # guard is visible on the path that persists a row (ATTENTION.md #8 — it
    # raises rather than rolling anything back, so it must fire before any write).
    pit.assert_no_lookahead(raw, as_of)

    if raw.empty:
        return pd.DataFrame()

    window_start = as_of - timedelta(days=lookback_days)
    raw = raw[raw["obs_date"] >= window_start]
    if raw.empty:
        return pd.DataFrame()

    matrix = raw.pivot_table(
        index="obs_date",
        columns="feature_id",
        values="value",
        aggfunc="first",
    )
    matrix.index = pd.DatetimeIndex(matrix.index, name="obs_date")
    return matrix.sort_index()


def _frame_observation_dates(
    frame: "pd.DataFrame", as_of: date
) -> dict[Any, date]:
    """Newest real observation per column, as known on ``as_of``.

    Must be called on the raw pivot. After ``ffill`` every column carries a
    value on the last row whether or not anything was observed there, so a
    frame measured afterwards reports the same date for every column — which
    is precisely how a feature that stopped in April looked as current as one
    that updated this morning.

    Observations dated after ``as_of`` are excluded: ``store/pit.py`` bounds
    ``release_date`` but says nothing about ``obs_date``, and a frame cannot
    report knowing something the decision date could not.

    Returns:
        ``{column: newest obs_date}``, omitting columns with no usable value.
    """
    if frame.empty:
        return {}
    within = frame[[d.date() <= as_of for d in frame.index]]
    if within.empty:
        return {}
    dates: dict[Any, date] = {}
    for col in within.columns:
        last = within[col].last_valid_index()
        if last is not None:
            dates[col] = last.date()
    return dates


def _frame_data_as_of(frame: "pd.DataFrame", as_of: date) -> date | None:
    """Newest real observation date anywhere in ``frame``, as known on ``as_of``.

    See ``_frame_observation_dates`` — call this on the raw pivot, before any
    forward-fill.

    Returns:
        The newest qualifying ``obs_date``, or None if the frame has none.
    """
    dates = _frame_observation_dates(frame, as_of)
    return max(dates.values()) if dates else None


def _stale_frame_features(
    frame: "pd.DataFrame",
    fid_to_name: dict[int, str],
    as_of: date,
    max_age_days: int | None = None,
) -> dict[str, int]:
    """Concepts whose own newest observation is older than ``max_age_days``.

    ``data_as_of`` is the newest observation *anywhere* in the frame, so one
    feature a direct writer keeps current makes the whole frame look fresh.
    This is the per-concept view behind that number: which inputs are actually
    stale, and by how much.

    Returns:
        ``{concept: age in days}``, worst first.
    """
    if max_age_days is None:
        max_age_days = MAX_FRESH_DATA_AGE_DAYS
    dates = _frame_observation_dates(frame, as_of)
    stale = {
        fid_to_name[col]: (as_of - obs).days
        for col, obs in dates.items()
        if col in fid_to_name and (as_of - obs).days > max_age_days
    }
    return dict(sorted(stale.items(), key=lambda kv: kv[1], reverse=True))


# One statement, one round trip: every candidate feature for every weighted
# concept, with the resolved coverage that decides whether it can back one.
# LEFT JOIN so a registered-but-never-fed candidate comes back with n_obs = 0
# and is rejected here rather than silently contributing an all-NaN column.
_REGIME_CANDIDATE_SQL = text(
    "SELECT f.id, f.name, count(r.id) AS n_obs, max(r.obs_date) AS newest_obs "
    "FROM feature_registry f "
    "LEFT JOIN resolved_series r ON r.feature_id = f.id "
    "WHERE f.model_eligible = TRUE AND f.name = ANY(:names) "
    "GROUP BY f.id, f.name"
)


def _candidate_names(concept: str) -> list[str]:
    """Ordered feature_registry candidates that may back ``concept``.

    An entry present but empty means "nothing feeds this" and returns no
    candidates — distinct from a concept with no entry at all (a weight the
    operator added by hand), which falls back to its own name.
    """
    if concept in REGIME_FEATURE_SOURCES:
        return list(REGIME_FEATURE_SOURCES[concept])
    return [concept]


def _resolve_regime_bindings(
    engine, weights: dict[str, float]
) -> dict[str, dict[str, Any]]:
    """Bind each weighted concept to the feature that actually carries data.

    The weights are keyed by concept ("vix"), not by feature_registry row, so
    this is where a concept is bound to a real feature: the first candidate in
    ``REGIME_FEATURE_SOURCES`` that is model-eligible and has at least
    ``MIN_CANDIDATE_OBSERVATIONS`` resolved observations.

    A concept with no fed candidate is simply absent from the result, which
    drops its weight out of the index (``_compute_stress_index`` normalizes by
    the weights it actually used). That is logged, not hidden — a concept
    nobody feeds must not be quietly replaced by a neighbour's z-score.

    Returns:
        ``{concept: {"feature", "feature_id", "n_obs", "newest_obs"}}``.
    """
    wanted = list(weights.keys())
    candidates: list[str] = []
    for concept in wanted:
        for name in _candidate_names(concept):
            if name not in candidates:
                candidates.append(name)

    if not candidates:
        return {}

    with engine.connect() as conn:
        rows = conn.execute(_REGIME_CANDIDATE_SQL, {"names": candidates}).fetchall()

    by_name: dict[str, tuple[int, int, Any]] = {
        r[1]: (int(r[0]), int(r[2] or 0), r[3]) for r in rows
    }

    bindings: dict[str, dict[str, Any]] = {}
    claimed: set[int] = set()
    unfed: list[str] = []
    for concept in wanted:
        for name in _candidate_names(concept):
            entry = by_name.get(name)
            if entry is None:
                continue
            fid, n_obs, newest = entry
            if n_obs < MIN_CANDIDATE_OBSERVATIONS:
                continue
            if fid in claimed:
                # Two concepts cannot share one column — the second would
                # overwrite the first's weight lookup.
                continue
            claimed.add(fid)
            bindings[concept] = {
                "feature": name,
                "feature_id": fid,
                "n_obs": n_obs,
                "newest_obs": newest,
            }
            break
        else:
            unfed.append(concept)

    if bindings:
        log.info(
            "Regime concepts bound to features: {b}",
            b=", ".join(
                f"{c}←{b['feature']}(id={b['feature_id']}, n={b['n_obs']}, "
                f"newest={b['newest_obs']})"
                for c, b in bindings.items()
            ),
        )
    if unfed:
        log.warning(
            "Regime concepts with no fed feature — weight dropped to zero: {c}",
            c=unfed,
        )
    return bindings


def _resolve_regime_features(engine, weights: dict[str, float]) -> dict[int, str]:
    """Return ``{feature_id: concept}`` for every weighted concept that is fed.

    Keying the value by *concept* rather than by the feature's own name is what
    keeps ``DEFAULT_FEATURE_WEIGHTS``, ``outputs/regime_weights.json`` and the
    API sliders working unchanged: ``_compute_stress_index`` looks the weight up
    by this value.
    """
    return _bindings_to_feature_map(_resolve_regime_bindings(engine, weights))


def _bindings_to_feature_map(bindings: dict[str, dict[str, Any]]) -> dict[int, str]:
    """``{concept: binding}`` → ``{feature_id: concept}``."""
    return {b["feature_id"]: concept for concept, b in bindings.items()}


def _binding_summary(bindings: dict[str, dict[str, Any]]) -> dict[str, str]:
    """``{concept: feature_name}`` — what backed each concept on this run."""
    return {concept: b["feature"] for concept, b in bindings.items()}


def compute_regime_at(
    engine,
    as_of: date,
    weights: dict[str, float] | None = None,
    fid_to_name: dict[int, str] | None = None,
) -> dict[str, Any]:
    """Classify the regime as it would have been read on ``as_of``.

    This is the PIT-correct core shared by the daily run and the backfill:
    every value it sees was already released on ``as_of``.

    Parameters:
        engine: SQLAlchemy engine.
        as_of: Decision date.
        weights: Feature weights; defaults to the module's active weights.
        fid_to_name: Pre-resolved feature map, to avoid re-querying
            feature_registry once per day in a backfill loop.

    Returns:
        Dict with regime, confidence, stress_index, stress_derivative,
        n_observations, ``data_as_of`` (the newest real observation behind the
        reading, never later than ``as_of``), ``data_staleness_days`` and
        ``stale_features`` — or an ``error`` key when there is not enough
        point-in-time data to classify.
    """
    active_weights = weights if weights is not None else FEATURE_WEIGHTS
    if fid_to_name is None:
        fid_to_name = _resolve_regime_features(engine, active_weights)

    if len(fid_to_name) < 3:
        return {
            "regime": "UNKNOWN",
            "confidence": 0.0,
            "error": f"insufficient features: {len(fid_to_name)}",
        }

    pit = PITStore(engine)
    df = _pit_feature_frame(pit, list(fid_to_name), as_of)
    if df.empty or len(df) < 50:
        return {
            "regime": "UNKNOWN",
            "confidence": 0.0,
            "error": f"insufficient data: {df.shape}",
        }

    # Measured before ffill: after the fill every column reaches the last row,
    # so the frame would claim to be as fresh as the date it was computed for
    # no matter how old the last real observation is. That is exactly how a
    # five-month-old reading came to be stamped with today's date.
    data_as_of = _frame_data_as_of(df, as_of)
    stale_features = _stale_frame_features(df, fid_to_name, as_of)

    df = df.ffill().bfill().dropna(axis=1, how="all")

    stress_series, contributions = _compute_stress_index(df, fid_to_name, active_weights)
    s_current = float(stress_series[-1]) if len(stress_series) > 0 else 0.0
    if len(stress_series) > DERIVATIVE_WINDOW:
        ds = float(stress_series[-1] - stress_series[-1 - DERIVATIVE_WINDOW]) / DERIVATIVE_WINDOW
    else:
        ds = 0.0

    regime, confidence = _classify_regime(s_current, ds)

    return {
        "regime": regime,
        "confidence": round(confidence, 4),
        "posture": POSTURE_MAP.get(regime, "BALANCED"),
        "stress_index": round(s_current, 4),
        "stress_derivative": round(ds, 4),
        "contributions": contributions,
        "n_features": len(fid_to_name),
        "n_observations": len(df),
        "data_as_of": data_as_of,
        "data_staleness_days": (
            None if data_as_of is None else (as_of - data_as_of).days
        ),
        "stale_features": stale_features,
    }


# Two complete literal statements rather than one assembled from a fragment.
# The fragment was constant, so there was no injection path, but
# .claude/rules/security.md bans f-strings, .format() and concatenation in SQL
# outright — a rule worth keeping absolute, because the moment a fragment stops
# being constant the review that would have caught it has already happened.
_REGIME_HISTORY_UPSERT_SQL = text(
    "INSERT INTO regime_history (obs_date, regime, confidence, source, data_as_of) "
    "VALUES (:obs_date, :regime, :confidence, :source, :data_as_of) "
    "ON CONFLICT (obs_date) DO UPDATE SET "
    "regime = EXCLUDED.regime, "
    "confidence = EXCLUDED.confidence, "
    "source = EXCLUDED.source, "
    "data_as_of = EXCLUDED.data_as_of"
)

_REGIME_HISTORY_INSERT_IGNORE_SQL = text(
    "INSERT INTO regime_history (obs_date, regime, confidence, source, data_as_of) "
    "VALUES (:obs_date, :regime, :confidence, :source, :data_as_of) "
    "ON CONFLICT (obs_date) DO NOTHING"
)


def persist_regime_history(
    engine,
    obs_date: date,
    label: str,
    confidence: float,
    overwrite: bool = True,
    data_as_of: date | None = None,
) -> bool:
    """Upsert one row into ``regime_history``.

    One row per observation date, stamped with ``source`` so a reader can tell
    a row this writer produced from one the 2026-03 one-off load left behind.

    Parameters:
        engine: SQLAlchemy engine.
        obs_date: The date the label describes.
        label: One of ``REGIME_HISTORY_LABELS``.
        confidence: 0-1 confidence score.
        overwrite: When False, an existing row for ``obs_date`` is left alone.
        data_as_of: Newest real observation the label was computed from. This
            is what says whether the row is a fresh read or a stale one wearing
            today's date; None when the caller could not establish it.

    Returns:
        True if a row was written or updated.

    Raises:
        ValueError: If ``label`` is outside the vocabulary, ``confidence`` is
            not a finite number in [0, 1], or ``data_as_of`` is later than
            ``obs_date``. A label the readers do not recognize is worse than no
            row — trial_signal would store it as UNKNOWN and downgrade every
            BUY — so this refuses rather than writing something downstream will
            misread. Data dated after the day it describes would be lookahead.
    """
    if label not in REGIME_HISTORY_LABELS:
        raise ValueError(
            f"regime_history label {label!r} is outside the vocabulary "
            f"{REGIME_HISTORY_LABELS}"
        )
    conf = float(confidence)
    if not np.isfinite(conf) or not (0.0 <= conf <= 1.0):
        raise ValueError(f"regime confidence must be finite and in [0, 1], got {confidence!r}")
    if data_as_of is not None and data_as_of > obs_date:
        raise ValueError(
            f"data_as_of {data_as_of} is after the row's obs_date {obs_date}"
        )

    stmt = _REGIME_HISTORY_UPSERT_SQL if overwrite else _REGIME_HISTORY_INSERT_IGNORE_SQL
    with engine.begin() as conn:
        result = conn.execute(
            stmt,
            {
                "obs_date": obs_date,
                "regime": label,
                "confidence": conf,
                "source": REGIME_HISTORY_SOURCE,
                "data_as_of": data_as_of,
            },
        )
    return bool(result.rowcount)


def backfill_regime_history(
    engine,
    start: date,
    end: date,
    overwrite: bool = False,
    max_days: int = 400,
    weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Recompute and persist daily regime_history rows over a bounded range.

    Each day is classified with ``as_of`` set to that day, so a backfilled row
    only ever reflects data that had actually been released by then. Days the
    PIT store cannot support (not enough released history) are skipped and
    reported rather than written with a fabricated label.

    Parameters:
        engine: SQLAlchemy engine.
        start: First date to compute, inclusive.
        end: Last date to compute, inclusive.
        overwrite: Replace rows that already exist for a date.
        max_days: Hard cap on the span, so a typo cannot start a multi-year job.
        weights: Feature weights; defaults to the module's active weights.

    Returns:
        Dict with written / skipped / failed counts and the per-day labels.

    Raises:
        ValueError: If the range is inverted or wider than ``max_days``.
    """
    if end < start:
        raise ValueError(f"backfill end {end} is before start {start}")
    span = (end - start).days + 1
    if span > max_days:
        raise ValueError(
            f"backfill span of {span} days exceeds max_days={max_days}; "
            "narrow the range or raise --max-days deliberately"
        )

    active_weights = weights if weights is not None else _load_effective_weights()
    fid_to_name = _resolve_regime_features(engine, active_weights)
    if len(fid_to_name) < 3:
        return {
            "written": 0,
            "skipped": 0,
            "failed": span,
            "error": f"insufficient features: {len(fid_to_name)}",
        }

    existing: set[date] = set()
    if not overwrite:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT obs_date FROM regime_history "
                    "WHERE obs_date >= :start AND obs_date <= :end"
                ),
                {"start": start, "end": end},
            ).fetchall()
        existing = {r[0] for r in rows}

    written = 0
    skipped = 0
    failed = 0
    labels: dict[str, str] = {}

    current = start
    while current <= end:
        if current in existing:
            skipped += 1
            current += timedelta(days=1)
            continue
        try:
            result = compute_regime_at(
                engine, current, weights=active_weights, fid_to_name=fid_to_name
            )
            if result.get("error"):
                log.warning(
                    "Regime backfill {d}: {e}", d=current.isoformat(), e=result["error"]
                )
                failed += 1
            else:
                persist_regime_history(
                    engine,
                    current,
                    result["regime"],
                    result["confidence"],
                    overwrite=True,
                    data_as_of=result.get("data_as_of"),
                )
                labels[current.isoformat()] = result["regime"]
                written += 1
        except Exception as exc:
            log.warning(
                "Regime backfill {d} failed: {e}", d=current.isoformat(), e=str(exc)
            )
            failed += 1
        current += timedelta(days=1)

    log.info(
        "Regime backfill {s} to {e} — {w} written, {sk} skipped, {f} failed",
        s=start.isoformat(), e=end.isoformat(), w=written, sk=skipped, f=failed,
    )
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "written": written,
        "skipped": skipped,
        "failed": failed,
        "labels": labels,
    }


def run_with_weights(engine, weights: dict[str, float], save: bool = False) -> dict[str, Any]:
    """Run regime classification with custom weights, optionally saving them.

    Args:
        engine: SQLAlchemy engine.
        weights: Full weight dict (merged with defaults by caller).
        save: If True, persist weights to the override file.

    Returns:
        Dict with regime, confidence, stress_index, etc.
    """
    if save:
        WEIGHTS_OVERRIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(WEIGHTS_OVERRIDE_PATH, "w") as f:
            # Only save overrides that differ from defaults
            overrides = {k: v for k, v in weights.items() if v != DEFAULT_FEATURE_WEIGHTS.get(k)}
            json.dump(overrides, f, indent=2)
        log.info("Saved {n} weight overrides to {p}", n=len(overrides), p=WEIGHTS_OVERRIDE_PATH)
        # Refresh module-level weights
        global FEATURE_WEIGHTS
        FEATURE_WEIGHTS = _load_effective_weights()

    pit = PITStore(engine)

    # Concept → fed feature, same alias layer the scheduled run uses, so the
    # sliders move the same index the daily row is written from.
    bindings = _resolve_regime_bindings(engine, weights)
    fid_to_name = _bindings_to_feature_map(bindings)
    if len(fid_to_name) < 3:
        return {"regime": "UNKNOWN", "confidence": 0.0, "error": "insufficient features"}

    fids = list(fid_to_name)

    today = date.today()
    df = _pit_feature_frame(pit, fids, today)
    if df.empty or len(df) < 50:
        return {"regime": "UNKNOWN", "confidence": 0.0, "error": f"insufficient data: {df.shape}"}

    data_as_of = _frame_data_as_of(df, today)
    stale_features = _stale_frame_features(df, fid_to_name, today)
    df = df.ffill().bfill().dropna(axis=1, how="all")

    stress_series, contributions = _compute_stress_index(df, fid_to_name, weights)

    s_current = float(stress_series[-1]) if len(stress_series) > 0 else 0.0
    if len(stress_series) > DERIVATIVE_WINDOW:
        ds = float(stress_series[-1] - stress_series[-1 - DERIVATIVE_WINDOW]) / DERIVATIVE_WINDOW
    else:
        ds = 0.0

    regime, confidence = _classify_regime(s_current, ds)
    posture = POSTURE_MAP.get(regime, "BALANCED")

    sorted_contribs = sorted(contributions.items(), key=lambda x: abs(x[1]["contribution"]), reverse=True)

    return {
        "regime": regime,
        "confidence": round(confidence, 4),
        "posture": posture,
        "stress_index": round(s_current, 4),
        "stress_derivative": round(ds, 4),
        "contributions": {k: v for k, v in sorted_contribs[:10]},
        "n_features": len(fid_to_name),
        "weights": weights,
        "data_as_of": data_as_of.isoformat() if data_as_of else None,
        "data_staleness_days": (
            None if data_as_of is None else (today - data_as_of).days
        ),
        "features_used": _binding_summary(bindings),
        "stale_features": stale_features,
    }


def run() -> dict[str, Any]:
    """Run regime detection and update decision_journal."""
    # Refresh weights from override file each run
    global FEATURE_WEIGHTS
    FEATURE_WEIGHTS = _load_effective_weights()

    engine = get_engine()
    pit = PITStore(engine)

    # Get production model ID
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM model_registry WHERE state='PRODUCTION' AND layer='REGIME' LIMIT 1")
        ).fetchone()
    if not row:
        log.warning("No production regime model found")
        return {"regime": "UNKNOWN", "confidence": 0.0, "error": "no production model"}
    model_id = row[0]

    # Bind every weighted concept to the feature that is actually fed. The
    # concept names are the knobs (weights file, API sliders); the features
    # behind them are whatever the pipeline currently keeps current.
    feature_names_list = list(FEATURE_WEIGHTS.keys())
    bindings = _resolve_regime_bindings(engine, FEATURE_WEIGHTS)
    fid_to_name = _bindings_to_feature_map(bindings)

    if len(fid_to_name) < 3:
        log.warning("Only {n} regime concepts are fed", n=len(fid_to_name))
        return {"regime": "UNKNOWN", "confidence": 0.0, "error": "insufficient features"}

    fids = list(fid_to_name)
    found_names = set(fid_to_name.values())
    missing = [n for n in feature_names_list if n not in found_names]
    if missing:
        log.info("Regime concepts with no fed feature: {m}", m=missing)

    log.info("Regime detection using {n} features", n=len(fids))

    # Build feature matrix (2+ years for rolling z-score computation).
    # _pit_feature_frame is the shared PIT core — FIRST_RELEASE vintages and an
    # explicit assert_no_lookahead before anything downstream persists a row.
    today = date.today()
    df = _pit_feature_frame(pit, fids, today)
    if df.empty or len(df) < 50:
        return {"regime": "UNKNOWN", "confidence": 0.0, "error": f"insufficient data: {df.shape}"}

    # Before ffill — see _frame_data_as_of. This is the date the reading is
    # really about, as opposed to the date it was computed on.
    data_as_of = _frame_data_as_of(df, today)
    data_staleness_days = None if data_as_of is None else (today - data_as_of).days
    stale_features = _stale_frame_features(df, fid_to_name, today)
    if data_staleness_days is None:
        log.warning("Regime inputs carry no usable observation date")
    elif data_staleness_days > MAX_FRESH_DATA_AGE_DAYS:
        log.warning(
            "Regime inputs are {n} days old (newest observation {d}) — the "
            "label describes that date, not today",
            n=data_staleness_days, d=data_as_of.isoformat(),
        )
    if stale_features:
        # data_as_of is the newest observation anywhere in the frame, so a
        # single feature a direct writer keeps current hides the rest. Name
        # them: these are the concepts whose z-score is being carried forward.
        log.warning(
            "Regime inputs stale beyond {m} days: {f}",
            m=MAX_FRESH_DATA_AGE_DAYS,
            f=", ".join(f"{k}({v}d)" for k, v in stale_features.items()),
        )

    df = df.ffill().bfill().dropna(axis=1, how="all")

    # Compute stress index
    stress_series, contributions = _compute_stress_index(df, fid_to_name)

    # Current stress value (last valid)
    s_current = float(stress_series[-1]) if len(stress_series) > 0 else 0.0

    # First derivative: rate of change over DERIVATIVE_WINDOW days
    if len(stress_series) > DERIVATIVE_WINDOW:
        ds = float(stress_series[-1] - stress_series[-1 - DERIVATIVE_WINDOW]) / DERIVATIVE_WINDOW
    else:
        ds = 0.0

    # Classify
    regime, confidence = _classify_regime(s_current, ds)
    posture = POSTURE_MAP.get(regime, "BALANCED")

    # Historical regime distribution (last 504 days)
    lookback = stress_series[-504:] if len(stress_series) > 504 else stress_series
    dist = {"GROWTH": 0, "NEUTRAL": 0, "FRAGILE": 0, "CRISIS": 0}
    for i, s_val in enumerate(lookback):
        if i > DERIVATIVE_WINDOW:
            d = (lookback[i] - lookback[i - DERIVATIVE_WINDOW]) / DERIVATIVE_WINDOW
        else:
            d = 0.0
        r, _ = _classify_regime(float(s_val), float(d))
        dist[r] = dist.get(r, 0) + 1

    # Transition probability (how often regime changed in last 252 days)
    recent = lookback[-252:] if len(lookback) > 252 else lookback
    regimes_recent = []
    for i, s_val in enumerate(recent):
        if i > DERIVATIVE_WINDOW:
            d = (recent[i] - recent[i - DERIVATIVE_WINDOW]) / DERIVATIVE_WINDOW
        else:
            d = 0.0
        r, _ = _classify_regime(float(s_val), float(d))
        regimes_recent.append(r)
    transitions = sum(1 for i in range(1, len(regimes_recent)) if regimes_recent[i] != regimes_recent[i - 1])
    trans_prob = transitions / max(len(regimes_recent), 1)

    # Contradiction flags
    contradictions = {}
    # Sort contributions by absolute contribution
    sorted_contribs = sorted(contributions.items(), key=lambda x: abs(x[1]["contribution"]), reverse=True)
    top_stress = [c for c in sorted_contribs if c[1]["contribution"] > 0][:3]
    top_calm = [c for c in sorted_contribs if c[1]["contribution"] < 0][:3]

    if regime == "GROWTH" and top_stress:
        biggest_stress = top_stress[0]
        if biggest_stress[1]["contribution"] > 0.05:
            contradictions["stress_in_growth"] = (
                f"{biggest_stress[0]} z={biggest_stress[1]['z']:.1f} is elevated despite GROWTH regime"
            )
    if regime in ("FRAGILE", "CRISIS") and top_calm:
        biggest_calm = top_calm[0]
        if biggest_calm[1]["contribution"] < -0.05:
            contradictions["calm_in_stress"] = (
                f"{biggest_calm[0]} z={biggest_calm[1]['z']:.1f} is calm despite {regime} regime"
            )

    # Insert into decision_journal via DecisionJournal class
    # (ensures validation: NaN/Inf rejection, confidence range, etc.)
    from journal.log import DecisionJournal

    journal = DecisionJournal(db_engine=engine)
    journal.log_decision(
        model_version_id=model_id,
        inferred_state=regime,
        state_confidence=confidence,
        transition_probability=trans_prob,
        contradiction_flags=contradictions,
        grid_recommendation=posture,
        baseline_recommendation="NEUTRAL",
        action_taken=f"AUTO_{posture}",
        counterfactual=f"S={s_current:.2f}, dS/dt={ds:.3f}",
        operator_confidence="HIGH",
    )

    # Persist the canonical daily row into regime_history. This is the table
    # the chat regime context, the oracle prediction context, AstroGrid and the
    # HMM transition model read. Until this call existed, auto_regime wrote only
    # decision_journal and analytical_snapshots, so regime_history froze at
    # whatever a one-off load had put there.
    regime_history_written = False
    regime_history_error: str | None = None
    try:
        regime_history_written = persist_regime_history(
            engine, today, regime, confidence, data_as_of=data_as_of
        )
        log.info(
            "regime_history updated — {d} = {l} (data_as_of {da}, {n} days old)",
            d=today.isoformat(), l=regime,
            da=data_as_of.isoformat() if data_as_of else "unknown",
            n=data_staleness_days if data_staleness_days is not None else "?",
        )
    except Exception as exc:
        regime_history_error = str(exc)
        log.warning("regime_history persistence failed: {e}", e=regime_history_error)

    # Persist snapshot
    try:
        from store.snapshots import AnalyticalSnapshotStore

        snap_store = AnalyticalSnapshotStore(db_engine=engine)
        snap_store.save_snapshot(
            category="regime_detection",
            payload={
                "regime": regime,
                "confidence": confidence,
                "posture": posture,
                "stress_index": round(s_current, 4),
                "stress_derivative": round(ds, 4),
                "transition_probability": trans_prob,
                "distribution": dist,
                "contradictions": contradictions,
                "contributions": {k: v for k, v in sorted_contribs[:10]},
                "n_features": len(fid_to_name),
                "n_observations": len(df),
                "features_used": _binding_summary(bindings),
                "features_missing": missing,
                "data_as_of": data_as_of.isoformat() if data_as_of else None,
                "data_staleness_days": data_staleness_days,
                "stale_features": stale_features,
            },
            as_of_date=today,
            metrics={
                "regime": regime,
                "confidence": round(confidence, 4),
                "stress_index": round(s_current, 4),
                "stress_derivative": round(ds, 4),
            },
        )
    except Exception as exc:
        log.warning("Snapshot persistence failed: {e}", e=str(exc))

    result = {
        "regime": regime,
        "regime_history_written": regime_history_written,
        "regime_history_error": regime_history_error,
        "data_as_of": data_as_of.isoformat() if data_as_of else None,
        "data_staleness_days": data_staleness_days,
        "stale_features": stale_features,
        "confidence": confidence,
        "posture": posture,
        "stress_index": round(s_current, 4),
        "stress_derivative": round(ds, 4),
        "transition_probability": trans_prob,
        "distribution": dist,
        "contradictions": contradictions,
        "top_stress_drivers": [(k, v) for k, v in top_stress],
        "top_calm_drivers": [(k, v) for k, v in top_calm],
        "n_features": len(fid_to_name),
    }

    log.info("=== AUTO REGIME UPDATE ===")
    log.info("Regime:      {}", regime)
    log.info("Confidence:  {:.1%}", confidence)
    log.info("Posture:     {}", posture)
    log.info("Stress(S):   {:.3f}", s_current)
    log.info("dS/dt:       {:.4f}", ds)
    log.info("Trans prob:  {:.1%}", trans_prob)
    log.info("Distribution: {}", dist)
    stress_str = ', '.join(f"{k}({v['contribution']:+.3f})" for k, v in top_stress)
    calm_str = ', '.join(f"{k}({v['contribution']:+.3f})" for k, v in top_calm)
    log.info("Top stress:  {}", stress_str)
    log.info("Top calm:    {}", calm_str)
    if contradictions:
        log.info("Flags:       {}", contradictions)
    log.info("Features:    {} used, {} missing", len(fid_to_name), len(missing))
    log.info(
        "Data as of:  {} ({} days old)",
        data_as_of.isoformat() if data_as_of else "unknown",
        data_staleness_days if data_staleness_days is not None else "?",
    )
    log.info("Updated decision_journal")

    # Broadcast regime change to WebSocket clients if regime shifted
    try:
        from api.main import broadcast_event
        # Check previous regime from journal
        with engine.connect() as conn:
            prev_row = conn.execute(
                text(
                    "SELECT inferred_state FROM decision_journal "
                    "ORDER BY decision_timestamp DESC OFFSET 1 LIMIT 1"
                )
            ).fetchone()
        prev_regime = prev_row[0] if prev_row else None
        if prev_regime and prev_regime != regime:
            broadcast_event("regime_change", {
                "from": prev_regime,
                "to": regime,
                "confidence": round(confidence, 4),
                "stress_index": round(s_current, 4),
                "posture": posture,
            })
    except Exception:
        pass  # graceful degradation

    return result


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: daily run, or a bounded point-in-time backfill."""
    parser = argparse.ArgumentParser(description="GRID auto-regime detection")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Recompute regime_history rows over a date range instead of running for today",
    )
    parser.add_argument("--start", help="Backfill start date, YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--end",
        help="Backfill end date, YYYY-MM-DD (inclusive). Defaults to today.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace regime_history rows that already exist in the range",
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=400,
        help="Hard cap on the backfill span (default: 400)",
    )
    args = parser.parse_args(argv)

    if not args.backfill:
        run()
        return 0

    if not args.start:
        parser.error("--backfill requires --start")
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else date.today()

    engine = get_engine()
    result = backfill_regime_history(
        engine,
        start,
        end,
        overwrite=args.overwrite,
        max_days=args.max_days,
    )
    print(json.dumps({k: v for k, v in result.items() if k != "labels"}, indent=2))
    return 0 if not result.get("error") else 1


if __name__ == "__main__":
    raise SystemExit(main())
