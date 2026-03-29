#!/usr/bin/env python3
"""Compute derived features from existing data — fills gaps without new ingestion.

Reads resolved_series inputs, applies transformations (ratios, spreads, rolling
changes, slopes, momentum), and inserts results back into resolved_series using
the bulk ON CONFLICT DO NOTHING pattern.

Usage:
  python scripts/compute_derived_features.py                 # compute all
  python scripts/compute_derived_features.py --family rates  # one family
  python scripts/compute_derived_features.py --dry-run       # preview only
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_engine

# ── Source catalog ID for computed features ──────────────────────────────────
COMPUTED_SOURCE_ID = 183  # source_catalog.name = 'computed'

# ── Computation definitions ──────────────────────────────────────────────────
# Each entry defines:
#   name       — target feature_registry.name
#   inputs     — list of feature_registry.name(s) to load from resolved_series
#   op         — operation to apply
#   params     — extra parameters for the operation
#   family     — for filtering (informational)
#   raw_inputs — optional: load from raw_series instead of resolved_series
#                format: list of (series_id, label) tuples
#
# Operations:
#   "alias"       — copy input as-is (identity mapping)
#   "spread"      — a - b
#   "ratio"       — a / b
#   "pct_change"  — rolling % change over N periods
#   "diff"        — rolling absolute difference over N periods
#   "slope"       — rolling OLS slope over N periods (normalised by mean)
#   "momentum"    — (price / price_lag) - 1
#   "rolling_max_pct" — current / rolling_max(all_history)
#   "cumsum_ratio"    — cumulative sum ratio for adline
#   "slope_of"        — slope of another computed series

COMPUTATIONS = [
    # ── FX ────────────────────────────────────────────────────────────────
    {
        "name": "dxy_index",
        "inputs": ["dxy_etf"],
        "op": "alias",
        "params": {},
        "family": "fx",
    },
    {
        "name": "dxy_3m_chg",
        "inputs": ["dxy_etf"],
        "op": "pct_change",
        "params": {"periods": 63},
        "family": "fx",
    },

    # ── Rates ─────────────────────────────────────────────────────────────
    {
        "name": "real_ffr",
        "inputs": ["fed_funds_rate", "cpi_yoy"],
        "op": "spread",
        "params": {},
        "family": "rates",
    },
    {
        "name": "sofr_spread_to_ffr",
        "inputs": ["sofr", "fed_funds_rate"],
        "op": "spread",
        "params": {},
        "family": "rates",
    },
    {
        "name": "treasury_bill_spread",
        "inputs": ["yld_curve_3m10y"],
        "op": "negate",
        "params": {},
        "family": "rates",
        "note": "3m10y spread is 10y-3m; negate for 3m-FFR proxy",
    },
    {
        "name": "rrp_as_pct_of_peak",
        "inputs": ["reverse_repo"],
        "op": "rolling_max_pct",
        "params": {},
        "family": "rates",
    },

    # ── Breadth ───────────────────────────────────────────────────────────
    {
        "name": "sp500_mom_3m",
        "inputs": ["sp500"],
        "op": "pct_change",
        "params": {"periods": 63},
        "family": "breadth",
    },
    {
        "name": "sp500_mom_12_1",
        "inputs": ["sp500"],
        "op": "momentum_12_1",
        "params": {},
        "family": "breadth",
        "note": "12-month return minus 1-month return (Carhart momentum)",
    },
    {
        "name": "sp500_adline",
        "inputs": ["sp500"],
        "op": "adline_proxy",
        "params": {},
        "family": "breadth",
        "note": "Cumulative sign of daily returns as AD-line proxy",
    },
    {
        "name": "sp500_adline_slope",
        "inputs": ["sp500"],
        "op": "adline_slope",
        "params": {"window": 63},
        "family": "breadth",
    },
    {
        "name": "sp500_pct_above_200ma",
        "inputs": ["sp500"],
        "op": "pct_above_ma",
        "params": {"window": 200},
        "family": "breadth",
        "note": "Binary 0/1 whether index is above its 200-day MA (proxy for breadth)",
    },

    # ── Credit ────────────────────────────────────────────────────────────
    {
        "name": "hy_spread_3m_chg",
        "inputs": ["hy_oas_spread"],
        "op": "diff",
        "params": {"periods": 63},
        "family": "credit",
    },
    {
        "name": "hy_spread_proxy",
        "inputs": ["hy_oas_spread"],
        "op": "alias",
        "params": {},
        "family": "credit",
        "note": "hy_spread_proxy is hy_oas_spread under a different name",
    },
    {
        "name": "copper_gold_ratio",
        "inputs": ["copper", "gold"],
        "op": "ratio",
        "params": {},
        "family": "commodity",
    },
    {
        "name": "copper_gold_slope",
        "inputs": ["copper", "gold"],
        "op": "ratio_slope",
        "params": {"window": 63},
        "family": "commodity",
    },

    # ── Vol ────────────────────────────────────────────────────────────────
    {
        "name": "vix_3m_ratio",
        "inputs": [],
        "raw_inputs": [("CBOE:VIX3M", "vix3m")],
        "resolved_inputs": ["vix_spot"],
        "op": "vix_term_structure",
        "params": {},
        "family": "vol",
        "note": "VIX / VIX3M from raw CBOE data + resolved vix_spot",
    },

    # ── Macro ─────────────────────────────────────────────────────────────
    {
        "name": "conf_board_lei_slope",
        "inputs": ["conf_board_lei"],
        "op": "slope",
        "params": {"window": 63},
        "family": "macro",
    },
]


# ── Helper: load a resolved series as a pandas Series ────────────────────────

def load_resolved(engine, feature_name: str) -> pd.Series:
    """Load a resolved_series feature into a pandas Series indexed by obs_date."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT rs.obs_date, rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON fr.id = rs.feature_id
                WHERE fr.name = :name
                ORDER BY rs.obs_date
            """),
            {"name": feature_name},
        ).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series(
        [float(r[1]) for r in rows],
        index=pd.DatetimeIndex([r[0] for r in rows]),
        name=feature_name,
    )
    return s[~s.index.duplicated(keep="last")]


def load_raw(engine, series_id: str) -> pd.Series:
    """Load a raw_series by series_id into a pandas Series indexed by obs_date."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT obs_date, value FROM raw_series
                WHERE series_id = :sid
                ORDER BY obs_date
            """),
            {"sid": series_id},
        ).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series(
        [float(r[1]) for r in rows],
        index=pd.DatetimeIndex([r[0] for r in rows]),
        name=series_id,
    )
    return s[~s.index.duplicated(keep="last")]


def get_feature_id(engine, feature_name: str) -> int | None:
    """Look up feature_registry.id for a given name."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM feature_registry WHERE name = :name"),
            {"name": feature_name},
        ).fetchone()
    return row[0] if row else None


# ── Operation implementations ────────────────────────────────────────────────

def op_alias(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """Identity — return first input as-is."""
    key = list(inputs.keys())[0]
    return inputs[key].copy()


def op_spread(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """a - b, aligned on date."""
    keys = list(inputs.keys())
    a, b = inputs[keys[0]], inputs[keys[1]]
    df = pd.DataFrame({"a": a, "b": b}).dropna()
    return df["a"] - df["b"]


def op_ratio(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """a / b, aligned on date."""
    keys = list(inputs.keys())
    a, b = inputs[keys[0]], inputs[keys[1]]
    df = pd.DataFrame({"a": a, "b": b}).dropna()
    result = df["a"] / df["b"]
    return result.replace([np.inf, -np.inf], np.nan).dropna()


def op_negate(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """Return -1 * input (for inverting spreads)."""
    key = list(inputs.keys())[0]
    return -inputs[key]


def op_pct_change(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """Rolling percentage change over N periods."""
    key = list(inputs.keys())[0]
    s = inputs[key]
    periods = params.get("periods", 63)
    result = s.pct_change(periods=periods)
    return result.replace([np.inf, -np.inf], np.nan).dropna()


def op_diff(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """Rolling absolute difference over N periods."""
    key = list(inputs.keys())[0]
    s = inputs[key]
    periods = params.get("periods", 63)
    return s.diff(periods=periods).dropna()


def op_slope(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """Rolling OLS slope over a window, normalised by rolling mean."""
    key = list(inputs.keys())[0]
    s = inputs[key]
    window = params.get("window", 63)
    x = np.arange(window, dtype=float)
    x_demean = x - x.mean()
    denom = (x_demean ** 2).sum()

    values = s.values.astype(float)
    slopes = np.full(len(values), np.nan)
    for i in range(window, len(values)):
        y = values[i - window : i]
        if np.any(np.isnan(y)):
            continue
        y_demean = y - y.mean()
        slopes[i] = (x_demean * y_demean).sum() / denom

    result = pd.Series(slopes, index=s.index, name="slope")
    return result.dropna()


def op_rolling_max_pct(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """current / expanding_max(history) — shows drawdown from peak."""
    key = list(inputs.keys())[0]
    s = inputs[key]
    expanding_max = s.expanding().max()
    result = s / expanding_max
    return result.replace([np.inf, -np.inf], np.nan).dropna()


def op_momentum_12_1(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """Carhart-style momentum: 12-month return minus 1-month return."""
    key = list(inputs.keys())[0]
    s = inputs[key]
    ret_12m = s.pct_change(periods=252)
    ret_1m = s.pct_change(periods=21)
    result = ret_12m - ret_1m
    return result.replace([np.inf, -np.inf], np.nan).dropna()


def op_adline_proxy(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """Cumulative sign of daily returns as an AD-line proxy."""
    key = list(inputs.keys())[0]
    s = inputs[key]
    daily_ret = s.pct_change().dropna()
    signs = np.sign(daily_ret)
    adline = signs.cumsum()
    return adline


def op_adline_slope(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """Slope of the AD-line proxy over a rolling window."""
    key = list(inputs.keys())[0]
    s = inputs[key]
    daily_ret = s.pct_change().dropna()
    signs = np.sign(daily_ret)
    adline = signs.cumsum()

    window = params.get("window", 63)
    x = np.arange(window, dtype=float)
    x_demean = x - x.mean()
    denom = (x_demean ** 2).sum()

    values = adline.values.astype(float)
    slopes = np.full(len(values), np.nan)
    for i in range(window, len(values)):
        y = values[i - window : i]
        y_demean = y - y.mean()
        slopes[i] = (x_demean * y_demean).sum() / denom

    return pd.Series(slopes, index=adline.index).dropna()


def op_pct_above_ma(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """1 if price > moving average, 0 otherwise. Proxy for breadth."""
    key = list(inputs.keys())[0]
    s = inputs[key]
    window = params.get("window", 200)
    ma = s.rolling(window=window).mean()
    above = (s > ma).astype(float)
    return above.iloc[window:]  # drop warmup


def op_ratio_slope(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """Slope of a ratio (a/b) over a rolling window."""
    keys = list(inputs.keys())
    a, b = inputs[keys[0]], inputs[keys[1]]
    df = pd.DataFrame({"a": a, "b": b}).dropna()
    ratio = (df["a"] / df["b"]).replace([np.inf, -np.inf], np.nan).dropna()

    window = params.get("window", 63)
    x = np.arange(window, dtype=float)
    x_demean = x - x.mean()
    denom = (x_demean ** 2).sum()

    values = ratio.values.astype(float)
    slopes = np.full(len(values), np.nan)
    for i in range(window, len(values)):
        y = values[i - window : i]
        if np.any(np.isnan(y)):
            continue
        y_demean = y - y.mean()
        slopes[i] = (x_demean * y_demean).sum() / denom

    return pd.Series(slopes, index=ratio.index).dropna()


def op_vix_term_structure(inputs: dict[str, pd.Series], params: dict) -> pd.Series:
    """VIX / VIX3M from mixed resolved + raw inputs."""
    # inputs["vix_spot"] is from resolved, inputs["vix3m"] is from raw
    vix = inputs.get("vix_spot", pd.Series(dtype=float))
    vix3m = inputs.get("vix3m", pd.Series(dtype=float))
    if vix.empty or vix3m.empty:
        return pd.Series(dtype=float)
    df = pd.DataFrame({"vix": vix, "vix3m": vix3m}).dropna()
    result = df["vix"] / df["vix3m"]
    return result.replace([np.inf, -np.inf], np.nan).dropna()


# Operation dispatch table
OPS = {
    "alias": op_alias,
    "spread": op_spread,
    "ratio": op_ratio,
    "negate": op_negate,
    "pct_change": op_pct_change,
    "diff": op_diff,
    "slope": op_slope,
    "rolling_max_pct": op_rolling_max_pct,
    "momentum_12_1": op_momentum_12_1,
    "adline_proxy": op_adline_proxy,
    "adline_slope": op_adline_slope,
    "pct_above_ma": op_pct_above_ma,
    "ratio_slope": op_ratio_slope,
    "vix_term_structure": op_vix_term_structure,
}


# ── Bulk insert into resolved_series ─────────────────────────────────────────

BATCH_SIZE = 5000


def insert_computed(engine, feature_id: int, series: pd.Series, dry_run: bool = False) -> int:
    """Insert a computed pd.Series into resolved_series. Returns rows inserted."""
    if series.empty:
        return 0

    rows = []
    for dt, val in series.items():
        obs = dt.date() if hasattr(dt, "date") else dt
        if pd.isna(val) or np.isinf(val):
            continue
        rows.append({
            "feature_id": feature_id,
            "obs_date": obs,
            "release_date": obs,
            "vintage_date": obs,
            "value": float(val),
            "source_priority_used": COMPUTED_SOURCE_ID,
            "conflict_flag": False,
            "resolution_version": 1,
        })

    if not rows:
        return 0

    if dry_run:
        log.info("  [DRY-RUN] Would insert {n} rows for feature_id={fid}", n=len(rows), fid=feature_id)
        return len(rows)

    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO resolved_series
                        (feature_id, obs_date, release_date, vintage_date, value,
                         source_priority_used, conflict_flag, resolution_version)
                    VALUES
                        (:feature_id, :obs_date, :release_date, :vintage_date, :value,
                         :source_priority_used, :conflict_flag, :resolution_version)
                    ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING
                """),
                batch,
            )
        inserted += len(batch)
    return inserted


# ── Main pipeline ────────────────────────────────────────────────────────────

def run(family_filter: str | None = None, dry_run: bool = False):
    engine = get_engine()
    t0 = time.time()

    total_inserted = 0
    total_features = 0
    total_skipped = 0

    for comp in COMPUTATIONS:
        name = comp["name"]
        fam = comp["family"]

        if family_filter and fam != family_filter:
            continue

        log.info("── Computing {name} ({fam}) ──", name=name, fam=fam)

        # Resolve target feature_id
        fid = get_feature_id(engine, name)
        if fid is None:
            log.warning("  Feature '{name}' not in feature_registry — skipping", name=name)
            total_skipped += 1
            continue

        # Load inputs
        inputs: dict[str, pd.Series] = {}

        # Standard resolved inputs
        for inp_name in comp.get("inputs", []):
            s = load_resolved(engine, inp_name)
            if s.empty:
                log.warning("  Input '{inp}' has no resolved data — skipping", inp=inp_name)
                break
            inputs[inp_name] = s
            log.info("  Loaded {inp}: {n} rows [{mn} → {mx}]",
                      inp=inp_name, n=len(s),
                      mn=s.index.min().date(), mx=s.index.max().date())
        else:
            # Load raw inputs if specified
            for sid, label in comp.get("raw_inputs", []):
                s = load_raw(engine, sid)
                if s.empty:
                    log.warning("  Raw input '{sid}' has no data — skipping", sid=sid)
                    break
                inputs[label] = s
                log.info("  Loaded raw {sid} as '{label}': {n} rows", sid=sid, label=label, n=len(s))

            # Load additional resolved inputs (for mixed ops like vix_term_structure)
            for inp_name in comp.get("resolved_inputs", []):
                s = load_resolved(engine, inp_name)
                if s.empty:
                    log.warning("  Resolved input '{inp}' has no data — skipping", inp=inp_name)
                    break
                inputs[inp_name] = s
                log.info("  Loaded resolved {inp}: {n} rows", inp=inp_name, n=len(s))

        if not inputs:
            log.warning("  No inputs available for {name} — skipping", name=name)
            total_skipped += 1
            continue

        # Get the operation
        op_name = comp["op"]
        op_fn = OPS.get(op_name)
        if op_fn is None:
            log.error("  Unknown operation '{op}' — skipping", op=op_name)
            total_skipped += 1
            continue

        # Compute
        try:
            result = op_fn(inputs, comp.get("params", {}))
        except Exception as e:
            log.error("  Computation failed for {name}: {e}", name=name, e=e)
            total_skipped += 1
            continue

        if result.empty:
            log.warning("  Computation produced empty result for {name}", name=name)
            total_skipped += 1
            continue

        log.info("  Computed {n} values [{mn} → {mx}]",
                 n=len(result), mn=result.index.min().date(), mx=result.index.max().date())

        # Insert
        n_ins = insert_computed(engine, fid, result, dry_run=dry_run)
        log.info("  Inserted {n} rows for {name} (feature_id={fid})", n=n_ins, name=name, fid=fid)
        total_inserted += n_ins
        total_features += 1

    elapsed = time.time() - t0
    log.info(
        "═══ Done: {feat} features computed, {ins} total rows inserted, "
        "{skip} skipped in {t:.1f}s ═══",
        feat=total_features, ins=total_inserted, skip=total_skipped, t=elapsed,
    )

    # Show remaining gaps
    if not dry_run:
        with engine.connect() as conn:
            gaps = conn.execute(text("""
                SELECT fr.name, fr.family
                FROM feature_registry fr
                LEFT JOIN resolved_series rs ON fr.id = rs.feature_id
                WHERE fr.deprecated_at IS NULL
                GROUP BY fr.name, fr.family
                HAVING COUNT(rs.id) = 0
                ORDER BY fr.family, fr.name
            """)).fetchall()
        log.info("Remaining features with zero data: {n}", n=len(gaps))


def main():
    parser = argparse.ArgumentParser(description="Compute derived features from existing data")
    parser.add_argument("--family", type=str, default=None, help="Only compute features in this family")
    parser.add_argument("--dry-run", action="store_true", help="Preview what would be inserted")
    args = parser.parse_args()
    run(family_filter=args.family, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
