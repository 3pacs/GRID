"""
Build PIT-correct ticker panel data from GRID's resolved_series.

GRID stores close prices as {ticker}_full features and volume as {ticker}_avg_volume.
This module builds the multi-ticker DataFrames that signals operate on.

All price panels are automatically split-adjusted via the universal
split adjuster. This is a top-level data concern — every downstream
consumer (signals, backtest, models, scanners) gets clean data.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Sequence

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from alpha_research.data.split_adjuster import adjust_panel
from store.pit import PITStore

#: Exact feature_registry name for the CBOE VIX close/spot (~11-20 scale).
#: VERIFIED 2026-09-24: id 11, fed by VIXCLS + YF:^VIX:close via
#: normalization/entity_map.py, alive through today. Deliberately NOT an
#: ILIKE '%vix%' match — that pattern also caught `vvix` (VIX-of-VIX,
#: ~80-100 scale, a different instrument) and the dead `vix` feature
#: (id 105, no data since 2026-04-02), see scripts/auto_regime.py's
#: REGIME_FEATURE_SOURCES and tests/test_regime_data_truth.py for the same
#: aliasing decision made elsewhere in this codebase.
VIX_FEATURE_NAME = "vix_spot"


def _feature_name_to_ticker(name: str) -> str:
    """spy_full -> SPY, qqq_avg_volume -> QQQ."""
    return name.replace("_full", "").replace("_avg_volume", "").upper()


def _resolve_feature_id(engine: Engine, name: str) -> int | None:
    """Look up a feature_registry ID by its EXACT name (no ILIKE/fuzzy match —
    a pattern match can silently pull in an unrelated series, see
    VIX_FEATURE_NAME's docstring)."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM feature_registry WHERE name = :name"),
            {"name": name},
        ).fetchone()
    return int(row[0]) if row else None


def _resolve_feature_ids(engine: Engine, names: list[str]) -> dict[int, str]:
    """Batch version of ``_resolve_feature_id``. Returns {feature_id: name}."""
    if not names:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name FROM feature_registry WHERE name = ANY(:names)"),
            {"names": names},
        ).fetchall()
    return {int(r[0]): r[1] for r in rows}


def build_price_panel(
    engine: Engine,
    tickers: Sequence[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    as_of_date: date | None = None,
) -> pd.DataFrame:
    """
    Build a dates x tickers close-price panel from resolved_series.

    PIT constraint: release_date <= as_of_date (default: today). One row per
    (ticker, obs_date) — deterministically the LATEST vintage as of
    ``as_of_date``, resolved via store/pit.py's ``DISTINCT ON ... ORDER BY
    vintage_date DESC`` rather than an unordered ``drop_duplicates(keep=
    "first")`` over arbitrary DB row order (SPY's 2025-11-28 obs_date carries
    both 679.52 and a later 683.39 revision — the old code picked whichever
    row Postgres happened to return first, non-reproducibly).
    Returns DataFrame with DatetimeIndex and uppercase ticker columns.
    """
    if as_of_date is None:
        as_of_date = date.today()
    if end_date is None:
        end_date = as_of_date
    if start_date is None:
        start_date = end_date - timedelta(days=365 * 5)

    if tickers:
        names = [f"{t.lower()}_full" for t in tickers]
        id_to_ticker = _resolve_feature_ids(engine, names)
    else:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM feature_registry WHERE name LIKE '%%_full'")
            ).fetchall()
        id_to_ticker = {int(r[0]): r[1] for r in rows}

    if not id_to_ticker:
        return pd.DataFrame()

    id_to_ticker = {fid: _feature_name_to_ticker(name) for fid, name in id_to_ticker.items()}

    matrix = PITStore(engine).get_feature_matrix(
        list(id_to_ticker),
        start_date,
        end_date,
        as_of_date,
        vintage_policy="LATEST_AS_OF",
    )
    if matrix.empty:
        return pd.DataFrame()

    panel = matrix.rename(columns=id_to_ticker)
    panel.sort_index(inplace=True)

    # Universal split adjustment — every downstream consumer gets clean data
    panel = adjust_panel(panel)

    return panel


def build_volume_panel(
    engine: Engine,
    tickers: Sequence[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    as_of_date: date | None = None,
) -> pd.DataFrame:
    """
    Build a dates x tickers volume panel from resolved_series.

    Uses {ticker}_avg_volume features.
    """
    if as_of_date is None:
        as_of_date = date.today()
    if end_date is None:
        end_date = as_of_date
    if start_date is None:
        start_date = end_date - timedelta(days=365 * 5)

    ticker_filter = ""
    params: dict = {
        "start": start_date,
        "end": end_date,
        "as_of": as_of_date,
    }

    if tickers:
        names = [f"{t.lower()}_avg_volume" for t in tickers]
        ticker_filter = "AND fr.name = ANY(:names)"
        params["names"] = names

    query = text(f"""
        SELECT fr.name, rs.obs_date, rs.value
        FROM resolved_series rs
        JOIN feature_registry fr ON rs.feature_id = fr.id
        WHERE fr.name LIKE '%%_avg_volume'
          AND rs.obs_date BETWEEN :start AND :end
          AND rs.release_date <= :as_of
          {ticker_filter}
        ORDER BY rs.obs_date, fr.name
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["feature_name", "obs_date", "value"])
    df["ticker"] = df["feature_name"].apply(_feature_name_to_ticker)
    df["obs_date"] = pd.to_datetime(df["obs_date"])

    panel = df.pivot(index="obs_date", columns="ticker", values="value")
    panel.sort_index(inplace=True)
    return panel


def build_returns_panel(price_panel: pd.DataFrame) -> pd.DataFrame:
    """Compute daily returns from close prices."""
    return price_panel.pct_change(fill_method=None)


def get_available_tickers(engine: Engine) -> list[str]:
    """Return all tickers that have _full price data."""
    query = text(
        "SELECT name FROM feature_registry WHERE name LIKE '%%_full' ORDER BY name"
    )
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    return [_feature_name_to_ticker(row[0]) for row in rows]


def get_vix_series(
    engine: Engine,
    start_date: date | None = None,
    end_date: date | None = None,
    as_of_date: date | None = None,
) -> pd.Series:
    """
    Get the PIT-correct VIX time series from resolved_series.

    Reads exactly ``VIX_FEATURE_NAME`` ("vix_spot", the CBOE VIX close/spot)
    by its exact feature_registry name — never an ``ILIKE '%vix%'`` match,
    which also matches `vvix` (VIX-of-VIX, a different instrument on a
    ~80-100 scale) and the dead `vix` feature (id 105, no data since
    2026-04-02). One value per obs_date: the latest vintage with
    release_date <= as_of_date (store/pit.py's LATEST_AS_OF policy), so
    repeated calls are deterministic.
    """
    if as_of_date is None:
        as_of_date = date.today()
    if end_date is None:
        end_date = as_of_date
    if start_date is None:
        start_date = end_date - timedelta(days=365 * 5)

    fid = _resolve_feature_id(engine, VIX_FEATURE_NAME)
    if fid is None:
        log.warning("Feature '{n}' not found in feature_registry", n=VIX_FEATURE_NAME)
        return pd.Series(dtype=float, name="VIX")

    pit_df = PITStore(engine).get_pit([fid], as_of_date, vintage_policy="LATEST_AS_OF")
    if pit_df.empty:
        return pd.Series(dtype=float, name="VIX")

    pit_df = pit_df[
        (pit_df["obs_date"] >= start_date) & (pit_df["obs_date"] <= end_date)
    ].sort_values("obs_date")
    if pit_df.empty:
        return pd.Series(dtype=float, name="VIX")

    s = pd.Series(
        pit_df["value"].astype(float).to_numpy(),
        index=pd.to_datetime(pit_df["obs_date"]),
        name="VIX",
    )
    return s.sort_index()
