"""
Build PIT-correct ticker panel data from GRID's resolved_series.

GRID stores close prices as {ticker}_full features and volume as {ticker}_avg_volume.
This module builds the multi-ticker DataFrames that signals operate on.

Price basis (docs/reference/PRICE_PANEL_BASIS.md, 2026-11-08): a ``_full``
feature is fed by exactly one raw price field -- ``YF:<T>:adj_close`` for
equities and ETFs (split- and dividend-adjusted as of each vintage's pull),
the as-traded close only for instruments without corporate actions and for
``spy_full`` (pinned to the receipted unadjusted SPY close by contract).
``build_price_panel`` therefore no longer runs the heuristic split adjuster
(``alpha_research.data.split_adjuster.adjust_panel``), which read every
-40 % day as a split and compounded the raw/adjusted vintage flips of the
old mixed-basis panel into first-to-last ratios of 10^201 (XLP). Instead it
re-bases each cell from its own vintage's share basis to ``as_of_date``'s
with the *recorded* split events (``YF:<T>:split_factor`` rows via
``store.observations.read_splits``), which is exact, idempotent and PIT.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Sequence

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.observations import SplitEvent, cumulative_split_factor, read_splits
from store.pit import PITStore, retraction_cutoff

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


def _ids_for_names(engine: Engine, names: list[str], pit: PITStore | None) -> dict[int, str]:
    """``{feature_id: name}`` for registered *names*, via *pit* when given.

    A ``PITWindowCache`` answers from the name map it recorded at build time,
    so a snapshot-backed backtest needs no feature_registry read.
    """
    if pit is None:
        return _resolve_feature_ids(engine, names)
    return {fid: name for name, fid in pit.feature_ids_by_name(names).items()}


def _capped_start_date(start_date: date, end_date: date) -> date:
    """``PITStore.get_feature_matrix``'s GRID_PIT_MAX_YEARS cap, kept here
    because the panel reads vintages through ``get_pit`` directly."""
    import os

    max_years = int(os.getenv("GRID_PIT_MAX_YEARS", "10"))
    if (end_date - start_date).days / 365.25 > max_years:
        capped = date(end_date.year - max_years, end_date.month, end_date.day)
        log.warning(
            "Price panel date range {sd} to {ed} exceeds GRID_PIT_MAX_YEARS={cap}. "
            "Truncating start_date to {capped}.",
            sd=start_date, ed=end_date, cap=max_years, capped=capped,
        )
        return capped
    return start_date


def _read_recorded_splits(
    engine: Engine, ticker: str, start_date: date, as_of_date: date,
) -> list[SplitEvent]:
    """Recorded ``YF:<ticker>:split_factor`` events known by ``as_of_date``.

    Best effort: a reader without ``raw_series`` (a stub engine, a snapshot
    replay) yields no events, which means "no re-basing", never a guess.
    """
    try:
        with engine.connect() as conn:
            return read_splits(
                conn, ticker, start=start_date, end=as_of_date,
                as_of_ts=retraction_cutoff(as_of_date),
            )
    except Exception as exc:  # noqa: BLE001 -- degrade to "no recorded splits"
        log.warning("Recorded splits unavailable for {t}: {e}", t=ticker, e=str(exc))
        return []


def rebase_vintages_to_as_of(
    values: pd.Series,
    vintages: pd.Series,
    splits: Sequence[SplitEvent],
    as_of_date: date,
) -> pd.Series:
    """Express every cell in ``as_of_date``'s share basis using recorded splits.

    A ``_full`` value written from a pull dated ``v`` (its ``vintage_date``)
    is adjusted for every split effective on or before ``v`` -- Tiingo's
    ``adjClose`` and Yahoo's ``Adj Close`` are re-adjusted at each pull. A
    split effective in ``(v, as_of_date]`` is not in it yet, so the cell is
    divided by the product of those ratios (``cumulative_split_factor``).
    Cells whose vintage already carries every split are unchanged, so the
    transform is idempotent per vintage: no price jump is ever inferred
    from the prices themselves.

    ``values`` and ``vintages`` share an index (obs_date); ``vintages`` holds
    ``date`` objects. Without splits the input is returned as a float copy.
    """
    out = values.astype(float).copy()
    if not splits or out.empty:
        return out
    # psycopg hands back ``date``; a cache or a fake may hand back a
    # datetime/Timestamp (a ``date`` subclass, and datetime64 once inside a
    # Series), so normalise every vintage to a plain date first.
    vdates = pd.Series(
        [v.date() if isinstance(v, datetime) else v for v in vintages.tolist()],
        index=vintages.index, dtype=object,
    )
    factors: dict[date, float] = {
        vd: cumulative_split_factor(splits, vd, as_of_date)
        for vd in set(vdates.dropna().tolist())
    }
    f = vdates.map(factors).astype(float).fillna(1.0)
    return out / f.to_numpy()


def build_price_panel(
    engine: Engine,
    tickers: Sequence[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    as_of_date: date | None = None,
    *,
    pit: PITStore | None = None,
    split_rebase: bool = True,
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

    Basis: each cell is the ``adj_close`` (or, for RAW_CLOSE_FULL_FEATURES,
    the as-traded close) its vintage carried, re-based to ``as_of_date``'s
    share count with the recorded split events when ``split_rebase`` is
    True (see ``rebase_vintages_to_as_of``). No heuristic split detection
    runs here any more -- see the module docstring for why.

    ``pit`` (optional) is the PIT reader to use; default ``PITStore(engine)``.
    A ``store.pit.PITWindowCache`` answers repeated reads over one window
    from memory with identical results (rotation backtests, W2-7).
    """
    if as_of_date is None:
        as_of_date = date.today()
    if end_date is None:
        end_date = as_of_date
    if start_date is None:
        start_date = end_date - timedelta(days=365 * 5)
    start_date = _capped_start_date(start_date, end_date)

    if tickers:
        names = [f"{t.lower()}_full" for t in tickers]
        id_to_ticker = _ids_for_names(engine, names, pit)
    else:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM feature_registry WHERE name LIKE '%%_full'")
            ).fetchall()
        id_to_ticker = {int(r[0]): r[1] for r in rows}

    if not id_to_ticker:
        return pd.DataFrame()

    id_to_ticker = {fid: _feature_name_to_ticker(name) for fid, name in id_to_ticker.items()}

    pit_df = (pit or PITStore(engine)).get_pit(
        list(id_to_ticker),
        as_of_date,
        vintage_policy="LATEST_AS_OF",
        start_date=start_date,
        end_date=end_date,
    )
    if pit_df.empty:
        return pd.DataFrame()
    pit_df = pit_df[
        (pit_df["obs_date"] >= start_date) & (pit_df["obs_date"] <= end_date)
    ]
    if pit_df.empty:
        return pd.DataFrame()

    values = pit_df.pivot_table(
        index="obs_date", columns="feature_id", values="value", aggfunc="first",
    )
    vintages = pit_df.pivot_table(
        index="obs_date", columns="feature_id", values="vintage_date", aggfunc="first",
    )
    values.index = pd.DatetimeIndex(values.index, name="obs_date")
    vintages.index = values.index

    panel = values.rename(columns=id_to_ticker).sort_index()
    vintages = vintages.rename(columns=id_to_ticker).sort_index()

    if split_rebase:
        for ticker in panel.columns:
            splits = _read_recorded_splits(engine, ticker, start_date, as_of_date)
            if not splits:
                continue
            col = panel[ticker]
            mask = col.notna()
            panel.loc[mask, ticker] = rebase_vintages_to_as_of(
                col[mask], vintages.loc[mask, ticker], splits, as_of_date,
            ).to_numpy()
            log.debug("Re-based {t} with {n} recorded split(s)", t=ticker, n=len(splits))

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
        "retraction_cutoff": retraction_cutoff(as_of_date),
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
          AND NOT EXISTS (
              SELECT 1 FROM resolved_series_retractions rr
              WHERE rr.feature_id = rs.feature_id
                AND rr.obs_date = rs.obs_date
                AND rr.vintage_date = rs.vintage_date
                AND rr.retracted_at <= :retraction_cutoff
          )
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
    *,
    pit: PITStore | None = None,
) -> pd.Series:
    """
    Get the PIT-correct VIX time series from resolved_series.

    Reads exactly ``VIX_FEATURE_NAME`` ("vix_spot", the CBOE VIX close/spot)
    by its exact feature_registry name — never an ``ILIKE '%vix%'`` match,
    which also matches `vvix` (VIX-of-VIX, a different instrument on a
    ~80-100 scale) and the dead `vix` feature (id 105, no data since
    2026-04-02). One value per obs_date: the latest vintage with
    release_date <= as_of_date (store/pit.py's LATEST_AS_OF policy), so
    repeated calls are deterministic. The read is bounded to
    ``[start_date, end_date]`` in SQL (it used to read the whole history and
    filter here). ``pit`` as in ``build_price_panel``.
    """
    if as_of_date is None:
        as_of_date = date.today()
    if end_date is None:
        end_date = as_of_date
    if start_date is None:
        start_date = end_date - timedelta(days=365 * 5)

    if pit is None:
        fid = _resolve_feature_id(engine, VIX_FEATURE_NAME)
    else:
        fid = pit.feature_ids_by_name([VIX_FEATURE_NAME]).get(VIX_FEATURE_NAME)
    if fid is None:
        log.warning("Feature '{n}' not found in feature_registry", n=VIX_FEATURE_NAME)
        return pd.Series(dtype=float, name="VIX")

    pit_df = (pit or PITStore(engine)).get_pit(
        [fid],
        as_of_date,
        vintage_policy="LATEST_AS_OF",
        start_date=start_date,
        end_date=end_date,
    )
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
