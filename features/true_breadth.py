"""Point-in-time S&P 500 breadth and cross-sector dispersion (pure computation).

The legacy ``sp500_pct_above_200ma`` and ``sp500_adline`` features
(``scripts/compute_derived_features.py``) are computed from the index level
alone. This module computes real cross-sectional breadth over constituent
prices, counting on each session ``d`` only the tickers that were index
members on ``d``:

  - ``pct_above_50ma``   fraction of members with close > 50-session SMA
  - ``pct_above_200ma``  fraction of members with close > 200-session SMA
  - ``net_adv_ratio``    (advancers - decliners) / members with a 1-day return
  - ``mcclellan_osc``    19-row EMA minus 39-row EMA of 1000 * net_adv_ratio
  - ``sector_iqr_5d``    interquartile range of 5-session sector-ETF returns

Point in time:

* Membership is required, never defaulted: ``members_on(d)`` must return the
  members *as of* ``d``. :func:`sp500_members_on` builds it from the dated
  membership file ``config/sp500_membership.json`` (base set + dated changes,
  via ``ingestion.altdata.finra_dix.Membership``). Dates after the file's
  ``covers_to`` get the last known set: stale, not future. Passing today's
  list for history would re-introduce survivorship bias, which is why there is
  no "all columns" fallback.
* Every statistic is trailing: rolling means, one-day returns and EMAs use
  rows at or before ``d`` only.
* Prices should be split/dividend-adjusted closes (``adj_close``, the
  ``{ticker}_full`` basis). Above/below-MA and return signs are ratios within
  one series, so a later back-adjustment rescales a whole window together and
  cannot change them; raw closes would turn a split inside the window into a
  false cross.

No writer: nothing stores these series in this change.
"""

from __future__ import annotations

from collections.abc import Callable, Collection
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

MIN_CONSTITUENTS_FOR_BREADTH = 50
MIN_SECTORS_FOR_IQR = 6

_BREADTH_COLUMNS = ["pct_above_50ma", "pct_above_200ma", "net_adv_ratio", "mcclellan_osc"]


def sp500_members_on(path: Path | None = None) -> Callable[[date], frozenset[str]]:
    """``members_on`` callable over the dated S&P 500 membership file.

    Tickers are as the membership file spells them (``BRK.B``); the price
    frame's columns must use the same spelling.
    """
    from ingestion.altdata.finra_dix import MEMBERSHIP_PATH, Membership

    membership = Membership.load(path or MEMBERSHIP_PATH)
    return membership.on


def compute_true_breadth_panel(
    prices: pd.DataFrame,
    *,
    members_on: Callable[[date], Collection[str]],
    min_constituents: int = MIN_CONSTITUENTS_FOR_BREADTH,
) -> pd.DataFrame:
    """Breadth metrics from a daily close frame (index = dates, columns = tickers).

    Row ``d`` counts only the columns in ``members_on(d)``; a date with fewer
    than ``min_constituents`` members in the frame is left out.
    """
    if prices is None or prices.empty:
        return pd.DataFrame(columns=_BREADTH_COLUMNS)

    px = prices.sort_index().astype(float)
    ma50 = px.rolling(50, min_periods=50).mean()
    ma200 = px.rolling(200, min_periods=200).mean()
    ret1 = px.pct_change(1, fill_method=None)

    rows: list[dict[str, float]] = []
    out_dates: list[date] = []
    all_cols = list(px.columns)

    for idx_val in px.index:
        d = idx_val.date() if hasattr(idx_val, "date") else idx_val
        allowed = members_on(d)
        cols = [c for c in all_cols if c in allowed]
        if len(cols) < min_constituents:
            continue

        cur = px.loc[idx_val, cols]
        m50 = ma50.loc[idx_val, cols]
        m200 = ma200.loc[idx_val, cols]
        r1 = ret1.loc[idx_val, cols]

        valid50 = cur.notna() & m50.notna()
        valid200 = cur.notna() & m200.notna()
        valid_r1 = r1.notna()

        pct50 = (
            float((cur[valid50] > m50[valid50]).mean())
            if int(valid50.sum()) >= min_constituents
            else float("nan")
        )
        pct200 = (
            float((cur[valid200] > m200[valid200]).mean())
            if int(valid200.sum()) >= min_constituents
            else float("nan")
        )
        if int(valid_r1.sum()) >= min_constituents:
            adv = int((r1[valid_r1] > 0).sum())
            dec = int((r1[valid_r1] < 0).sum())
            net_adv = (adv - dec) / float(valid_r1.sum())
        else:
            net_adv = float("nan")

        out_dates.append(d)
        rows.append({"pct_above_50ma": pct50, "pct_above_200ma": pct200, "net_adv_ratio": net_adv})

    if not rows:
        return pd.DataFrame(columns=_BREADTH_COLUMNS)

    df = pd.DataFrame(rows, index=out_dates)
    scaled = df["net_adv_ratio"] * 1000.0
    ema19 = scaled.ewm(span=19, adjust=False, min_periods=19).mean()
    ema39 = scaled.ewm(span=39, adjust=False, min_periods=39).mean()
    df["mcclellan_osc"] = ema19 - ema39
    return df


def compute_sector_dispersion_iqr(
    sector_prices: pd.DataFrame,
    *,
    horizon_days: int = 5,
    min_sectors: int = MIN_SECTORS_FOR_IQR,
) -> pd.Series:
    """Cross-sectional IQR of trailing ``horizon_days`` returns across sector ETFs (XLK..XLU)."""
    if sector_prices is None or sector_prices.empty:
        return pd.Series(dtype=float, name="sector_iqr_5d")

    rets = sector_prices.sort_index().astype(float).pct_change(horizon_days, fill_method=None)
    out: dict[date, float] = {}
    for idx_val, row in rets.iterrows():
        vals = row.dropna().to_numpy(dtype=float)
        vals = vals[np.isfinite(vals)]
        if len(vals) < min_sectors:
            continue
        q75, q25 = np.percentile(vals, [75.0, 25.0])
        d = idx_val.date() if hasattr(idx_val, "date") else idx_val
        out[d] = float(q75 - q25)
    return pd.Series(out, dtype=float, name="sector_iqr_5d").sort_index()
