"""Point-in-time S&P 500 breadth and cross-sector dispersion (pure computation).

The legacy ``sp500_pct_above_200ma`` and ``sp500_adline`` features
(``scripts/compute_derived_features.py``) are computed from the index level
alone. This module computes real cross-sectional breadth over constituent
prices, counting on each session ``d`` only the tickers that were index
members on ``d``:

  - ``pct_above_50ma``   fraction of observed members with close > 50-session SMA
  - ``pct_above_200ma``  fraction of observed members with close > 200-session SMA
  - ``net_adv_ratio``    (advancers - decliners) / observed members with a 1-day return
  - ``mcclellan_osc``    19-row EMA minus 39-row EMA of 1000 * net_adv_ratio
  - ``sector_iqr_5d``    interquartile range of 5-session sector-ETF returns

Coverage, and UNKNOWN. Every row reports ``members`` (index members on ``d``)
and, per statistic, how many of them were observed (a price on ``d`` plus the
history the statistic needs) and the ratio. A statistic whose coverage is
below ``min_coverage`` (default :data:`MIN_MEMBER_COVERAGE` = 0.95) is UNKNOWN
(NaN), so a panel holding 50 names never stands in for a 500-name index. 0.95
bounds the error of a fraction statistic: with ``k`` of ``N`` members
unobserved, the full-index value lies within ``k / N`` (at most 5 percentage
points) of the observed one. ``status`` is ``ok`` (all three base statistics
known), ``partial`` or ``unknown``, and ``reason`` says why. ``mcclellan_osc``
is NaN until 39 consecutive rows have a known ``net_adv_ratio`` and restarts
after any UNKNOWN row.

Membership is required, never defaulted: ``members_on(d)`` returns the members
*as of* ``d``, or :class:`UnknownMembership` with a reason. :func:`sp500_members_on`
builds it from the dated file ``config/sp500_membership.json`` (base set +
dated changes, via ``ingestion.altdata.finra_dix.Membership``) and returns
UNKNOWN before the file's ``base_date`` and after its ``covers_to`` (the last
change date the file records): the last member set is not carried forward.

Exact availability. The price panel is plain (no vintages), so the caller must
pass a known-at-aligned panel: row ``d`` holds only values known by the end of
``d`` (for example closes read with ``store.observations.read_window_known_at``
at ``as_of = d``, or a PIT store read). Every statistic is trailing (rolling
means, one-day returns and EMAs use rows at or before ``d``), so rows after
``d`` cannot change row ``d``; ``as_of`` drops them explicitly. A breadth value
for ``d`` is available after ``d``'s close. Split/dividend-adjusted closes
(``adj_close``, the ``{ticker}_full`` basis) are fine here even though the
adjustment is a later vintage: a later corporate action rescales every price in
a trailing window by the same factor, which cannot change an above/below-MA
comparison or a return's sign. Raw closes would turn a split inside the window
into a false cross.

No writer: nothing stores these series in this change.
"""

from __future__ import annotations

from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

MIN_MEMBER_COVERAGE: float = 0.95
MIN_SECTORS_FOR_IQR = 6
_EMA_FAST, _EMA_SLOW = 19, 39

STATUS_OK = "ok"
STATUS_PARTIAL = "partial"
STATUS_UNKNOWN = "unknown"

_STATS: tuple[tuple[str, str], ...] = (
    ("pct_above_50ma", "50ma"),
    ("pct_above_200ma", "200ma"),
    ("net_adv_ratio", "ret1"),
)

BREADTH_COLUMNS: list[str] = [
    "members",
    "observed_50ma", "observed_200ma", "observed_ret1",
    "coverage_50ma", "coverage_200ma", "coverage_ret1",
    "pct_above_50ma", "pct_above_200ma", "net_adv_ratio", "mcclellan_osc",
    "status", "reason",
]


@dataclass(frozen=True)
class UnknownMembership:
    """``members_on`` result for a date whose membership is not known."""

    reason: str


MembersOn = Callable[[date], Collection[str] | UnknownMembership]


def sp500_members_on(path: Path | None = None) -> Callable[[date], frozenset[str] | UnknownMembership]:
    """``members_on`` over the dated S&P 500 membership file.

    Known on ``[base_date, covers_to]``; UNKNOWN (with the reason) outside it.
    Tickers are as the file spells them (``BRK.B``); the price frame's columns
    must use the same spelling.
    """
    from ingestion.altdata.finra_dix import MEMBERSHIP_PATH, Membership

    membership = Membership.load(path or MEMBERSHIP_PATH)
    base = date.fromisoformat(str(membership.base_date))
    covers_to = date.fromisoformat(str(membership.covers_to))

    def members_on(d: date) -> frozenset[str] | UnknownMembership:
        if d < base:
            return UnknownMembership(f"before membership base_date {base.isoformat()}")
        if d > covers_to:
            return UnknownMembership(f"after membership covers_to {covers_to.isoformat()}")
        return membership.on(d)

    return members_on


def _mcclellan(net_adv: pd.Series) -> pd.Series:
    """19/39 EMA spread of ``1000 * net_adv`` over runs of known values only."""
    out = pd.Series(np.nan, index=net_adv.index, dtype=float)
    known = net_adv.notna()
    run_id = (~known).cumsum()
    for _, run in net_adv[known].groupby(run_id[known]):
        scaled = run * 1000.0
        fast = scaled.ewm(span=_EMA_FAST, adjust=False, min_periods=_EMA_FAST).mean()
        slow = scaled.ewm(span=_EMA_SLOW, adjust=False, min_periods=_EMA_SLOW).mean()
        out.loc[run.index] = (fast - slow).to_numpy()
    return out


def compute_true_breadth_panel(
    prices: pd.DataFrame,
    *,
    members_on: MembersOn,
    as_of: date | None = None,
    min_coverage: float = MIN_MEMBER_COVERAGE,
) -> pd.DataFrame:
    """Breadth with coverage from a known-at-aligned daily close frame.

    ``prices``: index = session dates, columns = tickers. One output row per
    session at or before ``as_of`` (all sessions when ``as_of`` is None);
    columns :data:`BREADTH_COLUMNS`. A statistic is NaN when it is UNKNOWN.
    """
    if prices is None or prices.empty:
        return pd.DataFrame(columns=BREADTH_COLUMNS)

    if prices.index.has_duplicates:
        raise ValueError("prices has duplicate session dates; pass one known-at row per session")
    px = prices.sort_index().astype(float)
    if as_of is not None:
        px = px.loc[[(i.date() if hasattr(i, "date") else i) <= as_of for i in px.index]]
        if px.empty:
            return pd.DataFrame(columns=BREADTH_COLUMNS)

    ma50 = px.rolling(50, min_periods=50).mean()
    ma200 = px.rolling(200, min_periods=200).mean()
    ret1 = px.pct_change(1, fill_method=None)
    valid = {
        "50ma": px.notna() & ma50.notna(),
        "200ma": px.notna() & ma200.notna(),
        "ret1": ret1.notna(),
    }
    up = {"50ma": px > ma50, "200ma": px > ma200, "ret1": ret1 > 0}
    down_ret1 = ret1 < 0
    columns = set(px.columns)

    rows: list[dict[str, object]] = []
    out_dates: list[date] = []
    for idx_val in px.index:
        d = idx_val.date() if hasattr(idx_val, "date") else idx_val
        out_dates.append(d)
        row: dict[str, object] = {c: np.nan for c in BREADTH_COLUMNS}
        members = members_on(d)
        if isinstance(members, UnknownMembership):
            row.update(members=0, status=STATUS_UNKNOWN, reason=members.reason)
            rows.append(row)
            continue
        n_members = len(members)
        if n_members == 0:
            row.update(members=0, status=STATUS_UNKNOWN, reason="no members on this date")
            rows.append(row)
            continue
        cols = [c for c in members if c in columns]
        row["members"] = n_members
        reasons: list[str] = []
        known = 0
        for stat, key in _STATS:
            ok = valid[key].loc[idx_val, cols] if cols else pd.Series(dtype=bool)
            n_obs = int(ok.sum())
            coverage = n_obs / n_members
            row[f"observed_{key}"] = n_obs
            row[f"coverage_{key}"] = coverage
            if coverage < min_coverage or n_obs == 0:
                reasons.append(f"{stat} coverage {coverage:.3f} below {min_coverage:.2f}")
                continue
            ok_cols = ok[ok].index
            n_up = int(up[key].loc[idx_val, ok_cols].sum())
            if key == "ret1":
                n_down = int(down_ret1.loc[idx_val, ok_cols].sum())
                row[stat] = (n_up - n_down) / n_obs
            else:
                row[stat] = n_up / n_obs
            known += 1
        row["status"] = STATUS_OK if known == len(_STATS) else (STATUS_PARTIAL if known else STATUS_UNKNOWN)
        row["reason"] = "; ".join(reasons) or None
        rows.append(row)

    df = pd.DataFrame(rows, index=out_dates, columns=BREADTH_COLUMNS)
    df["mcclellan_osc"] = _mcclellan(df["net_adv_ratio"].astype(float))
    return df


def compute_sector_dispersion_iqr(
    sector_prices: pd.DataFrame,
    *,
    horizon_days: int = 5,
    min_sectors: int = MIN_SECTORS_FOR_IQR,
) -> pd.Series:
    """Cross-sectional IQR of trailing ``horizon_days`` returns across the sector ETFs given.

    Computed over the columns present (XLK..XLU); a date with fewer than
    ``min_sectors`` finite returns is left out. Same known-at contract as
    :func:`compute_true_breadth_panel`.
    """
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
