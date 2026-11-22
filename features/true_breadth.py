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

Bad input. A price that is present but not usable -- ``inf``, ``-inf``, zero
or negative -- is treated as unobserved (so it lowers coverage) and counted in
``invalid_prices``; a zero previous close can therefore never produce an
infinite return. Every emitted statistic is finite or UNKNOWN (NaN with status
``unknown``/``partial`` and a reason); a non-finite value is never returned.
Invalid arguments raise ``ValueError`` at the boundary: a non-numeric price
panel, session labels that are not dates (or ``NaT``), duplicate sessions,
``as_of`` that is not a ``datetime.date``, ``min_coverage`` outside ``(0, 1]``,
and a ``members_on`` that returns something other than a collection of
tickers or :class:`UnknownMembership`.

No writer: nothing stores these series in this change.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import date, datetime
from numbers import Real
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
    "members", "invalid_prices",
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


def _session_frame(prices: pd.DataFrame, what: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(clean float frame, invalid-price mask) on a sorted, de-duplicated date index.

    Raises ``ValueError`` for non-numeric values, non-date or NaT labels and
    duplicate sessions. Present-but-unusable prices (non-finite or ``<= 0``)
    become NaN in the clean frame and True in the mask.
    """
    try:
        raw = prices.astype(float)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{what} must be numeric: {exc}") from exc
    try:
        with warnings.catch_warnings():  # garbage labels fall back to dateutil; the ValueError below is the answer
            warnings.simplefilter("ignore", UserWarning)
            idx = pd.DatetimeIndex(pd.to_datetime(raw.index))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{what} index must be session dates: {exc}") from exc
    if idx.hasnans:
        raise ValueError(f"{what} index has NaT session labels")
    idx = idx.normalize()
    if idx.has_duplicates:
        raise ValueError(f"{what} has duplicate session dates; pass one known-at row per session")
    raw.index = idx
    raw = raw.sort_index()
    values = raw.to_numpy(dtype=float)
    invalid = ~np.isnan(values) & (~np.isfinite(values) | (values <= 0.0))
    invalid_mask = pd.DataFrame(invalid, index=raw.index, columns=raw.columns)
    clean = raw.mask(invalid_mask)
    return clean, invalid_mask


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
    if as_of is not None and (not isinstance(as_of, date) or isinstance(as_of, datetime)):
        raise ValueError(f"as_of must be a datetime.date, got {as_of!r}")
    if (not isinstance(min_coverage, Real) or isinstance(min_coverage, bool)
            or not np.isfinite(float(min_coverage)) or not 0.0 < float(min_coverage) <= 1.0):
        raise ValueError(f"min_coverage must be in (0, 1], got {min_coverage!r}")
    if prices is None or prices.empty:
        return pd.DataFrame(columns=BREADTH_COLUMNS)

    px, invalid = _session_frame(prices, "prices")
    if as_of is not None:
        keep = px.index <= pd.Timestamp(as_of)
        px, invalid = px.loc[keep], invalid.loc[keep]
        if px.empty:
            return pd.DataFrame(columns=BREADTH_COLUMNS)

    ma50 = px.rolling(50, min_periods=50).mean()
    ma200 = px.rolling(200, min_periods=200).mean()
    ret1 = px.pct_change(1, fill_method=None)
    valid = {
        "50ma": px.notna() & ma50.notna(),
        "200ma": px.notna() & ma200.notna(),
        "ret1": ret1.notna() & np.isfinite(ret1),
    }
    up = {"50ma": px > ma50, "200ma": px > ma200, "ret1": ret1 > 0}
    down_ret1 = ret1 < 0
    columns = set(px.columns)

    rows: list[dict[str, object]] = []
    out_dates: list[date] = []
    for idx_val in px.index:
        d = idx_val.date()
        out_dates.append(d)
        row: dict[str, object] = {c: np.nan for c in BREADTH_COLUMNS}
        members = members_on(d)
        if isinstance(members, UnknownMembership):
            row.update(members=0, invalid_prices=0, status=STATUS_UNKNOWN, reason=members.reason)
            rows.append(row)
            continue
        if isinstance(members, (str, bytes)) or not isinstance(members, Collection):
            raise ValueError(f"members_on({d}) must return a collection of tickers or UnknownMembership, "
                             f"got {type(members).__name__}")
        n_members = len(members)
        if n_members == 0:
            row.update(members=0, invalid_prices=0, status=STATUS_UNKNOWN, reason="no members on this date")
            rows.append(row)
            continue
        cols = [c for c in members if c in columns]
        row["members"] = n_members
        row["invalid_prices"] = int(invalid.loc[idx_val, cols].sum()) if cols else 0
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
                value = (n_up - n_down) / n_obs
            else:
                value = n_up / n_obs
            if not np.isfinite(value):  # counts over a positive n_obs; kept as a hard guard
                reasons.append(f"{stat} not finite")
                continue
            row[stat] = value
            known += 1
        if row["invalid_prices"]:
            reasons.append(f"{row['invalid_prices']} member price(s) non-finite or <= 0, treated as unobserved")
        row["status"] = STATUS_OK if known == len(_STATS) else (STATUS_PARTIAL if known else STATUS_UNKNOWN)
        row["reason"] = "; ".join(reasons) or None
        rows.append(row)

    df = pd.DataFrame(rows, index=out_dates, columns=BREADTH_COLUMNS)
    osc = _mcclellan(df["net_adv_ratio"].astype(float))
    df["mcclellan_osc"] = osc.where(np.isfinite(osc))
    return df


def compute_sector_dispersion_iqr(
    sector_prices: pd.DataFrame,
    *,
    horizon_days: int = 5,
    min_sectors: int = MIN_SECTORS_FOR_IQR,
) -> pd.Series:
    """Cross-sectional IQR of trailing ``horizon_days`` returns across the sector ETFs given.

    Computed over the columns present (XLK..XLU); a date with fewer than
    ``min_sectors`` finite returns is left out, never emitted as a value.
    Non-finite or ``<= 0`` prices are unobserved; invalid arguments raise
    ``ValueError``. Same known-at contract as :func:`compute_true_breadth_panel`.
    """
    if not isinstance(horizon_days, int) or isinstance(horizon_days, bool) or horizon_days < 1:
        raise ValueError(f"horizon_days must be an integer >= 1, got {horizon_days!r}")
    if not isinstance(min_sectors, int) or isinstance(min_sectors, bool) or min_sectors < 2:
        raise ValueError(f"min_sectors must be an integer >= 2, got {min_sectors!r}")
    if sector_prices is None or sector_prices.empty:
        return pd.Series(dtype=float, name="sector_iqr_5d")

    clean, _invalid = _session_frame(sector_prices, "sector_prices")
    rets = clean.pct_change(horizon_days, fill_method=None)
    out: dict[date, float] = {}
    for idx_val, row in rets.iterrows():
        vals = row.dropna().to_numpy(dtype=float)
        vals = vals[np.isfinite(vals)]
        if len(vals) < min_sectors:
            continue
        q75, q25 = np.percentile(vals, [75.0, 25.0])
        iqr = float(q75 - q25)
        if np.isfinite(iqr):
            out[idx_val.date()] = iqr
    return pd.Series(out, dtype=float, name="sector_iqr_5d").sort_index()
