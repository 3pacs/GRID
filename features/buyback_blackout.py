"""Corporate buyback blackout calendar: share of index market cap in blackout (pure).

US issuers usually stop discretionary repurchases ahead of quarterly results
and resume shortly after. This module models that window as
``[earnings_date - pre_days, earnings_date + post_days]`` (defaults 35 and 2
calendar days, a modelling convention, not a disclosed rule) and reports the
market-cap share of the index inside it, with its coverage.

Existing coverage: ``analysis/flow_thesis_data.py::_get_corporate_buyback_cycle_state``
and ``analysis/money_flow_engine/layer_corporate.py`` estimate the same thing
from the calendar month alone. This computes it from per-company earnings
dates instead; neither of them is rewired here.

Point in time, exactly. A schedule counts on ``as_of`` only if it was known by
then (``known_at <= as_of``; for ``earnings_calendar`` rows, the date the row
was first pulled). For one (ticker, fiscal_period) the latest-known vintage
replaces earlier ones; two vintages known on the same day that disagree are a
**conflict**, never a pick. The result never depends on input order.

Unknown is not "open". A ticker is *known* on ``as_of`` only when GRID knows
its next report: a non-conflicting known schedule dated on or after
``as_of - post_days`` and at most ``max_lookahead_days`` after ``as_of``
(default 100, one quarter plus slack: further out, a quarterly report GRID has
not seen could fall in between). Every other ticker is UNKNOWN on that date --
no schedule, only past schedules, or a conflict on a relevant period -- and is
reported, never counted as "not in blackout".

Coverage. :class:`BlackoutReading` carries the weight with a known status
against the total. ``share`` is the blackout share of the *covered* weight;
``share_lower`` / ``share_upper`` bound the true share over the whole index
(every unknown name out of, or in, blackout). Below ``min_coverage`` (default
:data:`MIN_WEIGHT_COVERAGE` = 0.90) the reading is UNKNOWN and ``share`` is
None. 0.90 keeps the bound interval at most 10 percentage points wide: its
width is exactly the unknown share of weight.

No writer: nothing stores this series in this change.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

DEFAULT_PRE_EARNINGS_BLACKOUT_DAYS: int = 35
DEFAULT_POST_EARNINGS_BLACKOUT_DAYS: int = 2
DEFAULT_MAX_LOOKAHEAD_DAYS: int = 100
MIN_WEIGHT_COVERAGE: float = 0.90

STATUS_OK = "ok"
STATUS_UNKNOWN = "unknown"

SERIES_COLUMNS: list[str] = [
    "share", "share_lower", "share_upper", "coverage", "status", "reason",
    "unknown_tickers", "conflicted_tickers",
]


@dataclass(frozen=True)
class ScheduledEarnings:
    """One scheduled earnings date as GRID knew it.

    ``known_at``: first date GRID had this schedule (never earlier).
    ``fiscal_period``: e.g. ``"2026Q3"``; lets a later vintage of the same
    period replace an earlier one. ``None`` treats the event on its own (it is
    never replaced, so pass the period whenever it is known).
    """

    ticker: str
    earnings_date: date
    known_at: date
    fiscal_period: str | None = None


@dataclass(frozen=True)
class BlackoutReading:
    """Blackout share on one date, with coverage. ``share is None`` = UNKNOWN."""

    as_of: date
    status: str
    reason: str | None
    share: float | None
    share_lower: float | None
    share_upper: float | None
    coverage: float
    covered_weight: float
    blackout_weight: float
    total_weight: float
    unknown_tickers: tuple[str, ...]
    conflicted_tickers: tuple[str, ...]


def _reports_known_at(as_of: date, earnings: Iterable[ScheduledEarnings]) -> dict[str, list[frozenset[date]]]:
    """Per ticker, the reports known by ``as_of`` as date sets (more than one date = conflict).

    Order-independent: within one (ticker, fiscal_period) only the latest
    ``known_at`` counts, and every date carried by that latest vintage is kept.
    """
    periods: dict[tuple[str, str], tuple[date, set[date]]] = {}
    standalone: dict[str, set[date]] = {}
    for ev in earnings:
        if ev.known_at > as_of:
            continue
        tk = ev.ticker.strip().upper()
        if ev.fiscal_period is None:
            standalone.setdefault(tk, set()).add(ev.earnings_date)
            continue
        key = (tk, ev.fiscal_period)
        cur = periods.get(key)
        if cur is None or ev.known_at > cur[0]:
            periods[key] = (ev.known_at, {ev.earnings_date})
        elif ev.known_at == cur[0]:
            cur[1].add(ev.earnings_date)

    out: dict[str, list[frozenset[date]]] = {}
    for (tk, _period), (_known, dates) in periods.items():
        out.setdefault(tk, []).append(frozenset(dates))
    for tk, dates in standalone.items():
        out.setdefault(tk, []).extend(frozenset({d}) for d in dates)
    return out


def _ticker_status(
    as_of: date,
    reports: list[frozenset[date]],
    *,
    pre_days: int,
    post_days: int,
    max_lookahead_days: int,
) -> str:
    """``"blackout"``, ``"open"``, ``"unknown"`` or ``"conflict"`` for one ticker on ``as_of``."""
    lo = as_of - timedelta(days=post_days)
    hi = as_of + timedelta(days=max_lookahead_days)
    relevant = [dates for dates in reports if any(lo <= d <= hi for d in dates)]
    if any(len(dates) > 1 for dates in relevant):
        return "conflict"
    if not relevant:
        return "unknown"
    in_window = any(
        d - timedelta(days=pre_days) <= as_of <= d + timedelta(days=post_days)
        for dates in relevant for d in dates
    )
    return "blackout" if in_window else "open"


def compute_blackout_reading(
    as_of: date,
    earnings: Iterable[ScheduledEarnings],
    market_cap_weights: Mapping[str, float],
    *,
    pre_days: int = DEFAULT_PRE_EARNINGS_BLACKOUT_DAYS,
    post_days: int = DEFAULT_POST_EARNINGS_BLACKOUT_DAYS,
    max_lookahead_days: int = DEFAULT_MAX_LOOKAHEAD_DAYS,
    min_coverage: float = MIN_WEIGHT_COVERAGE,
) -> BlackoutReading:
    """Blackout share of index market cap on ``as_of``, with coverage.

    ``market_cap_weights`` must be the weights as of ``as_of`` (the index
    universe on that date); non-positive weights are ignored.
    """
    weights: dict[str, float] = {}
    for tk, w in market_cap_weights.items():
        if w is None:
            continue
        fw = float(w)
        if fw > 0.0:
            key = tk.strip().upper()
            weights[key] = weights.get(key, 0.0) + fw
    total = sum(weights.values())
    if total <= 0.0:
        return BlackoutReading(as_of, STATUS_UNKNOWN, "no_positive_weight", None, None, None,
                               0.0, 0.0, 0.0, 0.0, (), ())

    reports = _reports_known_at(as_of, earnings)
    covered = blackout = 0.0
    unknown: list[str] = []
    conflicted: list[str] = []
    for tk in sorted(weights):
        status = _ticker_status(as_of, reports.get(tk, []), pre_days=pre_days,
                                post_days=post_days, max_lookahead_days=max_lookahead_days)
        if status == "conflict":
            conflicted.append(tk)
        elif status == "unknown":
            unknown.append(tk)
        else:
            covered += weights[tk]
            if status == "blackout":
                blackout += weights[tk]

    coverage = covered / total
    lower = blackout / total
    upper = (blackout + (total - covered)) / total
    if covered <= 0.0 or coverage < min_coverage:
        return BlackoutReading(
            as_of, STATUS_UNKNOWN, f"coverage {coverage:.3f} below {min_coverage:.2f}",
            None, lower, upper, coverage, covered, blackout, total, tuple(unknown), tuple(conflicted),
        )
    return BlackoutReading(
        as_of, STATUS_OK, None, min(1.0, max(0.0, blackout / covered)), lower, upper,
        coverage, covered, blackout, total, tuple(unknown), tuple(conflicted),
    )


def compute_blackout_share_on_date(
    as_of: date,
    earnings: Iterable[ScheduledEarnings],
    market_cap_weights: Mapping[str, float],
    *,
    pre_days: int = DEFAULT_PRE_EARNINGS_BLACKOUT_DAYS,
    post_days: int = DEFAULT_POST_EARNINGS_BLACKOUT_DAYS,
    max_lookahead_days: int = DEFAULT_MAX_LOOKAHEAD_DAYS,
    min_coverage: float = MIN_WEIGHT_COVERAGE,
) -> float | None:
    """The covered blackout share on ``as_of``, or None when it is UNKNOWN."""
    return compute_blackout_reading(
        as_of, earnings, market_cap_weights, pre_days=pre_days, post_days=post_days,
        max_lookahead_days=max_lookahead_days, min_coverage=min_coverage,
    ).share


def compute_blackout_series(
    dates: Iterable[date],
    earnings: Iterable[ScheduledEarnings],
    weights_on: Callable[[date], Mapping[str, float]],
    *,
    pre_days: int = DEFAULT_PRE_EARNINGS_BLACKOUT_DAYS,
    post_days: int = DEFAULT_POST_EARNINGS_BLACKOUT_DAYS,
    max_lookahead_days: int = DEFAULT_MAX_LOOKAHEAD_DAYS,
    min_coverage: float = MIN_WEIGHT_COVERAGE,
) -> pd.DataFrame:
    """One row per date (:data:`SERIES_COLUMNS`); ``share`` is NaN when UNKNOWN.

    ``weights_on(d)`` gives the weights as of ``d``.
    """
    ev_list = list(earnings)
    rows: list[dict[str, object]] = []
    index: list[date] = []
    for d in sorted(set(dates)):
        r = compute_blackout_reading(
            d, ev_list, weights_on(d), pre_days=pre_days, post_days=post_days,
            max_lookahead_days=max_lookahead_days, min_coverage=min_coverage,
        )
        index.append(d)
        rows.append({
            "share": float("nan") if r.share is None else r.share,
            "share_lower": float("nan") if r.share_lower is None else r.share_lower,
            "share_upper": float("nan") if r.share_upper is None else r.share_upper,
            "coverage": r.coverage,
            "status": r.status,
            "reason": r.reason,
            "unknown_tickers": len(r.unknown_tickers),
            "conflicted_tickers": len(r.conflicted_tickers),
        })
    return pd.DataFrame(rows, index=index, columns=SERIES_COLUMNS)
