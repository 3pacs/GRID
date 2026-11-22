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

Invalid input raises ``ValueError`` at the boundary; it is never coerced. A
weight must be a real, finite number ``>= 0`` (NaN, inf, negative, boolean,
``None`` and non-numeric weights raise, naming the ticker); zero weights are
ignored, and a date whose weights sum to zero is UNKNOWN (``no_positive_weight``).
A weight total that overflows raises. Weight keys are stripped and
upper-cased, and two keys that collide after that (``AAPL`` and `` aapl``)
raise, naming them, instead of being summed. Every date (``as_of``,
``earnings_date``, ``known_at``, series ``dates``) must be a real
``datetime.date`` between :data:`MIN_DATE` and :data:`MAX_DATE`: a
``datetime`` or ``pandas.Timestamp`` (naive or timezone-aware), ``NaT`` or a
string raises, so no instant is ever silently mapped to a session date in some
zone; tickers must be non-empty strings; ``pre_days``, ``post_days`` and ``max_lookahead_days`` must
be non-negative integers and ``min_coverage`` in ``(0, 1]``. Raising (rather
than marking the name UNKNOWN) is deliberate: a weight that is not a number
leaves the index total itself unknown, so no coverage could be stated. With
valid input every returned share, bound and coverage is finite and in
``[0, 1]``.

No writer: nothing stores this series in this change.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from numbers import Real

import pandas as pd

DEFAULT_PRE_EARNINGS_BLACKOUT_DAYS: int = 35
DEFAULT_POST_EARNINGS_BLACKOUT_DAYS: int = 2
DEFAULT_MAX_LOOKAHEAD_DAYS: int = 100
MIN_WEIGHT_COVERAGE: float = 0.90
#: Plausible date range for every input date (keeps window arithmetic in range).
MIN_DATE: date = date(1900, 1, 1)
MAX_DATE: date = date(2200, 12, 31)

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

    def __post_init__(self) -> None:
        if not isinstance(self.ticker, str) or not self.ticker.strip():
            raise ValueError(f"ScheduledEarnings.ticker must be a non-empty string, got {self.ticker!r}")
        _check_date("earnings_date", self.earnings_date)
        _check_date("known_at", self.known_at)
        if self.fiscal_period is not None and (
                not isinstance(self.fiscal_period, str) or not self.fiscal_period.strip()):
            raise ValueError(f"ScheduledEarnings.fiscal_period must be None or a non-empty string, "
                             f"got {self.fiscal_period!r}")


def _check_date(name: str, value: object) -> None:
    """A real ``date`` (not a ``datetime``/``NaT``/string) inside [MIN_DATE, MAX_DATE]."""
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValueError(f"{name} must be a datetime.date, got {value!r}")
    if not MIN_DATE <= value <= MAX_DATE:
        raise ValueError(f"{name} {value} is outside {MIN_DATE}..{MAX_DATE}")


def _check_params(pre_days: int, post_days: int, max_lookahead_days: int, min_coverage: float) -> None:
    for name, v in (("pre_days", pre_days), ("post_days", post_days),
                    ("max_lookahead_days", max_lookahead_days)):
        if not isinstance(v, int) or isinstance(v, bool) or v < 0:
            raise ValueError(f"{name} must be a non-negative integer, got {v!r}")
    if (not isinstance(min_coverage, Real) or isinstance(min_coverage, bool)
            or not math.isfinite(min_coverage) or not 0.0 < min_coverage <= 1.0):
        raise ValueError(f"min_coverage must be in (0, 1], got {min_coverage!r}")


def _clean_weights(market_cap_weights: Mapping[str, float]) -> dict[str, float]:
    """Validated weights by stripped, upper-cased ticker.

    Two keys that normalize to one ticker (``"AAPL"`` and ``" aapl"``) raise
    ``ValueError`` naming them: summing them would count the name twice.
    """
    if not isinstance(market_cap_weights, Mapping):
        raise ValueError("market_cap_weights must be a mapping of ticker -> weight")
    weights: dict[str, float] = {}
    spelled: dict[str, list[str]] = {}
    for tk, w in market_cap_weights.items():
        if not isinstance(tk, str) or not tk.strip():
            raise ValueError(f"weight key must be a non-empty ticker string, got {tk!r}")
        spelled.setdefault(tk.strip().upper(), []).append(tk)
        try:
            fw = float(w) if isinstance(w, Real) and not isinstance(w, bool) else math.nan
        except OverflowError:
            fw = math.inf
        if not math.isfinite(fw) or fw < 0.0:
            raise ValueError(f"weight for {tk!r} must be a finite number >= 0, got {w!r}")
        if fw > 0.0:
            weights[tk.strip().upper()] = fw
    dupes = {n: keys for n, keys in sorted(spelled.items()) if len(keys) > 1}
    if dupes:
        raise ValueError("market_cap_weights has duplicate tickers after strip/upper-case: "
                         + "; ".join(f"{n} <- {', '.join(repr(k) for k in sorted(keys))}"
                                     for n, keys in dupes.items()))
    if not math.isfinite(sum(weights.values())):
        raise ValueError("weights overflow: their total is not finite")
    return weights


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
        if not isinstance(ev, ScheduledEarnings):
            raise ValueError(f"earnings entries must be ScheduledEarnings, got {type(ev).__name__}")
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
    universe on that date); zero weights are ignored and invalid ones raise
    ``ValueError`` (see the module docstring).
    """
    _check_date("as_of", as_of)
    _check_params(pre_days, post_days, max_lookahead_days, min_coverage)
    weights = _clean_weights(market_cap_weights)
    total = sum(weights.values())
    if not total > 0.0:
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

    def _unit(x: float) -> float:  # finite by construction; clamp float rounding into [0, 1]
        return min(1.0, max(0.0, x))

    coverage = _unit(covered / total)
    lower = _unit(blackout / total)
    upper = _unit((blackout + (total - covered)) / total)
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
    day_list = list(dates)
    for d in day_list:
        _check_date("dates entry", d)
    rows: list[dict[str, object]] = []
    index: list[date] = []
    for d in sorted(set(day_list)):
        try:
            r = compute_blackout_reading(
                d, ev_list, weights_on(d), pre_days=pre_days, post_days=post_days,
                max_lookahead_days=max_lookahead_days, min_coverage=min_coverage,
            )
        except ValueError as exc:
            raise ValueError(f"{d.isoformat()}: {exc}") from exc
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
