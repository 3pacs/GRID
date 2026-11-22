"""Corporate buyback blackout calendar: share of index market cap in blackout (pure).

US issuers usually stop discretionary repurchases ahead of quarterly results
and resume shortly after. This module models that window as
``[earnings_date - pre_days, earnings_date + post_days]`` (defaults 35 and 2
calendar days, a modelling convention, not a disclosed rule) and returns the
market-cap share of the index inside it:

  - ``buyback_blackout_pct`` in ``[0.0, 1.0]``

Existing coverage: ``analysis/flow_thesis_data.py::_get_corporate_buyback_cycle_state``
and ``analysis/money_flow_engine/layer_corporate.py`` estimate the same thing
from the calendar month alone. This computes it from per-company earnings
dates instead; neither of them is rewired here.

Point in time. A scheduled earnings date only counts on ``as_of`` if it was
known by then: every :class:`ScheduledEarnings` carries ``known_at`` (for
``earnings_calendar`` rows, the date the row was first pulled) and events with
``known_at > as_of`` are ignored. When the same company and fiscal period has
several schedule vintages (a date that moved), only the latest one known by
``as_of`` counts. Weights come from ``weights_on(as_of)``, so a caller can
pass caps as of each date instead of today's caps applied to history.

No writer: nothing stores this series in this change.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

DEFAULT_PRE_EARNINGS_BLACKOUT_DAYS: int = 35
DEFAULT_POST_EARNINGS_BLACKOUT_DAYS: int = 2


@dataclass(frozen=True)
class ScheduledEarnings:
    """One scheduled earnings date as GRID knew it.

    ``known_at``: first date GRID had this schedule (never earlier).
    ``fiscal_period``: e.g. ``"2026Q3"``; lets a later vintage of the same
    period replace an earlier one. ``None`` treats the event on its own.
    """

    ticker: str
    earnings_date: date
    known_at: date
    fiscal_period: str | None = None


def _events_known_at(as_of: date, earnings: Iterable[ScheduledEarnings]) -> list[ScheduledEarnings]:
    """Events known by ``as_of``; latest-known vintage per (ticker, fiscal_period)."""
    latest: dict[tuple[str, str], ScheduledEarnings] = {}
    standalone: list[ScheduledEarnings] = []
    for ev in earnings:
        if ev.known_at > as_of:
            continue
        if ev.fiscal_period is None:
            standalone.append(ev)
            continue
        key = (ev.ticker.upper(), ev.fiscal_period)
        prev = latest.get(key)
        if prev is None or ev.known_at > prev.known_at:
            latest[key] = ev
    return standalone + list(latest.values())


def compute_blackout_share_on_date(
    as_of: date,
    earnings: Iterable[ScheduledEarnings],
    market_cap_weights: Mapping[str, float],
    *,
    pre_days: int = DEFAULT_PRE_EARNINGS_BLACKOUT_DAYS,
    post_days: int = DEFAULT_POST_EARNINGS_BLACKOUT_DAYS,
) -> float | None:
    """Fraction of index market cap in an earnings buyback blackout on ``as_of``.

    ``market_cap_weights`` must be the weights as of ``as_of``. Returns
    ``None`` when they have no positive total weight.
    """
    clean_weights = {
        tk.upper(): float(w)
        for tk, w in market_cap_weights.items()
        if w is not None and float(w) > 0.0
    }
    total_weight = sum(clean_weights.values())
    if total_weight <= 0.0:
        return None

    in_blackout: set[str] = set()
    for ev in _events_known_at(as_of, earnings):
        tk = ev.ticker.upper()
        if tk not in clean_weights:
            continue
        start_d = ev.earnings_date - timedelta(days=pre_days)
        end_d = ev.earnings_date + timedelta(days=post_days)
        if start_d <= as_of <= end_d:
            in_blackout.add(tk)

    blackout_weight = sum(clean_weights[tk] for tk in in_blackout)
    return min(1.0, max(0.0, blackout_weight / total_weight))


def compute_blackout_series(
    dates: Iterable[date],
    earnings: Iterable[ScheduledEarnings],
    weights_on: Callable[[date], Mapping[str, float]],
    *,
    pre_days: int = DEFAULT_PRE_EARNINGS_BLACKOUT_DAYS,
    post_days: int = DEFAULT_POST_EARNINGS_BLACKOUT_DAYS,
) -> pd.Series:
    """Daily blackout fraction over ``dates``; ``weights_on(d)`` gives the weights as of ``d``."""
    ev_list = list(earnings)
    out: dict[date, float] = {}
    for d in sorted(set(dates)):
        val = compute_blackout_share_on_date(
            d,
            ev_list,
            weights_on(d),
            pre_days=pre_days,
            post_days=post_days,
        )
        if val is not None:
            out[d] = val
    return pd.Series(out, dtype=float, name="buyback_blackout_pct").sort_index()
