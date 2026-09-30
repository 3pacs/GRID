"""A small, seeded VS1 (insider-density panel) world for the E1 gates.

Twenty-four synthetic issuers inside the VS1 discovery window (2014-2019), built only from the
public VS1 builders' own input shapes (``Form4Events``, ``Admission``,
``PricePanel``), so the gates run the real VS1 v7 panel path
(``panel_insider_density_v6.build_trial_panels`` -> ``v2`` -> ``v1``
density / masks / labels) without any registry key, file or network.

Every issuer files a holdings-only Form 4 each quarter naming its current
ticker (so it stays admitted: Section 16 activity, >= 2 Form 4s in 730 days,
ticker rule) and makes random open-market purchases, filed two business days
after the trade. ``leak=True`` plants, at every horizon-spaced decision
session, a purchase in each issuer whose *forward* 20-session relative return
is in the top quintile -- traded on the decision session but filed 45
business days later, i.e. public only after the outcome it "predicts" is
realised. A point-in-time feature builder cannot use it at the decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from store.observations import Observation

START = date(2014, 1, 2)
END = date(2019, 12, 31)  # discovery closes stop before the split
POST_SPLIT_END = date(2020, 3, 31)  # closes after the split, which discovery must never label with
AS_OF_TS = datetime(2026, 9, 30, tzinfo=timezone.utc)
N_ISSUERS = 24
LEAK_HORIZON = 20
LEAK_FILING_LAG_BDAYS = 45
HEX = "e1" * 32


@dataclass
class VS1World:
    events: v1.Form4Events
    admission: v2.Admission
    universe: pd.DataFrame
    closes: pd.DataFrame  # session x ticker (benchmark included), through POST_SPLIT_END
    sessions: list[date]


def _tickers() -> list[str]:
    return [f"T{i:02d}" for i in range(N_ISSUERS)]


def _ciks() -> list[int]:
    return list(range(5000, 5000 + N_ISSUERS))


def _closes(seed: int) -> pd.DataFrame:
    days = pd.bdate_range(START, POST_SPLIT_END)
    rng = np.random.default_rng(seed)
    rets = rng.normal(0.0002, 0.015, (len(days), N_ISSUERS))
    frame = pd.DataFrame(100 * np.exp(np.cumsum(rets, axis=0)), index=days, columns=_tickers())
    frame[v6.BENCHMARK] = 100 * np.exp(np.cumsum(rng.normal(0.0002, 0.009, len(days))))
    return frame


def _bday_after(days: pd.DatetimeIndex, i: int, lag: int) -> pd.Timestamp:
    j = min(i + lag, len(days) - 1)
    return days[j]


def build(seed: int = 11, *, leak: bool = False, leak_known_at_trade: bool = False) -> VS1World:
    """The world. ``leak_known_at_trade`` models a *leaky builder*: the planted
    purchases' availability is the trade instant instead of the filing instant
    (used only by the canary self-tests)."""
    rng = np.random.default_rng(seed)
    closes = _closes(seed)
    days = closes.index
    tickers, ciks = _tickers(), _ciks()
    purchases, subs = [], []
    acc = 0

    def add_submission(cik: int, filed: pd.Timestamp, ticker: str) -> str:
        nonlocal acc
        acc += 1
        accession = f"e1-{cik}-{acc:06d}"
        subs.append({"ACCESSION_NUMBER": accession, "FILING_DATE": filed.date().isoformat(),
                     "ISSUER_CIK": str(cik), "DOCUMENT_TYPE": "4", "ISSUER_TICKER": ticker})
        return accession

    for j, (cik, ticker) in enumerate(zip(ciks, tickers)):
        for filed in days[::63]:  # quarterly holdings-only filings keep the issuer admitted
            add_submission(cik, filed, ticker)
        for i in np.flatnonzero(rng.random(len(days)) < 1 / 60):
            if days[i].date() > END:
                continue
            filed = _bday_after(days, i, 2)
            add_submission(cik, filed, ticker)
            shares = float(rng.integers(1, 20)) * 1000.0
            price = float(closes.iat[i, j])
            purchases.append({"issuer_cik": cik, "actor": int(rng.integers(1, 6)), "filing_date": filed,
                              "trans_date": days[i], "shares": shares, "price": price,
                              "value": shares * price, "n_reports": 1, "known_at": None})

    if leak:
        rel = closes[tickers].shift(-LEAK_HORIZON) / closes[tickers] - 1.0
        bench = closes[v6.BENCHMARK].shift(-LEAK_HORIZON) / closes[v6.BENCHMARK] - 1.0
        rel = rel.sub(bench, axis=0)
        for i in range(0, len(days) - LEAK_HORIZON, LEAK_HORIZON):
            if days[i].date() > END:
                break
            row = rel.iloc[i]
            top = row[row >= row.quantile(0.8)].index
            for ticker in top:
                j = tickers.index(ticker)
                filed = _bday_after(days, i, LEAK_FILING_LAG_BDAYS)
                add_submission(ciks[j], filed, ticker)
                purchases.append({"issuer_cik": ciks[j], "actor": 99, "filing_date": filed,
                                  "trans_date": days[i], "shares": 5000.0, "price": float(closes.iat[i, j]),
                                  "value": 5000.0 * float(closes.iat[i, j]), "n_reports": 1,
                                  "known_at": "trade" if leak_known_at_trade else None})

    frame = pd.DataFrame(purchases)
    known = v1.filing_known_at(frame["filing_date"])
    trade_instant = pd.to_datetime(frame["trans_date"]).dt.tz_localize("UTC")
    frame["known_at"] = known.where(frame["known_at"].ne("trade"), trade_instant)
    frame = frame.sort_values(["issuer_cik", "known_at", "actor"], kind="mergesort").reset_index(drop=True)
    submissions = pd.DataFrame(subs)
    activity = pd.DataFrame({
        "issuer_cik": submissions["ISSUER_CIK"].astype("int64"),
        "known_at": v1.filing_known_at(pd.to_datetime(submissions["FILING_DATE"])),
    })
    events = v1.Form4Events(purchases=frame, activity=activity, receipt={"e1": "synthetic", "seed": seed})
    universe = pd.DataFrame({
        "ticker": tickers, "cik": ciks, "source": "sic", "sic": 7372, "sic_group": "7370-7379",
        "current_tickers": [[t] for t in tickers],
    })
    admission = v2.build_admission(submissions, universe)
    return VS1World(events, admission, universe, closes, [d.date() for d in days])


def manifest() -> v6.PriceManifest:
    tickers = _tickers()
    return v6.PriceManifest(
        source=v6.PRICE_SOURCE, series_template=v6.SERIES_TEMPLATE, basis=v6.BASIS,
        benchmark=v6.BENCHMARK, admitted=tuple(sorted(tickers + [v6.BENCHMARK])),
        probe_report_sha256=HEX, listed_from=tuple((t, START.isoformat()) for t in tickers),
        crosscheck_report_sha256=HEX, tiingo_meta_report_sha256=HEX,
    )


def price_panel(closes: pd.DataFrame, *, as_of: date = END) -> v1.PricePanel:
    """A discovery ``PricePanel`` over ``closes`` (no DB, no registry key).

    ``as_of`` bounds what is handed to the panel; pass a later date to hand
    it closes past the split and prove the label purge ignores them."""
    data = {}
    for ticker in closes.columns:
        series = closes[ticker].dropna()
        series = series[series.index <= pd.Timestamp(as_of)]
        data[ticker] = tuple(
            Observation(f"YF:{ticker}:adj_close", ts.date(), float(v), None, "TIINGO")
            for ts, v in series.items()
        )
    return v1.PricePanel(
        token=v1._PRICE_LOADER, manifest=manifest(), start=START, as_of=as_of, as_of_ts=AS_OF_TS,
        window="discovery", data=data,
    )


def trial_panels(world: VS1World, *, closes: pd.DataFrame | None = None,
                 as_of: date = END) -> dict[str, v2.TrialPanel]:
    """The VS1 v7 discovery panels (v6 construction) for this world."""
    prices = price_panel(world.closes if closes is None else closes, as_of=as_of)
    with v1.discovery_window(v7.DISCOVERY_START):
        return v6.build_trial_panels(world.events, world.admission, world.universe, prices, "discovery")
