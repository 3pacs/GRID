"""A synthetic market and a provider stub for the ETF flow partial-bar repair tests.

The stub serves a market "as of" a session the way yfinance does: bars up to
that session, the closes before each ex-dividend date already adjusted by it,
and, when asked, that session still trading (part of its volume, an intraday
price). ``old_writer_values`` is the pre-#1496 writer's arithmetic on such a
view: the defect the repair corrects.
"""
from __future__ import annotations

import json
import sys
import types
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text

from ingestion.altdata import institutional_flows as inst

NY = inst._NEW_YORK
SOURCE_ID = 7                               # seeded by thirteenf_holdings_support.seed_base


class Market:
    def __init__(self, end: date = date(2026, 6, 30), periods: int = 130,
                 dividends: dict[date, float] | None = None, seed: int = 7) -> None:
        self.days = pd.bdate_range(end=pd.Timestamp(end), periods=periods)
        rng = np.random.default_rng(seed)
        self.raw = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.01, periods))), index=self.days)
        self.vol = pd.Series(rng.integers(5_000_000, 20_000_000, periods).astype(float), index=self.days)
        self.dividends = dividends if dividends is not None else {}
        self.splits: dict[date, float] = {}

    @property
    def sessions(self) -> list[date]:
        return [d.date() for d in self.days]

    def next_session(self, d: date) -> date:
        return next(s for s in self.sessions if s > d)

    def view(self, asof: date, partial: tuple[float, float] | None = None,
             seen_on: date | None = None) -> pd.DataFrame:
        """Bars to ``asof`` as the provider served them on New York date ``seen_on``.

        A dividend is in the adjusted history from the day after its ex-date
        (as yfinance served it on production), so a request on the ex-date
        itself, during or after the session, gets the closes before it
        unadjusted by it. ``seen_on`` defaults to the day after ``asof``.
        """
        seen_on = seen_on or asof + timedelta(days=1)
        raw = self.raw[self.raw.index <= pd.Timestamp(asof)].copy()
        vol = self.vol[self.vol.index <= pd.Timestamp(asof)].copy()
        if partial is not None:
            frac, price = partial
            vol.iloc[-1] = float(int(vol.iloc[-1] * frac))
            raw.iloc[-1] = price
        factor = pd.Series(1.0, index=raw.index)
        divs = pd.Series(0.0, index=raw.index)
        for ex, amount in self.dividends.items():
            ex_ts = pd.Timestamp(ex)
            if ex_ts > pd.Timestamp(asof) or ex_ts >= pd.Timestamp(seen_on) or ex_ts not in raw.index:
                continue
            prev = raw[raw.index < ex_ts].iloc[-1]
            factor[factor.index < ex_ts] *= 1 - amount / prev
            divs[ex_ts] = amount
        splits = pd.Series([self.splits.get(d.date(), 0.0) for d in raw.index], index=raw.index)
        return pd.DataFrame({"raw": raw, "adj": raw * factor, "vol": vol, "div": divs, "split": splits})

    def download(self, asof: date, partial=None, *, start=None, end=None, auto_adjust=True, **_kw) -> pd.DataFrame:
        v = self.view(asof, partial)
        if start is not None:
            v = v[v.index >= pd.Timestamp(start)]
        if end is not None:
            v = v[v.index < pd.Timestamp(end)]
        if auto_adjust:
            return pd.DataFrame({"Close": v.adj, "Volume": v.vol}, index=v.index)
        return pd.DataFrame({"Close": v.raw, "Adj Close": v.adj, "Volume": v.vol,
                             "Dividends": v["div"], "Stock Splits": v.split}, index=v.index)


class FakeYF:
    """``yfinance`` as seen at ``asof`` (default: the market's last session)."""

    def __init__(self, market: Market, asof: date | None = None, partial=None, fail: set[str] | None = None):
        self.market, self.asof, self.partial = market, asof or market.sessions[-1], partial
        self.fail = fail or set()
        self.asked: list[dict] = []

    def download(self, ticker: str, **kw: Any) -> pd.DataFrame:
        self.asked.append({"ticker": ticker, **kw})
        if ticker in self.fail:
            return pd.DataFrame()
        return self.market.download(self.asof, self.partial, **kw)


def install_yfinance(monkeypatch, fake: FakeYF) -> None:
    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(download=fake.download))


def old_writer_values(market: Market, d: date, partial: tuple[float, float] | None,
                      seen_on: date | None = None) -> dict[str, Any]:
    """What the pre-#1496 writer stored for ``d`` from the provider's view of ``d`` on ``seen_on``."""
    v = market.view(d, partial, seen_on)
    dv = v.adj * v.vol
    f5 = dv.rolling(5).sum()
    f20 = dv.rolling(20).sum()
    acc = f5.diff(5)
    ts = pd.Timestamp(d)
    return {"5d": float(f5[ts]), "20d": float(f20[ts]), "accel": float(acc[ts]),
            "close": float(v.adj[ts]), "volume": int(v.vol[ts])}


def ny(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime.combine(d, time(hh, mm), NY).astimezone(timezone.utc)


def seed_row(engine, ticker: str, d: date, values: dict[str, Any], pulled: datetime,
             extra: dict | None = None) -> None:
    """Three stored rows for ``d`` written at ``pulled``, as the writer stores them."""
    payload = {"ticker": ticker, "close": values["close"], "volume": values["volume"], **(extra or {})}
    with engine.begin() as conn:
        for metric in ("5d", "20d", "accel"):
            conn.execute(text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status) "
                "VALUES (:s, :src, :d, :p, :v, :pl, 'SUCCESS')"),
                {"s": f"ETF_FLOW:{ticker}:{metric}", "src": SOURCE_ID, "d": d,
                 "p": pulled if engine.dialect.name == "postgresql" else pulled.strftime("%Y-%m-%d %H:%M:%S.%f"),
                 "v": values[metric], "pl": json.dumps(payload)})


def vintages(engine, series_id: str, d: date) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, value, pull_timestamp, raw_payload FROM raw_series "
            "WHERE series_id = :s AND obs_date = :d ORDER BY id"), {"s": series_id, "d": d}).fetchall()
    out = []
    for r in rows:
        p = r[3] if isinstance(r[3], dict) else json.loads(r[3])
        out.append({"id": r[0], "value": float(r[1]), "pulled": r[2], "payload": p})
    return out


def row_count(engine) -> int:
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM raw_series")).scalar()
