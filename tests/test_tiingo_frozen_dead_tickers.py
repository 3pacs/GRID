"""Delisted symbols Tiingo keeps "pricing" and mutual funds in the stock list.

2026-11-10 audit: Tiingo's daily endpoint answers for a delisted symbol with
one row a day at the last price and volume 0 (CHL 36,358 rows since its
2021-01-08 endDate). The 16 sector-map tickers found by
scripts/classify_tiingo_dead_tickers.py are in
analysis/sector_map_ticker_status.yaml with ``last_session``, so the price
list skips them and tiingo_symbol_contract drops their later TIINGO rows.
Mutual funds (MOALX, TPYTX, from insider filings) post NAV late and always
looked lapsed to the pull's false-green check; they leave the routine list.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)

import tiingo_symbol_contract as tsc
from analysis import sector_map as sm
from ingestion import tiingo_pull as tp
from store import observations as obs

STOPPED = {"CHL": date(2021, 1, 8), "CEO": date(2021, 3, 8), "TTM": date(2023, 1, 23),
           "LSXMA": date(2024, 9, 9), "GOEV": date(2025, 1, 28), "NOVA": date(2025, 6, 9),
           "JBSAY": date(2025, 6, 10), "TPIC": date(2025, 8, 18), "FL": date(2025, 9, 5),
           "GHLD": date(2025, 11, 26), "HBI": date(2025, 12, 1), "ZK": date(2025, 12, 19),
           "SDIG": date(2025, 3, 18), "SNBR": date(2026, 6, 22), "LXP": date(2026, 10, 30),
           "RAMP": date(2026, 10, 30)}


@pytest.mark.parametrize(("ticker", "last"), sorted(STOPPED.items()))
def test_stopped_tickers_price_nothing_and_cut_their_tiingo_rows(ticker, last):
    tsc._last_sessions.cache_clear()
    assert sm.TICKER_STATUS[ticker]["status"] == "delisted"
    assert sm.price_ticker(ticker) is None
    assert tsc.tiingo_cutoff(f"YF:{ticker}:close") == last
    assert tsc.is_foreign_tiingo_row(f"YF:{ticker}:adj_close", "TIINGO", date(2026, 11, 6))
    assert not tsc.is_foreign_tiingo_row(f"YF:{ticker}:close", "TIINGO", last)


def test_the_frozen_rows_never_reach_a_reader():
    tsc._last_sessions.cache_clear()
    engine = create_engine("sqlite://")
    md = MetaData()
    Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String))
    raw = Table("raw_series", md, Column("series_id", String), Column("source_id", Integer),
                Column("obs_date", Date), Column("pull_timestamp", DateTime), Column("value", Float),
                Column("raw_payload", Text), Column("pull_status", String))
    md.create_all(engine)
    ts = datetime(2026, 11, 7, tzinfo=timezone.utc).replace(tzinfo=None)
    with engine.begin() as c:
        c.execute(md.tables["source_catalog"].insert(), [{"id": 524, "name": "TIINGO"}])
        c.execute(raw.insert(), [
            {"series_id": "YF:CHL:close", "source_id": 524, "obs_date": d, "value": v, "raw_payload": "{}",
             "pull_timestamp": ts, "pull_status": "SUCCESS"}
            for d, v in ((date(2021, 1, 8), 26.0), (date(2026, 11, 5), 26.0), (date(2026, 11, 6), 26.0))])
    with engine.connect() as c:
        assert [o.obs_date for o in obs.read_window(c, "YF:CHL:close", source="TIINGO")] == [date(2021, 1, 8)]
        assert obs.read_latest(c, "YF:CHL:close", source="TIINGO").obs_date == date(2021, 1, 8)


@pytest.mark.parametrize(("sym", "fund"), [
    ("MOALX", True), ("TPYTX", True), ("ROKU", False), ("SPY", False), ("BRK-B", False),
    ("ABCDE", False), ("moalx", False), ("ABCX", False),
])
def test_is_mutual_fund_symbol(sym, fund):
    assert tp.is_mutual_fund_symbol(sym) is fund


def test_default_tickers_skip_funds_and_stopped_names(monkeypatch):
    monkeypatch.setattr(tp.TiingoPuller, "oracle_tickers", staticmethod(lambda e: ["ROKU"]))
    monkeypatch.setattr(tp.TiingoPuller, "hypothesis_tickers", staticmethod(lambda e: []))
    monkeypatch.setattr(tp.TiingoPuller, "insider_buyer_tickers", staticmethod(lambda e: ["MOALX", "TPYTX", "LXP"]))
    tickers = set(tp.TiingoPuller.default_tickers(object()))
    assert "ROKU" in tickers
    assert not tickers & {"MOALX", "TPYTX"}
    assert not tickers & set(STOPPED)   # whichever list brought them in (LXP came via insiders)
