"""BKH, GWHT, KPTI and ROKU are retired in the lifecycle sidecar, and nothing
that reads the sidecar loses their history.

Tiingo stopped serving the four symbols between 2026-11-06 and 2026-11-13
(its listing, read 2026-11-23: ROKU endDate 2026-11-06, KPTI endDate
2026-11-10, BKH no dates, GWHT not found). Each kept the routine price run
PARTIAL for days. Three live on under another symbol in Tiingo's own listing
(HZN, KPTIQ, GWHTQ); ROKU was acquired.

Owner decision 2026-11-23: "HZN only". BKH is followed to HZN, a live merged
utility with continuous history. KPTIQ and GWHTQ, the OTC shares of two
companies in bankruptcy, are named in the notes and never fetched.

The sidecar has two readers: ``analysis.sector_map.price_ticker`` (what to
fetch today) and ``tiingo_symbol_contract.tiingo_cutoff`` (TIINGO rows after
``last_session`` are not this instrument).
"""

from __future__ import annotations

from datetime import date

import pytest

import tiingo_symbol_contract as contract
from analysis import sector_map as sm
from ingestion import tiingo_pull as tp


@pytest.fixture(autouse=True)
def _fresh_cutoffs():
    contract._last_sessions.cache_clear()
    yield
    contract._last_sessions.cache_clear()


@pytest.mark.parametrize(("ticker", "status", "today"), [
    ("BKH", "renamed", "HZN"),        # the one successor that is followed
    ("GWHT", "delisted", None),
    ("GWH", "delisted", None),        # the map's symbol for ESS Tech; was renamed -> GWHT
    ("KPTI", "delisted", None),
    ("ROKU", "acquired", None),
])
def test_the_four_symbols_are_retired(ticker, status, today):
    assert sm.TICKER_STATUS[ticker]["status"] == status
    assert sm.price_ticker(ticker) == today


@pytest.mark.parametrize(("ticker", "otc_symbol"), [("KPTI", "KPTIQ"), ("GWHT", "GWHTQ"), ("GWH", "GWHTQ")])
def test_the_otc_symbols_are_recorded_but_never_followed(ticker, otc_symbol):
    entry = sm.TICKER_STATUS[ticker]
    assert "successor" not in entry
    assert otc_symbol in entry["note"] and "owner, 2026-11-23" in entry["note"]
    assert otc_symbol not in sm.TICKER_STATUS
    assert otc_symbol not in sm.price_tickers()


def test_roku_names_its_acquirer_and_no_successor():
    entry = sm.TICKER_STATUS["ROKU"]
    assert entry["acquirer"] == "Fox" and "successor" not in entry


@pytest.mark.parametrize(("ticker", "last_session"), [
    ("ROKU", date(2026, 11, 6)),      # Tiingo's listing endDate
    ("KPTI", date(2026, 11, 10)),     # Tiingo's listing endDate
    ("BKH", None),                    # the listing has no dates: none invented
    ("GWHT", None),                   # the listing is not found: none invented
])
def test_last_session_is_tiingos_end_date_or_absent(ticker, last_session):
    assert sm.TICKER_STATUS[ticker].get("last_session") == last_session
    assert contract.tiingo_cutoff(f"YF:{ticker}:close") == last_session


@pytest.mark.parametrize("ticker", ["BKH", "GWHT", "KPTI", "ROKU"])
def test_every_note_says_where_its_facts_come_from(ticker):
    note = sm.TICKER_STATUS[ticker]["note"]
    assert "Tiingo" in note and "read 2026-11-23" in note
    # An issuer filing where one could be read; the news page for BKH, whose
    # issuer note and OCC memo refused this session (said in the note).
    assert ("Form 8-K" in note and "sec.gov/Archives/edgar/data/" in note) or (
        ticker == "BKH" and "WOWT" in note and "HTTP 403" in note)


def test_the_bkh_note_does_not_cite_another_issuers_occ_memo():
    """OCC information memo 60193 is about Karyopharm, not Black Hills (Codex
    review 6032263432)."""
    note = sm.TICKER_STATUS["BKH"]["note"]
    assert "60193" not in note and "OCC" not in note
    assert "HTTP 403" in note and "not read" in note


def test_no_note_rests_on_an_ai_written_summary_page():
    for ticker in ("BKH", "GWH", "GWHT", "KPTI", "ROKU"):
        note = sm.TICKER_STATUS[ticker]["note"]
        assert "Panabee" not in note and "TheWrap" not in note, ticker


@pytest.mark.parametrize("ticker", ["BKH", "GWH", "GWHT", "KPTI", "ROKU"])
def test_vendor_dates_are_not_presented_as_venue_dates(ticker):
    entry = sm.TICKER_STATUS[ticker]
    note = entry["note"]
    if "last_session" in entry:
        assert "vendor cutoff" in note
    else:
        assert "no venue date is inferred" in note


def test_stored_history_stays_readable():
    """Rows up to the last session are kept; only TIINGO rows after it are cut."""
    assert not contract.is_foreign_tiingo_row("YF:ROKU:close", "TIINGO", date(2026, 11, 6))
    assert contract.is_foreign_tiingo_row("YF:ROKU:close", "TIINGO", date(2026, 11, 9))
    assert not contract.is_foreign_tiingo_row("YF:KPTI:close", "TIINGO", date(2026, 11, 10))
    assert contract.is_foreign_tiingo_row("YF:KPTI:close", "TIINGO", date(2026, 11, 11))
    # Another source's rows are never cut (yfinance holds KPTI closes to 11-18).
    assert not contract.is_foreign_tiingo_row("YF:KPTI:close", "yfinance", date(2026, 11, 18))
    # No end date known: every stored row stays (BKH and GWHT closes run to 11-13).
    for ticker in ("BKH", "GWHT"):
        assert not contract.is_foreign_tiingo_row(f"YF:{ticker}:close", "TIINGO", date(2026, 11, 13))


def test_map_actors_keep_their_place_in_the_map():
    """Survivorship: the panels read the map historically, so nothing leaves it."""
    tickers = {a["ticker"] for sector in sm.SECTOR_MAP.values()
               for sub in sector.get("subsectors", {}).values()
               for a in sub.get("actors", []) if a.get("ticker")}
    assert {"BKH", "ROKU", "GWH"} <= tickers


def test_the_routine_price_list_fetches_hzn_and_none_of_the_others(monkeypatch):
    monkeypatch.setattr(tp.TiingoPuller, "oracle_tickers", staticmethod(lambda e: ["KPTI", "ROKU"]))
    monkeypatch.setattr(tp.TiingoPuller, "hypothesis_tickers", staticmethod(lambda e: ["GWHT"]))
    monkeypatch.setattr(tp.TiingoPuller, "insider_buyer_tickers", staticmethod(lambda e: ["BKH"]))
    monkeypatch.setattr(tp.TiingoPuller, "supply_chain_tickers", staticmethod(lambda e: []))
    tickers = set(tp.TiingoPuller.default_tickers(object()))
    assert "HZN" in tickers
    assert not tickers & {"BKH", "GWHT", "GWH", "KPTI", "ROKU", "KPTIQ", "GWHTQ"}
