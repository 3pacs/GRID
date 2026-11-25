"""UTZ is marked acquired in the lifecycle sidecar.

Utz Brands' merger with Intersnack Group's acquisition vehicle closed on
2026-11-20 (Form 8-K filed that day; NYSE Form 25-NSE the same day). Tiingo's
listing ends 2026-11-19. Until it was marked, UTZ alone kept the routine
Tiingo price run PARTIAL (a close in the lapsed window, none for the expected
session) from 2026-11-24 until the window passed.

Same readers as the other retired names: ``price_ticker`` (what to fetch) and
``tiingo_cutoff`` (TIINGO rows after ``last_session`` are not this instrument).
"""

from __future__ import annotations

from datetime import date

import tiingo_symbol_contract as contract
from analysis import sector_map as sm


def test_utz_is_acquired_by_a_private_group_and_names_no_successor():
    entry = sm.TICKER_STATUS["UTZ"]
    assert entry["status"] == "acquired"
    assert "Intersnack" in entry["acquirer"]
    assert "successor" not in entry


def test_utz_prices_nothing_and_stays_in_the_map():
    assert sm.price_ticker("UTZ") is None
    assert "UTZ" not in sm.price_tickers()
    in_map = {
        a["ticker"]
        for sector in sm.SECTOR_MAP.values()
        for sub in sector.get("subsectors", {}).values()
        for a in sub.get("actors", [])
        if a.get("ticker")
    }
    assert "UTZ" in in_map          # survivorship: the map keeps every actor


def test_last_session_is_tiingos_end_date_and_cuts_later_rows():
    assert sm.TICKER_STATUS["UTZ"]["last_session"] == date(2026, 11, 19)
    assert contract.tiingo_cutoff("YF:UTZ:close") == date(2026, 11, 19)
    assert not contract.is_foreign_tiingo_row("YF:UTZ:close", "TIINGO", date(2026, 11, 19))
    assert contract.is_foreign_tiingo_row("YF:UTZ:close", "TIINGO", date(2026, 11, 20))


def test_note_cites_the_filing_and_where_the_facts_were_read():
    note = sm.TICKER_STATUS["UTZ"]["note"]
    assert "Form 8-K" in note and "Form 25-NSE" in note
    assert "sec.gov/Archives/edgar/data/1739566/" in note
    assert "Tiingo" in note and "read 2026-11-23" in note
