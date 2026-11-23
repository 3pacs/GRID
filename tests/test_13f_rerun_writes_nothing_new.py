"""A 13F re-run that reads the same filings writes no change row.

grid-svr, 2026-11-23: every ``etf_flows`` run more than an hour after the last
appended every 13F change row again (167,475 rows for 41,762 distinct
``(series_id, obs_date)`` in one day). The writer used
``BasePuller._row_exists``, whose window is one hour. It now skips a change row
only when the newest stored vintage is the same observation: the same filing
pair and identical content. Anything else is a new vintage and is stored.

The real writer runs against the fake EDGAR of
``tests/thirteenf_holdings_support.py`` (network replaced only). The filing
receipts (``13F-FILING:...``) are not part of this: one per run is the #1393
design, and the holdings reader ranks filings by their receipt time.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from ingestion.altdata import institutional_flows as inst
from intelligence import dollar_flows
from intelligence import holdings_observations_13f as h13f
from tests import thirteenf_holdings_support as s


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    h13f.clear_snapshot_cache()
    monkeypatch.setattr(dollar_flows, "_get_cusip_ticker_map", lambda: None)
    yield
    h13f.clear_snapshot_cache()


@pytest.fixture()
def engine():
    return s.make_sqlite_engine()


def _count(engine, pattern: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM raw_series WHERE series_id LIKE :p"),
                            {"p": pattern}).scalar_one()


def _age_stored_rows(engine) -> int:
    """Make every stored 13F row look at least a day old (far outside the old
    one-hour window), one second apart in write order: production timestamps
    are unique per series and date, SQLite's CURRENT_TIMESTAMP is not.
    Returns the number of change rows."""
    base = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0) - timedelta(days=2)
    with engine.begin() as conn:
        ids = [r[0] for r in conn.execute(text(
            "SELECT id FROM raw_series WHERE series_id LIKE '13F%' ORDER BY id"))]
        for n, row_id in enumerate(ids):
            conn.execute(text("UPDATE raw_series SET pull_timestamp = :ts WHERE id = :id"),
                         {"ts": base + timedelta(seconds=n), "id": row_id})
    return _count(engine, "13F:%")


def _holdings(engine) -> dict:
    h13f.clear_snapshot_cache()
    snap = h13f.read_13f_holdings_snapshot(engine)
    return {(h.manager_cik, h.ticker): (h.reported_value_usd, h.shares, h.filing_accession, h.received_at)
            for h in snap.holdings}


def test_a_rerun_a_day_later_writes_no_change_row(engine, monkeypatch):
    edgar = s.two_quarter_scenario()
    first = s.run_writer(engine, monkeypatch, edgar)
    stored = _count(engine, "13F:%")
    assert stored > 0 and sum(r["rows_inserted"] for r in first) == stored
    assert sum(r["rows_unchanged"] for r in first) == 0
    assert _age_stored_rows(engine) == stored

    second = s.run_writer(engine, monkeypatch, edgar)

    assert _count(engine, "13F:%") == stored
    assert sum(r["rows_inserted"] for r in second) == 0
    assert sum(r["rows_unchanged"] for r in second) == stored
    assert all(r["status"] == "SUCCESS" for r in second)


def test_many_reruns_keep_one_row_per_observation(engine, monkeypatch):
    edgar = s.two_quarter_scenario()
    s.run_writer(engine, monkeypatch, edgar)
    stored = _count(engine, "13F:%")
    for _ in range(4):
        _age_stored_rows(engine)
        s.run_writer(engine, monkeypatch, edgar)
    with engine.connect() as conn:
        copies = conn.execute(text(
            "SELECT MAX(n) FROM (SELECT COUNT(*) AS n FROM raw_series WHERE series_id LIKE '13F:%' "
            "GROUP BY series_id, obs_date) per_key")).scalar_one()
    assert _count(engine, "13F:%") == stored and copies == 1


def test_the_served_holdings_do_not_move_on_a_rerun(engine, monkeypatch):
    """Same holdings, and received_at stays the first receipt instead of
    being re-stamped by each run's copy."""
    edgar = s.two_quarter_scenario()
    s.run_writer(engine, monkeypatch, edgar)
    _age_stored_rows(engine)
    before = _holdings(engine)
    assert before

    s.run_writer(engine, monkeypatch, edgar)

    assert _holdings(engine) == before


def test_a_changed_filing_is_a_new_vintage_and_only_for_the_changed_series(engine, monkeypatch):
    edgar = s.two_quarter_scenario()
    filers = {s.CIK_ALPHA: s.ALPHA}
    s.run_writer(engine, monkeypatch, edgar, filers)
    stored = _count(engine, "13F:%")
    _age_stored_rows(engine)

    curr = edgar.filings[s.CIK_ALPHA][1]
    curr.holdings[s.ROOT_CUSIP] = ("ROOT HOLDINGS INC", 280_000, 2_000)
    second = s.run_writer(engine, monkeypatch, edgar, filers)

    assert sum(r["rows_inserted"] for r in second) == 1
    assert sum(r["rows_unchanged"] for r in second) == stored - 1
    assert _count(engine, f"13F:{s.CIK_ALPHA}:{s.ROOT_CUSIP}:%") == 2
    assert _holdings(engine)[(s.CIK_ALPHA, s.ROOT)][0] == 280_000.0


def test_going_back_to_an_earlier_state_is_stored_again(engine, monkeypatch):
    """Only the newest stored vintage counts: A, B, A is three rows, so a
    point-in-time read between them still sees B."""
    edgar = s.two_quarter_scenario()
    filers = {s.CIK_ALPHA: s.ALPHA}
    curr = edgar.filings[s.CIK_ALPHA][1]
    original = curr.holdings[s.ROOT_CUSIP]
    s.run_writer(engine, monkeypatch, edgar, filers)
    _age_stored_rows(engine)
    curr.holdings[s.ROOT_CUSIP] = ("ROOT HOLDINGS INC", 280_000, 2_000)
    s.run_writer(engine, monkeypatch, edgar, filers)
    _age_stored_rows(engine)
    curr.holdings[s.ROOT_CUSIP] = original
    third = s.run_writer(engine, monkeypatch, edgar, filers)

    assert sum(r["rows_inserted"] for r in third) == 1
    assert _count(engine, f"13F:{s.CIK_ALPHA}:{s.ROOT_CUSIP}:%") == 3


def test_a_quarantined_copy_does_not_count_as_stored(engine, monkeypatch):
    edgar = s.two_quarter_scenario()
    s.run_writer(engine, monkeypatch, edgar)
    stored = _count(engine, "13F:%")
    with engine.begin() as conn:
        conn.execute(text("UPDATE raw_series SET pull_status = 'QUARANTINED' WHERE series_id LIKE '13F:%'"))

    second = s.run_writer(engine, monkeypatch, edgar)

    assert sum(r["rows_inserted"] for r in second) == stored
    with engine.connect() as conn:
        good = conn.execute(text("SELECT COUNT(*) FROM raw_series WHERE series_id LIKE '13F:%' "
                                 "AND pull_status = 'SUCCESS'")).scalar_one()
    assert good == stored


def test_lookup_is_chunked_without_changing_the_result(engine, monkeypatch):
    monkeypatch.setattr(inst, "_CHANGE_ROW_LOOKUP_CHUNK", 1)
    edgar = s.two_quarter_scenario()
    s.run_writer(engine, monkeypatch, edgar)
    stored = _count(engine, "13F:%")
    _age_stored_rows(engine)
    second = s.run_writer(engine, monkeypatch, edgar)
    assert _count(engine, "13F:%") == stored and sum(r["rows_unchanged"] for r in second) == stored


def test_receipts_are_still_one_per_run(engine, monkeypatch):
    """Not changed here: each run's receipt is a dated record (#1393)."""
    edgar = s.two_quarter_scenario()
    s.run_writer(engine, monkeypatch, edgar)
    receipts = _count(engine, "13F-FILING:%")
    _age_stored_rows(engine)
    s.run_writer(engine, monkeypatch, edgar)
    assert _count(engine, "13F-FILING:%") == 2 * receipts


PAYLOAD = {"filing_accession": "A-0", "prev_filing_accession": "A-1", "cusip": "X", "shares": 100,
           "valuation_price_usd": 12.345678, "report_date": "2026-09-30", "unqualified_reason": None}


@pytest.mark.parametrize(("stored", "same"), [
    (("SUCCESS", 1500.0, dict(PAYLOAD)), True),
    (("SUCCESS", 1500.0, json.dumps(PAYLOAD)), True),                       # SQLite/TEXT payload
    (("SUCCESS", 1500, json.dumps(PAYLOAD).encode()), True),
    (None, False),                                                          # nothing stored
    (("SUCCESS", 1500.5, dict(PAYLOAD)), False),                            # value differs
    (("SUCCESS", 1500.0, {**PAYLOAD, "shares": 101}), False),               # content differs
    (("SUCCESS", 1500.0, {**PAYLOAD, "filing_accession": "A-0R"}), False),  # an amendment
    (("SUCCESS", 1500.0, {**PAYLOAD, "prev_filing_accession": "A-1R"}), False),  # other prior filing
    (("SUCCESS", 1500.0, {k: v for k, v in PAYLOAD.items() if k != "prev_filing_accession"}), False),
    (("PARTIAL", 1500.0, dict(PAYLOAD)), False),
    (("FAILED", 0.0, dict(PAYLOAD)), False),
    (("SUCCESS", None, dict(PAYLOAD)), False),
    (("SUCCESS", 1500.0, "not json"), False),
    (("SUCCESS", 1500.0, None), False),
    (("SUCCESS", 1500.0, ["a", "list"]), False),
])
def test_is_stored_observation(stored, same):
    assert inst._is_stored_observation(stored, 1500.0, dict(PAYLOAD)) is same


def test_a_payload_without_its_filing_identity_is_never_treated_as_stored():
    bare = {k: v for k, v in PAYLOAD.items() if k not in ("filing_accession", "prev_filing_accession")}
    assert inst._is_stored_observation(("SUCCESS", 1500.0, dict(bare)), 1500.0, dict(bare)) is False
