"""QuiverQuant writer: one row per act, and the fiscal-quarter mapping.

Before this change ``_store_signals`` wrote ``source_id = "qq_<endpoint>"`` (a
constant) and upserted ``ON CONFLICT (source_type, source_id, ticker, signal_date,
signal_type) DO UPDATE SET signal_value``, so every act sharing a ticker and a date
(and, for insiders, a side) collapsed into one row and the last payload won.
``gov_contracts`` additionally read the federal fiscal ``(Year, Qtr)`` as a calendar
quarter.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest

from ingestion.altdata import quiverquant as qq
from ingestion.altdata import quiverquant_identity as ident

# ── a minimal engine that behaves like the signal_sources upsert ────────────


class _UpsertConn:
    """Executes the writer's INSERT ... ON CONFLICT DO UPDATE against a dict."""

    def __init__(self, table: dict):
        self.table = table

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params):
        sql = str(statement)
        assert "INSERT INTO signal_sources" in sql
        assert "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type)" in sql
        key = (params["source_type"], params["source_id"], params["ticker"],
               params["signal_date"], params["signal_type"])
        self.table[key] = json.loads(params["signal_value"])  # DO UPDATE SET signal_value


class _Engine:
    def __init__(self):
        self.table: dict = {}

    def begin(self):
        return _UpsertConn(self.table)


def _store(records, endpoint_key):
    engine = _Engine()
    stored = qq._store_signals(engine, records, qq.ENDPOINTS[endpoint_key]["source_type"], endpoint_key)
    return engine.table, stored


# ── identity: pure, normalised, stable ──────────────────────────────────────

INSIDER = {
    "Ticker": "ACME", "Date": "2026-09-30", "Name": "Smith, John Q.", "TransactionCode": "S",
    "AcquiredDisposedCode": "D", "Shares": 1000, "PricePerShare": 12.5,
    "fileDate": "2026-09-30T17:45:09.115", "uploaded": "2026-09-30T17:45:09.115",
}


def test_insider_identity_is_name_code_shares_price():
    assert ident.act_identity("insider_trading", INSIDER) == "smith john q|s|1000|12.5"
    assert ident.source_id_for("insider_trading", INSIDER) == "qq_insider_trading:smith john q|s|1000|12.5"


@pytest.mark.parametrize("variant", [
    {"Name": "SMITH JOHN Q"},
    {"Name": "  smith,  john q. "},
    {"Shares": 1000.0},
    {"Shares": "1,000.00"},
    {"PricePerShare": "12.50"},
    {"PricePerShare": 12.5000},
    {"TransactionCode": " s "},
])
def test_insider_identity_ignores_formatting(variant):
    assert ident.act_identity("insider_trading", {**INSIDER, **variant}) == ident.act_identity("insider_trading", INSIDER)


@pytest.mark.parametrize("variant", [
    {"Name": "Smith, Jane Q."},
    {"TransactionCode": "P"},
    {"Shares": 1001},
    {"PricePerShare": 12.51},
])
def test_insider_identity_separates_different_acts(variant):
    assert ident.act_identity("insider_trading", {**INSIDER, **variant}) != ident.act_identity("insider_trading", INSIDER)


def test_insider_identity_ignores_fields_that_change_between_pulls():
    """Dates, upload stamps, ownership-after counts and titles are not part of the act."""
    changed = {**INSIDER, "fileDate": "2026-10-02T09:00:00", "uploaded": "2026-10-02T09:00:01",
               "SharesOwnedFollowing": 5, "officerTitle": "CFO", "isDirector": True}
    assert ident.act_identity("insider_trading", changed) == ident.act_identity("insider_trading", INSIDER)


def test_missing_numbers_do_not_collide_with_zero():
    assert ident.normalize_number(None) == "?"
    assert ident.normalize_number("") == "?"
    assert ident.normalize_number("n/a") == "?"
    assert ident.normalize_number("nan") == "?"
    assert ident.normalize_number(0) == "0"
    assert ident.normalize_number(-0.0) == "0"
    assert ident.normalize_number("1E+3") == "1000"
    assert ident.normalize_number(0.000125) == "0.000125"


HOUSE = {
    "Ticker": "ACME", "Date": "2026-09-01", "Representative": "Jane Doe", "BioGuideID": "d000123",
    "Transaction": "Purchase", "Range": "$1,001 - $15,000", "Amount": 1001.0, "last_modified": "2026-09-05",
}


def test_house_identity_is_bioguide_transaction_range():
    assert ident.act_identity("house_trading", HOUSE) == "D000123|purchase|1001-15000"
    assert ident.source_id_for("house_trading", HOUSE) == "qq_house_trading:D000123|purchase|1001-15000"


def test_house_identity_falls_back_to_normalised_name_without_bioguide():
    rec = {k: v for k, v in HOUSE.items() if k != "BioGuideID"}
    assert ident.act_identity("house_trading", rec) == "jane doe|purchase|1001-15000"


def test_house_identity_ignores_last_modified_and_range_formatting():
    other = {**HOUSE, "last_modified": "2026-10-01", "Range": "$1,001 – $15,000"}
    assert ident.act_identity("house_trading", other) == ident.act_identity("house_trading", HOUSE)


def test_senate_identity_reads_senator_field():
    rec = {"Ticker": "ACME", "Senator": "John Roe", "BioGuideID": "R000999", "Transaction": "Sale (Full)",
           "Range": "$15,001 - $50,000"}
    assert ident.source_id_for("senate_trading", rec) == "qq_senate_trading:R000999|sale (full)|15001-50000"
    no_id = {k: v for k, v in rec.items() if k != "BioGuideID"}
    assert ident.act_identity("senate_trading", no_id) == "john roe|sale (full)|15001-50000"


def test_lobbying_identity_is_registrant_client_amount():
    rec = {"Ticker": "ACME", "Date": "2026-08-01", "Registrant": "Big Lobby LLP", "Client": "Acme, Inc.",
           "Amount": "120000.00", "Issue": "TAX", "Specific_Issue": "text that is not part of the act"}
    assert ident.act_identity("lobbying", rec) == "big lobby llp|acme inc|120000"
    assert ident.source_id_for("lobbying", rec) == "qq_lobbying:big lobby llp|acme inc|120000"


@pytest.mark.parametrize("endpoint", ["wsb", "off_exchange", "flights", "twitter", "political_beta", "gov_contracts"])
def test_aggregate_endpoints_keep_the_constant_id(endpoint):
    assert ident.act_identity(endpoint, {"Ticker": "ACME", "Name": "x"}) is None
    assert ident.source_id_for(endpoint, {"Ticker": "ACME"}) == f"qq_{endpoint}"


def test_every_endpoint_id_matches_the_writers_old_constant():
    for key in qq.ENDPOINTS:
        assert ident.legacy_source_id(key) == f"qq_{key}"
        assert ident.FEED_IDS[key] == f"qq_{key}"


def test_long_identity_is_bounded_deterministic_and_still_distinct():
    long_a = {"Registrant": "R" * 300, "Client": "C" * 300, "Amount": 1}
    long_b = {"Registrant": "R" * 300, "Client": "C" * 299 + "D", "Amount": 1}
    ia, ib = ident.act_identity("lobbying", long_a), ident.act_identity("lobbying", long_b)
    assert len(ia) <= ident.MAX_IDENTITY_LEN and len(ib) <= ident.MAX_IDENTITY_LEN
    assert ia != ib
    assert ia == ident.act_identity("lobbying", dict(long_a))


def test_identity_of_the_raw_record_equals_identity_of_the_stored_payload():
    """The re-key script recomputes identity from signal_value; it must match the writer's."""
    for endpoint, rec in (("insider_trading", INSIDER), ("house_trading", HOUSE)):
        stored = {k: v for k, v in rec.items() if k not in ("Ticker", "ticker", "Date", "date", "ReportDate")}
        assert ident.source_id_for(endpoint, rec) == ident.source_id_for(endpoint, stored)


def test_feed_source_id_recovers_the_constant():
    assert ident.feed_source_id("qq_house_trading:D000123|purchase|1001-15000") == "qq_house_trading"
    assert ident.feed_source_id("qq_insider_trading") == "qq_insider_trading"
    assert ident.feed_source_id("whale_spy_450") == "whale_spy_450"
    assert ident.feed_source_id("edge:abc") == "edge:abc"
    assert ident.feed_source_id("") == ""


def test_feed_source_id_sql_is_text_safe():
    from sqlalchemy import text

    sql = ident.feed_source_id_sql("ss.source_id")
    assert "split_part(ss.source_id, ':', 1)" in sql and "substr(ss.source_id, 1, 3) = 'qq_'" in sql
    assert "%" not in sql
    assert text(sql).compile().params == {}  # no accidental bind parameters


# ── writer: two acts, two rows ──────────────────────────────────────────────


def test_two_insider_acts_on_one_ticker_date_and_side_make_two_rows():
    a = {**INSIDER, "Name": "Alice A", "Shares": 100}
    b = {**INSIDER, "Name": "Bob B", "Shares": 250}
    table, stored = _store([a, b], "insider_trading")
    assert stored == 2 and len(table) == 2
    assert {k[1] for k in table} == {"qq_insider_trading:alice a|s|100|12.5", "qq_insider_trading:bob b|s|250|12.5"}
    assert {k[4] for k in table} == {"insider_sell"}
    assert {(k[2], k[3]) for k in table} == {("ACME", date(2026, 9, 30))}


def test_same_insider_two_lines_with_different_shares_make_two_rows():
    table, _ = _store([{**INSIDER, "Shares": 100}, {**INSIDER, "Shares": 200}], "insider_trading")
    assert len(table) == 2


def test_the_same_act_pulled_twice_is_one_row_with_the_latest_payload():
    first = {**INSIDER, "uploaded": "2026-09-30T17:45:09.115"}
    again = {**INSIDER, "uploaded": "2026-10-01T06:00:00.000", "SharesOwnedFollowing": 42}
    engine = _Engine()
    qq._store_signals(engine, [first], "quiverquant:insider", "insider_trading")
    qq._store_signals(engine, [again], "quiverquant:insider", "insider_trading")
    assert len(engine.table) == 1
    assert next(iter(engine.table.values()))["SharesOwnedFollowing"] == 42


def test_house_acts_on_one_ticker_and_date_make_one_row_per_member_and_trade():
    other_member = {**HOUSE, "Representative": "Sam Poe", "BioGuideID": "P000777"}
    other_range = {**HOUSE, "Range": "$15,001 - $50,000"}
    table, _ = _store([HOUSE, other_member, other_range], "house_trading")
    assert len(table) == 3


def test_senate_acts_make_one_row_per_member_and_trade():
    a = {"Ticker": "ACME", "Date": "2026-09-01", "Senator": "John Roe", "BioGuideID": "R000999",
         "Transaction": "Purchase", "Range": "$1,001 - $15,000"}
    b = {**a, "Transaction": "Sale"}
    table, _ = _store([a, b], "senate_trading")
    assert len(table) == 2


def test_lobbying_acts_make_one_row_per_registrant_client_amount():
    a = {"Ticker": "ACME", "Date": "2026-08-01", "Registrant": "Big Lobby LLP", "Client": "Acme", "Amount": 100}
    b = {**a, "Registrant": "Small Lobby LLC"}
    c = {**a, "Amount": 200}
    table, _ = _store([a, b, c], "lobbying")
    assert len(table) == 3
    assert all(k[1].startswith("qq_lobbying:") for k in table)


def test_aggregate_endpoints_still_write_the_constant_id():
    table, _ = _store([{"Ticker": "ACME", "Date": "2026-09-30", "Sentiment": 0.4, "Mentions": 9}], "wsb")
    assert {k[1] for k in table} == {"qq_wsb"}
    table, _ = _store([{"Ticker": "ACME", "Year": 2026, "Qtr": 4, "Amount": 5}], "gov_contracts")
    assert {k[1] for k in table} == {"qq_gov_contracts"}


def test_payload_still_excludes_only_ticker_and_date_fields():
    table, _ = _store([INSIDER], "insider_trading")
    payload = next(iter(table.values()))
    assert "Ticker" not in payload and "Date" not in payload and payload["Name"] == "Smith, John Q."


# ── gov_contracts: federal fiscal quarter ends ──────────────────────────────


@pytest.mark.parametrize("qtr,expected", [
    (1, date(2025, 12, 31)),   # FY2026 Q1 = Oct-Dec 2025
    (2, date(2026, 3, 31)),    # Jan-Mar 2026
    (3, date(2026, 6, 30)),    # Apr-Jun 2026
    (4, date(2026, 9, 30)),    # Jul-Sep 2026
])
def test_fiscal_quarter_end_covers_all_four_quarters(qtr, expected):
    assert ident.fiscal_quarter_end(2026, qtr) == expected
    assert qq._gov_contract_period_date({"Year": 2026, "Qtr": qtr}) == expected


def test_the_canary_case_fy2026_q4_is_not_after_its_first_sighting():
    """Seen first on 2026-09-11: its period end must not be in a calendar quarter yet to begin."""
    assert qq._gov_contract_period_date({"Year": 2026, "Qtr": 4}) == date(2026, 9, 30)


def test_gov_contract_period_date_still_accepts_alternate_keys_and_rejects_bad_ones():
    assert qq._gov_contract_period_date({"year": "2025", "qtr": "3"}) == date(2025, 6, 30)
    assert qq._gov_contract_period_date({"Year": 2026, "Quarter": 2}) == date(2026, 3, 31)
    assert qq._gov_contract_period_date({"Year": 2026, "Qtr": 5}) is None
    assert qq._gov_contract_period_date({"Year": 2026}) is None
    assert qq._gov_contract_period_date({}) is None


def test_gov_contracts_rows_are_dated_at_the_fiscal_end():
    table, _ = _store([{"Ticker": "LMT", "Year": 2026, "Qtr": 4, "Amount": 5_000_000}], "gov_contracts")
    assert {k[3] for k in table} == {date(2026, 9, 30)}


def test_calendar_end_of_a_quarter_is_the_fiscal_end_of_the_next_one():
    """The identity that makes a naive re-date collide (and the re-date script order it)."""
    for year in (2024, 2025, 2026):
        for qtr in (1, 2, 3):
            assert ident.calendar_quarter_end(year, qtr) == ident.fiscal_quarter_end(year, qtr + 1)
        assert ident.calendar_quarter_end(year, 4) == ident.fiscal_quarter_end(year + 1, 1)


def test_people_events_adapter_calendar_table_matches_the_identity_module():
    from intelligence.people_events_pipeline import adapters as A

    for qtr in (1, 2, 3, 4):
        assert date(2026, *A._WRITER_QUARTER_END[qtr]) == ident.calendar_quarter_end(2026, qtr)
        assert A.federal_fiscal_quarter(2026, qtr)[1] == ident.fiscal_quarter_end(2026, qtr)


# ── consumers keep working with keyed ids ───────────────────────────────────


def test_puller_identity_still_reads_the_person_from_the_payload():
    from intelligence import lever_pullers as lp

    sid = ident.source_id_for("house_trading", HOUSE)
    assert lp.puller_identity("quiverquant:house", sid, {"Representative": "Jane Doe"}) == "Jane Doe"
    sid = ident.source_id_for("insider_trading", INSIDER)
    assert lp.puller_identity("quiverquant:insider", sid, {"Name": "Smith, John Q."}) == "Smith, John Q."


def test_puller_identity_falls_back_to_the_feed_id_not_the_act_key():
    """Same fallback as before act keys existed (scripts/connect_dots* exclude exactly these ids)."""
    from intelligence import lever_pullers as lp

    sid = ident.source_id_for("senate_trading", {"BioGuideID": "R000999", "Transaction": "Purchase", "Range": "x"})
    assert lp.puller_identity("quiverquant:senate", sid, {}) == "qq_senate_trading"
    assert lp.puller_identity("quiverquant:senate", "qq_senate_trading", "not json") == "qq_senate_trading"


def test_identity_sql_falls_back_to_the_feed_id():
    from intelligence import lever_pullers as lp

    assert "split_part(source_id, ':', 1)" in lp._IDENTITY_SQL
    assert lp._IDENTITY_SQL.count("position(':' in source_id) > 0") == 4
    assert "regexp_replace(source_id, '_[0-9.]+$', '')" in lp._IDENTITY_SQL


def test_aggregate_and_keyed_ids_all_keep_the_artefact_prefix():
    """graph_analytics / intelligence_actors / connect_dots filters key on ``qq_``."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    assert 'ARTEFACT_ID_PREFIX = "qq_"' in (root / "scripts" / "graph_analytics.py").read_text(encoding="utf-8")
    assert "NOT LIKE 'qq_%%'" in (root / "api" / "routers" / "intelligence_actors.py").read_text(encoding="utf-8")
    for key in qq.ENDPOINTS:
        assert ident.FEED_IDS[key].startswith("qq_")
    assert ident.source_id_for("insider_trading", INSIDER).startswith("qq_")


def test_trust_scorer_scores_a_quiverquant_feed_as_one_source(monkeypatch):
    """Per-act ids must not turn one feed into thousands of one-signal 'sources'."""
    from datetime import date as _date

    from intelligence import trust_scorer

    monkeypatch.setattr(trust_scorer, "_ensure_tables", lambda engine: None)

    class _Rows:
        def __init__(self, rows):
            self._rows = rows
            self.rowcount = len(rows)

        def fetchall(self):
            return self._rows

    class _Conn:
        def __init__(self):
            self.updates: list[dict] = []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, statement, params=None):
            sql = str(statement)
            if "SELECT source_type, source_id, outcome" in sql:
                today = _date.today()
                return _Rows([
                    ("quiverquant:house", "qq_house_trading:a|buy|1", "CORRECT", 0.02, today, "AAA"),
                    ("quiverquant:house", "qq_house_trading:b|sell|2", "WRONG", -0.01, today, "BBB"),
                    ("quiverquant:house", "qq_house_trading", "CORRECT", 0.03, today, "CCC"),
                    ("congressional", "Jane Doe", "CORRECT", 0.01, today, "DDD"),
                ])
            if "SELECT id FROM signal_sources" in sql:
                return _Rows([(1,)] if params["after_id"] == 0 else [])
            if "UPDATE signal_sources" in sql:
                self.updates.append(params)
                return _Rows([(1,)])
            return _Rows([])

    class _Engine:
        def __init__(self):
            self.conn = _Conn()

        def connect(self):
            return self.conn

        begin = connect

    engine = _Engine()
    result = trust_scorer.update_trust_scores(engine)

    by_id = {(s["source_type"], s["source_id"]): s for s in result["sources"]}
    assert set(by_id) == {("quiverquant:house", "qq_house_trading"), ("congressional", "Jane Doe")}
    feed = by_id[("quiverquant:house", "qq_house_trading")]
    assert (feed["hit_count"], feed["miss_count"]) == (2, 1)

    feed_update = next(u for u in engine.conn.updates if u["si"] == "qq_house_trading")
    assert feed_update["keyed_prefix"] == "qq_house_trading:"
    assert feed_update["keyed_len"] == len("qq_house_trading:")
    other_update = next(u for u in engine.conn.updates if u["si"] == "Jane Doe")
    assert other_update["keyed_prefix"] is None


# ── identity: partial identities, adapter-aligned aliases, BioGuide-less key ──


def test_partial_identity_is_reported():
    assert not ident.is_partial_identity("insider_trading", INSIDER)
    assert ident.is_partial_identity("insider_trading", {k: v for k, v in INSIDER.items() if k != "PricePerShare"})
    assert ident.is_partial_identity("house_trading", {"Representative": "Jane Doe", "Transaction": "Purchase"})
    assert not ident.is_partial_identity("wsb", {})  # aggregate endpoints have no identity


def test_field_aliases_match_the_people_events_adapters():
    from intelligence.people_events_pipeline import adapters as A

    for key in A._QQ_MEMBER:
        rec = {key: "Jane Doe", "Transaction": "Purchase", "Range": "$1,001 - $15,000"}
        assert ident.act_identity("house_trading", rec) == "jane doe|purchase|1001-15000", key
    for key in A._QQ_OWNER:
        rec = {key: "Jane Doe", "TransactionCode": "S", "Shares": 1, "PricePerShare": 2}
        assert ident.act_identity("insider_trading", rec) == "jane doe|s|1|2", key
    # Range falls back to Amount, as in the adapter
    assert ident.act_identity("senate_trading", {"Senator": "J R", "Transaction": "Sale", "Amount": 15001}) \
        == "j r|sale|15001"
    assert ident.act_identity("house_trading", {"Representative": "J D", "Transaction": "Sale", "Range": "$1 - $2",
                                                "Amount": 1}) == "j d|sale|1-2"  # Range wins


def test_member_loose_key_ignores_bioguide():
    with_id = {**HOUSE}
    without_id = {k: v for k, v in HOUSE.items() if k != "BioGuideID"}
    assert ident.has_bioguide(with_id) and not ident.has_bioguide(without_id)
    assert ident.act_identity("house_trading", with_id) != ident.act_identity("house_trading", without_id)
    assert ident.member_loose_key("house_trading", with_id) == ident.member_loose_key("house_trading", without_id)
    assert ident.member_loose_key("insider_trading", INSIDER) is None


# ── trust_scorer: last_signal_date is the newest, not the first row seen ──────


def test_trust_scorer_last_signal_date_is_the_max_across_a_feeds_rows(monkeypatch):
    from datetime import timedelta

    from intelligence import trust_scorer

    monkeypatch.setattr(trust_scorer, "_ensure_tables", lambda engine: None)
    today = date.today()

    class _Rows:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, statement, params=None):
            if "SELECT source_type, source_id, outcome" in str(statement):
                # ordered by the per-act source_id, so the newest row is NOT first
                return _Rows([
                    ("quiverquant:house", "qq_house_trading:a|buy|1", "CORRECT", 0.01, today - timedelta(days=30), "AAA"),
                    ("quiverquant:house", "qq_house_trading:b|buy|1", "CORRECT", 0.01, today, "BBB"),
                    ("quiverquant:house", "qq_house_trading:c|buy|1", "WRONG", -0.01, today - timedelta(days=9), "CCC"),
                ])
            return _Rows([])

    class _Engine:
        def connect(self):
            return _Conn()

        begin = connect

    result = trust_scorer.update_trust_scores(_Engine())
    assert [s["last_signal_date"] for s in result["sources"]] == [str(today)]


# ── the transition guard ─────────────────────────────────────────────────────


@pytest.fixture()
def guard(monkeypatch, tmp_path):
    marker = tmp_path / "quiverquant_transition_done"
    monkeypatch.setenv(ident.MARKER_FILE_ENV, str(marker))
    monkeypatch.delenv(ident.GUARD_OFF_ENV, raising=False)
    return marker


def _no_network(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("a guarded pull must not reach the API")

    monkeypatch.setattr(qq, "_get_api_key", _boom)
    monkeypatch.setattr(qq, "_fetch_endpoint", _boom)
    monkeypatch.setattr(qq, "_store_signals", _boom)


@pytest.mark.parametrize("endpoint", sorted(ident.TRANSITION_GUARDED_ENDPOINTS))
def test_guarded_endpoints_do_not_pull_until_the_marker_exists(guard, monkeypatch, endpoint):
    _no_network(monkeypatch)
    out = qq.pull_endpoint(object(), endpoint)
    assert out["status"] == "SKIPPED" and out["stored"] == 0
    assert str(guard) in out["skipped_reason"] and "transition marker" in out["skipped_reason"]


def test_guard_covers_the_four_keyed_endpoints_and_gov_contracts_only():
    assert ident.TRANSITION_GUARDED_ENDPOINTS == {
        "insider_trading", "house_trading", "senate_trading", "lobbying", "gov_contracts"}


@pytest.mark.parametrize("endpoint", ["wsb", "off_exchange", "flights", "twitter", "political_beta"])
def test_aggregate_endpoints_are_not_held(guard, monkeypatch, endpoint):
    monkeypatch.setattr(qq, "_get_api_key", lambda: "k")
    monkeypatch.setattr(qq, "_fetch_endpoint", lambda path, key: [])
    monkeypatch.setattr(qq, "_store_signals", lambda *a, **k: 0)
    monkeypatch.setattr(qq.time, "sleep", lambda s: None)
    assert qq.pull_endpoint(object(), endpoint)["status"] == "SUCCESS"


def test_pulls_resume_once_the_marker_exists(guard, monkeypatch):
    monkeypatch.setattr(qq, "_get_api_key", lambda: "k")
    monkeypatch.setattr(qq, "_fetch_endpoint", lambda path, key: [{"Ticker": "A"}])
    monkeypatch.setattr(qq, "_store_signals", lambda *a, **k: 1)
    monkeypatch.setattr(qq.time, "sleep", lambda s: None)
    assert qq.pull_endpoint(object(), "insider_trading")["status"] == "SKIPPED"
    guard.write_text("done")
    out = qq.pull_endpoint(object(), "insider_trading")
    assert out["status"] == "SUCCESS" and out["stored"] == 1


def test_guard_off_switch(guard, monkeypatch):
    monkeypatch.setenv(ident.GUARD_OFF_ENV, "off")
    assert not ident.transition_guard_blocks("insider_trading")


def test_the_skip_logs_a_warning(guard, monkeypatch):
    _no_network(monkeypatch)
    messages: list[str] = []
    sink = qq.log.add(lambda m: messages.append(str(m)), level="WARNING")
    try:
        qq.pull_endpoint(object(), "lobbying")
    finally:
        qq.log.remove(sink)
    assert any("SKIPPED" in m and "transition marker" in m for m in messages)


def test_pull_all_holds_the_whole_job_while_any_endpoint_is_guarded(guard, monkeypatch):
    """No aggregate runs while held: nothing is pulled, nothing real can be hidden, no API call."""
    _no_network(monkeypatch)
    out = qq.pull_all(object())
    assert [r["endpoint"] for r in out] == list(qq.ENDPOINTS)
    assert {r["status"] for r in out} == {"SKIPPED"} and all(r["stored"] == 0 for r in out)
    assert all(str(guard) in r["skipped_reason"] and "transition marker" in r["skipped_reason"] for r in out)


def test_pull_all_runs_everything_once_the_marker_exists(guard, monkeypatch):
    guard.write_text("done")
    monkeypatch.setattr(qq, "_get_api_key", lambda: "k")
    monkeypatch.setattr(qq, "_fetch_endpoint", lambda path, key: [])
    monkeypatch.setattr(qq, "_store_signals", lambda *a, **k: 0)
    monkeypatch.setattr(qq.time, "sleep", lambda s: None)
    assert {r["status"] for r in qq.pull_all(object())} == {"SUCCESS"}


def test_scheduler_reads_a_held_job_as_skipped_and_never_feeds_the_failure_backoff(guard, monkeypatch):
    """Honest-success contract and no backoff: a held pull is SKIPPED (flat 30 min retry), not PARTIAL."""
    from ingestion import smart_scheduler as sched

    _no_network(monkeypatch)
    held = qq.pull_all(object())
    outcome, rows, note = sched._classify_outcome(held)
    assert outcome == sched.OUTCOME_SKIPPED
    assert outcome not in (sched.OUTCOME_SUCCESS, sched.OUTCOME_NO_NEW_DATA, sched.OUTCOME_PARTIAL, sched.OUTCOME_FAILED)

    scheduler = sched.SmartScheduler.__new__(sched.SmartScheduler)
    scheduler._state = {"quiverquant": {"consecutive_fails": 0}}
    for _ in range(4):   # many held ticks in a row
        scheduler._record_result("quiverquant", False, note, skipped=True)
    state = scheduler._state["quiverquant"]
    assert state["consecutive_fails"] == 0 and state.get("last_success") is None
    remaining = state["cooldown_until"] - datetime.now(timezone.utc)
    assert remaining.total_seconds() <= sched.SKIP_RETRY_MINUTES * 60  # flat retry, no growth


def test_a_real_aggregate_failure_is_still_a_failure_once_pulls_run(guard, monkeypatch):
    from ingestion import smart_scheduler as sched

    guard.write_text("done")
    monkeypatch.setattr(qq, "_get_api_key", lambda: "k")
    monkeypatch.setattr(qq.time, "sleep", lambda s: None)

    def _fetch(path, key):
        raise RuntimeError("boom")

    monkeypatch.setattr(qq, "_fetch_endpoint", _fetch)
    assert sched._classify_outcome(qq.pull_all(object()))[0] == sched.OUTCOME_FAILED


# ── consumers that showed or counted the act key ─────────────────────────────


def test_actor_context_shows_the_person_not_the_act_key(monkeypatch):
    from intelligence.actors import analysis

    monkeypatch.setattr(analysis, "_ensure_tables", lambda engine: None)
    monkeypatch.setattr(analysis, "_load_actors_from_db", lambda engine: {})

    class _Rows:
        def fetchall(self):
            return [
                ("quiverquant:house", ident.source_id_for("house_trading", HOUSE), "BUY", date(2026, 9, 1),
                 json.dumps({"Representative": "Jane Doe"}), 0.6),
            ]

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, statement, params=None):
            return _Rows()

    class _Engine:
        def connect(self):
            return _Conn()

    out = analysis.get_actor_context_for_ticker(_Engine(), "ACME")
    assert [a["actor"] for a in out["recent_actions"]] == ["Jane Doe"]


def test_readers_resolve_the_person_in_sql():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    sleuth = (root / "intelligence" / "sleuth.py").read_text(encoding="utf-8")
    assert "COUNT(DISTINCT source_id)" not in sleuth and "DISTINCT {_IDENTITY_SQL}" in sleuth
    discovery = (root / "intelligence" / "actor_discovery.py").read_text(encoding="utf-8")
    assert discovery.count("<> 'qq_'") == 2


def test_a_numeric_amount_in_place_of_a_range_is_one_key():
    """House/Senate payloads with no Range fall back to Amount: 1001, 1001.0 and "1,001.00" are one value."""
    keys = {
        ident.act_identity("house_trading", {"Representative": "J D", "Transaction": "Sale", "Amount": amount})
        for amount in (1001, 1001.0, "1001", "1,001.00", "$1,001")
    }
    assert keys == {"j d|sale|1001"}
    assert ident.normalize_range("1,001.50") == "1001.5"
    # real ranges are unchanged
    assert ident.normalize_range("$1,001 - $15,000") == "1001-15000"
    assert ident.normalize_range("1001 - 15000.0") == "1001-15000.0"


def test_the_marker_path_documents_its_home_dependency():
    assert "GRID_QQ_TRANSITION_DONE_FILE" in ident.transition_marker_path.__doc__
    assert "$HOME" in ident.transition_marker_path.__doc__
