"""Actual SEC writer/reader/consumer controls; synthetic local SQLite only."""

import ast
import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, event, text

from api.dad_sec_fundamentals import read_sec_profile
from ingestion.altdata.sec_edgar_company import SECEdgarCompanyPuller


@pytest.fixture
def engine():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE source_catalog (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO source_catalog VALUES (1, 'SEC_EDGAR_Fundamentals')"))
        conn.execute(text("""CREATE TABLE raw_series (
            series_id TEXT, source_id INTEGER, obs_date DATE, value REAL,
            raw_payload TEXT, pull_status TEXT,
            pull_timestamp TEXT DEFAULT CURRENT_TIMESTAMP)"""))
    yield engine
    engine.dispose()


def fact(value=12.5, **overrides):
    return {"start": "2025-01-01", "end": "2025-12-31", "val": value,
            "filed": "2026-02-01", "accn": "0000320193-26-000001", "form": "10-K",
            "fy": 2025, "fp": "FY", **overrides}


def facts(entry=None, unit="USD", concept="Revenues", cik=320193):
    return {"cik": cik, "facts": {"us-gaap": {
        concept: {"units": {unit: [entry or fact()]}}
    }}}


def puller(engine, data):
    p = SECEdgarCompanyPuller.__new__(SECEdgarCompanyPuller)
    p.engine, p.source_id, p._cik_cache = engine, 1, {"AAPL": "0000320193", "BRK-B": "0001067983"}
    p._fetch_json = lambda url: copy.deepcopy(data)
    return p


def test_writer_then_reader_preserves_period_filing_accession_and_zero(engine):
    p = puller(engine, facts(fact(0)))
    assert p.pull_ticker("AAPL")["rows_inserted"] == 1
    profile = read_sec_profile(engine, "AAPL", refresh=True)
    item = profile["fields"]["revenue"]
    assert item["numeric_value"] == 0.0  # Genuine reported zero, not unavailable.
    assert item["period_start"] == "2025-01-01"
    assert item["period_end"] == "2025-12-31"
    assert item["filed"] == "2026-02-01"
    assert item["accession"] == "0000320193-26-000001"
    assert item["known_at_precision"] == "date"
    assert profile["source"] == "SEC EDGAR/XBRL"
    assert profile["refresh_available"] is False
    assert all(value is None for value in profile["unavailable_fields"].values())
    assert "eps_ttm" not in profile["fields"]
    # Today's capture cannot freshen an old filing.
    assert profile["freshness"]["state"] == "stale"
    assert p.pull_ticker("AAPL")["status"] == "UNCHANGED"


@pytest.mark.parametrize("overrides", [
    {"filed": None}, {"filed": "bad"}, {"filed": "2099-01-01"},
    {"accn": "unknown"}, {"val": float("nan")}, {"val": float("inf")},
    {"val": True}, {"start": None}, {"start": "2026-01-02"},
    {"end": "2026-03-01"}, {"form": "8-K"},
])
def test_invalid_or_unknown_facts_cannot_write_success(engine, overrides):
    p = puller(engine, facts(fact(**overrides)))
    assert p.pull_ticker("AAPL")["status"] == "FAILED"
    assert read_sec_profile(engine, "AAPL")["status"] == "unavailable"
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM raw_series")).scalar_one() == 0


@pytest.mark.parametrize("unit,concept", [("EUR", "Revenues"), ("USD", "EarningsPerShareBasic")])
def test_wrong_currency_or_eps_unit_stays_unavailable(engine, unit, concept):
    assert puller(engine, facts(unit=unit, concept=concept)).pull_ticker("AAPL")["status"] == "FAILED"


def test_wrong_company_response_fails_before_writes(engine):
    p = puller(engine, facts(cik=789019))
    assert p.pull_ticker("AAPL")["error"] == "SEC companyfacts CIK mismatch"
    assert read_sec_profile(engine, "AAPL")["fields"] == {}
    assert p._resolve_cik("BRK.B") == "0001067983"


@pytest.mark.parametrize("results,expected", [
    ([{"status": "SUCCESS", "rows_inserted": 2}], "SUCCESS"),
    ([{"status": "UNCHANGED", "rows_inserted": 0}], "UNCHANGED"),
    ([{"status": "FAILED", "rows_inserted": 0}], "FAILED"),
    ([{"status": "SUCCESS", "rows_inserted": 2},
      {"status": "FAILED", "rows_inserted": 0}], "PARTIAL"),
])
def test_standard_sec_entrypoint_never_claims_zero_or_partial_success(results, expected):
    p = SECEdgarCompanyPuller.__new__(SECEdgarCompanyPuller)
    p.pull_all = lambda: results
    result = p.pull()
    assert result["status"] == expected
    assert result["rows_inserted"] == sum(row["rows_inserted"] for row in results)


def test_revised_filing_appends_once_without_backfilling_older_comparatives(engine):
    first = fact()
    p = puller(engine, facts(first))
    assert p.pull_ticker("AAPL")["rows_inserted"] == 1
    amendment = fact(15, filed="2026-02-15", accn="0000320193-26-000002", form="10-K/A")
    data = facts()
    data["facts"]["us-gaap"]["Revenues"]["units"]["USD"] = [first, amendment]
    p._fetch_json = lambda url: data
    assert p.pull_ticker("AAPL")["rows_inserted"] == 1
    assert p.pull_ticker("AAPL")["status"] == "UNCHANGED"
    # Give capture times distinct fixture values, matching separate invocations.
    with engine.begin() as conn:
        conn.execute(text("UPDATE raw_series SET pull_timestamp = CASE "
                          "WHEN raw_payload LIKE '%0000320193-26-000002%' THEN '2026-02-16' "
                          "ELSE '2026-02-02' END"))
    item = read_sec_profile(engine, "AAPL")["fields"]["revenue"]
    assert item["numeric_value"] == 15
    assert item["filed"] == "2026-02-15"
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM raw_series")).scalar_one() == 2


def test_annual_and_quarter_same_end_do_not_become_ttm(engine):
    data = facts()
    data["facts"]["us-gaap"]["Revenues"]["units"]["USD"] += [
        fact(4, start="2025-10-01", filed="2026-02-20", form="10-Q")
    ]
    assert puller(engine, data).pull_ticker("AAPL")["rows_inserted"] == 1
    item = read_sec_profile(engine, "AAPL")["fields"]["revenue"]
    assert item["period_start"] == "2025-01-01"
    assert item["numeric_value"] == 12.5
    assert "TTM" not in item["label"]


def test_legacy_finviz_and_provenance_missing_rows_never_reappear(engine):
    with engine.begin() as conn:
        for sid in ("finviz.AAPL.pe_ratio", "edgar_fundamentals.AAPL.eps_basic",
                    "sec_filed_fundamentals.AAPL.eps_basic"):
            conn.execute(text("INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status) "
                              "VALUES (:sid, 1, '2026-01-01', 0, '{}', 'SUCCESS')"), {"sid": sid})
    profile = read_sec_profile(engine, "AAPL")
    assert profile["status"] == "unavailable"
    assert profile["fields"] == {}


def test_dad_refresh_is_read_only_no_network_and_no_old_score_inputs(engine, monkeypatch):
    import api.routers.dad as dad
    p = puller(engine, facts())
    p.pull_ticker("AAPL")
    statements = []
    event.listen(engine, "before_cursor_execute", lambda _a, _b, sql, *_rest: statements.append(sql))
    monkeypatch.setattr(dad, "_fetch_finviz_snapshot", lambda *_a: pytest.fail("retired provider invoked"))
    monkeypatch.setattr(dad, "_store_finviz_snapshot", lambda *_a: pytest.fail("GET wrote data"))
    for persist in (False, True):
        profile = dad._get_finviz_profile(engine, "AAPL", refresh=True, persist_refresh=persist)
        assert profile["source"] == "SEC EDGAR/XBRL"
        compact = dad._compact_finviz(profile)
        assert compact["unavailable_fields"]["pe_ratio"] is None
        assert compact["stats"][0]["filed"] == "2026-02-01"
        assert compact["refresh_available"] is False
    assert all(sql.lstrip().upper().startswith("SELECT") for sql in statements)
    contaminated = {**profile, "fields": {"forward_pe": {"parsed": 20}, "roe": {"parsed": 90}}}
    decision = dad._grid_decision_stack(None, {"score": 0}, {}, contaminated, None, {})
    card = next(card for card in decision["cards"] if "SEC EDGAR" in card["source"])
    assert card["state"] == "missing" and card["points"] == 0
    assert any("unavailable from SEC" in blocker for blocker in decision["blockers"])


def test_finviz_entrypoints_refuse_before_network_or_source_reactivation():
    from ingestion.altdata.finviz_scraper import FinvizScraperPuller
    from ingestion.altdata.smart_money import SmartMoneyPuller
    with pytest.raises(RuntimeError, match="retired"):
        FinvizScraperPuller(MagicMock())
    p = SmartMoneyPuller.__new__(SmartMoneyPuller)
    p.engine = MagicMock()
    assert p.pull_finviz_insiders()["status"] == "SKIPPED"
    with pytest.raises(RuntimeError, match="retired"):
        p._fetch_finviz_insiders()
    p.engine.begin.assert_not_called()


def test_real_daily_registration_uses_sec_and_never_constructs_finviz(monkeypatch):
    source = Path("ingestion/scheduler.py").read_text(encoding="utf-8")
    node = next(node for node in ast.parse(source).body
                if isinstance(node, ast.FunctionDef) and node.name == "_get_pullers_for_group")
    class FixturePuller:
        def __init__(self, *args, **kwargs):
            pass
    for module in (n for n in ast.walk(node) if isinstance(n, ast.ImportFrom)):
        monkeypatch.setitem(sys.modules, module.module, SimpleNamespace(**{
            alias.name: FixturePuller for alias in module.names
        }))
    namespace = {"Engine": object, "Any": object, "log": MagicMock()}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "scheduler-registration", "exec"), namespace)
    registered = namespace["_get_pullers_for_group"]("daily", None, {})
    names = [entry[0] for entry in registered]
    assert "Finviz_Fundamentals" not in names
    assert names.count("SEC_EDGAR_Fundamentals") == 1
