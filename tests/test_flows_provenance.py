"""Provenance/unavailable envelope on the flows API's materialized-table reads.

Offline (no database): a fake engine dispatches the helper's fixed SQL, so the
thresholds, the reason tokens (empty / stale / timeout / missing_table / error)
and the "existing response fields are unchanged" contract are pinned without
PostgreSQL. Real-PostgreSQL behaviour (a genuine statement timeout, real
tables) is in tests/test_flows_provenance_pg.py.

Provenance describes materialization state, not data truth:
docs/flows_provenance_contract.md.
"""

from __future__ import annotations

import asyncio
import copy
import os
import sys
import types
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import OperationalError

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

from api.routers import flows  # noqa: E402
from store import materialization_provenance as mp  # noqa: E402
from utils.ttl_cache import TTLCache  # noqa: E402

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
TODAY = NOW.date()
CREATED = datetime(2026, 10, 18, 3, 0, tzinfo=timezone.utc)


class _PgError(Exception):
    def __init__(self, pgcode: str, msg: str) -> None:
        super().__init__(msg)
        self.pgcode = pgcode


def _op_error(pgcode: str, msg: str) -> OperationalError:
    return OperationalError("SELECT ...", {}, _PgError(pgcode, msg))


def _row(res):
    m = MagicMock()
    m.fetchone.return_value = res
    m.scalar.return_value = res[0] if res else None
    return m


class FakeEngine:
    """tables: {name: (row_count, max_obs_date, materialized_at)}; others are missing."""

    def __init__(self, tables=None, raise_on=None, source_ts=None, fail_all=None):
        self.tables = tables or {}
        self.raise_on = raise_on or {}        # table -> exception raised by its main SELECT
        self.source_ts = source_ts
        self.fail_all = fail_all              # exception raised by every execute
        self.statements: list[str] = []

    def connect(self):
        engine = self
        conn = MagicMock()

        def execute(stmt, params=None):
            sql = str(getattr(stmt, "text", stmt))
            engine.statements.append(sql)
            if engine.fail_all:
                raise engine.fail_all
            if sql.startswith("SET LOCAL"):
                return MagicMock()
            if "to_regclass" in sql:
                name = params["t"]
                return _row((name,) if name in engine.tables or name in engine.raise_on else (None,))
            for name, exc in engine.raise_on.items():
                if f"FROM {name}" in sql:
                    raise exc
            if "max(created_at) FROM signal_sources WHERE source_type = :source_type" in sql \
                    or "FROM raw_series" in sql:
                return _row((engine.source_ts,))
            for name, vals in engine.tables.items():
                if f"FROM {name}" in sql:
                    return _row(vals)
            raise AssertionError(f"unexpected SQL: {sql}")

        conn.execute.side_effect = execute
        ctx = MagicMock()
        ctx.__enter__.return_value = conn
        ctx.__exit__.return_value = False
        return ctx


@pytest.fixture(autouse=True)
def _fresh_cache():
    mp.clear_cache()
    yield
    mp.clear_cache()


def _prov(engine, table):
    return mp.table_provenance(engine, table, now=NOW)


# ── helper: availability rules ───────────────────────────────────────────


def test_empty_table_is_unavailable():
    p = _prov(FakeEngine({"insider_trades": (0, None, None)}), "insider_trades")
    assert p["unavailable"] is True and p["reason"] == "empty"
    assert p["row_count"] == 0 and p["max_obs_date"] is None


def test_fresh_table_is_available():
    engine = FakeEngine(
        {"insider_trades": (120, TODAY - timedelta(days=1), CREATED)},
        source_ts=datetime(2026, 10, 19, 4, 0, tzinfo=timezone.utc),
    )
    p = _prov(engine, "insider_trades")
    assert p["unavailable"] is False and p["reason"] is None
    assert p["table"] == "insider_trades" and p["row_count"] == 120
    assert p["max_obs_date"] == (TODAY - timedelta(days=1)).isoformat()
    assert p["materialized_at"] == CREATED.isoformat()
    assert p["source_last_ingested"] == "2026-10-19T04:00:00+00:00"
    assert p["age_days"] == 1 and p["stale_after_days"] == 3


@pytest.mark.parametrize("age,unavailable", [(0, False), (3, False), (4, True), (30, True)])
def test_daily_threshold_is_three_calendar_days(age, unavailable):
    engine = FakeEngine({"etf_flows": (50, TODAY - timedelta(days=age), CREATED)})
    p = _prov(engine, "etf_flows")
    assert p["unavailable"] is unavailable
    assert p["age_days"] == age
    assert p["reason"] == ("stale" if unavailable else None)


@pytest.mark.parametrize("age,unavailable", [
    (14, False),   # week published this morning, minimum healthy age
    (21, False),   # typical healthy age: week start + ~2 weeks FINRA lag
    (28, False),   # just before the next weekly publication
    (35, False),   # threshold (a week of slack)
    (36, True),
])
def test_weekly_threshold_covers_the_finra_publication_lag(age, unavailable):
    engine = FakeEngine({"dark_pool_weekly": (50, TODAY - timedelta(days=age), CREATED)})
    p = _prov(engine, "dark_pool_weekly")
    assert p["stale_after_days"] == 35
    assert p["unavailable"] is unavailable
    assert p["reason"] == ("stale" if unavailable else None)


def test_fresh_weekly_row_reads_available():
    """A healthy dark-pool table (newest week started 3 weeks ago) is available."""
    monday = TODAY - timedelta(days=TODAY.weekday())
    newest_week = monday - timedelta(weeks=3)
    p = _prov(FakeEngine({"dark_pool_weekly": (900, newest_week, CREATED)}), "dark_pool_weekly")
    assert p["unavailable"] is False and p["reason"] is None
    assert 14 <= p["age_days"] <= 28


def test_stale_reports_age_and_threshold():
    engine = FakeEngine({"junction_point_readings": (400, TODAY - timedelta(days=6), CREATED)})
    p = _prov(engine, "junction_point_readings")
    assert (p["unavailable"], p["reason"], p["age_days"], p["stale_after_days"]) == (True, "stale", 6, 3)
    # Staleness never hides what is there.
    assert p["row_count"] == 400 and p["max_obs_date"] == (TODAY - timedelta(days=6)).isoformat()


def test_timeout_is_unavailable_timeout():
    exc = _op_error("57014", "canceling statement due to statement timeout")
    p = _prov(FakeEngine({"etf_flows": (1, TODAY, CREATED)}, raise_on={"etf_flows": exc}), "etf_flows")
    assert p["unavailable"] is True and p["reason"] == "timeout"


def test_timeout_detected_from_message_without_pgcode():
    exc = RuntimeError("canceling statement due to statement timeout")
    p = _prov(FakeEngine(fail_all=exc), "etf_flows")
    assert p["reason"] == "timeout"


def test_missing_table_is_unavailable():
    p = _prov(FakeEngine({}), "insider_trades")
    assert p["unavailable"] is True and p["reason"] == "missing_table"


def test_undefined_table_error_is_missing_table():
    exc = _op_error("42P01", 'relation "etf_flows" does not exist')
    p = _prov(FakeEngine({"etf_flows": (1, TODAY, CREATED)}, raise_on={"etf_flows": exc}), "etf_flows")
    assert p["reason"] == "missing_table"


def test_other_error_is_contained():
    p = _prov(FakeEngine(fail_all=ValueError("boom")), "etf_flows")
    assert p["unavailable"] is True and p["reason"] == "error:ValueError"


def test_unknown_table_is_unavailable_without_sql():
    engine = FakeEngine({})
    p = _prov(engine, "users; DROP TABLE x")
    assert p["unavailable"] is True and p["reason"] == "unknown_table"
    assert engine.statements == []


def test_source_lookup_failure_never_changes_availability():
    engine = FakeEngine({"insider_trades": (5, TODAY, CREATED)})
    original = engine.connect

    def connect():
        ctx = original()
        conn = ctx.__enter__.return_value
        inner = conn.execute.side_effect

        def execute(stmt, params=None):
            if "source_type" in str(stmt):
                raise RuntimeError("source lookup down")
            return inner(stmt, params)

        conn.execute.side_effect = execute
        return ctx

    engine.connect = connect
    p = _prov(engine, "insider_trades")
    assert p["unavailable"] is False and p["source_last_ingested"] is None


def test_statements_are_bounded_and_read_only():
    engine = FakeEngine({t: (1, TODAY, CREATED) for t in mp.TABLE_SPECS})
    for table in mp.TABLE_SPECS:
        mp.table_provenance(engine, table, now=NOW, use_cache=False)
    assert engine.statements
    for sql in engine.statements:
        head = sql.lstrip().split()[0].upper()
        assert head in {"SELECT", "SET"}, sql
        if head == "SET":
            assert sql == "SET LOCAL statement_timeout = 5000"
    assert mp.STATEMENT_TIMEOUT_MS <= 5000
    # raw_series is only ever read with series_id and obs_date bounds.
    for sql in engine.statements:
        if "raw_series" in sql:
            assert "series_id = ANY(:series_ids)" in sql and "obs_date >= " in sql
    # Every probe of the big tables carries a date bound.
    for name in ("signal_sources", "options_daily_signals"):
        assert "signal_date >= CURRENT_DATE" in mp.TABLE_SPECS[name].sql


def test_results_are_cached_briefly():
    engine = FakeEngine({"etf_flows": (1, TODAY, CREATED)})
    mp.table_provenance(engine, "etf_flows", now=NOW)
    n = len(engine.statements)
    mp.table_provenance(engine, "etf_flows", now=NOW)
    assert len(engine.statements) == n


def test_endpoint_provenance_shape():
    engine = FakeEngine({"etf_flows": (1, TODAY, CREATED)})
    out = mp.endpoint_provenance(engine, ["etf_flows", "insider_trades"], now=NOW)
    assert set(out) == {"etf_flows", "insider_trades", "generated_at"}
    assert out["generated_at"] == NOW.isoformat()
    assert out["etf_flows"]["unavailable"] is False
    assert out["insider_trades"]["reason"] == "missing_table"


# ── endpoints: the key exists, existing fields are unchanged ─────────────


def _good_engine():
    # Endpoints use the real clock, so "fresh" is relative to it (never future-dated).
    return FakeEngine({t: _good_row(t) for t in mp.TABLE_SPECS})


def _good_row(table):
    row = (10, date.today(), CREATED)
    return row + (0,) if mp.TABLE_SPECS[table].reports_future else row


@pytest.fixture()
def api_engine(monkeypatch):
    engine = _good_engine()
    monkeypatch.setattr(flows, "get_db_engine", lambda: engine)
    return engine


def _assert_envelope(result, tables):
    prov = result["provenance"]
    assert set(prov) == set(tables) | {"generated_at"}
    for t in tables:
        assert prov[t]["table"] == t and prov[t]["unavailable"] is False


def test_sectors_cached_payload_unchanged_plus_provenance(api_engine, monkeypatch):
    monkeypatch.setattr(flows, "_ensure_sector_warm_thread", lambda: None)
    monkeypatch.setattr(flows, "_sector_cache", TTLCache(ttl=60, max_size=2))
    payload = {"sectors": {"Technology": {"etf": "XLK"}}, "computed_at": "2026-10-19T00:00:00+00:00"}
    before = copy.deepcopy(payload)
    flows._sector_cache.set(flows._SECTOR_CACHE_KEY, payload)
    out = flows.get_sectors("t")
    assert {k: v for k, v in out.items() if k != "provenance"} == before
    _assert_envelope(out, flows._PROV_SECTORS)
    assert payload == before  # cached object never mutated


def test_sectors_cold_process_keeps_unavailable_flags_and_adds_provenance(api_engine, monkeypatch):
    monkeypatch.setattr(flows, "_ensure_sector_warm_thread", lambda: None)
    monkeypatch.setattr(flows, "_sector_cache", TTLCache(ttl=60, max_size=2))
    monkeypatch.setattr(flows, "_sector_stale_cache", TTLCache(ttl=60, max_size=2))
    out = flows.get_sectors("t")
    assert out["sectors"] == {} and out["stale"] is True and out["unavailable"] is True
    assert "provenance" in out


def test_sankey_cached_payload_unchanged_plus_provenance(api_engine, monkeypatch):
    monkeypatch.setattr(flows, "_sankey_cache", TTLCache(ttl=60, max_size=2))
    payload = {"nodes": [{"id": 0}], "links": [], "snapshots": []}
    before = copy.deepcopy(payload)
    flows._sankey_cache.set("today", payload)
    out = flows.get_sankey_data(None, "t")
    assert {k: v for k, v in out.items() if k != "provenance"} == before
    _assert_envelope(out, flows._PROV_SANKEY)
    assert payload == before


def test_money_map_cached_payload_unchanged_plus_provenance(api_engine, monkeypatch):
    monkeypatch.setattr(flows, "_money_map_cache", TTLCache(ttl=60, max_size=1))
    payload = {"layers": [{"id": "fed"}], "source": "fixture"}
    before = copy.deepcopy(payload)
    flows._money_map_cache.set("money_map", payload)
    out = flows.get_money_map("t")
    assert {k: v for k, v in out.items() if k != "provenance"} == before
    _assert_envelope(out, flows._PROV_MONEY_MAP)
    assert payload == before


def test_money_map_fresh_build_returns_provenance_and_caches_raw(api_engine, monkeypatch):
    monkeypatch.setattr(flows, "_money_map_cache", TTLCache(ttl=60, max_size=1))
    monkeypatch.setitem(sys.modules, "analysis.money_flow",
                        types.SimpleNamespace(build_flow_map=lambda engine: {"layers": []}))
    out = flows.get_money_map("t")
    assert out["layers"] == [] and "provenance" in out
    assert "provenance" not in flows._money_map_cache.get("money_map")


def test_company_drill_unchanged_plus_provenance(api_engine, monkeypatch):
    drill = {"ticker": "AAPL", "actors": [], "insider_summary": None}
    monkeypatch.setitem(sys.modules, "analysis.money_flow",
                        types.SimpleNamespace(get_company_drill=lambda engine, ticker: dict(drill)))
    out = asyncio.run(flows.get_company_drill("AAPL", "t"))
    assert {k: v for k, v in out.items() if k != "provenance"} == drill
    _assert_envelope(out, flows._PROV_COMPANY)


def test_junction_points_unchanged_plus_provenance(api_engine, monkeypatch):
    monkeypatch.setattr(flows, "_get_flow_map", lambda engine, as_of=None: types.SimpleNamespace(layers=[]))
    out = flows.get_junction_points("t")
    assert out["junction_points"] == [] and out["layer_summaries"] == []
    assert out["total_layers"] == 0 and out["total_junction_points"] == 0
    _assert_envelope(out, flows._PROV_JUNCTION_POINTS)


def test_junction_points_stale_table_is_flagged_not_hidden(monkeypatch):
    engine = FakeEngine({"junction_point_readings": (400, date.today() - timedelta(days=6), CREATED)})
    monkeypatch.setattr(flows, "get_db_engine", lambda: engine)
    monkeypatch.setattr(flows, "_get_flow_map", lambda engine, as_of=None: types.SimpleNamespace(layers=[]))
    # The endpoint uses the real clock, so the fixture date is relative to it.
    out = flows.get_junction_points("t")
    p = out["provenance"]["junction_point_readings"]
    assert p["unavailable"] is True and p["reason"] == "stale"


def test_sector_detail_adds_provenance_without_changing_fields(api_engine, monkeypatch):
    """Every section of the detail view is individually guarded; with all its
    reads failing it still returns its normal shape plus the envelope."""
    from analysis.sector_map import SECTOR_MAP

    def no_llm():
        raise RuntimeError("LLM disabled in tests")

    def off(*_a, **_k):
        raise RuntimeError("disabled in tests")

    # Keep the heavy optional sections (and their import cost) out of the test.
    monkeypatch.setitem(sys.modules, "ollama.client", types.SimpleNamespace(get_client=no_llm))
    monkeypatch.setitem(sys.modules, "intelligence.trust_scorer", types.SimpleNamespace(TrustScorer=off))
    monkeypatch.setitem(sys.modules, "intelligence.lever_pullers", types.SimpleNamespace(get_lever_pullers=off))
    monkeypatch.setitem(sys.modules, "inference.live", types.SimpleNamespace(LiveInference=off))
    sector = next(iter(SECTOR_MAP))
    out = flows.get_sector_detail(sector, "t")
    for key in ("sector", "etf", "price", "change_1m", "subsectors", "etf_options",
                "sector_metrics", "intelligence", "connections"):
        assert key in out
    assert set(out["sector_metrics"]) == {
        "relative_strength_1m", "etf_flow_5d", "dark_pool_signal", "dark_pool_as_of",
        "dark_pool_reason", "congressional_activity", "insider_activity",
    }
    _assert_envelope(out, flows._PROV_SECTOR_DETAIL)


def test_provenance_failure_never_breaks_an_endpoint(monkeypatch):
    def boom():
        raise RuntimeError("no engine")

    monkeypatch.setattr(flows, "get_db_engine", boom)
    monkeypatch.setattr(flows, "_money_map_cache", TTLCache(ttl=60, max_size=1))
    flows._money_map_cache.set("money_map", {"layers": []})
    out = flows.get_money_map("t")
    assert out["layers"] == []
    for t in flows._PROV_MONEY_MAP:
        assert out["provenance"][t]["unavailable"] is True
        assert out["provenance"][t]["reason"] == "error:RuntimeError"
    assert "generated_at" in out["provenance"]


def test_sector_detail_unknown_sector_still_404(api_engine):
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as err:
        flows.get_sector_detail("No Such Sector", "t")
    assert err.value.status_code == 404


# ── future-dated disclosure dates ────────────────────────────────────────


def test_congressional_future_rows_are_counted_not_treated_as_fresh():
    """The newest observation is the newest row dated on/before today; future-dated
    rows are only counted."""
    engine = FakeEngine({"congressional_trades": (300, TODAY - timedelta(days=1), CREATED, 41)})
    p = _prov(engine, "congressional_trades")
    assert p["unavailable"] is False and p["reason"] is None
    assert p["future_dated_rows"] == 41 and p["row_count"] == 300
    assert date.fromisoformat(p["max_obs_date"]) <= TODAY
    assert p["age_days"] == 1


def test_future_dated_rows_do_not_hide_an_old_table():
    engine = FakeEngine({"congressional_trades": (300, TODAY - timedelta(days=12), CREATED, 41)})
    p = _prov(engine, "congressional_trades")
    assert (p["unavailable"], p["reason"], p["age_days"]) == (True, "stale", 12)
    assert p["future_dated_rows"] == 41


def test_only_future_dated_rows_is_unavailable_not_fresh():
    engine = FakeEngine({"congressional_trades": (41, None, CREATED, 41)})
    p = _prov(engine, "congressional_trades")
    assert (p["unavailable"], p["reason"]) == (True, "only_future_dated")
    assert p["max_obs_date"] is None and p["future_dated_rows"] == 41 and p["row_count"] == 41


def test_congressional_no_future_rows_reports_zero():
    engine = FakeEngine({"congressional_trades": (5, TODAY, CREATED, 0)})
    assert _prov(engine, "congressional_trades")["future_dated_rows"] == 0


def test_congressional_sql_filters_future_rows_in_the_database():
    sql = mp.TABLE_SPECS["congressional_trades"].sql
    assert "max(disclosure_date) FILTER (WHERE disclosure_date <= CURRENT_DATE)" in sql
    assert "count(*) FILTER (WHERE disclosure_date > CURRENT_DATE)" in sql
    assert mp.TABLE_SPECS["congressional_trades"].reports_future is True


@pytest.mark.parametrize("days_ahead", [1, 41])
def test_as_of_never_exceeds_today_for_any_table(days_ahead):
    """A max date in the future (here on a table with no future handling) is
    flagged, clamped to today, and never reported as fresh."""
    engine = FakeEngine({"etf_flows": (50, TODAY + timedelta(days=days_ahead), CREATED)})
    p = _prov(engine, "etf_flows")
    assert (p["unavailable"], p["reason"]) == (True, "future_dated")
    assert date.fromisoformat(p["max_obs_date"]) <= TODAY and p["age_days"] == 0


def test_future_dated_rows_field_is_null_where_not_computed():
    p = _prov(FakeEngine({"etf_flows": (1, TODAY, CREATED)}), "etf_flows")
    assert p["future_dated_rows"] is None
