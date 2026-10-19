"""Reader fixes in /flows/sectors/{name}/detail: dark pool and congressional.

Reproduced 2026-10-19 (CLAUDE-SLICE2-ACCEPTANCE-TRACE, cases B and C):

* B: ``dark_pool_weekly`` rows dated 2026-03-09 (224 days old) with
  ``short_volume`` NULL gave ``dark_pool_signal = "accumulation"``: the newest
  week had no age bound and NULL was summed as 0, so ratio 0 < 0.40.
* C: ``congressional_trades`` rows with a 2026-11-22 ``disclosure_date`` were
  listed first as "recent" activity (no upper date bound).

Offline: a fake engine serves only the two reads under test and fails every
other statement (each section of the endpoint is individually guarded). The
same helpers run against real tables in tests/test_flows_reader_fixes_pg.py.
"""

from __future__ import annotations

import os
import sys
import types
from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

from api.routers import flows  # noqa: E402
from store.materialization_provenance import WEEKLY_STALE_AFTER_DAYS  # noqa: E402

TODAY = date.today()
TICKERS = ["AAPL", "AMD", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "PLTR", "TSLA"]


class _Engine:
    """Serves dark-pool / congressional reads from fixtures; records all SQL."""

    def __init__(self, dark_pool=None, congressional=None):
        self.dark_pool = dark_pool if dark_pool is not None else []
        self.congressional = congressional if congressional is not None else []
        self.sql: list[str] = []

    def connect(self):
        engine = self
        conn = MagicMock()

        def execute(stmt, params=None):
            sql = str(getattr(stmt, "text", stmt))
            engine.sql.append(sql)
            res = MagicMock()
            if "FROM dark_pool_weekly" in sql and "ticker, short_volume, total_volume" in sql:
                res.fetchall.return_value = engine.dark_pool
                return res
            if "FROM congressional_trades" in sql and "representative" in sql:
                res.fetchall.return_value = engine.congressional
                return res
            raise AssertionError(f"unexpected SQL: {sql[:80]}")

        conn.execute.side_effect = execute
        ctx = MagicMock()
        ctx.__enter__.return_value = conn
        ctx.__exit__.return_value = False
        return ctx


def _dp(report_date, short, total=1000.0, tickers=TICKERS):
    return [(t, short, total, report_date) for t in tickers]


@pytest.fixture()
def detail(monkeypatch):
    """Call get_sector_detail('Technology') against a fixture engine."""
    from analysis.sector_map import SECTOR_MAP

    def off(*_a, **_k):
        raise RuntimeError("disabled in tests")

    monkeypatch.setitem(sys.modules, "ollama.client", types.SimpleNamespace(get_client=off))
    monkeypatch.setitem(sys.modules, "intelligence.trust_scorer", types.SimpleNamespace(TrustScorer=off))
    monkeypatch.setitem(sys.modules, "intelligence.lever_pullers", types.SimpleNamespace(get_lever_pullers=off))
    monkeypatch.setitem(sys.modules, "inference.live", types.SimpleNamespace(LiveInference=off))
    monkeypatch.setattr(flows, "_provenance", lambda tables: {"generated_at": "fixture"})
    flows._sector_narrative_cache.clear()
    flows._sector_connections_cache.clear()
    sector = "Technology" if "Technology" in SECTOR_MAP else next(iter(SECTOR_MAP))

    def call(engine):
        monkeypatch.setattr(flows, "get_db_engine", lambda: engine)
        return flows.get_sector_detail(sector, "t")

    return call


# ── B: dark pool ─────────────────────────────────────────────────────────


def test_null_short_volume_is_not_accumulation(detail):
    """Case B: total volume present, short volume NULL -> ratio 0 -> 'accumulation' on main."""
    engine = _Engine(dark_pool=_dp(TODAY - timedelta(days=14), None))
    m = detail(engine)["sector_metrics"]
    assert m["dark_pool_signal"] is None
    assert m["dark_pool_reason"] == "no_short_volume"
    assert m["dark_pool_as_of"] == (TODAY - timedelta(days=14)).isoformat()


def test_stale_week_is_not_a_signal_even_with_short_volume(detail):
    """Case B: a 224-day-old week (2026-03-09 on the day of the trace) served as current."""
    stale = TODAY - timedelta(days=224)
    m = detail(_Engine(dark_pool=_dp(stale, 200.0)))["sector_metrics"]   # ratio 0.2 would be accumulation
    assert m["dark_pool_signal"] is None
    assert m["dark_pool_reason"] == "stale"
    assert m["dark_pool_as_of"] == stale.isoformat()


@pytest.mark.parametrize("short,expected", [(300.0, "accumulation"), (500.0, "neutral"), (600.0, "distribution")])
def test_fresh_week_with_short_volume_keeps_the_directional_signal(detail, short, expected):
    m = detail(_Engine(dark_pool=_dp(TODAY - timedelta(days=21), short)))["sector_metrics"]
    assert m["dark_pool_signal"] == expected
    assert m["dark_pool_reason"] is None
    assert m["dark_pool_as_of"] == (TODAY - timedelta(days=21)).isoformat()


def test_threshold_boundary_matches_the_provenance_envelope(detail):
    fresh = detail(_Engine(dark_pool=_dp(TODAY - timedelta(days=WEEKLY_STALE_AFTER_DAYS), 300.0)))
    stale = detail(_Engine(dark_pool=_dp(TODAY - timedelta(days=WEEKLY_STALE_AFTER_DAYS + 1), 300.0)))
    assert fresh["sector_metrics"]["dark_pool_signal"] == "accumulation"
    assert stale["sector_metrics"]["dark_pool_signal"] is None


def test_only_rows_with_short_volume_enter_the_ratio(detail):
    rows = _dp(TODAY - timedelta(days=14), 700.0, tickers=["AAPL"]) + _dp(TODAY - timedelta(days=14), None, tickers=["AMD"])
    m = detail(_Engine(dark_pool=rows))["sector_metrics"]
    assert m["dark_pool_signal"] == "distribution"   # AMD's NULL is not a 0 that drags the ratio down


def test_no_rows_is_unavailable_not_neutral(detail):
    m = detail(_Engine(dark_pool=[]))["sector_metrics"]
    assert m["dark_pool_signal"] is None and m["dark_pool_reason"] == "no_rows"


def test_dark_pool_read_failure_is_unavailable_not_neutral(detail):
    class Boom(_Engine):
        def connect(self):
            raise RuntimeError("db down")

    m = detail(Boom())["sector_metrics"]
    assert m["dark_pool_signal"] is None and m["dark_pool_reason"] == "error"


# ── C: congressional ─────────────────────────────────────────────────────


def _cong(d, rep="Rick Larsen", ticker="ADI"):
    return (ticker, d, rep, "BUY", "$15,001 - $50,000")


def test_future_dated_congressional_row_is_not_listed(detail):
    """Case C: ADI / Rick Larsen / 2026-11-22 was the FIRST item of congressional_activity."""
    rows = [
        _cong(TODAY + timedelta(days=34)),
        _cong(TODAY - timedelta(days=30), "Nancy Example", "AAPL"),
        _cong(TODAY - timedelta(days=45), "Other Member", "GOOGL"),
    ]
    out = detail(_Engine(congressional=rows))
    acts = out["sector_metrics"]["congressional_activity"]
    assert [a["representative"] for a in acts] == ["Nancy Example", "Other Member"]
    assert all(date.fromisoformat(a["date"]) <= TODAY for a in acts)
    # Dates are part of every row so the UI can show them.
    assert acts[0]["date"] == (TODAY - timedelta(days=30)).isoformat()


def test_congressional_queries_are_bounded_to_today_in_sql(detail):
    engine = _Engine(congressional=[])
    detail(engine)
    cong = [s for s in engine.sql if "FROM congressional_trades" in s]
    assert cong and all("disclosure_date <= CURRENT_DATE" in s for s in cong)


def test_connection_graph_queries_exclude_future_rows_and_null_short_volume():
    flows._sector_connections_cache.clear()
    engine = MagicMock()
    seen: list[str] = []
    conn = MagicMock()

    def execute(stmt, params=None):
        sql = str(getattr(stmt, "text", stmt))
        seen.append(sql)
        res = MagicMock()
        res.fetchone.return_value = ("t",)
        res.fetchall.return_value = []
        return res

    conn.execute.side_effect = execute
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    ctx.__exit__.return_value = False
    engine.connect.return_value = ctx
    flows._build_sector_connections(engine, "Technology", ["AAPL", "AMD"], {})
    cong = [s for s in seen if "FROM congressional_trades" in s]
    dark = [s for s in seen if "FROM dark_pool_weekly" in s]
    assert cong and all("disclosure_date <= CURRENT_DATE" in s for s in cong)
    assert dark and all("short_volume IS NOT NULL" in s for s in dark)


def test_response_keeps_existing_sector_metrics_fields(detail):
    m = detail(_Engine())["sector_metrics"]
    assert {"relative_strength_1m", "etf_flow_5d", "dark_pool_signal",
            "congressional_activity", "insider_activity"} <= set(m)
