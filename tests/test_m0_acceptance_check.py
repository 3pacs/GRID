"""Offline tests for scripts/m0_acceptance_check.py (held, read-only M0 acceptance check).

No database, no network: the script is exercised through --dry-run, the flag
gate, the pure SQL builders, a recording fake engine and canned query results.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import m0_acceptance_check as m0

TARGET = date(2026, 10, 19)


def _boom() -> None:
    raise AssertionError("database engine must not be opened")


# ---------------------------------------------------------------- flag gate

def test_refuses_without_authorized_flag(capsys):
    rc = m0.main(["--date", "2026-10-19"], engine_factory=_boom)
    assert rc == 2
    err = capsys.readouterr().err
    assert "REFUSED" in err and "--i-am-authorized" in err


def test_dry_run_never_opens_engine_even_when_authorized(capsys):
    rc = m0.main(["--date", "2026-10-19", "--dry-run", "--i-am-authorized"], engine_factory=_boom)
    assert rc == 0
    assert "DRY RUN" in capsys.readouterr().out


def test_dry_run_needs_no_authorization(capsys):
    assert m0.main(["--date", "2026-10-19", "--dry-run"], engine_factory=_boom) == 0


def test_bad_date_and_bounds_rejected(capsys):
    assert m0.main(["--date", "19-10-2026", "--dry-run"]) == 2
    assert m0.main(["--date", "2026-10-19", "--dry-run", "--lookback-days", "0"]) == 2
    assert m0.main(["--date", "2026-10-19", "--dry-run", "--tickers", "SPY;DROP"]) == 2


# ---------------------------------------------------------------- dry-run SQL

@pytest.fixture()
def dry_text(capsys) -> str:
    assert m0.main(["--date", "2026-10-19", "--dry-run"]) == 0
    return capsys.readouterr().out


def test_dry_run_prints_read_only_wrapper_for_every_query(dry_text):
    n = len(m0.build_queries(TARGET))
    assert dry_text.count("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;") == n
    assert dry_text.count("ROLLBACK;") == n
    assert dry_text.count("SET LOCAL statement_timeout") == n
    for secs in re.findall(r"statement_timeout = '(\d+)s'", dry_text):
        assert 1 <= int(secs) <= 20


def test_dry_run_contains_the_index_friendly_bounds(dry_text):
    assert "obs_date = CAST(:od AS date)" in dry_text            # raw_series vintages (idx_raw_series_lookup)
    assert "pull_status = 'SUCCESS'" in dry_text
    assert "started_at >= CAST(:od AS date)" in dry_text          # pull_log (idx_pull_log_puller)
    assert "started_at <  CAST(:od AS date) + 1" in dry_text
    assert "obs_date >= CAST(:since AS date)" in dry_text         # Q7 lookback
    assert "WHERE trade_date >= CAST(:floor AS date)" in dry_text  # flow tables windowed
    assert "WHERE report_date >= CAST(:floor AS date)" in dry_text
    assert "rs.obs_date >= CAST(:floor AS date)" in dry_text       # resolved_series
    assert "WHERE date = CAST(:od AS date)" in dry_text            # market_diary
    assert "WHERE briefing_date = CAST(:od AS date)" in dry_text   # market_briefings
    assert '"floor": "2026-08-20"' in dry_text and '"od": "2026-10-19"' in dry_text


def test_raw_series_queries_carry_planner_hints_and_others_do_not():
    for q in m0.build_queries(TARGET):
        pre = m0.transaction_preamble(q)
        hinted = any("enable_seqscan = 'off'" in s for s in pre)
        assert hinted == q.raw_series
        assert ("raw_series" in q.sql) == q.raw_series


def test_every_statement_is_bounded_and_read_only():
    for q in m0.build_queries(TARGET):
        m0.assert_read_only_sql(q.sql)
        assert "LIMIT" in q.sql or q.qid.startswith(("Q1a", "H2:")) or "max(" in q.sql, q.qid
        assert q.timeout_s <= 20


def test_q1_q2_q7_match_the_freshness_request_statements():
    assert "string_to_array(:tickers, ',')" in m0.SQL_Q2_CENSUS
    assert "(array_agg(raw_payload->>'capture_state'  ORDER BY pull_timestamp DESC))[1] AS last_state" in m0.SQL_Q2_CENSUS
    assert "ORDER BY t.ticker LIMIT 80" in m0.SQL_Q2_CENSUS
    assert "s(sid) AS (VALUES (2), (524), (1995))" in m0.SQL_Q7_SOURCE_MIX
    assert "ORDER BY t.ticker, s.sid LIMIT 220" in m0.SQL_Q7_SOURCE_MIX
    assert "WHERE id IN (1, 2, 182, 184, 524, 1995) OR name IN ('TWELVEDATA', 'yfinance')" in m0.SQL_Q1_SOURCES
    assert "transaction_read_only" in m0.SQL_Q1_CLOCK


# ---------------------------------------------------------------- SQL guard

@pytest.mark.parametrize("bad", [
    "INSERT INTO pull_log VALUES (1)",
    "UPDATE raw_series SET value = 0",
    "DELETE FROM raw_series",
    "SELECT 1; DROP TABLE x",
    "WITH d AS (DELETE FROM x RETURNING *) SELECT * FROM d",
    "SELECT * FROM x FOR UPDATE; SELECT 1",
    "CREATE TABLE t AS SELECT 1",
    "REFRESH MATERIALIZED VIEW v",
    "COPY x TO '/tmp/y'",
])
def test_sql_guard_rejects_writes(bad):
    with pytest.raises(ValueError):
        m0.assert_read_only_sql(bad)


def test_sql_guard_accepts_select_and_with_and_ignores_created_at_column():
    m0.assert_read_only_sql("SELECT max(created_at) FROM t WHERE x = 1")
    m0.assert_read_only_sql("WITH a AS (SELECT 1) SELECT * FROM a;")


def test_timeout_over_cap_is_rejected():
    with pytest.raises(ValueError):
        m0.QuerySpec("x", "H1", "p", "SELECT 1", {}, timeout_s=21)


# ---------------------------------------------------------------- runner

class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def keys(self):
        return list(self._rows[0].keys()) if self._rows else ["c"]

    def fetchall(self):
        return [tuple(r.values()) for r in self._rows]


class _FakeConn:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execution_options(self, **kw):
        self.log.append(("options", kw))
        return self

    def execute(self, stmt, params=None):
        self.log.append(("sql", str(stmt), params))
        return _FakeResult([{"n": 1}])

    def rollback(self):
        self.log.append(("rollback",))


class _FakeEngine:
    def __init__(self):
        self.log: list = []

    def connect(self):
        return _FakeConn(self.log)


def test_run_query_sets_read_only_first_and_rolls_back():
    eng = _FakeEngine()
    spec = m0.build_queries(TARGET)[3]  # H1b raw_series
    out = m0.run_query(eng, spec)
    assert out["error"] is None and out["rows"] == [{"n": 1}]
    assert eng.log[0] == ("options", {"isolation_level": "REPEATABLE READ"})
    assert eng.log[1][1] == "SET TRANSACTION READ ONLY"
    assert eng.log[2][1].startswith("SELECT set_config(") and eng.log[2][2] == {"name": "statement_timeout", "value": "15s"}
    assert any(e[0] == "sql" and e[2] == {"name": "enable_seqscan", "value": "off"} for e in eng.log)
    assert eng.log[-1] == ("rollback",)
    assert sum(1 for e in eng.log if e[0] == "sql" and re.match(r"\s*(insert|update|delete)", e[1], re.IGNORECASE)) == 0


def test_run_query_turns_exceptions_into_receipt_errors():
    class Bad(_FakeEngine):
        def connect(self):
            raise RuntimeError("canceling statement due to statement timeout")

    out = m0.run_query(Bad(), m0.build_queries(TARGET)[0])
    assert out["rows"] is None and "statement timeout" in out["error"]


# ---------------------------------------------------------------- receipt

def _canned() -> dict[str, dict]:
    def r(qid, rows):
        return {"qid": qid, "purpose": qid, "timestamp_kind": "t", "rows": rows, "error": None}

    res = {
        "Q1a": r("Q1a", [{"db_now_utc": "2026-10-19T22:20:00", "ro": "on", "st": "5s"}]),
        "Q1b": r("Q1b", [{"id": 2, "name": "yfinance"}]),
        "H1a": r("H1a", [{"puller_name": "YFinance_Daily", "status": "SUCCESS", "rows_inserted": 330}]),
        "H1b": r("H1b", [{"capture_state": "INCOMPLETE", "pull_timestamp": "2026-10-19T22:04:00"}]),
        "Q2": r("Q2", [{"ticker": "^GSPC"}]),
        "Q7": r("Q7", [{"ticker": "^GSPC", "source_id": 2}]),
        "H2:resolved": r("H2:resolved", [
            {"name": "spy_full", "max_vintage_date": "2026-10-19", "max_obs_date": "2026-10-16", "rows_in_window": 10},
            {"name": "xlk_full", "max_vintage_date": "2026-10-19", "max_obs_date": "2026-10-16", "rows_in_window": 10},
        ]),
        "H3:diary": r("H3:diary", [{
            "date": "2026-10-19", "generated_at": "2026-10-19T22:34:00", "narrative_model": "qwen",
            "narrative_fallback": False, "prices_enabled": "true", "price_basis": "raw_close", "price_source": "yfinance",
            "indices": json.dumps({"S&P 500": {"status": "no close for date"},
                                    "VIX": {"price_basis": "raw_close", "source": "yfinance", "close": 17.0}}),
        }]),
        "H4:briefings": r("H4:briefings", [{"briefing_type": "daily", "n": 1}]),
    }
    for table, col in m0.FLOW_TABLES:
        res[f"H2:{table}"] = r(f"H2:{table}", [{"max_date": "2026-10-18", "materialized_at": "2026-10-18T05:00:00", "row_count": 5}])
    return res


def test_evaluate_pass_and_schema(tmp_path):
    cap = tmp_path / "capture"
    cap.mkdir()
    (cap / "a.txt").write_text("x")
    hops = m0.evaluate(_canned(), TARGET, max_age_days=5, capture_dir=str(cap))
    assert set(hops) == {"H1", "H2", "H3", "H4"}
    assert all(h["status"] == "PASS" for h in hops.values()), {k: v["checks"] for k, v in hops.items()}
    assert hops["H3"]["evidence"]["indices_no_close_for_date"] == ["S&P 500"]
    assert hops["H4"]["evidence"]["capture_folder"]["entry_count"] == 1
    for h in hops.values():
        assert h["proves"] and h["does_not_prove"]


def test_evaluate_marks_stale_flow_table_fail_and_missing_capture_unknown():
    res = _canned()
    res["H2:etf_flows"]["rows"] = [{"max_date": None, "materialized_at": None, "row_count": 0}]
    hops = m0.evaluate(res, TARGET, max_age_days=5, capture_dir=None)
    assert hops["H2"]["status"] == "FAIL" and hops["H2"]["checks"]["etf_flows_fresh"] is False
    assert hops["H4"]["checks"]["capture_folder_present_and_nonempty"] is None
    assert hops["H4"]["status"] == "UNKNOWN"


def test_query_error_becomes_unknown_not_pass():
    res = _canned()
    res["H3:diary"]["rows"], res["H3:diary"]["error"] = None, "TimeoutError: x"
    hops = m0.evaluate(res, TARGET, max_age_days=5, capture_dir=None)
    assert hops["H3"]["status"] == "UNKNOWN"


def test_main_writes_dated_receipt_and_refuses_overwrite(tmp_path, monkeypatch, capsys):
    canned = _canned()
    monkeypatch.setattr(m0, "run_query", lambda eng, spec: canned.get(spec.qid) or {
        "qid": spec.qid, "purpose": spec.purpose, "timestamp_kind": spec.timestamp_kind, "rows": [], "error": None})
    out = tmp_path / "receipt.json"
    argv = ["--date", "2026-10-19", "--i-am-authorized", "--out", str(out), "--capture-dir", str(tmp_path)]
    assert m0.main(argv, engine_factory=lambda: object()) == 0
    rec = json.loads(out.read_text())
    assert rec["schema"] == m0.SCHEMA and rec["target_date"] == "2026-10-19"
    assert rec["read_only"] is True and rec["authorized_flag"] is True
    assert set(rec["hops"]) == {"H1", "H2", "H3", "H4"}
    assert rec["overall"] in {"PASS", "FAIL", "UNKNOWN"}
    assert len(rec["queries"]) == len(m0.build_queries(TARGET))
    assert "authorized" not in rec["arguments"]
    # second run must not overwrite
    assert m0.main(argv, engine_factory=lambda: object()) == 2
    assert "refusing to overwrite" in capsys.readouterr().err


def test_default_receipt_name_is_dated():
    assert re.fullmatch(r"m0-acceptance-receipt-2026-10-19-\d{8}T\d{6}Z\.json", m0.default_out_path(TARGET))


def test_importing_the_script_does_not_import_db_or_config():
    assert "db" not in m0.__dict__ and "config" not in m0.__dict__
