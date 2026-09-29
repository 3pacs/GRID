"""Current-main #733 callers and #742 PG expectation; SQLite/mocks only."""
from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from ingestion import scheduler, smart_scheduler as ss, tiingo_pull as tp
from scripts import hermes_fixers, hermes_operator
from tests import test_scheduler_pull_logging as group_tests
from tests import test_smart_scheduler_honest_success as honest
from tests import test_smart_scheduler_honest_success_pg as pg_tests
from tests.test_tiingo_prices_worker import _FakeIncremental, _SESSION

_TIINGO_ENTRY = deepcopy(next(p for p in ss.PULLER_REGISTRY if p["name"] == "tiingo"))
_TIINGO_REPAIR = deepcopy(hermes_operator._SOURCE_REGISTRY["tiingo"])
sched = honest.sched


def _envelope(*, current=0, succeeded=1, no_data=0, failed=1, unattempted=0, rows=6):
    return {
        "status": "SUCCESS", "tickers": current + succeeded + no_data + failed + unattempted,
        "current": current, "fetched": succeeded + no_data + failed,
        "succeeded": succeeded, "no_data": no_data, "failed": failed,
        "unattempted": unattempted, "rows_inserted": rows,
        "failed_tickers": [f"BAD{i}" for i in range(failed)],
    }


@pytest.mark.parametrize("out,expected,rows", [
    (_envelope(), "PARTIAL", 6),
    (_envelope(succeeded=0, rows=0), "FAILED", 0),
    (_envelope(current=1, succeeded=0, rows=0), "PARTIAL", 0),
    (_envelope(no_data=1, succeeded=0, rows=0), "PARTIAL", 0),
    (_envelope(failed=0), "SUCCESS", 6),
    (_envelope(current=2, succeeded=0, failed=0, rows=0), "NO_NEW_DATA", 0),
    (_envelope(no_data=2, succeeded=0, failed=0, rows=0), "NO_NEW_DATA", 0),
    (_envelope(failed=0, unattempted=1), "PARTIAL", 6),
    (_envelope(succeeded=0, failed=0, unattempted=2, rows=0), "PARTIAL", 0),
    ({"status": "SUCCESS", "rows_inserted": 6, "failed_tickers": ["ZZZ"]}, "PARTIAL", 6),
    ({"status": "SUCCESS", "rows_inserted": 6, "failed": 1}, "PARTIAL", 6),
    ({"status": "SUCCESS", "rows_inserted": 6, "unattempted": 1}, "PARTIAL", 6),
    ({**_envelope(failed=0), "tickers": 2}, "PARTIAL", 6),
    ({**_envelope(failed=0), "fetched": 2}, "FAILED", 6),
    ({**_envelope(), "failed": None}, "FAILED", 6),
    ({**_envelope(), "failed": True}, "FAILED", 6),
    ({**_envelope(), "failed": -1}, "FAILED", 6),
    ({**_envelope(), "failed_tickers": None}, "FAILED", 6),
    ({"status": "SUCCESS", "rows_inserted": None}, "FAILED", None),
    ({"status": "DEFERRED", "rows_inserted": 0}, "SKIPPED", 0),
])
def test_explicit_aggregate_coverage(out, expected, rows):
    assert ss._classify_outcome(out)[:2] == (expected, rows)


@pytest.mark.parametrize("out,expected", [
    (_envelope(), "PARTIAL"),
    (_envelope(failed=0), "SUCCESS"),
    (_envelope(current=2, succeeded=0, failed=0, rows=0), "NO_NEW_DATA"),
    (_envelope(no_data=2, succeeded=0, failed=0, rows=0), "NO_NEW_DATA"),
    (_envelope(succeeded=0, rows=0), "FAILED"),
    (_envelope(failed=0, unattempted=1), "PARTIAL"),
    ({"status": "SUCCESS"}, "FAILED"),
    ({"status": "SKIPPED", "rows_inserted": 0, "skipped_reason": "lock held"}, "SKIPPED"),
])
def test_dedicated_worker_returns_and_persists_normalized_terminal_outcome(out, expected):
    engine = honest._engine()  # production status CHECK, nullable counts
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO source_catalog (id, name) VALUES (524, 'TIINGO')"))

    class Puller:
        SOURCE_NAME = "TIINGO"

        def pull_incremental(self):
            return out

    try:
        result = scheduler.run_tiingo_prices(engine, puller=Puller())
        assert (result["status"], result["rows_inserted"]) == (expected, out.get("rows_inserted"))
        assert (honest._last_pull(engine, "TIINGO") is not None) == (expected == "SUCCESS")
        logs = honest._pull_log(engine)
        if expected == "SKIPPED":
            assert logs == []
        else:
            assert len(logs) == 1
            assert logs[0][1:3] == ("SUCCESS" if expected == "NO_NEW_DATA" else expected,
                                   out.get("rows_inserted"))
            if expected == "NO_NEW_DATA":
                assert logs[0][3].startswith("NO_NEW_DATA:")
            elif expected in {"FAILED", "PARTIAL"}:
                assert logs[0][3]  # useful diagnostic, not erased
    finally:
        engine.dispose()


@pytest.mark.parametrize("mode,expected,rows", [
    ("mixed", "PARTIAL", 6), ("complete", "SUCCESS", 12),
    ("current", "NO_NEW_DATA", 0), ("no_data", "NO_NEW_DATA", 0),
    ("all_failed", "FAILED", 0), ("budget", "PARTIAL", 6),
])
def test_real_incremental_algorithm_through_current_main_callers(sched, monkeypatch, mode, expected, rows):
    """Keep real aggregation/locks/coverage; replace only DB/provider boundaries."""
    instances = []

    class Tiingo(_FakeIncremental):
        def __init__(self, db_engine=None, **_constructor_boundary):
            latest = {t: _SESSION if mode == "current" else None for t in ("SPY", "ZZZ")}
            super().__init__(latest)
            self.calls = 0
            instances.append(self)

        def pull_ticker(self, ticker, **kwargs):
            if mode == "all_failed" or (mode == "mixed" and ticker == "ZZZ"):
                return {"ticker": ticker, "status": "FAILED", "rows_inserted": 0, "errors": ["mock outage"]}
            if mode == "no_data":
                return {"ticker": ticker, "status": "PARTIAL", "rows_inserted": 0, "errors": ["No data returned"]}
            return {"ticker": ticker, "status": "SUCCESS", "rows_inserted": 6, "errors": []}

        def pull_incremental(self, *, should_continue=None):
            def keep_going():
                self.calls += 1
                return (should_continue is None or should_continue()) and (mode != "budget" or self.calls <= 1)

            return super().pull_incremental(list(self.latest), max_workers=1,
                                           should_continue=keep_going,
                                           now=datetime(2026, 9, 30, tzinfo=timezone.utc))

    monkeypatch.setattr(tp, "TiingoPuller", Tiingo)
    monkeypatch.setattr(tp.requests, "get", lambda *_a, **_k: pytest.fail("provider reached"))
    monkeypatch.setenv("TIINGO_API_KEY", "dummy-offline-fixture")
    monkeypatch.setitem(hermes_operator._SOURCE_REGISTRY, "tiingo", deepcopy(_TIINGO_REPAIR))
    smart = sched({**_TIINGO_ENTRY, "_catalog": "TIINGO"})
    result = smart.tick()["results"][0]
    assert (result["status"], result["rows_inserted"]) == (expected, rows)
    assert (honest._last_pull(sched.engine, "TIINGO") is not None) == (expected == "SUCCESS")
    assert honest._pull_log(sched.engine)[0][1:3] == (
        "SUCCESS" if expected == "NO_NEW_DATA" else expected, rows)
    if expected == "NO_NEW_DATA":
        assert ss.SmartScheduler(sched.engine)._get_due_pullers() == []  # checked cadence survives restart

    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    retry = hermes_fixers._retry_source("tiingo", engine)
    assert (retry["outcome"], retry["rows_inserted"]) == (expected, rows)
    assert any("last_pull_at" in str(c.args[0]) for c in conn.execute.call_args_list) == (expected == "SUCCESS")

    # The worker parent is reached with its default import/constructor too.
    with sched.engine.begin() as conn:
        conn.execute(text("UPDATE source_catalog SET last_pull_at = NULL"))
        conn.execute(text("DELETE FROM pull_log"))
    monkeypatch.setattr(scheduler, "_resolve_source_catalog_entry", lambda *_a: (1, "TIINGO"))
    scheduler._tiingo_worker_main(sched.engine)
    assert honest._pull_log(sched.engine)[0][1:3] == (
        "SUCCESS" if expected == "NO_NEW_DATA" else expected, rows)
    assert (honest._last_pull(sched.engine, "TIINGO") is not None) == (expected == "SUCCESS")
    assert len(instances) == 3 and all(p.released for p in instances)


def test_real_pg_mixed_parameter_assertion_offline(monkeypatch):
    """Check the real PG parameter against the real group's mock terminal log.

    PostgreSQL fixture execution remains mandatory in CI, not invoked here.
    """
    fn = pg_tests.test_grid_scheduler_pull_group_is_honest_on_postgres
    params = next(m.args[1] for m in fn.pytestmark if m.name == "parametrize")
    mixed = next(p for p in params if p[1:3] == ("PARTIAL", 6))
    assert mixed[3] == "1 of 2 items failed or partial, 0 skipped, 6 rows written: puller reported FAILED"
    engine, summary = group_tests._run_list_result(monkeypatch, mixed[0])
    terminal = engine.pull_logs[1]
    assert (terminal["status"], terminal["rows_inserted"]) == mixed[1:3]
    assert mixed[3] in terminal["error_message"]
    assert summary["results"][0]["status"] == "PARTIAL"
    assert mixed[4] is False and engine.touched_source_ids == []


@pytest.mark.parametrize("summary,expected", [
    ({"status": "SUCCESS", "rows_inserted": 6}, "SUCCESS"),
    ({"status": "PARTIAL", "rows_inserted": 6, "error": "publication failed"}, "PARTIAL"),
    ({"status": "PARTIAL", "rows_inserted": 6, "scope": "GEM subset"}, "PARTIAL"),
    ({"status": "SUCCESS", "rows_inserted": None}, "FAILED"),
    ({"status": "SUCCESS", "rows_inserted": 0}, "NO_NEW_DATA"),
    ({"status": "SKIPPED", "rows_inserted": 0}, "SKIPPED"),
])
def test_options_authoritative_summary_and_owned_publication_in_all_wrappers(sched, monkeypatch, summary, expected):
    # #735 code is deliberately not composed here: only its duck contract.
    class Results(list):
        def __init__(self):
            super().__init__([{"status": "SUCCESS", "rows_inserted": 99}])
            self.summary = summary

    out = Results()

    class Options(honest._ListPuller):
        SOURCE_NAME = "YFINANCE_OPTIONS"
        RETURN = out

    monkeypatch.setattr(honest, "_Repair2Options", Options, raising=False)
    smart = sched(honest._entry("options", "_Repair2Options", _catalog="YFINANCE_OPTIONS"))
    result = smart.tick()["results"][0]
    assert (result["status"], result["rows_inserted"]) == (expected, summary["rows_inserted"])
    assert honest._last_pull(sched.engine, "YFINANCE_OPTIONS") is None
    engine = group_tests._FakeEngine({"YFINANCE_OPTIONS": 185})
    monkeypatch.setattr(scheduler, "_get_pullers_for_group", lambda *_a: [("Options", Options(engine), "pull_all", {})])
    group = scheduler.run_pull_group("daily", engine, config={})
    assert group["results"][0]["status"] == expected
    assert engine.pull_logs[1]["rows_inserted"] == summary["rows_inserted"]
    assert engine.touched_source_ids == []

    honest._install_retry_puller(monkeypatch, "options", out)
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    result = hermes_fixers._retry_source("options", engine)
    assert (result["outcome"], result["rows_inserted"]) == (expected, summary["rows_inserted"])
    assert not any("last_pull_at" in str(c.args[0]) for c in conn.execute.call_args_list)
