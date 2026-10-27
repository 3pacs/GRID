from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock

import pytest


class _FakeResult:
    def __init__(self, row=None, rows=None):
        self._row = row
        self._rows = rows or []

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, statement, params=None):
        params = dict(params or {})
        sql = " ".join(str(statement).lower().split())
        self.engine.statements.append((sql, params))

        if "select id, name from source_catalog" in sql:
            source = self.engine.catalog.get(params["name"].lower())
            return _FakeResult(source)

        if "select max(rs.obs_date)" in sql:
            self.engine.incremental_source_names.append(params["name"])
            row = (self.engine.max_obs_date,) if self.engine.max_obs_date else None
            return _FakeResult(row)

        if "insert into pull_log" in sql:
            log_id = len(self.engine.pull_logs) + 1
            self.engine.pull_logs[log_id] = {
                "puller_name": params["name"],
                "source_id": params["sid"],
                "started_at": params["started"],
                "status": "RUNNING",
                "node_name": params["node"],
            }
            return _FakeResult((log_id,))

        if "update pull_log set" in sql:
            self.engine.pull_logs[params["id"]].update(
                {
                    "completed_at": params["completed"],
                    "status": params["status"],
                    "rows_inserted": params["rows"],
                    "rows_expected": params["expected"],
                    "error_message": params["error"],
                    "features_affected": params["features"],
                }
            )
            return _FakeResult()

        if "select rows_inserted from pull_log" in sql:
            return _FakeResult(rows=[])

        if "insert into event_bus" in sql:
            self.engine.events.append(params)
            return _FakeResult()

        if "update source_catalog set last_pull_at" in sql:
            self.engine.touched_source_ids.append(params["id"])
            return _FakeResult()

        return _FakeResult()


class _FakeEngine:
    def __init__(self, catalog=None, max_obs_date=None):
        self.catalog = {
            name.lower(): (source_id, name)
            for name, source_id in (catalog or {}).items()
        }
        self.max_obs_date = max_obs_date
        self.pull_logs = {}
        self.touched_source_ids = []
        self.incremental_source_names = []
        self.events = []
        self.statements = []

    def begin(self):
        return _FakeConnection(self)

    def connect(self):
        return _FakeConnection(self)


class _SuccessfulPuller:
    def __init__(self, result):
        self.result = result
        self.kwargs = None
        self.called = False

    def pull(self, **kwargs):
        self.called = True
        self.kwargs = kwargs
        return self.result


class _FailingPuller:
    def pull(self, **kwargs):
        raise RuntimeError("upstream refused the request")


def test_run_pull_group_logs_success_and_touches_canonical_source(monkeypatch):
    import ingestion.scheduler as sched

    assert sched._extract_rows_inserted({"total_inserted": 11}) == 11

    engine = _FakeEngine({"tiingo_news": 782})
    puller = _SuccessfulPuller({"rows_inserted": 7})
    monkeypatch.setattr(
        sched,
        "_get_pullers_for_group",
        lambda group, db_engine, config: [("Tiingo_News", puller, "pull", {})],
    )

    summary = sched.run_pull_group("daily", engine, config={})

    assert summary["success_count"] == 1
    assert summary["failure_count"] == 0
    assert engine.pull_logs[1]["puller_name"] == "Tiingo_News"
    assert engine.pull_logs[1]["source_id"] == 782
    assert engine.pull_logs[1]["status"] == "SUCCESS"
    assert engine.pull_logs[1]["rows_inserted"] == 7
    assert engine.touched_source_ids == [782]
    assert json.loads(engine.events[0]["payload"])["status"] == "SUCCESS"


def test_run_pull_group_logs_failure_without_touching_source(monkeypatch):
    import ingestion.scheduler as sched

    engine = _FakeEngine({"polygon": 722})
    monkeypatch.setattr(
        sched,
        "_get_pullers_for_group",
        lambda group, db_engine, config: [("Polygon", _FailingPuller(), "pull", {})],
    )

    summary = sched.run_pull_group("daily", engine, config={})

    assert summary["success_count"] == 0
    assert summary["failure_count"] == 1
    assert engine.pull_logs[1]["source_id"] == 722
    assert engine.pull_logs[1]["status"] == "FAILED"
    assert "upstream refused" in engine.pull_logs[1]["error_message"]
    assert engine.touched_source_ids == []


def test_skip_sources_match_canonical_aliases(monkeypatch):
    import ingestion.scheduler as sched

    engine = _FakeEngine()
    puller = _SuccessfulPuller({"rows_inserted": 1})
    monkeypatch.setattr(
        sched,
        "_get_pullers_for_group",
        lambda group, db_engine, config: [("Tiingo_News", puller, "pull", {})],
    )

    summary = sched.run_pull_group(
        "daily",
        engine,
        config={},
        skip_sources={"tiingo_news"},
    )

    assert summary["skipped_count"] == 1
    assert puller.called is False
    assert engine.pull_logs == {}


def test_incremental_start_uses_canonical_source_alias(monkeypatch):
    import ingestion.scheduler as sched

    engine = _FakeEngine({"TIINGO": 524}, max_obs_date=date(2026, 5, 20))
    puller = _SuccessfulPuller({"rows_inserted": 2})
    monkeypatch.setattr(
        sched,
        "_get_pullers_for_group",
        lambda group, db_engine, config: [
            ("Tiingo_Prices", puller, "pull", {"start_date": "incremental"})
        ],
    )

    summary = sched.run_pull_group("daily", engine, config={})

    assert summary["success_count"] == 1
    assert puller.kwargs["start_date"] == "2026-04-20"
    assert engine.incremental_source_names == ["TIINGO"]
    assert engine.touched_source_ids == [524]


# ── Honest outcomes (2026-09-29, PR #727 review follow-up) ───────────────


def _run_list_result(monkeypatch, result):
    import ingestion.scheduler as sched

    engine = _FakeEngine({"TIINGO": 524})
    monkeypatch.setattr(
        sched,
        "_get_pullers_for_group",
        lambda group, db_engine, config: [("Tiingo_Prices", _SuccessfulPuller(result), "pull", {})],
    )
    return engine, sched.run_pull_group("daily", engine, config={})


def test_list_row_count_is_the_items_rows_not_the_list_length(monkeypatch):
    import ingestion.scheduler as sched

    items = [
        {"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 6},
        {"ticker": "QQQ", "status": "SUCCESS", "rows_inserted": 0},
        {"ticker": "ZZZ", "status": "PARTIAL", "rows_inserted": 0, "errors": ["404"]},
    ]
    assert sched._extract_rows_inserted(items) == 6  # was len(items) == 3
    assert sched._extract_rows_inserted(["a", "b"]) is None  # opaque does not prove writes

    engine, summary = _run_list_result(monkeypatch, items)
    assert summary["success_count"] == 0
    assert summary["partial_count"] == 1
    assert engine.pull_logs[1]["status"] == "PARTIAL"
    assert engine.pull_logs[1]["rows_inserted"] == 6
    assert engine.touched_source_ids == []


def test_all_items_skipped_is_not_fresh(monkeypatch):
    items = [{"ticker": t, "status": "SKIPPED", "reason": "non-equity-session"} for t in ("SPY", "QQQ")]
    engine, summary = _run_list_result(monkeypatch, items)

    assert summary["success_count"] == 0
    assert summary["skipped_count"] == 1
    assert summary["results"][0]["status"] == "SKIPPED"
    log_row = engine.pull_logs[1]
    assert (log_row["status"], log_row["rows_inserted"]) == ("SUCCESS", 0)
    assert log_row["error_message"].startswith("SKIPPED: all 2 items skipped")
    assert engine.touched_source_ids == []
    assert json.loads(engine.events[0]["payload"])["status"] == "SKIPPED"


def test_all_items_failed_is_failed(monkeypatch):
    items = [{"ticker": "SPY", "status": "FAILED", "rows_inserted": 0, "errors": ["HTTP 503"]}]
    engine, summary = _run_list_result(monkeypatch, items)

    assert summary["failure_count"] == 1
    assert summary["success_count"] == 0
    assert engine.pull_logs[1]["status"] == "FAILED"
    assert "PullerReportedFailure" in engine.pull_logs[1]["error_message"]
    assert engine.touched_source_ids == []


def test_clean_zero_row_run_is_logged_but_not_fresh(monkeypatch):
    items = [{"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 0}]
    engine, summary = _run_list_result(monkeypatch, items)

    assert summary["success_count"] == 0
    assert summary["no_new_data_count"] == 1
    log_row = engine.pull_logs[1]
    assert (log_row["status"], log_row["rows_inserted"]) == ("SUCCESS", 0)
    assert log_row["error_message"].startswith("NO_NEW_DATA:")
    assert engine.touched_source_ids == []
    assert json.loads(engine.events[0]["payload"])["status"] == "NO_NEW_DATA"


@pytest.mark.parametrize("start_date", ["2026-10-02", date(2026, 10, 1)])
def test_equity_fred_keeps_incremental_overlap(monkeypatch, start_date):
    """The equity date floor must not erase FRED's revision overlap."""
    import db
    import ingestion.fred as fred
    import ingestion.scheduler as sched

    class StopAfterFred(BaseException):
        """Stop before the unrelated equity providers can run."""

    # Run the real FRED incremental logic without its API/DB constructor.
    puller = fred.FREDPuller.__new__(fred.FREDPuller)
    puller._get_latest_date = MagicMock(return_value=date(2026, 10, 1))
    puller.pull_series = MagicMock(side_effect=StopAfterFred)
    monkeypatch.setattr(fred, "FRED_SERIES_LIST", ["DFF"])
    monkeypatch.setattr(fred, "FREDPuller", lambda **_kwargs: puller)
    monkeypatch.setattr(db, "get_engine", lambda: object())

    with pytest.raises(StopAfterFred):
        sched._run_equity_pulls(start_date=start_date)

    puller.pull_series.assert_called_once_with("DFF", "2026-09-24", None)


def test_equity_fred_unknown_commit_summary_is_an_acknowledged_lower_bound(monkeypatch):
    import db
    import ingestion.fred as fred
    import ingestion.scheduler as sched
    import ingestion.yfinance_pull as yf

    class StopAfterFred(BaseException):
        pass

    puller = MagicMock()
    puller.pull_all.return_value = [
        {"series_id": "DFF", "status": "PARTIAL", "rows_inserted": 50,
         "commit_outcome_unknown": True, "rows_inserted_total": None},
        {"series_id": "UNRATE", "status": "SKIPPED", "rows_inserted": 0, "aborted": True},
    ]
    yf_puller = MagicMock()
    yf_puller.pull_all.side_effect = StopAfterFred
    monkeypatch.setattr(fred, "FREDPuller", lambda **_kwargs: puller)
    monkeypatch.setattr(yf, "YFinancePuller", lambda **_kwargs: yf_puller)
    monkeypatch.setattr(db, "get_engine", lambda: object())
    monkeypatch.setattr(sched, "log", MagicMock())
    with pytest.raises(StopAfterFred):
        sched._run_equity_pulls(start_date="2026-10-02")
    summary = next(c for c in sched.log.info.call_args_list if c.args[0].startswith("FRED daily pull complete"))
    assert summary.kwargs == {"ok": 0, "total": 2, "rows": 50}
    assert "acknowledged inserts" in summary.args[0]
    assert any("unknown commit outcomes" in c.args[0] and "lower bound" in c.args[0]
               for c in sched.log.warning.call_args_list)
    yf_puller.pull_all.assert_called_once_with(start_date="2026-10-02")
