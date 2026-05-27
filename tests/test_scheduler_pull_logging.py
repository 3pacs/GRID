from __future__ import annotations

import json
from datetime import date


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
