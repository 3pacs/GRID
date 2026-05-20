from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock

from scripts import hermes_fixers


class _FakeCooldowns:
    def __init__(self) -> None:
        self.blacklisted: list[str] = []

    def blacklist_for_timeout(self, source: str) -> None:
        self.blacklisted.append(source)


class _FakeState:
    cycle_count = 7
    cooldowns = _FakeCooldowns()
    task_status = {
        "trust_cycle": {
            "success": False,
            "error": "SSL connection closed",
            "last_run": "2026-05-20T01:53:27+00:00",
            "duration_s": 761.2,
        },
        "active_hypo_scoring": {
            "success": True,
            "error": None,
            "last_run": "2026-05-20T04:12:11+00:00",
            "duration_s": 22.1,
        },
    }


def test_parse_pull_diagnosis_actions_accepts_common_separators() -> None:
    text = "\n".join(
        [
            "FRED: check_key - missing or throttled key",
            "WorldNewsAPI: backfill - stale gap",
            "GDELT_NEWS: retry — transient 503",
            "TIINGO: escalate - repeated auth failure",
        ]
    )

    actions = hermes_fixers._parse_pull_diagnosis_actions(text)

    assert actions == {
        "fred": "CHECK_KEY",
        "worldnewsapi": "BACKFILL",
        "gdelt_news": "RETRY",
        "tiingo": "ESCALATE",
    }


def test_repair_skill_catalog_exposes_existing_and_new_fixers() -> None:
    catalog = hermes_fixers._format_repair_skill_catalog()

    assert "FIX_DATA_QUALITY[:family]" in catalog
    assert "FIX_OUTPUT_DIRS" in catalog
    assert "ENSURE_OPERATOR_TABLES" in catalog
    assert "CHECK_SCHEMA:<table>" in catalog
    assert "INSPECT_SOURCE:<source_name>" in catalog
    assert "COOLDOWN_SOURCE:<source_name>" in catalog
    assert "RUN_WIRING_AUDIT" in catalog
    assert "SCOUT_FREE_DATA:<source_name>" in catalog
    assert "CHECK_SOURCE_QUALITY" in catalog
    assert "LOG_FOLLOWUP:<category>:<severity>:<title>" in catalog
    assert "LIST_SUBAGENTS" in catalog
    assert "DISPATCH_SUBAGENT:<role>:<target_id>[:priority]" in catalog
    assert "CHECK_SUBAGENTS" in catalog


def test_fix_output_dirs_skill_creates_common_output_directories(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    result = hermes_fixers._execute_hermes_repair_command(
        "FIX_OUTPUT_DIRS",
        engine=None,
        health={},
        state=_FakeState(),
    )

    assert result["status"] == "ok"
    for rel_path in (
        "outputs/backtest",
        "outputs/market_briefings",
        "outputs/paper_trades",
        "outputs/llm_insights",
    ):
        assert (tmp_path / rel_path).is_dir()
        assert str(tmp_path / rel_path) in result["paths"][rel_path]


def test_cooldown_source_skill_pauses_noisy_source() -> None:
    state = _FakeState()

    result = hermes_fixers._execute_hermes_repair_command(
        "COOLDOWN_SOURCE:Baltic_Exchange",
        engine=None,
        health={},
        state=state,
    )

    assert result == {
        "cmd": "COOLDOWN_SOURCE:Baltic_Exchange",
        "status": "ok",
        "source": "Baltic_Exchange",
        "cooldown": "timeout_blacklist",
    }
    assert state.cooldowns.blacklisted == ["Baltic_Exchange"]


def test_check_task_failures_skill_summarizes_failed_operator_tasks() -> None:
    result = hermes_fixers._execute_hermes_repair_command(
        "CHECK_TASK_FAILURES",
        engine=None,
        health={},
        state=_FakeState(),
    )

    assert result["status"] == "ok"
    assert result["failed_count"] == 1
    assert result["failures"][0]["task"] == "trust_cycle"
    assert "SSL connection closed" in result["failures"][0]["error"]


def test_inspect_source_tolerates_missing_frequency_column(monkeypatch) -> None:
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    source_result = MagicMock()
    source_result.fetchone.return_value = (2013, "Baltic_Exchange", True, None, None)
    failures_result = MagicMock()
    failures_result.fetchone.return_value = (0, None, None)
    latest_result = MagicMock()
    latest_result.fetchone.return_value = (None,)
    conn.execute.side_effect = [source_result, failures_result, latest_result]

    monkeypatch.setattr(
        hermes_fixers,
        "_source_catalog_column_exists",
        lambda _conn, column: False,
    )

    result = hermes_fixers._inspect_source(engine, "Baltic_Exchange")

    assert result["status"] == "ok"
    assert result["source"]["frequency"] is None
    assert "NULL AS frequency" in str(conn.execute.call_args_list[0].args[0])


def test_log_followup_skill_records_pending_operator_issue(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_log_issue(*_args, **kwargs):
        calls.append(kwargs)
        return 42

    monkeypatch.setattr(hermes_fixers, "log_issue", fake_log_issue)

    result = hermes_fixers._execute_hermes_repair_command(
        "LOG_FOLLOWUP:ingestion:WARNING:Check Baltic Exchange key",
        engine=object(),
        health={},
        state=_FakeState(),
    )

    assert result["status"] == "ok"
    assert result["issue_id"] == 42
    assert calls[0]["category"] == "ingestion"
    assert calls[0]["severity"] == "WARNING"
    assert calls[0]["title"] == "Check Baltic Exchange key"
    assert calls[0]["fix_result"] == "PENDING"


def test_list_subagents_skill_exposes_dedicated_roles() -> None:
    result = hermes_fixers._execute_hermes_repair_command(
        "LIST_SUBAGENTS",
        engine=None,
        health={},
        state=_FakeState(),
    )

    roles = {entry["role"] for entry in result["subagents"]}
    assert result["status"] == "ok"
    assert "source_doctor" in roles
    assert "free_data_scout" in roles
    assert "wiring_auditor" in roles
    assert "hypothesis_scorer" in roles


def test_dispatch_subagent_enqueues_known_role(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_enqueue_goal(_engine, **kwargs):
        calls.append(kwargs)
        return 101

    import intelligence.goal_queue as goal_queue

    monkeypatch.setattr(goal_queue, "enqueue_goal", fake_enqueue_goal)

    result = hermes_fixers._execute_hermes_repair_command(
        "DISPATCH_SUBAGENT:source_doctor:Baltic_Exchange:180",
        engine=object(),
        health={},
        state=_FakeState(),
    )

    assert result["status"] == "queued"
    assert result["goal_id"] == 101
    assert calls[0]["goal_type"] == "hermes_diagnose_source"
    assert calls[0]["target_id"] == "Baltic_Exchange"
    assert calls[0]["priority"] == 180
    assert calls[0]["payload"]["requested_by"] == "hermes"


def test_dispatch_free_data_scout_enqueues_known_role(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_enqueue_goal(_engine, **kwargs):
        calls.append(kwargs)
        return 202

    import intelligence.goal_queue as goal_queue

    monkeypatch.setattr(goal_queue, "enqueue_goal", fake_enqueue_goal)

    result = hermes_fixers._execute_hermes_repair_command(
        "DISPATCH_SUBAGENT:free_data_scout:Tiingo:170",
        engine=object(),
        health={},
        state=_FakeState(),
    )

    assert result["status"] == "queued"
    assert result["goal_type"] == "hermes_scout_free_data"
    assert calls[0]["goal_type"] == "hermes_scout_free_data"
    assert calls[0]["target_id"] == "Tiingo"
    assert calls[0]["allow_cloud"] is False


def test_scout_free_data_skill_logs_public_candidates(monkeypatch) -> None:
    issues: list[dict] = []

    monkeypatch.setattr(
        hermes_fixers,
        "_inspect_source",
        lambda _engine, source: {"status": "ok", "source": source},
    )

    def fake_log_issue(*_args, **kwargs):
        issues.append(kwargs)
        return 303

    monkeypatch.setattr(hermes_fixers, "log_issue", fake_log_issue)

    result = hermes_fixers._execute_hermes_repair_command(
        "SCOUT_FREE_DATA:Baltic_Exchange",
        engine=object(),
        health={},
        state=_FakeState(),
    )

    providers = {entry["provider"] for entry in result["candidates"]}
    assert result["status"] == "ok"
    assert "balticdryindex_github_latest" in providers
    assert result["issue_id"] == 303
    assert issues[0]["fix_applied"] == "free_data_scout"
    assert issues[0]["fix_result"] == "PENDING"


def test_check_source_quality_skill_runs_ablation(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_run(engine, **kwargs):
        calls.append({"engine": engine, **kwargs})
        return {
            "status": "ok",
            "summary": {"paid_sources": 2, "free_sources": 5},
            "json_path": "outputs/source_quality/source_quality_ablation_latest.json",
            "markdown_path": "outputs/source_quality/source_quality_ablation_latest.md",
        }

    import intelligence.source_quality_ablation as source_quality_ablation

    monkeypatch.setattr(source_quality_ablation, "run_source_quality_ablation", fake_run)

    engine = object()
    result = hermes_fixers._execute_hermes_repair_command(
        "CHECK_SOURCE_QUALITY",
        engine=engine,
        health={},
        state=_FakeState(),
    )

    assert result["status"] == "ok"
    assert result["cmd"] == "CHECK_SOURCE_QUALITY"
    assert result["summary"]["paid_sources"] == 2
    assert calls[0]["engine"] is engine
    assert calls[0]["days"] == 30


def test_retry_source_handles_function_based_registry_entries(monkeypatch) -> None:
    calls: list[dict] = []
    module_name = "_grid_test_fn_puller"
    fake_module = ModuleType(module_name)

    def run_weekly(db_engine=None, days_back=None):
        calls.append({"db_engine": db_engine, "days_back": days_back})
        return {"status": "ok", "source": "fn"}

    fake_module.run_weekly = run_weekly
    monkeypatch.setitem(sys.modules, module_name, fake_module)

    from scripts import hermes_operator

    monkeypatch.setitem(
        hermes_operator._SOURCE_REGISTRY,
        "regulatory_events",
        {
            "mod": module_name,
            "fn": "run_weekly",
            "pull_kwargs": {"days_back": 7},
        },
    )

    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    result = hermes_fixers._retry_source("regulatory_events", engine)

    assert result == {"status": "ok", "source": "fn"}
    assert calls == [{"db_engine": engine, "days_back": 7}]
