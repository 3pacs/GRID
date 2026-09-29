from __future__ import annotations

import sys
from pathlib import Path
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
    assert "CHECK_STORAGE" in catalog
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
        assert str(tmp_path / rel_path) in result["paths"][str(Path(rel_path))]


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
    assert "storage_maintainer" in roles
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


def test_dispatch_storage_maintainer_enqueues_known_role(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_enqueue_goal(_engine, **kwargs):
        calls.append(kwargs)
        return 203

    import intelligence.goal_queue as goal_queue

    monkeypatch.setattr(goal_queue, "enqueue_goal", fake_enqueue_goal)

    result = hermes_fixers._execute_hermes_repair_command(
        "DISPATCH_SUBAGENT:storage_maintainer:grid-svr-data:165",
        engine=object(),
        health={},
        state=_FakeState(),
    )

    assert result["status"] == "queued"
    assert result["goal_type"] == "hermes_storage_maintenance"
    assert calls[0]["goal_type"] == "hermes_storage_maintenance"
    assert calls[0]["target_id"] == "grid-svr-data"
    assert calls[0]["hardware_tier"] == "cpu"
    assert calls[0]["allow_cloud"] is False


def test_check_storage_skill_runs_storage_curator(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_run(engine, **kwargs):
        calls.append({"engine": engine, **kwargs})
        return {
            "status": "ingest_gap",
            "target_id": kwargs["target_id"],
            "cleanup_candidates": 2,
            "ingest_actions": 1,
            "summary": {"gdelt": {"ingest_status": "tables_empty"}},
        }

    from scripts import storage_curator

    monkeypatch.setattr(storage_curator, "run_storage_maintenance", fake_run)
    monkeypatch.setattr(hermes_fixers, "log_issue", lambda *_args, **_kwargs: 404)

    engine = object()
    result = hermes_fixers._execute_hermes_repair_command(
        "CHECK_STORAGE:grid-svr-data",
        engine=engine,
        health={},
        state=_FakeState(),
    )

    assert result["status"] == "ingest_gap"
    assert result["cmd"] == "CHECK_STORAGE:grid-svr-data"
    assert calls == [{"engine": engine, "target_id": "grid-svr-data"}]


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

    assert result["status"] == "ok"
    assert result["source"] == "fn"
    assert result["outcome"] == "FAILED"
    assert result["rows_inserted"] is None
    assert calls == [{"db_engine": engine, "days_back": 7}]


def test_retry_source_resolves_callable_kwargs_at_call_time(monkeypatch) -> None:
    """PULLER_REGISTRY kwargs may be zero-arg callables (e.g. FINRA's
    "anchor_date": _finra_short_volume_trade_date, resolved fresh on every
    call rather than frozen at import time -- see
    SmartScheduler._run_puller's identical resolution). Before this fix,
    _retry_source passed such a callable STRAIGHT THROUGH to the puller's
    method as a function object instead of the value it computes.
    """
    calls: list[dict] = []
    module_name = "_grid_test_callable_kwarg_puller"
    fake_module = ModuleType(module_name)

    class _Puller:
        def __init__(self, db_engine=None):
            self.db_engine = db_engine

        def pull_recent(self, anchor_date=None, weekdays_back=None):
            calls.append({"anchor_date": anchor_date, "weekdays_back": weekdays_back})
            return {"status": "SUCCESS", "rows_inserted": 0}

    fake_module._Puller = _Puller
    monkeypatch.setitem(sys.modules, module_name, fake_module)

    from scripts import hermes_operator

    monkeypatch.setitem(
        hermes_operator._SOURCE_REGISTRY,
        "finra_short_volume",
        {
            "mod": module_name,
            "cls": "_Puller",
            "pull_method": "pull_recent",
            "pull_kwargs": {
                "anchor_date": lambda: "2026-09-16",
                "weekdays_back": 5,
            },
        },
    )

    engine = MagicMock()

    result = hermes_fixers._retry_source("finra_short_volume", engine)

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 0
    assert result["outcome"] == "NO_NEW_DATA"
    # The callable was CALLED, not passed through as a function object.
    assert calls == [{"anchor_date": "2026-09-16", "weekdays_back": 5}]


def test_retry_source_never_resolves_should_continue_kwarg(monkeypatch) -> None:
    """should_continue is a cooperative-cancellation callback BY CONTRACT
    (called repeatedly BY the puller, not a value to precompute once) --
    the new callable-kwarg resolution loop must skip it by name, same as
    SmartScheduler._run_puller does. Uses a puller method whose signature
    does NOT itself declare "should_continue" (unlike YFinancePuller-style
    pullers) so _retry_source's OWN "wire a combined should_continue"
    branch (which would overwrite it regardless) never fires -- isolating
    exactly the callable-kwarg-resolution behaviour this test targets.
    """
    module_name = "_grid_test_should_continue_puller"
    fake_module = ModuleType(module_name)
    captured: dict = {}

    class _Puller:
        def __init__(self, db_engine=None):
            pass

        # Deliberately no literal "should_continue" PARAMETER NAME (a
        # **_kwargs catch-all instead) -- that keeps _retry_source's own
        # "if 'should_continue' in params: wire a combined one" branch
        # from firing, which would otherwise overwrite kwargs
        # unconditionally and mask what this test is actually checking.
        def pull_all(self, other=None, **_kwargs):
            captured["should_continue"] = _kwargs.get("should_continue")
            captured["other"] = other
            return {"status": "SUCCESS"}

    fake_module._Puller = _Puller
    monkeypatch.setitem(sys.modules, module_name, fake_module)

    from scripts import hermes_operator

    sentinel = lambda: True  # noqa: E731
    monkeypatch.setitem(
        hermes_operator._SOURCE_REGISTRY,
        "should_continue_source",
        {
            "mod": module_name,
            "cls": "_Puller",
            "pull_kwargs": {"should_continue": sentinel, "other": lambda: "resolved"},
        },
    )

    hermes_fixers._retry_source("should_continue_source", MagicMock())

    assert captured["should_continue"] is sentinel  # untouched, not called
    assert captured["other"] == "resolved"  # a normal callable WAS resolved


def test_catalog_to_registry_eia_maps_to_eia_not_fred() -> None:
    """2026-09-27 review: this used to map "EIA" -> "fred", so a REPULL/
    retry for the "EIA" source silently pulled FRED's data under EIA's
    name instead of raising or resolving to EIAPuller."""
    assert hermes_fixers._CATALOG_TO_REGISTRY["EIA"] == "eia"


def test_source_overrides_pops_api_key_for_eia() -> None:
    """EIAPuller.__init__ only accepts db_engine (it reads EIA_API_KEY from
    os.environ itself) -- PULLER_REGISTRY's "eia" entry carries
    "api_key": "EIA_API_KEY" for the SCHEDULER's own fail-closed check
    (SmartScheduler._build_puller_instance), but
    hermes_fixers._resolve_puller's OLDER ctor-kwargs convention would
    otherwise pass that straight through as an EXPLICIT kwarg, raising
    `TypeError: EIAPuller.__init__() got an unexpected keyword argument
    'api_key'` on every REPULL/retry. `_SOURCE_OVERRIDES["eia"] =
    {"api_key": None}` pops it back out of the DERIVED registry (see
    `_build_source_registry`'s `if value is None: entry.pop(field, None)`)
    without touching PULLER_REGISTRY / the scheduler's own copy."""
    from ingestion.smart_scheduler import PULLER_REGISTRY
    from scripts import hermes_operator

    puller_entry = next(p for p in PULLER_REGISTRY if p["name"] == "eia")
    assert puller_entry["api_key"] == "EIA_API_KEY"  # scheduler side: unchanged

    assert "api_key" not in hermes_operator._SOURCE_REGISTRY["eia"]


def test_resolve_puller_for_eia_does_not_pass_api_key_kwarg(monkeypatch) -> None:
    """End-to-end through `_resolve_puller`: building an "eia" puller
    instance must not raise from an unexpected `api_key` ctor kwarg."""
    from scripts import hermes_operator

    captured: dict = {}

    class _FakeEIAPullerCls:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    module_name = "_grid_test_eia_ctor"
    fake_module = ModuleType(module_name)
    fake_module.EIAPuller = _FakeEIAPullerCls
    monkeypatch.setitem(sys.modules, module_name, fake_module)
    monkeypatch.setitem(
        hermes_operator._SOURCE_REGISTRY,
        "eia",
        {**hermes_operator._SOURCE_REGISTRY["eia"], "mod": module_name, "cls": "EIAPuller"},
    )

    puller, _method, _kwargs = hermes_fixers._resolve_puller("eia", MagicMock())

    assert isinstance(puller, _FakeEIAPullerCls)
    assert "api_key" not in captured
    assert "db_engine" in captured
