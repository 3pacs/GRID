"""Hermes's failed-pull repair keeps smart:finra_dix runs apart from FINRA_SHORT_VOLUME's (2026-11-21).

finra_dix writes under FINRA_SHORT_VOLUME's source_catalog row
(smart_scheduler.REGISTRY_CATALOG_NAMES), so its pull_log runs carry that
row's source_id. diagnose_and_fix_pulls named every run by its catalog row,
so a DIX failure was repaired by re-running the short-volume puller
(2026-11-18 07:53Z), and a DIX success marked a short-volume failure
recovered.

The query side runs on real SQL in test_hermes_latest_failure_diagnostic.py
(SQLite) and test_hermes_latest_failure_diagnostic_pg.py (opt-in
PostgreSQL). This file covers the repair side: the real module, with fake
failed-pull rows in the shape that query returns.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from ingestion.source_freshness import GUEST_PULL_LOG_NAMES
from scripts import hermes_fixers as hf
from scripts.hermes_health import OperatorState

T0 = datetime(2026, 11, 18, 7, 53, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _fresh_state(monkeypatch):
    for name in ("_REPAIR_LAST_STARTED", "_DETERMINISTIC_SKIPS", "_DIAGNOSIS_CACHE",
                 "_THROTTLED_WARNINGS", "_REPEATED_FAILURES", "_REPAIR_RECOVERED_AT",
                 "_REPAIRS_IN_FLIGHT", "_UNROUTED_FAILURES_REPORTED"):
        monkeypatch.setattr(hf, name, {})
    monkeypatch.setattr(hf, "_relation_exists", lambda *_a: True)
    monkeypatch.setattr(hf, "_held_source_names", lambda: {})
    monkeypatch.setattr(hf, "log_issue", MagicMock())
    monkeypatch.setattr(hf, "resolve_source_issues", MagicMock(return_value=0))
    yield


def _engine(rows: list[tuple[str, int, datetime, str]]) -> Any:
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = rows
    return engine


class _Client:
    """A REASON-tier double that records each prompt."""

    def __init__(self, answer: str) -> None:
        self.answer = answer
        self.prompts: list[str] = []

    def chat(self, messages: list[dict[str, str]], **_kwargs: Any) -> str:
        self.prompts.append(messages[1]["content"])
        return self.answer


def _record_retries(monkeypatch) -> list[str]:
    retried: list[str] = []

    def fake_retry(name, engine, *, attempt, state, should_continue):
        retried.append(name)
        return {"status": "SUCCESS", "outcome": "NO_NEW_DATA", "rows_inserted": 0}

    monkeypatch.setattr(hf, "_retry_source", fake_retry)
    return retried


def _issues() -> list[dict[str, Any]]:
    return [call.kwargs for call in hf.log_issue.call_args_list]


# ── where the guest names come from, and how the query uses them ─────


def test_guest_names_are_source_freshness_guest_names():
    assert hf._guest_pull_log_names() is GUEST_PULL_LOG_NAMES
    assert GUEST_PULL_LOG_NAMES == frozenset({"smart:finra_dix"})


def test_query_binds_guest_names_on_the_failure_and_recovery_sides(monkeypatch):
    _record_retries(monkeypatch)
    engine = _engine([])
    hf.diagnose_and_fix_pulls(engine, False, OperatorState())
    conn = engine.connect.return_value.__enter__.return_value
    (statement, params), _ = conn.execute.call_args
    sql = str(statement)
    assert "pl.puller_name = ANY(:guest_pull_log_names)" in sql
    assert "ok.puller_name = ANY(:guest_pull_log_names)" in sql
    assert params == {"guest_pull_log_names": ["smart:finra_dix"]}


# ── a DIX failure: no retry under any name, reported once ────────────


def test_dix_failure_does_not_retry_the_short_volume_puller(monkeypatch):
    retried = _record_retries(monkeypatch)
    monkeypatch.setattr("llm.router.get_llm", lambda *a, **k: pytest.fail("LLM not expected"))
    state = OperatorState()

    result = hf.diagnose_and_fix_pulls(
        _engine([("smart:finra_dix", 2, T0, "finra_dix timed out after 600s")]), True, state)

    assert retried == []
    assert result["skipped_no_handler"] == 1
    assert result["retried"] == result["escalated"] == result["diagnosed"] == 0
    assert state.cooldowns.get_status("smart:finra_dix") is None
    assert state.cooldowns.get_status("FINRA_SHORT_VOLUME") is None
    [issue] = _issues()
    assert issue["severity"] == "WARNING"
    assert issue["source"] == "smart:finra_dix"
    assert issue["title"] == "No Hermes pull handler — smart:finra_dix"
    assert issue["fix_result"] == "SKIPPED"


def test_dix_failure_is_reported_once_and_again_only_when_it_fails_again(monkeypatch):
    retried = _record_retries(monkeypatch)
    log = MagicMock()
    monkeypatch.setattr(hf, "log", log)
    state = OperatorState()

    def reports() -> int:
        return sum(1 for c in log.warning.call_args_list if c.args[0].startswith("Not repairing"))

    for _ in range(3):
        result = hf.diagnose_and_fix_pulls(
            _engine([("smart:finra_dix", 1, T0, "DIX_FAIL")]), False, state)
        assert result["skipped_no_handler"] == 1
    assert reports() == 1
    assert len(_issues()) == 1

    later = T0 + timedelta(days=1)
    hf.diagnose_and_fix_pulls(_engine([("smart:finra_dix", 2, later, "DIX_FAIL_AGAIN")]), False, state)
    assert reports() == 2
    assert len(_issues()) == 2
    assert retried == []
    assert not [i for i in _issues() if i["severity"] == "CRITICAL"]


def test_dry_run_reports_a_dix_failure_without_recording_it(monkeypatch):
    _record_retries(monkeypatch)
    log = MagicMock()
    monkeypatch.setattr(hf, "log", log)
    for _ in range(2):
        hf.diagnose_and_fix_pulls(_engine([("smart:finra_dix", 1, T0, "DIX_FAIL")]), False,
                                  OperatorState(), dry_run=True)
    assert sum(1 for c in log.warning.call_args_list if c.args[0].startswith("Not repairing")) == 2
    assert hf._UNROUTED_FAILURES_REPORTED == {}
    assert _issues() == []


def test_dix_failure_takes_no_diagnosis_slot_and_the_owner_is_still_repaired(monkeypatch):
    retried = _record_retries(monkeypatch)
    client = _Client("FINRA_SHORT_VOLUME: RETRY - transient")
    monkeypatch.setattr("llm.router.get_llm", lambda *a, **k: client)

    result = hf.diagnose_and_fix_pulls(_engine([
        ("FINRA_SHORT_VOLUME", 1, T0, "SV_FAIL"),
        ("smart:finra_dix", 1, T0 + timedelta(minutes=5), "DIX_FAIL"),
    ]), True, OperatorState())

    assert retried == ["FINRA_SHORT_VOLUME"]
    [prompt] = client.prompts
    assert '"FINRA_SHORT_VOLUME"' in prompt and "finra_dix" not in prompt
    assert result["diagnosed"] == 1
    assert result["skipped_no_handler"] == 1


def test_single_writer_source_is_retried_as_before(monkeypatch):
    retried = _record_retries(monkeypatch)
    monkeypatch.setattr("llm.router.get_llm", lambda *a, **k: pytest.fail("LLM not expected"))
    result = hf.diagnose_and_fix_pulls(
        _engine([("SYNTH_SINGLE", 3, T0, "synthetic failure")]), False, OperatorState())
    assert retried == ["SYNTH_SINGLE"]
    assert result["skipped_no_handler"] == 0
    assert result["retried"] == 1
    assert _issues() == []


# ── why there is no route to finra_dix ───────────────────────────────


def test_no_retry_handler_can_claim_a_dix_run(monkeypatch):
    built: list[Any] = []
    monkeypatch.setattr(hf, "_construct_retry_puller",
                        lambda entry, cls, engine: built.append(cls) or object())
    for name in ("smart:finra_dix", "finra_dix"):
        with pytest.raises(hf.NoRetryHandler):
            hf._resolve_puller(name, MagicMock())
    assert built == []

    # FINRADixPuller declares the short-volume row, so the identity check
    # refuses it for every name but that row's; the row itself resolves to
    # its owner, the short-volume producer.
    from ingestion.altdata.finra_dix import FINRADixPuller

    assert FINRADixPuller.SOURCE_NAME == "FINRA_SHORT_VOLUME"
    assert not hf._retry_target_matches("finra_dix", FINRADixPuller)
    hf._resolve_puller("FINRA_SHORT_VOLUME", MagicMock())
    assert [cls.__name__ for cls in built] == ["FINRAShortVolumeAggPuller"]
