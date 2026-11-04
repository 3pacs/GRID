"""diagnose_and_fix_pulls makes progress inside its budget (2026-11-04).

Production shape (grid-hermes journal, 24h to 2026-11-04 05:28Z): the step
timed out 16x/day and skipped "46 remaining action(s) — repair deadline
passed" 55x/day because (1) the REASON-tier diagnosis call spent the whole
budget thinking, (2) uninterruptible pullers ran past the 240s step timeout,
(3) every pass restarted at the same slow head of the list, and (4)
deterministic failures were retried every 30 minutes. Also covers the
api_key_mode-aware retry construction and the stale-refresh skip of
not-configured sources. All provider/DB/LLM calls are fakes.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from scripts import hermes_fixers as hf
from scripts import hermes_operator as ho
from scripts.hermes_health import OperatorState


@pytest.fixture(autouse=True)
def _fresh_schedule(monkeypatch):
    monkeypatch.setattr(hf, "_REPAIR_LAST_STARTED", {})
    monkeypatch.setattr(hf, "_DETERMINISTIC_SKIPS", {})
    monkeypatch.setattr(hf, "_DIAGNOSIS_CACHE", {})
    monkeypatch.setattr(hf, "_THROTTLED_WARNINGS", {})
    monkeypatch.setattr(hf, "_REPEATED_FAILURES", {})
    monkeypatch.setattr(hf, "_REPAIRS_IN_FLIGHT", {})
    monkeypatch.setattr(hf, "_relation_exists", lambda *_a: True)
    monkeypatch.setattr(hf, "log_issue", MagicMock())
    monkeypatch.setattr(hf, "resolve_source_issues", MagicMock(return_value=0))
    yield


def _engine(sources: list[str]) -> Any:
    rows = [(name, 3, datetime(2026, 11, 4, tzinfo=timezone.utc), "synthetic failure")
            for name in sources]
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = rows
    return engine


def _no_llm(monkeypatch):
    monkeypatch.setattr("llm.router.get_llm", lambda *a, **k: pytest.fail("LLM not expected"))


# ── per-source cap + resume order ─────────────────────────────────────


def test_slow_source_is_capped_and_pass_returns_before_its_deadline(monkeypatch):
    _no_llm(monkeypatch)
    monkeypatch.setattr(hf, "REPAIR_BUDGET_SECONDS", 3.0)
    monkeypatch.setattr(hf, "PER_SOURCE_REPAIR_SECONDS", 0.3)
    release = threading.Event()
    calls: list[str] = []

    def fake_retry(name, engine, *, attempt, state, should_continue):
        calls.append(name)
        if name == "SLOW":
            release.wait(5)  # an uninterruptible provider call
            return {"status": "abandoned", "reason": "deadline_passed"}
        return {"status": "SUCCESS", "outcome": "NO_NEW_DATA", "rows_inserted": 0}

    monkeypatch.setattr(hf, "_retry_source", fake_retry)
    state = OperatorState()
    started = time.monotonic()
    try:
        result = hf.diagnose_and_fix_pulls(_engine(["SLOW", "FAST"]), False, state)
    finally:
        release.set()
    elapsed = time.monotonic() - started

    assert elapsed < 2.0, "a slow source must not hold the pass past its per-source cap"
    assert calls == ["SLOW", "FAST"]
    assert result["per_source_timeouts"] == 1
    # The overrun is a failed attempt (ordinary cooldown), not a recovery.
    assert state.cooldowns.get_status("SLOW")["consecutive_fails"] == 1
    assert "per-source cap" in state.cooldowns.get_status("SLOW")["last_error"]
    assert state.cooldowns.get_status("FAST")["consecutive_fails"] == 0


def test_next_pass_resumes_with_sources_the_previous_pass_did_not_reach(monkeypatch):
    infos = [{"source": s, "fail_count": n} for s, n in (("A", 9), ("B", 5), ("C", 1))]
    assert hf._order_repair_candidates(infos) == ["A", "B", "C"]
    hf._note_repair_started("a")  # previous pass reached only A
    assert hf._order_repair_candidates(infos) == ["B", "C", "A"]
    hf._note_repair_started("b")
    hf._note_repair_started("c")
    assert hf._order_repair_candidates(infos) == ["A", "B", "C"]


def test_whole_pass_never_exceeds_the_repair_budget(monkeypatch):
    _no_llm(monkeypatch)
    monkeypatch.setattr(hf, "REPAIR_BUDGET_SECONDS", 0.6)
    monkeypatch.setattr(hf, "PER_SOURCE_REPAIR_SECONDS", 0.25)
    release = threading.Event()

    def blocking_retry(name, engine, *, attempt, state, should_continue):
        release.wait(5)
        return {"status": "abandoned", "reason": "deadline_passed"}

    monkeypatch.setattr(hf, "_retry_source", blocking_retry)
    started = time.monotonic()
    try:
        result = hf.diagnose_and_fix_pulls(
            _engine([f"S{i}" for i in range(10)]), False, OperatorState())
    finally:
        release.set()
    assert time.monotonic() - started < 1.5
    assert result["skipped_actions"], "the rest is left for the next pass"
    assert result["per_source_timeouts"] >= 2


# ── deterministic failures ───────────────────────────────────────────


@pytest.mark.parametrize("exc", [
    AttributeError("'GDELTPuller' object has no attribute 'pull_all'"),
    TypeError("BasePuller.__init__() got an unexpected keyword argument 'api_key'"),
])
def test_deterministic_failure_is_skipped_on_later_passes(monkeypatch, exc):
    _no_llm(monkeypatch)
    calls: list[str] = []

    def failing_retry(name, engine, *, attempt, state, should_continue):
        calls.append(name)
        raise exc

    monkeypatch.setattr(hf, "_retry_source", failing_retry)
    state = OperatorState()
    hf.diagnose_and_fix_pulls(_engine(["Broken"]), False, state)
    assert hf._deterministic_skip_reason("Broken") is not None
    # Even with the ordinary cooldown cleared, the next pass skips it.
    state.cooldowns = OperatorState().cooldowns
    result = hf.diagnose_and_fix_pulls(_engine(["Broken"]), False, state)
    assert calls == ["Broken"]
    assert result["skipped_deterministic"] == 1
    assert hf.log_issue.call_count == 1, "the failure is recorded once, not every pass"


def test_transient_typeerror_keeps_the_ordinary_cooldown(monkeypatch):
    assert not hf._is_deterministic_retry_error(TypeError("'NoneType' object is not subscriptable"))
    assert hf._is_deterministic_retry_error(TypeError("f() missing 1 required positional argument: 'x'"))
    assert not hf._is_deterministic_retry_error(RuntimeError("HTTP 503"))


def test_no_handler_is_recorded_once(monkeypatch):
    _no_llm(monkeypatch)

    def no_handler(name, engine, *, attempt, state, should_continue):
        raise ValueError("Retry handler catalog identity does not match requested source")

    monkeypatch.setattr(hf, "_retry_source", no_handler)
    state = OperatorState()
    hf.diagnose_and_fix_pulls(_engine(["BCB_BR"]), False, state)
    result = hf.diagnose_and_fix_pulls(_engine(["BCB_BR"]), False, state)
    assert result["skipped_deterministic"] == 1
    assert hf.log_issue.call_count == 1


def test_deterministic_skip_expires(monkeypatch):
    monkeypatch.setattr(hf, "DETERMINISTIC_FAILURE_COOLDOWN_SECONDS", 0.05)
    hf._record_deterministic_failure("X", "AttributeError: nope")
    assert hf.repair_deterministically_blocked("x") is not None
    time.sleep(0.08)
    assert hf.repair_deterministically_blocked("x") is None


# ── bounded diagnosis LLM ────────────────────────────────────────────


class _ThinkingAwareClient:
    is_available = True

    def __init__(self, answer: str, delay: float = 0.0) -> None:
        self.answer = answer
        self.delay = delay
        self.calls: list[dict[str, Any]] = []

    def chat(self, messages, temperature=0.3, num_predict=None,
             system_knowledge=None, extra_metadata=None, enable_thinking=None):
        self.calls.append({"messages": messages, "enable_thinking": enable_thinking})
        time.sleep(self.delay)
        return self.answer


def test_diagnosis_disables_thinking_and_asks_only_retryable_sources(monkeypatch):
    client = _ThinkingAwareClient("A: SKIP - known outage\nB: RETRY - transient")
    monkeypatch.setattr("llm.router.get_llm", lambda *a, **k: client)
    monkeypatch.setattr(hf, "_retry_source",
                        lambda *a, **k: {"status": "SUCCESS", "outcome": "NO_NEW_DATA"})
    state = OperatorState()
    state.cooldowns.record_attempt("Cooling", success=False, error="x")  # 30-min cooldown
    hf._record_deterministic_failure("Broken", "AttributeError: nope")

    result = hf.diagnose_and_fix_pulls(_engine(["A", "B", "Cooling", "Broken"]), True, state)

    assert len(client.calls) == 1
    assert client.calls[0]["enable_thinking"] is False
    prompt = client.calls[0]["messages"][1]["content"]
    assert '"A"' in prompt and '"B"' in prompt
    assert "Cooling" not in prompt and "Broken" not in prompt
    assert result["diagnosed"] == 2

    # Same errors next pass: answered from the cache, no second LLM call.
    state.cooldowns = OperatorState().cooldowns
    hf.diagnose_and_fix_pulls(_engine(["A", "B"]), True, state)
    assert len(client.calls) == 1


def test_slow_diagnosis_cannot_spend_the_repair_budget(monkeypatch):
    client = _ThinkingAwareClient("A: SKIP - x", delay=2.0)
    monkeypatch.setattr("llm.router.get_llm", lambda *a, **k: client)
    monkeypatch.setattr(hf, "DIAGNOSIS_LLM_BUDGET_SECONDS", 0.2)
    retried: list[str] = []

    def fake_retry(name, engine, *, attempt, state, should_continue):
        retried.append(name)
        return {"status": "SUCCESS", "outcome": "NO_NEW_DATA"}

    monkeypatch.setattr(hf, "_retry_source", fake_retry)
    started = time.monotonic()
    result = hf.diagnose_and_fix_pulls(_engine(["A"]), True, OperatorState())
    assert time.monotonic() - started < 1.5
    assert retried == ["A"], "default RETRY proceeds when the diagnosis does not answer in time"
    assert "diagnosis" not in result


# ── api_key_mode-aware retry construction ────────────────────────────


class _Recorder:
    SOURCE_NAME = "synthetic"

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


@pytest.mark.parametrize("mode,expected_args,expected_kwargs", [
    ("env", (), {"db_engine": "ENGINE"}),
    ("first", ("k-123", "ENGINE"), {}),
    ("keyword", (), {"db_engine": "ENGINE", "api_key": "k-123"}),
])
def test_retry_construction_mirrors_scheduler_modes(monkeypatch, mode, expected_args, expected_kwargs):
    monkeypatch.setenv("SYNTH_RETRY_KEY", "k-123")
    puller = hf._construct_retry_puller(
        {"api_key": "SYNTH_RETRY_KEY", "api_key_mode": mode}, _Recorder, "ENGINE")
    assert puller.args == expected_args
    assert puller.kwargs == expected_kwargs


@pytest.mark.parametrize("mode", ["env", "first", "keyword"])
def test_missing_key_is_a_skip_not_a_crash(monkeypatch, mode):
    # A key name config.Settings does not declare at all (the QUIVERQUANT
    # case): getattr default + environment lookup, never AttributeError.
    monkeypatch.delenv("SYNTH_UNDECLARED_KEY", raising=False)
    with pytest.raises(hf.MissingRetryApiKey):
        hf._construct_retry_puller(
            {"api_key": "SYNTH_UNDECLARED_KEY", "api_key_mode": mode}, _Recorder, "ENGINE")


def test_retry_source_returns_skipped_for_missing_key(monkeypatch):
    def resolve(name, engine):
        raise hf.MissingRetryApiKey("QUIVERQUANT_API_KEY")

    monkeypatch.setattr(hf, "_resolve_puller", resolve)
    engine = MagicMock()
    result = hf._retry_source("quiverquant", engine)
    assert result["outcome"] == "SKIPPED"
    assert result["reason"] == "missing_api_key"
    assert hf.retry_not_fresh_reason(result).startswith("puller reported SKIPPED")
    engine.begin.assert_not_called()  # freshness never advanced
    assert "quiverquant" not in hf._REPAIRS_IN_FLIGHT


def test_derived_registry_carries_scheduler_key_mode():
    assert ho._SOURCE_REGISTRY["quiverquant"]["api_key_mode"] == "env"
    assert ho._SOURCE_REGISTRY["kosis"]["api_key_mode"] == "keyword"
    # Scheduler default ("first") for entries that do not declare a mode.
    assert ho._SOURCE_REGISTRY["fred"]["api_key_mode"] == "first"


def test_quiverquant_retry_constructs_with_db_engine_only(monkeypatch):
    # Settings may not declare this field; the retry reads the environment.
    monkeypatch.setenv("QUIVERQUANT_API_KEY", "synthetic")
    captured: dict[str, Any] = {}

    class FakeQuiver:
        SOURCE_NAME = "quiverquant"

        def __init__(self, db_engine):
            captured["db_engine"] = db_engine

    import types
    import sys
    module = types.ModuleType("_grid_test_quiver")
    module.QuiverQuantPuller = FakeQuiver
    monkeypatch.setitem(sys.modules, "_grid_test_quiver", module)
    monkeypatch.setitem(ho._SOURCE_REGISTRY, "quiverquant", {
        **ho._SOURCE_REGISTRY["quiverquant"], "mod": "_grid_test_quiver", "cls": "QuiverQuantPuller"})
    puller, _method, _kwargs = hf._resolve_puller("quiverquant", "ENGINE")
    assert isinstance(puller, FakeQuiver) and captured == {"db_engine": "ENGINE"}


# ── stale refresh skips not-configured sources ───────────────────────


def test_stale_refresh_skips_not_configured_once_per_process(monkeypatch):
    monkeypatch.setattr(ho, "_STALE_NOT_ACTIONABLE_LOGGED", set())
    infos: list[str] = []
    monkeypatch.setattr(ho.log, "info", lambda template, **kw: infos.append(template.format(**kw)))
    entry = {"source": "WorldNewsAPI", "not_configured": True,
             "last_attempt_reason": "WORLDNEWS_API_KEY not set"}
    assert ho._stale_refresh_not_actionable(entry) is True
    assert ho._stale_refresh_not_actionable(entry) is True
    assert len(infos) == 1 and "not configured" in infos[0]
    # Older health payloads (no field) behave exactly as before.
    assert ho._stale_refresh_not_actionable({"source": "FRED"}) is False
    assert ho._stale_refresh_not_actionable({"source": "FRED", "not_configured": False}) is False


def test_stale_refresh_skips_deterministically_failing_source(monkeypatch):
    monkeypatch.setattr(ho, "_STALE_NOT_ACTIONABLE_LOGGED", set())
    hf._record_deterministic_failure("GDELT", "AttributeError: no pull_all")
    assert ho._stale_refresh_not_actionable({"source": "GDELT"}) is True


# ── USDA_NASS routing + handler-less stale sources ───────────────────


def test_usda_nass_routes_to_its_own_bounded_handler(monkeypatch):
    from ingestion.physical.usda_nass import USDAPuller

    assert hf._CATALOG_TO_REGISTRY["USDA_NASS"] == "usda_nass"
    entry = ho._SOURCE_REGISTRY["usda_nass"]
    assert entry["cls"] == "USDAPuller" and entry["api_key_mode"] == "keyword"
    start_year = entry["pull_kwargs"]["start_year"]
    assert callable(start_year)
    assert start_year() == datetime.now(timezone.utc).year - 1  # never 1970

    monkeypatch.setenv("USDA_NASS_API_KEY", "synthetic-nass")
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (77,)
    puller, method, kwargs = hf._resolve_puller("USDA_NASS", engine)
    assert type(puller) is USDAPuller and puller.api_key == "synthetic-nass"
    assert method == "pull_all" and "start_year" in kwargs


def test_usda_nass_without_key_is_a_skip(monkeypatch):
    monkeypatch.delenv("USDA_NASS_API_KEY", raising=False)
    from config import settings

    if hasattr(settings, "USDA_NASS_API_KEY"):
        monkeypatch.setattr(settings, "USDA_NASS_API_KEY", "")
    result = hf._retry_source("USDA_NASS", MagicMock())
    assert (result["outcome"], result["reason"]) == ("SKIPPED", "missing_api_key")


@pytest.mark.parametrize("source", ["finviz_fundamentals", "ecb_tltro"])
def test_handlerless_sources_raise_no_retry_handler(source):
    with pytest.raises(hf.NoRetryHandler):
        hf._resolve_puller(source, MagicMock())


def test_stale_loop_returns_slots_of_handlerless_sources(monkeypatch):
    from tests.test_hermes_recovery_guard_accounting import _read, _setup

    source, state, engine, active = _setup(monkeypatch)
    monkeypatch.setattr(ho, "_STALE_NOT_ACTIONABLE_LOGGED", set())
    monkeypatch.setattr(ho, "_STALE_NO_HANDLER", set())
    infos: list[str] = []
    monkeypatch.setattr(ho.log, "info", lambda template, **kw: infos.append(template.format(**kw)))
    resolved: list[str] = []

    class Completed:
        SOURCE_NAME = source

        def pull(self):
            return {"status": "SUCCESS", "rows_inserted": 6}

    def resolve(name, *_a):
        resolved.append(name)
        if name != source:
            raise hf.NoRetryHandler("No explicit dataset handler registered for requested source")
        return Completed(), "pull", {}

    monkeypatch.setattr(hf, "_resolve_puller", resolve)
    handlerless = [{"source": f"NOHANDLER_{i}"} for i in range(16)]
    # 16 handler-less entries ahead of a fixable one: before the fix they
    # used all 15 slots and the fixable source never ran.
    assert _read("stale", source, state, engine,
                 stale_sources=handlerless + [{"source": source}]) == 1
    assert resolved[-1] == source
    assert sum("no retry handler for NOHANDLER_0" in line for line in infos) == 1

    # Next cycle: handler-less sources are skipped without a resolve call
    # and without another log line.
    resolved.clear()
    before = len(infos)
    assert _read("stale", source, state, engine,
                 stale_sources=handlerless + [{"source": source}]) == 1
    assert resolved == [source]
    assert not any("NOHANDLER" in line for line in infos[before:])


# ── source -> handler map identity (no guessing) ─────────────────────


def _declared(entry):
    import importlib

    return getattr(getattr(importlib.import_module(entry["mod"]), entry["cls"]), "SOURCE_NAME", None)


def test_every_catalog_map_entry_targets_a_writer_for_that_catalog():
    """A mapped key must name a class that writes exactly this catalog.

    The 2026-11-04 map routed polygon->yfinance, fmp->earnings_calendar,
    nasa_firms->noaa_swpc, AKShare->yfinance and 29 more; the identity
    guard refused them before construction, but each burned a stale-refresh
    slot every cycle. This pins the map so a wrong guess cannot come back.
    """
    wrong = []
    for catalog, key in hf._CATALOG_TO_REGISTRY.items():
        entry = ho._SOURCE_REGISTRY.get(key)
        if entry is None or "cls" not in entry or entry.get("skip_runtime"):
            wrong.append((catalog, key, "no class entry"))
            continue
        cls = type("Declared", (), {"SOURCE_NAME": _declared(entry)})
        if not hf._retry_target_matches(catalog, cls):
            wrong.append((catalog, key, cls.SOURCE_NAME))
    assert wrong == []


@pytest.mark.parametrize("catalog,cls_name,method", [
    ("polygon", "PolygonPuller", "pull"),
    ("fmp", "FMPPuller", "pull"),
    ("nasa_firms", "NASAFirmsPuller", "pull"),
    ("AKShare", "AKShareMacroPuller", "pull_all"),
    ("USDA_NASS", "USDAPuller", "pull_all"),
    ("treasury_auction", "TreasuryAuctionPuller", "pull_all"),
    ("fed_h8", "H8BankBalancePuller", "pull_all"),
])
def test_real_pullers_route_to_their_own_class(catalog, cls_name, method):
    low = catalog.lower()
    entry = ho._SOURCE_REGISTRY.get(low) or ho._SOURCE_REGISTRY[hf._CATALOG_TO_REGISTRY[catalog]]
    assert entry["cls"] == cls_name
    assert entry.get("pull_method", "pull_all") == method
    assert hf._retry_target_matches(catalog, type("D", (), {"SOURCE_NAME": _declared(entry)}))


@pytest.mark.parametrize("catalog", ["ecb_tltro", "taiwan_exports", "semi_book_to_bill",
                                     "CEPII_BACI", "finviz_fundamentals", "world_bank"])
def test_sources_without_a_correct_handler_are_no_handler(catalog):
    with pytest.raises(hf.NoRetryHandler):
        hf._resolve_puller(catalog, MagicMock())


def test_catalog_name_aliases_are_derived_from_the_scheduler_registry():
    from ingestion.smart_scheduler import REGISTRY_CATALOG_NAMES

    checked = 0
    for name, catalog in REGISTRY_CATALOG_NAMES.items():
        if name not in ho._SOURCE_REGISTRY:
            continue
        alias = catalog.strip().lower().replace(" ", "_").replace("-", "_")
        assert alias in ho._SOURCE_REGISTRY
        checked += 1
    assert checked > 10


# ── repeated identical failures -> long, doubling skip ───────────────


def test_repeated_same_failure_gets_a_long_doubling_skip(monkeypatch):
    reason = "puller reported FAILED: 5 of 5 items failed or partial"
    assert not hf.note_repair_failure("Baltic_Exchange", reason)
    assert not hf.note_repair_failure("Baltic_Exchange", reason.replace("5", "4"))
    assert hf.note_repair_failure("Baltic_Exchange", reason)
    until, why = hf._DETERMINISTIC_SKIPS["baltic_exchange"]
    first = until - time.monotonic()
    assert first > 23 * 3600 and "same failure 3x" in why

    for _ in range(3):
        triggered = hf.note_repair_failure("Baltic_Exchange", reason)
    assert triggered
    second = hf._DETERMINISTIC_SKIPS["baltic_exchange"][0] - time.monotonic()
    assert second > 1.9 * first, "each further long skip doubles"


def test_changed_reason_or_recovery_restarts_the_count():
    hf.note_repair_failure("S", "HTTP 503")
    hf.note_repair_failure("S", "HTTP 503")
    assert not hf.note_repair_failure("S", "timeout")  # different reason
    hf.note_repair_recovered("S")
    assert not hf.note_repair_failure("S", "timeout")
    assert hf.repair_deterministically_blocked("S") is None


def test_diagnose_escalates_a_dead_source_after_repeated_failures(monkeypatch):
    _no_llm(monkeypatch)
    calls = []

    def dead(name, engine, *, attempt, state, should_continue):
        calls.append(name)
        return {"status": "FAILED", "outcome": "FAILED", "rows_inserted": 0,
                "outcome_note": "5 of 5 items failed or partial"}

    monkeypatch.setattr(hf, "_retry_source", dead)
    state = OperatorState()
    for _ in range(4):
        state.cooldowns = OperatorState().cooldowns  # ignore the ordinary backoff
        hf.diagnose_and_fix_pulls(_engine(["Baltic_Exchange"]), False, state)
    assert calls == ["Baltic_Exchange"] * 3, "the 4th pass skips the dead source"
