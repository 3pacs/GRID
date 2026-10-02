"""Options coverage, actual rows and deadline behavior; no DB or network."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import ingestion.options as options
import ingestion.smart_scheduler as ss


def _puller(monkeypatch, statuses):
    obj = options.OptionsPuller.__new__(options.OptionsPuller)
    obj.engine = MagicMock()
    obj._mark_catalog_pulled = MagicMock(return_value=True)
    monkeypatch.setattr(options, "EQUITY_TICKERS", list(statuses))
    monkeypatch.setattr(options, "catalyst_options_universe", lambda _engine, **_kwargs: [])
    monkeypatch.setattr(options, "is_market_open", lambda _day: True)
    monkeypatch.setattr(options, "_utc_now", lambda: datetime(2026, 9, 28, 15, tzinfo=timezone.utc))
    monkeypatch.setattr(options, "YahooOptionsClient", lambda: MagicMock(is_available=True))
    monkeypatch.setattr(options.time, "sleep", lambda _seconds: None)

    def capture(ticker, _today, **_kwargs):
        status = statuses[ticker]
        return {"ticker": ticker, "status": status, "snapshots": 10 if status == "SUCCESS" else 0,
                "rows_inserted": 12 if status == "SUCCESS" else 0}

    obj._pull_ticker = capture
    return obj


def test_full_default_scope_with_rows_is_success_and_fresh(monkeypatch):
    obj = _puller(monkeypatch, {"SPY": "SUCCESS", "QQQ": "SUCCESS"})
    result = obj.pull_all()
    assert isinstance(result, list)
    assert result.summary["status"] == "SUCCESS"
    assert result.summary["rows_inserted"] == 24
    assert result.summary["scope"] == "full_universe"
    obj._mark_catalog_pulled.assert_called_once_with(should_continue=None)


def test_one_success_does_not_make_whole_feed_fresh(monkeypatch):
    obj = _puller(monkeypatch, {"SPY": "SUCCESS", "QQQ": "FAILED", "IWM": "SKIPPED"})
    result = obj.pull_all()
    assert result.summary["status"] == "PARTIAL"
    assert result.summary["rows_inserted"] == 12
    obj._mark_catalog_pulled.assert_not_called()


def test_failed_catalyst_lookup_cannot_claim_full_universe(monkeypatch):
    obj = _puller(monkeypatch, {"SPY": "SUCCESS"})
    monkeypatch.undo()
    obj.engine.connect.side_effect = RuntimeError("unavailable")
    with pytest.raises(RuntimeError, match="options catalyst universe unavailable"):
        obj.pull_all()
    obj._mark_catalog_pulled.assert_not_called()


def test_eight_gem_tickers_are_list_compatible_and_never_fresh(monkeypatch):
    from scripts.pull_options_gem_tickers import GEM_TICKERS

    obj = _puller(monkeypatch, dict.fromkeys(GEM_TICKERS, "SUCCESS"))
    result = obj.pull_all(tickers=list(GEM_TICKERS), include_catalyst_universe=False, max_expirations=6)
    assert [r["ticker"] for r in result] == list(GEM_TICKERS)
    assert all(r["status"] == "SUCCESS" for r in result)
    assert result.summary["status"] == "PARTIAL"
    assert result.summary["scope"] == "subset"
    assert result.summary["rows_inserted"] == len(GEM_TICKERS) * 12 == 8 * 12
    obj._mark_catalog_pulled.assert_not_called()


@pytest.mark.parametrize("kwargs", [{"include_catalyst_universe": False}, {"max_expirations": 6}])
def test_reduced_default_scope_is_not_fresh(monkeypatch, kwargs):
    obj = _puller(monkeypatch, {"SPY": "SUCCESS"})
    assert obj.pull_all(**kwargs).summary["status"] == "PARTIAL"
    obj._mark_catalog_pulled.assert_not_called()


def test_budget_defers_remaining_tickers_and_keeps_real_rows(monkeypatch):
    obj = _puller(monkeypatch, {"SPY": "SUCCESS", "QQQ": "SUCCESS"})
    calls = iter([True, False])
    result = obj.pull_all(should_continue=lambda: next(calls))
    assert [r["status"] for r in result] == ["SUCCESS", "DEFERRED"]
    assert result.summary["status"] == "PARTIAL"
    assert result.summary["rows_inserted"] == 12
    obj._mark_catalog_pulled.assert_not_called()


def test_zero_rows_and_unknown_counts_never_claim_freshness():
    zero = options.OptionsPullResults([{"status": "SUCCESS", "rows_inserted": 0}], full_universe=True)
    unknown = options.OptionsPullResults([{"status": "SUCCESS", "snapshots": 50}], full_universe=True)
    assert zero.summary["status"] == "NO_NEW_DATA"
    assert unknown.summary["status"] == "PARTIAL"
    assert unknown.summary["rows_inserted"] is None


def test_adapter_forwards_deadline_and_summary():
    adapter = ss._OptionsSchedulerAdapter.__new__(ss._OptionsSchedulerAdapter)
    outcome = options.OptionsPullResults([{"status": "SUCCESS", "rows_inserted": 3}], full_universe=False)
    adapter._puller = MagicMock()
    adapter._puller.pull_all.return_value = outcome
    def deadline():
        return True
    assert adapter.pull(deadline)["status"] == "PARTIAL"
    adapter._puller.pull_all.assert_called_once_with(should_continue=deadline)


def test_registry_budget_can_cover_measured_universe():
    entry = next(p for p in ss.PULLER_REGISTRY if p["name"] == "options")
    assert entry["cls"] == "_OptionsSchedulerAdapter"
    assert entry["timeout_s"] == 900
    assert entry["stop_margin_s"] == 60
    assert entry["timeout_s"] - entry["stop_margin_s"] > 600


def test_scheduler_wires_options_margin_and_does_not_bump_partial(monkeypatch):
    import threading

    sched = ss.SmartScheduler.__new__(ss.SmartScheduler)
    sched.engine = MagicMock()
    sched._thread_semaphore = threading.Semaphore(1)
    sched._threads_lock = threading.Lock()
    sched._active_threads = set()
    sched._orphan_thread_count = 0
    sched._update_last_pull = MagicMock()
    clock = [1000.0]
    monkeypatch.setattr(ss.time, "monotonic", lambda: clock[0])
    checks = []

    class Runner:
        def pull(self, should_continue=None):
            checks.append(should_continue())
            clock[0] = 1841.0  # 900s hard timeout minus the 60s stop margin
            checks.append(should_continue())
            return {"status": "PARTIAL", "rows_inserted": 7, "error": "time budget"}

    monkeypatch.setattr(sched, "_build_puller_instance", lambda *_args: Runner())
    entry = next(p for p in ss.PULLER_REGISTRY if p["name"] == "options")
    result = sched._run_puller(entry)
    assert checks == [True, False]
    assert result["status"] == "PARTIAL"
    assert result["rows_inserted"] == 7
    assert sched._orphan_thread_count == 0
    assert not sched._active_threads
    sched._update_last_pull.assert_not_called()


def test_partial_options_retry_does_not_grow_to_24h():
    sched = ss.SmartScheduler.__new__(ss.SmartScheduler)
    sched._state = {}
    for _ in range(12):
        sched._record_result("options", False, "incomplete coverage", partial=True)
    state = sched._state["options"]
    retry = state["cooldown_until"] - state["last_attempt"]
    assert timedelta(minutes=29) < retry < timedelta(minutes=31)
    assert state.get("last_success") is None


def test_restart_state_and_bump_use_catalog_name(monkeypatch):
    from tests.test_smart_scheduler_restart_state import _engine, _add_catalog

    entry = next(p for p in ss.PULLER_REGISTRY if p["name"] == "options")
    monkeypatch.setattr(ss, "PULLER_REGISTRY", [entry])
    monkeypatch.setattr(ss.SmartScheduler, "_warn_registry_divergence", lambda _self: None)
    engine = _engine()
    last = datetime.now(timezone.utc) - timedelta(hours=1)
    _add_catalog(engine, 185, "yfinance_options", last)
    sched = ss.SmartScheduler(engine)
    assert sched._state["options"]["last_success"] == last
    sched._update_last_pull("options")
    from sqlalchemy import text
    with engine.connect() as conn:
        updated = conn.execute(text("SELECT last_pull_at FROM source_catalog WHERE id = 185")).scalar_one()
    assert ss._as_utc(updated) > last


def test_partial_retry_survives_restart(monkeypatch):
    from tests.test_smart_scheduler_restart_state import _engine, _add_catalog
    from sqlalchemy import text

    entry = next(p for p in ss.PULLER_REGISTRY if p["name"] == "options")
    monkeypatch.setattr(ss, "PULLER_REGISTRY", [entry])
    monkeypatch.setattr(ss.SmartScheduler, "_warn_registry_divergence", lambda _self: None)
    engine = _engine()
    _add_catalog(engine, 185, "yfinance_options", None)
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        for i in range(12):
            conn.execute(text("INSERT INTO pull_log (puller_name, status, started_at, completed_at) "
                              "VALUES ('smart:options', 'PARTIAL', :t, :t)"), {"t": now - timedelta(minutes=i)})
    sched = ss.SmartScheduler(engine)
    assert sched._state["options"]["cooldown_until"] == now + timedelta(minutes=30)
    assert sched._state["options"]["last_success"] is None


@pytest.mark.parametrize("policy", ["", "legacy"])
def test_owner_policy_is_disabled_until_owner_selects_it(monkeypatch, policy):
    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", policy)
    engine = MagicMock()
    assert options.automatic_options_capture_guard(engine) is None
    engine.connect.assert_not_called()


@pytest.mark.parametrize("counts", [(0, 0), (1, 1), (122, 122), (366, 122)])
def test_daily_owner_policy_defers_before_during_and_after_partial_capture(monkeypatch, counts):
    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "daily_scheduler")
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchone.return_value = counts
    result = options.automatic_options_capture_guard(engine)
    assert result["status"] == "SKIPPED" and result["rows_inserted"] == 0
    assert result["captured_tickers"] == counts[1]
    assert result["registered_non_gem_batches"] == counts[0]
    sql = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert sql[0] == "SET TRANSACTION READ ONLY"
    assert "capture_source <> 'gem'" in sql[-1]
    assert not any(word in statement for statement in sql for word in ["INSERT", "UPDATE", "DELETE"])


def test_owner_policy_cannot_fall_back_to_a_writer_when_metadata_is_unavailable(monkeypatch):
    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "daily_scheduler")
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("unavailable")
    result = options.automatic_options_capture_guard(engine)
    assert result["outcome"] == "SKIPPED"
    assert result["captured_tickers"] is None
    assert result["observation_error"] == "RuntimeError"


def test_smart_options_owner_policy_skips_before_constructor(monkeypatch):
    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "daily_scheduler")
    engine = MagicMock()
    constructor = MagicMock(side_effect=AssertionError("secondary writer constructed"))
    monkeypatch.setattr(options, "OptionsPuller", constructor)
    adapter = ss._OptionsSchedulerAdapter(engine)
    result = adapter.pull()
    assert result["status"] == "SKIPPED"
    constructor.assert_not_called()


@pytest.mark.parametrize("source", ["options", "yfinance_options", "YFINANCE_OPTIONS", "yfinance-options"])
def test_hermes_owner_policy_skips_before_resolution_and_keeps_freshness_unknown(monkeypatch, source):
    from scripts import hermes_fixers as hf

    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "daily_scheduler")
    resolve = MagicMock(side_effect=AssertionError("secondary writer resolved"))
    monkeypatch.setattr(hf, "_resolve_puller", resolve)
    engine = MagicMock()
    result = hf._retry_source(source, engine, attempt=3)
    assert result["status"] == "SKIPPED" and result["rows_inserted"] == 0
    assert hf.retry_not_fresh_reason(result) == "puller reported SKIPPED"
    resolve.assert_not_called()
    engine.begin.assert_not_called()


def test_daily_owner_policy_does_not_gate_explicit_gem_capture(monkeypatch):
    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "daily_scheduler")
    obj = _puller(monkeypatch, {"SPY": "SUCCESS"})
    result = obj.pull_all(tickers=["SPY"], include_catalyst_universe=False, capture_source="gem")
    assert result[0]["status"] == "SUCCESS"
    assert result.summary["status"] == "PARTIAL"  # never full-source freshness


def test_invalid_owner_policy_defers_without_querying_or_writing(monkeypatch):
    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "typo")
    engine = MagicMock()
    result = options.automatic_options_capture_guard(engine)
    assert result["reason"] == "invalid_options_writer_policy"
    assert result["status"] == "SKIPPED"
    engine.connect.assert_not_called()
