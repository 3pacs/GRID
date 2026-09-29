"""Options coverage, actual rows and deadline behavior; no DB or network."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import ingestion.options as options
import ingestion.smart_scheduler as ss


def _puller(monkeypatch, statuses):
    obj = options.OptionsPuller.__new__(options.OptionsPuller)
    obj.engine = MagicMock()
    obj._mark_catalog_pulled = MagicMock()
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
    obj._mark_catalog_pulled.assert_called_once_with()


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


def test_nine_gem_tickers_are_list_compatible_and_never_fresh(monkeypatch):
    from scripts.pull_options_gem_tickers import GEM_TICKERS

    obj = _puller(monkeypatch, dict.fromkeys(GEM_TICKERS, "SUCCESS"))
    result = obj.pull_all(tickers=list(GEM_TICKERS), include_catalyst_universe=False, max_expirations=6)
    assert [r["ticker"] for r in result] == list(GEM_TICKERS)
    assert all(r["status"] == "SUCCESS" for r in result)
    assert result.summary["status"] == "PARTIAL"
    assert result.summary["scope"] == "subset"
    assert result.summary["rows_inserted"] == 9 * 12
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
