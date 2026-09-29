"""The ``options`` feed: cooperative budget, honest status, catalog bump.

No network and no DB: OptionsPuller internals are stubbed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

import ingestion.options as options_mod
import ingestion.smart_scheduler as ss
from ingestion.options import OptionsPuller


def _bare_puller(monkeypatch: pytest.MonkeyPatch, outcomes: dict[str, str]) -> tuple[OptionsPuller, MagicMock]:
    """OptionsPuller with no DB/HTTP: each ticker returns outcomes[ticker]."""
    puller = OptionsPuller.__new__(OptionsPuller)
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    puller.engine = engine
    puller.source_id = 185

    class _Yahoo:
        is_available = True

    monkeypatch.setattr(options_mod, "YahooOptionsClient", lambda: _Yahoo())
    monkeypatch.setattr(options_mod, "is_market_open", lambda d: True)
    monkeypatch.setattr(
        options_mod, "_utc_now",
        lambda: datetime(2026, 9, 28, 15, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(options_mod.time, "sleep", lambda s: None)

    def _pull_ticker(self: Any, ticker: str, today_str: str, *, max_expirations: int = 12) -> dict:
        return {"ticker": ticker, "status": outcomes[ticker], "snapshots": 10}

    monkeypatch.setattr(OptionsPuller, "_pull_ticker", _pull_ticker)
    return puller, conn


def _catalog_bumps(conn: MagicMock) -> list[dict]:
    return [
        c.args[1] for c in conn.execute.call_args_list
        if "UPDATE source_catalog SET last_pull_at" in str(c.args[0])
    ]


def test_full_run_bumps_own_catalog_row(monkeypatch: pytest.MonkeyPatch) -> None:
    puller, conn = _bare_puller(monkeypatch, {"SPY": "SUCCESS", "XYZ": "FAILED"})
    results = puller.pull_all(tickers=["SPY", "XYZ"])
    assert [r["status"] for r in results] == ["SUCCESS", "FAILED"]
    assert _catalog_bumps(conn) == [{"sid": 185}]


def test_should_continue_defers_rest_and_does_not_bump(monkeypatch: pytest.MonkeyPatch) -> None:
    puller, conn = _bare_puller(monkeypatch, {"SPY": "SUCCESS", "QQQ": "SUCCESS", "IWM": "SUCCESS"})
    budget = iter([True, False, False])
    results = puller.pull_all(tickers=["SPY", "QQQ", "IWM"], should_continue=lambda: next(budget))
    assert [r["status"] for r in results] == ["SUCCESS", "DEFERRED", "DEFERRED"]
    assert _catalog_bumps(conn) == []


def test_all_failed_does_not_bump(monkeypatch: pytest.MonkeyPatch) -> None:
    puller, conn = _bare_puller(monkeypatch, {"SPY": "FAILED"})
    puller.pull_all(tickers=["SPY"])
    assert _catalog_bumps(conn) == []


class _FakePuller:
    def __init__(self, statuses: list[str]) -> None:
        self.statuses = statuses
        self.kwargs: dict = {}

    def pull_all(self, **kwargs: Any) -> list[dict]:
        self.kwargs = kwargs
        return [{"ticker": f"T{i}", "status": s, "snapshots": 1} for i, s in enumerate(self.statuses)]


def _adapter(statuses: list[str]) -> ss._OptionsSchedulerAdapter:
    a = ss._OptionsSchedulerAdapter.__new__(ss._OptionsSchedulerAdapter)
    a._puller = _FakePuller(statuses)
    return a


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        (["SUCCESS", "FAILED"], "SUCCESS"),
        (["SUCCESS", "DEFERRED"], "PARTIAL"),
        (["FAILED", "FAILED"], "FAILED"),
        (["DEFERRED", "DEFERRED"], "FAILED"),
        (["SKIPPED", "SKIPPED"], "SKIPPED"),
    ],
)
def test_adapter_status(statuses: list[str], expected: str) -> None:
    assert _adapter(statuses).pull(should_continue=lambda: True)["status"] == expected


def test_adapter_forwards_should_continue() -> None:
    a = _adapter(["SUCCESS"])
    marker = lambda: True  # noqa: E731
    a.pull(should_continue=marker)
    assert a._puller.kwargs["should_continue"] is marker


def test_registry_entry_uses_budget_aware_adapter() -> None:
    entry = next(p for p in ss.PULLER_REGISTRY if p["name"] == "options")
    assert entry["mod"] == "ingestion.smart_scheduler"
    assert entry["cls"] == "_OptionsSchedulerAdapter"
    import inspect
    assert "should_continue" in inspect.signature(ss._OptionsSchedulerAdapter.pull).parameters


def test_restart_state_and_bump_use_catalog_name() -> None:
    sched = ss.SmartScheduler.__new__(ss.SmartScheduler)
    sched._state = {}
    last = datetime(2026, 9, 28, 20, 26, tzinfo=timezone.utc)
    engine = MagicMock()
    cconn = MagicMock()
    cconn.execute.return_value.fetchall.return_value = [("yfinance_options", last)]
    engine.connect.return_value.__enter__ = MagicMock(return_value=cconn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    bconn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=bconn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    sched.engine = engine

    sched._load_state_from_db()
    assert sched._state["options"]["last_success"] == last

    sched._update_last_pull("options")
    assert bconn.execute.call_args.args[1] == {"n": "yfinance_options"}
