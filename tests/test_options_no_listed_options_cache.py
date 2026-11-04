"""Tickers with no listed options are remembered, logged once, and count as checked.

grid-hermes, 24 h to 2026-11-04: ``<T>: no options expirations`` 312 times and
``Yahoo options <T>: no chain result`` 24 times -- the same ~30 catalyst-universe
names (NUOX CRNX VXRT TKPHF SNYNF ...) every run, each reported SKIPPED/FAILED,
so the options source could never be anything but PARTIAL. Probed from
grid-svr 2026-11-04: VXRT returns a result with 0 expirationDates and a 0.40
spot; NUOX/CRNX return ``{"result": [], "error": null}``.

No network: the Yahoo client and the database are doubles.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from loguru import logger as log

from ingestion import options
from tests.test_options_capture_batch import SESSION_NOW, _DB


class _Yahoo:
    is_available = True

    def __init__(self, pages: dict[str, Any]) -> None:
        self.pages = pages
        self.calls: list[str] = []

    def get_options(self, ticker: str, _expiry: int | None = None) -> Any:
        self.calls.append(ticker)
        return self.pages[ticker]


def _no_expirations_page() -> dict[str, Any]:
    return {
        "quote": {"regularMarketPrice": 0.4,
                  "regularMarketTime": int((SESSION_NOW - timedelta(hours=2)).timestamp())},
        "expirations": [], "strikes": [], "calls": [], "puts": [],
    }


@pytest.fixture
def puller(monkeypatch: pytest.MonkeyPatch) -> options.OptionsPuller:
    monkeypatch.setattr(options, "_NO_OPTIONS_UNTIL", {})
    obj = options.OptionsPuller.__new__(options.OptionsPuller)
    obj.engine = _DB()
    monkeypatch.setattr(options.time, "sleep", lambda _s: None)
    monkeypatch.setattr(options, "_utc_now", lambda: SESSION_NOW)
    return obj


@pytest.fixture
def info_lines():
    records: list[str] = []
    sink = log.add(lambda m: records.append(m.record["message"]), level="INFO")
    yield records
    log.remove(sink)


def _ticker(puller: options.OptionsPuller, ticker: str) -> dict[str, Any]:
    return puller._pull_ticker(ticker, SESSION_NOW.date().isoformat())


@pytest.mark.unit
class TestNoListedOptions:
    @pytest.mark.parametrize("page, why", [
        (_no_expirations_page(), "no expirations"),
        (options.NO_CHAIN, "provider returned no chain"),
    ])
    def test_reported_as_checked_no_data(self, puller, page, why):
        puller._yahoo = _Yahoo({"VXRT": page})
        result = _ticker(puller, "VXRT")
        assert result == {"ticker": "VXRT", "status": "NO_NEW_DATA", "rows_inserted": 0,
                          "reason": f"no listed options: {why}", "no_options": True}

    def test_cached_ticker_is_not_refetched_and_logs_once(self, puller, info_lines):
        yahoo = _Yahoo({"NUOX": options.NO_CHAIN})
        puller._yahoo = yahoo
        for _ in range(12):  # one day of runs
            result = _ticker(puller, "NUOX")
            assert result["status"] == "NO_NEW_DATA" and result["no_options"] is True
        assert yahoo.calls == ["NUOX"]
        assert sum("NUOX: no listed options" in m for m in info_lines) == 1

    def test_ttl_expiry_reprobes(self, puller, monkeypatch):
        clock = [1000.0]
        monkeypatch.setattr(options.time, "monotonic", lambda: clock[0])
        yahoo = _Yahoo({"VXRT": _no_expirations_page()})
        puller._yahoo = yahoo
        _ticker(puller, "VXRT")
        clock[0] += options.NO_OPTIONS_TTL_SECONDS - 1
        _ticker(puller, "VXRT")
        assert yahoo.calls == ["VXRT"]
        clock[0] += 2
        _ticker(puller, "VXRT")
        assert yahoo.calls == ["VXRT", "VXRT"]

    def test_hand_listed_ticker_is_never_cached(self, puller):
        yahoo = _Yahoo({"SPY": _no_expirations_page()})
        puller._yahoo = yahoo
        first = _ticker(puller, "SPY")
        second = _ticker(puller, "SPY")
        assert first["status"] == second["status"] == "SKIPPED"
        assert "no_options" not in first
        assert yahoo.calls == ["SPY", "SPY"]
        assert "SPY" not in options._NO_OPTIONS_UNTIL

    def test_transport_failure_is_not_cached(self, puller):
        yahoo = _Yahoo({"ABCL": None})
        puller._yahoo = yahoo
        assert _ticker(puller, "ABCL")["status"] == "FAILED"
        assert _ticker(puller, "ABCL")["status"] == "FAILED"
        assert yahoo.calls == ["ABCL", "ABCL"]

    def test_no_chain_is_falsy_for_legacy_callers(self):
        assert not options.NO_CHAIN


@pytest.mark.unit
class TestSummaryCountsThemAsCovered:
    def _results(self, items):
        return options.OptionsPullResults(items, full_universe=True)

    def test_success_plus_no_options_is_full_coverage(self):
        r = self._results([
            {"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 5, "snapshots": 5},
            {"ticker": "NUOX", "status": "NO_NEW_DATA", "rows_inserted": 0,
             "reason": "no listed options (cached)", "no_options": True},
        ])
        s = r.summary
        assert s["status"] == "SUCCESS" and s["tickers_no_options"] == 1 and s["error"] is None

    def test_a_real_failure_still_makes_it_partial(self):
        r = self._results([
            {"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 5},
            {"ticker": "QQQ", "status": "FAILED", "rows_inserted": 0, "error": "no data from Yahoo"},
            {"ticker": "NUOX", "status": "NO_NEW_DATA", "rows_inserted": 0, "no_options": True},
        ])
        assert r.summary["status"] == "PARTIAL"

    def test_scheduler_classifies_the_covered_run_as_success(self):
        from ingestion.smart_scheduler import OUTCOME_SUCCESS, _classify_outcome

        r = self._results([
            {"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 5},
            {"ticker": "NUOX", "status": "NO_NEW_DATA", "rows_inserted": 0, "no_options": True},
        ])
        assert _classify_outcome(r)[0] == OUTCOME_SUCCESS

    def test_a_no_options_item_alone_classifies_as_no_new_data(self):
        from ingestion.smart_scheduler import OUTCOME_NO_NEW_DATA, _classify_outcome

        item = {"ticker": "NUOX", "status": "NO_NEW_DATA", "rows_inserted": 0,
                "reason": "no listed options (cached)", "no_options": True}
        assert _classify_outcome(item)[0] == OUTCOME_NO_NEW_DATA
