"""WorldNewsAPI 402: one warning per cooldown, no zero rows, no wasted requests.

grid-hermes, 24 h to 2026-11-04: 105 x "WorldNewsAPI returned 402 (Payment
Required)". _search_news turned the 402 into ``{"news": []}``, so the run kept
requesting and pull_day stored the empty result as SUCCESS zeros (griddb:
wn_total_volume latest vintages 0 for 2026-11-01..03). No network here.
"""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from loguru import logger as log

from ingestion.altdata import world_news
from ingestion.altdata.world_news import WorldNewsPaymentRequired, WorldNewsPuller


class _Resp:
    def __init__(self, status: int, body: dict | None = None) -> None:
        self.status_code = status
        self._body = body or {"news": [{"sentiment": 0.1, "source_country": "us"}]}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        return self._body


@pytest.fixture
def puller(monkeypatch):
    monkeypatch.setattr(world_news, "_payment_required_until", 0.0)
    monkeypatch.setattr(world_news.time, "sleep", lambda *_: None)
    obj = WorldNewsPuller.__new__(WorldNewsPuller)
    obj.api_key = "test-key"
    obj._api_expired = False
    obj.source_id = 1
    obj.engine = MagicMock()
    obj.inserted = []
    monkeypatch.setattr(obj, "_ensure_features", lambda: None)
    monkeypatch.setattr(obj, "_row_exists", lambda *a, **k: False)
    monkeypatch.setattr(obj, "_insert_raw", lambda **kw: obj.inserted.append(kw))
    monkeypatch.setattr(obj, "_captured_days", lambda *_a: set())
    return obj


@pytest.fixture
def warnings_captured():
    records: list[str] = []
    sink = log.add(lambda m: records.append(m.record["message"]), level="WARNING")
    yield records
    log.remove(sink)


def _http(monkeypatch, statuses: list[int]) -> list[int]:
    calls: list[int] = []

    def fake_get(*_a, **_k):
        status = statuses[min(len(calls), len(statuses) - 1)]
        calls.append(status)
        return _Resp(status)

    monkeypatch.setattr(world_news.requests, "get", fake_get)
    return calls


@pytest.mark.unit
class TestPaymentRequired:
    def test_402_raises_not_an_empty_article_list(self, puller, monkeypatch):
        _http(monkeypatch, [402])
        with pytest.raises(WorldNewsPaymentRequired):
            puller._search_news("q", "2026-11-01", "2026-11-01")

    def test_402_is_not_retried(self, puller, monkeypatch):
        calls = _http(monkeypatch, [402])
        with pytest.raises(WorldNewsPaymentRequired):
            puller._search_news("q", "2026-11-01", "2026-11-01")
        assert calls == [402]

    def test_first_402_stops_the_run_and_writes_nothing_for_that_day(
        self, puller, monkeypatch, warnings_captured,
    ):
        # Day 1: all 10 categories succeed. Day 2: the 3rd request is a 402.
        calls = _http(monkeypatch, [200] * 12 + [402])
        results = puller.pull_all(days_back=3)
        assert len(calls) == 13                       # no request after the 402
        assert results[-1]["status"] == "FAILED"
        assert "402" in results[-1]["error"] and "renew" in results[-1]["error"]
        assert len(results) == 2                      # day 3 never attempted
        # Day 1 wrote its 30 category rows + 3 composites. Day 2's two good
        # categories were inserted inside the day's transaction, which the
        # 402 rolls back (engine.begin's __exit__ receives the exception).
        assert results[0]["rows_inserted"] == 33
        exits = puller.engine.begin.return_value.__exit__.call_args_list
        assert exits[-1].args[0] is WorldNewsPaymentRequired
        assert sum(1 for w in warnings_captured if "402" in w) == 1

    def test_breaker_holds_later_runs_without_requests(self, puller, monkeypatch, warnings_captured):
        calls = _http(monkeypatch, [402])
        puller.pull_all(days_back=1)
        assert calls == [402]
        fresh = WorldNewsPuller.__new__(WorldNewsPuller)  # the scheduler builds a new one per run
        fresh.api_key, fresh._api_expired = "test-key", False
        out = fresh.pull_all(days_back=7)
        assert out == [world_news._payment_required_result()]
        assert calls == [402]
        assert sum(1 for w in warnings_captured if "402" in w) == 1

    def test_breaker_reopens_after_cooldown(self, puller, monkeypatch):
        clock = [5000.0]
        monkeypatch.setattr(world_news.time, "monotonic", lambda: clock[0])
        calls = _http(monkeypatch, [402, 200])
        puller.pull_all(days_back=1)
        clock[0] += world_news.PAYMENT_REQUIRED_COOLDOWN_SECONDS + 1
        fresh = WorldNewsPuller.__new__(WorldNewsPuller)
        fresh.__dict__.update({k: v for k, v in puller.__dict__.items() if k != "_api_expired"})
        fresh._api_expired = False
        out = fresh.pull_all(days_back=1)
        assert len(calls) == 11 and out[0]["rows_inserted"] == 33

    def test_scheduler_sees_failed(self, puller, monkeypatch):
        from ingestion.smart_scheduler import OUTCOME_FAILED, _classify_outcome

        _http(monkeypatch, [402])
        assert _classify_outcome(puller.pull_all(days_back=2))[0] == OUTCOME_FAILED


@pytest.mark.unit
class TestCapturedDaysAreNotRepulled:
    def test_captured_days_skip_requests(self, puller, monkeypatch):
        calls = _http(monkeypatch, [200])
        yesterday = date.fromordinal(date.today().toordinal() - 1)
        before = date.fromordinal(yesterday.toordinal() - 1)
        monkeypatch.setattr(puller, "_captured_days", lambda *_a: {before})
        results = puller.pull_all(days_back=2)
        assert results[0] == {"date": before.isoformat(), "status": "UNCHANGED",
                              "rows_inserted": 0, "reason": "day already captured"}
        assert len(calls) == 10  # only yesterday's 10 categories

    def test_zero_latest_vintage_is_not_captured(self, monkeypatch):
        obj = WorldNewsPuller.__new__(WorldNewsPuller)
        obj.engine = MagicMock()
        d1, d2 = date(2026, 11, 1), date(2026, 11, 2)
        rows = [SimpleNamespace(obs_date=d1, value=0.0), SimpleNamespace(obs_date=d2, value=411.0)]
        seen = {}

        def fake_read_window(conn, series_id, **kw):
            seen.update(kw, series_id=series_id)
            return rows

        monkeypatch.setattr(world_news, "read_window", fake_read_window)
        assert obj._captured_days(d1, d2) == {d2}
        assert seen == {"series_id": "wn_total_volume", "source": "WorldNewsAPI",
                        "start": d1, "as_of": d2}

    def test_read_failure_skips_nothing(self, monkeypatch):
        obj = WorldNewsPuller.__new__(WorldNewsPuller)
        obj.engine = MagicMock()
        obj.engine.connect.side_effect = RuntimeError("db down")
        assert obj._captured_days(date(2026, 11, 1), date(2026, 11, 2)) == set()
