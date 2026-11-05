"""UNRUN fake-clock/session regressions; never contact providers or a DB."""
from __future__ import annotations

import ast
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest
import requests

from ingestion import base
from ingestion.altdata import actor_news_puller as news


ACTORS = [{"actor_id": name, "name": name.title()} for name in ("alpha", "beta", "gamma")]


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []
        self.oversleep = 0.0

    def monotonic(self):
        return self.now

    def time(self):
        return 1000.0 + self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds + self.oversleep


class Response:
    def __init__(self, source, status=200, retry_after="1200"):
        self.status_code = status
        self.headers = {"Retry-After": retry_after}
        self.source = source
        self.text = "{}"
        self.content = b"<rss><channel><item><title>Actor news</title><link>https://example.test/news</link></item></channel></rss>"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("fixture HTTP " + str(self.status_code), response=self)

    def json(self):
        if self.source == "gdelt":
            return {"articles": [{"title": "GDELT news", "url": "https://example.test/gdelt"}]}
        if self.source == "wikipedia":
            return {"extract": "A short biography.", "content_urls": {"desktop": {"page": "https://example.test/bio"}}}
        return {}


class Session:
    def __init__(self, clock, statuses, on_get=None):
        self.headers = {}
        self.clock, self.statuses, self.on_get = clock, list(statuses), on_get
        self.calls = []

    def get(self, url, timeout):
        source = next(name for fragment, name in (
            ("news.google.com", "google_news"), ("api.gdeltproject.org", "gdelt"),
            ("en.wikipedia.org", "wikipedia"), ("efts.sec.gov", "sec_edgar"),
            ("api.crossref.org", "crossref")) if fragment in url)
        self.calls.append((source, timeout))
        if self.on_get:
            self.on_get(source, self.clock)
        status = self.statuses.pop(0) if source == "gdelt" and self.statuses else 200
        return Response(source, status)


def make_puller(monkeypatch, statuses=(429,), on_get=None):
    clock = Clock()
    session = Session(clock, statuses, on_get)
    monkeypatch.setattr(news, "time", clock)
    monkeypatch.setattr(base, "time", clock)  # any mistaken decorator retry cannot sleep in real time
    monkeypatch.setattr(news.requests, "Session", lambda: session)
    puller = news.ActorNewsPuller(object())
    writes, bios = [], []
    monkeypatch.setattr(puller, "_insert_news_rows", lambda rows: writes.append(list(rows)))
    monkeypatch.setattr(puller, "_upsert_bio", lambda bio: bios.append(bio))
    return puller, clock, session, writes, bios


def test_first_scheduled_429_stops_entire_batch_without_retry_sleep(monkeypatch):
    puller, clock, session, writes, bios = make_puller(monkeypatch)
    result = puller.pull_batch(ACTORS[:2], ["google_news", "gdelt", "wikipedia"], budget_seconds=30)
    assert result["stop_reason"] == "gdelt_rate_limited"
    assert result["retry_after"] == "1200"
    assert result["actors_attempted"] == 1 and result["actors_completed"] == 0
    assert result["deferred_actor_ids"] == ["alpha", "beta"]
    assert result["failed_actor_ids"] == []
    assert result["source_rows_fetched"] == {"google_news": 1}
    partial = result["incomplete_actors"][0]
    assert partial["source_rows_fetched"] == {"google_news": 1}
    assert partial["news_write_attempted"] is partial["news_write_completed"] is False
    assert partial["bio_write_attempted"] is partial["bio_write_completed"] is False
    assert session.calls == [("google_news", 20), ("gdelt", 20)]
    assert clock.sleeps == [] and writes == [] and bios == []


def test_late_429_keeps_completed_actor_and_defers_current_and_tail(monkeypatch):
    puller, clock, session, writes, bios = make_puller(monkeypatch, statuses=(200, 429))
    result = puller.pull_batch(ACTORS, ["google_news", "gdelt", "wikipedia"], budget_seconds=30)
    assert result["actors_requested"] == 3 and result["actors_attempted"] == 2
    assert result["actors_completed"] == 1
    assert result["deferred_actor_ids"] == ["beta", "gamma"]
    assert result["source_rows_fetched"] == {"google_news": 2, "gdelt": 1, "wikipedia": 1}
    assert result["incomplete_actors"][0]["actor_id"] == "beta"
    assert len(writes) == 1 and len(writes[0]) == 2 and len(bios) == 1
    assert [source for source, _ in session.calls] == ["google_news", "gdelt", "wikipedia", "google_news", "gdelt"]
    assert all(seconds <= 2 for seconds in clock.sleeps)  # throttle only, no Retry-After wait
    assert puller._batch_deadline is None and puller._batch_progress is None


def test_exact_deadline_after_fetch_defers_before_next_source_and_write(monkeypatch):
    def advance(source, clock):
        if source == "google_news":
            clock.now += 5
    puller, _, session, writes, _ = make_puller(monkeypatch, on_get=advance)
    result = puller.pull_batch(ACTORS[:2], ["google_news", "gdelt"], budget_seconds=5)
    assert result["stop_reason"] == "budget_exhausted"
    assert result["actors_completed"] == 0 and result["deferred_actor_ids"] == ["alpha", "beta"]
    assert result["source_rows_fetched"] == {"google_news": 1}
    assert session.calls == [("google_news", 20)] and writes == []


def test_throttle_wait_equal_to_remaining_budget_does_not_sleep_or_request(monkeypatch):
    puller, clock, session, _, _ = make_puller(monkeypatch)
    puller._last_call["gdelt"] = clock.time()
    result = puller.pull_batch(ACTORS[:1], ["gdelt"], budget_seconds=2)
    assert result["stop_reason"] == "budget_exhausted"
    assert clock.sleeps == [] and session.calls == []


def test_throttle_oversleep_checks_budget_before_http(monkeypatch):
    puller, clock, session, _, _ = make_puller(monkeypatch)
    puller._last_call["gdelt"] = clock.time()
    clock.oversleep = 5
    result = puller.pull_batch(ACTORS[:1], ["gdelt"], budget_seconds=3)
    assert result["stop_reason"] == "budget_exhausted"
    assert clock.sleeps == [2] and session.calls == []


def test_expiry_after_news_write_keeps_partial_write_and_skips_bio(monkeypatch):
    puller, clock, session, writes, bios = make_puller(monkeypatch)
    def write(rows):
        writes.append(list(rows))
        clock.now += 5
    monkeypatch.setattr(puller, "_insert_news_rows", write)
    result = puller.pull_batch(ACTORS[:1], ["google_news", "wikipedia"], budget_seconds=5)
    assert result["actors_completed"] == 0 and result["stop_reason"] == "budget_exhausted"
    partial = result["incomplete_actors"][0]
    assert partial["news_write_attempted"] is partial["news_write_completed"] is True
    assert partial["bio_write_attempted"] is False
    assert len(writes) == 1 and bios == []
    assert session.calls == [("google_news", 20)]


def test_expiry_after_last_news_fetch_skips_unstarted_write(monkeypatch):
    puller, _, _, writes, _ = make_puller(monkeypatch, on_get=lambda source, clock: setattr(clock, "now", 5))
    result = puller.pull_batch(ACTORS[:1], ["google_news"], budget_seconds=5)
    assert result["stop_reason"] == "budget_exhausted" and writes == []
    assert result["incomplete_actors"][0]["news_write_attempted"] is False
    assert result["source_rows_fetched"] == {"google_news": 1}


def test_bio_fetched_at_deadline_is_not_written(monkeypatch):
    puller, _, session, _, bios = make_puller(monkeypatch, on_get=lambda source, clock: setattr(clock, "now", 5))
    result = puller.pull_batch(ACTORS[:1], ["wikipedia"], budget_seconds=5)
    assert result["stop_reason"] == "budget_exhausted" and bios == []
    assert result["source_rows_fetched"] == {"wikipedia": 1}
    assert result["incomplete_actors"][0]["bio_write_attempted"] is False
    assert session.calls == [("wikipedia", 20)]


def test_inflight_write_can_overrun_then_next_actor_is_deferred(monkeypatch):
    puller, clock, _, _, bios = make_puller(monkeypatch)
    def write(bio):
        bios.append(bio)
        clock.now += 50
    monkeypatch.setattr(puller, "_upsert_bio", write)
    result = puller.pull_batch(ACTORS[:2], ["wikipedia"], budget_seconds=5)
    assert clock.now == 50  # cooperative target, explicitly not hard cancellation
    assert result["actors_attempted"] == result["actors_completed"] == 1
    assert result["deferred_actor_ids"] == ["beta"] and result["incomplete_actors"] == []
    assert result["stop_reason"] == "budget_exhausted" and len(bios) == 1


def test_standalone_legacy_429_still_returns_counts_and_continues(monkeypatch):
    puller, clock, session, writes, bios = make_puller(monkeypatch)
    result = puller.pull_one_actor(ACTORS[0], ["google_news", "gdelt", "wikipedia"])
    assert result == {"google_news": 1, "gdelt": 0, "wikipedia": 1}
    assert [source for source, _ in session.calls] == ["google_news", "gdelt", "wikipedia"]
    assert len(writes) == len(bios) == 1 and clock.sleeps == []


def test_batch_yield_resets_state_without_claiming_durable_cooldown(monkeypatch):
    puller, _, session, _, _ = make_puller(monkeypatch, statuses=(429, 429))
    assert puller.pull_batch(ACTORS[:1], ["gdelt"], budget_seconds=30)["stop_reason"] == "gdelt_rate_limited"
    assert puller._batch_deadline is None and puller._batch_progress is None
    assert puller.pull_one_actor(ACTORS[0], ["gdelt"]) == {"gdelt": 0}
    assert session.calls == [("gdelt", 20), ("gdelt", 20)]


def test_actor_error_does_not_fabricate_completion_or_stop_unrelated_actor(monkeypatch):
    puller, _, _, writes, _ = make_puller(monkeypatch)
    fetch = puller._fetch_google_news
    def fail_one(actor_id, query):
        if actor_id == "alpha":
            raise ValueError("fixture actor failure")
        return fetch(actor_id, query)
    monkeypatch.setattr(puller, "_fetch_google_news", fail_one)
    result = puller.pull_batch(ACTORS[:2], ["google_news"], budget_seconds=30)
    assert result["actors_attempted"] == 2 and result["actors_completed"] == 1
    assert result["failed_actor_ids"] == ["alpha"] and result["deferred_actor_ids"] == []
    assert result["incomplete_actors"][0]["reason"] == "actor_error"
    assert result["source_rows_fetched"] == {"google_news": 1} and len(writes) == 1


@pytest.mark.parametrize("budget", [0, -1, float("nan"), float("inf"), True, "30", None])
def test_invalid_budget_rejects_without_io(monkeypatch, budget):
    puller, _, session, _, _ = make_puller(monkeypatch)
    with pytest.raises(ValueError):
        puller.pull_batch(ACTORS, ["gdelt"], budget_seconds=budget)
    assert session.calls == [] and puller._batch_deadline is None


def test_reentrant_batch_rejects_without_resetting_active_deadline(monkeypatch):
    puller, _, session, _, _ = make_puller(monkeypatch)
    puller._batch_deadline = 10
    with pytest.raises(RuntimeError, match="already active"):
        puller.pull_batch(ACTORS, ["gdelt"], budget_seconds=5)
    assert puller._batch_deadline == 10 and session.calls == []


def test_empty_batch_is_completed_and_state_cleared(monkeypatch):
    puller, _, session, _, _ = make_puller(monkeypatch)
    result = puller.pull_batch([], ["gdelt"], budget_seconds=5)
    assert result["actors_requested"] == result["actors_attempted"] == result["actors_completed"] == 0
    assert result["stop_reason"] == "completed" and result["source_rows_fetched"] == {}
    assert puller._batch_deadline is None and puller._batch_progress is None and session.calls == []


@pytest.mark.parametrize("job,priority,size,sources", [
    ("_actor_news_top200", True, 200, ["google_news", "gdelt", "wikipedia"]),
    ("_actor_news_weekly_tail", False, 5, ["google_news", "wikipedia", "sec_edgar", "crossref"]),
])
def test_actual_scheduler_wrappers_use_budget_and_fetched_counts(monkeypatch, job, priority, size, sources):
    source = Path(__file__).resolve().parents[1] / "intelligence" / "scheduler.py"
    tree = ast.parse(source.read_text())
    loop = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_intelligence_loop")
    function = next(n for n in loop.body if isinstance(n, ast.FunctionDef) and n.name == job)
    constant = next(n for n in tree.body if isinstance(n, ast.Assign) and
                    any(isinstance(t, ast.Name) and t.id == "_ACTOR_NEWS_BATCH_BUDGET_SECONDS" for t in n.targets))
    budget = ast.literal_eval(constant.value)
    assert budget == 30.0
    calls, logs = [], []
    actors = [{"actor_id": str(i), "name": str(i)} for i in range(205)]
    def enumerate_actors(*, priority_only):
        assert priority_only is priority
        return actors
    engine = object()
    class Puller:
        def __init__(self, actual_engine):
            assert actual_engine is engine
        def pull_batch(self, chosen, sources, *, budget_seconds):
            calls.append((chosen, sources, budget_seconds))
            return {"actors_requested": len(chosen), "actors_completed": 1,
                    "source_rows_fetched": {"google_news": 2, "wikipedia": 1},
                    "deferred_actor_ids": ["x", "y"], "failed_actor_ids": ["z"],
                    "stop_reason": "budget_exhausted"}
    db = ModuleType("db")
    db.get_engine = lambda: engine
    monkeypatch.setitem(sys.modules, "db", db)
    monkeypatch.setattr(news, "ActorNewsPuller", Puller)
    monkeypatch.setattr(news, "enumerate_sector_map_actors", enumerate_actors)
    namespace = {"_ACTOR_NEWS_BATCH_BUDGET_SECONDS": budget,
                 "log": SimpleNamespace(info=lambda *a, **kw: logs.append(kw),
                                        warning=lambda *a, **kw: pytest.fail("unexpected wrapper exception"))}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
    namespace[job]()
    assert len(calls) == 1 and len(calls[0][0]) == size
    assert calls[0][1:] == (sources, 30.0)
    assert logs[0]["n"] == 2 and logs[0]["b"] == 1
    assert logs[0]["a"] == 1 and logs[0]["d"] == 2 and logs[0]["f"] == 1


def test_registration_and_held_job_gates_remain_exact():
    source = Path(__file__).resolve().parents[1] / "intelligence" / "scheduler.py"
    loop = next(n for n in ast.parse(source.read_text()).body
                if isinstance(n, ast.FunctionDef) and n.name == "run_intelligence_loop")
    statements = {ast.unparse(n) for n in loop.body}
    assert "_sched.every().day.at('03:30').do(_actor_news_top200)" in statements
    assert "_sched.every().sunday.at('04:00').do(_actor_news_weekly_tail)" in statements
    assert "_sched.every(1).hours.do(_hourly_briefing)" in statements
    for flag, registration in (
        ("GRID_ENABLE_LEGACY_PAPER_TRADING_JOB", "_sched.every(1).hours.do(_paper_trading_signals)"),
        ("GRID_ENABLE_BULK_HYPOTHESIS_JOB", "_sched.every().day.at('02:45').do(_nightly_research)"),
        ("GRID_ENABLE_SCANNER_WEIGHTS_JOB", "_sched.every(7).days.do(_options_tracker)"),
    ):
        gates = [n for n in loop.body if isinstance(n, ast.If) and
                 ast.unparse(n.test) == f"getattr(_s, '{flag}', False)"]
        assert len(gates) == 1 and registration in {ast.unparse(n) for n in gates[0].body}
        assert registration not in statements
