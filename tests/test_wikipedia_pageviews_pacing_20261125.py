"""Wikipedia_Pageviews: paced requests and a descriptive User-Agent.

2026-11-17..24 every run was PARTIAL: 26 to 37 of the 48 lookups got HTTP 429
from wikimedia.org, sent as ``User-Agent: GRID/1.0`` with no pause. Local fakes
only: no network, no database.
"""

from __future__ import annotations

from datetime import date

import pytest

import ingestion.altdata.wikipedia_text as mod

START, END = date(2026, 10, 1), date(2026, 10, 31)


class _Resp:
    status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"items": [{"timestamp": "2026102000", "views": 7}]}


def _puller() -> mod.WikipediaPuller:
    return object.__new__(mod.WikipediaPuller)          # no engine: only the fetch path is under test


def test_user_agent_names_the_client_and_the_public_repo_and_no_personal_contact():
    ua = mod.USER_AGENT
    assert ua.startswith("GRID-DataPuller/") and "https://github.com/3pacs/GRID" in ua
    assert "@" not in ua and "gmail" not in ua.lower()


def test_every_request_sends_that_user_agent(monkeypatch):
    seen: list[dict] = []
    monkeypatch.setattr(mod.requests, "get", lambda url, **kw: seen.append(kw["headers"]) or _Resp())
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    assert _puller()._fetch_pageviews("Nvidia", START, END) == [{"date": date(2026, 10, 20), "views": 7}]
    assert seen == [{"User-Agent": mod.USER_AGENT}]


def test_requests_of_one_run_are_at_least_the_minimum_interval_apart(monkeypatch):
    clock = {"now": 100.0}
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(mod.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(mod.time, "sleep", fake_sleep)
    monkeypatch.setattr(mod.requests, "get", lambda url, **kw: _Resp())
    puller = _puller()
    for _ in range(48):
        puller._fetch_pageviews("Nvidia", START, END)
    assert len(sleeps) == 47                                    # none before the first request
    assert all(abs(s - mod.MIN_REQUEST_INTERVAL_S) < 1e-9 for s in sleeps)


def test_no_wait_when_the_interval_has_already_passed(monkeypatch):
    clock = {"now": 10.0}
    sleeps: list[float] = []
    monkeypatch.setattr(mod.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(mod.time, "sleep", sleeps.append)
    monkeypatch.setattr(mod.requests, "get", lambda url, **kw: _Resp())
    puller = _puller()
    puller._fetch_pageviews("Nvidia", START, END)
    clock["now"] += 5 * mod.MIN_REQUEST_INTERVAL_S
    puller._fetch_pageviews("Nvidia", START, END)
    assert sleeps == []


def test_a_retry_after_429_is_paced_too_and_the_run_recovers(monkeypatch):
    import requests

    calls = {"n": 0}
    slept: list[float] = []

    class _TooMany(_Resp):
        status_code = 429
        headers = {"Retry-After": "3"}

        def raise_for_status(self) -> None:
            raise requests.HTTPError("429 Client Error: Too Many Requests", response=self)

    def fake_get(url, **kw):
        calls["n"] += 1
        return _TooMany() if calls["n"] == 1 else _Resp()

    monkeypatch.setattr(mod.requests, "get", fake_get)
    monkeypatch.setattr(mod.time, "sleep", slept.append)
    import ingestion.base as base
    monkeypatch.setattr(base.time, "sleep", slept.append, raising=False)
    data = _puller()._fetch_pageviews("Nvidia", START, END)
    assert data == [{"date": date(2026, 10, 20), "views": 7}] and calls["n"] == 2
    assert any(abs(s - 3) < 1e-6 or s >= 3 for s in slept)          # the Retry-After was honoured


# ── a throttle ends or defers the run (Codex review 6061698849) ──────────────
# The retry decorator skips a Retry-After over its 30 s cap, and the next lookup
# used to start one second later against the same API. The run now keeps an
# origin-level cooldown and stops, with honest accounting, instead.

from datetime import timedelta
from typing import Any
from unittest.mock import patch

from tests.test_puller_write_contract_20261105 import FakeEngine, Resp, _bare, _no_sleep, _views

ARTICLES = {f"Entity {i}": f"Article_{i}" for i in range(48)}


def _run(get, watchlist=None, engine=None) -> tuple[dict, list[str]]:
    urls: list[str] = []

    def counted(url: str, **kw: Any):
        urls.append(url)
        return get(url, **kw)

    puller = _bare(mod.WikipediaPuller, engine or FakeEngine())
    with patch.object(mod, "WATCHLIST", watchlist or ARTICLES), \
         patch.object(mod.requests, "get", side_effect=counted), _no_sleep():
        return puller.pull(), urls


def test_a_retry_after_over_the_cap_defers_the_rest_of_the_run_not_one_second_later():
    out, urls = _run(lambda url, **kw: Resp(429, headers={"Retry-After": "90"}))
    assert len(urls) == 1                                           # no second article is requested
    assert out["status"] == "FAILED" and out["rows_inserted"] == 0
    assert out["entities_deferred"] == 48 and "48 of 48 lookups failed" in out["error"]
    assert "deferred" in out["error"]


def test_a_cooldown_within_the_cap_is_waited_out_then_the_run_continues():
    slept: list[float] = []
    state = {"n": 0}

    def get(url: str, **kw: Any):
        state["n"] += 1
        return Resp(429, headers={"Retry-After": "20"}) if state["n"] == 1 else Resp(200, _views(60))

    puller = _bare(mod.WikipediaPuller, FakeEngine())
    with patch.object(mod, "WATCHLIST", {"A": "A", "B": "B"}), patch.object(mod.requests, "get", side_effect=get), \
         _no_sleep(), patch.object(mod.time, "sleep", slept.append):
        out = puller.pull()
    assert out["status"] == "SUCCESS" and out["rows_inserted"] == 120
    assert any(s >= 19 for s in slept)                              # the 20 s cooldown was honoured before the next request


def test_three_throttled_lookups_in_a_row_stop_the_run_with_honest_counts():
    out, urls = _run(lambda url, **kw: Resp(429))
    assert out["status"] == "FAILED" and out["rows_inserted"] == 0
    assert len(urls) == 9                                           # 3 lookups x 3 attempts, then nothing more
    assert out["entities_deferred"] == 45 and "48 of 48 lookups failed" in out["error"]


def test_a_success_between_throttles_resets_the_streak_and_stays_partial():
    def get(url: str, **kw: Any):
        index = int(url.split("Article_")[1].split("/")[0])
        return Resp(429) if index % 3 == 0 else Resp(200, _views(60))

    out, urls = _run(get)
    assert "entities_deferred" not in out                           # never three in a row
    assert out["status"] == "PARTIAL" and out["rows_inserted"] == 60 * 32
    assert "16 of 48 lookups failed" in out["error"]
