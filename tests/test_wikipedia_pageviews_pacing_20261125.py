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
