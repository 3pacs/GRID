"""GDELT DOC API: one host-wide gate, honest 429 handling (2026-11-09).

Production (grid-svr pull_log, 7 days to 2026-11-08): ``smart:gdelt`` ran 51
times, 34 FAILED, mostly "GDELT DOC API rate-limited (HTTP 429); 3 theme
queries not attempted" with 0 rows. The scheduled puller got 429 on its
*first* request because grid-breaking-news sent ~3,500 DOC requests a day
from the same IP and kept sending into 429s, and nothing coordinated the
five GDELT callers on that host.

These tests pin the shared gate (``ingestion/altdata/gdelt_doc.py``) and the
callers' outcomes. All HTTP is faked; time is a fake clock.
"""

from __future__ import annotations

import fcntl
import os
from datetime import datetime, timezone
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

from ingestion.altdata import gdelt, gdelt_doc
from ingestion.altdata.gdelt_doc import GDELTRateLimited


class Clock:
    """Fake wall clock; sleeping advances it."""

    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        self.sleeps.append(seconds)
        self.now += seconds


class FakeHTTP:
    """``requests``-like GET that answers from a script and takes ``latency_s``."""

    def __init__(self, clock: Clock, statuses: list[int | tuple[int, dict]],
                 latency_s: float = 12.0, body: dict | None = None) -> None:
        self.clock = clock
        self.statuses = list(statuses)
        self.latency_s = latency_s
        self.body = body if body is not None else {"timeline": []}
        self.starts: list[float] = []
        self.params: list[dict] = []

    def get(self, url, params=None, headers=None, timeout=None):
        assert url == gdelt_doc.DOC_API_URL
        self.starts.append(self.clock.now)
        self.params.append(dict(params or {}))
        self.clock.now += self.latency_s
        item = self.statuses.pop(0) if self.statuses else 200
        status, hdrs = item if isinstance(item, tuple) else (item, {})
        resp = MagicMock()
        resp.status_code = status
        resp.headers = hdrs
        resp.json.return_value = self.body
        resp.text = "{}"
        if status >= 400:
            resp.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status}")
        else:
            resp.raise_for_status.return_value = None
        return resp


@pytest.fixture
def clock(monkeypatch) -> Clock:
    c = Clock()
    monkeypatch.setattr(gdelt_doc, "_now", c.time)
    monkeypatch.setattr(gdelt_doc, "_sleep", c.sleep)
    monkeypatch.setattr(gdelt_doc, "_PROCESS_STATE", gdelt_doc._GateState())
    return c


def _state() -> dict:
    import json
    with open(os.environ[gdelt_doc.LOCK_PATH_ENV], encoding="utf-8") as fh:
        return json.loads(fh.read())


def _write_state(**values: float) -> None:
    import json
    path = os.environ[gdelt_doc.LOCK_PATH_ENV]
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        os.write(fd, json.dumps(values).encode())
    finally:
        os.close(fd)


# ── the gate ────────────────────────────────────────────────────────────


def test_requests_are_spaced_from_the_end_of_the_previous_one(clock) -> None:
    http = FakeHTTP(clock, [200, 200, 200], latency_s=12.0)
    for _ in range(3):
        gdelt_doc.doc_get({"query": "q"}, session=http)
    gaps = [b - (a + 12.0) for a, b in zip(http.starts, http.starts[1:])]
    assert all(g >= gdelt_doc.MIN_INTERVAL_S - 1e-9 for g in gaps)
    assert gdelt_doc.MIN_INTERVAL_S >= 5.0  # GDELT: "one every 5 seconds"


def test_429_without_retry_after_cools_the_host_then_resumes(clock) -> None:
    http = FakeHTTP(clock, [429, 200])
    resp = gdelt_doc.doc_get({"query": "q"}, session=http, max_wait_s=120, attempts=2)
    assert resp.status_code == 200
    first_end = http.starts[0] + http.latency_s
    assert http.starts[1] - first_end >= gdelt_doc.BASE_COOLDOWN_S - 1e-9
    assert _state()["strikes"] == 0  # a 200 resets the 429 streak


def test_retry_after_seconds_is_honoured(clock) -> None:
    http = FakeHTTP(clock, [(429, {"Retry-After": "120"}), 200])
    gdelt_doc.doc_get({"query": "q"}, session=http, max_wait_s=200, attempts=2)
    assert http.starts[1] - (http.starts[0] + http.latency_s) >= 120 - 1e-9


def test_retry_after_http_date_and_cap() -> None:
    now = 1_800_000_000.0
    when = format_datetime(datetime.fromtimestamp(now + 300, timezone.utc), usegmt=True)
    assert gdelt_doc.parse_retry_after(when, now) == pytest.approx(300, abs=1)
    assert gdelt_doc.parse_retry_after("garbage", now) is None
    assert gdelt_doc.parse_retry_after("nan", now) is None
    assert gdelt_doc.cooldown_after_429(1, "86400", now) == gdelt_doc.MAX_COOLDOWN_S
    assert gdelt_doc.cooldown_after_429(1, "0", now) == gdelt_doc.MIN_INTERVAL_S


def test_consecutive_429s_double_the_cooldown_up_to_the_cap() -> None:
    base = gdelt_doc.BASE_COOLDOWN_S
    assert [gdelt_doc.cooldown_after_429(k, None, 0.0) for k in (1, 2, 3)] == [base, 2 * base, 4 * base]
    assert gdelt_doc.cooldown_after_429(50, None, 0.0) == gdelt_doc.MAX_COOLDOWN_S


def test_retry_that_does_not_fit_the_budget_raises_after_one_request(clock) -> None:
    http = FakeHTTP(clock, [429, 200])
    with pytest.raises(GDELTRateLimited) as caught:
        gdelt_doc.doc_get({"query": "q"}, session=http, max_wait_s=30, attempts=2)
    assert caught.value.sent is True and caught.value.retry_at is not None
    assert len(http.starts) == 1  # nothing sent into the cooldown
    assert _state()["blocked_until"] == pytest.approx(
        http.starts[0] + http.latency_s + gdelt_doc.BASE_COOLDOWN_S, abs=0.01)


def test_cooldown_is_shared_by_every_caller(clock) -> None:
    # One caller's 429 ...
    first = FakeHTTP(clock, [429])
    with pytest.raises(GDELTRateLimited):
        gdelt_doc.doc_get({"query": "a"}, session=first, max_wait_s=10, attempts=1)
    # ... stops another caller (another process reads the same file) from sending.
    second = FakeHTTP(clock, [200])
    with pytest.raises(GDELTRateLimited) as caught:
        gdelt_doc.doc_get({"query": "b"}, session=second, max_wait_s=10)
    assert caught.value.sent is False and second.starts == []
    assert "cooling down" in str(caught.value)


def test_a_caller_in_another_process_holding_the_slot_defers_without_sending(clock) -> None:
    http = FakeHTTP(clock, [200])
    holder = os.open(os.environ[gdelt_doc.LOCK_PATH_ENV], os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(holder, fcntl.LOCK_EX)  # a separate open file description: flock conflicts
        with pytest.raises(GDELTRateLimited) as caught:
            gdelt_doc.doc_get({"query": "q"}, session=http, max_wait_s=2)
    finally:
        fcntl.flock(holder, fcntl.LOCK_UN)
        os.close(holder)
    assert caught.value.sent is False and http.starts == []


def test_low_priority_waits_longer_so_a_scheduled_puller_goes_first() -> None:
    state = gdelt_doc._GateState(last_end=1000.0)
    assert gdelt_doc.next_slot_at(state, "normal") == 1000.0 + gdelt_doc.MIN_INTERVAL_S
    assert gdelt_doc.next_slot_at(state, "low") > gdelt_doc.next_slot_at(state, "normal")
    with pytest.raises(ValueError):
        gdelt_doc.doc_get({"query": "q"}, priority="urgent")


def test_world_writable_gate_file_is_not_trusted_but_still_paces(clock) -> None:
    path = os.environ[gdelt_doc.LOCK_PATH_ENV]
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    os.close(fd)
    os.chmod(path, 0o666)
    http = FakeHTTP(clock, [200, 200])
    gdelt_doc.doc_get({"query": "q"}, session=http)
    gdelt_doc.doc_get({"query": "q"}, session=http)
    assert http.starts[1] - (http.starts[0] + http.latency_s) >= gdelt_doc.MIN_INTERVAL_S - 1e-9


# ── the scheduled puller ────────────────────────────────────────────────

_TIMELINE = {"timeline": [{"series": "Average Tone",
                           "data": [{"date": "20261108120000", "value": -1.5}]}]}


@pytest.fixture
def puller(monkeypatch):
    with patch("ingestion.altdata.gdelt.os.makedirs"):
        p = gdelt.GDELTPuller.__new__(gdelt.GDELTPuller)
    conn = MagicMock()
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    p.engine, p.source_id = engine, 43
    monkeypatch.setattr(p, "_row_exists", lambda *a, **k: False)
    return p, conn


def _bounded(p, budget: float = 150):
    return p.pull_recent(days_back=1, max_theme_queries=4, include_actor_tones=False,
                         include_tensions=False, include_signals=False, run_budget_s=budget)


@pytest.fixture
def mono(monkeypatch, clock):
    """Drive the puller's run budget off the same fake clock."""
    monkeypatch.setattr(gdelt, "time", SimpleNamespace(monotonic=clock.time, sleep=clock.sleep))
    return clock


def test_run_resumes_after_a_429_instead_of_abandoning_its_queries(puller, mono, monkeypatch) -> None:
    from ingestion.smart_scheduler import _classify_outcome

    p, _ = puller
    http = FakeHTTP(mono, [429, 200, 200, 200, 200], latency_s=12.0, body=_TIMELINE)
    monkeypatch.setattr(gdelt_doc, "requests", SimpleNamespace(get=http.get, RequestException=requests.RequestException))
    result = _bounded(p, budget=400)
    assert len(http.starts) == 5  # the 429'd query was retried, then the other three
    assert result["errors"] == [] and result["status"] == "SUCCESS"
    assert result["total_rows"] == 4
    assert _classify_outcome(result)[0] == "SUCCESS"


def test_host_cooling_down_before_the_run_is_a_clean_deferral(puller, mono, monkeypatch) -> None:
    from ingestion.smart_scheduler import _classify_outcome

    p, conn = puller
    _write_state(last_end=mono.now - 30, blocked_until=mono.now + 600, strikes=3)
    http = FakeHTTP(mono, [200])
    monkeypatch.setattr(gdelt_doc, "requests", SimpleNamespace(get=http.get, RequestException=requests.RequestException))
    result = _bounded(p)
    assert http.starts == []  # nothing was sent into another caller's cooldown
    assert result["status"] == "DEFERRED" and result["total_rows"] == 0
    assert "4 DOC queries not attempted" in result["reason"]
    assert "nothing was sent" in result["reason"]
    outcome, rows, _note = _classify_outcome(result)
    assert (outcome, rows) == ("SKIPPED", 0)  # SmartScheduler: no pull_log FAILED row
    assert not conn.execute.called


def test_429_after_some_rows_is_partial(puller, mono, monkeypatch) -> None:
    from ingestion.smart_scheduler import _classify_outcome

    p, _ = puller
    http = FakeHTTP(mono, [200, 429, 429], latency_s=12.0, body=_TIMELINE)
    monkeypatch.setattr(gdelt_doc, "requests", SimpleNamespace(get=http.get, RequestException=requests.RequestException))
    result = _bounded(p, budget=150)
    assert result["status"] == "PARTIAL" and result["total_rows"] == 1
    assert "HTTP 429) on 1 DOC queries" in result["errors"][0]
    # the refused query moved on; the next found the doubled cooldown past the budget
    assert "2 DOC queries not attempted" in result["errors"][0]
    assert result["doc_queries_refused"] == 1
    assert _classify_outcome(result)[:2] == ("PARTIAL", 1)


def test_long_run_waits_out_the_doubled_cooldown_and_resumes(puller, mono, monkeypatch) -> None:
    # grid-scheduler's daily run has a 600 s budget: a refused query no
    # longer ends the run; the next one waits out the 120 s cooldown.
    p, _ = puller
    http = FakeHTTP(mono, [429, 429, 200, 200, 200], latency_s=12.0, body=_TIMELINE)
    monkeypatch.setattr(gdelt_doc, "requests", SimpleNamespace(get=http.get, RequestException=requests.RequestException))
    result = _bounded(p, budget=600)
    assert len(http.starts) == 5
    assert http.starts[2] - (http.starts[1] + 12.0) >= 2 * gdelt_doc.BASE_COOLDOWN_S - 1e-9
    assert result["status"] == "PARTIAL" and result["total_rows"] == 3
    assert result["doc_queries_refused"] == 1 and result["doc_queries_not_attempted"] == 0


def test_every_request_refused_is_failed(puller, mono, monkeypatch) -> None:
    from ingestion.smart_scheduler import _classify_outcome

    p, _ = puller
    http = FakeHTTP(mono, [429, 429], latency_s=12.0)
    monkeypatch.setattr(gdelt_doc, "requests", SimpleNamespace(get=http.get, RequestException=requests.RequestException))
    result = _bounded(p, budget=150)
    assert len(http.starts) == 2  # first try + one retry after the cooldown, then stop
    assert result["status"] == "FAILED" and result["total_rows"] == 0
    assert _classify_outcome(result)[0] == "FAILED"


def test_rate_limit_stop_also_skips_actor_and_tension_queries(puller, mono, monkeypatch) -> None:
    p, _ = puller
    http = FakeHTTP(mono, [429, 429], latency_s=12.0)
    monkeypatch.setattr(gdelt_doc, "requests", SimpleNamespace(get=http.get, RequestException=requests.RequestException))
    result = p.pull_recent(days_back=2, max_theme_queries=2, include_signals=False, run_budget_s=150)
    n_doc = 2 + len(gdelt.GDELT_ACTOR_QUERIES) + len(gdelt.GDELT_TENSION_PAIRS)
    assert len(http.starts) == 2
    assert result["doc_queries_refused"] == 1
    assert result["doc_queries_not_attempted"] == n_doc - 1


def test_scheduler_timeout_fits_four_queries_with_cooldown_retries() -> None:
    import inspect

    from ingestion.smart_scheduler import PULLER_REGISTRY

    entry = next(e for e in PULLER_REGISTRY if e["name"] == "gdelt")
    # SmartScheduler hands pull_recent its cooperative deadline_at.
    assert "deadline_at" in inspect.signature(gdelt.GDELTPuller.pull_recent).parameters
    # four queries at ~12 s each, each answered after one 60 s cooldown retry
    per_query = 2 * 12 + gdelt_doc.BASE_COOLDOWN_S
    assert entry["timeout_s"] >= 4 * per_query


def test_deadline_at_bounds_the_run(puller, mono, monkeypatch) -> None:
    p, _ = puller
    http = FakeHTTP(mono, [200, 200, 200, 200], latency_s=12.0, body=_TIMELINE)
    monkeypatch.setattr(gdelt_doc, "requests", SimpleNamespace(get=http.get, RequestException=requests.RequestException))
    result = p.pull_recent(days_back=1, max_theme_queries=4, include_actor_tones=False,
                           include_tensions=False, include_signals=False,
                           deadline_at=mono.now + 80)
    # 80 s minus the 45 s request reserve leaves room for two queries only
    assert len(http.starts) == 2
    assert result["status"] == "PARTIAL" and result["doc_queries_not_attempted"] == 2


# ── the breaking-news monitor and the spider share the gate ─────────────


def test_breaking_news_429_raises_and_cycle_stops_without_sending_more(clock, monkeypatch) -> None:
    from intelligence import breaking_news

    http = FakeHTTP(clock, [200, 429, 200, 200])
    monkeypatch.setattr(gdelt_doc, "requests", SimpleNamespace(get=http.get, RequestException=requests.RequestException))
    monkeypatch.setattr(breaking_news, "time", SimpleNamespace(
        time=clock.time, monotonic=clock.time, sleep=clock.sleep))
    with patch("db.get_engine", return_value=MagicMock()):
        breaking_news.run_monitor(interval=60, once=True)
    assert len(http.starts) == 2  # stopped at the 429 instead of sending 10 more
    gap = http.starts[1] - (http.starts[0] + http.latency_s)
    assert gap >= breaking_news.GDELT_REQUEST_SPACING - 1e-9  # low priority spacing


def test_breaking_news_check_gdelt_uses_low_priority_gate(monkeypatch) -> None:
    from intelligence import breaking_news

    seen = {}

    def fake_doc_get(params, **kwargs):
        seen.update(kwargs)
        raise GDELTRateLimited("cooling", sent=False, retry_at=None)

    monkeypatch.setattr(gdelt_doc, "doc_get", fake_doc_get)
    with pytest.raises(GDELTRateLimited):
        breaking_news.check_gdelt("tariff OR trade war")
    assert seen["priority"] == "low" and seen["attempts"] == 1


def test_spider_skips_actor_while_gdelt_is_throttled(monkeypatch) -> None:
    from intelligence.spider.sources.news_cooccurrence import NewsCooccurrenceAdapter

    monkeypatch.setattr(gdelt_doc, "doc_get", MagicMock(
        side_effect=GDELTRateLimited("cooling", sent=False, retry_at=None)))
    assert NewsCooccurrenceAdapter().discover("Jerome Powell", {}) == []
