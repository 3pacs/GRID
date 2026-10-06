"""FRED transport failures: no failure row dated today, a real read timeout,
and a retry budget that fits the scheduler's job deadline.

Through September 2026 the FRED job at 16:02Z/20:01Z wrote ``FAILED value=0``
rows for *today's* VIXCLS/T10Y2Y/DFF with payload ``RetryError[ReadTimeout]``
(fedfred's hard-coded 10 s timeout), logged as an application ERROR.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest
import tenacity
from loguru import logger

from ingestion import fred


def _retry_error(inner: BaseException) -> tenacity.RetryError:
    """What fedfred raises after its three tenacity attempts."""
    attempt = tenacity.Future(attempt_number=3)
    attempt.set_exception(inner)
    return tenacity.RetryError(attempt)


def _puller(monkeypatch, raise_exc: BaseException):
    puller = fred.FREDPuller.__new__(fred.FREDPuller)
    puller.engine, puller.source_id = MagicMock(), 1

    def fetch(_sid, **_kwargs):
        raise raise_exc

    puller.fred = SimpleNamespace(get_series_observations=fetch)
    monkeypatch.setattr(fred.time, "sleep", lambda *_a: None)
    return puller


def _levels(fn) -> list[tuple[str, str]]:
    records: list[tuple[str, str]] = []
    sink = logger.add(lambda m: records.append((m.record["level"].name, m.record["message"])),
                      level="WARNING")
    try:
        fn()
    finally:
        logger.remove(sink)
    return records


@pytest.mark.unit
@pytest.mark.parametrize("exc", [
    _retry_error(httpx.ReadTimeout("FRED slow")),
    _retry_error(httpx.ConnectError("reset")),
    httpx.ReadTimeout("bare timeout"),
    _retry_error(TimeoutError("socket")),
])
def test_transport_failure_is_skipped_without_a_failure_row(monkeypatch, exc):
    puller = _puller(monkeypatch, exc)
    with patch.object(puller, "_record_failure") as record_failure:
        records = _levels(lambda: setattr(puller, "_out", puller.pull_series("VIXCLS")))

    out = puller._out
    assert out["status"] == "SKIPPED"
    assert out["rows_inserted"] == 0
    assert any(e.startswith("transient ") for e in out["errors"])
    record_failure.assert_not_called()
    puller.engine.connect.assert_not_called()
    levels = {lvl for lvl, _ in records}
    assert "WARNING" in levels and "ERROR" not in levels, records
    assert "VIXCLS" in " ".join(msg for _, msg in records)


@pytest.mark.unit
def test_non_transport_bug_still_fails_with_a_failure_row(monkeypatch):
    puller = _puller(monkeypatch, _retry_error(ValueError("bad frame")))
    with patch.object(puller, "_record_failure") as record_failure:
        records = _levels(lambda: setattr(puller, "_out", puller.pull_series("VIXCLS")))

    assert puller._out["status"] == "FAILED"
    record_failure.assert_called_once()
    assert "ERROR" in {lvl for lvl, _ in records}


@pytest.mark.unit
def test_fedfred_client_uses_the_configured_read_timeout():
    import fedfred.clients as fedfred_clients

    fred._install_patient_httpx()
    fred._install_patient_httpx()  # idempotent: second call must not re-wrap
    assert isinstance(fedfred_clients.httpx, fred._PatientHttpx)
    assert fedfred_clients.httpx.Client is fred._PatientClient
    assert fedfred_clients.httpx.Timeout is httpx.Timeout  # delegation intact

    seen: dict[str, object] = {}

    def fake_get(self, url, *args, timeout=None, **kwargs):
        seen["timeout"] = timeout
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {})

    with patch.object(httpx.Client, "get", fake_get):
        with fedfred_clients.httpx.Client() as client:
            # Exactly what fedfred does: an explicit timeout=10 on every GET.
            client.get("https://api.stlouisfed.org/fred/series/observations", timeout=10)

    assert seen["timeout"] == fred.FRED_HTTP_TIMEOUT
    assert fred.FRED_HTTP_TIMEOUT > 10


# ---------------------------------------------------------------------------
# Budget: fedfred's real retry loop must fit the scheduler's registered job
# deadline, and an expired deadline must stop requests and writes.
# ---------------------------------------------------------------------------


def _frame():
    import pandas as pd

    return pd.DataFrame({"value": [15.52]}, index=pd.to_datetime(["2026-10-05"]))


def _stub_puller(fetch, store):
    puller = fred.FREDPuller.__new__(fred.FREDPuller)
    puller.fred = SimpleNamespace(get_series_observations=fetch)
    puller._get_latest_date = lambda _sid: None
    puller._store_batch = store
    puller.engine, puller.source_id = MagicMock(), 1
    return puller


def _budget(n: int):
    """``should_continue`` that answers True ``n`` times, then False forever."""
    state = {"left": n}

    def should_continue() -> bool:
        state["left"] -= 1
        return state["left"] >= 0

    return should_continue


@pytest.mark.unit
def test_real_fedfred_retries_fit_the_registered_job_budget(monkeypatch):
    """One stalled series (three reads plus fedfred's waits) fits one FRED job."""
    from fedfred import FredAPI

    from ingestion.smart_scheduler import PULLER_REGISTRY

    fred._install_patient_httpx()
    monkeypatch.setattr(fred.time, "sleep", lambda *_a: None)
    read_budgets: list[float] = []
    waits: list[float] = []

    def stalled(_transport, request):
        read_budgets.append(request.extensions["timeout"]["read"])
        raise httpx.ReadTimeout("synthetic stalled read", request=request)

    puller = fred.FREDPuller.__new__(fred.FREDPuller)
    puller.fred = FredAPI("0" * 32)
    puller.engine, puller.source_id = MagicMock(), 1
    retry = FredAPI._FredAPI__fred_get_request.retry
    with patch.object(httpx.HTTPTransport, "handle_request", stalled), \
         patch.object(retry, "sleep", lambda wait: waits.append(float(wait))), \
         patch.object(puller, "_record_failure") as record_failure:
        out = puller.pull_series("VIXCLS")

    assert out["status"] == "SKIPPED"
    record_failure.assert_not_called()
    assert read_budgets == [fred.FRED_HTTP_TIMEOUT] * 3  # fedfred: stop_after_attempt(3)
    job = next(p for p in PULLER_REGISTRY if p["name"] == "fred")
    assert job["timeout_s"] == fred.FRED_JOB_BUDGET_S  # registry and self-budget stay pinned
    worst_case = sum(read_budgets) + sum(waits)
    assert worst_case < fred.FRED_JOB_BUDGET_S - fred.FRED_DEADLINE_MARGIN_S, (
        worst_case, fred.FRED_JOB_BUDGET_S)


@pytest.mark.unit
def test_pull_series_makes_no_request_and_no_write_after_the_deadline(monkeypatch):
    monkeypatch.setattr(fred.time, "sleep", lambda *_a: None)
    calls, writes = [], []

    def fetch(sid, **_kw):
        calls.append(sid)
        return _frame()

    def store(sid, points):
        writes.append(sid)
        return len(points), 0, []

    puller = _stub_puller(fetch, store)
    with patch.object(puller, "_record_failure") as record_failure:
        expired = puller.pull_series("VIXCLS", should_continue=lambda: False)
        # Deadline passes between the provider response and the store.
        mid = puller.pull_series("DFF", should_continue=_budget(1))

    assert expired["status"] == "SKIPPED" and expired["deadline"] is True
    assert mid["status"] == "SKIPPED" and mid["deadline"] is True
    assert calls == ["DFF"]  # VIXCLS never requested; DFF fetched but not stored
    assert writes == []
    record_failure.assert_not_called()


@pytest.mark.unit
def test_pull_all_stops_at_the_scheduler_deadline(monkeypatch):
    monkeypatch.setattr(fred.time, "sleep", lambda *_a: None)
    calls, writes = [], []

    def fetch(sid, **_kw):
        calls.append(sid)
        return _frame()

    def store(sid, points):
        writes.append(sid)
        return len(points), 0, []

    puller = _stub_puller(fetch, store)
    # Checks: pull_all before VIXCLS, pull_series before request, pull_series
    # before store (all True), then pull_all before DFF -> False.
    results = puller.pull_all(["VIXCLS", "DFF", "T10Y2Y"], should_continue=_budget(3))

    assert [r["status"] for r in results] == ["SUCCESS", "SKIPPED", "SKIPPED"]
    assert calls == ["VIXCLS"]
    assert writes == ["VIXCLS"]
    assert all(r.get("deadline") for r in results[1:])
    assert all("deadline" in r["errors"][0] for r in results[1:])


@pytest.mark.unit
def test_pull_all_wall_clock_budget_stops_requests_and_writes(monkeypatch):
    """A stalled first series that returns after the budget is not stored."""
    from threading import Event

    # fred.time is the time module, so this also disables time.sleep for the
    # test body: stall with Event.wait instead.
    monkeypatch.setattr(fred.time, "sleep", lambda *_a: None)
    monkeypatch.setattr(fred, "FRED_DEADLINE_MARGIN_S", 0.0)
    calls, writes = [], []

    def fetch(sid, **_kw):
        calls.append(sid)
        Event().wait(0.3)  # provider stalls past the budget
        return _frame()

    def store(sid, points):
        writes.append(sid)
        return len(points), 0, []

    puller = _stub_puller(fetch, store)
    results = puller.pull_all(["VIXCLS", "DFF"], budget_s=0.1)

    assert calls == ["VIXCLS"]  # DFF never requested
    assert writes == []  # VIXCLS response arrived after the deadline
    assert [r["status"] for r in results] == ["SKIPPED", "SKIPPED"]
    assert all(r.get("deadline") for r in results)


@pytest.mark.unit
def test_scheduler_timeout_stops_fred_before_later_series(monkeypatch):
    """After the scheduler reports TIMEOUT, the detached worker stores nothing more."""
    from threading import Event, Lock, Semaphore

    from ingestion.smart_scheduler import PULLER_REGISTRY, SmartScheduler

    monkeypatch.setattr(fred.time, "sleep", lambda *_a: None)
    # FRED budgets itself from the same number the scheduler enforces; scale
    # both down so the test runs in milliseconds.
    monkeypatch.setattr(fred, "FRED_JOB_BUDGET_S", 0.05)
    monkeypatch.setattr(fred, "FRED_DEADLINE_MARGIN_S", 0.0)
    release, done = Event(), Event()
    calls, writes = [], []

    def fetch(sid, **_kw):
        calls.append(sid)
        if len(calls) == 1:
            assert release.wait(5)
            Event().wait(0.1)  # the stalled response lands after the deadline
        return _frame()

    def store(sid, points):
        writes.append(sid)
        return len(points), 0, []

    puller = _stub_puller(fetch, store)
    original_pull_all = puller.pull_all

    # The scheduler only passes its deadline to methods whose signature names
    # ``should_continue``; keep the production signature on this wrapper.
    def pull_all(series_list=None, start_date="1990-01-01", end_date=None,
                 should_continue=None):
        try:
            return original_pull_all(series_list, start_date, end_date,
                                     should_continue=should_continue)
        finally:
            done.set()

    puller.pull_all = pull_all

    scheduler = SmartScheduler.__new__(SmartScheduler)
    scheduler._thread_semaphore = Semaphore(1)
    scheduler._threads_lock = Lock()
    scheduler._active_threads = set()
    scheduler._orphan_thread_count = 0
    scheduler._build_puller_instance = lambda *_args: puller
    spec = next(p.copy() for p in PULLER_REGISTRY if p["name"] == "fred")
    spec["timeout_s"] = fred.FRED_JOB_BUDGET_S
    spec["kwargs"] = {"series_list": ["VIXCLS", "DFF"]}
    try:
        result = scheduler._run_puller(spec)
    finally:
        release.set()

    assert result["status"] == "TIMEOUT"
    assert done.wait(5)
    assert calls == ["VIXCLS"]  # DFF never requested after the deadline
    assert writes == []  # the stalled VIXCLS response is not stored either
