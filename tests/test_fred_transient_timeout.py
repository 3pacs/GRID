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
    assert job["stop_margin_s"] == fred.FRED_DEADLINE_MARGIN_S
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


# ---------------------------------------------------------------------------
# After expiry: no retry attempt is sent, an allowed attempt cannot wait past
# the deadline, and recovery opens no new transaction.
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_fedfred_retry_attempts_stop_at_the_deadline(monkeypatch):
    """The deadline passes during attempt 1: attempts 2 and 3 are never sent."""
    from fedfred import FredAPI

    fred._install_patient_httpx()
    monkeypatch.setattr(fred.time, "sleep", lambda *_a: None)
    expired = {"v": False}
    sent: list[str] = []
    waits: list[float] = []

    def stalled(_transport, request):
        sent.append(str(request.url))
        expired["v"] = True  # the pass deadline passes while this attempt is in flight
        raise httpx.ReadTimeout("synthetic stalled read", request=request)

    puller = fred.FREDPuller.__new__(fred.FREDPuller)
    puller.fred = FredAPI("0" * 32)
    puller.engine, puller.source_id = MagicMock(), 1
    retry = FredAPI._FredAPI__fred_get_request.retry
    with patch.object(httpx.HTTPTransport, "handle_request", stalled), \
         patch.object(retry, "sleep", lambda wait: waits.append(float(wait))), \
         patch.object(puller, "_record_failure") as record_failure:
        out = puller.pull_series("VIXCLS", should_continue=lambda: not expired["v"])

    assert len(sent) == 1  # fedfred's two further attempts were refused before sending
    assert out["status"] == "SKIPPED" and out["deadline"] is True
    record_failure.assert_not_called()
    puller.engine.connect.assert_not_called()
    assert len(waits) <= 2


@pytest.mark.unit
def test_attempt_timeout_shrinks_to_the_remaining_budget(monkeypatch):
    """An attempt that is still allowed may not wait past the pass deadline."""
    from fedfred import FredAPI

    fred._install_patient_httpx()
    monkeypatch.setattr(fred.time, "sleep", lambda *_a: None)
    seen: list[float] = []

    def respond(_transport, request):
        seen.append(request.extensions["timeout"]["read"])
        return httpx.Response(200, request=request, json={"observations": [{
            "date": "2026-10-05", "value": "15.52",
            "realtime_start": "2026-10-06", "realtime_end": "2026-10-06",
        }]})

    writes: list[str] = []
    puller = fred.FREDPuller.__new__(fred.FREDPuller)
    puller.fred = FredAPI("0" * 32)
    puller.engine, puller.source_id = MagicMock(), 1
    puller._get_latest_date = lambda _sid: None
    puller._store_batch = lambda sid, points: (writes.append(sid) or (len(points), 0, []))
    with patch.object(httpx.HTTPTransport, "handle_request", respond):
        results = puller.pull_all(["VIXCLS"], budget_s=fred.FRED_DEADLINE_MARGIN_S + 0.5)

    assert results[0]["status"] == "SUCCESS" and writes == ["VIXCLS"]
    assert len(seen) == 1
    assert 0.0 < seen[0] <= 0.5 < fred.FRED_HTTP_TIMEOUT


@pytest.mark.unit
def test_no_failure_row_is_written_after_the_deadline(monkeypatch):
    """A genuine bug surfacing after expiry is reported but not persisted."""
    monkeypatch.setattr(fred.time, "sleep", lambda *_a: None)
    expired = {"v": False}

    def fetch(_sid, **_kw):
        expired["v"] = True  # deadline passes while the provider call is in flight
        raise ValueError("bad frame")

    puller = fred.FREDPuller.__new__(fred.FREDPuller)
    puller.fred = SimpleNamespace(get_series_observations=fetch)
    puller.engine, puller.source_id = MagicMock(), 1
    with patch.object(puller, "_record_failure") as record_failure:
        out = puller.pull_series("VIXCLS", should_continue=lambda: not expired["v"])

    assert out["status"] == "FAILED"  # still a real error in the result
    assert out["deadline"] is True
    assert any("failure row not recorded" in e for e in out["errors"])
    record_failure.assert_not_called()
    puller.engine.connect.assert_not_called()


# ---------------------------------------------------------------------------
# Alignment with the scheduler's cooperative deadline, and the reviewer's
# modeled-clock cases (Codex PR-825 handoff, 2026-10-06).
# ---------------------------------------------------------------------------

try:
    from tests.test_fred_short_transactions import FakeConnection, db_error, make_puller
except ImportError:  # tests/ not importable as a package
    from test_fred_short_transactions import FakeConnection, db_error, make_puller  # type: ignore


def _scheduler_for(puller):
    from threading import Lock, Semaphore

    from ingestion.smart_scheduler import SmartScheduler

    scheduler = SmartScheduler.__new__(SmartScheduler)
    scheduler._thread_semaphore = Semaphore(1)
    scheduler._threads_lock = Lock()
    scheduler._active_threads = set()
    scheduler._orphan_thread_count = 0
    scheduler._build_puller_instance = lambda *_args: puller
    scheduler._update_last_pull = MagicMock()
    return scheduler


@pytest.mark.unit
def test_scheduler_callback_and_fred_self_deadline_agree(monkeypatch):
    """The injected callback and FRED's own deadline fall at the same instant."""
    from ingestion.smart_scheduler import PULLER_REGISTRY

    clock, captured = [0.0], []
    monkeypatch.setattr(fred.time, "monotonic", lambda: clock[0])

    def pull_all(series_list=None, should_continue=None):
        captured.append(should_continue)
        return [{"status": "SKIPPED", "rows_inserted": 0}]

    scheduler = _scheduler_for(SimpleNamespace(pull_all=pull_all))
    spec = next(p.copy() for p in PULLER_REGISTRY if p["name"] == "fred")
    assert scheduler._run_puller(spec)["status"] == "SKIPPED"
    callback = captured[0]
    deadline = fred.FRED_JOB_BUDGET_S - fred.FRED_DEADLINE_MARGIN_S
    samples = {}
    for instant in (0.0, deadline - 0.001, deadline, spec["timeout_s"], spec["timeout_s"] + 1):
        clock[0] = instant
        samples[instant] = callback()
    assert samples == {0.0: True, deadline - 0.001: True, deadline: False,
                       spec["timeout_s"]: False, spec["timeout_s"] + 1: False}, samples
    assert deadline == 285.0


@pytest.mark.unit
def test_retry_attempts_respect_the_cooperative_deadline(monkeypatch):
    """One second from the deadline: one bounded attempt, no later attempt, no row."""
    from fedfred import FredAPI

    clock, starts, allowances, waits = [0.0], [], [], []
    monkeypatch.setattr(fred.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(fred.time, "sleep", lambda *_a: None)
    fred._install_patient_httpx()
    puller = fred.FREDPuller.__new__(fred.FREDPuller)
    puller.fred = FredAPI("0" * 32)
    puller.engine, puller.source_id = MagicMock(), 1

    def latest(_sid):
        clock[0] = 284.0  # one second before the 285 s cooperative deadline
        return None

    puller._get_latest_date = latest

    def stalled(_transport, request):
        starts.append(clock[0])
        allowance = request.extensions["timeout"]["read"]
        allowances.append(allowance)
        clock[0] += allowance
        raise httpx.ReadTimeout("synthetic stalled read", request=request)

    def retry_sleep(wait):
        waits.append(float(wait))
        clock[0] += float(wait)

    retry = FredAPI._FredAPI__fred_get_request.retry
    with patch.object(httpx.HTTPTransport, "handle_request", stalled), \
         patch.object(retry, "sleep", retry_sleep), \
         patch.object(puller, "_record_failure") as record_failure:
        results = puller.pull_all(["VIXCLS", "DFF"], should_continue=lambda: clock[0] < 285)

    assert starts == [284.0]  # attempts 2 and 3 were refused before sending
    assert allowances == [1.0]  # the one allowed read could not wait past 285 s
    record_failure.assert_not_called()
    puller.engine.connect.assert_not_called()
    assert [r["status"] for r in results] == ["SKIPPED", "SKIPPED"]
    assert all(r.get("deadline") for r in results)


@pytest.mark.unit
def test_store_fallback_does_not_open_new_writes_after_deadline(monkeypatch):
    """A batch rejected after expiry is not retried point by point."""
    from sqlalchemy import exc as sa_exc

    clock, inserts = [0.0], []
    monkeypatch.setattr(fred.time, "monotonic", lambda: clock[0])
    puller, engine, _frame, _calls = make_puller(monkeypatch, 3)
    original = FakeConnection.execute
    rejected = [False]

    def expire_and_reject(self, sql, params):
        if "INSERT" in str(sql):
            if not rejected[0]:
                rejected[0] = True
                clock[0] = 301.0  # the batch's answer arrives after the deadline
                raise db_error("23514", sa_exc.IntegrityError)
            inserts.append(clock[0])
        return original(self, sql, params)

    monkeypatch.setattr(FakeConnection, "execute", expire_and_reject)
    result = puller.pull_series("VIXCLS", should_continue=lambda: clock[0] < 295)

    assert inserts == []  # no per-point fallback transaction after expiry
    assert not engine.rows  # and no failure-metadata row either
    assert result["status"] == "FAILED" and result["rows_inserted"] == 0
    assert result["rows_failed"] == 3 and result["deadline"] is True


@pytest.mark.unit
def test_inflight_successful_batch_still_acknowledges_after_deadline(monkeypatch):
    """A COMMIT already in flight at expiry is counted; only new work stops."""
    clock = [0.0]
    monkeypatch.setattr(fred.time, "monotonic", lambda: clock[0])
    puller, engine, _frame, _calls = make_puller(monkeypatch, 1)
    original = FakeConnection.commit

    def late_commit(self):
        clock[0] = 301.0
        return original(self)

    monkeypatch.setattr(FakeConnection, "commit", late_commit)
    result = puller.pull_series("VIXCLS", should_continue=lambda: clock[0] < 295)

    assert (result["status"], result["rows_inserted"], result["rows_failed"]) == ("SUCCESS", 1, 0)
    assert len(engine.rows) == 1


# ---------------------------------------------------------------------------
# Codex's fresh boundary cases against 416e6cbb: a pool checkout that returns
# after expiry must not begin a transaction, and scheduler setup time must not
# extend the attempt budget.
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize("path", ["observations", "failure_metadata", "point_fallback"])
def test_slow_pool_checkout_does_not_begin_a_transaction_after_the_deadline(monkeypatch, path):
    from sqlalchemy import exc as sa_exc

    clock, begins, inserts = [284.0], [], []
    monkeypatch.setattr(fred.time, "monotonic", lambda: clock[0])
    puller, engine, _frame, _calls = make_puller(monkeypatch, 2)
    original_connect, original_begin, original_execute = engine.connect, FakeConnection.begin, FakeConnection.execute
    connect_count = [0]

    def slow_connect():
        connect_count[0] += 1
        # In the fallback variant the batch is admitted and rolled back before
        # expiry; its first per-point checkout is the one that returns late.
        if path != "point_fallback" or connect_count[0] == 2:
            clock[0] = 301.0
        return original_connect()

    def begin(self):
        begins.append(clock[0])
        return original_begin(self)

    rejected = [False]

    def execute(self, sql, params):
        if path == "point_fallback" and "INSERT" in str(sql) and not rejected[0]:
            rejected[0] = True
            raise db_error("23514", sa_exc.IntegrityError)
        if "INSERT" in str(sql):
            inserts.append(clock[0])
        return original_execute(self, sql, params)

    if path == "failure_metadata":
        def failing_fetch(*_a, **_kw):
            raise ValueError("synthetic frame error")
        puller.fred = SimpleNamespace(get_series_observations=failing_fetch)
    engine.connect = slow_connect
    monkeypatch.setattr(FakeConnection, "begin", begin)
    monkeypatch.setattr(FakeConnection, "execute", execute)

    out = puller.pull_series("DFF", should_continue=lambda: clock[0] < 285)

    assert not any(t >= 285 for t in begins), begins
    assert not any(t >= 285 for t in inserts), inserts
    assert out["deadline"] is True
    assert len(engine.rows) == 0
    if path == "observations":
        assert out["status"] == "SKIPPED" and out["rows_inserted"] == 0
    elif path == "failure_metadata":
        assert out["status"] == "FAILED"
    else:
        assert out["status"] == "SKIPPED" and out["rows_failed"] == 2


@pytest.mark.unit
@pytest.mark.parametrize("setup_seconds", [0.0, 10.0, 20.0])
def test_scheduler_setup_delay_does_not_extend_the_attempt_budget(monkeypatch, setup_seconds):
    from fedfred import FredAPI

    from ingestion.smart_scheduler import PULLER_REGISTRY

    clock, starts, budgets = [0.0], [], []
    monkeypatch.setattr(fred.time, "monotonic", lambda: clock[0])
    fred._install_patient_httpx()
    puller = fred.FREDPuller.__new__(fred.FREDPuller)
    puller.fred = FredAPI("0" * 32)
    puller.engine, puller.source_id = MagicMock(), 1

    def latest(_sid):
        clock[0] = 284.0
        return None

    puller._get_latest_date = latest
    scheduler = _scheduler_for(puller)

    def slow_build(*_args):
        clock[0] = setup_seconds  # construction ran after the scheduler's job start
        return puller

    scheduler._build_puller_instance = slow_build

    def stalled(_transport, request):
        starts.append(clock[0])
        allowance = request.extensions["timeout"]["read"]
        budgets.append(allowance)
        clock[0] += allowance
        raise httpx.ReadTimeout("synthetic stall", request=request)

    retry = FredAPI._FredAPI__fred_get_request.retry
    spec = next(p.copy() for p in PULLER_REGISTRY if p["name"] == "fred")
    spec["kwargs"] = {"series_list": ["DFF"]}
    with patch.object(httpx.HTTPTransport, "handle_request", stalled), \
         patch.object(retry, "sleep", lambda w: clock.__setitem__(0, clock[0] + float(w))):
        out = scheduler._run_puller(spec)

    assert starts == [284.0]
    assert budgets == [1.0], budgets  # capped against the job-start deadline (285), not entry + 285
    assert out["status"] == "SKIPPED"


@pytest.mark.unit
def test_scheduler_passes_its_absolute_deadline_to_pull_all(monkeypatch):
    from ingestion.smart_scheduler import PULLER_REGISTRY

    clock, captured = [0.0], {}
    monkeypatch.setattr(fred.time, "monotonic", lambda: clock[0])

    def pull_all(series_list=None, should_continue=None, deadline_at=None):
        captured["deadline_at"] = deadline_at
        captured["callback"] = should_continue
        return [{"status": "SKIPPED", "rows_inserted": 0}]

    scheduler = _scheduler_for(SimpleNamespace(pull_all=pull_all))
    spec = next(p.copy() for p in PULLER_REGISTRY if p["name"] == "fred")
    scheduler._run_puller(spec)
    assert captured["deadline_at"] == fred.FRED_JOB_BUDGET_S - fred.FRED_DEADLINE_MARGIN_S == 285.0
    clock[0] = 284.999
    assert captured["callback"]() is True
    clock[0] = 285.0
    assert captured["callback"]() is False
