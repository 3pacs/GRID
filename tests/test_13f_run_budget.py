"""A 13F run stops itself at its caller's deadline and reports honestly.

Production, 2026-11-23..26: ``smart:etf_flows`` (11 ETF proxies, then the two
latest 13F filings of 75 managers, 200-346 s in all) was logged
``TIMEOUT: Exceeded 120s timeout`` on every run. The timeout did not stop the
run: its thread was abandoned and finished minutes later. The Hermes repair
then retried the whole read under a 60 s cap, abandoned that too, and did so
13 to 44 times a day.

The run now takes the budget its caller already offers (``should_continue``,
``deadline_at``), stops before writing anything for the manager in hand, says
how many managers it read, and starts the next run where it stopped.

A second thing kept a finished run from ever being logged complete: a manager
whose filings were read in full but cannot be compared came back with a status
the outcome classifier does not know, which it counts as failed. That is now
NO_DATA (a finished check); a filing that could not be fetched is FAILED and
names the failure.

The real writer runs against the fake EDGAR of
``tests/thirteenf_holdings_support.py`` (network replaced only).
"""

from __future__ import annotations

import ast
import inspect
import sys
import time
import types
from datetime import date, timedelta
from pathlib import Path

import pytest
import requests
from sqlalchemy import text

from ingestion.altdata import institutional_flows as inst
from ingestion.smart_scheduler import _classify_outcome
from tests import thirteenf_holdings_support as s

STOP = "13F:run_budget"


@pytest.fixture(autouse=True)
def _no_resume_point(monkeypatch):
    monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_resume_13f_at_cik", None)


@pytest.fixture()
def engine():
    return s.make_sqlite_engine()


def _count(engine, pattern: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM raw_series WHERE series_id LIKE :p"),
                            {"p": pattern}).scalar_one()


def _receipts(engine) -> int:
    return _count(engine, "13F-FILING:%")


class _Recorder:
    """The fake EDGAR, recording each request and able to act on one."""

    def __init__(self, edgar: s.FakeEdgar) -> None:
        self.edgar = edgar
        self.urls: list[str] = []
        self.timeouts: list[float] = []
        self.on_request = None

    def get(self, url, headers=None, timeout=None):
        self.urls.append(url)
        self.timeouts.append(timeout)
        if self.on_request is not None:
            self.on_request(url)
        return self.edgar.get(url, headers=headers, timeout=timeout)

    def ciks(self) -> list[str]:
        seen: list[str] = []
        for url in self.urls:
            for cik in s.FILERS:
                if cik in url and cik not in seen:
                    seen.append(cik)
        return seen


def _puller(engine, monkeypatch, get, filers=None) -> inst.InstitutionalFlowsPuller:
    monkeypatch.setattr(inst.requests, "get", get)
    monkeypatch.setattr(inst.time, "sleep", lambda *_a: None)
    monkeypatch.setattr(inst, "TOP_13F_FILERS", dict(filers or s.FILERS))
    inst._DEAD_CIKS.clear()
    return inst.InstitutionalFlowsPuller(engine)


def _run(engine, monkeypatch, get, filers=None, **budget) -> list[dict]:
    return _puller(engine, monkeypatch, get, filers).pull_13f_only(**budget)


def _stop(results: list[dict]) -> dict:
    (item,) = [r for r in results if r.get("stopped_by_budget")]
    return item


# ── The run stops itself and says how far it got ───────────────────────────


def test_out_of_budget_between_managers_the_run_stops_and_says_how_far_it_got(engine, monkeypatch):
    edgar = s.two_quarter_scenario()

    results = _run(engine, monkeypatch, edgar.get, should_continue=lambda: _receipts(engine) == 0)

    assert [r["feature"] for r in results] == [STOP, f"13F:{s.CIK_ALPHA}"]    # the stop is listed first
    assert results[1]["status"] == "SUCCESS" and results[1]["rows_inserted"] > 0
    stop = _stop(results)
    assert stop["status"] == "PARTIAL" and stop["stopped_phase"] == "between_managers"
    assert stop["deadline_overrun_s"] is None                  # no deadline_at was given
    assert (stop["items_read"], stop["items_total"], stop["items_not_attempted"]) == (1, 2, 1)
    assert stop["next_manager_cik"] == s.CIK_BETA
    assert _count(engine, f"13F%:{s.CIK_BETA}:%") == 0

    outcome, _rows, note = _classify_outcome(results)
    assert outcome == "PARTIAL"
    assert "13F managers stopped at the run deadline: 1 of 2 read, 1 not attempted" in note


def test_a_deadline_that_passes_while_a_manager_is_being_read_writes_nothing_for_it(engine, monkeypatch):
    edgar = s.two_quarter_scenario()
    recorder = _Recorder(edgar)
    stopped = {"now": False}

    def on_request(url: str) -> None:
        if "/Archives/" in url and s.CIK_BETA in url:
            stopped["now"] = True

    recorder.on_request = on_request
    results = _run(engine, monkeypatch, recorder.get, should_continue=lambda: not stopped["now"])

    assert any("/Archives/" in u and s.CIK_BETA in u for u in recorder.urls)      # Beta was being read
    assert _stop(results)["items_read"] == 1
    assert _stop(results)["next_manager_cik"] == s.CIK_BETA
    # No change row and no receipt: a read cut short is not a finding.
    assert _count(engine, f"13F%:{s.CIK_BETA}:%") == 0
    assert not [r for r in results if r["feature"] == f"13F:{s.CIK_BETA}"]


def test_a_request_that_fails_at_the_deadline_is_not_attempted_rather_than_failed(engine, monkeypatch):
    edgar = s.two_quarter_scenario()
    stopped = {"now": False}

    def get(url, headers=None, timeout=None):
        if s.CIK_BETA in url:
            stopped["now"] = True
            raise requests.Timeout("read timed out")
        return edgar.get(url, headers=headers, timeout=timeout)

    results = _run(engine, monkeypatch, get, should_continue=lambda: not stopped["now"])

    assert not [r for r in results if r["status"] == "FAILED"]
    assert _stop(results)["items_not_attempted"] == 1


def test_control_the_same_failure_with_budget_left_is_a_failed_manager(engine, monkeypatch):
    edgar = s.two_quarter_scenario()

    def get(url, headers=None, timeout=None):
        if s.CIK_BETA in url:
            raise requests.Timeout("read timed out")
        return edgar.get(url, headers=headers, timeout=timeout)

    results = _run(engine, monkeypatch, get)

    assert [r["status"] for r in results] == ["SUCCESS", "FAILED"]
    assert not [r for r in results if r.get("stopped_by_budget")]


def test_a_run_with_no_budget_reads_every_manager_and_reports_no_stop(engine, monkeypatch):
    results = _run(engine, monkeypatch, s.two_quarter_scenario().get)

    assert [r["status"] for r in results] == ["SUCCESS", "SUCCESS"]
    assert inst.InstitutionalFlowsPuller._resume_13f_at_cik is None


# ── The next run starts where the last one stopped ─────────────────────────


def test_runs_cut_short_every_time_still_reach_every_manager(engine, monkeypatch):
    edgar = s.two_quarter_scenario()
    _run(engine, monkeypatch, edgar.get, should_continue=lambda: _receipts(engine) == 0)
    assert inst.InstitutionalFlowsPuller._resume_13f_at_cik == s.CIK_BETA

    second = _Recorder(edgar)
    results = _run(engine, monkeypatch, second.get, should_continue=lambda: _receipts(engine) < 2)

    assert second.ciks() == [s.CIK_BETA]                       # not Alpha again
    assert [r["feature"] for r in results] == [STOP, f"13F:{s.CIK_BETA}"]
    assert _count(engine, f"13F-FILING:{s.CIK_BETA}:%") == 1
    assert _stop(results)["next_manager_cik"] == s.CIK_ALPHA

    third = _Recorder(edgar)
    results = _run(engine, monkeypatch, third.get)

    assert third.ciks() == [s.CIK_ALPHA, s.CIK_BETA]           # from the resume point, wrapping round
    assert not [r for r in results if r.get("stopped_by_budget")]
    assert inst.InstitutionalFlowsPuller._resume_13f_at_cik is None


def test_a_resume_point_that_is_no_longer_a_filer_starts_at_the_top():
    managers = [("1", "A"), ("2", "B"), ("3", "C")]
    assert inst._from_resume_point(managers, "2") == [("2", "B"), ("3", "C"), ("1", "A")]
    assert inst._from_resume_point(managers, "gone") == managers
    assert inst._from_resume_point(managers, None) == managers


# ── deadline_at caps each request ──────────────────────────────────────────


def test_requests_are_capped_to_the_time_left_before_the_deadline(engine, monkeypatch):
    recorder = _Recorder(s.two_quarter_scenario())

    results = _run(engine, monkeypatch, recorder.get, deadline_at=time.monotonic() + 5)

    assert [r["status"] for r in results] == ["SUCCESS", "SUCCESS"]
    assert recorder.timeouts and all(inst._MIN_REQUEST_TIMEOUT <= t <= 5 for t in recorder.timeouts)


def test_without_a_deadline_requests_keep_the_full_timeout(engine, monkeypatch):
    recorder = _Recorder(s.two_quarter_scenario())

    _run(engine, monkeypatch, recorder.get)

    assert recorder.timeouts and set(recorder.timeouts) == {float(inst._REQUEST_TIMEOUT)}


def test_a_deadline_already_passed_reads_nothing(engine, monkeypatch):
    recorder = _Recorder(s.two_quarter_scenario())

    results = _run(engine, monkeypatch, recorder.get, deadline_at=time.monotonic() - 1)

    assert recorder.urls == []
    assert [r["feature"] for r in results] == [STOP]
    assert _stop(results)["items_read"] == 0 and _stop(results)["next_manager_cik"] == s.CIK_ALPHA
    # Nothing was read, so the run is a failure that names the stop.
    outcome, _rows, note = _classify_outcome(results)
    assert outcome == "FAILED" and "stopped at the run deadline: 0 of 2 read" in note
    assert _count(engine, "13F%") == 0


def test_run_budget():
    assert inst._RunBudget().spent() is False
    assert inst._RunBudget().request_timeout() == float(inst._REQUEST_TIMEOUT)
    assert inst._RunBudget(should_continue=lambda: False).spent() is True
    assert inst._RunBudget(should_continue=lambda: True, deadline_at=time.monotonic() - 1).spent() is True
    assert inst._RunBudget(deadline_at=time.monotonic() - 1).request_timeout() == inst._MIN_REQUEST_TIMEOUT
    assert inst._RunBudget(deadline_at=time.monotonic() + 3600).request_timeout() == float(inst._REQUEST_TIMEOUT)


# ── A withheld comparison: finished check, or this run's failure ───────────


def test_filings_read_in_full_that_cannot_be_compared_are_a_finished_check(engine, monkeypatch):
    edgar = s.two_quarter_scenario()
    edgar.filings[s.CIK_ALPHA][1].cover_entry_total = 99       # cover counts more entries than the table has

    results = _run(engine, monkeypatch, edgar.get)

    alpha, beta = results
    assert alpha["status"] == "NO_DATA" and alpha["comparison_status"] == "withheld_incomplete"
    assert alpha["curr_snapshot"] == "entry_count_mismatch" and "error" not in alpha
    assert alpha["rows_inserted"] == 0 and alpha["receipts_inserted"] == 1    # the withheld receipt is stored
    assert _count(engine, f"13F:{s.CIK_ALPHA}:%") == 0                        # and no change row
    assert _classify_outcome([alpha])[0] == "NO_NEW_DATA"
    assert beta["status"] == "SUCCESS"
    assert _classify_outcome(results)[0] == "SUCCESS"          # was PARTIAL: one unknown status failed the run


@pytest.mark.parametrize("fail,kind", [("503", "http_503"), ("429", "http_429"), ("timeout", "timeout")])
def test_a_filing_that_could_not_be_fetched_is_a_failed_manager_naming_the_failure(engine, monkeypatch, fail, kind):
    edgar = s.two_quarter_scenario()
    curr = edgar.filings[s.CIK_ALPHA][1]
    curr.fail, curr.fail_on = fail, "cover"

    results = _run(engine, monkeypatch, edgar.get)

    alpha = results[0]
    assert alpha["status"] == "FAILED" and alpha["comparison_status"] == "withheld_incomplete"
    assert alpha["curr_snapshot"] == f"fetch_failed:{kind}"
    assert f"fetch_failed:{kind}" in alpha["error"] and s.CIK_ALPHA in alpha["error"]
    assert alpha["receipts_inserted"] == 1                     # the unverified read is still on record
    outcome, _rows, note = _classify_outcome(results)
    assert outcome == "PARTIAL" and f"fetch_failed:{kind}" in note


def _snap(status: str, **evidence) -> inst.FilingSnapshot:
    return inst.FilingSnapshot(accession="A", status=status, evidence=evidence)


@pytest.mark.parametrize("status", sorted(inst.READ_IN_FULL_NOT_COMPARABLE) + ["complete", "verified_empty"])
def test_read_in_full_is_a_named_list_of_statuses(status):
    assert inst._not_read_in_full(_snap(status)) is None


@pytest.mark.parametrize("status", [
    "fetch_failed:timeout", "fetch_failed:http_503", "fetch_failed:http_403",   # failed or refused fetch
    "unparseable_cover_page", "unparseable_infotable",                         # a document that did not parse
    "unknown_filing_date_or_accession",                                        # nothing was fetched
    "a_status_added_later",                                                    # not listed: not read in full
])
def test_everything_else_is_not_read_in_full_and_says_why(status):
    assert inst._not_read_in_full(_snap(status)) == status


def test_an_amendment_side_is_read_in_full_only_when_its_cover_was_read():
    for reason in sorted(inst.AMENDMENT_COVER_READ):
        assert inst._not_read_in_full(_snap("unclassified_amendment", classification_reason=reason)) is None
        assert inst._not_read_in_full(_snap(
            "unresolved_undated_amendment", unresolved_amendments=[{"accession": "B", "reason": reason}])) is None
    for reason in ("fetch_failed:http_503", "unparseable_cover_page"):
        assert inst._not_read_in_full(_snap("unclassified_amendment", classification_reason=reason)) == reason
        assert inst._not_read_in_full(_snap("unresolved_undated_amendment", unresolved_amendments=[
            {"accession": "B", "reason": "missing_cover_page"}, {"accession": "C", "reason": reason},
        ])) == reason
    # No recorded reason is not evidence that anything was read.
    assert inst._not_read_in_full(_snap("unclassified_amendment")) == "unknown"
    assert inst._not_read_in_full(_snap("unresolved_undated_amendment")) == "unknown"


@pytest.mark.parametrize("knob,value,status", [
    ("infotable", "malformed", "unparseable_infotable"),
    ("cover", "malformed", "unparseable_cover_page"),
])
def test_a_document_that_did_not_parse_is_a_failed_manager_not_a_finished_check(
    engine, monkeypatch, knob, value, status
):
    edgar = s.two_quarter_scenario()
    setattr(edgar.filings[s.CIK_ALPHA][1], knob, value)

    alpha = _run(engine, monkeypatch, edgar.get)[0]

    assert alpha["status"] == "FAILED" and alpha["curr_snapshot"] == status
    assert status in alpha["error"] and _count(engine, f"13F:{s.CIK_ALPHA}:%") == 0


def test_a_refused_fetch_is_a_failed_manager(engine, monkeypatch):
    edgar = s.two_quarter_scenario()
    curr = edgar.filings[s.CIK_ALPHA][1]
    curr.fail, curr.fail_on = "403", "cover"

    alpha = _run(engine, monkeypatch, edgar.get)[0]

    assert alpha["status"] == "FAILED" and "fetch_failed:http_403" in alpha["error"]


def test_a_finished_check_still_reads_downstream_as_an_unverified_latest_filing(engine, monkeypatch):
    """NO_DATA is the run's status only. The holdings reader goes by the
    stored receipt: the manager's latest filing is unverified and none of its
    holdings are served."""
    from intelligence import dollar_flows
    from intelligence import holdings_observations_13f as h13f

    monkeypatch.setattr(dollar_flows, "_get_cusip_ticker_map", lambda: None)
    edgar = s.two_quarter_scenario()
    edgar.filings[s.CIK_ALPHA][1].cover_entry_total = 99

    results = _run(engine, monkeypatch, edgar.get)
    assert results[0]["status"] == "NO_DATA"

    h13f.clear_snapshot_cache()
    snapshot = h13f.read_13f_holdings_snapshot(engine)
    h13f.clear_snapshot_cache()
    assert snapshot.counts.get("managers_latest_filing_unverified") == 1
    assert s.CIK_ALPHA not in {h.manager_cik for h in snapshot.holdings}
    assert s.CIK_BETA in {h.manager_cik for h in snapshot.holdings}            # control: Beta compared


# ── pull_all: what SmartScheduler and the Hermes repair call ───────────────


def test_pull_all_takes_the_budget_both_callers_offer_by_name():
    for method in (inst.InstitutionalFlowsPuller.pull_all, inst.InstitutionalFlowsPuller.pull_13f_only):
        params = inspect.signature(method).parameters
        assert {"should_continue", "deadline_at"} <= set(params)
        assert params["should_continue"].default is None and params["deadline_at"].default is None


def test_pull_all_puts_the_stop_first_so_the_run_status_names_it(engine, monkeypatch):
    edgar = s.two_quarter_scenario()
    puller = _puller(engine, monkeypatch, edgar.get)
    monkeypatch.setattr(puller, "_get_latest_date", lambda _sid: None)
    monkeypatch.setattr(puller, "_pull_etf_flows", lambda _start, _end, _budget=None: [
        {"feature": "ETF_FLOW:SPY", "status": "FAILED", "rows_inserted": 0,
         "error": "yfinance returned no bars for SPY"},
        {"feature": "ETF_FLOW:QQQ:5d", "status": "SUCCESS", "rows_inserted": 3},
    ])

    results = puller.pull_all(should_continue=lambda: _receipts(engine) == 0)

    assert results[0]["feature"] == STOP
    assert [r["feature"] for r in results[1:]] == ["ETF_FLOW:SPY", "ETF_FLOW:QQQ:5d", f"13F:{s.CIK_ALPHA}"]
    outcome, _rows, note = _classify_outcome(results)
    assert outcome == "PARTIAL"
    assert "13F managers stopped at the run deadline: 1 of 2 read, 1 not attempted" in note


def test_etf_flows_stop_at_the_deadline_without_calling_the_provider(engine, monkeypatch):
    def download(*_a, **_k):
        raise AssertionError("yfinance was called after the deadline")

    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(download=download))
    puller = _puller(engine, monkeypatch, s.two_quarter_scenario().get)

    results = puller._pull_etf_flows(
        date(2026, 11, 1), date(2026, 11, 26), inst._RunBudget(should_continue=lambda: False))

    (stop,) = results
    assert stop["feature"] == "ETF_FLOW:run_budget" and stop["stopped_by_budget"] is True
    assert (stop["items_read"], stop["items_total"]) == (0, len(inst.ETF_FLOW_TICKERS))
    assert _classify_outcome(results)[0] == "FAILED"


# ── No new request after the deadline: replay on a fake clock ──────────────
#
# Codex review 6080519392: with the deadline exhausted by the current filing,
# the loop still started the prior filing's read, and a request timeout is not
# a wall clock. Every 13F request now starts in _edgar_get, which refuses once
# the budget is spent. These replay a whole run on a clock that only the fake
# EDGAR moves (one tick per request) with the deadline placed after every
# request in turn.

CIK_GAMMA = "9000000003"
GAMMA = "Fixture One Filing Gamma LP"
# Alpha: two quarters plus a restatement of the current one (amendment-cover
# path). Gamma: a single filing (the one-filing fallback). Beta: two quarters,
# last, so the run's final request belongs to a manager that would be written.
REPLAY_FILERS = {s.CIK_ALPHA: s.ALPHA, CIK_GAMMA: GAMMA, s.CIK_BETA: s.BETA}
START = 1000.0


def _replay_edgar() -> s.FakeEdgar:
    edgar = s.two_quarter_scenario()
    curr = edgar.filings[s.CIK_ALPHA][1]
    edgar.add(s.CIK_ALPHA, s.Filing(
        "9000000001-26-000003", curr.filing_date + timedelta(days=3), curr.report_date,
        dict(curr.holdings), form="13F-HR/A"))
    edgar.add(CIK_GAMMA, s.Filing("9000000003-26-000001", curr.filing_date, curr.report_date, {
        s.ROOT_CUSIP: ("ROOT HOLDINGS INC", 10_000, 100)}))
    return edgar


class _Clock:
    def __init__(self) -> None:
        self.now = START

    def __call__(self) -> float:
        return self.now


def _replay(monkeypatch, deadline_after: float | None, get_wrapper=None, sleeps: list | None = None):
    """One run on the fake clock. Each request starts at the current tick and
    takes one tick. Returns (engine, recorder, request start times, results).
    With ``sleeps``, every time.sleep is recorded as (started at, seconds) and
    moves the clock; without it sleeps take no time."""
    engine = s.make_sqlite_engine()
    clock = _Clock()
    monkeypatch.setattr(inst, "_monotonic", clock)
    # Each replay is a first run: no resume point left by the one before it.
    monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_resume_13f_at_cik", None)
    recorder = _Recorder(_replay_edgar())
    started: list[float] = []

    def on_request(_url: str) -> None:
        started.append(clock.now)
        clock.now += 1.0

    recorder.on_request = on_request
    get = get_wrapper(recorder, clock) if get_wrapper else recorder.get
    budget = {} if deadline_after is None else {"deadline_at": START + deadline_after}
    puller = _puller(engine, monkeypatch, get, REPLAY_FILERS)
    if sleeps is not None:
        def fake_sleep(seconds: float) -> None:
            sleeps.append((clock.now, seconds))
            clock.now += seconds

        monkeypatch.setattr(inst.time, "sleep", fake_sleep)
    results = puller.pull_13f_only(**budget)
    return engine, recorder, started, results


def _full_run_urls(monkeypatch) -> list[str]:
    _engine, recorder, _started, results = _replay(monkeypatch, None)
    assert not [r for r in results if r.get("stopped_by_budget")]
    assert {r["feature"]: r["status"] for r in results} == {
        f"13F:{s.CIK_ALPHA}": "SUCCESS", f"13F:{CIK_GAMMA}": "PARTIAL", f"13F:{s.CIK_BETA}": "SUCCESS"}
    return list(recorder.urls)


def test_the_replay_covers_every_request_path(monkeypatch):
    urls = _full_run_urls(monkeypatch)
    assert any("9000000001-26-000003".replace("-", "") in u for u in urls)      # amendment cover / restated filing
    assert any(CIK_GAMMA in u and "/Archives/" in u for u in urls)             # one-filing fallback
    assert sum("submissions" in u for u in urls) == 3                          # one index per manager


def test_wherever_the_deadline_falls_no_request_starts_after_it_and_the_run_is_never_green(monkeypatch):
    full = _full_run_urls(monkeypatch)
    phases: set[str] = set()
    for k in range(len(full)):
        # The deadline falls half a tick into request k: that request is in
        # flight when it passes, and it is the last one.
        deadline = k + 0.5
        engine, recorder, started, results = _replay(monkeypatch, deadline)
        where = f"deadline during request {k}: {full[k]}"

        assert recorder.urls == full[:k + 1], where                             # no new call, on any path
        assert all(t < START + deadline for t in started), where
        stop = _stop(results)
        phases.add(stop["stopped_phase"])
        assert stop["deadline_overrun_s"] == 0.5, where                         # the in-flight request, accounted
        # False-green refusal: the run cannot be logged complete, and nothing
        # is on record for a manager that was not finished.
        outcome, _rows, note = _classify_outcome(results)
        assert outcome in ("PARTIAL", "FAILED") and "stopped at the run deadline" in note, where
        for cik in stop["managers_not_attempted"]:
            assert not [r for r in results if r["feature"] == f"13F:{cik}"], where
            assert _count(engine, f"13F%:{cik}:%") == 0, where
        assert stop["managers_not_attempted"][0] == stop["next_manager_cik"], where

    # between_managers: the deadline passed during Gamma's one-filing read,
    # which writes nothing; the stop is noticed before the next manager.
    assert phases == {"after_index_read", "after_current_filing_read", "after_filing_reads",
                      "request_refused", "between_managers"}


def test_a_current_filing_that_uses_up_the_deadline_does_not_start_the_prior_read(monkeypatch):
    full = _full_run_urls(monkeypatch)
    prior = "9000000001-26-000001".replace("-", "")
    first_prior = next(i for i, u in enumerate(full) if prior in u)

    # The deadline passes during the request just before the prior filing's first.
    engine, recorder, _started, results = _replay(monkeypatch, first_prior - 0.5)

    assert not [u for u in recorder.urls if prior in u]
    stop = _stop(results)
    assert stop["stopped_phase"] == "after_current_filing_read" and stop["next_manager_cik"] == s.CIK_ALPHA
    assert _count(engine, "13F%") == 0                                          # no receipt, no change row


def test_a_retry_after_the_deadline_is_not_sent(monkeypatch):
    """retry_on_failure retries a connection error. Its second attempt comes
    back through _edgar_get, which refuses it."""
    attempts: list[str] = []

    def wrapper(recorder, clock):
        def get(url, headers=None, timeout=None):
            if s.CIK_BETA in url:
                attempts.append(url)
                clock.now += 5.0                                                # the failed attempt crosses the deadline
                raise requests.ConnectionError("connection reset")
            return recorder.get(url, headers=headers, timeout=timeout)
        return get

    full = _full_run_urls(monkeypatch)
    beta_index = next(i for i, u in enumerate(full) if s.CIK_BETA in u)

    _engine, _recorder, _started, results = _replay(monkeypatch, beta_index + 2.0, wrapper)

    assert len(attempts) == 1                                                   # no retry was sent
    assert not [r for r in results if r["status"] == "FAILED"]
    assert _stop(results)["stopped_phase"] == "retry_wait_refused"
    assert _stop(results)["next_manager_cik"] == s.CIK_BETA


def test_control_the_same_connection_error_with_budget_left_is_retried(monkeypatch):
    attempts: list[str] = []

    def wrapper(recorder, _clock):
        def get(url, headers=None, timeout=None):
            if s.CIK_BETA in url:
                attempts.append(url)
                raise requests.ConnectionError("connection reset")
            return recorder.get(url, headers=headers, timeout=timeout)
        return get

    _engine, _recorder, _started, results = _replay(monkeypatch, None, wrapper)

    assert len(attempts) > 1
    assert [r["status"] for r in results if r["feature"] == f"13F:{s.CIK_BETA}"] == ["FAILED"]


def test_every_13f_request_starts_in_the_one_gateway():
    """A request path added later cannot skip the deadline check unnoticed."""
    tree = ast.parse(Path(inst.__file__).read_text())
    callers = []
    for fn in ast.walk(tree):
        if not isinstance(fn, ast.FunctionDef):
            continue
        for node in ast.walk(fn):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "get"
                    and isinstance(node.func.value, ast.Name) and node.func.value.id == "requests"):
                callers.append(fn.name)
    assert callers == ["_edgar_get"]


def test_the_gateway_refuses_once_the_budget_is_spent(engine, monkeypatch):
    def get(*_a, **_k):
        raise AssertionError("a request was started after the deadline")

    puller = _puller(engine, monkeypatch, get)
    puller._budget = inst._RunBudget(should_continue=lambda: False)

    with pytest.raises(inst._BudgetSpent):
        puller._edgar_get("https://data.sec.gov/submissions/CIK0000000001.json")
    # Not one of the errors the fetchers record as a finding or the retry decorator retries.
    assert not issubclass(inst._BudgetSpent, (requests.RequestException, OSError, TimeoutError, ValueError))


# ── Waits are bounded to the deadline too ──────────────────────────────────
#
# Codex review 6080581843: "Bound in-flight waits separately." retry_on_failure
# waits between attempts (its back-off, or a 429's Retry-After up to 30 s);
# that wait used to be free to run past the deadline. The puller now refuses a
# wait that would end after it, and the change-row lock wait is cut the same way.

DEADLINE = 1000.0


def _beta_index_fails_once(fail, seconds_left: float | None):
    """Beta's manager index fails on its first request (``fail`` builds the
    failure), optionally with only ``seconds_left`` before the deadline."""
    calls: list[str] = []

    def wrapper(recorder, clock):
        def get(url, headers=None, timeout=None):
            if "submissions" in url and s.CIK_BETA in url:
                calls.append(url)
                if len(calls) == 1:
                    if seconds_left is not None:
                        clock.now = START + DEADLINE - seconds_left
                    return fail()
            return recorder.get(url, headers=headers, timeout=timeout)
        return get

    return wrapper, calls


def _rate_limited_20s():
    response = s._Resp(status=429)
    response.headers = {"Retry-After": "20"}
    return response


def _connection_reset():
    raise requests.ConnectionError("connection reset")


@pytest.mark.parametrize("fail,seconds_left", [(_rate_limited_20s, 5.0), (_connection_reset, 1.0)],
                         ids=["retry_after_20s_with_5s_left", "backoff_with_1s_left"])
def test_a_retry_wait_that_would_pass_the_deadline_is_not_started(monkeypatch, fail, seconds_left):
    wrapper, calls = _beta_index_fails_once(fail, seconds_left)
    sleeps: list[tuple[float, float]] = []

    engine, _recorder, _started, results = _replay(monkeypatch, DEADLINE, wrapper, sleeps)

    assert len(calls) == 1                                                      # no second attempt
    assert all(at + seconds <= START + DEADLINE for at, seconds in sleeps)      # no wait ran past the deadline
    stop = _stop(results)
    assert stop["stopped_phase"] == "retry_wait_refused" and stop["next_manager_cik"] == s.CIK_BETA
    assert stop["deadline_overrun_s"] == 0.0                                    # stopped before it, not after
    assert not [r for r in results if r["status"] == "FAILED"]
    assert _count(engine, f"13F%:{s.CIK_BETA}:%") == 0
    assert _classify_outcome(results)[0] == "PARTIAL"


@pytest.mark.parametrize("fail,least_wait", [(_rate_limited_20s, 20.0), (_connection_reset, 2.0)],
                         ids=["retry_after_20s", "backoff"])
def test_control_with_time_left_the_wait_is_taken_and_the_retry_succeeds(monkeypatch, fail, least_wait):
    wrapper, calls = _beta_index_fails_once(fail, None)
    sleeps: list[tuple[float, float]] = []

    _engine, _recorder, _started, results = _replay(monkeypatch, DEADLINE, wrapper, sleeps)

    assert len(calls) == 2 and any(seconds >= least_wait for _at, seconds in sleeps)
    assert not [r for r in results if r.get("stopped_by_budget")]
    assert [r["status"] for r in results if r["feature"] == f"13F:{s.CIK_BETA}"] == ["SUCCESS"]


def test_refuses_wait():
    assert inst._RunBudget().refuses_wait(3600) is False                        # no budget: never refuses
    assert inst._RunBudget(should_continue=lambda: False).refuses_wait(0) is True
    budget = inst._RunBudget(deadline_at=time.monotonic() + 60)
    assert budget.refuses_wait(1) is False and budget.refuses_wait(120) is True
    assert 0 < budget.time_left() <= 60 and inst._RunBudget().time_left() is None
    assert inst._RunBudget(deadline_at=time.monotonic() - 5).time_left() == 0.0


class _PgConn:
    dialect = type("D", (), {"name": "postgresql"})()

    def __init__(self) -> None:
        self.sent: list[tuple[str, dict]] = []

    def execute(self, sql, params=None):
        self.sent.append((str(sql), dict(params or {})))
        return type("R", (), {"scalar": lambda _s: True})()


def test_the_change_row_lock_wait_is_cut_to_the_time_left_and_only_tried_after_the_deadline(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(inst, "_monotonic", clock)
    puller = inst.InstitutionalFlowsPuller.__new__(inst.InstitutionalFlowsPuller)
    puller._change_row_lock_wait_s = 0.0
    puller._budget = inst._RunBudget(deadline_at=START + 1.5)

    conn = _PgConn()
    puller._lock_change_rows(conn, "1067983", date(2026, 11, 14))
    assert conn.sent[0][1] == {"ms": "1500ms"}                                  # not the usual 5 s

    clock.now = START + 2.0
    puller._change_row_lock_wait_s = 0.0
    conn = _PgConn()
    puller._lock_change_rows(conn, "1067983", date(2026, 11, 14))
    assert ["pg_try_advisory_xact_lock" in sql for sql, _p in conn.sent] == [True]   # tried, never waited for
