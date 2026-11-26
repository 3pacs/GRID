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

import inspect
import sys
import time
import types
from datetime import date

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


def _puller(engine, monkeypatch, get) -> inst.InstitutionalFlowsPuller:
    monkeypatch.setattr(inst.requests, "get", get)
    monkeypatch.setattr(inst.time, "sleep", lambda *_a: None)
    monkeypatch.setattr(inst, "TOP_13F_FILERS", dict(s.FILERS))
    inst._DEAD_CIKS.clear()
    return inst.InstitutionalFlowsPuller(engine)


def _run(engine, monkeypatch, get, **budget) -> list[dict]:
    return _puller(engine, monkeypatch, get).pull_13f_only(**budget)


def _stop(results: list[dict]) -> dict:
    (item,) = [r for r in results if r.get("stopped_by_budget")]
    return item


# ── The run stops itself and says how far it got ───────────────────────────


def test_out_of_budget_between_managers_the_run_stops_and_says_how_far_it_got(engine, monkeypatch):
    edgar = s.two_quarter_scenario()

    results = _run(engine, monkeypatch, edgar.get, should_continue=lambda: _receipts(engine) == 0)

    assert [r["feature"] for r in results] == [f"13F:{s.CIK_ALPHA}", STOP]
    assert results[0]["status"] == "SUCCESS" and results[0]["rows_inserted"] > 0
    stop = _stop(results)
    assert stop["status"] == "PARTIAL"
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
    assert [r["feature"] for r in results] == [f"13F:{s.CIK_BETA}", STOP]
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


def test_snapshot_fetch_failure_reads_every_place_a_failed_fetch_is_recorded():
    def snap(status: str, **evidence) -> inst.FilingSnapshot:
        return inst.FilingSnapshot(accession="A", status=status, evidence=evidence)

    assert inst._snapshot_fetch_failure(snap("fetch_failed:timeout")) == "fetch_failed:timeout"
    assert inst._snapshot_fetch_failure(
        snap("unclassified_amendment", classification_reason="fetch_failed:http_503")) == "fetch_failed:http_503"
    assert inst._snapshot_fetch_failure(snap(
        "unresolved_undated_amendment",
        unresolved_amendments=[{"accession": "B", "reason": "fetch_failed:connection"}],
    )) == "fetch_failed:connection"
    for status in ("entry_count_mismatch", "missing_cover_page", "additive_supplement_not_merged"):
        assert inst._snapshot_fetch_failure(snap(status)) is None
    assert inst._snapshot_fetch_failure(
        snap("unclassified_amendment", classification_reason="unknown_amendment_type")) is None


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
