"""Schwab mode: the Yahoo tail resumes across runs, and its budget fits the scheduler's window.

A full schwab-mode run (about 80 Schwab calls for the core plus about 960 paced
Yahoo requests) needs about 19 minutes; the smart scheduler's options job has
840 s. These tests pin the two answers (2026-11-21): a per-writer cursor that
starts each run's Yahoo names after the last one the previous run finished,
with session-wide coverage deciding freshness, and a Yahoo request budget sized
to the time left in the caller's window. They also pin that no other provider
sees any of it. Offline: every client is a stand-in, every file is under
tmp_path (conftest points GRID_OPTIONS_TAIL_CURSOR_DIR there).
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

import ingestion.smart_scheduler as ss
from ingestion import options, options_schwab
from tests.test_options_schwab_provider import (
    SESSION_NOW,
    _body,
    _FakeYahoo,
    _puller,
    _schwab_run,
)

CORE = ["KO"]
TAIL = ["ACIU", "AGEN", "NUOX", "OLMA"]   # catalyst_options_universe orders by ticker
DAY = SESSION_NOW.date().isoformat()


@pytest.fixture(autouse=True)
def _session(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(options, "_utc_now", lambda: SESSION_NOW)
    monkeypatch.setenv("GRID_OPTIONS_YAHOO_HOLD_FILE", str(tmp_path / "hold.json"))


def _cursor_file() -> Path:
    return options_schwab.tail_cursor_file(options.DEFAULT_CAPTURE_SOURCE)


def _tail_order(yahoo: _FakeYahoo) -> list[str]:
    seen: list[str] = []
    for ticker, _expiry in yahoo.calls:
        if ticker not in seen:
            seen.append(ticker)
    return seen


def _run(monkeypatch: pytest.MonkeyPatch, *, core_held: bool = False, catalysts: list[str] | None = None,
         yahoo: _FakeYahoo | None = None, publish: bool = False, **kw: Any) -> tuple[Any, Any, _FakeYahoo, Any]:
    """One full-universe schwab run: KO from Schwab (or held by the 45-min gap), the rest from Yahoo."""
    answers = () if core_held else (_body(),)
    return _schwab_run(monkeypatch, CORE, list(TAIL if catalysts is None else catalysts), *answers,
                       block="schwab_recent_capture" if core_held else None,
                       yahoo=yahoo if yahoo is not None else _FakeYahoo(), publish=publish, **kw)


# ── the cursor ───────────────────────────────────────────────────────────


def test_a_budget_limited_run_is_resumed_where_it_stopped_and_the_cycle_then_publishes(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options_schwab, "YAHOO_RUN_REQUEST_BUDGET", 3)   # two names a run (two expiries each)

    _p, market, yahoo, results = _run(monkeypatch)
    assert len(market.calls) == 1 and _tail_order(yahoo) == ["ACIU", "AGEN"]
    assert [(r["ticker"], r.get("reason")) for r in results[3:]] == [
        ("NUOX", "yahoo_request_budget"), ("OLMA", "yahoo_request_budget")]
    assert results.summary["status"] == "PARTIAL" and "2 tickers not captured (NUOX, OLMA)" in results.summary["error"]
    assert ss._classify_outcome(results.summary)[0] == ss.OUTCOME_PARTIAL           # the 30-minute retry follows
    assert results.summary["coverage"] == "session_cycle" and results.summary["tickers_covered_earlier"] == 0
    state = json.loads(_cursor_file().read_text())
    assert state["last_completed"] == "AGEN" and state["covered"] == ["ACIU", "AGEN", "KO"]
    assert state["session_day"] == DAY and state["universe"] == options_schwab.universe_identity(CORE + TAIL)

    # The 30-minute retry: the core is held by the writer's own 45-minute Schwab gap.
    puller, market, yahoo, results = _run(monkeypatch, core_held=True, publish=True)
    assert market.calls == [] and _tail_order(yahoo) == ["NUOX", "OLMA"]           # not ACIU again
    assert [r["ticker"] for r in results] == ["KO", "NUOX", "OLMA", "ACIU", "AGEN"]
    assert results[0]["reason"] == "schwab_recent_capture"
    assert [r.get("reason") for r in results[3:]] == ["yahoo_request_budget"] * 2
    summary = results.summary
    assert summary["status"] == "SUCCESS", summary                                  # every name, this session
    assert summary["reason"] == "full default universe captured across this session's runs"
    assert summary["tickers_covered_earlier"] == 3                                  # KO, ACIU, AGEN
    assert puller.published == [True]
    # The smart scheduler's own classifier agrees, so the job's streak closes and it waits freq_h.
    assert ss._classify_outcome(summary)[0] == ss.OUTCOME_SUCCESS


def test_the_cursor_wraps_around(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options_schwab, "YAHOO_RUN_REQUEST_BUDGET", 5)   # three names a run
    _p, _m, yahoo, _r = _run(monkeypatch)
    assert _tail_order(yahoo) == ["ACIU", "AGEN", "NUOX"]
    _p, _m, yahoo, _r = _run(monkeypatch, core_held=True, publish=True)
    assert _tail_order(yahoo) == ["OLMA", "ACIU", "AGEN"]                          # past the end, back to the start
    assert json.loads(_cursor_file().read_text())["last_completed"] == "AGEN"
    _p, _m, yahoo, _r = _run(monkeypatch, core_held=True, publish=True)
    assert _tail_order(yahoo) == ["NUOX", "OLMA", "ACIU"]


def test_a_run_that_reads_every_name_ends_where_it_began(monkeypatch: pytest.MonkeyPatch) -> None:
    _p, _m, yahoo, results = _run(monkeypatch, publish=True)
    assert _tail_order(yahoo) == TAIL and results.summary["status"] == "SUCCESS"
    assert results.summary["reason"] == "full default universe captured"          # one run did it all
    _p, _m, yahoo, _r = _run(monkeypatch, publish=True)
    assert _tail_order(yahoo) == ["ACIU", "AGEN", "NUOX", "OLMA"]                  # after OLMA comes ACIU


def test_a_changed_catalyst_list_starts_from_the_first_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options_schwab, "YAHOO_RUN_REQUEST_BUDGET", 3)
    _run(monkeypatch)
    assert json.loads(_cursor_file().read_text())["last_completed"] == "AGEN"
    changed = ["ACIU", "AGEN", "BEAM", "NUOX", "OLMA"]
    _p, _m, yahoo, results = _run(monkeypatch, core_held=True, catalysts=changed)
    assert _tail_order(yahoo) == ["ACIU", "AGEN"]                                  # not resumed after AGEN
    state = json.loads(_cursor_file().read_text())
    assert state["universe"] == options_schwab.universe_identity(CORE + changed)
    # Captures are facts of the session whatever the list: KO still counts, so the run says 3 remain.
    assert results.summary["tickers_covered_earlier"] == 1
    assert "3 tickers not captured (BEAM, NUOX, OLMA)" in results.summary["error"]


def test_coverage_from_an_earlier_session_day_does_not_count(monkeypatch: pytest.MonkeyPatch) -> None:
    path = _cursor_file()
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"version": 1, "universe": options_schwab.universe_identity(CORE + TAIL),
                                "session_day": "2026-11-04", "last_completed": "AGEN",
                                "covered": ["KO", "ACIU", "AGEN", "NUOX", "OLMA"]}))
    _p, _m, yahoo, results = _run(monkeypatch, core_held=True, publish=False)
    assert _tail_order(yahoo) == ["NUOX", "OLMA", "ACIU", "AGEN"]                  # the position carries over
    assert results.summary["status"] == "PARTIAL" and results.summary["tickers_covered_earlier"] == 0
    assert "1 tickers not captured (KO)" in results.summary["error"]
    assert json.loads(path.read_text())["session_day"] == DAY


class _FailingYahoo(_FakeYahoo):
    """Serves no page at all for the named tickers: a failed fetch, not "no chain"."""

    def __init__(self, failing: tuple[str, ...], **kw: Any) -> None:
        super().__init__(**kw)
        self.failing = set(failing)

    def get_options(self, ticker: str, expiry_ts: int | None = None) -> Any:
        if ticker in self.failing:
            self.calls.append((ticker, expiry_ts))
            self.last_status = 200
            return None
        return super().get_options(ticker, expiry_ts)


def test_a_failed_name_moves_the_cursor_but_is_not_covered(monkeypatch: pytest.MonkeyPatch) -> None:
    _p, _m, _y, results = _run(monkeypatch, yahoo=_FailingYahoo(("AGEN",)))
    assert [r["status"] for r in results] == ["SUCCESS", "SUCCESS", "FAILED", "SUCCESS", "SUCCESS"]
    state = json.loads(_cursor_file().read_text())
    assert state["last_completed"] == "OLMA" and "AGEN" not in state["covered"]   # a failing name cannot pin it
    assert results.summary["status"] == "PARTIAL"
    _p, _m, _y, again = _run(monkeypatch, core_held=True, yahoo=_FailingYahoo(("AGEN",)))
    assert again.summary["status"] == "PARTIAL" and "1 tickers not captured (AGEN)" in again.summary["error"]


def test_the_name_a_yahoo_429_cut_short_is_read_first_next_time(monkeypatch: pytest.MonkeyPatch,
                                                                 tmp_path: Path) -> None:
    _p, _m, _y, results = _run(monkeypatch, yahoo=_FakeYahoo(limited_at_request=4))   # AGEN's second expiry
    assert results[2]["ticker"] == "AGEN" and results[2]["rate_limited"]
    assert json.loads(_cursor_file().read_text())["last_completed"] == "ACIU"
    monkeypatch.setenv("GRID_OPTIONS_YAHOO_HOLD_FILE", str(tmp_path / "later-hold.json"))   # the hold has passed
    _p, _m, yahoo, results = _run(monkeypatch, core_held=True, publish=True)
    assert _tail_order(yahoo) == ["AGEN", "NUOX", "OLMA", "ACIU"]
    assert results.summary["status"] == "SUCCESS"                                 # the session's cycle is complete


def test_a_held_tail_leaves_the_cursor_where_it_was(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options_schwab, "YAHOO_RUN_REQUEST_BUDGET", 3)
    _run(monkeypatch)
    before = _cursor_file().read_text()
    monkeypatch.setattr(options_schwab, "yahoo_tail_hold", lambda *_a, **_k: "yahoo_recent_rate_limit")
    _p, _m, yahoo, results = _run(monkeypatch, core_held=True)
    assert yahoo.calls == [] and {r.get("reason") for r in results[1:]} == {"yahoo_recent_rate_limit"}
    assert _cursor_file().read_text() == before


@pytest.mark.parametrize("content", [
    b"", b"not json", b"[]", b"null", b"\xff\xfe\x00", b"[" * 200_000 + b"]" * 200_000,
    b'{"version": 2, "universe": "x", "session_day": "2026-11-05", "last_completed": null, "covered": []}',
    b'{"version": 1, "universe": "x", "session_day": "2026-11-05", "last_completed": 5, "covered": []}',
    b'{"version": 1, "universe": "x", "session_day": "2026-11-05", "last_completed": null, "covered": [1]}',
    b'{"version": 1, "session_day": "2026-11-05", "last_completed": null, "covered": []}',
], ids=["empty", "text", "list", "null", "not-utf8", "nested", "version", "last-type", "covered-type", "missing"])
def test_a_corrupt_cursor_fails_open_and_is_replaced(monkeypatch: pytest.MonkeyPatch, content: bytes) -> None:
    path = _cursor_file()
    path.parent.mkdir(parents=True)
    path.write_bytes(content)
    warnings: list[str] = []
    monkeypatch.setattr(options_schwab.log, "warning", lambda message, **_kw: warnings.append(message))
    _p, market, yahoo, results = _run(monkeypatch, publish=True)
    assert len(market.calls) == 1 and _tail_order(yahoo) == TAIL                   # from the first name
    assert results.summary["status"] == "SUCCESS" and results.summary["tickers_covered_earlier"] == 0
    assert any("tail cursor is not in the expected form" in w for w in warnings)
    assert json.loads(path.read_text())["version"] == 1                            # rewritten whole


def test_an_unreadable_or_unwritable_cursor_never_stops_a_run(monkeypatch: pytest.MonkeyPatch) -> None:
    _cursor_file().mkdir(parents=True)                       # a directory where the file should be
    warnings: list[str] = []
    monkeypatch.setattr(options_schwab.log, "warning", lambda message, **_kw: warnings.append(message))
    _p, _m, yahoo, results = _run(monkeypatch, publish=True)
    assert _tail_order(yahoo) == TAIL and results.summary["status"] == "SUCCESS"
    assert any("unreadable" in w for w in warnings) and any("not written" in w for w in warnings)


def test_the_cursor_file_is_owner_only_and_leaves_a_shared_directory_alone(
        monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    shared = tmp_path / "state"
    shared.mkdir(mode=0o755)
    shared.chmod(0o755)
    monkeypatch.setenv("GRID_OPTIONS_TAIL_CURSOR_DIR", str(shared))
    _run(monkeypatch, publish=True)
    path = shared / "options_tail_cursor.options_puller.json"
    assert path.stat().st_mode & 0o777 == 0o600 and shared.stat().st_mode & 0o777 == 0o755
    assert sorted(p.name for p in shared.iterdir()) == [path.name]                 # no temp file left behind
    assert set(json.loads(path.read_text())) == {"version", "universe", "session_day", "last_completed", "covered"}


def test_each_writer_has_its_own_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GRID_OPTIONS_TAIL_CURSOR_DIR")
    assert options_schwab.tail_cursor_file("smart_scheduler") == Path(
        "~/.local/state/grid/options_tail_cursor.smart_scheduler.json").expanduser()
    monkeypatch.setenv("GRID_OPTIONS_TAIL_CURSOR_DIR", "/var/tmp/x")
    assert options_schwab.tail_cursor_file("daily_scheduler") == Path("/var/tmp/x/options_tail_cursor.daily_scheduler.json")
    assert options_schwab.tail_cursor_file("../../etc") == Path("/var/tmp/x/options_tail_cursor.______etc.json")


def test_subset_runs_neither_read_nor_move_the_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options_schwab.TailCursor, "load", classmethod(lambda *_a, **_k: pytest.fail("no cursor")))
    _p, _m, yahoo, results = _schwab_run(monkeypatch, CORE, TAIL, tickers=["OLMA", "ACIU"], max_expirations=6,
                                         capture_source="gem_daily", yahoo=_FakeYahoo())
    assert _tail_order(yahoo) == ["OLMA", "ACIU"] and "coverage" not in results.summary
    _p, _m, _y, results = _schwab_run(monkeypatch, CORE, TAIL, _body(), max_expirations=6, yahoo=_FakeYahoo())
    assert results.summary["scope"] == "subset" and "coverage" not in results.summary


def test_order_and_load_units(tmp_path: Path) -> None:
    path = tmp_path / "c.json"
    cursor = options_schwab.TailCursor.load(CORE + TAIL, DAY, path=path)
    assert cursor.order(TAIL) == TAIL and not path.exists()                        # loading writes nothing
    cursor.last_completed = "NUOX"
    assert cursor.order(TAIL) == ["OLMA", "ACIU", "AGEN", "NUOX"]
    cursor.last_completed = "OLMA"
    assert cursor.order(TAIL) == TAIL
    cursor.last_completed = "ZZZ"                                                  # not in the list
    assert cursor.order(TAIL) == TAIL and cursor.order([]) == []
    cursor.last_completed, cursor.covered = "AGEN", {"KO"}
    assert cursor.save() is True
    same = options_schwab.TailCursor.load(list(reversed(CORE + TAIL)), DAY, path=path)   # order-free identity
    assert same.last_completed == "AGEN" and same.covered_before == frozenset({"KO"})
    other_day = options_schwab.TailCursor.load(CORE + TAIL, "2026-11-06", path=path)
    assert other_day.last_completed == "AGEN" and other_day.covered_before == frozenset()


# ── the budget ───────────────────────────────────────────────────────────


def test_budget_for_window_units() -> None:
    reserve, per = options_schwab.YAHOO_WINDOW_RESERVE_SECONDS, options_schwab.YAHOO_BUDGET_SECONDS_PER_REQUEST
    assert options_schwab.yahoo_budget_for_window(reserve + 3 * per) == 3
    assert options_schwab.yahoo_budget_for_window(reserve) == 0
    assert options_schwab.yahoo_budget_for_window(-50.0) == 0
    assert options_schwab.yahoo_budget_for_window(float("nan")) == 0
    assert options_schwab.yahoo_budget_for_window(10_000.0) == options_schwab.YAHOO_RUN_REQUEST_BUDGET
    # The smart scheduler's window (900 s less 60 s): a run whose core was held, and one that read
    # the core (about 80 Schwab calls 2 s apart plus 41 database writes, ~240 s). Both are short of
    # the ~960 requests of a full tail, which is what the cursor is for.
    window = 900 - 60
    assert options_schwab.yahoo_budget_for_window(window) == 530
    assert options_schwab.yahoo_budget_for_window(window - 240) == 370


def test_a_caller_deadline_sizes_the_yahoo_budget_to_the_time_left(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = 5_000.0
    monkeypatch.setattr(options.time, "monotonic", lambda: clock)
    deadline = clock + options_schwab.YAHOO_WINDOW_RESERVE_SECONDS + 3 * options_schwab.YAHOO_BUDGET_SECONDS_PER_REQUEST
    puller, _m, yahoo, results = _run(monkeypatch, deadline_at=deadline, should_continue=lambda: True)
    assert puller._yahoo.yahoo_request_budget == 3
    assert _tail_order(yahoo) == ["ACIU", "AGEN"]                                  # 4 >= 3 after AGEN
    assert [r.get("reason") for r in results[3:]] == ["yahoo_request_budget"] * 2


def test_without_a_deadline_the_module_cap_applies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options_schwab, "yahoo_budget_for_window", lambda *_a: pytest.fail("no deadline, no sizing"))
    puller, _m, yahoo, _results = _run(monkeypatch, publish=True)
    assert puller._yahoo.yahoo_request_budget is None and _tail_order(yahoo) == TAIL


def test_a_held_tail_never_sizes_a_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options_schwab, "yahoo_tail_hold", lambda *_a, **_k: "yahoo_recent_rate_limit")
    monkeypatch.setattr(options_schwab, "yahoo_budget_for_window", lambda *_a: pytest.fail("tail is held"))
    puller, _m, _y, _r = _run(monkeypatch, deadline_at=0.0)
    assert puller._yahoo.yahoo_request_budget is None


# ── the scheduler ────────────────────────────────────────────────────────


def test_options_job_keeps_its_window_and_the_hermes_step_still_encloses_it() -> None:
    from scripts import hermes_operator as ho

    entry = next(p for p in ss.PULLER_REGISTRY if p["name"] == "options")
    assert (entry["timeout_s"], entry["stop_margin_s"]) == (900, 60)              # unchanged in every mode
    assert ss.TICK_TIME_BUDGET_S + entry["timeout_s"] + 60 <= ho.SMART_INGESTION_TIMEOUT_SECONDS == 1260


def _adapter() -> Any:
    adapter = ss._OptionsSchedulerAdapter.__new__(ss._OptionsSchedulerAdapter)
    adapter._engine = None
    adapter._puller = MagicMock()
    adapter._puller.pull_all.return_value = options.OptionsPullResults([], full_universe=False)
    return adapter


def test_the_adapter_passes_the_deadline_in_schwab_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", "schwab")
    monkeypatch.setenv("GRID_OPTIONS_SCHWAB_READERS", "admin-only")
    adapter, check = _adapter(), (lambda: True)
    adapter.pull(check, deadline_at=1840.0)
    adapter._puller.pull_all.assert_called_once_with(
        should_continue=check, capture_source="smart_scheduler", deadline_at=1840.0)


@pytest.mark.parametrize("provider", [None, "cboe", "yahoo", "schwab-no-readers"])
def test_every_other_provider_gets_the_adapter_call_it_got_before(
        monkeypatch: pytest.MonkeyPatch, provider: str | None) -> None:
    monkeypatch.delenv("GRID_OPTIONS_SCHWAB_READERS", raising=False)
    if provider is None:
        monkeypatch.delenv("GRID_OPTIONS_PRIMARY_PROVIDER", raising=False)
    else:
        monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", provider.removesuffix("-no-readers"))
    if provider != "schwab-no-readers":
        monkeypatch.setattr(options, "options_primary_provider", lambda: pytest.fail("not even consulted"))
    adapter, check = _adapter(), (lambda: True)
    adapter.pull(check, deadline_at=1840.0)
    adapter._puller.pull_all.assert_called_once_with(should_continue=check, capture_source="smart_scheduler")


def test_the_scheduler_hands_the_adapter_its_cooperative_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    sched = ss.SmartScheduler.__new__(ss.SmartScheduler)
    sched.engine = MagicMock()
    sched._thread_semaphore = threading.Semaphore(1)
    sched._threads_lock = threading.Lock()
    sched._active_threads = set()
    sched._orphan_thread_count = 0
    sched._update_last_pull = MagicMock()
    monkeypatch.setattr(ss.time, "monotonic", lambda: 1000.0)
    seen: dict[str, Any] = {}

    class Adapter(ss._OptionsSchedulerAdapter):
        def pull(self, should_continue: Any = None, deadline_at: float | None = None) -> dict[str, Any]:
            seen["deadline_at"] = deadline_at
            return {"status": "PARTIAL", "rows_inserted": 0, "error": "x"}

    monkeypatch.setattr(sched, "_build_puller_instance", lambda *_a: Adapter.__new__(Adapter))
    sched._run_puller(next(p for p in ss.PULLER_REGISTRY if p["name"] == "options"))
    assert seen["deadline_at"] == 1000.0 + 900 - 60


# ── the default path is untouched ────────────────────────────────────────

_LEGACY_SUMMARY_KEYS = {
    "status", "rows_inserted", "scope", "tickers_total", "tickers_ok", "tickers_deferred", "tickers_no_chain",
    "no_chain_tickers", "tickers_no_options", "snapshots", "error", "reason",
}


@pytest.mark.parametrize("provider", [None, "cboe", "yahoo"])
def test_the_default_path_is_identical_with_or_without_a_deadline(
        monkeypatch: pytest.MonkeyPatch, provider: str | None) -> None:
    if provider is None:
        monkeypatch.delenv("GRID_OPTIONS_PRIMARY_PROVIDER", raising=False)
    else:
        monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", provider)
    monkeypatch.setenv("GRID_OPTIONS_SCHWAB_READERS", "admin-only")   # inert without the schwab provider
    monkeypatch.setattr(options, "EQUITY_TICKERS", CORE)
    monkeypatch.setattr(options, "catalyst_options_universe", lambda *_a, **_kw: TAIL)
    for name in ("schwab_post_session", "yahoo_tail_hold", "schwab_core_block", "yahoo_budget_for_window",
                 "SchwabFirstOptionsClient"):
        monkeypatch.setattr(options_schwab, name, lambda *_a, **_kw: pytest.fail("schwab-mode only"))
    monkeypatch.setattr(options_schwab.TailCursor, "load", classmethod(lambda *_a, **_k: pytest.fail("no cursor")))
    runs: list[tuple[list[dict], list[tuple], dict]] = []
    for kwargs in ({}, {"deadline_at": 0.0}, {}, {"deadline_at": 1e9, "should_continue": lambda: True}):
        yahoo = _FakeYahoo()
        monkeypatch.setattr(options, "CboeFirstOptionsClient", lambda y=yahoo: y)
        monkeypatch.setattr(options, "YahooOptionsClient", lambda y=yahoo: y)
        puller = _puller(monkeypatch)
        monkeypatch.setattr(puller, "_mark_catalog_pulled", lambda **_kw: True)
        results = puller.pull_all(**kwargs)
        # capture_batch_id is a fresh uuid4 per batch; everything else must match exactly.
        runs.append(([{k: v for k, v in r.items() if k != "capture_batch_id"} for r in results],
                     list(yahoo.calls), results.summary))
    first = runs[0]
    assert [r["ticker"] for r in first[0]] == CORE + TAIL and first[2]["status"] == "SUCCESS"
    assert all(run == first for run in runs)                         # same order, requests and outcome every time
    assert {t for t, _e in first[1]} == set(CORE + TAIL)             # every name from one client, no tail split
    assert set(first[2]) == _LEGACY_SUMMARY_KEYS
    assert not Path(os.environ["GRID_OPTIONS_TAIL_CURSOR_DIR"]).exists()


@pytest.mark.parametrize("items", [
    [{"ticker": "KO", "status": "SUCCESS", "rows_inserted": 4}],
    [{"ticker": "KO", "status": "SUCCESS", "rows_inserted": 4}, {"ticker": "OLMA", "status": "DEFERRED", "rows_inserted": 0}],
    [{"ticker": "KO", "status": "SKIPPED", "rows_inserted": 0, "reason": "schwab_recent_capture"},
     {"ticker": "OLMA", "status": "SUCCESS", "rows_inserted": 2}],
    [{"ticker": "OLMA", "status": "SKIPPED", "rows_inserted": 0, "reason": "no chain listed"},
     {"ticker": "KO", "status": "FAILED", "rows_inserted": 0, "error": "x"}],
])
def test_without_session_coverage_the_summary_is_what_it_was(items: list[dict]) -> None:
    for full in (True, False):
        plain = options.OptionsPullResults(items, full_universe=full).summary
        assert options.OptionsPullResults(items, full_universe=full, covered_earlier=None).summary == plain
        assert set(plain) == _LEGACY_SUMMARY_KEYS


def test_session_coverage_only_counts_names_this_run_did_not_capture() -> None:
    items = [{"ticker": "KO", "status": "SKIPPED", "rows_inserted": 0, "reason": "schwab_recent_capture"},
             {"ticker": "OLMA", "status": "SUCCESS", "rows_inserted": 2},
             {"ticker": "NUOX", "status": "DEFERRED", "rows_inserted": 0, "reason": "time budget"}]
    partial = options.OptionsPullResults(items, full_universe=True, covered_earlier=frozenset({"KO", "OLMA"})).summary
    assert partial["status"] == "PARTIAL" and partial["tickers_covered_earlier"] == 1
    assert "1 tickers not captured (NUOX)" in partial["error"]
    done = options.OptionsPullResults(items, full_universe=True, covered_earlier=frozenset({"KO", "NUOX"})).summary
    assert done["status"] == "SUCCESS" and done["tickers_covered_earlier"] == 2 and done["rows_inserted"] == 2
    subset = options.OptionsPullResults(items, full_universe=False, covered_earlier=frozenset({"KO", "NUOX"})).summary
    assert subset["status"] == "PARTIAL"                             # an explicit list is never the source
