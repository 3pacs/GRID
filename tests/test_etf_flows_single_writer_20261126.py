"""etf_flows has one 13F writer: its scheduled run (GRID #1492).

Read-only on griddb, 2026-11-26: from 11-24 03:47Z to 11-26 18:27Z there were
6,622 ``13F-FILING`` receipts for 73 filings, about 92 full 13F passes,
against 7 logged ``smart:etf_flows`` runs. The grid-hermes journal shows 89
``Retrying INSTITUTIONAL_FLOWS`` repairs in that window, each "abandoned" at
60 s while its thread went on writing, and passes overlapping (7 in one
31-minute window). These tests pin the containment:

1. Hermes never repairs etf_flows (registry ``repair_hold_reason``): the
   failed-list filter of ``diagnose_and_fix_pulls`` and the REPULL command
   both drop it before ``_retry_source``.
2. ``pull_all`` takes SmartScheduler's cooperative deadline, and the 13F pass
   stops between managers instead of being abandoned by the hard join.
3. A whole-pass advisory lock lets only one 13F writer run; a second one
   skips without reading or writing.

Written for GRID #1492 (head 98b1e3bd) and carried, with the lock and the
repair hold, by GRID #1493, whose run budget (``tests/test_13f_run_budget.py``)
replaces the between-manager stop of point 2: the same stop, plus checks
after the reads, ``deadline_at`` and a resume point.
"""

from __future__ import annotations

import inspect
import os
import time

import pytest

from ingestion import smart_scheduler as smart
from ingestion.altdata import institutional_flows as inst
from scripts import hermes_fixers as hf


@pytest.fixture(autouse=True)
def _no_resume_point(monkeypatch):
    # A stopped 13F pass leaves a process-wide resume point for the next run.
    monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_resume_13f_at_cik", None)


def _entry() -> dict:
    (entry,) = [p for p in smart.PULLER_REGISTRY if p["name"] == "etf_flows"]
    return entry


def _puller() -> inst.InstitutionalFlowsPuller:
    return inst.InstitutionalFlowsPuller.__new__(inst.InstitutionalFlowsPuller)


# ── 1. No Hermes repair ────────────────────────────────────────────────────

def test_etf_flows_runs_on_its_schedule_but_is_not_repaired() -> None:
    entry = _entry()
    assert "hold_reason" not in entry                     # still scheduled
    assert entry["repair_hold_reason"]
    held = hf._held_source_names()
    for alias in ("etf_flows", "INSTITUTIONAL_FLOWS", "smart:etf_flows"):
        assert held[hf._normalize_source_key(alias)] == entry["repair_hold_reason"]
    assert smart.hold_reason_for("etf_flows") is None


def test_the_failed_list_filter_drops_etf_flows_before_diagnosis() -> None:
    # diagnose_and_fix_pulls drops every failed row whose normalized name is
    # in _held_source_names() before any diagnosis or _retry_source call.
    import ast
    import pathlib

    source = pathlib.Path(hf.__file__).read_text(encoding="utf-8")
    (fn,) = [n for n in ast.parse(source).body
             if isinstance(n, ast.FunctionDef) and n.name == "diagnose_and_fix_pulls"]
    body = ast.get_source_segment(source, fn)
    assert body.index("held = _held_source_names()") < body.index("_retry_source(")
    held = hf._held_source_names()
    assert hf._normalize_source_key("INSTITUTIONAL_FLOWS") in held


# ── 2. Cooperative deadline ────────────────────────────────────────────────

def test_pull_all_accepts_the_scheduler_deadline() -> None:
    # SmartScheduler wires should_continue only when the method accepts it.
    params = inspect.signature(inst.InstitutionalFlowsPuller.pull_all).parameters
    assert "should_continue" in params and params["should_continue"].default is None


def test_the_13f_pass_stops_between_managers(monkeypatch) -> None:
    managers = {"111": "A", "222": "B", "333": "C"}
    monkeypatch.setattr(inst, "TOP_13F_FILERS", managers)
    monkeypatch.setattr(inst, "_EDGAR_RATE_DELAY", 0)
    fetched: list[str] = []

    def _index(_self, cik, count=2):
        fetched.append(cik)
        return []                                         # "fewer than 2 filings": nothing written

    monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_fetch_13f_index", _index)
    # The budget is checked before a manager and again after its index read:
    # True for manager 111's two checks, False before manager 222.
    calls = iter([True, True, False])
    out = _puller()._pull_13f_managers(should_continue=lambda: next(calls))

    assert fetched == ["111"]
    (stop,) = [r for r in out if r.get("stopped_by_budget")]
    assert stop["status"] == "PARTIAL" and stop["rows_inserted"] == 0
    assert stop["managers_not_attempted"] == ["222", "333"]
    assert stop["items_read"] == 1 and stop["next_manager_cik"] == "222"
    outcome, _rows, note = smart._classify_outcome(out)
    # Never logged as a full SUCCESS. FAILED rather than PARTIAL here only
    # because manager 111 had nothing to compare and so left no checked item.
    assert outcome in (smart.OUTCOME_PARTIAL, smart.OUTCOME_FAILED)
    assert "1 of 3 read, 2 not attempted" in note


def test_without_a_deadline_every_manager_is_attempted(monkeypatch) -> None:
    monkeypatch.setattr(inst, "TOP_13F_FILERS", {"111": "A", "222": "B"})
    monkeypatch.setattr(inst, "_EDGAR_RATE_DELAY", 0)
    fetched: list[str] = []
    monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_fetch_13f_index",
                        lambda _s, cik, count=2: fetched.append(cik) or [])
    assert not [r for r in _puller()._pull_13f_managers() if r.get("stopped_by_budget")]
    assert fetched == ["111", "222"]


def test_pull_all_hands_the_deadline_to_the_13f_pass(monkeypatch) -> None:
    seen: list = []
    puller = _puller()
    monkeypatch.setattr(puller, "_get_latest_date", lambda _sid: None, raising=False)
    monkeypatch.setattr(puller, "_pull_etf_flows", lambda _s, _e, _budget=None: [], raising=False)
    monkeypatch.setattr(
        puller, "_pull_13f_filings",
        lambda should_continue=None, deadline_at=None: seen.append((should_continue, deadline_at)) or [],
        raising=False)
    gate = lambda: True  # noqa: E731
    deadline = time.monotonic() + 3600
    puller.pull_all(should_continue=gate, deadline_at=deadline)
    assert seen == [(gate, deadline)]


# ── 3. One writer per 13F pass ─────────────────────────────────────────────

class _Conn:
    def __init__(self, acquired: bool) -> None:
        self.acquired, self.sent, self.closed = acquired, [], False

    def execute(self, sql, params=None):
        self.sent.append((str(sql), dict(params or {})))
        return type("R", (), {"scalar": lambda _s: self.acquired})()

    def commit(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


class _Engine:
    def __init__(self, conn: _Conn, dialect: str = "postgresql") -> None:
        self.dialect = type("D", (), {"name": dialect})()
        self._conn = conn

    def connect(self) -> _Conn:
        return self._conn


def _with_engine(engine) -> inst.InstitutionalFlowsPuller:
    puller = _puller()
    puller.engine = engine
    return puller


def test_a_held_run_lock_skips_the_whole_pass(monkeypatch) -> None:
    conn = _Conn(acquired=False)
    puller = _with_engine(_Engine(conn))
    monkeypatch.setattr(puller, "_pull_13f_managers",
                        lambda *_a, **_k: pytest.fail("no read or write without the run lock"),
                        raising=False)
    (out,) = puller._pull_13f_filings()
    assert out["status"] == "SKIPPED" and out["rows_inserted"] == 0
    assert "pg_try_advisory_lock" in conn.sent[0][0]
    assert conn.sent[0][1] == {"k": inst._13F_RUN_LOCK_KEY}
    assert conn.closed
    assert smart._classify_outcome([out])[0] == smart.OUTCOME_SKIPPED


def test_the_run_lock_is_released_even_when_the_pass_raises(monkeypatch) -> None:
    conn = _Conn(acquired=True)
    puller = _with_engine(_Engine(conn))

    def _boom(*_a, **_k):
        raise RuntimeError("EDGAR down")

    monkeypatch.setattr(puller, "_pull_13f_managers", _boom, raising=False)
    with pytest.raises(RuntimeError):
        puller._pull_13f_filings()
    statements = [s for s, _p in conn.sent]
    assert "pg_try_advisory_lock" in statements[0] and "pg_advisory_unlock" in statements[-1]
    assert conn.closed


def test_the_held_lock_passes_the_deadline_through(monkeypatch) -> None:
    puller = _with_engine(_Engine(_Conn(acquired=True)))
    seen: list = []
    monkeypatch.setattr(
        puller, "_pull_13f_managers",
        lambda should_continue=None, deadline_at=None: seen.append((should_continue, deadline_at)) or [],
        raising=False)
    gate = lambda: True  # noqa: E731
    puller._pull_13f_filings(should_continue=gate, deadline_at=12.5)
    assert seen == [(gate, 12.5)]


def test_sqlite_takes_no_run_lock(monkeypatch) -> None:
    conn = _Conn(acquired=False)
    puller = _with_engine(_Engine(conn, dialect="sqlite"))
    monkeypatch.setattr(puller, "_pull_13f_managers", lambda *_a, **_k: [], raising=False)
    assert puller._pull_13f_filings() == []
    assert conn.sent == []


def test_the_run_lock_key_is_distinct_from_the_change_row_locks() -> None:
    from ingestion.base import _stable_lock_key

    assert inst._13F_RUN_LOCK_KEY == _stable_lock_key("INSTITUTIONAL_FLOWS", "13F-run")
    assert inst._13F_RUN_LOCK_KEY != _stable_lock_key(
        "INSTITUTIONAL_FLOWS", "13F-change-rows", "1067983", "2026-11-14")


@pytest.mark.xdist_group("postgres")
def test_a_second_real_session_skips_while_the_first_holds_the_lock(monkeypatch) -> None:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get("REQUIRE_ETF_FLOWS_LOCK_PG") == "1":
            pytest.fail("GRID_TEST_DB_URL is required for the 13F run-lock PG proof (REQUIRE_ETF_FLOWS_LOCK_PG=1)")
        pytest.skip("GRID_TEST_DB_URL is required for the PostgreSQL lock proof")
    from sqlalchemy import create_engine, text

    engine = create_engine(url)
    try:
        holder = engine.connect()
        assert holder.execute(text("SELECT pg_try_advisory_lock(:k)"),
                              {"k": inst._13F_RUN_LOCK_KEY}).scalar()
        puller = _with_engine(engine)
        ran: list[int] = []
        monkeypatch.setattr(puller, "_pull_13f_managers",
                            lambda *_a, **_k: ran.append(1) or [], raising=False)
        (out,) = puller._pull_13f_filings()
        assert out["status"] == "SKIPPED" and ran == []
        holder.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": inst._13F_RUN_LOCK_KEY})
        holder.close()
        assert puller._pull_13f_filings() == [] and ran == [1]
    finally:
        engine.dispose()


def test_a_repull_command_for_etf_flows_runs_nothing_and_records_no_cooldown(monkeypatch) -> None:
    from unittest.mock import MagicMock

    monkeypatch.setattr(hf, "_resolve_puller",
                        lambda *_a, **_k: pytest.fail("REPULL must not construct the puller"))
    state = MagicMock()
    state.cooldowns.can_retry.return_value = True
    out = hf._execute_hermes_repair_command(
        "REPULL:INSTITUTIONAL_FLOWS", engine=object(), health={}, state=state,
    )
    assert out["status"] == "skipped" and out["reason"] == "held"
    state.cooldowns.record_attempt.assert_not_called()


def test_a_repull_command_for_another_source_still_reaches_the_retry(monkeypatch) -> None:
    from unittest.mock import MagicMock

    reached: list[str] = []
    monkeypatch.setattr(hf, "_retry_source",
                        lambda name, *_a, **_k: reached.append(name) or {"status": "in_flight_test"})
    state = MagicMock()
    state.cooldowns.can_retry.return_value = True
    hf._execute_hermes_repair_command("REPULL:quiverquant", engine=object(), health={}, state=state)
    assert reached == ["quiverquant"]
