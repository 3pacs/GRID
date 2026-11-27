"""A forward pass in which every candidate abstained is loud (oracle/forward_health.py).

2026-11-23 to 2026-11-25: 1,830 ``no_data`` rows over three sessions, each pass
reported ``completed``, nobody was told. These tests replay that shape.

SQLite runs the real statements (the provenance fragments included): it proves
the classification and the cohort filter, not PostgreSQL plans. Offline: no
provider, network or PostgreSQL access.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import types
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

from oracle import forward_health as fh
from store import options_provenance as op

ROOT = Path(__file__).resolve().parents[1]

OLD_REASON = "quote is older than the 20-minute reference limit"
SESSION_REASON = "quote belongs to a different market session"
WED = date(2026, 11, 25)             # a full session; the third day of the incident
DECISION = datetime(2026, 11, 25, 15, 43, 32, tzinfo=timezone.utc)


def _utc(day: date, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=timezone.utc)


@pytest.fixture
def db():
    sqlite3.register_converter("DATE", lambda raw: date.fromisoformat(raw.decode()))
    sqlite3.register_converter("TIMESTAMPTZ", lambda raw: datetime.fromisoformat(raw.decode()))
    engine = create_engine("sqlite://", connect_args={"detect_types": sqlite3.PARSE_DECLTYPES})
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE oracle_iterations (iteration_at TIMESTAMPTZ, models_updated INTEGER,"
            " predictions_scored INTEGER, notes TEXT)"))
        conn.execute(text(
            "CREATE TABLE oracle_predictions (id TEXT PRIMARY KEY, created_at TIMESTAMPTZ, ticker TEXT,"
            " direction TEXT, verdict TEXT, score_notes TEXT, recording_policy TEXT, flow_context TEXT,"
            " dedup_keep BOOLEAN DEFAULT TRUE)"))
        conn.execute(text(
            "CREATE TABLE options_capture_batches (capture_batch_id TEXT PRIMARY KEY, ticker TEXT,"
            " snap_date DATE, capture_ordinal INTEGER, capture_completed_at TIMESTAMPTZ,"
            " registered_at TIMESTAMPTZ, spot_price REAL, spot_provider TEXT, clock_metadata TEXT)"))
    yield engine
    engine.dispose()


_SEQ = iter(range(1, 10**9))


def _attempts(conn: Any, day: date, n: int, *, first_hour: int = 14) -> None:
    for i in range(n):
        conn.execute(text(
            "INSERT INTO oracle_iterations VALUES (:at, 0, 0, :note)"),
            {"at": _utc(day, first_hour + i, 31).isoformat(sep=" "), "note": fh.FORWARD_ATTEMPT_NOTE})


def _rows(conn: Any, ticker: str, at: datetime, n: int, *, direction: str = "NONE",
          reason: str | None = OLD_REASON, private: bool = False,
          policy: str = fh.FORWARD_RECORDING_POLICY) -> None:
    abstained = direction == "NONE"
    for _ in range(n):
        conn.execute(text(
            "INSERT INTO oracle_predictions (id, created_at, ticker, direction, verdict, score_notes,"
            " recording_policy, flow_context) VALUES (:id, :at, :t, :d, :v, :notes, :rp, :fc)"),
            {"id": f"of2:{next(_SEQ)}", "at": at.isoformat(sep=" "), "t": ticker, "d": direction,
             "v": "no_data" if abstained else "pending", "notes": reason if abstained else None,
             "rp": policy, "fc": json.dumps({"private_use": "schwab"} if private else {})})


def _batch(conn: Any, ticker: str, completed: datetime, provider: str | None, ordinal: int) -> None:
    conn.execute(text(
        "INSERT INTO options_capture_batches VALUES (:id, :t, :d, :o, :c, :c, 100.0, :p, :m)"),
        {"id": f"{ticker}-{ordinal}", "t": ticker, "d": completed.date().isoformat(), "o": ordinal,
         "c": completed.isoformat(sep=" "), "p": provider,
         "m": json.dumps({"provider": provider}) if provider == "schwab" else None})


def _incident(conn: Any) -> None:
    """2026-11-25 as production recorded it: six attempts, 305 abstentions per ticker."""
    _attempts(conn, WED, 6)
    for ticker in ("SPY", "QQQ"):
        _rows(conn, ticker, DECISION, 305)
        _batch(conn, ticker, _utc(WED, 14, 45, 37), "yahoo", 10)       # the quote the Oracle froze
        _batch(conn, ticker, _utc(WED, 15, 43, 17), "schwab", 20)      # 15 s old, and withheld
    _rows(conn, "QQQ", _utc(date(2026, 11, 19), 15, 42, 44), 111, direction="CALL")


# ── Constants stay tied to the writer ─────────────────────────────────


def test_cohort_note_and_policy_match_the_operator_and_the_writer() -> None:
    import scripts.hermes_operator as ho
    from oracle.dedup_index import FORWARD_RECORDING_POLICY

    assert fh.FORWARD_TICKERS == ho.ORACLE_FORWARD_TICKERS
    assert fh.FORWARD_ATTEMPT_NOTE == ho.ORACLE_FORWARD_ATTEMPT_NOTE
    assert fh.FORWARD_RECORDING_POLICY == FORWARD_RECORDING_POLICY


# ── One pass ──────────────────────────────────────────────────────────


def _detail(**tickers: dict[str, int]) -> dict[str, Any]:
    return {"tickers": tickers, "abstention_reasons": {OLD_REASON: 610}}


def test_a_pass_where_every_ticker_abstained_is_all_abstained() -> None:
    outcome = fh.pass_outcome(["SPY", "QQQ"], _detail(SPY={"abstained": 305}, QQQ={"abstained": 305}))
    assert outcome["all_abstained"] is True
    assert outcome["tickers_abstained"] == ["SPY", "QQQ"]
    assert outcome["tickers_with_forecast"] == []
    assert outcome["abstention_reasons"] == {OLD_REASON: 610}


def test_repeated_abstentions_later_in_the_day_are_still_all_abstained() -> None:
    # Passes 2..6 of the incident stored nothing new: every candidate was a duplicate abstention.
    outcome = fh.pass_outcome(["SPY", "QQQ"], _detail(
        SPY={"duplicate_abstentions": 305}, QQQ={"duplicate_abstentions": 305}))
    assert outcome["all_abstained"] is True


def test_repeating_this_mornings_forecasts_is_not_a_failure() -> None:
    # Zero new qualified rows, but the tickers already have today's forecast.
    outcome = fh.pass_outcome(["SPY", "QQQ"], _detail(
        SPY={"duplicate_forecasts": 40}, QQQ={"duplicate_forecasts": 111}))
    assert outcome["all_abstained"] is False
    assert outcome["tickers_with_forecast"] == ["SPY", "QQQ"]


def test_one_ticker_with_a_forecast_is_not_all_abstained_and_names_the_other() -> None:
    outcome = fh.pass_outcome(["SPY", "QQQ"], _detail(SPY={"abstained": 305}, QQQ={"qualified": 111}))
    assert outcome["all_abstained"] is False
    assert outcome["tickers_with_forecast"] == ["QQQ"]
    assert outcome["tickers_abstained"] == ["SPY"]


def test_a_captured_ticker_with_no_candidate_is_named() -> None:
    outcome = fh.pass_outcome(["SPY", "QQQ"], _detail(QQQ={"qualified": 111}))
    assert outcome["tickers_without_candidates"] == ["SPY"]
    assert outcome["all_abstained"] is False


@pytest.mark.parametrize("detail", [None, {}, {"tickers": None}, "junk"])
def test_missing_detail_never_claims_all_abstained(detail: Any) -> None:
    outcome = fh.pass_outcome(["SPY", "QQQ"], detail)
    assert outcome["all_abstained"] is False
    assert outcome["tickers_without_candidates"] == []


def test_nothing_captured_is_not_all_abstained() -> None:
    assert fh.pass_outcome([], _detail(SPY={"abstained": 305}))["all_abstained"] is False


# ── Which session ─────────────────────────────────────────────────────


@pytest.mark.parametrize(("now", "day", "state"), [
    ("2026-11-25T15:43:32+00:00", "2026-11-25", "open"),
    ("2026-11-25T14:29:59+00:00", "2026-11-24", "ended"),     # before the open: yesterday's
    ("2026-11-25T21:00:00+00:00", "2026-11-25", "ended"),
    ("2026-11-26T16:00:00+00:00", "2026-11-25", "ended"),     # Thanksgiving
    ("2026-11-27T17:59:59+00:00", "2026-11-27", "open"),      # half session, closes 13:00 ET
    ("2026-11-27T18:00:00+00:00", "2026-11-27", "ended"),
    ("2026-11-29T16:00:00+00:00", "2026-11-27", "ended"),     # Sunday
])
def test_session_follows_the_market_calendar(now: str, day: str, state: str) -> None:
    session = fh.forward_session(datetime.fromisoformat(now))
    assert (session["session_date"].isoformat(), session["state"]) == (day, state)


def test_ended_only_skips_the_session_that_is_still_open() -> None:
    session = fh.forward_session(datetime.fromisoformat("2026-11-25T15:43:32+00:00"), ended_only=True)
    assert (session["session_date"].isoformat(), session["state"]) == ("2026-11-24", "ended")


def test_session_needs_a_zoned_clock() -> None:
    with pytest.raises(ValueError):
        fh.forward_session(datetime(2026, 11, 25, 15))


# ── A whole session, from the durable record ──────────────────────────


def test_the_incident_session_is_failing_and_says_why(db) -> None:
    with db.begin() as conn:
        _incident(conn)
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=True)
    assert health["status"] == fh.STATUS_ALL_ABSTAINED
    assert health["level"] == "failing"
    assert health["attempts"] == 6
    assert health["tickers"]["SPY"] == {
        "qualified": 0, "abstained": 305, "top_reason": OLD_REASON,
        "first_forecast_at": None, "first_abstention_at": DECISION.isoformat(),
    }
    assert health["abstention_reasons"] == {OLD_REASON: 610}
    assert health["last_qualified_at"] == "2026-11-19T15:42:44+00:00"
    assert "all 610 candidates abstained over 6 attempt(s)" in health["message"]
    assert OLD_REASON in health["message"]
    assert "Last real forecast: 2026-11-19T15:42:44+00:00" in health["message"]


def test_admin_is_told_a_newer_private_use_capture_was_withheld(db) -> None:
    with db.begin() as conn:
        _incident(conn)
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=True)
    assert set(health["operator_notes"]) == {"SPY", "QQQ"}
    note = health["operator_notes"]["SPY"]
    assert "2026-11-25T15:43:17+00:00" in note and "private-use (Schwab) batch" in note
    assert op.ORACLE_SCHWAB_ENV + "=" + op.ORACLE_SCHWAB_ON in note
    assert "100.0" not in note                       # never a price


@pytest.mark.parametrize("flag", [False, None, 1, "true"])
def test_everyone_else_gets_the_status_without_the_private_capture_note(db, flag: Any) -> None:
    with db.begin() as conn:
        _incident(conn)
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=flag)
    assert health["status"] == fh.STATUS_ALL_ABSTAINED      # these abstentions are public rows
    assert health["cohort"] == "public"
    assert "operator_notes" not in health
    assert "Schwab" not in json.dumps(health) and "private" not in json.dumps(health)


def test_forecasts_for_every_ticker_are_ok(db) -> None:
    with db.begin() as conn:
        _attempts(conn, WED, 2)
        _rows(conn, "SPY", DECISION, 40, direction="PUT")
        _rows(conn, "QQQ", DECISION, 111, direction="CALL")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 16), include_schwab=True)
    assert (health["status"], health["level"]) == (fh.STATUS_OK, "ok")
    assert health["operator_notes"] == {}


def test_an_abstention_before_a_later_forecast_is_not_a_failure(db) -> None:
    # 14:31 pass abstains, the 15:43 pass has a qualified quote: the session has its forecast.
    with db.begin() as conn:
        _attempts(conn, WED, 2)
        for ticker in ("SPY", "QQQ"):
            _rows(conn, ticker, _utc(WED, 14, 31, 6), 305, reason=SESSION_REASON)
            _rows(conn, ticker, DECISION, 80, direction="CALL")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 16), include_schwab=True)
    assert health["status"] == fh.STATUS_OK
    assert health["tickers"]["SPY"]["abstained"] == 305


def test_a_cohort_ticker_without_a_forecast_is_degraded_and_named(db) -> None:
    # SPY has never produced a forward forecast; QQQ has. That must not read as healthy.
    with db.begin() as conn:
        _attempts(conn, WED, 2)
        _rows(conn, "QQQ", DECISION, 111, direction="CALL")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 16), include_schwab=True)
    assert (health["status"], health["level"]) == (fh.STATUS_PARTIAL, "degraded")
    assert health["message"] == "Oracle forward pass has no forecast for SPY in the 2026-11-25 session."


def test_passes_that_stored_nothing_are_failing(db) -> None:
    # 2026-11-20: six attempts, the provider refused every capture, no row at all.
    with db.begin() as conn:
        _attempts(conn, WED, 6)
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=True)
    assert (health["status"], health["level"]) == (fh.STATUS_NO_ROWS, "failing")
    assert "6 attempt(s) ran and stored nothing" in health["message"]


def test_no_pass_at_all_is_failing_once_the_grace_period_is_over(db) -> None:
    with db.connect() as conn:
        early = fh.forward_health(conn, now=_utc(WED, 15, 59), include_schwab=True)
        late = fh.forward_health(conn, now=_utc(WED, 16, 1), include_schwab=True)
        ended = fh.forward_health(conn, now=_utc(WED, 22), include_schwab=True)
    assert (early["status"], early["level"]) == (fh.STATUS_PENDING, "pending")
    assert (late["status"], late["level"]) == (fh.STATUS_NO_ATTEMPTS, "failing")
    assert ended["status"] == fh.STATUS_NO_ATTEMPTS
    assert "No real forecast in the 90 days before it." in ended["message"]


def test_private_use_rows_never_reach_the_public_cohort(db) -> None:
    # After the switch is on, every row of the session is tagged; abstentions too.
    with db.begin() as conn:
        _attempts(conn, WED, 2)
        _rows(conn, "SPY", DECISION, 40, direction="PUT", private=True)
        _rows(conn, "QQQ", DECISION, 111, direction="CALL", private=True)
    with db.connect() as conn:
        admin = fh.forward_health(conn, now=_utc(WED, 16), include_schwab=True)
        public = fh.forward_health(conn, now=_utc(WED, 16), include_schwab=False)
    assert admin["status"] == fh.STATUS_OK
    assert (public["status"], public["level"]) == (fh.STATUS_NO_PUBLIC_FORECAST, "info")
    assert public["tickers"]["QQQ"]["qualified"] == 0 and public["last_qualified_at"] is None


def test_the_last_real_forecast_is_looked_for_in_a_bounded_window(db) -> None:
    with db.begin() as conn:
        _attempts(conn, WED, 1)
        _rows(conn, "QQQ", _utc(WED, 15) - fh.LAST_FORECAST_LOOKBACK - timedelta(days=2), 3, direction="CALL")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=True)
    assert health["last_qualified_at"] is None
    assert "No real forecast in the 90 days before it." in health["message"]


def test_other_recording_policies_and_other_days_do_not_count(db) -> None:
    with db.begin() as conn:
        _attempts(conn, WED, 1)
        _rows(conn, "SPY", DECISION, 5, direction="CALL", policy="oracle_external_v2")
        _rows(conn, "SPY", _utc(date(2026, 11, 24), 15), 5, direction="CALL")
        _rows(conn, "QQQ", _utc(date(2026, 11, 24), 15), 5, direction="CALL")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=True)
    assert health["status"] == fh.STATUS_NO_ROWS
    assert health["last_qualified_at"] == "2026-11-24T15:00:00+00:00"


def test_the_sweep_judges_the_last_session_that_closed(db) -> None:
    with db.begin() as conn:
        _incident(conn)
    with db.connect() as conn:                   # Friday 11-27 mid-session; Thursday was a holiday
        health = fh.forward_health(conn, now=_utc(date(2026, 11, 27), 15), include_schwab=True,
                                   ended_only=True)
    assert (health["session_date"], health["status"]) == ("2026-11-25", fh.STATUS_ALL_ABSTAINED)


def test_alert_text_carries_the_counts_reasons_and_notes(db) -> None:
    with db.begin() as conn:
        _incident(conn)
    with db.connect() as conn:
        subject, body = fh.alert_text(fh.forward_health(conn, now=_utc(WED, 21), include_schwab=True))
    assert subject == "Oracle forward pass: no usable forecast (2026-11-25)"
    assert "SPY: 0 forecast(s), 305 abstention(s); " + OLD_REASON in body
    assert "private-use (Schwab) batch" in body
    assert "Nothing was repaired or regenerated." in body


# ── The same statements on PostgreSQL ─────────────────────────────────


@pytest.fixture
def pgdb(pg_engine):
    """The three tables with production column types, in a throwaway schema of a ``_test`` database."""
    from uuid import uuid4

    from sqlalchemy.schema import CreateSchema, DropSchema

    if not (pg_engine.url.database or "").endswith("_test"):
        pytest.skip("needs a disposable _test database")
    schema = "forward_health_" + uuid4().hex
    with pg_engine.begin() as conn:
        conn.execute(CreateSchema(schema))
    engine = create_engine(pg_engine.url, connect_args={"options": "-c search_path=" + schema})
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "CREATE TABLE oracle_iterations (iteration_at TIMESTAMPTZ, models_updated INTEGER,"
                " predictions_scored INTEGER, notes TEXT)"))
            conn.execute(text(
                "CREATE TABLE oracle_predictions (id TEXT PRIMARY KEY, created_at TIMESTAMPTZ, ticker TEXT,"
                " direction TEXT, verdict TEXT, score_notes TEXT, recording_policy TEXT, flow_context JSONB,"
                " dedup_keep BOOLEAN DEFAULT TRUE)"))
            conn.execute(text(
                "CREATE TABLE options_capture_batches (capture_batch_id TEXT PRIMARY KEY, ticker TEXT,"
                " snap_date DATE, capture_ordinal BIGINT, capture_completed_at TIMESTAMPTZ,"
                " registered_at TIMESTAMPTZ, spot_price DOUBLE PRECISION, spot_provider TEXT,"
                " clock_metadata JSONB)"))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(DropSchema(schema, cascade=True))


def test_the_incident_reads_the_same_on_postgresql(pgdb) -> None:
    with pgdb.begin() as conn:
        _incident(conn)
        _rows(conn, "SPY", _utc(date(2026, 11, 24), 15), 3, direction="CALL", private=True)
    with pgdb.connect() as conn:
        admin = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=True)
        public = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=False)
        assert op.withheld_capture_note(conn, "SPY", as_of=_utc(WED, 15, 43, 16)) is None
    assert admin["status"] == public["status"] == fh.STATUS_ALL_ABSTAINED
    assert admin["attempts"] == 6 and admin["abstention_reasons"] == {OLD_REASON: 610}
    assert admin["tickers"]["QQQ"]["first_abstention_at"] == DECISION.isoformat()
    assert "completed 2026-11-25T15:43:17+00:00" in admin["operator_notes"]["SPY"]
    # The private-use forecast of the day before is the admin's last real forecast, never the public one.
    assert admin["last_qualified_at"] == "2026-11-24T15:00:00+00:00"
    assert public["last_qualified_at"] == "2026-11-19T15:42:44+00:00"
    assert "operator_notes" not in public


# ── The withheld-capture note ─────────────────────────────────────────


def test_note_is_none_when_the_newest_capture_is_public(db) -> None:
    with db.begin() as conn:
        _batch(conn, "SPY", _utc(WED, 15, 43, 17), "schwab", 10)
        _batch(conn, "SPY", _utc(WED, 15, 43, 20), "yahoo", 20)
    with db.connect() as conn:
        assert op.withheld_capture_note(conn, "SPY", as_of=DECISION) is None
        assert op.withheld_capture_note(conn, "QQQ", as_of=DECISION) is None      # no capture at all


def test_note_judges_what_was_in_the_table_at_the_decision(db) -> None:
    with db.begin() as conn:
        _batch(conn, "SPY", _utc(WED, 14, 45, 37), "yahoo", 10)
        _batch(conn, "SPY", _utc(WED, 15, 43, 17), "schwab", 20)
    with db.connect() as conn:
        assert op.withheld_capture_note(conn, "SPY", as_of=_utc(WED, 15, 43, 16)) is None
        assert "completed 2026-11-25T15:43:17+00:00" in op.withheld_capture_note(conn, "SPY", as_of=DECISION)


def test_note_fails_closed_on_an_unknown_provider_after_the_cutoff(db) -> None:
    with db.begin() as conn:
        _batch(conn, "SPY", _utc(WED, 15, 43, 17), None, 20)
    with db.connect() as conn:
        assert op.withheld_capture_note(conn, "SPY", as_of=DECISION) is not None


# ── The engine records what each ticker ended with ────────────────────


def test_generation_adds_up_ticker_outcomes_and_reasons_across_batches(monkeypatch) -> None:
    from oracle import calibration, hallucination_guard
    from oracle import engine as oe

    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = object()
    oracle._load_models = lambda: [SimpleNamespace(name="fixture", hits=0, misses=0, partials=0)]
    monkeypatch.setenv("GRID_ORACLE_PARALLELISM", "1")
    monkeypatch.setenv("GRID_ORACLE_BATCH_SIZE", "1")
    monkeypatch.setattr(calibration, "compute_calibration", lambda engine: None)
    monkeypatch.setattr(hallucination_guard, "verify_predictions", lambda preds, **kw: (preds, []))
    oracle._oracle_one_ticker = lambda idx, ticker, total, now, cache, **kw: []
    per_call = iter([
        {"tickers": {"SPY": {"abstained": 305}}, "abstention_reasons": {OLD_REASON: 305}},
        {"tickers": {"QQQ": {"abstained": 300, "qualified": 5}}, "abstention_reasons": {OLD_REASON: 300}},
    ])

    def store(preds):
        oracle._last_store_detail = next(per_call)
        oracle._last_store_counts = {}
        return []

    oracle._store_predictions = store
    oracle.generate_predictions(["SPY", "QQQ"], horizon_days=1)
    assert oracle._last_forward_detail == {
        "tickers": {"SPY": {"abstained": 305}, "QQQ": {"abstained": 300, "qualified": 5}},
        "abstention_reasons": {OLD_REASON: 605},
    }
    assert fh.pass_outcome(["SPY", "QQQ"], oracle._last_forward_detail)["all_abstained"] is False


def test_a_store_that_reports_no_detail_does_not_inherit_the_previous_batch(monkeypatch) -> None:
    from oracle import calibration, hallucination_guard
    from oracle import engine as oe

    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = object()
    oracle._load_models = lambda: [SimpleNamespace(name="fixture", hits=0, misses=0, partials=0)]
    monkeypatch.setenv("GRID_ORACLE_PARALLELISM", "1")
    monkeypatch.setenv("GRID_ORACLE_BATCH_SIZE", "1")
    monkeypatch.setattr(calibration, "compute_calibration", lambda engine: None)
    monkeypatch.setattr(hallucination_guard, "verify_predictions", lambda preds, **kw: (preds, []))
    oracle._oracle_one_ticker = lambda idx, ticker, total, now, cache, **kw: []
    calls = []

    def store(preds):
        calls.append(1)
        if len(calls) == 1:
            oracle._last_store_detail = {"tickers": {"SPY": {"abstained": 305}},
                                         "abstention_reasons": {OLD_REASON: 305}}
        oracle._last_store_counts = {}
        return []

    oracle._store_predictions = store
    oracle.generate_predictions(["SPY", "QQQ"], horizon_days=1)
    assert oracle._last_forward_detail["abstention_reasons"] == {OLD_REASON: 305}


# ── The operator: status and alert ────────────────────────────────────


@pytest.fixture
def ho():
    import scripts.hermes_operator as module

    return module


class _Lease:
    """The two reads the pass makes on its lease, and nothing else."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: Any, params: Any = None) -> Any:
        sql = str(statement)
        self.statements.append(sql)
        value = True if "pg_try_advisory_lock" in sql else None
        return SimpleNamespace(scalar_one=lambda: value)

    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def __enter__(self) -> "_Lease":
        return self

    def __exit__(self, *exc: Any) -> None: ...


class _PassEngine:
    def __init__(self) -> None:
        self.lease = _Lease()

    def connect(self) -> _Lease:
        return self.lease

    def begin(self) -> _Lease:
        return _Lease()


def _stub_pass(monkeypatch, ho, *, detail: Any, counts: dict[str, int]) -> None:
    from ingestion import options
    from oracle import engine as oracle_engine

    class Oracle:
        def __init__(self, db_engine: Any) -> None:
            self._last_store_counts = counts
            self._last_forward_detail = detail

        def score_expired_predictions(self) -> dict:
            return {"total": 0}

        def generate_predictions(self, tickers, *, horizon_days, should_continue) -> list:
            return []

        def _get_leaderboard(self) -> list:
            return []

    class Capture:
        def __init__(self, db_engine: Any) -> None: ...

        def pull_all(self, tickers, *, capture_source, should_continue) -> list:
            return [{"ticker": ticker, "status": "SUCCESS"} for ticker in tickers]

    monkeypatch.setattr(oracle_engine, "OracleEngine", Oracle)
    monkeypatch.setattr(options, "OptionsPuller", Capture)
    monkeypatch.setattr(options, "automatic_options_capture_guard", lambda engine: None)
    monkeypatch.setattr(ho, "_oracle_forward_regular_session", lambda now: True)


def test_the_incident_pass_reports_all_abstained_not_completed(monkeypatch, ho) -> None:
    _stub_pass(monkeypatch, ho,
               detail=_detail(SPY={"abstained": 305}, QQQ={"abstained": 305}),
               counts={"qualified": 0, "inserted": 610, "abstentions": 610})
    result = ho._run_oracle_forward_pass(_PassEngine(), should_continue=lambda: True)
    assert result["status"] == "all_abstained"
    assert result["new_predictions"] == 0 and result["new_abstentions"] == 610
    assert result["tickers_abstained"] == ["SPY", "QQQ"]
    assert result["abstention_reasons"] == {OLD_REASON: 610}


def test_a_pass_that_repeats_the_mornings_forecasts_still_reports_completed(monkeypatch, ho) -> None:
    _stub_pass(monkeypatch, ho,
               detail=_detail(SPY={"duplicate_forecasts": 40}, QQQ={"duplicate_forecasts": 111}),
               counts={"qualified": 0, "inserted": 0, "duplicates": 151})
    result = ho._run_oracle_forward_pass(_PassEngine(), should_continue=lambda: True)
    assert result["status"] == "completed"
    assert result["tickers_with_forecast"] == ["SPY", "QQQ"]


def _alert_harness(monkeypatch, health_status: str = fh.STATUS_ALL_ABSTAINED, sends: bool = True):
    sent: list[tuple[str, str, str]] = []
    email = types.ModuleType("alerts.email")

    def send_alert(subject: str, body: str, severity: str = "info") -> bool:
        sent.append((subject, body, severity))
        return sends

    email.send_alert = send_alert                               # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "alerts.email", email)
    health = {
        "status": health_status, "session_date": "2026-11-25", "attempts": 2,
        "last_attempt_at": None, "message": "No usable Oracle forecast in the 2026-11-25 session.",
        "tickers": {"SPY": {"qualified": 0, "abstained": 305, "top_reason": OLD_REASON}},
        "operator_notes": {"SPY": "the newest SPY capture is a private-use (Schwab) batch"},
    }
    seen: list[Any] = []

    def fake_health(conn, *, now, include_schwab=False, **kw):
        seen.append(include_schwab)
        return health

    monkeypatch.setattr(fh, "forward_health", fake_health)
    return sent, seen, SimpleNamespace(), MagicMock()


def test_all_abstained_alerts_on_the_first_pass_and_once_per_session(monkeypatch, ho) -> None:
    sent, seen, state, engine = _alert_harness(monkeypatch)
    result = {"status": "all_abstained"}
    assert ho._oracle_forward_alert(engine, state, result, DECISION) is True
    assert ho._oracle_forward_alert(engine, state, result, DECISION + timedelta(hours=1)) is False
    assert len(sent) == 1
    subject, body, severity = sent[0]
    assert subject == "Oracle forward pass: no usable forecast (2026-11-25)"
    assert severity == "critical"
    assert "private-use (Schwab) batch" in body and "This pass: all_abstained" in body
    assert seen == [True]                                # the operator reads every row
    # The next session alerts again.
    assert ho._oracle_forward_alert(engine, state, result, DECISION + timedelta(days=5)) is True
    assert len(sent) == 2


def test_one_pass_without_a_capture_waits_and_the_second_alerts(monkeypatch, ho) -> None:
    sent, _seen, state, engine = _alert_harness(monkeypatch, fh.STATUS_NO_ROWS)
    no_capture = {"status": "abstained", "reason": "no_fresh_cohort_capture"}
    assert ho._oracle_forward_alert(engine, state, no_capture, DECISION) is False
    assert sent == []
    failed = {"status": "failed", "reason": "timeout_or_worker_failure"}
    assert ho._oracle_forward_alert(engine, state, failed, DECISION + timedelta(hours=1)) is True
    assert "This pass: failed (timeout_or_worker_failure)" in sent[0][1]


def test_a_pass_with_a_forecast_resets_the_count(monkeypatch, ho) -> None:
    sent, _seen, state, engine = _alert_harness(monkeypatch, fh.STATUS_NO_ROWS)
    no_capture = {"status": "abstained", "reason": "no_fresh_cohort_capture"}
    ho._oracle_forward_alert(engine, state, no_capture, DECISION)
    ho._oracle_forward_alert(engine, state, {"status": "completed"}, DECISION)
    assert ho._oracle_forward_alert(engine, state, no_capture, DECISION) is False
    assert sent == []


@pytest.mark.parametrize("result", [None, {}, {"status": "deferred", "reason": "hourly_cadence"}])
def test_a_deferred_pass_is_not_a_pass(monkeypatch, ho, result: Any) -> None:
    sent, seen, state, engine = _alert_harness(monkeypatch)
    assert ho._oracle_forward_alert(engine, state, result, DECISION) is False
    assert sent == [] and seen == [] and not hasattr(state, "oracle_forward_alerts")


def test_no_alert_when_the_record_shows_a_forecast_for_the_session(monkeypatch, ho) -> None:
    sent, _seen, state, engine = _alert_harness(monkeypatch, fh.STATUS_PARTIAL)
    assert ho._oracle_forward_alert(engine, state, {"status": "all_abstained"}, DECISION) is False
    assert sent == []


def test_an_alert_that_could_not_be_sent_is_tried_again(monkeypatch, ho) -> None:
    sent, _seen, state, engine = _alert_harness(monkeypatch, sends=False)
    result = {"status": "all_abstained"}
    assert ho._oracle_forward_alert(engine, state, result, DECISION) is False
    assert ho._oracle_forward_alert(engine, state, result, DECISION + timedelta(hours=1)) is False
    assert len(sent) == 2


def _run_step(monkeypatch, ho, worker_result: Any, *, ok: bool = True):
    class SessionClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return DECISION.astimezone(tz)

    monkeypatch.setattr(ho, "datetime", SessionClock)
    monkeypatch.setattr(ho, "time", SimpleNamespace(monotonic=lambda: 1000.0))
    monkeypatch.setattr(ho, "_run_with_timeout", lambda name, work, timeout, state: (worker_result, ok))
    alerts: list[Any] = []
    monkeypatch.setattr(ho, "_oracle_forward_alert",
                        lambda engine, state, result, now: alerts.append((result, now)))
    report = types.ModuleType("oracle.report")
    report.send_oracle_report = MagicMock()                     # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "oracle.report", report)
    state = SimpleNamespace(last_oracle_cycle=None, current_step=None, cooldowns=MagicMock())
    state.cooldowns.can_retry.return_value = True
    cycle_result: dict[str, Any] = {}
    ho._run_oracle_forward_step(MagicMock(), state, False, cycle_result, 1000.0 + 4500)
    return state, cycle_result, alerts, report.send_oracle_report


def test_the_step_hands_an_all_abstained_pass_to_the_alert(monkeypatch, ho) -> None:
    result = {"status": "all_abstained", "new_predictions": 0}
    state, cycle_result, alerts, report = _run_step(monkeypatch, ho, result)
    assert cycle_result["oracle"] == result
    assert alerts == [(result, DECISION)]
    # The worker ran to the end: the timeout cooldown is unchanged, and no report is sent.
    state.cooldowns.record_attempt.assert_called_once_with("oracle_cycle", success=True)
    report.assert_not_called()


def test_the_step_hands_a_timed_out_pass_to_the_alert(monkeypatch, ho) -> None:
    _state, cycle_result, alerts, _report = _run_step(monkeypatch, ho, None, ok=False)
    assert cycle_result["oracle"] == {"status": "failed", "reason": "timeout_or_worker_failure"}
    assert alerts == [(cycle_result["oracle"], DECISION)]


def test_an_alert_that_raises_never_breaks_the_cycle(monkeypatch, ho) -> None:
    class SessionClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return DECISION.astimezone(tz)

    monkeypatch.setattr(ho, "datetime", SessionClock)
    monkeypatch.setattr(ho, "time", SimpleNamespace(monotonic=lambda: 1000.0))
    result = {"status": "all_abstained", "new_predictions": 0}
    monkeypatch.setattr(ho, "_run_with_timeout", lambda name, work, timeout, state: (result, True))

    def boom(*args: Any) -> None:
        raise RuntimeError("mail server down")

    monkeypatch.setattr(ho, "_oracle_forward_alert", boom)
    state = SimpleNamespace(last_oracle_cycle=None, current_step=None, cooldowns=MagicMock())
    state.cooldowns.can_retry.return_value = True
    cycle_result: dict[str, Any] = {}
    ho._run_oracle_forward_step(MagicMock(), state, False, cycle_result, 1000.0 + 4500)
    assert cycle_result["oracle"] == result


# ── Served and swept ──────────────────────────────────────────────────


def test_the_route_passes_the_viewer_flag_and_the_admin_dependency(monkeypatch) -> None:
    import inspect

    from api.auth import require_auth, viewer_is_admin
    from api.routers import oracle as route

    signature = inspect.signature(route.get_forward_health)
    assert signature.parameters["_token"].default.dependency is require_auth
    assert signature.parameters["include_schwab"].default.dependency is viewer_is_admin
    seen: list[Any] = []
    monkeypatch.setattr(route, "get_db_engine", lambda: MagicMock())
    monkeypatch.setattr(route, "_forward_health_cache", route.TTLCache(ttl=60, max_size=2))
    monkeypatch.setattr(route, "forward_health",
                        lambda conn, *, now, include_schwab: seen.append(include_schwab)
                        or {"status": "ok", "cohort": "all" if include_schwab is True else "public"})
    assert route.get_forward_health(_token="t", include_schwab=False)["cohort"] == "public"
    assert route.get_forward_health(_token="t", include_schwab=True)["cohort"] == "all"
    # A minute's cache, one entry per cohort: an admin answer never reaches anyone else.
    assert route.get_forward_health(_token="t", include_schwab=False)["cohort"] == "public"
    assert route.get_forward_health(_token="t", include_schwab=True)["cohort"] == "all"
    assert seen == [False, True]
    assert any(r.path == "/api/v1/oracle/forward-health" for r in route.router.routes)


def test_the_check_script_exits_one_for_a_failing_session(monkeypatch, capsys, db) -> None:
    import db as db_module
    from scripts import oracle_forward_health_check as check

    with db.begin() as conn:
        _incident(conn)
    monkeypatch.setattr(db_module, "get_engine", lambda: db)
    real = fh.forward_health
    monkeypatch.setattr(check, "forward_health",
                        lambda conn, **kw: real(conn, **{**kw, "now": _utc(date(2026, 11, 27), 5)}))
    assert check.main(["--ended"]) == 1
    out = capsys.readouterr().out
    assert "all 610 candidates abstained" in out and "private-use (Schwab) batch" in out


def test_the_check_script_exits_zero_for_a_session_with_forecasts(monkeypatch, capsys, db) -> None:
    import db as db_module
    from scripts import oracle_forward_health_check as check

    with db.begin() as conn:
        _attempts(conn, WED, 2)
        _rows(conn, "SPY", DECISION, 40, direction="PUT")
        _rows(conn, "QQQ", DECISION, 111, direction="CALL")
    monkeypatch.setattr(db_module, "get_engine", lambda: db)
    real = fh.forward_health
    monkeypatch.setattr(check, "forward_health",
                        lambda conn, **kw: real(conn, **{**kw, "now": _utc(date(2026, 11, 27), 5)}))
    assert check.main(["--ended", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == fh.STATUS_OK


def test_the_daily_sweep_runs_the_check_on_the_last_ended_session() -> None:
    sweep = (ROOT / "server_setup" / "grid-data-freshness-check.sh").read_text()
    assert "scripts/oracle_forward_health_check.py --ended --alert" in sweep
    assert 'oracle-forward-${TODAY}.txt' in sweep
