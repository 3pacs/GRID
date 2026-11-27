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

from oracle import forward_alert as fa
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
    assert outcome["all_abstained"] is False and outcome["no_result"] is False
    assert outcome["tickers_without_candidates"] == []


def test_a_pass_with_no_forecast_and_no_candidate_is_no_result() -> None:
    # Both tickers captured, quotes qualified, and no model produced a candidate
    # (or none is admitted): the engine stored nothing and reports empty detail.
    outcome = fh.pass_outcome(["SPY", "QQQ"], {"tickers": {}, "abstention_reasons": {}})
    assert outcome["no_result"] is True and outcome["all_abstained"] is False
    assert outcome["tickers_without_candidates"] == ["SPY", "QQQ"]
    assert outcome["tickers_with_forecast"] == [] and outcome["tickers_abstained"] == []


def test_repeated_forecasts_are_the_control_for_no_result() -> None:
    # Also zero new rows, and the opposite finding: the tickers hold this morning's forecast.
    outcome = fh.pass_outcome(["SPY", "QQQ"], _detail(
        SPY={"duplicate_forecasts": 40}, QQQ={"duplicate_forecasts": 111}))
    assert outcome["no_result"] is False and outcome["all_abstained"] is False
    one = fh.pass_outcome(["SPY", "QQQ"], _detail(QQQ={"duplicate_forecasts": 111}))
    assert one["no_result"] is False and one["tickers_without_candidates"] == ["SPY"]


def test_an_abstention_beside_a_ticker_without_candidates_is_all_abstained() -> None:
    outcome = fh.pass_outcome(["SPY", "QQQ"], _detail(SPY={"abstained": 305}))
    assert outcome["all_abstained"] is True and outcome["no_result"] is False


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
    assert note.startswith("a private-use capture of SPY was the newest one registered by that time"
                           " (completed 2026-11-25T15:43:17+00:00).")
    # Conditional: the table shows the capture, not the state of the switch or the cause.
    assert "If the Oracle's private-use switch (" + op.ORACLE_SCHWAB_ENV + "=" + op.ORACLE_SCHWAB_ON + ") was off" in note
    assert note.endswith("This shows what was in the table, not why the pass abstained.")
    assert "100.0" not in note                       # never a price


@pytest.mark.parametrize("flag", [False, None, 1, "true"])
def test_everyone_else_gets_the_status_without_the_private_capture_note(db, flag: Any) -> None:
    with db.begin() as conn:
        _incident(conn)
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=flag)
    assert health["status"] == fh.STATUS_ALL_ABSTAINED      # these abstentions are public rows
    assert health["cohort"] == "public"
    for admin_only in ("operator_notes", "attempts", "last_attempt_at", "alert_sent"):
        assert admin_only not in health
    assert "Schwab" not in json.dumps(health) and "private" not in json.dumps(health)
    assert "attempt" not in health["message"]
    assert health["message"].startswith(
        "No usable Oracle forecast in the 2026-11-25 session: all 610 candidates abstained. Top reason: ")


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


GENERIC_SUBJECT = "Oracle forward pass needs attention (2026-11-25)"
GENERIC_BODY = (
    "The Oracle forward pass for the 2026-11-25 session needs attention.\n\n"
    "This message carries no detail on purpose. An authenticated admin can read what happened on the"
    " Oracle page or at GET /api/v1/oracle/forward-health; on the host,"
    " scripts/oracle_forward_health_check.py prints it.\n\n"
    "One alert per session is intended. A second copy is possible if the mail was accepted and the"
    " record of sending it could not then be saved."
)


def test_the_e_mail_is_generic_and_built_from_an_allowlist_of_one_field(db) -> None:
    """Counts, tickers, reasons, status and notes come from tables that include the private cohort."""
    with db.begin() as conn:
        _incident(conn)
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=True)
    assert fa.OUTBOUND_ALERT_FIELDS == ("session_date",)
    assert fa.outbound_alert(health) == (GENERIC_SUBJECT, GENERIC_BODY)
    # Nothing else of the admin answer can reach it: the same text for any other content.
    other = {**health, "status": "ok", "message": "x", "tickers": {}, "attempts": 99, "operator_notes": {"SPY": "y"}}
    assert fa.outbound_alert(other) == (GENERIC_SUBJECT, GENERIC_BODY)
    for leaked in ("610", "305", "SPY", "QQQ", OLD_REASON, "abstain", "attempt(s)", health["status"],
                   "private-use", "Schwab", "15:43"):
        assert leaked not in GENERIC_SUBJECT + GENERIC_BODY


def test_the_host_local_report_carries_the_counts_reasons_and_optionally_the_notes(db) -> None:
    with db.begin() as conn:
        _incident(conn)
    with db.connect() as conn:
        health = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=True)
    report, with_notes = fh.local_report(health), fh.local_report(health, with_notes=True)
    assert "SPY: 0 forecast(s), 305 abstention(s); " + OLD_REASON in report
    assert "Nothing was repaired or regenerated." in report
    # The alert file is written from this form: no capture metadata in it.
    assert "private-use" not in report and "15:43:17" not in report and "Schwab" not in report
    assert "completed 2026-11-25T15:43:17+00:00" in with_notes


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
    assert "(completed 2026-11-25T15:43:17+00:00)" in admin["operator_notes"]["SPY"]
    # The private-use forecast of the day before is the admin's last real forecast, never the public one.
    assert admin["last_qualified_at"] == "2026-11-24T15:00:00+00:00"
    assert public["last_qualified_at"] == "2026-11-19T15:42:44+00:00"
    assert "operator_notes" not in public


def test_the_marker_and_lock_work_on_postgresql_and_two_callers_send_once(pgdb) -> None:
    with pgdb.begin() as conn:
        _incident(conn)
    health = _incident_health(pgdb)
    calls: list[str] = []
    send = lambda subject, body, severity: bool(calls.append(subject)) or True  # noqa: E731
    assert fa.send_session_alert(pgdb, health, send=send) == fa.SENT
    assert fa.send_session_alert(pgdb, health, send=send) == fa.ALREADY_SENT
    assert calls == [GENERIC_SUBJECT]
    with pgdb.connect() as conn:
        rows = conn.execute(text("SELECT notes, models_updated FROM oracle_iterations WHERE notes LIKE :p"),
                            {"p": fh.FORWARD_ALERT_NOTE_PREFIX + "%"}).fetchall()
    assert [tuple(r) for r in rows] == [(fh.alert_note("2026-11-25"), 0)]
    assert _incident_health(pgdb)["attempts"] == 6                 # a marker is not an attempt


# ── What a non-admin answer may depend on ─────────────────────────────


def _public(engine: Any, now: datetime) -> dict[str, Any]:
    with engine.connect() as conn:
        health = fh.forward_health(conn, now=now, include_schwab=False)
    health.pop("checked_at")
    return health


@pytest.mark.parametrize("now", [_utc(WED, 15), _utc(WED, 17), _utc(WED, 22)])
def test_the_public_answer_is_the_same_whatever_happened_in_the_private_cohort(db, now: datetime) -> None:
    """Attempts, alert markers and private-use rows live in unfiltered tables. None may show through."""
    quiet = _public(db, now)
    with db.begin() as conn:
        _attempts(conn, WED, 6)
        _rows(conn, "SPY", _utc(WED, 14, 31, 6), 40, direction="PUT", private=True)
        _rows(conn, "QQQ", _utc(WED, 14, 31, 6), 305, private=True)
        _batch(conn, "SPY", _utc(WED, 14, 31), "schwab", 20)
        conn.execute(text("INSERT INTO oracle_iterations VALUES (:at, 0, 0, :note)"),
                     {"at": _utc(WED, 14, 32).isoformat(sep=" "), "note": fh.alert_note(WED)})
    busy = _public(db, now)
    assert busy == quiet
    for admin_only in ("attempts", "last_attempt_at", "alert_sent", "operator_notes"):
        assert admin_only not in busy
    assert busy["status"] in {fh.STATUS_PENDING, fh.STATUS_NO_PUBLIC_FORECAST}
    assert "attempt" not in busy["message"]


def test_the_public_answer_still_reports_public_rows(db) -> None:
    with db.begin() as conn:
        _attempts(conn, WED, 2)
        _rows(conn, "QQQ", DECISION, 111, direction="CALL")
        _rows(conn, "SPY", DECISION, 40, direction="PUT", private=True)
    public = _public(db, _utc(WED, 17))
    assert public["status"] == fh.STATUS_PARTIAL
    assert (public["tickers"]["QQQ"]["qualified"], public["tickers"]["SPY"]["qualified"]) == (111, 0)


# ── Nothing later than the supplied instant is read ───────────────────


@pytest.mark.parametrize("admin", [True, False])
def test_a_midday_replay_does_not_see_the_afternoon(db, admin: bool) -> None:
    """Asked about 15:00Z, the answer must not contain a 15:43Z row or a 16:31Z attempt of the same day."""
    with db.begin() as conn:
        _attempts(conn, WED, 3)                                # 14:31, 15:31, 16:31
        for ticker in ("SPY", "QQQ"):
            _rows(conn, ticker, _utc(WED, 14, 31, 6), 305, reason=SESSION_REASON)
            _rows(conn, ticker, DECISION, 80, direction="CALL")          # 15:43:32
    with db.connect() as conn:
        at_noon = fh.forward_health(conn, now=_utc(WED, 15), include_schwab=admin)
        at_close = fh.forward_health(conn, now=_utc(WED, 21), include_schwab=admin)
        on_the_row = fh.forward_health(conn, now=DECISION, include_schwab=admin)
    assert at_noon["status"] == fh.STATUS_ALL_ABSTAINED
    assert at_noon["tickers"]["SPY"] == {
        "qualified": 0, "abstained": 305, "top_reason": SESSION_REASON,
        "first_forecast_at": None, "first_abstention_at": _utc(WED, 14, 31, 6).isoformat(),
    }
    assert at_noon["last_qualified_at"] is None
    assert at_close["status"] == fh.STATUS_OK and at_close["last_qualified_at"] == DECISION.isoformat()
    assert on_the_row["status"] == fh.STATUS_OK                # a row at the instant itself is known
    if admin:
        assert (at_noon["attempts"], at_noon["last_attempt_at"]) == (1, _utc(WED, 14, 31).isoformat())
        assert at_close["attempts"] == 3
    else:
        assert "attempts" not in at_noon and "attempts" not in at_close


def test_an_alert_sent_later_is_not_known_to_an_earlier_instant(db) -> None:
    with db.begin() as conn:
        _incident(conn)
        conn.execute(text("INSERT INTO oracle_iterations VALUES (:at, 0, 0, :note)"),
                     {"at": _utc(WED, 15, 44).isoformat(sep=" "), "note": fh.alert_note(WED)})
    assert _incident_health(db, _utc(WED, 15, 43, 40))["alert_sent"] is False
    assert _incident_health(db, _utc(WED, 15, 45))["alert_sent"] is True


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
        assert "(completed 2026-11-25T15:43:17+00:00)" in op.withheld_capture_note(conn, "SPY", as_of=DECISION)


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


def test_a_pass_with_no_forecast_and_no_candidate_reports_no_result_not_completed(monkeypatch, ho) -> None:
    _stub_pass(monkeypatch, ho, detail={"tickers": {}, "abstention_reasons": {}},
               counts={"qualified": 0, "inserted": 0})
    result = ho._run_oracle_forward_pass(_PassEngine(), should_continue=lambda: True)
    assert result["status"] == "no_result"
    assert result["tickers_without_candidates"] == ["SPY", "QQQ"]


def test_a_partial_capture_without_a_forecast_is_not_called_partial(monkeypatch, ho) -> None:
    # "partial" would reset nothing and alert nothing; it now means a ticker holds a forecast.
    _stub_pass(monkeypatch, ho, detail={"tickers": {}, "abstention_reasons": {}},
               counts={"qualified": 0, "failed_tickers": 1})
    assert ho._run_oracle_forward_pass(_PassEngine(), should_continue=lambda: True)["status"] == "no_result"
    _stub_pass(monkeypatch, ho, detail=_detail(QQQ={"qualified": 111}),
               counts={"qualified": 111, "failed_tickers": 1})
    assert ho._run_oracle_forward_pass(_PassEngine(), should_continue=lambda: True)["status"] == "partial"


def test_a_pass_that_repeats_the_mornings_forecasts_still_reports_completed(monkeypatch, ho) -> None:
    _stub_pass(monkeypatch, ho,
               detail=_detail(SPY={"duplicate_forecasts": 40}, QQQ={"duplicate_forecasts": 111}),
               counts={"qualified": 0, "inserted": 0, "duplicates": 151})
    result = ho._run_oracle_forward_pass(_PassEngine(), should_continue=lambda: True)
    assert result["status"] == "completed"
    assert result["tickers_with_forecast"] == ["SPY", "QQQ"]


def _mail(monkeypatch, sends: bool = True) -> list[tuple[str, str, str]]:
    sent: list[tuple[str, str, str]] = []
    email = types.ModuleType("alerts.email")

    def send_alert(subject: str, body: str, severity: str = "info") -> bool:
        sent.append((subject, body, severity))
        return sends

    email.send_alert = send_alert                               # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "alerts.email", email)
    return sent


def _clock(monkeypatch, ho, now: datetime) -> None:
    class SessionClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz)

    monkeypatch.setattr(ho, "datetime", SessionClock)


def _markers(db) -> list[str]:
    with db.connect() as conn:
        return [r[0] for r in conn.execute(text(
            "SELECT notes FROM oracle_iterations WHERE notes LIKE :p ORDER BY notes"),
            {"p": fh.FORWARD_ALERT_NOTE_PREFIX + "%"})]


def test_all_abstained_alerts_on_the_first_pass_and_once_per_session(monkeypatch, ho, db) -> None:
    with db.begin() as conn:
        _incident(conn)
    sent = _mail(monkeypatch)
    _clock(monkeypatch, ho, DECISION + timedelta(seconds=5))
    result = {"status": "all_abstained"}
    assert ho._oracle_forward_alert(db, SimpleNamespace(), result, DECISION) is True
    assert ho._oracle_forward_alert(db, SimpleNamespace(), result, DECISION) is False
    assert len(sent) == 1
    # Exactly the generic text is handed to the mailer: no counts, reasons, tickers or pass summary.
    assert sent == [(GENERIC_SUBJECT, GENERIC_BODY, "critical")]
    assert _markers(db) == [fh.alert_note("2026-11-25")]


def test_a_restarted_operator_does_not_send_the_same_session_again(monkeypatch, ho, db) -> None:
    with db.begin() as conn:
        _incident(conn)
    sent = _mail(monkeypatch)
    _clock(monkeypatch, ho, DECISION + timedelta(seconds=5))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, DECISION) is True
    # A restart: nothing in memory survives. The marker row does.
    fresh_state = SimpleNamespace()
    _clock(monkeypatch, ho, DECISION + timedelta(hours=1))
    assert ho._oracle_forward_alert(db, fresh_state, {"status": "all_abstained"}, DECISION) is False
    assert len(sent) == 1 and vars(fresh_state) == {}


def test_the_next_session_alerts_again(monkeypatch, ho, db) -> None:
    friday = date(2026, 11, 27)
    with db.begin() as conn:
        _incident(conn)
        _attempts(conn, friday, 1)
        _rows(conn, "SPY", _utc(friday, 14, 31, 6), 305, reason=SESSION_REASON)
    sent = _mail(monkeypatch)
    _clock(monkeypatch, ho, DECISION + timedelta(seconds=5))
    ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, DECISION)
    _clock(monkeypatch, ho, _utc(friday, 14, 32))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, DECISION) is True
    assert [subject for subject, _b, _s in sent] == [
        GENERIC_SUBJECT, "Oracle forward pass needs attention (2026-11-27)"]
    assert _markers(db) == [fh.alert_note("2026-11-25"), fh.alert_note("2026-11-27")]


@pytest.mark.parametrize("status", ["no_result", "abstained", "failed"])
def test_two_passes_that_stored_nothing_alert_and_one_does_not(monkeypatch, ho, db, status: str) -> None:
    sent = _mail(monkeypatch)
    result = {"status": status, "reason": "no_fresh_cohort_capture" if status == "abstained" else None}
    with db.begin() as conn:
        _attempts(conn, WED, 1)
    _clock(monkeypatch, ho, _utc(WED, 14, 40))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), result, DECISION) is False
    assert sent == []
    with db.begin() as conn:                                  # the second pass, an hour later
        _attempts(conn, WED, 1, first_hour=15)
    _clock(monkeypatch, ho, _utc(WED, 15, 40))
    # A new state object each time: the count comes from the stored attempts.
    assert ho._oracle_forward_alert(db, SimpleNamespace(), result, DECISION) is True
    assert sent == [(GENERIC_SUBJECT, GENERIC_BODY, "critical")]


def test_a_pass_that_left_a_forecast_never_alerts_and_reads_nothing(monkeypatch, ho) -> None:
    sent = _mail(monkeypatch)
    engine = MagicMock()
    for result in ({"status": "completed"}, {"status": "partial"}, None, {},
                   {"status": "deferred", "reason": "hourly_cadence"}):
        assert ho._oracle_forward_alert(engine, SimpleNamespace(), result, DECISION) is False
    engine.connect.assert_not_called()
    assert sent == []


def test_no_alert_when_the_record_shows_a_forecast_for_the_session(monkeypatch, ho, db) -> None:
    with db.begin() as conn:
        _attempts(conn, WED, 2)
        _rows(conn, "QQQ", _utc(WED, 14, 31, 6), 111, direction="CALL")
    sent = _mail(monkeypatch)
    _clock(monkeypatch, ho, DECISION)
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, DECISION) is False
    assert sent == [] and _markers(db) == []


def test_an_alert_that_could_not_be_sent_leaves_no_marker_and_is_tried_again(monkeypatch, ho, db) -> None:
    with db.begin() as conn:
        _incident(conn)
    _clock(monkeypatch, ho, DECISION + timedelta(seconds=5))
    failed = _mail(monkeypatch, sends=False)
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, DECISION) is False
    assert len(failed) == 1 and _markers(db) == []
    sent = _mail(monkeypatch)
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, DECISION) is True
    assert len(sent) == 1 and _markers(db) == [fh.alert_note("2026-11-25")]


# ── When the alert is due, and the shared marker ──────────────────────


@pytest.mark.parametrize(("health", "due"), [
    ({"status": fh.STATUS_ALL_ABSTAINED, "attempts": 1}, True),
    ({"status": fh.STATUS_NO_ROWS, "attempts": 1, "first_attempt_at": "2026-11-25T14:31:00+00:00",
      "checked_at": "2026-11-25T16:00:00+00:00"}, False),
    ({"status": fh.STATUS_NO_ROWS, "attempts": 2, "first_attempt_at": "2026-11-25T14:31:00+00:00",
      "checked_at": "2026-11-25T15:31:00+00:00"}, True),
    # Two attempts ten minutes apart are a slow morning, not yet a dead session.
    ({"status": fh.STATUS_NO_ROWS, "attempts": 2, "first_attempt_at": "2026-11-25T14:31:00+00:00",
      "checked_at": "2026-11-25T14:41:30+00:00"}, False),
    ({"status": fh.STATUS_NO_ROWS, "attempts": 2}, False),      # no clock on record: not due
    # Ended: the grace is over, even for a single attempt.
    ({"status": fh.STATUS_NO_ROWS, "attempts": 1, "session_state": "ended"}, True),
    ({"status": fh.STATUS_NO_ROWS, "attempts": 1, "session_state": "open",
      "first_attempt_at": "2026-11-25T14:31:00+00:00", "checked_at": "2026-11-25T20:50:00+00:00"}, False),
    ({"status": fh.STATUS_NO_ATTEMPTS, "attempts": 0, "session_state": "open"}, False),
    ({"status": fh.STATUS_NO_ATTEMPTS, "attempts": 0, "session_state": "ended"}, True),
    ({"status": fh.STATUS_OK, "attempts": 6}, False),
    ({"status": fh.STATUS_PARTIAL, "attempts": 6, "session_state": "open"}, False),   # a later pass may fix it
    ({"status": fh.STATUS_PARTIAL, "attempts": 6, "session_state": "ended"}, True),
    ({"status": fh.STATUS_PENDING, "attempts": 0}, False),
    ({"status": fh.STATUS_NO_PUBLIC_FORECAST}, False),
])
def test_when_the_alert_is_due(health: dict[str, Any], due: bool) -> None:
    assert fa.alert_due(health)[0] is due


def _incident_health(engine: Any, now: datetime | None = None) -> dict[str, Any]:
    with engine.connect() as conn:
        return fh.forward_health(conn, now=now or _utc(WED, 21), include_schwab=True)


def test_the_marker_is_written_only_after_the_mail_was_accepted(db) -> None:
    with db.begin() as conn:
        _incident(conn)
    health = _incident_health(db)
    assert health["alert_sent"] is False
    assert fa.send_session_alert(db, health, send=lambda *a: False) == fa.SEND_FAILED
    assert _markers(db) == []
    calls: list[tuple[str, str, str]] = []
    assert fa.send_session_alert(db, health, send=lambda *a: bool(calls.append(a)) or True) == fa.SENT
    assert fa.send_session_alert(db, health, send=lambda *a: bool(calls.append(a)) or True) == fa.ALREADY_SENT
    assert calls == [(GENERIC_SUBJECT, GENERIC_BODY, "critical")]
    # The session's attempt count is not changed by the marker row, and an admin answer shows it was sent.
    after = _incident_health(db, datetime.now(timezone.utc) + timedelta(minutes=1))
    assert after["alert_sent"] is True
    assert _incident_health(db)["attempts"] == 6


def test_hermes_then_the_sweep_send_one_alert_for_the_session(monkeypatch, capsys, ho, db, tmp_path) -> None:
    import db as db_module
    from scripts import oracle_forward_health_check as check

    with db.begin() as conn:
        _incident(conn)
    sent = _mail(monkeypatch)
    _clock(monkeypatch, ho, DECISION + timedelta(seconds=5))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, DECISION) is True
    # 05:00Z on Thursday (a holiday) and on Friday before the open: each sweep judges Wednesday's session.
    monkeypatch.setattr(db_module, "get_engine", lambda: db)
    real = fh.forward_health
    for day in (26, 27):
        at = _utc(date(2026, 11, day), 5)
        monkeypatch.setattr(check, "forward_health", lambda conn, _at=at, **kw: real(conn, **{**kw, "now": _at}))
        assert check.main(["--ended", "--alert", "--write-alert", str(tmp_path)]) == 1
        assert "alert: already_sent" in capsys.readouterr().out
    assert len(sent) == 1
    assert [p.name for p in tmp_path.iterdir()] == ["oracle-forward-2026-11-25.txt"]   # one file per session


def test_the_sweep_then_hermes_send_one_alert_for_the_session(monkeypatch, capsys, ho, db, tmp_path) -> None:
    import db as db_module
    from scripts import oracle_forward_health_check as check

    with db.begin() as conn:
        _incident(conn)
    sent = _mail(monkeypatch)
    monkeypatch.setattr(db_module, "get_engine", lambda: db)
    real = fh.forward_health
    at = _utc(date(2026, 11, 26), 5)
    monkeypatch.setattr(check, "forward_health", lambda conn, **kw: real(conn, **{**kw, "now": at}))
    assert check.main(["--ended", "--alert"]) == 1
    assert "alert: sent" in capsys.readouterr().out
    assert check.main(["--ended", "--alert"]) == 1                  # the next day's sweep, same session
    assert "alert: already_sent" in capsys.readouterr().out
    _clock(monkeypatch, ho, _utc(WED, 20))                           # an operator replaying the same session
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, DECISION) is False
    assert sent == [(GENERIC_SUBJECT, GENERIC_BODY, "critical")]      # the sweep's path hands over the same text
    assert list(tmp_path.iterdir()) == []                            # no --write-alert, no file


def test_the_sweep_alerts_once_for_a_session_that_left_a_ticker_without_a_forecast(
        monkeypatch, capsys, ho, db, tmp_path) -> None:
    """SPY: captured every pass, no model produced a candidate, so no row of any kind was written."""
    import db as db_module
    from scripts import oracle_forward_health_check as check

    with db.begin() as conn:
        _attempts(conn, WED, 6)
        _rows(conn, "QQQ", DECISION, 111, direction="CALL")
    sent = _mail(monkeypatch)
    # During the session Hermes says nothing about it: a later pass may still give SPY a forecast.
    _clock(monkeypatch, ho, _utc(WED, 16))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "completed"}, DECISION) is False
    assert sent == []
    monkeypatch.setattr(db_module, "get_engine", lambda: db)
    real = fh.forward_health
    at = _utc(date(2026, 11, 26), 5)
    monkeypatch.setattr(check, "forward_health", lambda conn, **kw: real(conn, **{**kw, "now": at}))
    assert check.main(["--ended", "--alert", "--write-alert", str(tmp_path)]) == 1
    assert check.main(["--ended", "--alert", "--write-alert", str(tmp_path)]) == 1
    out = capsys.readouterr().out
    assert "alert: sent" in out and "alert: already_sent" in out
    assert len(sent) == 1
    assert sent == [(GENERIC_SUBJECT, GENERIC_BODY, "critical")]      # not even the ticker leaves the host
    assert [p.name for p in tmp_path.iterdir()] == ["oracle-forward-2026-11-25.txt"]
    # The detail is in the host-local file.
    local = (tmp_path / "oracle-forward-2026-11-25.txt").read_text()
    assert "no forecast for SPY" in local and "QQQ: 111 forecast(s)" in local


def test_the_sweep_alerts_for_a_session_in_which_no_pass_ran(monkeypatch, capsys, db) -> None:
    import db as db_module
    from scripts import oracle_forward_health_check as check

    sent = _mail(monkeypatch)
    monkeypatch.setattr(db_module, "get_engine", lambda: db)
    real = fh.forward_health
    at = _utc(date(2026, 11, 26), 5)
    monkeypatch.setattr(check, "forward_health", lambda conn, **kw: real(conn, **{**kw, "now": at}))
    assert check.main(["--ended", "--alert"]) == 1
    assert sent == [(GENERIC_SUBJECT, GENERIC_BODY, "critical")]


def test_the_sweep_alerts_for_an_ended_session_with_one_attempt_and_no_row(monkeypatch, capsys, ho, db, tmp_path) -> None:
    """One pass ran, stored nothing, and the session ended. While open that is within the grace;
    ended, it is a session without a forecast, and only the sweep can say so."""
    import db as db_module
    from scripts import oracle_forward_health_check as check

    with db.begin() as conn:
        _attempts(conn, WED, 1)
    sent = _mail(monkeypatch)
    # During the session one attempt with nothing stored is not yet an alert, however late it gets.
    _clock(monkeypatch, ho, _utc(WED, 20, 50))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "no_result"}, DECISION) is False
    assert sent == []
    monkeypatch.setattr(db_module, "get_engine", lambda: db)
    real = fh.forward_health
    outputs = []
    for day in (26, 27):          # Thursday (holiday) and Friday before the open: the same ended session
        at = _utc(date(2026, 11, day), 5)
        monkeypatch.setattr(check, "forward_health", lambda conn, _at=at, **kw: real(conn, **{**kw, "now": _at}))
        assert check.main(["--ended", "--alert", "--write-alert", str(tmp_path)]) == 1
        outputs.append(capsys.readouterr().out)
    assert "alert: sent" in outputs[0] and "alert: already_sent" in outputs[1]
    assert sent == [(GENERIC_SUBJECT, GENERIC_BODY, "critical")]
    assert "1 attempt(s) ran and stored nothing" in (tmp_path / "oracle-forward-2026-11-25.txt").read_text()


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


def test_the_step_treats_no_result_as_a_pass_that_ran_to_the_end(monkeypatch, ho) -> None:
    result = {"status": "no_result", "new_predictions": 0}
    state, cycle_result, alerts, report = _run_step(monkeypatch, ho, result)
    assert alerts == [(result, DECISION)]
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
    assert "all 610 candidates abstained" in out
    assert "private-use" not in out and "alert:" not in out      # no --notes, no --alert
    assert check.main(["--ended", "--notes"]) == 1
    assert "a private-use capture of SPY was the newest one registered by that time" in capsys.readouterr().out
    assert check.main(["--ended", "--json"]) == 1
    assert "operator_notes" not in json.loads(capsys.readouterr().out)


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
    assert 'scripts/oracle_forward_health_check.py --ended --alert --write-alert "$ALERT_DIR"' in sweep
    assert "${TODAY}.txt" not in sweep.split("Oracle forward pass")[1]      # named for the session, not today
