"""A later qualified quote in the same session makes the forecast (oracle/forward_health.retry_plan).

Before this, a forward pass with no qualified quote wrote its abstentions and
the next chance was an hour away, whatever happened in between. Now the next
pass may start after ten minutes while the session still waits for a quote.

What must stay true, and is pinned here:

* every pass is a new decision: a fresh capture, its own decision clock, its
  own frozen quote. Nothing of an earlier pass is completed or backdated;
* the entry-price policy is untouched (no test here changes its limits);
* the short interval is bounded: an attempt cap, a cut-off before the close,
  no quick retry when the newest capture is a private-use one, and none when
  the last pass found qualified quotes and no model produced a candidate.

The models' options-signal input is not changed here. A retried forecast can
therefore still pair a fresh quote with an options row written earlier, as
an hourly pass can today; that input gets its own decision-time reader in a
separate change, which must land before this one.

Offline except the two ``pgdb`` tests, which need a disposable PostgreSQL.
"""

from __future__ import annotations

import json
import sys
import types
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

from oracle import engine as oe
from oracle import forward_health as fh

WED = date(2026, 11, 25)
OPEN = datetime(2026, 11, 25, 14, 30, tzinfo=timezone.utc)        # 09:30 New York
CLOSE = datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc)        # 16:00 New York
REASON = "quote is older than the 20-minute reference limit"


def _health(*, status: str = fh.STATUS_ALL_ABSTAINED, attempts: int = 1, state: str = "open",
            spy: tuple[int, int] = (0, 305), qqq: tuple[int, int] = (0, 305),
            last_outcome: str | None = None) -> dict[str, Any]:
    return {
        "abstained_without_quote": {name: n for name, n in (("SPY", spy[1]), ("QQQ", qqq[1])) if n},
        "status": status, "attempts": attempts, "session_state": state, "last_outcome": last_outcome,
        "session_closes_at": CLOSE.isoformat(),
        "tickers": {"SPY": {"qualified": spy[0], "abstained": spy[1]},
                    "QQQ": {"qualified": qqq[0], "abstained": qqq[1]}},
    }


# ── The plan ──────────────────────────────────────────────────────────


def test_a_session_waiting_for_a_quote_retries_in_ten_minutes() -> None:
    plan = fh.retry_plan(_health(), OPEN + timedelta(minutes=2))
    assert plan["quick"] is True
    assert plan["interval_seconds"] == 600
    assert plan["reason"] == "waiting_for_a_qualified_quote"
    assert plan["waiting_tickers"] == ["SPY", "QQQ"]
    assert plan["retry_cutoff_at"] == "2026-11-25T20:30:00+00:00"


def test_a_pass_with_no_capture_retries_and_one_with_no_candidate_does_not() -> None:
    """Both store nothing. A fresh capture can fix the first and cannot fix the second."""
    no_capture = _health(status=fh.STATUS_NO_ROWS, spy=(0, 0), qqq=(0, 0), last_outcome="no_capture")
    plan = fh.retry_plan(no_capture, OPEN + timedelta(minutes=2))
    assert (plan["quick"], plan["reason"], plan["waiting_tickers"]) == (True, "waiting_for_a_fresh_capture", [])
    for outcome in ("no_result", "completed", None):       # None: the last pass never finished
        health = _health(status=fh.STATUS_NO_ROWS, spy=(0, 0), qqq=(0, 0), last_outcome=outcome)
        assert fh.retry_plan(health, OPEN + timedelta(minutes=2))["quick"] is False


@pytest.mark.parametrize("health", [
    _health(status=fh.STATUS_OK, spy=(40, 0), qqq=(111, 0)),
    # An abstention at 14:31 and a forecast at 14:41: the session has what it waited for.
    _health(status=fh.STATUS_OK, spy=(40, 305), qqq=(111, 305)),
    # SPY has no row while QQQ has its forecast: SPY's quote qualified, no model voted.
    _health(status=fh.STATUS_PARTIAL, spy=(0, 0), qqq=(111, 0)),
    _health(status=fh.STATUS_PENDING, attempts=0, spy=(0, 0), qqq=(0, 0)),
    _health(status=fh.STATUS_NO_ATTEMPTS, attempts=0, spy=(0, 0), qqq=(0, 0)),
])
def test_a_session_that_is_not_waiting_for_a_quote_stays_hourly(health: dict[str, Any]) -> None:
    plan = fh.retry_plan(health, OPEN + timedelta(minutes=12))
    assert (plan["quick"], plan["interval_seconds"], plan["reason"]) == (
        False, 3600, "not_waiting")


def test_one_ticker_still_abstaining_keeps_the_retry_going() -> None:
    plan = fh.retry_plan(_health(status=fh.STATUS_PARTIAL, qqq=(111, 0)), OPEN + timedelta(minutes=12))
    assert plan["quick"] is True and plan["waiting_tickers"] == ["SPY"]


def test_the_short_interval_is_capped_per_session() -> None:
    last_quick = fh.retry_plan(_health(attempts=fh.MAX_QUICK_ATTEMPTS - 1), OPEN + timedelta(hours=2))
    capped = fh.retry_plan(_health(attempts=fh.MAX_QUICK_ATTEMPTS), OPEN + timedelta(hours=2))
    assert last_quick["quick"] is True
    assert (capped["quick"], capped["interval_seconds"], capped["reason"]) == (
        False, 3600, "attempt_cap_reached")


def test_no_quick_retry_in_the_last_half_hour_or_after_the_close() -> None:
    before = fh.retry_plan(_health(), CLOSE - timedelta(minutes=30, seconds=1))
    at_cutoff = fh.retry_plan(_health(), CLOSE - timedelta(minutes=30))
    ended = fh.retry_plan(_health(state="ended"), CLOSE + timedelta(hours=1))
    assert before["quick"] is True
    assert (at_cutoff["quick"], at_cutoff["reason"]) == (False, "past_retry_cutoff")
    assert (ended["quick"], ended["reason"]) == (False, "session_ended")


def test_the_cutoff_follows_an_early_close() -> None:
    # 2026-11-27 closes at 13:00 New York (18:00Z): the cut-off is 17:30Z.
    session = fh.forward_session(datetime(2026, 11, 27, 15, tzinfo=timezone.utc))
    health = _health() | {"session_closes_at": session["closes_at"]}
    assert fh.retry_plan(health, datetime(2026, 11, 27, 17, 29, tzinfo=timezone.utc))["quick"] is True
    late = fh.retry_plan(health, datetime(2026, 11, 27, 17, 30, tzinfo=timezone.utc))
    assert (late["quick"], late["retry_cutoff_at"]) == (False, "2026-11-27T17:30:00+00:00")


def test_no_quick_retry_when_the_newest_capture_is_a_private_use_one() -> None:
    # 2026-11-23..25: a fresh capture every pass, each one withheld. Another would be too.
    blocked = fh.retry_plan(_health(), OPEN + timedelta(minutes=2), blocked={"SPY": "…", "QQQ": "…"})
    assert (blocked["quick"], blocked["interval_seconds"], blocked["reason"]) == (
        False, 3600, "newest_capture_is_private_use")
    # One ticker can still be helped by a fresh capture.
    assert fh.retry_plan(_health(), OPEN + timedelta(minutes=2), blocked={"SPY": "…"})["quick"] is True


def test_the_limits_are_the_documented_ones() -> None:
    assert fh.RETRY_INTERVAL == timedelta(minutes=10)
    assert fh.MAX_QUICK_ATTEMPTS == 13                  # the first pass and twelve retries
    assert fh.RETRY_CUTOFF_BEFORE_CLOSE == timedelta(minutes=30)
    from oracle import entry_price_policy as policy

    assert policy.QUOTE_MAX_AGE_SECONDS == 20 * 60      # the reference rule is not loosened


# ── The plan inside the session's health ──────────────────────────────

_SCHEMA = (
    "CREATE TABLE oracle_iterations (iteration_at TIMESTAMPTZ, models_updated INTEGER,"
    " predictions_scored INTEGER, notes TEXT)",
    "CREATE TABLE oracle_predictions (id TEXT PRIMARY KEY, created_at TIMESTAMPTZ, ticker TEXT,"
    " direction TEXT, verdict TEXT, score_notes TEXT, recording_policy TEXT, flow_context {json},"
    " dedup_keep BOOLEAN DEFAULT TRUE)",
    "CREATE TABLE options_capture_batches (capture_batch_id TEXT PRIMARY KEY, ticker TEXT,"
    " snap_date DATE, capture_ordinal BIGINT, capture_completed_at TIMESTAMPTZ,"
    " registered_at TIMESTAMPTZ, spot_price DOUBLE PRECISION, spot_provider TEXT,"
    " clock_metadata {json})",
)
_SEQ = iter(range(1, 10**9))


def _at(clock: datetime) -> str:
    return clock.isoformat(sep=" ")


def _attempt(conn: Any, clock: datetime) -> None:
    conn.execute(text("INSERT INTO oracle_iterations VALUES (:at, 0, 0, :note)"),
                 {"at": _at(clock), "note": fh.FORWARD_ATTEMPT_NOTE})


def _rows(conn: Any, ticker: str, clock: datetime, n: int, direction: str = "NONE",
          kind: str | None = "no_qualified_quote") -> None:
    """``kind`` is what the engine writes on an abstention for want of a qualified quote."""
    context = json.dumps({"abstention_kind": kind} if direction == "NONE" and kind else {})
    for _ in range(n):
        conn.execute(text(
            "INSERT INTO oracle_predictions (id, created_at, ticker, direction, verdict, score_notes,"
            " recording_policy, flow_context) VALUES (:id, :at, :t, :d, :v, :notes, :rp, :fc)"),
            {"id": f"of2:{next(_SEQ)}", "at": _at(clock), "t": ticker, "d": direction,
             "v": "no_data" if direction == "NONE" else "pending", "fc": context,
             "notes": REASON if direction == "NONE" else None, "rp": fh.FORWARD_RECORDING_POLICY})


def _batch(conn: Any, ticker: str, completed: datetime, provider: str, ordinal: int) -> None:
    conn.execute(text(
        "INSERT INTO options_capture_batches VALUES (:id, :t, :d, :o, :c, :c, 100.0, :p, :m)"),
        {"id": f"{ticker}-{ordinal}", "t": ticker, "d": completed.date().isoformat(), "o": ordinal,
         "c": _at(completed), "p": provider,
         "m": json.dumps({"provider": provider}) if provider == "schwab" else None})


@pytest.fixture
def db():
    import sqlite3

    sqlite3.register_converter("DATE", lambda raw: date.fromisoformat(raw.decode()))
    sqlite3.register_converter("TIMESTAMPTZ", lambda raw: datetime.fromisoformat(raw.decode()))
    engine = create_engine("sqlite://", connect_args={"detect_types": sqlite3.PARSE_DECLTYPES})
    with engine.begin() as conn:
        for ddl in _SCHEMA:
            conn.execute(text(ddl.format(json="TEXT")))
    yield engine
    engine.dispose()


@pytest.fixture
def pgdb(pg_engine):
    from uuid import uuid4

    from sqlalchemy.schema import CreateSchema, DropSchema

    if not (pg_engine.url.database or "").endswith("_test"):
        pytest.skip("needs a disposable _test database")
    schema = "forward_retry_" + uuid4().hex
    with pg_engine.begin() as conn:
        conn.execute(CreateSchema(schema))
    engine = create_engine(pg_engine.url, connect_args={"options": "-c search_path=" + schema})
    try:
        with engine.begin() as conn:
            for ddl in _SCHEMA:
                conn.execute(text(ddl.format(json="JSONB")))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(DropSchema(schema, cascade=True))


def _abstained_session(conn: Any, *, withheld: bool) -> None:
    """One pass at 14:31Z in which both tickers abstained on a public quote that was too old."""
    first = OPEN + timedelta(minutes=1)
    _attempt(conn, first)
    for ticker in ("SPY", "QQQ"):
        _rows(conn, ticker, first, 305)
        _batch(conn, ticker, OPEN - timedelta(hours=18), "yahoo", 10)
        if withheld:
            _batch(conn, ticker, first - timedelta(seconds=15), "schwab", 20)


def test_while_retrying_the_status_says_so_and_is_not_yet_failing(db) -> None:
    with db.begin() as conn:
        _abstained_session(conn, withheld=False)
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=True)
    assert health["status"] == fh.STATUS_ALL_ABSTAINED        # still true, and still alerts
    assert health["level"] == "retrying"
    assert health["retry"]["quick"] is True and health["retry"]["attempts"] == 1
    assert ("Trying again about every 10 minutes with a fresh capture until a quote qualifies,"
            " 13 attempts have been made, or 2026-11-25T20:30:00+00:00.") in health["message"]


def test_once_retries_are_over_the_session_is_failing(db) -> None:
    with db.begin() as conn:
        _abstained_session(conn, withheld=False)
    with db.connect() as conn:
        late = fh.forward_health(conn, now=CLOSE - timedelta(minutes=10), include_schwab=True)
        ended = fh.forward_health(conn, now=CLOSE + timedelta(hours=1), include_schwab=True)
    assert (late["level"], late["retry"]["reason"]) == ("failing", "past_retry_cutoff")
    assert (ended["level"], ended["retry"]["reason"]) == ("failing", "session_ended")
    assert "Trying again" not in late["message"] and "Trying again" not in ended["message"]


def test_the_incident_is_failing_at_once_because_another_capture_would_be_withheld_too(db) -> None:
    with db.begin() as conn:
        _abstained_session(conn, withheld=True)
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=True)
    assert health["level"] == "failing"
    assert (health["retry"]["quick"], health["retry"]["reason"]) == (False, "newest_capture_is_private_use")
    assert set(health["operator_notes"]) == {"SPY", "QQQ"}
    assert "Trying again" not in health["message"]


def test_the_cadence_is_the_operators_and_is_not_shown_to_everyone_else(db) -> None:
    with db.begin() as conn:
        _abstained_session(conn, withheld=True)
    with db.connect() as conn:
        public = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=False)
    # Whether a retry would help depends on attempts, outcomes and a capture that only an
    # admin request is told about.
    for admin_only in ("retry", "operator_notes", "attempts", "last_outcome", "alert_sent"):
        assert admin_only not in public
    assert public["level"] == "failing" and "Trying again" not in public["message"]


def _outcome(conn: Any, clock: datetime, outcome: str) -> None:
    conn.execute(text("INSERT INTO oracle_iterations VALUES (:at, 0, 0, :note)"),
                 {"at": _at(clock), "note": fh.FORWARD_OUTCOME_NOTE_PREFIX + outcome})


def test_the_stored_outcome_tells_a_failed_capture_from_no_candidate(db) -> None:
    first = OPEN + timedelta(minutes=1)
    with db.begin() as conn:
        _attempt(conn, first)
        _outcome(conn, first + timedelta(seconds=20), "no_capture")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=True)
    assert (health["status"], health["last_outcome"], health["attempts"]) == (fh.STATUS_NO_ROWS, "no_capture", 1)
    assert (health["level"], health["retry"]["reason"]) == ("retrying", "waiting_for_a_fresh_capture")
    with db.begin() as conn:                                  # the retry captured, and no model voted
        _attempt(conn, first + timedelta(minutes=10))
        _outcome(conn, first + timedelta(minutes=10, seconds=20), "no_result")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=15), include_schwab=True)
    assert (health["last_outcome"], health["attempts"]) == ("no_result", 2)        # outcomes are not attempts
    assert (health["level"], health["retry"]["quick"], health["retry"]["reason"]) == ("failing", False, "not_waiting")


def test_an_attempt_that_never_reported_how_it_ended_has_no_outcome(db) -> None:
    first = OPEN + timedelta(minutes=1)
    with db.begin() as conn:
        _attempt(conn, first)
        _outcome(conn, first + timedelta(seconds=20), "no_capture")
        _attempt(conn, first + timedelta(minutes=10))         # timed out: no outcome row
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=15), include_schwab=True)
        earlier = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=True)
    assert health["last_outcome"] is None and health["retry"]["quick"] is False
    assert earlier["last_outcome"] == "no_capture"            # and a replay of 14:35 still sees its own past


@pytest.mark.parametrize("kind", ["no_direction", None])
def test_an_abstention_of_another_kind_is_not_waited_for(db, kind: str | None) -> None:
    """Only "no qualified quote" can be changed by a fresh capture. Anything else stays hourly."""
    first = OPEN + timedelta(minutes=1)
    with db.begin() as conn:
        _attempt(conn, first)
        for ticker in ("SPY", "QQQ"):
            _rows(conn, ticker, first, 305, kind=kind)
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=True)
    assert health["status"] == fh.STATUS_ALL_ABSTAINED and health["abstained_without_quote"] == {}
    assert health["abstention_kinds"] == {kind or "unspecified": 610}
    assert (health["level"], health["retry"]["quick"], health["retry"]["reason"]) == ("failing", False, "not_waiting")


def test_a_session_mixing_kinds_names_each_and_waits_only_for_the_quote(db) -> None:
    first = OPEN + timedelta(minutes=1)
    with db.begin() as conn:
        _attempt(conn, first)
        _rows(conn, "SPY", first, 305, kind="no_direction")
        _rows(conn, "QQQ", first, 305)                          # no qualified quote
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=True)
        public = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=False)
    assert health["abstention_kinds"] == public["abstention_kinds"] == {"no_direction": 305, "no_qualified_quote": 305}
    assert health["abstained_without_quote"] == {"QQQ": 305}
    assert (health["retry"]["quick"], health["retry"]["waiting_tickers"]) == (True, ["QQQ"])


def test_a_later_forecast_ends_the_wait(db) -> None:
    with db.begin() as conn:
        _abstained_session(conn, withheld=False)
        second = OPEN + timedelta(minutes=12)
        _attempt(conn, second)
        _rows(conn, "SPY", second, 40, "PUT")
        _rows(conn, "QQQ", second, 111, "CALL")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=13), include_schwab=True)
    assert (health["status"], health["level"]) == (fh.STATUS_OK, "ok")
    assert (health["retry"]["quick"], health["retry"]["interval_seconds"]) == (False, 3600)
    # The forecast carries the clock of the pass that made it; the abstention keeps its own.
    assert health["tickers"]["SPY"]["first_forecast_at"] == second.isoformat()
    assert health["tickers"]["SPY"]["first_abstention_at"] == (OPEN + timedelta(minutes=1)).isoformat()


# ── The operator's in-memory gate ─────────────────────────────────────


@pytest.fixture
def ho():
    import scripts.hermes_operator as module

    return module


@pytest.mark.parametrize(("result", "gap"), [
    ({"status": "all_abstained"}, 600),
    ({"status": "abstained", "reason": "no_fresh_cohort_capture"}, 600),
    ({"status": "failed", "reason": "timeout_or_worker_failure"}, 3600),    # a timeout has its own cooldown
    ({"status": "no_result"}, 3600),                                        # a fresh capture would not help
    ({"status": "completed"}, 3600),
    ({"status": "partial"}, 3600),
    ({"status": "deferred", "reason": "retry_interval_not_elapsed", "retry_after_s": 240}, 240),
    ({"status": "deferred", "reason": "hourly_attempt_already_recorded", "retry_after_s": 2900}, 2900),
    ({"status": "deferred", "reason": "retry_interval_not_elapsed", "retry_after_s": 5}, 60),
    ({"status": "deferred", "reason": "hourly_attempt_already_recorded", "retry_after_s": 99999}, 3600),
    ({"status": "deferred", "reason": "forward_pass_in_flight"}, 3600),
    (None, 3600),
])
def test_next_gap_follows_how_the_pass_ended(ho, result: Any, gap: int) -> None:
    assert ho._oracle_forward_next_gap(result) == gap


def _step(monkeypatch, ho, *, last: datetime | None, gap: Any, worker_result: Any = None):
    now = OPEN + timedelta(minutes=30)

    class SessionClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz)

    monkeypatch.setattr(ho, "datetime", SessionClock)
    monkeypatch.setattr(ho, "time", SimpleNamespace(monotonic=lambda: 1000.0))
    dispatched: list[str] = []

    def run(name, work, timeout, state):
        dispatched.append(name)
        return worker_result, True

    monkeypatch.setattr(ho, "_run_with_timeout", run)
    monkeypatch.setattr(ho, "_oracle_forward_alert", lambda *args: False)
    report = types.ModuleType("oracle.report")
    report.send_oracle_report = MagicMock()                     # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "oracle.report", report)
    state = SimpleNamespace(last_oracle_cycle=last, current_step=None, cooldowns=MagicMock())
    if gap is not None:
        state.oracle_forward_gap_s = gap
    state.cooldowns.can_retry.return_value = True
    cycle_result: dict[str, Any] = {}
    ho._run_oracle_forward_step(MagicMock(), state, False, cycle_result, 1000.0 + 4500)
    return state, cycle_result["oracle"], dispatched, now


def test_after_an_all_abstained_pass_the_next_cycle_may_try_again_in_ten_minutes(monkeypatch, ho) -> None:
    state, result, dispatched, now = _step(
        monkeypatch, ho, last=None, gap=None, worker_result={"status": "all_abstained"})
    assert dispatched == ["oracle_cycle"] and state.oracle_forward_gap_s == 600
    # Nine minutes later: not yet. Ten minutes later: dispatched.
    early = _step(monkeypatch, ho, last=now - timedelta(minutes=9), gap=600)
    assert early[1] == {"status": "deferred", "reason": "retry_cadence"} and early[2] == []
    due = _step(monkeypatch, ho, last=now - timedelta(minutes=10), gap=600,
                worker_result={"status": "completed", "new_predictions": 0})
    assert due[2] == ["oracle_cycle"]
    assert due[0].oracle_forward_gap_s == 3600               # it has its forecast: hourly again


def test_a_pass_with_a_forecast_keeps_the_hourly_gate(monkeypatch, ho) -> None:
    state, _result, _dispatched, now = _step(
        monkeypatch, ho, last=None, gap=None, worker_result={"status": "completed", "new_predictions": 3})
    assert state.oracle_forward_gap_s == 3600
    again = _step(monkeypatch, ho, last=now - timedelta(seconds=3599), gap=3600)
    assert again[1] == {"status": "deferred", "reason": "hourly_cadence"} and again[2] == []


@pytest.mark.parametrize("gap", [None, 0, -5, "600", 600.5])
def test_a_missing_or_malformed_gap_means_hourly(monkeypatch, ho, gap: Any) -> None:
    _state, result, dispatched, _now = _step(
        monkeypatch, ho, last=OPEN + timedelta(minutes=30) - timedelta(minutes=30), gap=gap)
    assert result == {"status": "deferred", "reason": "hourly_cadence"} and dispatched == []


def test_a_deferral_does_not_dispatch_again_before_the_durable_gate_opens(monkeypatch, ho) -> None:
    deferred = {"status": "deferred", "reason": "retry_interval_not_elapsed", "retry_after_s": 240}
    state, _result, _dispatched, _now = _step(monkeypatch, ho, last=None, gap=None, worker_result=deferred)
    assert state.oracle_forward_gap_s == 240
    state.cooldowns.record_attempt.assert_not_called()


# ── The alert waits for the retry, but not for long ───────────────────


GENERIC = "The Oracle forward pass for the 2026-11-25 session needs attention."


def _mail(monkeypatch) -> list[str]:
    sent: list[str] = []
    email = types.ModuleType("alerts.email")
    email.send_alert = lambda subject, body, severity="info": bool(sent.append(body)) or True  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "alerts.email", email)
    return sent


def _at_clock(monkeypatch, ho, now: datetime) -> None:
    class SessionClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz)

    monkeypatch.setattr(ho, "datetime", SessionClock)


def test_one_abstained_pass_with_a_retry_minutes_away_does_not_e_mail_yet(monkeypatch, ho, db) -> None:
    with db.begin() as conn:
        _abstained_session(conn, withheld=False)
    sent = _mail(monkeypatch)
    first = OPEN + timedelta(minutes=1)
    _at_clock(monkeypatch, ho, first + timedelta(seconds=10))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, first) is False
    assert sent == []
    with db.begin() as conn:                                  # the retry abstained too
        _attempt(conn, first + timedelta(minutes=10))
    _at_clock(monkeypatch, ho, first + timedelta(minutes=10, seconds=10))
    # A fresh state object: the count is the stored attempts, not operator memory.
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, first) is True
    assert len(sent) == 1 and sent[0].startswith(GENERIC)        # nothing about the retry leaves the host


def test_a_retry_that_qualifies_means_no_e_mail_at_all(monkeypatch, ho, db) -> None:
    with db.begin() as conn:
        _abstained_session(conn, withheld=False)
    sent = _mail(monkeypatch)
    first = OPEN + timedelta(minutes=1)
    _at_clock(monkeypatch, ho, first + timedelta(seconds=10))
    ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, first)
    second = first + timedelta(minutes=10)
    with db.begin() as conn:
        _attempt(conn, second)
        _rows(conn, "SPY", second, 40, "PUT")
        _rows(conn, "QQQ", second, 111, "CALL")
    _at_clock(monkeypatch, ho, second + timedelta(seconds=10))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "completed"}, first) is False
    assert sent == []


def test_an_abstained_pass_with_no_retry_to_come_e_mails_at_once(monkeypatch, ho, db) -> None:
    # The incident: the newest capture is a private-use one, so no retry is planned.
    with db.begin() as conn:
        _abstained_session(conn, withheld=True)
    sent = _mail(monkeypatch)
    first = OPEN + timedelta(minutes=1)
    _at_clock(monkeypatch, ho, first + timedelta(seconds=10))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, first) is True
    assert len(sent) == 1
    assert sent[0].startswith(GENERIC) and "Trying again" not in sent[0] and "private-use" not in sent[0]


def test_two_quick_attempts_without_a_capture_are_a_slow_morning_not_an_alert(monkeypatch, ho, db) -> None:
    """11-18 and 11-19 both began with a failed capture. Trying again sooner must not make that an e-mail."""
    sent = _mail(monkeypatch)
    first = OPEN + timedelta(minutes=1)
    with db.begin() as conn:
        for i in range(3):                                    # 14:31, 14:41, 14:51: no capture each time
            _attempt(conn, first + timedelta(minutes=10 * i))
            _outcome(conn, first + timedelta(minutes=10 * i, seconds=20), "no_capture")
    no_capture = {"status": "abstained", "reason": "no_fresh_cohort_capture"}
    _at_clock(monkeypatch, ho, first + timedelta(minutes=21))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), no_capture, first) is False
    assert sent == []
    with db.begin() as conn:                                  # still nothing an hour after the first attempt
        _attempt(conn, first + timedelta(minutes=60))
        _outcome(conn, first + timedelta(minutes=60, seconds=20), "no_capture")
    _at_clock(monkeypatch, ho, first + timedelta(minutes=61))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), no_capture, first) is True
    assert len(sent) == 1 and sent[0].startswith(GENERIC) and "attempt(s)" not in sent[0]


# ── The durable gate inside the pass ──────────────────────────────────


class _Lease:
    def __init__(self, last: datetime | None, log: list[str]) -> None:
        self.last, self.log = last, log

    def execute(self, statement: Any, params: Any = None) -> Any:
        sql = str(statement)
        if "INSERT INTO oracle_iterations" in sql:
            note = str((params or {}).get("note", ""))
            self.log.append("outcome:" + note[len(fh.FORWARD_OUTCOME_NOTE_PREFIX):]
                            if note.startswith(fh.FORWARD_OUTCOME_NOTE_PREFIX) else "attempt")
        value = True if "pg_try_advisory_lock" in sql else self.last if "MAX(iteration_at)" in sql else None
        return SimpleNamespace(scalar_one=lambda: value)

    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def __enter__(self):
        return self

    def __exit__(self, *exc: object) -> None: ...


class _Engine:
    def __init__(self, last: datetime | None) -> None:
        self.log: list[str] = []
        self.last = last

    def connect(self):
        return _Lease(self.last, self.log)

    def begin(self):
        return _Lease(self.last, self.log)


def _pass(monkeypatch, ho, *, plan: dict[str, Any], last_ago: timedelta | None, captured: bool = True):
    from ingestion import options
    from oracle import engine as oracle_engine

    now = OPEN + timedelta(minutes=30)

    class SessionClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz)

    monkeypatch.setattr(ho, "datetime", SessionClock)
    engine = _Engine(None if last_ago is None else now - last_ago)

    class Oracle:
        def __init__(self, db_engine: Any) -> None:
            self._last_store_counts = {"qualified": 2}
            self._last_forward_detail = {"tickers": {"SPY": {"qualified": 1}, "QQQ": {"qualified": 1}},
                                         "abstention_reasons": {}}

        def score_expired_predictions(self) -> dict:
            return {}

        def generate_predictions(self, tickers, *, horizon_days, should_continue) -> list:
            engine.log.append("generate")
            return []

        def _get_leaderboard(self) -> list:
            return []

    class Capture:
        def __init__(self, db_engine: Any) -> None: ...

        def pull_all(self, tickers, *, capture_source, should_continue) -> list:
            engine.log.append("capture")
            return [{"ticker": t, "status": "SUCCESS" if captured else "FAILED"} for t in tickers]

    monkeypatch.setattr(oracle_engine, "OracleEngine", Oracle)
    monkeypatch.setattr(options, "OptionsPuller", Capture)
    monkeypatch.setattr(options, "automatic_options_capture_guard", lambda engine: None)
    monkeypatch.setattr(ho, "_oracle_forward_regular_session", lambda now: True)
    monkeypatch.setattr(ho, "_oracle_forward_retry_plan", lambda engine: plan)
    return ho._run_oracle_forward_pass(engine, should_continue=lambda: True), engine.log


_QUICK = {"quick": True, "interval_seconds": 600, "reason": "waiting_for_a_qualified_quote"}
_HOURLY = {"quick": False, "interval_seconds": 3600, "reason": "not_waiting_for_a_quote"}


def test_while_waiting_a_pass_five_minutes_after_the_last_is_deferred(monkeypatch, ho) -> None:
    result, log = _pass(monkeypatch, ho, plan=_QUICK, last_ago=timedelta(minutes=5))
    assert (result["status"], result["reason"], result["retry_after_s"]) == (
        "deferred", "retry_interval_not_elapsed", 300)
    assert log == []                                  # no attempt recorded, no provider request


def test_while_waiting_a_pass_ten_minutes_after_the_last_is_a_new_decision(monkeypatch, ho) -> None:
    result, log = _pass(monkeypatch, ho, plan=_QUICK, last_ago=timedelta(minutes=10))
    # The attempt is committed before the provider request, then a fresh capture, then the models,
    # then how the pass ended.
    assert log == ["attempt", "capture", "generate", "outcome:completed"]
    assert result["status"] == "completed"


def test_with_a_forecast_on_record_the_pass_is_hourly_as_before(monkeypatch, ho) -> None:
    result, log = _pass(monkeypatch, ho, plan=_HOURLY, last_ago=timedelta(minutes=10))
    assert (result["status"], result["reason"], result["retry_after_s"]) == (
        "deferred", "hourly_attempt_already_recorded", 3000)
    assert log == []
    result, log = _pass(monkeypatch, ho, plan=_HOURLY, last_ago=timedelta(hours=1))
    assert log == ["attempt", "capture", "generate", "outcome:completed"]


def test_the_first_pass_of_the_session_never_waits(monkeypatch, ho) -> None:
    _result, log = _pass(monkeypatch, ho, plan=_HOURLY, last_ago=None)
    assert log == ["attempt", "capture", "generate", "outcome:completed"]


def test_a_pass_without_a_capture_records_that_and_never_generates(monkeypatch, ho) -> None:
    result, log = _pass(monkeypatch, ho, plan=_HOURLY, last_ago=None, captured=False)
    assert (result["status"], result["reason"]) == ("abstained", "no_fresh_cohort_capture")
    assert log == ["attempt", "capture", "outcome:no_capture"]


def test_a_cadence_check_that_fails_keeps_the_hourly_behaviour(monkeypatch, ho) -> None:
    def broken(conn, **kw):
        raise RuntimeError("relation does not exist")

    monkeypatch.setattr(fh, "forward_health", broken)
    plan = ho._oracle_forward_retry_plan(MagicMock())
    assert (plan["quick"], plan["interval_seconds"]) == (False, 3600)


def test_the_cadence_check_reads_every_row_and_returns_the_stored_plan(monkeypatch, ho) -> None:
    seen: list[Any] = []

    def health(conn, *, now, include_schwab=False, **kw):
        seen.append(include_schwab)
        return {"retry": _QUICK}

    monkeypatch.setattr(fh, "forward_health", health)
    assert ho._oracle_forward_retry_plan(MagicMock()) == _QUICK
    assert seen == [True]


def test_the_durable_gate_on_postgresql_follows_the_stored_session(pgdb, monkeypatch, ho) -> None:
    """Restart-proof: the interval comes from stored attempts and rows, not operator memory."""
    from ingestion import options
    from oracle import engine as oracle_engine

    clock = [OPEN + timedelta(minutes=1)]

    class SessionClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz)

    monkeypatch.setattr(ho, "datetime", SessionClock)
    calls: list[str] = []
    outcome = {"abstain": True}

    class Oracle:
        def __init__(self, db_engine: Any) -> None:
            self._last_store_counts = {}
            self._last_forward_detail = None

        def score_expired_predictions(self) -> dict:
            return {}

        def generate_predictions(self, tickers, *, horizon_days, should_continue) -> list:
            calls.append("generate")
            with pgdb.begin() as conn:                 # what the real engine would have stored
                for ticker in tickers:
                    _rows(conn, ticker, clock[0], 3, "NONE" if outcome["abstain"] else "CALL")
            detail = {t: ({"abstained": 3} if outcome["abstain"] else {"qualified": 3}) for t in tickers}
            self._last_forward_detail = {"tickers": detail, "abstention_reasons": {}}
            return []

        def _get_leaderboard(self) -> list:
            return []

    class Capture:
        def __init__(self, db_engine: Any) -> None: ...

        def pull_all(self, tickers, *, capture_source, should_continue) -> list:
            calls.append("capture")
            return [{"ticker": t, "status": "SUCCESS"} for t in tickers]

    monkeypatch.setattr(oracle_engine, "OracleEngine", Oracle)
    monkeypatch.setattr(options, "OptionsPuller", Capture)
    monkeypatch.setattr(options, "automatic_options_capture_guard", lambda engine: None)
    monkeypatch.setattr(ho, "_oracle_forward_regular_session", lambda now: True)

    def run() -> dict[str, Any]:
        # The pass stamps its attempt with the database clock; pin it to the test's.
        result = ho._run_oracle_forward_pass(pgdb, should_continue=lambda: True)
        with pgdb.begin() as conn:
            conn.execute(text("UPDATE oracle_iterations SET iteration_at = :at WHERE iteration_at > :future"),
                         {"at": clock[0], "future": CLOSE})
        return result

    first = run()                                             # 14:31 every candidate abstains
    assert first["status"] == "all_abstained" and calls == ["capture", "generate"]
    clock[0] += timedelta(minutes=5)                          # 14:36 too soon
    assert run()["reason"] == "retry_interval_not_elapsed"
    assert calls == ["capture", "generate"]
    clock[0] += timedelta(minutes=5)                          # 14:41 a new decision, and it qualifies
    outcome["abstain"] = False
    assert run()["status"] == "completed"
    assert calls == ["capture", "generate"] * 2
    clock[0] += timedelta(minutes=10)                         # 14:51 it has its forecast: hourly again
    assert run()["reason"] == "hourly_attempt_already_recorded"
    with pgdb.connect() as conn:
        rows = conn.execute(text(
            "SELECT direction, MIN(created_at), COUNT(*) FROM oracle_predictions"
            " WHERE ticker = 'SPY' GROUP BY direction ORDER BY 2")).fetchall()
        attempts = conn.execute(text("SELECT COUNT(*) FROM oracle_iterations WHERE notes = :n"),
                                {"n": fh.FORWARD_ATTEMPT_NOTE}).scalar_one()
        outcomes = [r[0][len(fh.FORWARD_OUTCOME_NOTE_PREFIX):] for r in conn.execute(text(
            "SELECT notes FROM oracle_iterations WHERE notes LIKE :p ORDER BY iteration_at"),
            {"p": fh.FORWARD_OUTCOME_NOTE_PREFIX + "%"})]
    # Two rows of history, each with the clock of the pass that wrote it. Nothing was rewritten.
    assert [(r[0], r[1].astimezone(timezone.utc), r[2]) for r in rows] == [
        ("NONE", OPEN + timedelta(minutes=1), 3), ("CALL", OPEN + timedelta(minutes=11), 3)]
    assert attempts == 2 and outcomes == ["all_abstained", "completed"]


# ── The engine says on the row that it lacked a quote ─────────────────

DECISION = datetime(2026, 11, 25, 15, 43, 32, tzinfo=timezone.utc)


def test_an_abstention_for_want_of_a_quote_says_so_on_its_row() -> None:
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = MagicMock()
    oracle.models = [oe.OracleModel("test_model", "1.0", "test", ["equity", "vol"])]
    oracle._get_flow_context = lambda ticker: {}
    oracle._get_entry_price_reference = lambda ticker: {"status": "unavailable", "reason": REASON}
    oracle._read_options_signal = lambda ticker: oe.OptionsSignalRead(None, False)
    oracle._schwab_private_inputs = lambda ticker, ref, options_read: False
    preds = oracle._oracle_one_ticker(1, "SPY", 1, DECISION, {}, horizon_days=1)
    assert [p.verdict for p in preds] == [oe.Verdict.NO_DATA]
    assert preds[0].flow_context["abstention_kind"] == "no_qualified_quote"


def test_the_kind_the_engine_writes_is_the_one_the_plan_reads() -> None:
    from oracle import entry_price_policy as policy

    assert (policy.ABSTENTION_KIND_KEY, policy.ABSTENTION_NO_QUALIFIED_QUOTE) == (
        "abstention_kind", "no_qualified_quote")
