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
            outcomes: dict[str, str] | None = None) -> dict[str, Any]:
    """An admin answer. By default each ticker's newest outcome follows its counts:
    a forecast, or (abstentions only) "no_quote", or nothing recorded."""
    if outcomes is None:
        outcomes = {name: ("forecast" if q else "no_quote")
                    for name, (q, a) in (("SPY", spy), ("QQQ", qqq)) if q or a}
    return {
        "status": status, "attempts": attempts, "session_state": state, "ticker_outcomes": outcomes,
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
    both = lambda outcome: {"SPY": outcome, "QQQ": outcome}  # noqa: E731
    no_capture = _health(status=fh.STATUS_NO_ROWS, spy=(0, 0), qqq=(0, 0), outcomes=both("no_capture"))
    plan = fh.retry_plan(no_capture, OPEN + timedelta(minutes=2))
    assert (plan["quick"], plan["reason"], plan["waiting_tickers"]) == (
        True, "waiting_for_a_fresh_capture", ["SPY", "QQQ"])
    for outcomes in (both("no_vote"), both("forecast"), both("abstained_other"), {}):   # {}: no account
        health = _health(status=fh.STATUS_NO_ROWS, spy=(0, 0), qqq=(0, 0), outcomes=outcomes)
        assert fh.retry_plan(health, OPEN + timedelta(minutes=2))["quick"] is False


def test_an_old_quote_abstention_does_not_keep_the_retry_going_after_a_no_vote_pass() -> None:
    """Codex review 6089033512. SPY abstained for its quote at 14:31. At 14:41 the quote qualified
    and no model voted. The 14:31 abstention rows are still there and SPY still has no forecast,
    but a fresh capture cannot change a no-vote: the retry stops."""
    after_first = _health(spy=(0, 305), qqq=(0, 305))                    # both: no_quote
    assert fh.retry_plan(after_first, OPEN + timedelta(minutes=2))["waiting_tickers"] == ["SPY", "QQQ"]
    after_second = _health(spy=(0, 305), qqq=(0, 305), attempts=2,       # same accumulated rows
                           outcomes={"SPY": "no_vote", "QQQ": "no_vote"})
    plan = fh.retry_plan(after_second, OPEN + timedelta(minutes=12))
    assert (plan["quick"], plan["interval_seconds"], plan["reason"], plan["waiting_tickers"]) == (
        False, 3600, "not_waiting", [])


@pytest.mark.parametrize(("outcomes", "waiting", "reason"), [
    ({"SPY": "no_vote", "QQQ": "no_quote"}, ["QQQ"], "waiting_for_a_qualified_quote"),
    ({"SPY": "no_quote", "QQQ": "forecast"}, ["SPY"], "waiting_for_a_qualified_quote"),
    ({"SPY": "no_vote", "QQQ": "no_capture"}, ["QQQ"], "waiting_for_a_fresh_capture"),
    ({"SPY": "no_quote", "QQQ": "no_capture"}, ["SPY", "QQQ"], "waiting_for_a_qualified_quote"),
    ({"SPY": "abstained_other", "QQQ": "no_quote"}, ["QQQ"], "waiting_for_a_qualified_quote"),
])
def test_mixed_tickers_wait_only_for_what_a_capture_can_change(outcomes, waiting, reason) -> None:
    plan = fh.retry_plan(_health(spy=(0, 305), qqq=(0, 305), attempts=2, outcomes=outcomes),
                         OPEN + timedelta(minutes=12))
    assert (plan["quick"], plan["waiting_tickers"], plan["reason"]) == (True, waiting, reason)


def test_a_blocked_quote_does_not_stop_a_retry_for_a_missing_capture() -> None:
    health = _health(attempts=2, outcomes={"SPY": "no_quote", "QQQ": "no_capture"})
    plan = fh.retry_plan(health, OPEN + timedelta(minutes=12), blocked={"SPY": "…"})
    assert (plan["quick"], plan["reason"]) == (True, "waiting_for_a_qualified_quote")


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


def _outcome(conn: Any, written: datetime, status: str, attempt_at: datetime, **tickers: str) -> None:
    """The outcome row a pass writes at its end, for the attempt it made at ``attempt_at``."""
    conn.execute(text("INSERT INTO oracle_iterations VALUES (:at, 0, 0, :note)"),
                 {"at": _at(written), "note": fh.outcome_note(status, attempt_at, tickers)})


def _abstained_session(conn: Any, *, withheld: bool) -> None:
    """One pass at 14:31Z in which both tickers abstained on a public quote that was too old."""
    first = OPEN + timedelta(minutes=1)
    _attempt(conn, first)
    _outcome(conn, first + timedelta(seconds=20), "all_abstained", first, SPY="no_quote", QQQ="no_quote")
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
    for admin_only in ("retry", "operator_notes", "attempts", "last_outcome", "ticker_outcomes", "alert_sent"):
        assert admin_only not in public
    assert public["level"] == "failing" and "Trying again" not in public["message"]


def test_the_stored_account_tells_a_failed_capture_from_no_candidate(db) -> None:
    first = OPEN + timedelta(minutes=1)
    with db.begin() as conn:
        _attempt(conn, first)
        _outcome(conn, first + timedelta(seconds=20), "no_capture", first, SPY="no_capture", QQQ="no_capture")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=True)
    assert (health["status"], health["last_outcome"], health["attempts"]) == (fh.STATUS_NO_ROWS, "no_capture", 1)
    assert health["ticker_outcomes"] == {"SPY": "no_capture", "QQQ": "no_capture"}
    assert (health["level"], health["retry"]["reason"]) == ("retrying", "waiting_for_a_fresh_capture")
    second = first + timedelta(minutes=10)
    with db.begin() as conn:                                  # the retry captured, and no model voted
        _attempt(conn, second)
        _outcome(conn, second + timedelta(seconds=20), "no_result", second, SPY="no_vote", QQQ="no_vote")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=15), include_schwab=True)
    assert (health["last_outcome"], health["attempts"]) == ("no_result", 2)        # outcomes are not attempts
    assert (health["level"], health["retry"]["quick"], health["retry"]["reason"]) == ("failing", False, "not_waiting")


def test_no_quote_then_a_qualified_quote_with_no_vote_stops_the_retry_from_the_stored_record(db, ho, monkeypatch) -> None:
    """The control for review 6089033512, from stored rows only, as a restarted operator reads them."""
    first, second = OPEN + timedelta(minutes=1), OPEN + timedelta(minutes=11)
    with db.begin() as conn:
        _abstained_session(conn, withheld=False)              # 14:31 SPY and QQQ: no qualified quote
    with db.connect() as conn:
        assert fh.forward_health(conn, now=first + timedelta(minutes=1),
                                 include_schwab=True)["retry"]["waiting_tickers"] == ["SPY", "QQQ"]
    with db.begin() as conn:                                  # 14:41 SPY: qualified, no vote. QQQ: forecast.
        _attempt(conn, second)
        _rows(conn, "QQQ", second, 111, "CALL")
        _outcome(conn, second + timedelta(seconds=20), "completed", second, SPY="no_vote", QQQ="forecast")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=second + timedelta(minutes=1), include_schwab=True)
    # SPY's 14:31 abstentions are still on record and it has no forecast...
    assert health["tickers"]["SPY"]["abstained"] == 305 and health["tickers"]["SPY"]["qualified"] == 0
    assert health["status"] == fh.STATUS_PARTIAL
    # ...and nothing waits: the newest pass's account says no_vote, which a capture cannot change.
    assert health["ticker_outcomes"] == {"SPY": "no_vote", "QQQ": "forecast"}
    assert (health["retry"]["quick"], health["retry"]["interval_seconds"], health["retry"]["waiting_tickers"]) == (
        False, 3600, [])
    # A restarted operator has no memory: it reads the same rows and makes the same choice.
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return (second + timedelta(minutes=1)).astimezone(tz)

    monkeypatch.setattr(ho, "datetime", Clock)
    assert ho._oracle_forward_retry_plan(db) == health["retry"]


def test_a_mixed_session_keeps_retrying_only_for_the_ticker_that_lacks_a_quote(db) -> None:
    second = OPEN + timedelta(minutes=11)
    with db.begin() as conn:
        _abstained_session(conn, withheld=False)
        _attempt(conn, second)                                # 14:41 SPY: no vote. QQQ: quote refused again.
        _outcome(conn, second + timedelta(seconds=20), "all_abstained", second, SPY="no_vote", QQQ="no_quote")
    with db.connect() as conn:
        health = fh.forward_health(conn, now=second + timedelta(minutes=1), include_schwab=True)
    assert (health["retry"]["quick"], health["retry"]["waiting_tickers"]) == (True, ["QQQ"])


def test_only_the_newest_attempts_own_account_is_used(db) -> None:
    """An attempt that never reported how it ended has no account; an earlier pass's says nothing about it."""
    first, second = OPEN + timedelta(minutes=1), OPEN + timedelta(minutes=11)
    with db.begin() as conn:
        _attempt(conn, first)
        _outcome(conn, first + timedelta(seconds=20), "no_capture", first, SPY="no_capture", QQQ="no_capture")
        _attempt(conn, second)                                # timed out: no outcome row
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=15), include_schwab=True)
        earlier = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=True)
    assert health["last_outcome"] is None and health["ticker_outcomes"] == {}
    assert health["retry"]["quick"] is False
    assert earlier["last_outcome"] == "no_capture"            # a replay of 14:35 still sees its own past
    # An account written late, for the first attempt, after the second began: still not the second's.
    with db.begin() as conn:
        _outcome(conn, second + timedelta(minutes=1), "all_abstained", first, SPY="no_quote", QQQ="no_quote")
    with db.connect() as conn:
        late = fh.forward_health(conn, now=OPEN + timedelta(minutes=15), include_schwab=True)
    assert late["last_outcome"] is None and late["retry"]["quick"] is False


def test_the_note_round_trips_and_other_notes_are_not_outcomes() -> None:
    attempt = OPEN + timedelta(minutes=1, microseconds=123456)
    note = fh.outcome_note("partial", attempt, {"SPY": "no_capture", "QQQ": "forecast"})
    assert note == ("oracle_forward_v2:SPY_QQQ:1d:outcome:partial|attempt=2026-11-25T14:31:00.123456+00:00"
                    "|QQQ=forecast|SPY=no_capture")
    assert fh.parse_outcome_note(note) == {
        "status": "partial", "attempt": "2026-11-25T14:31:00.123456+00:00",
        "tickers": {"QQQ": "forecast", "SPY": "no_capture"}}
    assert fh.parse_outcome_note(fh.FORWARD_ATTEMPT_NOTE) is None
    assert fh.parse_outcome_note(fh.alert_note("2026-11-25")) is None
    assert fh.parse_outcome_note(None) is None
    # A pass that could not read its attempt clock writes an untied account, which matches no attempt.
    assert fh.parse_outcome_note(fh.outcome_note("completed", None, {}))["attempt"] is None


@pytest.mark.parametrize(("detail", "expected"), [
    ({"SPY": {"abstained": 305, "without_quote": 305}, "QQQ": {"qualified": 111}},
     {"SPY": "no_quote", "QQQ": "forecast"}),
    ({"SPY": {"duplicate_abstentions": 305, "without_quote": 305}, "QQQ": {"duplicate_forecasts": 111}},
     {"SPY": "no_quote", "QQQ": "forecast"}),
    ({"SPY": {"abstained": 305, "without_quote": 0}}, {"SPY": "abstained_other", "QQQ": "no_vote"}),
    ({"SPY": {"abstained": 305, "without_quote": 100}}, {"SPY": "abstained_other", "QQQ": "no_vote"}),
    ({}, {"SPY": "no_vote", "QQQ": "no_vote"}),
])
def test_a_pass_says_what_each_captured_ticker_ended_with(detail, expected) -> None:
    outcome = fh.pass_outcome(["SPY", "QQQ"], {"tickers": detail, "abstention_reasons": {}})
    assert outcome["ticker_outcomes"] == expected
    assert fh.pass_outcome(["SPY", "QQQ"], None)["ticker_outcomes"] == {}      # unknown detail: no account


@pytest.mark.parametrize("kind", ["no_direction", None])
def test_an_abstention_of_another_kind_is_not_waited_for(db, kind: str | None) -> None:
    """Only "no qualified quote" can be changed by a fresh capture. Anything else stays hourly."""
    first = OPEN + timedelta(minutes=1)
    with db.begin() as conn:
        _attempt(conn, first)
        _outcome(conn, first + timedelta(seconds=20), "all_abstained", first,
                 SPY="abstained_other", QQQ="abstained_other")
        for ticker in ("SPY", "QQQ"):
            _rows(conn, ticker, first, 305, kind=kind)
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=True)
    assert health["status"] == fh.STATUS_ALL_ABSTAINED
    assert health["abstention_kinds"] == {kind or "unspecified": 610}
    assert (health["level"], health["retry"]["quick"], health["retry"]["reason"]) == ("failing", False, "not_waiting")


def test_a_session_mixing_kinds_names_each_and_waits_only_for_the_quote(db) -> None:
    first = OPEN + timedelta(minutes=1)
    with db.begin() as conn:
        _attempt(conn, first)
        _outcome(conn, first + timedelta(seconds=20), "all_abstained", first, SPY="abstained_other", QQQ="no_quote")
        _rows(conn, "SPY", first, 305, kind="no_direction")
        _rows(conn, "QQQ", first, 305)                          # no qualified quote
    with db.connect() as conn:
        health = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=True)
        public = fh.forward_health(conn, now=OPEN + timedelta(minutes=5), include_schwab=False)
    assert health["abstention_kinds"] == public["abstention_kinds"] == {"no_direction": 305, "no_qualified_quote": 305}
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
    _at_clock(monkeypatch, ho, first + timedelta(seconds=30))       # after the pass wrote its outcome
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, first) is False
    assert sent == []
    second = first + timedelta(minutes=10)
    with db.begin() as conn:                                  # the retry abstained too
        _attempt(conn, second)
        _outcome(conn, second + timedelta(seconds=20), "all_abstained", second, SPY="no_quote", QQQ="no_quote")
    _at_clock(monkeypatch, ho, second + timedelta(seconds=30))
    # A fresh state object: the count is the stored attempts, not operator memory.
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, first) is True
    assert len(sent) == 1 and sent[0].startswith(GENERIC)        # nothing about the retry leaves the host


def test_a_retry_that_qualifies_means_no_e_mail_at_all(monkeypatch, ho, db) -> None:
    with db.begin() as conn:
        _abstained_session(conn, withheld=False)
    sent = _mail(monkeypatch)
    first = OPEN + timedelta(minutes=1)
    _at_clock(monkeypatch, ho, first + timedelta(seconds=30))       # after the pass wrote its outcome
    ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, first)
    second = first + timedelta(minutes=10)
    with db.begin() as conn:
        _attempt(conn, second)
        _rows(conn, "SPY", second, 40, "PUT")
        _rows(conn, "QQQ", second, 111, "CALL")
        _outcome(conn, second + timedelta(seconds=20), "completed", second, SPY="forecast", QQQ="forecast")
    _at_clock(monkeypatch, ho, second + timedelta(seconds=30))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "completed"}, first) is False
    assert sent == []


def test_an_abstained_pass_with_no_retry_to_come_e_mails_at_once(monkeypatch, ho, db) -> None:
    # The incident: the newest capture is a private-use one, so no retry is planned.
    with db.begin() as conn:
        _abstained_session(conn, withheld=True)
    sent = _mail(monkeypatch)
    first = OPEN + timedelta(minutes=1)
    _at_clock(monkeypatch, ho, first + timedelta(seconds=30))       # after the pass wrote its outcome
    assert ho._oracle_forward_alert(db, SimpleNamespace(), {"status": "all_abstained"}, first) is True
    assert len(sent) == 1
    assert sent[0].startswith(GENERIC) and "Trying again" not in sent[0] and "private-use" not in sent[0]


def test_two_quick_attempts_without_a_capture_are_a_slow_morning_not_an_alert(monkeypatch, ho, db) -> None:
    """11-18 and 11-19 both began with a failed capture. Trying again sooner must not make that an e-mail."""
    sent = _mail(monkeypatch)
    first = OPEN + timedelta(minutes=1)
    with db.begin() as conn:
        for i in range(3):                                    # 14:31, 14:41, 14:51: no capture each time
            at = first + timedelta(minutes=10 * i)
            _attempt(conn, at)
            _outcome(conn, at + timedelta(seconds=20), "no_capture", at, SPY="no_capture", QQQ="no_capture")
    no_capture = {"status": "abstained", "reason": "no_fresh_cohort_capture"}
    _at_clock(monkeypatch, ho, first + timedelta(minutes=21))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), no_capture, first) is False
    assert sent == []
    with db.begin() as conn:                                  # still nothing an hour after the first attempt
        hour_later = first + timedelta(minutes=60)
        _attempt(conn, hour_later)
        _outcome(conn, hour_later + timedelta(seconds=20), "no_capture", hour_later,
                 SPY="no_capture", QQQ="no_capture")
    _at_clock(monkeypatch, ho, first + timedelta(minutes=61))
    assert ho._oracle_forward_alert(db, SimpleNamespace(), no_capture, first) is True
    assert len(sent) == 1 and sent[0].startswith(GENERIC) and "attempt(s)" not in sent[0]


# ── The durable gate inside the pass ──────────────────────────────────


ATTEMPT_CLOCK = OPEN + timedelta(minutes=30, seconds=1)      # what the fake database stamps on an attempt


class _Lease:
    def __init__(self, last: datetime | None, log: list[str], notes: list[str] | None = None) -> None:
        self.last, self.log, self.notes = last, log, notes if notes is not None else []

    def execute(self, statement: Any, params: Any = None) -> Any:
        sql = str(statement)
        if "INSERT INTO oracle_iterations" in sql:
            note = str((params or {}).get("note", ""))
            parsed = fh.parse_outcome_note(note)
            self.log.append("attempt" if parsed is None else "outcome:" + parsed["status"])
            self.notes.append(note)
        value = (True if "pg_try_advisory_lock" in sql else self.last if "MAX(iteration_at)" in sql
                 else ATTEMPT_CLOCK if "RETURNING iteration_at" in sql else None)
        return SimpleNamespace(scalar_one=lambda: value)

    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def __enter__(self):
        return self

    def __exit__(self, *exc: object) -> None: ...


class _Engine:
    def __init__(self, last: datetime | None) -> None:
        self.log: list[str] = []
        self.notes: list[str] = []
        self.last = last

    def connect(self):
        return _Lease(self.last, self.log, self.notes)

    def begin(self):
        return _Lease(self.last, self.log, self.notes)


def _pass(monkeypatch, ho, *, plan: dict[str, Any], last_ago: timedelta | None, captured: Any = True,
          detail: Any = None, notes: list[str] | None = None):
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
            self._last_forward_detail = detail if detail is not None else {
                "tickers": {"SPY": {"qualified": 1}, "QQQ": {"qualified": 1}}, "abstention_reasons": {}}

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
            ok = set(tickers) if captured is True else set(captured or ())
            return [{"ticker": t, "status": "SUCCESS" if t in ok else "FAILED"} for t in tickers]

    monkeypatch.setattr(oracle_engine, "OracleEngine", Oracle)
    monkeypatch.setattr(options, "OptionsPuller", Capture)
    monkeypatch.setattr(options, "automatic_options_capture_guard", lambda engine: None)
    monkeypatch.setattr(ho, "_oracle_forward_regular_session", lambda now: True)
    monkeypatch.setattr(ho, "_oracle_forward_retry_plan", lambda engine: plan)
    result = ho._run_oracle_forward_pass(engine, should_continue=lambda: True)
    if notes is not None:
        notes.extend(engine.notes)
    return result, engine.log


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
    notes: list[str] = []
    result, log = _pass(monkeypatch, ho, plan=_HOURLY, last_ago=None, captured=False, notes=notes)
    assert (result["status"], result["reason"]) == ("abstained", "no_fresh_cohort_capture")
    assert log == ["attempt", "capture", "outcome:no_capture"]
    assert fh.parse_outcome_note(notes[-1]) == {
        "status": "no_capture", "attempt": ATTEMPT_CLOCK.isoformat(),
        "tickers": {"SPY": "no_capture", "QQQ": "no_capture"}}


def test_the_pass_writes_each_tickers_outcome_tied_to_its_own_attempt(monkeypatch, ho) -> None:
    """SPY's quote qualified and no model voted; QQQ abstained for its quote."""
    notes: list[str] = []
    detail = {"tickers": {"QQQ": {"abstained": 305, "without_quote": 305}}, "abstention_reasons": {REASON: 305}}
    result, _log = _pass(monkeypatch, ho, plan=_HOURLY, last_ago=None, detail=detail, notes=notes)
    assert result["status"] == "all_abstained"
    assert [n for n in notes if fh.parse_outcome_note(n) is None] == [ho.ORACLE_FORWARD_ATTEMPT_NOTE]
    assert fh.parse_outcome_note(notes[-1]) == {
        "status": "all_abstained", "attempt": ATTEMPT_CLOCK.isoformat(),
        "tickers": {"SPY": "no_vote", "QQQ": "no_quote"}}


def test_a_ticker_that_was_not_captured_is_recorded_as_no_capture(monkeypatch, ho) -> None:
    notes: list[str] = []
    detail = {"tickers": {"QQQ": {"qualified": 111}}, "abstention_reasons": {}}
    result, _log = _pass(monkeypatch, ho, plan=_HOURLY, last_ago=None, captured=["QQQ"], detail=detail, notes=notes)
    assert result["status"] == "partial"
    assert fh.parse_outcome_note(notes[-1])["tickers"] == {"SPY": "no_capture", "QQQ": "forecast"}


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
            detail = {t: ({"abstained": 3, "without_quote": 3} if outcome["abstain"] else {"qualified": 3})
                      for t in tickers}
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
        # The pass stamps its rows with the database clock. Move this pass's attempt to the test's
        # clock and its outcome row with it, keeping the attempt key inside the note in step.
        result = ho._run_oracle_forward_pass(pgdb, should_continue=lambda: True)
        with pgdb.begin() as conn:
            fresh = conn.execute(text(
                "SELECT iteration_at, notes FROM oracle_iterations WHERE iteration_at > :future"
                " ORDER BY iteration_at"), {"future": CLOSE}).fetchall()
            stamped = next((at for at, note in fresh if note == fh.FORWARD_ATTEMPT_NOTE), None)
            for at, note in fresh:
                parsed = fh.parse_outcome_note(note)
                moved = (clock[0] if parsed is None
                         else clock[0] + timedelta(seconds=20))
                new_note = (note if parsed is None or stamped is None
                            else fh.outcome_note(parsed["status"], clock[0], parsed["tickers"]))
                conn.execute(text("UPDATE oracle_iterations SET iteration_at = :to, notes = :n"
                                  " WHERE iteration_at = :at AND notes = :old"),
                             {"to": moved, "n": new_note, "at": at, "old": note})
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
        outcomes = [fh.parse_outcome_note(r[0]) for r in conn.execute(text(
            "SELECT notes FROM oracle_iterations WHERE notes LIKE :p ORDER BY iteration_at"),
            {"p": fh.FORWARD_OUTCOME_NOTE_PREFIX + "%"})]
    # Two rows of history, each with the clock of the pass that wrote it. Nothing was rewritten.
    assert [(r[0], r[1].astimezone(timezone.utc), r[2]) for r in rows] == [
        ("NONE", OPEN + timedelta(minutes=1), 3), ("CALL", OPEN + timedelta(minutes=11), 3)]
    assert attempts == 2
    assert outcomes == [
        {"status": "all_abstained", "attempt": (OPEN + timedelta(minutes=1)).isoformat(),
         "tickers": {"SPY": "no_quote", "QQQ": "no_quote"}},
        {"status": "completed", "attempt": (OPEN + timedelta(minutes=11)).isoformat(),
         "tickers": {"SPY": "forecast", "QQQ": "forecast"}},
    ]


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
