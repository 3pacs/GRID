"""S1: oracle expiry-close lookup (oracle_expiry_close_v2) and the hindsight rescore.

The bug (griddb, 2026-11-06): every one of the 15,792 dedup_keep rows noted
"No price data at expiry" was stamped on its own expiry date between 00:00
and 01:59 UTC. The old lookup returned None for "the expiry-day close does not
exist yet", and score_one_chunk() turned that None into a final no_data. A
weekend expiry could never price at all.

Pinned here:

* trading-day expiry uses that day's close only; a non-trading expiry uses the
  previous trading close, never the next one (no lookahead);
* nothing dated after the expiry is read, and an intraday (unsettled) vintage
  is never a close; reads go through store.observations (SUCCESS only, latest
  vintage), TIINGO first, yfinance second, raw ``:close`` never ``adj_close``;
* a missing close leaves the row pending inside the grace window (the exact
  production failure), and the chunk walk still terminates;
* the rescore is additive (oracle_predictions untouched), idempotent, and
  every rescored row is marked exploratory hindsight / not official.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import IntegrityError

import oracle.engine  # noqa: F401  (import the real oracle package first; see test_oracle_null_readers)
import scripts.score_oracle_trades as sot
from store.observations import Observation

REPO_ROOT = Path(__file__).resolve().parents[1]
NY = ZoneInfo("America/New_York")
UTC = timezone.utc

sqlite3.register_adapter(date, date.isoformat)
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))
sqlite3.register_converter("DATE", lambda b: date.fromisoformat(b.decode()))

# 2026-10-30 Fri, 10-31 Sat, 11-01 Sun, 11-02 Mon, 11-03 Tue.
FRI, SAT, SUN, MON, TUE = (date(2026, 10, 30), date(2026, 10, 31), date(2026, 11, 1),
                           date(2026, 11, 2), date(2026, 11, 3))


def _obs(ticker: str, d: date, value: float, *, pulled: datetime | None = None,
         source: str = "TIINGO") -> Observation:
    pulled = pulled or datetime.combine(d, time(19, 30), NY).astimezone(UTC)
    return Observation(sot.close_series_id(ticker), d, value, pulled, source)


def _book(ticker: str, closes: dict[date, float]) -> dict:
    return {ticker: {d: _obs(ticker, d, v) for d, v in closes.items()}}


CREATED = datetime(2026, 10, 20, 14, 0, tzinfo=UTC)


# ── session rule ────────────────────────────────────────────────────────────

@pytest.mark.parametrize(("expiry", "session"), [
    (TUE, TUE),                                   # trading day: itself
    (SAT, FRI), (SUN, FRI),                       # weekend: previous close
    (date(2026, 11, 26), date(2026, 11, 25)),     # Thanksgiving -> Wednesday
    (date(2026, 4, 3), date(2026, 4, 2)),         # Good Friday -> Thursday
    (date(2026, 4, 5), date(2026, 4, 2)),         # Easter Sunday -> Thursday
])
def test_expiry_session_is_the_last_trading_day_on_or_before_expiry(expiry, session):
    assert sot.expiry_session_date("SPY", expiry) == session
    assert sot.expiry_session_date("SPY", expiry) <= expiry


def test_crypto_trades_every_day():
    assert sot.expiry_session_date("BTC", SAT) == SAT
    assert sot.close_series_id("BTC") == "YF:BTC-USD:close"


@pytest.mark.parametrize("day", [date(2026, 11, 27), date(2026, 12, 24),
                                 date(2027, 11, 26), date(2028, 7, 3)])
def test_cash_equity_half_day_closes_at_one_eastern(day):
    assert sot.session_close_utc("SPY", day) == datetime.combine(day, time(13), NY).astimezone(UTC)
    # A decision after an early close has not forecast that session's result.
    result = sot.resolve_expiry_close(
        _book("SPY", {day: 101}), "SPY", day,
        created_at=datetime.combine(day, time(13, 1), NY),
        now=datetime.combine(day, time(20), NY),
    )
    assert result.status == "no_data" and result.note == sot.NOTE_NO_SESSION


def test_day_before_observed_independence_holiday_is_not_invented_half_day():
    day = date(2026, 7, 2)
    assert sot.session_close_utc("SPY", day) == datetime.combine(day, time(16), NY).astimezone(UTC)


def test_close_series_is_raw_close_never_adjusted():
    assert sot.close_series_id("AAPL") == "YF:AAPL:close"
    assert "adj_close" not in sot.close_series_id("AAPL")


# ── resolve_expiry_close ────────────────────────────────────────────────────

def test_trading_day_expiry_uses_that_days_close_only():
    book = _book("SPY", {MON: 752.10, TUE: 754.35})
    res = sot.resolve_expiry_close(book, "SPY", TUE, created_at=CREATED,
                                   now=datetime(2026, 11, 4, 1, tzinfo=UTC))
    assert (res.status, res.price, res.session_date) == ("priced", 754.35, TUE)


def test_trading_day_without_its_close_never_falls_back_to_an_earlier_close():
    book = _book("SPY", {MON: 752.10})
    inside = sot.resolve_expiry_close(book, "SPY", TUE, created_at=CREATED,
                                      now=datetime(2026, 11, 5, tzinfo=UTC))
    assert inside.status == "pending" and inside.price is None
    after = sot.resolve_expiry_close(book, "SPY", TUE, created_at=CREATED,
                                     now=datetime(2026, 11, 8, tzinfo=UTC))
    assert after.status == "no_data" and after.note == sot.NOTE_NO_PRICE


def test_the_production_failure_expiry_day_at_midnight_stays_pending():
    """Exactly what closed 15,792 rows: run at 00:00Z on the expiry date."""
    res = sot.resolve_expiry_close({}, "SPY", TUE, created_at=CREATED,
                                   now=datetime(2026, 11, 3, 0, 0, 7, tzinfo=UTC))
    assert res.status == "pending"


def test_weekend_expiry_uses_the_previous_close_not_the_next():
    book = _book("SPY", {FRI: 755.72, MON: 752.10})
    for expiry in (SAT, SUN):
        res = sot.resolve_expiry_close(book, "SPY", expiry, created_at=CREATED,
                                       now=datetime(2026, 11, 4, tzinfo=UTC))
        assert (res.status, res.price, res.session_date) == ("priced", 755.72, FRI)


def test_weekend_expiry_prices_at_saturday_midnight_run():
    book = _book("SPY", {FRI: 755.72})
    res = sot.resolve_expiry_close(book, "SPY", SAT, created_at=CREATED,
                                   now=datetime(2026, 10, 31, 0, 0, 7, tzinfo=UTC))
    assert res.status == "priced" and res.price == 755.72


def test_prediction_created_after_the_session_closed_is_unscorable():
    """Created Saturday, expiring Sunday: no session between the two."""
    book = _book("SPY", {FRI: 755.72})
    res = sot.resolve_expiry_close(
        book, "SPY", SUN, created_at=datetime(2026, 10, 31, 9, tzinfo=UTC),
        now=datetime(2026, 11, 4, tzinfo=UTC),
    )
    assert res.status == "no_data" and res.note == sot.NOTE_NO_SESSION


def test_classify_outcome_matches_the_scorer_thresholds():
    assert sot.classify_outcome("CALL", 100.0, 106.0, 10.0)[0] == "hit"
    assert sot.classify_outcome("CALL", 100.0, 102.0, 10.0)[0] == "partial"
    assert sot.classify_outcome("PUT", 100.0, 102.0, -10.0)[0] == "miss"
    assert sot.classify_outcome("NONE", 100.0, 102.0, 1.0) is None


def test_explicit_zero_expected_move_is_not_replaced_with_a_one_percent_target():
    assert sot.classify_outcome("CALL", 100.0, 100.1, 0.0)[0] == "hit"
    assert sot.classify_outcome("CALL", 100.0, 100.1, None)[0] == "partial"


# ── load_expiry_closes over raw_series (SQLite stand-in) ───────────────────

def _price_engine():
    eng = create_engine("sqlite://", connect_args={"detect_types": sqlite3.PARSE_DECLTYPES})

    @event.listens_for(eng, "connect")
    def _now(dbapi_conn, _record):
        dbapi_conn.create_function("NOW", 0, lambda: "2026-11-06 00:00:00")

    with eng.begin() as c:
        c.execute(text("CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT)"))
        c.execute(text(
            "CREATE TABLE raw_series (id INTEGER PRIMARY KEY, series_id TEXT, source_id INTEGER, "
            "obs_date DATE, value DOUBLE PRECISION, pull_timestamp TEXT, pull_status TEXT, "
            "raw_payload TEXT)"
        ))
        c.execute(text("INSERT INTO source_catalog (id, name) VALUES (1, 'TIINGO'), (2, 'yfinance')"))
    return eng


def _raw(c, sid: str, src: int, d: date, value: float, pulled: datetime, status: str = "SUCCESS"):
    c.execute(text(
        "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_timestamp, pull_status) "
        "VALUES (:s, :src, :d, :v, :p, :st)"
    ), {"s": sid, "src": src, "d": d, "v": value, "p": pulled.astimezone(UTC), "st": status})


def _ny(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime.combine(d, time(hh, mm), NY)


def test_load_reads_settled_success_closes_and_never_after_end():
    eng = _price_engine()
    with eng.begin() as c:
        _raw(c, "YF:SPY:close", 1, FRI, 755.72, _ny(FRI, 19, 30))
        _raw(c, "YF:SPY:close", 1, MON, 752.10, _ny(MON, 19, 30))       # after end
        _raw(c, "YF:SPY:adj_close", 1, FRI, 700.00, _ny(FRI, 19, 30))   # wrong basis
        _raw(c, "YF:SPY:close", 1, TUE, 0.0, _ny(TUE, 19), "FAILED")    # failure marker
    with eng.connect() as c:
        book = sot.load_expiry_closes(c, ["SPY"], FRI - timedelta(days=7), SAT)
    assert set(book["SPY"]) == {FRI}
    assert book["SPY"][FRI].value == 755.72


def test_load_rejects_an_intraday_vintage_and_takes_the_settled_one():
    eng = _price_engine()
    with eng.begin() as c:
        _raw(c, "YF:AAPL:close", 1, TUE, 330.00, _ny(TUE, 11))          # intraday only
        _raw(c, "YF:AAPL:close", 1, MON, 338.10, _ny(MON, 12))
        _raw(c, "YF:AAPL:close", 1, MON, 338.32, _ny(MON, 18, 1))       # settled re-pull
    with eng.connect() as c:
        book = sot.load_expiry_closes(c, ["AAPL"], MON, TUE)
    assert TUE not in book["AAPL"]
    assert book["AAPL"][MON].value == 338.32


def test_load_prefers_tiingo_and_falls_back_to_yfinance_per_date():
    eng = _price_engine()
    with eng.begin() as c:
        _raw(c, "YF:SPY:close", 1, MON, 752.10, _ny(MON, 19))
        _raw(c, "YF:SPY:close", 2, MON, 752.0999, _ny(MON, 23))
        _raw(c, "YF:SPY:close", 2, TUE, 754.3499, _ny(TUE, 23))
    with eng.connect() as c:
        book = sot.load_expiry_closes(c, ["SPY"], MON, TUE)
    assert (book["SPY"][MON].source, book["SPY"][MON].value) == ("TIINGO", 752.10)
    assert book["SPY"][TUE].source == "yfinance"


# ── score_one_chunk: pending rows are left alone and the walk terminates ───

_PRED_DDL = """
CREATE TABLE oracle_predictions (
    id TEXT PRIMARY KEY, created_at TEXT, ticker TEXT NOT NULL, prediction_type TEXT,
    direction TEXT NOT NULL, target_price DOUBLE PRECISION, entry_price DOUBLE PRECISION,
    expiry DATE NOT NULL, confidence DOUBLE PRECISION, expected_move_pct DOUBLE PRECISION,
    model_name TEXT, signals TEXT, verdict TEXT DEFAULT 'pending',
    actual_price DOUBLE PRECISION, actual_move_pct DOUBLE PRECISION, pnl_pct DOUBLE PRECISION,
    scored_at TEXT, score_notes TEXT, dedup_keep BOOLEAN NOT NULL DEFAULT 1,
    null_write_policy TEXT
)
"""


def _pred_engine():
    eng = _price_engine()
    with eng.begin() as c:
        c.execute(text(_PRED_DDL))
        c.execute(text(
            "CREATE TABLE oracle_models (name TEXT PRIMARY KEY, predictions_made INTEGER DEFAULT 0, "
            "hits INTEGER DEFAULT 0, misses INTEGER DEFAULT 0, partials INTEGER DEFAULT 0, "
            "cumulative_pnl DOUBLE PRECISION DEFAULT 0, last_updated TEXT)"
        ))
        c.execute(text("INSERT INTO oracle_models (name) VALUES ('m')"))
    return eng


def _pred(c, pid: str, ticker: str, expiry: date, *, entry: float = 100.0, direction: str = "CALL",
          verdict: str = "pending", notes: str | None = None, keep: int = 1,
          created: str = "2026-10-20 14:00:00", expected: float = 2.0) -> None:
    c.execute(text(
        "INSERT INTO oracle_predictions (id, created_at, ticker, direction, entry_price, expiry, "
        "confidence, expected_move_pct, model_name, signals, verdict, score_notes, dedup_keep) "
        "VALUES (:id, :cr, :t, :dir, :e, :x, 0.6, :exp, 'm', '{}', :v, :n, :k)"
    ), {"id": pid, "cr": created, "t": ticker, "dir": direction, "e": entry, "x": expiry,
        "exp": expected, "v": verdict, "n": notes, "k": keep})


def test_chunk_walk_leaves_awaiting_rows_pending_and_terminates():
    eng = _pred_engine()
    with eng.begin() as c:
        _pred(c, "a-1", "SPY", TUE)
        _pred(c, "a-2", "SPY", TUE)
        _pred(c, "b-1", "QQQ", MON)      # priced
    # A losing row keeps the success-lesson writer (its own connection on the
    # shared SQLite memory DB) out of the way; lessons are not under test.
    book = _book("QQQ", {MON: 90.0})
    now = datetime(2026, 11, 3, 0, 0, 7, tzinfo=UTC)
    totals: dict[str, int] = {}
    cursor, calls = None, 0
    while True:
        calls += 1
        assert calls < 10, "chunk walk did not terminate"
        with eng.begin() as c:
            counters = sot.score_one_chunk(c, engine=eng, prices=book, today=TUE,
                                           chunk_size=1, now=now, after=cursor)
        fetched, cursor = counters.pop("fetched"), counters.pop("cursor")
        for k, v in counters.items():
            totals[k] = totals.get(k, 0) + v
        if fetched == 0 or cursor is None:
            break
    assert totals["pending_price"] == 2 and totals["no_data"] == 0
    assert totals["scored"] == 1
    with eng.connect() as c:
        verdicts = dict(c.execute(text("SELECT id, verdict FROM oracle_predictions")).fetchall())
        notes = c.execute(text("SELECT score_notes FROM oracle_predictions WHERE id='b-1'")).scalar()
    assert verdicts == {"a-1": "pending", "a-2": "pending", "b-1": "miss"}
    assert sot.EXPIRY_CLOSE_RULE in notes and "close 2026-11-02 TIINGO" in notes


def _scheduled_oracle(eng):
    """Real scheduled scorer, no schema bootstrapping or model calls."""
    obj = object.__new__(oracle.engine.OracleEngine)
    obj.engine = eng
    obj.models = []
    obj._write_verdict_lessons = lambda verdicts: 0
    return obj


def test_scheduled_engine_waits_for_settled_close_and_scores_once():
    eng = _pred_engine()
    with eng.begin() as c:
        _pred(c, "live", "SPY", TUE)
        _raw(c, "YF:SPY:close", 1, MON, 130.0, _ny(MON, 19))
        _raw(c, "YF:SPY:close", 1, TUE, 120.0, _ny(TUE, 11))
        _raw(c, "YF:SPY:adj_close", 1, TUE, 70.0, _ny(TUE, 19))
        # Present in the fixture but unavailable to a preclose scoring clock.
        _raw(c, "YF:SPY:close", 1, TUE, 101.0, _ny(TUE, 19))
    obj = _scheduled_oracle(eng)
    before = obj.score_expired_predictions(now=_ny(TUE, 12))
    assert before["pending_price"] == 1 and before["total"] == 0
    with eng.connect() as c:
        assert c.execute(text("SELECT verdict FROM oracle_predictions")).scalar() == "pending"
    after = obj.score_expired_predictions(now=_ny(TUE, 20))
    repeated = obj.score_expired_predictions(now=_ny(TUE, 21))
    assert after["hits"] == 1 and repeated["total"] == 0
    with eng.connect() as c:
        row = c.execute(text("SELECT actual_price, score_notes FROM oracle_predictions")).one()
        assert row[0] == 101.0
        assert "close 2026-11-03 TIINGO" in row[1]
        assert c.execute(text("SELECT predictions_made FROM oracle_models")).scalar() == 1


def test_scheduled_engine_weekend_and_postclose_creation_boundary():
    eng = _pred_engine()
    with eng.begin() as c:
        _pred(c, "before", "SPY", SUN)
        _pred(c, "after", "SPY", SUN, created="2026-10-31 09:00:00")
        _raw(c, "YF:SPY:close", 1, FRI, 101.0, _ny(FRI, 19))
    counts = _scheduled_oracle(eng).score_expired_predictions(now=_ny(MON, 8))
    assert counts["hits"] == 1 and counts["no_data"] == 1
    with eng.connect() as c:
        rows = {r[0]: r for r in c.execute(text("SELECT id, verdict, score_notes FROM oracle_predictions"))}
    assert rows["before"][1] == "hit"
    assert rows["after"][1:] == ("no_data", sot.NOTE_NO_SESSION)


def test_scheduled_engine_missing_close_grace_does_not_substitute_old_price():
    eng = _pred_engine()
    with eng.begin() as c:
        _pred(c, "missing", "SPY", TUE)
        _raw(c, "YF:SPY:close", 1, MON, 150.0, _ny(MON, 19))
    obj = _scheduled_oracle(eng)
    assert obj.score_expired_predictions(now=_ny(TUE, 20))["pending_price"] == 1
    assert obj.score_expired_predictions(now=_ny(TUE + timedelta(days=5), 20))["no_data"] == 1
    with eng.connect() as c:
        row = c.execute(text("SELECT verdict, actual_price, score_notes FROM oracle_predictions")).one()
    assert row == ("no_data", None, sot.NOTE_NO_PRICE)


@pytest.mark.parametrize("scorer", ["scheduled", "manual"])
def test_stale_selected_row_cannot_double_count_or_overwrite_verdict(monkeypatch, scorer):
    """Replay the stale SELECT an overlapping worker held before the winner committed."""
    from contextlib import contextmanager
    from types import SimpleNamespace

    eng = _pred_engine()
    now = _ny(TUE, 20)
    with eng.begin() as c:
        _pred(c, "race", "SPY", TUE)
        _raw(c, "YF:SPY:close", 1, TUE, 101.0, _ny(TUE, 19))
        cols = ("id, ticker, direction, target_price, entry_price, expiry, confidence, "
                "expected_move_pct, model_name, ")
        cols += "null_write_policy, created_at" if scorer == "scheduled" else "signals, created_at"
        stale = c.execute(text("SELECT " + cols + " FROM oracle_predictions")).fetchall()
    obj = _scheduled_oracle(eng)
    assert obj.score_expired_predictions(now=now)["hits"] == 1

    class StaleSelection:
        def __init__(self, conn):
            self.conn = conn
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if sql.lstrip().startswith("SELECT id,") and "FROM oracle_predictions" in sql:
                return SimpleNamespace(fetchall=lambda: stale)
            return self.conn.execute(stmt, params or {})
        def __getattr__(self, key):
            return getattr(self.conn, key)

    @contextmanager
    def stale_begin():
        with eng.begin() as conn:
            yield StaleSelection(conn)

    lessons = []
    if scorer == "scheduled":
        obj.engine = SimpleNamespace(begin=stale_begin)
        obj._write_verdict_lessons = lambda rows: lessons.extend(rows) or len(rows)
        counts = obj.score_expired_predictions(now=now)
        assert counts["total"] == 0 and counts["hits"] == 0
    else:
        monkeypatch.setattr(sot, "_record_success_lesson_safe", lambda **kw: lessons.append(kw))
        with stale_begin() as c:
            counts = sot.score_one_chunk(c, engine=eng, prices=_book("SPY", {TUE: 101.0}),
                                         today=TUE, chunk_size=10, now=now)
        assert counts["scored"] == 0 and counts["hits"] == 0
    assert lessons == []
    with eng.connect() as c:
        assert c.execute(text("SELECT predictions_made, hits FROM oracle_models")).one() == (1, 1)


def test_generation_passes_database_engine_to_calibration(monkeypatch):
    obj = _scheduled_oracle(_pred_engine())
    obj._store_predictions = lambda rows: rows
    obj._load_models = lambda: [oracle.engine.OracleModel("m", "1", "fixture", [])]
    obj._oracle_one_ticker = lambda *args, **kwargs: []
    captured = []
    monkeypatch.setattr("oracle.hallucination_guard.verify_predictions", lambda rows, **kw:
                        (captured.append(kw["calibration_report"]) or rows, []))
    assert obj.generate_predictions(["SPY"]) == []
    assert captured[0] is not None
    assert captured[0].label == "insufficient_data"


def test_decision_timestamp_is_after_guard_inputs_and_only_committed_rows_return(monkeypatch):
    from dataclasses import replace
    from types import SimpleNamespace

    oe = oracle.engine
    before = datetime(2026, 11, 10, 20, 59, tzinfo=UTC)
    after = datetime(2026, 11, 10, 21, 1, tzinfo=UTC)
    clock = {"now": before}

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"]

    model = oe.OracleModel("fixture", "1", "fixture", [])
    prediction = oe.OraclePrediction(
        id="candidate", timestamp=before, ticker="SPY",
        prediction_type=oe.PredictionType.DIRECTION, direction="CALL",
        target_price=110, current_price=100, expiry=date(2026, 11, 13),
        confidence=0.6, expected_move_pct=10, model_name="fixture",
    )
    obj = oe.OracleEngine.__new__(oe.OracleEngine)
    obj.engine = object()
    obj._load_models = lambda: [model]
    obj._oracle_one_ticker = lambda *a, **kw: [prediction]
    captured = []

    def guard(rows, **kwargs):
        clock["now"] = after
        return rows, []

    def store(rows):
        captured.extend(rows)
        return [replace(rows[0], id="committed")]

    obj._store_predictions = store
    monkeypatch.setenv("GRID_ORACLE_PARALLELISM", "1")
    monkeypatch.setattr(oe, "datetime", Clock)
    monkeypatch.setattr("oracle.calibration.compute_calibration", lambda engine: SimpleNamespace())
    monkeypatch.setattr("oracle.hallucination_guard.verify_predictions", guard)
    result = obj.generate_predictions(["SPY"])
    assert captured[0].timestamp == after
    assert captured[0].timestamp > sot.session_close_utc("SPY", before.date())
    assert [p.id for p in result] == ["committed"]


def test_hourly_verdict_lessons_are_queued_without_writing_before_commit(monkeypatch):
    eng = _pred_engine()
    with eng.begin() as conn:
        _pred(conn, "rollback", "SPY", TUE)
    emitted, queued = [], []
    monkeypatch.setattr(sot, "_record_success_lesson_safe", lambda **kw: emitted.append(kw))
    with pytest.raises(RuntimeError, match="force rollback"):
        with eng.begin() as conn:
            result = sot.score_one_chunk(
                conn, engine=eng, prices=_book("SPY", {TUE: 101}), today=TUE,
                chunk_size=10, now=_ny(TUE, 20), verdicts_after_commit=queued,
            )
            assert result["hits"] == 1 and len(queued) == 1
            assert emitted == []
            raise RuntimeError("force rollback")
    assert emitted == []
    with eng.connect() as conn:
        assert conn.execute(text("SELECT verdict FROM oracle_predictions WHERE id='rollback'")).scalar() == "pending"


@pytest.mark.parametrize("competitor", ["scheduled", "hourly"])
def test_postgres_competing_scorers_count_and_emit_once(pg_engine, monkeypatch, competitor):
    """Two real PG transactions select pending before either updates it."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from uuid import uuid4

    from sqlalchemy.schema import CreateSchema, DropSchema

    if not (pg_engine.url.database or "").endswith("_test"):
        pytest.skip("Scorer concurrency proof requires a disposable _test database")
    schema = "oracle_race_" + uuid4().hex
    with pg_engine.begin() as c:
        assert c.execute(text("SELECT current_database()")).scalar_one().endswith("_test")
        c.execute(CreateSchema(schema))
    eng = create_engine(pg_engine.url, connect_args={
        "options": "-csearch_path=" + schema + " -cstatement_timeout=10000 -clock_timeout=5000",
    })
    try:
        with eng.begin() as c:
            c.execute(text(_PRED_DDL.replace("DEFAULT 1", "DEFAULT TRUE")))
            c.execute(text(
                "CREATE TABLE oracle_models (name TEXT PRIMARY KEY, predictions_made INTEGER DEFAULT 0, "
                "hits INTEGER DEFAULT 0, misses INTEGER DEFAULT 0, partials INTEGER DEFAULT 0, "
                "cumulative_pnl DOUBLE PRECISION DEFAULT 0, last_updated TIMESTAMPTZ)"
            ))
            c.execute(text("INSERT INTO oracle_models (name) VALUES ('m')"))
            _pred(c, "race", "SPY", TUE, keep=True)
        book = _book("SPY", {TUE: 101.0})
        monkeypatch.setattr(sot, "load_expiry_closes", lambda *a, **kw: book)
        monkeypatch.setattr(sot, "load_split_book", lambda *a, **kw: {})
        lessons = []
        monkeypatch.setattr(sot, "_record_success_lesson_safe", lambda **kw: lessons.append(kw))
        barrier = Barrier(2, timeout=5)

        @event.listens_for(eng, "after_cursor_execute")
        def after_select(conn, cursor, statement, parameters, context, executemany):
            if statement.lstrip().startswith("SELECT id,") and "FROM oracle_predictions" in statement:
                barrier.wait()

        def run(which):
            if which == "hourly":
                queued = []
                with eng.begin() as c:
                    result = sot.score_one_chunk(c, engine=eng, prices=book, today=TUE,
                                                 chunk_size=10, now=_ny(TUE, 20),
                                                 verdicts_after_commit=queued)["scored"]
                lessons.extend(queued)
                return result
            obj = _scheduled_oracle(eng)
            obj._write_verdict_lessons = lambda rows: lessons.extend(rows) or len(rows)
            return obj.score_expired_predictions(now=_ny(TUE, 20))["total"]

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(run, "scheduled"), pool.submit(run, competitor)]
            assert sorted(f.result(timeout=15) for f in futures) == [0, 1]
        assert len(lessons) == 1
        with eng.connect() as c:
            assert c.execute(text("SELECT predictions_made, hits FROM oracle_models")).one() == (1, 1)
            assert c.execute(text("SELECT actual_price, verdict FROM oracle_predictions")).one() == (101.0, "hit")
    finally:
        eng.dispose()
        with pg_engine.begin() as c:
            c.execute(DropSchema(schema, cascade=True))


# ── hindsight rescore ───────────────────────────────────────────────────────

def _seed_rescore(eng) -> None:
    with eng.begin() as c:
        bug = sot.NOTE_NO_PRICE
        _pred(c, "r-hit", "SPY", SAT, verdict="no_data", notes=bug, entry=750.0, expected=0.5)
        _pred(c, "r-miss", "SPY", TUE, verdict="no_data", notes=bug, entry=760.0)
        _pred(c, "r-dup", "SPY", TUE, verdict="no_data", notes=bug, entry=700.0, keep=0)
        _pred(c, "r-none", "ZZZ", TUE, verdict="no_data", notes=bug)              # no closes
        _pred(c, "r-neutral", "SPY", TUE, verdict="no_data", direction="NEUTRAL",
              notes="NEUTRAL direction is unscorable")
        _pred(c, "r-official", "SPY", MON, verdict="hit", entry=740.0)
        _raw(c, "YF:SPY:close", 1, FRI, 755.72, _ny(FRI, 19, 30))
        _raw(c, "YF:SPY:close", 1, MON, 752.10, _ny(MON, 19, 30))
        _raw(c, "YF:SPY:close", 1, TUE, 754.35, _ny(TUE, 19, 30))


def _snapshot(eng) -> list[tuple]:
    with eng.connect() as c:
        return [tuple(r) for r in c.execute(text("SELECT * FROM oracle_predictions ORDER BY id"))]


RUN = datetime(2026, 11, 20, tzinfo=UTC)


def test_rescore_dry_run_projects_and_writes_nothing():
    eng = _pred_engine()
    _seed_rescore(eng)
    before = _snapshot(eng)
    proj = sot.rescore_no_data(eng, apply=False, now=RUN)
    assert proj["mode"] == "dry_run" and proj["rows_appended"] == 0
    assert proj["tally"]["dedup_keep"] == {"candidates": 3, "hit": 1, "miss": 1, "still_no_price": 1}
    assert proj["tally"]["other"] == {"candidates": 1, "hit": 1}
    assert proj["official_scored_expiry_dates"] == 1
    assert proj["rescored_expiry_dates"] == 2
    assert proj["expiry_dates_scored_combined"] == 3
    assert proj["rows_to_append"] == 3
    assert _snapshot(eng) == before
    with eng.connect() as c:
        assert not sot._rescore_table_exists(c)


def test_rescore_apply_is_additive_idempotent_and_marked_hindsight():
    eng = _pred_engine()
    _seed_rescore(eng)
    before = _snapshot(eng)
    first = sot.rescore_no_data(eng, apply=True, now=RUN)
    second = sot.rescore_no_data(eng, apply=True, now=RUN)
    assert first["rows_appended"] == 3
    assert second["rows_appended"] == 0 and second["rows_to_append"] == 0
    assert second["tally"]["dedup_keep"]["already_rescored"] == 2
    assert _snapshot(eng) == before, "oracle_predictions must never be updated by a rescore"
    with eng.connect() as c:
        rows = c.execute(text(
            "SELECT prediction_id, verdict, score_class, official, scoring_version, "
            "price_session_date, actual_price, expiry_rule FROM oracle_prediction_rescores "
            "ORDER BY prediction_id"
        )).fetchall()
        models = c.execute(text("SELECT predictions_made FROM oracle_models")).scalar()
    assert [r[0] for r in rows] == ["r-dup", "r-hit", "r-miss"]
    by_id = {r[0]: r for r in rows}
    assert by_id["r-hit"][1] == "hit" and by_id["r-hit"][6] == 755.72   # Saturday -> Friday close
    assert str(by_id["r-hit"][5]) == "2026-10-30"
    assert by_id["r-miss"][1] == "miss"
    for r in rows:
        assert r[2] == "exploratory_hindsight" and not r[3]
        assert r[4] == sot.RESCORE_SCORING_VERSION and r[7] == sot.EXPIRY_CLOSE_RULE
    assert models == 0, "a rescore must never touch oracle_models"


def test_rescore_table_refuses_an_official_row():
    eng = _pred_engine()
    with eng.begin() as c:
        c.execute(text(sot._RESCORE_DDL))
    row = {
        "prediction_id": "x", "scoring_version": "v", "prior_verdict": "no_data",
        "prior_score_notes": None, "verdict": "hit", "entry_price": 1.0, "actual_price": 2.0,
        "actual_move_pct": 100.0, "pnl_pct": 100.0, "price_session_date": FRI,
        "price_source": "TIINGO", "price_pull_timestamp": None, "expiry_rule": "r",
        "notes": None, "rescored_at": RUN,
    }
    official = sot._RESCORE_INSERT.replace(":score_class, FALSE", ":score_class, TRUE")
    with pytest.raises(IntegrityError), eng.begin() as c:
        c.execute(text(official), {**row, "score_class": sot.RESCORE_SCORE_CLASS})
    with pytest.raises(IntegrityError), eng.begin() as c:
        c.execute(text(sot._RESCORE_INSERT), {**row, "score_class": "official"})


def test_e2_scoreboard_never_reads_rescored_rows():
    hits = [p for p in (REPO_ROOT / "evals").rglob("*")
            if p.is_file() and sot.RESCORE_TABLE in p.read_text(encoding="utf-8", errors="ignore")]
    assert hits == []


def test_apply_requires_rescore_mode():
    with pytest.raises(SystemExit):
        sot._parse_args(["--apply"])
