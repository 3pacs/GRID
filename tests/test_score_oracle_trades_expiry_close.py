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
