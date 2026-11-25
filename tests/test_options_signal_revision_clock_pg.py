"""options_daily_signals write clock vs the thesis scorer (disposable PostgreSQL).

Codex HOLDs on #1382:
- 6ab82ac7: a same-key revision kept a pre-clock created_at.
- a5bfc4ac: excluding a late revision is not replay (the scorer fell back to
  the prior session's 1.25 and could not recover the original 0.45), and
  NOW() is the writing transaction's start, so a write that straddles a
  cutoff carries a created_at from before its commit.

So created_at is a last-write lower bound, the scorer scores live only, a
session row written after its clock is unavailable (not replaced), and an
explicit decision clock is refused. These tests run the real upsert
(ingestion.options._DAILY_SIGNALS_UPSERT_SQL) and the real scorer against
PostgreSQL. Skipped without GRID_TEST_DB_URL (or DB_URL).
"""
from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

_DB_URL = os.environ.get("GRID_TEST_DB_URL") or os.environ.get("DB_URL")
pytestmark = pytest.mark.skipif(
    not _DB_URL, reason="requires disposable PostgreSQL via GRID_TEST_DB_URL or DB_URL"
)

_DDL = """
CREATE TABLE options_daily_signals (
    id                      BIGSERIAL PRIMARY KEY,
    ticker                  TEXT NOT NULL,
    signal_date             DATE NOT NULL,
    put_call_ratio          DOUBLE PRECISION,
    max_pain                DOUBLE PRECISION,
    iv_skew                 DOUBLE PRECISION,
    total_oi                BIGINT,
    total_volume            BIGINT,
    near_expiry             DATE,
    spot_price              DOUBLE PRECISION,
    iv_atm                  DOUBLE PRECISION,
    iv_25d_put              DOUBLE PRECISION,
    iv_25d_call             DOUBLE PRECISION,
    iv_m90_put_v1           DOUBLE PRECISION,
    iv_m110_call_v1         DOUBLE PRECISION,
    wing_iv_metadata        JSONB,
    term_structure_slope    DOUBLE PRECISION,
    oi_concentration        DOUBLE PRECISION,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker, signal_date)
)
"""


_SCHEMA = "rc_options_revision_clock_test"
_SCHEMA_LOCK_KEY = 1382_2026_1122


@pytest.fixture
def engine():
    # A connection option, not a SET in a connect hook: that SET runs in an
    # implicit transaction, and the first rollback quietly reverts it.
    eng = create_engine(_DB_URL, connect_args={"options": "-csearch_path=rc_options_revision_clock_test"})
    # One fixed schema, so tests on parallel workers (pytest -n) take turns: a
    # session-level advisory lock, held on its own connection, survives the
    # tests' commits and rollbacks.
    lock = eng.connect()
    lock.execute(text("SELECT pg_advisory_lock(:k)"), {"k": _SCHEMA_LOCK_KEY})
    try:
        with eng.begin() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS rc_options_revision_clock_test CASCADE"))
            conn.execute(text("CREATE SCHEMA rc_options_revision_clock_test"))
            conn.execute(text(_DDL))
        yield eng
        with eng.begin() as conn:
            conn.execute(text("DROP SCHEMA rc_options_revision_clock_test CASCADE"))
    finally:
        lock.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _SCHEMA_LOCK_KEY})
        lock.close()
        eng.dispose()


def _params(sd: date, pcr: float, spot: float = 600.0) -> dict:
    from ingestion.options import CALL_WING, PUT_WING

    return {
        "ticker": "SPY", "sd": sd.isoformat(), "pcr": pcr, "mp": 598.0, "ivs": 0.05,
        "oi": 1_000_000, "vol": 500_000, "ne": (sd + timedelta(days=1)).isoformat(),
        "spot": spot, "iv_atm": 0.16, PUT_WING: 0.2, CALL_WING: 0.14,
        "wing_meta": json.dumps({"source": "cboe"}), "ts_slope": 0.01, "oi_conc": 0.03,
    }


def _upsert(engine, params: dict) -> None:
    from ingestion.options import _DAILY_SIGNALS_UPSERT_SQL

    with engine.begin() as conn:
        conn.execute(_DAILY_SIGNALS_UPSERT_SQL, params)


def _created_at(engine, sd: date) -> datetime:
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT created_at FROM options_daily_signals WHERE ticker = 'SPY' AND signal_date = :sd"),
            {"sd": sd},
        ).scalar_one()


def _set_created_at(engine, sd: date, ts: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE options_daily_signals SET created_at = :ts WHERE ticker = 'SPY' AND signal_date = :sd"),
            {"ts": ts, "sd": sd},
        )


def test_identical_rewrite_keeps_the_clock_and_a_changed_value_moves_it(engine):
    today = datetime.now(timezone.utc).date()
    _upsert(engine, _params(today, 0.45))
    first_capture = datetime.now(timezone.utc) - timedelta(hours=3)
    _set_created_at(engine, today, first_capture)

    _upsert(engine, _params(today, 0.45))  # same values: not a revision
    assert _created_at(engine, today) == first_capture

    before = datetime.now(timezone.utc)
    _upsert(engine, _params(today, 0.40))  # PCR revised in place
    revised_at = _created_at(engine, today)
    assert revised_at >= before - timedelta(seconds=1)


def _prior_weekday(day: date) -> date:
    prior = day - timedelta(days=1)
    while prior.weekday() >= 5:
        prior -= timedelta(days=1)
    return prior


def _mentions(text: str, value: str) -> bool:
    """True when ``value`` appears in ``text`` as a number of its own.

    A bare ``value in text`` also matches inside a longer number, and the
    verdict text carries ISO timestamps: a decision clock of ``...:50.458010``
    contains ``0.45``, so the test failed on about one run in a few hundred
    (GRID#1440) although the 0.45 PCR was not leaked."""
    return re.search(rf"(?<![\d.]){re.escape(value)}(?!\d)", text) is not None


def test_mentions_matches_a_value_but_not_a_slice_of_a_timestamp():
    assert _mentions("PCR 0.45 as of the clock", "0.45")
    assert _mentions("value=0.45, next", "0.45")
    assert _mentions("0.45", "0.45")
    # the failing text from CI: seconds ending in 0, fraction starting 45
    stamp = "decision clock 2026-11-25T03:12:50.458010+00:00"
    assert not _mentions(stamp, "0.45")
    assert not _mentions("2026-11-25T06:25:41.250000+00:00", "1.25")
    assert not _mentions("2026-11-25T06:25:40.400000+00:00", "0.40")
    assert not _mentions("a 10.45 reading and 0.451", "0.45")


def _freeze_scorer_clock(monkeypatch, ts, decision: datetime) -> None:
    class _FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return decision if tz is not None else decision.replace(tzinfo=None)

    class _FakeDate(date):
        @classmethod
        def today(cls):
            return decision.date()

    monkeypatch.setattr(ts, "datetime", _FakeDatetime)
    monkeypatch.setattr(ts, "date", _FakeDate)


def test_a_value_revised_after_the_clock_is_unavailable_not_replaced(engine, monkeypatch):
    import analysis.thesis_scorer as ts

    now = datetime.now(timezone.utc)
    today = now.date()
    prior = _prior_weekday(today)

    _upsert(engine, _params(prior, 1.25))
    _set_created_at(engine, prior, datetime.combine(prior, datetime.min.time(), timezone.utc) + timedelta(hours=21))
    # Every clock stays on today's UTC date: a decision clock that crossed
    # midnight would make today's row future-dated and the test would pass
    # or fail with the time of day (it failed at 00:10Z on 11-23).
    midnight = datetime.combine(today, datetime.min.time(), timezone.utc)
    if now - midnight < timedelta(seconds=4):
        pytest.skip("too close to 00:00 UTC to place a same-day decision clock")
    first_capture = midnight + (now - midnight) / 4
    decision = midnight + (now - midnight) / 2  # after the first capture, before the revision

    _upsert(engine, _params(today, 0.45))
    _set_created_at(engine, today, first_capture)  # captured before the clock
    _upsert(engine, _params(today, 0.40))  # revised in place now, after the clock
    _freeze_scorer_clock(monkeypatch, ts, decision)
    monkeypatch.setattr(ts, "VISIBLE_DAILY_SIGNALS", "options_daily_signals")

    verdict = ts._score_dealer_gamma(engine, 0.5)
    # The 0.45 seen at the clock is gone; the prior session's 1.25 is not a
    # substitute for it. The score is unavailable, and says why.
    assert verdict["status"] == "no_data", verdict
    assert verdict["evidence_availability"] == ts.OPTIONS_WRITTEN_AFTER_CLOCK
    # The text states the clock's timestamps, so match each PCR as a whole
    # number: a timestamp such as 03:12:50.458010 contains "0.45" (see _mentions).
    for value in ("1.25", "0.40", "0.45"):
        assert not _mentions(verdict["data_point"], value), (value, verdict["data_point"])
        assert not _mentions(verdict["reasoning"], value), (value, verdict["reasoning"])


def test_a_writer_transaction_straddling_a_live_read_is_invisible_until_commit(engine, monkeypatch):
    """begin < read < commit: the live read sees only committed rows, and the
    committed row's created_at (NOW() = transaction start) predates that read.
    That is why a created_at-fenced replay is refused instead of answered."""
    import analysis.thesis_scorer as ts

    monkeypatch.setattr(ts, "VISIBLE_DAILY_SIGNALS", "options_daily_signals")
    today = datetime.now(timezone.utc).date()
    _upsert(engine, _params(today, 0.45))
    _set_created_at(engine, today, datetime.now(timezone.utc) - timedelta(hours=3))

    writer = engine.connect()
    tx = writer.begin()
    try:
        writer.execute(text("SELECT 1"))  # the transaction (and its NOW()) starts here
        from ingestion.options import _DAILY_SIGNALS_UPSERT_SQL

        writer.execute(_DAILY_SIGNALS_UPSERT_SQL, _params(today, 0.40))

        during = ts._score_dealer_gamma(engine, 0.5)  # live read while the write is open
        read_clock = datetime.now(timezone.utc)
        assert during["status"] == "active", during
        assert "0.45" in during["data_point"]
        assert during["evidence_basis"] == "same_session"
        tx.commit()
    finally:
        writer.close()

    revised_at = _created_at(engine, today)
    assert revised_at < read_clock  # stamped before the read that could not see it

    # A created_at fence at read_clock would now return the value that read
    # could not see: the lookahead a replay would commit.
    with engine.connect() as conn:
        fenced = conn.execute(
            text(
                "SELECT put_call_ratio FROM options_daily_signals "
                "WHERE ticker = 'SPY' AND signal_date <= :d AND created_at <= :c "
                "ORDER BY signal_date DESC LIMIT 1"
            ),
            {"d": today, "c": read_clock},
        ).scalar_one()
    assert fenced == pytest.approx(0.40)

    replay = ts._score_dealer_gamma(engine, 0.5, as_of_ts=read_clock)
    assert replay["status"] == "no_data"
    assert replay["evidence_availability"] == ts.OPTIONS_REPLAY_UNAVAILABLE

    after = ts._score_dealer_gamma(engine, 0.5)  # live, after the commit
    assert "0.40" in after["data_point"]
