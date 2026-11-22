"""Same-key revision clock for options_daily_signals (disposable PostgreSQL).

Codex HOLD on #1382 (6ab82ac7): the daily-signals upsert replaced PCR /
max-pain / spot on the same (ticker, signal_date) key without moving
created_at, so a value revised after a decision clock kept a pre-clock
timestamp and passed the thesis scorer's as-of fence.

These tests run the real upsert (ingestion.options._DAILY_SIGNALS_UPSERT_SQL)
and the real scorer query (analysis.thesis_scorer._score_dealer_gamma)
against PostgreSQL. Skipped without GRID_TEST_DB_URL (or DB_URL).
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, event, text

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


@pytest.fixture
def engine():
    eng = create_engine(_DB_URL)

    @event.listens_for(eng, "connect")
    def _search_path(dbapi_conn, _record):  # pragma: no cover - driver hook
        cur = dbapi_conn.cursor()
        cur.execute("SET search_path TO rc_options_revision_clock_test")
        cur.close()

    with eng.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS rc_options_revision_clock_test CASCADE"))
        conn.execute(text("CREATE SCHEMA rc_options_revision_clock_test"))
        conn.execute(text(_DDL))
    yield eng
    with eng.begin() as conn:
        conn.execute(text("DROP SCHEMA rc_options_revision_clock_test CASCADE"))
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


def test_value_revised_after_the_clock_falls_back_to_the_earlier_eligible_row(engine, monkeypatch):
    import analysis.thesis_scorer as ts

    now = datetime.now(timezone.utc)
    today = now.date()
    prior = today - timedelta(days=1)
    while prior.weekday() >= 5:
        prior -= timedelta(days=1)

    _upsert(engine, _params(prior, 1.25))
    _set_created_at(engine, prior, datetime.combine(prior, datetime.min.time(), timezone.utc) + timedelta(hours=21))
    _upsert(engine, _params(today, 0.45))
    _set_created_at(engine, today, now - timedelta(hours=3))  # captured before the clock
    _upsert(engine, _params(today, 0.40))  # revised in place now, after the clock below

    decision = now - timedelta(hours=1)  # after first capture, before the revision

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
    monkeypatch.setattr(ts, "VISIBLE_DAILY_SIGNALS", "options_daily_signals")

    verdict = ts._score_dealer_gamma(engine, 0.5)
    # Before the fix, the revised 0.40 kept its pre-clock created_at and was
    # scored. Now the revision is invisible at the decision clock, and the
    # earlier eligible row (prior session, PCR 1.25) is used instead.
    assert verdict["status"] in ("active", "stale"), verdict
    assert "1.25" in verdict["reasoning"], verdict["reasoning"]
    assert "0.40" not in verdict["reasoning"] and "0.45" not in verdict["reasoning"]
