"""Opt-in PG14 regressions for the canonical GEM options entrypoint."""
from __future__ import annotations

import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from ingestion import options
from physics.dealer_gamma import DealerGammaEngine
from scripts import pull_options_gem_tickers as gem
from tests.test_options_capture_pg14_scratch import _puller, _yahoo

pytest_plugins = ("tests.test_options_capture_pg14_scratch",)


def _fast_calculations(monkeypatch):
    monkeypatch.setattr(options.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(options, "compute_max_pain", lambda *_args: 100.0)
    monkeypatch.setattr(options, "compute_iv_skew", lambda *_args: 0.0)
    monkeypatch.setattr(options, "_compute_atm_iv", lambda *_args: 0.2)
    monkeypatch.setattr(options, "_compute_wing_iv", lambda *_args, **_kwargs: 0.2)
    monkeypatch.setattr(options, "_compute_oi_concentration", lambda *_args: 0.5)


def test_gem_wrapper_real_batch_writer_nine_tickers_six_expiries(scratch_pg14, monkeypatch):
    """Use the real wrapper and writer, but a local deterministic provider."""
    engine = scratch_pg14
    _fast_calculations(monkeypatch)
    monkeypatch.setitem(sys.modules, "db", SimpleNamespace(get_engine=lambda: engine))

    class BoundPuller(options.OptionsPuller):
        def __init__(self, db_engine):
            # The opt-in fixture owns only options tables, not source_catalog.
            self.engine = db_engine
            self._push_to_resolved = lambda *_args: None

    monkeypatch.setattr(options, "OptionsPuller", BoundPuller)
    calls = defaultdict(list)
    start = datetime.now(timezone.utc)
    expirations = [int((start + timedelta(days=10 + n)).timestamp()) for n in range(8)]

    class FakeYahoo:
        is_available = True

        def get_options(self, ticker, expiry_ts=None):
            calls[ticker].append(expiry_ts)
            chain = [{"strike": 100.0, "volume": 3, "openInterest": 10,
                      "impliedVolatility": 0.2, "lastPrice": 2.0,
                      "bid": 1.0, "ask": 3.0, "inTheMoney": False}]
            return {"quote": {"regularMarketPrice": 100.0},
                    "expirations": expirations, "calls": chain, "puts": chain}

    monkeypatch.setattr(options, "YahooOptionsClient", FakeYahoo)
    monkeypatch.setattr(options.requests.Session, "get",
                        lambda *_args, **_kwargs: pytest.fail("network provider call attempted"))

    assert gem.main() == 0
    expected = list(gem.GEM_TICKERS)
    assert len(expected) == 9
    assert list(calls) == expected
    assert all(values == [None, *expirations[1:6]] for values in calls.values())
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT ticker, COUNT(*), COUNT(DISTINCT expiry),
                   COUNT(DISTINCT capture_batch_id), COUNT(DISTINCT capture_ordinal),
                   BOOL_AND(capture_batch_id IS NOT NULL AND capture_ordinal IS NOT NULL
                            AND capture_started_at IS NOT NULL AND capture_completed_at IS NOT NULL)
              FROM options_snapshots GROUP BY ticker ORDER BY ticker
        """)).fetchall()
        signals = conn.execute(text("SELECT COUNT(*) FROM options_daily_signals")).scalar_one()
        snap_date = conn.execute(text(
            "SELECT DISTINCT snap_date FROM options_snapshots WHERE ticker = 'SPY'"
        )).scalar_one()
    assert {row[0] for row in rows} == set(expected)
    assert all((row[1], row[2], row[3], row[4], row[5]) == (12, 6, 1, 1, True)
               for row in rows)
    assert signals == 9
    assert not DealerGammaEngine(engine)._load_chain(
        "SPY", snap_date,
    ).empty


def test_legacy_style_upsert_retains_provenance_until_canonical_replacement(scratch_pg14, monkeypatch):
    """Demonstrate the SQL failure mode without importing the installed legacy artifact."""
    engine = scratch_pg14
    _fast_calculations(monkeypatch)
    now = datetime.now(timezone.utc)
    day = now.date()
    expirations = [int((now + timedelta(days=n)).timestamp()) for n in (10, 20)]

    assert _puller(engine, _yahoo(expirations, [100.0]))._pull_ticker(
        "SPY", day.isoformat(),
    )["status"] == "SUCCESS"
    with engine.connect() as conn:
        original = conn.execute(text("""
            SELECT expiry, capture_batch_id, capture_ordinal,
                   capture_started_at, capture_completed_at, last_price, open_interest
              FROM options_snapshots
             WHERE ticker='SPY' AND snap_date=:day AND opt_type='call' AND strike=100
             ORDER BY expiry LIMIT 1
        """), {"day": day}).one()

    # Synthetic legacy SQL updates a captured row's measurements while
    # omitting every capture column from the conflict update. No legacy script
    # or credentials are imported or run.
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO options_snapshots
                (ticker, snap_date, expiry, opt_type, strike, last_price, open_interest)
            VALUES ('SPY', :day, :expiry, 'call', 100, 99, 777)
            ON CONFLICT (ticker, snap_date, expiry, opt_type, strike)
            DO UPDATE SET last_price=EXCLUDED.last_price,
                          open_interest=EXCLUDED.open_interest
        """), {"day": day, "expiry": original[0]})
    with engine.connect() as conn:
        changed = conn.execute(text("""
            SELECT expiry, capture_batch_id, capture_ordinal,
                   capture_started_at, capture_completed_at, last_price, open_interest
              FROM options_snapshots
             WHERE ticker='SPY' AND snap_date=:day AND opt_type='call' AND strike=100
             ORDER BY expiry LIMIT 1
        """), {"day": day}).one()
    assert changed[:5] == original[:5]
    assert (changed[5], changed[6]) == (99, 777)
    assert not DealerGammaEngine(engine)._load_chain("SPY", day).empty

    assert _puller(engine, _yahoo(expirations, [120.0]))._pull_ticker(
        "SPY", day.isoformat(),
    )["status"] == "SUCCESS"
    with engine.connect() as conn:
        final = conn.execute(text("""
            SELECT strike, capture_batch_id, capture_ordinal, last_price,
                   capture_started_at, capture_completed_at
              FROM options_snapshots WHERE ticker='SPY' AND snap_date=:day
        """), {"day": day}).fetchall()
    assert len(final) == 4
    assert {row[0] for row in final} == {120.0}
    assert len({row[1] for row in final}) == 1
    assert final[0][1] != original[1]
    assert all(row[2] > original[2] and row[3] == 2.0
               and row[4] is not None and row[5] is not None for row in final)
    assert not DealerGammaEngine(engine)._load_chain("SPY", day).empty
