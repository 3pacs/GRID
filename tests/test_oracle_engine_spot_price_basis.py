"""Oracle entries use fresh raw spot quotes. Expiry scoring uses settled raw closes."""
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, text

from oracle.engine import OracleEngine
from scripts.score_oracle_trades import classify_outcome, close_series_id


DECISION = datetime(2026, 11, 10, 16, 0, tzinfo=timezone.utc)


@pytest.fixture
def oracle(monkeypatch):
    db = create_engine("sqlite://")
    with db.begin() as conn:
        conn.execute(text("""CREATE TABLE options_capture_batches (
            ticker TEXT, spot_price REAL, provider_regular_market_at TEXT, spot_provider TEXT,
            capture_batch_id TEXT, capture_source TEXT, capture_started_at TEXT,
            capture_completed_at TEXT, registered_at TEXT, snap_date TEXT,
            backfilled BOOLEAN, capture_ordinal INTEGER)"""))
        # Populate the retired sources with plausible but different prices so a
        # reintroduced fallback is distinguishable from a measured spot.
        conn.execute(text("""CREATE TABLE options_daily_signals (
            ticker TEXT, signal_date TEXT, spot_price REAL)"""))
        conn.execute(text("INSERT INTO options_daily_signals VALUES ('AAPL','2026-11-09',176)"))
        conn.execute(text("""CREATE TABLE raw_series (
            series_id TEXT, obs_date TEXT, pull_status TEXT, value REAL)"""))
        conn.execute(text("""INSERT INTO raw_series VALUES
            ('YF:AAPL:close','2026-11-09','SUCCESS',180),
            ('YF:AAPL:adj_close','2026-11-09','SUCCESS',174)"""))
    clock = Mock(wraps=datetime)
    clock.now.return_value = DECISION
    monkeypatch.setattr("oracle.engine.datetime", clock)
    # Only exercise the price reader. The rest of the engine initializes the
    # model registry and PostgreSQL schema and has its own integration tests.
    engine = object.__new__(OracleEngine)
    engine.engine = db
    try:
        yield engine
    finally:
        db.dispose()


def _insert_capture(engine, quote_time):
    with engine.engine.begin() as conn:
        conn.execute(text("""INSERT INTO options_capture_batches VALUES
            ('AAPL',182,:quote,'cboe','capture-1','oracle_forward',:started,
             :completed,:registered,'2026-11-10',FALSE,1)"""),
            {"quote": quote_time, "started": DECISION - timedelta(minutes=2),
             "completed": DECISION - timedelta(minutes=1),
             "registered": DECISION - timedelta(seconds=30)})


def test_entry_uses_dated_raw_spot_and_exit_uses_raw_close(oracle):
    _insert_capture(oracle, DECISION - timedelta(minutes=3))
    receipt = oracle._get_entry_price_reference("AAPL")
    assert (receipt["status"], receipt["basis"], receipt["provider"]) == ("measured", "raw_spot", "cboe")
    entry = oracle._get_spot_price("AAPL")
    assert entry == 182.0

    # The two prices measure different instants. Choosing the adjusted close
    # here would change both the sign of the return and the grade.
    expiry_prices = {"YF:AAPL:close": 184, "YF:AAPL:adj_close": 178}
    verdict, move, _ = classify_outcome("CALL", entry, expiry_prices[close_series_id("AAPL")], 1)
    assert verdict == "hit"
    assert move == pytest.approx((184 / 182 - 1) * 100)


def test_daily_prices_cannot_supply_a_missing_entry_quote(oracle):
    receipt = oracle._get_entry_price_reference("AAPL")
    assert receipt["status"] == "unavailable"
    assert "no timestamped options quote" in receipt["reason"]
    assert oracle._get_spot_price("AAPL") is None


@pytest.mark.parametrize(("quote_time", "reason"), [
    (None, "missing or unzoned"),
    (DECISION - timedelta(minutes=21), "older than the 20-minute reference limit"),
])
def test_capture_with_missing_or_stale_provider_time_abstains(oracle, quote_time, reason):
    _insert_capture(oracle, quote_time)
    receipt = oracle._get_entry_price_reference("AAPL")
    assert receipt["status"] == "unavailable"
    assert reason in receipt["reason"]
    assert oracle._get_spot_price("AAPL") is None
