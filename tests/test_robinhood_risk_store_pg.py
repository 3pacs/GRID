"""Postgres contract tests for trading/robinhood_risk_store.py::PostgresRiskStore.

Requires a live database — uses the ``pg_engine`` fixture (tests/conftest.py),
which skips the whole file when one isn't reachable (set GRID_TEST_DB_URL to
point at one). CI wires this file into its own step with GRID_TEST_DB_URL
pointed at the griddb_test service (see .github/workflows/test.yml), the same
pattern every other ``*_pg.py`` contract file in this suite already uses.

Every test uses a venue name prefixed "test-" and unique to itself, and the
`store` fixture deletes every "test-%" row on teardown, so this is safe to
run repeatedly against a shared database without colliding with real
"robinhood" rows or with itself.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from trading.robinhood_risk_store import PostgresRiskStore, utc_today


@pytest.fixture
def store(pg_engine):
    s = PostgresRiskStore(pg_engine)
    s._ensure_tables()
    yield s
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM trading_order_log WHERE venue LIKE :pattern"), {"pattern": "test-%"})
        conn.execute(text("DELETE FROM trading_risk_state WHERE venue LIKE :pattern"), {"pattern": "test-%"})


def _venue(name: str) -> str:
    """A venue name unique to one test — trading_risk_state's primary key IS
    the venue, so two tests (or two runs) sharing a bare "robinhood" would
    collide. Every venue here starts with "test-" so the fixture's teardown
    can find and remove it without touching real data."""
    return f"test-{name}"


class TestPersistedPeakSurvivesANewStore:
    def test_peak_survives_a_second_store_instance_same_engine(self, pg_engine, store):
        """The scenario get_robinhood_trader() actually hits: a fresh
        RobinhoodCryptoTrader (and a fresh PostgresRiskStore inside it) is
        built on every call, so the peak has to live in the database, not on
        either Python object, to survive that."""
        venue = _venue("peak-survives")
        store.touch(venue, 1000.0, utc_today())

        store2 = PostgresRiskStore(pg_engine)
        state = store2.touch(venue, 100.0, utc_today())
        assert state.peak_equity == pytest.approx(1000.0)
        assert state.venue == venue


class TestTouchUpsert:
    def test_creates_a_row_on_first_touch(self, store):
        venue = _venue("create")
        today = utc_today()
        state = store.touch(venue, 500.0, today)
        assert state.peak_equity == pytest.approx(500.0)
        assert state.day_start_equity == pytest.approx(500.0)
        assert state.day_start_date == today
        assert state.orders_today == 0

    def test_peak_never_decreases(self, store):
        venue = _venue("peak-monotonic")
        today = utc_today()
        store.touch(venue, 1000.0, today)
        state = store.touch(venue, 200.0, today)
        assert state.peak_equity == pytest.approx(1000.0)

    def test_day_rollover_resets_day_start_and_orders_not_peak(self, store):
        venue = _venue("rollover")
        yesterday = utc_today() - timedelta(days=1)
        store.touch(venue, 1000.0, yesterday, increment_order=True)
        today = utc_today()
        state = store.touch(venue, 800.0, today)
        assert state.peak_equity == pytest.approx(1000.0)
        assert state.day_start_equity == pytest.approx(800.0)
        assert state.orders_today == 0

    def test_increment_order_accumulates(self, store):
        venue = _venue("orders")
        today = utc_today()
        store.touch(venue, 100.0, today, increment_order=True)
        store.touch(venue, 100.0, today, increment_order=True)
        state = store.touch(venue, 100.0, today, increment_order=True)
        assert state.orders_today == 3

    def test_get_state_matches_touch(self, store):
        venue = _venue("get-state")
        today = utc_today()
        store.touch(venue, 750.0, today)
        state = store.get_state(venue)
        assert state is not None
        assert state.peak_equity == pytest.approx(750.0)

    def test_get_state_none_for_an_unknown_venue(self, store):
        assert store.get_state(_venue("never-touched")) is None


class TestIdempotencyLog:
    def test_duplicate_detection_round_trips_through_real_sql(self, store):
        venue = _venue("dup")
        assert store.is_duplicate(venue, "k1") is False
        store.log_order(venue, "k1", ticker="BTC-USD", side="buy", direction="LONG",
                        size_usd=10.0, quantity="0.0001", status="dry_run", simulated=True)
        assert store.is_duplicate(venue, "k1") is True

    def test_blocked_status_is_not_a_duplicate(self, store):
        venue = _venue("blocked")
        store.log_order(venue, "k1", ticker="BTC-USD", side="buy", direction="LONG",
                        size_usd=10.0, status="blocked", simulated=False)
        assert store.is_duplicate(venue, "k1") is False

    def test_guard_results_and_raw_response_round_trip_as_jsonb(self, pg_engine, store):
        venue = _venue("jsonb")
        store.log_order(venue, "k1", ticker="BTC-USD", side="buy", direction="LONG", size_usd=10.0,
                        status="dry_run", simulated=True,
                        guard_results={"drawdown": "ok"}, raw_response={"id": "o-1"})
        with pg_engine.connect() as conn:
            row = conn.execute(text(
                "SELECT guard_results, raw_response FROM trading_order_log "
                "WHERE venue = :v AND client_order_id = :k"
            ), {"v": venue, "k": "k1"}).fetchone()
        assert row is not None
        assert row[0] == {"drawdown": "ok"}
        assert row[1] == {"id": "o-1"}


class TestAverageCost:
    def test_volume_weighted_since_the_last_sell(self, store):
        venue = _venue("avg-cost")
        store.log_order(venue, "buy-1", ticker="BTC-USD", side="buy", direction="LONG",
                        size_usd=100.0, quantity="1.0", fill_price=100.0, status="dry_run", simulated=True)
        store.log_order(venue, "buy-2", ticker="BTC-USD", side="buy", direction="LONG",
                        size_usd=300.0, quantity="3.0", fill_price=200.0, status="dry_run", simulated=True)
        assert store.average_cost(venue, "BTC-USD") == pytest.approx(175.0)

    def test_none_without_history(self, store):
        assert store.average_cost(_venue("no-history"), "BTC-USD") is None
