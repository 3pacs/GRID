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

PostgresRiskStore itself does NOT create tables at runtime (see its
docstring) — the migration owns the schema. Test setup here creates the
schema explicitly, deliberately duplicating the migration's DDL rather than
importing it, for the same reason the migration doesn't import from
trading/robinhood_risk_store.py: a migration (and by extension what test
setup needs to reproduce it) must stay a frozen, self-contained record, not
something that silently changes if application code changes later.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from trading.robinhood_risk_store import PostgresRiskStore, utc_today

# Mirrors migrations/versions/robinhood_guards_20260924.py's upgrade() DDL —
# test-only, so it deliberately does NOT import from that migration (or from
# PostgresRiskStore, which no longer carries this DDL at all after this was
# moved out of runtime code — see the module docstring above).
_SCHEMA_DDL = (
    """
    CREATE TABLE IF NOT EXISTS trading_risk_state (
        venue             TEXT PRIMARY KEY,
        peak_equity       DOUBLE PRECISION NOT NULL,
        day_start_equity  DOUBLE PRECISION NOT NULL,
        day_start_date    DATE NOT NULL,
        orders_today      INTEGER NOT NULL DEFAULT 0,
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trading_order_log (
        id                BIGSERIAL PRIMARY KEY,
        venue             TEXT NOT NULL,
        client_order_id   TEXT NOT NULL,
        wallet_id         TEXT,
        ticker            TEXT NOT NULL,
        side              TEXT NOT NULL,
        direction         TEXT NOT NULL,
        size_usd          DOUBLE PRECISION NOT NULL,
        quantity          TEXT,
        bid               DOUBLE PRECISION,
        ask               DOUBLE PRECISION,
        mid               DOUBLE PRECISION,
        executable_price  DOUBLE PRECISION,
        spread_bps        DOUBLE PRECISION,
        spread_cost_usd   DOUBLE PRECISION,
        order_type        TEXT NOT NULL,
        status            TEXT NOT NULL,
        fill_price        DOUBLE PRECISION,
        guard_results     JSONB,
        error             TEXT,
        raw_response      JSONB,
        simulated         BOOLEAN NOT NULL DEFAULT FALSE,
        created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_trading_order_log_lookup "
    "ON trading_order_log (venue, client_order_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_trading_order_log_recent "
    "ON trading_order_log (venue, created_at DESC)",
)


def _create_schema(engine) -> None:
    with engine.begin() as conn:
        for stmt in _SCHEMA_DDL:
            conn.execute(text(stmt))


@pytest.fixture
def store(pg_engine):
    _create_schema(pg_engine)
    s = PostgresRiskStore(pg_engine)
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
                        size_usd=10.0, quantity="0.0001", order_type="limit",
                        status="dry_run", simulated=True)
        assert store.is_duplicate(venue, "k1") is True

    def test_blocked_status_is_not_a_duplicate(self, store):
        venue = _venue("blocked")
        store.log_order(venue, "k1", ticker="BTC-USD", side="buy", direction="LONG",
                        size_usd=10.0, order_type="limit", status="blocked", simulated=False)
        assert store.is_duplicate(venue, "k1") is False

    def test_guard_results_and_raw_response_round_trip_as_jsonb(self, pg_engine, store):
        venue = _venue("jsonb")
        store.log_order(venue, "k1", ticker="BTC-USD", side="buy", direction="LONG", size_usd=10.0,
                        order_type="limit", status="dry_run", simulated=True,
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
                        size_usd=100.0, quantity="1.0", fill_price=100.0, order_type="limit",
                        status="submitted", simulated=False)
        store.log_order(venue, "buy-2", ticker="BTC-USD", side="buy", direction="LONG",
                        size_usd=300.0, quantity="3.0", fill_price=200.0, order_type="limit",
                        status="submitted", simulated=False)
        assert store.average_cost(venue, "BTC-USD") == pytest.approx(175.0)

    def test_none_without_history(self, store):
        assert store.average_cost(_venue("no-history"), "BTC-USD") is None

    def test_simulated_buys_are_excluded_from_the_real_basis(self, store):
        venue = _venue("avg-cost-sim")
        store.log_order(venue, "buy-1", ticker="BTC-USD", side="buy", direction="LONG",
                        size_usd=100.0, quantity="1.0", fill_price=100.0, order_type="limit",
                        status="submitted", simulated=False)
        store.log_order(venue, "buy-2", ticker="BTC-USD", side="buy", direction="LONG",
                        size_usd=600.0, quantity="3.0", fill_price=200.0, order_type="limit",
                        status="dry_run", simulated=True)
        assert store.average_cost(venue, "BTC-USD") == pytest.approx(100.0)
        assert store.average_cost(venue, "BTC-USD", include_simulated=True) == pytest.approx(175.0)


class TestFailsClosedWithoutMigration:
    """PostgresRiskStore creates no tables of its own — a database the
    migration hasn't been applied to must block orders (raise), not get a
    schema bootstrapped for it under whatever role the app happens to run
    as. This class owns its own drop/recreate around each test rather than
    depending on the `store` fixture, so it's correct regardless of test
    execution order relative to the rest of this file."""

    @pytest.fixture
    def dropped(self, pg_engine):
        with pg_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS trading_order_log"))
            conn.execute(text("DROP TABLE IF EXISTS trading_risk_state"))
        yield pg_engine
        _create_schema(pg_engine)  # restore for any other test in this file

    def test_touch_raises_when_the_table_is_missing(self, dropped):
        store = PostgresRiskStore(dropped)
        with pytest.raises(ProgrammingError):
            store.touch(_venue("no-migration-touch"), 100.0, utc_today())

    def test_is_duplicate_raises_when_the_table_is_missing(self, dropped):
        store = PostgresRiskStore(dropped)
        with pytest.raises(ProgrammingError):
            store.is_duplicate(_venue("no-migration-dup"), "k1")

    def test_get_state_raises_when_the_table_is_missing(self, dropped):
        store = PostgresRiskStore(dropped)
        with pytest.raises(ProgrammingError):
            store.get_state(_venue("no-migration-state"))
