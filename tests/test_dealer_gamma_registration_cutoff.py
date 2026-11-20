"""Exercise real selector SQL against an in-memory synthetic registry.

SQLite proves the exclusion predicate/order, not PostgreSQL MVCC or commit
visibility. Registration at/before cutoff is necessary, never sufficient PIT evidence.
"""
from datetime import date, datetime, timedelta, timezone
import sqlite3

import pytest
from sqlalchemy import create_engine, text

from physics.dealer_gamma import DealerGammaEngine

DAY = date(2026, 10, 20)
CUTOFF = datetime(2026, 10, 20, 23, tzinfo=timezone.utc)


@pytest.fixture
def registry():
    engine = create_engine("sqlite://", connect_args={"detect_types": sqlite3.PARSE_DECLTYPES})
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE options_capture_batches (
            ticker TEXT, snap_date DATE, capture_batch_id TEXT,
            capture_ordinal INTEGER, capture_completed_at TEXT, registered_at TEXT)"""))
    yield engine
    engine.dispose()


def insert(engine, batch, ordinal, registered, *, day=DAY, completed=None, ticker="SPY"):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO options_capture_batches VALUES (:ticker,:day,:batch,:ordinal,:completed,:registered)"),
                     dict(ticker=ticker, day=day, batch=batch, ordinal=ordinal,
                          completed=completed or CUTOFF - timedelta(minutes=5), registered=registered))


def select(engine):
    return DealerGammaEngine(engine).latest_completed_capture_batch("SPY", DAY, CUTOFF)


def test_late_registration_excludes_higher_ordinal(registry):
    insert(registry, "earlier", 1, CUTOFF - timedelta(seconds=1))
    insert(registry, "late", 2, CUTOFF + timedelta(microseconds=1))
    assert select(registry) == (DAY, "earlier")


@pytest.mark.parametrize("registered", [None, CUTOFF + timedelta(seconds=1)])
def test_only_late_or_unknown_registration_is_unavailable(registry, registered):
    insert(registry, "inadmissible", 1, registered)
    assert select(registry) is None


def test_registration_boundary_is_inclusive(registry):
    insert(registry, "at-cutoff", 1, CUTOFF)
    assert select(registry) == (DAY, "at-cutoff")


def test_retains_day_completion_ticker_bounds_and_ordinal_order(registry):
    for batch, ordinal in [("first", 1), ("second", 2)]:
        insert(registry, batch, ordinal, CUTOFF)
    insert(registry, "future-day", 9, CUTOFF, day=DAY + timedelta(days=1))
    insert(registry, "future-completion", 9, CUTOFF, completed=CUTOFF + timedelta(seconds=1))
    insert(registry, "other-symbol", 9, CUTOFF, ticker="QQQ")
    assert select(registry) == (DAY, "second")
