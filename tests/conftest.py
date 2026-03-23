"""Shared test fixtures for the GRID test suite.

Provides a reusable PostgreSQL engine fixture that skips tests
when the database is unavailable.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

_DB_URL = "postgresql://grid_user:changeme@localhost:5432/grid"


@pytest.fixture
def pg_engine():
    """Return a SQLAlchemy engine connected to the test PostgreSQL database.

    Skips the test if PostgreSQL is not available.  Does NOT set up or
    tear down any test-specific data — individual test modules should
    build on top of this fixture for their own setup/cleanup.
    """
    try:
        engine = create_engine(_DB_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        pytest.skip("PostgreSQL not available")

    yield engine

    engine.dispose()
