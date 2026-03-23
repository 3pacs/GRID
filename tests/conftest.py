"""
Shared pytest fixtures for the GRID test suite.

Provides mock database engine and mock PIT store fixtures so that tests
can run without a real PostgreSQL instance.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_DB_URL = "postgresql://grid_user:changeme@localhost:5432/grid"


@pytest.fixture
def pg_engine():
    """Return a SQLAlchemy engine connected to the test PostgreSQL database.

    Skips the test if PostgreSQL is not available.
    """
    try:
        engine = create_engine(_DB_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        pytest.skip("PostgreSQL not available")

    yield engine

    engine.dispose()


@pytest.fixture
def mock_engine():
    """Return a mock SQLAlchemy Engine.

    The engine supports ``.connect()`` and ``.begin()`` context managers
    that yield a mock connection with an ``.execute()`` method.
    """
    engine = create_autospec(Engine, instance=True)

    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    mock_result.fetchall.return_value = []
    mock_conn.execute.return_value = mock_result

    # context manager for engine.connect()
    engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    # context manager for engine.begin()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    return engine


@pytest.fixture
def mock_pit_store():
    """Return a mock PITStore with sensible defaults.

    ``get_pit()`` returns an empty DataFrame.
    ``get_latest_values()`` returns an empty DataFrame.
    """
    pit = MagicMock()
    pit.get_pit.return_value = pd.DataFrame(
        columns=["feature_id", "obs_date", "value"]
    )
    pit.get_latest_values.return_value = pd.DataFrame(
        columns=["feature_id", "obs_date", "value"]
    )
    return pit
