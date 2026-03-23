"""
Tests for the GRID Point-in-Time (PIT) store.

These are the most critical correctness tests in the system.
They verify that no future data leaks into historical queries.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from sqlalchemy import text

from store.pit import PITStore

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def test_engine(pg_engine):
    """Set up PIT test data using the shared pg_engine fixture.

    Inserts a test feature and two resolved_series rows with different
    vintages.  Cleans up after the test.
    """
    engine = pg_engine

    # Set up test data in a transaction
    with engine.begin() as conn:
        # Ensure tables exist (schema should already be applied)
        # Insert test feature
        conn.execute(
            text(
                "INSERT INTO feature_registry (name, family, description, "
                "transformation, normalization, missing_data_policy, "
                "eligible_from_date, model_eligible) "
                "VALUES ('test_feature_pit', 'rates', 'Test feature', "
                "'raw', 'RAW', 'FORWARD_FILL', '1990-01-01', TRUE) "
                "ON CONFLICT (name) DO NOTHING"
            )
        )
        fid = conn.execute(
            text("SELECT id FROM feature_registry WHERE name = 'test_feature_pit'")
        ).fetchone()[0]

        # Clean up any prior test data
        conn.execute(
            text("DELETE FROM resolved_series WHERE feature_id = :fid"),
            {"fid": fid},
        )

        # Insert test rows:
        # Row 1: obs_date=2024-01-10, release_date=2024-01-15, vintage=2024-01-15, value=100
        conn.execute(
            text(
                "INSERT INTO resolved_series "
                "(feature_id, obs_date, release_date, vintage_date, value, "
                "source_priority_used) "
                "VALUES (:fid, '2024-01-10', '2024-01-15', '2024-01-15', 100.0, 1)"
            ),
            {"fid": fid},
        )

        # Row 2: Same obs_date, later vintage with revised value
        conn.execute(
            text(
                "INSERT INTO resolved_series "
                "(feature_id, obs_date, release_date, vintage_date, value, "
                "source_priority_used) "
                "VALUES (:fid, '2024-01-10', '2024-01-20', '2024-01-20', 105.0, 1)"
            ),
            {"fid": fid},
        )

    yield engine, fid

    # Clean up
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM resolved_series WHERE feature_id = :fid"),
            {"fid": fid},
        )
        conn.execute(
            text("DELETE FROM feature_registry WHERE name = 'test_feature_pit'")
        )


class TestPITNoFutureData:
    """Verify that no data with release_date > as_of_date is returned."""

    def test_no_future_data_returned(self, test_engine):
        """Query before release_date should return no results."""
        engine, fid = test_engine
        pit = PITStore(engine)

        # as_of_date is before the release_date of 2024-01-15
        result = pit.get_pit([fid], as_of_date=date(2024, 1, 14))
        assert len(result) == 0, (
            f"Expected 0 rows for as_of_date before release, got {len(result)}"
        )

    def test_future_data_after_release(self, test_engine):
        """Query on release_date should include the data."""
        engine, fid = test_engine
        pit = PITStore(engine)

        # as_of_date is exactly the release_date
        result = pit.get_pit([fid], as_of_date=date(2024, 1, 15))
        assert len(result) == 1, (
            f"Expected 1 row for as_of_date == release_date, got {len(result)}"
        )
        assert result.iloc[0]["value"] == 100.0


class TestPITVintagePolicy:
    """Verify FIRST_RELEASE and LATEST_AS_OF vintage policies."""

    def test_first_release_returns_earliest_vintage(self, test_engine):
        """FIRST_RELEASE should return the earliest vintage_date."""
        engine, fid = test_engine
        pit = PITStore(engine)

        # Query after both releases
        result = pit.get_pit(
            [fid],
            as_of_date=date(2024, 1, 25),
            vintage_policy="FIRST_RELEASE",
        )
        assert len(result) == 1
        assert result.iloc[0]["value"] == 100.0, (
            "FIRST_RELEASE should return value from earliest vintage (100.0)"
        )

    def test_latest_as_of_returns_most_recent_vintage(self, test_engine):
        """LATEST_AS_OF should return the most recent vintage."""
        engine, fid = test_engine
        pit = PITStore(engine)

        # Query after both releases
        result = pit.get_pit(
            [fid],
            as_of_date=date(2024, 1, 25),
            vintage_policy="LATEST_AS_OF",
        )
        assert len(result) == 1
        assert result.iloc[0]["value"] == 105.0, (
            "LATEST_AS_OF should return value from latest vintage (105.0)"
        )


class TestPITAssertNoLookahead:
    """Verify the lookahead safety net."""

    def test_assert_no_lookahead_raises(self, test_engine):
        """assert_no_lookahead should raise ValueError for future data."""
        engine, fid = test_engine
        pit = PITStore(engine)

        # Create a DataFrame with a future release_date
        df = pd.DataFrame([{
            "feature_id": fid,
            "obs_date": date(2024, 1, 10),
            "value": 100.0,
            "release_date": date(2024, 2, 1),  # Future
            "vintage_date": date(2024, 2, 1),
        }])

        with pytest.raises(ValueError, match="LOOKAHEAD VIOLATION"):
            pit.assert_no_lookahead(df, as_of_date=date(2024, 1, 15))
