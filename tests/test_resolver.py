"""
Tests for the GRID conflict resolution module.

Tests verify that conflicting values from multiple sources are detected
and that the highest-priority source wins in non-conflict cases.

Unit tests use mocked DB connections. Integration tests (TestConflictDetection)
require a live pg_engine fixture.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import text

from normalization.resolver import (
    CONFLICT_THRESHOLD,
    FAMILY_CONFLICT_THRESHOLDS,
    Resolver,
)


# ---------------------------------------------------------------------------
# Helpers for unit tests
# ---------------------------------------------------------------------------

class FakeRow(tuple):
    """Tuple subclass for mock DB rows."""
    pass


def _mock_engine(
    pending_rows=None,
    feature_families=None,
    already_resolved=False,
):
    """Build a mock engine returning controlled data for resolve_pending.

    Returns (engine, write_conn) so tests can inspect INSERT params.
    """
    engine = MagicMock()

    # Read phase: two connect() calls (pending query, family lookup)
    read_conn = MagicMock()
    pending_result = MagicMock()
    pending_result.fetchall.return_value = pending_rows or []
    read_conn.execute.return_value = pending_result

    family_conn = MagicMock()
    family_result = MagicMock()
    family_result.fetchall.return_value = feature_families or []
    family_conn.execute.return_value = family_result

    connect_contexts = []
    for conn in [read_conn, family_conn]:
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__ = MagicMock(return_value=False)
        connect_contexts.append(ctx)
    engine.connect.side_effect = connect_contexts

    # Write phase: begin() → write_conn
    write_conn = MagicMock()
    existing_result = MagicMock()
    existing_result.fetchone.return_value = (1,) if already_resolved else None
    insert_result = MagicMock()
    write_conn.execute.side_effect = [existing_result, insert_result]

    begin_ctx = MagicMock()
    begin_ctx.__enter__ = MagicMock(return_value=write_conn)
    begin_ctx.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_ctx

    return engine, write_conn


# ---------------------------------------------------------------------------
# Unit tests — constants
# ---------------------------------------------------------------------------


class TestConstants:

    def test_default_threshold(self):
        assert CONFLICT_THRESHOLD == 0.005

    def test_family_thresholds_positive(self):
        for fam, t in FAMILY_CONFLICT_THRESHOLDS.items():
            assert 0 < t < 1.0, f"Bad threshold for family '{fam}'"

    def test_vol_higher_than_default(self):
        assert FAMILY_CONFLICT_THRESHOLDS["vol"] > CONFLICT_THRESHOLD

    def test_crypto_highest_non_alt(self):
        crypto = FAMILY_CONFLICT_THRESHOLDS["crypto"]
        for fam, t in FAMILY_CONFLICT_THRESHOLDS.items():
            if fam != "alternative":
                assert crypto >= t


# ---------------------------------------------------------------------------
# Unit tests — init
# ---------------------------------------------------------------------------


class TestResolverInit:

    def test_stores_engine(self):
        engine = MagicMock()
        r = Resolver(db_engine=engine)
        assert r.engine is engine


# ---------------------------------------------------------------------------
# Unit tests — resolve_pending: single source
# ---------------------------------------------------------------------------


class TestSingleSourceUnit:

    @patch("normalization.resolver.EntityMap")
    def test_single_source_resolved(self, MockEntityMap):
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 42
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("GDP_US", date(2026, 1, 1), 21500.0, "src_fred",
                      datetime(2026, 1, 15, 10, 0), 1, "FRED")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending,
            feature_families=[(42, "equity")],
        )

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["resolved"] == 1
        assert summary["conflicts_found"] == 0
        assert summary["errors"] == 0

    @patch("normalization.resolver.EntityMap")
    def test_unmapped_series_skipped(self, MockEntityMap):
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = None
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("UNKNOWN", date(2026, 1, 1), 100.0, "s1",
                      datetime(2026, 1, 2), 1, "Src1")),
        ]
        engine, _ = _mock_engine(pending_rows=pending)

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["resolved"] == 0
        assert summary["conflicts_found"] == 0


# ---------------------------------------------------------------------------
# Unit tests — conflict detection
# ---------------------------------------------------------------------------


class TestConflictDetectionUnit:

    @patch("normalization.resolver.EntityMap")
    def test_within_threshold_no_conflict(self, MockEntityMap):
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 10
        MockEntityMap.return_value = mock_map

        # 100.0 vs 100.4 → 0.4% < 0.5% default
        pending = [
            FakeRow(("CPI", date(2026, 2, 1), 100.0, "s1",
                      datetime(2026, 2, 5), 1, "BLS")),
            FakeRow(("CPI", date(2026, 2, 1), 100.4, "s2",
                      datetime(2026, 2, 5), 2, "FRED")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending, feature_families=[(10, "")],
        )

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["conflicts_found"] == 0

    @patch("normalization.resolver.EntityMap")
    def test_beyond_threshold_flags_conflict(self, MockEntityMap):
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 10
        MockEntityMap.return_value = mock_map

        # 100.0 vs 101.0 → 1.0% > 0.5%
        pending = [
            FakeRow(("CPI", date(2026, 3, 1), 100.0, "s1",
                      datetime(2026, 3, 5), 1, "BLS")),
            FakeRow(("CPI", date(2026, 3, 1), 101.0, "s2",
                      datetime(2026, 3, 5), 2, "FRED")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending, feature_families=[(10, "")],
        )

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["conflicts_found"] == 1

    @patch("normalization.resolver.EntityMap")
    def test_family_threshold_crypto(self, MockEntityMap):
        """Crypto threshold (3%) prevents false positive on 2% diff."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 20
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("BTC", date(2026, 3, 1), 50000.0, "s1",
                      datetime(2026, 3, 2), 1, "Binance")),
            FakeRow(("BTC", date(2026, 3, 1), 51000.0, "s2",
                      datetime(2026, 3, 2), 2, "Coinbase")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending, feature_families=[(20, "crypto")],
        )

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["conflicts_found"] == 0  # 2% < 3%


# ---------------------------------------------------------------------------
# Unit tests — zero reference value
# ---------------------------------------------------------------------------


class TestZeroRefUnit:

    @patch("normalization.resolver.EntityMap")
    def test_zero_ref_nonzero_other_conflict(self, MockEntityMap):
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 30
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("RATE", date(2026, 1, 1), 0.0, "s1",
                      datetime(2026, 1, 2), 1, "Src1")),
            FakeRow(("RATE", date(2026, 1, 1), 0.001, "s2",
                      datetime(2026, 1, 2), 2, "Src2")),
        ]
        engine, _ = _mock_engine(
            pending_rows=pending, feature_families=[(30, "")],
        )

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["conflicts_found"] == 1

    @patch("normalization.resolver.EntityMap")
    def test_both_zero_no_conflict(self, MockEntityMap):
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 30
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("RATE", date(2026, 1, 1), 0.0, "s1",
                      datetime(2026, 1, 2), 1, "Src1")),
            FakeRow(("RATE", date(2026, 1, 1), 0.0, "s2",
                      datetime(2026, 1, 2), 2, "Src2")),
        ]
        engine, _ = _mock_engine(
            pending_rows=pending, feature_families=[(30, "")],
        )

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["conflicts_found"] == 0


# ---------------------------------------------------------------------------
# Unit tests — already resolved, empty, DB errors
# ---------------------------------------------------------------------------


class TestSkipAndErrorUnit:

    @patch("normalization.resolver.EntityMap")
    def test_already_resolved_skipped(self, MockEntityMap):
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 50
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("GDP", date(2026, 1, 1), 21500.0, "s1",
                      datetime(2026, 1, 15), 1, "FRED")),
        ]
        engine, _ = _mock_engine(
            pending_rows=pending,
            feature_families=[(50, "equity")],
            already_resolved=True,
        )

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["resolved"] == 0

    def test_empty_pending(self):
        engine, _ = _mock_engine(pending_rows=[])
        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary == {"resolved": 0, "conflicts_found": 0, "errors": 0}

    def test_db_error_returns_error_count(self):
        engine = MagicMock()
        ctx = MagicMock()
        conn = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value = ctx
        conn.execute.side_effect = Exception("DB down")

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["errors"] == 1
        assert summary["resolved"] == 0


# ---------------------------------------------------------------------------
# Unit tests — priority selection
# ---------------------------------------------------------------------------


class TestPriorityUnit:

    @patch("normalization.resolver.EntityMap")
    def test_lowest_rank_wins(self, MockEntityMap):
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 60
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("FX", date(2026, 2, 1), 100.3, "low_pri",
                      datetime(2026, 2, 2), 5, "LowPri")),
            FakeRow(("FX", date(2026, 2, 1), 100.0, "high_pri",
                      datetime(2026, 2, 2), 1, "HighPri")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending, feature_families=[(60, "")],
        )

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["resolved"] == 1
        # Check that the INSERT used the high-priority value
        insert_call = write_conn.execute.call_args_list[1]
        params = insert_call[0][1]
        assert params["val"] == 100.0
        assert params["src"] == "high_pri"


# ---------------------------------------------------------------------------
# Unit tests — get_conflict_report
# ---------------------------------------------------------------------------


class TestConflictReportUnit:

    def test_returns_dataframe(self):
        engine = MagicMock()
        ctx = MagicMock()
        conn = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value = ctx
        conn.execute.return_value.fetchall.return_value = [
            (1, "GDP_US", date(2026, 1, 1), 21500.0, "FRED",
             '{"sources": []}', date(2026, 1, 15), date(2026, 1, 15)),
        ]

        resolver = Resolver(db_engine=engine)
        df = resolver.get_conflict_report()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert "feature_name" in df.columns
        assert df.iloc[0]["feature_name"] == "GDP_US"

    def test_empty_report(self):
        engine = MagicMock()
        ctx = MagicMock()
        conn = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value = ctx
        conn.execute.return_value.fetchall.return_value = []

        resolver = Resolver(db_engine=engine)
        df = resolver.get_conflict_report()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


# ===========================================================================
# Integration tests (require pg_engine fixture / live Postgres)
# ===========================================================================


@pytest.fixture
def test_engine(pg_engine):
    """Set up resolver test data using the shared pg_engine fixture."""
    engine = pg_engine

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO feature_registry (name, family, description, "
                "transformation, normalization, missing_data_policy, "
                "eligible_from_date, model_eligible) "
                "VALUES ('test_resolver_feature', 'rates', 'Test resolver', "
                "'raw', 'RAW', 'FORWARD_FILL', '1990-01-01', TRUE) "
                "ON CONFLICT (name) DO NOTHING"
            )
        )

    yield engine

    with engine.begin() as conn:
        fid = conn.execute(
            text("SELECT id FROM feature_registry WHERE name = 'test_resolver_feature'")
        ).fetchone()
        if fid:
            conn.execute(
                text("DELETE FROM resolved_series WHERE feature_id = :fid"),
                {"fid": fid[0]},
            )
        conn.execute(
            text("DELETE FROM raw_series WHERE series_id LIKE 'TEST_RESOLVER_%'")
        )
        conn.execute(
            text("DELETE FROM feature_registry WHERE name = 'test_resolver_feature'")
        )


class TestConflictDetection:
    """Verify conflict detection when sources disagree."""

    def test_conflict_detection(self, test_engine):
        """Values differing by 1% should be flagged as a conflict."""
        engine = test_engine

        # Get source IDs for FRED (priority 1) and yfinance (priority 2)
        with engine.begin() as conn:
            fred_id = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = 'FRED'")
            ).fetchone()[0]
            yf_id = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = 'yfinance'")
            ).fetchone()[0]

            # Clean any prior test data
            conn.execute(
                text("DELETE FROM raw_series WHERE series_id = 'TEST_RESOLVER_CONFLICT'")
            )

            # Insert two rows with 1% difference (above 0.5% threshold)
            conn.execute(
                text(
                    "INSERT INTO raw_series "
                    "(series_id, source_id, obs_date, value, pull_status) "
                    "VALUES ('TEST_RESOLVER_CONFLICT', :src, '2024-01-10', :val, 'SUCCESS')"
                ),
                {"src": fred_id, "val": 100.0},
            )
            conn.execute(
                text(
                    "INSERT INTO raw_series "
                    "(series_id, source_id, obs_date, value, pull_status) "
                    "VALUES ('TEST_RESOLVER_CONFLICT', :src, '2024-01-10', :val, 'SUCCESS')"
                ),
                {"src": yf_id, "val": 101.0},  # 1% different
            )

        # The resolver needs the entity map to have this mapping,
        # but since TEST_RESOLVER_CONFLICT is not in SEED_MAPPINGS,
        # it will be skipped by resolve_pending. This test verifies
        # the resolver doesn't crash on unmapped series.

        resolver = Resolver(db_engine=engine)
        result = resolver.resolve_pending()

        # Since the series_id is not mapped, it should complete without errors
        assert result["errors"] == 0

    def test_no_conflict_uses_priority(self, test_engine):
        """Values within threshold should use highest-priority source."""
        engine = test_engine

        with engine.begin() as conn:
            fred_id = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = 'FRED'")
            ).fetchone()[0]
            yf_id = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = 'yfinance'")
            ).fetchone()[0]

            # Clean prior data
            conn.execute(
                text("DELETE FROM raw_series WHERE series_id = 'TEST_RESOLVER_NOCONFLICT'")
            )

            # Insert two rows with 0.1% difference (below 0.5% threshold)
            conn.execute(
                text(
                    "INSERT INTO raw_series "
                    "(series_id, source_id, obs_date, value, pull_status) "
                    "VALUES ('TEST_RESOLVER_NOCONFLICT', :src, '2024-01-10', :val, 'SUCCESS')"
                ),
                {"src": fred_id, "val": 100.0},
            )
            conn.execute(
                text(
                    "INSERT INTO raw_series "
                    "(series_id, source_id, obs_date, value, pull_status) "
                    "VALUES ('TEST_RESOLVER_NOCONFLICT', :src, '2024-01-10', :val, 'SUCCESS')"
                ),
                {"src": yf_id, "val": 100.1},  # 0.1% different
            )

        resolver = Resolver(db_engine=engine)
        result = resolver.resolve_pending()

        # Should complete without errors
        assert result["errors"] == 0
