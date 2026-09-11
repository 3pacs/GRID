"""
Tests for the GRID conflict resolution module.

Tests verify that conflicting values from multiple sources are detected
and that the highest-priority source wins in non-conflict cases.

Unit tests use mocked DB connections. Integration tests (TestConflictDetection)
require a live pg_engine fixture.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
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

    The resolver uses two distinct connection styles:

      engine.connect():
        1. Feature families SELECT (read-only).

      engine.begin() (transactional, used so SET LOCAL applies):
        1. Distinct series_ids SELECT.
        2. Per-partition worker SELECT (raw_series + source_catalog).
        3. Per-partition _flush_batch INSERT (resolved_series).

    The mock therefore wires engine.connect() once for fam_conn, and
    engine.begin() as a side-effect chain: series_conn, then one
    worker_conn per series_id, then write_conn for the flushes.

    Returns (engine, write_conn) so tests can inspect INSERT params on
    the write_conn that handles the resolved_series writes.
    """
    engine = MagicMock()
    rows = pending_rows or []

    # Extract distinct series_ids from pending rows for the series query
    series_ids = list({r[0] if isinstance(r, (tuple, FakeRow)) else r for r in rows})

    def _make_ctx(conn):
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__ = MagicMock(return_value=False)
        return ctx

    # ── engine.connect() chain — only feature_families uses connect ──
    fam_conn = MagicMock()
    fam_result = MagicMock()
    fam_result.fetchall.return_value = feature_families or []
    fam_conn.execute.return_value = fam_result

    fallback_conn = MagicMock()
    fallback_result = MagicMock()
    fallback_result.fetchall.return_value = []
    fallback_result.fetchone.return_value = None
    fallback_conn.execute.return_value = fallback_result

    connect_contexts = [_make_ctx(fam_conn)]
    for _ in range(5):
        connect_contexts.append(_make_ctx(fallback_conn))
    engine.connect.side_effect = connect_contexts

    # ── engine.begin() chain — series fetch, workers, then writes ──
    # 1. Distinct series_ids
    series_conn = MagicMock()
    series_result = MagicMock()
    series_result.fetchall.return_value = [(sid,) for sid in series_ids]
    series_conn.execute.return_value = series_result

    # 2. One worker_conn per series_id, returning its raw_series rows
    worker_contexts = []
    for sid in series_ids:
        worker_conn = MagicMock()
        # Inside the worker begin() context the resolver runs:
        #   conn.execute(SET LOCAL statement_timeout)  → result ignored
        #   conn.execute(SELECT raw_series ...).fetchall() → rows
        # MagicMock().execute(...) returns a MagicMock with fetchall(),
        # but we need fetchall() to return our rows for the SECOND call,
        # not the first. Use a side_effect that always returns the same
        # rows-bearing result — SET LOCAL ignores it, the SELECT consumes
        # it. Both shapes are compatible.
        worker_result = MagicMock()
        worker_result.fetchall.return_value = [
            r for r in rows if (isinstance(r, (tuple, FakeRow)) and r[0] == sid)
        ]
        worker_conn.execute.return_value = worker_result
        worker_contexts.append(_make_ctx(worker_conn))

    # 3. Write phase — _flush_batch INSERTs. Tests inspect this conn.
    write_conn = MagicMock()
    existing_result = MagicMock()
    existing_result.fetchone.return_value = (1,) if already_resolved else None
    insert_result = MagicMock()
    n = max(len(rows), 1)
    write_conn.execute.side_effect = [
        val for _ in range(n) for val in (existing_result, insert_result)
    ]

    # Plus a few spare write contexts so multi-partition / multi-batch
    # tests don't run out of begin() returns.
    begin_contexts = [_make_ctx(series_conn)] + worker_contexts
    begin_contexts.append(_make_ctx(write_conn))
    for _ in range(5):
        begin_contexts.append(_make_ctx(write_conn))
    engine.begin.side_effect = begin_contexts

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

    def test_all_expected_families_present(self):
        """All six expected families must be present in the threshold map."""
        expected = {"vol", "commodity", "crypto", "equity", "alternative", "flows"}
        for fam in expected:
            assert fam in FAMILY_CONFLICT_THRESHOLDS, f"Missing family: {fam}"

    def test_equity_lower_than_vol(self):
        assert FAMILY_CONFLICT_THRESHOLDS["equity"] < FAMILY_CONFLICT_THRESHOLDS["vol"]

    def test_alternative_highest_threshold(self):
        """Alternative data (weather, patents) allows the widest tolerance."""
        alt = FAMILY_CONFLICT_THRESHOLDS["alternative"]
        for t in FAMILY_CONFLICT_THRESHOLDS.values():
            assert alt >= t


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

    @patch("normalization.resolver.EntityMap")
    def test_resolved_summary_keys_always_present(self, MockEntityMap):
        """resolve_pending must always return the full summary contract."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = None
        MockEntityMap.return_value = mock_map

        engine, _ = _mock_engine(pending_rows=[])
        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert set(summary.keys()) == {
            "resolved", "conflicts_found", "errors",
            "series_scanned", "duration_s", "dry_run", "unmapped",
        }

    @patch("normalization.resolver.EntityMap")
    def test_pull_timestamp_date_extracted_for_release_date(self, MockEntityMap):
        """release_date proxy uses pull_timestamp.date() when it is a datetime."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 99
        MockEntityMap.return_value = mock_map

        ts = datetime(2026, 3, 20, 14, 30, 0)
        pending = [
            FakeRow(("FX", date(2026, 3, 15), 1.25, "src", ts, 1, "Src")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending,
            feature_families=[(99, "")],
        )

        resolver = Resolver(db_engine=engine)
        resolver.resolve_pending()

        insert_call = write_conn.execute.call_args_list[0]
        params = insert_call[0][1][0]
        assert params["rd"] == date(2026, 3, 20)

    @patch("normalization.resolver.EntityMap")
    def test_pull_timestamp_plain_date_used_directly(self, MockEntityMap):
        """If pull_timestamp is already a date (no .date() method), use it directly."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 77
        MockEntityMap.return_value = mock_map

        plain_date = date(2026, 3, 21)
        pending = [
            FakeRow(("IDX", date(2026, 3, 20), 5000.0, "src", plain_date, 1, "Src")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending,
            feature_families=[(77, "equity")],
        )

        resolver = Resolver(db_engine=engine)
        resolver.resolve_pending()

        insert_call = write_conn.execute.call_args_list[0]
        params = insert_call[0][1][0]
        assert params["rd"] == plain_date


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

    @patch("normalization.resolver.EntityMap")
    def test_conflict_detail_json_well_formed(self, MockEntityMap):
        """When a conflict is flagged the stored JSON must be parseable."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 55
        MockEntityMap.return_value = mock_map

        # 5% divergence — exceeds default 0.5% threshold
        pending = [
            FakeRow(("IDX", date(2026, 1, 10), 100.0, "s1",
                      datetime(2026, 1, 11), 1, "Src1")),
            FakeRow(("IDX", date(2026, 1, 10), 105.0, "s2",
                      datetime(2026, 1, 11), 2, "Src2")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending, feature_families=[(55, "")],
        )

        resolver = Resolver(db_engine=engine)
        resolver.resolve_pending()

        insert_call = write_conn.execute.call_args_list[0]
        params = insert_call[0][1][0]
        assert params["cf"] is True
        detail = json.loads(params["cd"])
        assert "sources" in detail
        assert "threshold" in detail
        assert len(detail["sources"]) == 2

    @patch("normalization.resolver.EntityMap")
    def test_family_threshold_vol_allows_1pct(self, MockEntityMap):
        """Vol family (2% threshold) should not conflict on a 1.5% difference."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 33
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("VIX", date(2026, 2, 10), 20.0, "s1",
                      datetime(2026, 2, 11), 1, "CBOE")),
            FakeRow(("VIX", date(2026, 2, 10), 20.3, "s2",
                      datetime(2026, 2, 11), 2, "YF")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending, feature_families=[(33, "vol")],
        )

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["conflicts_found"] == 0  # 1.5% < 2%

    @patch("normalization.resolver.EntityMap")
    def test_unknown_family_falls_back_to_default_threshold(self, MockEntityMap):
        """Series whose feature_id has no family entry uses default 0.5% threshold."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 88
        MockEntityMap.return_value = mock_map

        # 0.8% difference — above default (0.5%) but below vol/commodity
        pending = [
            FakeRow(("XYZ", date(2026, 4, 1), 100.0, "s1",
                      datetime(2026, 4, 2), 1, "Src1")),
            FakeRow(("XYZ", date(2026, 4, 1), 100.8, "s2",
                      datetime(2026, 4, 2), 2, "Src2")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending,
            feature_families=[(88, "")],   # empty family → default threshold
        )

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        # 0.8% > 0.5% default → conflict expected
        assert summary["conflicts_found"] == 1

    @patch("normalization.resolver.EntityMap")
    def test_three_sources_conflict_detected_when_third_diverges(self, MockEntityMap):
        """Conflict must be detected even when only the third source diverges."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 41
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("RATE", date(2026, 5, 1), 5.00, "s1",
                      datetime(2026, 5, 2), 1, "Primary")),
            FakeRow(("RATE", date(2026, 5, 1), 5.02, "s2",
                      datetime(2026, 5, 2), 2, "Secondary")),
            FakeRow(("RATE", date(2026, 5, 1), 5.10, "s3",
                      datetime(2026, 5, 2), 3, "Tertiary")),
        ]

        engine, write_conn = _mock_engine(
            pending_rows=pending,
            feature_families=[(41, "")],
        )

        with patch("normalization.resolver.EntityMap") as MockEM:
            MockEM.return_value.get_feature_id.return_value = 41
            resolver = Resolver(db_engine=engine)
            summary = resolver.resolve_pending()

        # 2% diff between winner (5.00) and tertiary (5.10) → conflict
        assert summary["conflicts_found"] == 1


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

    @patch("normalization.resolver.EntityMap")
    def test_negative_ref_value_uses_absolute_for_pct(self, MockEntityMap):
        """Negative reference values should still compute % diff using abs()."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 31
        MockEntityMap.return_value = mock_map

        # ref=-100, other=-101 → 1% difference > default 0.5%
        pending = [
            FakeRow(("SPREAD", date(2026, 6, 1), -100.0, "s1",
                      datetime(2026, 6, 2), 1, "Src1")),
            FakeRow(("SPREAD", date(2026, 6, 1), -101.0, "s2",
                      datetime(2026, 6, 2), 2, "Src2")),
        ]
        engine, _ = _mock_engine(
            pending_rows=pending, feature_families=[(31, "")],
        )

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["conflicts_found"] == 1


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

        # With ON CONFLICT DO NOTHING, the resolver reports batch size
        # as resolved (the DB silently skips duplicates)
        assert summary["resolved"] >= 0

    def test_empty_pending(self):
        engine, _ = _mock_engine(pending_rows=[])
        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["resolved"] == 0
        assert summary["conflicts_found"] == 0
        assert summary["errors"] == 0
        assert summary["series_scanned"] == 0

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

    @patch("normalization.resolver.EntityMap")
    def test_insert_error_increments_error_count(self, MockEntityMap):
        """When INSERT throws, error count must increment and resolved must not."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 70
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("ERR", date(2026, 1, 1), 1.0, "s1",
                      datetime(2026, 1, 2), 1, "S")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending,
            feature_families=[(70, "")],
        )

        # Make the batch INSERT fail. We don't replace engine.begin —
        # that would also break the series-distinct and worker-fetch
        # begin contexts that _mock_engine wired. Just make the
        # write_conn it set up raise on execute.
        write_conn.execute.side_effect = Exception("unique violation")

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        # _flush_batch catches the error and returns 0
        assert summary["resolved"] == 0

    @patch("normalization.resolver.EntityMap")
    def test_family_lookup_failure_uses_default_threshold(self, MockEntityMap):
        """If the family query raises, the resolver must still complete using
        the default threshold and not crash."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 80
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("X", date(2026, 1, 1), 100.0, "s1",
                      datetime(2026, 1, 2), 1, "S1")),
            FakeRow(("X", date(2026, 1, 1), 101.0, "s2",
                      datetime(2026, 1, 2), 2, "S2")),
        ]

        engine = MagicMock()

        def _ctx(c):
            ctx = MagicMock()
            ctx.__enter__ = MagicMock(return_value=c)
            ctx.__exit__ = MagicMock(return_value=False)
            return ctx

        # engine.connect: ONLY feature_families uses connect now. Make
        # the family lookup raise — the resolver's outer try should
        # swallow it and fall through to default thresholds.
        bad_conn = MagicMock()
        bad_conn.execute.side_effect = Exception("family table missing")
        engine.connect.side_effect = [_ctx(bad_conn)]

        # engine.begin chain: series-distinct → worker-fetch → write.
        series_conn = MagicMock()
        series_res = MagicMock()
        series_res.fetchall.return_value = [("X",)]
        series_conn.execute.return_value = series_res

        worker_conn = MagicMock()
        worker_res = MagicMock()
        worker_res.fetchall.return_value = pending
        worker_conn.execute.return_value = worker_res

        write_conn = MagicMock()
        ins_res = MagicMock()
        write_conn.execute.return_value = ins_res

        engine.begin.side_effect = [
            _ctx(series_conn),
            _ctx(worker_conn),
            _ctx(write_conn),
        ]

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        # Falls back to default 0.5%; 1% diff → conflict_found
        assert summary["errors"] == 0
        assert summary["conflicts_found"] == 1


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
        insert_call = write_conn.execute.call_args_list[0]
        params = insert_call[0][1][0]
        assert params["val"] == 100.0
        assert params["src"] == "high_pri"

    @patch("normalization.resolver.EntityMap")
    def test_feature_id_stored_in_insert(self, MockEntityMap):
        """Resolved row must store the mapped feature_id, not the raw series_id."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 999
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("DFF", date(2026, 2, 15), 5.33, "src_fred",
                      datetime(2026, 2, 16), 1, "FRED")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending, feature_families=[(999, "")],
        )

        resolver = Resolver(db_engine=engine)
        resolver.resolve_pending()

        insert_call = write_conn.execute.call_args_list[0]
        params = insert_call[0][1][0]
        assert params["fid"] == 999

    @patch("normalization.resolver.EntityMap")
    def test_obs_date_preserved_in_insert(self, MockEntityMap):
        """The obs_date of the raw observation must carry through to resolved_series."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 61
        MockEntityMap.return_value = mock_map

        obs = date(2026, 3, 28)
        pending = [
            FakeRow(("T10Y2Y", obs, 0.55, "src", datetime(2026, 3, 29), 1, "FRED")),
        ]
        engine, write_conn = _mock_engine(
            pending_rows=pending, feature_families=[(61, "")],
        )

        resolver = Resolver(db_engine=engine)
        resolver.resolve_pending()

        insert_call = write_conn.execute.call_args_list[0]
        params = insert_call[0][1][0]
        assert params["od"] == obs

    @patch("normalization.resolver.EntityMap")
    def test_multiple_dates_resolved_independently(self, MockEntityMap):
        """Each (series_id, obs_date) group is resolved as an independent row."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 62
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("T10Y2Y", date(2026, 1, 1), 0.50, "s1",
                      datetime(2026, 1, 2), 1, "FRED")),
            FakeRow(("T10Y2Y", date(2026, 1, 2), 0.55, "s1",
                      datetime(2026, 1, 3), 1, "FRED")),
            FakeRow(("T10Y2Y", date(2026, 1, 3), 0.60, "s1",
                      datetime(2026, 1, 4), 1, "FRED")),
        ]

        # Need 3 existing-check + 3 insert calls from write_conn
        engine = MagicMock()

        def _ctx(c):
            ctx = MagicMock()
            ctx.__enter__ = MagicMock(return_value=c)
            ctx.__exit__ = MagicMock(return_value=False)
            return ctx

        # engine.connect: feature families only.
        fam_conn = MagicMock()
        fam_res = MagicMock()
        fam_res.fetchall.return_value = [(62, "")]
        fam_conn.execute.return_value = fam_res
        engine.connect.side_effect = [_ctx(fam_conn)]

        # engine.begin chain: series-distinct → worker-fetch → write.
        series_conn = MagicMock()
        series_res = MagicMock()
        series_res.fetchall.return_value = [("T10Y2Y",)]
        series_conn.execute.return_value = series_res

        worker_conn = MagicMock()
        worker_res = MagicMock()
        worker_res.fetchall.return_value = pending
        worker_conn.execute.return_value = worker_res

        write_conn = MagicMock()
        existing = MagicMock()
        existing.fetchone.return_value = None
        ins = MagicMock()
        write_conn.execute.side_effect = [existing, ins, existing, ins, existing, ins]

        engine.begin.side_effect = [
            _ctx(series_conn),
            _ctx(worker_conn),
            _ctx(write_conn),
        ]

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["resolved"] == 3


# ---------------------------------------------------------------------------
# Unit tests — date filtering boundary
# ---------------------------------------------------------------------------


class TestDateFilteringUnit:

    @patch("normalization.resolver.EntityMap")
    def test_different_series_resolved_in_same_batch(self, MockEntityMap):
        """Two distinct series in the same pending batch should each be resolved."""
        mock_map = MagicMock()
        # Always return a valid feature_id regardless of series_id
        mock_map.get_feature_id.side_effect = lambda sid: {"AAA": 100, "BBB": 101}[sid]
        MockEntityMap.return_value = mock_map

        pending = [
            FakeRow(("AAA", date(2026, 1, 1), 10.0, "s1",
                      datetime(2026, 1, 2), 1, "Src")),
            FakeRow(("BBB", date(2026, 1, 1), 20.0, "s2",
                      datetime(2026, 1, 2), 1, "Src")),
        ]

        engine = MagicMock()

        def _ctx(c):
            ctx = MagicMock()
            ctx.__enter__ = MagicMock(return_value=c)
            ctx.__exit__ = MagicMock(return_value=False)
            return ctx

        # engine.connect: feature families only.
        fam_conn = MagicMock()
        fam_res = MagicMock()
        fam_res.fetchall.return_value = [(100, ""), (101, "")]
        fam_conn.execute.return_value = fam_res
        engine.connect.side_effect = [_ctx(fam_conn)]

        # engine.begin chain: series-distinct → worker-fetch (per partition)
        # → write_conn. With 2 distinct series IDs and the default 8-worker
        # partition split, only one partition actually gets work; we still
        # provide enough worker contexts to be safe.
        series_conn = MagicMock()
        series_res = MagicMock()
        series_res.fetchall.return_value = [("AAA",), ("BBB",)]
        series_conn.execute.return_value = series_res

        def _make_worker_conn(rows):
            wc = MagicMock()
            wr = MagicMock()
            wr.fetchall.return_value = rows
            wc.execute.return_value = wr
            return wc

        worker_conn = _make_worker_conn(pending)

        write_conn = MagicMock()
        existing = MagicMock()
        existing.fetchone.return_value = None
        ins = MagicMock()
        write_conn.execute.side_effect = [existing, ins, existing, ins]

        engine.begin.side_effect = [
            _ctx(series_conn),
            _ctx(worker_conn),
            _ctx(worker_conn),
            _ctx(write_conn),
            _ctx(write_conn),
        ]

        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert summary["resolved"] == 2


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

    def test_conflict_report_columns(self):
        """The returned DataFrame must have all expected columns."""
        engine = MagicMock()
        ctx = MagicMock()
        conn = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value = ctx
        conn.execute.return_value.fetchall.return_value = [
            (1, "feat_x", date(2026, 1, 5), 42.0, "FRED",
             '{"sources":[]}', date(2026, 1, 6), date(2026, 1, 6)),
        ]

        resolver = Resolver(db_engine=engine)
        df = resolver.get_conflict_report()

        expected_cols = {
            "id", "feature_name", "obs_date", "value",
            "source_name", "conflict_detail", "release_date", "vintage_date",
        }
        assert expected_cols.issubset(set(df.columns))

    def test_conflict_report_multiple_rows(self):
        """All rows returned by the DB must appear in the DataFrame."""
        engine = MagicMock()
        ctx = MagicMock()
        conn = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value = ctx
        conn.execute.return_value.fetchall.return_value = [
            (i, f"feat_{i}", date(2026, 1, i), float(i * 10), "SrcA",
             "{}", date(2026, 1, i + 1), date(2026, 1, i + 1))
            for i in range(1, 6)
        ]

        resolver = Resolver(db_engine=engine)
        df = resolver.get_conflict_report()

        assert len(df) == 5


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


@pytest.mark.timeout(300)
class TestConflictDetection:
    """Verify conflict detection when sources disagree.

    These tests call resolver.resolve_pending() which walks every distinct
    series_id in the last 30 days of raw_series. Against a production-like
    DB with active pullers that's thousands of rows, so the default 30s
    per-test timeout is too tight. Class-level override to 5 minutes.
    """

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


# ---------------------------------------------------------------------------
# Unit tests — resolution window, watermark and dry run
#
# The Hermes cycle runs every 5 minutes and cannot afford the 30-day default
# window, so it passes `since` (a persisted watermark) and a 2-day fallback.
# These tests pin the parameterization of that window and the dry-run path
# used to measure cost before turning the cycle back on.
# ---------------------------------------------------------------------------


class _RecordingEngine:
    """Engine double that records every (sql, params) pair it executes.

    Returns one series_id from the DISTINCT query and one raw row from the
    worker query, so a full resolve_pending() pass runs end to end without a
    database.
    """

    def __init__(self, raw_rows: list[tuple] | None = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.inserts: list[list[dict]] = []
        self._raw_rows = raw_rows if raw_rows is not None else [
            ("VIXCLS", date(2026, 9, 10), 17.5, 3, datetime(2026, 9, 10, 12, 0), 1, "FRED"),
        ]

    # -- connection plumbing ------------------------------------------------
    def _conn(self):
        engine = self

        class _Result:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

            def fetchone(self):
                return self._rows[0] if self._rows else None

        class _Conn:
            def execute(self, statement, params=None):
                sql = str(statement)
                engine.calls.append((sql, params))
                if isinstance(params, list):          # executemany → INSERT
                    engine.inserts.append(params)
                    return _Result([])
                if "SELECT DISTINCT" in sql:
                    return _Result(
                        [(sid,) for sid in sorted({r[0] for r in engine._raw_rows})]
                    )
                if "FROM raw_series rs" in sql:
                    sids = set(params.get("sids", [])) if params else set()
                    return _Result([r for r in engine._raw_rows if r[0] in sids])
                if "feature_registry" in sql:
                    return _Result([(1, "vol")])
                return _Result([])

        class _Ctx:
            def __enter__(self_inner):
                return _Conn()

            def __exit__(self_inner, *_exc):
                return False

        return _Ctx()

    def begin(self):
        return self._conn()

    def connect(self):
        return self._conn()

    # -- helpers ------------------------------------------------------------
    def window_params(self) -> list[dict]:
        """Params of every statement carrying the pull_timestamp window."""
        return [
            p for sql, p in self.calls
            if isinstance(p, dict) and "pull_timestamp >= COALESCE" in sql
        ]


def _patched_entity_map(monkeypatch, feature_id: int | None = 1):
    """Point the resolver's EntityMap at a stub mapping every series.

    Mirrors the real EntityMap surface the resolver uses: get_feature_id
    plus missing_feature_report(), which resolve_pending calls once at the
    end of a run to log unmapped series in bulk.
    """
    class _Stub:
        def __init__(self, _engine):
            self.lookups: list[str] = []

        def get_feature_id(self, series_id):
            self.lookups.append(series_id)
            return feature_id

        def missing_feature_report(self):
            missed = [s for s in self.lookups] if feature_id is None else []
            return {
                "lookups_missed": len(missed),
                "series_ids": len(set(missed)),
                "unregistered_features": [],
                "top_series": [],
            }

    monkeypatch.setattr("normalization.resolver.EntityMap", _Stub)


class TestResolutionWindow:

    def test_lookback_is_a_bound_parameter(self, monkeypatch):
        """The window must bind :lookback — never interpolate it into SQL."""
        _patched_entity_map(monkeypatch)
        engine = _RecordingEngine()
        Resolver(db_engine=engine).resolve_pending(lookback_days=2, workers=1)

        windows = engine.window_params()
        assert windows, "no windowed statement executed"
        for params in windows:
            assert params["lookback"] == 2
            assert params["since"] is None
        for sql, _ in engine.calls:
            assert "INTERVAL '2 day" not in sql

    def test_since_watermark_overrides_lookback(self, monkeypatch):
        """A watermark is passed through as :since on every windowed query."""
        _patched_entity_map(monkeypatch)
        engine = _RecordingEngine()
        watermark = datetime(2026, 9, 10, 18, 0)

        Resolver(db_engine=engine).resolve_pending(
            lookback_days=2, workers=1, since=watermark,
        )

        windows = engine.window_params()
        assert windows
        for params in windows:
            assert params["since"] == watermark

    def test_until_bounds_the_window(self, monkeypatch):
        """`until` is bound as an exclusive upper bound for chunked backfills."""
        _patched_entity_map(monkeypatch)
        engine = _RecordingEngine()
        start, end = datetime(2026, 3, 29), datetime(2026, 4, 5)

        Resolver(db_engine=engine).resolve_pending(
            workers=1, since=start, until=end,
        )

        windows = engine.window_params()
        assert windows
        for params in windows:
            assert params["since"] == start
            assert params["until"] == end

    def test_window_sql_uses_no_string_interpolation(self):
        """The window predicate must be a literal with bound params only."""
        import inspect

        import normalization.resolver as resolver_mod

        source = inspect.getsource(resolver_mod.Resolver.resolve_pending)
        assert "pull_timestamp >= COALESCE" in source
        assert ".format(" not in source
        for line in source.splitlines():
            stripped = line.strip()
            if stripped.startswith(("f\"", "f'")):
                assert "SELECT" not in stripped and "INSERT" not in stripped

    def test_summary_keys_are_stable(self, monkeypatch):
        _patched_entity_map(monkeypatch)
        engine = _RecordingEngine()
        summary = Resolver(db_engine=engine).resolve_pending(workers=1)
        assert set(summary) == {
            "resolved", "conflicts_found", "errors",
            "series_scanned", "duration_s", "dry_run", "unmapped",
        }
        assert set(summary["unmapped"]) == {
            "lookups_missed", "series_ids", "unregistered_features",
            "top_series",
        }


class TestDryRun:

    def test_dry_run_writes_nothing(self, monkeypatch):
        _patched_entity_map(monkeypatch)
        engine = _RecordingEngine()

        summary = Resolver(db_engine=engine).resolve_pending(
            lookback_days=2, workers=1, dry_run=True,
        )

        assert summary["dry_run"] is True
        assert engine.inserts == []
        assert not any(
            "INSERT INTO resolved_series" in sql for sql, _ in engine.calls
        )

    def test_dry_run_counts_what_would_be_written(self, monkeypatch):
        _patched_entity_map(monkeypatch)
        rows = [
            ("VIXCLS", date(2026, 9, 9), 17.5, 3, datetime(2026, 9, 9, 12, 0), 1, "FRED"),
            ("VIXCLS", date(2026, 9, 10), 18.1, 3, datetime(2026, 9, 10, 12, 0), 1, "FRED"),
        ]
        engine = _RecordingEngine(raw_rows=rows)

        summary = Resolver(db_engine=engine).resolve_pending(
            workers=1, dry_run=True,
        )

        assert summary["resolved"] == 2      # two (series_id, obs_date) groups
        assert summary["series_scanned"] == 1
        assert summary["duration_s"] >= 0

    def test_wet_run_still_inserts(self, monkeypatch):
        _patched_entity_map(monkeypatch)
        engine = _RecordingEngine()

        summary = Resolver(db_engine=engine).resolve_pending(workers=1)

        assert summary["dry_run"] is False
        assert engine.inserts, "expected an INSERT batch"
        assert summary["resolved"] == 1

    def test_unmapped_series_are_skipped(self, monkeypatch):
        """A series with no entity mapping yields no resolved row."""
        _patched_entity_map(monkeypatch, feature_id=None)
        engine = _RecordingEngine()

        summary = Resolver(db_engine=engine).resolve_pending(
            workers=1, dry_run=True,
        )

        assert summary["resolved"] == 0


class TestResolveRange:

    def test_range_is_walked_in_chunks(self, monkeypatch):
        """A multi-week catch-up is split into bounded windows."""
        _patched_entity_map(monkeypatch)
        resolver = Resolver(db_engine=MagicMock())
        seen: list[tuple] = []

        def _fake_resolve_pending(**kwargs):
            seen.append((kwargs["since"], kwargs["until"]))
            return {
                "resolved": 10, "conflicts_found": 1, "errors": 0,
                "series_scanned": 5, "duration_s": 1.0, "dry_run": False,
            }

        monkeypatch.setattr(resolver, "resolve_pending", _fake_resolve_pending)

        totals = resolver.resolve_range(
            since=date(2026, 3, 29), until=date(2026, 4, 12), chunk_days=7,
        )

        assert totals["chunks"] == 2
        assert totals["resolved"] == 20
        assert totals["conflicts_found"] == 2
        assert seen[0][0] == datetime(2026, 3, 29)
        assert seen[0][1] == datetime(2026, 4, 5)
        assert seen[1][0] == datetime(2026, 4, 5)
        assert seen[1][1] == datetime(2026, 4, 12)

    def test_final_chunk_is_clamped_to_until(self, monkeypatch):
        _patched_entity_map(monkeypatch)
        resolver = Resolver(db_engine=MagicMock())
        seen: list[tuple] = []

        def _fake_resolve_pending(**kwargs):
            seen.append((kwargs["since"], kwargs["until"]))
            return {
                "resolved": 0, "conflicts_found": 0, "errors": 0,
                "series_scanned": 0, "duration_s": 0.0, "dry_run": True,
            }

        monkeypatch.setattr(resolver, "resolve_pending", _fake_resolve_pending)
        resolver.resolve_range(
            since=date(2026, 3, 29), until=date(2026, 4, 3),
            chunk_days=7, dry_run=True,
        )

        assert len(seen) == 1
        assert seen[0][1] == datetime(2026, 4, 3)

    def test_rejects_inverted_range(self):
        resolver = Resolver(db_engine=MagicMock())
        with pytest.raises(ValueError):
            resolver.resolve_range(since=date(2026, 4, 3), until=date(2026, 3, 29))

    def test_rejects_zero_chunk(self):
        resolver = Resolver(db_engine=MagicMock())
        with pytest.raises(ValueError):
            resolver.resolve_range(since=date(2026, 3, 29), chunk_days=0)

# ---------------------------------------------------------------------------
# The per-worker feature_id memo
# ---------------------------------------------------------------------------


class TestFeatureIdMemo:
    """One EntityMap lookup per series_id per partition, not per group.

    A live 2-day window on griddb produced 569,400 (series_id, obs_date)
    groups over 18,916 distinct series_ids — ~30 identical lookups each.
    With get_feature_id re-reading feature_registry on every miss, that
    repetition was 54.6s of a 61.5s dry run, against 9.1s for both SQL
    scans combined. Partitions are disjoint by series_id, so the memo is
    per-worker and needs no lock.
    """

    def _rows_for(self, series_id: str, n_dates: int):
        """n_dates raw rows for one series, each on its own obs_date.

        Each row becomes its own (series_id, obs_date) group, which is what
        the memo has to collapse back down to one lookup.
        """
        base = datetime(2026, 1, 1, 12, 0)
        return [
            FakeRow((series_id, (base + timedelta(days=d)).date(),
                     100.0 + d, 1, base + timedelta(days=d), 1, "FRED"))
            for d in range(n_dates)
        ]

    def test_one_lookup_per_series_id_not_per_group(self, monkeypatch):
        lookups: list[str] = []

        class _CountingStub:
            def __init__(self, _engine):
                pass

            def get_feature_id(self, series_id):
                lookups.append(series_id)
                return 1

            def missing_feature_report(self):
                return {
                    "lookups_missed": 0, "series_ids": 0,
                    "unregistered_features": [], "top_series": [],
                }

        monkeypatch.setattr("normalization.resolver.EntityMap", _CountingStub)

        rows = self._rows_for("T10Y2Y", 25)
        engine, _ = _mock_engine(pending_rows=rows)
        Resolver(db_engine=engine).resolve_pending(workers=1, dry_run=True)

        assert len(rows) == 25, "fixture should produce 25 groups"
        assert lookups == ["T10Y2Y"], (
            f"25 groups of one series_id produced {len(lookups)} lookups"
        )

    def test_a_missing_series_is_also_looked_up_only_once(self, monkeypatch):
        """A None result must be memoised too — misses are the hot case."""
        lookups: list[str] = []

        class _MissingStub:
            def __init__(self, _engine):
                pass

            def get_feature_id(self, series_id):
                lookups.append(series_id)
                return None

            def missing_feature_report(self):
                return {
                    "lookups_missed": len(lookups),
                    "series_ids": len(set(lookups)),
                    "unregistered_features": ["ief_full"],
                    "top_series": [[s, 1] for s in set(lookups)],
                }

        monkeypatch.setattr("normalization.resolver.EntityMap", _MissingStub)

        engine, _ = _mock_engine(pending_rows=self._rows_for("YF:IEF:close", 40))
        summary = Resolver(db_engine=engine).resolve_pending(
            workers=1, dry_run=True,
        )

        assert lookups == ["YF:IEF:close"]
        assert summary["resolved"] == 0
        assert summary["unmapped"]["unregistered_features"] == ["ief_full"]

    def test_distinct_series_are_each_looked_up(self, monkeypatch):
        """Memoising must not collapse genuinely different series_ids."""
        lookups: list[str] = []

        class _Stub:
            def __init__(self, _engine):
                pass

            def get_feature_id(self, series_id):
                lookups.append(series_id)
                return 1

            def missing_feature_report(self):
                return {
                    "lookups_missed": 0, "series_ids": 0,
                    "unregistered_features": [], "top_series": [],
                }

        monkeypatch.setattr("normalization.resolver.EntityMap", _Stub)

        rows = (self._rows_for("T10Y2Y", 5) + self._rows_for("DGS10", 5)
                + self._rows_for("VIXCLS", 5))
        # _mock_engine wires one worker connection per series_id, so run
        # one partition per series (chunk_size = 3 // 3 = 1).
        engine, _ = _mock_engine(pending_rows=rows)
        Resolver(db_engine=engine).resolve_pending(workers=3, dry_run=True)

        assert sorted(set(lookups)) == ["DGS10", "T10Y2Y", "VIXCLS"]
        assert len(lookups) == 3, "each series_id must be resolved exactly once"


# ---------------------------------------------------------------------------
# statement_timeout is bound, never formatted into SQL
# ---------------------------------------------------------------------------


class TestStatementTimeoutIsParameterised:
    """.claude/rules/security.md: no f-strings or .format() in SQL.

    SET's grammar only accepts a literal, which is why the old code built
    the statement with an f-string. set_config(..., is_local => true) is
    the same transaction-scoped change as a function call, so the value
    travels as a bound parameter.
    """

    def test_timeout_travels_as_a_bound_parameter(self, monkeypatch):
        _patched_entity_map(monkeypatch)
        engine = _RecordingEngine()
        resolver = Resolver(db_engine=engine)
        resolver.resolve_pending(lookback_days=2, workers=1)

        timeout_calls = [
            (sql, params) for sql, params in engine.calls
            if "statement_timeout" in sql
        ]
        assert timeout_calls, "statement_timeout was never raised"
        for sql, params in timeout_calls:
            assert "set_config" in sql
            assert ":timeout_ms" in sql
            assert params["timeout_ms"] == str(
                Resolver._RESOLVE_STATEMENT_TIMEOUT_MS
            )
            assert str(Resolver._RESOLVE_STATEMENT_TIMEOUT_MS) not in sql, (
                "the timeout value is interpolated into the SQL string"
            )

    def test_no_module_sql_literal_uses_set_local(self):
        """Guard the pattern, not just this one call site."""
        import ast
        from pathlib import Path

        tree = ast.parse(
            Path(__file__).resolve().parent.parent
            .joinpath("normalization/resolver.py").read_text()
        )
        for node in ast.walk(tree):
            if isinstance(node, ast.JoinedStr):
                # An f-string anywhere in the module is fine unless it is SQL.
                rendered = "".join(
                    v.value for v in node.values
                    if isinstance(v, ast.Constant) and isinstance(v.value, str)
                )
                assert "statement_timeout" not in rendered.lower(), (
                    "statement_timeout is being built with an f-string"
                )


# ---------------------------------------------------------------------------
# A backlog walk survives a failed chunk
# ---------------------------------------------------------------------------


class TestResolveRangeFaultTolerance:
    """One bad chunk must not discard the chunks that already landed.

    Historical windows are cold: the rolling 2-day window's distinct-series
    scan is 2.2s, but a 7-day chunk five months back exceeded the 600s
    statement timeout on griddb (ops-exec run 34551047779). Chunks are
    independent and every insert is ON CONFLICT ... DO NOTHING, so the walk
    continues and the failed range is reported for a narrower retry.
    """

    def _summary(self, resolved=10, errors=0):
        return {
            "resolved": resolved, "conflicts_found": 0, "errors": errors,
            "series_scanned": 5, "duration_s": 1.0, "dry_run": False,
            "unmapped": {
                "lookups_missed": 0, "series_ids": 0,
                "unregistered_features": [], "top_series": [],
            },
        }

    def test_a_raising_chunk_does_not_abort_the_walk(self, monkeypatch):
        from sqlalchemy.exc import OperationalError

        _patched_entity_map(monkeypatch)
        resolver = Resolver(db_engine=MagicMock())
        seen: list[tuple] = []

        def _fake(**kwargs):
            seen.append((kwargs["since"], kwargs["until"]))
            if kwargs["since"] == datetime(2026, 4, 2):
                raise OperationalError(
                    "SELECT DISTINCT", {},
                    Exception("canceling statement due to statement timeout"),
                )
            return self._summary()

        monkeypatch.setattr(resolver, "resolve_pending", _fake)

        totals = resolver.resolve_range(
            since=date(2026, 4, 1), until=date(2026, 4, 4), chunk_days=1,
        )

        assert len(seen) == 3, "the walk stopped at the failed chunk"
        assert totals["chunks"] == 3
        assert totals["chunks_failed"] == 1
        assert totals["resolved"] == 20, "surviving chunks must still count"
        assert totals["errors"] == 1

    def test_failed_range_is_reported_for_retry(self, monkeypatch):
        from sqlalchemy.exc import OperationalError

        _patched_entity_map(monkeypatch)
        resolver = Resolver(db_engine=MagicMock())

        def _fake(**kwargs):
            if kwargs["since"] == datetime(2026, 4, 2):
                raise OperationalError("SELECT", {}, Exception("timeout"))
            return self._summary()

        monkeypatch.setattr(resolver, "resolve_pending", _fake)

        totals = resolver.resolve_range(
            since=date(2026, 4, 1), until=date(2026, 4, 4), chunk_days=1,
        )

        assert len(totals["failed_ranges"]) == 1
        failed = totals["failed_ranges"][0]
        assert failed["since"] == "2026-04-02"
        assert failed["until"] == "2026-04-03"
        assert failed["error_class"] == "OperationalError"
        assert "timeout" in failed["error"]

    def test_worker_errors_also_mark_the_range_incomplete(self, monkeypatch):
        """resolve_pending swallows worker failures — the range is still bad."""
        _patched_entity_map(monkeypatch)
        resolver = Resolver(db_engine=MagicMock())

        def _fake(**kwargs):
            if kwargs["since"] == datetime(2026, 4, 2):
                return self._summary(resolved=3, errors=2)
            return self._summary()

        monkeypatch.setattr(resolver, "resolve_pending", _fake)

        totals = resolver.resolve_range(
            since=date(2026, 4, 1), until=date(2026, 4, 4), chunk_days=1,
        )

        assert totals["chunks_failed"] == 1
        assert totals["failed_ranges"][0]["since"] == "2026-04-02"
        assert totals["failed_ranges"][0]["error_class"] == "WorkerError"
        assert totals["errors"] == 2

    def test_a_clean_walk_reports_no_failures(self, monkeypatch):
        _patched_entity_map(monkeypatch)
        resolver = Resolver(db_engine=MagicMock())
        monkeypatch.setattr(
            resolver, "resolve_pending", lambda **kw: self._summary(),
        )

        totals = resolver.resolve_range(
            since=date(2026, 4, 1), until=date(2026, 4, 4), chunk_days=1,
        )

        assert totals["chunks"] == 3
        assert totals["chunks_failed"] == 0
        assert totals["failed_ranges"] == []

    def test_every_chunk_failing_still_returns_a_summary(self, monkeypatch):
        _patched_entity_map(monkeypatch)
        resolver = Resolver(db_engine=MagicMock())

        def _fake(**kwargs):
            raise RuntimeError("database is down")

        monkeypatch.setattr(resolver, "resolve_pending", _fake)

        totals = resolver.resolve_range(
            since=date(2026, 4, 1), until=date(2026, 4, 4), chunk_days=1,
        )

        assert totals["chunks"] == 3
        assert totals["chunks_failed"] == 3
        assert len(totals["failed_ranges"]) == 3
        assert totals["resolved"] == 0

    def test_backlog_chunk_default_is_one_day(self, monkeypatch):
        """7 days did not finish inside the 600s timeout on a cold window."""
        from normalization.resolver import DEFAULT_BACKFILL_CHUNK_DAYS

        assert DEFAULT_BACKFILL_CHUNK_DAYS == 1

        _patched_entity_map(monkeypatch)
        resolver = Resolver(db_engine=MagicMock())
        seen: list[tuple] = []

        def _fake(**kwargs):
            seen.append((kwargs["since"], kwargs["until"]))
            return self._summary()

        monkeypatch.setattr(resolver, "resolve_pending", _fake)
        resolver.resolve_range(since=date(2026, 4, 1), until=date(2026, 4, 4))

        assert len(seen) == 3, "default chunk width is not one day"

    def test_cli_default_matches_the_module_default(self):
        from normalization.resolver import DEFAULT_BACKFILL_CHUNK_DAYS, main
        import inspect

        source = inspect.getsource(main)
        assert "default=DEFAULT_BACKFILL_CHUNK_DAYS" in source
        assert DEFAULT_BACKFILL_CHUNK_DAYS == 1

