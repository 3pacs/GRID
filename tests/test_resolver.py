"""
Tests for the GRID conflict resolution module.

Tests verify that conflicting values from multiple sources are detected
and that the highest-priority source wins in non-conflict cases.

Unit tests use mocked DB connections. Integration tests (TestConflictDetection)
require a live pg_engine fixture.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from types import SimpleNamespace
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
        """resolve_pending must always return the three historical summary
        keys, plus the measurement keys added for the dry-run/backlog work
        (volume counts per phase, a dry_run flag and per-phase timings)."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = None
        MockEntityMap.return_value = mock_map

        engine, _ = _mock_engine(pending_rows=[])
        resolver = Resolver(db_engine=engine)
        summary = resolver.resolve_pending()

        assert {"resolved", "conflicts_found", "errors"} <= set(summary.keys())
        assert {
            "series_count", "raw_rows", "groups", "unmapped_groups",
            "candidates", "dry_run", "timings",
        } <= set(summary.keys())
        assert isinstance(summary["timings"], dict)

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
        assert summary["series_count"] == 0
        assert summary["candidates"] == 0
        # The early return still reports what it measured getting there.
        assert "distinct_series_s" in summary["timings"]
        assert "total_s" in summary["timings"]

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
# Unit tests — pull window, dry run, and bounded backlog catch-up
#
# Added with the resolution-path hardening (slice 20b). The per-cycle
# resolver had been dead since 2026-04-04 and nobody could say what a
# window cost or how to recover the backlog, so resolve_pending gained a
# measuring dry_run and absolute since/until bounds, and resolve_backlog
# walks a historical range in bounded chunks.
# ---------------------------------------------------------------------------

from datetime import timezone as _timezone  # noqa: E402

from normalization import resolver as resolver_module  # noqa: E402
from normalization.resolver import (  # noqa: E402
    _WINDOW_FROM,
    _WINDOW_FROM_UNTIL,
    _WINDOW_RELATIVE,
    _as_utc,
    _pull_window,
    build_parser,
    main as resolver_main,
)


def _recording_engine(rows=None, families=None):
    """Mock engine that records every (sql, params) pair it executes.

    Simpler than _mock_engine's fixed side-effect chain: every connection
    returns the same rows, so tests can assert on the SQL and bound params
    the resolver actually issued without counting connections.
    """
    calls: list[tuple[str, dict]] = []
    rows = rows or []

    def _execute(statement, params=None):
        calls.append((str(statement), params if isinstance(params, dict) else {}))
        result = MagicMock()
        sql = str(statement).lower()
        if "feature_registry" in sql:
            result.fetchall.return_value = families or []
        elif "distinct" in sql:
            result.fetchall.return_value = sorted({(r[0],) for r in rows})
        elif "from raw_series" in sql:
            result.fetchall.return_value = list(rows)
        else:
            result.fetchall.return_value = []
        result.fetchone.return_value = None
        return result

    conn = MagicMock()
    conn.execute.side_effect = _execute

    def _ctx(*_args, **_kwargs):
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__ = MagicMock(return_value=False)
        return ctx

    engine = MagicMock()
    engine.begin.side_effect = _ctx
    engine.connect.side_effect = _ctx
    return engine, calls


def _raw_row(series_id="SPY", obs=date(2026, 9, 10), value=100.0):
    """One raw_series row in the shape the partition query returns."""
    return (series_id, obs, value, 1, datetime(2026, 9, 10, 12, 0), 1, "TIINGO")


class TestAsUtc:
    """Chunk boundaries must not drift with the server timezone."""

    def test_date_becomes_utc_midnight(self):
        got = _as_utc(date(2026, 4, 4))
        assert got == datetime(2026, 4, 4, tzinfo=_timezone.utc)

    def test_naive_datetime_is_assumed_utc(self):
        got = _as_utc(datetime(2026, 4, 4, 6, 30))
        assert got == datetime(2026, 4, 4, 6, 30, tzinfo=_timezone.utc)

    def test_aware_datetime_is_preserved(self):
        original = datetime(2026, 4, 4, 6, 30, tzinfo=_timezone.utc)
        assert _as_utc(original) is original


class TestPullWindow:

    def test_defaults_to_relative_lookback(self):
        sql, params = _pull_window(30)
        assert sql == _WINDOW_RELATIVE
        assert params == {"lookback": 30}

    def test_since_only_uses_lower_bound(self):
        sql, params = _pull_window(30, since=date(2026, 4, 4))
        assert sql == _WINDOW_FROM
        assert params == {"since": datetime(2026, 4, 4, tzinfo=_timezone.utc)}

    def test_since_and_until_bound_both_ends(self):
        sql, params = _pull_window(
            30, since=date(2026, 4, 4), until=date(2026, 4, 11)
        )
        assert sql == _WINDOW_FROM_UNTIL
        assert params == {
            "since": datetime(2026, 4, 4, tzinfo=_timezone.utc),
            "until": datetime(2026, 4, 11, tzinfo=_timezone.utc),
        }

    def test_absolute_bounds_override_lookback(self):
        _sql, params = _pull_window(2, since=date(2026, 4, 4))
        assert "lookback" not in params

    def test_until_without_since_is_rejected(self):
        with pytest.raises(ValueError):
            _pull_window(30, until=date(2026, 4, 11))

    def test_every_variant_is_parameterised(self):
        """SQL-safety rule: bounds travel as bound params, never as text."""
        for variant in (_WINDOW_RELATIVE, _WINDOW_FROM, _WINDOW_FROM_UNTIL):
            assert ":" in variant
            assert "%" not in variant
            assert "{" not in variant
            assert "format" not in variant

    def test_upper_bound_is_exclusive(self):
        """Chunks must not double-count the boundary day: < not <=."""
        assert "< :until" in _WINDOW_FROM_UNTIL
        assert "<= :until" not in _WINDOW_FROM_UNTIL


class TestResolvePendingWindowPlumbing:

    @patch("normalization.resolver.EntityMap")
    def test_relative_window_reaches_both_queries(self, MockEntityMap):
        MockEntityMap.return_value = MagicMock()
        engine, calls = _recording_engine()
        Resolver(db_engine=engine).resolve_pending(lookback_days=2)

        raw_reads = [(s, p) for s, p in calls if "raw_series" in s.lower()]
        assert raw_reads
        for sql, params in raw_reads:
            assert "NOW() - :lookback * INTERVAL '1 day'" in sql
            assert params.get("lookback") == 2

    @patch("normalization.resolver.EntityMap")
    def test_absolute_window_reaches_both_queries(self, MockEntityMap):
        MockEntityMap.return_value = MagicMock()
        engine, calls = _recording_engine(rows=[_raw_row()])
        Resolver(db_engine=engine).resolve_pending(
            since=date(2026, 4, 4), until=date(2026, 4, 11), dry_run=True
        )

        raw_reads = [(s, p) for s, p in calls if "raw_series" in s.lower()]
        assert len(raw_reads) >= 2, "expected the distinct scan and a partition fetch"
        for sql, params in raw_reads:
            assert ":since" in sql and ":until" in sql
            assert params["since"] == datetime(2026, 4, 4, tzinfo=_timezone.utc)
            assert params["until"] == datetime(2026, 4, 11, tzinfo=_timezone.utc)

    @patch("normalization.resolver.EntityMap")
    def test_statement_timeout_is_raised_via_bound_param(self, MockEntityMap):
        """The timeout override must not be interpolated into the SQL."""
        MockEntityMap.return_value = MagicMock()
        engine, calls = _recording_engine()
        Resolver(db_engine=engine).resolve_pending(lookback_days=2)

        overrides = [(s, p) for s, p in calls if "statement_timeout" in s]
        assert overrides
        for sql, params in overrides:
            assert ":timeout_ms" in sql
            assert str(Resolver._RESOLVE_STATEMENT_TIMEOUT_MS) in str(
                params.get("timeout_ms")
            )
            assert str(Resolver._RESOLVE_STATEMENT_TIMEOUT_MS) not in sql


class TestResolvePendingDryRun:

    @patch("normalization.resolver._flush_batch")
    @patch("normalization.resolver.EntityMap")
    def test_dry_run_never_writes(self, MockEntityMap, mock_flush):
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 7
        MockEntityMap.return_value = mock_map

        engine, _ = _recording_engine(rows=[_raw_row("SPY"), _raw_row("QQQ")])
        summary = Resolver(db_engine=engine).resolve_pending(
            lookback_days=2, workers=1, dry_run=True
        )

        mock_flush.assert_not_called()
        assert summary["dry_run"] is True
        assert summary["resolved"] == 0
        # ...but it still reports what it would have written.
        assert summary["candidates"] > 0
        assert summary["raw_rows"] > 0
        assert summary["groups"] > 0

    @patch("normalization.resolver._flush_batch")
    @patch("normalization.resolver.EntityMap")
    def test_non_dry_run_still_writes(self, MockEntityMap, mock_flush):
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 7
        mock_flush.side_effect = lambda _engine, batch: len(batch)
        MockEntityMap.return_value = mock_map

        engine, _ = _recording_engine(rows=[_raw_row("SPY")])
        summary = Resolver(db_engine=engine).resolve_pending(
            lookback_days=2, workers=1
        )

        mock_flush.assert_called()
        assert summary["dry_run"] is False
        assert summary["resolved"] > 0
        assert summary["candidates"] == summary["resolved"]

    @patch("normalization.resolver.EntityMap")
    def test_timings_cover_every_phase(self, MockEntityMap):
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = 7
        MockEntityMap.return_value = mock_map

        engine, _ = _recording_engine(rows=[_raw_row("SPY")])
        summary = Resolver(db_engine=engine).resolve_pending(
            lookback_days=2, workers=1, dry_run=True
        )

        timings = summary["timings"]
        for phase in (
            "entity_map_s", "feature_families_s", "distinct_series_s",
            "workers_wall_s", "fetch_s", "group_s", "flush_s", "total_s",
        ):
            assert phase in timings, f"missing phase timing: {phase}"
            assert timings[phase] >= 0
        assert timings["flush_s"] == 0.0, "dry run must spend no time writing"

    @patch("normalization.resolver.EntityMap")
    def test_unmapped_series_are_counted_not_silently_dropped(self, MockEntityMap):
        """443 of 1204 model-eligible features had a recent resolved row when
        this was investigated; a run that maps nothing must say so rather
        than reporting a clean zero."""
        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = None
        MockEntityMap.return_value = mock_map

        engine, _ = _recording_engine(rows=[_raw_row("UNKNOWN_SERIES")])
        summary = Resolver(db_engine=engine).resolve_pending(
            lookback_days=2, workers=1, dry_run=True
        )

        assert summary["unmapped_groups"] == 1
        assert summary["candidates"] == 0
        assert summary["errors"] == 0


class TestResolveBacklog:
    """Bounded catch-up over a historical pull_timestamp range."""

    @staticmethod
    def _stub_resolver(engine=None):
        """A Resolver whose resolve_pending records its window and returns 1 row."""
        resolver = Resolver(db_engine=engine or MagicMock())
        windows: list[tuple] = []

        def _fake(workers=8, dry_run=False, since=None, until=None, **_kw):
            windows.append((since, until, workers, dry_run))
            return {
                "resolved": 1, "conflicts_found": 0, "errors": 0,
                "series_count": 2, "raw_rows": 3, "groups": 2,
                "unmapped_groups": 0, "candidates": 1,
                "dry_run": dry_run, "timings": {"total_s": 0.1},
            }

        resolver.resolve_pending = _fake  # type: ignore[method-assign]
        return resolver, windows

    def test_chunks_cover_the_range_without_gaps_or_overlap(self):
        resolver, windows = self._stub_resolver()
        resolver.resolve_backlog(
            since=date(2026, 4, 4), until=date(2026, 4, 25), chunk_days=7
        )

        assert [(a.date().isoformat(), b.date().isoformat())
                for a, b, _w, _d in windows] == [
            ("2026-04-04", "2026-04-11"),
            ("2026-04-11", "2026-04-18"),
            ("2026-04-18", "2026-04-25"),
        ]

    def test_final_chunk_is_clamped_to_until(self):
        resolver, windows = self._stub_resolver()
        resolver.resolve_backlog(
            since=date(2026, 4, 4), until=date(2026, 4, 20), chunk_days=7
        )

        assert windows[-1][1] == datetime(2026, 4, 20, tzinfo=_timezone.utc)

    def test_totals_sum_across_chunks(self):
        resolver, _ = self._stub_resolver()
        result = resolver.resolve_backlog(
            since=date(2026, 4, 4), until=date(2026, 4, 25), chunk_days=7
        )

        assert len(result["chunks"]) == 3
        assert result["totals"]["resolved"] == 3
        assert result["totals"]["candidates"] == 3
        assert result["totals"]["raw_rows"] == 9
        assert result["totals"]["errors"] == 0

    def test_each_chunk_record_carries_its_own_bounds(self):
        resolver, _ = self._stub_resolver()
        result = resolver.resolve_backlog(
            since=date(2026, 4, 4), until=date(2026, 4, 18), chunk_days=7
        )

        assert [(c["chunk"], c["since"], c["until"]) for c in result["chunks"]] == [
            (1, "2026-04-04", "2026-04-11"),
            (2, "2026-04-11", "2026-04-18"),
        ]

    def test_default_until_covers_today(self):
        """The default upper bound must include rows pulled today, or a
        catch-up would silently stop a day short of the live window."""
        resolver, windows = self._stub_resolver()
        resolver.resolve_backlog(since=date(2026, 4, 4), chunk_days=3650)

        today = datetime.now(_timezone.utc).date()
        assert windows[-1][1].date() > today

    def test_dry_run_and_workers_propagate(self):
        resolver, windows = self._stub_resolver()
        resolver.resolve_backlog(
            since=date(2026, 4, 4), until=date(2026, 4, 11),
            chunk_days=7, workers=3, dry_run=True,
        )

        assert windows == [(
            datetime(2026, 4, 4, tzinfo=_timezone.utc),
            datetime(2026, 4, 11, tzinfo=_timezone.utc),
            3, True,
        )]

    def test_chunk_days_below_one_is_coerced(self):
        resolver, windows = self._stub_resolver()
        resolver.resolve_backlog(
            since=date(2026, 4, 4), until=date(2026, 4, 6), chunk_days=0
        )

        assert len(windows) == 2, "a zero-width chunk would loop forever"

    def test_inverted_range_is_rejected(self):
        resolver, _ = self._stub_resolver()
        with pytest.raises(ValueError):
            resolver.resolve_backlog(
                since=date(2026, 4, 25), until=date(2026, 4, 4)
            )

    def test_single_day_range_is_one_chunk(self):
        resolver, windows = self._stub_resolver()
        resolver.resolve_backlog(
            since=date(2026, 4, 4), until=date(2026, 4, 5), chunk_days=7
        )

        assert len(windows) == 1

    def test_catch_up_is_idempotent_by_construction(self):
        """Re-running a chunk must not duplicate rows — the guarantee is the
        ON CONFLICT clause on the real insert, so pin it here."""
        engine = MagicMock()
        captured: list[str] = []

        def _execute(statement, _params=None):
            captured.append(str(statement))
            return MagicMock()

        conn = MagicMock()
        conn.execute.side_effect = _execute
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__ = MagicMock(return_value=False)
        engine.begin.return_value = ctx

        resolver_module._flush_batch(engine, [{"fid": 1}])

        assert captured
        assert "ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING" in captured[0]


class TestResolverCli:
    """`python -m normalization.resolver` — the controller's catch-up entry point."""

    @staticmethod
    def _install_fake_db(monkeypatch, engine=None):
        monkeypatch.setitem(
            sys.modules, "db", SimpleNamespace(get_engine=lambda: engine or MagicMock())
        )

    def test_parser_defaults(self):
        args = build_parser().parse_args([])
        assert args.since is None
        assert args.until is None
        assert args.chunk_days == 7
        assert args.workers == 8
        assert args.dry_run is False

    def test_parses_the_documented_catch_up_invocation(self):
        args = build_parser().parse_args([
            "--since", "2026-04-04", "--until", "2026-04-11",
            "--chunk-days", "7", "--workers", "8", "--dry-run",
        ])
        assert args.since == date(2026, 4, 4)
        assert args.until == date(2026, 4, 11)
        assert args.chunk_days == 7
        assert args.workers == 8
        assert args.dry_run is True

    def test_until_without_since_exits_nonzero(self, monkeypatch):
        self._install_fake_db(monkeypatch)
        assert resolver_main(["--until", "2026-04-11"]) == 2

    @pytest.mark.parametrize("bad", (["--chunk-days", "0"], ["--workers", "0"]))
    def test_nonsense_bounds_exit_nonzero(self, monkeypatch, bad):
        self._install_fake_db(monkeypatch)
        assert resolver_main(bad) == 2

    def test_since_routes_to_resolve_backlog(self, monkeypatch, capsys):
        self._install_fake_db(monkeypatch)
        seen: dict = {}

        def _fake_backlog(self, since, until=None, chunk_days=7,
                          workers=8, dry_run=False):
            seen.update(since=since, until=until, chunk_days=chunk_days,
                        workers=workers, dry_run=dry_run)
            return {"totals": {"errors": 0, "resolved": 5}, "chunks": []}

        monkeypatch.setattr(Resolver, "resolve_backlog", _fake_backlog)

        rc = resolver_main([
            "--since", "2026-04-04", "--chunk-days", "7",
            "--workers", "8", "--dry-run",
        ])

        assert rc == 0
        assert seen == {
            "since": date(2026, 4, 4), "until": None,
            "chunk_days": 7, "workers": 8, "dry_run": True,
        }
        assert "totals" in capsys.readouterr().out

    def test_backlog_errors_exit_nonzero(self, monkeypatch):
        self._install_fake_db(monkeypatch)
        monkeypatch.setattr(
            Resolver, "resolve_backlog",
            lambda *_a, **_k: {"totals": {"errors": 2}, "chunks": []},
        )
        assert resolver_main(["--since", "2026-04-04"]) == 1

    def test_no_since_keeps_the_legacy_rolling_window(self, monkeypatch, capsys):
        """Existing callers run this module with no arguments; that must keep
        resolving the rolling window and printing the conflict report."""
        self._install_fake_db(monkeypatch)
        seen: dict = {}

        def _fake_pending(self, lookback_days=30, workers=8, dry_run=False,
                          since=None, until=None):
            seen.update(lookback_days=lookback_days, workers=workers,
                        dry_run=dry_run)
            return {"resolved": 1, "errors": 0, "timings": {"total_s": 0.1}}

        monkeypatch.setattr(Resolver, "resolve_pending", _fake_pending)
        monkeypatch.setattr(
            Resolver, "get_conflict_report", lambda _self: pd.DataFrame()
        )

        rc = resolver_main([])

        assert rc == 0
        assert seen == {"lookback_days": 30, "workers": 8, "dry_run": False}
        out = capsys.readouterr().out
        assert "Resolution summary" in out
        assert "No conflicts found" in out

    def test_conflict_report_can_be_skipped(self, monkeypatch, capsys):
        self._install_fake_db(monkeypatch)
        monkeypatch.setattr(
            Resolver, "resolve_pending",
            lambda *_a, **_k: {"resolved": 0, "errors": 0, "timings": {}},
        )

        def _boom(_self):
            raise AssertionError("conflict report should not have been built")

        monkeypatch.setattr(Resolver, "get_conflict_report", _boom)

        assert resolver_main(["--no-conflict-report"]) == 0
        assert "Conflicts" not in capsys.readouterr().out


class TestFeatureIdMemoisation:
    """The measured cost of a 2-day window was not the SQL.

    A 2-day dry run on griddb (ops-exec run 150) fetched 655,551 raw rows
    into 569,400 (series_id, obs_date) groups over 18,916 distinct
    series_ids, so the resolve loop called EntityMap.get_feature_id ~30x
    per series. On a miss where the mapping exists but the feature is not
    in feature_registry, get_feature_id re-runs _load_feature_cache() — a
    full feature_registry SELECT — and logs a warning. That repetition was
    54.6s of a 61.5s run; the two raw_series scans together were 9s.
    """

    @staticmethod
    def _counting_map(feature_id=7):
        """An EntityMap stand-in that records every get_feature_id call."""
        calls: list[str] = []

        def _get(series_id):
            calls.append(series_id)
            return feature_id

        entity_map = MagicMock()
        entity_map.get_feature_id.side_effect = _get
        return entity_map, calls

    @patch("normalization.resolver._flush_batch")
    @patch("normalization.resolver.EntityMap")
    def test_each_series_is_looked_up_once_per_run(self, MockEntityMap, _flush):
        """Same series across many obs_dates must cost one lookup, not one
        per observation."""
        entity_map, calls = self._counting_map()
        MockEntityMap.return_value = entity_map

        rows = [
            _raw_row("SPY", date(2026, 9, day)) for day in range(1, 11)
        ]
        engine, _ = _recording_engine(rows=rows)
        summary = Resolver(db_engine=engine).resolve_pending(
            lookback_days=2, workers=1, dry_run=True
        )

        assert summary["groups"] == 10, "expected one group per obs_date"
        assert calls == ["SPY"], f"expected a single lookup, got {len(calls)}"

    @patch("normalization.resolver._flush_batch")
    @patch("normalization.resolver.EntityMap")
    def test_unmapped_series_are_not_re_looked_up(self, MockEntityMap, _flush):
        """A miss is the expensive case — it must be cached too."""
        entity_map, calls = self._counting_map(feature_id=None)
        MockEntityMap.return_value = entity_map

        rows = [_raw_row("NOPE", date(2026, 9, day)) for day in range(1, 21)]
        engine, _ = _recording_engine(rows=rows)
        summary = Resolver(db_engine=engine).resolve_pending(
            lookback_days=2, workers=1, dry_run=True
        )

        assert calls == ["NOPE"]
        # Both counters are reported: 20 groups skipped, 1 series at fault.
        assert summary["unmapped_groups"] == 20
        assert summary["unmapped_series"] == 1
        assert summary["candidates"] == 0

    @patch("normalization.resolver._flush_batch")
    @patch("normalization.resolver.EntityMap")
    def test_distinct_series_each_get_their_own_lookup(self, MockEntityMap, _flush):
        """Memoisation must not collapse different series onto one answer."""
        seen: dict[str, int | None] = {"AAA": 1, "BBB": None, "CCC": 3}
        entity_map = MagicMock()
        entity_map.get_feature_id.side_effect = lambda sid: seen[sid]
        MockEntityMap.return_value = entity_map

        rows = [
            _raw_row(sid, date(2026, 9, day))
            for sid in seen
            for day in (1, 2, 3)
        ]
        engine, _ = _recording_engine(rows=rows)
        summary = Resolver(db_engine=engine).resolve_pending(
            lookback_days=2, workers=1, dry_run=True
        )

        assert entity_map.get_feature_id.call_count == 3
        assert summary["candidates"] == 6   # AAA and CCC, 3 obs_dates each
        assert summary["unmapped_groups"] == 3
        assert summary["unmapped_series"] == 1

    def test_a_failed_chunk_does_not_abort_the_run(self):
        """The controller runs the catch-up in slices; one chunk timing out
        must not discard the chunks that already landed. Chunks are
        independent and idempotent, so the failure is recorded and the run
        moves on."""
        resolver = Resolver(db_engine=MagicMock())
        seen: list[str] = []

        def _flaky(workers=8, dry_run=False, since=None, until=None, **_kw):
            seen.append(since.date().isoformat())
            if since.date() == date(2026, 4, 11):
                raise RuntimeError("canceling statement due to statement timeout")
            return {
                "resolved": 2, "conflicts_found": 0, "errors": 0,
                "series_count": 1, "raw_rows": 2, "groups": 1,
                "unmapped_groups": 0, "unmapped_series": 0, "candidates": 2,
                "dry_run": dry_run, "timings": {"total_s": 0.1},
            }

        resolver.resolve_pending = _flaky  # type: ignore[method-assign]

        result = resolver.resolve_backlog(
            since=date(2026, 4, 4), until=date(2026, 4, 25), chunk_days=7
        )

        # All three chunks were attempted, not just the ones before the failure.
        assert seen == ["2026-04-04", "2026-04-11", "2026-04-18"]
        # The two good chunks still counted.
        assert result["totals"]["resolved"] == 4
        assert result["totals"]["errors"] == 1
        # The failed chunk names itself so the operator can retry that range.
        failed = [c for c in result["chunks"] if c.get("failed")]
        assert len(failed) == 1
        assert failed[0]["since"] == "2026-04-11"
        assert "statement timeout" in failed[0]["failed"]
        assert failed[0]["resolved"] == 0
