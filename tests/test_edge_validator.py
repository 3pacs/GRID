"""Tests for intelligence/supply_chain_edge_validator.py.

Covers:

    * ``next_edge_state`` state machine — the pure function that decides
      whether an edge is weak based on correlation and prior weak_since.
    * ``compute_edge_correlation`` happy path (mocked fetch).
    * ``compute_edge_correlation`` missing-series short-circuit.
    * ``validate_edge`` end-to-end, with a fake engine capturing writes
      via ``persist_result``.
    * ``validate_edge`` weak-flag transition after the min-duration window.
    * ``validate_edge`` idempotency — running twice with the same data
      should produce the same post-condition.
    * ``summarise_results`` histogram buckets.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from intelligence.supply_chain_edge_validator import (
    EdgeRow,
    MIN_OBSERVATIONS,
    ValidationResult,
    WEAK_CORRELATION_FLOOR,
    WEAK_MIN_DURATION_DAYS,
    compute_edge_correlation,
    next_edge_state,
    summarise_results,
    validate_edge,
)


# ── next_edge_state ──────────────────────────────────────────────────────


class TestNextEdgeState:
    def test_recovered_clears_state(self):
        today = date(2026, 4, 11)
        weak_since, weak_flag = next_edge_state(
            correlation=0.42,
            prior_weak_since=date(2025, 9, 1),
            today=today,
        )
        assert weak_since is None
        assert weak_flag is False

    def test_recovered_clears_even_when_flagged(self):
        today = date(2026, 4, 11)
        weak_since, weak_flag = next_edge_state(
            correlation=-0.55,  # inverse but strong -> still recovered
            prior_weak_since=date(2025, 1, 1),
            today=today,
        )
        assert weak_since is None
        assert weak_flag is False

    def test_first_dip_starts_clock_but_does_not_flag(self):
        today = date(2026, 4, 11)
        weak_since, weak_flag = next_edge_state(
            correlation=0.05,
            prior_weak_since=None,
            today=today,
        )
        assert weak_since == today
        assert weak_flag is False

    def test_persistent_dip_flags_after_min_duration(self):
        today = date(2026, 4, 11)
        prior = today - timedelta(days=WEAK_MIN_DURATION_DAYS + 1)
        weak_since, weak_flag = next_edge_state(
            correlation=0.02,
            prior_weak_since=prior,
            today=today,
        )
        assert weak_since == prior
        assert weak_flag is True

    def test_recent_dip_does_not_flag(self):
        today = date(2026, 4, 11)
        prior = today - timedelta(days=30)
        weak_since, weak_flag = next_edge_state(
            correlation=0.02,
            prior_weak_since=prior,
            today=today,
        )
        assert weak_since == prior
        assert weak_flag is False


# ── compute_edge_correlation ─────────────────────────────────────────────


def _make_df(values: list[float], start: str = "2025-10-01") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "obs_date": pd.date_range(start=start, periods=len(values), freq="D"),
            "value": values,
        }
    )


class TestComputeEdgeCorrelation:
    def test_happy_path_perfect_correlation(self):
        # Two identical price series -> log returns are identical -> corr = 1
        values = [100.0 + i for i in range(60)]
        up_df = _make_df(values)
        down_df = _make_df(values)

        with patch(
            "intelligence.supply_chain_edge_validator.resolve_price_series_id",
            side_effect=lambda s: f"YF:{s.upper()}:close",
        ), patch(
            "intelligence.supply_chain_edge_validator.fetch_close_series",
            side_effect=[up_df, down_df],
        ):
            corr, n, detail = compute_edge_correlation(
                engine=None, upstream_id="aaa", downstream_id="bbb"
            )
        assert detail == "ok"
        assert n >= MIN_OBSERVATIONS
        assert corr is not None
        assert abs(corr - 1.0) < 1e-9

    def test_missing_upstream_series(self):
        with patch(
            "intelligence.supply_chain_edge_validator.resolve_price_series_id",
            return_value=None,
        ):
            corr, n, detail = compute_edge_correlation(
                engine=None, upstream_id="west_africa", downstream_id="HSY"
            )
        assert corr is None
        assert detail == "no_upstream_series"
        assert n == 0

    def test_missing_downstream_data(self):
        up_df = _make_df([100.0 + i for i in range(60)])
        with patch(
            "intelligence.supply_chain_edge_validator.resolve_price_series_id",
            side_effect=lambda s: f"YF:{s.upper()}:close",
        ), patch(
            "intelligence.supply_chain_edge_validator.fetch_close_series",
            side_effect=[up_df, pd.DataFrame(columns=["obs_date", "value"])],
        ):
            corr, n, detail = compute_edge_correlation(
                engine=None, upstream_id="aaa", downstream_id="bbb"
            )
        assert corr is None
        assert detail == "no_downstream_data"

    def test_insufficient_overlap(self):
        # Only 10 days of data — below MIN_OBSERVATIONS
        up_df = _make_df([100.0 + i for i in range(10)])
        down_df = _make_df([50.0 + i for i in range(10)])
        with patch(
            "intelligence.supply_chain_edge_validator.resolve_price_series_id",
            side_effect=lambda s: f"YF:{s.upper()}:close",
        ), patch(
            "intelligence.supply_chain_edge_validator.fetch_close_series",
            side_effect=[up_df, down_df],
        ):
            corr, n, detail = compute_edge_correlation(
                engine=None, upstream_id="aaa", downstream_id="bbb"
            )
        assert corr is None
        assert detail == "insufficient_overlap"


# ── validate_edge end-to-end ─────────────────────────────────────────────


class _FakeEngine:
    """Minimal stand-in that records persist_result UPDATEs.

    The validator module's ``persist_result`` uses ``engine.begin()`` + a
    ``text()`` UPDATE. We don't want to hit a real DB, so we patch
    ``persist_result`` entirely in these tests — but keep an engine handle
    so the module's internal type hints stay honest.
    """

    def __init__(self) -> None:
        self.writes: list[dict[str, Any]] = []


def _patch_correlation(corr: float | None, detail: str = "ok", n: int = 60):
    return patch(
        "intelligence.supply_chain_edge_validator.compute_edge_correlation",
        return_value=(corr, n, detail),
    )


def _patch_persist(fake: _FakeEngine):
    def _capture(engine, edge_id, correlation, weak_since, relationship_weak):
        fake.writes.append(
            {
                "edge_id": edge_id,
                "correlation": correlation,
                "weak_since": weak_since,
                "relationship_weak": relationship_weak,
            }
        )

    return patch(
        "intelligence.supply_chain_edge_validator.persist_result",
        side_effect=_capture,
    )


class TestValidateEdge:
    def test_strong_correlation_clears_weak_state(self):
        fake = _FakeEngine()
        edge = EdgeRow(
            edge_id=1,
            upstream_id="cocoa",
            downstream_id="HSY",
            weak_since=date(2025, 8, 1),
            relationship_weak=True,
        )
        today = date(2026, 4, 11)
        with _patch_correlation(0.65), _patch_persist(fake):
            result = validate_edge(fake, edge, today)
        assert result.action == "updated"
        assert result.correlation == pytest.approx(0.65, abs=1e-9)
        assert result.weak_since is None
        assert result.relationship_weak is False
        assert fake.writes[0]["weak_since"] is None
        assert fake.writes[0]["relationship_weak"] is False

    def test_weak_correlation_starts_clock_but_no_flag(self):
        fake = _FakeEngine()
        edge = EdgeRow(
            edge_id=2,
            upstream_id="cocoa",
            downstream_id="HSY",
            weak_since=None,
            relationship_weak=False,
        )
        today = date(2026, 4, 11)
        with _patch_correlation(0.04), _patch_persist(fake):
            result = validate_edge(fake, edge, today)
        assert result.action == "updated"
        assert result.weak_since == today
        assert result.relationship_weak is False

    def test_weak_correlation_flags_after_duration(self):
        fake = _FakeEngine()
        prior = date(2026, 4, 11) - timedelta(days=WEAK_MIN_DURATION_DAYS + 5)
        edge = EdgeRow(
            edge_id=3,
            upstream_id="cocoa",
            downstream_id="HSY",
            weak_since=prior,
            relationship_weak=False,
        )
        today = date(2026, 4, 11)
        with _patch_correlation(0.02), _patch_persist(fake):
            result = validate_edge(fake, edge, today)
        assert result.action == "updated"
        assert result.weak_since == prior
        assert result.relationship_weak is True

    def test_missing_data_does_not_persist(self):
        fake = _FakeEngine()
        edge = EdgeRow(
            edge_id=4,
            upstream_id="west_africa",
            downstream_id="HSY",
            weak_since=None,
            relationship_weak=False,
        )
        today = date(2026, 4, 11)
        with _patch_correlation(
            None, detail="no_upstream_series", n=0
        ), _patch_persist(fake):
            result = validate_edge(fake, edge, today)
        assert result.action == "skipped_up"
        assert result.correlation is None
        # Missing-data runs MUST NOT touch persistence
        assert fake.writes == []

    def test_idempotent_rerun(self):
        fake = _FakeEngine()
        edge = EdgeRow(
            edge_id=5,
            upstream_id="cocoa",
            downstream_id="HSY",
            weak_since=None,
            relationship_weak=False,
        )
        today = date(2026, 4, 11)

        # First pass: strong correlation -> clears.
        with _patch_correlation(0.55), _patch_persist(fake):
            r1 = validate_edge(fake, edge, today)

        # Second pass sees the post-condition of the first run and should
        # arrive at the same state.
        updated_edge = EdgeRow(
            edge_id=edge.edge_id,
            upstream_id=edge.upstream_id,
            downstream_id=edge.downstream_id,
            weak_since=r1.weak_since,
            relationship_weak=r1.relationship_weak,
        )
        with _patch_correlation(0.55), _patch_persist(fake):
            r2 = validate_edge(fake, updated_edge, today)

        assert r1.weak_since == r2.weak_since
        assert r1.relationship_weak == r2.relationship_weak
        assert r1.correlation == r2.correlation


# ── summarise_results ────────────────────────────────────────────────────


class TestSummariseResults:
    def test_histogram_buckets(self):
        today = date(2026, 4, 11)
        results = [
            ValidationResult(1, "a", "b", 0.85, "updated", None, False),
            ValidationResult(2, "a", "b", -0.55, "updated", None, False),
            ValidationResult(3, "a", "b", 0.25, "updated", None, False),
            ValidationResult(4, "a", "b", 0.05, "updated", today, False),
            ValidationResult(
                5, "a", "b", 0.03, "updated", today - timedelta(days=200), True
            ),
            ValidationResult(6, "x", "y", None, "skipped_up", None, False),
            ValidationResult(7, "x", "y", None, "skipped_down", None, False),
            ValidationResult(8, "x", "y", None, "skipped_data", None, False),
        ]
        summary = summarise_results(results)
        assert summary["total"] == 8
        assert summary["validated"] == 5
        assert summary["flagged_weak"] == 1
        assert summary["weak_clock_running"] == 1
        assert summary["skipped_upstream_no_series"] == 1
        assert summary["skipped_downstream_no_series"] == 1
        assert summary["skipped_insufficient_data"] == 1
        hist = summary["correlation_histogram"]
        assert hist["abs_ge_0.7"] == 1
        assert hist["abs_0.4_0.7"] == 1
        assert hist["abs_0.1_0.4"] == 1
        assert hist["abs_lt_0.1"] == 2
