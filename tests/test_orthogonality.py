"""Tests for discovery/orthogonality.py — OrthogonalityAudit.

All tests use mocked database and PIT store fixtures; no real PostgreSQL required.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from discovery.orthogonality import OrthogonalityAudit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_feature_matrix(
    n_rows: int = 100,
    n_cols: int = 5,
    seed: int = 42,
    col_prefix: str = "feat",
) -> pd.DataFrame:
    """Create a synthetic feature matrix with a DatetimeIndex."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2020-01-01", periods=n_rows, freq="B")
    data = rng.standard_normal((n_rows, n_cols))
    columns = list(range(1, n_cols + 1))  # feature IDs as ints
    return pd.DataFrame(data, index=dates, columns=columns)


def _setup_audit(
    mock_engine: MagicMock,
    mock_pit_store: MagicMock,
    feature_ids: list[int] | None = None,
    feature_names: dict[int, str] | None = None,
    matrix: pd.DataFrame | None = None,
) -> OrthogonalityAudit:
    """Wire up mocks and return an OrthogonalityAudit instance."""
    # Default: no eligible features
    if feature_ids is None:
        feature_ids = []

    # Mock engine.connect() → rows for _get_eligible_feature_ids / _get_feature_names
    mock_conn = MagicMock()

    def _execute_side_effect(stmt, params=None):
        sql_text = str(stmt)
        result = MagicMock()
        if "model_eligible" in sql_text and "ANY" not in sql_text:
            result.fetchall.return_value = [(fid,) for fid in feature_ids]
        elif "ANY" in sql_text:
            names = feature_names or {fid: f"feat_{fid}" for fid in feature_ids}
            result.fetchall.return_value = [
                (fid, names.get(fid, f"feat_{fid}")) for fid in feature_ids
            ]
        else:
            result.fetchall.return_value = []
        return result

    mock_conn.execute.side_effect = _execute_side_effect
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    # Mock PIT store
    if matrix is not None:
        mock_pit_store.get_feature_matrix.return_value = matrix
    else:
        mock_pit_store.get_feature_matrix.return_value = pd.DataFrame()

    return OrthogonalityAudit(db_engine=mock_engine, pit_store=mock_pit_store)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestOrthogonalityAudit:
    """Tests for OrthogonalityAudit."""

    def test_no_eligible_features(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock, tmp_path
    ) -> None:
        """When no features are model-eligible, return n_features_analyzed=0."""
        audit = _setup_audit(mock_engine, mock_pit_store, feature_ids=[])
        result = audit.run_full_audit(output_dir=str(tmp_path))
        assert result["n_features_analyzed"] == 0

    def test_empty_feature_matrix(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock, tmp_path
    ) -> None:
        """When PIT store returns an empty DataFrame, return an error."""
        audit = _setup_audit(
            mock_engine,
            mock_pit_store,
            feature_ids=[1, 2, 3],
            matrix=pd.DataFrame(),
        )
        result = audit.run_full_audit(output_dir=str(tmp_path))
        assert result["n_features_analyzed"] == 0
        assert "error" in result

    def test_drops_high_missing_features(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock, tmp_path
    ) -> None:
        """Features with >50% NaN should be dropped before analysis."""
        rng = np.random.default_rng(99)
        n_rows = 100
        dates = pd.date_range("2020-01-01", periods=n_rows, freq="B")

        # Create 3 good columns and 1 column that is entirely NaN.
        # Note: run_full_audit does ffill().bfill() before the missing check,
        # so any column with at least one real value gets fully filled.
        # Only a 100% NaN column survives as >50% missing after fill.
        good_data = rng.standard_normal((n_rows, 3))
        bad_col = np.full(n_rows, np.nan)

        data = np.column_stack([good_data, bad_col])
        matrix = pd.DataFrame(data, index=dates, columns=[1, 2, 3, 4])

        feature_names = {1: "good_a", 2: "good_b", 3: "good_c", 4: "bad_col"}
        audit = _setup_audit(
            mock_engine,
            mock_pit_store,
            feature_ids=[1, 2, 3, 4],
            feature_names=feature_names,
            matrix=matrix,
        )

        result = audit.run_full_audit(
            as_of_date=date(2020, 6, 15),
            output_dir=str(tmp_path),
        )

        # The bad column should have been dropped; 3 remain
        assert result["n_features_analyzed"] == 3
        assert result["n_features_dropped"] == 1

    def test_basic_audit_produces_summary(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock, tmp_path
    ) -> None:
        """A small synthetic matrix should produce a valid summary dict."""
        matrix = _make_feature_matrix(n_rows=100, n_cols=5, seed=42)
        audit = _setup_audit(
            mock_engine,
            mock_pit_store,
            feature_ids=[1, 2, 3, 4, 5],
            matrix=matrix,
        )
        result = audit.run_full_audit(
            as_of_date=date(2020, 6, 15),
            output_dir=str(tmp_path),
        )

        assert "n_features_analyzed" in result
        assert "true_dimensionality" in result
        assert "highly_correlated_pairs" in result
        assert result["n_features_analyzed"] == 5
        assert result["true_dimensionality"] >= 1

    def test_highly_correlated_pair_detection(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock, tmp_path
    ) -> None:
        """Two nearly-identical columns (corr > 0.8) should be detected."""
        rng = np.random.default_rng(7)
        n_rows = 100
        dates = pd.date_range("2020-01-01", periods=n_rows, freq="B")

        base = rng.standard_normal(n_rows)
        col_a = base
        col_b = base + rng.standard_normal(n_rows) * 0.05  # nearly identical
        col_c = rng.standard_normal(n_rows)  # independent

        matrix = pd.DataFrame(
            {1: col_a, 2: col_b, 3: col_c}, index=dates
        )

        audit = _setup_audit(
            mock_engine,
            mock_pit_store,
            feature_ids=[1, 2, 3],
            feature_names={1: "base", 2: "clone", 3: "indep"},
            matrix=matrix,
        )

        result = audit.run_full_audit(
            as_of_date=date(2020, 6, 15),
            output_dir=str(tmp_path),
        )

        pairs = result["highly_correlated_pairs"]
        assert len(pairs) >= 1
        # The correlated pair should involve "base" and "clone"
        pair_names = {(p[0], p[1]) for p in pairs}
        assert ("base", "clone") in pair_names or ("clone", "base") in pair_names
