"""Staleness guard for wide PIT feature matrices.

Every discovery view (orthogonality, clustering, correlation matrices)
builds a wide matrix from ``PITStore.get_feature_matrix``, forward-fills
at most five days and then drops every row that still has a NaN. One
feature that stopped updating in March therefore silently truncates the
whole matrix to March: the result is labelled "as of today" but describes
the market as of the stalest input.

``drop_stale_columns`` removes such columns *before* the row-wise dropna
and reports what it removed, so callers can fail closed per feature and
say so in their payload instead of reporting a stale window as current.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd


def last_valid_dates(matrix: pd.DataFrame) -> dict[Any, date | None]:
    """Return each column's last non-null observation date (or None)."""
    out: dict[Any, date | None] = {}
    for col in matrix.columns:
        idx = matrix[col].last_valid_index()
        if idx is None:
            out[col] = None
        else:
            out[col] = pd.Timestamp(idx).date()
    return out


def drop_stale_columns(
    matrix: pd.DataFrame,
    as_of_date: date,
    max_age_days: int | None,
) -> tuple[pd.DataFrame, dict[Any, str | None]]:
    """Drop columns whose last observation is older than ``max_age_days``.

    Parameters:
        matrix: Wide matrix indexed by obs_date (DatetimeIndex).
        as_of_date: The decision date the caller reports.
        max_age_days: Maximum calendar-day age of a column's last valid
            observation. ``None`` disables the guard (legacy behaviour).

    Returns:
        (kept_matrix, excluded) where ``excluded`` maps each removed column
        to its last observation date as ISO text (None when the column had
        no observation at all).
    """
    if max_age_days is None or matrix.empty:
        return matrix, {}
    cutoff = as_of_date - timedelta(days=int(max_age_days))
    excluded: dict[Any, str | None] = {}
    for col, last in last_valid_dates(matrix).items():
        if last is None or last < cutoff:
            excluded[col] = last.isoformat() if last is not None else None
    if excluded:
        matrix = matrix.drop(columns=list(excluded))
    return matrix, excluded


def matrix_window(matrix: pd.DataFrame) -> dict[str, str | None]:
    """Return the effective first/last obs_date of a (cleaned) matrix."""
    if matrix.empty:
        return {"matrix_start": None, "matrix_end": None}
    return {
        "matrix_start": pd.Timestamp(matrix.index.min()).date().isoformat(),
        "matrix_end": pd.Timestamp(matrix.index.max()).date().isoformat(),
    }
