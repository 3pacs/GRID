"""Tests for the NY Fed Nowcast Excel parser.

Regression: the parser selected ``numeric_cols[0]``/``[1]`` (the first two
numeric columns) as the current/next quarter nowcast. The real NY Fed Nowcast
sheet is *wide* — one column per reference quarter from 2002 to the present —
so the first numeric column is the OLDEST quarter (2002), and the puller stored
ancient values as today's GDP nowcast.

The fix extracts, per vintage row, the latest populated reference quarter as
the current-quarter value (and the next quarter as Q2). These tests build tiny
in-memory spreadsheets so the parse is validated offline (no live download).
"""

from __future__ import annotations

import io

import pandas as pd
import pytest

from ingestion.altdata.nyfed import _extract_nowcast_series, _quarter_sort_key


# ── quarter-label parsing ──

@pytest.mark.parametrize(
    "label,expected",
    [
        ("2026q1", (2026, 1)),
        ("2026:Q2", (2026, 2)),
        ("2026 q3", (2026, 3)),
        ("2002-Q1", (2002, 1)),
        ("Q4-2025", (2025, 4)),
        ("q1 2026", (2026, 1)),
        ("forecast date", None),
        ("gdp", None),
        ("", None),
    ],
)
def test_quarter_sort_key(label, expected):
    assert _quarter_sort_key(label) == expected


def _wide_df() -> pd.DataFrame:
    """Mimic the real wide Nowcast sheet: date + one col per quarter.

    Each vintage row only populates the quarter(s) being forecast at that
    date — earlier quarters are NaN (already realized), later ones are NaN
    (not yet forecast). Late in a quarter the Fed forecasts the next quarter
    too, so a row can populate two adjacent quarters (current + next).
    """
    nan = float("nan")
    data = {
        "forecast date": ["2025-12-05", "2026-01-09", "2026-03-27"],
        # oldest -> newest reference quarters
        "2025q3": [nan, nan, nan],
        "2025q4": [2.2, nan, nan],          # only current quarter forecast
        "2026q1": [nan, 3.1, 3.5],          # current quarter for Jan + Mar
        "2026q2": [nan, nan, 1.9],          # next quarter, added late in Q1
    }
    return pd.DataFrame(data)


def _prep(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """Replicate the puller's pre-processing (lowercase cols, parse dates)."""
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    date_col = "forecast date"
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    return df, date_col


def test_wide_format_picks_latest_quarter_not_oldest():
    df, date_col = _prep(_wide_df())
    rows = _extract_nowcast_series(df, date_col)

    by_date = {d: (q1, q2, c1, c2) for d, q1, q2, c1, c2 in rows}

    # 2025-12-05: only 2025q4 (=2.2) is populated -> current quarter, no next.
    # The buggy code would have returned the oldest numeric column instead.
    q1, q2, c1, c2 = by_date[pd.Timestamp("2025-12-05").date()]
    assert q1 == 2.2
    assert c1 == "2025q4"
    assert q2 is None

    # 2026-01-09: only 2026q1 (=3.1) populated -> current quarter, no next.
    q1, q2, c1, c2 = by_date[pd.Timestamp("2026-01-09").date()]
    assert q1 == 3.1
    assert c1 == "2026q1"
    assert q2 is None

    # 2026-03-27: two adjacent quarters populated -> current=2026q1 (3.5),
    # next=2026q2 (1.9).
    q1, q2, c1, c2 = by_date[pd.Timestamp("2026-03-27").date()]
    assert q1 == 3.5
    assert c1 == "2026q1"
    assert q2 == 1.9
    assert c2 == "2026q2"


def test_wide_format_via_real_xlsx_roundtrip():
    # Write to an actual .xlsx and read it back the way the puller does,
    # proving the openpyxl path works end-to-end (minus the HTTP fetch).
    buf = io.BytesIO()
    _wide_df().to_excel(buf, index=False, sheet_name="Forecasts By Quarter")
    buf.seek(0)
    xls = pd.ExcelFile(buf)
    df = pd.read_excel(xls, sheet_name=xls.sheet_names[0])
    df, date_col = _prep(df)

    rows = _extract_nowcast_series(df, date_col)
    assert len(rows) == 3
    # Latest row's current quarter must be 2026q1 = 3.5, never the 2002-style
    # oldest column.
    last = max(rows, key=lambda r: r[0])
    assert last[1] == 3.5
    assert last[3] == "2026q1"


def test_two_column_fallback_when_no_quarter_labels():
    df = pd.DataFrame({
        "date": ["2026-01-01", "2026-01-08"],
        "current_q": [2.0, 2.5],
        "next_q": [1.5, 1.6],
    })
    df, date_col = _prep(df.rename(columns={"date": "forecast date"}))
    rows = _extract_nowcast_series(df, date_col)
    assert len(rows) == 2
    q1, q2, c1, c2 = rows[0][1:]
    assert q1 == 2.0 and c1 == "current_q"
    assert q2 == 1.5 and c2 == "next_q"


def test_no_numeric_columns_returns_empty():
    df = pd.DataFrame({"forecast date": ["2026-01-01"], "note": ["n/a"]})
    df, date_col = _prep(df)
    assert _extract_nowcast_series(df, date_col) == []


def test_skips_rows_with_all_nan_quarters():
    nan = float("nan")
    df = pd.DataFrame({
        "forecast date": ["2025-06-01", "2026-01-01"],
        "2026q1": [nan, 3.0],
    })
    df, date_col = _prep(df)
    rows = _extract_nowcast_series(df, date_col)
    # First row has no populated quarter -> skipped; second yields one row.
    assert len(rows) == 1
    assert rows[0][1] == 3.0
