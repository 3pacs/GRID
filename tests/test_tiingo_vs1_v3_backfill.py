"""Tests for the VS1 v3 Tiingo backfill driver.

Covers the pure/offline parts only (ticker-file loading and validation,
and the read-only summary query against a mocked engine). The actual
Tiingo HTTP calls and inserts are exercised by the existing
``ingestion/tiingo_pull.py`` tests — this script is a thin driver over
that unchanged puller, not a new writer.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from scripts.tiingo_vs1_v3_backfill import load_tickers, obs_date_summary


def test_load_tickers_reads_json_list(tmp_path):
    path = tmp_path / "tickers.json"
    path.write_text(json.dumps(["AAOI", "ACIW", "ADTN"]))
    assert load_tickers(str(path)) == ["AAOI", "ACIW", "ADTN"]


def test_load_tickers_rejects_non_list(tmp_path):
    path = tmp_path / "tickers.json"
    path.write_text(json.dumps({"AAOI": True}))
    with pytest.raises(ValueError):
        load_tickers(str(path))


def test_load_tickers_rejects_non_string_entries(tmp_path):
    path = tmp_path / "tickers.json"
    path.write_text(json.dumps(["AAOI", 123]))
    with pytest.raises(ValueError):
        load_tickers(str(path))


def test_obs_date_summary_queries_adj_close_series_read_only():
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.one.return_value = ("2015-01-02", "2026-09-25", 2937)

    first, last, count = obs_date_summary(engine, source_id=524, ticker="AAOI")

    assert (first, last, count) == ("2015-01-02", "2026-09-25", 2937)
    # Read-only: exactly one execute call, via a plain connect() (not begin()).
    engine.begin.assert_not_called()
    conn.execute.assert_called_once()
    sql_text = str(conn.execute.call_args[0][0])
    assert "SELECT" in sql_text.upper()
    assert "DELETE" not in sql_text.upper()
    assert "UPDATE" not in sql_text.upper()
    params = conn.execute.call_args[0][1]
    assert params == {"sid": 524, "series": "YF:AAOI:adj_close"}


def test_obs_date_summary_handles_no_rows():
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.one.return_value = (None, None, 0)

    first, last, count = obs_date_summary(engine, source_id=524, ticker="ZZZZ")

    assert (first, last, count) == (None, None, 0)
