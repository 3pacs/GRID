"""Test for GD-FIX: InsiderFilingsPuller._emit_signal now carries filing_date.

Before this fix, the Form 4 filing date was captured into raw_series'
raw_payload (pull_recent) but dropped when writing the signal_sources row
via _emit_signal — the only place flow_materializer.sync_insider_trades
reads from — so insider_trades.filing_date could never be populated.
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock

from ingestion.altdata.insider_filings import InsiderFilingsPuller


def test_emit_signal_includes_filing_date():
    puller = InsiderFilingsPuller.__new__(InsiderFilingsPuller)

    trade = {
        "transaction_type": "BUY",
        "insider_name": "Jane Doe",
        "ticker": "AAPL",
        "transaction_date": date(2026, 3, 1),
        "insider_title": "CFO",
        "shares": 1000,
        "price": 150.0,
        "value": 150_000.0,
        "is_derivative": False,
        "transaction_code": "P",
        "is_10b5_1": False,
        "direct_or_indirect": "D",
        "filing_date": "2026-03-03",
        "accession": "0000950103-26-004828",
    }

    conn = MagicMock()
    puller._emit_signal(conn, trade)

    assert conn.execute.called
    _stmt, params = conn.execute.call_args[0]
    payload = json.loads(params["sval"])
    assert payload["filing_date"] == "2026-03-03"
    assert payload["accession"] == "0000950103-26-004828"


def test_emit_signal_filing_date_defaults_to_empty_when_absent():
    puller = InsiderFilingsPuller.__new__(InsiderFilingsPuller)

    trade = {
        "transaction_type": "SELL",
        "insider_name": "Jane Doe",
        "ticker": "AAPL",
        "transaction_date": date(2026, 3, 1),
        "value": 10_000.0,
    }

    conn = MagicMock()
    puller._emit_signal(conn, trade)

    _stmt, params = conn.execute.call_args[0]
    payload = json.loads(params["sval"])
    # Absent, not fabricated as the transaction date.
    assert payload["filing_date"] == ""
