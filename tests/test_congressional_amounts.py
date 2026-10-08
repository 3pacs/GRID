"""Amount parsing for congressional disclosures.

QuiverQuant's ``Amount`` is only the band's lower bound ("1001.0") and its
``Range`` is the band ("$1,001 - $15,000"). The old parser read "1001.0" as
the range 1001..0, storing a $1,001-$15,000 trade with a $500.50 midpoint.
"""

from __future__ import annotations

import pytest

from ingestion.altdata.congressional import CongressionalTradingPuller, _midpoint_amount
from ingestion.flow_materializer import _midpoint_for_range


@pytest.mark.parametrize("fn", [_midpoint_amount, _midpoint_for_range])
@pytest.mark.parametrize(
    "text, expected",
    [
        ("$1,001 - $15,000", 8000.5),
        ("$100,001 - $250,000", 175000.5),
        ("1001.0", 8000.5),          # bare lower bound -> its band
        ("100001.0", 175000.5),
        ("15001", 32500.5),
        ("A", 8000.5),
        ("", 0.0),
        ("12345.0", 0.0),            # not a band lower bound: unknown, not guessed
        ("Unknown", 0.0),
    ],
)
def test_midpoint(fn, text, expected):
    assert fn(text) == expected


def test_parse_quiverquant_prefers_range_and_uses_last_modified():
    puller = CongressionalTradingPuller.__new__(CongressionalTradingPuller)
    trades = puller._parse_quiverquant_records([{
        "Representative": "Josh Gottheimer", "Ticker": "MCD", "Transaction": "Sale (Partial)",
        "Range": "$1,001 - $15,000", "Amount": "1001.0",
        "TransactionDate": "2026-09-25", "last_modified": "2026-10-08",
    }])
    assert len(trades) == 1
    t = trades[0]
    assert t["amount_range"] == "$1,001 - $15,000"
    assert t["amount_midpoint"] == 8000.5
    assert t["transaction_type"] == "SELL"
    assert str(t["disclosure_date"]) == "2026-10-08"
    assert t["disclosure_basis"] == "qq_last_modified"
