"""Unit tests for scripts/people_events_backfill.py (no database)."""

from __future__ import annotations

import pandas as pd

from scripts import people_events_backfill as B


def _row(**over):
    row = {"accession_number": "acc-1", "document_type": "4", "amended": False, "filing_date": "2026-04-03",
           "issuer_cik": "0000320193", "issuer_ticker": "AAPL", "owner_cik": "0001214156",
           "owner_name": "COOK TIMOTHY D", "is_director": False, "is_officer": True, "is_ten_pct_owner": False,
           "nonderiv_trans_sk": "1", "transaction_date": "2026-04-01", "transaction_date_raw": "01-APR-2026",
           "transaction_code": "P", "shares": 1000.0, "price_per_share": 200.0, "acquired_disposed_code": "A"}
    row.update(over)
    return row


def test_build_events_keeps_only_selected_codes_and_resolves_by_cik():
    rows = [_row(), _row(nonderiv_trans_sk="2", transaction_code="M", shares=5.0),
            _row(nonderiv_trans_sk="3", transaction_code="S", shares=7.0),
            _row(nonderiv_trans_sk="4", transaction_code="F", shares=9.0)]
    ids = pd.DataFrame([{"entity_id": "sm_0000320193", "id_scheme": "cik", "id_value": "320193",
                         "valid_from": "2026-09-27", "valid_to": None, "is_primary": True, "conflict_flag": False}])
    events, stats = B.build_events(pd.DataFrame(rows), ("P", "S", "A"), ids)
    assert sorted(events["transaction_code"]) == ["P", "S"]
    assert stats["events_all_codes"] == 4 and stats["events_selected"] == 2
    assert set(events["security_id"]) == {"sm_0000320193"}
    assert stats["pit"]["known_before_event"] == 0


def test_expected_counts_by_known_year_and_code():
    rows = [_row(), _row(accession_number="a2", filing_date="2025-12-31", transaction_date="2025-12-30",
                         transaction_date_raw="30-DEC-2025", nonderiv_trans_sk="9", transaction_code="S")]
    events, _ = B.build_events(pd.DataFrame(rows), ("P", "S"), pd.DataFrame())
    # 22:00 New York on 2025-12-31 is 2026-01-01 03:00Z: counted in the UTC year of known_at.
    assert B.expected_counts(events) == {"2026|P": 1, "2026|S": 1}


def test_growth_check_band_and_projection():
    ok, info = B.growth_check(0, 50_000_000, 50_000, 4_000_000, 8.0)
    assert ok and "bytes_per_row" not in info  # too few rows to judge
    ok, info = B.growth_check(0, 100_000_000, 100_000, 4_460_000, 8.0)
    assert ok and info["bytes_per_row"] == 1000.0 and info["projected_gb"] == 4.46
    ok, _ = B.growth_check(0, 300_000_000, 100_000, 4_460_000, 8.0)  # 3 KB/row: outside the band
    assert not ok
    ok, _ = B.growth_check(0, 190_000_000, 100_000, 4_460_000, 8.0)  # 1.9 KB/row -> 8.5 GB projected
    assert not ok
