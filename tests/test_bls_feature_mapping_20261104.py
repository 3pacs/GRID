"""BLS payrolls/unemployment resolve into the FRED-backed features.

Owner decision 2026-11-04: CES0000000001 and LNS14000000 are the same
series as PAYEMS and UNRATE (identical 2026-09 values), so they are a second
source for nonfarm_payrolls / unemployment_rate, not duplicate bls_* features.
"""

from __future__ import annotations

from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS


def _all() -> dict[str, str]:
    return {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}


def test_bls_and_fred_payrolls_share_one_feature() -> None:
    m = _all()
    assert m["CES0000000001"] == m["PAYEMS"] == "nonfarm_payrolls"


def test_bls_and_fred_unemployment_share_one_feature() -> None:
    m = _all()
    assert m["LNS14000000"] == m["UNRATE"] == "unemployment_rate"


def test_jolts_from_bls_and_fred_share_one_feature() -> None:
    m = _all()
    assert m["JTS000000000000000JOL"] == m["JTSJOL"] == "bls_jolts_openings"
