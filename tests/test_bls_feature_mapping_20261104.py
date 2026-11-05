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


def test_mislabeled_series_map_to_their_correct_features() -> None:
    m = _all()
    assert m["H8B1023NCBCMG"] == "ci_loans_growth"
    assert m["TOTRESNS"] == "bank_reserves_at_fed"
    assert m["brazil_usd_brl_fx"] == "brazil_usd_brl"
    assert m["brazil_fx_reserves"] == "brazil_fx_reserves"
    # old mislabeled raw ids stay unmapped, so their rows never resolve
    assert "brazil_discount_rate" not in m and "brazil_usd_brl" not in m


def test_live_series_feed_the_registered_duplicate_names() -> None:
    m = _all()
    assert m["RRPONTSYD"] == "reverse_repo_usage"
    assert m["COMPUTED:reverse_repo_pct_of_peak"] == "rrp_as_pct_of_peak"
    assert m["DGS2"] == "yc_2y"


def test_bcb_writes_correct_raw_ids() -> None:
    from ingestion.international.bcb import BCB_SERIES

    assert BCB_SERIES[1] == "brazil_usd_brl_fx" and BCB_SERIES[3545] == "brazil_fx_reserves"


def test_repo_market_no_longer_derives_window_peak_ratio() -> None:
    from ingestion.altdata import repo_market

    assert "rrp_as_pct_of_peak" not in repo_market.DERIVED_FEATURES


def test_live_10y_feeds_treasury_10y() -> None:
    m = _all()
    assert m["yc_10y"] == m["DGS10"] == m["treasury_10y"] == "treasury_10y"


def test_derived_spreads_read_the_live_names() -> None:
    import ast
    from pathlib import Path

    src = Path(__file__).resolve().parents[1].joinpath("scripts/compute_derived_features.py").read_text()
    assert '"yc_10y"' not in src and '"treasury_10y"' in src and '"yc_2y"' in src
    ast.parse(src)
