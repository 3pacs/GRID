"""Tests for the "sectors v2" registration (``analysis/panel_insider_density_sectors_v2.py``).

Synthetic data only: no network, no production DB, no price or outcome.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
from analysis import panel_insider_density_sectors_v2 as s2
from tests.test_panel_insider_density import NOW, _map


def test_prereg_hashes_to_the_pin_and_states_every_sector_range():
    assert s2.check_prereg() == s2.PREREG_BODY_SHA256
    body = v1.prereg_body((s2.REPO / s2.PREREG_PATH).read_text(encoding="utf-8"))
    for sector, ranges in s2.SECTOR_SIC_RANGES.items():
        line = next(line for line in body.splitlines() if line.startswith(f"| {sector} ("))
        for lo, hi in ranges:
            text = f"{lo:04d}" if lo == hi else f"{lo:04d}–{hi:04d}"
            assert text in line or (lo, hi) in ((2000, 2099), (2100, 2199), (3720, 3729), (3730, 3749)), (sector, lo, hi)
    for pin in (v1.PREREG_BODY_SHA256, v3.PREREG_BODY_SHA256, v3.REGISTERED_RECORD_SHA256[1],
                v2.SIC_MAP_SHA256, v2.ISSUER_MAP_SHA256, v1.SECTOR_MAP_SHA256):
        assert pin in body


def test_sector_ranges_are_disjoint_and_exclude_technology_and_shells():
    s2.check_ranges()
    assert set(s2.SECTOR_SIC_RANGES) == set(v1.OTHER_SECTORS)
    for code in (3571, 3674, 7372, 6770, 5331, 5541, 8731, 3826, 9995):
        assert s2.sic_sector(code) is None
    assert s2.sic_sector(4922) == "Energy" and s2.sic_sector(4911) == "Utilities"
    assert s2.sic_sector(6324) == "Healthcare" and s2.sic_sector(6311) == "Financials"
    assert s2.sic_sector(6798) == "Real Estate" and s2.sic_sector(7311) == "Communication Services"


def test_the_run_is_one_40_trial_family_at_run_k_2_with_a_5_session_primary():
    assert s2.PRIMARY_TRIAL == "A90|fwd5" and set(s2.SECONDARY_TRIALS) == set(v1.trial_names()) - {"A90|fwd5"}
    assert s2.RUN_K == 2 and s2.RUN_ALPHA == pytest.approx(0.10 / 6)
    assert s2.SECTOR_ETF["Real Estate"] == "XLRE" and set(s2.LATE_ETF_START) == {"XLRE", "XLC"}
    record = s2.registration_records(s2.REGISTERED_AT or NOW, s2.REGISTERED_CODE_SHA or "c" * 40)[1]
    assert record["run"]["holm_family"] == "all 40 trials" and record["run"]["alpha"] == pytest.approx(0.10 / 6)
    assert record["supersedes"]["v1_registry_head_sha256"] == v1.REGISTERED_RECORD_SHA256[1]


def test_universes_are_disjoint_tech_first_then_sector_map_then_sic():
    sector_map = _map([("Technology", "a", "TECH", 0.3, "company"), ("Energy", "e", "OILCO", 0.3, "company"),
                       ("Utilities", "u", "POWER", 0.3, "company")])
    issuers = pd.DataFrame([("TECH", 1), ("OILCO", 2), ("POWER", 3), ("BANK", 4), ("CHIP", 5), ("BLANK", 6)],
                           columns=["ticker", "cik"])
    sic = pd.DataFrame([{"cik": c, "sic": s, "name": "x", "tickers": [], "former_names": [], "http_status": 200}
                        for c, s in ((1, 2911), (2, 4911), (3, 4911), (4, 6021), (5, 3674), (6, 6770), (7, 1311))])
    universes, info = s2.sector_universes(sector_map, issuers, sic)
    flat = {int(c): s for s, u in universes.items() for c in u["cik"]}
    assert flat == {2: "Energy", 3: "Utilities", 4: "Financials"}  # 1 and 5 Technology, 6 shell, 7 no ticker
    assert info["sector_map_conflicts_resolved_to_sector_map"] == 1  # OILCO's SIC is a utility code


def test_sectors_v2_is_superseded_by_sectors_v3_and_never_opens():
    from analysis import panel_insider_density_sectors_v3 as s3

    assert s2.SUPERSEDED_BY["version"] == "vs1-sectors-v3"
    assert s2.SUPERSEDED_BY["prereg_sha256"] == s3.PREREG_BODY_SHA256
    assert s2.SUPERSEDED_BY["registry_head_sha256"] == s3.REGISTERED_RECORD_SHA256[1]
    with pytest.raises(PermissionError, match="superseded by vs1-sectors-v3"):
        s2.check_open()
    # the review-round-2 defect sectors v3 fixes: alpha_2/40 below the smallest p of 999 sign-flips
    assert s2.RUN_ALPHA / 40 < 1 / (v1.POWER_PERMS + 1)


def test_the_sectors_registration_is_pinned(tmp_path):
    records = s2.register(tmp_path, s2.REGISTERED_AT, s2.REGISTERED_CODE_SHA)
    assert tuple(v1.chained_sha256(records)) == s2.REGISTERED_RECORD_SHA256
    assert (tmp_path / s2.REGISTRY_ANCHORS).read_bytes() == s2.REGISTERED_ANCHOR_LINE + b"\n"
    assert json.loads(s2.REGISTERED_ANCHOR_LINE)["records"] == 2
    with pytest.raises(PermissionError, match="fork"):
        s2.register(tmp_path / "b", NOW, "c" * 40)
    assert v1.SECTORS_WITNESS.match(s2.WITNESS_PATH)
