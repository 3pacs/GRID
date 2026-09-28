"""Tests for the "sectors v3" registration (``analysis/panel_insider_density_sectors_v3.py``).

Synthetic data only: no network, no production DB, no price or outcome, no Stage-0 computed.
"""

from __future__ import annotations

import json

import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v2 as s2
from analysis import panel_insider_density_sectors_v3 as s3
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
from tests.test_panel_insider_density import NOW


def test_prereg_hashes_to_the_pin_and_differs_from_sectors_v2():
    assert s3.check_prereg() == s3.PREREG_BODY_SHA256 != s2.PREREG_BODY_SHA256
    body = v1.prereg_body((s3.REPO / s3.PREREG_PATH).read_text(encoding="utf-8"))
    for pin in (s2.PREREG_BODY_SHA256, s2.REGISTERED_RECORD_SHA256[1], v3.PREREG_BODY_SHA256,
                v3.REGISTERED_RECORD_SHA256[1], v2.SIC_MAP_SHA256, v2.ISSUER_MAP_SHA256, v1.SECTOR_MAP_SHA256):
        assert pin in body
    assert "9,999 sign-flip draws" in body and "Two thirds" not in body
    assert "granular_panel_prereg_sectors_v3.anchors.jsonl" in body and "`sectors-v3`" in body


def test_stage0_threshold_is_attainable_now():
    assert s3.STAGE0_THRESHOLD == pytest.approx(s3.RUN_ALPHA / 40)
    assert s3.stage0_attainable() and 1 / (s3.STAGE0_PERMS + 1) < s3.STAGE0_THRESHOLD
    assert s3.STAGE0_SIMS == v1.POWER_SIMS


def test_universe_trials_and_family_are_sectors_v2s():
    assert s3.SECTOR_SIC_RANGES is s2.SECTOR_SIC_RANGES and s3.sector_universes is s2.sector_universes
    assert s3.PRIMARY_TRIAL == "A90|fwd5" and s3.RUN_K == 2 and s3.RUN_ALPHA == pytest.approx(0.10 / 6)


def test_the_witness_is_the_canonical_sectors_v3_path():
    assert s3.REGISTRY_ID == "sectors-v3"
    assert s3.WITNESS_PATH == v1.canonical_witness_path("sectors-v3")
    assert v1.SECTORS_WITNESS.match(s3.WITNESS_PATH)


def test_sectors_v3_is_superseded_by_sectors_v4_and_never_opens():
    """Review round 3: sectors v3's §7 gates on VS1 v3's holdout, and v3 was superseded, so it could never open."""
    from analysis import panel_insider_density_sectors_v4 as s4

    assert s3.SUPERSEDED_BY["version"] == v1.SECTOR_PLAN_SUPERSEDED_BY["version"] == "vs1-sectors-v4"
    assert s3.SUPERSEDED_BY["prereg_sha256"] == s4.PREREG_BODY_SHA256
    with pytest.raises(PermissionError, match="superseded by vs1-sectors-v4"):
        s3.check_open()
    body = v1.prereg_body((s3.REPO / s3.PREREG_PATH).read_text(encoding="utf-8"))
    assert "**VS1 v3** may cover more than 2 records **only once v3's holdout is\n    sealed.**" in body


def test_the_sectors_v3_registration_is_pinned(tmp_path):
    records = s3.register(tmp_path, s3.REGISTERED_AT, s3.REGISTERED_CODE_SHA)
    assert tuple(v1.chained_sha256(records)) == s3.REGISTERED_RECORD_SHA256
    assert (tmp_path / s3.REGISTRY_ANCHORS).read_bytes() == s3.REGISTERED_ANCHOR_LINE + b"\n"
    assert json.loads(s3.REGISTERED_ANCHOR_LINE)["records"] == 2
    record = records[1]
    assert record["stage0"]["perms"] == 9_999 and record["registry_id"] == "sectors-v3"
    assert record["supersedes"]["sectors_v2"]["registry_head_sha256"] == s2.REGISTERED_RECORD_SHA256[1]
    with pytest.raises(PermissionError, match="fork"):
        s3.register(tmp_path / "b", NOW, "c" * 40)
