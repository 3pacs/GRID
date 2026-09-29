"""Tests for the "sectors v4" registration (``analysis/panel_insider_density_sectors_v4.py``).

Synthetic data only: no network, no production DB, no price or outcome, no Stage-0 computed.
"""

from __future__ import annotations

import json
import types

import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v3 as s3
from analysis import panel_insider_density_sectors_v4 as s4
from analysis import panel_insider_density_v5 as v5
from analysis import panel_insider_density_v6 as v6
from tests.test_panel_insider_density import NOW
from tests.test_panel_insider_density_v2 import _inputs, _observed, _register

SECTOR_LINE = b'{"records":2}\n'


@pytest.fixture
def historical_v6_terminal(monkeypatch):
    """Replay the recorded sectors-v4 checks before v7 source was present."""
    assert v6.SUPERSEDED_BY == {"version": "vs1-v7"}
    modules = s4._technology_modules()
    assert modules[-1].VERSION == "vs1-v7"
    monkeypatch.setattr(s4, "_technology_modules", lambda: modules[:-1])
    monkeypatch.setattr(v6, "SUPERSEDED_BY", None)


def test_prereg_hashes_to_the_pin_gates_on_v6_and_differs_from_sectors_v3():
    assert s4.check_prereg() == s4.PREREG_BODY_SHA256 != s3.PREREG_BODY_SHA256
    body = v1.prereg_body((s4.REPO / s4.PREREG_PATH).read_text(encoding="utf-8"))
    for pin in (s3.PREREG_BODY_SHA256, s3.REGISTERED_RECORD_SHA256[1], v6.PREREG_BODY_SHA256,
                v6.REGISTERED_RECORD_SHA256[1], v6.WITNESS_PATH):
        assert pin in body
    assert "terminal member of the pinned supersession chain" in body
    assert "Any later Technology registration (v7 or\n  beyond) itself requires a new sectors registration" in body
    assert "VS1 v1, v2, v3, v4 and v5, sectors-v2 and sectors-v3" in body
    assert "Technology universe of the **VS1 v6** run" in body and "The 11 are the VS1 v6 Technology run" in body
    assert "granular_panel_prereg_sectors_v4.anchors.jsonl" in body and "`sectors-v4`" in body
    assert "VS1 v3 Technology plus" not in body and "v3 may cover more than 2" not in body


def test_the_run_is_sectors_v3s_unchanged():
    assert s4.SECTOR_SIC_RANGES is s3.SECTOR_SIC_RANGES and s4.sector_universes is s3.sector_universes
    assert (s4.PRIMARY_TRIAL, s4.RUN_K, s4.STAGE0_PERMS, s4.STAGE0_THRESHOLD) == (
        s3.PRIMARY_TRIAL, s3.RUN_K, s3.STAGE0_PERMS, s3.STAGE0_THRESHOLD)
    assert s4.stage0_attainable()


def test_the_witness_is_the_canonical_sectors_v4_path():
    assert s4.REGISTRY_ID == "sectors-v4" and s4.WITNESS_PATH == v1.canonical_witness_path("sectors-v4")
    assert v1.SECTORS_WITNESS.match(s4.WITNESS_PATH)


def test_the_technology_run_remains_pinned_to_v6_and_refuses_v7():
    assert s4.TECHNOLOGY_RUN["version"] == "vs1-v6" and s4.TECHNOLOGY_RUN["registry_id"] == "vs1-v6"
    assert s4.TECHNOLOGY_RUN["registry_head_sha256"] == v6.REGISTERED_RECORD_SHA256[1]
    assert s4.SUPERSEDED_BY["version"] == "vs1-sectors-v5"
    with pytest.raises(PermissionError):
        s4.check_technology_run()


def test_the_historical_technology_run_was_v6(historical_v6_terminal):
    assert s4.check_technology_run() == s4.TECHNOLOGY_RUN == s4.technology_terminal()


def test_v7_unregistered_blocks_sectors_v4_without_an_anchor(tmp_path):
    assert v6.SUPERSEDED_BY == {"version": "vs1-v7"}
    with pytest.raises(PermissionError, match="not registered"):
        s4.check_technology_run()
    with pytest.raises(PermissionError, match="not registered"):
        s4.register(tmp_path, s4.REGISTERED_AT, s4.REGISTERED_CODE_SHA)
    assert not (tmp_path / s4.REGISTRY_ANCHORS).exists()


def test_a_later_technology_registration_requires_a_new_sectors_registration(monkeypatch):
    v7 = types.SimpleNamespace(VERSION="vs1-v7", SUPERSEDED_BY=None, PREREG_BODY_SHA256="7" * 64,
                               REGISTERED_RECORD_SHA256=("a" * 64, "b" * 64),
                               WITNESS_PATH=v1.canonical_witness_path("vs1-v7"))
    pin7 = {"version": "vs1-v7", "prereg_sha256": "7" * 64, "registry_head_sha256": "b" * 64}
    chain = s4._technology_modules()[:-1]  # historical v1-v6 chain, before the real v7 scaffold
    fakes = [types.SimpleNamespace(**{**vars(m), "SUPERSEDED_BY": pin7}) for m in chain]
    monkeypatch.setattr(s4, "_technology_modules", lambda: [*fakes, v7])
    assert s4.technology_terminal()["version"] == "vs1-v7"
    with pytest.raises(PermissionError, match="requires a new sectors registration"):
        s4.check_technology_run()


def test_a_broken_technology_chain_refuses(monkeypatch, historical_v6_terminal):
    monkeypatch.setattr(v5, "SUPERSEDED_BY", None)
    with pytest.raises(PermissionError, match="no single terminal member"):
        s4.technology_terminal()
    monkeypatch.setattr(v5, "SUPERSEDED_BY", {"version": "vs1-v6", "prereg_sha256": "0" * 64,
                                              "registry_head_sha256": v6.REGISTERED_RECORD_SHA256[1]})
    with pytest.raises(PermissionError, match="does not name the terminal"):
        s4.technology_terminal()


def _census(**records):
    ids = {**{k: 2 for k in (*s4.FROZEN_REGISTRIES, "vs1-v6", "sectors-v4")}, **records}
    ids = {k: n for k, n in ids.items() if n is not None}
    return {"files": {k: v1.canonical_witness_path(k) for k in ids}, "records": ids, "unknown": []}


def test_the_census_check_opens_only_after_v6s_holdout_is_sealed():
    s4.check_census(_census(**{"vs1-v6": 9}), technology_sealed_records=9)
    with pytest.raises(PermissionError, match="witnessed holdout_result"):
        s4.check_census(_census())  # v6 never opened: VS1's holdout verdict does not exist
    with pytest.raises(PermissionError, match="witnessed holdout_result"):
        s4.check_census(_census(**{"vs1-v6": 9}))  # past 2, but not established as sealed
    with pytest.raises(PermissionError, match="witnessed holdout_result"):
        s4.check_census(_census(**{"vs1-v6": 10}), technology_sealed_records=9)  # grew past the holdout_result


@pytest.mark.parametrize("registry", s4.FROZEN_REGISTRIES)
def test_every_other_registry_must_stay_at_its_registration(registry):
    with pytest.raises(PermissionError, match="past their registration records"):
        s4.check_census(_census(**{"vs1-v6": 9, registry: 4}), technology_sealed_records=9)


@pytest.mark.parametrize("extra", ["vs1-v7", "sectors-v5", "sectors-v1"])
def test_no_other_vs1_registry_is_allowed(extra):
    with pytest.raises(PermissionError, match="must be exactly"):
        s4.check_census(_census(**{"vs1-v6": 9, extra: 2}), technology_sealed_records=9)


def test_unknown_missing_and_non_canonical_witnesses_refuse():
    census = _census(**{"vs1-v6": 9})
    census["unknown"] = ["05-GRID/Paper-Log/vs1/granular_panel_prereg_sectors_v04.anchors.jsonl"]
    with pytest.raises(PermissionError, match="unknown"):
        s4.check_census(census, technology_sealed_records=9)
    with pytest.raises(PermissionError, match="missing"):
        s4.check_census(_census(**{"vs1-v6": 9, "sectors-v4": None}), technology_sealed_records=9)
    census = _census(**{"vs1-v6": 9})
    census["files"]["sectors-v4"] = "05-GRID/Paper-Log/vs1/elsewhere.anchors.jsonl"
    with pytest.raises(PermissionError, match="canonical"):
        s4.check_census(census, technology_sealed_records=9)


def _real_vault(tmp_path, sectors_v4=SECTOR_LINE):
    from tests.test_panel_insider_density_v6 import _vault

    vault = _vault(tmp_path / "vault")
    vault.add_file(s3.WITNESS_PATH, s3.REGISTERED_ANCHOR_LINE + b"\n")
    from analysis import panel_insider_density_sectors_v2 as s2

    vault.add_file(s2.WITNESS_PATH, s2.REGISTERED_ANCHOR_LINE + b"\n")
    vault.add_file(s4.WITNESS_PATH, sectors_v4)
    return vault


def test_the_census_knows_sectors_v4_on_a_git_vault(tmp_path):
    vault = _real_vault(tmp_path)
    vault.add_file(v6.WITNESS_PATH, v6.REGISTERED_ANCHOR_LINE + b"\n")
    census = v1.vs1_witness_census(vault.worktree, v1._git(vault.worktree, "rev-parse", "HEAD").strip())
    assert census["files"]["sectors-v4"] == s4.WITNESS_PATH and census["records"]["sectors-v4"] == 2
    assert not census["unknown"]
    with pytest.raises(PermissionError, match="witnessed holdout_result"):
        s4.check_census(census)  # at this registration v6 has not opened: sectors v4 cannot open


def test_v6_refuses_to_open_while_sectors_v4_is_opened(tmp_path, historical_v6_terminal):
    vault = _real_vault(tmp_path, SECTOR_LINE + b'{"head_sha256":"' + b"e" * 64
                        + b'","prev_anchor_sha256":"x","records":4,"run_at":"2026-10-01T00:00:00+00:00"}\n')
    _register(tmp_path / "reg", v6)
    vault.publish(tmp_path / "reg")
    v6.freeze_inputs(tmp_path / "reg", NOW, _inputs())
    with pytest.raises(PermissionError, match="VS1 registry was opened"):
        v6.open_discovery(tmp_path / "reg", NOW, _observed(_inputs()), vault.witness())


def test_no_harness_opens_sectors_v4_yet():
    with pytest.raises(PermissionError, match="no sectors-v4 joint-run harness"):
        s4.check_open()


def test_v1_and_sectors_v3_point_at_sectors_v4():
    for pin in (v1.SECTOR_PLAN_SUPERSEDED_BY, s3.SUPERSEDED_BY):
        assert pin["version"] == "vs1-sectors-v4" and pin["prereg_sha256"] == s4.PREREG_BODY_SHA256
        assert pin["registry_head_sha256"] == s4.REGISTERED_RECORD_SHA256[1]
    spec = v1.RunSpec(run_id="x", sector="Energy", run_k=2, trials=v1.trial_names())
    with pytest.raises(ValueError, match="superseded by vs1-sectors-v4"):
        spec.validate()
    with pytest.raises(PermissionError, match="superseded by vs1-sectors-v4"):
        s3.check_open()


def test_the_sectors_v4_registration_is_pinned(tmp_path, historical_v6_terminal):
    records = s4.register(tmp_path, s4.REGISTERED_AT, s4.REGISTERED_CODE_SHA)
    assert tuple(v1.chained_sha256(records)) == s4.REGISTERED_RECORD_SHA256
    assert (tmp_path / s4.REGISTRY_ANCHORS).read_bytes() == s4.REGISTERED_ANCHOR_LINE + b"\n"
    assert json.loads(s4.REGISTERED_ANCHOR_LINE)["records"] == 2
    record = records[1]
    assert record["registry_id"] == "sectors-v4" and record["technology_run"]["version"] == "vs1-v6"
    assert record["supersedes"]["sectors_v3"]["registry_head_sha256"] == s3.REGISTERED_RECORD_SHA256[1]
    with pytest.raises(PermissionError, match="fork"):
        s4.register(tmp_path / "b", NOW, "c" * 40)
