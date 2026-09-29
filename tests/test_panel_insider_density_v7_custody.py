"""Synthetic local custody tests. No provider, price or production registry access."""

from __future__ import annotations

import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from tests.test_panel_insider_density import NOW
from tests.test_panel_insider_density_v2 import _register
from tests.test_panel_insider_density_v6 import _vault


def test_v6_stop_requires_exact_witness_and_cannot_open_after_supersession(tmp_path):
    vault = _vault(tmp_path / "vault")
    log_dir = tmp_path / "registry"
    _register(log_dir, v6)
    vault.publish(log_dir)
    prior = vault.witness()

    preview = v6.append_stop_status(
        log_dir, NOW, power_receipt_sha256="a" * 64,
        owner_decision_ref="owner-design-decision", expected_prev_sha256=v6.REGISTERED_RECORD_SHA256[1],
        witness=prior, dry_run=True,
    )
    assert preview["dry_run"] is True
    assert v6.registry(log_dir).verify_chain()["records"] == 2
    stop = v6.append_stop_status(
        log_dir, NOW, power_receipt_sha256="a" * 64,
        owner_decision_ref="owner-design-decision", expected_prev_sha256=v6.REGISTERED_RECORD_SHA256[1],
        witness=prior,
    )
    assert stop["prev_sha256"] == v6.REGISTERED_RECORD_SHA256[1]
    assert stop == preview["would_append"]
    assert v1._record_sha256(stop) == preview["would_be_head_sha256"]
    assert stop["status"] == v6.STOP_STATUS
    assert stop["raw_primary_power_ic_0_01"] == 0.48
    assert stop["gate_passed"] is False
    assert stop["discovery_opened"] is False and stop["holdout_opened"] is False
    assert "v7_prereg_sha256" not in stop and "v7_registry_head_sha256" not in stop
    with pytest.raises(PermissionError, match="off-host anchor log"):
        v6.verify_terminal_stop(log_dir, prior)

    vault.publish(log_dir)
    proof = v6.verify_terminal_stop(log_dir, vault.witness())
    assert proof["records"] == 3 and proof["head_sha256"] == v1._record_sha256(stop)
    with pytest.raises(PermissionError, match="two-record baseline"):
        v6.append_stop_status(log_dir, NOW, power_receipt_sha256="a" * 64,
                              owner_decision_ref="owner-design-decision",
                              expected_prev_sha256=v6.REGISTERED_RECORD_SHA256[1], witness=prior)
    with pytest.raises(PermissionError, match="superseded by vs1-v7"):
        v1.refuse_superseded(6, v6.SUPERSEDED_BY)


def test_v7_census_accepts_only_pinned_v6_stop_and_sealed_older_registries(monkeypatch, tmp_path):
    stop_head = "b" * 64
    monkeypatch.setattr(v7, "PREREG_BODY_SHA256", "c" * 64)
    monkeypatch.setattr(v7, "V6_STOP_HEAD_SHA256", stop_head)
    monkeypatch.setattr(v7, "DESIGN", {"selected": "owner-selected", "candidate_power": {"candidate": 0.6}})
    seen = []
    monkeypatch.setattr(v7, "_v6_anchor_at_tip", lambda repo, tip, head: seen.append((repo, tip, head)))
    counts = {key: 2 for key in v7.BASELINE_REGISTRIES}
    counts[v6.VERSION] = 3
    files = {key: v1.canonical_witness_path(key) for key in counts}
    census = {"tip": "vault-tip", "files": files, "records": counts, "unknown": []}
    v7.check_census(census, stop_head_sha256=stop_head, witness_repo=tmp_path)
    assert seen == [(tmp_path, "vault-tip", stop_head)]
    assert not v7.contamination([census])["contaminated"]

    opened = {"tip": "vault-tip", "files": {**files, v7.REGISTRY_ID: v1.canonical_witness_path(v7.REGISTRY_ID)},
              "records": {**counts, v7.REGISTRY_ID: 2}, "unknown": []}
    v7.check_census(opened, stop_head_sha256=stop_head, witness_repo=tmp_path, opening=True)
    sectors = {"tip": "vault-tip", "files": {**opened["files"], "sectors-v5": v1.canonical_witness_path("sectors-v5")},
               "records": {**opened["records"], "sectors-v5": 2}, "unknown": []}
    v7.check_census(sectors, stop_head_sha256=stop_head, witness_repo=tmp_path, opening=True)
    with pytest.raises(PermissionError, match="missing or extra"):
        v7.check_census(sectors, stop_head_sha256=stop_head, witness_repo=tmp_path)

    for key, count in ((v6.VERSION, 4), ("sectors-v4", 3)):
        changed = {**census, "records": {**counts, key: count}}
        with pytest.raises(PermissionError):
            v7.check_census(changed, stop_head_sha256=stop_head, witness_repo=tmp_path)
        assert v7.contamination([changed])["contaminated"]
    changed = {**sectors, "records": {**sectors["records"], "sectors-v5": 3}}
    with pytest.raises(PermissionError, match="sectors-v5"):
        v7.check_census(changed, stop_head_sha256=stop_head, witness_repo=tmp_path, opening=True)
    assert v7.contamination([changed])["contaminated"]
    changed = {**census, "unknown": ["05-GRID/Paper-Log/vs1/unknown.anchors.jsonl"]}
    with pytest.raises(PermissionError, match="unknown"):
        v7.check_census(changed, stop_head_sha256=stop_head, witness_repo=tmp_path)
    with pytest.raises(PermissionError, match="verified v6 STOP head"):
        v7.check_census(census, stop_head_sha256="d" * 64, witness_repo=tmp_path)
