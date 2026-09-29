"""Local synthetic custody check for the witnessed VS1 v6 STOP record."""

from __future__ import annotations

import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v6 as v6
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
        owner_decision_ref="owner-design-decision",
        expected_prev_sha256=v6.REGISTERED_RECORD_SHA256[1],
        witness=prior, dry_run=True,
    )
    assert preview["dry_run"] is True
    assert v6.registry(log_dir).verify_chain()["records"] == 2

    stop = v6.append_stop_status(
        log_dir, NOW, power_receipt_sha256="a" * 64,
        owner_decision_ref="owner-design-decision",
        expected_prev_sha256=v6.REGISTERED_RECORD_SHA256[1], witness=prior,
    )
    assert stop == preview["would_append"]
    assert stop["prev_sha256"] == v6.REGISTERED_RECORD_SHA256[1]
    assert v1._record_sha256(stop) == preview["would_be_head_sha256"]
    assert stop["status"] == v6.STOP_STATUS
    assert stop["raw_primary_power_ic_0_01"] == 0.48
    assert stop["gate_passed"] is False
    assert stop["discovery_opened"] is False and stop["holdout_opened"] is False
    assert "v7_prereg_sha256" not in stop and "v7_registry_head_sha256" not in stop
    with pytest.raises(PermissionError, match="missing or extra fields"):
        v6._check_stop_record({**stop, "unregistered_field": "forbidden"})
    with pytest.raises(PermissionError, match="off-host anchor log"):
        v6.verify_terminal_stop(log_dir, prior)

    vault.publish(log_dir)
    proof = v6.verify_terminal_stop(log_dir, vault.witness())
    assert proof["records"] == 3
    assert proof["head_sha256"] == v1._record_sha256(stop)
    with pytest.raises(PermissionError, match="two-record baseline"):
        v6.append_stop_status(
            log_dir, NOW, power_receipt_sha256="a" * 64,
            owner_decision_ref="owner-design-decision",
            expected_prev_sha256=v6.REGISTERED_RECORD_SHA256[1], witness=prior,
        )
    with pytest.raises(PermissionError, match="superseded by vs1-v7"):
        v1.refuse_superseded(6, v6.SUPERSEDED_BY)
