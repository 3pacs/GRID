"""Synthetic local custody check for the VS1 v7 terminal STOP. No provider, price or registry access."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v3 as v3
from analysis import panel_insider_density_v4 as v4
from analysis import panel_insider_density_v5 as v5
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from tests.test_panel_insider_density_v2 import _Vault

REGISTERED_AT = datetime(2026, 9, 30, 1, 43, tzinfo=timezone.utc)
REGISTERED_CODE_SHA = "999c021b63158e0b43d3f4cf466574db688a6a47"
RUN_AT = datetime(2026, 10, 1, 3, 0, tzinfo=timezone.utc)
V6_STOP_ANCHOR = (
    b'{"head_sha256":"b9d9ab5a3eb82df3d7cd3e5be177cc058b28cb92ea86b7ab124673a43309d284",'
    b'"prev_anchor_sha256":"011693a214618385aa9b80d5f8fc528e166dfd6c478898dd34118c4b95c7bac3",'
    b'"records":3,"run_at":"2026-09-29T23:40:00+00:00"}'
)


def _registered(log_dir):
    """The exact witnessed v7 registration, re-materialised from its pinned inputs."""
    log = v7.registry(log_dir)
    log.append(v7.registration_records(REGISTERED_AT, REGISTERED_CODE_SHA))
    assert tuple(v1._line_sha256(log)) == v7.REGISTERED_RECORD_SHA256
    return log


def _vault(root, monkeypatch):
    # The census rules have their own tests; here only the v7 chain/witness custody is exercised.
    monkeypatch.setattr(v7, "check_census", lambda *a, **k: None)
    vault = _Vault(root, h=v7, seeds=("vs1-v1", "vs1-v2"))
    for m in (v3, v4, v5):
        vault.add_file(m.WITNESS_PATH, m.REGISTERED_ANCHOR_LINE + b"\n")
    vault.add_file(v6.WITNESS_PATH, v6.REGISTERED_ANCHOR_LINE + b"\n" + V6_STOP_ANCHOR + b"\n")
    return vault


def _power(tmp_path, primary_power, name="power.json", **overrides):
    table = {trial: [{"target_ic": ic, "power": primary_power if (trial, ic) == (v7.PRIMARY_TRIAL, 0.01) else 0.9,
                      "sims": v1.POWER_SIMS, "usable_dates": 414} for ic in v1.POWER_TARGET_ICS]
             for trial in v1.trial_names()}
    doc = {"version": v7.VERSION, "primary_trial": v7.PRIMARY_TRIAL, "settings": v1.power_settings(),
           "table": table, "gate_passed": primary_power >= v1.POWER_GATE,
           "inputs": {"prereg_sha256": v7.PREREG_BODY_SHA256, "price_manifest_sha256": "a" * 64,
                      "form4_receipt_sha256": "b" * 64},
           **overrides}
    path = tmp_path / name
    path.write_text(json.dumps(doc, sort_keys=True), encoding="utf-8")
    return path


def test_v7_stop_requires_exact_witness_and_sealed_underpowered_receipt(tmp_path, monkeypatch):
    vault = _vault(tmp_path / "vault", monkeypatch)
    log_dir = tmp_path / "registry"
    _registered(log_dir)
    vault.publish(log_dir)
    prior = vault.witness()
    power = _power(tmp_path, 0.455)
    head = v7.REGISTERED_RECORD_SHA256[1]

    preview = v7.append_stop_status(log_dir, RUN_AT, power_path=power, decision_ref="owner-standing-auth",
                                    expected_prev_sha256=head, witness=prior, dry_run=True)
    assert preview["dry_run"] is True
    assert v7.registry(log_dir).verify_chain()["records"] == 2

    stop = v7.append_stop_status(log_dir, RUN_AT, power_path=power, decision_ref="owner-standing-auth",
                                 expected_prev_sha256=head, witness=prior)
    assert stop == preview["would_append"]
    assert v1._record_sha256(stop) == preview["would_be_head_sha256"]
    assert stop["prev_sha256"] == head
    assert stop["status"] == v7.STOP_STATUS and stop["superseded_by"] == "vs1-v8"
    assert stop["raw_primary_power_ic_0_01"] == 0.455 and stop["usable_dates"] == 414
    assert stop["power_receipt_sha256"] == hashlib.sha256(power.read_bytes()).hexdigest()
    assert stop["gate_passed"] is False and stop["inputs_frozen"] is False
    assert stop["discovery_opened"] is False and stop["holdout_opened"] is False
    assert not any(k.startswith("v8_") for k in stop)
    with pytest.raises(PermissionError, match="missing or extra fields"):
        v7._check_stop_record({**stop, "v8_prereg_sha256": "c" * 64})
    with pytest.raises(PermissionError, match="off-host anchor log"):
        v7.verify_terminal_stop(log_dir, prior)

    # No freeze or opening after a STOP, even with otherwise valid arguments.
    with pytest.raises(PermissionError, match="terminal STOP"):
        v7.freeze_inputs(log_dir, RUN_AT, {"accept_underpowered": False})
    with pytest.raises(PermissionError, match="terminal STOP"):
        v7.open_discovery(log_dir, RUN_AT, {}, prior)

    vault.publish(log_dir)
    proof = v7.verify_terminal_stop(log_dir, vault.witness())
    assert proof["records"] == 3 and proof["head_sha256"] == v1._record_sha256(stop)
    with pytest.raises(PermissionError, match="two-record baseline"):
        v7.append_stop_status(log_dir, RUN_AT, power_path=power, decision_ref="owner-standing-auth",
                              expected_prev_sha256=head, witness=prior)


def test_v7_stop_refuses_a_passing_or_foreign_power_receipt(tmp_path, monkeypatch):
    vault = _vault(tmp_path / "vault", monkeypatch)
    log_dir = tmp_path / "registry"
    _registered(log_dir)
    vault.publish(log_dir)
    prior = vault.witness()
    head = v7.REGISTERED_RECORD_SHA256[1]
    kw = dict(decision_ref="owner-standing-auth", expected_prev_sha256=head, witness=prior, dry_run=True)

    with pytest.raises(PermissionError, match="passed its power gate"):
        v7.append_stop_status(log_dir, RUN_AT, power_path=_power(tmp_path, 0.5, "pass.json"), **kw)
    with pytest.raises(ValueError, match="gate_passed disagrees"):
        v7.append_stop_status(log_dir, RUN_AT, power_path=_power(tmp_path, 0.45, "lie.json", gate_passed=True), **kw)
    with pytest.raises(ValueError, match="harness"):
        v7.append_stop_status(log_dir, RUN_AT, power_path=_power(tmp_path, 0.45, "v6.json", version=v6.VERSION),
                              **kw)
    foreign = {"prereg_sha256": v6.PREREG_BODY_SHA256, "price_manifest_sha256": "a" * 64}
    with pytest.raises(PermissionError, match="not a v7 post-admission"):
        v7.append_stop_status(log_dir, RUN_AT, power_path=_power(tmp_path, 0.45, "f.json", inputs=foreign), **kw)
    with pytest.raises(ValueError, match="registration head"):
        v7.append_stop_status(log_dir, RUN_AT, power_path=_power(tmp_path, 0.45, "ok.json"),
                              decision_ref="x", expected_prev_sha256="d" * 64, witness=prior)
    assert v7.registry(log_dir).verify_chain()["records"] == 2
