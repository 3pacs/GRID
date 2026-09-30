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


def _scorecard():
    return v7.v2.REPO / "docs/paper_log/vs1-v7-stop-e0-scorecard.json"


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


def test_pinned_e0_evidence_is_below_the_gate_under_realistic_models():
    data = _scorecard().read_bytes()
    assert hashlib.sha256(data).hexdigest() == v7.E0_SCORECARD_SHA256
    powers = v7.e0_v7_power(data)
    assert powers == {"gaussian_idio": 0.496, "factor_t_garch": 0.446, "factor_t_garch_exposed": 0.414}
    with pytest.raises(PermissionError, match="pinned evidence"):
        v7.e0_v7_power(data.replace(b'"e0_power_raw_threshold": 0.414', b'"e0_power_raw_threshold": 0.514'))


def test_v7_stop_requires_exact_witness_and_pinned_e0_evidence(tmp_path, monkeypatch):
    vault = _vault(tmp_path / "vault", monkeypatch)
    log_dir = tmp_path / "registry"
    _registered(log_dir)
    vault.publish(log_dir)
    prior = vault.witness()
    head = v7.REGISTERED_RECORD_SHA256[1]
    v7.V7._refuse_if_stopped(log_dir)  # silent before a STOP
    with pytest.raises(PermissionError, match="superseded by vs1-v8"):
        v1.refuse_superseded(7, v7.SUPERSEDED_BY)

    kw = {"e0_scorecard": _scorecard(), "decision_ref": "owner-standing-auth", "expected_prev_sha256": head,
          "witness": prior}
    preview = v7.append_stop_status(log_dir, RUN_AT, dry_run=True, **kw)
    assert preview["dry_run"] is True
    assert v7.registry(log_dir).verify_chain()["records"] == 2
    stop = v7.append_stop_status(log_dir, RUN_AT, **kw)
    assert stop == preview["would_append"]
    assert v1._record_sha256(stop) == preview["would_be_head_sha256"]
    assert stop["prev_sha256"] == head
    assert stop["status"] == v7.STOP_STATUS and stop["superseded_by"] == "vs1-v8"
    assert stop["e0_scorecard_sha256"] == v7.E0_SCORECARD_SHA256
    assert stop["e0_v7_power_ic_0_01"]["factor_t_garch_exposed"] == 0.414
    assert stop["stage0_run"] is False and stop["inputs_frozen"] is False
    assert stop["discovery_opened"] is False and stop["holdout_opened"] is False
    assert not any(k.startswith("v8_") for k in stop)
    with pytest.raises(PermissionError, match="missing or extra fields"):
        v7._check_stop_record({**stop, "v8_prereg_sha256": "c" * 64})
    passing = {**stop["e0_v7_power_ic_0_01"], "factor_t_garch_exposed": 0.5}
    with pytest.raises(PermissionError, match="below the gate"):
        v7._check_stop_record({**stop, "e0_v7_power_ic_0_01": passing})
    with pytest.raises(PermissionError, match="off-host anchor log"):
        v7.verify_terminal_stop(log_dir, prior)

    # No freeze or opening after a STOP, even with otherwise valid arguments.
    with pytest.raises(PermissionError, match="terminal STOP"):
        v7.freeze_inputs(log_dir, RUN_AT, {"accept_underpowered": False})
    with pytest.raises(PermissionError, match="terminal STOP"):
        v7.open_discovery(log_dir, RUN_AT, {}, prior)
    with pytest.raises(PermissionError, match="terminal STOP"):
        v7.open_holdout({}, allow_holdout=True, prereg_sha256=v7.PREREG_BODY_SHA256, log_dir=log_dir,
                        now=RUN_AT, observed={}, witness=prior)

    vault.publish(log_dir)
    proof = v7.verify_terminal_stop(log_dir, vault.witness())
    assert proof["records"] == 3 and proof["head_sha256"] == v1._record_sha256(stop)
    with pytest.raises(PermissionError, match="two-record baseline"):
        v7.append_stop_status(log_dir, RUN_AT, **kw)


def test_v7_stop_refuses_bad_evidence_time_head_or_a_grown_chain(tmp_path, monkeypatch):
    vault = _vault(tmp_path / "vault", monkeypatch)
    log_dir = tmp_path / "registry"
    log = _registered(log_dir)
    vault.publish(log_dir)
    prior = vault.witness()
    head = v7.REGISTERED_RECORD_SHA256[1]
    kw = {"decision_ref": "owner-standing-auth", "expected_prev_sha256": head, "witness": prior, "dry_run": True}

    other = tmp_path / "other.json"
    other.write_bytes(_scorecard().read_bytes() + b" ")
    with pytest.raises(PermissionError, match="pinned evidence"):
        v7.append_stop_status(log_dir, RUN_AT, e0_scorecard=other, **kw)
    with pytest.raises(ValueError, match="UTC time"):
        v7.append_stop_status(log_dir, RUN_AT.replace(tzinfo=None), e0_scorecard=_scorecard(), **kw)
    with pytest.raises(ValueError, match="registration head"):
        v7.append_stop_status(log_dir, RUN_AT, e0_scorecard=_scorecard(), decision_ref="x",
                              expected_prev_sha256="d" * 64, witness=prior)
    # A chain that already carries a later record (e.g. a freeze) cannot be stopped this way.
    log.append([{"kind": "inputs_frozen", "prereg_sha256": v7.PREREG_BODY_SHA256,
                 "run_at": RUN_AT.isoformat()}])
    with pytest.raises(PermissionError):
        v7.append_stop_status(log_dir, RUN_AT, e0_scorecard=_scorecard(), **kw)
    assert json.loads(_scorecard().read_bytes())["version"] == "e0-v1"
