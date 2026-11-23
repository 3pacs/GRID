"""Synthetic custody tests for VS1 sectors-v6 STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED.

No database, network, or /data access.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v2 as s2
from analysis import panel_insider_density_sectors_v3 as s3
from analysis import panel_insider_density_sectors_v4 as s4
from analysis import panel_insider_density_sectors_v6 as s6
from analysis import panel_insider_density_v3 as v3
from analysis import panel_insider_density_v4 as v4
from analysis import panel_insider_density_v5 as v5
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_v8 as v8
from analysis import panel_insider_density_v9 as v9
from analysis.research_forward_log import canonical
from scripts import run_vs1_sectors_v6_registration as s6_cli
from tests.test_panel_insider_density_v2 import _Vault
from tests.test_panel_insider_density_v8 import _power_doc
from tests.test_panel_insider_density_v8_stop import (
    DECISION_REF,
    V8_REGISTERED_AT,
    V8_REGISTERED_CODE,
    _bind_superseded_pins,
    _synthetic_probe_report,
)
from tests.test_panel_insider_density_v9 import V6_STOP_LINE

S6_REGISTERED_AT = datetime(2026, 10, 1, 16, 9, 28, tzinfo=timezone.utc)
S6_REGISTERED_CODE = "4dfc25c9dd0eb1bd2f9f0c18e7ecb895f8e55c01"
V8_STOP_AT = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)
S6_STOP_AT = datetime(2026, 11, 22, 12, 5, tzinfo=timezone.utc)
S6_DECISION_REF = (
    "owner-ruling-R9-2026-10-07; owner-decision-go-with-v9-2026-11-22; "
    "sectors-v6-superseded-by-sectors-v7"
)


def _registered_v8(log_dir: Path):
    log = v8.registry(log_dir)
    log.append(v8.registration_records(V8_REGISTERED_AT, V8_REGISTERED_CODE))
    assert tuple(v1._line_sha256(log)) == v8.REGISTERED_RECORD_SHA256
    return log


def _registered_s6(log_dir: Path):
    log = s6.registry(log_dir)
    log.append(s6.registration_records(S6_REGISTERED_AT, S6_REGISTERED_CODE))
    assert tuple(v1._line_sha256(log)) == s6.REGISTERED_RECORD_SHA256
    return log


def _base_vault(root: Path) -> _Vault:
    vault = _Vault(root, h=v8, seeds=("vs1-v1", "vs1-v2"))
    nl = b"\n"
    for m in (v3, v4, v5, s2, s3, s4):
        vault.add_file(m.WITNESS_PATH, m.REGISTERED_ANCHOR_LINE + nl)
    vault.add_file(v6.WITNESS_PATH, v6.REGISTERED_ANCHOR_LINE + nl + V6_STOP_LINE + nl)
    vault.add_file(v7.WITNESS_PATH, v7.REGISTERED_ANCHOR_LINE + nl + v8.V7_STOP_ANCHOR_LINE + nl)
    return vault


def _setup_v8_stopped_and_s6_registered(tmp_path: Path, monkeypatch):
    """Vault with witnessed v8 STOP_SUPERSEDED_BY_V9_UNOPENED (3 records) and sectors-v6 (2 records)."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    probe_bytes = _synthetic_probe_report()
    _bind_superseded_pins(monkeypatch, probe_bytes)
    probe_path = tmp_path / "probe_report.json"
    probe_path.write_bytes(probe_bytes)

    vault = _base_vault(tmp_path / "vault")
    v8_reg = tmp_path / "v8_reg"
    _registered_v8(v8_reg)
    vault.publish(v8_reg)

    s6_reg = tmp_path / "s6_reg"
    _registered_s6(s6_reg)
    vault.add_file(s6.WITNESS_PATH, (s6_reg / s6.REGISTRY_ANCHORS).read_bytes())

    prior_v8 = vault.witness()
    stop8 = v8.append_superseded_stop(
        v8_reg,
        V8_STOP_AT,
        probe_report_path=probe_path,
        decision_ref=DECISION_REF,
        expected_prev_sha256=v8.REGISTERED_RECORD_SHA256[1],
        witness=prior_v8,
    )
    vault.publish(v8_reg)
    v8_stop_head = v1._record_sha256(stop8)
    return vault, v8_reg, s6_reg, v8_stop_head


def test_sectors_v6_is_superseded_by_sectors_v7_and_stop_constants():
    assert s6.SUPERSEDED_BY == {"version": "vs1-sectors-v7"}
    assert s6.STOP_STATUS == "STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED"
    assert s6.STOP_SUCCESSOR == "vs1-sectors-v7"
    assert s6.STOP_RECORDS == 3
    assert s6.REGISTERED_RECORD_SHA256[1] == (
        "55c05e31b10621043a09f468fb2a6cf9a1fb8dafb75cbf15904db1e6ef7efa67"
    )
    with pytest.raises(PermissionError, match="no reviewed sectors-v6 joint-run harness"):
        s6.check_open()


def test_sectors_v6_stop_exact_fields_dry_run_default_and_witnessed_verification(tmp_path, monkeypatch):
    vault, v8_reg, s6_reg, v8_stop_head = _setup_v8_stopped_and_s6_registered(tmp_path, monkeypatch)
    witness = s6.check_offhost(vault.cache, remote_url=str(vault.remote))
    assert witness.path == s6.WITNESS_PATH
    assert witness.census["records"]["vs1-v8"] == 3
    assert witness.census["records"]["sectors-v6"] == 2
    assert "vs1-v9" not in witness.census["records"]

    kw = {
        "v8_log_dir": v8_reg,
        "decision_ref": S6_DECISION_REF,
        "expected_prev_sha256": s6.REGISTERED_RECORD_SHA256[1],
        "witness": witness,
    }
    # Dry-run by default: appends nothing.
    preview = s6.append_superseded_stop(s6_reg, S6_STOP_AT, **kw)
    assert preview["dry_run"] is True
    assert s6.registry(s6_reg).verify_chain()["records"] == 2

    # Execute once.
    stop = s6.append_superseded_stop(s6_reg, S6_STOP_AT, dry_run=False, **kw)
    assert stop == preview["would_append"]
    stop_head = v1._record_sha256(stop)
    assert stop_head == preview["would_be_head_sha256"]
    assert stop["kind"] == "status"
    assert stop["version"] == "vs1-sectors-v6"
    assert stop["registry_id"] == "sectors-v6"
    assert stop["status"] == "STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED"
    assert stop["superseded_by"] == "vs1-sectors-v7"
    assert stop["stage0_run"] is False
    assert stop["prereg_gate_evaluated"] is False
    assert stop["inputs_frozen"] is False
    assert stop["discovery_opened"] is False
    assert stop["holdout_opened"] is False
    assert stop["prereg_sha256"] == s6.PREREG_BODY_SHA256
    assert stop["promotion_allowed"] is False
    assert stop["reason"] == s6.STOP_REASON
    assert stop["basis"] == s6.STOP_BASIS
    assert stop["decision_ref"] == S6_DECISION_REF
    assert stop["prev_sha256"] == "55c05e31b10621043a09f468fb2a6cf9a1fb8dafb75cbf15904db1e6ef7efa67"
    assert stop["run_at"] == S6_STOP_AT.isoformat()
    assert stop["v8_terminal"] == {
        "version": "vs1-v8",
        "head_sha256": v8_stop_head,
        "records": 3,
        "status": "STOP_SUPERSEDED_BY_V9_UNOPENED",
        "witness_path": v8.WITNESS_PATH,
        "witness_tip": witness.tip,
    }
    assert not any(k.startswith(("v9_", "sectors_v7_")) for k in stop)

    # Second execution refuses.
    with pytest.raises(PermissionError, match="two-record baseline"):
        s6.append_superseded_stop(s6_reg, S6_STOP_AT, dry_run=False, **kw)

    # Unwitnessed STOP refuses verify_terminal_stop.
    with pytest.raises(PermissionError, match="off-host anchor log"):
        s6.verify_terminal_stop(s6_reg, witness)

    # Publish sectors-v6 STOP anchor to vault and verify off-host.
    vault.add_file(s6.WITNESS_PATH, (s6_reg / s6.REGISTRY_ANCHORS).read_bytes())
    after = s6.check_offhost(vault.cache, remote_url=str(vault.remote))
    proof = s6.verify_terminal_stop(s6_reg, after)
    assert proof["records"] == 3
    assert proof["head_sha256"] == stop_head
    assert proof["status"] == "STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED"
    assert proof["witness_path"] == s6.WITNESS_PATH
    assert proof["witness_tip"] == after.tip
    assert proof["v8_terminal"] == stop["v8_terminal"]

    # The published sectors-v6 STOP anchor satisfies v9._sectors_v6_stop_anchor_at_tip.
    s6_stop_line = (s6_reg / s6.REGISTRY_ANCHORS).read_bytes().splitlines()[1]
    monkeypatch.setattr(v9, "SECTORS_V6_STOP_ANCHOR_LINE", s6_stop_line)
    v9._sectors_v6_stop_anchor_at_tip(after.repo, after.tip, stop_head)


def test_sectors_v6_stop_ordering_guards_refuse_nonterminal_v8_and_registered_v9(tmp_path, monkeypatch):
    # 1. Refuses when v8 is NOT terminal (at 2 records, registered only).
    vault_early = _base_vault(tmp_path / "vault_early")
    v8_early = tmp_path / "v8_early"
    _registered_v8(v8_early)
    vault_early.publish(v8_early)
    s6_early = tmp_path / "s6_early"
    _registered_s6(s6_early)
    vault_early.add_file(s6.WITNESS_PATH, (s6_early / s6.REGISTRY_ANCHORS).read_bytes())
    w_early = s6.check_offhost(vault_early.cache, remote_url=str(vault_early.remote))
    with pytest.raises(PermissionError, match="v8.*terminal|STOP"):
        s6.append_superseded_stop(
            s6_early,
            S6_STOP_AT,
            v8_log_dir=v8_early,
            decision_ref=S6_DECISION_REF,
            expected_prev_sha256=s6.REGISTERED_RECORD_SHA256[1],
            witness=w_early,
        )

    # 2. Refuses when v8 STOP exists locally ONLY (not witnessed on vault main).
    probe_bytes = _synthetic_probe_report()
    _bind_superseded_pins(monkeypatch, probe_bytes)
    probe_path = tmp_path / "probe_early.json"
    probe_path.write_bytes(probe_bytes)
    v8.append_superseded_stop(
        v8_early,
        V8_STOP_AT,
        probe_report_path=probe_path,
        decision_ref=DECISION_REF,
        expected_prev_sha256=v8.REGISTERED_RECORD_SHA256[1],
        witness=vault_early.witness(),
    )
    # Vault still has v8 at 2 records!
    w_unwitnessed = s6.check_offhost(vault_early.cache, remote_url=str(vault_early.remote))
    with pytest.raises(PermissionError, match="v8.*terminal|STOP"):
        s6.append_superseded_stop(
            s6_early,
            S6_STOP_AT,
            v8_log_dir=v8_early,
            decision_ref=S6_DECISION_REF,
            expected_prev_sha256=s6.REGISTERED_RECORD_SHA256[1],
            witness=w_unwitnessed,
        )

    # 3. Refuses when v8 ended in STOP_FOR_OWNER_UNDERPOWERED_UNOPENED instead of STOP_SUPERSEDED_BY_V9_UNOPENED.
    vault_under = _base_vault(tmp_path / "vault_under")
    v8_under = tmp_path / "v8_under"
    _registered_v8(v8_under)
    vault_under.publish(v8_under)
    s6_under = tmp_path / "s6_under"
    _registered_s6(s6_under)
    vault_under.add_file(s6.WITNESS_PATH, (s6_under / s6.REGISTRY_ANCHORS).read_bytes())
    power_path = tmp_path / "power.json"
    power_path.write_text(json.dumps(_power_doc(exposed=0.45)), encoding="utf-8")
    v8.append_stop_status(
        v8_under,
        V8_STOP_AT,
        power_path=power_path,
        decision_ref="underpowered",
        expected_prev_sha256=v8.REGISTERED_RECORD_SHA256[1],
        witness=vault_under.witness(),
    )
    vault_under.publish(v8_under)
    w_under = s6.check_offhost(vault_under.cache, remote_url=str(vault_under.remote))
    with pytest.raises(PermissionError, match="STOP_SUPERSEDED_BY_V9_UNOPENED"):
        s6.append_superseded_stop(
            s6_under,
            S6_STOP_AT,
            v8_log_dir=v8_under,
            decision_ref=S6_DECISION_REF,
            expected_prev_sha256=s6.REGISTERED_RECORD_SHA256[1],
            witness=w_under,
        )

    # 4. Refuses when vs1-v9 is ALREADY registered on vault main (ordering violation!).
    vault_v9, v8_reg, s6_reg, _ = _setup_v8_stopped_and_s6_registered(tmp_path / "with_v9", monkeypatch)
    v9_line = canonical({
        "head_sha256": "a" * 64,
        "prev_anchor_sha256": None,
        "records": 2,
        "run_at": S6_STOP_AT.isoformat(),
    })
    vault_v9.add_file(v9.WITNESS_PATH, v9_line + b"\n")
    w_v9 = s6.check_offhost(vault_v9.cache, remote_url=str(vault_v9.remote))
    with pytest.raises(PermissionError, match="vs1-v9"):
        s6.append_superseded_stop(
            s6_reg,
            S6_STOP_AT,
            v8_log_dir=v8_reg,
            decision_ref=S6_DECISION_REF,
            expected_prev_sha256=s6.REGISTERED_RECORD_SHA256[1],
            witness=w_v9,
        )

    # 5. Refuses when sectors-v7 or an unknown witness file is on vault main.
    vault_s7, v8_reg2, s6_reg2, _ = _setup_v8_stopped_and_s6_registered(tmp_path / "with_s7", monkeypatch)
    s7_line = canonical({
        "head_sha256": "b" * 64,
        "prev_anchor_sha256": None,
        "records": 2,
        "run_at": S6_STOP_AT.isoformat(),
    })
    vault_s7.add_file(v1.canonical_witness_path("sectors-v7"), s7_line + b"\n")
    w_s7 = s6.check_offhost(vault_s7.cache, remote_url=str(vault_s7.remote))
    with pytest.raises(PermissionError, match="sectors-v7"):
        s6.append_superseded_stop(
            s6_reg2,
            S6_STOP_AT,
            v8_log_dir=v8_reg2,
            decision_ref=S6_DECISION_REF,
            expected_prev_sha256=s6.REGISTERED_RECORD_SHA256[1],
            witness=w_s7,
        )

    vault_unk, v8_reg3, s6_reg3, _ = _setup_v8_stopped_and_s6_registered(tmp_path / "with_unk", monkeypatch)
    vault_unk.add_file("05-GRID/Paper-Log/vs1/rogue.anchors.jsonl", s7_line + b"\n")
    w_unk = s6.check_offhost(vault_unk.cache, remote_url=str(vault_unk.remote))
    with pytest.raises(PermissionError, match="unknown"):
        s6.append_superseded_stop(
            s6_reg3,
            S6_STOP_AT,
            v8_log_dir=v8_reg3,
            decision_ref=S6_DECISION_REF,
            expected_prev_sha256=s6.REGISTERED_RECORD_SHA256[1],
            witness=w_unk,
        )


def test_sectors_v6_stop_input_refusals_and_single_stop_kind_only(tmp_path, monkeypatch):
    vault, v8_reg, s6_reg, v8_stop_head = _setup_v8_stopped_and_s6_registered(tmp_path, monkeypatch)
    witness = s6.check_offhost(vault.cache, remote_url=str(vault.remote))
    base = {
        "v8_log_dir": v8_reg,
        "decision_ref": S6_DECISION_REF,
        "expected_prev_sha256": s6.REGISTERED_RECORD_SHA256[1],
        "witness": witness,
    }

    # Wrong prev head.
    with pytest.raises(ValueError, match="registration head"):
        s6.append_superseded_stop(s6_reg, S6_STOP_AT, **{**base, "expected_prev_sha256": "d" * 64})

    # Empty / whitespace decision_ref.
    for empty in ("", "   "):
        with pytest.raises(ValueError, match="decision reference"):
            s6.append_superseded_stop(s6_reg, S6_STOP_AT, **{**base, "decision_ref": empty})

    # Non-UTC run_at.
    for bad_time in (
        S6_STOP_AT.replace(tzinfo=None),
        S6_STOP_AT.astimezone(timezone(timedelta(hours=2))),
    ):
        with pytest.raises(ValueError, match="UTC"):
            s6.append_superseded_stop(s6_reg, bad_time, **base)

    # Wrong witness object / wrong witness path (v8 witness instead of sectors-v6 witness).
    with pytest.raises(PermissionError, match="off-host witness"):
        s6.append_superseded_stop(s6_reg, S6_STOP_AT, **{**base, "witness": witness.census})
    v8_witness = vault.witness()
    with pytest.raises(PermissionError, match="off-host witness"):
        s6.append_superseded_stop(s6_reg, S6_STOP_AT, **{**base, "witness": v8_witness})

    # Build a valid STOP record via dry-run, then verify _check_stop_record accepts ONLY the one kind.
    valid = s6.append_superseded_stop(s6_reg, S6_STOP_AT, **base)["would_append"]
    s6._check_stop_record(valid)

    for bad_status in (
        "STOP_FOR_OWNER_UNDERPOWERED_UNOPENED",
        "STOP_SUPERSEDED_BY_V9_UNOPENED",
        "STOP_UNKNOWN",
    ):
        with pytest.raises(PermissionError):
            s6._check_stop_record({**valid, "status": bad_status})

    # Extra fields (including circular v9 or sectors_v7 hashes) refuse.
    with pytest.raises(PermissionError, match="missing or extra fields|hash"):
        s6._check_stop_record({**valid, "v9_prereg_sha256": "c" * 64})
    with pytest.raises(PermissionError, match="missing or extra fields|hash"):
        s6._check_stop_record({**valid, "sectors_v7_prereg_sha256": "c" * 64})

    # Malformed v8_terminal refuses.
    for bad_v8 in (
        {**valid["v8_terminal"], "records": 2},
        {**valid["v8_terminal"], "status": "STOP_FOR_OWNER_UNDERPOWERED_UNOPENED"},
        {**valid["v8_terminal"], "head_sha256": "not-hex"},
        {**valid["v8_terminal"], "witness_tip": "not-hex40"},
    ):
        with pytest.raises(PermissionError, match="v8_terminal"):
            s6._check_stop_record({**valid, "v8_terminal": bad_v8})

    # verify_terminal_stop rejects a witnessed chain whose 3rd record is another STOP kind!
    bad_reg = tmp_path / "bad_s6_reg"
    bad_log = _registered_s6(bad_reg)
    bad_log.append([{
        **valid,
        "status": "STOP_FOR_OWNER_UNDERPOWERED_UNOPENED",
    }])
    vault.add_file(s6.WITNESS_PATH, (bad_reg / s6.REGISTRY_ANCHORS).read_bytes())
    w_bad = s6.check_offhost(vault.cache, remote_url=str(vault.remote))
    with pytest.raises(PermissionError):
        s6.verify_terminal_stop(bad_reg, w_bad)


def test_sectors_v6_cli_stop_superseded_dry_run_default_and_execute_head_check(tmp_path, monkeypatch, capsys):
    vault, v8_reg, s6_reg, v8_stop_head = _setup_v8_stopped_and_s6_registered(tmp_path, monkeypatch)
    monkeypatch.setattr(s6, "WITNESS_REMOTE_URL", str(vault.remote))

    base = [
        "stop-superseded",
        "--log-dir", str(s6_reg),
        "--v8-log-dir", str(v8_reg),
        "--vault-repo", str(vault.cache),
        "--decision-ref", S6_DECISION_REF,
        "--expected-prev-sha256", s6.REGISTERED_RECORD_SHA256[1],
        "--run-at", "2026-11-22T00:00:00+00:00",
    ]
    # Dry-run default.
    s6_cli.main(base)
    preview = json.loads(capsys.readouterr().out)
    assert preview["dry_run"] is True
    assert s6.registry(s6_reg).verify_chain()["records"] == 2

    # Wrong --expected-stop-head refuses.
    with pytest.raises(SystemExit, match="expected-stop-head"):
        s6_cli.main([*base, "--execute", "--expected-stop-head", "f" * 64])
    assert s6.registry(s6_reg).verify_chain()["records"] == 2

    # Exact --expected-stop-head executes.
    s6_cli.main([*base, "--execute", "--expected-stop-head", preview["would_be_head_sha256"]])
    out = json.loads(capsys.readouterr().out)
    assert out["head_sha256"] == preview["would_be_head_sha256"]
    assert out["chain"]["records"] == 3
    assert s6.registry(s6_reg).verify_chain()["records"] == 3

    # Second execution refuses.
    with pytest.raises(PermissionError, match="two-record baseline"):
        s6_cli.main([*base, "--execute", "--expected-stop-head", preview["would_be_head_sha256"]])

    # Publish anchor and run verify-stop CLI.
    vault.add_file(s6.WITNESS_PATH, (s6_reg / s6.REGISTRY_ANCHORS).read_bytes())
    s6_cli.main(["verify-stop", "--log-dir", str(s6_reg), "--vault-repo", str(vault.cache)])
    verified = json.loads(capsys.readouterr().out)
    assert verified["records"] == 3
    assert verified["head_sha256"] == preview["would_be_head_sha256"]
    assert verified["status"] == "STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED"
    assert verified["v8_terminal"]["head_sha256"] == v8_stop_head
