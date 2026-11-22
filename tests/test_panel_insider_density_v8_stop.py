"""Synthetic custody tests for VS1 v8 STOP_SUPERSEDED_BY_V9_UNOPENED; no DB, network or /data."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v3 as v3
from analysis import panel_insider_density_v4 as v4
from analysis import panel_insider_density_v5 as v5
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_v8 as v8
from analysis.research_forward_log import canonical
from scripts import run_vs1_v8_insider_density as v8_cli
from tests.test_panel_insider_density_v2 import _Vault
from tests.test_panel_insider_density_v8 import _power_doc

V8_REGISTERED_AT = datetime(2026, 10, 1, 15, 5, 27, tzinfo=timezone.utc)
V8_REGISTERED_CODE = "1f7c6b19efa467b1bcc3af6c8772a8e75e251965"
RUN_AT = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)
SYNTH_CODE_SHA = "390dbbed" + "0" * 32
SYNTH_R9_COMMIT = "4b9f2c80" + "0" * 32
SYNTH_R9_SHA = "051f6d4c" + "0" * 56
SYNTH_CROSS_SHA = "ab78b1b0" + "0" * 56
SYNTH_META_SHA = "84671265" + "0" * 56
DECISION_REF = "owner-ruling-R9-2026-10-07; owner-decision-go-with-v9-2026-11-22"


def _registered_v8(log_dir: Path):
    log = v8.registry(log_dir)
    log.append(v8.registration_records(V8_REGISTERED_AT, V8_REGISTERED_CODE))
    assert tuple(v1._line_sha256(log)) == v8.REGISTERED_RECORD_SHA256
    return log


def _vault_v8(root: Path, monkeypatch) -> _Vault:
    monkeypatch.setattr(v8, "check_census", lambda *a, **k: None)
    vault = _Vault(root, h=v8, seeds=("vs1-v1", "vs1-v2"))
    nl = b"\n"
    for m in (v3, v4, v5):
        vault.add_file(m.WITNESS_PATH, m.REGISTERED_ANCHOR_LINE + nl)
    v6_stop = canonical({
        "head_sha256": v7.V6_STOP_HEAD_SHA256,
        "prev_anchor_sha256": hashlib.sha256(v6.REGISTERED_ANCHOR_LINE).hexdigest(),
        "records": 3,
        "run_at": "2026-09-29T23:40:00+00:00",
    })
    vault.add_file(v6.WITNESS_PATH, v6.REGISTERED_ANCHOR_LINE + nl + v6_stop + nl)
    vault.add_file(v7.WITNESS_PATH, v7.REGISTERED_ANCHOR_LINE + nl + v8.V7_STOP_ANCHOR_LINE + nl)
    return vault


def _synthetic_probe_report(
    *,
    study: str = "vs1-v8",
    body_sha256: str = v8.PREREG_BODY_SHA256,
    benchmark_admitted: bool = False,
    code_sha: str = SYNTH_CODE_SHA,
    crosscheck_sha: str = SYNTH_CROSS_SHA,
    meta_sha: str = SYNTH_META_SHA,
    admitted_count: int = 195,
) -> bytes:
    admitted = [f"T{i:03d}" for i in range(admitted_count)]
    doc = {
        "probe": "vs1-gd4-price-admission-probe",
        "probe_version": 2,
        "prereg": {"study": study, "body_sha256": body_sha256, "rules": "v6 §2.2 rules 3-4, §2.3"},
        "code_sha": code_sha,
        "snapshot_as_of_ts": "2026-10-03T00:17:40+00:00",
        "window": "discovery",
        "read_window": {"start": "2007-11-02", "end": "2019-12-31"},
        "source": {"name": "TIINGO", "id": 524},
        "series_template": "YF:{ticker}:adj_close",
        "basis": "split+dividend adjusted",
        "benchmark": "XLK",
        "benchmark_admitted": benchmark_admitted,
        "benchmark_calendar_sessions": 3061,
        "benchmark_calendar": ["2007-11-02", "2019-12-31"],
        "crosscheck_report_sha256": crosscheck_sha,
        "tiingo_meta_report_sha256": meta_sha,
        "summary": {
            "candidates": 307,
            "admitted": admitted_count + (1 if benchmark_admitted else 0),
            "admitted_excluding_benchmark": admitted_count,
            "promotion_allowed": False,
        },
        "admitted": sorted(admitted + (["XLK"] if benchmark_admitted else [])),
        "listed_from": {t: "2007-11-02" for t in admitted},
        "tickers": {
            "XLK": {
                "ticker": "XLK",
                "benchmark": True,
                "admitted": benchmark_admitted,
                "reasons": [] if benchmark_admitted else ["crosscheck_disagreement"],
                "crosscheck": {
                    "passed": benchmark_admitted,
                    "reason": "pass" if benchmark_admitted else "disagreement",
                    "pairs": 3011,
                    "share_within": 0.995 if benchmark_admitted else 0.9810694121554301,
                },
            }
        },
    }
    return (json.dumps(doc, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _bind_superseded_pins(monkeypatch, probe_bytes: bytes) -> str:
    probe_sha = hashlib.sha256(probe_bytes).hexdigest()
    monkeypatch.setattr(v8, "PROBE_REPORT_SHA256", probe_sha)
    monkeypatch.setattr(v8, "CROSSCHECK_REPORT_SHA256", SYNTH_CROSS_SHA)
    monkeypatch.setattr(v8, "TIINGO_META_REPORT_SHA256", SYNTH_META_SHA)
    monkeypatch.setattr(v8, "PROBE_CODE_SHA", SYNTH_CODE_SHA)
    monkeypatch.setattr(v8, "OWNER_RULING_R9_VAULT_COMMIT", SYNTH_R9_COMMIT)
    monkeypatch.setattr(v8, "OWNER_RULING_R9_FILE_SHA256", SYNTH_R9_SHA)
    return probe_sha


def test_v8_is_superseded_by_v9_and_refuses_while_placeholders_remain(tmp_path, monkeypatch):
    assert v8.SUPERSEDED_BY == {"version": "vs1-v9"}
    with pytest.raises(PermissionError, match="superseded by vs1-v9"):
        v1.refuse_superseded(8, v8.SUPERSEDED_BY)
    log_dir = tmp_path / "reg"
    _registered_v8(log_dir)
    vault = _vault_v8(tmp_path / "vault", monkeypatch)
    vault.publish(log_dir)
    probe_path = tmp_path / "probe_report.json"
    probe_path.write_bytes(_synthetic_probe_report())
    with pytest.raises(PermissionError, match="placeholder"):
        v8.append_superseded_stop(
            log_dir,
            RUN_AT,
            probe_report_path=probe_path,
            decision_ref=DECISION_REF,
            expected_prev_sha256=v8.REGISTERED_RECORD_SHA256[1],
            witness=vault.witness(),
            dry_run=True,
        )


def test_v8_superseded_stop_exact_fields_and_openings_refused(tmp_path, monkeypatch):
    probe_bytes = _synthetic_probe_report()
    probe_sha = _bind_superseded_pins(monkeypatch, probe_bytes)
    probe_path = tmp_path / "probe_report.json"
    probe_path.write_bytes(probe_bytes)

    vault = _vault_v8(tmp_path / "vault", monkeypatch)
    log_dir = tmp_path / "reg"
    _registered_v8(log_dir)
    vault.publish(log_dir)
    prior = vault.witness()

    kw = {
        "probe_report_path": probe_path,
        "decision_ref": DECISION_REF,
        "expected_prev_sha256": v8.REGISTERED_RECORD_SHA256[1],
        "witness": prior,
    }
    preview = v8.append_superseded_stop(log_dir, RUN_AT, dry_run=True, **kw)
    assert preview["dry_run"] is True
    assert v8.registry(log_dir).verify_chain()["records"] == 2

    stop = v8.append_superseded_stop(log_dir, RUN_AT, **kw)
    assert stop == preview["would_append"]
    assert v1._record_sha256(stop) == preview["would_be_head_sha256"]
    assert stop["kind"] == "status"
    assert stop["version"] == "vs1-v8"
    assert stop["status"] == "STOP_SUPERSEDED_BY_V9_UNOPENED"
    assert stop["superseded_by"] == "vs1-v9"
    assert stop["stage0_run"] is False
    assert stop["prereg_gate_evaluated"] is False
    assert stop["inputs_frozen"] is False
    assert stop["discovery_opened"] is False
    assert stop["holdout_opened"] is False
    assert stop["prereg_sha256"] == v8.PREREG_BODY_SHA256
    assert stop["promotion_allowed"] is False
    assert stop["reason"] == v8.STOP_SUPERSEDED_REASON
    assert stop["basis"] == v8.STOP_SUPERSEDED_BASIS
    assert stop["decision_ref"] == DECISION_REF
    assert stop["prev_sha256"] == v8.REGISTERED_RECORD_SHA256[1]
    assert stop["run_at"] == RUN_AT.isoformat()
    assert not any(k.startswith("v9_") for k in stop)
    assert stop["probe"] == {
        "dir": "probe_20261003_001948",
        "probe_report_sha256": probe_sha,
        "crosscheck_report_sha256": SYNTH_CROSS_SHA,
        "tiingo_meta_report_sha256": SYNTH_META_SHA,
        "snapshot_as_of_ts": "2026-10-03T00:17:40+00:00",
        "code_sha": SYNTH_CODE_SHA,
        "benchmark_admitted": False,
        "benchmark_reason": "crosscheck_disagreement",
        "benchmark_share_within": 0.9810694121554301,
        "benchmark_pairs": 3011,
        "admitted_issuers": 195,
    }
    assert stop["owner_ruling"] == {
        "ruling": "R9",
        "vault_path": "00-Agent-Reports/2026-10-07/claude__grid-svr__grid-owner-rulings-r1-r12-confirmed-20261007.md",
        "vault_commit": SYNTH_R9_COMMIT,
        "file_sha256": SYNTH_R9_SHA,
        "text": (
            "Accept the rejection as a result. No rerun, no tolerance change, no power claim. "
            "A new adjudication rule needs its own reviewed scope before the two-model chain advances."
        ),
    }
    with pytest.raises(PermissionError, match="missing or extra fields"):
        v8._check_stop_record({**stop, "v9_prereg_sha256": "c" * 64})

    # Second execution refuses.
    with pytest.raises(PermissionError, match="two-record baseline"):
        v8.append_superseded_stop(log_dir, RUN_AT, **kw)

    # Openings refuse after STOP (and on a stale two-record copy).
    with pytest.raises(PermissionError, match="terminal STOP|superseded by vs1-v9"):
        v8.freeze_inputs(log_dir, RUN_AT, {"accept_underpowered": False})
    with pytest.raises(PermissionError, match="terminal STOP|superseded by vs1-v9"):
        v8.open_discovery(log_dir, RUN_AT, {}, prior)
    with pytest.raises(PermissionError, match="terminal STOP|superseded by vs1-v9"):
        v8.open_holdout(
            {},
            allow_holdout=True,
            prereg_sha256=v8.PREREG_BODY_SHA256,
            log_dir=log_dir,
            now=RUN_AT,
            observed={},
            witness=prior,
        )
    stale = tmp_path / "stale"
    _registered_v8(stale)
    with pytest.raises(PermissionError, match="superseded by vs1-v9"):
        v8.freeze_inputs(stale, RUN_AT, {"accept_underpowered": False})

    # Unwitnessed STOP refuses verify_terminal_stop; witnessed STOP passes.
    with pytest.raises(PermissionError, match="off-host anchor log"):
        v8.verify_terminal_stop(log_dir, prior)
    vault.publish(log_dir)
    proof = v8.verify_terminal_stop(log_dir, vault.witness())
    assert proof["records"] == 3
    assert proof["head_sha256"] == v1._record_sha256(stop)
    assert proof["status"] == "STOP_SUPERSEDED_BY_V9_UNOPENED"


def test_v8_superseded_stop_refusals(tmp_path, monkeypatch):
    probe_bytes = _synthetic_probe_report()
    _bind_superseded_pins(monkeypatch, probe_bytes)
    probe_path = tmp_path / "probe_report.json"
    probe_path.write_bytes(probe_bytes)

    vault = _vault_v8(tmp_path / "vault", monkeypatch)
    log_dir = tmp_path / "reg"
    log = _registered_v8(log_dir)
    vault.publish(log_dir)
    prior = vault.witness()
    head = v8.REGISTERED_RECORD_SHA256[1]
    base = {
        "probe_report_path": probe_path,
        "decision_ref": DECISION_REF,
        "expected_prev_sha256": head,
        "witness": prior,
        "dry_run": True,
    }

    # Wrong probe sha (1-byte change).
    tampered = tmp_path / "tampered.json"
    tampered.write_bytes(probe_bytes + b" ")
    with pytest.raises(PermissionError, match="pinned"):
        v8.append_superseded_stop(log_dir, RUN_AT, **{**base, "probe_report_path": tampered})

    # benchmark_admitted True (even if hash were pinned to it).
    admitted_bytes = _synthetic_probe_report(benchmark_admitted=True)
    admitted_path = tmp_path / "admitted.json"
    admitted_path.write_bytes(admitted_bytes)
    with monkeypatch.context() as m:
        m.setattr(v8, "PROBE_REPORT_SHA256", hashlib.sha256(admitted_bytes).hexdigest())
        with pytest.raises(PermissionError, match="benchmark"):
            v8.append_superseded_stop(log_dir, RUN_AT, **{**base, "probe_report_path": admitted_path})

    # Wrong prereg.study.
    wrong_study = _synthetic_probe_report(study="vs1-v7")
    wrong_study_path = tmp_path / "wrong_study.json"
    wrong_study_path.write_bytes(wrong_study)
    with monkeypatch.context() as m:
        m.setattr(v8, "PROBE_REPORT_SHA256", hashlib.sha256(wrong_study).hexdigest())
        with pytest.raises(PermissionError, match="vs1-v8"):
            v8.append_superseded_stop(log_dir, RUN_AT, **{**base, "probe_report_path": wrong_study_path})

    # Prev mismatch.
    with pytest.raises(ValueError, match="registration head"):
        v8.append_superseded_stop(log_dir, RUN_AT, **{**base, "expected_prev_sha256": "d" * 64})

    # Empty decision_ref.
    for empty in ("", "   "):
        with pytest.raises(ValueError, match="decision reference"):
            v8.append_superseded_stop(log_dir, RUN_AT, **{**base, "decision_ref": empty})

    # Non-UTC time (naive and non-zero offset).
    for bad_time in (RUN_AT.replace(tzinfo=None), RUN_AT.astimezone(timezone(timedelta(hours=2)))):
        with pytest.raises(ValueError, match="UTC"):
            v8.append_superseded_stop(log_dir, bad_time, **{**base})

    # Witness not at exactly 2 records.
    bad_witness = _vault_v8(tmp_path / "vault2", monkeypatch)
    reg2 = tmp_path / "reg2"
    l2 = _registered_v8(reg2)
    bad_witness.publish(reg2)
    l2.append([{"kind": "inputs_frozen", "prereg_sha256": v8.PREREG_BODY_SHA256, "run_at": RUN_AT.isoformat()}])
    bad_witness.publish(reg2)
    witness_at_3 = bad_witness.witness()
    with pytest.raises(PermissionError, match="two-record baseline"):
        v8.append_superseded_stop(log_dir, RUN_AT, **{**base, "witness": witness_at_3})

    # Local chain not at exactly 2 records.
    log.append([{"kind": "inputs_frozen", "prereg_sha256": v8.PREREG_BODY_SHA256, "run_at": RUN_AT.isoformat()}])
    with pytest.raises(PermissionError, match="two-record baseline"):
        v8.append_superseded_stop(log_dir, RUN_AT, **base)


def test_verify_terminal_stop_accepts_both_stop_kinds_and_rejects_unknown(tmp_path, monkeypatch):
    # Kind 1: STOP_FOR_OWNER_UNDERPOWERED_UNOPENED.
    vault1 = _vault_v8(tmp_path / "v1", monkeypatch)
    reg1 = tmp_path / "reg1"
    _registered_v8(reg1)
    vault1.publish(reg1)
    power = tmp_path / "power.json"
    power.write_text(json.dumps(_power_doc(exposed=0.45)), encoding="utf-8")
    v8.append_stop_status(
        reg1,
        RUN_AT,
        power_path=power,
        decision_ref="underpowered",
        expected_prev_sha256=v8.REGISTERED_RECORD_SHA256[1],
        witness=vault1.witness(),
    )
    vault1.publish(reg1)
    p1 = v8.verify_terminal_stop(reg1, vault1.witness())
    assert p1["records"] == 3 and p1["status"] == v8.STOP_STATUS

    # Kind 2: STOP_SUPERSEDED_BY_V9_UNOPENED.
    probe_bytes = _synthetic_probe_report()
    _bind_superseded_pins(monkeypatch, probe_bytes)
    probe_path = tmp_path / "probe.json"
    probe_path.write_bytes(probe_bytes)
    vault2 = _vault_v8(tmp_path / "v2", monkeypatch)
    reg2 = tmp_path / "reg2"
    _registered_v8(reg2)
    vault2.publish(reg2)
    v8.append_superseded_stop(
        reg2,
        RUN_AT,
        probe_report_path=probe_path,
        decision_ref=DECISION_REF,
        expected_prev_sha256=v8.REGISTERED_RECORD_SHA256[1],
        witness=vault2.witness(),
    )
    vault2.publish(reg2)
    p2 = v8.verify_terminal_stop(reg2, vault2.witness())
    assert p2["records"] == 3 and p2["status"] == v8.STOP_SUPERSEDED_STATUS

    # Unknown status refuses.
    with pytest.raises(PermissionError):
        v8._check_stop_record({"kind": "status", "version": "vs1-v8", "status": "STOP_UNKNOWN"})


def test_v8_cli_stop_superseded_dry_run_default_and_execute_head_check(tmp_path, monkeypatch, capsys):
    probe_bytes = _synthetic_probe_report()
    _bind_superseded_pins(monkeypatch, probe_bytes)
    probe_path = tmp_path / "probe.json"
    probe_path.write_bytes(probe_bytes)

    vault = _vault_v8(tmp_path / "vault", monkeypatch)
    log_dir = tmp_path / "reg"
    _registered_v8(log_dir)
    vault.publish(log_dir)
    witness = vault.witness()
    monkeypatch.setattr(v8, "check_offhost", lambda repo: witness)

    base = [
        "stop-superseded",
        "--log-dir", str(log_dir),
        "--vault-repo", str(tmp_path / "vault"),
        "--probe-report", str(probe_path),
        "--decision-ref", DECISION_REF,
        "--expected-prev-sha256", v8.REGISTERED_RECORD_SHA256[1],
        "--run-at", "2026-11-22T00:00:00+00:00",
    ]
    v8_cli.main(base)
    preview = json.loads(capsys.readouterr().out)
    assert preview["dry_run"] is True
    assert v8.registry(log_dir).verify_chain()["records"] == 2

    with pytest.raises(SystemExit, match="expected-stop-head"):
        v8_cli.main([*base, "--execute", "--expected-stop-head", "f" * 64])
    assert v8.registry(log_dir).verify_chain()["records"] == 2

    v8_cli.main([*base, "--execute", "--expected-stop-head", preview["would_be_head_sha256"]])
    out = json.loads(capsys.readouterr().out)
    assert out["head_sha256"] == preview["would_be_head_sha256"]
    assert v8.registry(log_dir).verify_chain()["records"] == 3
