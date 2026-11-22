"""Synthetic custody and benchmark-rule tests for VS1 sectors-v7.

No database, network, or /data access.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import threading
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v2 as s2
from analysis import panel_insider_density_sectors_v3 as s3
from analysis import panel_insider_density_sectors_v4 as s4
from analysis import panel_insider_density_sectors_v6 as s6
from analysis import panel_insider_density_sectors_v7 as s7
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
from analysis import panel_insider_density_v4 as v4
from analysis import panel_insider_density_v5 as v5
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_v8 as v8
from analysis import panel_insider_density_v9 as v9
from analysis.research_forward_log import ForwardLog, canonical
from scripts import run_vs1_sectors_v7_registration as s7_cli
from tests.test_panel_insider_density_sectors_v6_stop import (
    S6_DECISION_REF,
    S6_REGISTERED_AT,
    S6_REGISTERED_CODE,
    S6_STOP_AT,
    V8_STOP_AT,
)
from tests.test_panel_insider_density_v2 import _Vault
from tests.test_panel_insider_density_v8_stop import (
    DECISION_REF,
    SYNTH_CODE_SHA,
    SYNTH_R9_COMMIT,
    SYNTH_R9_SHA,
    V8_REGISTERED_AT,
    V8_REGISTERED_CODE,
)
from tests.test_panel_insider_density_v9 import (
    V6_STOP_LINE,
    _build_synthetic_probe_dir,
    _pin_probe_triple,
)

REPO = s4.REPO
V9_REG_AT = datetime(2026, 11, 22, 12, 10, tzinfo=timezone.utc)
S7_REG_AT = datetime(2026, 11, 22, 12, 15, tzinfo=timezone.utc)
V9_TERM_AT = datetime(2026, 11, 22, 12, 30, tzinfo=timezone.utc)


# --- 1. Preregistration body hash, one-byte tamper, required clauses & non-circular order ---


def test_sectors_v7_prereg_body_hash_one_byte_change_refuses_and_clauses(tmp_path):
    assert s7.check_prereg() == s7.PREREG_BODY_SHA256
    assert s7.PREREG_BODY_SHA256 != s6.PREREG_BODY_SHA256
    assert s7.REGISTRY_ID == "sectors-v7"
    assert s7.WITNESS_PATH == v1.canonical_witness_path("sectors-v7")
    assert s7.XNYS_MANIFEST_PATH == v9.XNYS_MANIFEST_PATH
    assert s7.XNYS_MANIFEST_SHA256 == v9.XNYS_MANIFEST_SHA256
    assert s7.load_xnys_calendar is v9.load_xnys_calendar

    body = v1.prereg_body((REPO / s7.PREREG_PATH).read_text(encoding="utf-8"))
    for phrase in (
        "VS1 v9",
        "@@V9_PREREG_BODY_SHA256@@",
        "@@V9_REGISTRATION_HEAD_SHA256@@",
        "@@V8_STOP_HEAD_SHA256@@",
        "@@SECTORS_V6_STOP_HEAD_SHA256@@",
        "STOP_SUPERSEDED_BY_V9_UNOPENED",
        "STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED",
        "vs1-xnys-2007-11-01-2026-06-30.json",
        v9.XNYS_MANIFEST_SHA256,
        s7.GATE_SPEC_SHA256,
        "75489d5091d82af64312f4523c41bdb52b0951a11e017644a022baa08a172822",
        "not available: benchmark not admitted",
        "XLRE",
        "XLC",
        "4 of 11",
        "4 of 10",
        "0.10",
        "promotion_allowed=false",
    ):
        assert phrase in body, f"missing required clause: {phrase!r}"

    # "benchmark admission" must NOT remain as a cause of an untestable sector.
    assert "admission probe, benchmark admission" not in body

    # Non-circular operator step order: v8 STOP -> sectors-v6 STOP -> v9 body approval ->
    # v9 registration -> fill sectors-v7 tokens -> owner approves final sectors-v7 body SHA ->
    # sectors-v7 registration.
    steps_section = body[body.index("## 5."):]
    numbered_steps = steps_section[steps_section.index("1. "):]
    ordered_markers = (
        "STOP_SUPERSEDED_BY_V9_UNOPENED",
        "STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED",
        "final v9 body SHA-256",
        "Register `vs1-v9`",
        "final `sectors-v7` body SHA-256",
        "Register `sectors-v7`",
    )
    positions = [numbered_steps.index(m) for m in ordered_markers]
    assert positions == sorted(positions), f"operator step order is circular or out of sequence: {positions}"

    # One-byte change inside the body must refuse.
    fake_repo = tmp_path / "repo"
    target = fake_repo / s7.PREREG_PATH
    target.parent.mkdir(parents=True)
    raw = (REPO / s7.PREREG_PATH).read_bytes()
    idx = raw.index(b"<!-- PREREG-BODY-START -->") + len(b"<!-- PREREG-BODY-START -->") + 5
    mutated = raw[:idx] + (b"X" if raw[idx : idx + 1] != b"X" else b"Y") + raw[idx + 1 :]
    target.write_bytes(mutated)
    with pytest.raises(PermissionError, match="differs from its pin"):
        s7.check_prereg(fake_repo)


# --- 2. Placeholder guards & GateSpec SHA check ---


def test_sectors_v7_placeholder_guards_and_gatespec_sha_check(monkeypatch, tmp_path):
    from analysis import generalization_gate as gate

    assert s7.GATE_SPEC_SHA256 == "fa2fa2bd1b1ef5cb81168f393135795824991e08829b60f66a109decdf401174"
    assert s7.V9_REGISTRATION_HEAD_SHA256 is None
    assert s7.REGISTERED_RECORD_SHA256 is None
    assert s7.REGISTERED_ANCHOR_LINE is None

    # Placeholders refuse registration, readiness, and registry construction.
    for call in (
        s7.check_ready_to_register,
        lambda: s7.registry(tmp_path),
        lambda: s7.registration_records(S7_REG_AT, "a" * 40),
        lambda: s7.register(tmp_path / "s7reg", S7_REG_AT, "a" * 40, v9_log_dir=tmp_path / "v9", witness=object()),
    ):
        with pytest.raises(PermissionError, match="placeholder|not bound"):
            call()

    # GateSpec SHA check refuses mismatch in code or pin.
    with monkeypatch.context() as m:
        m.setattr(gate, "GATE_SPEC_V1_SHA256", "0" * 64)
        with pytest.raises(PermissionError, match="GateSpec v1 in code differs"):
            s7.check_prereg()
    with monkeypatch.context() as m:
        m.setattr(s7, "GATE_SPEC_SHA256", None)
        with pytest.raises(PermissionError, match="GateSpec v1 in code differs"):
            s7.check_prereg()
    with monkeypatch.context() as m:
        m.setattr(s7, "PREREG_BODY_SHA256", None)
        with pytest.raises(PermissionError, match="not bound"):
            s7.check_prereg()


# --- 3. Benchmark rule: ETF failure never causes untestability; magnitudes "not available"; XLRE/XLC ---


def test_sectors_v7_etf_failure_does_not_make_sector_untestable_and_magnitudes_not_available():
    assert "benchmark_admission_failed" not in s7.UNTESTABLE_CAUSES
    assert s7.BENCHMARK_RULE["gates_admission"] is False
    assert s7.BENCHMARK_RULE["gates_testability"] is False
    assert s7.BENCHMARK_RULE["late_etf_pre_inception_applies_to"] == "magnitudes_only"

    # Sector ETF failure does NOT make a sector untestable (including XLRE and XLC!).
    for sector in ("Energy", "Financials", "Real Estate", "Communication Services"):
        res = s7.assess_sector_testability(
            sector,
            issuer_probe_passed=True,
            vendor_coverage_passed=True,
            benchmark_admitted=False,
            stage0_gaussian_power=0.80,
            stage0_e0_power=0.72,
        )
        assert res["testable"] is True
        assert res["untestable_reasons"] == ()
        assert res["terminal_kind_if_untestable"] is None
        assert res["benchmark_admitted"] is False
        assert res["etf_magnitudes_available"] is False
        assert res["magnitude_status"] == "not available: benchmark not admitted"

    # Real untestable causes still fail closed.
    for kwargs, expected_reason in (
        (
            {"issuer_probe_passed": False, "vendor_coverage_passed": True, "benchmark_admitted": True,
             "stage0_gaussian_power": 0.80, "stage0_e0_power": 0.72},
            "issuer_admission_probe_failed",
        ),
        (
            {"issuer_probe_passed": True, "vendor_coverage_passed": False, "benchmark_admitted": True,
             "stage0_gaussian_power": 0.80, "stage0_e0_power": 0.72},
            "vendor_coverage_failed",
        ),
        (
            {"issuer_probe_passed": True, "vendor_coverage_passed": True, "benchmark_admitted": True,
             "stage0_gaussian_power": 0.80, "stage0_e0_power": 0.49},
            "stage0_below_gate",
        ),
        (
            {"issuer_probe_passed": True, "vendor_coverage_passed": True, "benchmark_admitted": True,
             "stage0_gaussian_power": 0.49, "stage0_e0_power": 0.72},
            "stage0_below_gate",
        ),
        (
            {"issuer_probe_passed": True, "vendor_coverage_passed": True, "benchmark_admitted": True,
             "stage0_gaussian_power": None, "stage0_e0_power": 0.72},
            "stage0_below_gate",
        ),
    ):
        res = s7.assess_sector_testability("Energy", **kwargs)
        assert res["testable"] is False
        assert res["terminal_kind_if_untestable"] == "stage0_untestable"
        assert expected_reason in res["untestable_reasons"]

    with pytest.raises(ValueError, match="sector"):
        s7.assess_sector_testability(
            "Technology",
            issuer_probe_passed=True,
            vendor_coverage_passed=True,
            benchmark_admitted=False,
            stage0_gaussian_power=0.8,
            stage0_e0_power=0.8,
        )


def test_sectors_v7_xnys_sessions_raw_labels_magnitudes_and_xlre_pre_inception():
    # Sessions come from the pinned XNYS manifest via v9's loader.
    all_xnys = s7.load_xnys_calendar(REPO)
    sample_dates = [d for d in all_xnys if date(2015, 1, 2) <= d <= date(2016, 6, 30)]
    index = pd.DatetimeIndex([pd.Timestamp(d) for d in sample_dates])

    rng = np.random.default_rng(19)
    tickers = [f"RE{i:02d}" for i in range(28)]
    rets = rng.normal(0.0004, 0.015, size=(len(sample_dates), len(tickers)))
    closes = pd.DataFrame(100.0 * np.exp(np.cumsum(rets, axis=0)), index=index, columns=tickers)
    # XLRE starts in 2015-10 (NaN before inception, finite from 2015-10-08 onward).
    xlre_vals = 30.0 * np.exp(np.cumsum(rng.normal(0.0003, 0.01, size=len(sample_dates))))
    xlre_series = pd.Series(xlre_vals, index=index)
    xlre_series.loc[index < pd.Timestamp("2015-10-08")] = np.nan
    closes_with_etf = closes.assign(XLRE=xlre_series)

    with v1.discovery_window(v9.DISCOVERY_START):
        positions, labels, momentum = s7.raw_labels(closes, tickers, 5, "discovery")

    assert len(positions) >= v1.MIN_N
    feature = rng.poisson(0.4, size=labels.shape).astype(float)
    largest = np.where(feature > 0, rng.uniform(100_000, 900_000, size=labels.shape), 0.0)
    decided = [d.isoformat() for d in v1.decision_instants([sample_dates[i] for i in positions])]
    ends = [d.isoformat() for d in v1.decision_instants([sample_dates[i + 5] for i in positions])]
    groups = ["Real Estate"] * len(tickers)

    # Mode A: XLRE fails admission (benchmark_admitted=False, XLRE not even in closes!).
    with v1.discovery_window(v9.DISCOVERY_START):
        bench_fail = s7.sector_benchmark_returns(
            "Real Estate", closes, tickers, positions, 5, window="discovery", benchmark_admitted=False
        )
    assert bench_fail is None
    panel_fail = s7.TrialPanel(
        trial="A90|fwd5", window="discovery", horizon=5, decision_at=decided, label_end=ends,
        entities=tickers, feature=feature, label=labels, momentum=momentum, largest=largest,
        groups=groups, benchmark_admitted=False, benchmark_return=bench_fail,
    )
    m_fail = s7.measure_trial(panel_fail, perms=99, sensitivity=True)
    assert m_fail["status"] == "tested"
    mag_fail = m_fail["magnitude"]
    assert mag_fail["benchmark_admitted"] is False
    for key in (
        "buyer_minus_benchmark",
        "nonbuyer_minus_benchmark",
        "large_line_buyer_minus_benchmark",
        "small_line_buyer_minus_benchmark",
    ):
        assert mag_fail[key] == "not available: benchmark not admitted"
    assert isinstance(mag_fail["buyer_minus_nonbuyer"], float) and np.isfinite(mag_fail["buyer_minus_nonbuyer"])

    # Mode B: XLRE passes admission (benchmark_admitted=True): pre-inception dates use EW issuer mean,
    # post-inception dates use XLRE return; ONLY *_minus_benchmark magnitudes differ, IC/p identical!
    with v1.discovery_window(v9.DISCOVERY_START):
        bench_ok = s7.sector_benchmark_returns(
            "Real Estate", closes_with_etf, tickers, positions, 5, window="discovery", benchmark_admitted=True
        )
    assert bench_ok is not None and len(bench_ok) == len(positions)
    # Pre-inception decision date (index 0, in Jan 2015) uses equal-weighted issuer forward return!
    assert sample_dates[positions[0]] < date(2015, 10, 8)
    expected_ew0 = float(np.nanmean(labels[0]))
    assert bench_ok[0] == pytest.approx(expected_ew0, abs=1e-12)
    # Post-inception decision date (last position, in 2016) uses XLRE forward return!
    last_pos = positions[-1]
    assert sample_dates[last_pos] >= date(2015, 10, 8)
    expected_xlre_last = float(closes_with_etf["XLRE"].iloc[last_pos + 5] / closes_with_etf["XLRE"].iloc[last_pos] - 1.0)
    assert bench_ok[-1] == pytest.approx(expected_xlre_last, abs=1e-12)

    panel_ok = s7.TrialPanel(
        trial="A90|fwd5", window="discovery", horizon=5, decision_at=decided, label_end=ends,
        entities=tickers, feature=feature, label=labels, momentum=momentum, largest=largest,
        groups=groups, benchmark_admitted=True, benchmark_return=bench_ok,
    )
    m_ok = s7.measure_trial(panel_ok, perms=99, sensitivity=True)
    assert m_ok["status"] == "tested"
    for field in ("mean_ic", "p", "p_one_sided_positive", "p_one_sided_negative"):
        assert m_ok[field] == m_fail[field]
    mag_ok = m_ok["magnitude"]
    assert mag_ok["benchmark_admitted"] is True
    for key in (
        "buyer_minus_benchmark",
        "nonbuyer_minus_benchmark",
        "large_line_buyer_minus_benchmark",
        "small_line_buyer_minus_benchmark",
    ):
        assert isinstance(mag_ok[key], float) and np.isfinite(mag_ok[key])
    assert mag_ok["buyer_minus_nonbuyer"] == pytest.approx(mag_fail["buyer_minus_nonbuyer"], abs=1e-12)


# --- 4. Synthetic full custody fixture: v8 STOP -> sectors-v6 STOP -> v9 reg -> sectors-v7 ---


def _bind_v9_and_s7(monkeypatch, tmp_path: Path):
    """Build real synthetic v8 STOP -> sectors-v6 STOP -> v9 2-record registration, and bind s7."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(v9, "REGISTERED_RECORD_SHA256", None)
    monkeypatch.setattr(v9, "REGISTERED_ANCHOR_LINE", None)
    probe_dir = tmp_path / "probe_dir"
    probe_sha, cross_sha, meta_sha = _build_synthetic_probe_dir(probe_dir, n_issuers=195)
    _pin_probe_triple(monkeypatch, probe_sha, cross_sha, meta_sha)

    vault = _Vault(tmp_path / "vault", h=v8, seeds=("vs1-v1", "vs1-v2"))
    nl = b"\n"
    for m in (v3, v4, v5, s2, s3, s4):
        vault.add_file(m.WITNESS_PATH, m.REGISTERED_ANCHOR_LINE + nl)
    vault.add_file(v6.WITNESS_PATH, v6.REGISTERED_ANCHOR_LINE + nl + V6_STOP_LINE + nl)
    vault.add_file(v7.WITNESS_PATH, v7.REGISTERED_ANCHOR_LINE + nl + v8.V7_STOP_ANCHOR_LINE + nl)

    # v8 registered + published, sectors-v6 registered + published.
    v8_reg = tmp_path / "v8_reg"
    v8.registry(v8_reg).append(v8.registration_records(V8_REGISTERED_AT, V8_REGISTERED_CODE))
    vault.publish(v8_reg)
    s6_reg = tmp_path / "s6_reg"
    s6.registry(s6_reg).append(s6.registration_records(S6_REGISTERED_AT, S6_REGISTERED_CODE))
    vault.add_file(s6.WITNESS_PATH, (s6_reg / s6.REGISTRY_ANCHORS).read_bytes())

    # Step 2: v8 terminal STOP witnessed.
    stop8 = v8.append_superseded_stop(
        v8_reg,
        V8_STOP_AT,
        probe_report_path=probe_dir / "probe_report.json",
        decision_ref=DECISION_REF,
        expected_prev_sha256=v8.REGISTERED_RECORD_SHA256[1],
        witness=vault.witness(),
    )
    vault.publish(v8_reg)
    v8_stop_head = v1._record_sha256(stop8)
    v8_stop_line = (v8_reg / v8.REGISTRY_ANCHORS).read_bytes().splitlines()[1]

    # Step 3: sectors-v6 terminal STOP witnessed.
    w_s6 = s6.check_offhost(vault.cache, remote_url=str(vault.remote))
    stop6 = s6.append_superseded_stop(
        s6_reg,
        S6_STOP_AT,
        v8_log_dir=v8_reg,
        decision_ref=S6_DECISION_REF,
        expected_prev_sha256=s6.REGISTERED_RECORD_SHA256[1],
        witness=w_s6,
        dry_run=False,
    )
    vault.add_file(s6.WITNESS_PATH, (s6_reg / s6.REGISTRY_ANCHORS).read_bytes())
    s6_stop_head = v1._record_sha256(stop6)
    s6_stop_line = (s6_reg / s6.REGISTRY_ANCHORS).read_bytes().splitlines()[1]

    # Step 4: fill v9 body tokens and bind v9 pins (always read from the committed template).
    v9_text = (REPO / "docs/paper_log/vs1-insider-density-v9-preregistration.md").read_text(encoding="utf-8")
    for old, new in {
        "@@PROBE_REPORT_SHA256@@": probe_sha,
        "@@CROSSCHECK_REPORT_SHA256@@": cross_sha,
        "@@TIINGO_META_REPORT_SHA256@@": meta_sha,
        "@@PROBE_CODE_SHA@@": SYNTH_CODE_SHA,
        "@@OWNER_RULING_R9_VAULT_COMMIT@@": SYNTH_R9_COMMIT,
        "@@OWNER_RULING_R9_FILE_SHA256@@": SYNTH_R9_SHA,
        "@@V8_STOP_HEAD_SHA256@@": v8_stop_head,
        "@@SECTORS_V6_STOP_HEAD_SHA256@@": s6_stop_head,
    }.items():
        v9_text = v9_text.replace(old, new)
    v9_filled = tmp_path / "vs1-v9-prereg.md"
    v9_filled.write_text(v9_text, encoding="utf-8")
    v9_body_sha = v1.prereg_body_sha256(v9_filled)

    monkeypatch.setattr(v9, "PREREG_PATH", v9_filled)
    monkeypatch.setattr(v9, "PREREG_BODY_SHA256", v9_body_sha)
    monkeypatch.setattr(v9, "V8_STOP_HEAD_SHA256", v8_stop_head)
    monkeypatch.setattr(v9, "V8_STOP_ANCHOR_LINE", v8_stop_line)
    monkeypatch.setattr(v9, "SECTORS_V6_STOP_HEAD_SHA256", s6_stop_head)
    monkeypatch.setattr(v9, "SECTORS_V6_STOP_ANCHOR_LINE", s6_stop_line)

    # Step 5: register v9 (2 records) and publish to vault.
    v9_reg = tmp_path / "v9_reg"
    v9.register(v9_reg, V9_REG_AT, "a" * 40, v8_log_dir=v8_reg, vault_repo=vault.cache, remote_url=str(vault.remote))
    v9_heads = tuple(v1._line_sha256(v9.registry(v9_reg)))
    v9_anchor = (v9_reg / v9.REGISTRY_ANCHORS).read_bytes().splitlines()[0]
    monkeypatch.setattr(v9, "REGISTERED_RECORD_SHA256", v9_heads)
    monkeypatch.setattr(v9, "REGISTERED_ANCHOR_LINE", v9_anchor)
    vault.add_file(v9.WITNESS_PATH, v9_anchor + nl)

    # Step 6: fill sectors-v7 body tokens (now that ALL embedded tokens exist!) and bind s7 pins.
    s7_text = (REPO / "docs/paper_log/vs1-sectors-v7-preregistration.md").read_text(encoding="utf-8")
    for old, new in {
        "@@V9_PREREG_BODY_SHA256@@": v9_body_sha,
        "@@V9_REGISTRATION_HEAD_SHA256@@": v9_heads[1],
        "@@V8_STOP_HEAD_SHA256@@": v8_stop_head,
        "@@SECTORS_V6_STOP_HEAD_SHA256@@": s6_stop_head,
    }.items():
        s7_text = s7_text.replace(old, new)
    s7_filled = tmp_path / "vs1-sectors-v7-prereg.md"
    s7_filled.write_text(s7_text, encoding="utf-8")
    s7_body_sha = v1.prereg_body_sha256(s7_filled)

    monkeypatch.setattr(s7, "PREREG_PATH", s7_filled)
    monkeypatch.setattr(s7, "PREREG_BODY_SHA256", s7_body_sha)
    monkeypatch.setattr(s7, "V9_REGISTRATION_HEAD_SHA256", v9_heads[1])
    monkeypatch.setattr(s7, "REGISTERED_RECORD_SHA256", None)
    monkeypatch.setattr(s7, "REGISTERED_ANCHOR_LINE", None)

    return {
        "vault": vault,
        "v8_reg": v8_reg,
        "s6_reg": s6_reg,
        "v9_reg": v9_reg,
        "v8_stop_head": v8_stop_head,
        "s6_stop_head": s6_stop_head,
        "v9_body_sha": v9_body_sha,
        "v9_heads": v9_heads,
        "s7_body_sha": s7_body_sha,
    }


# --- 5. Registration binds v9 head and succeeds ONLY while v9 is at exactly 2 records ---


def test_sectors_v7_register_binds_v9_head_and_only_while_v9_at_two_records(monkeypatch, tmp_path):
    ctx = _bind_v9_and_s7(monkeypatch, tmp_path)
    vault, v9_reg, v9_heads = ctx["vault"], ctx["v9_reg"], ctx["v9_heads"]
    witness = v9.check_offhost(vault.cache, remote_url=str(vault.remote))
    assert witness.census["records"]["vs1-v9"] == 2
    assert "sectors-v7" not in witness.census["records"]

    kw = {"v9_log_dir": v9_reg, "witness": witness}

    # Raw census dict instead of OffhostWitness refuses.
    with pytest.raises(PermissionError, match="fresh v9 off-host witness"):
        s7.register(tmp_path / "s7reg", S7_REG_AT, "c" * 40, v9_log_dir=v9_reg, witness=witness.census)

    # Registration time before v9 registration refuses.
    with pytest.raises(ValueError, match="follow v9's registration"):
        s7.register(tmp_path / "s7reg", datetime(2026, 11, 22, 12, 9, tzinfo=timezone.utc), "c" * 40, **kw)

    # Dry-run by default creates no log.
    preview = s7.register(tmp_path / "s7reg", S7_REG_AT, "c" * 40, **kw)
    assert preview["dry_run"] is True
    assert not (tmp_path / "s7reg" / s7.REGISTRY_LOG).exists()

    header, prereg = s7.registration_records(S7_REG_AT, "c" * 40)
    assert header["kind"] == "header" and header["version"] == "vs1-sectors-v7"
    assert prereg["technology_run"]["version"] == "vs1-v9"
    assert prereg["technology_run"]["registration_head_sha256"] == v9_heads[1]
    assert prereg["technology_run"]["prereg_sha256"] == ctx["v9_body_sha"]
    assert prereg["technology_run"]["registered_while_v9_records"] == 2
    assert prereg["terminal_stops"]["vs1-v8"]["head_sha256"] == ctx["v8_stop_head"]
    assert prereg["terminal_stops"]["vs1-sectors-v6"]["head_sha256"] == ctx["s6_stop_head"]
    assert prereg["terminal_stops"]["vs1-v7"]["head_sha256"] == v8.V7_STOP_HEAD_SHA256
    assert prereg["terminal_stops"]["vs1-v6"]["head_sha256"] == v7.V6_STOP_HEAD_SHA256
    assert prereg["supersedes"]["version"] == "vs1-sectors-v6"
    assert prereg["supersedes"]["terminal_stop_head_sha256"] == ctx["s6_stop_head"]
    assert prereg["calendar_manifest"]["sha256"] == v9.XNYS_MANIFEST_SHA256
    assert prereg["benchmark_rule"]["gates_admission"] is False
    assert prereg["benchmark_rule"]["gates_testability"] is False
    assert prereg["generalization_gate"]["spec_sha256"] == s7.GATE_SPEC_SHA256
    assert prereg["stage0"]["e0"]["manifest_sha256"] == v9.E0_MANIFEST_SHA256
    assert prereg["discovery"]["start"] == v9.DISCOVERY_START
    assert prereg["promotion_allowed"] is False

    # Execute registration.
    out = s7.register(tmp_path / "s7reg", S7_REG_AT, "c" * 40, dry_run=False, **kw)
    assert out["head_sha256"] == preview["would_register_sha256"][-1]
    assert s7.registry(tmp_path / "s7reg").verify_chain()["records"] == 2

    # Second registration in same dir refuses.
    with pytest.raises(PermissionError, match="already registered"):
        s7.register(tmp_path / "s7reg", S7_REG_AT, "c" * 40, dry_run=False, **kw)

    # Published sectors-v7 anchor is accepted by v9's opening census, and pinning enforces exact anchor.
    s7_anchor = (tmp_path / "s7reg" / s7.REGISTRY_ANCHORS).read_bytes()
    vault.add_file(s7.WITNESS_PATH, s7_anchor)
    after = v9.check_offhost(vault.cache, remote_url=str(vault.remote))
    assert after.census["records"]["sectors-v7"] == 2
    assert not v9.contamination([after.census])["contaminated"]

    with monkeypatch.context() as m:
        m.setattr(s7, "REGISTERED_ANCHOR_LINE", s7_anchor.splitlines()[0])
        m.setattr(s7, "REGISTERED_RECORD_SHA256", tuple(preview["would_register_sha256"]))
        v9.check_offhost(vault.cache, remote_url=str(vault.remote))
        # Fork attempt refuses.
        with pytest.raises(PermissionError, match="fork its pinned chain"):
            s7.register(tmp_path / "s7fork", S7_REG_AT, "d" * 40, **kw)
        # Wrong pinned anchor line refuses in v9.
        m.setattr(s7, "REGISTERED_ANCHOR_LINE", s7_anchor.splitlines()[0].replace(b"2026-11-22", b"2026-11-23"))
        with pytest.raises(PermissionError, match="pinned two-record"):
            v9.check_offhost(vault.cache, remote_url=str(vault.remote))

    # Wrong bound v9 head refuses.
    with monkeypatch.context() as m:
        m.setattr(s7, "V9_REGISTRATION_HEAD_SHA256", "d" * 64)
        with pytest.raises(PermissionError, match="not bound"):
            s7.registration_records(S7_REG_AT, "c" * 40)

    # Leftover placeholder in body refuses even when code pins are bound.
    with monkeypatch.context() as m:
        leftover = tmp_path / "leftover.md"
        leftover.write_bytes(s7.PREREG_PATH.read_bytes().replace(v9_heads[1].encode(), b"@@V9_HEAD@@"))
        m.setattr(s7, "PREREG_PATH", leftover)
        m.setattr(s7, "PREREG_BODY_SHA256", v1.prereg_body_sha256(leftover))
        with pytest.raises(PermissionError, match="placeholder"):
            s7.check_ready_to_register()

    # Refuses when census shows v9 grew past 2 records!
    grown = {**witness.census, "records": {**witness.census["records"], "vs1-v9": 3}}
    with pytest.raises(PermissionError, match="before any v9 opening"):
        s7.check_registration_census(grown, v9_log_dir=v9_reg, witness_repo=witness.repo)

    # Refuses when sectors-v7 is ALREADY in census!
    with pytest.raises(PermissionError, match="exact pre-sectors-v7 baseline"):
        s7.check_registration_census(after.census, v9_log_dir=v9_reg, witness_repo=after.repo)

    # Refuses when local v9 chain grew past 2 records even while witness still shows 2!
    v9.registry(v9_reg).append([{
        "kind": "inputs_frozen",
        "prereg_sha256": ctx["v9_body_sha"],
        "run_at": S7_REG_AT.isoformat(),
    }])
    with pytest.raises(PermissionError, match="differs from its witness"):
        s7.verify_v9_registration(v9_reg, witness.repo, witness.tip)
    with pytest.raises(PermissionError):
        s7.register(tmp_path / "s7reg2", S7_REG_AT, "c" * 40, v9_log_dir=v9_reg, witness=witness)


# --- 6. check_open refuses until v9 is terminal (and stays fail-closed without reviewed harness) ---


def test_sectors_v7_check_open_refuses_until_v9_terminal(monkeypatch, tmp_path):
    # No-arg check_open always refuses.
    with pytest.raises(PermissionError, match="v9.*terminal|no reviewed sectors-v7"):
        s7.check_open()

    # Branch A: v9 progresses through non-terminal states to witnessed holdout_result.
    ctx = _bind_v9_and_s7(monkeypatch, tmp_path / "holdout_branch")
    vault, v9_reg, v9_body_sha = ctx["vault"], ctx["v9_reg"], ctx["v9_body_sha"]

    # Register sectors-v7 while v9 is at 2 records and publish.
    w2 = v9.check_offhost(vault.cache, remote_url=str(vault.remote))
    s7_reg = tmp_path / "holdout_branch" / "s7_reg"
    s7.register(s7_reg, S7_REG_AT, "c" * 40, v9_log_dir=v9_reg, witness=w2, dry_run=False)
    vault.add_file(s7.WITNESS_PATH, (s7_reg / s7.REGISTRY_ANCHORS).read_bytes())

    # At 2 records (registered only): NOT terminal -> refuses!
    w_reg = v9.check_offhost(vault.cache, remote_url=str(vault.remote))
    with pytest.raises(PermissionError, match="not terminal"):
        s7.verify_v9_terminal(v9_reg, w_reg)
    with pytest.raises(PermissionError, match="not terminal"):
        s7.check_open(v9_log_dir=v9_reg, witness=w_reg)

    # Non-terminal states: inputs_frozen, discovery_opened, discovery_frozen, holdout_opened.
    log9 = v9.registry(v9_reg)
    for kind in ("inputs_frozen", "discovery_opened", "discovery_frozen", "holdout_opened"):
        log9.append([{"kind": kind, "prereg_sha256": v9_body_sha, "run_at": V9_TERM_AT.isoformat()}])
        vault.add_file(v9.WITNESS_PATH, (v9_reg / v9.REGISTRY_ANCHORS).read_bytes())
        w_mid = v9.check_offhost(vault.cache, remote_url=str(vault.remote))
        with pytest.raises(PermissionError, match="not terminal"):
            s7.verify_v9_terminal(v9_reg, w_mid)
        with pytest.raises(PermissionError, match="not terminal"):
            s7.check_open(v9_log_dir=v9_reg, witness=w_mid)

    # Local holdout_result NOT YET witnessed on vault main -> refuses!
    log9.append([{
        "kind": "holdout_result",
        "prereg_sha256": v9_body_sha,
        "run_at": V9_TERM_AT.isoformat(),
        "verdict": "NO_SURVIVOR",
        "promotion_allowed": False,
    }])
    with pytest.raises(PermissionError, match="off-host|witness"):
        s7.verify_v9_terminal(v9_reg, w_mid)
    with pytest.raises(PermissionError, match="off-host|witness"):
        s7.check_open(v9_log_dir=v9_reg, witness=w_mid)

    # Witnessed holdout_result -> verify_v9_terminal SUCCEEDS; check_open passes terminal check
    # and refuses only because no reviewed sectors-v7 joint-run harness exists.
    vault.add_file(v9.WITNESS_PATH, (v9_reg / v9.REGISTRY_ANCHORS).read_bytes())
    w_done = v9.check_offhost(vault.cache, remote_url=str(vault.remote))
    term = s7.verify_v9_terminal(v9_reg, w_done)
    assert term["terminal_kind"] == "holdout_result"
    assert term["records"] == 7
    assert term["head_sha256"] == v1._line_sha256(log9)[-1]
    assert term["witness_tip"] == w_done.tip
    with pytest.raises(PermissionError, match="no reviewed sectors-v7 joint-run harness"):
        s7.check_open(v9_log_dir=v9_reg, witness=w_done)

    # Branch B: v9 ends in a witnessed STOP_FOR_OWNER_UNDERPOWERED_UNOPENED (3 records).
    ctx_b = _bind_v9_and_s7(monkeypatch, tmp_path / "stop_branch")
    vault_b, v9_reg_b, v9_body_b = ctx_b["vault"], ctx_b["v9_reg"], ctx_b["v9_body_sha"]
    w_b2 = v9.check_offhost(vault_b.cache, remote_url=str(vault_b.remote))
    proxy_n = len(v1.proxy_sessions(date(2008, 1, 1), date(2020, 1, 1)))
    table = {
        t: [{"target_ic": ic, "power": 0.4, "sims": v1.POWER_SIMS} for ic in v1.POWER_TARGET_ICS]
        for t in v1.trial_names()
    }
    e0row = {
        "sims": v9.E0_SIMS,
        "perms": v1.POWER_PERMS,
        "alpha_one_sided": 0.05,
        "target_ic": 0.01,
        "e0_manifest_sha256": v9.E0_MANIFEST_SHA256,
        "e0_seeds": {"base": 20260930, "pilot": 20260931},
    }
    power_doc = {
        "version": v9.VERSION,
        "primary_trial": v9.PRIMARY_TRIAL,
        "settings": v1.power_settings(),
        "table": table,
        "gate_passed": False,
        "v9_stage0": {
            "settings": json.loads(json.dumps(v9.STAGE0_SETTINGS)),
            "models": {
                "gaussian_v1": {
                    "model": "gaussian_v1", "power": 0.62, "sims": v9.GAUSSIAN_SIMS,
                    "perms": v1.POWER_PERMS, "seed": v1.SEED, "alpha_one_sided": 0.05, "target_ic": 0.01,
                },
                "e0:factor_t_garch_exposed": {**e0row, "model": "e0:factor_t_garch_exposed", "power": 0.44},
                "e0:factor_t_garch": {**e0row, "model": "e0:factor_t_garch", "power": 0.46},
            },
            "discovery_start": v9.DISCOVERY_START,
            "n_sessions": proxy_n,
        },
        "inputs": {
            "price_manifest_sha256": "1" * 64,
            "form4_receipt_sha256": "2" * 64,
            "admission_receipt_sha256": "3" * 64,
            "universe_sha256": "4" * 64,
            "prereg_sha256": v9_body_b,
        },
    }
    p_path = tmp_path / "stop_branch" / "power_under.json"
    p_path.write_text(json.dumps(power_doc), encoding="utf-8")
    v9.append_stop_status(
        v9_reg_b,
        V9_TERM_AT,
        power_path=p_path,
        decision_ref="owner-v9-stage0-stop",
        expected_prev_sha256=ctx_b["v9_heads"][1],
        witness=w_b2,
    )
    vault_b.add_file(v9.WITNESS_PATH, (v9_reg_b / v9.REGISTRY_ANCHORS).read_bytes())
    w_b_stop = v9.check_offhost(vault_b.cache, remote_url=str(vault_b.remote))
    term_b = s7.verify_v9_terminal(v9_reg_b, w_b_stop)
    assert term_b["terminal_kind"] == "stop"
    assert term_b["records"] == 3
    assert term_b["status"] == v9.STOP_STATUS
    with pytest.raises(PermissionError, match="no reviewed sectors-v7 joint-run harness"):
        s7.check_open(v9_log_dir=v9_reg_b, witness=w_b_stop)


# --- 7. Registration CLI dry-run default & --execute --expected-head ---


def test_sectors_v7_cli_dry_run_default_and_execute_head_check(monkeypatch, tmp_path, capsys):
    s7_cli.main(["hash-prereg"])
    hashed = json.loads(capsys.readouterr().out)
    assert hashed["body_sha256"] == s7.PREREG_BODY_SHA256
    assert hashed["matches_pinned"] is True

    monkeypatch.setattr(s7, "check_ready_to_register", lambda: "x")
    monkeypatch.setattr(v9, "check_offhost", lambda repo: object())
    calls = []

    def fake_register(log_dir, now, code_sha, *, dry_run=True, **kw):
        calls.append(dry_run)
        if dry_run:
            return {"dry_run": True, "would_register_sha256": ["a" * 64, "b" * 64]}
        return {"appended": ["header", "preregistration"], "head_sha256": "b" * 64}

    class FakeLog:
        def verify_chain(self):
            return {"ok": True, "records": 2}

    monkeypatch.setattr(s7, "register", fake_register)
    monkeypatch.setattr(s7, "registry", lambda log_dir: FakeLog())

    base = [
        "register",
        "--log-dir", str(tmp_path / "r"),
        "--v9-log-dir", str(tmp_path / "v9"),
        "--vault-repo", str(tmp_path),
        "--code-sha", "c" * 40,
        "--run-at", "2026-11-22T00:00:00Z",
    ]
    s7_cli.main(base)
    assert calls == [True]
    capsys.readouterr()

    with pytest.raises(SystemExit, match="expected-head"):
        s7_cli.main([*base, "--execute", "--expected-head", "f" * 64])
    assert calls == [True, True]

    s7_cli.main([*base, "--execute", "--expected-head", "b" * 64])
    assert calls == [True, True, True, False]
    out = json.loads(capsys.readouterr().out)
    assert out["head_sha256"] == "b" * 64

    for bad in ("2026-11-22T00:00:00", "2026-11-22T00:00:00+02:00", "2999-01-01T00:00:00+00:00", "nope"):
        with pytest.raises(SystemExit):
            s7_cli.main([*base[:-1], bad])


# --- 8. Concurrent v9 advance regressions, parent->child lock order & post-append validation ---


def test_sectors_v7_register_serializes_concurrent_v9_thread_under_parent_lock(monkeypatch, tmp_path):
    """A concurrent v9 writer landing between check and append cannot advance v9 before sectors-v7 validates."""
    ctx = _bind_v9_and_s7(monkeypatch, tmp_path)
    vault, v9_reg = ctx["vault"], ctx["v9_reg"]
    s7_reg = tmp_path / "s7_reg"
    witness = v9.check_offhost(vault.cache, remote_url=str(vault.remote))

    between_check_and_append = threading.Event()
    v9_writer_started = threading.Event()
    v9_appended = threading.Event()
    writer_errors: list[BaseException] = []
    observed: dict[str, object] = {}

    def v9_writer() -> None:
        try:
            between_check_and_append.wait(timeout=5.0)
            v9_writer_started.set()
            v9.registry(v9_reg).append([{
                "kind": "inputs_frozen",
                "prereg_sha256": ctx["v9_body_sha"],
                "run_at": S7_REG_AT.isoformat(),
            }])
            v9_appended.set()
        except BaseException as exc:  # pragma: no cover - surfaced on main thread
            writer_errors.append(exc)

    orig_registration_records = s7.registration_records

    def hooked_registration_records(*args, **kwargs):
        recs = orig_registration_records(*args, **kwargs)
        between_check_and_append.set()
        assert v9_writer_started.wait(timeout=2.0)
        # Give the concurrent v9 thread ample time to acquire v9's lock if it was released early.
        v9_appended.wait(timeout=0.4)
        return recs

    orig_append_locked = ForwardLog.append_locked

    def spied_append_locked(self, records):
        if self.path == s7_reg / s7.REGISTRY_LOG:
            v9_lines = [
                line for line in (v9_reg / v9.REGISTRY_LOG).read_bytes().splitlines() if line
            ]
            observed["v9_lines_at_s7_append"] = len(v9_lines)
            observed["v9_lock_held_at_s7_append"] = (v9_reg / v9.REGISTRY_LOCK).exists()
            observed["s7_lock_held_at_s7_append"] = (s7_reg / s7.REGISTRY_LOCK).exists()
            observed["v9_appended_before_s7_write"] = v9_appended.is_set()
        return orig_append_locked(self, records)

    monkeypatch.setattr(s7, "registration_records", hooked_registration_records)
    monkeypatch.setattr(ForwardLog, "append_locked", spied_append_locked)

    t = threading.Thread(target=v9_writer, daemon=True)
    t.start()
    try:
        out = s7.register(
            s7_reg,
            S7_REG_AT,
            "c" * 40,
            v9_log_dir=v9_reg,
            witness=witness,
            dry_run=False,
        )
    finally:
        between_check_and_append.set()
        t.join(timeout=5.0)

    assert not t.is_alive()
    assert not writer_errors, f"concurrent v9 writer raised: {writer_errors}"
    assert v9_appended.is_set()
    assert observed["v9_lock_held_at_s7_append"] is True
    assert observed["s7_lock_held_at_s7_append"] is True
    assert observed["v9_lines_at_s7_append"] == 2
    assert observed["v9_appended_before_s7_write"] is False
    assert out["appended"] == ["header", "preregistration"]
    assert s7.registry(s7_reg).verify_chain()["records"] == 2
    assert len(v9.registry(v9_reg).read_all()) == 3
    assert not (v9_reg / v9.REGISTRY_LOCK).exists()
    assert not (s7_reg / s7.REGISTRY_LOCK).exists()


def test_sectors_v7_register_refuses_injected_v9_advance_between_check_and_append(monkeypatch, tmp_path):
    """If v9 advances out-of-band between check and append, sectors-v7 refuses and writes nothing."""
    ctx = _bind_v9_and_s7(monkeypatch, tmp_path)
    vault, v9_reg = ctx["vault"], ctx["v9_reg"]
    s7_reg = tmp_path / "s7_reg"
    witness = v9.check_offhost(vault.cache, remote_url=str(vault.remote))

    orig_registration_records = s7.registration_records

    def inject_v9_advance(*args, **kwargs):
        recs = orig_registration_records(*args, **kwargs)
        # Bypass lock acquisition to simulate an out-of-band v9 write landing right before sectors-v7 append.
        v9.registry(v9_reg).append_locked([{
            "kind": "inputs_frozen",
            "prereg_sha256": ctx["v9_body_sha"],
            "run_at": S7_REG_AT.isoformat(),
        }])
        return recs

    monkeypatch.setattr(s7, "registration_records", inject_v9_advance)

    with pytest.raises(PermissionError, match="differs from its witness"):
        s7.register(
            s7_reg,
            S7_REG_AT,
            "c" * 40,
            v9_log_dir=v9_reg,
            witness=witness,
            dry_run=False,
        )

    assert not (s7_reg / s7.REGISTRY_LOG).exists()
    assert not (s7_reg / s7.REGISTRY_ANCHORS).exists()
    assert not (v9_reg / v9.REGISTRY_LOCK).exists()
    assert not (s7_reg / s7.REGISTRY_LOCK).exists()


def test_sectors_v7_register_lock_order_parent_then_child_and_same_dir_safe(monkeypatch, tmp_path):
    """Locks follow strict parent (v9) -> child (sectors-v7) order through validated append, even in one dir."""
    ctx = _bind_v9_and_s7(monkeypatch, tmp_path)
    vault, v9_reg = ctx["vault"], ctx["v9_reg"]
    # Use the SAME directory for v9 and sectors-v7 to confirm distinct lock files never self-deadlock.
    shared_dir = v9_reg
    witness = v9.check_offhost(vault.cache, remote_url=str(vault.remote))

    assert s7.LOCK_ORDER == (v9.REGISTRY_ID, s7.REGISTRY_ID)

    events: list[tuple[str, str]] = []
    orig_locked = ForwardLog.locked
    orig_append_locked = ForwardLog.append_locked
    orig_verify_chain = ForwardLog.verify_chain
    s7_appended = False

    @contextlib.contextmanager
    def tracked_locked(self):
        with orig_locked(self):
            events.append(("acquire", self.lock_path.name))
            try:
                yield
            finally:
                events.append(("release", self.lock_path.name))

    def tracked_append_locked(self, records):
        nonlocal s7_appended
        if self.lock_path.name == s7.REGISTRY_LOCK:
            events.append(("append", self.lock_path.name))
        res = orig_append_locked(self, records)
        if self.lock_path.name == s7.REGISTRY_LOCK:
            s7_appended = True
        return res

    def tracked_verify_chain(self, external_anchors=None):
        res = orig_verify_chain(self, external_anchors=external_anchors)
        if self.lock_path.name == s7.REGISTRY_LOCK and s7_appended:
            events.append(("post_verify", self.lock_path.name))
        return res

    monkeypatch.setattr(ForwardLog, "locked", tracked_locked)
    monkeypatch.setattr(ForwardLog, "append_locked", tracked_append_locked)
    monkeypatch.setattr(ForwardLog, "verify_chain", tracked_verify_chain)

    out = s7.register(
        shared_dir,
        S7_REG_AT,
        "c" * 40,
        v9_log_dir=shared_dir,
        witness=witness,
        dry_run=False,
    )
    assert out["appended"] == ["header", "preregistration"]
    assert events == [
        ("acquire", v9.REGISTRY_LOCK),
        ("acquire", s7.REGISTRY_LOCK),
        ("append", s7.REGISTRY_LOCK),
        ("post_verify", s7.REGISTRY_LOCK),
        ("release", s7.REGISTRY_LOCK),
        ("release", v9.REGISTRY_LOCK),
    ]
    assert not (shared_dir / v9.REGISTRY_LOCK).exists()
    assert not (shared_dir / s7.REGISTRY_LOCK).exists()


def test_sectors_v7_register_refuses_when_post_append_validation_fails_and_releases_locks(monkeypatch, tmp_path):
    """Post-append chain validation runs under both locks and fails closed while releasing locks cleanly."""
    ctx = _bind_v9_and_s7(monkeypatch, tmp_path)
    vault, v9_reg = ctx["vault"], ctx["v9_reg"]
    s7_reg = tmp_path / "s7_reg"
    witness = v9.check_offhost(vault.cache, remote_url=str(vault.remote))

    orig_verify_chain = ForwardLog.verify_chain
    s7_verify_calls = 0
    locks_during_post_verify: dict[str, bool] = {}

    def failing_post_verify(self, external_anchors=None):
        nonlocal s7_verify_calls
        res = orig_verify_chain(self, external_anchors=external_anchors)
        if self.path == s7_reg / s7.REGISTRY_LOG:
            s7_verify_calls += 1
            # Call 1: pre-append check in register(); Call 2: internal check in append_locked();
            # Call 3: post-append validation in register().
            if s7_verify_calls == 3:
                locks_during_post_verify["v9"] = (v9_reg / v9.REGISTRY_LOCK).exists()
                locks_during_post_verify["s7"] = (s7_reg / s7.REGISTRY_LOCK).exists()
                return {**res, "ok": False, "detail": "synthetic post-append tamper"}
        return res

    monkeypatch.setattr(ForwardLog, "verify_chain", failing_post_verify)

    with pytest.raises(RuntimeError, match="validation failed after append"):
        s7.register(
            s7_reg,
            S7_REG_AT,
            "c" * 40,
            v9_log_dir=v9_reg,
            witness=witness,
            dry_run=False,
        )

    assert locks_during_post_verify == {"v9": True, "s7": True}
    assert not (v9_reg / v9.REGISTRY_LOCK).exists()
    assert not (s7_reg / s7.REGISTRY_LOCK).exists()


def test_sectors_v7_register_refuses_when_concurrent_v9_writer_wins_lock_first(tmp_path, monkeypatch):
    """If a concurrent v9 writer holds the parent lock and advances v9 first, sectors-v7 refuses."""
    ctx = _bind_v9_and_s7(monkeypatch, tmp_path)
    vault, v9_reg = ctx["vault"], ctx["v9_reg"]
    s7_reg = tmp_path / "s7_reg"
    witness = v9.check_offhost(vault.cache, remote_url=str(vault.remote))

    s7_errors: list[BaseException] = []
    v9_locked_event = threading.Event()
    s7_started_event = threading.Event()

    def s7_caller() -> None:
        try:
            v9_locked_event.wait(timeout=5.0)
            s7_started_event.set()
            s7.register(
                s7_reg,
                S7_REG_AT,
                "c" * 40,
                v9_log_dir=v9_reg,
                witness=witness,
                dry_run=False,
            )
        except BaseException as exc:
            s7_errors.append(exc)

    t = threading.Thread(target=s7_caller, daemon=True)
    t.start()
    log9 = v9.registry(v9_reg)
    try:
        with log9.locked():
            v9_locked_event.set()
            assert s7_started_event.wait(timeout=2.0)
            log9.append_locked([{
                "kind": "inputs_frozen",
                "prereg_sha256": ctx["v9_body_sha"],
                "run_at": S7_REG_AT.isoformat(),
            }])
    finally:
        v9_locked_event.set()
        t.join(timeout=5.0)

    assert not t.is_alive()
    assert len(s7_errors) == 1
    assert isinstance(s7_errors[0], PermissionError)
    assert "differs from its witness" in str(s7_errors[0])
    assert not (s7_reg / s7.REGISTRY_LOG).exists()
    assert not (v9_reg / v9.REGISTRY_LOCK).exists()
    assert not (s7_reg / s7.REGISTRY_LOCK).exists()
