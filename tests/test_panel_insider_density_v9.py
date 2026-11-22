"""Synthetic VS1 v9 core tests: prereg hash, calendar, rank IC invariance, manifest, custody, rows_sha256."""

from __future__ import annotations

import hashlib
import json
import shutil
import types
from collections import Counter
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
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
from analysis import panel_insider_density_v4 as v4
from analysis import panel_insider_density_v5 as v5
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_v8 as v8
from analysis import panel_insider_density_v9 as v9
from analysis import price_admission_probe as gd4
from analysis.research_forward_log import canonical
from scripts import run_vs1_v2_insider_density as runner
from scripts import run_vs1_v9_insider_density as v9_cli
from scripts import run_vs1_v9_manifest_from_v8_probe as v9_manifest_cli
from tests.test_panel_insider_density import _row, _submission
from tests.test_panel_insider_density_v2 import _Vault, _universe_frame
from tests.test_panel_insider_density_v8_stop import (
    DECISION_REF,
    SYNTH_CODE_SHA,
    SYNTH_CROSS_SHA,
    SYNTH_META_SHA,
    SYNTH_R9_COMMIT,
    SYNTH_R9_SHA,
    V8_REGISTERED_AT,
    V8_REGISTERED_CODE,
    _bind_superseded_pins,
    _synthetic_probe_report,
)

NOW = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)
REPO = v2.REPO
CAL_ROOT = REPO / "docs/reference/calendar-manifests"
V6_STOP_LINE = (
    b'{"head_sha256":"b9d9ab5a3eb82df3d7cd3e5be177cc058b28cb92ea86b7ab124673a43309d284",'
    b'"prev_anchor_sha256":"011693a214618385aa9b80d5f8fc528e166dfd6c478898dd34118c4b95c7bac3",'
    b'"records":3,"run_at":"2026-09-29T23:40:00+00:00"}'
)


# --- 1. Preregistration body hash, one-byte tamper, and placeholder guards ---


def test_v9_prereg_body_hash_and_one_byte_change_refuses(tmp_path):
    assert v9.check_prereg() == v9.PREREG_BODY_SHA256
    assert v9.PREREG_BODY_SHA256 != v8.PREREG_BODY_SHA256
    body = v1.prereg_body((REPO / v9.PREREG_PATH).read_text(encoding="utf-8"))
    for phrase in (
        "vs1-xnys-2007-11-01-2026-06-30.json",
        v9.XNYS_MANIFEST_SHA256,
        "3,061",
        "2007-11-02",
        "2019-12-31",
        "no XLK subtraction",
        "not available: benchmark not admitted",
        "2026-10-03T00:17:40+00:00",
        "0.9810694121554301",
        "3,011",
        "195",
        "Accept the rejection as a result.",
        "STOP_SUPERSEDED_BY_V9_UNOPENED",
        "STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED",
        "sectors-v7",
        "75489d5091d82af64312f4523c41bdb52b0951a11e017644a022baa08a172822",
        "promotion_allowed=false",
    ):
        assert phrase in body

    # One-byte change inside the body must refuse.
    fake_repo = tmp_path / "repo"
    target = fake_repo / v9.PREREG_PATH
    target.parent.mkdir(parents=True)
    raw = (REPO / v9.PREREG_PATH).read_bytes()
    idx = raw.index(b"<!-- PREREG-BODY-START -->") + len(b"<!-- PREREG-BODY-START -->") + 5
    mutated = raw[:idx] + (b"X" if raw[idx : idx + 1] != b"X" else b"Y") + raw[idx + 1 :]
    target.write_bytes(mutated)
    with pytest.raises(PermissionError, match="differs from its pin"):
        v9.check_prereg(fake_repo)


def test_v9_refuses_registration_and_manifest_while_placeholders_remain(tmp_path):
    with pytest.raises(PermissionError, match="placeholder|not bound"):
        v9.registration_records(NOW, "a" * 40)
    with pytest.raises(PermissionError, match="placeholder|not bound"):
        v9.register(tmp_path / "reg", NOW, "a" * 40, v8_log_dir=tmp_path / "v8", vault_repo=tmp_path / "vault")
    with pytest.raises(PermissionError, match="placeholder|not bound"):
        v9_manifest_cli.build_v9_manifest_from_v8_probe(tmp_path / "probe")


# --- 2. Calendar manifest pin, overlap equality, and probe session counts ---


def test_xnys_calendar_manifest_sha_overlap_and_probe_sessions(tmp_path):
    path = REPO / v9.XNYS_MANIFEST_PATH
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == v9.XNYS_MANIFEST_SHA256

    sessions = v9.load_xnys_calendar(REPO)
    doc = json.loads(raw)
    assert doc["schema"] == "grid.xnys-session-manifest.v1"
    assert doc["exchange"] == "XNYS" and doc["session_type"] == "RTH"
    assert doc["timezone"] == "America/New_York"
    assert doc["start"] == "2007-11-01" and doc["end"] == "2026-06-30"
    assert doc["calendar_version"] == (
        "exchange_calendars:4.11.2:pandas:2.3.3:numpy:2.4.6:tzdata:2026.6:XNYS:2007-11-01:2026-06-30"
    )
    assert sessions[0] == date(2007, 11, 1) and sessions[-1] == date(2026, 6, 30)

    # Probe span 2007-11-02..2019-12-31: 3,061 sessions and exact per-year counts.
    probe_sessions = [d for d in sessions if date(2007, 11, 2) <= d <= date(2019, 12, 31)]
    assert len(probe_sessions) == 3061
    assert probe_sessions[0] == date(2007, 11, 2) and probe_sessions[-1] == date(2019, 12, 31)
    by_year = Counter(d.year for d in probe_sessions)
    assert [by_year[y] for y in range(2007, 2020)] == [
        40, 253, 252, 252, 252, 250, 252, 252, 252, 252, 251, 251, 252
    ]

    # Exact DATE-SET equality with existing pinned parts on 2018-04-02..2026-06-30.
    existing_overlap: set[date] = set()
    for name in (
        "prescreen-xnys-2018-04-02-2020-12-31.json",
        "prescreen-xnys-2021-01-01-2022-11-11.json",
        "factory-xnys-2022-11-01-2024-12-31.json",
        "factory-xnys-2025-01-01-2026-11-10.json",
    ):
        part = json.loads((CAL_ROOT / name).read_bytes())
        for row in part["sessions"]:
            d = date.fromisoformat(row["key"].split(":")[1])
            if date(2018, 4, 2) <= d <= date(2026, 6, 30):
                existing_overlap.add(d)
    ours_overlap = {d for d in sessions if date(2018, 4, 2) <= d <= date(2026, 6, 30)}
    assert ours_overlap == existing_overlap and len(ours_overlap) > 2000

    # Tampered calendar manifest refuses.
    fake_repo = tmp_path / "repo"
    bad_cal = fake_repo / v9.XNYS_MANIFEST_PATH
    bad_cal.parent.mkdir(parents=True)
    bad_cal.write_bytes(raw + b" ")
    with pytest.raises((PermissionError, ValueError), match="hash|sha256|mismatch"):
        v9.load_xnys_calendar(fake_repo)


# --- 3. Property test: rank IC invariance and "not available" magnitudes ---


@pytest.mark.parametrize("seed", [1, 7, 42])
@pytest.mark.parametrize("horizon", [5, 20])
def test_v9_rank_ic_equals_v8_xlk_relative_per_date_and_magnitudes_never_gate(seed, horizon):
    rng = np.random.default_rng(seed)
    days = pd.bdate_range("2015-01-02", periods=220)
    tickers = [f"T{i:02d}" for i in range(30)]
    returns = rng.normal(0.0005, 0.02, size=(len(days), len(tickers) + 1))
    closes = pd.DataFrame(100.0 * np.exp(np.cumsum(returns, axis=0)), index=days, columns=tickers + ["XLK"])
    # Introduce realistic missing issuer closes (XLK stays finite).
    miss = rng.random((len(days), len(tickers))) < 0.03
    closes.iloc[:, : len(tickers)] = np.where(miss, np.nan, closes.iloc[:, : len(tickers)].to_numpy())

    with v1.discovery_window(v9.DISCOVERY_START):
        pos_v8, lbl_v8, mom_v8 = v1.relative_labels(closes, "XLK", tickers, horizon, "discovery")
        pos_v9, lbl_v9, mom_v9 = v9.raw_labels(closes, tickers, horizon, "discovery")

    assert pos_v9 == pos_v8
    feature = rng.poisson(0.4, size=lbl_v9.shape).astype(float)
    feature[rng.random(feature.shape) < 0.05] = np.nan
    largest = np.where(feature > 0, rng.uniform(50_000, 900_000, size=feature.shape), 0.0)

    ic_v8, cnt_v8 = v1.rank_ic_series(feature, lbl_v8)
    ic_v9, cnt_v9 = v1.rank_ic_series(feature, lbl_v9)
    assert np.array_equal(cnt_v9, cnt_v8)
    assert np.allclose(ic_v9, ic_v8, rtol=0.0, atol=1e-12, equal_nan=True)

    # Magnitudes read "not available: benchmark not admitted" when benchmark_admitted is False and never gate.
    decided = [d.isoformat() for d in v1.decision_instants([days[i].date() for i in pos_v9])]
    ends = [d.isoformat() for d in v1.decision_instants([days[i + horizon].date() for i in pos_v9])]
    groups = ["7370-7379"] * len(tickers)
    panel_v8 = v2.TrialPanel(
        trial=f"A90|fwd{horizon}", window="discovery", horizon=horizon, decision_at=decided, label_end=ends,
        entities=tickers, feature=feature, label=lbl_v8, momentum=mom_v8, largest=largest, groups=groups,
    )
    panel_v9_unadmitted = v9.TrialPanel(
        trial=f"A90|fwd{horizon}", window="discovery", horizon=horizon, decision_at=decided, label_end=ends,
        entities=tickers, feature=feature, label=lbl_v9, momentum=mom_v9, largest=largest, groups=groups,
        benchmark_admitted=False, benchmark_return=None,
    )
    if len(pos_v9) >= v1.MIN_N:
        m8 = v2.measure_trial(panel_v8, perms=99, sensitivity=True)
        m9 = v9.measure_trial(panel_v9_unadmitted, perms=99, sensitivity=True)
        assert m9["status"] == m8["status"] == "tested"
        assert m9["mean_ic"] == pytest.approx(m8["mean_ic"], abs=1e-12)
        assert m9["p"] == m8["p"]
        assert m9["p_one_sided_positive"] == m8["p_one_sided_positive"]
        assert m9["p_one_sided_negative"] == m8["p_one_sided_negative"]
        mag = m9["magnitude"]
        assert mag["benchmark_admitted"] is False
        for key in (
            "buyer_minus_benchmark",
            "nonbuyer_minus_benchmark",
            "large_line_buyer_minus_benchmark",
            "small_line_buyer_minus_benchmark",
        ):
            assert mag[key] == v9.BENCHMARK_MAGNITUDE_UNAVAILABLE
            assert "not available" in mag[key]
        assert mag["buyer_minus_nonbuyer"] == pytest.approx(m8["magnitude"]["buyer_minus_nonbuyer"], abs=1e-12)

        # When XLK IS admitted for a window, XLK-relative magnitudes match v8.
        xlk = closes["XLK"].to_numpy(dtype=float)
        bench_ret = np.array([xlk[i + horizon] / xlk[i] - 1.0 for i in pos_v9], dtype=float)
        panel_v9_admitted = v9.TrialPanel(
            trial=f"A90|fwd{horizon}", window="discovery", horizon=horizon, decision_at=decided, label_end=ends,
            entities=tickers, feature=feature, label=lbl_v9, momentum=mom_v9, largest=largest, groups=groups,
            benchmark_admitted=True, benchmark_return=bench_ret,
        )
        m9_adm = v9.measure_trial(panel_v9_admitted, perms=99, sensitivity=True)
        assert m9_adm["magnitude"]["benchmark_admitted"] is True
        assert m9_adm["magnitude"]["buyer_minus_benchmark"] == pytest.approx(
            m8["magnitude"]["buyer_minus_benchmark"], abs=1e-12
        )

        # End-to-end discover_panel: v9 selection/calibration match v8 while magnitudes stay "not available".
        if seed == 1 and horizon == 5:
            spec8 = v8.run_spec("prop-test")
            spec9 = v9.run_spec("prop-test")
            panels8 = {t: v2.TrialPanel(**{**panel_v8.__dict__, "trial": t}) for t in spec8.trials}
            panels9 = {t: v9.TrialPanel(**{**panel_v9_unadmitted.__dict__, "trial": t}) for t in spec9.trials}
            with v1.discovery_window(v9.DISCOVERY_START):
                f8 = v8.discover_panel(spec8, panels8, inputs={"synthetic": True}, sensitivity=True)
                f9 = v9.discover_panel(spec9, panels9, inputs={"synthetic": True}, sensitivity=True)
            assert [r["selected"] for r in f9["payload"]["ledger"]] == [r["selected"] for r in f8["payload"]["ledger"]]
            assert f9["payload"]["calibration"]["state"] == f8["payload"]["calibration"]["state"]
            for row in f9["payload"]["ledger"]:
                assert row["magnitude"]["benchmark_admitted"] is False
                assert row["magnitude"]["buyer_minus_benchmark"] == v9.BENCHMARK_MAGNITUDE_UNAVAILABLE


# --- 4. Synthetic 195-issuer probe dir fixture, deterministic manifest builder, pinned triple ---


def _build_synthetic_probe_dir(probe_dir: Path, *, rows_by_ticker: dict[str, str] | None = None, n_issuers: int = 195):
    probe_dir.mkdir(parents=True, exist_ok=True)
    admitted = [f"T{i:03d}" for i in range(n_issuers)]
    common = {
        "prereg": {"study": "vs1-v8", "body_sha256": v8.PREREG_BODY_SHA256, "rules": "v6 §2.2 rules 3-4, §2.3"},
        "code_sha": SYNTH_CODE_SHA,
        "snapshot_as_of_ts": "2026-10-03T00:17:40+00:00",
        "window": "discovery",
        "read_window": {"start": "2007-11-02", "end": "2019-12-31"},
        "source": {"name": "TIINGO", "id": 524},
        "promotion_allowed": False,
    }
    cross_doc = {
        "report": "vs1-v8-twelvedata-crosscheck",
        **common,
        "rule": {"discovery_window": ["2007-11-02", "2019-12-31"]},
        "tickers": {t: {"passed": True, "reason": "pass"} for t in admitted}
        | {"XLK": {"passed": False, "reason": "disagreement", "pairs": 3011, "share_within": 0.9810694121554301}},
    }
    meta_doc = {
        "report": "vs1-v8-tiingo-metadata",
        **common,
        "tickers": {t: {"startDate": "2007-11-02", "listed_from": "2007-11-02", "entity_check": "match"} for t in admitted},
    }
    cross_bytes = (json.dumps(cross_doc, indent=2, sort_keys=True) + "\n").encode("utf-8")
    meta_bytes = (json.dumps(meta_doc, indent=2, sort_keys=True) + "\n").encode("utf-8")
    cross_sha = hashlib.sha256(cross_bytes).hexdigest()
    meta_sha = hashlib.sha256(meta_bytes).hexdigest()
    (probe_dir / "crosscheck_report.json").write_bytes(cross_bytes)
    (probe_dir / "tiingo_meta_report.json").write_bytes(meta_bytes)

    tickers_rec = {
        "XLK": {
            "ticker": "XLK",
            "benchmark": True,
            "admitted": False,
            "listed_from": None,
            "reasons": ["crosscheck_disagreement"],
            "crosscheck": {"passed": False, "reason": "disagreement", "pairs": 3011, "share_within": 0.9810694121554301},
        }
    }
    for i, t in enumerate(admitted):
        rsha = (rows_by_ticker or {}).get(t) or hashlib.sha256(f"rows-{t}".encode()).hexdigest()
        tickers_rec[t] = {
            "ticker": t,
            "benchmark": False,
            "admitted": True,
            "listed_from": "2007-11-02",
            "reasons": [],
            "rows_examined": {
                "close": 3061,
                "adj_close": 3061,
                "close_sha256": rsha,
                "adj_close_sha256": rsha,
                "selected_close_sha256": rsha,
                "selected_adj_close_sha256": rsha,
            },
        }
    tickers_rec["EXCL0"] = {"ticker": "EXCL0", "benchmark": False, "admitted": False, "listed_from": None, "reasons": ["no_meta"]}

    probe_doc = {
        "probe": "vs1-gd4-price-admission-probe",
        "probe_version": 2,
        **common,
        "series_template": "YF:{ticker}:adj_close",
        "basis": "split+dividend adjusted",
        "benchmark": "XLK",
        "benchmark_admitted": False,
        "benchmark_calendar_sessions": 3061,
        "benchmark_calendar": ["2007-11-02", "2019-12-31"],
        "tolerances": {"crosscheck": {"discovery_window": ["2007-11-02", "2019-12-31"]}},
        "crosscheck_report_sha256": cross_sha,
        "tiingo_meta_report_sha256": meta_sha,
        "summary": {"candidates": n_issuers + 2, "admitted": n_issuers, "admitted_excluding_benchmark": n_issuers, "promotion_allowed": False},
        "admitted": sorted(admitted),
        "listed_from": {t: "2007-11-02" for t in admitted},
        "tickers": tickers_rec,
    }
    probe_bytes = (json.dumps(probe_doc, indent=2, sort_keys=True) + "\n").encode("utf-8")
    probe_sha = hashlib.sha256(probe_bytes).hexdigest()
    (probe_dir / "probe_report.json").write_bytes(probe_bytes)
    hashes = {
        "crosscheck_report.json": cross_sha,
        "probe_report.json": probe_sha,
        "tiingo_meta_report.json": meta_sha,
    }
    (probe_dir / "sha256s.txt").write_text(
        "".join(f"{h}  {name}\n" for name, h in sorted(hashes.items())), encoding="utf-8"
    )
    return probe_sha, cross_sha, meta_sha


def _pin_probe_triple(monkeypatch, probe_sha: str, cross_sha: str, meta_sha: str) -> None:
    for mod in (v8, v9):
        monkeypatch.setattr(mod, "PROBE_REPORT_SHA256", probe_sha)
        monkeypatch.setattr(mod, "CROSSCHECK_REPORT_SHA256", cross_sha)
        monkeypatch.setattr(mod, "TIINGO_META_REPORT_SHA256", meta_sha)
        monkeypatch.setattr(mod, "PROBE_CODE_SHA", SYNTH_CODE_SHA)
        monkeypatch.setattr(mod, "OWNER_RULING_R9_VAULT_COMMIT", SYNTH_R9_COMMIT)
        monkeypatch.setattr(mod, "OWNER_RULING_R9_FILE_SHA256", SYNTH_R9_SHA)


def test_manifest_builder_accepts_only_pinned_triple_and_is_deterministic_195_issuers(tmp_path, monkeypatch):
    probe_dir = tmp_path / "probe_20261003_001948"
    probe_sha, cross_sha, meta_sha = _build_synthetic_probe_dir(probe_dir, n_issuers=195)
    _pin_probe_triple(monkeypatch, probe_sha, cross_sha, meta_sha)

    out1 = tmp_path / "m1.json"
    out2 = tmp_path / "m2.json"
    v9_manifest_cli.main(["--probe-dir", str(probe_dir), "--out", str(out1)])
    v9_manifest_cli.main(["--probe-dir", str(probe_dir), "--out", str(out2)])
    assert out1.read_bytes() == out2.read_bytes()

    manifest = v9.PriceManifest.from_file(out1)
    assert len(manifest.admitted) == 195
    assert "XLK" not in manifest.admitted and manifest.benchmark == "XLK" and manifest.benchmark_admitted is False
    assert manifest.probe_report_sha256 == probe_sha
    assert manifest.crosscheck_report_sha256 == cross_sha
    assert manifest.tiingo_meta_report_sha256 == meta_sha
    assert manifest.calendar_manifest_sha256 == v9.XNYS_MANIFEST_SHA256
    assert manifest.probe_snapshot_as_of_ts == "2026-10-03T00:17:40+00:00"
    assert len(manifest.listed_from) == 195 and len(manifest.selected_adj_close_sha256) == 195

    # Refuses write-once overwrite.
    with pytest.raises((FileExistsError, SystemExit)):
        v9_manifest_cli.main(["--probe-dir", str(probe_dir), "--out", str(out1)])

    # Changing 1 byte in ANY of the three reports refuses.
    for victim in ("probe_report.json", "crosscheck_report.json", "tiingo_meta_report.json"):
        bad_dir = tmp_path / f"bad_{victim}"
        shutil.copytree(probe_dir, bad_dir)
        (bad_dir / victim).write_bytes((bad_dir / victim).read_bytes() + b" ")
        with pytest.raises(PermissionError, match="pinned"):
            v9_manifest_cli.build_v9_manifest_from_v8_probe(bad_dir)


# --- 5. Stage-0 / selection / holdout settings equal v8's & V9_CODE_FILES ---


def test_v9_stage0_selection_holdout_settings_equal_v8_and_code_files_pinned(tmp_path, monkeypatch):
    assert v9.DISCOVERY_START == v8.DISCOVERY_START == "2008-01-01T00:00:00+00:00"
    assert v9.PROBE_START == v8.PROBE_START == "2007-11-02"
    assert v9.PRIMARY_TRIAL == v8.PRIMARY_TRIAL == "A90|fwd5"
    assert v9.CONFIRMATORY_ALPHA_ONE_SIDED == v8.CONFIRMATORY_ALPHA_ONE_SIDED == 0.05
    assert v9.GAUSSIAN_SIMS == v8.GAUSSIAN_SIMS == 200
    assert v9.E0_SIMS == v8.E0_SIMS == 500
    assert v9.E0_GATED_SCENARIO == v8.E0_GATED_SCENARIO == "factor_t_garch_exposed"
    assert v9.E0_REPORTED_SCENARIOS == v8.E0_REPORTED_SCENARIOS == ("factor_t_garch",)
    assert v9.E0_MANIFEST_SHA256 == v8.E0_MANIFEST_SHA256
    assert v9.DESIGN_TARGET_POWER == v8.DESIGN_TARGET_POWER == 0.80
    assert v9.STAGE0_SETTINGS == v8.STAGE0_SETTINGS
    assert v9.V9.pins.holdout_probe_required is True

    # Stage-0 verify_power keeps v1.proxy_sessions for n_sessions (NOT XNYS calendar count).
    proxy_n = len(v1.proxy_sessions(date(2008, 1, 1), date(2020, 1, 1)))
    xnys_n = len([d for d in v9.load_xnys_calendar(REPO) if date(2008, 1, 1) <= d < date(2020, 1, 1)])
    assert proxy_n != xnys_n
    table = {t: [{"target_ic": ic, "power": 0.5, "sims": v1.POWER_SIMS} for ic in v1.POWER_TARGET_ICS]
             for t in v1.trial_names()}
    e0row = {"sims": v9.E0_SIMS, "perms": v1.POWER_PERMS, "alpha_one_sided": 0.05, "target_ic": 0.01,
             "e0_manifest_sha256": v9.E0_MANIFEST_SHA256, "e0_seeds": {"base": 20260930, "pilot": 20260931}}
    models = {
        "gaussian_v1": {"model": "gaussian_v1", "power": 0.7, "sims": v9.GAUSSIAN_SIMS,
                        "perms": v1.POWER_PERMS, "seed": v1.SEED, "alpha_one_sided": 0.05, "target_ic": 0.01},
        "e0:factor_t_garch_exposed": {**e0row, "model": "e0:factor_t_garch_exposed", "power": 0.6},
        "e0:factor_t_garch": {**e0row, "model": "e0:factor_t_garch", "power": 0.65},
    }
    power = {
        "version": v9.VERSION, "primary_trial": v9.PRIMARY_TRIAL, "settings": v1.power_settings(),
        "table": table, "gate_passed": True,
        "v9_stage0": {"settings": json.loads(json.dumps(v9.STAGE0_SETTINGS)), "models": models,
                      "discovery_start": v9.DISCOVERY_START, "n_sessions": proxy_n},
    }
    v9.verify_power(power)
    moved = json.loads(json.dumps(power))
    moved["v9_stage0"]["n_sessions"] = xnys_n
    with pytest.raises(ValueError, match="discovery window"):
        v9.verify_power(moved)

    # V9_CODE_FILES covers v9 harness, CLI, manifest builder, calendar manifest, and v8/E0 code.
    files = runner._code_files(v9)
    assert set(files) == set(runner.V9_CODE_FILES)
    assert {
        "analysis/panel_insider_density_v9.py",
        "scripts/run_vs1_v9_insider_density.py",
        "scripts/run_vs1_v9_manifest_from_v8_probe.py",
        "docs/reference/calendar-manifests/vs1-xnys-2007-11-01-2026-06-30.json",
        "evals/e0/MANIFEST.sha256",
    } <= set(files)


# --- 6. v9 census, verify_v8_terminal, sectors-v7 late appearance & growth ---


def _bind_v9_all(monkeypatch, tmp_path: Path):
    probe_dir = tmp_path / "probe_dir"
    probe_sha, cross_sha, meta_sha = _build_synthetic_probe_dir(probe_dir, n_issuers=195)
    _pin_probe_triple(monkeypatch, probe_sha, cross_sha, meta_sha)

    # Create a real local v8 chain stopped with STOP_SUPERSEDED_BY_V9_UNOPENED.
    v8_reg = tmp_path / "v8_reg"
    log8 = v8.registry(v8_reg)
    log8.append(v8.registration_records(V8_REGISTERED_AT, V8_REGISTERED_CODE))
    nl = b"\n"
    vault = _Vault(tmp_path / "vault", h=v8, seeds=("vs1-v1", "vs1-v2"))
    for m in (v3, v4, v5, s2, s3, s4):
        vault.add_file(m.WITNESS_PATH, m.REGISTERED_ANCHOR_LINE + nl)
    vault.add_file(v6.WITNESS_PATH, v6.REGISTERED_ANCHOR_LINE + nl + V6_STOP_LINE + nl)
    vault.add_file(v7.WITNESS_PATH, v7.REGISTERED_ANCHOR_LINE + nl + v8.V7_STOP_ANCHOR_LINE + nl)
    vault.publish(v8_reg)
    with monkeypatch.context() as m:
        m.setattr(v8, "check_census", lambda *a, **k: None)
        prior8 = vault.witness()
        stop8 = v8.append_superseded_stop(
            v8_reg,
            NOW,
            probe_report_path=probe_dir / "probe_report.json",
            decision_ref=DECISION_REF,
            expected_prev_sha256=v8.REGISTERED_RECORD_SHA256[1],
            witness=prior8,
        )
    vault.publish(v8_reg)
    v8_stop_head = v1._record_sha256(stop8)
    v8_stop_line = (v8_reg / v8.REGISTRY_ANCHORS).read_bytes().splitlines()[1]

    # Add synthetic sectors-v6 3-record STOP anchor to vault.
    s6_stop_head = "9" * 64
    s6_stop_line = canonical({
        "head_sha256": s6_stop_head,
        "prev_anchor_sha256": hashlib.sha256(s6.REGISTERED_ANCHOR_LINE).hexdigest(),
        "records": 3,
        "run_at": NOW.isoformat(),
    })
    vault.add_file(s6.WITNESS_PATH, s6.REGISTERED_ANCHOR_LINE + nl + s6_stop_line + nl)

    # Fill a temp copy of the v9 preregistration body with bound synthetic hashes.
    text = (REPO / v9.PREREG_PATH).read_text(encoding="utf-8")
    replacements = {
        "@@PROBE_REPORT_SHA256@@": probe_sha,
        "@@CROSSCHECK_REPORT_SHA256@@": cross_sha,
        "@@TIINGO_META_REPORT_SHA256@@": meta_sha,
        "@@PROBE_CODE_SHA@@": SYNTH_CODE_SHA,
        "@@OWNER_RULING_R9_VAULT_COMMIT@@": SYNTH_R9_COMMIT,
        "@@OWNER_RULING_R9_FILE_SHA256@@": SYNTH_R9_SHA,
        "@@V8_STOP_HEAD_SHA256@@": v8_stop_head,
        "@@SECTORS_V6_STOP_HEAD_SHA256@@": s6_stop_head,
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    filled = tmp_path / "vs1-v9-prereg.md"
    filled.write_text(text, encoding="utf-8")
    body_sha = v1.prereg_body_sha256(filled)

    monkeypatch.setattr(v9, "PREREG_PATH", filled)
    monkeypatch.setattr(v9, "PREREG_BODY_SHA256", body_sha)
    monkeypatch.setattr(v9, "V8_STOP_HEAD_SHA256", v8_stop_head)
    monkeypatch.setattr(v9, "V8_STOP_ANCHOR_LINE", v8_stop_line)
    monkeypatch.setattr(v9, "SECTORS_V6_STOP_HEAD_SHA256", s6_stop_head)
    monkeypatch.setattr(v9, "SECTORS_V6_STOP_ANCHOR_LINE", s6_stop_line)
    return vault, v8_reg, v8_stop_head, s6_stop_head, probe_dir, body_sha


def test_v9_verify_v8_terminal_and_census_mirror_v8(tmp_path, monkeypatch):
    vault, v8_reg, v8_head, s6_head, _, _ = _bind_v9_all(monkeypatch, tmp_path)

    # v8.check_offhost refuses once sectors-v6 is at 3 records, but v9.verify_v8_terminal succeeds!
    with pytest.raises(PermissionError):
        v8.check_offhost(vault.cache, remote_url=str(vault.remote))
    proof = v9.verify_v8_terminal(v8_reg, vault.cache, remote_url=str(vault.remote))
    assert proof["records"] == 3 and proof["head_sha256"] == v8_head
    assert proof["status"] == v8.STOP_SUPERSEDED_STATUS

    # Registration census passes at baseline, refuses any growth/unknown/sectors-v7 before registration.
    census = proof["census"]
    v9.check_census(
        census,
        v8_stop_head_sha256=v8_head,
        sectors_v6_stop_head_sha256=s6_head,
        witness_repo=vault.cache,
    )
    assert not v9.contamination([census])["contaminated"]

    for key, bad_count in (("vs1-v5", 3), ("sectors-v4", 3), ("vs1-v6", 4), ("vs1-v7", 2), ("vs1-v8", 2), ("sectors-v6", 2)):
        bad = {**census, "records": {**census["records"], key: bad_count}}
        with pytest.raises(PermissionError):
            v9.check_census(bad, v8_stop_head_sha256=v8_head, sectors_v6_stop_head_sha256=s6_head, witness_repo=vault.cache)
        assert v9.contamination([bad])["contaminated"]

    unk = {**census, "unknown": ["05-GRID/Paper-Log/vs1/rogue.anchors.jsonl"]}
    with pytest.raises(PermissionError, match="unknown"):
        v9.check_census(unk, v8_stop_head_sha256=v8_head, sectors_v6_stop_head_sha256=s6_head, witness_repo=vault.cache)
    assert v9.contamination([unk])["contaminated"]

    # Register v9 cleanly, then exercise opening census with optional 2-record sectors-v7.
    v9_reg = tmp_path / "v9_reg"
    records = v9.register(v9_reg, NOW, "a" * 40, v8_log_dir=v8_reg, vault_repo=vault.cache, remote_url=str(vault.remote))
    assert [r["kind"] for r in records] == ["header", "preregistration"]
    heads = tuple(v1._line_sha256(v9.registry(v9_reg)))
    anchor = (v9_reg / v9.REGISTRY_ANCHORS).read_bytes().splitlines()[0]
    monkeypatch.setattr(v9, "REGISTERED_RECORD_SHA256", heads)
    monkeypatch.setattr(v9, "REGISTERED_ANCHOR_LINE", anchor)
    vault.add_file(v9.WITNESS_PATH, anchor + b"\n")
    w = v9.check_offhost(vault.cache, remote_url=str(vault.remote))
    assert w.census["records"]["vs1-v9"] == 2

    # sectors-v7 at 2 records is accepted on openings, refused on growth past 2 or before v9 registration.
    s7_line = canonical({"head_sha256": "c" * 64, "prev_anchor_sha256": None, "records": 2, "run_at": NOW.isoformat()})
    vault.add_file(v1.canonical_witness_path("sectors-v7"), s7_line + b"\n")
    w_s7 = v9.check_offhost(vault.cache, remote_url=str(vault.remote))
    assert w_s7.census["records"]["sectors-v7"] == 2
    assert not v9.contamination([w_s7.census])["contaminated"]

    # sectors-v7 present at registration (opening=False) refuses.
    pre_reg_with_s7 = {
        **w_s7.census,
        "files": {k: p for k, p in w_s7.census["files"].items() if k != "vs1-v9"},
        "records": {k: c for k, c in w_s7.census["records"].items() if k != "vs1-v9"},
    }
    with pytest.raises(PermissionError, match="missing or extra"):
        v9.check_census(
            pre_reg_with_s7,
            v8_stop_head_sha256=v8_head,
            sectors_v6_stop_head_sha256=s6_head,
            witness_repo=vault.cache,
            opening=False,
        )

    for grown in (1, 3, 4, None):
        bad_s7 = {**w_s7.census, "records": {**w_s7.census["records"], "sectors-v7": grown}}
        with pytest.raises(PermissionError, match="sectors-v7"):
            v9.check_census(
                bad_s7,
                v8_stop_head_sha256=v8_head,
                sectors_v6_stop_head_sha256=s6_head,
                witness_repo=vault.cache,
                opening=True,
            )
        assert v9.contamination([bad_s7])["contaminated"]

    # sectors-v7 appearing after discovery_opened is contamination and refuses holdout.
    without = {**w.census}
    with_s7 = {**w_s7.census}
    late = v9.contamination([without, with_s7])
    assert late["contaminated"] and "sectors-v7 (appeared after a v9 opening)" in str(late["detail"])
    opened = {"kind": "discovery_opened", "supersession": {"vs1_witness_census": without}}
    monkeypatch.setattr(v9.V9, "_chain", lambda log: [{"kind": "header"}, {"kind": "preregistration"}, opened])
    with pytest.raises(PermissionError, match="first appeared after v9 discovery_opened"):
        v9.open_holdout(
            {},
            allow_holdout=True,
            prereg_sha256=v9.PREREG_BODY_SHA256,
            log_dir=v9_reg,
            now=NOW,
            observed={},
            witness=types.SimpleNamespace(census=with_s7),
        )


# --- 7. Freeze as-of == probe snapshot & discovery read reproduces per-series rows_sha256 or refuses ---


def _sqlite_price_db(series: dict[str, pd.Series], pull_ts: datetime):
    from sqlalchemy import Column, Date, DateTime, Float, Integer, MetaData, String, Table, Text, create_engine

    engine = create_engine("sqlite://")
    md = MetaData()
    catalog = Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String))
    raw = Table(
        "raw_series", md,
        Column("series_id", String), Column("source_id", Integer),
        Column("obs_date", Date), Column("pull_timestamp", DateTime),
        Column("value", Float), Column("raw_payload", Text), Column("pull_status", String),
    )
    md.create_all(engine)
    rows = []
    for ticker, values in series.items():
        for d, v in values.items():
            rows.append({
                "series_id": f"YF:{ticker}:adj_close",
                "source_id": 524,
                "obs_date": d.date() if hasattr(d, "date") else d,
                "pull_timestamp": pull_ts,
                "value": float(v),
                "raw_payload": "{}",
                "pull_status": "SUCCESS",
            })
    with engine.begin() as c:
        c.execute(catalog.insert(), [{"id": 524, "name": "TIINGO"}])
        c.execute(raw.insert(), rows)
    return engine


def test_v9_freeze_as_of_equals_probe_snapshot_and_discovery_reproduces_rows_sha256(tmp_path, monkeypatch):
    vault, v8_reg, _, _, _, body_sha = _bind_v9_all(monkeypatch, tmp_path)
    v9_reg = tmp_path / "v9_reg"
    v9.register(v9_reg, NOW, "a" * 40, v8_log_dir=v8_reg, vault_repo=vault.cache, remote_url=str(vault.remote))
    heads = tuple(v1._line_sha256(v9.registry(v9_reg)))
    anchor = (v9_reg / v9.REGISTRY_ANCHORS).read_bytes().splitlines()[0]
    monkeypatch.setattr(v9, "REGISTERED_RECORD_SHA256", heads)
    monkeypatch.setattr(v9, "REGISTERED_ANCHOR_LINE", anchor)
    vault.add_file(v9.WITNESS_PATH, anchor + b"\n")

    # Build a small SQLite price fixture on the exact probe window 2007-11-02..2019-12-31 for 2 tickers.
    probe_sessions = [d for d in v9.load_xnys_calendar(REPO) if date(2007, 11, 2) <= d <= date(2019, 12, 31)]
    sample_dates = probe_sessions[:5] + probe_sessions[-5:]
    pull_ts = datetime(2026, 10, 2, 12, 0)  # naive in SQLite, before 2026-10-03T00:17:40+00:00
    series = {
        "T000": pd.Series([100.0 + i for i in range(len(sample_dates))], index=sample_dates),
        "T001": pd.Series([50.0 + i for i in range(len(sample_dates))], index=sample_dates),
    }
    engine = _sqlite_price_db(series, pull_ts)
    with engine.connect() as conn:
        expected_rows_sha = {
            t: gd4.rows_sha256(
                gd4.Row(o.obs_date, float(o.value), o.pull_timestamp)
                for o in v1.observations.read_window(
                    conn, f"YF:{t}:adj_close", source="TIINGO",
                    start=date(2007, 11, 2), as_of=date(2019, 12, 31),
                    as_of_ts=datetime(2026, 10, 3, 0, 17, 40, tzinfo=timezone.utc),
                )
            )
            for t in ("T000", "T001")
        }

    manifest = v9.PriceManifest(
        source="TIINGO",
        series_template="YF:{ticker}:adj_close",
        basis="split+dividend adjusted",
        benchmark="XLK",
        benchmark_admitted=False,
        admitted=("T000", "T001"),
        probe_report_sha256=v9.PROBE_REPORT_SHA256,
        crosscheck_report_sha256=v9.CROSSCHECK_REPORT_SHA256,
        tiingo_meta_report_sha256=v9.TIINGO_META_REPORT_SHA256,
        listed_from=(("T000", "2007-11-02"), ("T001", "2007-11-02")),
        calendar_manifest=v9.XNYS_MANIFEST_PATH.as_posix(),
        calendar_manifest_sha256=v9.XNYS_MANIFEST_SHA256,
        probe_snapshot_as_of_ts="2026-10-03T00:17:40+00:00",
        selected_adj_close_sha256=tuple(sorted(expected_rows_sha.items())),
    )
    manifest.validate()

    frozen_inputs = {
        "sector": v1.VS1_SECTOR,
        "price_manifest_sha256": manifest.digest(),
        "probe_report_sha256": manifest.probe_report_sha256,
        "form4_sha256": "1" * 64,
        "submissions_sha256": "2" * 64,
        "issuer_map_sha256": "3" * 64,
        "sic_map_sha256": "4" * 64,
        "power_sha256": "5" * 64,
        "accept_underpowered": False,
        "as_of_ts": "2026-10-03T00:17:40+00:00",
    }

    # Wrong as_of_ts refuses freeze_inputs.
    with pytest.raises(PermissionError, match="probe snapshot"):
        v9.freeze_inputs(v9_reg, NOW, {**frozen_inputs, "as_of_ts": "2026-10-04T00:00:00+00:00"})

    # Exact probe snapshot succeeds.
    v9.freeze_inputs(v9_reg, NOW, frozen_inputs)
    observed = {k: v for k, v in frozen_inputs.items() if k not in ("as_of_ts", "accept_underpowered")}
    w = v9.check_offhost(vault.cache, remote_url=str(vault.remote))
    v9.open_discovery(v9_reg, NOW, observed, w)
    vault.add_file(v9.WITNESS_PATH, (v9_reg / v9.REGISTRY_ANCHORS).read_bytes())
    w2 = v9.check_offhost(vault.cache, remote_url=str(vault.remote))
    key = v9.resume_discovery(v9_reg, observed, w2)

    # Matching DB rows succeed (XLK is never present in SQLite and never read!).
    with engine.connect() as conn:
        panel = v9.load_price_panel(
            conn, manifest, ["T000", "T001"],
            start=date(2007, 11, 2), as_of=date(2019, 12, 31),
            window="discovery", key=key,
        )
    closes = panel.closes()
    assert len(closes.index) == 3061
    assert "XLK" not in closes.columns and list(closes.columns) == ["T000", "T001"]

    # Mutating a single DB row breaks rows_sha256 reproduction and refuses!
    bad_series = {**series, "T000": series["T000"] + 0.01}
    bad_engine = _sqlite_price_db(bad_series, pull_ts)
    with bad_engine.connect() as conn:
        with pytest.raises(PermissionError, match="selected_adj_close_sha256|rows_sha256"):
            v9.load_price_panel(
                conn, manifest, ["T000", "T001"],
                start=date(2007, 11, 2), as_of=date(2019, 12, 31),
                window="discovery", key=key,
            )
