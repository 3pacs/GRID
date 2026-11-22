"""Sectors-v7 custody scaffold: the ten-sector generalization test bound to VS1 v9.

Registration and benchmark-rule scaffold only. There is no joint-run, probe, or price-opening
harness here, and :func:`check_open` refuses until v9 is terminal (and continues to fail closed
until a separately reviewed sectors-v7 joint-run harness exists).

Benchmark rule (mirrors VS1 v9):
* The sector ETF does not gate admission or testability.
* Sessions come from the same pinned XNYS session manifest as v9 (:func:`load_xnys_calendar`).
* Labels are raw split-first issuer forward returns (no sector-ETF subtraction; rank IC invariant).
* Sector-ETF-relative magnitudes are reported only where the sector ETF passes the unchanged
  admission rule for that window; otherwise they read ``"not available: benchmark not admitted"``.
* "Benchmark admission" is removed from the causes of an untestable sector; the XLRE/XLC
  pre-inception rule applies only to reported magnitudes when the ETF is admitted.

Order (v9 body section 0, this body section 5): sectors-v7 must be registered and witnessed
while the v9 witness covers exactly v9's two registration records, after v9's registration
witness and before any later v9 record (``inputs_frozen``, a STOP, or ``discovery_opened``).
"""

from __future__ import annotations

import json
import math
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v4 as s4
from analysis import panel_insider_density_sectors_v6 as s6
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_v8 as v8
from analysis import panel_insider_density_v9 as v9
from analysis.research_forward_log import ForwardLog

VERSION = "vs1-sectors-v7"
REGISTRY_ID = v9.SECTORS_V7  # "sectors-v7"
PREREG_PATH = Path("docs/paper_log/vs1-sectors-v7-preregistration.md")
#: SHA-256 of the UTF-8 LF body strictly between <!-- PREREG-BODY-START --> and <!-- PREREG-BODY-END -->.
#: Recompute and re-pin after filling operator placeholder tokens once v8 STOP, sectors-v6 STOP and v9 registration exist.
PREREG_BODY_SHA256: str | None = "5eeaa010f7fcb97a18c076bf89a517bb9127fb6ea381e775ab2aed790c2b00b0"
#: The exact witnessed two-record v9 registration head; placeholder (None) until v9 is registered and witnessed.
V9_REGISTRATION_HEAD_SHA256: str | None = None
#: sha256 of GD10b GateSpec v1 (analysis.generalization_gate.GATE_SPEC_V1_SHA256).
#: Checked at run time against the merged module in :func:`_check_gate_spec`.
GATE_SPEC_SHA256: str | None = "fa2fa2bd1b1ef5cb81168f393135795824991e08829b60f66a109decdf401174"

#: Pinned XNYS session calendar manifest (reuses v9's exact pin and loader).
XNYS_MANIFEST_PATH = v9.XNYS_MANIFEST_PATH
XNYS_MANIFEST_SHA256 = v9.XNYS_MANIFEST_SHA256
load_xnys_calendar = v9.load_xnys_calendar

#: Reported-only benchmark magnitude sentinel and label/measurement helpers (reuses v9).
BENCHMARK_MAGNITUDE_UNAVAILABLE = v9.BENCHMARK_MAGNITUDE_UNAVAILABLE
raw_labels = v9.raw_labels
TrialPanel = v9.TrialPanel
buyer_excess = v9.buyer_excess
measure_trial = v9.measure_trial

REGISTRY_LOG = "granular_panel_prereg_sectors_v7.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_sectors_v7.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_sectors_v7.lock"
WITNESS_PATH = v1.canonical_witness_path(REGISTRY_ID)
#: Populated only after the one witnessed sectors-v7 registration.
REGISTERED_RECORD_SHA256: tuple[str, str] | None = None
REGISTERED_ANCHOR_LINE: bytes | None = None
SUPERSEDED_BY: Mapping[str, Any] | None = None

DISCOVERY = {"start": v9.DISCOVERY_START, "end": v1.SPLIT}
HOLDOUT = {"start": v1.SPLIT, "end": v1.END}
PROBE_WINDOW = (v9.PROBE_START, v9.PROBE_END)
CONFIRMATORY = {
    "trial": s4.PRIMARY_TRIAL,
    "direction": v1.PRIMARY_DIRECTION,
    "discovery_alpha_one_sided": 0.05,
    "holdout_alpha_one_sided": 0.10,
}
GATE = {
    "spec": "GD10b GateSpec v1",
    "min_survivors": 4,
    "of": 11,
    "branch_10_min_survivors": 4,
    "holdout_alpha_one_sided": 0.10,
    "breadth_alpha": 0.05,
    "breadth_null": "joint_block_signflip_centred_union_grid_max_block",
    "loso_alpha": 0.05,
    "top_entity_max_share": 0.25,
    "min_forward": 2,
}
STAGE0 = {
    "target_ic": v1.POWER_GATE_IC,
    "gate": v1.POWER_GATE,
    "alpha_one_sided": 0.05,
    "gaussian": {"sims": v9.GAUSSIAN_SIMS, "perms": v1.POWER_PERMS, "seed": v1.SEED},
    "e0": {
        "scenario": v9.E0_GATED_SCENARIO,
        "sims": v9.E0_SIMS,
        "perms": v1.POWER_PERMS,
        "manifest_sha256": v9.E0_MANIFEST_SHA256,
    },
    "untestable": (
        "below 0.50 on either model, or issuer admission probe or vendor coverage failure after one attempt "
        "(sector ETF admission failure does not make a sector untestable)"
    ),
}
BENCHMARK_RULE: Mapping[str, Any] = {
    "gates_admission": False,
    "gates_testability": False,
    "sessions_source": XNYS_MANIFEST_PATH.as_posix(),
    "sessions_manifest_sha256": XNYS_MANIFEST_SHA256,
    "labels": "raw split-first issuer forward close-to-close return (no sector-ETF subtraction)",
    "magnitudes_when_etf_admitted": (
        "sector-ETF-relative (XLRE/XLC pre-inception equal-weighted issuer mean applies to magnitudes only)"
    ),
    "magnitudes_when_etf_not_admitted": BENCHMARK_MAGNITUDE_UNAVAILABLE,
    "late_etf_pre_inception_applies_to": "magnitudes_only",
    "late_etf_start": dict(s4.LATE_ETF_START),
}
#: Exhaustive causes of an untestable sector (§3.4); benchmark ETF admission failure is explicitly excluded.
UNTESTABLE_CAUSES = frozenset({
    "issuer_admission_probe_failed",
    "vendor_coverage_failed",
    "stage0_below_gate",
})


# --- pin, placeholder, GateSpec, and preregistration checks ----------------------------------


def _check_gate_spec() -> str:
    from analysis import generalization_gate as gate

    if (
        not v1._is_hex64(GATE_SPEC_SHA256)
        or gate.GATE_SPEC_V1_SHA256 != GATE_SPEC_SHA256
        or gate.spec_sha256(gate.GATE_SPEC_V1) != GATE_SPEC_SHA256
    ):
        raise PermissionError("GateSpec v1 in code differs from the sectors-v7 pin")
    return GATE_SPEC_SHA256


def check_prereg(repo_root: Path = s4.REPO) -> str:
    """Verify the sectors-v7 preregistration body matches its pin and GateSpec v1 matches."""
    _check_gate_spec()
    if not v1._is_hex64(PREREG_BODY_SHA256):
        raise PermissionError("sectors-v7 body hash is not bound")
    path = Path(repo_root) / PREREG_PATH
    text = v1.prereg_body(path.read_text(encoding="utf-8"))
    actual = v1.prereg_body_sha256(path)
    if actual != PREREG_BODY_SHA256:
        raise PermissionError("sectors-v7 preregistration body differs from its pin")
    if GATE_SPEC_SHA256 not in text or XNYS_MANIFEST_SHA256 not in text:
        raise PermissionError("sectors-v7 body does not cite the pinned GateSpec v1 and XNYS manifest hashes")
    return actual


def _bound() -> tuple[str, str, str, str]:
    """Require body hash, GateSpec v1, v9 STOP/probe pins, and witnessed v9 registration to be bound."""
    _check_gate_spec()
    _, v8_stop, s6_stop = v9._bound()
    if not (
        v1._is_hex64(PREREG_BODY_SHA256)
        and v1._is_hex64(V9_REGISTRATION_HEAD_SHA256)
        and "PLACEHOLDER" not in str(V9_REGISTRATION_HEAD_SHA256)
        and v1._is_hex64(XNYS_MANIFEST_SHA256)
        and XNYS_MANIFEST_SHA256 == v9.XNYS_MANIFEST_SHA256
        and v9.REGISTERED_RECORD_SHA256 is not None
        and v9.REGISTERED_RECORD_SHA256[1] == V9_REGISTRATION_HEAD_SHA256
        and v9.REGISTERED_ANCHOR_LINE is not None
    ):
        raise PermissionError("sectors-v7 body, GateSpec, and witnessed v9 registration are not bound")
    return PREREG_BODY_SHA256, V9_REGISTRATION_HEAD_SHA256, v8_stop, s6_stop


def check_ready_to_register(repo_root: Path = s4.REPO) -> str:
    """Refuse registration while any code pin or preregistration body placeholder remains."""
    body, v9_head, v8_stop, s6_stop = _bound()
    v9.check_ready_to_register(repo_root)
    check_prereg(repo_root)
    text = v1.prereg_body((Path(repo_root) / PREREG_PATH).read_text(encoding="utf-8"))
    if "@@" in text or "PLACEHOLDER" in text:
        raise PermissionError("sectors-v7 preregistration body still contains placeholder tokens")
    required_citations = (
        v9.PREREG_BODY_SHA256,
        v9_head,
        v8_stop,
        s6_stop,
        GATE_SPEC_SHA256,
        XNYS_MANIFEST_SHA256,
    )
    missing = [token for token in required_citations if token not in text]
    if missing:
        raise PermissionError(f"sectors-v7 preregistration body omits bound hashes: {missing[:3]}")
    return body


# --- benchmark rule: testability & reported-only magnitudes ----------------------------------


def assess_sector_testability(
    sector: str,
    *,
    issuer_probe_passed: bool,
    vendor_coverage_passed: bool,
    benchmark_admitted: bool,
    stage0_gaussian_power: float | None = None,
    stage0_e0_power: float | None = None,
) -> dict[str, Any]:
    """Evaluate sectors-v7 §3.4 testability: sector-ETF failure never makes a sector untestable."""
    if sector not in s4.SECTOR_ETF:
        raise ValueError(f"unknown non-Technology sector: {sector!r}")
    reasons: list[str] = []
    if issuer_probe_passed is not True:
        reasons.append("issuer_admission_probe_failed")
    if vendor_coverage_passed is not True:
        reasons.append("vendor_coverage_failed")
    stage0_ok = (
        isinstance(stage0_gaussian_power, (int, float))
        and not isinstance(stage0_gaussian_power, bool)
        and math.isfinite(float(stage0_gaussian_power))
        and float(stage0_gaussian_power) >= v1.POWER_GATE
        and isinstance(stage0_e0_power, (int, float))
        and not isinstance(stage0_e0_power, bool)
        and math.isfinite(float(stage0_e0_power))
        and float(stage0_e0_power) >= v1.POWER_GATE
    )
    if not stage0_ok:
        reasons.append("stage0_below_gate")
    testable = len(reasons) == 0
    etf_ok = benchmark_admitted is True
    return {
        "sector": sector,
        "benchmark_etf": s4.SECTOR_ETF[sector],
        "testable": testable,
        "terminal_kind_if_untestable": None if testable else "stage0_untestable",
        "untestable_reasons": tuple(reasons),
        "benchmark_admitted": etf_ok,
        "etf_magnitudes_available": etf_ok,
        "magnitude_status": "available" if etf_ok else BENCHMARK_MAGNITUDE_UNAVAILABLE,
    }


def sector_benchmark_returns(
    sector: str,
    closes: pd.DataFrame,
    issuer_tickers: list[str],
    positions: list[int],
    horizon: int,
    *,
    window: str,
    benchmark_admitted: bool,
) -> np.ndarray | None:
    """Per-decision benchmark return for reported magnitudes only; None when the sector ETF failed.

    For late-inception ETFs (``XLRE``, ``XLC``) when ``benchmark_admitted`` is True, decision
    dates before the ETF's first finite close inside ``window`` use the equal-weighted mean
    forward close-to-close return of the sector's admitted issuers, strictly for magnitudes.
    """
    if sector not in s4.SECTOR_ETF:
        raise ValueError(f"unknown non-Technology sector: {sector!r}")
    if horizon < 1:
        raise ValueError("horizon must be >= 1")
    if not benchmark_admitted:
        return None
    etf = s4.SECTOR_ETF[sector]
    if etf not in closes.columns:
        raise ValueError(f"benchmark_admitted=True requires {etf} closes in the price frame")

    lo, hi = v1.window_bounds(window)
    day = pd.DatetimeIndex(closes.index)
    day = day.tz_localize("UTC") if day.tz is None else day.tz_convert("UTC")
    inside = np.asarray((day >= lo.normalize()) & (day < hi.normalize()))

    etf_visible = closes[etf].to_numpy(dtype=float).copy()
    etf_visible[~inside] = np.nan
    finite_etf = np.flatnonzero(np.isfinite(etf_visible))
    first_etf_pos = int(finite_etf[0]) if len(finite_etf) else len(closes.index)

    issuer_visible = closes[issuer_tickers].to_numpy(dtype=float).copy()
    issuer_visible[~inside, :] = np.nan
    late_etf = etf in s4.LATE_ETF_START

    out = np.full(len(positions), np.nan, dtype=float)
    for k, i in enumerate(positions):
        if late_etf and i < first_etf_pos:
            with np.errstate(divide="ignore", invalid="ignore"):
                rets = issuer_visible[i + horizon] / issuer_visible[i] - 1.0
            finite = rets[np.isfinite(rets)]
            if len(finite):
                out[k] = float(np.mean(finite))
        else:
            with np.errstate(divide="ignore", invalid="ignore"):
                val = float(etf_visible[i + horizon] / etf_visible[i] - 1.0)
            if math.isfinite(val):
                out[k] = val
    return out


# --- registry, v9 registration binding, and terminal check -----------------------------------


def registry(log_dir: Path) -> ForwardLog:
    body, _, _, _ = _bound()
    return ForwardLog(
        log_dir,
        log_filename=REGISTRY_LOG,
        anchor_filename=REGISTRY_ANCHORS,
        lock_filename=REGISTRY_LOCK,
        prereg_sha256=body,
    )


def verify_v9_registration(log_dir: Path, vault_repo: Path, tip: str) -> dict:
    """Exactly two local v9 records and exactly their off-host registration anchor at ``tip``."""
    _, head, _, _ = _bound()
    content = v1._git(Path(vault_repo), "show", f"{tip}:{v9.WITNESS_PATH}", binary=True)
    if content != v9.REGISTERED_ANCHOR_LINE + b"\n":
        raise PermissionError("v9 is not exactly at its witnessed two-record registration")
    log = v9.registry(log_dir)
    with tempfile.TemporaryDirectory() as scratch:
        external = Path(scratch) / "v9.anchors.jsonl"
        external.write_bytes(content)
        with log.locked():
            check = log.verify_chain(external_anchors=external)
            records = log.read_all()
            heads = v1._line_sha256(log)
    if (
        not check["ok"]
        or len(records) != 2
        or tuple(heads) != v9.REGISTERED_RECORD_SHA256
        or heads[-1] != head
        or records[-1].get("kind") != "preregistration"
    ):
        raise PermissionError("v9 two-record registration differs from its witness")
    return {"records": 2, "head_sha256": head, "tip": tip}


def check_registration_census(census: Mapping[str, Any], *, v9_log_dir: Path, witness_repo: Path) -> None:
    """Registration is allowed only while v9 sits at exactly its two registration records."""
    _, _, v8_stop, s6_stop = _bound()
    v9.check_census(
        census,
        v8_stop_head_sha256=v8_stop,
        sectors_v6_stop_head_sha256=s6_stop,
        witness_repo=witness_repo,
        opening=True,
    )
    if set(census.get("files") or {}) != v9.BASELINE_REGISTRIES | {v9.REGISTRY_ID}:
        raise PermissionError("sectors-v7 registration needs the exact pre-sectors-v7 baseline")
    if (census.get("records") or {}).get(v9.REGISTRY_ID) != 2:
        raise PermissionError("sectors-v7 must register before any v9 opening (v9 at exactly two records)")
    verify_v9_registration(v9_log_dir, witness_repo, census["tip"])


def registration_records(now: datetime, code_sha: str, *, repo_root: Path = s4.REPO) -> list[dict]:
    body, v9_head, v8_stop, s6_stop = _bound()
    if now.tzinfo is None or not isinstance(code_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", code_sha):
        raise ValueError("registration needs a timezone and reviewed code SHA")
    check_ready_to_register(repo_root)
    common = {
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": body,
        "promotion_allowed": False,
    }
    return [
        {"kind": "header", "version": VERSION, **common},
        {
            "kind": "preregistration",
            "version": VERSION,
            **common,
            "registry_id": REGISTRY_ID,
            "witness_path": WITNESS_PATH,
            "technology_run": {
                "version": v9.VERSION,
                "prereg_sha256": v9.PREREG_BODY_SHA256,
                "registration_head_sha256": v9_head,
                "witness_path": v9.WITNESS_PATH,
                "registered_while_v9_records": 2,
            },
            "terminal_stops": {
                v8.VERSION: {
                    "head_sha256": v8_stop,
                    "records": v8.STOP_RECORDS,
                    "status": v8.STOP_SUPERSEDED_STATUS,
                    "witness_path": v8.WITNESS_PATH,
                },
                s6.VERSION: {
                    "head_sha256": s6_stop,
                    "records": s6.STOP_RECORDS,
                    "status": s6.STOP_STATUS,
                    "witness_path": s6.WITNESS_PATH,
                },
                v7.VERSION: {
                    "head_sha256": v8.V7_STOP_HEAD_SHA256,
                    "records": v8.V7_STOP_RECORDS,
                    "witness_path": v7.WITNESS_PATH,
                },
                v7.v6.VERSION: {
                    "head_sha256": v7.V6_STOP_HEAD_SHA256,
                    "records": v7.V6_STOP_RECORDS,
                    "witness_path": v7.v6.WITNESS_PATH,
                },
            },
            "supersedes": {
                "version": s6.VERSION,
                "prereg_sha256": s6.PREREG_BODY_SHA256,
                "registry_head_sha256": s6.REGISTERED_RECORD_SHA256[1],
                "terminal_stop_head_sha256": s6_stop,
                "earlier_sectors_v4": {
                    "version": s4.VERSION,
                    "prereg_sha256": s4.PREREG_BODY_SHA256,
                    "registry_head_sha256": s4.REGISTERED_RECORD_SHA256[1],
                },
                "never_registered": "vs1-sectors-v5 (names v7; can never open)",
            },
            "calendar_manifest": {
                "path": XNYS_MANIFEST_PATH.as_posix(),
                "sha256": XNYS_MANIFEST_SHA256,
                "calendar_version": v9.XNYS_CALENDAR_VERSION,
                "probe_sessions": v9.PROBE_CALENDAR_SESSIONS,
                "probe_span": list(PROBE_WINDOW),
            },
            "benchmark_rule": json.loads(json.dumps(BENCHMARK_RULE)),
            "run": {
                "sectors": list(s4.SECTOR_ETF),
                "benchmarks": dict(s4.SECTOR_ETF),
                "trials_per_sector": list(v1.trial_names()),
                "confirmatory": dict(CONFIRMATORY),
            },
            "discovery": dict(DISCOVERY),
            "holdout": dict(HOLDOUT),
            "probe_window": list(PROBE_WINDOW),
            "stage0": json.loads(json.dumps(STAGE0)),
            "generalization_gate": {**GATE, "spec_sha256": GATE_SPEC_SHA256},
        },
    ]


def register(
    log_dir: Path,
    now: datetime,
    code_sha: str,
    *,
    v9_log_dir: Path,
    witness,
    dry_run: bool = True,
    repo_root: Path = s4.REPO,
) -> dict:
    """One new sectors-v7 chain, only while v9 is exactly at its witnessed two-record registration.

    ``witness`` must be the fresh v9 ``OffhostWitness`` issued by ``v9.check_offhost`` (a raw
    census from an older tip is refused by type).
    """
    _bound()
    if not isinstance(witness, v2.OffhostWitness) or witness.path != v9.WITNESS_PATH:
        raise PermissionError("sectors-v7 registration needs a fresh v9 off-host witness (v9.check_offhost)")
    check_registration_census(witness.census, v9_log_dir=v9_log_dir, witness_repo=witness.repo)
    log9 = v9.registry(v9_log_dir)
    with log9.locked():
        registered_at = datetime.fromisoformat(log9.read_all()[1]["run_at"])  # verified two-record chain
    if now.tzinfo is None or now < registered_at:
        raise ValueError("sectors-v7 registration time must follow v9's registration")
    records = registration_records(now, code_sha, repo_root=repo_root)
    heads = v1.chained_sha256(records)
    if REGISTERED_RECORD_SHA256 is not None and tuple(heads) != REGISTERED_RECORD_SHA256:
        raise PermissionError("sectors-v7 registration would fork its pinned chain")
    if dry_run:
        return {"dry_run": True, "would_register_sha256": heads}
    log = registry(log_dir)
    with log.locked():
        check = log.verify_chain()
        if not check["ok"] or log.read_all():
            raise PermissionError("sectors-v7 registry is broken or already registered")
        appended = log.append_locked(records)
    return {"appended": [r["kind"] for r in appended], "head_sha256": heads[-1]}


def verify_v9_terminal(v9_log_dir: Path, witness: v2.OffhostWitness) -> dict:
    """Verify v9's local chain and off-host witness end in a witnessed terminal holdout_result or STOP."""
    _, v9_reg_head, v8_stop, s6_stop = _bound()
    if not isinstance(witness, v2.OffhostWitness) or witness.path != v9.WITNESS_PATH:
        raise PermissionError("sectors-v7 requires a fresh v9 off-host witness (v9.check_offhost)")
    v9.check_census(
        witness.census,
        v8_stop_head_sha256=v8_stop,
        sectors_v6_stop_head_sha256=s6_stop,
        witness_repo=witness.repo,
        opening=True,
    )
    log9 = v9.registry(Path(v9_log_dir))
    with tempfile.TemporaryDirectory() as scratch:
        external = Path(scratch) / Path(v9.WITNESS_PATH).name
        external.write_bytes(witness.content)
        with log9.locked():
            check = log9.verify_chain(external_anchors=external)
            records = log9.read_all()
            heads = v1._line_sha256(log9)
    if not check["ok"]:
        raise PermissionError(f"off-host anchor log does not witness the v9 registry: {check['detail']}")
    anchors = [json.loads(line) for line in witness.lines]
    if (
        len(records) < 2
        or tuple(heads[:2]) != v9.REGISTERED_RECORD_SHA256
        or heads[1] != v9_reg_head
        or not anchors
        or anchors[-1].get("records") != len(records)
        or anchors[-1].get("head_sha256") != heads[-1]
        or (witness.census.get("records") or {}).get(v9.REGISTRY_ID) != len(records)
    ):
        raise PermissionError("v9 local chain is not witnessed through its current head")
    if len(records) <= 2:
        raise PermissionError("v9 is not terminal (still at its two-record registration)")
    last = records[-1]
    kind = last.get("kind")
    if kind == "holdout_result":
        if last.get("prereg_sha256") != v9.PREREG_BODY_SHA256 or last.get("promotion_allowed") is not False:
            raise PermissionError("v9 holdout_result record fails prereg_sha256 or promotion_allowed check")
        return {
            "terminal_kind": "holdout_result",
            "records": len(records),
            "head_sha256": heads[-1],
            "verdict": last.get("verdict"),
            "witness_path": v9.WITNESS_PATH,
            "witness_tip": witness.tip,
        }
    if kind == "status":
        stop = v9.verify_terminal_stop(Path(v9_log_dir), witness)
        return {
            "terminal_kind": "stop",
            "records": stop["records"],
            "head_sha256": stop["head_sha256"],
            "status": stop["status"],
            "witness_path": v9.WITNESS_PATH,
            "witness_tip": witness.tip,
        }
    raise PermissionError(
        f"v9 is not terminal (last witnessed record is {kind!r}; needs holdout_result or STOP)"
    )


def check_open(v9_log_dir: Path | None = None, witness: v2.OffhostWitness | None = None) -> None:
    """Refuse unless v9 is terminal and witnessed; still fails closed until a reviewed joint-run harness exists."""
    if v9_log_dir is None or witness is None:
        raise PermissionError(
            "sectors-v7 cannot open: v9 must be terminal and witnessed first, and no reviewed "
            "sectors-v7 joint-run harness exists"
        )
    verify_v9_terminal(v9_log_dir, witness)
    raise PermissionError(
        "no reviewed sectors-v7 joint-run harness exists (v9 terminal record verified; "
        "opening awaits reviewed joint-run harness)"
    )
