"""Sectors-v5 custody scaffold; the ten other sectors wait for v7 holdout_result.

No joint-run or price-opening harness is present here. Registration pins are
bound only after the v7 body, registration and vault witness are verified.
"""

from __future__ import annotations

import json
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v4 as s4
from analysis import panel_insider_density_v7 as v7
from analysis.research_forward_log import ForwardLog

VERSION = "vs1-sectors-v5"
REGISTRY_ID = "sectors-v5"
PREREG_PATH = Path("docs/paper_log/vs1-sectors-v5-preregistration.md")
PREREG_BODY_SHA256 = "7763fa2b286725af430f7c69c1880610e78a4802fffcbdea9237ea29347b7f72"
V7_REGISTRATION_HEAD_SHA256: str | None = None
REGISTRY_LOG = "granular_panel_prereg_sectors_v5.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_sectors_v5.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_sectors_v5.lock"
WITNESS_PATH = v1.canonical_witness_path(REGISTRY_ID)
REGISTERED_RECORD_SHA256: tuple[str, str] | None = None
REGISTERED_ANCHOR_LINE: bytes | None = None

FROZEN_REGISTRIES = v7.FROZEN_REGISTRIES  # through sectors-v4
ALLOWED_REGISTRIES = frozenset((*FROZEN_REGISTRIES, v7.v6.VERSION, v7.VERSION, REGISTRY_ID))


def _bound() -> tuple[str, str]:
    if not (v1._is_hex64(PREREG_BODY_SHA256) and v1._is_hex64(V7_REGISTRATION_HEAD_SHA256)
            and v7.REGISTERED_RECORD_SHA256
            and v7.REGISTERED_RECORD_SHA256[1] == V7_REGISTRATION_HEAD_SHA256):
        raise PermissionError("sectors-v5 body and witnessed v7 registration are not bound")
    return PREREG_BODY_SHA256, V7_REGISTRATION_HEAD_SHA256


def check_prereg(repo_root: Path = s4.REPO) -> str:
    body, _ = _bound()
    actual = v1.prereg_body_sha256(Path(repo_root) / PREREG_PATH)
    if actual != body:
        raise PermissionError("sectors-v5 preregistration body differs from its pin")
    return actual


def registry(log_dir: Path) -> ForwardLog:
    body, _ = _bound()
    return ForwardLog(log_dir, log_filename=REGISTRY_LOG, anchor_filename=REGISTRY_ANCHORS,
                      lock_filename=REGISTRY_LOCK, prereg_sha256=body)


def verify_v7_registration(log_dir: Path, vault_repo: Path, tip: str) -> dict:
    """Require exactly two v7 records and their exact off-host registration anchor."""
    _, head = _bound()
    if v7.REGISTERED_ANCHOR_LINE is None:
        raise PermissionError("v7 registration anchor is not pinned")
    content = v1._git(Path(vault_repo), "show", f"{tip}:{v7.WITNESS_PATH}", binary=True)
    if content != v7.REGISTERED_ANCHOR_LINE + b"\n":
        raise PermissionError("v7 two-record registration is not witnessed exactly")
    log = v7.registry(log_dir)
    with tempfile.TemporaryDirectory() as scratch:
        external = Path(scratch) / "v7.anchors.jsonl"
        external.write_bytes(content)
        with log.locked():
            check = log.verify_chain(external_anchors=external)
            records = log.read_all()
            heads = v1._line_sha256(log)
    if not check["ok"] or len(records) != 2 or tuple(heads) != v7.REGISTERED_RECORD_SHA256 \
            or heads[-1] != head or records[-1].get("kind") != "preregistration":
        raise PermissionError("v7 two-record registration differs from its witness")
    return {"records": 2, "head_sha256": head, "tip": tip}


def check_registration_census(census: dict, *, v7_log_dir: Path,
                              witness_repo: Path) -> None:
    """The sectors-v5 registration can follow v7 registration, before holdout."""
    _bound()
    v7.check_census(census, stop_head_sha256=v7.V6_STOP_HEAD_SHA256,
                    witness_repo=witness_repo, opening=True)
    if set(census["files"]) != v7.BASELINE_REGISTRIES | {v7.REGISTRY_ID}:
        raise PermissionError("sectors-v5 registration needs the exact pre-sectors-v5 baseline")
    if census["records"][v7.REGISTRY_ID] != 2:
        raise PermissionError("sectors-v5 registration requires v7 before any opening")
    verify_v7_registration(v7_log_dir, witness_repo, census["tip"])


def register(log_dir: Path, now: datetime, code_sha: str, *, v7_log_dir: Path,
             witness_repo: Path, census: dict) -> list[dict]:
    """Create one new sectors-v5 chain only after v7's exact witnessed registration."""
    check_registration_census(census, v7_log_dir=v7_log_dir, witness_repo=witness_repo)
    records = registration_records(now, code_sha)
    log = registry(log_dir)
    with log.locked():
        check = log.verify_chain()
        if not check["ok"] or log.read_all():
            raise PermissionError("sectors-v5 registry is broken or already registered")
        if REGISTERED_RECORD_SHA256 is not None and tuple(v1.chained_sha256(records)) != REGISTERED_RECORD_SHA256:
            raise PermissionError("sectors-v5 registration would fork its pinned chain")
        return log.append_locked(records)


def verify_v7_holdout_result(log_dir: Path, vault_repo: Path, tip: str) -> dict:
    """Verify the exact local v7 chain against its witness at this vault tip."""
    _, registration_head = _bound()
    if v7.REGISTERED_ANCHOR_LINE is None:
        raise PermissionError("v7 registration anchor is not pinned")
    content = v1._git(Path(vault_repo), "show", f"{tip}:{v7.WITNESS_PATH}", binary=True)
    if b"\r" in content or not content.endswith(b"\n") or not content.startswith(v7.REGISTERED_ANCHOR_LINE + b"\n"):
        raise PermissionError("v7 witness is absent or differs from its registration anchor")
    log = v7.registry(log_dir)
    with tempfile.TemporaryDirectory() as scratch:
        external = Path(scratch) / "v7.anchors.jsonl"
        external.write_bytes(content)
        with log.locked():
            check = log.verify_chain(external_anchors=external)
            records = log.read_all()
            heads = v1._line_sha256(log)
    anchors = [json.loads(line) for line in content.splitlines()]
    if not check["ok"] or tuple(heads[:2]) != v7.REGISTERED_RECORD_SHA256 \
            or heads[1] != registration_head or len(records) <= 2 \
            or records[-1].get("kind") != "holdout_result" \
            or anchors[-1].get("records") != len(records) \
            or anchors[-1].get("head_sha256") != heads[-1]:
        raise PermissionError("v7 has no exact witnessed terminal holdout_result")
    return {"tip": tip, "records": len(records), "head_sha256": heads[-1],
            "witness_path": v7.WITNESS_PATH}


def check_census(census: dict, *, v7_log_dir: Path | None = None,
                 witness_repo: Path | None = None) -> None:
    """Only the exact v6 STOP, sealed v7 result and two-record older chains pass."""
    _bound()
    if census.get("unknown"):
        raise PermissionError(f"unknown VS1 witnesses: {census['unknown']}")
    files, counts = census.get("files") or {}, census.get("records") or {}
    if set(files) != ALLOWED_REGISTRIES or set(counts) != ALLOWED_REGISTRIES:
        raise PermissionError("sectors-v5 witness census has missing or extra registries")
    if any(path != v1.canonical_witness_path(key) for key, path in files.items()):
        raise PermissionError("a VS1 witness is not at its canonical path")
    if any(counts[key] != 2 for key in FROZEN_REGISTRIES):
        raise PermissionError("an older Technology or sectors registry grew past two records")
    if v7_log_dir is None or witness_repo is None or not census.get("tip"):
        raise PermissionError("sectors-v5 requires v7's witnessed terminal holdout_result")
    v7_holdout = verify_v7_holdout_result(v7_log_dir, witness_repo, census["tip"])
    if counts.get(v7.REGISTRY_ID) != v7_holdout["records"]:
        raise PermissionError("sectors-v5 requires v7's witnessed terminal holdout_result")
    if counts.get(REGISTRY_ID) != 2:
        raise PermissionError("sectors-v5 witness grew past registration before its opening")
    if counts.get(v7.v6.VERSION) != v7.V6_STOP_RECORDS:
        raise PermissionError("v6 STOP witness must remain at three records")
    if witness_repo is None or not v1._is_hex64(v7.V6_STOP_HEAD_SHA256):
        raise PermissionError("sectors-v5 needs v6's pinned STOP at the same vault tip")
    v7._v6_anchor_at_tip(Path(witness_repo), census["tip"], v7.V6_STOP_HEAD_SHA256)


def registration_records(now: datetime, code_sha: str) -> list[dict]:
    body, v7_head = _bound()
    if now.tzinfo is None or not isinstance(code_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", code_sha):
        raise ValueError("registration needs a timezone and reviewed code SHA")
    check_prereg()
    common = {"run_at": now.isoformat(), "code_sha": code_sha,
              "prereg_path": PREREG_PATH.as_posix(), "prereg_sha256": body,
              "promotion_allowed": False}
    return [
        {"kind": "header", "version": VERSION, **common},
        {"kind": "preregistration", "version": VERSION, **common,
         "registry_id": REGISTRY_ID, "witness_path": WITNESS_PATH,
         "technology_run": {"version": v7.VERSION, "prereg_sha256": v7.PREREG_BODY_SHA256,
                            "registration_head_sha256": v7_head, "witness_path": v7.WITNESS_PATH,
                            "opening": "only after an exact witnessed holdout_result"},
         "v6_stop": {"head_sha256": v7.V6_STOP_HEAD_SHA256, "records": v7.V6_STOP_RECORDS,
                     "witness_path": v7.v6.WITNESS_PATH},
         "supersedes": {"version": s4.VERSION, "prereg_sha256": s4.PREREG_BODY_SHA256,
                        "registry_head_sha256": s4.REGISTERED_RECORD_SHA256[1]},
         "run": {"sectors": list(s4.SECTOR_ETF), "benchmarks": s4.SECTOR_ETF,
                 "k": s4.RUN_K, "alpha": s4.RUN_ALPHA, "primary_trial": s4.PRIMARY_TRIAL,
                 "trials_per_sector": list(v1.trial_names())},
         "technology_sector": "v7 is the eleventh sector for the generalization gate",
         "holdout": {"start": v1.SPLIT, "end": v1.END}},
    ]


def check_open() -> None:
    raise PermissionError("no reviewed sectors-v5 joint-run harness exists")
