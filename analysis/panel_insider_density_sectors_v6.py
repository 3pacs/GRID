"""Sectors-v6 custody scaffold: the ten-sector generalization test bound to VS1 v8 (GD-10a).

Registration only. There is no joint-run, probe or price-opening harness here, and
``check_open`` always refuses. Pins stay fail-closed until the witnessed v8
registration head and this body are bound by reviewed pin changes.

Order (v8 body section 0, this body section 5): sectors-v6 must be registered and
witnessed while the v8 witness covers exactly v8's two registration records, i.e.
after v8's registration witness and before v8 ``discovery_opened``.
"""

from __future__ import annotations

import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v4 as s4
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_v8 as v8
from analysis.research_forward_log import ForwardLog

VERSION = "vs1-sectors-v6"
REGISTRY_ID = v8.SECTORS_V6  # "sectors-v6"
PREREG_PATH = Path("docs/paper_log/vs1-sectors-v6-preregistration.md")
PREREG_BODY_SHA256: str | None = None  # pinned at review, after the GateSpec v1 hash is cited
#: The exact witnessed two-record v8 registration head; bound only after v8 is registered and witnessed.
V8_REGISTRATION_HEAD_SHA256: str | None = None
REGISTRY_LOG = "granular_panel_prereg_sectors_v6.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_sectors_v6.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_sectors_v6.lock"
WITNESS_PATH = v1.canonical_witness_path(REGISTRY_ID)
REGISTERED_RECORD_SHA256: tuple[str, str] | None = None
REGISTERED_ANCHOR_LINE: bytes | None = None

DISCOVERY = {"start": v8.DISCOVERY_START, "end": v1.SPLIT}
HOLDOUT = {"start": v1.SPLIT, "end": v1.END}
PROBE_WINDOW = (v8.PROBE_START, "2019-12-31")
CONFIRMATORY = {"trial": s4.PRIMARY_TRIAL, "direction": v1.PRIMARY_DIRECTION,
                "discovery_alpha_one_sided": 0.05, "holdout_alpha_one_sided": 0.10}
GATE = {"breadth_sectors": 4, "of": 11, "of_if_v8_stopped": 10, "survival_alpha_one_sided": 0.10,
        "loso_alpha": 0.05, "top_entity_share_max": 0.25, "forward_supported_min": 2,
        "gate_spec": "GD10b GateSpec v1 (sha256 cited in the body)"}


def _bound() -> tuple[str, str]:
    if not (v1._is_hex64(PREREG_BODY_SHA256) and v1._is_hex64(V8_REGISTRATION_HEAD_SHA256)
            and v8.REGISTERED_RECORD_SHA256 and v8.REGISTERED_RECORD_SHA256[1] == V8_REGISTRATION_HEAD_SHA256
            and v8.REGISTERED_ANCHOR_LINE is not None):
        raise PermissionError("sectors-v6 body and the witnessed v8 registration are not bound")
    return PREREG_BODY_SHA256, V8_REGISTRATION_HEAD_SHA256


def check_prereg(repo_root: Path = s4.REPO) -> str:
    body, _ = _bound()
    actual = v1.prereg_body_sha256(Path(repo_root) / PREREG_PATH)
    if actual != body:
        raise PermissionError("sectors-v6 preregistration body differs from its pin")
    return actual


def registry(log_dir: Path) -> ForwardLog:
    body, _ = _bound()
    return ForwardLog(log_dir, log_filename=REGISTRY_LOG, anchor_filename=REGISTRY_ANCHORS,
                      lock_filename=REGISTRY_LOCK, prereg_sha256=body)


def verify_v8_registration(log_dir: Path, vault_repo: Path, tip: str) -> dict:
    """Exactly two local v8 records and exactly their off-host registration anchor at ``tip``."""
    _, head = _bound()
    content = v1._git(Path(vault_repo), "show", f"{tip}:{v8.WITNESS_PATH}", binary=True)
    if content != v8.REGISTERED_ANCHOR_LINE + b"\n":
        raise PermissionError("v8 is not exactly at its witnessed two-record registration")
    log = v8.registry(log_dir)
    with tempfile.TemporaryDirectory() as scratch:
        external = Path(scratch) / "v8.anchors.jsonl"
        external.write_bytes(content)
        with log.locked():
            check = log.verify_chain(external_anchors=external)
            records = log.read_all()
            heads = v1._line_sha256(log)
    if not check["ok"] or len(records) != 2 or tuple(heads) != v8.REGISTERED_RECORD_SHA256 \
            or heads[-1] != head or records[-1].get("kind") != "preregistration":
        raise PermissionError("v8 two-record registration differs from its witness")
    return {"records": 2, "head_sha256": head, "tip": tip}


def check_registration_census(census: Mapping[str, Any], *, v8_log_dir: Path, witness_repo: Path) -> None:
    """Registration is allowed only while v8 sits at exactly its two registration records."""
    _bound()
    v8.check_census(census, stop_head_sha256=v8.V7_STOP_HEAD_SHA256, witness_repo=witness_repo, opening=True)
    if set(census.get("files") or {}) != v8.BASELINE_REGISTRIES | {v8.REGISTRY_ID}:
        raise PermissionError("sectors-v6 registration needs the exact pre-sectors-v6 baseline")
    if (census.get("records") or {}).get(v8.REGISTRY_ID) != 2:
        raise PermissionError("sectors-v6 must register before any v8 opening (v8 at exactly two records)")
    verify_v8_registration(v8_log_dir, witness_repo, census["tip"])


def registration_records(now: datetime, code_sha: str) -> list[dict]:
    body, v8_head = _bound()
    if now.tzinfo is None or not isinstance(code_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", code_sha):
        raise ValueError("registration needs a timezone and reviewed code SHA")
    check_prereg()
    common = {"run_at": now.isoformat(), "code_sha": code_sha, "prereg_path": PREREG_PATH.as_posix(),
              "prereg_sha256": body, "promotion_allowed": False}
    return [
        {"kind": "header", "version": VERSION, **common},
        {"kind": "preregistration", "version": VERSION, **common,
         "registry_id": REGISTRY_ID, "witness_path": WITNESS_PATH,
         "technology_run": {"version": v8.VERSION, "prereg_sha256": v8.PREREG_BODY_SHA256,
                            "registration_head_sha256": v8_head, "witness_path": v8.WITNESS_PATH,
                            "registered_while_v8_records": 2},
         "terminal_stops": {v7.VERSION: {"head_sha256": v8.V7_STOP_HEAD_SHA256, "records": 3,
                                         "witness_path": v7.WITNESS_PATH},
                            v7.v6.VERSION: {"head_sha256": v7.V6_STOP_HEAD_SHA256, "records": 3,
                                            "witness_path": v7.v6.WITNESS_PATH}},
         "supersedes": {"version": s4.VERSION, "prereg_sha256": s4.PREREG_BODY_SHA256,
                        "registry_head_sha256": s4.REGISTERED_RECORD_SHA256[1],
                        "never_registered": "vs1-sectors-v5 (names v7; can never open)"},
         "run": {"sectors": list(s4.SECTOR_ETF), "benchmarks": dict(s4.SECTOR_ETF),
                 "trials_per_sector": list(v1.trial_names()), "confirmatory": dict(CONFIRMATORY)},
         "discovery": dict(DISCOVERY), "holdout": dict(HOLDOUT), "probe_window": list(PROBE_WINDOW),
         "generalization_gate": dict(GATE)},
    ]


def register(log_dir: Path, now: datetime, code_sha: str, *, v8_log_dir: Path, witness_repo: Path,
             census: Mapping[str, Any], dry_run: bool = True) -> dict:
    """One new sectors-v6 chain, only while v8 is exactly at its witnessed registration."""
    check_registration_census(census, v8_log_dir=v8_log_dir, witness_repo=witness_repo)
    records = registration_records(now, code_sha)
    heads = v1.chained_sha256(records)
    if REGISTERED_RECORD_SHA256 is not None and tuple(heads) != REGISTERED_RECORD_SHA256:
        raise PermissionError("sectors-v6 registration would fork its pinned chain")
    if dry_run:
        return {"dry_run": True, "would_register_sha256": heads}
    log = registry(log_dir)
    with log.locked():
        check = log.verify_chain()
        if not check["ok"] or log.read_all():
            raise PermissionError("sectors-v6 registry is broken or already registered")
        appended = log.append_locked(records)
    return {"appended": [r["kind"] for r in appended], "head_sha256": heads[-1]}


def check_open() -> None:
    raise PermissionError("no reviewed sectors-v6 joint-run harness exists; v8 must be terminal first")
