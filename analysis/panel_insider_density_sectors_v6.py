"""Sectors-v6 custody scaffold: the ten-sector generalization test bound to VS1 v8 (GD-10a).

Registration only. There is no joint-run, probe or price-opening harness here, and
``check_open`` always refuses. Pins stay fail-closed until the witnessed v8
registration head and this body are bound by reviewed pin changes.

Order (v8 body section 0, this body section 5): sectors-v6 must be registered and
witnessed while the v8 witness covers exactly v8's two registration records, i.e.
after v8's registration witness and before any later v8 record (``inputs_frozen``,
a STOP or ``discovery_opened``) is witnessed.
"""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v4 as s4
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_v8 as v8
from analysis.research_forward_log import ForwardLog, canonical

VERSION = "vs1-sectors-v6"
REGISTRY_ID = v8.SECTORS_V6  # "sectors-v6"
PREREG_PATH = Path("docs/paper_log/vs1-sectors-v6-preregistration.md")
PREREG_BODY_SHA256: str | None = "5a8f7b6e73f8924d9578ee4a53f990127f22510ca357018da2bb63e147c3da76"
#: The exact witnessed two-record v8 registration head; bound only after v8 is registered and witnessed.
V8_REGISTRATION_HEAD_SHA256: str | None = "69a7d3276da1fffc10f0ea023151ff283dd0e154b9ee3509495c670ddff42bb5"
#: sha256 of GD10b GateSpec v1 (analysis.generalization_gate.GATE_SPEC_V1_SHA256, PR #784 head 05c7a89a).
#: Checked at run time against the merged module in :func:`_bound`.
GATE_SPEC_SHA256: str | None = "fa2fa2bd1b1ef5cb81168f393135795824991e08829b60f66a109decdf401174"
REGISTRY_LOG = "granular_panel_prereg_sectors_v6.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_sectors_v6.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_sectors_v6.lock"
WITNESS_PATH = v1.canonical_witness_path(REGISTRY_ID)
#: Registered once on grid-svr 2026-10-01T16:09:28Z (merged main 4dfc25c9, v8 at exactly two records);
#: witnessed on vault main 3ec4ffb7 at WITNESS_PATH (anchor file SHA-256 98369529...).
REGISTERED_RECORD_SHA256: tuple[str, str] | None = (
    "507fe8a8f659b9b8c847ccd6389a1a2672a4da1b4951955ac4d4c7b59f3c9416",
    "55c05e31b10621043a09f468fb2a6cf9a1fb8dafb75cbf15904db1e6ef7efa67",
)
REGISTERED_ANCHOR_LINE: bytes | None = (
    b'{"head_sha256":"55c05e31b10621043a09f468fb2a6cf9a1fb8dafb75cbf15904db1e6ef7efa67",'
    b'"prev_anchor_sha256":null,"records":2,"run_at":"2026-10-01T16:09:28+00:00"}'
)

DISCOVERY = {"start": v8.DISCOVERY_START, "end": v1.SPLIT}
HOLDOUT = {"start": v1.SPLIT, "end": v1.END}
PROBE_WINDOW = (v8.PROBE_START, "2019-12-31")
CONFIRMATORY = {"trial": s4.PRIMARY_TRIAL, "direction": v1.PRIMARY_DIRECTION,
                "discovery_alpha_one_sided": 0.05, "holdout_alpha_one_sided": 0.10}
GATE = {"spec": "GD10b GateSpec v1", "min_survivors": 4, "of": 11, "branch_10_min_survivors": 4,
        "holdout_alpha_one_sided": 0.10, "breadth_alpha": 0.05,
        "breadth_null": "joint_block_signflip_centred_union_grid_max_block",
        "loso_alpha": 0.05, "top_entity_max_share": 0.25, "min_forward": 2}
STAGE0 = {"target_ic": v1.POWER_GATE_IC, "gate": v1.POWER_GATE, "alpha_one_sided": 0.05,
          "gaussian": {"sims": v8.GAUSSIAN_SIMS, "perms": v1.POWER_PERMS, "seed": v1.SEED},
          "e0": {"scenario": v8.E0_GATED_SCENARIO, "sims": v8.E0_SIMS, "perms": v1.POWER_PERMS,
                 "manifest_sha256": v8.E0_MANIFEST_SHA256},
          "untestable": "below 0.50 on either model, or admission/probe/benchmark failure after one attempt"}


def _bound() -> tuple[str, str]:
    from analysis import generalization_gate as gate

    if gate.GATE_SPEC_V1_SHA256 != GATE_SPEC_SHA256 or gate.spec_sha256(gate.GATE_SPEC_V1) != GATE_SPEC_SHA256:
        raise PermissionError("GateSpec v1 in code differs from the sectors-v6 pin")
    if not (v1._is_hex64(PREREG_BODY_SHA256) and v1._is_hex64(V8_REGISTRATION_HEAD_SHA256)
            and v1._is_hex64(GATE_SPEC_SHA256)
            and v8.REGISTERED_RECORD_SHA256 and v8.REGISTERED_RECORD_SHA256[1] == V8_REGISTRATION_HEAD_SHA256
            and v8.REGISTERED_ANCHOR_LINE is not None):
        raise PermissionError("sectors-v6 body, GateSpec and the witnessed v8 registration are not bound")
    return PREREG_BODY_SHA256, V8_REGISTRATION_HEAD_SHA256


def check_prereg(repo_root: Path = s4.REPO) -> str:
    """The body hashes to its pin, has no placeholder left, and cites the bound v8 head and GateSpec."""
    body, v8_head = _bound()
    text = v1.prereg_body((Path(repo_root) / PREREG_PATH).read_text(encoding="utf-8"))
    if "@@" in text or v8_head not in text or GATE_SPEC_SHA256 not in text:
        raise PermissionError("sectors-v6 body still has a placeholder or does not cite the bound pins")
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
         "stage0": json.loads(json.dumps(STAGE0)),
         "generalization_gate": {**GATE, "spec_sha256": GATE_SPEC_SHA256}},
    ]


def register(log_dir: Path, now: datetime, code_sha: str, *, v8_log_dir: Path, witness,
             dry_run: bool = True) -> dict:
    """One new sectors-v6 chain, only while v8 is exactly at its witnessed registration.

    ``witness`` must be the fresh v8 ``OffhostWitness`` issued by ``v8.check_offhost`` (a raw
    census from an older tip is refused by type).
    """
    if not isinstance(witness, v2.OffhostWitness) or witness.path != v8.WITNESS_PATH:
        raise PermissionError("sectors-v6 registration needs a fresh v8 off-host witness (v8.check_offhost)")
    check_registration_census(witness.census, v8_log_dir=v8_log_dir, witness_repo=witness.repo)
    log8 = v8.registry(v8_log_dir)
    with log8.locked():
        registered_at = datetime.fromisoformat(log8.read_all()[1]["run_at"])  # verified two-record chain
    if now.tzinfo is None or now < registered_at:
        raise ValueError("sectors-v6 registration time must follow v8's registration")
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
    raise PermissionError(
        "no reviewed sectors-v6 joint-run harness exists; sectors-v6 is superseded by vs1-sectors-v7 and can never open"
    )


# --- terminal STOP: superseded unopened by sectors-v7 after v8 terminal STOP ----------------

SUPERSEDED_BY: Mapping[str, Any] | None = {"version": "vs1-sectors-v7"}
WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = v1.WITNESS_BRANCH
WITNESS_REF = "refs/vs1-sectors-v6-witness/main"

STOP_STATUS = "STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED"
STOP_SUCCESSOR = "vs1-sectors-v7"
STOP_RECORDS = 3
STOP_REASON = (
    "VS1 v8 ended in witnessed terminal STOP (STOP_SUPERSEDED_BY_V9_UNOPENED) after the XLK benchmark "
    "failed the TwelveData cross-check under owner ruling R9; sectors-v6 is bound to v8 and is stopped "
    "unopened, superseded by vs1-sectors-v7 bound to vs1-v9, with no sector probe, Stage-0 or outcome read"
)
STOP_BASIS = (
    "witnessed v8 terminal STOP (owner ruling R9, 2026-10-07, and owner decision 2026-11-22 to proceed "
    "with vs1-v9); no sectors-v6 probe, Stage-0, freeze, discovery or holdout ran"
)
_V8_TERMINAL_KEYS = frozenset({"version", "head_sha256", "records", "status", "witness_path", "witness_tip"})


def _is_hex40(value: Any) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"[0-9a-f]{40}", value))


def _stop_required() -> dict[str, Any]:
    return {
        "kind": "status",
        "version": VERSION,
        "registry_id": REGISTRY_ID,
        "status": STOP_STATUS,
        "superseded_by": STOP_SUCCESSOR,
        "stage0_run": False,
        "prereg_gate_evaluated": False,
        "inputs_frozen": False,
        "discovery_opened": False,
        "holdout_opened": False,
        "prereg_sha256": PREREG_BODY_SHA256,
        "reason": STOP_REASON,
        "basis": STOP_BASIS,
        "promotion_allowed": False,
    }


def _chain(log: ForwardLog) -> list[dict]:
    check = log.verify_chain()
    if not check["ok"]:
        raise PermissionError(f"sectors-v6 registry chain is broken: {check['detail']}")
    records = log.read_all()
    heads = v1._line_sha256(log)
    if (
        len(records) < 2
        or records[0].get("kind") != "header"
        or records[1].get("kind") != "preregistration"
        or REGISTERED_RECORD_SHA256 is None
        or tuple(heads[:2]) != REGISTERED_RECORD_SHA256
    ):
        raise PermissionError("sectors-v6 registry does not start with its pinned two-record registration")
    return records


def check_offhost(vault_repo: Path, *, remote_url: str | None = None) -> v2.OffhostWitness:
    """Fetch vault main and verify append-only histories of sectors-v6 and v8 witnesses at one tip."""
    if REGISTERED_ANCHOR_LINE is None:
        raise PermissionError("sectors-v6 is not registered yet (no pinned anchor line in code)")
    if v8.REGISTERED_ANCHOR_LINE is None:
        raise PermissionError("v8 registration anchor is not pinned in code")
    url = WITNESS_REMOTE_URL if remote_url is None else remote_url
    repo = Path(vault_repo)
    v1._git(repo, "rev-parse", "--git-dir")
    v1._git(
        repo, "fetch", "--quiet", "--no-tags", "--no-write-fetch-head", url,
        f"+refs/heads/{WITNESS_BRANCH}:{WITNESS_REF}",
    )
    tip = v1._git(repo, "rev-parse", "--verify", f"{WITNESS_REF}^{{commit}}").strip()
    versions, previous = v2._walk_witness(repo, WITNESS_REF, WITNESS_PATH, REGISTERED_ANCHOR_LINE, url)
    at_tip = v1._git(repo, "show", f"{tip}:{WITNESS_PATH}", binary=True).replace(b"\r\n", b"\n")
    if [line for line in at_tip.split(b"\n") if line] != previous:
        raise PermissionError("sectors-v6 witness at fetched tip is not its last walked version")
    v8_versions, _ = v2._walk_witness(repo, WITNESS_REF, v8.WITNESS_PATH, v8.REGISTERED_ANCHOR_LINE, url)
    census = v1.vs1_witness_census(repo, tip)
    return v2.OffhostWitness(
        v2._WITNESS_TOKEN,
        repo,
        tip,
        b"\n".join(previous) + b"\n",
        versions,
        url,
        path=WITNESS_PATH,
        branch=WITNESS_BRANCH,
        earlier={v8.VERSION: v8_versions[-1]["covered_records"]},
        census=census,
    )


def require_witness(log_dir: Path, witness: v2.OffhostWitness | None, records_needed: int) -> dict:
    """Confirm the off-host sectors-v6 witness covers the local chain through ``records_needed``."""
    if not isinstance(witness, v2.OffhostWitness) or witness.path != WITNESS_PATH:
        raise PermissionError("sectors-v6 requires its pinned off-host witness (sectors_v6.check_offhost)")
    log = registry(log_dir)
    with tempfile.TemporaryDirectory() as scratch:
        path = Path(scratch) / Path(WITNESS_PATH).name
        path.write_bytes(witness.content)
        with log.locked():
            records = _chain(log)
            check = log.verify_chain(external_anchors=path)
    if not check["ok"]:
        raise PermissionError(f"off-host anchor log does not witness the sectors-v6 registry: {check['detail']}")
    covered = json.loads(witness.lines[-1])["records"]
    if covered < records_needed:
        raise PermissionError(
            f"off-host anchor log covers {covered} records; it must cover {records_needed} records"
        )
    return {"records": len(records), "witnessed_records": covered, "tip": witness.tip}


def _check_stop_census(witness: v2.OffhostWitness) -> None:
    """Ordering guard: v8 must be terminal (3 records) and vs1-v9 / sectors-v7 must not exist yet."""
    census = witness.census
    if census.get("unknown"):
        raise PermissionError(f"unknown VS1 witnesses: {census['unknown']}")
    files, counts = census.get("files") or {}, census.get("records") or {}
    if "vs1-v9" in files or "vs1-v9" in counts:
        raise PermissionError("sectors-v6 STOP must precede vs1-v9 registration (vs1-v9 already witnessed)")
    if "sectors-v7" in files or "sectors-v7" in counts:
        raise PermissionError("sectors-v6 STOP must precede sectors-v7 registration (sectors-v7 already witnessed)")
    expected = v8.BASELINE_REGISTRIES | {v8.REGISTRY_ID, REGISTRY_ID}
    if set(files) != expected or set(counts) != expected:
        raise PermissionError("sectors-v6 STOP witness census has missing or extra registries")
    for key, path in files.items():
        if path != v1.canonical_witness_path(key):
            raise PermissionError(f"{key} is not at its canonical witness path")
    if any(counts[key] != 2 for key in v8.FROZEN_REGISTRIES):
        raise PermissionError("an older Technology or sectors registry grew past two records")
    for key, records in v8.TERMINAL_REGISTRIES.items():
        if counts[key] != records:
            raise PermissionError(f"{key} witness must cover exactly its terminal STOP")
    if counts.get(v8.REGISTRY_ID) != v8.STOP_RECORDS:
        raise PermissionError("sectors-v6 STOP requires a witnessed v8 terminal STOP (3 records)")
    if counts.get(REGISTRY_ID) != 2 or json.loads(witness.lines[-1]).get("records") != 2:
        raise PermissionError("sectors-v6's canonical witness is not at the two-record baseline")
    v7._v6_anchor_at_tip(witness.repo, witness.tip, v7.V6_STOP_HEAD_SHA256)
    v8._v7_anchor_at_tip(witness.repo, witness.tip, v8.V7_STOP_HEAD_SHA256)


def _verify_v8_terminal_at_tip(v8_log_dir: Path, witness: v2.OffhostWitness) -> dict[str, Any]:
    """Verify v8's 3-record STOP_SUPERSEDED_BY_V9_UNOPENED locally and at ``witness.tip``."""
    content = v1._git(witness.repo, "show", f"{witness.tip}:{v8.WITNESS_PATH}", binary=True)
    if b"\r" in content or not content.endswith(b"\n"):
        raise PermissionError("v8 witness must have canonical LF line endings")
    lines = content.splitlines()
    if len(lines) != 2 or lines[0] != v8.REGISTERED_ANCHOR_LINE:
        raise PermissionError("v8 witness at tip is not an exact two-anchor terminal STOP")
    try:
        last_anchor = json.loads(lines[1])
    except ValueError as exc:
        raise PermissionError("v8 STOP anchor is not JSON") from exc
    if (
        lines[1] != canonical(last_anchor)
        or last_anchor.get("records") != v8.STOP_RECORDS
        or last_anchor.get("prev_anchor_sha256") != hashlib.sha256(lines[0]).hexdigest()
        or not v1._is_hex64(last_anchor.get("head_sha256"))
    ):
        raise PermissionError("v8 STOP anchor at tip is malformed")
    log8 = v8.registry(Path(v8_log_dir))
    with tempfile.TemporaryDirectory() as scratch:
        ext = Path(scratch) / Path(v8.WITNESS_PATH).name
        ext.write_bytes(content)
        with log8.locked():
            records8 = v8.V8._chain(log8)
            check8 = log8.verify_chain(external_anchors=ext)
            heads8 = v1._line_sha256(log8)
    if not check8["ok"]:
        raise PermissionError(f"off-host anchor log does not witness the v8 registry: {check8['detail']}")
    if len(records8) != v8.STOP_RECORDS or heads8[-1] != last_anchor["head_sha256"]:
        raise PermissionError("v8 local STOP differs from its witnessed terminal STOP")
    v8._check_stop_record(records8[-1])
    if records8[-1].get("status") != v8.STOP_SUPERSEDED_STATUS:
        raise PermissionError(f"v8 terminal STOP status must be {v8.STOP_SUPERSEDED_STATUS}")
    return {
        "version": v8.VERSION,
        "head_sha256": heads8[-1],
        "records": v8.STOP_RECORDS,
        "status": records8[-1]["status"],
        "witness_path": v8.WITNESS_PATH,
        "witness_tip": witness.tip,
    }


def _check_stop_record(record: Mapping[str, Any]) -> None:
    """Accept exactly one STOP kind: STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED."""
    if record.get("status") != STOP_STATUS:
        raise PermissionError(f"sectors-v6 terminal record accepts only {STOP_STATUS}")
    if any(k.startswith(("v9_", "sectors_v7_")) for k in record):
        raise PermissionError("sectors-v6 STOP must not contain a v9 or sectors-v7 hash")
    required = _stop_required()
    allowed = frozenset(required) | {"run_at", "decision_ref", "prev_sha256", "v8_terminal"}
    if set(record) != allowed:
        raise PermissionError("sectors-v6 terminal STOP record has missing or extra fields")
    if any(record.get(key) != value for key, value in required.items()):
        raise PermissionError("sectors-v6 terminal record is not the exact superseded STOP status")
    if not str(record.get("decision_ref") or "").strip():
        raise PermissionError("sectors-v6 STOP needs a decision reference")
    if not v1._is_hex64(record.get("prev_sha256")) or (
        REGISTERED_RECORD_SHA256 is not None and record.get("prev_sha256") != REGISTERED_RECORD_SHA256[1]
    ):
        raise PermissionError("sectors-v6 STOP needs the exact sectors-v6 registration head as prev_sha256")
    try:
        ts = datetime.fromisoformat(str(record.get("run_at") or ""))
    except ValueError as exc:
        raise PermissionError("sectors-v6 STOP needs a valid UTC run_at") from exc
    if ts.tzinfo is None or ts.utcoffset() != timedelta(0):
        raise PermissionError("sectors-v6 STOP needs a UTC run_at")
    v8_term = record.get("v8_terminal")
    if (
        not isinstance(v8_term, Mapping)
        or set(v8_term) != _V8_TERMINAL_KEYS
        or v8_term.get("version") != v8.VERSION
        or not v1._is_hex64(v8_term.get("head_sha256"))
        or v8_term.get("records") != v8.STOP_RECORDS
        or v8_term.get("status") != v8.STOP_SUPERSEDED_STATUS
        or v8_term.get("witness_path") != v8.WITNESS_PATH
        or not _is_hex40(v8_term.get("witness_tip"))
    ):
        raise PermissionError("sectors-v6 STOP v8_terminal proof is missing or malformed")


def append_superseded_stop(
    log_dir: Path,
    now: datetime,
    *,
    v8_log_dir: Path,
    decision_ref: str,
    expected_prev_sha256: str,
    witness: v2.OffhostWitness,
    dry_run: bool = True,
) -> dict:
    """Append sectors-v6's one STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED record (dry-run by default).

    Refuses unless v8 is at its witnessed 3-record STOP_SUPERSEDED_BY_V9_UNOPENED, vs1-v9 is not
    yet registered on vault main, and both local and off-host sectors-v6 chains sit at exactly
    their two-record registration baseline. A second execution refuses.
    """
    _bound()
    if REGISTERED_RECORD_SHA256 is None or REGISTERED_ANCHOR_LINE is None:
        raise PermissionError("sectors-v6 is not registered yet (no pinned registration in code)")
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("STOP needs a UTC time")
    if not str(decision_ref or "").strip():
        raise ValueError("STOP needs a non-empty decision reference")
    if expected_prev_sha256 != REGISTERED_RECORD_SHA256[1]:
        raise ValueError("STOP needs the exact sectors-v6 registration head")
    if not isinstance(witness, v2.OffhostWitness) or witness.path != WITNESS_PATH:
        raise PermissionError("sectors-v6 requires its pinned off-host witness (sectors_v6.check_offhost)")
    _check_stop_census(witness)
    v8_proof = _verify_v8_terminal_at_tip(v8_log_dir, witness)
    proof = require_witness(log_dir, witness, 2)
    if proof["witnessed_records"] != 2:
        raise PermissionError("sectors-v6's canonical witness is not at the two-record baseline")
    log = registry(log_dir)
    with log.locked():
        records = _chain(log)
        if len(records) != 2:
            raise PermissionError("sectors-v6 registry differs from its two-record baseline; reconcile before STOP")
        if v1._record_sha256(records[-1]) != expected_prev_sha256:
            raise PermissionError("sectors-v6 prior head differs from the expected head")
        candidate = {
            **_stop_required(),
            "run_at": now.isoformat(),
            "decision_ref": str(decision_ref).strip(),
            "prev_sha256": v1._record_sha256(records[-1]),
            "v8_terminal": v8_proof,
        }
        _check_stop_record(candidate)
        if dry_run:
            return {
                "dry_run": True,
                "would_append": candidate,
                "would_be_head_sha256": v1._record_sha256(candidate),
            }
        return log.append_locked([candidate])[0]


def verify_terminal_stop(log_dir: Path, witness: v2.OffhostWitness) -> dict:
    """Verify sectors-v6's exact 3-record STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED after witnessing."""
    _bound()
    proof = require_witness(log_dir, witness, STOP_RECORDS)
    log = registry(log_dir)
    with log.locked():
        records = _chain(log)
    if len(records) != STOP_RECORDS or proof["witnessed_records"] != STOP_RECORDS:
        raise PermissionError("sectors-v6 STOP witness must end at exactly three records")
    _check_stop_record(records[-1])
    if (witness.census.get("records") or {}).get(REGISTRY_ID) != STOP_RECORDS:
        raise PermissionError("sectors-v6 census does not cover exactly the STOP record")
    # Confirm v8's witnessed STOP anchor at witness.tip agrees with the carried v8_terminal proof.
    v8_term = records[-1]["v8_terminal"]
    if not witness.descends_from(v8_term["witness_tip"]):
        raise PermissionError("sectors-v6 witness does not descend from the carried v8_terminal witness_tip")
    v8_content = v1._git(witness.repo, "show", f"{witness.tip}:{v8.WITNESS_PATH}", binary=True)
    v8_lines = v8_content.splitlines()
    v8_last = json.loads(v8_lines[1]) if len(v8_lines) == 2 else {}
    if (
        len(v8_lines) != 2
        or v8_last.get("records") != v8.STOP_RECORDS
        or v8_last.get("head_sha256") != v8_term["head_sha256"]
    ):
        raise PermissionError("v8 STOP anchor at witness tip disagrees with sectors-v6 v8_terminal proof")
    return {
        "records": STOP_RECORDS,
        "head_sha256": v1._record_sha256(records[-1]),
        "status": records[-1]["status"],
        "witness_path": WITNESS_PATH,
        "witness_tip": witness.tip,
        "v8_terminal": dict(v8_term),
    }
