"""VS1 v7: v6 rules with a preregistered 2011-10 discovery start.

Registration and every opening require the exact witnessed v6 STOP. Price
admission, C1, TwelveData, four trials and the fixed holdout inherit v6.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v6 as v6
from analysis.research_forward_log import ForwardLog, canonical

VERSION = "vs1-v7"
REGISTRY_ID = VERSION
PREREG_PATH = Path("docs/paper_log/vs1-insider-density-v7-preregistration.md")
PREREG_BODY_SHA256 = "9b31d57d97d8472154b6cfe5cb243c8168f2284d4531e14fffa0bef6f5199dec"
V6_STOP_HEAD_SHA256 = "b9d9ab5a3eb82df3d7cd3e5be177cc058b28cb92ea86b7ab124673a43309d284"
V6_STOP_RECORDS = 3
DISCOVERY_START = "2011-10-01T00:00:00+00:00"
DESIGN: Mapping[str, Any] = {
    "selected": "first-of-month discovery start 2011-10-01, option (b)",
    "selection_grid": "latest passing first-of-month start tested, descending from 2011-12",
    "primary_trial": v6.PRIMARY_TRIAL,
    "candidate_power": {"v6_2012-01": 0.480, "2011-12": 0.425,
                        "2011-11": 0.480, "2011-10": 0.535},
    "basis": "feature-only synthetic preprice; current 202 admitted issuers, not earlier price admitted",
    "settings": {"target_ic": 0.01, "simulations": v1.POWER_SIMS,
                 "sign_flips": v1.POWER_PERMS, "seed": v1.SEED},
}

REGISTRY_LOG = "granular_panel_prereg_v7.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_v7.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_v7.lock"
WITNESS_PATH = v1.canonical_witness_path(REGISTRY_ID)
REGISTERED_RECORD_SHA256: tuple[str, str] | None = None
REGISTERED_ANCHOR_LINE: bytes | None = None
SUPERSEDED_BY: Mapping[str, Any] | None = None

FROZEN_REGISTRIES = (
    "vs1-v1", "vs1-v2", "vs1-v3", "vs1-v4", "vs1-v5",
    "sectors-v2", "sectors-v3", "sectors-v4",
)
BASELINE_REGISTRIES = frozenset((*FROZEN_REGISTRIES, v6.VERSION))
ALLOWED_REGISTRIES = frozenset((*BASELINE_REGISTRIES, REGISTRY_ID, "sectors-v5"))


def _bound() -> tuple[str, str, Mapping[str, Any]]:
    if not (v1._is_hex64(PREREG_BODY_SHA256) and v1._is_hex64(V6_STOP_HEAD_SHA256)
            and DESIGN.get("selected") and DESIGN.get("candidate_power")):
        raise PermissionError("v7 design, body hash and witnessed v6 STOP head are not bound")
    return PREREG_BODY_SHA256, V6_STOP_HEAD_SHA256, DESIGN


def check_prereg(repo_root: Path = v2.REPO) -> str:
    body, _, _ = _bound()
    actual = v1.prereg_body_sha256(Path(repo_root) / PREREG_PATH)
    if actual != body:
        raise PermissionError("v7 preregistration body differs from its pin")
    return actual


def _v6_anchor_at_tip(repo: Path, tip: str, expected_head: str) -> None:
    """Match the exact STOP anchor on the same vault tip as the v7 witness."""
    content = v1._git(repo, "show", f"{tip}:{v6.WITNESS_PATH}", binary=True)
    if b"\r" in content or not content.endswith(b"\n"):
        raise PermissionError("v6 witness must have canonical LF line endings")
    lines = content.splitlines()
    if len(lines) != 2 or lines[0] != v6.REGISTERED_ANCHOR_LINE:
        raise PermissionError("v6 witness is not an exact two-anchor STOP witness")
    try:
        last = json.loads(lines[1])
    except ValueError as exc:
        raise PermissionError("v6 STOP anchor is not JSON") from exc
    if lines[1] != canonical(last) or last.get("records") != V6_STOP_RECORDS \
            or last.get("head_sha256") != expected_head \
            or last.get("prev_anchor_sha256") != hashlib.sha256(lines[0]).hexdigest():
        raise PermissionError("v6 STOP anchor differs from the verified terminal head")


def check_census(census: Mapping[str, Any], *, stop_head_sha256: str | None = None,
                 witness_repo: Path | None = None, opening: bool = False) -> None:
    """Accept only the witnessed v6 STOP and two-record older registries.

    The STOP head is the result of verifying v6's local chain against the off-host
    witness. Passing just a count or an arbitrary third record is insufficient.
    """
    _, pinned_stop_head, _ = _bound()
    if stop_head_sha256 != pinned_stop_head or witness_repo is None or not census.get("tip"):
        raise PermissionError("v7 needs the pinned, verified v6 STOP head at the witness tip")
    if census.get("unknown"):
        raise PermissionError(f"unknown VS1 witnesses: {census['unknown']}")
    files, counts = census.get("files") or {}, census.get("records") or {}
    present = set(files)
    expected = BASELINE_REGISTRIES | ({REGISTRY_ID} if opening else set())
    if opening and "sectors-v5" in present:
        expected = expected | {"sectors-v5"}
    if present != expected or set(counts) != expected:
        raise PermissionError("v7 witness census has missing or extra registries")
    for key, path in files.items():
        if path != v1.canonical_witness_path(key):
            raise PermissionError(f"{key} is not at its canonical witness path")
    if any(counts[key] != 2 for key in FROZEN_REGISTRIES):
        raise PermissionError("an older Technology or sectors registry grew past two records")
    if counts[v6.VERSION] != V6_STOP_RECORDS:
        raise PermissionError("v6 witness must cover exactly its three-record STOP")
    if opening and (not isinstance(counts[REGISTRY_ID], int) or counts[REGISTRY_ID] < 2):
        raise PermissionError("v7 witness does not cover its registration")
    if opening and "sectors-v5" in counts and counts["sectors-v5"] != 2:
        raise PermissionError("sectors-v5 witness grew past its registration")
    _v6_anchor_at_tip(Path(witness_repo), census["tip"], pinned_stop_head)


def contamination(censuses: list[Mapping[str, Any] | None]) -> dict:
    """A known v6 STOP is clean; every other non-v7 growth or unknown is not."""
    found: dict[str, Any] = {}
    unknown: set[str] = set()
    for census in censuses:
        if not census:
            continue
        unknown.update(census.get("unknown") or ())
        counts = census.get("records") or {}
        for key in BASELINE_REGISTRIES | {"sectors-v5"}:
            if key == "sectors-v5" and key not in counts:
                continue
            expected = V6_STOP_RECORDS if key == v6.VERSION else 2
            if counts.get(key) != expected:
                found[key] = counts.get(key)
        for key in set(counts) - ALLOWED_REGISTRIES:
            found[key] = counts[key]
    return {"contaminated": bool(found or unknown),
            "detail": {"other_registries_outside_baseline": found,
                       "unknown_witness_files": sorted(unknown)}}


def registration_records(now: datetime, code_sha: str, prereg_sha256: str | None = None) -> list[dict]:
    """New v7 chain, cross-linked to the verified terminal v6 STOP."""
    body, stop_head, design = _bound()
    if prereg_sha256 not in (None, body):
        raise PermissionError("registration body differs from the pinned v7 body")
    if now.tzinfo is None or not isinstance(code_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", code_sha):
        raise ValueError("registration needs a timezone and reviewed code SHA")
    check_prereg()
    common = {"run_at": now.isoformat(), "code_sha": code_sha,
              "prereg_path": PREREG_PATH.as_posix(), "prereg_sha256": body,
              "promotion_allowed": False}
    header = {"kind": "header", "version": VERSION, **common}
    prereg = {
        "kind": "preregistration", "version": VERSION, **common,
        "parent_registry_id": v6.VERSION,
        "parent_prereg_sha256": v6.PREREG_BODY_SHA256,
        "parent_terminal_head_sha256": stop_head,
        "parent_terminal_records": V6_STOP_RECORDS,
        "parent_witness_path": v6.WITNESS_PATH,
        "prior_registrations": [
            {"version": e.version, "prereg_sha256": e.prereg_sha256,
             "registry_head_sha256": e.registry_head_sha256}
            for e in (v2.V1_EARLIER, v2.V2_EARLIER, v6.V3_EARLIER,
                      v6.V4_EARLIER, v6.V5_EARLIER)
        ],
        "design": dict(design), "primary_trial": design.get("primary_trial"),
        "discovery": {"start": DISCOVERY_START, "end": v1.SPLIT},
        "holdout": {"start": v1.SPLIT, "end": v1.END},
        "run_k": v1.VS1_RUN_K, "run_alpha": v1.run_alpha(v1.VS1_RUN_K),
        "power_gate": {"target_ic": v1.POWER_GATE_IC, "threshold": v1.POWER_GATE},
    }
    return [header, prereg]


# All data, price and statistical rules are the exact v6 implementations. The
# scoped window in the CLI changes only the first discovery session.
PRICE_SOURCE = v6.PRICE_SOURCE
PRICE_SOURCE_ID = v6.PRICE_SOURCE_ID
SERIES_TEMPLATE = v6.SERIES_TEMPLATE
BASIS = v6.BASIS
BENCHMARK = v6.BENCHMARK
PriceManifest = v6.PriceManifest
PRIMARY_TRIAL = v6.PRIMARY_TRIAL
SECONDARY_TRIALS = v6.SECONDARY_TRIALS
POST_ADMISSION_POWER = True
load_issuer_map = v6.load_issuer_map
load_sic_map = v6.load_sic_map
v2_universe = v6.v2_universe
load_inputs = v6.load_inputs
power_features = v6.power_features
admission_report = v6.admission_report
build_trial_panels = v6.build_trial_panels
write_frozen = v6.write_frozen
WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = v1.WITNESS_BRANCH
WITNESS_REF = "refs/vs1-v7-witness/main"


class V7Harness(v2.Harness):
    def register(self, log_dir: Path, now: datetime, code_sha: str, *,
                 v6_log_dir: Path | None = None, v6_witness=None, **kwargs) -> list[dict]:
        if v6_log_dir is None or v6_witness is None:
            raise PermissionError("v7 registration requires the exact witnessed v6 STOP")
        stop = v6.verify_terminal_stop(v6_log_dir, v6_witness)
        check_census(v6_witness.census, stop_head_sha256=stop["head_sha256"],
                     witness_repo=v6_witness.repo)
        return super().register(log_dir, now, code_sha, **kwargs)

    def check_offhost(self, vault_repo: Path, *, remote_url: str | None = None):
        witness = super().check_offhost(vault_repo, remote_url=remote_url)
        check_census(witness.census, stop_head_sha256=V6_STOP_HEAD_SHA256,
                     witness_repo=witness.repo, opening=True)
        return witness

    def require_supersession(self, witness) -> dict:
        v1.refuse_superseded(7, self.superseded_by, witness)
        if not isinstance(witness, v2.OffhostWitness) or witness.path != WITNESS_PATH:
            raise PermissionError("v7 requires its pinned off-host witness")
        check_census(witness.census, stop_head_sha256=V6_STOP_HEAD_SHA256,
                     witness_repo=witness.repo, opening=True)
        return {"earlier_witnessed_records": dict(witness.earlier),
                "witness_tip": witness.tip, "vs1_witness_census": witness.census}

    def run_contamination(self, key) -> dict:
        log = self.registry(key.log_dir)
        with log.locked():
            records = self._chain(log)
        censuses = [r.get("vs1_witness_census") or (r.get("supersession") or {}).get("vs1_witness_census")
                    for r in records]
        return contamination([*censuses, key.census])

    def freeze_inputs(self, log_dir: Path, now: datetime, inputs: Mapping[str, Any]) -> dict:
        if inputs.get("accept_underpowered") is not False:
            raise PermissionError("v7 must STOP below the raw 0.50 post-admission power gate")
        return super().freeze_inputs(log_dir, now, inputs)


V6_EARLIER = v2.EarlierVersion(
    version=v6.VERSION, number=6, prereg_sha256=v6.PREREG_BODY_SHA256,
    registry_head_sha256=v6.REGISTERED_RECORD_SHA256[1], witness_path=v6.WITNESS_PATH,
    anchor_line=v6.REGISTERED_ANCHOR_LINE,
)
V7 = V7Harness(v2.Pins(
    version=VERSION, number=7, prereg_path=PREREG_PATH,
    prereg_body_sha256=PREREG_BODY_SHA256, primary_trial=PRIMARY_TRIAL,
    registry_log=REGISTRY_LOG, registry_anchors=REGISTRY_ANCHORS,
    registry_lock=REGISTRY_LOCK, witness_path=WITNESS_PATH, witness_ref=WITNESS_REF,
    registration_records=registration_records, registered_at=None, registered_code_sha=None,
    registered_record_sha256=REGISTERED_RECORD_SHA256,
    registered_anchor_line=REGISTERED_ANCHOR_LINE,
    earlier=(v2.V1_EARLIER, v2.V2_EARLIER, v6.V3_EARLIER,
             v6.V4_EARLIER, v6.V5_EARLIER, V6_EARLIER),
    superseded_by=SUPERSEDED_BY, holdout_probe_required=True,
), module=sys.modules[__name__])

stage0_power = V7.stage0_power
verify_power = V7.verify_power
discover_panel = V7.discover_panel
check_holdout_request = V7.check_holdout_request
evaluate_panel_holdout = V7.evaluate_panel_holdout
load_price_panel = V7.load_price_panel
registry = V7.registry
register = V7.register
check_offhost = V7.check_offhost
require_supersession = V7.require_supersession
require_witness = V7.require_witness
export_anchors = V7.export_anchors
record_prices_read = V7.record_prices_read
freeze_inputs = V7.freeze_inputs
latest_frozen_inputs = V7.latest_frozen_inputs
open_discovery = V7.open_discovery
resume_discovery = V7.resume_discovery
seal_discovery = V7.seal_discovery
open_holdout = V7.open_holdout
resume_holdout = V7.resume_holdout
seal_holdout = V7.seal_holdout


def run_spec(run_id: str | None = None):
    return V7.run_spec(run_id)


def prereg_body_sha256(path: Path) -> str:
    return v1.prereg_body_sha256(path)
