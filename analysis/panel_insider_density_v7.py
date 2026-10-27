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
REGISTERED_RECORD_SHA256: tuple[str, str] | None = (
    "a04654d988f4705fb0620b2474684197bf1a170957f93c8dfcae32315aff7e48",  # header
    "4b42f649ce2a69191de5b73d14aebdd7bbe470b8ae65cb98a099201484c40e53",  # preregistration
)
REGISTERED_ANCHOR_LINE: bytes | None = (
    b'{"head_sha256":"4b42f649ce2a69191de5b73d14aebdd7bbe470b8ae65cb98a099201484c40e53",'
    b'"prev_anchor_sha256":null,"records":2,"run_at":"2026-09-30T01:43:00+00:00"}'
)
# Set by the terminal STOP change (v7 superseded unopened before Stage-0, see below): v7 can
# never open a discovery or holdout; the witnessed STOP record names the successor.
SUPERSEDED_BY: Mapping[str, Any] | None = {"version": "vs1-v8"}

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

    def _refuse_if_stopped(self, log_dir: Path) -> None:
        # Checked before the base method re-takes the lock. The pinned SUPERSEDED_BY refuses every
        # opening and (below) every freeze, so even a stale two-record copy cannot freeze.
        log = self.registry(log_dir)
        with log.locked():
            records = self._chain(log)
        if v1._kind(records, "status"):
            raise PermissionError("v7 carries a terminal STOP status record: no freeze or opening")

    def freeze_inputs(self, log_dir: Path, now: datetime, inputs: Mapping[str, Any]) -> dict:
        if inputs.get("accept_underpowered") is not False:
            raise PermissionError("v7 must STOP below the raw 0.50 post-admission power gate")
        v1.refuse_superseded(7, self.superseded_by)
        self._refuse_if_stopped(log_dir)
        return super().freeze_inputs(log_dir, now, inputs)

    def open_discovery(self, log_dir: Path, now: datetime, observed: Mapping[str, Any], witness) -> dict:
        self._refuse_if_stopped(log_dir)
        return super().open_discovery(log_dir, now, observed, witness)

    def open_holdout(self, frozen: dict, *, log_dir: Path, **kwargs) -> dict:
        self._refuse_if_stopped(log_dir)
        return super().open_holdout(frozen, log_dir=log_dir, **kwargs)


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


# --- terminal STOP: superseded unopened before its Stage-0 (mirrors v6's witnessed STOP) --------
#
# The owner's standing authorization (2026-09-30) is to finish VS1 at a solid design without
# opening outcomes. The E0 machinery-calibration benchmark (synthetic outcomes on the v7
# feature geometry) gives 0.496 under its Gaussian model (statistically consistent with v7's
# registered 0.535, z -0.93, but on the failing side) and 0.446 / 0.414 under its realistic
# factor + GARCH + t-tail (+ style tilt) models, below the 0.50 gate. v7 is therefore stopped,
# by a discretionary decision before its post-admission Stage-0 (this is not the body section 4
# step-3 gate), and superseded by a better-powered v8. No v7 Stage-0, freeze, discovery or
# holdout record exists; the STOP is bound to the exact E0 scorecard bytes and the v7 cross-check
# powers pinned below. (The scorecard also carries E0's pre-2011 non-Technology real-price
# replication block; the STOP reads only its synthetic ``v7_crosscheck``.)

STOP_STATUS = "STOP_SUPERSEDED_BY_V8_UNOPENED"
STOP_SUCCESSOR = "vs1-v8"
STOP_RECORDS = 3
#: SHA-256 of the E0 v1 full-profile scorecard (PR #763 head c724ce53, manifest 75489d50...).
E0_SCORECARD_SHA256 = "4b47f918433524d4ab36b93e6472ee557e6617bbfe7567e5c182d4834a7cd62a"
E0_MANIFEST_SHA256 = "75489d5091d82af64312f4523c41bdb52b0951a11e017644a022baa08a172822"
E0_CODE_REF = "3pacs/GRID PR #763 evals/e0 at c724ce536a0580117e8281286c7a7b3bfd80216e"
#: The scorecard's v7 cross-check powers (raw threshold 0.0125, IC 0.01, 500 synthetic worlds each).
E0_V7_POWERS: Mapping[str, float] = {"gaussian_idio": 0.496, "factor_t_garch": 0.446,
                                     "factor_t_garch_exposed": 0.414}
STOP_REASON = ("E0 synthetic-outcome power for the registered v7 design (A90|fwd5, IC 0.01, "
               "threshold 0.0125) is below the 0.50 gate under the realistic outcome models; "
               "discretionary supersession before Stage-0, no VS1 outcome read")
STOP_BASIS = ("discretionary pre-Stage-0 supersession on synthetic E0 evidence under the owner's standing "
              "authorization; the body section 4 step-3 Stage-0 gate was never evaluated")
_STOP_REQUIRED: Mapping[str, Any] = {
    "kind": "status", "version": VERSION, "status": STOP_STATUS,
    "stage0_run": False, "prereg_gate_evaluated": False, "basis": STOP_BASIS,
    "inputs_frozen": False, "discovery_opened": False, "holdout_opened": False,
    "superseded_by": STOP_SUCCESSOR, "prereg_sha256": PREREG_BODY_SHA256,
    "reason": STOP_REASON, "e0_scorecard_sha256": E0_SCORECARD_SHA256,
    "e0_manifest_sha256": E0_MANIFEST_SHA256, "e0_code_ref": E0_CODE_REF,
    "power_gate": {"target_ic": v1.POWER_GATE_IC, "threshold": v1.POWER_GATE},
    "promotion_allowed": False,
}
_STOP_FIELDS = frozenset(_STOP_REQUIRED) | {"run_at", "e0_v7_power_ic_0_01", "decision_ref", "prev_sha256"}
_E0_SCENARIOS = ("gaussian_idio", "factor_t_garch", "factor_t_garch_exposed")


def e0_v7_power(scorecard_bytes: bytes) -> dict[str, float]:
    """The pinned E0 scorecard's v7 cross-check powers (synthetic outcomes only)."""
    if hashlib.sha256(scorecard_bytes).hexdigest() != E0_SCORECARD_SHA256:
        raise PermissionError("E0 scorecard differs from the pinned evidence")
    card = json.loads(scorecard_bytes)
    cross = card.get("v7_crosscheck") or {}
    if card.get("version") != "e0-v1" or cross.get("synthetic_outcomes_only") is not True \
            or cross.get("target_ic") != v1.POWER_GATE_IC or cross.get("gate") != v1.POWER_GATE:
        raise PermissionError("E0 scorecard is not the v1 synthetic v7 cross-check")
    rows = cross.get("by_scenario") or {}
    return {name: float(rows[name]["e0_power_raw_threshold"]) for name in _E0_SCENARIOS}


def _check_stop_record(record: Mapping[str, Any]) -> None:
    if any(record.get(key) != value for key, value in _STOP_REQUIRED.items()):
        raise PermissionError("v7 terminal record is not the exact STOP status")
    if set(record) != _STOP_FIELDS:
        raise PermissionError("v7 terminal STOP record has missing or extra fields")
    powers = record.get("e0_v7_power_ic_0_01")
    if not isinstance(powers, Mapping) or set(powers) != set(_E0_SCENARIOS) \
            or not all(isinstance(p, float) and 0.0 <= p <= 1.0 for p in powers.values()) \
            or dict(powers) != dict(E0_V7_POWERS) or not powers["factor_t_garch_exposed"] < v1.POWER_GATE:
        raise PermissionError("v7 STOP needs the E0 realistic-model power below the gate")
    if not str(record.get("decision_ref") or "").strip():
        raise PermissionError("v7 STOP needs a decision reference")


def append_stop_status(log_dir: Path, now: datetime, *, e0_scorecard: Path, decision_ref: str,
                       expected_prev_sha256: str, witness, dry_run: bool = False) -> dict:
    """Append v7's one STOP record after checking the exact two-record witnessed chain.

    Refuses unless the local and off-host chains are exactly the two registration
    records (so no freeze or opening was ever recorded; a probe or Stage-0 writes no
    registry record, so their absence rests on the operator log) and the E0
    scorecard hashes to the pinned evidence with the pinned cross-check powers. ``dry_run=True`` returns the
    exact would-be record and head. Execute once after a backup and a dry run with
    the same ``now``; a second execution refuses. Publishing the anchor is a
    separate step, followed by :func:`verify_terminal_stop`.
    """
    from datetime import timedelta

    if now.tzinfo is None or now.utcoffset() != timedelta(0) or not str(decision_ref).strip() \
            or expected_prev_sha256 != REGISTERED_RECORD_SHA256[1]:
        raise ValueError("STOP needs a UTC time, a decision reference and the exact v7 registration head")
    powers = e0_v7_power(Path(e0_scorecard).read_bytes())
    if powers != dict(E0_V7_POWERS):
        raise PermissionError("E0 scorecard cross-check differs from the pinned powers")
    V7.require_witness(log_dir, witness, 2)
    if (witness.census.get("records") or {}).get(REGISTRY_ID) != 2:
        raise PermissionError("v7's canonical witness is not at the two-record baseline")
    log = V7.registry(log_dir)
    with log.locked():
        records = V7._chain(log)
        if len(records) != 2:
            raise PermissionError("v7 registry differs from its two-record baseline; reconcile before STOP")
        if v1._record_sha256(records[-1]) != expected_prev_sha256:
            raise PermissionError("v7 prior head differs from the expected head")
        candidate = {
            **_STOP_REQUIRED, "power_gate": dict(_STOP_REQUIRED["power_gate"]),
            "run_at": now.isoformat(), "e0_v7_power_ic_0_01": powers,
            "decision_ref": str(decision_ref).strip(), "prev_sha256": v1._record_sha256(records[-1]),
        }
        _check_stop_record(candidate)
        if dry_run:
            return {"dry_run": True, "would_append": candidate,
                    "would_be_head_sha256": v1._record_sha256(candidate)}
        return log.append_locked([candidate])[0]


def verify_terminal_stop(log_dir: Path, witness) -> dict:
    """Return the only acceptable v7 terminal head after exact off-host witnessing."""
    log = V7.registry(log_dir)
    proof = V7.require_witness(log_dir, witness, STOP_RECORDS)
    records = V7._chain(log)
    if len(records) != STOP_RECORDS or proof["witnessed_records"] != STOP_RECORDS:
        raise PermissionError("v7 STOP witness must end at exactly three records")
    _check_stop_record(records[-1])
    if (witness.census.get("records") or {}).get(REGISTRY_ID) != STOP_RECORDS:
        raise PermissionError("v7 census does not cover exactly the STOP record")
    return {"records": STOP_RECORDS, "head_sha256": v1._record_sha256(records[-1]),
            "witness_path": WITNESS_PATH, "witness_tip": witness.tip}


def run_spec(run_id: str | None = None):
    return V7.run_spec(run_id)


def prereg_body_sha256(path: Path) -> str:
    return v1.prereg_body_sha256(path)
