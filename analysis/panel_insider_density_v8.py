"""VS1 v8: v7 rules with a 2008-01 discovery start, one confirmatory test and a two-model Stage-0.

Registration and every opening require the exact witnessed v7 STOP. Price admission, C1,
TwelveData, features and the fixed holdout inherit v6/v7. The confirmatory test is the
one-sided (pre-registered positive) block sign-flip on ``A90|fwd5`` at 0.05; the other three
trials are exploratory. Stage-0 gates on BOTH the v1 Gaussian planted-IC simulator and the
E0 v1 ``factor_t_garch_exposed`` synthetic-outcome model. Nothing here reads a price.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from analysis.offline_research_proof import autocorrelation_block, digest
from analysis.research_forward_log import canonical

VERSION = "vs1-v8"
REGISTRY_ID = VERSION
PREREG_PATH = Path("docs/paper_log/vs1-insider-density-v8-preregistration.md")
PREREG_BODY_SHA256 = "506c11657f033c252473ce52d3e67cfc09f500544c4ff68f11955477d0f88c14"
#: The witnessed v7 terminal STOP head (3 records; vault main 4456453d, anchor file SHA-256 bd018b11...).
V7_STOP_HEAD_SHA256: str | None = "5d8d7c9c2fc5c943fadc083c347e766586c6137352f609424e60d1e89c0440b4"
#: The exact second line of the witnessed v7 anchor file.
V7_STOP_ANCHOR_LINE = (
    b'{"head_sha256":"5d8d7c9c2fc5c943fadc083c347e766586c6137352f609424e60d1e89c0440b4",'
    b'"prev_anchor_sha256":"1648bfbeebd263d1489a11bf667c9f4656043806bebfe35c17f3084f131dd5a1",'
    b'"records":3,"run_at":"2026-10-01T00:38:30+00:00"}'
)
V7_STOP_RECORDS = v7.STOP_RECORDS
DISCOVERY_START = "2008-01-01T00:00:00+00:00"
PROBE_START = "2007-11-02"

#: The confirmatory rule (v8 body §1.2).
CONFIRMATORY_ALPHA_ONE_SIDED = 0.05
#: Stage-0 (v8 body §1.3).
GAUSSIAN_SIMS = v1.POWER_SIMS
E0_SIMS = 500
E0_GATED_SCENARIO = "factor_t_garch_exposed"
E0_REPORTED_SCENARIOS = ("factor_t_garch",)
E0_MANIFEST_SHA256 = v7.E0_MANIFEST_SHA256
E0_TRIAL_ORDER = ("A90|fwd5", "A30|fwd5", "A90|fwd20", "A30|fwd20")
DESIGN_TARGET_POWER = 0.80
DESIGN: Mapping[str, Any] = {
    "selected": "discovery start 2008-01-01; one-sided confirmatory A90|fwd5 at 0.05; two-model Stage-0",
    "primary_trial": v6.PRIMARY_TRIAL,
    "screen_one_sided_0_05": {  # E0 v1 generator, 202 fixed issuers, 500 sims, IC 0.01
        "2011-10-01": {"gaussian": 0.818, "factor": 0.780, "tilted": 0.730, "dates": 414},
        "2009-01-01": {"gaussian": 0.866, "factor": 0.858, "tilted": 0.800, "dates": 552},
        "2008-01-01": {"gaussian": 0.886, "factor": 0.850, "tilted": 0.850, "dates": 603},
        "2007-01-01": {"gaussian": 0.884, "factor": 0.896, "tilted": 0.844, "dates": 653},
        "2006-04-01": {"gaussian": 0.902, "factor": 0.902, "tilted": 0.866, "dates": 690},
    },
    "admission_stress_2008": {"drop_15pct": {"gaussian": 0.832, "tilted": 0.820},
                              "drop_30pct": {"gaussian": 0.756, "tilted": 0.754}},
    "basis": "feature-only synthetic outcomes; v6 admitted 202 issuers held fixed, not v8 price admitted",
}
STAGE0_SETTINGS: Mapping[str, Any] = {
    "target_ic": v1.POWER_GATE_IC, "gate": v1.POWER_GATE, "design_target": DESIGN_TARGET_POWER,
    "alpha_one_sided": CONFIRMATORY_ALPHA_ONE_SIDED, "direction": v1.PRIMARY_DIRECTION,
    "gaussian": {"model": "v1 planted rank IC, Gaussian idiosyncratic", "sims": GAUSSIAN_SIMS,
                 "perms": v1.POWER_PERMS, "seed": v1.SEED},
    "e0": {"gated": E0_GATED_SCENARIO, "reported": list(E0_REPORTED_SCENARIOS), "sims": E0_SIMS,
           "perms": v1.POWER_PERMS, "manifest_sha256": E0_MANIFEST_SHA256},
}

REGISTRY_LOG = "granular_panel_prereg_v8.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_v8.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_v8.lock"
WITNESS_PATH = v1.canonical_witness_path(REGISTRY_ID)
REGISTERED_RECORD_SHA256: tuple[str, str] | None = None
REGISTERED_ANCHOR_LINE: bytes | None = None
SUPERSEDED_BY: Mapping[str, Any] | None = None

FROZEN_REGISTRIES = v7.FROZEN_REGISTRIES
TERMINAL_REGISTRIES = {v6.VERSION: v7.V6_STOP_RECORDS, v7.VERSION: V7_STOP_RECORDS}
BASELINE_REGISTRIES = frozenset((*FROZEN_REGISTRIES, *TERMINAL_REGISTRIES))
#: The ten-sector generalization registration bound to v8 (body section 0). Optional: absent, or at
#: exactly its two registration records; it may not open before v8 is terminal.
SECTORS_V6 = "sectors-v6"
SECTORS_V6_RECORDS = 2
ALLOWED_REGISTRIES = frozenset((*BASELINE_REGISTRIES, REGISTRY_ID, SECTORS_V6))


def _sectors_v6_anchor_at_tip(repo: Path, tip: str) -> None:
    """Once sectors-v6's registration is pinned in its own code, its witness must be exactly that anchor.

    Before that pin exists the census rule (canonical path, exactly two records) is the whole check;
    sectors-v6's own registration verifies that its header names the witnessed v8 head.
    """
    import importlib.util

    if importlib.util.find_spec("analysis.panel_insider_density_sectors_v6") is None:
        return
    import importlib

    s6 = importlib.import_module("analysis.panel_insider_density_sectors_v6")
    line = getattr(s6, "REGISTERED_ANCHOR_LINE", None)
    if line is None:
        return
    content = v1._git(repo, "show", f"{tip}:{v1.canonical_witness_path(SECTORS_V6)}", binary=True)
    if content != line + b"\n":
        raise PermissionError("sectors-v6 witness is not exactly its pinned two-record registration anchor")


def _bound() -> tuple[str, str]:
    if not (v1._is_hex64(PREREG_BODY_SHA256) and v1._is_hex64(V7_STOP_HEAD_SHA256)):
        raise PermissionError("v8 body hash and the witnessed v7 STOP head are not bound")
    return PREREG_BODY_SHA256, V7_STOP_HEAD_SHA256


def check_prereg(repo_root: Path = v2.REPO) -> str:
    if not v1._is_hex64(PREREG_BODY_SHA256):
        raise PermissionError("v8 body hash is not bound")
    actual = v1.prereg_body_sha256(Path(repo_root) / PREREG_PATH)
    if actual != PREREG_BODY_SHA256:
        raise PermissionError("v8 preregistration body differs from its pin")
    return actual


def _v7_anchor_at_tip(repo: Path, tip: str, expected_head: str) -> None:
    """Match the exact v7 STOP anchor on the same vault tip as the v8 witness."""
    content = v1._git(repo, "show", f"{tip}:{v7.WITNESS_PATH}", binary=True)
    if b"\r" in content or not content.endswith(b"\n"):
        raise PermissionError("v7 witness must have canonical LF line endings")
    lines = content.splitlines()
    if len(lines) != 2 or lines[0] != v7.REGISTERED_ANCHOR_LINE:
        raise PermissionError("v7 witness is not an exact two-anchor STOP witness")
    try:
        last = json.loads(lines[1])
    except ValueError as exc:
        raise PermissionError("v7 STOP anchor is not JSON") from exc
    if lines[1] != V7_STOP_ANCHOR_LINE or lines[1] != canonical(last) or last.get("records") != V7_STOP_RECORDS \
            or last.get("head_sha256") != expected_head \
            or last.get("prev_anchor_sha256") != hashlib.sha256(lines[0]).hexdigest():
        raise PermissionError("v7 STOP anchor differs from the verified terminal head")


def check_census(census: Mapping[str, Any], *, stop_head_sha256: str | None = None,
                 witness_repo: Path | None = None, opening: bool = False) -> None:
    """Accept only the witnessed v6 and v7 STOPs and two-record older registries."""
    _, pinned_stop_head = _bound()
    if stop_head_sha256 != pinned_stop_head or witness_repo is None or not census.get("tip"):
        raise PermissionError("v8 needs the pinned, verified v7 STOP head at the witness tip")
    if census.get("unknown"):
        raise PermissionError(f"unknown VS1 witnesses: {census['unknown']}")
    files, counts = census.get("files") or {}, census.get("records") or {}
    expected = BASELINE_REGISTRIES | ({REGISTRY_ID} if opening else set())
    if opening and (SECTORS_V6 in files or SECTORS_V6 in counts):
        expected = expected | {SECTORS_V6}  # optional; a sectors-v6 cannot precede v8's registration
    if set(files) != expected or set(counts) != expected:
        raise PermissionError("v8 witness census has missing or extra registries")
    for key, path in files.items():
        if path != v1.canonical_witness_path(key):
            raise PermissionError(f"{key} is not at its canonical witness path")
    if any(counts[key] != 2 for key in FROZEN_REGISTRIES):
        raise PermissionError("an older Technology or sectors registry grew past two records")
    for key, records in TERMINAL_REGISTRIES.items():
        if counts[key] != records:
            raise PermissionError(f"{key} witness must cover exactly its terminal STOP")
    if opening and (not isinstance(counts[REGISTRY_ID], int) or counts[REGISTRY_ID] < 2):
        raise PermissionError("v8 witness does not cover its registration")
    if SECTORS_V6 in counts:
        if counts[SECTORS_V6] != SECTORS_V6_RECORDS:
            raise PermissionError("sectors-v6 witness must cover exactly its two registration records")
        _sectors_v6_anchor_at_tip(Path(witness_repo), census["tip"])
    v7._v6_anchor_at_tip(Path(witness_repo), census["tip"], v7.V6_STOP_HEAD_SHA256)
    _v7_anchor_at_tip(Path(witness_repo), census["tip"], pinned_stop_head)


def contamination(censuses: list[Mapping[str, Any] | None]) -> dict:
    """The known v6/v7 STOPs are clean; every other non-v8 growth or unknown is not."""
    found: dict[str, Any] = {}
    unknown: set[str] = set()
    for census in censuses:
        if not census:
            continue
        unknown.update(census.get("unknown") or ())
        counts = census.get("records") or {}
        for key in BASELINE_REGISTRIES:
            if counts.get(key) != TERMINAL_REGISTRIES.get(key, 2):
                found[key] = counts.get(key)
        if SECTORS_V6 in counts and counts[SECTORS_V6] != SECTORS_V6_RECORDS:
            found[SECTORS_V6] = counts[SECTORS_V6]
        for key in set(counts) - ALLOWED_REGISTRIES:
            found[key] = counts[key]
    return {"contaminated": bool(found or unknown),
            "detail": {"other_registries_outside_baseline": found,
                       "unknown_witness_files": sorted(unknown)}}


def registration_records(now: datetime, code_sha: str, prereg_sha256: str | None = None) -> list[dict]:
    """New v8 chain, cross-linked to the verified terminal v7 STOP."""
    body, stop_head = _bound()
    if prereg_sha256 not in (None, body):
        raise PermissionError("registration body differs from the pinned v8 body")
    if now.tzinfo is None or not isinstance(code_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", code_sha):
        raise ValueError("registration needs a timezone and reviewed code SHA")
    check_prereg()
    common = {"run_at": now.isoformat(), "code_sha": code_sha,
              "prereg_path": PREREG_PATH.as_posix(), "prereg_sha256": body,
              "promotion_allowed": False}
    header = {"kind": "header", "version": VERSION, **common}
    prereg = {
        "kind": "preregistration", "version": VERSION, **common,
        "parent_registry_id": v7.VERSION,
        "parent_prereg_sha256": v7.PREREG_BODY_SHA256,
        "parent_registry_head_sha256": v7.REGISTERED_RECORD_SHA256[1],
        "parent_terminal_head_sha256": stop_head,
        "parent_terminal_records": V7_STOP_RECORDS,
        "parent_terminal_status": v7.STOP_STATUS,
        "parent_witness_path": v7.WITNESS_PATH,
        "prior_registrations": [
            {"version": e.version, "prereg_sha256": e.prereg_sha256,
             "registry_head_sha256": e.registry_head_sha256}
            for e in (v2.V1_EARLIER, v2.V2_EARLIER, v6.V3_EARLIER,
                      v6.V4_EARLIER, v6.V5_EARLIER, v7.V6_EARLIER)
        ],
        "design": json.loads(json.dumps(DESIGN)), "primary_trial": PRIMARY_TRIAL,
        "confirmatory": {"trial": PRIMARY_TRIAL, "direction": v1.PRIMARY_DIRECTION,
                         "alpha_one_sided": CONFIRMATORY_ALPHA_ONE_SIDED,
                         "exploratory": [t for t in v1.trial_names() if t != PRIMARY_TRIAL]},
        "discovery": {"start": DISCOVERY_START, "end": v1.SPLIT},
        "holdout": {"start": v1.SPLIT, "end": v1.END},
        "probe_window": [PROBE_START, "2019-12-31"],
        "power_gate": json.loads(json.dumps(STAGE0_SETTINGS)),
    }
    return [header, prereg]


# All data, price and statistical rules are the exact v6 implementations. The scoped window in
# the CLI changes only the first discovery session; the selection and Stage-0 are overridden below.
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
WITNESS_REF = "refs/vs1-v8-witness/main"


# --- Stage-0 (v8 body §1.3): synthetic outcomes on the admitted feature geometry only ----------

def planted_power_one_sided(feature: np.ndarray, target_ic: float, *, sims: int = GAUSSIAN_SIMS,
                            perms: int = v1.POWER_PERMS, seed: int = v1.SEED,
                            alpha: float = CONFIRMATORY_ALPHA_ONE_SIDED) -> dict:
    """v1's planted-IC Gaussian simulator (same draws), scored by the one-sided confirmatory p."""
    rng = np.random.default_rng([seed, int(round(target_ic * 1e6)), sims, 4])
    usable = []
    for row in feature:
        m = np.isfinite(row)
        if m.sum() >= v1.MIN_ENTITIES and np.ptp(row[m]) > 0:
            r = v1.rankdata(row[m])
            usable.append((r - r.mean()) / r.std())
    if len(usable) < v1.MIN_N:
        return {"model": "gaussian_v1", "target_ic": target_ic, "usable_dates": len(usable), "power": 0.0,
                "sims": 0, "alpha_one_sided": alpha}
    pilot_rho = 0.2
    scale = float(v1._planted_ics(usable, pilot_rho, 50, rng).mean()) / pilot_rho
    rho = min(0.99, target_ic / scale) if scale > 0 else 0.99
    ics = v1._planted_ics(usable, rho, sims, rng)
    block, _ = autocorrelation_block(ics[0].tolist(), 0)
    hits = 0
    for s in range(sims):
        _, _, p_one = v1.signflip_pvalues(ics[s], block, perms, seed + s, v1.PRIMARY_DIRECTION)
        hits += p_one <= alpha
    return {"model": "gaussian_v1", "target_ic": target_ic, "usable_dates": len(usable),
            "planted_rho": rho, "power": hits / sims, "realized_mean_ic": float(ics.mean()),
            "sims": sims, "perms": perms, "seed": seed, "alpha_one_sided": alpha}


def e0_power(features: Mapping[str, np.ndarray], n_sessions: int, scenario_name: str, *,
             target_ic: float = v1.POWER_GATE_IC, sims: int = E0_SIMS, perms: int = v1.POWER_PERMS,
             alpha: float = CONFIRMATORY_ALPHA_ONE_SIDED) -> dict:
    """E0 v1 synthetic-outcome power of the confirmatory test on this feature geometry.

    Uses the merged, manifest-verified ``evals/e0`` generator and seeds and the production
    statistic (``evals.e0.machinery.measure``); the manifest hash must equal the pin.
    """
    from evals.e0 import generator, manifest, runners
    from evals.e0 import machinery as e0m
    from evals.e0.benchmark import load_config
    from evals.e0.structure import Structure, TrialStructure

    verified = manifest.verify()
    if verified.get("manifest_sha256") != E0_MANIFEST_SHA256:
        raise PermissionError("evals/e0 differs from the pinned E0 v1 manifest")
    config = load_config()
    scenario = config["scenarios"][scenario_name]
    trials = {}
    for trial in E0_TRIAL_ORDER:
        horizon = int(trial.split("|fwd")[1])
        positions = np.arange(0, n_sessions, horizon, dtype=np.int64)
        feature = np.asarray(features[trial], dtype=float)
        if feature.shape[0] != len(positions):
            raise ValueError(f"{trial}: feature rows differ from the horizon-spaced session grid")
        trials[trial] = TrialStructure(trial=trial, horizon=horizon, positions=positions, feature=feature)
    structure = Structure(name="vs1_v8_admitted", n_sessions=n_sessions, trials=trials,
                          primary_trial=PRIMARY_TRIAL, propensity_trial=PRIMARY_TRIAL)
    ts = structure.trials[PRIMARY_TRIAL]
    z = ts.standardized_ranks()
    scale = runners.calibrate_scales(structure, scenario_name, scenario, [target_ic], [PRIMARY_TRIAL],
                                     pilot_sims=config["planted"]["pilot_sims"],
                                     pilot_scale=config["planted"]["pilot_scale"],
                                     pilot_seed=config["seeds"]["pilot"])[PRIMARY_TRIAL]["scales"][f"{target_ic:g}"]
    hits, usable, realized = 0, [], []
    for s in range(sims):
        rng = runners.world_rng(config["seeds"]["base"], scenario_name, s)
        world = generator.simulate_world(structure, scenario, rng)
        base = {t: generator.base_label(world, structure.trials[t], scenario, rng) for t in E0_TRIAL_ORDER}
        label = generator.plant(base[PRIMARY_TRIAL][0], base[PRIMARY_TRIAL][1], z, scale)
        record = e0m.measure(ts.feature, label, ts.horizon, PRIMARY_TRIAL, perms=perms)
        if record["status"] == "tested":
            hits += record["p_one_sided_positive"] <= alpha
            realized.append(record["mean_ic"])
        usable.append(record["n"])
    return {"model": f"e0:{scenario_name}", "target_ic": target_ic, "usable_dates": int(np.median(usable)),
            "power": hits / sims, "realized_mean_ic": float(np.mean(realized)) if realized else None,
            "plant_scale": float(scale), "sims": sims, "perms": perms, "alpha_one_sided": alpha,
            "e0_manifest_sha256": verified["manifest_sha256"],
            "e0_seeds": dict(config["seeds"])}


def confirmatory_selection(ledger: list[dict], primary: str = PRIMARY_TRIAL) -> list[dict]:
    """v8 body §1.2, in place: only the primary can be selected (one-sided positive p <= 0.05)."""
    for trial in ledger:
        confirmatory = trial["trial"] == primary
        trial["confirmatory"] = confirmatory
        trial["selected"] = bool(
            confirmatory and trial["status"] == "tested" and trial["mean_ic"] is not None
            and trial["mean_ic"] > 0 and trial["p_one_sided_positive"] <= CONFIRMATORY_ALPHA_ONE_SIDED)
    return ledger


def _gate_passed(gate: Mapping[str, Any]) -> bool:
    gated = [gate["gaussian_v1"], gate[f"e0:{E0_GATED_SCENARIO}"]]
    return all(isinstance(r.get("power"), float) and math.isfinite(r["power"]) and r["power"] >= v1.POWER_GATE
               for r in gated)


class V8Harness(v2.Harness):
    def register(self, log_dir: Path, now: datetime, code_sha: str, *,
                 v7_log_dir: Path | None = None, v7_witness=None, **kwargs) -> list[dict]:
        if v7_log_dir is None or v7_witness is None:
            raise PermissionError("v8 registration requires the exact witnessed v7 STOP")
        stop = v7.verify_terminal_stop(v7_log_dir, v7_witness)
        check_census(v7_witness.census, stop_head_sha256=stop["head_sha256"], witness_repo=v7_witness.repo)
        return super().register(log_dir, now, code_sha, **kwargs)

    def check_offhost(self, vault_repo: Path, *, remote_url: str | None = None):
        witness = super().check_offhost(vault_repo, remote_url=remote_url)
        check_census(witness.census, stop_head_sha256=V7_STOP_HEAD_SHA256,
                     witness_repo=witness.repo, opening=True)
        return witness

    def require_supersession(self, witness) -> dict:
        v1.refuse_superseded(8, self.superseded_by, witness)
        if not isinstance(witness, v2.OffhostWitness) or witness.path != WITNESS_PATH:
            raise PermissionError("v8 requires its pinned off-host witness")
        check_census(witness.census, stop_head_sha256=V7_STOP_HEAD_SHA256,
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
        log = self.registry(log_dir)
        with log.locked():
            records = self._chain(log)
        if v1._kind(records, "status"):
            raise PermissionError("v8 carries a terminal STOP status record: no freeze or opening")

    def freeze_inputs(self, log_dir: Path, now: datetime, inputs: Mapping[str, Any]) -> dict:
        if inputs.get("accept_underpowered") is not False:
            raise PermissionError("v8 must STOP below either gated 0.50 Stage-0 model")
        self._refuse_if_stopped(log_dir)
        return super().freeze_inputs(log_dir, now, inputs)

    def open_discovery(self, log_dir: Path, now: datetime, observed: Mapping[str, Any], witness) -> dict:
        self._refuse_if_stopped(log_dir)
        return super().open_discovery(log_dir, now, observed, witness)

    def open_holdout(self, frozen: dict, *, log_dir: Path, **kwargs) -> dict:
        self._refuse_if_stopped(log_dir)
        return super().open_holdout(frozen, log_dir=log_dir, **kwargs)

    # -- Stage-0 --
    def stage0_power(self, features: Mapping[str, np.ndarray]) -> dict:
        """v1 four-trial table (continuity only) plus the v8 two-model confirmatory gate."""
        out = v1.stage0_power(features)
        lo, hi = v1.window_bounds("discovery")
        n_sessions = len(v1.proxy_sessions(lo.date(), hi.date()))
        primary = np.asarray(features[PRIMARY_TRIAL], dtype=float)
        gate = {"gaussian_v1": planted_power_one_sided(primary, v1.POWER_GATE_IC)}
        for scenario in (E0_GATED_SCENARIO, *E0_REPORTED_SCENARIOS):
            gate[f"e0:{scenario}"] = e0_power(features, n_sessions, scenario)
        passed = _gate_passed(gate)
        return {**out, "version": self.version, "primary_trial": self.primary,
                "gate": (f"one-sided confirmatory {self.primary} at {CONFIRMATORY_ALPHA_ONE_SIDED}: "
                         f"power(IC {v1.POWER_GATE_IC}) >= {v1.POWER_GATE} under gaussian_v1 AND "
                         f"e0:{E0_GATED_SCENARIO}"),
                "v8_stage0": {"settings": json.loads(json.dumps(STAGE0_SETTINGS)), "models": gate,
                              "discovery_start": v1.discovery_start(), "n_sessions": n_sessions,
                              "design_target_met": bool(
                                  gate[f"e0:{E0_GATED_SCENARIO}"]["power"] >= DESIGN_TARGET_POWER)},
                "gate_passed": passed}

    def verify_power(self, power: Mapping[str, Any]) -> None:
        if power.get("version") != self.version or power.get("primary_trial") != self.primary:
            raise ValueError("power file was not computed by the vs1-v8 harness")
        if power.get("settings") != v1.power_settings():
            raise ValueError("power file was not computed at the pre-registered v1 table settings")
        table = power.get("table") or {}
        if set(table) != set(v1.trial_names()):
            raise ValueError("power file must cover exactly the declared trials")
        for trial, rows in table.items():
            if [r.get("target_ic") for r in rows] != list(v1.POWER_TARGET_ICS) \
                    or any(r.get("sims") not in (v1.POWER_SIMS, 0) for r in rows):
                raise ValueError(f"{trial}: continuity table rows are not at the pre-registered settings")
        stage0 = power.get("v8_stage0") or {}
        if stage0.get("settings") != json.loads(json.dumps(STAGE0_SETTINGS)):
            raise ValueError("power file was not computed at the pre-registered v8 Stage-0 settings")
        start = DISCOVERY_START[:10]
        sessions = len(v1.proxy_sessions(datetime.fromisoformat(start).date(),
                                         datetime.fromisoformat(v1.SPLIT[:10]).date()))
        if stage0.get("discovery_start") != DISCOVERY_START or stage0.get("n_sessions") != sessions:
            raise ValueError("power file was not computed on the v8 discovery window")
        models = stage0.get("models") or {}
        expected = {"gaussian_v1", f"e0:{E0_GATED_SCENARIO}", *(f"e0:{s}" for s in E0_REPORTED_SCENARIOS)}
        if set(models) != expected:
            raise ValueError("power file must carry exactly the v8 Stage-0 models")
        g = models["gaussian_v1"]
        degenerate = g.get("sims") == 0 and g.get("power") == 0.0  # too few usable dates: power 0
        if g.get("model") != "gaussian_v1" or (g.get("alpha_one_sided"), g.get("target_ic")) != (
                CONFIRMATORY_ALPHA_ONE_SIDED, v1.POWER_GATE_IC) or not degenerate and (
                (g.get("sims"), g.get("perms"), g.get("seed")) != (GAUSSIAN_SIMS, v1.POWER_PERMS, v1.SEED)):
            raise ValueError("gaussian_v1 Stage-0 row is not at the pre-registered settings")
        seeds = {"base": 20260930, "pilot": 20260931}  # E0 v1 config.json seeds (manifest-pinned)
        for name, row in models.items():
            if name.startswith("e0:") and (row.get("model") != name or (
                    row.get("sims"), row.get("perms"), row.get("alpha_one_sided"), row.get("target_ic"),
                    row.get("e0_manifest_sha256"), row.get("e0_seeds")) != (
                    E0_SIMS, v1.POWER_PERMS, CONFIRMATORY_ALPHA_ONE_SIDED, v1.POWER_GATE_IC, E0_MANIFEST_SHA256,
                    seeds)):
                raise ValueError(f"{name} Stage-0 row is not at the pre-registered settings")
        if power.get("gate_passed") is not _gate_passed(models):
            raise ValueError("power file's gate_passed disagrees with its gated models")

    # -- discovery selection (v8 body §1.2) --
    def discover_panel(self, spec: v1.RunSpec, panels: Mapping[str, v1.TrialPanel], *, inputs: dict,
                       repo_root: Path = v2.REPO, sensitivity: bool = True) -> dict:
        frozen = super().discover_panel(spec, panels, inputs=inputs, repo_root=repo_root, sensitivity=sensitivity)
        payload = frozen["payload"]
        confirmatory_selection(payload["ledger"], self.primary)
        payload["selection"] = (f"one confirmatory trial {self.primary}: one-sided (positive) block sign-flip "
                                f"p <= {CONFIRMATORY_ALPHA_ONE_SIDED} with positive mean IC; the other trials are "
                                "exploratory (reported with Holm/BH-adjusted p, never selected)")
        payload["calibration"] = self.calibration(payload["ledger"], spec.alpha)
        return {"payload": payload, "sha256": digest(payload)}


V7_EARLIER = v2.EarlierVersion(
    version=v7.VERSION, number=7, prereg_sha256=v7.PREREG_BODY_SHA256,
    registry_head_sha256=v7.REGISTERED_RECORD_SHA256[1], witness_path=v7.WITNESS_PATH,
    anchor_line=v7.REGISTERED_ANCHOR_LINE,
)
V8 = V8Harness(v2.Pins(
    version=VERSION, number=8, prereg_path=PREREG_PATH,
    prereg_body_sha256=PREREG_BODY_SHA256, primary_trial=PRIMARY_TRIAL,
    registry_log=REGISTRY_LOG, registry_anchors=REGISTRY_ANCHORS,
    registry_lock=REGISTRY_LOCK, witness_path=WITNESS_PATH, witness_ref=WITNESS_REF,
    registration_records=registration_records, registered_at=None, registered_code_sha=None,
    registered_record_sha256=REGISTERED_RECORD_SHA256,
    registered_anchor_line=REGISTERED_ANCHOR_LINE,
    earlier=(v2.V1_EARLIER, v2.V2_EARLIER, v6.V3_EARLIER,
             v6.V4_EARLIER, v6.V5_EARLIER, v7.V6_EARLIER, V7_EARLIER),
    superseded_by=SUPERSEDED_BY, holdout_probe_required=True,
), module=sys.modules[__name__])

stage0_power = V8.stage0_power
verify_power = V8.verify_power
discover_panel = V8.discover_panel
check_holdout_request = V8.check_holdout_request
evaluate_panel_holdout = V8.evaluate_panel_holdout
load_price_panel = V8.load_price_panel
registry = V8.registry
register = V8.register
check_offhost = V8.check_offhost
require_supersession = V8.require_supersession
require_witness = V8.require_witness
export_anchors = V8.export_anchors
record_prices_read = V8.record_prices_read
freeze_inputs = V8.freeze_inputs
latest_frozen_inputs = V8.latest_frozen_inputs
open_discovery = V8.open_discovery
resume_discovery = V8.resume_discovery
seal_discovery = V8.seal_discovery
open_holdout = V8.open_holdout
resume_holdout = V8.resume_holdout
seal_holdout = V8.seal_holdout


# --- terminal STOP below either gated Stage-0 model (v8 body §5 step 3) ----------------------

STOP_STATUS = "STOP_FOR_OWNER_UNDERPOWERED_UNOPENED"
STOP_RECORDS = 3
_STOP_REQUIRED: Mapping[str, Any] = {
    "kind": "status", "version": VERSION, "status": STOP_STATUS,
    "gate_passed": False, "inputs_frozen": False, "discovery_opened": False, "holdout_opened": False,
    "prereg_sha256": PREREG_BODY_SHA256, "primary_trial": PRIMARY_TRIAL,
    "power_gate": json.loads(json.dumps(STAGE0_SETTINGS)), "promotion_allowed": False,
}
_STOP_FIELDS = frozenset(_STOP_REQUIRED) | {
    "run_at", "stage0_power_ic_0_01", "power_receipt_sha256", "power_inputs", "decision_ref", "prev_sha256",
}


#: Exactly the inputs a post-admission ``cmd_power`` binds into ``power.json``.
POWER_INPUT_KEYS = frozenset({
    "price_manifest_sha256", "form4_receipt_sha256", "admission_receipt_sha256",
    "universe_sha256", "prereg_sha256",
})


def _check_power_inputs(inputs: Any) -> None:
    if not isinstance(inputs, Mapping) or set(inputs) != POWER_INPUT_KEYS \
            or inputs.get("prereg_sha256") != PREREG_BODY_SHA256 \
            or not all(v1._is_hex64(inputs[k]) for k in POWER_INPUT_KEYS):
        raise PermissionError("power receipt is not a v8 post-admission Stage-0 on a price manifest")


def _gated_powers(power: Mapping[str, Any]) -> dict[str, float]:
    models = (power.get("v8_stage0") or {}).get("models") or {}
    return {name: float(models[name]["power"]) for name in ("gaussian_v1", f"e0:{E0_GATED_SCENARIO}")}


def _check_stop_record(record: Mapping[str, Any]) -> None:
    if any(record.get(key) != value for key, value in _STOP_REQUIRED.items()):
        raise PermissionError("v8 terminal record is not the exact STOP status")
    if set(record) != _STOP_FIELDS:
        raise PermissionError("v8 terminal STOP record has missing or extra fields")
    powers = record.get("stage0_power_ic_0_01")
    if not isinstance(powers, Mapping) or set(powers) != {"gaussian_v1", f"e0:{E0_GATED_SCENARIO}"} \
            or not all(isinstance(p, float) and 0.0 <= p <= 1.0 for p in powers.values()) \
            or min(powers.values()) >= v1.POWER_GATE:
        raise PermissionError("v8 STOP needs a gated Stage-0 model below the 0.50 gate")
    if not v1._is_hex64(record.get("power_receipt_sha256")) or not str(record.get("decision_ref") or "").strip():
        raise PermissionError("v8 STOP needs the sealed power receipt hash and a decision reference")
    _check_power_inputs(record.get("power_inputs"))


def append_stop_status(log_dir: Path, now: datetime, *, power_path: Path, decision_ref: str,
                       expected_prev_sha256: str, witness, dry_run: bool = False) -> dict:
    """Append v8's one STOP after a verified, sealed, underpowered Stage-0 (exact 2-record chain)."""
    from datetime import timedelta

    if REGISTERED_RECORD_SHA256 is None:
        raise PermissionError("v8 is not registered yet (no pinned registration in code)")
    if now.tzinfo is None or now.utcoffset() != timedelta(0) or not str(decision_ref).strip() \
            or expected_prev_sha256 != REGISTERED_RECORD_SHA256[1]:
        raise ValueError("STOP needs a UTC time, a decision reference and the exact v8 registration head")
    body = Path(power_path).read_bytes()
    power = json.loads(body)
    verify_power(power)
    _check_power_inputs(power.get("inputs"))
    if power.get("gate_passed") is not False:
        raise PermissionError("v8 passed its Stage-0 gate: a STOP is refused (freeze instead)")
    V8.require_witness(log_dir, witness, 2)
    if (witness.census.get("records") or {}).get(REGISTRY_ID) != 2:
        raise PermissionError("v8's canonical witness is not at the two-record baseline")
    log = V8.registry(log_dir)
    with log.locked():
        records = V8._chain(log)
        if len(records) != 2:
            raise PermissionError("v8 registry differs from its two-record baseline; reconcile before STOP")
        if v1._record_sha256(records[-1]) != expected_prev_sha256:
            raise PermissionError("v8 prior head differs from the expected head")
        candidate = {
            **json.loads(json.dumps(_STOP_REQUIRED)), "run_at": now.isoformat(),
            "stage0_power_ic_0_01": _gated_powers(power),
            "power_receipt_sha256": hashlib.sha256(body).hexdigest(), "power_inputs": dict(power["inputs"]),
            "decision_ref": str(decision_ref).strip(), "prev_sha256": v1._record_sha256(records[-1]),
        }
        _check_stop_record(candidate)
        if dry_run:
            return {"dry_run": True, "would_append": candidate,
                    "would_be_head_sha256": v1._record_sha256(candidate)}
        return log.append_locked([candidate])[0]


def verify_terminal_stop(log_dir: Path, witness) -> dict:
    """Return the only acceptable v8 terminal head after exact off-host witnessing."""
    log = V8.registry(log_dir)
    proof = V8.require_witness(log_dir, witness, STOP_RECORDS)
    records = V8._chain(log)
    if len(records) != STOP_RECORDS or proof["witnessed_records"] != STOP_RECORDS:
        raise PermissionError("v8 STOP witness must end at exactly three records")
    _check_stop_record(records[-1])
    if (witness.census.get("records") or {}).get(REGISTRY_ID) != STOP_RECORDS:
        raise PermissionError("v8 census does not cover exactly the STOP record")
    return {"records": STOP_RECORDS, "head_sha256": v1._record_sha256(records[-1]),
            "witness_path": WITNESS_PATH, "witness_tip": witness.tip}


def run_spec(run_id: str | None = None):
    return V8.run_spec(run_id)


def prereg_body_sha256(path: Path) -> str:
    return v1.prereg_body_sha256(path)

