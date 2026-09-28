"""VS1 v3 panel harness: v2's SIC-expanded Technology study with the 5-session trial primary (research only).

Pre-registration: ``docs/paper_log/vs1-insider-density-v3-preregistration.md``
(body sha256 pinned in :data:`PREREG_BODY_SHA256`).

v3 is v2 (``analysis.panel_insider_density_v2``) with one design change, made
from v2's Stage-0 power table alone (no price or outcome was ever read): the
primary trial is ``A90|fwd5`` instead of ``A90|fwd20``. The universe, SIC map,
admission rules, features, horizons, the 4-trial Holm family, statistic, null,
holdout rule, reported items and alarms are v2's, unchanged; the 20-session
trials stay in the same Holm family as secondary trials.

v3 is its own pinned registry (``granular_panel_prereg_v3.jsonl``) with its own
off-host witness (``05-GRID/Paper-Log/vs1/granular_panel_prereg_v3.anchors.jsonl``
on vault ``main``). v1 and v2 are superseded: their code refuses to open a
discovery, and v3 refuses to open one unless the witnesses of v1 and v2 still
cover only their 2 registration records. Only v3 can ever read VS1 prices.
Nothing here is a trading signal.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis.panel_insider_density_v2 import (  # noqa: F401  (the version-free v2 API, unchanged)
    ISSUER_MAP_SHA256,
    SIC_MAP_SHA256,
    SIC_RANGES,
    Admission,
    DiscoveryKey,
    HoldoutKey,
    OffhostWitness,
    PriceManifest,
    TrialPanel,
    admission_report,
    build_admission,
    build_trial_panels,
    delisting_sensitivity,
    feature_panel,
    industry_neutral_ic,
    load_inputs,
    load_issuer_map,
    load_sic_map,
    measure_trial,
    power_features,
    v2_universe,
    write_frozen,
)

REPO = v2.REPO
PREREG_PATH = Path("docs/paper_log/vs1-insider-density-v3-preregistration.md")
# sha256 of the LF bytes strictly between the two markers. Changing the body is
# a new pre-registration (v4): re-pin only before any VS1 price is read.
PREREG_BODY_SHA256 = "fa7eda1c70906720b36dd84d0bb8b65a53f7badc35cd05055e08d7a9b40c2e42"
VERSION = "vs1-v3"
#: v3's one design change: the 5-session A90 trial is primary (v2 Stage-0 power table).
PRIMARY_TRIAL = "A90|fwd5"
SECONDARY_TRIALS: tuple[str, ...] = tuple(t for t in v1.trial_names() if t != PRIMARY_TRIAL)

REGISTRY_LOG = "granular_panel_prereg_v3.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_v3.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_v3.lock"

#: The one real v3 registration: registered once, locally, on 2026-09-28T01:05:51Z
#: against code 061a097d, chain head c110b193... at 2 records. The original lives in
#: the operator's ``Documents/Codex/2026-09-14/wha/outputs/vs1-v3-prereg-registry/``;
#: the off-host witness decides which copy counts.
REGISTERED_AT: datetime | None = datetime(2026, 9, 28, 1, 5, 51, 160636, tzinfo=timezone.utc)
REGISTERED_CODE_SHA: str | None = "061a097d2664607bdebdb4e35805dc8b1c5265f5"
REGISTERED_RECORD_SHA256: tuple[str, str] | None = (
    "02f8a473314b3ad66288cd2c7a358690f522b1045e17d8582b25463cbf65c519",  # header
    "c110b193660d5ce073d7badcddf360c739811fd86799874f3c786a16c2babbc9",  # preregistration (head at 2)
)
REGISTERED_PREREG_SHA256 = PREREG_BODY_SHA256

WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = v1.WITNESS_BRANCH
WITNESS_PATH = "05-GRID/Paper-Log/vs1/granular_panel_prereg_v3.anchors.jsonl"
WITNESS_REF = "refs/vs1-v3-witness/main"
#: The first line every committed version of the v3 witness file starts with.
REGISTERED_ANCHOR_LINE: bytes | None = (
    b'{"head_sha256":"c110b193660d5ce073d7badcddf360c739811fd86799874f3c786a16c2babbc9",'
    b'"prev_anchor_sha256":null,"records":2,"run_at":"2026-09-28T01:05:51.160636+00:00"}'
)
#: v3 was superseded by v4 before any price read (owner decisions 2026-09-28 03:40Z: TIINGO-only
#: price admission with a TwelveData cross-check, and a post-admission power gate).
SUPERSEDED_BY: Mapping[str, Any] | None = {
    "version": "vs1-v6",
    "prereg_sha256": "5a87d4a4130e184a8b9e53d7eec040a7b26b697a3ad07500aac6ef4b17d6a32d",
    "registry_head_sha256": None,
}


def registration_records(now: datetime, code_sha: str, prereg_sha256: str = PREREG_BODY_SHA256) -> list[dict]:
    """v3's header and ``preregistration`` records (before chaining)."""
    header = {
        "kind": "header",
        "version": VERSION,
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": prereg_sha256,
        "promotion_allowed": False,
    }
    record = {
        "kind": "preregistration",
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": prereg_sha256,
        "sector_map_sha256": v1.SECTOR_MAP_SHA256,
        "issuer_map_sha256": ISSUER_MAP_SHA256,
        "sic_map_sha256": SIC_MAP_SHA256,
        "sic_ranges": [list(r) for r in SIC_RANGES],
        "ledger_id": v1.LEDGER_ID,
        "runs": {
            "vs1": {"sector": v1.VS1_SECTOR, "k": v1.VS1_RUN_K, "alpha": v1.run_alpha(v1.VS1_RUN_K),
                    "trials": list(v1.trial_names()), "primary_trial": PRIMARY_TRIAL,
                    "secondary_trials": list(SECONDARY_TRIALS)},
            "other_sectors": {"sectors": list(v1.OTHER_SECTORS), "k": v1.OTHER_SECTORS_RUN_K,
                              "alpha": v1.run_alpha(v1.OTHER_SECTORS_RUN_K), "trials": list(v1.trial_names()),
                              "rules": "vs1-v1 (v1 pre-registration section 13, unchanged)"},
        },
        "supersedes": [
            {"version": e.version, "prereg_sha256": e.prereg_sha256, "registry_head_sha256": e.registry_head_sha256,
             "status": "superseded before any price read"}
            for e in (v2.V1_EARLIER, v2.V2_EARLIER)
        ],
        "windows": {"discovery_start": v1.DISCOVERY_START, "split": v1.SPLIT, "end": v1.END},
        "promotion_allowed": False,
    }
    return [header, record]


V3 = v2.Harness(v2.Pins(
    version=VERSION, number=3, prereg_path=PREREG_PATH, prereg_body_sha256=PREREG_BODY_SHA256,
    primary_trial=PRIMARY_TRIAL, registry_log=REGISTRY_LOG, registry_anchors=REGISTRY_ANCHORS,
    registry_lock=REGISTRY_LOCK, witness_path=WITNESS_PATH, witness_ref=WITNESS_REF,
    registration_records=registration_records, registered_at=REGISTERED_AT, registered_code_sha=REGISTERED_CODE_SHA,
    registered_record_sha256=REGISTERED_RECORD_SHA256, registered_anchor_line=REGISTERED_ANCHOR_LINE,
    earlier=(v2.V1_EARLIER, v2.V2_EARLIER), superseded_by=SUPERSEDED_BY,
), module=sys.modules[__name__])


def prereg_body_sha256(path: Path) -> str:
    return v1.prereg_body_sha256(path)


def run_spec(run_id: str | None = None) -> v1.RunSpec:
    return V3.run_spec(run_id)


def calibration(ledger: list[dict], alpha: float = v1.run_alpha(v1.VS1_RUN_K)) -> dict:
    return V3.calibration(ledger, alpha)


def verdict(payload: dict, checks: list[dict], power: dict | None) -> dict:
    return V3.verdict(payload, checks, power)


check_prereg = V3.check_prereg
stage0_power = V3.stage0_power
verify_power = V3.verify_power
discover_panel = V3.discover_panel
check_holdout_request = V3.check_holdout_request
evaluate_panel_holdout = V3.evaluate_panel_holdout
load_price_panel = V3.load_price_panel
registry = V3.registry
register = V3.register
check_offhost = V3.check_offhost
require_supersession = V3.require_supersession
require_witness = V3.require_witness
export_anchors = V3.export_anchors
record_prices_read = V3.record_prices_read
freeze_inputs = V3.freeze_inputs
latest_frozen_inputs = V3.latest_frozen_inputs
open_discovery = V3.open_discovery
resume_discovery = V3.resume_discovery
seal_discovery = V3.seal_discovery
open_holdout = V3.open_holdout
resume_holdout = V3.resume_holdout
seal_holdout = V3.seal_holdout

