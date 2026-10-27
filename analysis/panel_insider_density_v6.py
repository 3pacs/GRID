"""VS1 v6 panel harness: v5 with the ticker-reuse price bound as candidate amendment C1 (research only).

Pre-registration: ``docs/paper_log/vs1-insider-density-v6-preregistration.md``
(body sha256 pinned in :data:`PREREG_BODY_SHA256`). v6 is VS1 v5 (source
filtering, pull-batch splice check, Tiingo-metadata entity check, TwelveData
cross-check, post-admission Stage-0, holdout basis check at open-holdout) with
one change: the price-side ticker-reuse bound is C1
(``docs/paper_log/vs1-v2-candidate-amendments.md``), the same point-in-time
ticker interval (``analysis.panel_insider_density_v2.ticker_mask``, §2.2 rule 3)
that already gates the features, applied to the closes, plus the price source's
listing-date cross-check (Tiingo ``startDate`` = manifest ``listed_from``).
One interval definition; v5's L_T is gone.

Own pinned registry and canonical witness
``05-GRID/Paper-Log/vs1/granular_panel_prereg_v6.anchors.jsonl``; v1-v5 refuse.
Nothing here is a trading signal.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
from analysis import panel_insider_density_v4 as v4
from analysis import panel_insider_density_v5 as v5
from analysis.panel_insider_density_v2 import (  # noqa: F401  (the version-free v2/v3 API, unchanged)
    ISSUER_MAP_SHA256,
    SIC_MAP_SHA256,
    SIC_RANGES,
    Admission,
    DiscoveryKey,
    HoldoutKey,
    OffhostWitness,
    TrialPanel,
    admission_report,
    build_admission,
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
PREREG_PATH = Path("docs/paper_log/vs1-insider-density-v6-preregistration.md")
PREREG_BODY_SHA256 = "5a87d4a4130e184a8b9e53d7eec040a7b26b697a3ad07500aac6ef4b17d6a32d"
VERSION = "vs1-v6"
PRIMARY_TRIAL = v3.PRIMARY_TRIAL  # A90|fwd5, unchanged from v3
SECONDARY_TRIALS: tuple[str, ...] = v3.SECONDARY_TRIALS
#: v4 §10: the binding Stage-0 runs on the price-admitted panel (the CLI's ``power --price-manifest``).
POST_ADMISSION_POWER = True

# --- v4 §2.3: the price-admission rule --------------------------------------------------

PRICE_SOURCE = "TIINGO"  # source_catalog id 524
PRICE_SOURCE_ID = 524
SERIES_TEMPLATE = "YF:{ticker}:adj_close"
BASIS = "split+dividend adjusted"
BENCHMARK = "XLK"


CrossCheckRule = v4.CrossCheckRule
CROSSCHECK = v4.CROSSCHECK
crosscheck_statistics = v4.crosscheck_statistics


@dataclass(frozen=True)
class PriceManifest(v2.PriceManifest):
    """v6's admitted-price contract: v5's (TIINGO adj_close only, cross-checked, Tiingo startDate as listed_from)."""

    crosscheck_report_sha256: str = ""
    tiingo_meta_report_sha256: str = ""

    def validate(self) -> None:
        super().validate()
        if self.source != PRICE_SOURCE:
            raise ValueError(f"v6 admits only the {PRICE_SOURCE} source, not {self.source!r}")
        if self.series_template != SERIES_TEMPLATE:
            raise ValueError(f"v6 reads only {SERIES_TEMPLATE}")
        if self.basis != BASIS or self.benchmark != BENCHMARK:
            raise ValueError(f"v6 declares basis {BASIS!r} and benchmark {BENCHMARK}")
        if not v1._is_hex64(self.crosscheck_report_sha256):
            raise ValueError("v6 manifests carry the TwelveData cross-check report's sha256")
        if not v1._is_hex64(self.tiingo_meta_report_sha256):
            raise ValueError("v6 manifests carry the Tiingo metadata report's sha256")
        listed = dict(self.listed_from)
        missing = [t for t in self.admitted if t != self.benchmark and t not in listed]
        if missing:
            raise ValueError(f"v6 manifests carry listed_from (Tiingo startDate) for every admitted ticker; missing {missing[:10]}")


# --- v5 §2.3 rules, unchanged; v6 §2.3: the price bound is the C1 ticker interval ---------------------

SPLICE_TOL = v5.SPLICE_TOL
NAME_JACCARD_MIN = v5.NAME_JACCARD_MIN
NAME_STOP_TOKENS = v5.NAME_STOP_TOKENS
splice_check = v5.splice_check
name_match = v5.name_match


def interval_close_mask(admission: Admission):
    """The C1 ticker interval (§2.2 rule 3, ``ticker_mask``) at each session's 16:00 New York close, per ticker."""

    def mask(closes: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
        sessions = [ts.date() for ts in pd.DatetimeIndex(closes.index)]
        instants = v1.decision_instants(sessions)
        ciks = [int(c) for c in universe["cik"]]
        inside = v2.ticker_mask(admission, ciks, instants)
        out = pd.DataFrame(inside.to_numpy(), index=closes.index, columns=list(universe["ticker"]))
        return out

    return mask


def build_trial_panels(events, admission, universe, prices, window):
    """v2's trial panels with closes outside the C1 interval, or before Tiingo startDate, blanked first (v6 §2.3)."""
    return v2.build_trial_panels(events, admission, universe, prices, window, blank_before_listing=True,
                                 close_mask=interval_close_mask(admission))


# --- pins ---------------------------------------------------------------------------------

REGISTRY_LOG = "granular_panel_prereg_v6.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_v6.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_v6.lock"
WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = v1.WITNESS_BRANCH
WITNESS_PATH = v1.canonical_witness_path("vs1-v6")
WITNESS_REF = "refs/vs1-v6-witness/main"

#: The one v6 registration: registered once, locally, on 2026-09-28T05:03:48Z against code
#: 25dcda41, chain head 3dfa6ee3... at 2 records. The original lives in the operator's
#: ``Documents/Codex/2026-09-14/wha/outputs/vs1-v6-prereg-registry/``.
REGISTERED_AT: datetime | None = datetime(2026, 9, 28, 5, 3, 48, 895875, tzinfo=timezone.utc)
REGISTERED_CODE_SHA: str | None = "25dcda410d8e769e698c54f4f4f38dfa2174d9a2"
REGISTERED_RECORD_SHA256: tuple[str, str] | None = (
    "a1e0455b910f6e5b4fd8bf7d8008b8b0b6d0590109c7c47c8af80031bb10edeb",  # header
    "3dfa6ee30359205c84ba4fb3bdb0858eba13b6ed0c8aa0711d98fa6aade505e7",  # preregistration (head at 2)
)
REGISTERED_PREREG_SHA256 = PREREG_BODY_SHA256
#: The first line every committed version of the v6 witness file starts with.
REGISTERED_ANCHOR_LINE: bytes | None = (
    b'{"head_sha256":"3dfa6ee30359205c84ba4fb3bdb0858eba13b6ed0c8aa0711d98fa6aade505e7",'
    b'"prev_anchor_sha256":null,"records":2,"run_at":"2026-09-28T05:03:48.895875+00:00"}'
)
# The v7 body and registry head are bound only after the owner selects its design.
# The version pin alone is sufficient to make every v6 discovery/holdout opening refuse.
SUPERSEDED_BY: Mapping[str, Any] | None = {"version": "vs1-v7"}

STOP_STATUS = "STOP_FOR_OWNER_SUPERSEDED_BY_V7_UNOPENED"
STOP_RAW_PRIMARY_POWER_IC_0_01 = 0.48


def _check_stop_record(record: Mapping[str, Any]) -> None:
    required = {
        "kind": "status", "version": VERSION, "status": STOP_STATUS,
        "raw_primary_power_ic_0_01": STOP_RAW_PRIMARY_POWER_IC_0_01,
        "gate_passed": False, "discovery_opened": False, "holdout_opened": False,
        "superseded_by": "vs1-v7", "prereg_sha256": PREREG_BODY_SHA256,
        "promotion_allowed": False,
    }
    if any(record.get(key) != value for key, value in required.items()):
        raise PermissionError("v6 terminal record is not the exact STOP status")
    allowed = set(required) | {"run_at", "power_receipt_sha256", "owner_decision_ref", "prev_sha256"}
    if set(record) != allowed:
        raise PermissionError("v6 terminal STOP record has missing or extra fields")
    if not v1._is_hex64(record.get("power_receipt_sha256")) or not record.get("owner_decision_ref"):
        raise PermissionError("v6 STOP needs the immutable power receipt and owner decision reference")
    if "v7_prereg_sha256" in record or "v7_registry_head_sha256" in record:
        raise PermissionError("the v6 STOP must not contain a v7 hash")


def append_stop_status(log_dir: Path, now: datetime, *, power_receipt_sha256: str,
                       owner_decision_ref: str, expected_prev_sha256: str,
                       witness: OffhostWitness,
                       dry_run: bool = False) -> dict:
    """Append v6's one STOP record after checking the exact two-record witnessed chain.

    ``dry_run=True`` verifies the copied chain and witness and returns the exact
    would-be record/head without writing. Execute only once after a backup and
    dry run; a second execution refuses rather than appending another status.
    Publishing the anchor is a separate step, followed by verification.
    """
    if now.tzinfo is None or now.utcoffset() != timedelta(0) \
            or not v1._is_hex64(power_receipt_sha256) or not owner_decision_ref.strip() \
            or expected_prev_sha256 != REGISTERED_RECORD_SHA256[1]:
        raise ValueError("STOP needs a UTC time, receipt sha256, owner reference and exact v6 prior head")
    V6.require_witness(log_dir, witness, 2)
    if witness.census.get("records", {}).get(VERSION) != 2:
        raise PermissionError("v6's canonical witness is not at the two-record baseline")
    log = V6.registry(log_dir)
    with log.locked():
        records = V6._chain(log)
        if len(records) != 2:
            raise PermissionError("v6 registry differs from its two-record baseline; reconcile before STOP")
        if v1._record_sha256(records[-1]) != expected_prev_sha256:
            raise PermissionError("v6 prior head differs from the expected head")
        candidate = {
            "kind": "status", "run_at": now.isoformat(), "version": VERSION,
            "status": STOP_STATUS, "raw_primary_power_ic_0_01": STOP_RAW_PRIMARY_POWER_IC_0_01,
            "gate_passed": False, "power_receipt_sha256": power_receipt_sha256,
            "discovery_opened": False, "holdout_opened": False,
            "owner_decision_ref": owner_decision_ref.strip(), "superseded_by": "vs1-v7",
            "prereg_sha256": PREREG_BODY_SHA256, "promotion_allowed": False,
            "prev_sha256": v1._record_sha256(records[-1]),
        }
        _check_stop_record(candidate)
        if dry_run:
            return {"dry_run": True, "would_append": candidate,
                    "would_be_head_sha256": v1._record_sha256(candidate)}
        return log.append_locked([candidate])[0]


def verify_terminal_stop(log_dir: Path, witness: OffhostWitness) -> dict:
    """Return the only acceptable v6 terminal head after exact off-host witnessing."""
    log = V6.registry(log_dir)
    proof = V6.require_witness(log_dir, witness, 3)
    records = V6._chain(log)
    if len(records) != 3 or proof["witnessed_records"] != 3:
        raise PermissionError("v6 STOP witness must end at exactly three records")
    _check_stop_record(records[-1])
    if witness.census.get("records", {}).get(VERSION) != 3:
        raise PermissionError("v6 census does not cover exactly the STOP record")
    return {"records": 3, "head_sha256": v1._record_sha256(records[-1]),
            "witness_path": WITNESS_PATH, "witness_tip": witness.tip}

V3_EARLIER = v4.V3_EARLIER
V4_EARLIER = v5.V4_EARLIER
V5_EARLIER = v2.EarlierVersion(
    version=v5.VERSION, number=5, prereg_sha256=v5.PREREG_BODY_SHA256,
    registry_head_sha256=v5.REGISTERED_RECORD_SHA256[1], witness_path=v5.WITNESS_PATH,
    anchor_line=v5.REGISTERED_ANCHOR_LINE,
)


def registration_records(now: datetime, code_sha: str, prereg_sha256: str = PREREG_BODY_SHA256) -> list[dict]:
    """v4's header and ``preregistration`` records (before chaining)."""
    header = {"kind": "header", "version": VERSION, "run_at": now.isoformat(), "code_sha": code_sha,
              "prereg_path": PREREG_PATH.as_posix(), "prereg_sha256": prereg_sha256, "promotion_allowed": False}
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
        "runs": {"vs1": {"sector": v1.VS1_SECTOR, "k": v1.VS1_RUN_K, "alpha": v1.run_alpha(v1.VS1_RUN_K),
                         "trials": list(v1.trial_names()), "primary_trial": PRIMARY_TRIAL,
                         "secondary_trials": list(SECONDARY_TRIALS)}},
        "price_admission": {
            "source": PRICE_SOURCE, "source_id": PRICE_SOURCE_ID, "series_template": SERIES_TEMPLATE, "basis": BASIS,
            "refused": ["yfinance (every series id and status, QUARANTINED included)", "KAGGLE_BULK",
                        "every source other than TIINGO"],
            "basis_checks": ["no multi-valued dates", "split-consistent adjustment steps", "no gaps",
                             "no QUARANTINED rows"],
            "crosscheck": {"provider": CROSSCHECK.provider, "X_min_share_within": CROSSCHECK.min_share_within,
                           "Y_tolerance": CROSSCHECK.tolerance, "N_min_pairs": CROSSCHECK.min_pairs,
                           "max_excluded_share": CROSSCHECK.max_excluded_share,
                           "factor_rel_tol": CROSSCHECK.factor_rel_tol,
                           "discovery_window": list(CROSSCHECK.discovery_window),
                           "holdout_window": list(CROSSCHECK.holdout_window)},
            "post_admission_power_gate": "primary A90|fwd5 power at IC 0.01 >= 0.50 before freeze-inputs",
            "holdout_basis_check": "only at open-holdout, after discovery_frozen (incl. the 2020-01-01 boundary)",
            "source_filtering": "reads source-filtered to TIINGO; other-source rows under the same series id "
                                "ignored, counted, reported, not disqualifying; only multi-valued TIINGO dates disqualify",
            "splice": {"tolerance": SPLICE_TOL, "rule": "|f_t/f_s - 1| <= tol at each pull-batch boundary, or the "
                                                        "same step within tol in TwelveData's factor"},
            "ticker_reuse_bound": "C1: closes used only inside the issuer's point-in-time ticker interval "
                                  "(section 2.2 rule 3, one definition) and on or after Tiingo startDate",
            "entity_check": {"name_jaccard_min": NAME_JACCARD_MIN, "stop_tokens": sorted(NAME_STOP_TOKENS),
                             "or": "one joined token string prefixes the other"},
        },
        "supersedes": [
            {"version": e.version, "prereg_sha256": e.prereg_sha256, "registry_head_sha256": e.registry_head_sha256,
             "status": "superseded before any price read"}
            for e in (v2.V1_EARLIER, v2.V2_EARLIER, V3_EARLIER, V4_EARLIER, V5_EARLIER)
        ],
        "windows": {"discovery_start": v1.DISCOVERY_START, "split": v1.SPLIT, "end": v1.END},
        "promotion_allowed": False,
    }
    return [header, record]


V6 = v2.Harness(v2.Pins(
    version=VERSION, number=6, prereg_path=PREREG_PATH, prereg_body_sha256=PREREG_BODY_SHA256,
    primary_trial=PRIMARY_TRIAL, registry_log=REGISTRY_LOG, registry_anchors=REGISTRY_ANCHORS,
    registry_lock=REGISTRY_LOCK, witness_path=WITNESS_PATH, witness_ref=WITNESS_REF,
    registration_records=registration_records, registered_at=REGISTERED_AT, registered_code_sha=REGISTERED_CODE_SHA,
    registered_record_sha256=REGISTERED_RECORD_SHA256, registered_anchor_line=REGISTERED_ANCHOR_LINE,
    earlier=(v2.V1_EARLIER, v2.V2_EARLIER, V3_EARLIER, V4_EARLIER, V5_EARLIER), superseded_by=SUPERSEDED_BY,
    holdout_probe_required=True,
), module=sys.modules[__name__])


def prereg_body_sha256(path: Path) -> str:
    return v1.prereg_body_sha256(path)


def run_spec(run_id: str | None = None) -> v1.RunSpec:
    return V6.run_spec(run_id)


def calibration(ledger: list[dict], alpha: float = v1.run_alpha(v1.VS1_RUN_K)) -> dict:
    return V6.calibration(ledger, alpha)


def verdict(payload: dict, checks: list[dict], power: dict | None, contamination: Mapping[str, Any] | None = None) -> dict:
    return V6.verdict(payload, checks, power, contamination)


check_prereg = V6.check_prereg
stage0_power = V6.stage0_power
verify_power = V6.verify_power
discover_panel = V6.discover_panel
check_holdout_request = V6.check_holdout_request
evaluate_panel_holdout = V6.evaluate_panel_holdout
load_price_panel = V6.load_price_panel
registry = V6.registry
register = V6.register
check_offhost = V6.check_offhost
require_supersession = V6.require_supersession
require_witness = V6.require_witness
export_anchors = V6.export_anchors
record_prices_read = V6.record_prices_read
freeze_inputs = V6.freeze_inputs
latest_frozen_inputs = V6.latest_frozen_inputs
open_discovery = V6.open_discovery
resume_discovery = V6.resume_discovery
seal_discovery = V6.seal_discovery
open_holdout = V6.open_holdout
resume_holdout = V6.resume_holdout
seal_holdout = V6.seal_holdout
