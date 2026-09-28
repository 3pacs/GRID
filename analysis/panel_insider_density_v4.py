"""VS1 v4 panel harness: v3 plus a price-admission rule and a post-admission power gate (research only).

Pre-registration: ``docs/paper_log/vs1-insider-density-v4-preregistration.md``
(body sha256 pinned in :data:`PREREG_BODY_SHA256`).

v4 is v3 (``analysis.panel_insider_density_v3``) with the owner decisions of
2026-09-28 03:40Z, taken on price coverage and basis metadata only (no outcome):

* price source TIINGO ``adj_close`` only; yfinance (every id and status),
  Kaggle bulk and every other source refused (:class:`PriceManifest`);
* a ticker is admitted only if it passes the GD4 basis checks and the
  TwelveData return cross-check (:func:`crosscheck_statistics`,
  :data:`CROSSCHECK`), whose report's sha256 the manifest carries;
* a binding post-admission Stage-0 on the price-admitted panel before
  ``freeze-inputs`` (:data:`POST_ADMISSION_POWER`);
* the holdout-period basis check only at ``open-holdout`` (the Harness pin
  ``holdout_probe_required``).

Everything else, including the primary trial ``A90|fwd5``, is v3's. v4 has its
own pinned registry and canonical witness
``05-GRID/Paper-Log/vs1/granular_panel_prereg_v4.anchors.jsonl``; v1, v2 and v3
are superseded and refuse. Nothing here is a trading signal.
"""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
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
PREREG_PATH = Path("docs/paper_log/vs1-insider-density-v4-preregistration.md")
PREREG_BODY_SHA256 = "0b5e8c559743da83affe82549069994b0146b9185d1e968ae7961bc8b6107425"
VERSION = "vs1-v4"
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


@dataclass(frozen=True)
class CrossCheckRule:
    """The TwelveData return cross-check, pinned numerically (v4 §2.3)."""

    min_share_within: float = 0.99  # X
    tolerance: float = 0.0010  # Y = 10 basis points, on |r_tiingo - r_twelvedata|
    min_pairs: int = 250  # N
    max_excluded_share: float = 0.10  # adjustment-date pairs excluded; more than this -> not admitted
    factor_rel_tol: float = 1e-4  # an adjustment-factor move larger than 1 bp relative marks an ex-date pair
    discovery_window: tuple[str, str] = ("2011-11-02", "2019-12-31")
    holdout_window: tuple[str, str] = ("2020-01-01", "2026-06-30")
    provider: str = "TwelveData time_series interval=1day, adjust=all and adjust=none, files only"


CROSSCHECK = CrossCheckRule()


def crosscheck_statistics(
    sessions: Sequence[str],
    tiingo_adj: Mapping[str, float],
    tiingo_close: Mapping[str, float],
    td_adj: Mapping[str, float],
    td_raw: Mapping[str, float],
    rule: CrossCheckRule = CROSSCHECK,
) -> dict:
    """Agreement statistics of one ticker's TIINGO and TwelveData daily returns (v4 §2.3), and the verdict.

    ``sessions``: the benchmark session dates (ISO) of the window, ascending.
    Price maps: date -> close for TIINGO adjusted/raw and TwelveData
    ``adjust=all``/``adjust=none``. Only returns between consecutive benchmark
    sessions that both vendors cover are compared; pairs whose adjustment factor
    (adjusted / raw) moves by more than ``factor_rel_tol`` at either vendor are
    excluded and counted. No event, label or forward return is involved.
    """
    common = [d for d in sessions if d in tiingo_adj and d in td_adj]
    position = {d: i for i, d in enumerate(sessions)}
    pairs = dropped = excluded = within = 0

    def factor(adj: Mapping[str, float], raw: Mapping[str, float], d: str) -> float | None:
        a, r = adj.get(d), raw.get(d)
        return a / r if a is not None and r not in (None, 0) else None

    for s, t in zip(common, common[1:]):
        if position[t] != position[s] + 1:
            dropped += 1
            continue
        moved = False
        for adj, raw in ((tiingo_adj, tiingo_close), (td_adj, td_raw)):
            fs, ft = factor(adj, raw, s), factor(adj, raw, t)
            if fs is None or ft is None or abs(ft / fs - 1.0) > rule.factor_rel_tol:
                moved = True
        if moved:
            excluded += 1
            continue
        r_t = tiingo_adj[t] / tiingo_adj[s] - 1.0
        r_d = td_adj[t] / td_adj[s] - 1.0
        pairs += 1
        within += abs(r_t - r_d) <= rule.tolerance + 1e-15
    considered = pairs + excluded
    share = within / pairs if pairs else 0.0
    excluded_share = excluded / considered if considered else 1.0
    passed = bool(pairs >= rule.min_pairs and share >= rule.min_share_within
                  and excluded_share <= rule.max_excluded_share)
    reason = ("pass" if passed else
              "too_few_pairs" if pairs < rule.min_pairs else
              "adjustment_dominated" if excluded_share > rule.max_excluded_share else
              "disagreement")
    return {"common_dates": len(common), "pairs": pairs, "within_tolerance": within,
            "share_within": share if math.isfinite(share) else 0.0, "excluded_adjustment_pairs": excluded,
            "excluded_share": excluded_share, "dropped_nonconsecutive": dropped,
            "first": common[0] if common else None, "last": common[-1] if common else None,
            "passed": passed, "reason": reason}


@dataclass(frozen=True)
class PriceManifest(v2.PriceManifest):
    """v4's admitted-price contract: TIINGO adj_close only, cross-checked (report sha256 pinned)."""

    crosscheck_report_sha256: str = ""

    def validate(self) -> None:
        super().validate()
        if self.source != PRICE_SOURCE:
            raise ValueError(f"v4 admits only the {PRICE_SOURCE} source, not {self.source!r}")
        if self.series_template != SERIES_TEMPLATE:
            raise ValueError(f"v4 reads only {SERIES_TEMPLATE}")
        if self.basis != BASIS or self.benchmark != BENCHMARK:
            raise ValueError(f"v4 declares basis {BASIS!r} and benchmark {BENCHMARK}")
        if not v1._is_hex64(self.crosscheck_report_sha256):
            raise ValueError("v4 manifests carry the TwelveData cross-check report's sha256")


# --- pins ---------------------------------------------------------------------------------

REGISTRY_LOG = "granular_panel_prereg_v4.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_v4.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_v4.lock"
WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = v1.WITNESS_BRANCH
WITNESS_PATH = v1.canonical_witness_path("vs1-v4")
WITNESS_REF = "refs/vs1-v4-witness/main"

#: The one v4 registration: registered once, locally, on 2026-09-28T03:43:05Z against code
#: acb30ce1, chain head 425047c2... at 2 records. The original lives in the operator's
#: ``Documents/Codex/2026-09-14/wha/outputs/vs1-v4-prereg-registry/``.
REGISTERED_AT: datetime | None = datetime(2026, 9, 28, 3, 43, 5, 115607, tzinfo=timezone.utc)
REGISTERED_CODE_SHA: str | None = "acb30ce12ae5a5c622bb729ddf394da21f35e2c1"
REGISTERED_RECORD_SHA256: tuple[str, str] | None = (
    "4eb1a433fe760e828e34bbd967aa1712a66d18408b80ff608e8c3ad75a01b0ea",  # header
    "425047c26e57eff55928272a88f6c3490da8c431aaaf4e4911d147986c51cac8",  # preregistration (head at 2)
)
REGISTERED_PREREG_SHA256 = PREREG_BODY_SHA256
#: The first line every committed version of the v4 witness file starts with.
REGISTERED_ANCHOR_LINE: bytes | None = (
    b'{"head_sha256":"425047c26e57eff55928272a88f6c3490da8c431aaaf4e4911d147986c51cac8",'
    b'"prev_anchor_sha256":null,"records":2,"run_at":"2026-09-28T03:43:05.115607+00:00"}'
)
#: v4 was superseded by v5 before any price read (the #706 backfill review: source filtering,
#: pull-batch splice check, ticker-reuse price bound).
SUPERSEDED_BY: Mapping[str, Any] | None = {
    "version": "vs1-v5",
    "prereg_sha256": "6242a45f2f21f3429bf20b36bc13d6c1c382f28e1f0970e1f802374079e1b556",
    "registry_head_sha256": None,
}

V3_EARLIER = v2.EarlierVersion(
    version=v3.VERSION, number=3, prereg_sha256=v3.PREREG_BODY_SHA256,
    registry_head_sha256=v3.REGISTERED_RECORD_SHA256[1], witness_path=v3.WITNESS_PATH,
    anchor_line=v3.REGISTERED_ANCHOR_LINE,
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
            "holdout_basis_check": "only at open-holdout, after discovery_frozen",
        },
        "supersedes": [
            {"version": e.version, "prereg_sha256": e.prereg_sha256, "registry_head_sha256": e.registry_head_sha256,
             "status": "superseded before any price read"}
            for e in (v2.V1_EARLIER, v2.V2_EARLIER, V3_EARLIER)
        ],
        "windows": {"discovery_start": v1.DISCOVERY_START, "split": v1.SPLIT, "end": v1.END},
        "promotion_allowed": False,
    }
    return [header, record]


V4 = v2.Harness(v2.Pins(
    version=VERSION, number=4, prereg_path=PREREG_PATH, prereg_body_sha256=PREREG_BODY_SHA256,
    primary_trial=PRIMARY_TRIAL, registry_log=REGISTRY_LOG, registry_anchors=REGISTRY_ANCHORS,
    registry_lock=REGISTRY_LOCK, witness_path=WITNESS_PATH, witness_ref=WITNESS_REF,
    registration_records=registration_records, registered_at=REGISTERED_AT, registered_code_sha=REGISTERED_CODE_SHA,
    registered_record_sha256=REGISTERED_RECORD_SHA256, registered_anchor_line=REGISTERED_ANCHOR_LINE,
    earlier=(v2.V1_EARLIER, v2.V2_EARLIER, V3_EARLIER), superseded_by=SUPERSEDED_BY, holdout_probe_required=True,
), module=sys.modules[__name__])


def prereg_body_sha256(path: Path) -> str:
    return v1.prereg_body_sha256(path)


def run_spec(run_id: str | None = None) -> v1.RunSpec:
    return V4.run_spec(run_id)


def calibration(ledger: list[dict], alpha: float = v1.run_alpha(v1.VS1_RUN_K)) -> dict:
    return V4.calibration(ledger, alpha)


def verdict(payload: dict, checks: list[dict], power: dict | None, contamination: Mapping[str, Any] | None = None) -> dict:
    return V4.verdict(payload, checks, power, contamination)


check_prereg = V4.check_prereg
stage0_power = V4.stage0_power
verify_power = V4.verify_power
discover_panel = V4.discover_panel
check_holdout_request = V4.check_holdout_request
evaluate_panel_holdout = V4.evaluate_panel_holdout
load_price_panel = V4.load_price_panel
registry = V4.registry
register = V4.register
check_offhost = V4.check_offhost
require_supersession = V4.require_supersession
require_witness = V4.require_witness
export_anchors = V4.export_anchors
record_prices_read = V4.record_prices_read
freeze_inputs = V4.freeze_inputs
latest_frozen_inputs = V4.latest_frozen_inputs
open_discovery = V4.open_discovery
resume_discovery = V4.resume_discovery
seal_discovery = V4.seal_discovery
open_holdout = V4.open_holdout
resume_holdout = V4.resume_holdout
seal_holdout = V4.seal_holdout
