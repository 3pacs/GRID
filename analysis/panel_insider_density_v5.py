"""VS1 v5 panel harness: v4 plus source filtering, a pull-batch splice check and a ticker-reuse price bound.

Pre-registration: ``docs/paper_log/vs1-insider-density-v5-preregistration.md``
(body sha256 pinned in :data:`PREREG_BODY_SHA256`). v5 is VS1 v4
(``analysis.panel_insider_density_v4``: TIINGO adj_close only, TwelveData return
cross-check X=99%/Y=10bp/N=250, binding post-admission Stage-0, holdout basis
check at open-holdout) plus three rules from the review of the #706 backfill,
all basis/coverage metadata, no outcome:

* reads are source-filtered to TIINGO; other-source rows under the same series
  id are ignored, counted and reported, not disqualifying;
* a pull-batch splice check at every batch boundary in the read window
  (:func:`splice_check`, tolerance :data:`SPLICE_TOL`);
* a ticker-reuse price bound: closes used only from L_T = max(Tiingo meta
  startDate, the issuer's first filing naming T) (:func:`listing_bound`,
  :func:`first_naming_dates`, blanking in :func:`build_trial_panels`), and a
  Tiingo-metadata entity check (:func:`name_match`).

Own pinned registry and canonical witness
``05-GRID/Paper-Log/vs1/granular_panel_prereg_v5.anchors.jsonl``; v1-v4 refuse.
Nothing here is a trading signal.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
from analysis import panel_insider_density_v4 as v4
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
PREREG_PATH = Path("docs/paper_log/vs1-insider-density-v5-preregistration.md")
PREREG_BODY_SHA256 = "6242a45f2f21f3429bf20b36bc13d6c1c382f28e1f0970e1f802374079e1b556"
VERSION = "vs1-v5"
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
    """v5's admitted-price contract: v4's (TIINGO adj_close only, cross-checked) plus L_T and Tiingo meta."""

    crosscheck_report_sha256: str = ""
    tiingo_meta_report_sha256: str = ""

    def validate(self) -> None:
        super().validate()
        if self.source != PRICE_SOURCE:
            raise ValueError(f"v5 admits only the {PRICE_SOURCE} source, not {self.source!r}")
        if self.series_template != SERIES_TEMPLATE:
            raise ValueError(f"v5 reads only {SERIES_TEMPLATE}")
        if self.basis != BASIS or self.benchmark != BENCHMARK:
            raise ValueError(f"v5 declares basis {BASIS!r} and benchmark {BENCHMARK}")
        if not v1._is_hex64(self.crosscheck_report_sha256):
            raise ValueError("v5 manifests carry the TwelveData cross-check report's sha256")
        if not v1._is_hex64(self.tiingo_meta_report_sha256):
            raise ValueError("v5 manifests carry the Tiingo metadata report's sha256")
        listed = dict(self.listed_from)
        missing = [t for t in self.admitted if t != self.benchmark and t not in listed]
        if missing:
            raise ValueError(f"v5 manifests carry listed_from (L_T) for every admitted ticker; missing {missing[:10]}")


# --- v5 §2.3 additions: splice check, ticker-reuse price bound, entity check ---------------------------

#: Pull-batch splice tolerance on the adjustment factor (adj_close / close), relative.
SPLICE_TOL = 1e-4
#: Name-match rule (entity check against Tiingo metadata).
NAME_JACCARD_MIN = 0.5
NAME_STOP_TOKENS = frozenset({
    "INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY", "LTD", "LIMITED", "PLC", "LLC", "LP", "LLP",
    "NV", "SA", "AG", "SE", "THE", "HOLDINGS", "HOLDING", "GROUP", "CLASS", "A", "B", "C", "DE", "NEW",
})


def splice_check(
    sessions: Sequence[str],
    batch: Mapping[str, str],
    tiingo_adj: Mapping[str, float],
    tiingo_close: Mapping[str, float],
    td_adj: Mapping[str, float] | None = None,
    td_raw: Mapping[str, float] | None = None,
    tol: float = SPLICE_TOL,
) -> dict:
    """v5 §2.3 pull-batch splice check for one ticker (no return, label or event is involved).

    ``batch``: session date -> pull batch (UTC pull date) of the selected rows (a
    date whose ``adj_close`` and ``close`` rows come from different batches is
    given as a joined label, e.g. ``"2026-04-07|2026-09-28"``). A boundary is a
    pair of consecutive sessions with different batch labels; it passes iff the
    TIINGO factor step is within ``tol`` or matches TwelveData's step within ``tol``.
    """
    present = [d for d in sessions if d in tiingo_adj and d in tiingo_close and d in batch]
    position = {d: i for i, d in enumerate(sessions)}
    boundaries, failed = [], []
    for s, t in zip(present, present[1:]):
        if position[t] != position[s] + 1 or batch[s] == batch[t]:
            continue
        step = (tiingo_adj[t] / tiingo_close[t]) / (tiingo_adj[s] / tiingo_close[s])
        ok = abs(step - 1.0) <= tol
        if not ok and td_adj is not None and td_raw is not None and all(
                d in td_adj and d in td_raw and td_raw[d] for d in (s, t)):
            td_step = (td_adj[t] / td_raw[t]) / (td_adj[s] / td_raw[s])
            ok = abs(step / td_step - 1.0) <= tol
        boundaries.append({"pair": [s, t], "step": step, "passed": ok})
        if not ok:
            failed.append([s, t])
    return {"boundaries": len(boundaries), "failed": failed, "passed": not failed, "detail": boundaries}


def _name_tokens(name: str) -> list[str]:
    import re as _re

    tokens = _re.sub(r"[^A-Z0-9]+", " ", str(name or "").upper()).split()
    return [t for t in tokens if t not in NAME_STOP_TOKENS]


def name_match(sec_name: str, tiingo_name: str) -> bool:
    """v5 §2.3 entity check: token-set Jaccard >= 0.5, or one joined token string prefixes the other."""
    a, b = _name_tokens(sec_name), _name_tokens(tiingo_name)
    if not a or not b:
        return False
    sa, sb = set(a), set(b)
    if len(sa & sb) / len(sa | sb) >= NAME_JACCARD_MIN:
        return True
    ja, jb = " ".join(a), " ".join(b)
    return ja.startswith(jb) or jb.startswith(ja)


def first_naming_dates(admission: Admission, universe: pd.DataFrame) -> dict[str, str]:
    """F_{e,T}: per price ticker, the filing date of the issuer's first Section 16 filing naming a current ticker."""
    first = (admission.tickers[admission.tickers["match"]]
             .groupby("issuer_cik")["known_at"].min())
    out = {}
    for row in universe.itertuples(index=False):
        known = first.get(int(row.cik))
        if known is not None and not pd.isna(known):
            out[row.ticker] = pd.Timestamp(known).tz_convert(v1.NEW_YORK).date().isoformat()
    return out


def listing_bound(tiingo_start: str | None, first_naming: str | None) -> str | None:
    """L_T = max(Tiingo startDate, first filing naming T); None when either is unknown (not admitted)."""
    if not tiingo_start or not first_naming:
        return None
    return max(str(tiingo_start)[:10], str(first_naming)[:10])


def build_trial_panels(events, admission, universe, prices, window):
    """v2's trial panels with closes before each ticker's ``listed_from`` (L_T) blanked first (v5 §2.3)."""
    return v2.build_trial_panels(events, admission, universe, prices, window, blank_before_listing=True)


# --- pins ---------------------------------------------------------------------------------

REGISTRY_LOG = "granular_panel_prereg_v5.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_v5.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_v5.lock"
WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = v1.WITNESS_BRANCH
WITNESS_PATH = v1.canonical_witness_path("vs1-v5")
WITNESS_REF = "refs/vs1-v5-witness/main"

REGISTERED_AT: datetime | None = None
REGISTERED_CODE_SHA: str | None = None
REGISTERED_RECORD_SHA256: tuple[str, str] | None = None
REGISTERED_PREREG_SHA256 = PREREG_BODY_SHA256
REGISTERED_ANCHOR_LINE: bytes | None = None
SUPERSEDED_BY: Mapping[str, Any] | None = None

V3_EARLIER = v4.V3_EARLIER
V4_EARLIER = v2.EarlierVersion(
    version=v4.VERSION, number=4, prereg_sha256=v4.PREREG_BODY_SHA256,
    registry_head_sha256=v4.REGISTERED_RECORD_SHA256[1], witness_path=v4.WITNESS_PATH,
    anchor_line=v4.REGISTERED_ANCHOR_LINE,
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
            "ticker_reuse_bound": "closes used only on dates >= max(Tiingo meta startDate, first own filing naming T)",
            "entity_check": {"name_jaccard_min": NAME_JACCARD_MIN, "stop_tokens": sorted(NAME_STOP_TOKENS),
                             "or": "one joined token string prefixes the other"},
        },
        "supersedes": [
            {"version": e.version, "prereg_sha256": e.prereg_sha256, "registry_head_sha256": e.registry_head_sha256,
             "status": "superseded before any price read"}
            for e in (v2.V1_EARLIER, v2.V2_EARLIER, V3_EARLIER, V4_EARLIER)
        ],
        "windows": {"discovery_start": v1.DISCOVERY_START, "split": v1.SPLIT, "end": v1.END},
        "promotion_allowed": False,
    }
    return [header, record]


V5 = v2.Harness(v2.Pins(
    version=VERSION, number=5, prereg_path=PREREG_PATH, prereg_body_sha256=PREREG_BODY_SHA256,
    primary_trial=PRIMARY_TRIAL, registry_log=REGISTRY_LOG, registry_anchors=REGISTRY_ANCHORS,
    registry_lock=REGISTRY_LOCK, witness_path=WITNESS_PATH, witness_ref=WITNESS_REF,
    registration_records=registration_records, registered_at=REGISTERED_AT, registered_code_sha=REGISTERED_CODE_SHA,
    registered_record_sha256=REGISTERED_RECORD_SHA256, registered_anchor_line=REGISTERED_ANCHOR_LINE,
    earlier=(v2.V1_EARLIER, v2.V2_EARLIER, V3_EARLIER, V4_EARLIER), superseded_by=SUPERSEDED_BY,
    holdout_probe_required=True,
), module=sys.modules[__name__])


def prereg_body_sha256(path: Path) -> str:
    return v1.prereg_body_sha256(path)


def run_spec(run_id: str | None = None) -> v1.RunSpec:
    return V5.run_spec(run_id)


def calibration(ledger: list[dict], alpha: float = v1.run_alpha(v1.VS1_RUN_K)) -> dict:
    return V5.calibration(ledger, alpha)


def verdict(payload: dict, checks: list[dict], power: dict | None, contamination: Mapping[str, Any] | None = None) -> dict:
    return V5.verdict(payload, checks, power, contamination)


check_prereg = V5.check_prereg
stage0_power = V5.stage0_power
verify_power = V5.verify_power
discover_panel = V5.discover_panel
check_holdout_request = V5.check_holdout_request
evaluate_panel_holdout = V5.evaluate_panel_holdout
load_price_panel = V5.load_price_panel
registry = V5.registry
register = V5.register
check_offhost = V5.check_offhost
require_supersession = V5.require_supersession
require_witness = V5.require_witness
export_anchors = V5.export_anchors
record_prices_read = V5.record_prices_read
freeze_inputs = V5.freeze_inputs
latest_frozen_inputs = V5.latest_frozen_inputs
open_discovery = V5.open_discovery
resume_discovery = V5.resume_discovery
seal_discovery = V5.seal_discovery
open_holdout = V5.open_holdout
resume_holdout = V5.resume_holdout
seal_holdout = V5.seal_holdout
