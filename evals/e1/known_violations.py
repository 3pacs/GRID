"""Real violations the E1 gates found on main, tracked as strict, named xfails.

A gate is never weakened to pass. When it finds a genuine defect on main, the
failing case is marked ``@known_violation("<ID>")``: a *strict* xfail, so

* the suite stays green while the defect is open and recorded here, and
* the day the code is fixed, the xfail turns into an XPASS **failure** until
  the entry is deleted from this registry (and ``MANIFEST.sha256`` re-pinned).

``test_manifest_guard.py`` checks every ID here is used by at least one gate
and every use names an ID here. Each entry names the code location, what is
wrong and why it matters. Report: ``GRID-E1-GATES-V1-20260930.md``.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest


@dataclass(frozen=True)
class Violation:
    gate: str
    title: str
    location: str
    detail: str


KNOWN: dict[str, Violation] = {
    "E1-V1": Violation(
        gate="look-ahead canary",
        title="state vector SPY fallback reads prices pulled after as_of",
        location="intelligence/regime/state_vector.py::_fetch_spy_prices (raw YF:SPY:close fallback)",
        detail=(
            "The raw fallback calls store.observations.read_window(..., as_of=as_of) with no "
            "as_of_ts, so the latest vintage wins even when it was pulled after as_of. A later "
            "re-pull that restates closes (a basis change, a repaired contamination) silently "
            "rewrites spy_momentum / spy_rsi of every historical vector, and a planted restatement "
            "equal to the next-period return shows up in spy_rsi at the decision date. Applies "
            "whenever the resolved spy_full feature is unavailable (always, before the re-resolve)."
        ),
    ),
    "E1-V2": Violation(
        gate="look-ahead canary",
        title="state vector insider sentiment counts Form 4s filed after as_of",
        location="intelligence/regime/state_vector.py::_get_insider_sentiment (_INSIDER_SENTIMENT_SQL)",
        detail=(
            "INSIDER:* rows are dated by transaction date (ingestion/altdata/insider_filings.py) "
            "and the batched read bounds only obs_date to [as_of - 30d, as_of]; nothing bounds "
            "pull_timestamp or the filing date. Form 4s are filed up to two business days (late "
            "filers: weeks) after the trade, so a historical vector (the regime history rebuild) "
            "counts filings that were not public at as_of, and appending late filings changes it."
        ),
    ),
    "E1-V3": Violation(
        gate="honest success (registry completeness)",
        title="registered pullers with no callable pull method (bls, wiki_history, pumpfun)",
        location="ingestion/smart_scheduler.py::PULLER_REGISTRY entries 'bls' (BLSPuller.pull_all), "
                 "'wiki_history' (WikiHistoryPuller.pull_all), 'pumpfun' (PumpFunPuller.pull_all)",
        detail=(
            "The registry names a pull method the class does not define. bls and wiki_history carry a "
            "hold_reason, so the scheduler reports SKIPPED without importing them (honest, but the "
            "source can never be pulled). pumpfun is NOT held: every 6-hour run raises AttributeError "
            "(PumpFunPuller only defines pull_aggregate_signals), is logged FAILED, backs off, and the "
            "PumpFun source can never become fresh."
        ),
    ),
    "E1-V4": Violation(
        gate="provenance",
        title="raw_series writers that omit pull_status, write non-schema columns or rewrite stored rows",
        location="api/routers/watchlist_helpers.py (price cache), ingestion/altdata/offshore_leaks.py, "
                 "ingestion/social_sentiment.py, ingestion/wiki_history.py, scripts/full_universe_pull.py",
        detail=(
            "pull_status is NOT NULL with no default, so these inserts cannot record how a value was "
            "obtained and fail on the production schema (dead writers whose errors are swallowed). "
            "offshore_leaks and full_universe_pull also write a release_date column raw_series does not "
            "have; watchlist_helpers and full_universe_pull use ON CONFLICT ... DO UPDATE SET value "
            "(watchlist_helpers also pull_timestamp = NOW()), i.e. they would rewrite a stored vintage "
            "in place instead of appending one."
        ),
    ),
    "E1-V5": Violation(
        gate="reproducibility",
        title="real-panel scan summary.json embeds wall-clock timings",
        location="scripts/run_real_panel_scan.py::scan (summary keys read_seconds, total_seconds)",
        detail=(
            "The scan's report is not a pure function of inputs and code: summary.json carries the "
            "host's elapsed read/total seconds, so the same data, code SHA and seed give different "
            "bytes on a slower or loaded host. The ledger artifacts (trial-ledger.csv, "
            "discovery-frozen.json, holdout-result.json, frozen-candidates.json) are byte-identical; "
            "timings belong in a separate, unhashed run log."
        ),
    ),
    "E1-V6": Violation(
        gate="honest success (registry completeness) / provenance",
        title="coingecko registry entry writes resolved_series directly, with no raw_series provenance",
        location="ingestion/coingecko.py::CoinGeckoPuller._save_to_db / pull_history",
        detail=(
            "CoinGeckoPuller has no SOURCE_NAME and never writes the 'coingecko' source_catalog row the "
            "scheduler marks fresh. It inserts prices straight into resolved_series (no source_id, "
            "pull_timestamp or pull_status; release_date = vintage_date = obs_date, so pull_history "
            "backdates history fetched today as vintages GRID held on each past day) and uses ON "
            "CONFLICT ... DO UPDATE SET value, so a re-pull rewrites a stored vintage in place and the "
            "resolver's multi-source conflict checks never see these values."
        ),
    ),
}


def known_violation(vid: str):
    """Strict xfail for a registered violation (unknown IDs fail at import).

    Only an ``AssertionError`` (the gate's own check) counts as the known
    failure; any other exception is a real error, not the tracked violation.
    """
    v = KNOWN[vid]
    return pytest.mark.xfail(strict=True, raises=AssertionError, reason=f"{vid} [{v.gate}] {v.title} -- {v.location}")
