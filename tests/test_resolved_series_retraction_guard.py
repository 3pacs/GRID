"""Guard: no new direct ``resolved_series`` reads that ignore retractions.

Background: ``resolved_series_retractions``
(migrations/versions/resolved_retractions_20260927.py) marks resolved rows that
hold a wrong-instrument value with no clean replacement (GRID-RERESOLVE-PLAN-
20260927: 70,633 LATEST_AS_OF and 76,204 FIRST_RELEASE cells). A retracted row
must read as "no data" to point-in-time readers from ``retracted_at`` on (a
date ``as_of``: from the retraction's UTC date on), and stay visible to
``as_of`` dates before that day, so those replays are reproduced exactly.

``store/pit.py`` (``PITStore.get_pit`` / ``get_feature_matrix`` /
``get_latest_values``) is the sanctioned reader and honours retractions. The
direct PIT readers that already filtered ``release_date <= as_of`` were
migrated with the table (``alpha_research/conviction_scorer.py``
``_load_latest``/``_load_price``, ``alpha_research/data/panel_builder.py``
``build_volume_panel``, ``alpha_research/signals/credit_cycle.py``,
``oracle/prediction_context.py``).

This test freezes the *legacy* direct reads that do not honour retractions
yet. Counts may only go down. A new direct read in an analytical package fails
the build: read through ``store.pit.PITStore``, or add the anti-join::

    AND NOT EXISTS (
        SELECT 1 FROM resolved_series_retractions rr
        WHERE rr.feature_id = rs.feature_id
          AND rr.obs_date = rs.obs_date
          AND rr.vintage_date = rs.vintage_date
          AND rr.retracted_at <= :retraction_cutoff
    )

with ``retraction_cutoff=store.pit.retraction_cutoff(as_of)``.

What counts as "honouring": the string ``resolved_series_retractions`` within
the same SQL statement (300 chars before / 900 after the
``FROM|JOIN resolved_series`` token). ``normalization/`` (the resolver, the
only writer), ``ingestion/``, ``scripts/`` and ``tests/`` are out of scope.
The baseline mixes reads that are not point-in-time value reads at all
(freshness/row counts, health and audit, universe selection, contract
receipts) with live/API reads that should migrate; see the PR that added this
file for the classification.
"""

from __future__ import annotations

import pathlib
import re
from datetime import date, datetime, timedelta, timezone

from store.pit import retraction_cutoff

REPO = pathlib.Path(__file__).resolve().parent.parent
SCANNED_ROOTS = (
    "alerts", "alpha_research", "analysis", "api", "astrogrid_api", "backtest",
    "discovery", "evaluation", "features", "inference", "intelligence", "ollama",
    "oracle", "orchestration", "paper_log", "physics", "store", "trading",
    "validation", "valuation",
)
_READ = re.compile(r"\b(?:from|join)\s+resolved_series\b", re.I)
_TOKEN = "resolved_series_retractions"

# Legacy baseline on 2026-09-27 (branch feat/resolved-series-retractions-20260927).
# Migrate a file onto store.pit (or add the anti-join) and lower its entry.
LEGACY_READS_WITHOUT_RETRACTIONS: dict[str, int] = {
    "alpha_research/conviction_scorer.py": 1,
    "alpha_research/data/panel_builder.py": 3,
    "alpha_research/heartbeat.py": 1,
    "analysis/astro_correlations.py": 2,
    "analysis/backtest_scanner.py": 2,
    "analysis/capital_flows.py": 4,
    "analysis/flow_thesis_data.py": 4,
    "analysis/hypothesis_tester.py": 3,
    "analysis/money_flow.py": 7,
    "analysis/money_flow_engine/helpers.py": 2,
    "analysis/money_flow_engine/layer_credit.py": 1,
    "analysis/research_agent.py": 3,
    "analysis/taxonomy_audit.py": 7,
    "analysis/vol_surface.py": 1,
    "api/routers/astrogrid_celestial.py": 5,
    "api/routers/astrogrid_helpers.py": 4,
    "api/routers/chat.py": 4,
    "api/routers/dad.py": 3,
    "api/routers/flows.py": 6,
    "api/routers/forecasts.py": 3,
    "api/routers/intelligence_risk.py": 14,
    "api/routers/price_alerts.py": 1,
    "api/routers/regime.py": 4,
    "api/routers/signals.py": 3,
    "api/routers/system.py": 4,
    "api/routers/ten_year_portfolio.py": 1,
    "api/routers/watchlist_analysis.py": 4,
    "api/routers/watchlist_core.py": 2,
    "api/routers/watchlist_helpers.py": 3,
    "api/routers/watchlist_overview.py": 5,
    "astrogrid_api/astrogrid_celestial.py": 5,
    "astrogrid_api/astrogrid_helpers.py": 4,
    "discovery/changepoint_detector.py": 1,
    "discovery/clustering.py": 1,
    "inference/timesfm_service.py": 3,
    "intelligence/adapters/feature_adapter.py": 2,
    "intelligence/attention_anomaly.py": 1,
    "intelligence/codebase_context.py": 1,
    "intelligence/cross_reference.py": 2,
    "intelligence/dollar_flows.py": 3,
    "intelligence/forensics.py": 1,
    "intelligence/freshness_guard.py": 1,
    "intelligence/post_query_scanner.py": 1,
    "intelligence/prediction_calibration.py": 4,
    "intelligence/resolution_audit.py": 11,
    "intelligence/scheduler.py": 1,
    "intelligence/sleuth.py": 5,
    "intelligence/trend_tracker.py": 2,
    "ollama/celestial_briefing.py": 3,
    "ollama/market_briefing.py": 4,
    "oracle/astrogrid_universe.py": 1,
    "oracle/claim_verifier.py": 3,
    "oracle/engine.py": 1,
    "oracle/psi_model.py": 2,
    "orchestration/grid_worker.py": 3,
    "orchestration/llm_task_workers.py": 5,
    "physics/verify.py": 4,
    "store/astrogrid.py": 5,
    "trading/options_recommender.py": 2,
    "trading/signal_executor.py": 2,
}

# Readers that must keep honouring retractions (a refactor that drops the
# anti-join from one of these fails here even if the file count stays low).
MIGRATED_READERS: dict[str, int] = {
    "store/pit.py": 2,
    "alpha_research/conviction_scorer.py": 2,
    "alpha_research/data/panel_builder.py": 1,
    "alpha_research/signals/credit_cycle.py": 1,
    "oracle/prediction_context.py": 1,
}


def _scan() -> tuple[dict[str, int], dict[str, int]]:
    without: dict[str, int] = {}
    with_: dict[str, int] = {}
    for root in SCANNED_ROOTS:
        base = REPO / root
        if not base.exists():
            continue
        for f in base.rglob("*.py"):
            text = f.read_text(encoding="utf-8", errors="ignore")
            n_without = n_with = 0
            for m in _READ.finditer(text):
                seg = text[max(0, m.start() - 300): m.end() + 900]
                if _TOKEN in seg:
                    n_with += 1
                else:
                    n_without += 1
            rel = f.relative_to(REPO).as_posix()
            if n_without:
                without[rel] = n_without
            if n_with:
                with_[rel] = n_with
    return without, with_


def test_no_new_resolved_series_reads_without_retractions():
    found, _ = _scan()
    regressions = {
        path: (n, LEGACY_READS_WITHOUT_RETRACTIONS.get(path, 0))
        for path, n in found.items()
        if n > LEGACY_READS_WITHOUT_RETRACTIONS.get(path, 0)
    }
    assert not regressions, (
        "New direct resolved_series read(s) that ignore resolved_series_retractions "
        f"(file: found > allowed): {regressions}. Read through store.pit.PITStore, "
        "or add the NOT EXISTS anti-join shown in this module's docstring."
    )


def test_baseline_entries_are_still_accurate():
    """Keep the allowlist honest: a migrated read must lower its entry."""
    found, _ = _scan()
    stale = {
        path: (found.get(path, 0), allowed)
        for path, allowed in LEGACY_READS_WITHOUT_RETRACTIONS.items()
        if found.get(path, 0) < allowed
    }
    assert not stale, (
        f"These files now have fewer retraction-blind reads than the baseline "
        f"allows (found, allowed): {stale}. Lower the baseline so it sticks."
    )


def test_migrated_readers_still_honour_retractions():
    _, honouring = _scan()
    missing = {
        path: (honouring.get(path, 0), need)
        for path, need in MIGRATED_READERS.items()
        if honouring.get(path, 0) < need
    }
    assert not missing, f"Reads lost their retraction anti-join (found, need): {missing}"


def test_retraction_cutoff_for_a_date_is_the_end_of_that_utc_day():
    cut = retraction_cutoff(date(2026, 9, 27))
    assert cut == datetime(2026, 9, 27, 23, 59, 59, 999999, tzinfo=timezone.utc)
    # A retraction at the first instant of the next day is not yet visible.
    assert datetime(2026, 9, 28, tzinfo=timezone.utc) > cut


def test_retraction_cutoff_for_a_timestamp_is_the_instant_itself():
    aware = datetime(2026, 9, 27, 14, 30, tzinfo=timezone(timedelta(hours=-4)))
    assert retraction_cutoff(aware) is aware
    naive = datetime(2026, 9, 27, 14, 30)
    assert retraction_cutoff(naive) == naive.replace(tzinfo=timezone.utc)
