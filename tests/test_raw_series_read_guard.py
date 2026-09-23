"""Guard: no new direct ``raw_series`` reads without a ``pull_status`` filter.

Background (griddb, measured 2026-09-17, read-only): several pullers record
a failed pull as ``pull_status='FAILED', value=0, obs_date=today``, and every
re-pull appends another vintage for the same ``obs_date``. A reader that
selects from ``raw_series`` by ``obs_date`` alone therefore serves a literal
``0`` on failure days and an arbitrary vintage on revision days.
``store/observations.py`` is the sanctioned reader; ``normalization/resolver``
already filters ``SUCCESS`` when building ``resolved_series``.

This test freezes the *legacy* set of unfiltered reads that existed when the
guard was introduced. Counts may only go down. A new unfiltered read in any
analytical package fails the build with the file and the fix.

What counts as "filtered": the string ``pull_status`` within the same SQL
statement (200 chars before / 600 after the ``FROM raw_series`` token).
Ingestion modules (``ingestion/``) are writers and are out of scope;
``scripts/`` and ``tests/`` are out of scope.
"""

from __future__ import annotations

import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parent.parent
SCANNED_ROOTS = ("api", "intelligence", "analysis", "physics", "oracle", "trading",
                 "valuation", "features", "alerts", "store")
_FROM_RAW = re.compile(r"from\s+raw_series\b", re.I)

# Legacy baseline on 2026-09-17 (branch fix/raw-series-success-vintage-reads).
# Migrate a file onto store.observations and lower (or delete) its entry.
LEGACY_UNFILTERED_READS: dict[str, int] = {
    "alerts/email.py": 1,
    "analysis/capital_flows.py": 8,
    "analysis/money_flow.py": 1,
    "analysis/thesis_scorer.py": 14,
    "api/routers/dad.py": 3,
    "api/routers/flows.py": 1,
    "api/routers/system.py": 6,
    "api/routers/tradingview.py": 1,
    "api/routers/watchlist_analysis.py": 1,
    "api/routers/watchlist_helpers.py": 1,
    "intelligence/actor_discovery.py": 22,
    "intelligence/contagion_backtest.py": 1,
    "intelligence/dollar_flows.py": 7,
    "intelligence/earnings_transcript_analyzer.py": 3,
    "intelligence/fundamental_divergence.py": 3,
    "intelligence/global_levers.py": 1,
    "intelligence/news_impact.py": 1,
    "intelligence/news_momentum.py": 3,
    "intelligence/pattern_library.py": 1,
    "intelligence/sec_filing_extractor.py": 1,
    "intelligence/sentiment_scorer.py": 1,
    "intelligence/signal_extractor.py": 1,
    "intelligence/signal_health_monitor.py": 4,
    "valuation/derivatives_support.py": 4,
    "valuation/intrinsic.py": 3,
}


def _unfiltered_reads() -> dict[str, int]:
    found: dict[str, int] = {}
    for root in SCANNED_ROOTS:
        for f in (REPO / root).rglob("*.py"):
            text = f.read_text(encoding="utf-8", errors="ignore")
            n = 0
            for m in _FROM_RAW.finditer(text):
                seg = text[max(0, m.start() - 200): m.end() + 600]
                if "pull_status" not in seg:
                    n += 1
            if n:
                found[f.relative_to(REPO).as_posix()] = n
    return found


def test_no_new_unfiltered_raw_series_reads():
    found = _unfiltered_reads()
    regressions = {
        path: (n, LEGACY_UNFILTERED_READS.get(path, 0))
        for path, n in found.items()
        if n > LEGACY_UNFILTERED_READS.get(path, 0)
    }
    assert not regressions, (
        "New direct raw_series read(s) without a pull_status filter "
        f"(file: found > allowed): {regressions}. Read through "
        "store.observations (read_latest / read_latest_n / read_window) or add "
        "`AND pull_status = 'SUCCESS'` plus a pull_timestamp tiebreak. "
        "See tests/test_store_observations.py for why."
    )


def test_baseline_entries_are_still_accurate():
    """Keep the allowlist honest: an entry that has been fixed must be lowered."""
    found = _unfiltered_reads()
    stale = {
        path: (found.get(path, 0), allowed)
        for path, allowed in LEGACY_UNFILTERED_READS.items()
        if found.get(path, 0) < allowed
    }
    assert not stale, (
        f"These files now have fewer unfiltered reads than the baseline allows "
        f"(found, allowed): {stale}. Lower the baseline so the improvement sticks."
    )
