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
#
# Updated 2026-09-27 (branch fix/raw-series-readers-success-only-20260927):
# ~40 production readers across api/, analysis/, intelligence/, valuation/,
# ingestion/, ollama/, alpha_research/ and scripts/ were audited against
# migration #671 (raw_series pull_status gets a QUARANTINED value for
# wrong-instrument price batches). Every analytical read of `value` now
# filters `pull_status = 'SUCCESS'`; the entries remaining below are health/
# freshness/audit reads (recent pull activity, row counts, freshness
# timestamps) that intentionally look at every status — see the inline
# comments at each site for why. `intelligence/signal_health_monitor.py`'s
# count dropped from 4 to 2: its row_count/nan_count pull-activity probe
# stays unfiltered by design, but its latest_value/history_mean/history_std
# queries (which feed anomaly thresholds) now require SUCCESS.
#
# Corrected 2026-09-27 (branch fix/pre-retraction-and-679-followups-20260927,
# #679 review follow-up): the paragraph above was itself stale/incomplete —
# `_fetch_series_stats`'s remaining unfiltered "FROM raw_series" in the 2026-
# 09-27 combined query was NOT only the row_count/nan_count probe; that same
# query's MAX(obs_date) (-> last_observation -> days_since_last -> staleness)
# was riding along unfiltered too, so a series whose newest rows were
# QUARANTINED or FAILED could still read as freshly pulled. That is now
# split into its own query with an explicit `pull_status = 'SUCCESS'` filter.
# The count stays at 2 (last_observation's occurrence is now filtered, but
# row_count/nan_count -- unfiltered by design, unchanged -- now has its own
# unfiltered occurrence instead of sharing last_observation's), so no baseline
# number changed here; this note exists only so the next reader does not
# mistake "count stayed 2" for "nothing changed".
LEGACY_UNFILTERED_READS: dict[str, int] = {
    "alerts/email.py": 1,
    "analysis/money_flow.py": 1,
    "analysis/thesis_scorer.py": 2,
    "api/routers/dad.py": 1,
    "api/routers/flows.py": 1,
    "api/routers/system.py": 6,
    "intelligence/actor_discovery.py": 1,
    "intelligence/dollar_flows.py": 3,
    "intelligence/earnings_transcript_analyzer.py": 1,
    "intelligence/global_levers.py": 1,
    "intelligence/pattern_library.py": 1,
    "intelligence/sentiment_scorer.py": 1,
    "intelligence/signal_health_monitor.py": 2,
    "valuation/intrinsic.py": 1,
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
