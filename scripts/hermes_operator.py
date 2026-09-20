#!/usr/bin/env python3
"""
GRID Hermes Operator — autonomous 24/7 self-healing daemon.

Hermes (the local llama.cpp model) runs continuously, performing:

1. HEALTH MONITOR — checks DB, data freshness, LLM availability every cycle
2. PULL FIXER — detects failed ingestion pulls, diagnoses why, retries with fixes
3. PIPELINE RUNNER — runs the full pipeline on schedule (or when data arrives)
4. DATA GATHERER — fills historical gaps, pulls missing series
5. AUTORESEARCH — generates and tests hypotheses when system is healthy
6. SELF-DIAGNOSTICS — reads its own error logs, proposes and applies fixes

Each cycle:
  - Check system health
  - Fix anything broken
  - Run any due scheduled work
  - If healthy, gather data or research
  - Log everything to analytical_snapshots + server_log

Usage:
    python scripts/hermes_operator.py                # run forever
    python scripts/hermes_operator.py --once          # single cycle
    python scripts/hermes_operator.py --dry-run       # diagnose only, don't fix

NOTE (hermes/scheduler split, verified 2026-04-13):
    `ingestion/scheduler.py` is the canonical per-puller scheduler (~48 pullers
    registered there, see `scheduler.build_puller_list`). This operator layers
    ADDITIONAL intelligence-side tasks on top:
        - `intelligence.icij_linker.link_actors`
        - `intelligence.milestone_tracker.scan_all_tickers`
        - `intelligence.obsidian_agent.run_agent_cycle`
        - `intelligence.attention_anomaly.get_alerts`
        - `intelligence.actor_researcher` + `actor_discovery`
    These are intentionally NOT in `ingestion/scheduler.py` because they run
    AFTER ingestion (they consume the raw data the scheduler just pulled).
    Do not "reconcile" by deleting them or by moving them into scheduler.py —
    they are the intentional hermes-only half of the split. Previous drift
    references to `power_mapper` and `gdelt_news` have been removed; the
    three above are the live surface.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, NamedTuple

# Ensure grid/ is on sys.path
_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from loguru import logger as log
from sqlalchemy import text  # used by run_cycle DB writes


# ─── Configuration ───────────────────────────────────────────────────

CYCLE_INTERVAL_SECONDS = 300          # 5 minutes between cycles
CYCLE_TIMEOUT_SECONDS = 4500          # 75 min max per cycle (oracle dominates one in N cycles)
                                       # (per-step timeouts kick in earlier; this
                                       # is a safety net for unforeseen hangs)
# The per-cycle pool_stats log (below, in run_cycle) only fires when a cycle
# completes -- a stuck or long-running cycle (up to CYCLE_TIMEOUT_SECONDS)
# would otherwise produce zero visibility into connections it opened and
# never returned. This poll runs on its own thread, independent of cycle
# completion, so an outstanding checkout is visible well before -- or even
# without -- a cycle ever finishing.
OUTSTANDING_CHECKOUT_POLL_SECONDS = 60
PIPELINE_INTERVAL_HOURS = 6           # run full pipeline every 6 hours
DATA_FRESHNESS_THRESHOLD_HOURS = 26   # flag stale sources after 26h
MAX_PULL_RETRIES = 3                  # retry failed pulls up to 3 times
AUTORESEARCH_MAX_ITER = 5             # hypothesis iterations per cycle
# Autoresearch had NO per-step timeout at all before this task (see
# docs/handoffs/2026-09-18/fable-w4-research-states.md's "Activation
# condition" section) — the cycle-6 gate called maybe_run_autoresearch()
# directly inside a plain try/except, never through _run_with_timeout.
# Duplicated from scripts/autoresearch.py's own AUTORESEARCH_TIMEOUT_SECONDS
# (same env var, same default) for the same reason AUTORESEARCH_MAX_ITER is
# duplicated in scripts/hermes_fixers.py: avoiding a circular import, since
# scripts/autoresearch.py is only ever imported lazily, inside the function
# that calls it. Conservative default: one iteration can chain an LLM
# generate call, a walk-forward backtest, and an LLM critique call, so this
# is sized like the other multi-call LLM steps below (ORACLE_CYCLE_TIMEOUT_
# SECONDS=4000 for 41 tickers), not the single-call steps (120-240s).
AUTORESEARCH_TIMEOUT_SECONDS = int(os.getenv("GRID_AUTORESEARCH_TIMEOUT_SECONDS", "1800"))
HERMES_TEMPERATURE = 0.3              # LLM temperature for diagnostics
# git-sync committed analytical outputs into the repo (data-exhaust pollution) and the pushes were failing; disabled by default. Set GRID_HERMES_GIT_SYNC=true only with a proper external sync target.
GIT_SYNC_ENABLED = os.getenv("GRID_HERMES_GIT_SYNC", "false").lower() in ("1", "true", "yes")  # pull/push on each cycle
GIT_REMOTE = "origin"
GIT_BRANCH = "main"

# Per-source cooldown: don't retry a source more often than this
SOURCE_COOLDOWN_MINUTES = 30          # min minutes between retries of same source
SOURCE_MAX_CONSECUTIVE_FAILS = 5      # after N consecutive fails, extend cooldown to 6h
TIMEOUT_BLACKLIST_HOURS = 24          # blacklist sources that cause cycle timeouts

# Per-step timeouts — caps how long a single step can hold up the cycle.
# Hung LLM calls used to consume the full 900s cycle budget; these caps + the
# cooldown blacklist break the loop after a single timeout.
ORACLE_CYCLE_TIMEOUT_SECONDS = 4000           # oracle.run_cycle: 41 tickers x ~80s + headroom (was 300, caused 24h blacklist loop)
SIGNAL_CLASSIFICATION_TIMEOUT_SECONDS = 120   # gemma micro classifier batch
ANOMALY_NARRATION_TIMEOUT_SECONDS = 90        # gemma micro anomaly narrator
KNOWLEDGE_MAP_TIMEOUT_SECONDS = 120           # gemma micro knowledge mapper
DIAGNOSE_PULLS_TIMEOUT_SECONDS = 240          # Hermes pull diagnosis/fix step — bumped 2026-05-08 because diagnose runs per-source retry which can chain HTTP calls
# Self-diagnostics step (cycle step 5, every 6th cycle). Added
# fable-hermes-repair-bound (2026-09-19): this step used to call
# run_self_diagnostics() inside a plain try/except with NO _run_with_timeout
# at all (see docs/handoffs/2026-09-19/fable-hermes-repair-bound.md). A
# REPULL action inside diagnostics ran _retry_source synchronously, and
# because that call defaulted to a full-history pull, cycle 6300 stayed on
# this one step for 71 minutes, starving every step scheduled after it —
# including the new sector_health step (SECTOR_HEALTH_TIMEOUT_SECONDS
# above) and intelligence_tasks. Repair pulls are now bounded to a
# REPAIR_LOOKBACK_DAYS window with their own REPAIR_BUDGET_SECONDS
# cooperative budget (scripts/hermes_fixers.py) — this timeout is the
# step-level backstop: LLM call (~60s observed) + REPAIR_BUDGET_SECONDS
# (180) + headroom for the rest of run_self_diagnostics's own work.
# REPAIR_BUDGET_SECONDS must stay under this AND under
# DIAGNOSE_PULLS_TIMEOUT_SECONDS above — pinned by
# tests/test_hermes_repair_bounded.py.
DIAGNOSTICS_TIMEOUT_SECONDS = 300
RESOLUTION_TIMEOUT_SECONDS = 420              # normalization.resolver.Resolver.resolve_pending. Outer guard only — RESOLUTION_SCAN_BUDGET_SECONDS is what bounds the step. Must hold that budget (180) + one slice of overshoot capped at MIN_SCAN_SLICE_TIMEOUT_S (60) + the worst resolve phase observed live on 2026-09-14 (77.5s, cycle 6014) = 317.5s. Was 240, which the 371-411s cold scan of ops-exec run 292 did not fit inside. tests/test_hermes_resolution_watermark.py pins the invariant.
SMART_INGESTION_TIMEOUT_SECONDS = 300         # smart_scheduler.tick() — matches TICK_TIME_BUDGET_S in ingestion/smart_scheduler.py so Hermes doesn't pull the plug while SmartScheduler is mid-shutdown
TIMESFM_TIMEOUT_SECONDS = 240                 # oracle/forecaster_adapter.run_timesfm_forecast_cycle
ASTROGRID_CELESTIAL_TIMEOUT_SECONDS = 240      # oracle.astrogrid_cycle.run_celestial_cycle: deterministic sky build is sub-second; the budget is almost entirely the one local-LLM interpretation call (num_predict=1200). Degrades to a deterministic fallback if the model is offline, so a timeout here means the model was slow, not absent.
DAILY_INTEL_BATCH_OBSERVED_S = 360            # HISTORICAL — observed run length of the OLD monolithic 02:00 daily block (source_audit → backtest_scan → postmortem → options_improvement → hypothesis_review → auto_discover) with LLM calls, measured 2026-05-08. Superseded by DAILY_INTEL_CYCLE_BUDGET_SECONDS below for the timeout-budget pin (fable-daily-intel-resumable, 2026-09-20) — kept only because it is a documented historical measurement other notes reference; nothing computes with it anymore.
INTELLIGENCE_TASKS_TIMEOUT_SECONDS = 900      # whole run_intelligence_tasks() step. MUST exceed ACTIVE_HYPO_SCORING_MAX_RUNTIME_S + DAILY_INTEL_CYCLE_BUDGET_SECONDS + 60: the 30-min active-hypothesis scorer runs FIRST inside this step, so if its budget plus the daily-intel block's own per-cycle budget (plus headroom for the earnings-sync bookkeeping ahead of both) exceeds this cap, the step times out before the daily block ever gets a turn. That inversion (360s cap vs 600s scorer) is how hypothesis discovery starved from 2026-05-15 onward, and — after the 2026-09-10 raise — the still-unbounded daily block re-created the same starvation one level down (fable-daily-intel-resumable, 2026-09-20: the block itself, not just the scorer, could run unbounded and get orphaned mid-list every cycle after 02:00Z). The per-task/per-cycle budgets on DAILY_INTEL_TASKS + DAILY_INTEL_CYCLE_BUDGET_SECONDS below fix that. tests/test_hermes_timeout_budgets.py pins the invariant.
SECTOR_HEALTH_TIMEOUT_SECONDS = 120           # whole sector-health snapshot step (scripts/hermes_operator.py::_run_sector_health_step), split out of run_intelligence_tasks on 2026-09-19 into its own dispatch with its own timeout. Observed run time for ~20 sectors is 3-8s; 120s is generous headroom, not a sized budget like INTELLIGENCE_TASKS_TIMEOUT_SECONDS above. Deliberately independent of that 900s budget: production traces show intelligence_tasks times out on essentially every cycle (the daily block runs with catch_up=True every cycle, so it never reaches state.last_daily_intel = now, and the sector-health call used to run AFTER that point — i.e. never). Giving this step its own short timeout, dispatched before intelligence_tasks, makes it reachable regardless of whether intelligence_tasks times out. See docs/handoffs/2026-09-19/fable-hermes-sector-schedule.md ("Parent-timeout blocker and own-step fix").
POSTMORTEM_BATCH_LIMIT = 20                   # Drain postmortem backlog in bounded chunks instead of orphaning long LLM loops.

# Active-hypothesis scoring — periodic batch that closes the loop on the
# auto_discover() pipeline. The bottleneck is per-row score_hypothesis()
# calls which the 2026-05-15 manual run timed at ~15/sec on grid-svr;
# 200 rows / 240s is still ~1 row/sec of budget against a ~15 rows/sec
# engine, so a full batch completes in ~15s and the cap only matters when
# the DB is contended. The cap was 600s until 2026-09-10 — larger than the
# 360s INTELLIGENCE_TASKS_TIMEOUT_SECONDS that wrapped it, which orphaned
# the step twice an hour and could starve the daily block (see the note on
# INTELLIGENCE_TASKS_TIMEOUT_SECONDS above). Runtime budget must satisfy
# ACTIVE_HYPO_SCORING_MAX_RUNTIME_S + DAILY_INTEL_BATCH_OBSERVED_S
# <= INTELLIGENCE_TASKS_TIMEOUT_SECONDS.
ACTIVE_HYPO_SCORING_BATCH_SIZE = 200
ACTIVE_HYPO_SCORING_MAX_RUNTIME_S = 240
ACTIVE_HYPO_SCORING_INTERVAL_MINUTES = 30

# Daily intelligence batch (fable-daily-intel-resumable, 2026-09-20) — see
# DAILY_INTEL_TASKS and _run_daily_intel_block below.
#
# Traced defect: the old daily block (formerly inline in
# run_intelligence_tasks under "Daily at 2:00 AM") ran ~20 sequential tasks
# in one undifferentiated try/except chain with NO timeout of its own,
# inside the 900s INTELLIGENCE_TASKS_TIMEOUT_SECONDS step. Production was
# abandoned at 900s on every post-02:00Z cycle before reaching
# `state.last_daily_intel = now`, so the whole block restarted from the
# top as catch-up every cycle and nothing past the first ~10 minutes of it
# (hypothesis_discovery, rag_index, actor_research, ... onward) ever ran —
# hypothesis discovery starved since 2026-09-17.
#
# Fix: each task now runs under its OWN _run_with_timeout budget
# (DailyIntelTask.budget_s) and a persisted per-period ledger
# (OperatorState.daily_intel_done/daily_intel_attempts) tracks which tasks
# are already done for the current due period, so a restart or a new cycle
# resumes from the first undone task instead of re-running everything.
DAILY_INTEL_BOUNDARY_HOUR = 2                 # UTC hour the daily-intel due-period opens — matches the block's pre-existing "Daily at 2:00 AM" schedule (daily_task_due's boundary-hour convention, same helper the sector-health scheduler uses with boundary_hour=3).
DAILY_INTEL_MAX_ATTEMPTS = 3                  # a task that fails (timeout or exception) this many times within one due period is marked skipped_for_period so it cannot block the tasks behind it forever.
DAILY_INTEL_CYCLE_BUDGET_SECONDS = 480        # cumulative wall-time budget for the daily-intel block PER CYCLE, checked before starting each task (not mid-task). When exhausted, the block stops for this cycle and _run_daily_intel_block resumes from the first undone task on the next due call. Pin: ACTIVE_HYPO_SCORING_MAX_RUNTIME_S + DAILY_INTEL_CYCLE_BUDGET_SECONDS + 60 <= INTELLIGENCE_TASKS_TIMEOUT_SECONDS (240 + 480 + 60 = 780 <= 900) — the +60 covers the earnings-calendar-sync SQL call and active-hypo-scoring bookkeeping that run ahead of both in the same step. tests/test_hermes_timeout_budgets.py pins this.
DAILY_INTEL_LLM_TASK_BUDGET_S = 180           # documented default per-task budget for LLM-backed daily-intel tasks (source_audit, backtest_scan, options_improvement, hypothesis_review, hypothesis_discovery, rag_index, actor_research, edgar_transcripts) — sized like the other single-to-few-call LLM steps above (e.g. TIMESFM_TIMEOUT_SECONDS, KNOWLEDGE_MAP_TIMEOUT_SECONDS), not the many-ticker ORACLE_CYCLE_TIMEOUT_SECONDS.
DAILY_INTEL_SQL_TASK_BUDGET_S = 60            # documented default per-task budget for SQL/CPU-only daily-intel tasks (flow_materialize, icij_linking, milestone_scoring, attention_anomaly, corporate_actions, capital_flow_rollups, fundamental_divergence, holder_deal_overlap) — matches SMART_INGESTION/RESOLUTION-class steps, generous headroom over the sub-10s runtimes those steps observe.
DAILY_INTEL_CLEANUP_TASK_BUDGET_S = 30        # documented default per-task budget for the three daily-intel file/log cleanup tasks (insight_cleanup, briefing_cleanup, errors_jsonl_cleanup) — cheap filesystem work, not DB or LLM bound.
DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S = DAILY_INTEL_LLM_TASK_BUDGET_S  # postmortem_batch is LLM-backed but bounded on the WORK axis by POSTMORTEM_BATCH_LIMIT (20 rows/cycle, see above) rather than its own time constant; the time budget still uses the LLM default.
DAILY_INTEL_DISPATCH_TASK_BUDGET_S = DAILY_INTEL_SQL_TASK_BUDGET_S  # storage_maintenance_subagent only QUEUES a subagent dispatch command (_execute_hermes_repair_command) — no LLM call in this step itself — so it gets the SQL-class default, not the LLM one.

# Sector health snapshot — daily due-period scheduling (2026-09-19). Was
# "now.hour == 3 and now.minute < 10", which only fired on the rare cycle
# evaluated inside that 10-minute slice; production ran it successfully
# twice in the last 400 snapshots (2026-07-13, 2026-09-13). See
# docs/handoffs/2026-09-19/fable-hermes-sector-schedule.md.
SECTOR_HEALTH_BOUNDARY_HOUR = 3               # UTC hour the daily due-period opens
SECTOR_HEALTH_RETRY_BACKOFF_MINUTES = 60      # min minutes between failed-attempt retries
SECTOR_HEALTH_MAX_ATTEMPTS_PER_DAY = 5        # cap on attempts per due period so a
                                               # persistent failure doesn't retry every cycle forever

# Earnings events → earnings_calendar back-compat sync. The DB-side
# function ``sync_earnings_events_to_calendar()`` (installed
# 2026-05-17 via migration ``20260517_earnings_events_compat.sql``)
# mirrors rows from the new ``earnings_events`` table into the legacy
# ``earnings_calendar`` table so old consumers keep working. The
# function exists but nothing was calling it on a schedule; this job
# runs it every 30 minutes from Hermes alongside the active-hypothesis
# scorer (same cadence, same scheduling mechanism).
EARNINGS_CALENDAR_SYNC_INTERVAL_MINUTES = 30

# Conflict resolution (cycle step 3b). This is the ONLY writer of
# resolved_series, the table every PIT/analytical surface reads.
#
# History: commit b0a02b4 (2026-03-29) replaced the Python resolver with an
# "INSERT ... SELECT ... JOIN entity_map" fast path that referenced a table
# and columns that do not exist, inside `except: log.debug(...)`. It failed on
# every cycle in silence and resolved_series stopped advancing for 5.5 months.
# The resolver is back, and the failure path now logs at warning and lands in
# cycle_result["resolution"].
#
# Cost control: the resolver's 30-day default window is for manual runs. The
# cycle runs every 5 minutes, so it passes a 2-day window, narrowed further by
# a watermark (state.last_resolution, persisted with the rest of OperatorState
# in the hermes_operator analytical_snapshots payload and rehydrated on
# restart). The watermark is how far the last run actually scanned, minus an
# overlap margin, so a row pulled while the resolver was running is picked up
# next cycle instead of being skipped.
#
# MEASUREMENT — where this step's time actually goes
# --------------------------------------------------
# Two griddb measurements, both real, taken under different cache states.
# raw_series is 511 GB / 1.93e9 rows with ten indexes totalling ~344 GB.
#
#   ops-exec run 292 (2026-09-14, COLD): sampling pg_stat_activity every 20s
#     caught the rolling 2-day DISTINCT at 371s, 391s and 411s, on
#     IO:DataFileRead throughout. Re-run ~2 minutes later: 33.9s warm,
#     246,093 rows yielding 9,636 distinct series.
#
#   ops-exec run 312 (2026-09-14 05:41 UTC, WARM, and under load — host load
#     average 23, sdc 82% util / 90ms r_await with a pg_basebackup at 84.8%):
#     the same 2-day window, now sliced 13 ways by #487, scanned in 0.2s
#     total, max slice 0.1s, 233,664 rows yielding 8,617 distinct series. A
#     second identical pass also took 0.2s. The full resolve_pending (dry
#     run) over that window took 12.2s — so ~0.2s of scan and ~12s of
#     resolve.
#
# Which corrects the note this replaces (ops-exec run 34548358613, 1.8s for
# the DISTINCT plus 6.2s to fetch 655,376 rows) on two points. It is not
# wrong about the number — 1.8s is the same order as run 312's 0.2s — but it
# was labelled COLD and it is a warm measurement; run 292 is what this
# statement costs when the cache really is cold, and that is 200x larger and
# larger than the step budget. And the scan is not where the step's time
# goes: eight consecutive live cycles on 2026-09-14 (6008-6015) ran the step
# in 10.5s, 22.3s, 10.8s, 15.9s, 20.6s, 19.3s, 77.5s and 10.5s, and in the
# 77.5s one the scan finished in ~1s and a single resolver worker accounted
# for ~76s.
#
# So the budget below is sized for the cold tail of the scan, and
# RESOLUTION_TIMEOUT_SECONDS is sized to hold that budget plus the observed
# worst-case resolve phase. Once the watermark is current the window is the
# overlap plus one cycle — one slice — not two days.
RESOLUTION_CYCLE_LOOKBACK_DAYS = 2
RESOLUTION_WATERMARK_OVERLAP_HOURS = 2
RESOLUTION_CYCLE_WORKERS = 4

# Wall seconds the resolver's distinct-series scan may spend before it stops
# starting slices and resolves the prefix it enumerated.
#
# This is the fix, not the timeout above it. Before this, a scan that outran
# RESOLUTION_TIMEOUT_SECONDS threw away everything it had done:
# _run_with_timeout abandoned the step, the watermark advanced only on a fully
# clean run, so the next cycle re-scanned the identical cold window with the
# identical budget. Nothing about the second attempt was more likely to
# succeed than the first — the failure was self-perpetuating rather than
# self-correcting.
#
# With a scan budget the truncated case is an ordinary successful return
# carrying "scanned through here", so the watermark advances over the prefix
# and the next cycle RESUMES instead of restarting. Progress is monotonic even
# when every cycle is truncated. Raising the timeout alone would have left
# that in place and moved the cliff.
#
# 180s at the ~34s-per-slice cold rate of run 292 (411s / 12 slices) is ~5
# slices, so a cold 2-day catch-up completes over three cycles instead of
# never. Against the steady-state window — the 2h overlap plus one cycle,
# which is a single slice — it is ~5x headroom on a cold slice and ~1000x on
# the 0.2s warm scan run 312 measured.
RESOLUTION_SCAN_BUDGET_SECONDS = 180

# Keep signal classification under its per-step timeout. The classifier makes
# one local LLM call per signal and live calls can approach 15s each.
SIGNAL_CLASSIFICATION_LIMIT = int(os.getenv("GRID_SIGNAL_CLASSIFICATION_LIMIT", "5"))


def _run_with_timeout(name: str, fn, timeout_s: int, state):
    """Execute fn() with a hard timeout. On timeout, blacklist via cooldown.

    Uses concurrent.futures so the call returns even if the worker thread is
    still alive (it becomes a daemon-like orphan). This is acceptable because
    the orphan eventually finishes (LLM eventually returns) and no destructive
    side-effect is in flight on these read-mostly steps.

    NOTE: do NOT use `with ThreadPoolExecutor(...) as ex:`. The context
    manager calls `shutdown(wait=True)` on exit, which blocks until the
    orphan worker finishes — completely defeating the timeout. This bug
    silently broke every stage timeout in Hermes for months: the cycle
    appeared to time out at the cycle-level (600s) "stuck on stage X"
    when actually the stage HAD already passed its budget but the
    shutdown() call was waiting for the still-running thread.

    Fix: explicit shutdown(wait=False) on timeout so we genuinely hand
    back control. The orphan thread keeps running but as a true daemon;
    the next cycle starts on schedule.

    Returns:
        (result, ok) — fn's return value (or None on timeout/error), success bool.
    """
    import concurrent.futures
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    fut = ex.submit(fn)
    try:
        result = fut.result(timeout=timeout_s)
        ex.shutdown(wait=True)  # success path — let the worker tear down cleanly
        return result, True
    except concurrent.futures.TimeoutError:
        log.warning(
            "Step '{n}' timed out after {s}s — blacklisting for {h}h",
            n=name, s=timeout_s, h=TIMEOUT_BLACKLIST_HOURS,
        )
        state.cooldowns.blacklist_for_timeout(name)
        # CRITICAL: wait=False so we don't block on the orphan thread.
        # cancel_futures=True attempts to cancel anything still queued
        # (no-op here since we only submitted one future).
        ex.shutdown(wait=False, cancel_futures=True)
        return None, False
    except Exception as exc:
        log.warning("Step '{n}' raised: {e}", n=name, e=str(exc))
        state.cooldowns.record_attempt(name, success=False, error=str(exc))
        ex.shutdown(wait=False, cancel_futures=True)
        return None, False


class _AutoresearchGenerationTracker:
    """In-process generation counter, used to fence autoresearch writes.

    ``_run_with_timeout`` above abandons a timed-out worker rather than
    cancelling it (its own docstring explains why: ``ThreadPoolExecutor``/
    ``concurrent.futures`` has no API to kill a running thread). Without
    something else stopping it, that orphaned worker keeps running
    scripts/autoresearch.py::run_autoresearch() to completion and can still
    insert into hypothesis_registry / model_registry / the research_run
    snapshot trail, arbitrarily long after the operator moved on to the
    next cycle.

    This tracker is the compensating control. Every autoresearch
    invocation is assigned a generation (``next()``) before it is handed to
    the worker thread. run_autoresearch() (scripts/autoresearch.py) checks
    ``is_current(generation)`` — via the closure captured in
    ``is_current_generation`` below — before every write it makes; once
    this tracker's ``current`` has moved past that generation, the check
    fails and the write is skipped with a recorded "fenced" reason instead
    of being made.

    SCOPE: this fences a stale worker THREAD within this SAME PROCESS only.
    ``current`` is a plain int behind the GIL, which is enough for an
    orphan thread in the same interpreter to observe a bump made by the
    main operator thread — it is NOT enough to fence a second Hermes
    process, or a worker that survives past a process restart. Cross-
    process fencing needs a DB-backed lease (a row with an owner/epoch
    that every writer re-checks transactionally, e.g. ``SELECT ... FOR
    UPDATE`` or an optimistic version column) — not implemented here. That
    gap is why autoresearch remains explicitly not-yet-safe-to-activate on
    a schedule; this task only makes it observable and safe to restart
    within one process.
    """

    def __init__(self) -> None:
        self.current = 0

    def next(self) -> int:
        """Advance to a new generation and return it."""
        self.current += 1
        return self.current

    def is_current(self, generation: int) -> bool:
        """Return whether *generation* is still the latest one assigned."""
        return generation == self.current


# Module-level: one tracker per Hermes operator process, shared by every
# autoresearch invocation across cycles (see class docstring for scope).
_autoresearch_generation = _AutoresearchGenerationTracker()


# ─── Source registry (DERIVED from PULLER_REGISTRY — task #179) ────────────
#
# Historical bug class (#161, #170): _SOURCE_REGISTRY and PULLER_REGISTRY were
# maintained as two independent literals. Items in one but not the other were
# silently dropped — either a puller ran but had no operator-side catalog
# entry, or it was catalogued but never actually scheduled. The #170 startup
# divergence-guard logged warnings; #179 (this block) removes the bug class.
#
# Single source of truth: ``ingestion.smart_scheduler.PULLER_REGISTRY``.
# _SOURCE_REGISTRY is computed at import time from PULLER_REGISTRY plus a
# small explicit ``_SOURCE_EXTRAS`` overlay for entries PULLER_REGISTRY
# structurally cannot represent (module-level ``fn`` pullers, ``skip_runtime``
# audit-only stubs, and a few class-based entries that exist as pullers but
# have not yet been wired into the SmartScheduler tick loop).
#
# Aliases (``_SOURCE_ALIASES``): historical operator-side names that map to
# the same underlying (mod, cls) as a PULLER_REGISTRY entry but with a
# different key. These are exposed in _SOURCE_REGISTRY so existing consumers
# (``hermes_fixers._resolve_puller``, ``_CATALOG_TO_REGISTRY``) keep working.
#
# Field translation (PULLER_REGISTRY → _SOURCE_REGISTRY):
#   mod    → mod       (1:1)
#   cls    → cls       (1:1)
#   method → pull_method (omitted when default "pull_all")
#   kwargs → pull_kwargs (omitted when empty)
#   api_key → api_key   (1:1)
#   freq_h → interval_h (1:1)
#
# Consumers (``scripts.hermes_fixers._resolve_puller``,
# ``hermes_fixers._CATALOG_TO_REGISTRY``) read these fields by name and ignore
# unknown ones, so the resulting dict is shape-compatible with the previous
# static literal.

_SOURCE_ALIASES: dict[str, str] = {
    # cfg-side name → PULLER_REGISTRY name (same mod+cls)
    "yfinance_options":     "options",
    "crucix":               "crucix_bridge",
    "fedspeeches":          "fed_speeches",
    "noaa_swpc":            "solar",
    "lunar_ephemeris":      "lunar",
    "planetary_ephemeris":  "planetary",
    "institutional_flows":  "etf_flows",
}

# Entries that PULLER_REGISTRY structurally cannot carry (fn-based, audit-only
# skip_runtime stubs) or that simply haven't been wired into the scheduler yet
# but are catalogued for ``hermes_fixers._resolve_puller`` consumers.
_SOURCE_EXTRAS: dict[str, dict[str, Any]] = {
    # ── Module-level fn pullers (not class-based, scheduled via dedicated paths)
    "sec_13f_live":                {"mod": "ingestion.altdata.sec_13f_live",            "fn": "run",          "interval_h": 168},
    "supply_chain_parser":         {"mod": "ingestion.altdata.supply_chain_parser",     "fn": "run_weekly",   "interval_h": 168},
    "pct_cogs_enrichment":         {"mod": "intelligence.pct_cogs_enrichment",          "fn": "run_weekly",   "interval_h": 168},
    "supply_chain_edge_validator": {"mod": "intelligence.supply_chain_edge_validator",  "fn": "run_weekly",   "interval_h": 168},
    "apple_supplier_list":         {"mod": "ingestion.altdata.apple_supplier_list",     "fn": "run_annual",   "interval_h": 8760},
    "sec_item_1c_cyber":           {"mod": "ingestion.altdata.sec_item_1c_cyber",       "fn": "run_weekly",   "interval_h": 168},
    "regulatory_events":           {"mod": "ingestion.altdata.regulatory_events",       "fn": "run_weekly",   "interval_h": 168},
    "obsidian":                    {"mod": "ingestion.altdata.obsidian_sync",           "fn": "run_sync",     "interval_h": 0.083},
    "trial_ingestor":              {"mod": "grid.ingestors.trial_ingestor",             "fn": "main",         "interval_h": 24},
    # Trial-gem feed for the Long Plays board (task #28). Order matters:
    # ingestor (CT.gov → trial_cache/catalyst_calendar) → signal (trial_signals)
    # → small-cap enrichment (company_profiles cash / burn / runway / mcap).
    "trial_signal":                {"mod": "grid.signals.trial_signal",                 "fn": "run_daily",    "interval_h": 24},
    "small_cap_enrichment":        {"mod": "ingestion.altdata.small_cap_enrichment",    "fn": "pull_all",     "interval_h": 24},

    # ── Class-based pullers catalogued but not yet scheduler-wired
    # (Adding to PULLER_REGISTRY is the eventual fix; tracking here so
    # _resolve_puller still finds them when called directly via hermes_fixers.)
    "vedic_jyotish":   {"mod": "ingestion.celestial.vedic",                    "cls": "VedicAstroPuller"},
    "chinese_calendar":{"mod": "ingestion.celestial.chinese",                  "cls": "ChineseCalendarPuller"},
    "pmxt_archive":    {"mod": "ingestion.altdata.pmxt_archive",               "cls": "PmxtArchivePuller"},
    "pm_history":      {"mod": "ingestion.altdata.prediction_market_history",  "cls": "PredictionMarketHistoryPuller"},
    "warn_layoffs":    {"mod": "ingestion.altdata.warn_layoffs",               "cls": "WARNLayoffsPuller",     "interval_h": 24},
    # FMP market-cap refresh for trial tickers (pull(), not pull_all()).
    "company_profiles_puller": {"mod": "ingestion.altdata.company_profiles_puller", "cls": "CompanyProfilesPuller", "pull_method": "pull", "interval_h": 24},

    # ── skip_runtime stubs (ctor/method signature mismatch — needs wrapper
    # or _resolve_puller upgrade before they can actually run).
    "crypto_etf_flows":   {"mod": "ingestion.altdata.crypto_etf_flows",   "cls": "CryptoETFPuller",     "interval_h": 24, "skip_runtime": "engine= ctor / pull() method mismatch"},
    "hyperliquid_puller": {"mod": "ingestion.altdata.hyperliquid_puller", "cls": "HyperliquidPuller",   "interval_h": 1,  "skip_runtime": "engine= ctor / pull() method mismatch"},
    "onchain_rpc":        {"mod": "ingestion.altdata.onchain_rpc",        "cls": "OnChainRPCPoller",    "interval_h": 1,  "skip_runtime": "engine= ctor / pull() method mismatch"},
    "whale_alert":        {"mod": "ingestion.altdata.whale_alert",        "cls": "WhaleAlertPuller",    "interval_h": 1,  "skip_runtime": "engine= ctor / pull() method mismatch"},
}

# Partial overrides applied AFTER PULLER_REGISTRY derivation. Operator-side
# retry path (``hermes_fixers._retry_source``) needs fields that differ from
# the scheduler-side ``method``/``kwargs``. ``edgar`` is the prototypical
# case: the scheduler runs ``pull_all`` once a day; the retry path runs
# ``pull_form4_transactions(days_back=3)`` for a fast, scoped recovery.
# Values are merged on top of the derived entry; pass ``None`` to clear a
# key set by PULLER_REGISTRY.
_SOURCE_OVERRIDES: dict[str, dict[str, Any]] = {
    "edgar":           {"pull_method": "pull_form4_transactions",
                        "pull_kwargs": {"days_back": 3}},
    "insider_filings": {"pull_kwargs": {"days_back": 3}},
    "gov_contracts":   {"pull_kwargs": {"days_back": 7}},
    "legislation":     {"pull_kwargs": {"days_back": 7}},
    "fed_speeches":    {"pull_kwargs": {"days_back": 30}},  # alias 'fedspeeches' inherits
    "cboe":            {"pull_kwargs": {"days_back": 30}},
    # gdelt retry path uses the default pull_all + no kwargs (faster than the
    # scheduler's bounded pull_recent path which is tuned for breaking-news
    # cadence, not for catch-up after a failure).
    "gdelt":           {"pull_method": None, "pull_kwargs": None},
}


def _build_source_registry() -> dict[str, dict[str, Any]]:
    """Derive _SOURCE_REGISTRY from PULLER_REGISTRY + extras + aliases.

    Pure function — no I/O. Idempotent: importing this module N times
    produces N identical dicts.

    Strategy:
        1. Pull each ``PULLER_REGISTRY`` entry and translate fields.
        2. Layer ``_SOURCE_EXTRAS`` on top (fn-based + skip_runtime + cls-but-
           not-scheduler-wired entries).
        3. Add ``_SOURCE_ALIASES`` so historical cfg-side names continue to
           resolve to the same underlying entry as their PULLER_REGISTRY twin.

    Returns:
        dict[str, dict[str, Any]] — same shape as the previous static literal.
    """
    # Local import to avoid circular import at module load
    from ingestion.smart_scheduler import PULLER_REGISTRY

    registry: dict[str, dict[str, Any]] = {}
    for entry in PULLER_REGISTRY:
        name = entry["name"]
        cfg: dict[str, Any] = {"mod": entry["mod"], "cls": entry["cls"]}
        if "api_key" in entry:
            cfg["api_key"] = entry["api_key"]
        method = entry.get("method")
        if method and method != "pull_all":
            cfg["pull_method"] = method
        kwargs = entry.get("kwargs")
        if kwargs:
            cfg["pull_kwargs"] = dict(kwargs)
        freq_h = entry.get("freq_h")
        if freq_h is not None:
            cfg["interval_h"] = freq_h
        registry[name] = cfg

    # Operator-side overrides for ``hermes_fixers._retry_source`` paths that
    # need different method/kwargs than the scheduler's normal run.
    for name, override in _SOURCE_OVERRIDES.items():
        if name in registry:
            entry = registry[name]
            for field, value in override.items():
                if value is None:
                    entry.pop(field, None)
                else:
                    entry[field] = value

    # Extras fill in gaps — PULLER_REGISTRY entries win when a name appears
    # in both (so that future class-puller additions don't get silently
    # shadowed by a stale extras stub).
    for name, cfg in _SOURCE_EXTRAS.items():
        registry.setdefault(name, dict(cfg))

    # Aliases: cfg-side names that point at the same underlying entry as a
    # PULLER_REGISTRY name. We share-reference deliberately so that runtime
    # mutations (none today, defensive for the future) stay in lock-step.
    for alias, canonical in _SOURCE_ALIASES.items():
        if canonical in registry:
            registry[alias] = registry[canonical]

    return registry


# Source name → (module_path, class_name, needs_api_key, pull_method).
# COMPUTED at import time from PULLER_REGISTRY. To add or change a puller,
# edit ``ingestion/smart_scheduler.py::PULLER_REGISTRY`` (class-based) or
# ``_SOURCE_EXTRAS`` (fn-based / audit-only) above — NOT a separate literal.
_SOURCE_REGISTRY: dict[str, dict[str, Any]] = _build_source_registry()


# ─── Git sync ────────────────────────────────────────────────────────

def _git(args: list[str], cwd: str | Path | None = None) -> tuple[int, str]:
    """Run a git command and return (returncode, output)."""
    if cwd is None:
        cwd = _GRID_DIR
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result.returncode, (result.stdout + result.stderr).strip()
    except Exception as exc:
        return 1, str(exc)


def git_pull() -> dict[str, Any]:
    """Pull latest changes from remote.

    Safe-by-default semantics — never overwrites operator-applied local
    commits or uncommitted edits:

      1. **Skip on non-target branch.** If the operator has checked out
         a feature branch (e.g. mid-hotfix), auto-pulling ``main`` over
         it is almost never what they want. We bail with a clear log
         line and let them merge/rebase manually when ready.

      2. **Fast-forward only.** We use ``git pull --ff-only`` so the pull
         either applies cleanly (no divergence) or refuses (returns
         non-zero). Prior implementation used ``--rebase`` plus a
         ``pull`` fallback, both of which could silently rewrite local
         commits or merge ``main`` into a feature branch.

    The pre-2026-05-13 implementation lost a session's worth of
    cherry-picked hot-fixes once (caught in time because the working
    tree happened to be dirty); this guards against the next time.
    """
    if not GIT_SYNC_ENABLED:
        return {"skipped": "disabled"}

    rc_repo, out_repo = _git(["rev-parse", "--is-inside-work-tree"])
    if rc_repo != 0 or out_repo.strip().splitlines()[-1:] != ["true"]:
        log.info("Git pull skipped: {o}", o=out_repo[:200])
        return {"skipped": "not_a_git_worktree", "output": out_repo[:200]}

    rc_branch, current_branch = _git(["rev-parse", "--abbrev-ref", "HEAD"])
    branch_name = current_branch.strip() if rc_branch == 0 else ""
    if branch_name and branch_name != GIT_BRANCH:
        log.info(
            "Git pull skipped: on branch {b}, not {target}. "
            "Merge or rebase to {target} manually when ready.",
            b=branch_name, target=GIT_BRANCH,
        )
        return {"skipped": "non_target_branch", "branch": branch_name}

    log.info("Git pull — syncing latest changes (--ff-only)")
    rc, out = _git(["pull", "--ff-only", GIT_REMOTE, GIT_BRANCH])
    if rc == 0:
        log.info("Git pull OK: {o}", o=out[:200])
        return {"status": "ok", "output": out[:200]}

    log.warning(
        "Git pull failed (not fast-forward — local branch has unique "
        "commits or working tree dirty): {o}",
        o=out[:300],
    )
    return {"status": "failed_non_ff", "output": out[:300]}


def git_push_outputs() -> dict[str, Any]:
    """Commit and push any new analytical outputs."""
    if not GIT_SYNC_ENABLED:
        return {"skipped": "disabled"}

    # Check for changes in outputs/ and .server-logs/
    rc, status = _git(["status", "--porcelain", "outputs/", ".server-logs/"])
    if rc != 0 or not status.strip():
        return {"status": "nothing_to_push"}

    changed_files = [line.strip().split(maxsplit=1)[-1] for line in status.strip().split("\n") if line.strip()]
    log.info("Git push — {n} changed output files", n=len(changed_files))

    # Stage output files only (never code)
    _git(["add", "outputs/", ".server-logs/"])

    # Commit
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    rc, out = _git(["commit", "-m", f"[hermes-operator] analytical outputs {ts}"])
    if rc != 0:
        log.warning("Git commit failed: {o}", o=out[:200])
        return {"status": "commit_failed", "output": out[:200]}

    # Push with retry
    for attempt in range(4):
        rc, out = _git(["push", GIT_REMOTE, GIT_BRANCH])
        if rc == 0:
            log.info("Git push OK")
            return {"status": "ok", "files": len(changed_files)}
        wait = 2 ** (attempt + 1)
        log.warning("Git push attempt {a} failed, retry in {w}s", a=attempt + 1, w=wait)
        time.sleep(wait)

    return {"status": "push_failed", "output": out[:200]}


# ─── Health, State, and Issue Tracking (extracted to hermes_health.py) ──
from scripts.hermes_health import (  # noqa: E402, F401
    _ensure_issues_table,
    log_issue,
    export_issues,
    SourceCooldown,
    OperatorState,
    check_db_health,
    check_hermes_health,
    check_system_health,
)

# ─── Pull Fixers, Pipeline, Diagnostics (extracted to hermes_fixers.py) ──
from scripts.hermes_fixers import (  # noqa: E402, F401
    _resolve_puller,
    _retry_source,
    diagnose_and_fix_pulls,
    maybe_run_pipeline,
    fill_data_gaps,
    run_self_diagnostics,
    maybe_run_autoresearch,
    save_cycle_snapshot,
    _run_intel_task,
    _hours_since,
    _execute_hermes_repair_command,
    _refresh_signal_registry,
)


# ─── Intelligence task runner (remains in this file) ────────────────────


def _minutes_since(ts: datetime | None) -> float:
    """Return minutes elapsed since *ts*, or a large sentinel if ts is None.

    Mirrors the ``_hours_since`` semantics in scripts/hermes_fixers.py for use
    by sub-hour cadences like the periodic active-hypothesis scorer.
    """
    if ts is None:
        return 1e9
    return (datetime.now(timezone.utc) - ts).total_seconds() / 60.0


def _dispatch_daily_storage_maintenance(engine: Any, state: OperatorState) -> dict[str, Any]:
    """Queue the bounded storage-maintenance subagent during Hermes daily work."""
    return _execute_hermes_repair_command(
        "DISPATCH_SUBAGENT:storage_maintainer:grid-svr-data:130",
        engine=engine,
        health={},
        state=state,
    )


def _period_boundary(now: datetime, boundary_hour: int) -> datetime:
    """Return the most recent UTC boundary crossing (``boundary_hour:00``)
    at or before *now*.

    Internal to :func:`daily_task_due`; also reused by the sector-health
    retry-attempt bookkeeping in :func:`run_intelligence_tasks` so both
    share one definition of "due period". *now* must already be
    timezone-aware (callers normalise before calling this).
    """
    today_boundary = now.replace(hour=boundary_hour, minute=0, second=0, microsecond=0)
    if now >= today_boundary:
        return today_boundary
    return today_boundary - timedelta(days=1)


def daily_task_due(
    last_success: datetime | None,
    now: datetime,
    boundary_hour: int,
) -> bool:
    """Return True if a once-per-due-period daily task is due.

    Replaces the old ``now.hour == H and now.minute < 10`` window pattern,
    which only executes the task on the rare cycle that happens to be
    evaluated inside that 10-minute slice. Traced live on the sector-health
    step (2026-09-19): production went from 2026-07-13 to 2026-09-13
    between successful runs — 62 days — because cycles routinely take long
    enough, or start late enough, to miss the window (a cycle starting
    02:55Z had not reached the check by 03:13Z). See
    docs/handoffs/2026-09-19/fable-hermes-sector-schedule.md.

    A due *period* is the UTC day starting at ``boundary_hour:00``. The
    task is due at any evaluation at or after the most recent boundary
    crossing, as long as no successful run (``last_success``) has landed
    since that boundary. There is no upper bound on the window: if the
    process is idle, mid-cycle, or was just restarted, the first
    evaluation after the boundary still runs the task instead of skipping
    the period entirely.

    Timezone handling: both arguments are expected to be timezone-aware
    UTC datetimes — every call site in this module uses
    ``datetime.now(timezone.utc)``. A naive value is NOT rejected; it is
    normalised by assuming it is already UTC, the same convention
    ``OperatorState.hydrate_from_snapshot`` uses when restoring timestamps
    from a JSON snapshot that predates tzinfo-aware storage.
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if last_success is None:
        return True
    if last_success.tzinfo is None:
        last_success = last_success.replace(tzinfo=timezone.utc)
    return last_success < _period_boundary(now, boundary_hour)


# Guards the in-process sector-health state marker (state.last_sector_health*,
# state.sector_health_attempt*) against the check-to-write race described in
# _maybe_run_sector_health_snapshot's docstring below: without it, Python can
# switch threads between _commit's token check and its subsequent writes, so
# an abandoned worker's belated _commit can pass the check, a newer attempt
# can then start and commit, and the old worker's writes land last anyway.
# Deliberately module-level, NOT an attribute on OperatorState — OperatorState
# is serialised via to_dict() (analytical-snapshot persistence), and a
# threading.Lock is not picklable/JSON-able. The DB row race for the
# sector_health_snapshots table itself is closed separately, by the upsert's
# `WHERE ... as_of < EXCLUDED.as_of` guard in
# intelligence/sector_health.py::snapshot_all_sectors (evaluated atomically
# on the locked conflicting row in PostgreSQL); this lock only closes the
# in-process marker race, which the DB guard does not touch.
_SECTOR_HEALTH_STATE_LOCK = threading.Lock()

# Test-only seam: called inside _commit, between the token check and the
# state/results writes, so a test can force a specific thread interleaving
# at that exact point (see tests/test_sector_health_upsert_ordering_pg.py,
# scenario d). Default no-op; production code never sets this.
_SECTOR_HEALTH_COMMIT_TEST_HOOK: Callable[[], None] | None = None


def _maybe_run_sector_health_snapshot(
    engine: Any,
    state: OperatorState,
    now: datetime,
    results: dict[str, Any],
) -> None:
    """Run the daily sector-health snapshot if its due period has arrived.

    Computes the composite health score for every sector in ``SECTOR_MAP``
    and upserts one row per (sector, today) into ``sector_health_snapshots``
    (``intelligence/sector_health.py::snapshot_all_sectors`` — the INSERT is
    ``ON CONFLICT (sector_name, snapshot_date) DO UPDATE``, so a re-run
    inside the same UTC day is idempotent by construction; the state marker
    below exists to skip redundant compute/DB work, not to guard against
    duplicate rows). The row ~30 days back is read by the API to label
    ``trend_30d``.

    Scheduling uses :func:`daily_task_due` (see its docstring) instead of
    the old ``now.hour == 3 and now.minute < 10`` window — see
    ``docs/handoffs/2026-09-19/fable-hermes-sector-schedule.md`` for the
    production trace that motivated this (successful runs 62 days apart
    despite ~5-minute cycles, because most cycles land outside the
    10-minute slice).

    Snapshot date identity: every attempt (and retry) within one due
    period passes the SAME ``snapshot_date`` — the date of
    ``_period_boundary(now, SECTOR_HEALTH_BOUNDARY_HOUR)`` — to
    :func:`intelligence.sector_health.snapshot_all_sectors`, not "today"
    at the moment of the call. Without this, a 23:30 UTC attempt that
    fails and a 00:30 UTC retry that succeeds would target two different
    calendar dates even though they are one due period to this
    scheduler, defeating the (sector_name, snapshot_date) upsert's
    idempotency.

    Outcome semantics: a call either (a) ``success`` — at least one row
    written and no upsert failures, (b) ``no_eligible_sectors`` — zero
    rows written, every sector reported unavailable, no upsert failures;
    this is a legitimate empty day, so the due period IS marked done,
    (c) ``superseded`` — zero rows written, no upsert failures, but at
    least one sector was ``snapshots_stale_skipped`` (every row this
    attempt tried already had an as-new-or-newer row from a different
    attempt — see the ``as_of`` tie rule on
    ``intelligence.sector_health.snapshot_all_sectors``: "first committed
    wins on equal as_of"); this attempt did no useful work but is not a
    failure either, so the due period IS marked done, PROVIDED the token
    is still current — if a newer attempt is already in-process, that
    newer attempt owns marking its own due period done, and this stale
    attempt's ``_commit`` call is a no-op regardless (see the token guard
    below), or (d) ``failure`` — any upsert failure or an exception; the
    due period is NOT marked done. Failure handling: a failed execution
    does NOT advance ``state.last_sector_health`` (so the due period is
    not marked done and a later evaluation can retry), but retries are
    throttled to once every ``SECTOR_HEALTH_RETRY_BACKOFF_MINUTES`` (60)
    and capped at ``SECTOR_HEALTH_MAX_ATTEMPTS_PER_DAY`` (5) per due
    period so a persistent failure doesn't re-attempt on every ~5-minute
    cycle indefinitely. The outcome is recorded on
    ``state.last_sector_health_outcome`` regardless of which branch runs.

    Cross-cycle race guard: this step runs inside ``run_intelligence_tasks``,
    which the caller wraps in ``_run_with_timeout`` — a timeout abandons
    the worker thread rather than killing it, so an orphaned attempt can
    still be running when a later cycle starts a fresh attempt. To keep
    an abandoned worker from clobbering a newer attempt's result, this
    function captures ``state.sector_health_attempt_token`` (incremented
    at attempt start) locally and only commits ``last_sector_health`` /
    ``last_sector_health_outcome`` if the token is still current when the
    call completes; otherwise the result is discarded and logged as
    stale.

    Check-to-write race on the state marker: the token check above and the
    subsequent writes to ``state.last_sector_health*`` are NOT atomic on
    their own — Python can switch threads between ``_commit``'s check and
    its assignments, so an abandoned worker's ``_commit`` can pass the
    check, a newer attempt can then start AND commit, and the old worker's
    assignments can still land last, overwriting the newer attempt's
    marker. ``_SECTOR_HEALTH_STATE_LOCK`` (module-level, not stored on
    ``OperatorState`` — see its own docstring) is held around (a) the
    attempt-start block (token bump + attempt fields) below, (b) the whole
    of ``_commit`` (check + writes), and (c) the timeout-path token bump in
    ``_run_sector_and_intelligence_steps``, so the check and the writes for
    any one attempt happen atomically with respect to every other
    attempt's check-and-write. This is purely an in-process guard for the
    Python-level marker; the DB row race for the actual
    ``sector_health_snapshots`` table is independently closed by the
    upsert's ``WHERE`` guard (see ``snapshot_all_sectors``).
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    sector_health_period_due = daily_task_due(
        state.last_sector_health, now, SECTOR_HEALTH_BOUNDARY_HOUR,
    )

    sector_health_due = False
    if sector_health_period_due:
        attempt_in_this_period = (
            state.last_sector_health_attempt is not None
            and not daily_task_due(
                state.last_sector_health_attempt, now, SECTOR_HEALTH_BOUNDARY_HOUR,
            )
        )
        if not attempt_in_this_period:
            # Fresh due period (or a restart with no attempt recorded yet
            # for it) — always allowed, and the attempt counter resets.
            state.sector_health_attempt_count = 0
            sector_health_due = True
        else:
            # Computed from the *passed-in* now, not a fresh wall-clock
            # read (unlike _minutes_since) — this function is evaluated
            # with the caller's `now`, and callers (including tests) may
            # legitimately pass a `now` that differs from the real clock.
            last_attempt = state.last_sector_health_attempt
            if last_attempt.tzinfo is None:
                last_attempt = last_attempt.replace(tzinfo=timezone.utc)
            minutes_since_attempt = (now - last_attempt).total_seconds() / 60.0
            backoff_elapsed = minutes_since_attempt >= SECTOR_HEALTH_RETRY_BACKOFF_MINUTES
            under_attempt_cap = (
                state.sector_health_attempt_count < SECTOR_HEALTH_MAX_ATTEMPTS_PER_DAY
            )
            sector_health_due = backoff_elapsed and under_attempt_cap

    if not sector_health_due:
        return

    log.info(
        "Running daily sector health snapshot (due since {h}:00 UTC, attempt {a})",
        h=SECTOR_HEALTH_BOUNDARY_HOUR, a=state.sector_health_attempt_count + 1,
    )
    with _SECTOR_HEALTH_STATE_LOCK:
        state.last_sector_health_attempt = now
        state.sector_health_attempt_count += 1
        state.sector_health_attempt_token += 1
        attempt_token = state.sector_health_attempt_token
    due_period_date = _period_boundary(now, SECTOR_HEALTH_BOUNDARY_HOUR).date()

    def _commit(outcome: str, extra: dict[str, Any]) -> None:
        """Write the attempt's result to `state`/`results`, but only if no
        later attempt has started since this one (see docstring). The
        check and the writes happen under _SECTOR_HEALTH_STATE_LOCK so an
        abandoned worker cannot pass the check and then lose a race to
        write after a newer attempt has already committed (the
        check-to-write race described in this function's docstring)."""
        with _SECTOR_HEALTH_STATE_LOCK:
            if state.sector_health_attempt_token != attempt_token:
                log.warning(
                    "stale sector-health worker result ignored (token {t}, current {c})",
                    t=attempt_token, c=state.sector_health_attempt_token,
                )
                return
            if _SECTOR_HEALTH_COMMIT_TEST_HOOK is not None:
                _SECTOR_HEALTH_COMMIT_TEST_HOOK()
            results["sector_health_snapshot"] = {**extra, "outcome": outcome}
            state.last_sector_health_outcome = outcome
            if outcome in ("success", "no_eligible_sectors", "superseded"):
                state.last_sector_health = now

    try:
        from intelligence.sector_health import snapshot_all_sectors
        sh_result = snapshot_all_sectors(
            engine,
            snapshot_date=due_period_date,
            computed_at=now,
            # Cheap in-process staleness guard for an abandoned
            # _run_with_timeout worker (see this function's "Cross-cycle
            # race guard" docstring section above): checked by
            # snapshot_all_sectors before each sector's compute and again
            # immediately before each upsert. If a LATER attempt has
            # already bumped state.sector_health_attempt_token past this
            # attempt's captured value, this closure starts returning
            # False and the loop stops mid-run instead of racing a newer
            # attempt's writes. This is in-process only (same caveat as
            # the token guard below and _AutoresearchGenerationTracker):
            # it does not fence a second Hermes process.
            should_continue=lambda: state.sector_health_attempt_token == attempt_token,
        )

        if sh_result.get("aborted_stale"):
            # A later attempt already started (token moved past ours)
            # while snapshot_all_sectors was still running — the _commit
            # token guard below would discard this result anyway, so
            # return without touching `results` or state.last_sector_health*
            # at all, rather than committing a partial/stale outcome.
            log.info(
                "sector_health: attempt {a} aborted mid-run (stale token; a "
                "later attempt already started) — result discarded",
                a=state.sector_health_attempt_count,
            )
            return

        written = sh_result.get("snapshots_written", 0)
        skipped = sh_result.get("snapshots_skipped_unavailable", 0)
        upsert_failed = sh_result.get("upsert_failed", 0)
        stale_skipped = sh_result.get("snapshots_stale_skipped", 0)

        if upsert_failed > 0:
            outcome = "failure"
            log.warning(
                "sector_health: {n} upsert failure(s), due period not marked done",
                n=upsert_failed,
            )
        elif written == 0 and stale_skipped > 0:
            # Every row this attempt tried already had an as-new-or-newer
            # row from a different attempt (the snapshot_all_sectors
            # `as_of` WHERE guard rejected every write this attempt made).
            # Not a failure — a different attempt already did the work —
            # so the due period is marked done via _commit's outcome set,
            # but only if this attempt's token is still current (a newer
            # in-process attempt already handles its own marker).
            outcome = "superseded"
            log.info(
                "sector_health: superseded — {n} row(s) already had an "
                "as-new-or-newer as_of from a different attempt, nothing "
                "written this attempt",
                n=stale_skipped,
            )
        elif written == 0 and skipped > 0:
            outcome = "no_eligible_sectors"
            log.info(
                "sector_health: executed, no eligible sectors, nothing to write "
                "({k} unavailable)", k=skipped,
            )
        else:
            outcome = "success"
            log.info("sector_health: {n} snapshots written", n=written)

        _commit(outcome, sh_result)
    except Exception as exc:
        log.warning("sector_health snapshot failed: {e}", e=str(exc))
        _commit("failure", {"status": "failed", "error": str(exc)})


def _run_diagnostics_step(
    engine: Any,
    hermes_ok: bool,
    health: dict,
    state: OperatorState,
    dry_run: bool,
    cycle_result: dict[str, Any],
) -> None:
    """Run self-diagnostics (cycle step 5, every 6th cycle) under its own
    ``_run_with_timeout(..., DIAGNOSTICS_TIMEOUT_SECONDS)`` budget.

    Extracted 2026-09-19 (fable-hermes-repair-bound; see
    docs/handoffs/2026-09-19/fable-hermes-repair-bound.md). Before this the
    call site was::

        if state.cycle_count % 6 == 0:
            try:
                diag = run_self_diagnostics(engine, hermes_ok, health, state, dry_run=dry_run)
                cycle_result["diagnostics"] = diag
            except Exception as exc:
                log.warning(...)

    — a plain try/except with NO per-step timeout. run_self_diagnostics can
    execute a Hermes-emitted ``REPULL:<source>`` command via
    ``_execute_hermes_repair_command`` -> ``_retry_source``, which (before
    this task) called a full-history pull for any puller with a
    ``start_date`` parameter. Traced: cycle 6300 spent 71 minutes on this
    one step (``yfinance`` was diagnosed "stale" from a freshness-signal
    bug — see the E finding in the handoff doc — even though its data was
    current), and every step scheduled after diagnostics that cycle
    (including sector_health and intelligence_tasks) never ran.

    Repair pulls are now bounded on two independent axes (both in
    scripts/hermes_fixers.py): a ``REPAIR_LOOKBACK_DAYS`` window per
    attempt, and a shared ``REPAIR_BUDGET_SECONDS`` cooperative deadline
    that ``run_self_diagnostics`` computes once and threads through to the
    puller. This wrapper is the remaining backstop — it bounds the WHOLE
    step (including the LLM call itself, and any command that doesn't
    participate in the cooperative budget) so a hang anywhere inside
    diagnostics cannot starve due maintenance placed after it in
    ``run_cycle``.

    Deliberately no ``can_retry("diagnostics")`` check on timeout — same
    reasoning as ``_run_sector_and_intelligence_steps`` above: the
    blacklist entry ``_run_with_timeout`` writes on a timeout is only
    honoured by call sites that explicitly check
    ``state.cooldowns.can_retry(<name>)`` before running (traced to exactly
    four: ``oracle_cycle``, ``signal_classification``, ``anomaly_narration``,
    ``knowledge_mapping``). ``diagnostics`` is not one of them and adding
    that check now would make a single timeout block every diagnostics
    cycle for ``TIMEOUT_BLACKLIST_HOURS`` (24h) — this step already has its
    own per-call budget (``REPAIR_BUDGET_SECONDS``) and its natural
    cadence (every 6th cycle) as throttling.
    """
    if state.cycle_count % 6 != 0:
        return
    try:
        state.current_step = "diagnostics"
        diag, ok = _run_with_timeout(
            "diagnostics",
            lambda: run_self_diagnostics(engine, hermes_ok, health, state, dry_run=dry_run),
            DIAGNOSTICS_TIMEOUT_SECONDS,
            state,
        )
        if ok:
            cycle_result["diagnostics"] = diag
        else:
            cycle_result["diagnostics"] = {"timeout": True}
    except Exception as exc:
        log.warning("Self-diagnostics failed: {e}", e=str(exc))


def _run_sector_health_step(engine: Any, state: OperatorState, dry_run: bool) -> dict[str, Any]:
    """Entry point for the sector-health snapshot as its own ``run_cycle``
    step (dispatched by :func:`_run_sector_and_intelligence_steps` under
    ``_run_with_timeout(..., SECTOR_HEALTH_TIMEOUT_SECONDS)``).

    Split out of ``run_intelligence_tasks`` on 2026-09-19 — see that
    function's NOTE for why the old placement (inside, and after the daily
    block of, ``run_intelligence_tasks``) made this step effectively
    unreachable in production. This wrapper owns only what changed by the
    split: building ``now`` and a fresh per-call ``results`` dict, and the
    dry-run short-circuit. All due-period, retry/backoff, idempotency and
    cross-cycle-race handling is unchanged and still lives in
    :func:`_maybe_run_sector_health_snapshot`.
    """
    if dry_run:
        log.info("[DRY RUN] Would evaluate sector health")
        return {"skipped": "dry_run"}

    now = datetime.now(timezone.utc)
    results: dict[str, Any] = {}
    _maybe_run_sector_health_snapshot(engine, state, now, results)
    return results


def _run_sector_and_intelligence_steps(
    engine: Any,
    state: OperatorState,
    dry_run: bool,
    cycle_result: dict[str, Any],
) -> None:
    """Run the sector-health snapshot and the intelligence-tasks batch as
    two INDEPENDENT ``run_cycle`` steps, each under its own
    ``_run_with_timeout`` budget, sector-health dispatched first.

    Why split (2026-09-19; see
    docs/handoffs/2026-09-19/fable-hermes-sector-schedule.md, "Parent-
    timeout blocker and own-step fix"): the sector-health snapshot used to
    run INSIDE ``run_intelligence_tasks``, after its ``daily_due`` block
    (source_audit -> backtest_scan -> postmortem -> options_improvement ->
    hypothesis_review -> auto_discover -> ``state.last_daily_intel = now``).
    Production journal evidence (2026-09-19 03:48, 04:35, 05:02, 06:05 UTC,
    and the May-2026 log) shows the whole ``intelligence_tasks`` step times
    out at ``INTELLIGENCE_TASKS_TIMEOUT_SECONDS`` (900s) on EVERY observed
    cycle: the daily batch runs with ``catch_up=True`` every cycle because
    the step is abandoned before ``state.last_daily_intel = now`` is ever
    reached, so nothing placed after that point in the function — including
    the old sector-health call — ever ran. This is a separate, confirmed-
    current blocker from the due-period scheduling fix in
    ``daily_task_due``/``_maybe_run_sector_health_snapshot`` (which fixed a
    different, already-merged defect: a 10-minute evaluation window that
    made execution rare even when reached). Neither defect alone is claimed
    to explain the full historical gap; both are real and independent.
    Dispatching sector-health as its own step, ahead of intelligence_tasks
    and with its own short timeout (``SECTOR_HEALTH_TIMEOUT_SECONDS``,
    observed 3-8s in production), makes it reachable every cycle regardless
    of whether intelligence_tasks times out — which it still does; that
    900s budget is unchanged by this split and is explicitly NOT fixed
    here (see the handoff doc's "NOT fixed here" note).

    Blacklist trace (do not "fix" this by adding a can_retry check):
    ``_run_with_timeout`` calls
    ``state.cooldowns.blacklist_for_timeout("sector_health")`` on a
    timeout, same as it does for every named step. But that blacklist
    entry is only ever honoured by a call site that explicitly checks
    ``state.cooldowns.can_retry(<name>)`` before running — traced here to
    exactly four such call sites: ``oracle_cycle``, ``signal_classification``,
    ``anomaly_narration`` and ``knowledge_mapping``. ``intelligence_tasks``
    and ``resolution`` do not consult it either (see
    ``_run_resolution_step``'s docstring for the same trace on
    ``resolution``), so for those steps a timeout's blacklist entry is
    written but never read — it changes nothing about whether the step
    runs again. This new ``sector_health`` step deliberately joins that
    second group: it does NOT check ``can_retry("sector_health")``. Adding
    that check would make a single timeout block every retry for
    ``TIMEOUT_BLACKLIST_HOURS`` (24h), reintroducing a multi-day stall on
    top of a step that already has its own bounded retry/backoff
    (``SECTOR_HEALTH_RETRY_BACKOFF_MINUTES`` = 60, capped at
    ``SECTOR_HEALTH_MAX_ATTEMPTS_PER_DAY`` = 5 per due period, both enforced
    inside ``_maybe_run_sector_health_snapshot``). Retry throttling for
    this step comes entirely from that backoff, not from the cooldown
    blacklist.

    Abandoned-worker handling: ``_run_with_timeout`` abandons (does not
    kill) the worker thread on timeout, so a timed-out sector-health
    attempt can still be running — and can still call
    ``snapshot_all_sectors`` / ``_commit`` — after this function has moved
    on. ``_maybe_run_sector_health_snapshot`` already guards against a
    LATER attempt starting while an earlier one is still in flight (its
    ``state.sector_health_attempt_token`` check). This function closes the
    other half of that gap — an abandoned attempt with no later attempt
    ever starting — by bumping the token itself right here on timeout, so
    the orphan's eventual ``_commit``/upsert-guard sees a stale token
    either way. This bump is taken under ``_SECTOR_HEALTH_STATE_LOCK`` —
    the same lock ``_commit`` holds — so it can never land between an
    in-flight ``_commit``'s token check and its writes.
    """
    # ── Sector health snapshot — own step, own (short) timeout ─────────
    try:
        state.current_step = "sector_health"
        sector_result, ok = _run_with_timeout(
            "sector_health",
            lambda: _run_sector_health_step(engine, state, dry_run),
            SECTOR_HEALTH_TIMEOUT_SECONDS,
            state,
        )
        if ok and sector_result:
            cycle_result["sector_health"] = sector_result
        elif not ok:
            cycle_result["sector_health"] = {"timeout": True}
            # See docstring: bump the token so an abandoned worker's
            # belated _commit()/upsert is discarded as stale even if no
            # later attempt ever starts. Under the same lock _commit uses,
            # so this bump can never interleave with an in-flight
            # _commit's check-then-write.
            with _SECTOR_HEALTH_STATE_LOCK:
                state.sector_health_attempt_token += 1
    except Exception as exc:
        log.warning("Sector health step failed: {e}", e=str(exc))

    # ── Intelligence tasks — unchanged from before the split; still 900s,
    #    still the step production shows timing out on effectively every
    #    cycle (see docstring above). Dispatched second so a slow/timed-out
    #    intelligence_tasks step can never again prevent sector-health from
    #    running.
    try:
        state.current_step = "intelligence_tasks"
        intel_result, ok = _run_with_timeout(
            "intelligence_tasks",
            lambda: run_intelligence_tasks(engine, state, dry_run=dry_run),
            INTELLIGENCE_TASKS_TIMEOUT_SECONDS,
            state,
        )
        if ok and intel_result:
            cycle_result["intelligence"] = intel_result
        elif not ok:
            cycle_result["intelligence"] = {"timeout": True}
    except Exception as exc:
        log.warning("Intelligence tasks failed: {e}", e=str(exc))


# ─── Daily intelligence batch — task table (fable-daily-intel-resumable) ──
#
# Each function below is one step of the old monolithic 02:00 UTC daily
# block, moved verbatim (same imports, same log lines, same results[...]
# keys) into its own callable. The ONE behavioral change versus the
# pre-existing body: the bare `try/except Exception: log.warning(...)`
# that used to wrap each step (swallowing the failure so the sequential
# block could keep going) is gone — that job now belongs to
# _run_with_timeout, called once per task by _run_daily_intel_block below,
# which is what makes each task's success/failure visible to the per-period
# ledger (state.daily_intel_done/daily_intel_attempts). A task that used to
# silently log "X import failed" and move on now silently logs
# "Step 'daily_intel:X' raised: ..." (via _run_with_timeout) and moves on
# — same effect, but now the ledger also counts the attempt.
#
# `_run_intel_task` (scripts/hermes_fixers.py) already swallows exceptions
# from the call it wraps (returns None, records state.task_status, logs a
# warning) — for the six tasks that use it, _daily_intel_raise_if_task_
# status_failed re-raises when task_status shows failure, so
# _run_with_timeout still sees it.


def _daily_intel_raise_if_task_status_failed(state: OperatorState, name: str) -> None:
    """Make a `_run_intel_task`-swallowed failure visible to `_run_with_timeout`.

    `_run_intel_task` records the outcome on `state.task_status[name]` and
    returns None instead of raising. Without this check, every daily-intel
    task that goes through `_run_intel_task` would report ok=True to
    `_run_with_timeout` (and therefore "done" to the per-period ledger)
    even when the wrapped call actually raised — only a genuine timeout
    would ever be visible. This is the only behavioral addition versus the
    pre-existing task body.
    """
    status = state.task_status.get(name)
    if status is not None and status.get("success") is False:
        raise RuntimeError(status.get("error") or f"{name} failed")


def _daily_intel_storage_maintenance(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Queue the bounded storage-maintenance subagent. Dispatch only — no
    LLM call in this step itself (see DAILY_INTEL_DISPATCH_TASK_BUDGET_S).

    Done-vs-done_queued (fable-hermes-daily-intel-resumable review, part C,
    2026-09-20): this step's own work is fully synchronous and finished the
    moment ``_dispatch_daily_storage_maintenance`` returns — it INSERTs one
    ``goal_queue`` row (``enqueue_goal``, dedup-checked, goal_type
    ``hermes_storage_maintenance``; see ``_dispatch_subagent`` in
    scripts/hermes_fixers.py) and nothing else. That is why
    ``_run_daily_intel_block`` marks this task's own outcome
    "done_queued" rather than "done" on success (see
    ``DailyIntelTask.reports_done_queued`` below) — this step is DONE the
    instant the goal is queued; the queued goal's own execution is a
    SEPARATE, asynchronous unit of work tracked by ``goal_queue``/
    ``goal_results`` (intelligence/goal_queue.py), not by this ledger.
    Neither this ledger nor ``_run_daily_intel_block`` ever learns whether
    the queued ``hermes_storage_maintenance`` goal later succeeds, fails,
    or sits unclaimed.

    Held-category reachability (same review, part C): the queued goal is
    claimed by whichever ``scripts/goal_worker.py`` node next polls for a
    ``cpu``-tier goal and executes
    ``handle_hermes_storage_maintenance`` -> ``_inspect_storage_maintenance``
    (scripts/hermes_fixers.py) -> ``storage_curator.run_storage_maintenance``
    (scripts/storage_curator.py). Read end to end: that call chain only
    builds a read-only filesystem/DB inventory report
    (``build_storage_maintenance_report``, a plain ``engine.connect()``
    SELECT — no INSERT/UPDATE/DELETE), writes it to
    ``outputs/storage_maintenance/*.json``/``*.md`` on disk, and — only
    when the report's status is not "ok" — INSERTs one row into
    ``operator_issues`` via ``log_issue`` (scripts/hermes_health.py). No
    call anywhere in that chain reaches ``hypothesis_registry``,
    ``discovered_hypotheses``, ``scanner_weights``, ``trade_postmortems``,
    or any other scoring/learning/backfill/model-registry table — i.e. the
    HELD categories this allow-list withholds. The dispatched child work
    cannot be used to bypass a hold.
    """
    results["storage_maintenance_subagent"] = _dispatch_daily_storage_maintenance(engine, state)


def _daily_intel_source_audit(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    from intelligence.source_audit import run_full_audit
    results["source_audit"] = _run_intel_task(
        "source_audit", run_full_audit, state, engine,
    )
    _daily_intel_raise_if_task_status_failed(state, "source_audit")


def _daily_intel_flow_materialize(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Projects signal_sources into the relational flow tables
    (dark_pool_weekly, etf_flows, insider_trades, congressional_trades,
    junction_point_readings)."""
    from ingestion.flow_materializer import sync_all as _flow_sync_all
    results["flow_materialize"] = _run_intel_task(
        "flow_materialize", _flow_sync_all, state, engine,
    )
    _daily_intel_raise_if_task_status_failed(state, "flow_materialize")


def _daily_intel_backtest_scan(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    from analysis.backtest_scanner import run_full_scan
    results["backtest_scan"] = _run_intel_task(
        "backtest_scan", run_full_scan, state, engine,
    )
    _daily_intel_raise_if_task_status_failed(state, "backtest_scan")


def _daily_intel_postmortem_batch(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    from intelligence.postmortem import batch_postmortem
    results["postmortem_batch"] = _run_intel_task(
        "postmortem_batch", batch_postmortem, state, engine,
        limit=POSTMORTEM_BATCH_LIMIT,
    )
    _daily_intel_raise_if_task_status_failed(state, "postmortem_batch")


def _daily_intel_options_improvement(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    from trading.options_tracker import run_improvement_cycle
    results["options_improvement"] = _run_intel_task(
        "options_improvement", run_improvement_cycle, state, engine,
    )
    _daily_intel_raise_if_task_status_failed(state, "options_improvement")


def _daily_intel_hypothesis_review(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    from analysis.backtest_scanner import review_existing_hypotheses
    results["hypothesis_review"] = _run_intel_task(
        "hypothesis_review", review_existing_hypotheses, state, engine,
    )
    _daily_intel_raise_if_task_status_failed(state, "hypothesis_review")


def _daily_intel_hypothesis_discovery(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Auto-discover new hypotheses from data patterns. Kept its own
    ``_hours_since(last_hypothesis_discovery) >= 20`` guard from the
    pre-existing body — redundant with the per-period ledger now (a task
    only runs once per period regardless), but harmless, and
    ``state.last_hypothesis_discovery`` is still read elsewhere for
    diagnostics (see the log line near the bottom of this module)."""
    if _hours_since(state.last_hypothesis_discovery) >= 20:
        from intelligence.hypothesis_engine import HypothesisGenerator
        hyp_engine = HypothesisGenerator(engine)
        discovered = hyp_engine.auto_discover()
        results["hypothesis_discovery"] = {
            "new_hypotheses": len(discovered),
        }
        log.info(
            "Hypothesis discovery: {n} new hypotheses generated",
            n=len(discovered),
        )
        state.last_hypothesis_discovery = now


def _daily_intel_rag_index(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Re-embed latest intelligence data. Kept its own
    ``_hours_since(last_rag_index) >= 20`` guard — see
    _daily_intel_hypothesis_discovery's docstring for why that's harmless."""
    if _hours_since(state.last_rag_index) >= 20:
        from intelligence.rag import RAGIndexer
        indexer = RAGIndexer(engine)
        indexer.ensure_tables()
        snap_count = indexer.index_snapshots()
        actor_count = indexer.index_actors()
        results["rag_index"] = {
            "snapshots_indexed": snap_count,
            "actors_indexed": actor_count,
        }
        log.info(
            "RAG index refreshed: {s} snapshot chunks, {a} actor chunks",
            s=snap_count, a=actor_count,
        )
        state.last_rag_index = now


def _daily_intel_actor_research(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """LLM enriches sparse actors, follows rabbit holes."""
    from intelligence.actor_researcher import research_batch
    actor_result = research_batch(engine, batch_size=20)
    results["actor_research"] = actor_result
    log.info(
        "Actor research: {u} enriched, {n} new actors, {r} rabbit holes",
        u=actor_result.get("updated", 0),
        n=actor_result.get("new_actors", 0),
        r=actor_result.get("rabbit_holes", 0),
    )


def _daily_intel_icij_linking(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Fuzzy match actors against offshore entities."""
    from intelligence.icij_linker import link_actors
    icij_result = link_actors(engine, min_similarity=0.6, limit=500)
    results["icij_linking"] = {"matches": len(icij_result)}
    log.info("ICIJ linking: {n} matches found", n=len(icij_result))


def _daily_intel_milestone_scoring(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Execution scorecards for all companies."""
    from intelligence.milestone_tracker import scan_all_tickers
    milestones = scan_all_tickers(engine)
    results["milestone_scoring"] = {"companies_scored": len(milestones)}
    log.info("Milestone scoring: {n} companies scored", n=len(milestones))


def _daily_intel_attention_anomaly(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Wikipedia + Trends spike detection."""
    from intelligence.attention_anomaly import get_alerts
    alerts = get_alerts(engine, threshold=60.0)
    results["attention_alerts"] = {"high_alerts": len(alerts)}
    if alerts:
        log.info("ATTENTION: {n} entities with unusual attention", n=len(alerts))


def _daily_intel_edgar_transcripts(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """8-K filings with LLM milestone extraction."""
    from ingestion.altdata.edgar_transcripts import EdgarTranscriptPuller
    edgar = EdgarTranscriptPuller(engine)
    edgar_result = edgar.pull(days_back=30)
    results["edgar_transcripts"] = edgar_result
    log.info("EDGAR: {f} filings, {g} guidance phrases",
             f=edgar_result.get("filings_processed", 0),
             g=edgar_result.get("guidance_extracted", 0))


def _daily_intel_corporate_actions(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Regex-mine 8-Ks for M&A, buybacks, dividends, debt, equity issuance.
    Writes capital_flows rows with period_type='announcement'. Daily: last
    30 days of 8-Ks."""
    from ingestion.altdata.corporate_actions_parser import (
        CorporateActionsParser,
    )
    corp = CorporateActionsParser(engine)
    try:
        corp_result = corp.pull(days_back=30)
    finally:
        corp.close()
    results["corporate_actions"] = corp_result
    log.info(
        "corporate_actions: {r} rows from {f} filings "
        "({h} tickers with hits)",
        r=corp_result.get("rows_inserted", 0),
        f=corp_result.get("filings_scanned", 0),
        h=corp_result.get("tickers_with_hits", 0),
    )


def _daily_intel_capital_flow_rollups(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Derives ttm rows from quarterly XBRL data and folds announcement rows
    into annual_rolled rows. Runs after the XBRL ingestor + corporate_actions
    so it always sees the freshest base rows (corporate_actions dispatched
    just before this in DAILY_INTEL_TASKS, same as before this task)."""
    from intelligence.company_financial_rollups import run_all as cf_rollup_run
    cf_stats = cf_rollup_run(engine)
    results["capital_flow_rollups"] = cf_stats
    log.info(
        "capital_flow_rollups: ttm={t} rolled={r}",
        t=cf_stats.get("ttm_rows", 0),
        r=cf_stats.get("rolled_rows", 0),
    )


def _daily_intel_fundamental_divergence(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Snapshot fundamental-vs-price divergence daily. Runs after
    capital_flow_rollups (same ordering as before this task) so it sees the
    freshest revenue/margin rows."""
    from intelligence.fundamental_divergence import (
        snapshot_all as fd_snapshot_all,
    )
    fd_stats = fd_snapshot_all(engine)
    results["fundamental_divergence"] = fd_stats
    log.info(
        "fundamental_divergence: wrote={w} long={l} short={s}",
        w=fd_stats.get("written", 0),
        l=(fd_stats.get("counts") or {}).get("long_candidate", 0),
        s=(fd_stats.get("counts") or {}).get("short_candidate", 0),
    )


def _daily_intel_holder_deal_overlap(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Pre-positioning detector: cross-references institutional_holdings 13F
    snapshots against capital_flows acquisition announcements. Must run
    after corporate_actions and after the 13F ingestor (same ordering as
    before this task)."""
    from intelligence.holder_deal_overlap import run as hdo_run
    hdo_stats = hdo_run(engine)
    results["holder_deal_overlap"] = hdo_stats
    log.info(
        "holder_deal_overlap: deals={d} overlaps={o} pre={p}",
        d=hdo_stats.get("deals_scanned", 0),
        o=hdo_stats.get("overlaps_written", 0),
        p=hdo_stats.get("pre_positioned", 0),
    )


def _daily_intel_insight_cleanup(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """outputs/llm_insights/ 30-day retention (audit #49, #61)."""
    from outputs.llm_logger import cleanup_old_insights
    n_cleaned = cleanup_old_insights(max_age_days=30)
    if n_cleaned:
        log.info("Insight cleanup: deleted {n} files (>30d)", n=n_cleaned)


def _daily_intel_briefing_cleanup(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """Market briefings — 90-day retention (higher-value artifacts)."""
    from ollama.market_briefing import MarketBriefingEngine
    n_briefings = MarketBriefingEngine.cleanup_old_briefings(max_age_days=90)
    if n_briefings:
        log.info("Briefing cleanup: deleted {n} files (>90d)", n=n_briefings)


def _daily_intel_errors_jsonl_cleanup(
    engine: Any, state: OperatorState, now: datetime, results: dict[str, Any],
) -> None:
    """errors.jsonl — append-only log, truncate to last 5000 lines (~3-4
    days of errors at current rate). Cheap, atomic."""
    from pathlib import Path
    errfile = Path(_GRID_DIR) / ".server-logs" / "errors.jsonl"
    if errfile.exists() and errfile.stat().st_size > 1_000_000:
        lines = errfile.read_text(encoding="utf-8", errors="replace").splitlines()
        if len(lines) > 5000:
            keep = lines[-5000:]
            tmp = errfile.with_suffix(".jsonl.tmp")
            tmp.write_text("\n".join(keep) + "\n", encoding="utf-8")
            tmp.replace(errfile)
            log.info("errors.jsonl rotated: {n} → 5000 lines",
                     n=len(lines))


class DailyIntelTask(NamedTuple):
    """One step of the daily intelligence batch (see DAILY_INTEL_TASKS).

    ``fn(engine, state, now, results)`` runs the step's existing body —
    same imports, same log lines, same ``results[...]`` keys as the
    pre-existing inline block. ``budget_s`` is this step's own
    ``_run_with_timeout`` budget, independent of every other step's (see
    DAILY_INTEL_LLM_TASK_BUDGET_S / _SQL_ / _CLEANUP_ above for the
    documented defaults each task below draws from).
    """
    name: str
    fn: Callable[[Any, OperatorState, datetime, dict[str, Any]], Any]
    budget_s: int
    # True only for a task whose own step is DONE once it enqueues a
    # goal_queue row, before the enqueued work executes (currently only
    # storage_maintenance_subagent — see its docstring above). Makes
    # _run_daily_intel_block record daily_intel_task_outcome[name] =
    # "done_queued" instead of "done" on success, so the ledger cannot be
    # misread as "the dispatched subagent finished" — see part C of
    # docs/handoffs/2026-09-20/fable-hermes-daily-intel-resumable.md.
    reports_done_queued: bool = False


# Ordered exactly as the pre-existing inline block ran them. Do not
# reorder without checking the ordering-dependency notes on
# capital_flow_rollups, fundamental_divergence and holder_deal_overlap
# above — they assume the tasks before them in this tuple already ran
# this period.
DAILY_INTEL_TASKS: tuple[DailyIntelTask, ...] = (
    DailyIntelTask("storage_maintenance_subagent", _daily_intel_storage_maintenance, DAILY_INTEL_DISPATCH_TASK_BUDGET_S, reports_done_queued=True),
    DailyIntelTask("source_audit", _daily_intel_source_audit, DAILY_INTEL_LLM_TASK_BUDGET_S),
    DailyIntelTask("flow_materialize", _daily_intel_flow_materialize, DAILY_INTEL_SQL_TASK_BUDGET_S),
    DailyIntelTask("backtest_scan", _daily_intel_backtest_scan, DAILY_INTEL_LLM_TASK_BUDGET_S),
    DailyIntelTask("postmortem_batch", _daily_intel_postmortem_batch, DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S),
    DailyIntelTask("options_improvement", _daily_intel_options_improvement, DAILY_INTEL_LLM_TASK_BUDGET_S),
    DailyIntelTask("hypothesis_review", _daily_intel_hypothesis_review, DAILY_INTEL_LLM_TASK_BUDGET_S),
    DailyIntelTask("hypothesis_discovery", _daily_intel_hypothesis_discovery, DAILY_INTEL_LLM_TASK_BUDGET_S),
    DailyIntelTask("rag_index", _daily_intel_rag_index, DAILY_INTEL_LLM_TASK_BUDGET_S),
    DailyIntelTask("actor_research", _daily_intel_actor_research, DAILY_INTEL_LLM_TASK_BUDGET_S),
    DailyIntelTask("icij_linking", _daily_intel_icij_linking, DAILY_INTEL_SQL_TASK_BUDGET_S),
    DailyIntelTask("milestone_scoring", _daily_intel_milestone_scoring, DAILY_INTEL_SQL_TASK_BUDGET_S),
    DailyIntelTask("attention_anomaly", _daily_intel_attention_anomaly, DAILY_INTEL_SQL_TASK_BUDGET_S),
    DailyIntelTask("edgar_transcripts", _daily_intel_edgar_transcripts, DAILY_INTEL_LLM_TASK_BUDGET_S),
    DailyIntelTask("corporate_actions", _daily_intel_corporate_actions, DAILY_INTEL_SQL_TASK_BUDGET_S),
    DailyIntelTask("capital_flow_rollups", _daily_intel_capital_flow_rollups, DAILY_INTEL_SQL_TASK_BUDGET_S),
    DailyIntelTask("fundamental_divergence", _daily_intel_fundamental_divergence, DAILY_INTEL_SQL_TASK_BUDGET_S),
    DailyIntelTask("holder_deal_overlap", _daily_intel_holder_deal_overlap, DAILY_INTEL_SQL_TASK_BUDGET_S),
    DailyIntelTask("insight_cleanup", _daily_intel_insight_cleanup, DAILY_INTEL_CLEANUP_TASK_BUDGET_S),
    DailyIntelTask("briefing_cleanup", _daily_intel_briefing_cleanup, DAILY_INTEL_CLEANUP_TASK_BUDGET_S),
    DailyIntelTask("errors_jsonl_cleanup", _daily_intel_errors_jsonl_cleanup, DAILY_INTEL_CLEANUP_TASK_BUDGET_S),
)


# ─── Safe initial task allow-list (fable-daily-intel-resumable review ────
#     amendment, 2026-09-20) ─────────────────────────────────────────────
#
# Standing holds (controller instruction, not re-litigated here): scorer
# execution / signal scoring, historical repair or backfill, and
# learning/research writes (hypothesis registry, backtests, model
# registry, postmortems that feed learning) are NOT authorised to run on
# a schedule yet. DAILY_INTEL_TASKS above is the full ~21-task table this
# task's resumability work made independently retryable; that table is
# NOT itself an authorisation to run every task — DAILY_INTEL_INITIAL_
# ALLOWLIST is the actual gate _run_daily_intel_block enforces. A task
# absent from this frozenset is "held": _run_daily_intel_block skips its
# `fn` entirely (never dispatched, never attempted, never timed), and it
# can never appear in daily_intel_done/daily_intel_skipped_for_period —
# see DAILY_INTEL_HOLD_REASONS below and the per-task table in
# docs/handoffs/2026-09-20/fable-hermes-daily-intel-resumable.md for the
# full writes/classification evidence. Enabling a held task later means
# editing this frozenset in its own reviewed change — not a runtime flag,
# not something this scheduler decides on its own.
#
# Classification method: read each task's wrapped function body (not just
# its docstring) for (a) what table(s)/file(s) it writes, (b) whether it
# calls llm.router (an LLM/Tier reference) anywhere in its own file, and
# (c) whether what it writes is deterministic-derived (safe) versus
# learning/scoring/backfill (held). Two tasks whose PRE-EXISTING per-task
# budget constant name implied "LLM-backed" (DAILY_INTEL_LLM_TASK_BUDGET_S)
# turned out, on reading the code, to have NO llm.router call anywhere in
# their module and to write only derived audit tables — reclassified to
# `allow` below with the file:line evidence in DAILY_INTEL_HOLD_REASONS'
# sibling comments and the handoff doc; every other task keeps the
# controller's default hold.
DAILY_INTEL_INITIAL_ALLOWLIST: frozenset[str] = frozenset({
    # Dispatch only — enqueues a goal_queue row for a bounded subagent;
    # no LLM call and no learning-table write in this step itself
    # (scripts/hermes_fixers.py::_execute_hermes_repair_command,
    # DISPATCH_SUBAGENT branch -> intelligence.goal_queue.enqueue_goal).
    "storage_maintenance_subagent",
    # ingestion/flow_materializer.py::sync_all — deterministic projection
    # of signal_sources into relational flow tables (dark_pool_weekly,
    # etf_flows, insider_trades, congressional_trades,
    # junction_point_readings). No LLM, no learning table.
    "flow_materialize",
    # intelligence/icij_linker.py::link_actors — deterministic fuzzy
    # string matching against ICIJ offshore-entity records. No LLM.
    "icij_linking",
    # intelligence/attention_anomaly.py::get_alerts — deterministic
    # Wikipedia/Trends spike detection; read-only for this step (logs
    # only, no DB write of its own beyond what get_alerts's own upstream
    # ingestion already persists).
    "attention_anomaly",
    # ingestion/altdata/corporate_actions_parser.py — deterministic regex
    # mining of 8-Ks into capital_flows rows (period_type='announcement').
    # No LLM.
    "corporate_actions",
    # intelligence/company_financial_rollups.py::run_all — deterministic
    # TTM/annual-rolled capital_flows derivation from XBRL + the
    # announcement rows corporate_actions just wrote. No LLM.
    "capital_flow_rollups",
    # intelligence/fundamental_divergence.py::snapshot_all — deterministic
    # fundamental-vs-price divergence snapshot. No LLM.
    "fundamental_divergence",
    # intelligence/holder_deal_overlap.py::run — deterministic
    # cross-reference of 13F institutional holdings against capital_flows
    # acquisition announcements. No LLM.
    "holder_deal_overlap",
    # Three filesystem cleanups — pre-existing, bounded, deletion/
    # truncation only, no new data written.
    "insight_cleanup",
    "briefing_cleanup",
    "errors_jsonl_cleanup",
})

# Every DAILY_INTEL_TASKS name NOT in DAILY_INTEL_INITIAL_ALLOWLIST above,
# with the standing-hold category it falls under and the file:line
# evidence for the write that earns it that category. Exists so the
# classification is machine-checkable (see
# TestDailyIntelAllowlistClassification in
# tests/test_hermes_daily_intel_resumable.py: every DAILY_INTEL_TASKS name
# must appear in EXACTLY ONE of DAILY_INTEL_INITIAL_ALLOWLIST /
# DAILY_INTEL_HOLD_REASONS) rather than only documented in prose.
DAILY_INTEL_HOLD_REASONS: dict[str, str] = {
    "source_audit": (
        "held for the INITIAL subset by the release coordinator (2026-09-20): "
        "no LLM, but its writes are not idempotent — run_full_audit "
        "(intelligence/source_audit.py) appends plain-INSERT rows to "
        "source_accuracy and source_discrepancies on every run (no ON "
        "CONFLICT), so a retry after an abandoned run duplicates audit rows; "
        "it also rewrites source_catalog.priority_rank, which steers "
        "ingestion priority. Re-run semantics must be settled before it "
        "graduates"
    ),
    "rag_index": (
        "held for the INITIAL subset by the release coordinator (2026-09-20): "
        "no LLM, local embeddings only, but RAGIndexer rebuilds "
        "intelligence_embeddings by DELETE-then-bulk-INSERT per source_type; "
        "an abandoned run keeps executing in its orphan thread and readers "
        "see a partially emptied index until it finishes, and a later retry "
        "repeats the full delete/rebuild. Needs a swap-in rebuild (or an "
        "accepted window) before it graduates"
    ),
    "hypothesis_discovery": (
        "learning write — HypothesisGenerator.auto_discover() "
        "(intelligence/hypothesis_engine.py) inserts/updates "
        "discovered_hypotheses, hypothesis_postmortems and "
        "hypothesis_boost_log directly"
    ),
    "hypothesis_review": (
        "learning write — review_existing_hypotheses "
        "(analysis/backtest_scanner.py) is LLM-driven (llm.router "
        "Tier.ORACLE) and mutates hypothesis_registry state/kill_reason"
    ),
    "backtest_scan": (
        "learning write + backtest — run_full_scan "
        "(analysis/backtest_scanner.py) is LLM-gated (llm.router "
        "Tier.ORACLE sanity-checks winners) and inserts into "
        "hypothesis_registry via generate_hypotheses_from_winners"
    ),
    "postmortem_batch": (
        "postmortem write that feeds learning — batch_postmortem "
        "(intelligence/postmortem.py) is LLM-narrated (llm.router "
        "Tier.REASON) and inserts trade_postmortems rows"
    ),
    "options_improvement": (
        "model/weight registry write + scorer — run_improvement_cycle "
        "(trading/options_tracker.py) writes scanner_weights (a de-facto "
        "model registry) and updates options_recommendations scoring; "
        "its report step also calls llm.router Tier.REASON"
    ),
    "milestone_scoring": (
        "scorer execution (standing hold) — scan_all_tickers "
        "(intelligence/milestone_tracker.py) is execution/milestone "
        "scoring by category even though the current function body is "
        "read-only (no INSERT/UPDATE found); held on category, not on "
        "current write footprint, since a future change to persist "
        "scorecards must not silently graduate this task"
    ),
    "actor_research": (
        "LLM-driven write, not shown to be derived-only — research_batch "
        "(intelligence/actor_researcher.py) uses llm.router Tier.REASON "
        "to synthesize actor profile JSON, updates the actors entity "
        "registry, and can create new actor rows ('rabbit holes') that "
        "feed further LLM research"
    ),
    "edgar_transcripts": (
        "LLM-driven write, not shown to be derived-only — "
        "EdgarTranscriptPuller.pull (ingestion/altdata/edgar_transcripts.py) "
        "uses llm.router Tier.REASON (and a local Gemma extractor) to "
        "extract guidance/milestone figures and inserts them as raw_series "
        "data points, not just audit metadata"
    ),
}


# ─── No-overlap guard + late-publish fencing for daily-intel tasks ───────
#     (fable-daily-intel-resumable review amendment, 2026-09-20)
#
# Reuses the two patterns already established elsewhere in this module
# rather than inventing a third: the no-overlap in-flight registry from
# scripts/hermes_fixers.py::_REPAIRS_IN_FLIGHT (#582), and the
# capture-a-token-at-start / commit-only-if-still-current pattern from
# _maybe_run_sector_health_snapshot's _SECTOR_HEALTH_STATE_LOCK /
# sector_health_attempt_token (#580).
#
# _DAILY_INTEL_IN_FLIGHT: task name -> {"token": int, "thread": int | None,
# "started": float | None}. One entry per task, created just before that
# task's _run_with_timeout call and never deleted afterwards (bounded to
# len(DAILY_INTEL_TASKS) entries — harmless to keep). "thread" is filled
# in BY THE WORKER ITSELF (see _run_task closure below) the moment it
# starts running — unlike _retry_source, which runs directly inside an
# already-existing worker thread and can capture threading.get_ident()
# at its own top, _run_daily_intel_block's per-task call is dispatched
# via _run_with_timeout's ThreadPoolExecutor, so the new worker's ident
# does not exist yet at registration time in the driver (main) thread.
#
# RLock, not Lock: mirrors _REPAIRS_LOCK's own reasoning — the driver
# thread and a task's worker thread both take this lock, and a future
# amendment that has one call another under the same lock (as
# _retry_source already does with _next_repair_token) would self-deadlock
# on a plain Lock.
_DAILY_INTEL_LOCK = threading.RLock()
_DAILY_INTEL_IN_FLIGHT: dict[str, dict[str, Any]] = {}
_daily_intel_token_seq = 0


def _next_daily_intel_token() -> int:
    global _daily_intel_token_seq
    with _DAILY_INTEL_LOCK:
        _daily_intel_token_seq += 1
        return _daily_intel_token_seq


def _daily_intel_thread_alive(ident: int | None) -> bool:
    """True if a live thread with this identity still exists.

    Same safety-net reasoning as scripts/hermes_fixers.py::
    _thread_is_alive: an in-flight entry is not trusted indefinitely on
    its own — if the process somehow lost track of the thread, a stale
    entry must not permanently block retries for that task.
    """
    if ident is None:
        return False
    return any(t.ident == ident and t.is_alive() for t in threading.enumerate())


def _run_daily_intel_block(
    engine: Any,
    state: OperatorState,
    now: datetime,
    results: dict[str, Any],
) -> None:
    """Execute the ALLOW-LISTED subset of DAILY_INTEL_TASKS in order,
    resumable across cycles, with no-overlap and late-publish guards.

    Called from run_intelligence_tasks when ``daily_due`` (see that
    function's "Daily at 2:00 AM (with catch-up)" scheduling block) — same
    trigger conditions as before this task; only what happens once
    triggered has changed.

    Allow-list gate (review amendment): a task whose name is NOT in
    ``DAILY_INTEL_INITIAL_ALLOWLIST`` is "held" — its ``fn`` is never
    called, it is never attempted, it is recorded on
    ``state.daily_intel_task_outcome[name] = "held"`` every time the loop
    reaches it, and it is invisible to the period-completion check below
    (held tasks are excluded from both ``total`` and ``done_count``, so a
    held task can never block — or fake — period completion). Enabling a
    held task means editing ``DAILY_INTEL_INITIAL_ALLOWLIST`` in its own
    reviewed change, not a runtime decision this function makes.

    Per-period ledger: ``state.daily_intel_done`` /
    ``daily_intel_skipped_for_period`` / ``daily_intel_attempts`` /
    ``daily_intel_task_outcome``, keyed to ``state.daily_intel_period``
    (the due period's ISO date, boundary_hour=DAILY_INTEL_BOUNDARY_HOUR).
    A due period that differs from the ledger's recorded period means the
    ledger has rolled over: all four dicts (plus
    ``daily_intel_period_outcome``) are cleared/reset and
    ``state.daily_intel_period`` is updated before anything runs.

    Idempotent-redo note: state is only persisted at the END of a cycle
    (the analytical_snapshots write in save_cycle_snapshot), not after
    each task inside this function. A mid-cycle process restart therefore
    can re-run a task this call already finished but hadn't yet had a
    chance to persist — safe, because every task's own DB writes are
    idempotent upserts/inserts-with-dedupe (a property this task
    explicitly did NOT change), so a redo just repeats the same write.

    Per-cycle budget: ``DAILY_INTEL_CYCLE_BUDGET_SECONDS`` is checked
    BEFORE starting each task (not mid-task) against cumulative wall time
    already spent in this call. Once exhausted, the loop stops for this
    cycle; the next ``daily_due`` call (state.last_daily_intel is not set
    until every ALLOW-LISTED task is done-or-skipped — see below) resumes
    at the first undone task. A held task costs no budget (skipped before
    the budget check) and an in_flight skip costs no budget either
    (skipped before the per-task clock starts).

    No-overlap guard (review amendment): before starting a task, this
    loop checks ``_DAILY_INTEL_IN_FLIGHT[task.name]`` — if an entry exists
    AND its recorded thread is still alive (``_daily_intel_thread_alive``),
    a previous attempt's worker (orphaned by ``_run_with_timeout``'s
    timeout-abandons-rather-than-kills behaviour — see its own docstring)
    is still running. This retry is skipped: logged as ``in_flight``,
    ``state.daily_intel_task_outcome[name] = "in_flight"``, and it counts
    as NEITHER an attempt NOR a completion — ``daily_intel_attempts`` is
    not incremented and the loop proceeds to the next task. Otherwise a
    fresh attempt token is minted (``_next_daily_intel_token``) and
    registered before the task's ``_run_with_timeout`` call, so a
    concurrent registration race is impossible (both the check and the
    register happen under ``_DAILY_INTEL_LOCK``).

    Late-publish guard (review amendment): each attempt's task ``fn`` runs
    against a LOCAL ``results`` dict, not the shared one, via a small
    ``_run_task`` closure that (a) records its own thread ident into the
    in-flight entry the moment it starts (under the lock — this is the
    only place ``"thread"`` is ever set), (b) calls ``task.fn(...)``, then
    (c) checks — again under the lock — whether its token is still the
    entry's current token; if not, it logs ``"daily_intel task <name>
    abandoned — exiting without publishing"`` and does nothing further
    (its local results dict is simply discarded — never merged into the
    shared ``results``). After ``_run_with_timeout`` returns to the driver
    (synchronously, either because the task finished in time or because
    the budget was exceeded and the worker was abandoned), the driver
    itself re-checks the token under the same lock: on ``ok=True`` it
    merges the local results into the shared ``results`` and marks the
    task done (the token cannot have moved in this branch — nothing
    invalidates it before this point on the success path); on ``ok=False``
    it immediately invalidates the entry's token (mints a fresh one this
    attempt does not hold) BEFORE recording the attempt/skip outcome —
    this is what makes the timeout path itself bump the token even when
    no retry ever starts, so a late-returning orphaned worker's own
    ``_run_task`` epilogue (b)/(c) above sees a stale token and publishes
    nothing, exactly mirroring ``_run_sector_and_intelligence_steps``'s
    timeout-path bump of ``sector_health_attempt_token``. The in-flight
    entry itself (with its now-stale token but still-live thread ident)
    is deliberately NOT deleted on a timeout — the NEXT attempt's
    no-overlap check still needs that thread ident to detect the orphan
    is still running.

    Per-task attempts: ``ok=False`` (timeout or exception — see
    ``_daily_intel_raise_if_task_status_failed`` for the six tasks that go
    through ``_run_intel_task``) increments its attempt count; at
    ``DAILY_INTEL_MAX_ATTEMPTS`` the task is marked ``skipped_for_period``
    (also recorded in ``daily_intel_done``, so the loop treats it as done
    — it cannot block the tasks behind it;
    ``daily_intel_task_outcome[name] = "skipped_for_period"``) and the
    block continues to the NEXT task rather than aborting.

    ``cooldowns.can_retry`` is deliberately NOT consulted here — same
    reasoning ``_run_sector_and_intelligence_steps`` documents for the
    sector-health/intelligence-tasks split: the blacklist entry
    ``_run_with_timeout`` writes on a timeout is only honoured by call
    sites that explicitly check ``state.cooldowns.can_retry(<name>)``
    before running (exactly four elsewhere in this module — oracle_cycle,
    signal_classification, anomaly_narration, knowledge_mapping), and
    daily-intel task names are not among them. This ledger's own
    ``DAILY_INTEL_MAX_ATTEMPTS`` is the throttle for a
    persistently-failing daily-intel task; adding the 24h can_retry
    blacklist on top would mean a single timeout blocks that task for a
    full day regardless of the per-period ledger's own, much shorter,
    per-period skip.

    ``state.last_daily_intel = now`` is set ONLY when every ALLOW-LISTED
    task is done, done_queued, or skipped_for_period — i.e.
    ``state.daily_intel_done`` has an entry, dated to the current period,
    for every name in ``DAILY_INTEL_INITIAL_ALLOWLIST``. Held tasks are
    excluded from this check entirely. ``state.daily_intel_period_outcome``
    is set in the same branch to one of four values — see
    ``OperatorState.daily_intel_period_outcome``'s docstring
    (scripts/hermes_health.py) for the full matrix; in short, it is always
    one of the two ``"..._for_enabled_tasks[_with_skips]"`` values while
    any task is held (true today), and only the bare
    ``"complete"``/``"complete_with_skips"`` once none are. This is what
    ``daily_due`` (in ``run_intelligence_tasks``) reads to decide whether
    the whole block is due again.

    Abandonment truth (fable-hermes-daily-intel-resumable review, part B,
    2026-09-20) — read this before assuming a timeout means a task's work
    did not happen. The attempt-token check above runs AFTER
    ``task.fn(...)`` has already returned (or, for an abandoned worker,
    whenever it eventually does) — it decides only whether THIS ledger
    publishes that return, not whether the call happened. Concretely: on a
    timeout, ``_run_with_timeout`` abandons the worker thread rather than
    killing it (Python's ``concurrent.futures`` has no API to kill a
    running thread — see ``_run_with_timeout``'s own docstring), so
    ``task.fn`` keeps executing to completion in that orphaned thread and
    performs EVERY ONE of its underlying effects exactly as if it had
    finished on time: its DB writes (INSERT/UPDATE/UPSERT), its file
    writes/deletions, its enqueued goal_queue row (storage_maintenance_
    subagent only), all happen. What is prevented is narrower: (1) this
    ledger's ``daily_intel_done``/``daily_intel_task_outcome`` update for
    that attempt (the token check discards the orphan's local ``results``
    and skips the ledger write — see the late-publish guard above), and
    (2) a concurrent retry of the SAME task colliding with the still-
    running orphan (the in-flight registry above). Neither of those is the
    task's own work being prevented — none of the 13 allow-listed tasks'
    ``fn`` accepts a ``should_continue``/cooperative-cancellation
    parameter (checked: every ``DailyIntelTask.fn`` signature is
    ``fn(engine, state, now, results)``), so there is no cooperative exit
    point an abandoned run could even observe. See the per-task "effects
    an abandoned run can still perform" column in
    docs/handoffs/2026-09-20/fable-hermes-daily-intel-resumable.md (answer,
    for every one of the 13: all of its DB writes / dispatched child work
    / external calls / file writes — the same effects it would have
    performed on a timely return) and
    ``tests/test_hermes_daily_intel_resumable.py::
    TestAbandonmentDoesNotPreventTaskEffects``.
    """
    period_iso = _period_boundary(now, DAILY_INTEL_BOUNDARY_HOUR).date().isoformat()

    if state.daily_intel_period != period_iso:
        state.daily_intel_period = period_iso
        state.daily_intel_done = {}
        state.daily_intel_skipped_for_period = {}
        state.daily_intel_attempts = {}
        state.daily_intel_task_outcome = {}
        state.daily_intel_period_outcome = None

    ran: list[str] = []
    skipped_for_period: list[str] = []
    held: list[str] = []
    in_flight_skipped: list[str] = []
    budget_used = 0.0

    for task in DAILY_INTEL_TASKS:
        if task.name not in DAILY_INTEL_INITIAL_ALLOWLIST:
            state.daily_intel_task_outcome[task.name] = "held"
            held.append(task.name)
            continue
        if state.daily_intel_done.get(task.name) == period_iso:
            continue
        if budget_used >= DAILY_INTEL_CYCLE_BUDGET_SECONDS:
            break

        with _DAILY_INTEL_LOCK:
            existing = _DAILY_INTEL_IN_FLIGHT.get(task.name)
            if existing is not None and _daily_intel_thread_alive(existing.get("thread")):
                age_s = (
                    time.monotonic() - existing["started"]
                    if existing.get("started") is not None else 0.0
                )
                log.warning(
                    "daily_intel task '{n}' in_flight (previous worker "
                    "still running, age {a:.0f}s) — skipping this cycle",
                    n=task.name, a=age_s,
                )
                state.daily_intel_task_outcome[task.name] = "in_flight"
                in_flight_skipped.append(task.name)
                continue
            token = _next_daily_intel_token()
            _DAILY_INTEL_IN_FLIGHT[task.name] = {
                "token": token, "thread": None, "started": None,
            }

        t0 = time.monotonic()
        with _DAILY_INTEL_LOCK:
            _DAILY_INTEL_IN_FLIGHT[task.name]["started"] = t0

        local_results: dict[str, Any] = {}

        def _run_task(t=task, tok=token, lr=local_results) -> None:
            with _DAILY_INTEL_LOCK:
                entry = _DAILY_INTEL_IN_FLIGHT.get(t.name)
                if entry is not None and entry.get("token") == tok:
                    entry["thread"] = threading.get_ident()
            try:
                t.fn(engine, state, now, lr)
            finally:
                # Runs whether t.fn returned normally or raised — a
                # worker finishing (on time OR late/abandoned) must clear
                # its own "thread" marker itself, from inside the worker
                # thread, the instant it is actually done. Relying on the
                # NEXT call's _daily_intel_thread_alive(ident) check alone
                # would race the OS thread's own teardown timing: a
                # thread that just returned from t.fn can still show
                # is_alive()==True for a brief window while Python tears
                # it down, which made a fast synchronous failure look
                # "in_flight" to an immediately-following retry in
                # testing. Same fix shape as
                # scripts/hermes_fixers.py::_retry_source's `finally`
                # block deleting its own _REPAIRS_IN_FLIGHT entry before
                # returning, rather than trusting is_alive() for the
                # normal-completion case.
                with _DAILY_INTEL_LOCK:
                    entry = _DAILY_INTEL_IN_FLIGHT.get(t.name)
                    abandoned = entry is None or entry.get("token") != tok
                    if entry is not None and entry.get("thread") == threading.get_ident():
                        entry["thread"] = None
                    if abandoned:
                        log.warning(
                            "daily_intel task {n} abandoned — exiting "
                            "without publishing",
                            n=t.name,
                        )

        _, ok = _run_with_timeout(
            f"daily_intel:{task.name}",
            _run_task,
            task.budget_s,
            state,
        )
        budget_used += time.monotonic() - t0
        ran.append(task.name)

        with _DAILY_INTEL_LOCK:
            entry = _DAILY_INTEL_IN_FLIGHT.get(task.name)
            current = entry is not None and entry.get("token") == token
            if not ok and current:
                # Invalidate NOW so a late-returning orphan cannot publish
                # later even if no retry ever starts (see docstring). Keep
                # the entry (thread ident intact) for the next attempt's
                # no-overlap check.
                entry["token"] = _next_daily_intel_token()
                current = False

            if ok and current:
                results.update(local_results)
                state.daily_intel_done[task.name] = period_iso
                state.daily_intel_task_outcome[task.name] = (
                    "done_queued" if task.reports_done_queued else "done"
                )
            elif not ok:
                attempts = state.daily_intel_attempts.get(task.name, 0) + 1
                state.daily_intel_attempts[task.name] = attempts
                if attempts >= DAILY_INTEL_MAX_ATTEMPTS:
                    state.daily_intel_done[task.name] = period_iso
                    state.daily_intel_skipped_for_period[task.name] = period_iso
                    state.daily_intel_task_outcome[task.name] = "skipped_for_period"
                    skipped_for_period.append(task.name)
                    log.warning(
                        "daily_intel: task '{n}' skipped_for_period after "
                        "{a} failed attempts (period={p})",
                        n=task.name, a=attempts, p=period_iso,
                    )

    enabled_tasks = [t for t in DAILY_INTEL_TASKS if t.name in DAILY_INTEL_INITIAL_ALLOWLIST]
    total = len(enabled_tasks)
    done_count = sum(
        1 for t in enabled_tasks if state.daily_intel_done.get(t.name) == period_iso
    )
    remaining = [
        t.name for t in enabled_tasks
        if state.daily_intel_done.get(t.name) != period_iso
    ]

    if total and done_count == total:
        state.last_daily_intel = now
        any_skipped_this_period = any(
            v == period_iso for v in state.daily_intel_skipped_for_period.values()
        )
        # "_for_enabled_tasks" wording (fable-hermes-daily-intel-resumable
        # review, part E, 2026-09-20): the bare "complete"/"complete_with_
        # skips" values are reserved for the case where every
        # DAILY_INTEL_TASKS entry is allow-listed (no held tasks at all).
        # As long as any task is held — true today (13 of 21 allow-listed)
        # — "complete" must never be reported on its own, since that could
        # be misread as "the whole daily-intel batch ran." See
        # OperatorState.daily_intel_period_outcome's docstring
        # (scripts/hermes_health.py) for the full four-value matrix.
        all_tasks_enabled = len(DAILY_INTEL_INITIAL_ALLOWLIST) == len(DAILY_INTEL_TASKS)
        if all_tasks_enabled:
            state.daily_intel_period_outcome = (
                "complete_with_skips" if any_skipped_this_period else "complete"
            )
        else:
            state.daily_intel_period_outcome = (
                "complete_for_enabled_tasks_with_skips" if any_skipped_this_period
                else "complete_for_enabled_tasks"
            )

    log.info(
        "daily_intel: period={p} done={d}/{t} ran={r} skipped_for_period={s} "
        "held={h} in_flight={f} remaining={rem} budget_used={b:.1f}s",
        p=period_iso, d=done_count, t=total, r=ran, s=skipped_for_period,
        h=held, f=in_flight_skipped, rem=remaining, b=budget_used,
    )

    if total and done_count == total:
        # Completion-only summary, in the exact wording the release
        # controller asked for (part E): done vs done_queued vs
        # skipped_for_period are kept as SEPARATE counts (not folded
        # together) so a reader can tell "ran to completion in this step"
        # (done) apart from "only enqueued a subagent whose own completion
        # this ledger does not track" (done_queued) — see
        # DailyIntelTask.reports_done_queued and part C of
        # docs/handoffs/2026-09-20/fable-hermes-daily-intel-resumable.md.
        # held is reported separately from skipped_for_period too: a held
        # task was never attempted at all (standing controller hold),
        # while skipped_for_period means it WAS attempted DAILY_INTEL_MAX_
        # ATTEMPTS times and gave up — very different operational meanings
        # that must not be merged into one count. Emitted AFTER the
        # per-cycle progress line above (not instead of it) so existing
        # per-cycle log consumers/tests are unaffected.
        done_only = sum(
            1 for t in enabled_tasks
            if state.daily_intel_task_outcome.get(t.name) == "done"
        )
        done_queued = sum(
            1 for t in enabled_tasks
            if state.daily_intel_task_outcome.get(t.name) == "done_queued"
        )
        skipped_count = sum(
            1 for t in enabled_tasks
            if state.daily_intel_task_outcome.get(t.name) == "skipped_for_period"
        )
        log.info(
            "daily_intel: period={p} {outcome} enabled={n} done={d} "
            "done_queued={dq} skipped_for_period={s} held={h}",
            p=period_iso, outcome=state.daily_intel_period_outcome, n=total,
            d=done_only, dq=done_queued, s=skipped_count, h=len(held),
        )


def run_intelligence_tasks(
    engine: Any,
    state: OperatorState,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run all intelligence module tasks on their respective schedules.

    Schedule:
        Every 4 hours:
            - trust_scorer.run_trust_cycle
            - options_recommender.generate_recommendations
            - cross_reference.run_all_checks (checks only, no LLM narrative)

        Every 6 hours (aligned with oracle cycle):
            - options_tracker.score_expired_recommendations
            - lever_pullers.identify_lever_pullers
            - actor_network.track_wealth_migration

        Daily at 2:00 AM:
            - source_audit.run_full_audit
            - backtest_scanner.run_full_scan (with LLM sanity check)
            - postmortem.batch_postmortem
            - options_tracker.run_improvement_cycle
            - backtest_scanner.review_existing_hypotheses

        Weekly (Sunday 3:00 AM):
            - cross_reference.run_all_checks (full, with LLM narrative)
            - lever_pullers.generate_lever_report
            - trust_scorer.generate_trust_report
            - actor_network.generate_actor_report
    """
    results: dict[str, Any] = {}
    now = datetime.now(timezone.utc)

    if dry_run:
        log.info("[DRY RUN] Would run intelligence tasks")
        return {"skipped": "dry_run"}

    # ── Earnings events → earnings_calendar back-compat sync ────────
    # Cheap (~50ms single SQL call) so runs FIRST — before the
    # intelligence_tasks 360s budget can be consumed by slow LLM jobs
    # later in this function. Calls DB-side
    # ``sync_earnings_events_to_calendar()`` (see migration
    # ``20260517_earnings_events_compat.sql``) which mirrors new
    # ``earnings_events`` rows into the legacy ``earnings_calendar``
    # table. Function returns (inserted_count, total_events) — logged
    # for visibility on each fire.
    if _minutes_since(state.last_earnings_calendar_sync) >= EARNINGS_CALENDAR_SYNC_INTERVAL_MINUTES:
        def _sync_earnings_events_to_calendar(eng: Any) -> dict[str, Any]:
            with eng.begin() as conn:
                row = conn.execute(
                    text("SELECT inserted_count, total_events "
                         "FROM sync_earnings_events_to_calendar()")
                ).fetchone()
            inserted = int(row[0]) if row and row[0] is not None else 0
            total = int(row[1]) if row and row[1] is not None else 0
            return {"inserted": inserted, "total_events": total}

        try:
            sync_result = _run_intel_task(
                "earnings_events_to_calendar_sync",
                _sync_earnings_events_to_calendar,
                state,
                engine,
            )
            if sync_result:
                results["earnings_events_to_calendar_sync"] = sync_result
                log.info(
                    "earnings_events → earnings_calendar sync: {i} inserted, {t} total source rows",
                    i=sync_result.get("inserted", 0),
                    t=sync_result.get("total_events", 0),
                )
        except Exception as exc:
            log.warning("earnings_events_to_calendar sync failed: {e}", e=str(exc))
        state.last_earnings_calendar_sync = now

    # Periodic active-hypothesis scoring — every 30 minutes, batch up to
    # 200 overdue hypos per tick. Closes the loop that auto_discover() in
    # the daily batch was creating but nothing was scoring (the gap
    # documented in the memo ``project-active-hypo-scoring-gap`` and the
    # 2026-05-15 handoff). Cadence kept short so the ~25k current overdue
    # backlog drains across the next ~2-3 days at ~15 scored/sec on grid-svr.
    # Dedented out of daily_due — was only firing 2:00-2:10 UTC, now every loop
    if _minutes_since(state.last_active_hypo_scoring) >= ACTIVE_HYPO_SCORING_INTERVAL_MINUTES:
        try:
            from intelligence.hypothesis_engine import score_due_active_hypotheses
            results["active_hypo_scoring"] = _run_intel_task(
                "active_hypo_scoring",
                score_due_active_hypotheses,
                state,
                engine,
                batch_size=ACTIVE_HYPO_SCORING_BATCH_SIZE,
                max_runtime_s=ACTIVE_HYPO_SCORING_MAX_RUNTIME_S,
            )
        except Exception as exc:
            log.warning("Active hypothesis scoring failed: {e}", e=str(exc))
        state.last_active_hypo_scoring = now

    # ── Every 4 hours ────────────────────────────────────────────────

    if _hours_since(state.last_trust_cycle) >= 4:
        try:
            from intelligence.trust_scorer import run_trust_cycle
            tc_result = _run_intel_task(
                "trust_cycle", run_trust_cycle, state, engine,
            )
            results["trust_cycle"] = tc_result
            # Mirror to Obsidian session log (best-effort).
            try:
                from intelligence.obsidian_log import log_trust_cycle
                scoring = (tc_result or {}).get("scoring") or {}
                if isinstance(scoring, dict):
                    log_trust_cycle(scoring)
            except Exception as exc:  # noqa: BLE001
                log.debug("Obsidian log_trust_cycle skipped: {e}", e=str(exc))
        except Exception as exc:
            log.warning("Trust cycle import failed: {e}", e=str(exc))

        # TimesFM signal forecasts: run before thesis scorer so forecasts are fresh
        if _hours_since(state.last_signal_forecasts) >= 4:
            try:
                from inference.timesfm_service import forecast_signals
                fc_results = forecast_signals(engine, horizon=30)
                results["signal_forecasts"] = {
                    "forecasted": len(fc_results),
                    "directions": {
                        "UP": sum(1 for f in fc_results if f.direction == "UP"),
                        "DOWN": sum(1 for f in fc_results if f.direction == "DOWN"),
                        "FLAT": sum(1 for f in fc_results if f.direction == "FLAT"),
                    },
                }
                log.info("TimesFM forecasted {n} signals", n=len(fc_results))
            except Exception as exc:
                log.warning("TimesFM forecast cycle failed: {e}", e=str(exc))
            state.last_signal_forecasts = now

        # Thesis snapshot: score current thesis and persist for accuracy tracking
        try:
            from analysis.thesis_scorer import score_thesis, snapshot_thesis
            thesis = score_thesis(engine)
            snap_id = snapshot_thesis(engine, thesis)
            results["thesis_snapshot"] = {
                "direction": thesis["direction"],
                "score": thesis["score"],
                "conviction": thesis["conviction"],
                "snapshot_id": snap_id,
            }
            log.info("Thesis snapshot: {d} score={s} id={id}",
                     d=thesis["direction"], s=thesis["score"], id=snap_id)
        except Exception as exc:
            log.warning("Thesis snapshot failed: {e}", e=str(exc))

        state.last_trust_cycle = now

    if _hours_since(state.last_options_recommendations) >= 4:
        try:
            from trading.options_recommender import OptionsRecommender
            recommender = OptionsRecommender(db_engine=engine)
            results["options_recommendations"] = _run_intel_task(
                "options_recommendations",
                recommender.generate_recommendations,
                state,
                engine=engine,
            )
        except Exception as exc:
            log.warning("Options recommender import failed: {e}", e=str(exc))
        state.last_options_recommendations = now

    if _hours_since(state.last_cross_reference_checks) >= 4:
        try:
            from intelligence.cross_reference import run_all_checks
            results["cross_reference_checks"] = _run_intel_task(
                "cross_reference_checks",
                run_all_checks,
                state,
                engine,
                skip_narrative=True,
            )
        except Exception as exc:
            log.warning("Cross-reference import failed: {e}", e=str(exc))
        state.last_cross_reference_checks = now

    # ── Every 2 hours — signal registry refresh ──────────────────────

    if _hours_since(state.last_signal_registry) >= 2:
        _refresh_signal_registry(engine)
        state.last_signal_registry = now
        results["signal_registry"] = "refreshed"

    # ── Every 6 hours (alongside oracle) ─────────────────────────────

    if _hours_since(state.last_options_scoring) >= 6:
        try:
            from trading.options_tracker import score_expired_recommendations
            results["options_scoring"] = _run_intel_task(
                "options_scoring",
                score_expired_recommendations,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Options scoring import failed: {e}", e=str(exc))
        state.last_options_scoring = now

    if _hours_since(state.last_lever_pullers) >= 6:
        try:
            from intelligence.lever_pullers import identify_lever_pullers
            results["lever_pullers"] = _run_intel_task(
                "lever_pullers",
                identify_lever_pullers,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Lever pullers import failed: {e}", e=str(exc))
        state.last_lever_pullers = now

    if _hours_since(state.last_actor_wealth) >= 6:
        try:
            from intelligence.actor_network import track_wealth_migration
            results["actor_wealth_migration"] = _run_intel_task(
                "actor_wealth_migration",
                track_wealth_migration,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Actor network import failed: {e}", e=str(exc))

        # 13F mining block (power_mapper module deleted in Wave 1 — was zero-caller orphan).
        # Replaced by the canonical actor network + institutional_holdings queries above.
        try:
            pass
        except Exception as exc:
            log.warning("Power mapping failed: {e}", e=str(exc))

        state.last_actor_wealth = now

    # ── Daily at 2:00 AM (with catch-up) ─────────────────────────────
    # Fires if (a) we're in the 2:00-2:10 UTC window, OR (b) we're past 2 AM
    # UTC today and haven't run yet today (catches restarts, cycle timeouts,
    # long cycles, or any case where the 10-minute window was missed).
    # The _hours_since(last_daily_intel) >= 20 guard prevents double-runs.

    is_daily_window = (now.hour == 2 and now.minute < 10)
    is_catch_up = (
        now.hour >= 2
        and (state.last_daily_intel is None or state.last_daily_intel.date() < now.date())
    )
    daily_due = (is_daily_window or is_catch_up) and _hours_since(state.last_daily_intel) >= 20

    if daily_due:
        log.info(
            "Running daily intelligence batch (window={w} catch_up={c})",
            w=is_daily_window, c=(is_catch_up and not is_daily_window),
        )
        _run_daily_intel_block(engine, state, now, results)

    # NOTE (2026-09-19): the daily sector-health snapshot used to run here,
    # AFTER the daily-due block above. It now has its own dispatch and its
    # own timeout (SECTOR_HEALTH_TIMEOUT_SECONDS), run BEFORE this whole
    # step in run_cycle — see the sector/intelligence orchestration helper
    # near the run_cycle dispatch for the design and the traced reason:
    # production showed this step timing out at INTELLIGENCE_TASKS_TIMEOUT_
    # SECONDS on essentially every cycle, and the daily-due block above ran
    # with catch_up=True every time (state.last_daily_intel never advanced
    # far enough to reach code after it), so anything placed after this
    # block was never actually reached in production. Do not re-add a
    # sector-health call in this function.
    #
    # UPDATE (fable-daily-intel-resumable, 2026-09-20): the daily-due block
    # above is no longer monolithic or all-or-nothing (see
    # _run_daily_intel_block/DAILY_INTEL_TASKS) — it now makes bounded
    # per-cycle progress and can reach `state.last_daily_intel = now` over
    # several cycles instead of needing one uninterrupted ~360s+ run. This
    # does not change the sector-health placement/reasoning above; still
    # do not re-add a sector-health call in this function.

    # ── Daily at 6:30 UTC — forced-flow waterfall briefing ──────────
    # Implements docs/playbooks/opex_waterfall.md. Runs once per day,
    # pre-US-market-open, emits a LEVER/CONDITION/THESIS/INVALIDATION
    # posture and fires waterfall_watch alerts when >= 2 of the 5
    # forced-flow conditions are simultaneously tripped.

    is_forced_flow_window = (now.hour == 6 and now.minute < 40)
    forced_flow_due = (
        is_forced_flow_window
        and _hours_since(state.last_forced_flow_brief) >= 20
    )

    if forced_flow_due:
        log.info("Running forced-flow waterfall briefing (06:30 UTC)")
        try:
            from intelligence.forced_flow_monitor import run_forced_flow_cycle
            results["forced_flow_brief"] = _run_intel_task(
                "forced_flow_brief",
                run_forced_flow_cycle,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Forced flow monitor import failed: {e}", e=str(exc))
            results["forced_flow_brief"] = {"status": "failed", "error": str(exc)}
        state.last_forced_flow_brief = now

    # ── Daily at 4:00 AM — connection enrichment ────────────────────

    is_enrich_window = (now.hour == 4 and now.minute < 10)
    enrich_due = is_enrich_window and _hours_since(state.last_enrich_connections) >= 20

    if enrich_due:
        log.info("Running daily connection enrichment (4:00 AM)")
        try:
            from scripts.enrich_connections import main as enrich_main
            enrich_main()
            results["enrich_connections"] = {"status": "ok"}
            log.info("Connection enrichment complete")
        except Exception as exc:
            log.warning("Connection enrichment failed: {e}", e=str(exc))
            results["enrich_connections"] = {"status": "failed", "error": str(exc)}
        state.last_enrich_connections = now

        # News-to-signals: convert intelligence tables to signal_data
        try:
            from scripts.news_to_signals import main as news_signals_main
            n_signals = news_signals_main()
            results["news_to_signals"] = {"status": "ok", "signals": n_signals}
            log.info("News-to-signals complete: {n} signals", n=n_signals)
        except Exception as exc:
            log.warning("News-to-signals failed: {e}", e=str(exc))
            results["news_to_signals"] = {"status": "failed", "error": str(exc)}

    # ── Hourly catch-up — contagion backtest scoring ─────────────────
    #
    # Walks matured contagion_predictions rows and scores them against the
    # realised downstream price move in raw_series. The scorer is idempotent
    # and catches up older unscored rows, so hourly runs are safe.

    is_contagion_bt_window = now.minute < 10
    contagion_bt_due = (
        is_contagion_bt_window
        and _hours_since(state.last_contagion_backtest) >= 1
    )

    if contagion_bt_due:
        log.info("Running contagion backtest scoring (hourly catch-up)")
        try:
            from intelligence.contagion_backtest import score_all_windows
            bt_result = score_all_windows(engine)
            results["contagion_backtest"] = bt_result
            window_summary = " ".join(
                f"{days}d={rows}" for days, rows in sorted(bt_result.items())
            )
            log.info("contagion_backtest: {summary} rows", summary=window_summary)
        except Exception as exc:
            log.warning("contagion_backtest failed: {e}", e=str(exc))
            results["contagion_backtest"] = {"status": "failed", "error": str(exc)}
        state.last_contagion_backtest = now

        # Close the loop: decay/validate supply_chain_edges from the
        # freshly scored backtests. Runs immediately after contagion
        # backtest so the feedback sees the newest rows.
        try:
            from intelligence.postmortem import apply_contagion_feedback
            fb_result = apply_contagion_feedback(engine, since_hours=24)
            results["contagion_feedback"] = fb_result
            log.info(
                "contagion_feedback: decayed={d} confirmed={h} "
                "no_edge={ne} errors={e}",
                d=fb_result.get("decayed", 0),
                h=fb_result.get("confirmed", 0),
                ne=fb_result.get("skipped_no_edge", 0),
                e=fb_result.get("errors", 0),
            )
        except Exception as exc:
            log.warning("contagion_feedback failed: {e}", e=str(exc))
            results["contagion_feedback"] = {"status": "failed", "error": str(exc)}
        state.last_contagion_feedback = now

    # ── Weekly (Sunday 3:00 AM) ──────────────────────────────────────

    is_sunday = now.weekday() == 6
    is_weekly_window = is_sunday and (now.hour == 3 and now.minute < 10)
    weekly_due = is_weekly_window and _hours_since(state.last_weekly_intel) >= 160

    if weekly_due:
        log.info("Running weekly intelligence reports (Sunday 3:00 AM)")

        try:
            from intelligence.cross_reference import run_all_checks
            results["weekly_cross_reference"] = _run_intel_task(
                "weekly_cross_reference",
                run_all_checks,
                state,
                engine,
                skip_narrative=False,
            )
        except Exception as exc:
            log.warning("Weekly cross-reference import failed: {e}", e=str(exc))

        try:
            from intelligence.lever_pullers import generate_lever_report
            results["weekly_lever_report"] = _run_intel_task(
                "weekly_lever_report",
                generate_lever_report,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Lever report import failed: {e}", e=str(exc))

        try:
            from intelligence.trust_scorer import generate_trust_report
            results["weekly_trust_report"] = _run_intel_task(
                "weekly_trust_report",
                generate_trust_report,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Trust report import failed: {e}", e=str(exc))

        try:
            from intelligence.actor_network import generate_actor_report
            results["weekly_actor_report"] = _run_intel_task(
                "weekly_actor_report",
                generate_actor_report,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Actor report import failed: {e}", e=str(exc))

        state.last_weekly_intel = now

    return results


# ─── Obsidian vault sync ─────────────────────────────────────────────

def _run_obsidian_cycle(engine: Any) -> dict[str, Any]:
    """Run vault sync + agent loop, return combined result dict."""
    try:
        from ingestion.altdata.obsidian_sync import run_sync, regenerate_dashboard
        from intelligence.obsidian_agent import run_agent_cycle

        # 1. Sync vault <-> Postgres
        sync_result = run_sync(engine)
        log.info("Obsidian sync: {r}", r=sync_result)

        # 2. Run active agent
        agent_result = run_agent_cycle(engine)
        log.info("Obsidian agent: {r}", r=agent_result)

        # 3. Regenerate dashboard if anything changed
        total_changes = (
            sync_result.get("inserted", 0) + sync_result.get("updated", 0)
            + sync_result.get("outbound_written", 0)
            + agent_result.get("enriched", 0) + agent_result.get("acted", 0)
        )
        if total_changes > 0:
            regenerate_dashboard(engine)

        # 4. Refresh concept stub pages (idempotent — only writes if backlinks exist)
        stubs_created = 0
        try:
            from scripts.create_concept_stubs import CONCEPTS, WIKI_DIR, find_backlinks, create_stub
            from pathlib import Path as _Path

            WIKI_DIR.mkdir(parents=True, exist_ok=True)
            docs_dir = _Path(__file__).resolve().parent.parent / "docs"
            for target, (category, description, source_path) in CONCEPTS.items():
                target_file = WIKI_DIR / f"{target}.md"
                backlinks = find_backlinks(target, docs_dir)
                if backlinks:
                    content = create_stub(target, category, description, source_path, backlinks)
                    target_file.write_text(content, encoding="utf-8")
                    stubs_created += 1
            if stubs_created:
                log.info("Obsidian concept stubs: {n} pages refreshed", n=stubs_created)
        except Exception as exc:
            log.debug("Concept stubs skipped: {e}", e=str(exc))

        # 5. Add wikilinks to docs (only if concept stubs changed)
        #
        # IMPORTANT (2026-09-18 fix, see
        # docs/handoffs/2026-09-18/fable-w4d-hermes-docs-rewrite.md): this
        # used to write add_wikilinks()'s result straight back onto the
        # SAME tracked file it read via collect_markdown_files() — silently
        # rewriting README.md/CLAUDE.md/ATTENTION.md/docs/**/*.md in the
        # release tree on nearly every Hermes cycle. Source docs are now
        # read-only here; annotated copies go to
        # resolve_backlinks_output_dir() (env-configurable, defaults under
        # the Obsidian vault path this module already uses elsewhere), or
        # this step is skipped entirely (logged) when that directory is
        # unavailable. Never falls back to writing inside this checkout.
        backlinks_added = 0
        if stubs_created > 0:
            try:
                from scripts.obsidian_backlinks import (
                    collect_markdown_files, build_doc_registry,
                    add_wikilinks, CONCEPT_LINKS,
                    resolve_backlinks_output_dir, write_annotated_copy,
                )

                output_dir = resolve_backlinks_output_dir()
                if output_dir is None:
                    log.debug(
                        "Obsidian backlinks skipped this cycle: no output "
                        "directory configured/available (see "
                        "resolve_backlinks_output_dir)",
                    )
                else:
                    files = collect_markdown_files()
                    doc_registry = build_doc_registry(files)
                    all_entities = {**CONCEPT_LINKS}
                    skip_stems = {"README", "CLAUDE", "index", "plan", "config"}
                    for stem, target in doc_registry.items():
                        if stem not in skip_stems and len(stem) > 3:
                            all_entities[stem] = target

                    for f in files:
                        content = f.read_text(encoding="utf-8", errors="replace")
                        new_content, changes = add_wikilinks(content, f, all_entities)
                        if changes:
                            write_annotated_copy(output_dir, f, new_content)
                            backlinks_added += len(changes)

                    if backlinks_added:
                        log.info(
                            "Obsidian backlinks: {n} links added (written "
                            "to {d}; source docs untouched)",
                            n=backlinks_added, d=output_dir,
                        )
            except Exception as exc:
                log.debug("Backlinks skipped: {e}", e=str(exc))

        return {
            "sync": sync_result, "agent": agent_result,
            "dashboard_triggered": total_changes > 0,
            "concept_stubs": stubs_created,
            "backlinks_added": backlinks_added,
        }

    except Exception as e:
        log.error("Obsidian cycle failed: {e}", e=e)
        return {"error": str(e)}


# ─── Main loop ───────────────────────────────────────────────────────

def _is_transient_db_error(exc: BaseException) -> bool:
    """True when a failure is operational rather than a defect in our code.

    ``sqlalchemy.exc.OperationalError`` is the wrapper for everything the
    database refuses for reasons outside the statement itself — a
    statement_timeout cancellation, a dropped or exhausted connection, a
    server restart. Its sibling ``ProgrammingError`` (the 2026-03 regression's
    "column does not exist") is emphatically not transient, and must keep
    showing up as a real failure. Health surfaces use this to tell "the
    database was busy" from "this code is broken", which was exactly the
    distinction lost when the whole path sat inside ``except: log.debug``.

    Args:
        exc: The exception raised by the resolver.

    Returns:
        True if a retry on the next cycle could plausibly succeed unchanged.
    """
    try:
        from sqlalchemy.exc import OperationalError
    except Exception:  # pragma: no cover - SQLAlchemy is a hard dependency
        return False
    return isinstance(exc, OperationalError)


def _watermark_from(
    result: dict[str, Any], run_started: datetime,
) -> datetime:
    """How far a resolver summary proves the window was actually scanned.

    ``scanned_through`` is the exclusive upper bound of the range the run
    enumerated and resolved. It is the only honest watermark: advancing to
    ``run_started`` after a truncated scan would claim the slices the budget
    cut off, and those rows would never be resolved.

    A summary without the key — an older resolver, or a double in a test —
    falls back to ``run_started``, which is what the step used unconditionally
    before the budget existed.

    Timestamps survive a round trip through the snapshot payload as ISO
    strings, so a string is parsed rather than trusted to compare.
    """
    raw = result.get("scanned_through")
    if raw is None:
        return run_started
    if isinstance(raw, str):
        try:
            raw = datetime.fromisoformat(raw)
        except ValueError:
            log.warning(
                "Resolution reported an unparseable scanned_through ({v}) — "
                "holding the watermark at the run start instead",
                v=result.get("scanned_through"),
            )
            return run_started
    if not isinstance(raw, datetime):
        return run_started
    if raw.tzinfo is None:
        raw = raw.replace(tzinfo=timezone.utc)
    # Never claim more than the run could have seen: the scan's open end is
    # reported as the moment the resolver started, which is at or after
    # run_started, and a clock skew must not push the watermark into the
    # future of this cycle.
    return min(raw, run_started)


def _run_resolution_step(engine: Any, state: OperatorState) -> dict[str, Any]:
    """Run conflict resolution for this cycle and report the outcome.

    Resolution is the pipeline step that makes ingested data visible to every
    PIT consumer, so its failures must never be silent: any error is logged at
    warning and returned in ``cycle_result["resolution"]``, and the outcome is
    recorded on ``state.task_status`` (surfaced by the Hermes status payload).

    The watermark advances to how far the run actually SCANNED, never to how
    far it was asked to scan. A run that stops at RESOLUTION_SCAN_BUDGET_SECONDS
    returns normally, reporting ``scanned_through`` short of the window's end;
    the watermark moves there and the next cycle resumes from it. That is the
    difference between a slow database costing one slice of progress and
    costing all of it:

      * the watermark used to advance only on a fully clean run, so a cycle
        that ran out of budget re-scanned the same cold window next time,
        with the same budget, and ran out again. Nothing about the retry was
        more likely to succeed than the attempt before it;
      * a run abandoned at the timeout also leaves its worker thread alive
        with an open transaction (see _run_with_timeout), and a long-lived
        snapshot blocks CREATE/DROP INDEX CONCURRENTLY database-wide. The scan
        budget is what keeps the step inside its timeout, so that path is no
        longer the normal way a slow cycle ends.

    _run_with_timeout also calls ``blacklist_for_timeout("resolution")``, but
    unlike oracle_cycle, signal_classification, anomaly_narration and
    knowledge_mapping, nothing here consults ``cooldowns.can_retry()`` — so
    that entry has never actually skipped a resolution step. Production on
    2026-09-14 showed exactly that: ``resolution`` blacklisted until 10:15
    UTC while every cycle from 6008 to 6015 ran it and succeeded. The entry is
    left as-is deliberately; honouring it here would introduce a 24h stall
    that does not currently exist. It is a misleading health signal, not a
    live failure mode.

    A timeout, an exception, or a worker error still holds the watermark
    exactly where it was: those runs cannot say how much of their work
    landed. A run abandoned on timeout keeps going as an orphan thread; that
    is safe here because every insert is ``ON CONFLICT ... DO NOTHING``, so
    the worst case is duplicated effort.

    Args:
        engine: SQLAlchemy engine for the GRID database.
        state: OperatorState carrying the ``last_resolution`` watermark.

    Returns:
        The resolver summary, or a dict with ``timeout``/``error``.
    """
    state.current_step = "resolution"
    run_started = datetime.now(timezone.utc)
    since = None
    if getattr(state, "last_resolution", None):
        since = state.last_resolution - timedelta(
            hours=RESOLUTION_WATERMARK_OVERLAP_HOURS
        )
    step_t0 = time.time()
    # _run_with_timeout reports a raise and a timeout the same way, so the
    # exception is captured here to tell the two apart in cycle_result.
    failure: dict[str, Any] = {}

    def _resolve() -> dict[str, Any]:
        from normalization.resolver import Resolver

        try:
            return Resolver(db_engine=engine).resolve_pending(
                lookback_days=RESOLUTION_CYCLE_LOOKBACK_DAYS,
                workers=RESOLUTION_CYCLE_WORKERS,
                since=since,
                scan_budget_s=RESOLUTION_SCAN_BUDGET_SECONDS,
            )
        except Exception as exc:
            failure["error"] = str(exc)
            failure["error_class"] = type(exc).__name__
            failure["transient"] = _is_transient_db_error(exc)
            raise

    result, ok = _run_with_timeout(
        "resolution", _resolve, RESOLUTION_TIMEOUT_SECONDS, state,
    )

    if failure:
        # Warning for every failure (CLAUDE.md reserves log.error for
        # unhandled application bugs), but the class is in the message and
        # the transient flag rides along so the health surfaces can tell a
        # dropped connection from a programming error without parsing text.
        log.warning(
            "Resolution failed ({c}{t}): {e} — watermark held at {w}",
            c=failure["error_class"],
            t=", transient" if failure["transient"] else "",
            e=failure["error"], w=getattr(state, "last_resolution", None),
        )
        detail = f"{failure['error_class']}: {failure['error']}"
        state.record_task(
            "resolution", False, time.time() - step_t0, detail,
            transient=failure["transient"],
        )
        return {
            "error": failure["error"],
            "error_class": failure["error_class"],
            "transient": failure["transient"],
        }

    if not ok or result is None:
        # A step abandoned at the timeout is the operational case by
        # definition — it ran out of budget, it did not misbehave.
        log.warning(
            "Resolution step did not complete (timeout after {s}s) — "
            "watermark held at {w}",
            s=RESOLUTION_TIMEOUT_SECONDS, w=getattr(state, "last_resolution", None),
        )
        state.record_task(
            "resolution", False, time.time() - step_t0, "timeout", transient=True,
        )
        return {"timeout": True, "transient": True}

    if result.get("errors"):
        log.warning(
            "Resolution completed with {e} worker error(s) — resolved={r}, "
            "watermark held at {w}",
            e=result["errors"], r=result.get("resolved", 0),
            w=getattr(state, "last_resolution", None),
        )
        state.record_task(
            "resolution", False, time.time() - step_t0,
            f"{result['errors']} worker error(s)",
        )
        return result

    advanced = _watermark_from(result, run_started)
    if state.last_resolution is None or advanced > state.last_resolution:
        state.last_resolution = advanced
    state.record_task("resolution", True, time.time() - step_t0)
    if not result.get("scan_complete", True):
        # Operational, not a fault — the step did its job inside its budget
        # and made real progress. Worth a warning because a cycle that keeps
        # reporting this is behind and catching up a slice at a time.
        log.warning(
            "Resolution stopped at its scan budget — the window opened at "
            "{o} and the watermark advanced to {w}; the rest resumes next "
            "cycle",
            o=since, w=state.last_resolution,
        )
    log.info(
        "Resolution: {r} rows resolved, {c} conflicts, {s} series in {t}s "
        "(scanned through {w}, complete={k})",
        r=result.get("resolved", 0), c=result.get("conflicts_found", 0),
        s=result.get("series_scanned", 0), t=result.get("duration_s", 0),
        w=state.last_resolution, k=result.get("scan_complete", True),
    )
    return result


def run_cycle(state: OperatorState, dry_run: bool = False) -> dict[str, Any]:
    """Execute one operator cycle."""
    state.cycle_count += 1
    cycle_start = time.monotonic()
    cycle_result: dict[str, Any] = {
        "cycle": state.cycle_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dry_run": bool(dry_run),
    }

    log.info("═══ Hermes Operator — Cycle {n} ═══", n=state.cycle_count)

    # 0. Git pull — sync latest code/config
    if dry_run:
        cycle_result["git_pull"] = {"skipped": "dry_run"}
    else:
        try:
            pull_result = git_pull()
            cycle_result["git_pull"] = pull_result
        except Exception as exc:
            log.warning("Git pull failed: {e}", e=str(exc))

    # 1. Health check
    try:
        from db import get_engine
        engine = get_engine()
        # Pool telemetry is isolated in its own try/except: some callers
        # (dry-run tests, alternate `db` stand-ins) only provide
        # get_engine(), and instrumentation must never be able to take
        # down the actual health check that follows.
        try:
            from db import get_pool_stats, reset_pool_peak
            # Read the peak accumulated since the previous cycle's reset
            # first — a point sample of checked_out taken here would only
            # show this instant, missing whatever burst happened
            # mid-cycle. Reset after reading so the next cycle's peak
            # reflects only its own interval.
            pool_stats = get_pool_stats(engine)
            cycle_result["db_pool"] = pool_stats
            log.info(
                "DB pool (this process) — checked_out={co}/{cap} now, "
                "peak_since_last_cycle={pk} "
                "(pool_size={ps}, max_overflow={mo}, checked_in={ci})",
                co=pool_stats["checked_out"], cap=pool_stats["capacity"],
                pk=pool_stats["peak_checked_out"],
                ps=pool_stats["pool_size"], mo=pool_stats["max_overflow"],
                ci=pool_stats["checked_in"],
            )
            reset_pool_peak(pool_stats["checked_out"])
        except Exception as exc:
            log.warning("Pool stats logging failed: {e}", e=str(exc))
        health = check_system_health(engine)
        cycle_result["health"] = health
        hermes_ok = health["hermes"]["healthy"]
        db_ok = health["db"]["healthy"]
        log.info(
            "Health: DB={db}, Hermes={h}, stale={s}, failed_24h={f}",
            db=db_ok, h=hermes_ok,
            s=len(health["db"].get("stale_sources", [])),
            f=health["db"].get("failed_pulls_24h", 0),
        )
        # Audit #31 — fire alerts on threshold transitions. Best-effort,
        # cooldown-throttled (6h default), email via alerts.email.
        try:
            from alerts.health_alerter import check_and_alert
            if dry_run:
                cycle_result["alerts_fired"] = {"skipped": "dry_run"}
            else:
                fired = check_and_alert(health)
                if fired:
                    log.warning("Health alerts fired: {f}", f=", ".join(fired))
                    cycle_result["alerts_fired"] = fired
        except Exception as exc:
            log.debug("Health alerter skipped: {e}", e=str(exc))
    except Exception as exc:
        log.error("Health check failed: {e}", e=str(exc))
        cycle_result["health"] = {"error": str(exc)}
        state.consecutive_failures += 1
        return cycle_result

    if not db_ok:
        log.error("Database unhealthy — skipping all work this cycle")
        state.consecutive_failures += 1
        # Can't log to DB if DB is down, but log the state
        return cycle_result

    # Ensure issues table exists (first cycle only)
    if dry_run:
        cycle_result["issues_table"] = {"skipped": "dry_run"}
    else:
        try:
            _ensure_issues_table(engine)
        except Exception as exc:
            log.debug("Hermes: issues table ensure failed: {e}", e=str(exc))

    state.consecutive_failures = 0

    # 1b. Obsidian vault sync + agent cycle (every cycle, fast ~5 min cadence)
    if not dry_run:
        try:
            state.current_step = "obsidian_cycle"
            obsidian_result = _run_obsidian_cycle(engine)
            cycle_result["obsidian"] = obsidian_result
        except Exception as exc:
            log.warning("Obsidian cycle failed: {e}", e=str(exc))

    # 2. Fix broken pulls (with cooldown + smart retry)
    try:
        state.current_step = "diagnose_and_fix_pulls"
        pull_result, ok = _run_with_timeout(
            "diagnose_and_fix_pulls",
            lambda: diagnose_and_fix_pulls(
                engine,
                hermes_ok,
                state,
                dry_run=dry_run,
            ),
            DIAGNOSE_PULLS_TIMEOUT_SECONDS,
            state,
        )
        if ok and pull_result:
            cycle_result["pull_fixer"] = pull_result
            state.pulls_retried += pull_result.get("retried", 0)
            state.fixes_applied += pull_result.get("fixed", 0)
            state.errors_diagnosed += pull_result.get("diagnosed", 0)
        else:
            cycle_result["pull_fixer"] = {"timeout": True}
    except Exception as exc:
        log.error("Pull fixer failed: {e}", e=str(exc))
        cycle_result["pull_fixer"] = {"error": str(exc)}

    # 2b. Proactively re-pull stale sources (not just failed ones)
    stale_sources = health["db"].get("stale_sources", [])
    if stale_sources and not dry_run:
        stale_repulled = 0
        for stale in stale_sources[:15]:  # up to 15 per cycle
            src = stale["source"]
            state.current_step = f"stale_refresh:{src}"
            if state.cooldowns.can_retry(src):
                try:
                    _retry_source(src, engine, attempt=1, state=state)
                    state.cooldowns.record_attempt(src, success=True)
                    stale_repulled += 1
                    log.info("Proactively refreshed stale source: {s}", s=src)
                except ValueError:
                    pass  # no handler
                except Exception as exc:
                    state.cooldowns.record_attempt(src, success=False, error=str(exc))
                    log.warning("Stale refresh for {s} failed: {e}", s=src, e=str(exc))
        cycle_result["stale_refreshed"] = stale_repulled

    # 3. Smart ingestion — run only due/stale pullers (replaces full pipeline)
    if dry_run:
        cycle_result["ingestion"] = {"skipped": "dry_run"}
        log.info("[DRY RUN] Would run smart ingestion")
    else:
        try:
            state.current_step = "smart_ingestion"
            from ingestion.smart_scheduler import SmartScheduler
            if not hasattr(state, "_smart_sched") or state._smart_sched is None:
                state._smart_sched = SmartScheduler(engine)
            tick_result, ok = _run_with_timeout(
                "smart_ingestion",
                state._smart_sched.tick,
                SMART_INGESTION_TIMEOUT_SECONDS,
                state,
            )
            if ok and tick_result:
                cycle_result["ingestion"] = tick_result
                log.info(
                    "Smart ingestion: {ok}/{ran} succeeded, {due} still due",
                    ok=tick_result["succeeded"], ran=tick_result["ran"],
                    due=len(tick_result.get("still_due", [])),
                )
            else:
                cycle_result["ingestion"] = {"timeout": True}
        except Exception as exc:
            log.error("Smart ingestion failed: {e}", e=str(exc))
            cycle_result["ingestion"] = {"error": str(exc)}

    # 3b. Conflict resolution — raw_series → resolved_series via the
    # canonical resolver (priority_rank winner + per-family conflict
    # thresholds). See RESOLUTION_CYCLE_LOOKBACK_DAYS above for why this
    # is not a bare INSERT ... SELECT, and why a failure here is loud.
    if dry_run:
        cycle_result["resolution"] = {"skipped": "dry_run"}
        log.info("[DRY RUN] Would run conflict resolution")
    else:
        cycle_result["resolution"] = _run_resolution_step(engine, state)

    # 4. Fill data gaps — SKIP: SmartScheduler handles freshness now
    # The old gap filler re-pulled entire sources which was slow.
    # SmartScheduler's frequency tracking replaces this.
    cycle_result["data_gaps"] = {"skipped": "handled_by_smart_scheduler"}

    # 5. Self-diagnostics — only every 6th cycle (30 min), bounded by its
    # own timeout (see _run_diagnostics_step's docstring for the traced
    # 71-minute-stall defect this replaces).
    _run_diagnostics_step(engine, hermes_ok, health, state, dry_run, cycle_result)

    # 6. Autoresearch — only every 12th cycle (1 hour)
    #
    # Bounded + fenced (this task): previously called maybe_run_autoresearch
    # directly inside a plain try/except, with NO per-step timeout at all
    # (see docs/handoffs/2026-09-18/fable-w4-research-states.md's
    # "Activation condition" — this was the exact gap that made activating
    # autoresearch on a schedule unsafe). Now wrapped in _run_with_timeout
    # like resolution/oracle_cycle, AND every invocation gets a generation
    # id from _autoresearch_generation: if the timeout fires, the worker
    # thread is abandoned (not killed — see _run_with_timeout's docstring)
    # but the generation is bumped immediately below, so any write that
    # orphan later attempts is fenced by scripts/autoresearch.py's
    # generation checks (recorded there with a "fenced" reason).
    if state.cycle_count % 12 == 0 and health.get("overall_healthy") and hermes_ok:
        from config import settings as _ar_settings

        if not _ar_settings.AUTORESEARCH_ENABLED:
            # Same off-by-default gate as maybe_run_autoresearch, checked
            # here too so the disabled state shows up in this cycle's log
            # (and cycle_result) even though the cycle-modulo/health gate
            # above was otherwise satisfied — without this, "Running
            # autoresearch cycle" would never be reached anyway
            # (maybe_run_autoresearch's own check returns first), but the
            # operator's cycle log would stay silent about why.
            log.info("autoresearch disabled (AUTORESEARCH_ENABLED=false) — skipping")
            cycle_result["autoresearch"] = {"status": "skipped", "reason": "disabled"}
        else:
            try:
                state.current_step = "autoresearch"
                ar_run_id = str(uuid.uuid4())
                ar_generation = _autoresearch_generation.next()

                def _autoresearch_call():
                    return maybe_run_autoresearch(
                        state, dry_run=dry_run,
                        run_id=ar_run_id, generation=ar_generation,
                        is_current_generation=_autoresearch_generation.is_current,
                    )

                ar_result, ar_ok = _run_with_timeout(
                    "autoresearch", _autoresearch_call,
                    AUTORESEARCH_TIMEOUT_SECONDS, state,
                )
                if ar_ok:
                    if ar_result is not None:
                        cycle_result["autoresearch"] = ar_result
                else:
                    # Bump NOW, not on the next cycle-6 gate an hour from now —
                    # the abandoned worker thread is still running and could
                    # write at any point between now and then.
                    _autoresearch_generation.next()
                    cycle_result["autoresearch"] = {"status": "timeout", "run_id": ar_run_id}
                    try:
                        from scripts.autoresearch import _record_research_run
                        _record_research_run(
                            engine, ar_run_id, "timeout",
                            phase="operator_timeout",
                            error=f"exceeded {AUTORESEARCH_TIMEOUT_SECONDS}s",
                            error_category="timeout",
                            generation=ar_generation,
                        )
                    except Exception as exc:
                        log.warning("Failed to record autoresearch timeout: {e}", e=str(exc))
            except Exception as exc:
                log.warning("Autoresearch failed: {e}", e=str(exc))

    # 7. UX Audit — only every 72nd cycle (~6 hours)
    if state.cycle_count % 72 == 0 and health.get("overall_healthy") and hermes_ok:
        try:
            state.current_step = "ux_audit"
            from scripts.ux_auditor import maybe_run_ux_audit
            ux_result = maybe_run_ux_audit(state, engine, dry_run=dry_run)
            if ux_result is not None:
                cycle_result["ux_audit"] = ux_result
        except Exception as exc:
            log.warning("UX audit failed: {e}", e=str(exc))

    # 7b. Daily digest email (once per day)
    try:
        state.current_step = "daily_digest"
        from scripts.daily_digest import maybe_send_daily_digest
        digest_result = maybe_send_daily_digest(state, engine, dry_run=dry_run)
        if digest_result is not None:
            cycle_result["daily_digest"] = digest_result
    except Exception as exc:
        log.warning("Daily digest failed: {e}", e=str(exc))

    # 7c. 100x Digest (every 4 hours)
    try:
        now = datetime.now(timezone.utc)
        hours_since_100x = 999
        if state.last_100x_digest is not None:
            hours_since_100x = (now - state.last_100x_digest).total_seconds() / 3600
        if hours_since_100x >= 4:
            state.current_step = "hundredx_digest"
            log.info("Running 100x digest scan...")
            if not dry_run:
                from alerts.hundredx_digest import run_100x_digest
                digest_100x = run_100x_digest()
                cycle_result["100x_digest"] = digest_100x
                state.last_100x_digest = now
            else:
                log.info("[DRY RUN] Would run 100x digest")
    except Exception as exc:
        log.warning("100x digest failed: {e}", e=str(exc))

    # 7c-ii. Solana top-volume universe snapshot (every 4 hours)
    try:
        now = datetime.now(timezone.utc)
        hours_since_universe = 999
        last_universe = getattr(state, "last_solana_universe", None)
        if last_universe is not None:
            hours_since_universe = (now - last_universe).total_seconds() / 3600
        if hours_since_universe >= 4:
            log.info("Running Solana top-volume universe snapshot...")
            if not dry_run:
                from config import settings as _settings
                from ingestion.solana.top_volume import (
                    JupiterDexScreenerProvider,
                    TopVolumeIngestor,
                )
                from trading.solana import (
                    DeployerRegistry,
                    HeliusClient,
                    SafetyConfig,
                    SolanaSafetyChecker,
                    parse_mint_blocklist,
                )

                helius = HeliusClient(
                    api_key=getattr(_settings, "HELIUS_API_KEY", "") or None
                )
                deployer_registry = DeployerRegistry(engine=engine, provider=helius)
                safety_config = SafetyConfig(
                    blocked_mints=parse_mint_blocklist(
                        getattr(_settings, "SOLANA_MINT_BLOCKLIST", "") or ""
                    ),
                )
                safety = SolanaSafetyChecker(config=safety_config)

                provider = JupiterDexScreenerProvider(
                    jupiter_tokens_url=_settings.SOLANA_UNIVERSE_JUPITER_URL,
                    batch_size=_settings.SOLANA_UNIVERSE_DEX_BATCH,
                )
                try:
                    ingestor = TopVolumeIngestor(
                        engine=engine,
                        provider=provider,
                        safety=safety,
                        deploy_provider=helius,
                        deployer_registry=deployer_registry,
                        limit=_settings.SOLANA_UNIVERSE_LIMIT,
                        enrich_on_insert=_settings.SOLANA_UNIVERSE_ENRICH_ON_INSERT,
                    )
                    universe_summary = ingestor.ingest_once()
                    cycle_result["solana_universe"] = universe_summary.to_dict()
                    log.info(
                        "Solana universe: {n} tokens, {e} enriched, "
                        "{er} errors",
                        n=universe_summary.tokens_written,
                        e=universe_summary.new_mints_enriched,
                        er=universe_summary.enrichment_errors,
                    )
                finally:
                    provider.close()
                    helius.close()
                state.last_solana_universe = now
            else:
                log.info("[DRY RUN] Would run Solana universe snapshot")
    except Exception as exc:
        log.warning("Solana universe snapshot failed: {e}", e=str(exc))

    # 7c-iii. Supply Chain Pulse watchdog (every 6 hours)
    try:
        now = datetime.now(timezone.utc)
        hours_since_scp = 999
        last_scp = getattr(state, "last_supply_chain_pulse", None)
        if last_scp is not None:
            hours_since_scp = (now - last_scp).total_seconds() / 3600
        if hours_since_scp >= 6:
            state.current_step = "supply_chain_pulse"
            log.info("Running Supply Chain Pulse watchdog...")
            if not dry_run:
                from alerts.supply_chain_alerts import run_all as run_supply_chain_alerts
                scp_result = run_supply_chain_alerts(
                    engine, since_hours=24, send_email=True
                )
                cycle_result["supply_chain_pulse"] = {
                    "total": scp_result.get("total", 0),
                    "sent": scp_result.get("sent", False),
                    "snapshots": scp_result.get("snapshots_written", 0),
                    "counts": {
                        k: len(v)
                        for k, v in scp_result.get("findings", {}).items()
                    },
                }
                setattr(state, "last_supply_chain_pulse", now)
            else:
                log.info("[DRY RUN] Would run Supply Chain Pulse")
    except Exception as exc:
        log.warning("Supply Chain Pulse failed: {e}", e=str(exc))

    # 7c-iv. News contagion listener (every 15 minutes)
    #
    # Scans news_articles for shock-worthy events (bankruptcies, halts,
    # recalls, sanctions, commodity spikes) and auto-fires chain_contagion
    # simulations, persisting results with source='news_listener' and a
    # trigger_news_id back-pointer to the article that fired the shock.
    try:
        now = datetime.now(timezone.utc)
        last_ncl = getattr(state, "last_news_contagion", None)
        minutes_since_ncl = 9999.0
        if last_ncl is not None:
            minutes_since_ncl = (now - last_ncl).total_seconds() / 60
        if minutes_since_ncl >= 15:
            state.current_step = "news_contagion_listener"
            log.info("Running news_contagion_listener...")
            if not dry_run:
                from intelligence.news_contagion_listener import run_once as ncl_run
                ncl_result = ncl_run(
                    engine, since_hours=1, dry_run=False, limit=500
                )
                cycle_result["news_contagion_listener"] = {
                    "scanned": ncl_result.get("scanned_articles", 0),
                    "resolved": ncl_result.get("resolved", 0),
                    "fired": ncl_result.get("fired", 0),
                    "skipped_duplicate": ncl_result.get("skipped_duplicate", 0),
                    "errors": ncl_result.get("errors", 0),
                }
                setattr(state, "last_news_contagion", now)
                log.info(
                    "news_contagion: scanned={s} fired={f} dup={d}",
                    s=ncl_result.get("scanned_articles", 0),
                    f=ncl_result.get("fired", 0),
                    d=ncl_result.get("skipped_duplicate", 0),
                )
            else:
                log.info("[DRY RUN] Would run news_contagion_listener")
    except Exception as exc:
        log.warning("news_contagion_listener failed: {e}", e=str(exc))

    # 7c2. AstroGrid celestial cycle — hourly, ahead of the oracle.
    #
    # AstroGrid's learning half (scoring/backtest/review) has its own
    # astrogrid-learning.timer and is healthy. Its celestial half had no
    # scheduler at all: sky snapshots and interpretations were written only
    # when a browser hit the API, so astrogrid.sky_snapshot stopped on
    # 2026-04-28, persona_run on 2026-05-01, and seer_run/engine_run never
    # held a row. This step is the producer those tables were missing.
    #
    # Hourly rather than every cycle: the sky state a snapshot captures moves
    # on the order of hours, and the interpretation costs a local LLM call.
    #
    # Placed BEFORE the oracle, not after it. It first shipped as 7i, last in
    # the cycle, and in two hours on 2026-09-14 it never ran once: four
    # grid-hermes restarts (16:02, 16:10, 17:05, 17:48) each built a fresh
    # OperatorState, which reset last_oracle_cycle to None and reopened the
    # 6-hour oracle gate, so every cycle started a full oracle pass and none
    # reached the steps behind it -- "Alpha signals published" (7e) last
    # appeared at 15:40, before any of the restarts. A sub-second sky build
    # plus one bounded LLM call has no dependency on the oracle and no reason
    # to queue behind it. The starvation of 7e-7h is a separate, shared
    # problem: the oracle gate lives only in memory.
    try:
        now_utc = datetime.now(timezone.utc)
        last_celestial = getattr(state, "_last_astrogrid_celestial_hour", None)
        current_hour = now_utc.replace(minute=0, second=0, microsecond=0)
        # Yield to a cycle that has already spent its budget: a 240s step
        # started at 4400s would take the whole cycle down with it rather
        # than just itself. The hour marker is left untouched, so the next
        # cycle picks the hour up instead of losing it.
        #
        # Ahead of the oracle this is insurance rather than the common case:
        # only diagnose (240) + smart ingestion (300) + resolution (420) are
        # budgeted before it, well inside the 4500s cap. The unbudgeted steps
        # that also run first -- pipeline, data gatherer, autoresearch,
        # self-diagnostics, the 7a-7c digests -- have no cap of their own, so
        # a slow cycle can still arrive here late. Kept for that case.
        elapsed = time.monotonic() - cycle_start
        budget_left = CYCLE_TIMEOUT_SECONDS - elapsed
        if budget_left < ASTROGRID_CELESTIAL_TIMEOUT_SECONDS:
            log.info(
                "AstroGrid celestial cycle deferred — {b:.0f}s of cycle budget left,"
                " needs {n}s",
                b=budget_left,
                n=ASTROGRID_CELESTIAL_TIMEOUT_SECONDS,
            )
            cycle_result["astrogrid_celestial"] = {
                "deferred": "insufficient_cycle_budget",
                "budget_left_s": round(budget_left, 1),
            }
        elif last_celestial != current_hour:
            state.current_step = "astrogrid_celestial"
            from oracle.astrogrid_cycle import run_celestial_cycle

            celestial_result, ok = _run_with_timeout(
                "astrogrid_celestial",
                lambda: run_celestial_cycle(
                    engine, persist=not dry_run, interpret=not dry_run
                ),
                ASTROGRID_CELESTIAL_TIMEOUT_SECONDS,
                state,
            )
            if ok and celestial_result:
                cycle_result["astrogrid_celestial"] = celestial_result
                # Advance only on a completed run. A timeout leaves the marker
                # alone so the next cycle retries instead of skipping the hour.
                state._last_astrogrid_celestial_hour = current_hour
            elif not ok:
                cycle_result["astrogrid_celestial"] = {"timeout": True}
    except Exception as exc:
        log.warning("AstroGrid celestial cycle failed: {e}", e=str(exc))

    # 7d. Oracle prediction cycle (every 6 hours)
    try:
        now = datetime.now(timezone.utc)
        hours_since_oracle = 999
        if state.last_oracle_cycle is not None:
            hours_since_oracle = (now - state.last_oracle_cycle).total_seconds() / 3600
        if hours_since_oracle >= 6 and state.cooldowns.can_retry("oracle_cycle"):
            state.current_step = "oracle_cycle"
            log.info("Running Oracle prediction cycle...")
            # Record cycle start eagerly: even if the inner timeout fires
            # the orphan thread keeps running and writes predictions to DB.
            # We must NOT refire oracle for another 6h regardless. (2026-05-09)
            state.last_oracle_cycle = now
            if not dry_run:
                from oracle.engine import OracleEngine
                from oracle.report import send_oracle_report

                def _oracle_call():
                    oracle = OracleEngine(db_engine=engine)
                    return oracle.run_cycle()

                oracle_result, ok = _run_with_timeout(
                    "oracle_cycle", _oracle_call,
                    ORACLE_CYCLE_TIMEOUT_SECONDS, state,
                )
                if ok and oracle_result:
                    cycle_result["oracle"] = {
                        "predictions": oracle_result["new_predictions"],
                        "scoring": oracle_result["scoring"],
                        "leaderboard": oracle_result.get("leaderboard", [])[:3],
                    }
                    if oracle_result["new_predictions"] > 0:
                        send_oracle_report(oracle_result)
                    state.last_oracle_cycle = now
                    state.cooldowns.record_attempt("oracle_cycle", success=True)
            else:
                log.info("[DRY RUN] Would run Oracle cycle")
        elif hours_since_oracle >= 6:
            log.info(
                "Skipping oracle_cycle — blacklisted (timed out previously, "
                "blacklist clears in {h}h)",
                h=TIMEOUT_BLACKLIST_HOURS,
            )
    except Exception as exc:
        log.warning("Oracle cycle failed: {e}", e=str(exc))
        state.cooldowns.record_attempt("oracle_cycle", success=False, error=str(exc))

    # 7d-ii. TimesFM forecast cycle (every 6 hours, alongside oracle)
    try:
        now = datetime.now(timezone.utc)
        hours_since_timesfm = 999
        last_timesfm = getattr(state, "last_timesfm_cycle", None)
        if last_timesfm is not None:
            hours_since_timesfm = (now - last_timesfm).total_seconds() / 3600
        if hours_since_timesfm >= 6:
            state.current_step = "timesfm_cycle"
            log.info("Running TimesFM forecast cycle...")
            if not dry_run:
                from oracle.forecaster_adapter import run_timesfm_forecast_cycle
                # Wrap with the cycle's stage-timeout pattern. Without
                # this, run_timesfm_forecast_cycle could hang the entire
                # Hermes cycle past CYCLE_TIMEOUT_SECONDS — which is
                # exactly what cycle 292 logged (`TIMED OUT after 600s
                # stuck on: timesfm_cycle`). The function takes (engine,)
                # — _run_with_timeout calls fn() so wrap as a thunk.
                tfm_result, ok = _run_with_timeout(
                    "timesfm_cycle",
                    lambda: run_timesfm_forecast_cycle(engine),
                    TIMESFM_TIMEOUT_SECONDS,
                    state,
                )
                if ok and tfm_result:
                    cycle_result["timesfm"] = tfm_result
                    state.last_timesfm_cycle = now
                    log.info(
                        "TimesFM: {n} forecasts generated",
                        n=tfm_result.get("forecasts", 0),
                    )
                else:
                    cycle_result["timesfm"] = {"timeout": True}
            else:
                log.info("[DRY RUN] Would run TimesFM forecast cycle")
    except Exception as exc:
        log.warning("TimesFM forecast cycle failed: {e}", e=str(exc))

    # 7d-iii. AutoBNN changepoint detection (every 12 hours)
    try:
        now = datetime.now(timezone.utc)
        hours_since_changepoint = 999
        last_cp = getattr(state, "last_changepoint_cycle", None)
        if last_cp is not None:
            hours_since_changepoint = (now - last_cp).total_seconds() / 3600
        if hours_since_changepoint >= 12:
            state.current_step = "changepoint_detection"
            log.info("Running AutoBNN changepoint detection...")
            if not dry_run:
                from discovery.changepoint_detector import run_changepoint_cycle
                cp_result = run_changepoint_cycle(engine)
                cycle_result["changepoint_detection"] = cp_result
                state.last_changepoint_cycle = now
                log.info(
                    "Changepoint: {n} changes in {f} features",
                    n=cp_result.get("changepoints_found", 0),
                    f=cp_result.get("features_scanned", 0),
                )
            else:
                log.info("[DRY RUN] Would run changepoint detection")
    except Exception as exc:
        log.warning("Changepoint detection failed: {e}", e=str(exc))

    # 7d-iv. Gemma micro signal classification (every cycle)
    try:
        if not state.cooldowns.can_retry("signal_classification"):
            log.debug(
                "Skipping signal_classification — blacklisted (timed out previously, "
                "blacklist clears in {h}h)",
                h=TIMEOUT_BLACKLIST_HOURS,
            )
        else:
            state.current_step = "signal_classification"
            if not dry_run:
                from ingestion.signal_classifier import classify_recent_signals

                def _classify_call():
                    return classify_recent_signals(
                        engine,
                        limit=SIGNAL_CLASSIFICATION_LIMIT,
                    )

                cls_result, ok = _run_with_timeout(
                    "signal_classification", _classify_call,
                    SIGNAL_CLASSIFICATION_TIMEOUT_SECONDS, state,
                )
                if ok and cls_result and cls_result.get("classified", 0) > 0:
                    cycle_result["signal_classification"] = cls_result
                    log.info(
                        "Signal classification: {n} signals classified",
                        n=cls_result["classified"],
                    )
                if ok:
                    state.cooldowns.record_attempt("signal_classification", success=True)
    except Exception as exc:
        log.debug("Signal classification skipped: {e}", e=str(exc))
        state.cooldowns.record_attempt("signal_classification", success=False, error=str(exc))

    # 7d-v. Gemma micro anomaly narration (every cycle, after classification)
    # Reads recent high-z signals from signal_registry, asks the
    # anomaly_narrator (port 8083) for a one-line plain-English summary,
    # persists into anomaly_narratives. Idempotent: UNIQUE constraint on
    # (source_module, ticker, signal_ts) means re-runs are no-ops.
    try:
        if not state.cooldowns.can_retry("anomaly_narration"):
            log.debug(
                "Skipping anomaly_narration — blacklisted (timed out previously, "
                "blacklist clears in {h}h)",
                h=TIMEOUT_BLACKLIST_HOURS,
            )
        else:
            state.current_step = "anomaly_narration"
            if not dry_run:
                from ingestion.signal_classifier import narrate_anomalies

                def _narrate_call():
                    return narrate_anomalies(engine, z_threshold=3.0, limit=20)

                narratives, ok = _run_with_timeout(
                    "anomaly_narration", _narrate_call,
                    ANOMALY_NARRATION_TIMEOUT_SECONDS, state,
                )
                if ok and narratives:
                    inserted = 0
                    for n in narratives:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO anomaly_narratives
                                        (ticker, source_module, z_score,
                                         narrative, signal_ts)
                                    VALUES (:ticker, :src, :z, :narr, :ts)
                                    ON CONFLICT (source_module, ticker, signal_ts)
                                    DO NOTHING
                                """).bindparams(
                                    ticker=n.get("ticker"),
                                    src=n["source"],
                                    z=n["z_score"],
                                    narr=n["narrative"],
                                    ts=n.get("timestamp"),
                                ))
                                inserted += 1
                        except Exception as exc:
                            log.debug(
                                "Failed to persist narrative: {e}",
                                e=str(exc),
                            )
                    if inserted:
                        cycle_result["anomaly_narration"] = {
                            "narratives_generated": len(narratives),
                            "persisted": inserted,
                        }
                        log.info(
                            "Anomaly narration: {n} narratives persisted",
                            n=inserted,
                        )
                if ok:
                    state.cooldowns.record_attempt("anomaly_narration", success=True)
    except Exception as exc:
        log.debug("Anomaly narration skipped: {e}", e=str(exc))
        state.cooldowns.record_attempt("anomaly_narration", success=False, error=str(exc))

    # 7d-vi. Gemma micro knowledge mapping (every cycle, after classification)
    # Takes recently classified high-urgency signals and asks the
    # knowledge_mapper (port 8085) for a wiki-style entry with [[backlinks]].
    # Persists into signal_knowledge_entries. The helper itself flips
    # signal_registry.knowledge_mapped=TRUE so signals are processed once.
    try:
        if not state.cooldowns.can_retry("knowledge_mapping"):
            log.debug(
                "Skipping knowledge_mapping — blacklisted (timed out previously, "
                "blacklist clears in {h}h)",
                h=TIMEOUT_BLACKLIST_HOURS,
            )
        else:
            state.current_step = "knowledge_mapping"
            if not dry_run:
                from ingestion.signal_classifier import map_signal_knowledge

                def _map_call():
                    return map_signal_knowledge(
                        engine, urgency_filter="high", limit=10,
                    )

                entries, ok = _run_with_timeout(
                    "knowledge_mapping", _map_call,
                    KNOWLEDGE_MAP_TIMEOUT_SECONDS, state,
                )
                if ok and entries:
                    inserted = 0
                    for e in entries:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO signal_knowledge_entries
                                        (signal_id, ticker, category,
                                         knowledge_entry, signal_ts)
                                    VALUES (:sid, :ticker, :cat, :entry, :ts)
                                    ON CONFLICT (signal_id) DO NOTHING
                                """).bindparams(
                                    sid=e["signal_id"],
                                    ticker=e.get("ticker"),
                                    cat=e.get("category"),
                                    entry=e["knowledge_entry"],
                                    ts=e.get("timestamp"),
                                ))
                                inserted += 1
                        except Exception as exc:
                            log.debug(
                                "Failed to persist knowledge entry: {e}",
                                e=str(exc),
                            )
                    if inserted:
                        cycle_result["knowledge_mapping"] = {
                            "entries_generated": len(entries),
                            "persisted": inserted,
                        }
                        log.info(
                            "Knowledge mapping: {n} entries persisted",
                            n=inserted,
                        )
                if ok:
                    state.cooldowns.record_attempt("knowledge_mapping", success=True)
    except Exception as exc:
        log.debug("Knowledge mapping skipped: {e}", e=str(exc))
        state.cooldowns.record_attempt("knowledge_mapping", success=False, error=str(exc))

    # 7e. Alpha research heartbeat + signal publishing (every cycle)
    try:
        state.current_step = "alpha_heartbeat"
        from alpha_research.heartbeat import run_heartbeat, format_alerts

        hb_alerts = run_heartbeat(engine)
        if hb_alerts:
            log.info(format_alerts(hb_alerts))
        cycle_result["alpha_heartbeat"] = {
            "alerts": len(hb_alerts),
            "critical": sum(1 for a in hb_alerts if a.level == "CRITICAL"),
        }

        if not dry_run:
            from alpha_research.adapters.signal_adapter import publish_all_alpha_signals
            pub_result = publish_all_alpha_signals(engine)
            cycle_result["alpha_signals_published"] = pub_result
            log.info("Alpha signals published: {r}", r=pub_result)
        else:
            log.info("[DRY RUN] Would publish alpha signals")
    except Exception as exc:
        log.warning("Alpha research heartbeat failed: {e}", e=str(exc))

    # 7f. Sector health snapshot, then intelligence modules — trust scoring,
    #     cross-reference, lever pullers, actor network, source audit,
    #     postmortem, options tracking, backtests. Split into two
    #     independent steps (own dispatch, own _run_with_timeout budget)
    #     on 2026-09-19: intelligence_tasks (900s) was starving the
    #     sector-health snapshot, which used to run at the very end of it.
    #     See _run_sector_and_intelligence_steps's docstring for the traced
    #     production evidence and the blacklist-trace rationale for why the
    #     new sector-health step deliberately does not consult the cooldown
    #     retry-eligibility check that a few other steps use.
    _run_sector_and_intelligence_steps(engine, state, dry_run, cycle_result)

    # 7g. Rotation paper trading — daily after 17:00 UTC (market close)
    try:
        now_utc = datetime.now(timezone.utc)
        # Run once per day between 17:00-17:30 UTC (after US market close)
        if 17 <= now_utc.hour < 18 and now_utc.minute < 30:
            last_rotation = getattr(state, "_last_rotation_date", None)
            if last_rotation != now_utc.date():
                log.info("Running rotation paper trader...")
                if not dry_run:
                    from scripts.rotation_paper_trader import run_paper_trading
                    rotation_result = run_paper_trading(engine)
                    cycle_result["rotation_paper_trading"] = rotation_result
                    state._last_rotation_date = now_utc.date()
                else:
                    log.info("[DRY RUN] Would run rotation paper trader")
    except Exception as exc:
        log.warning("Rotation paper trading failed: {e}", e=str(exc))

    # 7h. Tiingo bulk data pull — overnight (02:00-06:00 UTC) to maximize 40GB/mo
    try:
        now_utc = datetime.now(timezone.utc)
        if 2 <= now_utc.hour < 6:
            last_tiingo_bulk = getattr(state, "_last_tiingo_bulk_date", None)
            if last_tiingo_bulk != now_utc.date():
                log.info("Running Tiingo bulk data pull (overnight window)...")
                if not dry_run:
                    try:
                        from ingestion.tiingo_pull import TiingoPuller
                        tp = TiingoPuller(engine)
                        # Pull all tracked tickers (daily update)
                        tiingo_result = tp.pull_all(start_date=str(now_utc.date() - timedelta(days=5)))
                        cycle_result["tiingo_daily"] = {
                            "succeeded": sum(1 for r in tiingo_result if r["status"] == "SUCCESS"),
                            "total": len(tiingo_result),
                        }
                    except Exception as exc:
                        log.warning("Tiingo price pull failed: {e}", e=str(exc))

                    try:
                        from ingestion.tiingo_news_pull import TiingoNewsPuller
                        tnp = TiingoNewsPuller(engine)
                        news_result = tnp.pull_all(start_date=str(now_utc.date() - timedelta(days=3)))
                        cycle_result["tiingo_news"] = {
                            "articles": sum(r.get("articles", 0) for r in news_result),
                            "tickers": len(news_result),
                        }
                    except Exception as exc:
                        log.warning("Tiingo news pull failed: {e}", e=str(exc))

                    state._last_tiingo_bulk_date = now_utc.date()
                else:
                    log.info("[DRY RUN] Would run Tiingo bulk pull")
    except Exception as exc:
        log.warning("Tiingo bulk pull failed: {e}", e=str(exc))

    # 8. Git push — commit and push any new outputs
    if dry_run:
        cycle_result["git_push"] = {"skipped": "dry_run"}
    else:
        try:
            state.current_step = "git_push"
            push_result = git_push_outputs()
            cycle_result["git_push"] = push_result
        except Exception as exc:
            log.warning("Git push failed: {e}", e=str(exc))

    # 8b. LLM Task Queue status — report throughput and queue depth
    try:
        from orchestration.llm_taskqueue import get_task_queue
        tq = get_task_queue(engine)
        cycle_result["llm_taskqueue"] = tq.get_status()
    except Exception as exc:
        log.debug("Hermes: LLM task queue status failed: {e}", e=str(exc))

    # 9. Save cycle snapshot
    state.current_step = "save_cycle_snapshot"
    elapsed = time.monotonic() - cycle_start
    cycle_result["elapsed_seconds"] = round(elapsed, 1)
    cycle_result["operator_state"] = state.to_dict()
    if dry_run:
        cycle_result["snapshot"] = {"skipped": "dry_run"}
        cycle_result["obsidian_report"] = {"skipped": "dry_run"}
    else:
        save_cycle_snapshot(engine, cycle_result)
        cycle_result["snapshot"] = {"status": "attempted"}

        # Fan a heartbeat / event report into the fleet-wide Obsidian agent-hub.
        # Idempotent: skips on quiet cycles unless the hourly heartbeat is due.
        _emit_obsidian_cycle_report(state, cycle_result)
        cycle_result["obsidian_report"] = {"status": "attempted"}

    log.info(
        "═══ Cycle {n} complete — {t:.1f}s ═══",
        n=state.cycle_count, t=elapsed,
    )
    state.current_step = "idle"
    return cycle_result


# ─── Obsidian fan-out ────────────────────────────────────────────────

# How often Hermes emits a "everything's fine" heartbeat report to the
# agent hub. Default 12 cycles ≈ 60 minutes at the 5-min cycle interval.
# Override with HERMES_OBSIDIAN_REPORT_EVERY_N_CYCLES env var; set 0 to
# disable cadence-based heartbeats entirely (event-only reports remain on).
HERMES_OBSIDIAN_REPORT_EVERY_N_CYCLES = int(
    os.environ.get("HERMES_OBSIDIAN_REPORT_EVERY_N_CYCLES", "12") or "0"
)
HERMES_OBSIDIAN_REPORT_CMD = os.environ.get(
    "HERMES_OBSIDIAN_REPORT_CMD", "/usr/local/bin/agent-report"
)


def _cycle_did_something(cycle_result: dict[str, Any]) -> tuple[bool, list[str]]:
    """Returns (did_something, event_labels). Used to decide whether to
    file an event-driven Obsidian report on top of the cadence heartbeat."""
    events: list[str] = []
    health = cycle_result.get("health") or {}
    if health.get("overall_healthy") is False:
        events.append("health-degraded")
    pull_fixer = cycle_result.get("pull_fixer") or {}
    retried = int(pull_fixer.get("retried") or 0)
    if retried > 0:
        events.append(f"pulls-retried={retried}")
    if cycle_result.get("pipeline"):
        events.append("pipeline-ran")
    weekly = cycle_result.get("weekly_intelligence_reports")
    if weekly:
        events.append(f"weekly-reports={len(weekly)}")
    autoresearch = cycle_result.get("autoresearch") or {}
    hypotheses = autoresearch.get("hypotheses_generated") or 0
    if hypotheses:
        events.append(f"hypotheses={hypotheses}")
    return (bool(events), events)


def _build_obsidian_cycle_body(
    cycle_result: dict[str, Any], events: list[str]
) -> str:
    """Build a short Markdown body for the agent-hub report. Cycle context
    + health snapshot + any noteworthy events. Kept under ~30 lines so the
    Obsidian feed stays scannable."""
    health = cycle_result.get("health") or {}
    db = health.get("db") or {}
    hermes = health.get("hermes") or {}
    cycle_n = cycle_result.get("cycle", "?")
    elapsed = cycle_result.get("elapsed_seconds", "?")
    raw_count = db.get("raw_series_count")
    latest_pull = db.get("latest_pull")
    failed_24h = db.get("failed_pulls_24h")
    failed_1h = db.get("failed_pulls_1h")
    overall = health.get("overall_healthy")
    overall_str = "healthy" if overall else "degraded"

    lines = [
        f"# Hermes cycle {cycle_n} — {overall_str}",
        "",
        f"- elapsed: {elapsed}s",
        f"- db.healthy: {db.get('healthy')!r}",
        f"- hermes.healthy: {hermes.get('healthy')!r}",
    ]
    if raw_count is not None:
        lines.append(f"- raw_series rows: {raw_count:,}")
    if latest_pull:
        lines.append(f"- latest pull: {latest_pull}")
    if failed_1h is not None:
        lines.append(f"- failed pulls (1h / 24h): {failed_1h} / {failed_24h}")
    if events:
        lines.append("")
        lines.append("## Events this cycle")
        for ev in events:
            lines.append(f"- {ev}")
    return "\n".join(lines) + "\n"


def _emit_obsidian_cycle_report(state: Any, cycle_result: dict[str, Any]) -> None:
    """Fan a hermes-cycle report into the agent hub. Fail-soft: any error
    is logged and swallowed so a broken hub never breaks the operator loop."""
    try:
        cycle_n = int(cycle_result.get("cycle") or state.cycle_count)
        did_something, events = _cycle_did_something(cycle_result)

        cadence = HERMES_OBSIDIAN_REPORT_EVERY_N_CYCLES
        cadence_due = (cadence > 0 and cycle_n % cadence == 0)

        if not did_something and not cadence_due:
            return  # quiet cycle, skip to keep the feed scannable

        if not os.path.exists(HERMES_OBSIDIAN_REPORT_CMD):
            log.debug(
                "obsidian-report wrapper missing at {p}; skipping fan-out",
                p=HERMES_OBSIDIAN_REPORT_CMD,
            )
            return

        body = _build_obsidian_cycle_body(cycle_result, events)
        # Slug: short, sortable, low-collision. Hub dedups on
        # (date, agent, host, slug).
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%MZ")
        slug = f"hermes-cycle-{cycle_n}-{timestamp}"

        # Write body to /tmp and invoke agent-report. Cap stdout / stderr
        # at 2KB each so any noisy wrapper output doesn't fill the log.
        body_path = f"/tmp/hermes-{slug}.md"
        with open(body_path, "w", encoding="utf-8") as fh:
            fh.write(body)

        rc = subprocess.run(
            [HERMES_OBSIDIAN_REPORT_CMD, "hermes-operator", slug, body_path],
            capture_output=True, text=True, timeout=20,
        )
        if rc.returncode == 0:
            log.info(
                "obsidian-report: filed hermes-cycle-{n} ({events})",
                n=cycle_n, events=",".join(events) or "heartbeat",
            )
        else:
            log.warning(
                "obsidian-report: agent-report rc={rc} stderr={se}",
                rc=rc.returncode, se=(rc.stderr or "")[:512],
            )
    except Exception as exc:
        log.warning("obsidian-report fan-out failed: {e}", e=str(exc))


def _log_outstanding_checkouts_once() -> list[dict[str, Any]] | None:
    """Poll and log this process's currently-open DB checkouts.

    This must run inside the same process as the engine it inspects --
    db.py's checkout tracking is a process-local module dict, so a
    separate Python invocation (e.g. a one-off diagnostic script) gets its
    own empty tracking state and can never see what this Hermes process
    has open. Wrapped in its own try/except, matching the isolation used
    for the per-cycle pool_stats log, so a telemetry bug can never affect
    real cycle work. Returns the outstanding-checkout list for tests;
    callers running the poll loop don't need the return value.
    """
    try:
        from db import get_outstanding_checkouts
        outstanding = get_outstanding_checkouts()
        if outstanding:
            log.info(
                "DB pool (this process) — {n} outstanding checkout(s): {rows}",
                n=len(outstanding), rows=outstanding,
            )
        return outstanding
    except Exception as exc:
        log.warning("Outstanding-checkout telemetry failed: {e}", e=str(exc))
        return None


def _outstanding_checkout_telemetry_loop(
    interval_seconds: float, *, sleep_fn: Callable[[float], None] = time.sleep,
) -> None:
    """Background loop: poll outstanding DB checkouts on a fixed interval.

    Runs independently of cycle completion or cycle length -- a cycle can
    run for up to CYCLE_TIMEOUT_SECONDS (75 min) or hang past it, during
    which the per-cycle pool_stats log in run_cycle never fires. This loop
    is the only source of outstanding-checkout visibility during that
    window. Intended to run as a daemon thread for the life of the process.

    ``sleep_fn`` is injectable (defaults to time.sleep) so tests can drive
    a bounded number of iterations without monkeypatching the global time
    module or actually sleeping.
    """
    while True:
        sleep_fn(interval_seconds)
        _log_outstanding_checkouts_once()


def main(args: list[str] | None = None) -> None:
    """Entry point for the Hermes operator daemon."""
    parser = argparse.ArgumentParser(description="GRID Hermes Operator — 24/7 self-healing daemon")
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    parser.add_argument("--dry-run", action="store_true", help="Diagnose only, don't fix anything")
    parser.add_argument(
        "--interval", type=int, default=CYCLE_INTERVAL_SECONDS,
        help=f"Seconds between cycles (default: {CYCLE_INTERVAL_SECONDS})",
    )
    opts = parser.parse_args(args)

    log.info("╔══════════════════════════════════════════╗")
    log.info("║   GRID Hermes Operator — Starting Up     ║")
    log.info("║   Mode: {m:33s}║", m="single cycle" if opts.once else f"continuous ({opts.interval}s)")
    log.info("║   Dry run: {d:30s}║", d=str(opts.dry_run))
    log.info("╚══════════════════════════════════════════╝")

    state = OperatorState()

    # Hydrate last_* timestamps from the most recent snapshot so a restart
    # doesn't re-fire schedules that already ran today. Silent on failure —
    # first boot (no snapshot yet) is a normal fresh-start.
    try:
        from db import get_engine as _get_engine_for_hydrate
        _hydrate_engine = _get_engine_for_hydrate()
        if state.hydrate_from_snapshot(_hydrate_engine):
            log.info(
                "Hermes state hydrated from snapshot "
                "(last_daily_intel={d}, last_autoresearch={a}, last_hypothesis_discovery={h})",
                d=state.last_daily_intel, a=state.last_autoresearch,
                h=state.last_hypothesis_discovery,
            )
        else:
            log.info("Hermes state: no prior snapshot found, fresh start")
    except Exception as exc:
        log.debug("Hermes state hydrate failed (starting fresh): {e}", e=str(exc))

    # Share state with the API for the /hermes-status endpoint
    try:
        from api.routers.system import set_hermes_state
        set_hermes_state(state)
        log.info("Hermes state shared with API for /hermes-status endpoint")
    except Exception as exc:
        log.debug("Hermes: state share with API failed (API may not be running): {e}", e=str(exc))

    # Start the LLM task queue as a background daemon thread so the
    # onboard model is never idle — processes real-time, scheduled, and
    # background tasks continuously.
    _tq_thread = None
    if not opts.dry_run:
        try:
            from orchestration.llm_taskqueue import start_task_queue_thread
            _tq_thread = start_task_queue_thread()
            log.info("LLM Task Queue daemon thread launched")
        except Exception as exc:
            log.warning("Failed to start LLM task queue: {e}", e=str(exc))

    # Start outstanding-checkout telemetry as its own background daemon
    # thread, independent of cycle completion (see
    # _outstanding_checkout_telemetry_loop). Must run in-process: it reads
    # db.py's process-local checkout tracking, which a separate script
    # invocation cannot see.
    _outstanding_checkout_thread = None
    if not opts.dry_run:
        try:
            import threading as _threading
            _outstanding_checkout_thread = _threading.Thread(
                target=_outstanding_checkout_telemetry_loop,
                args=(OUTSTANDING_CHECKOUT_POLL_SECONDS,),
                daemon=True,
                name="hermes-outstanding-checkout-telemetry",
            )
            _outstanding_checkout_thread.start()
            log.info(
                "Outstanding-checkout telemetry thread launched (interval={s}s)",
                s=OUTSTANDING_CHECKOUT_POLL_SECONDS,
            )
        except Exception as exc:
            log.warning("Failed to start outstanding-checkout telemetry: {e}", e=str(exc))

    # Run DB model migrations once on startup (idempotent)
    try:
        from db import get_engine as _get_engine_for_migrate
        from oracle.model_factory import migrate_default_models
        migrate_default_models(_get_engine_for_migrate())
    except Exception as exc:
        log.debug("migrate_default_models: {e}", e=str(exc))

    if opts.once:
        result = run_cycle(state, dry_run=opts.dry_run)
        print(json.dumps(result, default=str, indent=2))
        return

    # Continuous loop with per-cycle timeout
    import threading

    def _run_cycle_with_timeout(state, dry_run, timeout):
        """Run a cycle in a thread with a hard timeout."""
        result = [None]
        error = [None]
        def _target():
            try:
                result[0] = run_cycle(state, dry_run=dry_run)
            except Exception as exc:
                error[0] = exc
        # Named explicitly (default would be "Thread-N") so db.py's
        # per-thread checkout attribution (get_checkout_attribution) can
        # actually distinguish this cycle's connections from the
        # long-running llm-taskqueue background thread's, instead of both
        # showing up as anonymous thread names in a burst.
        t = threading.Thread(
            target=_target, daemon=True,
            name=f"hermes-cycle-{state.cycle_count + 1}",
        )
        t.start()
        t.join(timeout=timeout)
        if t.is_alive():
            stuck_on = state.current_step
            # WARNING — the cycle timeout is a handled degrade: the stuck
            # step gets blacklisted and the next cycle starts fresh. Real
            # failures inside cycle steps log ERROR at the call site.
            log.warning(
                "Cycle {n} TIMED OUT after {s}s (stuck on: {step}) "
                "— blacklisting and starting fresh",
                n=state.cycle_count, s=timeout,
                step=stuck_on or "unknown",
            )
            # Blacklist whatever was running when we timed out
            if stuck_on:
                state.cooldowns.blacklist_for_timeout(stuck_on)
            return  # Thread is daemon, will be abandoned
        if error[0]:
            raise error[0]

    while True:
        try:
            _run_cycle_with_timeout(state, opts.dry_run, CYCLE_TIMEOUT_SECONDS)
        except KeyboardInterrupt:
            log.info("Operator shutting down (keyboard interrupt)")
            break
        except Exception as exc:
            log.error("Unexpected error in operator cycle: {e}", e=str(exc))
            log.error(traceback.format_exc())
            state.consecutive_failures += 1
            if state.consecutive_failures > 10:
                log.error("10 consecutive failures — sleeping 30 minutes before retry")
                time.sleep(1800)
                state.consecutive_failures = 0

        log.info("Next cycle in {s}s...", s=opts.interval)
        time.sleep(opts.interval)


if __name__ == "__main__":
    main()
