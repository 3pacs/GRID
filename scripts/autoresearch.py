#!/usr/bin/env python3
"""
GRID Autoresearch — autonomous hypothesis generation, testing, and refinement.

Closed loop:
  1. Ollama generates a market hypothesis from current data + prior failures
  2. Hypothesis is registered in hypothesis_registry
  3. Walk-forward backtest validates it
  4. Results are critiqued by Ollama
  5. Ollama uses the critique to generate an improved hypothesis
  6. Repeat until a hypothesis PASSes or max iterations hit

Usage:
    python scripts/autoresearch.py                      # defaults
    python scripts/autoresearch.py --max-iter 10        # more iterations
    python scripts/autoresearch.py --layer TACTICAL     # different layer
    python scripts/autoresearch.py --seed "VIX term structure predicts..."
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger as log

from config import settings
from db import get_engine
from store.pit import PITStore
from ollama.client import get_client as get_ollama
from ollama.reasoner import OllamaReasoner, SYSTEM_PROMPT
from validation.backtest import WalkForwardBacktest
from governance.leases import OwnershipLost

# Per-step wall-clock budget for one run_autoresearch() invocation, read by
# scripts/hermes_operator.py when it wraps the call in _run_with_timeout
# (mirroring RESOLUTION_TIMEOUT_SECONDS / ORACLE_CYCLE_TIMEOUT_SECONDS there).
# Conservative default: max_iterations defaults to 5 and one iteration can
# include an LLM generate call, a walk-forward backtest, and an LLM critique
# call, so this is sized like the other multi-call LLM steps in hermes_operator.py
# (ORACLE_CYCLE_TIMEOUT_SECONDS=4000 for 41 tickers) rather than the
# single-call steps (SIGNAL_CLASSIFICATION_TIMEOUT_SECONDS=120). Not itself a
# cancellation guarantee — see _run_with_timeout's docstring in
# hermes_operator.py: a worker that outruns this budget is abandoned, not
# killed, which is exactly why the generation-fencing below exists.
import os as _os
AUTORESEARCH_TIMEOUT_SECONDS = int(_os.getenv("GRID_AUTORESEARCH_TIMEOUT_SECONDS", "1800"))


# ── Prompt templates ──────────────────────────────────────────────────

GENERATE_PROMPT = """\
You are GRID's autonomous researcher. Your job is to produce a single, \
testable market hypothesis that can be validated with walk-forward backtesting.

AVAILABLE FEATURES (from GRID's feature_registry — use these exact names):
{feature_list}

CURRENT MARKET SNAPSHOT:
{market_snapshot}

{history_block}

Generate ONE hypothesis. It MUST follow this exact JSON format — nothing else:

```json
{{
  "statement": "When X happens, Y follows within Z days",
  "feature_ids": [1, 5, 11],
  "lag_structure": {{"1": 0, "5": 21, "11": 5}},
  "layer": "{layer}",
  "proposed_metric": "sharpe",
  "proposed_threshold": 0.5
}}
```

Rules:
- feature_ids must be actual IDs from the list above
- lag_structure maps feature_id (as string) to the number of days lag
- statement must be specific and falsifiable — name the features, direction, and horizon
- proposed_threshold is the minimum Sharpe ratio you expect
- Do NOT repeat a hypothesis that already failed (see history)
- Think about economic CAUSATION, not just correlation
"""

REFINE_PROMPT = """\
You are GRID's autonomous researcher. A hypothesis just FAILED walk-forward validation.

FAILED HYPOTHESIS:
  Statement: {statement}
  Features: {features}
  Layer: {layer}

BACKTEST RESULTS:
  Verdict: {verdict}
  Full-period Sharpe: {sharpe}
  Baseline Sharpe: {baseline_sharpe}
  Era results: {era_summary}

CRITIQUE:
{critique}

ALL PRIOR ATTEMPTS:
{history_block}

AVAILABLE FEATURES:
{feature_list}

CURRENT MARKET SNAPSHOT:
{market_snapshot}

Using this feedback, generate an IMPROVED hypothesis. You may:
- Add or remove features
- Change the lag structure
- Change the mechanism entirely
- Combine ideas from prior attempts that partially worked

Output ONE hypothesis in this exact JSON format — nothing else:

```json
{{
  "statement": "...",
  "feature_ids": [...],
  "lag_structure": {{...}},
  "layer": "{layer}",
  "proposed_metric": "sharpe",
  "proposed_threshold": 0.5
}}
```
"""


# ── Helpers ───────────────────────────────────────────────────────────

_ortho_cache: list[int] | None = None


class AutoresearchDataError(RuntimeError):
    """Raised when a DB query in the autoresearch data-loading phase fails.

    Wraps the underlying driver/DB exception (e.g. a psycopg2 error from a
    bad column reference or a lost connection) so callers get a typed,
    identifiable signal naming which phase failed, instead of a bare
    exception whose origin is hard to trace once it has bubbled up through
    ``maybe_run_autoresearch``'s generic ``except Exception`` handler.
    """

    def __init__(self, phase: str, original: Exception):
        self.phase = phase
        self.original = original
        super().__init__(f"autoresearch data load failed in phase '{phase}': {original}")


def _failed_result(phase: str, error: str) -> dict[str, Any]:
    """Build the structured failure result returned by ``run_autoresearch``.

    Keeps the same key set as a successful run (see the return at the end
    of ``run_autoresearch``) so a caller — or a future status surface —
    can read ``status``/``phase``/``error`` to distinguish "ran and failed"
    from "ran and passed/failed hypotheses", instead of the failure being
    indistinguishable from an ordinary zero-iteration run.
    """
    return {
        "status": "failed",
        "phase": phase,
        "error": error,
        "iterations_run": 0,
        "iterations": 0,
        "best_result": None,
        "best_sharpe": None,
        "all_attempts": [],
        "passed": False,
    }


# ── Run-state persistence (W4b) ─────────────────────────────────────────
#
# Every run_autoresearch() invocation is recorded as one or more rows in
# analytical_snapshots (category="research_run", subcategory="autoresearch"):
# a "started" row when the run begins, "running" checkpoint rows after each
# iteration, and a terminal "ok"/"failed"/"abandoned" row when it ends.
# ("timeout" rows are written by the CALLER — scripts/hermes_operator.py —
# because a timed-out worker is abandoned, not killed; see its
# _run_with_timeout docstring. This module cannot know it has been timed out
# from the inside.)
#
# This reuses AnalyticalSnapshotStore — no new table, per this task's
# instructions. Persistence is best-effort: a failure to WRITE the run
# record must never break the research loop itself, so every call is
# wrapped and only logged on failure (matching
# AnalyticalSnapshotStore.save_snapshot's own fail-soft contract).

def _get_code_sha() -> str | None:
    """Return the current git HEAD sha, or None if it cannot be determined.

    Best-effort: no git binary, not a git checkout, or any other failure
    all just mean the run record's code_sha field is None.
    """
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=str(Path(__file__).resolve().parent.parent),
        )
        if proc.returncode == 0:
            sha = proc.stdout.strip()
            return sha or None
    except Exception:
        pass
    return None


def _record_research_run(
    engine: Any,
    run_id: str,
    status: str,
    *,
    phase: str | None = None,
    error: str | None = None,
    error_category: str | None = None,
    iteration: int | None = None,
    iterations: int | None = None,
    skip_reasons: list[str] | None = None,
    failure_reasons: list[str] | None = None,
    duration_s: float | None = None,
    generation: int | None = None,
    code_sha: str | None = None,
    inputs: dict[str, Any] | None = None,
) -> int | None:
    """Persist one research_run record to ``analytical_snapshots``.

    Parameters mirror the schema documented in
    docs/handoffs/2026-09-18/fable-w4b-runstate.md. ``status`` is one of
    "started", "running", "ok", "failed", "timeout", "abandoned".

    Returns:
        The new snapshot row id, or None if the write failed (logged, not
        raised — see module docstring above).
    """
    try:
        from store.snapshots import AnalyticalSnapshotStore

        store = AnalyticalSnapshotStore(db_engine=engine)
        payload = {
            "run_id": run_id,
            "status": status,
            "phase": phase,
            "error": error,
            "error_category": error_category,
            "iteration": iteration,
            "iterations": iterations,
            "skip_reasons": skip_reasons or [],
            "failure_reasons": failure_reasons or [],
            "duration_s": duration_s,
            "generation": generation,
            "code_sha": code_sha,
            "inputs": inputs,
        }
        metrics = {
            "run_id": run_id,
            "status": status,
            "iteration": iteration,
            "generation": generation,
        }
        return store.save_snapshot(
            category="research_run",
            subcategory="autoresearch",
            payload=payload,
            metrics=metrics,
        )
    except Exception as exc:
        log.warning(
            "Failed to record research run state (run_id={r}, status={s}): {e}",
            r=run_id, s=status, e=str(exc),
        )
        return None


def _generation_current(
    generation: int | None,
    is_current_generation: Callable[[int], bool] | None,
) -> bool:
    """Return whether ``generation`` is still the operator's current one.

    When either argument is None (e.g. a standalone CLI run with no
    operator-assigned generation), fencing is a no-op and every write is
    allowed — there is no operator to have moved on.

    IMPORTANT SCOPE NOTE: this is an in-process check only. ``generation``
    and ``is_current_generation`` are backed by a single mutable counter
    living in the SAME hermes_operator.py process
    (``_AutoresearchGenerationTracker``), so this fences a stale worker
    THREAD within that process — e.g. an orphaned thread left running after
    ``_run_with_timeout`` abandons it (it cannot be killed; see that
    function's docstring). It does NOT fence a second Hermes process, a
    worker that survives a process restart, or any other cross-process
    case. Cross-process fencing would need a DB-backed lease (a row with an
    owner/epoch that every writer re-checks transactionally against, e.g.
    ``SELECT ... FOR UPDATE`` or an optimistic version column) — not
    implemented here.
    """
    if generation is None or is_current_generation is None:
        return True
    return is_current_generation(generation)


def _guarded_or_direct(cur, pg, lease_generation: int | None, fn: Callable[[Any], Any]) -> Any:
    """Run ``fn(cur)`` directly, or -- when ``lease_generation`` is not
    None -- run it through ``governance.leases.run_guarded_dbapi`` against
    ``pg`` (the SAME psycopg2 connection ``cur`` belongs to) so the write
    only commits while the cross-process "autoresearch" lease still names
    this generation as current (GRID W4f, 2026-09-18 -- closes the
    cross-process gap documented in
    docs/handoffs/2026-09-18/fable-w4b-runstate.md's residual case #2).

    ``lease_generation`` is None for a standalone CLI run (no operator, no
    lease to check against -- matches the existing ``generation``/
    ``is_current_generation`` in-process fencing's own "None disables
    fencing" contract) and for every existing caller/test that predates
    this task, so this is purely additive: nothing changes unless a caller
    explicitly supplies a lease generation.

    ``governance.leases.OwnershipLost`` propagates unchanged so call sites
    can record a "fenced" outcome instead of treating it as an ordinary
    write failure.
    """
    if lease_generation is None:
        return fn(cur)
    from governance.leases import run_guarded_dbapi
    return run_guarded_dbapi(pg, "autoresearch", lease_generation, fn)


def _select_orthogonal_features(cur, max_features: int = 13, corr_threshold: float = 0.7) -> list[int]:
    """Select a set of uncorrelated features using greedy elimination.

    Picks features one at a time (by observation count, descending).
    Skips any feature whose absolute correlation with an already-selected
    feature exceeds *corr_threshold*.  Returns at most *max_features* IDs.
    """
    global _ortho_cache
    if _ortho_cache is not None:
        return _ortho_cache

    # Core features that must be included if available — one per asset class.
    # These represent the independent signal taxonomy identified by
    # orthogonality analysis (true dimensionality ~13).
    core_names = [
        'cpi', 'sp500', 'vix', 'btc', 'treasury_10y', 'yield_curve_10y2y',
        'crude_oil', 'gold', 'dollar_index', 'hy_spread', 'consumer_sentiment',
        'industrial_production', 'copper',
    ]

    # Resolve core feature IDs
    cur.execute("""
        SELECT f.id, f.name
        FROM feature_registry f
        WHERE f.name = ANY(%s) AND f.model_eligible = TRUE
    """, (core_names,))
    core_map = {row[1]: row[0] for row in cur.fetchall()}

    # Get all eligible feature IDs with enough data
    cur.execute("""
        SELECT f.id, f.name, f.family, COUNT(rs.id) as obs_count
        FROM feature_registry f
        JOIN resolved_series rs ON rs.feature_id = f.id
        WHERE f.model_eligible = TRUE
          AND rs.obs_date >= CURRENT_DATE - INTERVAL '1 year'
        GROUP BY f.id, f.name, f.family
        HAVING COUNT(rs.id) >= 30
        ORDER BY f.family, COUNT(rs.id) DESC, f.id
    """)
    candidates = cur.fetchall()
    if not candidates:
        return []

    # Build a value matrix for correlation computation
    feature_ids = [r[0] for r in candidates]
    cur.execute("""
        SELECT rs.feature_id, rs.obs_date, rs.value
        FROM resolved_series rs
        WHERE rs.feature_id = ANY(%s)
          AND rs.obs_date >= CURRENT_DATE - INTERVAL '1 year'
        ORDER BY rs.obs_date
    """, (feature_ids,))
    rows = cur.fetchall()

    # Pivot into {feature_id: {date: value}}
    from collections import defaultdict
    series: dict[int, dict] = defaultdict(dict)
    for fid, obs_date, value in rows:
        series[fid][obs_date] = float(value) if value is not None else None

    # Collect all dates, compute pairwise correlations lazily
    import math

    def _pearson(a_vals, b_vals):
        """Pearson correlation between two aligned value lists (skip NaN)."""
        pairs = [(x, y) for x, y in zip(a_vals, b_vals) if x is not None and y is not None]
        n = len(pairs)
        if n < 10:
            return 0.0  # not enough overlap — treat as uncorrelated
        mx = sum(p[0] for p in pairs) / n
        my = sum(p[1] for p in pairs) / n
        sx = math.sqrt(max(sum((p[0] - mx) ** 2 for p in pairs), 1e-30))
        sy = math.sqrt(max(sum((p[1] - my) ** 2 for p in pairs), 1e-30))
        cov = sum((p[0] - mx) * (p[1] - my) for p in pairs)
        return cov / (sx * sy) if sx > 0 and sy > 0 else 0.0

    all_dates = sorted(set(d for s in series.values() for d in s))

    selected: list[int] = []
    selected_series: list[list] = []  # aligned values for selected features

    # Phase 1: seed with core features (skip any that are too correlated)
    candidate_lookup = {r[0]: r for r in candidates}
    for core_name in core_names:
        if len(selected) >= max_features:
            break
        fid = core_map.get(core_name)
        if fid is None or fid not in series:
            continue

        vals = [series[fid].get(d) for d in all_dates]
        too_correlated = any(abs(_pearson(vals, sv)) > corr_threshold for sv in selected_series)
        if not too_correlated:
            selected.append(fid)
            selected_series.append(vals)
            info = candidate_lookup.get(fid)
            family = info[2] if info else "?"
            log.info("Ortho-select (core): {n} ({f}, ID={fid})", n=core_name, f=family, fid=fid)

    # Phase 2: fill remaining slots from other features
    for fid, name, family, _ in candidates:
        if len(selected) >= max_features:
            break
        if fid in selected or fid not in series:
            continue

        vals = [series[fid].get(d) for d in all_dates]
        too_correlated = any(abs(_pearson(vals, sv)) > corr_threshold for sv in selected_series)
        if not too_correlated:
            selected.append(fid)
            selected_series.append(vals)
            log.info("Ortho-select (fill): {n} ({f}, ID={fid})", n=name, f=family, fid=fid)

    log.info("Selected {n}/{t} orthogonal features (threshold={th})",
             n=len(selected), t=len(candidates), th=corr_threshold)
    _ortho_cache = selected
    return selected


def get_feature_list(cur) -> str:
    """Build a text list of orthogonal features for prompts."""
    ortho_ids = _select_orthogonal_features(cur)
    if not ortho_ids:
        return "(no features)"

    cur.execute("""
        SELECT f.id, f.name, f.family, COALESCE(f.subfamily, ''), f.description,
               COUNT(rs.id) as obs_count
        FROM feature_registry f
        JOIN resolved_series rs ON rs.feature_id = f.id
        WHERE f.id = ANY(%s)
          AND rs.obs_date >= CURRENT_DATE - INTERVAL '1 year'
        GROUP BY f.id, f.name, f.family, f.subfamily, f.description
        HAVING COUNT(rs.id) >= 30
        ORDER BY f.family, f.id
    """, (ortho_ids,))
    rows = cur.fetchall()
    lines = []
    for fid, name, family, subfamily, desc, cnt in rows:
        label = f"{family}/{subfamily}" if subfamily else family
        lines.append(f"  ID={fid}  {name} ({label}): {desc} [{cnt} obs]")
    return "\n".join(lines) if lines else "(no features)"


def get_market_snapshot(cur) -> str:
    """Build a snapshot of latest values for orthogonal features only."""
    ortho_ids = _select_orthogonal_features(cur)
    if not ortho_ids:
        return "(no data)"

    cur.execute("""
        SELECT f.name, f.family, r.value, r.obs_date
        FROM resolved_series r
        JOIN feature_registry f ON f.id = r.feature_id
        WHERE r.obs_date = (SELECT MAX(obs_date) FROM resolved_series WHERE feature_id = r.feature_id)
          AND f.id = ANY(%s)
        ORDER BY f.family, f.name
    """, (ortho_ids,))
    rows = cur.fetchall()
    if not rows:
        return "(no data)"
    return "\n".join(f"  {name} ({family}): {value} [{obs}]" for name, family, value, obs in rows)


def get_feature_name_map(cur) -> dict[int, str]:
    """Map feature IDs to names."""
    cur.execute("SELECT id, name FROM feature_registry WHERE model_eligible = TRUE")
    return {r[0]: r[1] for r in cur.fetchall()}


def _load_research_context(cur) -> dict[str, Any]:
    """Load the feature list, name map, and market snapshot that seed
    hypothesis generation.

    Each DB call is wrapped individually so that a query failure (a bad
    column reference, a lost connection, a permissions error, ...) raises
    a typed ``AutoresearchDataError`` naming which phase failed, rather
    than an unlabeled driver exception. ``run_autoresearch`` catches this
    and turns it into an explicit, structured failure result instead of
    letting it propagate up to be logged as a single generic warning by
    ``maybe_run_autoresearch`` (scripts/hermes_fixers.py).
    """
    try:
        feature_list = get_feature_list(cur)
    except Exception as exc:  # noqa: BLE001 - intentionally broad, re-typed below
        raise AutoresearchDataError("feature_list", exc) from exc

    try:
        feature_names = get_feature_name_map(cur)
    except Exception as exc:  # noqa: BLE001
        raise AutoresearchDataError("feature_name_map", exc) from exc

    try:
        market_snapshot = get_market_snapshot(cur)
    except Exception as exc:  # noqa: BLE001
        raise AutoresearchDataError("market_snapshot", exc) from exc

    return {
        "feature_list": feature_list,
        "feature_names": feature_names,
        "market_snapshot": market_snapshot,
    }


def parse_hypothesis_json(text: str) -> dict[str, Any] | None:
    """Extract hypothesis JSON from LLM output."""
    # Try to find JSON block
    json_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if json_match:
        raw = json_match.group(1)
    else:
        # Try bare JSON
        brace_match = re.search(r"\{[^{}]*\"statement\"[^{}]*\}", text, re.DOTALL)
        if brace_match:
            raw = brace_match.group(0)
        else:
            return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None

    # Validate required fields
    required = {"statement", "feature_ids", "lag_structure", "layer", "proposed_metric", "proposed_threshold"}
    if not required.issubset(data.keys()):
        return None

    if not isinstance(data["feature_ids"], list) or len(data["feature_ids"]) == 0:
        return None

    return data


def format_history(attempts: list[dict]) -> str:
    """Format prior attempts for inclusion in prompts."""
    if not attempts:
        return "No prior attempts."

    lines = []
    for i, a in enumerate(attempts, 1):
        sharpe = a.get("sharpe", "?")
        verdict = a.get("verdict", "?")
        statement = a.get("statement", a.get("error", "N/A"))
        lines.append(
            f"  Attempt {i}: \"{statement}\" → {verdict} (Sharpe={sharpe})"
        )
    return "\n".join(lines)


def format_era_summary(era_results: list[dict]) -> str:
    """Compact era summary for the LLM."""
    parts = []
    for e in era_results:
        status = e.get("status", "?")
        if status == "OK":
            parts.append(f"Era{e['era']}: ret={e.get('return', '?')}, sharpe={e.get('sharpe', '?')}")
        else:
            parts.append(f"Era{e['era']}: {status}")
    return " | ".join(parts)


def _create_model_from_hypothesis(
    cur, hyp_id: int, hyp: dict, layer: str, validation_result: dict
) -> int:
    """Create a CANDIDATE model from a PASSED hypothesis.

    Returns the new model_registry.id.
    """
    from datetime import datetime, timezone

    name = f"hyp-{hyp_id}-{layer.lower()}"
    version = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    lag_structure = hyp.get("lag_structure", {})
    if isinstance(lag_structure, str):
        lag_structure = json.loads(lag_structure)
    parameter_snapshot = json.dumps({
        "proposed_metric": hyp.get("proposed_metric", ""),
        "proposed_threshold": hyp.get("proposed_threshold", 0),
        "lag_structure": lag_structure,
    })

    # Find the validation_results.id for this hypothesis
    cur.execute(
        "SELECT id FROM validation_results "
        "WHERE hypothesis_id = %s AND overall_verdict = 'PASS' "
        "ORDER BY run_timestamp DESC LIMIT 1",
        (hyp_id,),
    )
    val_row = cur.fetchone()
    validation_run_id = val_row[0] if val_row else None

    cur.execute(
        "INSERT INTO model_registry "
        "(name, layer, version, state, hypothesis_id, validation_run_id, "
        " feature_set, parameter_snapshot) "
        "VALUES (%s, %s, %s, 'CANDIDATE', %s, %s, %s, %s) RETURNING id",
        (
            name, layer, version, hyp_id, validation_run_id,
            hyp["feature_ids"], parameter_snapshot,
        ),
    )
    model_id = cur.fetchone()[0]
    log.info(
        "Auto-created CANDIDATE model {m} from hypothesis {h}",
        m=model_id, h=hyp_id,
    )
    return model_id


# ── Core loop ─────────────────────────────────────────────────────────

def run_autoresearch(
    max_iterations: int = 5,
    layer: str = "REGIME",
    seed_hypothesis: str | None = None,
    backtest_start: date | None = None,
    backtest_end: date | None = None,
    n_splits: int = 5,
    cost_bps: float = 10.0,
    run_id: str | None = None,
    generation: int | None = None,
    is_current_generation: Callable[[int], bool] | None = None,
    lease_generation: int | None = None,
    lease_owner_id: str | None = None,
) -> dict[str, Any]:
    """Run the full autoresearch loop.

    Parameters:
        max_iterations: Max generate-test-refine cycles.
        layer: GRID layer (REGIME, TACTICAL, EXECUTION).
        seed_hypothesis: Optional starting hypothesis text to guide first generation.
        backtest_start: Backtest start date (default: 1 year ago).
        backtest_end: Backtest end date (default: today).
        n_splits: Walk-forward splits.
        cost_bps: Transaction cost assumption.
        run_id: Identifies this invocation in the ``research_run`` snapshot
            trail. Pass the SAME run_id to retry an aborted/timed-out
            invocation — hypothesis inserts are then deduplicated by
            (statement, layer) and a hypothesis already PASSED on a prior
            attempt is not re-backtested or re-notified. Defaults to a
            fresh uuid4 (a standalone CLI run has nothing to retry).
        generation: Operator-assigned generation id for write fencing (see
            ``_generation_current``). None (the CLI/default case) disables
            fencing — there is no operator to have moved on from.
        is_current_generation: Callable checking whether ``generation`` is
            still current. Passed by scripts/hermes_fixers.py::
            maybe_run_autoresearch, which forwards it from
            scripts/hermes_operator.py's ``_AutoresearchGenerationTracker``.
        lease_generation: Cross-process generation for the "autoresearch"
            row in ``research_leases`` (GRID W4f, 2026-09-18), obtained by
            the operator from ``governance.leases.acquire()``. Distinct
            from ``generation`` above: ``generation``/
            ``is_current_generation`` fence a stale worker THREAD within
            one Hermes process (cheap, in-memory, checked once per
            iteration as an early-exit optimization); ``lease_generation``
            is what actually makes every real write
            (hypothesis_registry/model_registry/validation_results)
            transactionally safe against a SECOND process, or a worker
            surviving a process restart, via ``governance.leases.
            guarded_write``/``guarded_write_dbapi``. None (the default)
            disables cross-process guarding, matching a standalone CLI run
            with no lease to check against.
        lease_owner_id: Owner id recorded on the lease by the caller's
            ``acquire()`` call. Only used for logging here (the guard
            itself only checks generation + expiry, not owner_id) --
            forwarded through for traceability in log lines.

    Returns:
        dict: Summary with best hypothesis, all attempts, and final verdict.
            "status" is one of started/ok/failed/abandoned/fenced.
            "abandoned" means an in-process generation check
            (``is_current_generation``) fired at some point; "fenced"
            (GRID W4f) means a cross-process ``governance.leases.
            OwnershipLost`` fired -- the lease was lost to another
            process/generation while a real write was attempted. "timeout"
            is recorded by the caller, not returned from here, since a
            timed-out call never returns at all (the worker thread is
            abandoned by _run_with_timeout).
    """
    import psycopg2

    if backtest_start is None:
        backtest_start = date.today() - timedelta(days=365)
    if backtest_end is None:
        backtest_end = date.today()

    run_id = run_id or str(uuid.uuid4())
    code_sha = _get_code_sha()
    _start_monotonic = time.monotonic()

    def _duration_s() -> float:
        return round(time.monotonic() - _start_monotonic, 3)

    engine = get_engine()
    _record_research_run(
        engine, run_id, "started",
        phase="init", generation=generation, code_sha=code_sha,
    )

    pit = PITStore(engine)
    # Cross-process write guard (GRID W4f) for the validation_results insert
    # validation/backtest.py performs internally. Only constructed when the
    # caller supplied a lease_generation -- every pre-existing caller/test
    # (lease_generation=None) gets the exact same WalkForwardBacktest(engine,
    # pit) 2-arg call as before, so nothing here changes their behavior.
    if lease_generation is not None:
        from governance.leases import run_guarded

        def _write_guard(fn, _engine=engine, _gen=lease_generation):
            return run_guarded(_engine, "autoresearch", _gen, fn)

        backtester = WalkForwardBacktest(engine, pit, write_guard=_write_guard)
    else:
        backtester = WalkForwardBacktest(engine, pit)
    ollama = get_ollama()
    reasoner = OllamaReasoner(ollama)

    if not ollama.is_available:
        log.error("Ollama not available — cannot run autoresearch")
        _record_research_run(
            engine, run_id, "failed",
            phase="ollama_availability", error="Ollama not available",
            error_category="ollama_unavailable", iterations=0,
            duration_s=_duration_s(), generation=generation, code_sha=code_sha,
        )
        return _failed_result(phase="ollama_availability", error="Ollama not available")

    try:
        pg = psycopg2.connect(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            dbname=settings.DB_NAME,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
        )
    except Exception as exc:
        log.error("Autoresearch DB connection failed: {e}", e=str(exc))
        _record_research_run(
            engine, run_id, "failed",
            phase="db_connect", error=str(exc), error_category="db_connect_failure",
            iterations=0, duration_s=_duration_s(), generation=generation, code_sha=code_sha,
        )
        return _failed_result(phase="db_connect", error=str(exc))

    pg.autocommit = True
    cur = pg.cursor()

    try:
        ctx = _load_research_context(cur)
    except AutoresearchDataError as exc:
        # Reserve log.error for unhandled application bugs (CLAUDE.md):
        # a DB query failing here means the code and the live schema have
        # drifted apart, which is exactly that class of bug.
        log.error(
            "Autoresearch data load failed in phase '{p}': {e}",
            p=exc.phase, e=exc.original,
        )
        pg.close()
        _record_research_run(
            engine, run_id, "failed",
            phase=exc.phase, error=str(exc.original), error_category="db_load_failure",
            iterations=0, duration_s=_duration_s(), generation=generation, code_sha=code_sha,
        )
        return _failed_result(phase=exc.phase, error=str(exc.original))

    feature_list = ctx["feature_list"]
    feature_names = ctx["feature_names"]
    market_snapshot = ctx["market_snapshot"]

    # Inputs manifest for the run record — derived from data already loaded
    # above, not a maintained literal (same "derive, don't hardcode"
    # principle store/snapshots.py uses for category discovery).
    _ortho_ids_for_manifest = _select_orthogonal_features(cur)  # cached, no re-query
    inputs_manifest = {
        "feature_ids_count": len(_ortho_ids_for_manifest),
        "market_snapshot_keys": [
            feature_names.get(fid, str(fid)) for fid in _ortho_ids_for_manifest
        ],
        # No evaluation-version concept exists anywhere in this codebase
        # today (checked validation/backtest.py and config.py) — left None
        # rather than inventing one.
        "evaluation_version": None,
    }
    _record_research_run(
        engine, run_id, "running",
        phase="context_loaded", iteration=0, generation=generation,
        code_sha=code_sha, inputs=inputs_manifest, duration_s=_duration_s(),
    )

    attempts: list[dict[str, Any]] = []
    best_result: dict[str, Any] | None = None
    best_sharpe: float = -999.0
    fence_events: list[str] = []

    log.info("=" * 70)
    log.info("GRID AUTORESEARCH ENGINE")
    log.info("Layer: {} | Max iterations: {}", layer, max_iterations)
    log.info("Backtest: {} to {} | Splits: {}", backtest_start, backtest_end, n_splits)
    log.info("=" * 70)

    for iteration in range(1, max_iterations + 1):
        log.info("─" * 70)
        log.info("ITERATION {}/{}", iteration, max_iterations)
        log.info("─" * 70)

        # ── Fencing check ──────────────────────────────────────────────
        # Checked once per iteration, before any write this iteration would
        # make (hypothesis insert, backtest -> validation_results insert,
        # model_registry insert). If the operator has moved on to a newer
        # generation — e.g. because _run_with_timeout gave up waiting on
        # this exact call and the caller bumped the generation counter —
        # this worker is an orphan and must stop trying to publish. It
        # cannot be killed (see hermes_operator.py's _run_with_timeout
        # docstring), so "stop" here means "stop writing", not "stop
        # existing"; the thread still runs to this point harmlessly.
        if not _generation_current(generation, is_current_generation):
            log.warning(
                "Autoresearch generation {g} superseded before iteration {i} "
                "— abandoning further writes (fenced)", g=generation, i=iteration,
            )
            fence_events.append(f"fenced_before_iteration_{iteration}")
            attempts.append({
                "iteration": iteration,
                "error": "fenced: operator generation superseded",
                "fenced": True,
            })
            break

        _record_research_run(
            engine, run_id, "running",
            phase="iteration", iteration=iteration, generation=generation,
            code_sha=code_sha, duration_s=_duration_s(),
        )

        # ── Step 1: Generate or refine hypothesis ─────────────────────
        if iteration == 1:
            history_block = ""
            if seed_hypothesis:
                history_block = f"SEED IDEA (use this as inspiration):\n  {seed_hypothesis}\n"

            prompt = GENERATE_PROMPT.format(
                feature_list=feature_list,
                market_snapshot=market_snapshot,
                history_block=history_block,
                layer=layer,
            )
        else:
            # Find last attempt that has a statement (skip errors)
            last = None
            for a in reversed(attempts):
                if "statement" in a:
                    last = a
                    break

            if last is None:
                # All prior attempts failed — regenerate from scratch
                prompt = GENERATE_PROMPT.format(
                    feature_list=feature_list,
                    market_snapshot=market_snapshot,
                    history_block=format_history(attempts),
                    layer=layer,
                )
            else:
                critique = reasoner.critique_backtest_result(
                    hypothesis=last["statement"],
                    metric_name="sharpe",
                    metric_value=last.get("sharpe", 0),
                    baseline_value=last.get("baseline_sharpe", 0),
                    n_periods=n_splits,
                ) or "No critique available."

                prompt = REFINE_PROMPT.format(
                    statement=last["statement"],
                    features=", ".join(
                        feature_names.get(fid, str(fid)) for fid in last.get("feature_ids", [])
                    ),
                    layer=layer,
                    verdict=last.get("verdict", "FAIL"),
                    sharpe=last.get("sharpe", "?"),
                    baseline_sharpe=last.get("baseline_sharpe", "?"),
                    era_summary=last.get("era_summary", "?"),
                    critique=critique,
                    history_block=format_history(attempts),
                    feature_list=feature_list,
                    market_snapshot=market_snapshot,
                )

        log.info("[1/4] Generating hypothesis via Ollama...")
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ]

        response = ollama.chat(
            messages,
            temperature=0.6,
            num_predict=800,
            system_knowledge=["04_regime_detection", "07_economic_mechanisms", "05_derived_signals"],
        )

        if response is None:
            log.error("Ollama returned no response at iteration {i}", i=iteration)
            attempts.append({"iteration": iteration, "error": "Ollama no response"})
            continue

        hyp = parse_hypothesis_json(response)
        if hyp is None:
            log.warning("Failed to parse hypothesis JSON from Ollama response")
            log.debug("Raw response: {r}", r=response[:500])
            attempts.append({"iteration": iteration, "error": "JSON parse failed", "raw": response[:500]})
            continue

        log.info("  Statement: {}", hyp['statement'])
        log.info("  Features:  {}", [feature_names.get(f, f) for f in hyp['feature_ids']])
        log.info("  Lags:      {}", hyp['lag_structure'])
        log.info("  Threshold: Sharpe >= {}", hyp['proposed_threshold'])

        # ── Step 2: Register hypothesis (idempotent) ──────────────────
        # A retry of this same run_id (e.g. after the operator abandoned a
        # timed-out prior attempt and the caller invokes run_autoresearch
        # again) must not insert a second hypothesis_registry row for the
        # same (statement, layer). hypothesis_registry has no run_id
        # column to key on directly (adding one is a schema change, out of
        # scope — "no new table" per this task, and a new column carries
        # the same weight), so (statement, layer) is the deterministic key
        # this loop already has in hand.
        log.info("[2/4] Registering hypothesis...")
        cur.execute(
            "SELECT id, state FROM hypothesis_registry "
            "WHERE statement = %s AND layer = %s ORDER BY id DESC LIMIT 1",
            (hyp["statement"], layer),
        )
        existing_hyp = cur.fetchone()
        reused_hyp = existing_hyp is not None

        if reused_hyp:
            hyp_id, existing_state = existing_hyp
            log.info(
                "  Reusing existing hypothesis_registry row {id} (state={s}) "
                "for identical statement+layer — idempotent retry, no duplicate insert",
                id=hyp_id, s=existing_state,
            )
        else:
            def _insert_hypothesis(gcur):
                gcur.execute(
                    "INSERT INTO hypothesis_registry "
                    "(statement, layer, feature_ids, lag_structure, proposed_metric, proposed_threshold, state) "
                    "VALUES (%s, %s, %s, %s, %s, %s, 'TESTING') RETURNING id",
                    (
                        hyp["statement"],
                        layer,
                        hyp["feature_ids"],
                        json.dumps(hyp["lag_structure"]),
                        hyp["proposed_metric"],
                        hyp["proposed_threshold"],
                    ),
                )
                return gcur.fetchone()[0]

            try:
                hyp_id = _guarded_or_direct(cur, pg, lease_generation, _insert_hypothesis)
                existing_state = "TESTING"
                log.info("  Registered as hypothesis_id={}", hyp_id)
            except OwnershipLost as exc:
                log.warning(
                    "Autoresearch lease lost before hypothesis insert "
                    "(iteration {i}): {e}", i=iteration, e=str(exc),
                )
                fence_events.append(f"lease_lost_before_hypothesis_insert_iteration_{iteration}")
                attempts.append({
                    "iteration": iteration,
                    "statement": hyp["statement"],
                    "error": "fenced: cross-process lease lost",
                    "fenced": True,
                })
                break
            except Exception as exc:
                log.error("Failed to register hypothesis: {e}", e=str(exc))
                attempts.append({
                    "iteration": iteration,
                    "statement": hyp["statement"],
                    "error": f"DB insert failed: {exc}",
                })
                continue

        # A hypothesis reused from a prior attempt that already reached a
        # terminal state must not be re-backtested (that would insert a
        # second validation_results row) or, if it already PASSED,
        # re-notified (see "no notification path called twice" below).
        if reused_hyp and existing_state == "PASSED":
            log.info(
                "  Hypothesis {id} already PASSED on a prior attempt with "
                "this statement — skipping duplicate backtest/notify "
                "(idempotent retry)", id=hyp_id,
            )
            reused_attempt = {
                "iteration": iteration,
                "hypothesis_id": hyp_id,
                "statement": hyp["statement"],
                "feature_ids": hyp["feature_ids"],
                "lag_structure": hyp["lag_structure"],
                "verdict": "PASS",
                "reused": True,
            }
            attempts.append(reused_attempt)
            if best_result is None:
                best_result = reused_attempt

            # Model idempotency: don't create a second CANDIDATE model for
            # a hypothesis that already has one.
            cur.execute(
                "SELECT id FROM model_registry WHERE hypothesis_id = %s LIMIT 1",
                (hyp_id,),
            )
            if cur.fetchone() is None and _generation_current(generation, is_current_generation):
                try:
                    _guarded_or_direct(
                        cur, pg, lease_generation,
                        lambda gcur: _create_model_from_hypothesis(gcur, hyp_id, hyp, layer, {}),
                    )
                except OwnershipLost as exc:
                    log.warning(
                        "Autoresearch lease lost before reused-PASSED model "
                        "creation (hypothesis {h}): {e}", h=hyp_id, e=str(exc),
                    )
                    fence_events.append(f"lease_lost_before_reused_model_creation_iteration_{iteration}")
                except Exception as exc:
                    log.warning("Auto model creation failed: {e}", e=str(exc))

            # Deliberately NOT calling notify_on_pass here — this branch
            # only runs when the hypothesis already reached PASSED on an
            # earlier attempt, which is exactly the attempt that called
            # notify_on_pass the one time it was genuinely new. Calling it
            # again here would be the "notification path called twice"
            # this task's idempotent-acceptance requirement rules out.
            break

        if reused_hyp and existing_state == "FAILED":
            log.info(
                "  Hypothesis {id} already FAILED on a prior attempt with "
                "this statement — skipping duplicate backtest, refining "
                "for next iteration", id=hyp_id,
            )
            attempts.append({
                "iteration": iteration,
                "hypothesis_id": hyp_id,
                "statement": hyp["statement"],
                "feature_ids": hyp["feature_ids"],
                "verdict": "FAIL",
                "reused": True,
            })
            continue

        # ── Step 3: Run walk-forward backtest ─────────────────────────
        # backtester.run_validation() writes a validation_results row
        # internally (validation/backtest.py, outside this task's editable
        # file set) — fenced here, at the call boundary, rather than
        # inside that module: if the operator has moved on since the
        # fencing check at the top of this iteration, don't even start a
        # backtest whose only DB effect is another write this worker has
        # already lost the right to make.
        if not _generation_current(generation, is_current_generation):
            log.warning(
                "Autoresearch generation {g} superseded before backtest "
                "(hypothesis {h}) — skipping (fenced)", g=generation, h=hyp_id,
            )
            fence_events.append(f"fenced_before_backtest_iteration_{iteration}")
            attempts.append({
                "iteration": iteration,
                "hypothesis_id": hyp_id,
                "statement": hyp["statement"],
                "error": "fenced: operator generation superseded",
                "fenced": True,
            })
            break

        log.info("[3/4] Running walk-forward backtest...")
        try:
            result = backtester.run_validation(
                hypothesis_id=hyp_id,
                feature_ids=hyp["feature_ids"],
                start_date=backtest_start,
                end_date=backtest_end,
                n_splits=n_splits,
                cost_bps=cost_bps,
            )
        except OwnershipLost as exc:
            # The validation_results insert inside run_validation() (its
            # own write_guard, constructed above) lost the cross-process
            # lease mid-backtest. Do NOT also try to mark the hypothesis
            # FAILED here -- that write would need the exact same guard,
            # and the honest outcome is "this worker no longer owns
            # anything", not "this hypothesis failed". Stop and record
            # fenced, same shape as every other fencing checkpoint.
            log.warning(
                "Autoresearch lease lost during backtest (hypothesis {h}): {e}",
                h=hyp_id, e=str(exc),
            )
            fence_events.append(f"lease_lost_during_backtest_iteration_{iteration}")
            attempts.append({
                "iteration": iteration,
                "hypothesis_id": hyp_id,
                "statement": hyp["statement"],
                "error": "fenced: cross-process lease lost",
                "fenced": True,
            })
            break
        except Exception as exc:
            log.error("Backtest failed: {e}", e=str(exc))
            kill_reason = f"Backtest error: {exc}"

            def _mark_failed(gcur):
                gcur.execute(
                    "UPDATE hypothesis_registry SET state='FAILED', kill_reason=%s WHERE id=%s",
                    (kill_reason, hyp_id),
                )

            try:
                _guarded_or_direct(cur, pg, lease_generation, _mark_failed)
            except OwnershipLost as lease_exc:
                log.warning(
                    "Autoresearch lease lost while marking hypothesis {h} "
                    "FAILED after a backtest error: {e}", h=hyp_id, e=str(lease_exc),
                )
                fence_events.append(f"lease_lost_marking_failed_iteration_{iteration}")
                attempts.append({
                    "iteration": iteration,
                    "hypothesis_id": hyp_id,
                    "statement": hyp["statement"],
                    "error": "fenced: cross-process lease lost",
                    "fenced": True,
                })
                break
            attempts.append({
                "iteration": iteration,
                "statement": hyp["statement"],
                "feature_ids": hyp["feature_ids"],
                "error": f"Backtest failed: {exc}",
            })
            continue

        verdict = result.get("overall_verdict", "FAIL")
        full_metrics = result.get("full_period_metrics", {})
        baseline = result.get("baseline_comparison", {})
        era_results = result.get("era_results", [])

        sharpe = full_metrics.get("sharpe", 0)
        baseline_sharpe = baseline.get("sharpe", 0)

        log.info("  Verdict:        {}", verdict)
        log.info("  Sharpe:         {}", sharpe)
        log.info("  Baseline:       {}", baseline_sharpe)
        log.info("  Return:         {}", full_metrics.get('return', '?'))
        log.info("  Max drawdown:   {}", full_metrics.get('max_drawdown', '?'))
        log.info("  Era summary:    {}", format_era_summary(era_results))

        # ── Step 4: Update hypothesis state ───────────────────────────
        # This is the mid-iteration state UPDATE named as the single
        # largest remaining window in
        # docs/handoffs/2026-09-18/fable-w4b-runstate.md's residual case
        # #1: nothing previously re-checked fencing between "backtest
        # returned" and "state written", so an orphan whose generation
        # went stale WHILE the backtest itself ran (unbounded from this
        # module's perspective) could still land this write. Routing it
        # through the lease guard closes that: the row lock guarded_write
        # takes cannot have been affected by anything that happened during
        # the backtest, because the check now happens transactionally at
        # the moment of THIS write, not before the backtest started.
        log.info("[4/4] Updating hypothesis state...")
        new_state = "PASSED" if verdict == "PASS" else "FAILED"
        kill_reason = None if verdict == "PASS" else f"Verdict={verdict}, Sharpe={sharpe}"

        def _update_state(gcur, _state=new_state, _reason=kill_reason):
            gcur.execute(
                "UPDATE hypothesis_registry SET state=%s, kill_reason=%s, updated_at=NOW() WHERE id=%s",
                (_state, _reason, hyp_id),
            )

        try:
            _guarded_or_direct(cur, pg, lease_generation, _update_state)
        except OwnershipLost as exc:
            log.warning(
                "Autoresearch lease lost writing hypothesis {h} state "
                "after backtest (verdict={v}): {e}", h=hyp_id, v=verdict, e=str(exc),
            )
            fence_events.append(f"lease_lost_after_backtest_iteration_{iteration}")
            attempts.append({
                "iteration": iteration,
                "hypothesis_id": hyp_id,
                "statement": hyp["statement"],
                "error": "fenced: cross-process lease lost",
                "fenced": True,
            })
            break

        attempt = {
            "iteration": iteration,
            "hypothesis_id": hyp_id,
            "statement": hyp["statement"],
            "feature_ids": hyp["feature_ids"],
            "lag_structure": hyp["lag_structure"],
            "verdict": verdict,
            "sharpe": sharpe,
            "baseline_sharpe": baseline_sharpe,
            "return": full_metrics.get("return", 0),
            "max_drawdown": full_metrics.get("max_drawdown", 0),
            "era_summary": format_era_summary(era_results),
            "era_results": era_results,
        }
        attempts.append(attempt)

        # Track best
        if sharpe > best_sharpe:
            best_sharpe = sharpe
            best_result = attempt

        # ── Early exit on PASS ────────────────────────────────────────
        if verdict == "PASS":
            log.info("*** HYPOTHESIS PASSED at iteration {} ***", iteration)
            log.info("    {}", hyp['statement'])
            log.info("    Sharpe={} (baseline={})", sharpe, baseline_sharpe)

            # Auto-create CANDIDATE model from passed hypothesis — fenced
            # (model_registry insert) and idempotent (skip if a candidate
            # already exists for this hypothesis, e.g. a retry landed here
            # a second time before the DB round-trip below could record it).
            #
            # `model_creation_fenced` gates the notification below (GRID
            # W4f, 2026-09-18 fix): previously this fenced/not-fenced
            # branch did NOT stop notify_on_pass from being called a few
            # lines down even when model creation was skipped as fenced —
            # a real "notification fires on a fenced path" bug found while
            # tracing every sink reachable from this branch, not something
            # this task introduced. Any fencing here (in-process OR
            # cross-process lease loss) now suppresses the notification.
            model_creation_fenced = False
            if not _generation_current(generation, is_current_generation):
                log.warning(
                    "Autoresearch generation {g} superseded before model "
                    "creation (hypothesis {h}) — skipping (fenced)",
                    g=generation, h=hyp_id,
                )
                fence_events.append(f"fenced_before_model_creation_iteration_{iteration}")
                model_creation_fenced = True
            else:
                cur.execute(
                    "SELECT id FROM model_registry WHERE hypothesis_id = %s LIMIT 1",
                    (hyp_id,),
                )
                if cur.fetchone() is None:
                    try:
                        _guarded_or_direct(
                            cur, pg, lease_generation,
                            lambda gcur: _create_model_from_hypothesis(gcur, hyp_id, hyp, layer, result),
                        )
                        log.info("    Model created (CANDIDATE) from hypothesis {}", hyp_id)
                    except OwnershipLost as exc:
                        log.warning(
                            "Autoresearch lease lost before model creation "
                            "(hypothesis {h}): {e}", h=hyp_id, e=str(exc),
                        )
                        fence_events.append(f"lease_lost_before_model_creation_iteration_{iteration}")
                        model_creation_fenced = True
                    except Exception as exc:
                        log.warning("Auto model creation failed: {e}", e=str(exc))
                else:
                    log.info("    CANDIDATE model already exists for hypothesis {} — skipping duplicate", hyp_id)

            # Send email notification. This is the ONE place a genuinely
            # new PASS notifies — the reused_hyp/"PASSED" idempotent
            # short-circuit above deliberately never reaches this branch,
            # so a retry with the same run_id cannot double-notify. Also
            # never notifies on a fenced path (see model_creation_fenced
            # above) — a notification is a real-world side effect (an
            # actual email send, see scripts/notify.py) with no
            # transactional rollback, so the guard's job here is to
            # prevent the call from ever happening, not to wrap it.
            if not model_creation_fenced:
                try:
                    from scripts.notify import notify_on_pass
                    notify_on_pass(attempt)
                except Exception as exc:
                    log.debug("Email notification skipped: {e}", e=str(exc))
            else:
                log.info(
                    "    Notification skipped — model creation was fenced "
                    "for hypothesis {}", hyp_id,
                )

            break

        log.info("  Hypothesis FAILED — refining for next iteration...")

    # ── Summary ───────────────────────────────────────────────────────
    pg.close()

    log.info("=" * 70)
    log.info("AUTORESEARCH COMPLETE")
    log.info("Iterations run: {}", len(attempts))
    if best_result:
        log.info("Best hypothesis (Sharpe={}):", best_sharpe)
        log.info("  {}", best_result['statement'])
        log.info("  Verdict: {}", best_result['verdict'])
        log.info("  Features: {}", [feature_names.get(f, f) for f in best_result.get('feature_ids', [])])
    else:
        log.info("No valid hypotheses were generated.")
    log.info("=" * 70)

    # The run-record end write itself is NOT lease-guarded (deliberately —
    # see docs/handoffs/2026-09-18/fable-w4f-write-sinks.md's "sinks that
    # cannot be guarded" section): it is what REPORTS a fenced outcome, so
    # gating it behind the same lease it is reporting the loss of would be
    # circular. It stays the existing best-effort/fail-soft
    # analytical_snapshots append.
    #
    # final_status distinguishes WHY a run did not reach a clean "ok":
    #   "fenced"    — a cross-process governance.leases.OwnershipLost fired
    #                 (lease_lost_* in fence_events) — GRID W4f.
    #   "abandoned" — only the in-process generation check
    #                 (is_current_generation) fired, no lease was even in
    #                 play (a standalone/no-lease run) — GRID W4b, unchanged.
    # A worker that only became stale AFTER its last per-iteration check
    # (e.g. the operator moved on while this call was doing its final
    # bookkeeping) must not publish an "ok" end record — that would let a
    # superseded run look like the authoritative outcome for run_id, even
    # though every insert-row write is append-only and does not literally
    # overwrite anything.
    lease_fenced = any(e.startswith("lease_lost_") for e in fence_events)
    final_status = "ok"
    if lease_fenced:
        final_status = "fenced"
    elif fence_events or not _generation_current(generation, is_current_generation):
        final_status = "abandoned"

    _record_research_run(
        engine, run_id, final_status,
        phase="complete", iteration=len(attempts), iterations=len(attempts),
        skip_reasons=fence_events, generation=generation, code_sha=code_sha,
        duration_s=_duration_s(),
    )

    return {
        "status": final_status,
        # "iterations_run" is the long-standing key (read by scripts/notify.py).
        # "iterations" is what scripts/hermes_fixers.py::maybe_run_autoresearch
        # actually reads into state.hypotheses_tested via result.get("iterations", 0).
        # That mismatch meant hypotheses_tested never advanced even on a fully
        # successful run; both keys are populated here so it does.
        "iterations_run": len(attempts),
        "iterations": len(attempts),
        "best_result": best_result,
        "best_sharpe": best_sharpe,
        "all_attempts": attempts,
        "passed": any(a.get("verdict") == "PASS" for a in attempts),
        "run_id": run_id,
        "fenced": bool(fence_events),
    }


# ── CLI ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GRID Autoresearch Engine")
    parser.add_argument("--max-iter", type=int, default=5, help="Max iterations (default: 5)")
    parser.add_argument("--layer", default="REGIME", choices=["REGIME", "TACTICAL", "EXECUTION"])
    parser.add_argument("--seed", type=str, default=None, help="Seed hypothesis idea")
    parser.add_argument("--start", type=str, default=None, help="Backtest start (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="Backtest end (YYYY-MM-DD)")
    parser.add_argument("--splits", type=int, default=5, help="Walk-forward splits")
    parser.add_argument("--cost-bps", type=float, default=10.0, help="Transaction cost (bps)")

    args = parser.parse_args()

    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None

    result = run_autoresearch(
        max_iterations=args.max_iter,
        layer=args.layer,
        seed_hypothesis=args.seed,
        backtest_start=start,
        backtest_end=end,
        n_splits=args.splits,
        cost_bps=args.cost_bps,
    )

    # Save summary
    out_path = f"outputs/autoresearch_{date.today().isoformat()}.json"
    import os
    os.makedirs("outputs", exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, default=str)
    log.info("Full results saved to {}", out_path)


if __name__ == "__main__":
    main()
