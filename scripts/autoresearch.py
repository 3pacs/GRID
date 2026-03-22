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
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger as log

from db import get_engine, execute_sql
from store.pit import PITStore
from ollama.client import get_client as get_ollama
from ollama.reasoner import OllamaReasoner, SYSTEM_PROMPT
from validation.backtest import WalkForwardBacktest


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

def get_feature_list(cur) -> str:
    """Build a text list of all features for prompts."""
    cur.execute(
        "SELECT id, name, family, description FROM feature_registry "
        "WHERE model_eligible = TRUE ORDER BY family, id"
    )
    rows = cur.fetchall()
    lines = []
    for fid, name, family, desc in rows:
        lines.append(f"  ID={fid}  {name} ({family}): {desc}")
    return "\n".join(lines) if lines else "(no features)"


def get_market_snapshot(cur) -> str:
    """Build a snapshot of latest feature values."""
    cur.execute("""
        SELECT f.name, f.family, r.value, r.obs_date
        FROM resolved_series r
        JOIN feature_registry f ON f.id = r.feature_id
        WHERE r.obs_date = (SELECT MAX(obs_date) FROM resolved_series WHERE feature_id = r.feature_id)
          AND f.model_eligible = TRUE
        ORDER BY f.family, f.name
    """)
    rows = cur.fetchall()
    if not rows:
        return "(no data)"
    return "\n".join(f"  {name} ({family}): {value} [{obs}]" for name, family, value, obs in rows)


def get_feature_name_map(cur) -> dict[int, str]:
    """Map feature IDs to names."""
    cur.execute("SELECT id, name FROM feature_registry WHERE model_eligible = TRUE")
    return {r[0]: r[1] for r in cur.fetchall()}


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


# ── Core loop ─────────────────────────────────────────────────────────

def run_autoresearch(
    max_iterations: int = 5,
    layer: str = "REGIME",
    seed_hypothesis: str | None = None,
    backtest_start: date | None = None,
    backtest_end: date | None = None,
    n_splits: int = 5,
    cost_bps: float = 10.0,
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

    Returns:
        dict: Summary with best hypothesis, all attempts, and final verdict.
    """
    import psycopg2
    from config import settings

    if backtest_start is None:
        backtest_start = date.today() - timedelta(days=365)
    if backtest_end is None:
        backtest_end = date.today()

    engine = get_engine()
    pit = PITStore(engine)
    backtester = WalkForwardBacktest(engine, pit)
    ollama = get_ollama()
    reasoner = OllamaReasoner(ollama)

    if not ollama.is_available:
        log.error("Ollama not available — cannot run autoresearch")
        return {"error": "Ollama not available"}

    pg = psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )
    pg.autocommit = True
    cur = pg.cursor()

    feature_list = get_feature_list(cur)
    feature_names = get_feature_name_map(cur)
    market_snapshot = get_market_snapshot(cur)

    attempts: list[dict[str, Any]] = []
    best_result: dict[str, Any] | None = None
    best_sharpe: float = -999.0

    print("=" * 70)
    print("GRID AUTORESEARCH ENGINE")
    print(f"Layer: {layer} | Max iterations: {max_iterations}")
    print(f"Backtest: {backtest_start} to {backtest_end} | Splits: {n_splits}")
    print("=" * 70)

    for iteration in range(1, max_iterations + 1):
        print(f"\n{'─' * 70}")
        print(f"ITERATION {iteration}/{max_iterations}")
        print(f"{'─' * 70}")

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

        print("\n[1/4] Generating hypothesis via Ollama...")
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ]

        response = ollama.chat(
            messages,
            temperature=0.6,
            num_predict=2000,
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

        print(f"  Statement: {hyp['statement']}")
        print(f"  Features:  {[feature_names.get(f, f) for f in hyp['feature_ids']]}")
        print(f"  Lags:      {hyp['lag_structure']}")
        print(f"  Threshold: Sharpe >= {hyp['proposed_threshold']}")

        # ── Step 2: Register hypothesis ───────────────────────────────
        print("\n[2/4] Registering hypothesis...")
        try:
            cur.execute(
                "INSERT INTO hypothesis_registry "
                "(statement, layer, feature_ids, lag_structure, proposed_metric, proposed_threshold, state) "
                "VALUES (%s, %s, %s, %s, %s, %s, 'TESTING') RETURNING id",
                (
                    hyp["statement"],
                    hyp["layer"],
                    hyp["feature_ids"],
                    json.dumps(hyp["lag_structure"]),
                    hyp["proposed_metric"],
                    hyp["proposed_threshold"],
                ),
            )
            hyp_id = cur.fetchone()[0]
            print(f"  Registered as hypothesis_id={hyp_id}")
        except Exception as exc:
            log.error("Failed to register hypothesis: {e}", e=str(exc))
            attempts.append({
                "iteration": iteration,
                "statement": hyp["statement"],
                "error": f"DB insert failed: {exc}",
            })
            continue

        # ── Step 3: Run walk-forward backtest ─────────────────────────
        print("\n[3/4] Running walk-forward backtest...")
        try:
            result = backtester.run_validation(
                hypothesis_id=hyp_id,
                feature_ids=hyp["feature_ids"],
                start_date=backtest_start,
                end_date=backtest_end,
                n_splits=n_splits,
                cost_bps=cost_bps,
            )
        except Exception as exc:
            log.error("Backtest failed: {e}", e=str(exc))
            cur.execute(
                "UPDATE hypothesis_registry SET state='FAILED', kill_reason=%s WHERE id=%s",
                (f"Backtest error: {exc}", hyp_id),
            )
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

        print(f"  Verdict:        {verdict}")
        print(f"  Sharpe:         {sharpe}")
        print(f"  Baseline:       {baseline_sharpe}")
        print(f"  Return:         {full_metrics.get('return', '?')}")
        print(f"  Max drawdown:   {full_metrics.get('max_drawdown', '?')}")
        print(f"  Era summary:    {format_era_summary(era_results)}")

        # ── Step 4: Update hypothesis state ───────────────────────────
        print("\n[4/4] Updating hypothesis state...")
        new_state = "PASSED" if verdict == "PASS" else "FAILED"
        kill_reason = None if verdict == "PASS" else f"Verdict={verdict}, Sharpe={sharpe}"

        cur.execute(
            "UPDATE hypothesis_registry SET state=%s, kill_reason=%s, updated_at=NOW() WHERE id=%s",
            (new_state, kill_reason, hyp_id),
        )

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
            print(f"\n*** HYPOTHESIS PASSED at iteration {iteration} ***")
            print(f"    {hyp['statement']}")
            print(f"    Sharpe={sharpe} (baseline={baseline_sharpe})")

            # Send email notification
            try:
                from scripts.notify import notify_on_pass
                notify_on_pass(attempt)
            except Exception as exc:
                log.debug("Email notification skipped: {e}", e=str(exc))

            break

        print(f"\n  Hypothesis FAILED — refining for next iteration...")

    # ── Summary ───────────────────────────────────────────────────────
    pg.close()

    print("\n" + "=" * 70)
    print("AUTORESEARCH COMPLETE")
    print(f"Iterations run: {len(attempts)}")
    if best_result:
        print(f"\nBest hypothesis (Sharpe={best_sharpe}):")
        print(f"  {best_result['statement']}")
        print(f"  Verdict: {best_result['verdict']}")
        print(f"  Features: {[feature_names.get(f, f) for f in best_result.get('feature_ids', [])]}")
    else:
        print("No valid hypotheses were generated.")
    print("=" * 70)

    return {
        "iterations_run": len(attempts),
        "best_result": best_result,
        "best_sharpe": best_sharpe,
        "all_attempts": attempts,
        "passed": any(a.get("verdict") == "PASS" for a in attempts),
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
    print(f"\nFull results saved to {out_path}")


if __name__ == "__main__":
    main()
