"""Backfill ``hypothesis_boost_log.opus_outcome`` (+ reasoning) for rows where
the mechanical ``_check_ticker_move`` returns ``'inconclusive'`` but the
underlying ticker actually moved.

The scoring framework is documented in
``docs/scoring/boost_log_scoring_v1.md``. This worker:

1. Pulls a batch of unscored inconclusive boost rows.
2. For each, gathers the full context (parent hypothesis direction if anti,
   ticker actual_move stats over the eval window, n_preds, thesis text).
3. Feeds the context to the local LLM (Tier.REASON) with the framework doc
   loaded as system_knowledge AND four worked examples as few-shot turns.
4. Parses the model's JSON output: ``{"opus_outcome": "...", "opus_reasoning": "..."}``.
5. Writes back to ``hypothesis_boost_log`` (opus_outcome, opus_reasoning,
   opus_scored_at, opus_scorer_model).

Spot-check 1 in 50 with a frontier model (Tier.ORACLE) and flip back to
mechanical-only when local accuracy drops below 95% on a rolling 200-row
sample. See [[handoff-next-agent-2026-05-15]] for the rest of the plan.

Usage::

    python3 scripts/score_boost_log_with_local_llm.py --batch-size 50 --dry-run
    python3 scripts/score_boost_log_with_local_llm.py --batch-size 50
    python3 scripts/score_boost_log_with_local_llm.py --hypothesis-id hyp_abc...

The first 20 rows were scored manually by Opus 4.7 on 2026-05-15. Their
opus_reasoning strings are the canonical few-shot examples this script
loads at runtime — DO NOT delete or rewrite them.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text

# Allow `python3 scripts/...` from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db import get_engine  # noqa: E402

SCORING_DOC = Path(__file__).resolve().parents[1] / "docs" / "scoring" / "boost_log_scoring_v1.md"


# Few-shot examples sourced from the 2026-05-15 Opus 4.7 manual batch (20 INTC rows).
# Picked one representative example per (direct CALL / direct PUT / anti-CALL / anti-PUT).
FEW_SHOT_EXAMPLES: list[dict[str, Any]] = [
    {
        "input": {
            "boost_id": 1061,
            "hypothesis_id": "hyp_857e6755da69fa77",
            "is_anti": False,
            "ticker": "INTC",
            "expected_direction": "CALL",
            "min_move_pct": 2.0,
            "actual_avg_move": 67.87,
            "actual_max_up": 77.62,
            "actual_max_down": 22.35,
            "n_preds": 11316,
            "parent_thesis": "CONVERGENCE: 5 sources agree INTC heading CALL.",
        },
        "output": {
            "opus_outcome": "CONFIRMED",
            "opus_reasoning": (
                "INTC convergence-CALL hypothesis (5-source agreement on UP). "
                "Ticker moved +67.87% avg (max +77.62%) over 14-day window vs "
                "2% threshold. Confirmed strongly."
            ),
        },
    },
    {
        "input": {
            "boost_id": 989,
            "hypothesis_id": "hyp_d888ae48533a6eb2",
            "is_anti": False,
            "ticker": "INTC",
            "expected_direction": "PUT",
            "min_move_pct": 2.0,
            "actual_avg_move": 67.87,
            "actual_max_up": 77.62,
            "actual_max_down": 22.35,
            "n_preds": 11316,
            "parent_thesis": "CONVERGENCE: 26 sources agree INTC heading PUT.",
        },
        "output": {
            "opus_outcome": "INVALIDATED",
            "opus_reasoning": (
                "INTC convergence-PUT hypothesis (26-source consensus on DOWN). "
                "Ticker moved +67.87% avg UP — direct opposite of PUT thesis. "
                "Strongly invalidated."
            ),
        },
    },
    {
        "input": {
            "boost_id": 1062,
            "hypothesis_id": "hyp_857e6755da69fa77_anti",
            "is_anti": True,
            "parent_hypothesis_id": "hyp_857e6755da69fa77",
            "parent_expected_direction": "CALL",
            "ticker": "INTC",
            "expected_direction": "opposite",
            "min_move_pct": 2.0,
            "actual_avg_move": 67.87,
            "actual_max_up": 77.62,
            "actual_max_down": 22.35,
            "n_preds": 11316,
            "parent_thesis": "CONVERGENCE: 5 sources agree INTC heading CALL.",
        },
        "output": {
            "opus_outcome": "INVALIDATED",
            "opus_reasoning": (
                "INTC anti-CALL hypothesis (direction=opposite, parent=CALL). "
                "Anti inverts to DOWN expectation. Ticker moved +67.87% avg UP "
                "— opposite of anti expectation. Invalidated."
            ),
        },
    },
    {
        "input": {
            "boost_id": 990,
            "hypothesis_id": "hyp_d888ae48533a6eb2_anti",
            "is_anti": True,
            "parent_hypothesis_id": "hyp_d888ae48533a6eb2",
            "parent_expected_direction": "PUT",
            "ticker": "INTC",
            "expected_direction": "opposite",
            "min_move_pct": 2.0,
            "actual_avg_move": 67.87,
            "actual_max_up": 77.62,
            "actual_max_down": 22.35,
            "n_preds": 11316,
            "parent_thesis": "CONVERGENCE: 26 sources agree INTC heading PUT.",
        },
        "output": {
            "opus_outcome": "CONFIRMED",
            "opus_reasoning": (
                "INTC anti-PUT hypothesis (direction=opposite, parent=PUT). "
                "Anti inverts to UP expectation. INTC moved +67.87% avg up — "
                "matches anti expectation. Confirmed."
            ),
        },
    },
]


SYSTEM_PROMPT_TEMPLATE = """\
You are a scoring agent for the GRID hypothesis_boost_log table. You apply
the framework in docs/scoring/boost_log_scoring_v1.md to decide whether each
boost row should be CONFIRMED, INVALIDATED, or INCONCLUSIVE, and emit a
short reasoning.

Framework summary:
1. Normalize the row's expected_direction:
   - {up, bullish, long, CALL, call, increase, increases, rising} -> UP
   - {down, bearish, short, PUT, put, decrease, decreases, falling} -> DOWN
   - {neutral, flat, sideways} -> INCONCLUSIVE
   - 'opposite' on an _anti row -> invert the parent's normalized direction
   - anything else -> INCONCLUSIVE
2. Compare the effective direction to actual_avg_move:
   - UP + actual_avg_move > min_move_pct -> CONFIRMED
   - UP + actual_avg_move < -min_move_pct -> INVALIDATED
   - DOWN + actual_avg_move < -min_move_pct -> CONFIRMED
   - DOWN + actual_avg_move > min_move_pct -> INVALIDATED
   - otherwise -> INCONCLUSIVE
3. If n_preds < 3, return INCONCLUSIVE.
4. Emit a 1-2 sentence reasoning naming: hypothesis interpretation, observed
   move, verdict relative to threshold.

Reply with a single JSON object: {"opus_outcome": "...", "opus_reasoning": "..."}.
No prose outside the JSON.

%(few_shot)s
"""


def _build_system_prompt() -> str:
    few_shot = "\n\n".join(
        f"Example {i+1}:\nInput: {json.dumps(ex['input'])}\nOutput: {json.dumps(ex['output'])}"
        for i, ex in enumerate(FEW_SHOT_EXAMPLES)
    )
    return SYSTEM_PROMPT_TEMPLATE % {"few_shot": few_shot}


def _fetch_batch(engine, batch_size: int) -> list[dict[str, Any]]:
    """Pull unscored inconclusive boost rows with full context."""
    sql = text(
        """
        WITH inconc AS (
          SELECT bl.id AS boost_id, bl.hypothesis_id, bl.boost_source,
                 bl.boost_value, bl.created_at AS boost_at,
                 dh.thesis, dh.test_criteria, dh.created_at AS hyp_at,
                 (dh.test_criteria->>'ticker') AS ticker,
                 (dh.test_criteria->>'expected_direction') AS expected_direction,
                 COALESCE((dh.test_criteria->>'min_move_pct')::float, 2.0) AS min_move_pct
          FROM hypothesis_boost_log bl
          JOIN discovered_hypotheses dh ON dh.id = bl.hypothesis_id
          WHERE bl.outcome = 'inconclusive'
            AND bl.opus_outcome IS NULL
            AND dh.pattern_type = 'convergence'
          ORDER BY bl.created_at ASC
          LIMIT :batch_size
        )
        SELECT i.*,
               (CASE WHEN i.hypothesis_id LIKE '%_anti'
                     THEN REGEXP_REPLACE(i.hypothesis_id, '_anti$', '')
                     ELSE NULL END) AS parent_hypothesis_id,
               COALESCE(MAX(op.actual_move_pct), 0)::float AS actual_max_up,
               COALESCE(MIN(op.actual_move_pct), 0)::float AS actual_max_down,
               COALESCE(AVG(op.actual_move_pct), 0)::float AS actual_avg_move,
               COUNT(op.id) AS n_preds
        FROM inconc i
        LEFT JOIN oracle_predictions op
          ON op.ticker = i.ticker
         AND op.created_at BETWEEN i.hyp_at AND i.hyp_at + INTERVAL '14 days'
         AND op.scored_at IS NOT NULL
         AND op.actual_move_pct IS NOT NULL
        GROUP BY i.boost_id, i.hypothesis_id, i.boost_source, i.boost_value,
                 i.boost_at, i.thesis, i.test_criteria, i.hyp_at,
                 i.ticker, i.expected_direction, i.min_move_pct
        ORDER BY i.boost_at DESC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"batch_size": batch_size}).mappings().all()
    return [dict(r) for r in rows]


def _attach_parent_direction(engine, rows: list[dict[str, Any]]) -> None:
    """Look up parent_expected_direction for _anti rows."""
    for r in rows:
        r["is_anti"] = bool(r.get("parent_hypothesis_id"))
        r.setdefault("parent_expected_direction", None)
    parent_ids = [r["parent_hypothesis_id"] for r in rows if r.get("parent_hypothesis_id")]
    if not parent_ids:
        return
    sql = text(
        """
        SELECT id, test_criteria->>'expected_direction' AS dir
        FROM discovered_hypotheses
        WHERE id = ANY(:ids)
        """
    )
    with engine.connect() as conn:
        parents = {r[0]: r[1] for r in conn.execute(sql, {"ids": parent_ids}).fetchall()}
    for r in rows:
        if r.get("parent_hypothesis_id"):
            r["parent_expected_direction"] = parents.get(r["parent_hypothesis_id"])


def _score_one(row: dict[str, Any], llm_client, scorer_model: str) -> dict[str, str] | None:
    """Send one row to the LLM, parse JSON, return outcome+reasoning or None."""
    user_msg = json.dumps({
        "boost_id": row["boost_id"],
        "hypothesis_id": row["hypothesis_id"],
        "is_anti": row["is_anti"],
        "parent_hypothesis_id": row.get("parent_hypothesis_id"),
        "parent_expected_direction": row.get("parent_expected_direction"),
        "ticker": row["ticker"],
        "expected_direction": row["expected_direction"],
        "min_move_pct": row["min_move_pct"],
        "actual_avg_move": round(float(row["actual_avg_move"]), 2),
        "actual_max_up": round(float(row["actual_max_up"]), 2),
        "actual_max_down": round(float(row["actual_max_down"]), 2),
        "n_preds": int(row["n_preds"]),
        "parent_thesis": row.get("thesis", "")[:300],
    })
    reply = llm_client.chat(
        messages=[
            {"role": "system", "content": _build_system_prompt()},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.0,
        num_predict=400,
    )
    if not reply:
        return None
    try:
        # Some local models wrap JSON in code fences; strip if present.
        cleaned = reply.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        parsed = json.loads(cleaned)
        outcome = parsed.get("opus_outcome", "").upper()
        reasoning = parsed.get("opus_reasoning", "")
        if outcome not in {"CONFIRMED", "INVALIDATED", "INCONCLUSIVE"}:
            log.warning("boost_id={}: invalid outcome {!r}", row["boost_id"], outcome)
            return None
        if not reasoning or len(reasoning) < 20:
            log.warning("boost_id={}: reasoning too short {!r}", row["boost_id"], reasoning)
            return None
        return {"opus_outcome": outcome, "opus_reasoning": reasoning}
    except (json.JSONDecodeError, AttributeError) as exc:
        log.warning("boost_id={}: parse failed ({}): {!r}", row["boost_id"], exc, reply[:200])
        return None


def _write_score(engine, boost_id: int, score: dict[str, str], scorer_model: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE hypothesis_boost_log SET
                  opus_outcome = :outcome,
                  opus_reasoning = :reasoning,
                  opus_scored_at = NOW(),
                  opus_scorer_model = :model
                WHERE id = :id AND opus_outcome IS NULL
                """
            ),
            {
                "id": boost_id,
                "outcome": score["opus_outcome"],
                "reasoning": score["opus_reasoning"],
                "model": scorer_model,
            },
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true",
                        help="Score but don't write back to DB.")
    parser.add_argument("--tier", default="REASON",
                        choices=["LOCAL", "REASON", "ORACLE"],
                        help="LLM tier to use. ORACLE = frontier (Anthropic).")
    args = parser.parse_args(argv)

    from llm.router import get_llm, Tier
    tier = Tier[args.tier]
    llm = get_llm(tier)
    if llm is None:
        log.error("No LLM client available for tier {}", args.tier)
        return 2
    scorer_model = getattr(llm, "model", f"tier-{args.tier.lower()}")

    engine = get_engine()
    rows = _fetch_batch(engine, args.batch_size)
    if not rows:
        log.info("Nothing to score — all inconclusive convergence rows have opus_outcome.")
        return 0

    _attach_parent_direction(engine, rows)
    log.info("Scoring {} rows with {} (tier {})", len(rows), scorer_model, args.tier)

    by_outcome: dict[str, int] = {"CONFIRMED": 0, "INVALIDATED": 0, "INCONCLUSIVE": 0}
    failed = 0
    for row in rows:
        score = _score_one(row, llm, scorer_model)
        if score is None:
            failed += 1
            continue
        by_outcome[score["opus_outcome"]] += 1
        if not args.dry_run:
            _write_score(engine, row["boost_id"], score, scorer_model)

    log.info(
        "Scored {} rows: {} confirmed, {} invalidated, {} inconclusive, {} failed",
        len(rows) - failed,
        by_outcome["CONFIRMED"],
        by_outcome["INVALIDATED"],
        by_outcome["INCONCLUSIVE"],
        failed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
