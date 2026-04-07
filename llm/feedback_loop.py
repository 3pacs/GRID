"""
GRID LLM Feedback Loop — Self-Learning Infrastructure

Logs every LLM call with input context, output, and metadata.
Later, outcomes are attached (was the prediction correct? did the postmortem
find the right root cause? did the signal hit?).

This creates a training dataset for fine-tuning Gemma and lets us:
1. Find the optimal context window (how much data before hallucinations spike)
2. Build preference pairs (good outputs vs bad outputs) for DPO/RLHF
3. Track which prompt versions produce better outcomes
4. Measure hallucination rate by context length

Usage:
    from llm.feedback_loop import log_llm_call, score_llm_output, get_training_pairs

    # Log every call
    call_id = log_llm_call(
        module="postmortem",
        tier="REASON",
        system_prompt=system,
        user_prompt=user,
        context_tokens=len(context) // 4,
        output=response,
        model="gemma-4-31b",
    )

    # Later, when outcome is known
    score_llm_output(call_id, score=0.8, outcome="prediction_correct")

    # Export training pairs for fine-tuning
    pairs = get_training_pairs(module="postmortem", min_score=0.7)
"""

from __future__ import annotations

import os
import json
import hashlib
import logging
import datetime
from typing import Optional, Any

log = logging.getLogger("grid.llm.feedback_loop")


def _get_conn():
    """Get a psycopg2 connection to griddb."""
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "griddb"),
        user=os.getenv("DB_USER", "grid"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def ensure_tables():
    """Create the llm_calls and llm_scores tables if they don't exist."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS llm_calls (
            id              TEXT PRIMARY KEY,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            module          TEXT NOT NULL,
            tier            TEXT NOT NULL,
            model           TEXT,
            provider        TEXT,
            system_prompt   TEXT,
            user_prompt     TEXT,
            context_tokens  INTEGER,
            output          TEXT,
            output_tokens   INTEGER,
            latency_ms      INTEGER,
            prompt_version  TEXT,
            metadata        JSONB DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_llm_calls_module ON llm_calls(module);
        CREATE INDEX IF NOT EXISTS idx_llm_calls_created ON llm_calls(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_llm_calls_tokens ON llm_calls(context_tokens);

        CREATE TABLE IF NOT EXISTS llm_scores (
            id              SERIAL PRIMARY KEY,
            call_id         TEXT REFERENCES llm_calls(id),
            scored_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            score           NUMERIC(5,4),
            outcome         TEXT,
            hallucination   BOOLEAN DEFAULT FALSE,
            notes           TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_llm_scores_call ON llm_scores(call_id);
        CREATE INDEX IF NOT EXISTS idx_llm_scores_score ON llm_scores(score DESC);

        CREATE TABLE IF NOT EXISTS llm_context_experiments (
            id              SERIAL PRIMARY KEY,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            module          TEXT NOT NULL,
            model           TEXT NOT NULL,
            context_tokens  INTEGER NOT NULL,
            total_calls     INTEGER DEFAULT 0,
            avg_score       NUMERIC(5,4),
            hallucination_rate NUMERIC(5,4),
            notes           TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_llm_ctx_module ON llm_context_experiments(module, model);
    """)
    conn.commit()
    cur.close()
    conn.close()


def log_llm_call(
    module: str,
    tier: str,
    system_prompt: str,
    user_prompt: str,
    output: str,
    context_tokens: int = 0,
    output_tokens: int = 0,
    latency_ms: int = 0,
    model: str = "",
    provider: str = "",
    prompt_version: str = "",
    metadata: dict = None,
) -> str:
    """
    Log an LLM call. Returns the call_id for later scoring.

    The call_id is a hash of module + prompt + timestamp for dedup.
    """
    ts = datetime.datetime.utcnow().isoformat()
    raw = f"{module}:{system_prompt[:100]}:{user_prompt[:100]}:{ts}"
    call_id = hashlib.sha256(raw.encode()).hexdigest()[:16]

    # Estimate tokens if not provided
    if not context_tokens:
        context_tokens = (len(system_prompt) + len(user_prompt)) // 4

    if not output_tokens and output:
        output_tokens = len(output) // 4

    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO llm_calls (
                id, module, tier, model, provider,
                system_prompt, user_prompt, context_tokens,
                output, output_tokens, latency_ms,
                prompt_version, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (
            call_id, module, tier, model, provider,
            system_prompt, user_prompt, context_tokens,
            output, output_tokens, latency_ms,
            prompt_version, json.dumps(metadata or {}),
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        log.debug(f"Failed to log LLM call: {e}")

    return call_id


def score_llm_output(
    call_id: str,
    score: float,
    outcome: str = "",
    hallucination: bool = False,
    notes: str = "",
) -> None:
    """
    Attach an outcome score to a previous LLM call.

    score: 0.0 (terrible) to 1.0 (perfect)
    outcome: free text (e.g., "prediction_correct", "root_cause_found", "hallucinated_ticker")
    hallucination: True if the output contained fabricated data
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO llm_scores (call_id, score, outcome, hallucination, notes)
            VALUES (%s, %s, %s, %s, %s)
        """, (call_id, score, outcome, hallucination, notes))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        log.debug(f"Failed to score LLM call: {e}")


def get_training_pairs(
    module: str = None,
    min_score: float = 0.7,
    max_hallucination_rate: float = 0.1,
    limit: int = 1000,
) -> list[dict]:
    """
    Export scored LLM calls as training pairs for fine-tuning.

    Returns list of {system, user, output, score, context_tokens, hallucination}
    filtered to high-quality outputs.
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()

        sql = """
            SELECT c.system_prompt, c.user_prompt, c.output,
                   s.score, c.context_tokens, s.hallucination,
                   c.module, c.model, c.prompt_version
            FROM llm_calls c
            JOIN llm_scores s ON s.call_id = c.id
            WHERE s.score >= %s
              AND (s.hallucination = FALSE OR s.hallucination IS NULL)
        """
        params = [min_score]

        if module:
            sql += " AND c.module = %s"
            params.append(module)

        sql += " ORDER BY s.score DESC LIMIT %s"
        params.append(limit)

        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return [
            {
                "system": r[0],
                "user": r[1],
                "output": r[2],
                "score": float(r[3]),
                "context_tokens": r[4],
                "hallucination": r[5],
                "module": r[6],
                "model": r[7],
                "prompt_version": r[8],
            }
            for r in rows
        ]
    except Exception as e:
        log.debug(f"Failed to get training pairs: {e}")
        return []


def get_context_sweet_spot(module: str = None, model: str = None) -> dict:
    """
    Analyze hallucination rate by context token count to find the sweet spot.

    Returns {optimal_tokens, hallucination_by_bucket, score_by_bucket}
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()

        sql = """
            SELECT
                CASE
                    WHEN c.context_tokens < 500 THEN '0-500'
                    WHEN c.context_tokens < 1000 THEN '500-1K'
                    WHEN c.context_tokens < 2000 THEN '1K-2K'
                    WHEN c.context_tokens < 4000 THEN '2K-4K'
                    WHEN c.context_tokens < 8000 THEN '4K-8K'
                    ELSE '8K+'
                END AS bucket,
                COUNT(*) AS total,
                AVG(s.score) AS avg_score,
                AVG(CASE WHEN s.hallucination THEN 1.0 ELSE 0.0 END) AS hallucination_rate
            FROM llm_calls c
            JOIN llm_scores s ON s.call_id = c.id
        """
        params = []
        wheres = []

        if module:
            wheres.append("c.module = %s")
            params.append(module)
        if model:
            wheres.append("c.model = %s")
            params.append(model)

        if wheres:
            sql += " WHERE " + " AND ".join(wheres)

        sql += """
            GROUP BY bucket
            ORDER BY MIN(c.context_tokens)
        """

        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        buckets = {}
        best_bucket = None
        best_score = -1

        for r in rows:
            bucket_name = r[0]
            total = r[1]
            avg_score = float(r[2]) if r[2] else 0
            hall_rate = float(r[3]) if r[3] else 0

            buckets[bucket_name] = {
                "total": total,
                "avg_score": round(avg_score, 4),
                "hallucination_rate": round(hall_rate, 4),
            }

            # Sweet spot = highest score with hallucination rate < 10%
            if hall_rate < 0.10 and avg_score > best_score:
                best_score = avg_score
                best_bucket = bucket_name

        return {
            "optimal_bucket": best_bucket,
            "buckets": buckets,
        }
    except Exception as e:
        log.debug(f"Failed to analyze context sweet spot: {e}")
        return {"optimal_bucket": None, "buckets": {}}


def export_finetune_jsonl(
    output_path: str,
    module: str = None,
    min_score: float = 0.7,
    format: str = "chatml",
) -> int:
    """
    Export training data as JSONL for Gemma fine-tuning.

    Formats:
    - chatml: {"messages": [{"role": "system", ...}, {"role": "user", ...}, {"role": "assistant", ...}]}
    - alpaca: {"instruction": ..., "input": ..., "output": ...}
    """
    pairs = get_training_pairs(module=module, min_score=min_score, limit=10000)

    with open(output_path, "w") as f:
        for p in pairs:
            if format == "chatml":
                row = {
                    "messages": [
                        {"role": "system", "content": p["system"]},
                        {"role": "user", "content": p["user"]},
                        {"role": "assistant", "content": p["output"]},
                    ]
                }
            else:  # alpaca
                row = {
                    "instruction": p["system"],
                    "input": p["user"],
                    "output": p["output"],
                }
            f.write(json.dumps(row) + "\n")

    log.info(f"Exported {len(pairs)} training pairs to {output_path}")
    return len(pairs)
