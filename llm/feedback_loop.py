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
import functools
import time
import re
from contextvars import ContextVar
from typing import Any, Callable

log = logging.getLogger("grid.llm.feedback_loop")


# -----------------------------------------------------------------
# Langfuse fan-out — every llm call also emits a trace+generation to
# the self-hosted Langfuse at LANGFUSE_HOST. Lazy singleton; silent
# no-op when keys are missing; never propagates exceptions.
# -----------------------------------------------------------------
_LANGFUSE_CLIENT: Any = None
_LANGFUSE_INIT_TRIED: bool = False
_ACTIVE_OBSERVATION: ContextVar[Any] = ContextVar("grid_llm_observation", default=None)
_ACTIVE_METADATA: ContextVar[dict[str, Any] | None] = ContextVar("grid_llm_metadata", default=None)

# Exact public identifiers from the current router/client/endpoint registry.
# Dynamic values are intentionally unknown, not guessed or substring-redacted.
_TRACE_MODULES = frozenset({
    "llamacpp", "llamacpp.embed", "embedding_router", "openai_compatible",
    "gemma", "gemma.vision", "gemma_micro", "ollama", "ollama.embed",
    "openai_legacy", "openai", "huggingface", "gemini", "anthropic",
    "gemma_micro.signal_classifier", "gemma_micro.anomaly_narrator",
    "gemma_micro.edgar_extractor", "gemma_micro.knowledge_mapper", "unknown",
})
_TRACE_PROVIDERS = frozenset({
    "llamacpp", "gemma", "gemma_micro", "ollama", "openai",
    "openai-compatible", "openrouter", "huggingface", "gemini", "anthropic",
    "router", "unknown",
})
_TRACE_MODELS = frozenset({
    "hermes", "llama3.1:8b", "nomic-embed-text", "gpt-4o-mini",
    "text-embedding-3-small", "gemma-4-31B-it-Q4_K_M", "gemini-2.5-flash",
    "claude-sonnet-4-6", "Qwen/Qwen2.5-72B-Instruct", "qwen2.5:32b",
    "gemma2:9b", "qwen2.5:7b-instruct-q4_K_M", "qwen3:8b", "qwen3.8-27b",
    "gemma-4-e4b-signal-classifier", "gemma-4-e4b-anomaly-narrator",
    "gemma-4-e4b-edgar-extractor", "gemma-4-e4b-knowledge-mapper", "unknown",
})


def _trace_identifier(value: Any, allowed: frozenset[str]) -> str:
    return value if type(value) is str and value in allowed else "unknown"


def _trace_integer(value: Any, maximum: int) -> int | None:
    return value if type(value) is int and 0 <= value <= maximum else None


def _trace_metadata(values: dict[str, Any]) -> dict[str, Any]:
    """Closed scalar allowlist; never forward arbitrary call-site metadata."""
    result: dict[str, Any] = {
        "module": _trace_identifier(values.get("module"), _TRACE_MODULES),
        "provider": _trace_identifier(values.get("provider"), _TRACE_PROVIDERS),
        "tier": _trace_identifier(values.get("tier"), frozenset({
            "LOCAL", "REASON", "ORACLE", "local", "reason", "oracle", "batch", "micro", "unknown",
        })),
        "operation": _trace_identifier(values.get("operation"), frozenset({"chat", "embed", "unknown"})),
        "call_status": _trace_identifier(values.get("call_status"), frozenset({
            "returned", "unavailable", "exception", "unknown",
        })),
    }
    for key in ("latency_ms", "elapsed_ms"):
        value = _trace_integer(values.get(key), 86_400_000)
        if value is not None:
            result[key] = value
    # The internal caller generates this ID; caller-supplied metadata cannot
    # replace it. Reject noncanonical shapes as an additional format check.
    call_id = values.get("call_id")
    if type(call_id) is str and re.fullmatch(r"[0-9a-f]{16}", call_id):
        result["call_id"] = call_id
    provenance = values.get("usage_provenance")
    if type(provenance) is dict:
        result["usage_provenance"] = {
            key: _trace_identifier(provenance.get(key), frozenset({"reported", "estimated", "unknown"}))
            for key in ("input", "output")
        }
    return result


def trace_llm_call(*, module: str, provider: str, operation: str = "chat",
                   provider_attribute: str = "") -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Time a provider call without capturing new prompts, vectors or errors.

    Success feedback updates this observation instead of emitting a second
    generation. Unknown usage, cost and prediction outcome remain absent.
    Telemetry failures must never change the wrapped call's behavior.
    """
    def decorate(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            observation = None
            observation_metadata: dict[str, Any] = {}
            token = None
            started = time.monotonic()
            result = None
            raised = False
            try:
                actual_provider = (
                    getattr(args[0], provider_attribute, provider)
                    if args and provider_attribute else provider
                )
                observation_metadata = _trace_metadata({
                    "module": module, "provider": actual_provider, "operation": operation,
                })
                client = _get_langfuse()
                if client is not None:
                    observation = client.start_observation(
                        name=observation_metadata["module"],
                        as_type="embedding" if operation == "embed" else "generation",
                        metadata=dict(observation_metadata),
                    )
            except Exception:
                pass
            # Set even when unavailable: nested calls cannot update a parent
            # observation if their own telemetry initialization failed.
            token = _ACTIVE_OBSERVATION.set(observation)
            metadata_token = _ACTIVE_METADATA.set(observation_metadata if observation is not None else None)
            try:
                result = func(*args, **kwargs)
                return result
            except BaseException:
                raised = True
                raise
            finally:
                _ACTIVE_OBSERVATION.reset(token)
                _ACTIVE_METADATA.reset(metadata_token)
                if observation is not None:
                    status = "exception" if raised else "unavailable" if result is None else "returned"
                    try:
                        observation.update(
                            level="ERROR" if raised else "WARNING" if result is None else "DEFAULT",
                            status_message=status,
                            metadata=_trace_metadata({**observation_metadata,
                                      "operation": operation, "call_status": status,
                                      "elapsed_ms": int((time.monotonic() - started) * 1000)}),
                        )
                    except Exception:
                        pass
                    try:
                        observation.end()
                    except Exception:
                        pass
        return wrapped
    return decorate


def _get_langfuse():
    global _LANGFUSE_CLIENT, _LANGFUSE_INIT_TRIED
    if _LANGFUSE_INIT_TRIED:
        return _LANGFUSE_CLIENT
    _LANGFUSE_INIT_TRIED = True

    pk = os.getenv("LANGFUSE_PUBLIC_KEY")
    sk = os.getenv("LANGFUSE_SECRET_KEY")
    if not pk or not sk:
        return None
    try:
        from langfuse import Langfuse
        _LANGFUSE_CLIENT = Langfuse(
            public_key=pk,
            secret_key=sk,
            host=os.getenv("LANGFUSE_HOST", "http://grid-svr:3000"),
        )
        return _LANGFUSE_CLIENT
    except Exception as e:
        log.debug(f"Langfuse init failed: {e}")
        return None


def _emit_to_langfuse(
    *,
    call_id: str,
    module: str,
    tier: str,
    model: str,
    provider: str,
    system_prompt: str,
    user_prompt: str,
    output: str,
    context_tokens: int,
    output_tokens: int,
    latency_ms: int,
    metadata: dict,
) -> None:
    """Best-effort fan-out to Langfuse. Silently swallows all errors."""
    try:
        # Calls wrapped by trace_llm_call already began before inference.
        # Older callers retain their existing post-call fan-out.
        obs = _ACTIVE_OBSERVATION.get()
        active = obs is not None
        client = _get_langfuse() if not active else None
        if not active and client is None:
            return
        active_metadata = _ACTIVE_METADATA.get()
        fields = dict(
            # Langfuse is metadata-only, including successful calls. The
            # training DB retains its existing separate payload contract.
            model=_trace_identifier(model, _TRACE_MODELS),
            usage_details={key: value for key, value in
                           (("input", context_tokens), ("output", output_tokens))
                           if _trace_integer(value, 1_000_000_000) is not None and value > 0},
            metadata=_trace_metadata({
                "tier": tier,
                "provider": provider,
                "module": module,
                "operation": active_metadata.get("operation") if active_metadata is not None else "unknown",
                "latency_ms": latency_ms,
                "call_id": call_id,
                "usage_provenance": metadata.get("usage_provenance") if type(metadata) is dict else None,
            }),
        )
        if active:
            # SDK metadata updates may replace the complete map. Keep a
            # context-local copy so final status cannot erase success tags.
            if active_metadata is not None:
                active_metadata.update(fields["metadata"])
                fields["metadata"] = dict(active_metadata)
            obs.update(**fields)
        else:
            obs = client.start_observation(name=_trace_identifier(module, _TRACE_MODULES),
                                           as_type="generation", **fields)
            obs.end()
    except Exception as e:
        log.debug(f"Langfuse emit failed: {e}")


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

    # Preserve the distinction between provider counts and text estimates.
    metadata = dict(metadata or {})
    metadata["usage_provenance"] = {
        "input": "reported" if context_tokens else "estimated" if system_prompt or user_prompt else "unknown",
        "output": "reported" if output_tokens else "estimated" if output else "unknown",
    }

    # Estimate tokens if not provided (existing training-store behavior).
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

    _emit_to_langfuse(
        call_id=call_id,
        module=module,
        tier=tier,
        model=model,
        provider=provider,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        output=output,
        context_tokens=context_tokens,
        output_tokens=output_tokens,
        latency_ms=latency_ms,
        metadata=metadata or {},
    )

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
