"""
GRID — Record prompt feedback (features available vs cited) for utility scoring.

Fire-and-forget: errors are logged but never block the response.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


def record_prompt_feedback(
    db_engine: Engine,
    source: str,
    features_available: list[str],
    features_cited: list[str],
    prediction_id: str | None = None,
    ticker: str | None = None,
    model_name: str | None = None,
    llm_model: str | None = None,
    prompt_token_count: int | None = None,
    response_length: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> int | None:
    """Insert a prompt_feedback row. Returns the row ID, or None on failure.

    This is fire-and-forget — failures are logged but never raised.
    """
    if not features_available:
        return None

    citation_ratio = len(features_cited) / len(features_available) if features_available else 0.0

    try:
        with db_engine.begin() as conn:
            row = conn.execute(
                text("""
                    INSERT INTO prompt_feedback
                        (source, prediction_id, ticker, model_name,
                         features_available, features_cited, citation_ratio,
                         response_length, llm_model, prompt_token_count, metadata)
                    VALUES
                        (:source, :pred_id, :ticker, :model,
                         :avail, :cited, :ratio,
                         :resp_len, :llm, :tok_count, CAST(:meta AS jsonb))
                    RETURNING id
                """),
                {
                    "source": source,
                    "pred_id": prediction_id,
                    "ticker": ticker,
                    "model": model_name,
                    "avail": features_available,
                    "cited": features_cited,
                    "ratio": round(citation_ratio, 4),
                    "resp_len": response_length,
                    "llm": llm_model,
                    "tok_count": prompt_token_count,
                    "meta": json.dumps(metadata) if metadata else None,
                },
            ).fetchone()

            row_id = row[0] if row else None
            log.debug(
                "prompt_feedback #{id}: {src} — {c}/{a} cited ({pct:.0f}%)",
                id=row_id, src=source,
                c=len(features_cited), a=len(features_available),
                pct=citation_ratio * 100,
            )
            return row_id

    except Exception as e:
        log.warning("Failed to record prompt_feedback: {e}", e=str(e))
        return None
