#!/usr/bin/env python3
"""Drain Surfacer data-backfill tasks through the local reasoning model.

This is intentionally narrower than ``scripts/drain_backlog.py``: it only
claims ``surfacer_data_backfill`` rows so alpha-data gaps do not wait behind
older generic research backlog.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings
from db import get_engine


TASK_TYPE = "surfacer_data_backfill"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drain Surfacer backfill LLM tasks.")
    parser.add_argument("--max-tasks", type=int, default=0, help="Stop after N tasks; 0 means no limit.")
    parser.add_argument("--batch-size", type=int, default=3, help="Rows to claim per DB batch.")
    parser.add_argument("--sleep", type=float, default=5.0, help="Sleep when no pending tasks.")
    parser.add_argument("--once", action="store_true", help="Exit after one empty poll.")
    return parser.parse_args(argv)


def _claim_tasks(engine: Any, batch_size: int) -> list[Any]:
    with engine.begin() as conn:
        return conn.execute(
            text(
                """
                UPDATE llm_task_backlog
                SET status = 'processing'
                WHERE id IN (
                    SELECT id
                    FROM llm_task_backlog
                    WHERE task_type = :task_type
                      AND status = 'pending'
                    ORDER BY priority ASC, created_at ASC
                    LIMIT :batch_size
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, task_type, prompt, context
                """
            ),
            {"task_type": TASK_TYPE, "batch_size": batch_size},
        ).fetchall()


def _call_model(prompt: str) -> str | None:
    full_prompt = (
        "You are GRID's Surfacer data backfill agent. Return strict JSON. "
        "Do not invent facts; mark unavailable data as missing. "
        "Prefer existing GRID tables and deterministic pullers before web research.\n\n"
        f"{prompt[:9000]}"
    )
    response = requests.post(
        f"{settings.LLAMACPP_BASE_URL}/completion",
        json={
            "prompt": full_prompt,
            "n_predict": 1200,
            "temperature": 0.2,
        },
        timeout=300,
    )
    response.raise_for_status()
    content = response.json().get("content", "").strip()
    return content or None


def _store_result(engine: Any, task_id: int, task_type: str, prompt: str, context: Any, result: str) -> None:
    payload = {
        "task_id": task_id,
        "task_type": task_type,
        "response": result[:6000],
        "prompt_preview": prompt[:500],
        "context": context if isinstance(context, dict) else {},
        "model": "qwen-reasoning-local",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO analytical_snapshots
                    (snapshot_date, as_of_date, category, subcategory, payload)
                VALUES (CURRENT_DATE, :as_of_date, :category, :subcategory, CAST(:payload AS jsonb))
                """
            ),
            {
                "as_of_date": date.today(),
                "category": f"llm_task_{task_type}",
                "subcategory": task_type,
                "payload": json.dumps(payload, default=str),
            },
        )
        conn.execute(
            text("UPDATE llm_task_backlog SET status = 'done' WHERE id = :id"),
            {"id": task_id},
        )


def _release_task(engine: Any, task_id: int, error: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE llm_task_backlog
                SET status = 'pending',
                    context = COALESCE(context, '{}'::jsonb)
                        || jsonb_build_object('last_error', :error, 'last_error_at', NOW())
                WHERE id = :id
                """
            ),
            {"id": task_id, "error": error[:300]},
        )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    engine = get_engine()
    processed = 0
    errors = 0
    log.info("Surfacer backfill drainer started")

    while True:
        if args.max_tasks and processed >= args.max_tasks:
            break
        tasks = _claim_tasks(engine, args.batch_size)
        if not tasks:
            if args.once:
                break
            log.info("No pending Surfacer backfill tasks; processed={p} errors={e}", p=processed, e=errors)
            time.sleep(args.sleep)
            continue

        for row in tasks:
            task_id, task_type, prompt, context = row
            try:
                result = _call_model(prompt or "")
                if not result:
                    raise RuntimeError("empty model result")
                _store_result(engine, task_id, task_type, prompt or "", context, result)
                processed += 1
                log.info("Surfacer backfill task done id={id} processed={p}", id=task_id, p=processed)
            except Exception as exc:
                errors += 1
                _release_task(engine, int(task_id), f"{type(exc).__name__}: {exc}")
                log.warning("Surfacer backfill task failed id={id}: {e}", id=task_id, e=str(exc)[:200])

    log.info("Surfacer backfill drainer stopped processed={p} errors={e}", p=processed, e=errors)
    return 0 if errors == 0 else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
