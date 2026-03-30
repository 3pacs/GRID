#!/usr/bin/env python3
"""Drain LLM task backlog through Qwen — run as background daemon.

Usage: nohup python3 scripts/drain_backlog.py > /tmp/drain_backlog.log 2>&1 &

Sanity checks before processing each task:
  1. Prompt is not empty/too short
  2. Task type is recognized
  3. Prompt is not a duplicate of a recently completed task
  4. Prompt contains actual research questions (not boilerplate)
"""
import os
import sys
import json
import time
import hashlib
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_engine
from sqlalchemy import text
from loguru import logger as log

engine = get_engine()
QWEN_URL = "http://localhost:8080/completion"
# Increase batch size — Qwen 32B on llama.cpp handles concurrent requests via its
# own queue. We pull more work, fire them in parallel threads, and skip the fixed
# inter-batch sleep when the queue is draining fast.
BATCH_SIZE = 10  # was 3 — pull 10 tasks per cycle
SLEEP_BETWEEN = 1  # was 3s — shorter sleep when queue is active
# How many tasks to process concurrently. Qwen serializes internally, but parallel
# requests avoid the Python→HTTP→Qwen→Python round-trip idle time.
WORKER_THREADS = 3  # conservative: don't slam Qwen with too many queued prompts

# Track recent prompt hashes to skip near-duplicates
_recent_hashes: set[str] = set()

VALID_TASK_TYPES = {
    "icij_officer", "icij_entity", "icij_investigate", "forensic",
    "crypto_angle", "crypto_forensic", "crypto_person", "crypto_thesis",
    "crypto_regulation", "onchain_forensic", "crypto_whale_track",
    "crypto_exchange_flow", "crypto_defi_analysis", "crypto_onchain",
    "company_profile", "sector_analysis", "signal_crossval",
    "earnings_preview", "price_decomposition", "test_pipeline",
    "deep_dive", "actor_discovery", "entity_research",
}


def sanity_check(task_id: int, task_type: str, prompt: str) -> str | None:
    """Return None if task passes, or a skip reason string."""
    # 1. Empty/short prompt
    if not prompt or len(prompt.strip()) < 20:
        return "prompt too short"

    # 2. Unknown task type (warn but don't skip — could be new type)
    # Just log it, process anyway
    if task_type not in VALID_TASK_TYPES:
        log.debug("Unknown task type '{t}' for task {id} — processing anyway", t=task_type, id=task_id)

    # 3. Near-duplicate detection (same prompt hash as recent task)
    prompt_hash = hashlib.md5(prompt.strip().lower().encode()).hexdigest()[:12]
    if prompt_hash in _recent_hashes:
        return f"near-duplicate (hash={prompt_hash})"
    _recent_hashes.add(prompt_hash)
    # Keep set bounded
    if len(_recent_hashes) > 5000:
        _recent_hashes.clear()

    # 4. Boilerplate detection — skip if prompt is just template with no substance
    boilerplate_markers = [
        "INSERT YOUR QUERY HERE",
        "TODO: fill in",
        "PLACEHOLDER",
        "[BLANK]",
    ]
    for marker in boilerplate_markers:
        if marker.lower() in prompt.lower():
            return f"boilerplate detected: {marker}"

    # 5. Prompt length sanity — extremely long prompts waste context
    if len(prompt) > 5000:
        log.debug("Long prompt ({n} chars) for task {id} — truncating", n=len(prompt), id=task_id)

    return None  # passes all checks


def process_task(prompt: str) -> str | None:
    """Send task to Qwen and return response."""
    full_prompt = (
        "You are a financial intelligence researcher for GRID. "
        "Be specific: names, numbers, dates, sources. "
        "Label confidence: confirmed/derived/estimated/rumored/inferred. "
        "No filler. No disclaimers.\n\n"
        f"{prompt[:4000]}"  # cap at 4K chars to leave room for response
    )

    try:
        resp = requests.post(QWEN_URL, json={
            "prompt": full_prompt,
            "n_predict": 500,
            "temperature": 0.3,
            "stop": ["\n\n\n"],
        }, timeout=180)
        result = resp.json().get("content", "").strip()
        if not result or len(result) < 30:
            return None
        return result
    except Exception as exc:
        log.warning("Qwen request failed: {e}", e=str(exc)[:80])
        return None


def main():
    processed = 0
    skipped = 0
    errors = 0
    start = time.time()

    log.info("Backlog drainer starting — batch_size={b}, sleep={s}s",
             b=BATCH_SIZE, s=SLEEP_BETWEEN)

    while True:
        try:
            # Pull a batch of pending tasks
            with engine.begin() as c:
                tasks = c.execute(text("""
                    UPDATE llm_task_backlog SET status = 'processing'
                    WHERE id IN (
                        SELECT id FROM llm_task_backlog
                        WHERE status = 'pending'
                        ORDER BY priority ASC, id ASC
                        LIMIT :batch
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, task_type, prompt, context
                """), {"batch": BATCH_SIZE}).fetchall()

            if not tasks:
                elapsed = (time.time() - start) / 60
                log.info("No pending tasks. processed={p}, skipped={s}, errors={e}, elapsed={m:.0f}min",
                         p=processed, s=skipped, e=errors, m=elapsed)
                time.sleep(60)
                continue

            # Separate tasks that pass sanity check from those to skip immediately
            to_process = []
            to_skip = []
            for task in tasks:
                tid, ttype, prompt, context = task
                skip_reason = sanity_check(tid, ttype or "", prompt or "")
                if skip_reason:
                    log.debug("Skipping task {id} ({t}): {r}", id=tid, t=ttype, r=skip_reason)
                    to_skip.append(tid)
                else:
                    to_process.append(task)

            # Batch-mark skipped tasks as done in a single DB round-trip
            if to_skip:
                with engine.begin() as c:
                    c.execute(
                        text("UPDATE llm_task_backlog SET status = 'done' WHERE id = ANY(:ids)"),
                        {"ids": to_skip},
                    )
                skipped += len(to_skip)

            def _handle_task(task):
                """Process one task and write result. Returns (ok: bool)."""
                tid, ttype, prompt, context = task
                result = process_task(prompt)
                if result:
                    with engine.begin() as c:
                        c.execute(text("""
                            INSERT INTO analytical_snapshots
                                (snapshot_date, as_of_date, category, subcategory, payload)
                            VALUES (CURRENT_DATE, CURRENT_DATE, :cat, :sub, :payload)
                        """), {
                            "cat": f"llm_{ttype}",
                            "sub": ttype or "unknown",
                            "payload": json.dumps({
                                "task_id": tid,
                                "response": result[:2000],
                                "model": "qwen-32b",
                                "prompt_preview": (prompt or "")[:200],
                            }),
                        })
                        c.execute(text(
                            "UPDATE llm_task_backlog SET status = 'done' WHERE id = :id"
                        ), {"id": tid})
                    return True
                else:
                    with engine.begin() as c:
                        c.execute(text(
                            "UPDATE llm_task_backlog SET status = 'pending' WHERE id = :id"
                        ), {"id": tid})
                    return False

            # Fire tasks concurrently — Qwen queues them internally, so we get
            # pipelined throughput instead of serial request/response cycles.
            if to_process:
                with ThreadPoolExecutor(max_workers=WORKER_THREADS) as pool:
                    futures = {pool.submit(_handle_task, t): t for t in to_process}
                    for fut in as_completed(futures):
                        try:
                            ok = fut.result()
                            if ok:
                                processed += 1
                            else:
                                errors += 1
                        except Exception as exc:
                            log.warning("Task future raised: {e}", e=str(exc)[:80])
                            errors += 1

            if processed % 10 == 0 and processed > 0:
                elapsed = time.time() - start
                rate = processed / (elapsed / 3600)
                log.info("Progress: processed={p}, skipped={s}, errors={e}, rate={r:.0f}/hr",
                         p=processed, s=skipped, e=errors, r=rate)

            # Only sleep when the queue is empty (handled above) or we got a full
            # batch — if we got fewer than BATCH_SIZE tasks the queue is nearly
            # empty, so sleep briefly to avoid busy-polling.
            if len(tasks) < BATCH_SIZE:
                time.sleep(SLEEP_BETWEEN)
            # else: dive straight back in — more work waiting

        except KeyboardInterrupt:
            log.info("Shutting down. processed={p}, skipped={s}, errors={e}",
                     p=processed, s=skipped, e=errors)
            break
        except Exception as exc:
            log.error("Loop error: {e}", e=str(exc)[:120])
            time.sleep(10)


if __name__ == "__main__":
    main()
