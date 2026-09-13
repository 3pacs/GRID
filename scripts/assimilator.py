#!/usr/bin/env python3
"""GRID — Human LLM Response Assimilator.

Polls for completed HUMAN_LLM_QUERY jobs, sends them to local Hermes
for structured extraction, and stores the parsed insights in the DB.

Flow:
  1. Poll coordinator for COMPLETED HUMAN_LLM_QUERY jobs
  2. Send raw response to Hermes with extraction prompt
  3. Store structured output in analytical_snapshots
  4. Mark job as VALID → ASSIMILATED

Run: python3 scripts/assimilator.py
     python3 scripts/assimilator.py --once   # single pass
"""

from __future__ import annotations

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from loguru import logger as log

from llm.router import get_llm as get_client

COORDINATOR_URL = os.getenv("GRID_COORDINATOR_URL", "http://localhost:8100")
POLL_INTERVAL = 15  # seconds

# analytical_snapshots.category written for every assimilated insight.
SNAPSHOT_CATEGORY = "human_llm_insight"

EXTRACTION_SYSTEM = """You are GRID's intelligence parser. You receive raw LLM responses
from external models (ChatGPT, Gemini, Claude) and extract structured trading intelligence.

For every response, extract:
1. **sentiment**: overall market sentiment (-1.0 to 1.0)
2. **regime**: identified market regime (risk-on, risk-off, transition, uncertain)
3. **key_signals**: list of specific actionable signals mentioned
4. **assets**: list of assets/tickers discussed with individual sentiment
5. **confidence**: how confident the analysis appears (0.0 to 1.0)
6. **time_horizon**: short-term, medium-term, long-term
7. **risks**: key risks identified
8. **summary**: 2-3 sentence summary of the core insight

Respond ONLY with valid JSON. No markdown, no explanation."""

EXTRACTION_TEMPLATE = """Parse the following LLM response into structured intelligence.

Original query: {query}
Context: {context}
Model used: {model}

--- RAW RESPONSE ---
{response}
--- END RESPONSE ---

Extract structured JSON with keys: sentiment, regime, key_signals, assets, confidence, time_horizon, risks, summary"""


def parse_response(raw_response: str, query: str, context: str, model_used: str) -> dict | None:
    """Send raw LLM response to Hermes for structured extraction."""
    client = get_client()
    if not client.is_available:
        log.warning("Hermes not available — storing raw response only")
        return {
            "raw": True,
            "response": raw_response[:5000],
            "model_used": model_used,
            "parsed": False,
        }

    prompt = EXTRACTION_TEMPLATE.format(
        query=query,
        context=context,
        model=model_used,
        response=raw_response[:8000],  # limit to avoid context overflow
    )

    result = client.chat(
        messages=[
            {"role": "system", "content": EXTRACTION_SYSTEM},
            {"role": "user", "content": prompt},
        ],
        temperature=0.1,
        num_predict=2000,
    )

    if not result:
        log.warning("Hermes returned empty result")
        return None

    # Try to parse JSON from response
    try:
        # Strip markdown code fences if present
        text = result.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()

        parsed = json.loads(text)
        parsed["parsed"] = True
        parsed["model_used"] = model_used
        return parsed

    except json.JSONDecodeError:
        log.warning("Failed to parse Hermes JSON output, storing raw")
        return {
            "raw": True,
            "hermes_response": result[:3000],
            "model_used": model_used,
            "parsed": False,
        }


def store_insight(job: dict, parsed: dict) -> int:
    """Store a parsed insight in analytical_snapshots.

    Parameters:
        job: The coordinator job dict the insight was extracted from.
        parsed: The structured insight to persist as the snapshot payload.

    Returns:
        int: The analytical_snapshots row id.

    Raises:
        RuntimeError: When the row did not land. `save_snapshot` catches its
            own exceptions and returns None, so a silent failure is otherwise
            indistinguishable from success — and the caller would go on to
            mark the job ASSIMILATED with nothing in the table.
    """
    from store.snapshots import AnalyticalSnapshotStore
    from db import get_engine

    engine = get_engine()
    store = AnalyticalSnapshotStore(db_engine=engine)

    snapshot_id = store.save_snapshot(
        category=SNAPSHOT_CATEGORY,
        subcategory=job.get("name", "unknown"),
        payload=parsed,
        metrics={
            "job_id": job["id"],
            "model_used": parsed.get("model_used", "unknown"),
            "sentiment": parsed.get("sentiment"),
            "confidence": parsed.get("confidence"),
            "regime": parsed.get("regime"),
            "parsed": parsed.get("parsed", False),
        },
    )
    if snapshot_id is None:
        raise RuntimeError(
            f"save_snapshot returned no id for job #{job['id']} — "
            f"the insight did not reach analytical_snapshots"
        )

    log.info(
        "Stored insight for job #{id} as analytical_snapshots #{sid}",
        id=job["id"], sid=snapshot_id,
    )
    return snapshot_id


def process_completed_jobs() -> tuple[int, int]:
    """Process all completed HUMAN_LLM_QUERY jobs.

    Returns:
        tuple[int, int]: ``(processed, failed)`` — jobs assimilated, and jobs
        whose insight could not be stored. `failed` is reported separately so
        a pass that persisted nothing is not indistinguishable from an idle
        one (the `--once` exit code depends on it).
    """
    processed = 0
    failed = 0

    # Get completed HUMAN_LLM_QUERY jobs
    try:
        r = requests.get(
            f"{COORDINATOR_URL}/jobs",
            params={"state": "COMPLETED", "job_type": "HUMAN_LLM_QUERY", "limit": 20},
            timeout=10,
        )
        r.raise_for_status()
        jobs = r.json()
    except Exception as e:
        log.warning("Failed to fetch completed jobs: {e}", e=e)
        return 0, 0

    for job in jobs:
        job_id = job["id"]
        params = job.get("params", {})
        query = params.get("prompt", "")
        context = params.get("context", "")

        # Get the result
        try:
            r = requests.get(f"{COORDINATOR_URL}/jobs/{job_id}", timeout=10)
            r.raise_for_status()
        except Exception as exc:
            log.warning("Failed to fetch job #{j} result: {e}", j=job_id, e=exc)
            continue

        # Fetch the actual result from compute_results
        try:
            from scripts.compute_coordinator import get_conn
            import psycopg2.extras
            conn = get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT output FROM compute_results WHERE job_id=%s ORDER BY created_at DESC LIMIT 1",
                (job_id,),
            )
            row = cur.fetchone()
            conn.close()

            if not row:
                log.warning("No result found for job #{id}", id=job_id)
                continue

            output = row["output"] if isinstance(row["output"], dict) else json.loads(row["output"])
            raw_response = output.get("response", "")
            model_used = output.get("model_used", "unknown")
        except Exception as e:
            log.warning("Failed to fetch result for job #{id}: {e}", id=job_id, e=e)
            continue

        if not raw_response:
            log.warning("Empty response for job #{id}", id=job_id)
            continue

        # Parse with Hermes
        log.info("Parsing response for job #{id} ({model})...", id=job_id, model=model_used)
        parsed = parse_response(raw_response, query, context, model_used)

        if parsed:
            try:
                store_insight(job, parsed)
            except Exception as exc:
                # log.error, not the log.warning the fetch failures above use:
                # reaching here means the insight was parsed but could not be
                # written, which is an application bug (a name store.snapshots
                # doesn't export, a column the table doesn't have) far more
                # often than an outage. opt(exception=True) so errors.jsonl
                # gets the traceback rather than a bare message.
                #
                # `continue` matters twice: this used to propagate out of the
                # function and abort every remaining job in the batch, and the
                # job must stay COMPLETED for a retry rather than be marked
                # ASSIMILATED with nothing persisted.
                failed += 1
                log.opt(exception=True).error(
                    "Failed to store insight for job #{id} ({err}) — "
                    "leaving it unassimilated",
                    id=job_id, err=str(exc),
                )
                continue

            # Mark as VALID then ASSIMILATED
            try:
                requests.post(f"{COORDINATOR_URL}/jobs/{job_id}/validate", timeout=10)
                requests.post(f"{COORDINATOR_URL}/jobs/{job_id}/assimilate", timeout=10)
                log.info("Job #{id} assimilated", id=job_id)
                processed += 1
            except Exception as e:
                log.warning("Failed to mark job #{id} as assimilated: {e}", id=job_id, e=e)

    return processed, failed


def main() -> int:
    """CLI entrypoint. Returns the process exit code."""
    import argparse
    parser = argparse.ArgumentParser(description="GRID Human LLM Response Assimilator")
    parser.add_argument("--once", action="store_true", help="Single pass, then exit")
    args = parser.parse_args()

    log.info("GRID Assimilator starting — coordinator: {url}", url=COORDINATOR_URL)

    if args.once:
        count, failed = process_completed_jobs()
        log.info("Processed {n} jobs ({f} failed)", n=count, f=failed)
        # A single pass that dropped insights must not look like a clean run
        # to whatever scheduled it.
        return 1 if failed else 0

    while True:
        try:
            count, failed = process_completed_jobs()
            if count or failed:
                log.info(
                    "Processed {n} jobs this cycle ({f} failed)", n=count, f=failed,
                )
        except KeyboardInterrupt:
            log.info("Assimilator shutting down")
            break
        except Exception as e:
            # opt(exception=True) so errors.jsonl gets the traceback rather
            # than a bare message with no origin.
            log.opt(exception=True).error("Assimilator error: {e}", e=e)

        time.sleep(POLL_INTERVAL)

    return 0


if __name__ == "__main__":
    sys.exit(main())
