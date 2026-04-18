#!/usr/bin/env python3
"""Drain Surfacer oracle calibration gaps one ticker at a time.

This worker claims pending ``surfacer_data_requirements`` rows for
``ticker_direction_calibration``, runs the existing Oracle cycle for each
ticker with a bounded timeout, and records only last-attempt metadata.
It does not mark the calibration requirement done.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import get_engine


REQUIREMENT_TYPE = "ticker_direction_calibration"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drain Surfacer oracle calibration gaps.")
    parser.add_argument("--limit", type=int, default=1, help="Maximum number of requirements to claim and process.")
    parser.add_argument(
        "--priority-max",
        type=int,
        default=3,
        help="Only claim requirements with priority at or below this value.",
    )
    parser.add_argument(
        "--ticker-timeout",
        type=int,
        default=600,
        help="Maximum seconds to spend on one ticker's Oracle cycle.",
    )
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep between ticker attempts.")
    parser.add_argument("--dry-run", action="store_true", help="Show the selected requirements without mutating them.")
    return parser.parse_args(argv)


def _claim_requirements(conn: Any, limit: int, priority_max: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    rows = conn.execute(
        text(
            """
            UPDATE surfacer_data_requirements
            SET status = 'processing',
                updated_at = NOW()
            WHERE id IN (
                SELECT id
                FROM surfacer_data_requirements
                WHERE requirement_type = :requirement_type
                  AND status = 'pending'
                  AND priority <= :priority_max
                ORDER BY priority ASC, volume_rank ASC, id ASC
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, ticker, requirement_type, priority, reason, payload, volume_rank, dollar_volume
            """
        ),
        {
            "requirement_type": REQUIREMENT_TYPE,
            "priority_max": priority_max,
            "limit": limit,
        },
    ).fetchall()
    return [dict(row._mapping) for row in rows]


def _preview_requirements(conn: Any, limit: int, priority_max: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    rows = conn.execute(
        text(
            """
            SELECT id, ticker, requirement_type, priority, reason, payload, volume_rank, dollar_volume
            FROM surfacer_data_requirements
            WHERE requirement_type = :requirement_type
              AND status = 'pending'
              AND priority <= :priority_max
            ORDER BY priority ASC, volume_rank ASC, id ASC
            LIMIT :limit
            """
        ),
        {
            "requirement_type": REQUIREMENT_TYPE,
            "priority_max": priority_max,
            "limit": limit,
        },
    ).fetchall()
    return [dict(row._mapping) for row in rows]


def _oracle_cycle_command(ticker: str) -> list[str]:
    return [
        sys.executable,
        str(ROOT / "oracle" / "run_cycle.py"),
        "--no-email",
        "--predict-only",
        "--tickers",
        ticker,
    ]


def _tail_text(value: Any, limit: int = 8000) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")[-limit:]
    return str(value)[-limit:]


def _run_oracle_cycle(ticker: str, timeout_seconds: int) -> dict[str, Any]:
    command = _oracle_cycle_command(ticker)
    started_at = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=max(1, int(timeout_seconds)),
        )
        duration_s = time.monotonic() - started_at
        stdout = _tail_text(completed.stdout).strip()
        stderr = _tail_text(completed.stderr).strip()
        parsed_output: dict[str, Any] | None = None
        if stdout:
            try:
                parsed_output = json.loads(stdout)
            except json.JSONDecodeError:
                parsed_output = None
        return {
            "outcome": "ok" if completed.returncode == 0 else "error",
            "returncode": completed.returncode,
            "duration_s": round(duration_s, 3),
            "command": command,
            "stdout": stdout,
            "stderr": stderr,
            "parsed_output": parsed_output,
        }
    except subprocess.TimeoutExpired as exc:
        duration_s = time.monotonic() - started_at
        stdout = _tail_text(exc.stdout).strip()
        stderr = _tail_text(exc.stderr).strip()
        return {
            "outcome": "timeout",
            "returncode": None,
            "duration_s": round(duration_s, 3),
            "command": command,
            "stdout": stdout,
            "stderr": stderr,
            "parsed_output": None,
            "timeout_seconds": int(timeout_seconds),
        }


def _record_attempt(conn: Any, row: dict[str, Any], result: dict[str, Any]) -> None:
    payload = {
        "last_attempt_at": datetime.now(timezone.utc).isoformat(),
        "last_attempt_outcome": result["outcome"],
        "last_attempt_returncode": result["returncode"],
        "last_attempt_duration_s": result["duration_s"],
        "last_attempt_timeout_s": result.get("timeout_seconds"),
        "last_attempt_command": result["command"],
        "last_attempt_stdout": result["stdout"],
        "last_attempt_stderr": result["stderr"],
    }
    if result.get("parsed_output") is not None:
        payload["last_attempt_output"] = result["parsed_output"]

    conn.execute(
        text(
            """
            UPDATE surfacer_data_requirements
            SET status = 'pending',
                payload = COALESCE(payload, '{}'::jsonb) || CAST(:payload AS jsonb),
                updated_at = NOW()
            WHERE id = :id
            """
        ),
        {
            "id": row["id"],
            "payload": json.dumps(payload, default=str),
        },
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    engine = get_engine()

    with engine.begin() as conn:
        rows = _preview_requirements(conn, args.limit, args.priority_max) if args.dry_run else _claim_requirements(conn, args.limit, args.priority_max)

    if args.dry_run:
        print(json.dumps({"dry_run": True, "count": len(rows), "rows": rows}, indent=2, default=str))
        return 0

    if not rows:
        log.info("No pending Surfacer oracle calibration requirements found")
        return 0

    processed = 0
    errors = 0
    for row in rows:
        ticker = str(row["ticker"]).strip().upper()
        try:
            result = _run_oracle_cycle(ticker, args.ticker_timeout)
            with engine.begin() as conn:
                _record_attempt(conn, row, result)
            processed += 1
            if result["outcome"] != "ok":
                errors += 1
            log.info(
                "Surfacer oracle gap attempted id={id} ticker={ticker} outcome={outcome} duration_s={duration_s}",
                id=row["id"],
                ticker=ticker,
                outcome=result["outcome"],
                duration_s=result["duration_s"],
            )
        except Exception as exc:
            errors += 1
            fallback = {
                "outcome": "error",
                "returncode": None,
                "duration_s": 0.0,
                "command": _oracle_cycle_command(ticker),
                "stdout": "",
                "stderr": f"{type(exc).__name__}: {str(exc)[:500]}",
                "parsed_output": None,
            }
            with engine.begin() as conn:
                _record_attempt(conn, row, fallback)
            log.warning("Surfacer oracle gap failed id={id} ticker={ticker}: {err}", id=row["id"], ticker=ticker, err=str(exc)[:200])

        time.sleep(args.sleep)

    log.info("Surfacer oracle gap worker complete processed={p} errors={e}", p=processed, e=errors)
    return 0 if errors == 0 else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
