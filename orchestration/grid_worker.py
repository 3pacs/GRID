#!/usr/bin/env python3
"""
GRID — Distributed Worker Daemon.

Runs on gridz4 (or any compute node). Polls the task_queue via
Tailscale Postgres, claims tasks, executes them, and writes results
back. Emits heartbeat events every 60s.

Usage:
    python3 grid_worker.py                      # uses GRID_DB_URL env var
    python3 grid_worker.py --node gridz4        # explicit node name
    python3 grid_worker.py --db-url postgresql://grid:pass@host/griddb

Runs forever. Deploy as systemd service for production.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import sys
import time
import traceback
from datetime import datetime, timezone

# Minimal imports — this runs standalone on worker nodes
try:
    from loguru import logger as log
except ImportError:
    import logging
    log = logging.getLogger("grid-worker")
    log.setLevel(logging.INFO)
    log.addHandler(logging.StreamHandler())

try:
    from sqlalchemy import create_engine, text
except ImportError:
    print("ERROR: sqlalchemy required. pip install sqlalchemy psycopg2-binary")
    sys.exit(1)


HEARTBEAT_INTERVAL = 60
POLL_INTERVAL = 5
IDLE_POLL_INTERVAL = 10


class GridWorker:
    """Task queue worker that claims and executes tasks from Postgres."""

    def __init__(self, node_name: str, db_url: str) -> None:
        self._node = node_name
        self._engine = create_engine(db_url, pool_pre_ping=True, pool_size=3)
        self._running = True
        self._last_heartbeat = 0.0
        self._tasks_completed = 0
        self._tasks_failed = 0

        # Handler registry
        self._handlers: dict[str, callable] = {
            "baseline_compute": self._handle_baseline,
            "timesfm_batch": self._handle_timesfm,
            "data_crunch": self._handle_data_crunch,
        }

    def run_forever(self) -> None:
        """Main loop: claim tasks, execute, heartbeat."""
        log.info("GridWorker started — node={n}, handlers={h}",
                 n=self._node, h=list(self._handlers.keys()))

        self._emit_heartbeat()

        while self._running:
            try:
                task = self._claim_task()

                if task:
                    self._execute(task)
                    time.sleep(POLL_INTERVAL)
                else:
                    time.sleep(IDLE_POLL_INTERVAL)

                now = time.monotonic()
                if now - self._last_heartbeat > HEARTBEAT_INTERVAL:
                    self._emit_heartbeat()

            except KeyboardInterrupt:
                log.info("GridWorker shutting down (SIGINT)")
                self._running = False
            except Exception as exc:
                log.error("GridWorker loop error: {e}", e=str(exc))
                time.sleep(30)

        log.info("GridWorker stopped — completed={c}, failed={f}",
                 c=self._tasks_completed, f=self._tasks_failed)

    def _claim_task(self) -> dict | None:
        """Atomically claim the next available task."""
        try:
            with self._engine.begin() as conn:
                row = conn.execute(text("""
                    UPDATE task_queue SET
                        status = 'running',
                        claimed_at = NOW(),
                        claimed_by = :node
                    WHERE id = (
                        SELECT id FROM task_queue
                        WHERE status = 'queued'
                        AND (node_target IS NULL OR node_target = :node)
                        ORDER BY priority ASC, created_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    RETURNING id, task_type, priority, payload, created_at
                """), {"node": self._node}).fetchone()

                if not row:
                    return None

                return {
                    "id": str(row[0]),
                    "task_type": row[1],
                    "priority": row[2],
                    "payload": row[3] if isinstance(row[3], dict) else json.loads(row[3]),
                    "created_at": str(row[4]),
                }
        except Exception as exc:
            log.debug("Claim error: {e}", e=str(exc))
            return None

    def _execute(self, task: dict) -> None:
        """Route task to handler and manage lifecycle."""
        task_type = task["task_type"]
        task_id = task["id"]

        handler = self._handlers.get(task_type)
        if not handler:
            self._fail_task(task_id, f"Unknown task_type: {task_type}")
            return

        log.info("Executing task {id} ({type})", id=task_id[:8], type=task_type)
        start = time.monotonic()

        try:
            result = handler(task["payload"])
            elapsed = time.monotonic() - start

            self._complete_task(task_id, {
                "elapsed_seconds": round(elapsed, 2),
                **(result or {}),
            })
            self._tasks_completed += 1
            log.info("Task {id} completed in {t:.1f}s", id=task_id[:8], t=elapsed)

        except Exception as exc:
            elapsed = time.monotonic() - start
            error = f"{type(exc).__name__}: {exc}"
            self._fail_task(task_id, error)
            self._tasks_failed += 1
            log.error("Task {id} failed after {t:.1f}s: {e}",
                      id=task_id[:8], t=elapsed, e=error)

    def _complete_task(self, task_id: str, result: dict) -> None:
        try:
            with self._engine.begin() as conn:
                conn.execute(text("""
                    UPDATE task_queue SET
                        status = 'completed',
                        completed_at = NOW(),
                        result = :result
                    WHERE id = CAST(:id AS uuid)
                """), {"id": task_id, "result": json.dumps(result)})
        except Exception as exc:
            log.warning("Failed to mark task complete: {e}", e=str(exc))

    def _fail_task(self, task_id: str, error: str) -> None:
        try:
            with self._engine.begin() as conn:
                row = conn.execute(text("""
                    SELECT retry_count, max_retries FROM task_queue
                    WHERE id = CAST(:id AS uuid)
                """), {"id": task_id}).fetchone()

                if row and row[0] < row[1]:
                    conn.execute(text("""
                        UPDATE task_queue SET
                            status = 'queued',
                            claimed_at = NULL,
                            claimed_by = NULL,
                            retry_count = retry_count + 1,
                            error = :error
                        WHERE id = CAST(:id AS uuid)
                    """), {"id": task_id, "error": error})
                else:
                    conn.execute(text("""
                        UPDATE task_queue SET
                            status = 'failed',
                            completed_at = NOW(),
                            error = :error
                        WHERE id = CAST(:id AS uuid)
                    """), {"id": task_id, "error": error})
        except Exception as exc:
            log.warning("Failed to mark task failed: {e}", e=str(exc))

    def _emit_heartbeat(self) -> None:
        try:
            with self._engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO event_bus (event_type, source_node, payload)
                    VALUES ('heartbeat', :node, :payload)
                """), {
                    "node": self._node,
                    "payload": json.dumps({
                        "tasks_completed": self._tasks_completed,
                        "tasks_failed": self._tasks_failed,
                        "hostname": platform.node(),
                        "uptime_seconds": int(time.monotonic()),
                    }),
                })
            self._last_heartbeat = time.monotonic()
        except Exception as exc:
            log.debug("Heartbeat failed: {e}", e=str(exc))

    # ── Task Handlers ───────────────────────────────────────────────────

    def _handle_baseline(self, payload: dict) -> dict:
        """Compute baseline statistics for a signal."""
        import numpy as np
        from scipy import stats as sp_stats

        signal_name = payload["signal"]
        feature_id = payload.get("feature_id")

        # Pull data from DB
        with self._engine.connect() as conn:
            if feature_id:
                rows = conn.execute(text("""
                    SELECT obs_date, value FROM resolved_series
                    WHERE feature_id = :fid AND value IS NOT NULL
                    ORDER BY obs_date ASC
                """), {"fid": feature_id}).fetchall()
            else:
                rows = conn.execute(text("""
                    SELECT rs.obs_date, rs.value
                    FROM resolved_series rs
                    JOIN feature_registry fr ON rs.feature_id = fr.id
                    WHERE fr.name = :name AND rs.value IS NOT NULL
                    ORDER BY rs.obs_date ASC
                """), {"name": signal_name}).fetchall()

        if len(rows) < 30:
            return {"status": "insufficient_data", "obs_count": len(rows)}

        values = np.array([float(r[1]) for r in rows])

        baseline = {
            "signal": signal_name,
            "obs_count": len(values),
            "mean": float(np.mean(values)),
            "std": float(np.std(values)),
            "skew": float(sp_stats.skew(values)),
            "kurtosis": float(sp_stats.kurtosis(values)),
            "percentiles": {
                str(p): float(np.percentile(values, p))
                for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]
            },
        }

        return {"status": "computed", "baseline": baseline}

    def _handle_timesfm(self, payload: dict) -> dict:
        """Run TimesFM batch forecast."""
        import numpy as np

        feature_ids = payload.get("feature_ids", [])
        horizon = payload.get("horizon", 30)

        from timesfm.timesfm_2p5.timesfm_2p5_torch import TimesFM_2p5_200M_torch
        from timesfm.configs import ForecastConfig

        model = TimesFM_2p5_200M_torch.from_pretrained("google/timesfm-2.5-200m-pytorch")
        fc = ForecastConfig(max_context=512, max_horizon=horizon, per_core_batch_size=64)
        model.compile(fc)

        results = {}
        with self._engine.connect() as conn:
            for fid in feature_ids:
                rows = conn.execute(text("""
                    SELECT value FROM resolved_series
                    WHERE feature_id = :fid AND value IS NOT NULL
                    ORDER BY obs_date ASC
                """), {"fid": fid}).fetchall()

                if len(rows) < 64:
                    continue

                values = np.array([float(r[0]) for r in rows], dtype=np.float32)
                if len(values) > 512:
                    values = values[-512:]

                point_fc, _ = model.forecast(horizon=horizon, inputs=[values])
                results[str(fid)] = {
                    "forecast": [float(x) for x in point_fc[0]],
                    "last_value": float(values[-1]),
                }

        return {"status": "computed", "forecasts": len(results)}

    def _handle_data_crunch(self, payload: dict) -> dict:
        """Generic data processing task."""
        script = payload.get("script")
        if not script:
            return {"status": "error", "message": "no script provided"}

        import subprocess
        result = subprocess.run(
            ["python3", "-c", script],
            capture_output=True, text=True, timeout=300,
        )

        return {
            "status": "completed" if result.returncode == 0 else "failed",
            "stdout": result.stdout[-2000:],
            "stderr": result.stderr[-1000:],
            "returncode": result.returncode,
        }


def main():
    parser = argparse.ArgumentParser(description="GRID Worker Daemon")
    parser.add_argument("--node", default=platform.node(), help="Node name")
    parser.add_argument("--db-url", default=os.environ.get("GRID_DB_URL"),
                        help="PostgreSQL connection URL")
    args = parser.parse_args()

    if not args.db_url:
        print("ERROR: --db-url or GRID_DB_URL env var required")
        sys.exit(1)

    worker = GridWorker(args.node, args.db_url)

    def _shutdown(signum, frame):
        worker._running = False

    signal.signal(signal.SIGTERM, _shutdown)

    worker.run_forever()


if __name__ == "__main__":
    main()
