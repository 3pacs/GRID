"""
GRID ↔ AutoAgent Integration Runner

Bridges AutoAgent's score-driven iteration loop with GRID's:
  - hypothesis_registry (logs each experiment as a hypothesis)
  - validation_results (stores per-run metrics)
  - compute_coordinator (optional: register as a HYPOTHESIS_TEST job)

Usage:
    python grid_autoagent_runner.py --task grid-signal-eog --iterations 50
    python grid_autoagent_runner.py --task grid-signal-eog --hours 24
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import Json

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
AUTOAGENT_ROOT = Path(__file__).parent
TASKS_DIR = AUTOAGENT_ROOT / "tasks"
RESULTS_TSV = AUTOAGENT_ROOT / "results.tsv"
GRID_REPO = AUTOAGENT_ROOT.parent

sys.path.insert(0, str(GRID_REPO))

DB_PARAMS = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", "5432")),
    "dbname": os.environ.get("DB_NAME", "grid"),
    "user": os.environ.get("DB_USER", "grid_user"),
    "password": os.environ.get("DB_PASSWORD", ""),
}


def get_db_conn():
    return psycopg2.connect(**DB_PARAMS)


def run_harbor_task(task_name: str, concurrency: int = 1) -> dict:
    """Execute a single AutoAgent run via harbor CLI.

    Returns dict with: score, detail (from reward_detail.json), duration_s, success.
    """
    start = time.time()

    cmd = [
        "uv", "run", "harbor", "run",
        "-p", str(TASKS_DIR),
        "--task-name", f"harbor/{task_name}",
        "--agent-import-path", "agent:AutoAgent",
        "-n", str(concurrency),
        "-o", str(AUTOAGENT_ROOT / "jobs"),
    ]

    result = subprocess.run(
        cmd,
        cwd=str(AUTOAGENT_ROOT),
        capture_output=True,
        text=True,
        timeout=1800,  # 30 min max
    )

    duration = time.time() - start

    # Find the most recent reward file in jobs/
    jobs_dir = AUTOAGENT_ROOT / "jobs"
    reward_file = None
    detail_file = None

    if jobs_dir.exists():
        # Walk job directories sorted by modification time (newest first)
        job_dirs = sorted(jobs_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
        for jd in job_dirs[:5]:  # Check recent jobs
            candidate = jd / "logs" / "verifier" / "reward.txt"
            if candidate.exists():
                reward_file = candidate
                detail_file = jd / "logs" / "verifier" / "reward_detail.json"
                break

    score = 0.0
    detail = {}

    if reward_file and reward_file.exists():
        try:
            score = float(reward_file.read_text().strip())
        except ValueError:
            score = 0.0

    if detail_file and detail_file.exists():
        try:
            detail = json.loads(detail_file.read_text())
        except (json.JSONDecodeError, FileNotFoundError):
            detail = {}

    return {
        "score": score,
        "detail": detail,
        "duration_s": round(duration, 1),
        "success": result.returncode == 0,
        "stdout": result.stdout[-2000:] if result.stdout else "",
        "stderr": result.stderr[-2000:] if result.stderr else "",
    }


def log_to_grid_db(task_name: str, iteration: int, run_result: dict) -> int | None:
    """Insert experiment into GRID's hypothesis_registry + validation_results.

    Returns hypothesis_id or None on failure.
    """
    detail = run_result.get("detail", {})
    components = detail.get("components", {})
    stats = detail.get("stats", {})

    statement = (
        f"AutoAgent iteration {iteration}: {task_name} — "
        f"composite={run_result['score']:.4f}, "
        f"sharpe={components.get('sharpe', {}).get('value', '?')}, "
        f"hit_rate={components.get('hit_rate', {}).get('value', '?')}"
    )

    try:
        with get_db_conn() as conn:
            cur = conn.cursor()

            # Insert as CANDIDATE hypothesis (autoagent-tagged)
            cur.execute(
                """
                INSERT INTO hypothesis_registry
                    (statement, layer, feature_ids, lag_structure,
                     proposed_metric, proposed_threshold, state)
                VALUES (%(stmt)s, 'TACTICAL', %(fids)s, %(lag)s,
                        'sharpe_ratio', %(thresh)s, 'TESTING')
                RETURNING id
                """,
                {
                    "stmt": statement,
                    "fids": [],
                    "lag": Json({
                        "autoagent": True,
                        "iteration": iteration,
                        "task": task_name,
                        "timestamp": datetime.utcnow().isoformat(),
                    }),
                    "thresh": components.get("sharpe", {}).get("value", 0.0),
                },
            )
            hyp_id = cur.fetchone()[0]

            # Insert validation results
            cur.execute(
                """
                INSERT INTO validation_results
                    (hypothesis_id, vintage_policy, era_results,
                     full_period_metrics, baseline_comparison,
                     simplicity_comparison, walk_forward_splits,
                     cost_assumption_bps, overall_verdict, gate_detail)
                VALUES (%(hid)s, 'LATEST_AS_OF', %(era)s,
                        %(full)s, %(base)s, %(simp)s,
                        %(splits)s, 10.0, %(verdict)s, %(gate)s)
                """,
                {
                    "hid": hyp_id,
                    "era": Json(components),
                    "full": Json({
                        "composite_score": run_result["score"],
                        **{k: v.get("value", 0) for k, v in components.items()},
                    }),
                    "base": Json({
                        "sharpe_baseline": components.get("sharpe", {}).get("baseline", 0),
                    }),
                    "simp": Json({
                        "n_features": components.get("parsimony", {}).get("n_features", 0),
                    }),
                    "splits": stats.get("n_walk_forward_windows", 4),
                    "verdict": "PASS" if run_result["score"] >= 0.5 else "FAIL",
                    "gate": Json({
                        "source": "autoagent",
                        "iteration": iteration,
                        "duration_s": run_result["duration_s"],
                    }),
                },
            )
            conn.commit()
            return hyp_id

    except Exception as e:
        print(f"[WARN] Failed to log to GRID DB: {e}", file=sys.stderr)
        return None


def log_to_results_tsv(iteration: int, run_result: dict) -> None:
    """Append to results.tsv for AutoAgent's meta-agent tracking."""
    header_needed = not RESULTS_TSV.exists()
    with open(RESULTS_TSV, "a") as f:
        if header_needed:
            f.write("iteration\ttimestamp\tscore\tsharpe\thit_rate\tmax_dd\tic\tn_features\tduration_s\n")

        c = run_result.get("detail", {}).get("components", {})
        f.write(
            f"{iteration}\t"
            f"{datetime.utcnow().isoformat()}\t"
            f"{run_result['score']:.4f}\t"
            f"{c.get('sharpe', {}).get('value', 0):.4f}\t"
            f"{c.get('hit_rate', {}).get('value', 0):.4f}\t"
            f"{c.get('max_drawdown', {}).get('value', 0):.4f}\t"
            f"{c.get('information_coefficient', {}).get('value', 0):.4f}\t"
            f"{c.get('parsimony', {}).get('n_features', 0)}\t"
            f"{run_result['duration_s']}\n"
        )


def main():
    parser = argparse.ArgumentParser(description="GRID AutoAgent Integration Runner")
    parser.add_argument("--task", default="grid-signal-eog", help="Task name (without harbor/ prefix)")
    parser.add_argument("--iterations", type=int, default=0, help="Max iterations (0=unlimited)")
    parser.add_argument("--hours", type=float, default=0, help="Max hours to run (0=unlimited)")
    parser.add_argument("--concurrency", type=int, default=1, help="Parallel Harbor workers")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    args = parser.parse_args()

    deadline = None
    if args.hours > 0:
        deadline = datetime.utcnow() + timedelta(hours=args.hours)

    best_score = 0.0
    iteration = 0

    print(f"=== GRID AutoAgent Runner ===")
    print(f"Task: {args.task}")
    print(f"Iterations: {'unlimited' if args.iterations == 0 else args.iterations}")
    print(f"Deadline: {deadline or 'none'}")
    print(f"Concurrency: {args.concurrency}")
    print()

    while True:
        iteration += 1

        # Check termination conditions
        if args.iterations > 0 and iteration > args.iterations:
            break
        if deadline and datetime.utcnow() > deadline:
            print(f"[!] Deadline reached after {iteration - 1} iterations")
            break

        print(f"--- Iteration {iteration} ---")

        if args.dry_run:
            print(f"  [DRY RUN] Would execute harbor run for {args.task}")
            continue

        # Run the task
        result = run_harbor_task(args.task, args.concurrency)

        # Log everywhere
        log_to_results_tsv(iteration, result)
        hyp_id = log_to_grid_db(args.task, iteration, result)

        # Hill-climb tracking
        improved = result["score"] > best_score
        if improved:
            best_score = result["score"]

        status = "NEW BEST" if improved else "no improvement"
        print(
            f"  Score: {result['score']:.4f} ({status}) | "
            f"Best: {best_score:.4f} | "
            f"Duration: {result['duration_s']}s | "
            f"DB hypothesis: {hyp_id or 'skip'}"
        )

        if not result["success"]:
            print(f"  [WARN] Harbor returned non-zero exit code")
            if result["stderr"]:
                print(f"  stderr: {result['stderr'][:200]}")

    print(f"\n=== Run complete ===")
    print(f"Iterations: {iteration - 1}")
    print(f"Best score: {best_score:.4f}")
    print(f"Results: {RESULTS_TSV}")


if __name__ == "__main__":
    main()
