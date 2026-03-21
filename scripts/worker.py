#!/usr/bin/env python3
"""GRID — Distributed Compute Worker.

Runs on any Tailscale node. Auto-detects hardware (CPU, RAM, GPU, Ollama, Docker).
Registers with the coordinator, polls for jobs, executes them, reports results.

Default coordinator: http://100.75.185.36:8100

Run: python3 worker.py
     python3 worker.py --coordinator http://10.254.111.80:8100
"""

import argparse
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from loguru import logger as log

DEFAULT_COORDINATOR = "http://100.75.185.36:8100"
HEARTBEAT_INTERVAL = 30  # seconds
POLL_INTERVAL = 5        # seconds between job checks


# ── Hardware Detection ─────────────────────────────────────────

def detect_cpu_cores():
    return os.cpu_count() or 1


def detect_ram_gb():
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return round(kb / 1024 / 1024, 1)
    except Exception:
        pass
    # Fallback for non-Linux
    try:
        import psutil
        return round(psutil.virtual_memory().total / 1e9, 1)
    except ImportError:
        return 1.0


def detect_gpu():
    """Detect NVIDIA GPU via nvidia-smi."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            parts = result.stdout.strip().split(",")
            name = parts[0].strip()
            vram = float(parts[1].strip()) / 1024  # MB to GB
            return name, round(vram, 1)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None, None


def detect_ollama():
    """Check if Ollama is running."""
    try:
        r = requests.get("http://localhost:11434/api/tags", timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def detect_docker():
    """Check if Docker is available."""
    return shutil.which("docker") is not None


def get_tailscale_ip():
    """Get Tailscale IP if available."""
    try:
        result = subprocess.run(
            ["tailscale", "ip", "-4"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    # Fallback to hostname IP
    return socket.gethostbyname(socket.gethostname())


# ── Job Execution ──────────────────────────────────────────────

def execute_job(job, coordinator_url):
    """Execute a compute job based on its type."""
    job_type = job["job_type"]
    params = job.get("params", {})
    job_id = job["id"]

    log.info("Executing job #{id}: {type} — {name}", id=job_id, type=job_type, name=job["name"])

    try:
        if job_type == "HYPOTHESIS_TEST":
            return run_hypothesis_test(params)
        elif job_type == "REGIME_DETECT":
            return run_regime_detect(params)
        elif job_type == "LLM_INFERENCE":
            return run_llm_inference(params)
        elif job_type == "BACKTEST":
            return run_backtest(params)
        elif job_type == "FEATURE_COMPUTE":
            return run_feature_compute(params)
        elif job_type == "SIMULATION":
            return run_simulation(params)
        elif job_type == "DATA_PULL":
            return run_data_pull(params)
        else:
            return {"error": f"Unknown job type: {job_type}"}
    except Exception as e:
        log.error("Job #{id} failed: {e}", id=job_id, e=e)
        return {"error": str(e)}


def run_hypothesis_test(params):
    """Test a hypothesis against historical data."""
    hypothesis_id = params.get("hypothesis_id")
    feature_ids = params.get("feature_ids", [])
    lookback_days = params.get("lookback_days", 365)

    # Import GRID modules
    from db import execute_sql
    from store.pit import PITStore
    from db import get_engine
    from datetime import date, timedelta

    engine = get_engine()
    pit = PITStore(engine)

    end = date.today()
    start = end - timedelta(days=lookback_days)

    if not feature_ids:
        rows = execute_sql("SELECT feature_ids FROM hypothesis_registry WHERE id=%s", (hypothesis_id,))
        if rows:
            feature_ids = rows[0]["feature_ids"]

    if not feature_ids:
        return {"error": "No feature_ids found for hypothesis"}

    df = pit.get_feature_matrix(feature_ids, start, end, end)
    if df.empty:
        return {"error": "No data available for feature set"}

    # Basic statistical test
    from sklearn.preprocessing import StandardScaler
    import numpy as np

    df = df.ffill().bfill().dropna(axis=1, how="all").dropna()
    scaler = StandardScaler()
    X = scaler.fit_transform(df.values)

    # Compute basic stats
    means = np.mean(X, axis=0)
    stds = np.std(X, axis=0)
    correlations = np.corrcoef(X.T) if X.shape[1] > 1 else np.array([[1.0]])

    return {
        "output": {
            "hypothesis_id": hypothesis_id,
            "n_features": X.shape[1],
            "n_observations": X.shape[0],
            "date_range": [start.isoformat(), end.isoformat()],
            "feature_means": means.tolist(),
            "feature_stds": stds.tolist(),
            "max_correlation": float(np.max(np.abs(correlations - np.eye(correlations.shape[0])))) if correlations.shape[0] > 1 else 0,
        },
        "metrics": {
            "compute_time_ms": 0,  # filled by caller
        },
    }


def run_regime_detect(params):
    """Run regime detection as a compute job."""
    n_components = params.get("n_components", 4)
    start_date = params.get("start_date", "2024-04-01")

    from db import get_engine, execute_sql
    from store.pit import PITStore
    from datetime import date
    from sklearn.preprocessing import StandardScaler
    from sklearn.mixture import GaussianMixture
    import numpy as np

    engine = get_engine()
    pit = PITStore(engine)

    rows = execute_sql("SELECT id FROM feature_registry WHERE model_eligible=TRUE ORDER BY id")
    fids = [r["id"] for r in rows]

    df = pit.get_feature_matrix(fids, date.fromisoformat(start_date), date.today(), date.today())
    df = df.ffill().bfill().dropna(axis=1, how="all").dropna()

    scaler = StandardScaler()
    X = scaler.fit_transform(df.values)

    gmm = GaussianMixture(n_components=n_components, random_state=42, n_init=5)
    gmm.fit(X)
    labels = gmm.predict(X)
    probs = gmm.predict_proba(X)

    latest = int(labels[-1])
    confidence = float(np.max(probs[-1]))

    return {
        "output": {
            "latest_cluster": latest,
            "confidence": confidence,
            "n_features_used": X.shape[1],
            "n_observations": X.shape[0],
            "cluster_sizes": {str(k): int(v) for k, v in zip(*np.unique(labels, return_counts=True))},
            "bic": float(gmm.bic(X)),
        },
        "metrics": {"n_components": n_components},
    }


def run_llm_inference(params):
    """Run LLM inference via local Ollama."""
    model = params.get("model", "llama3.2")
    prompt = params.get("prompt", "")
    system_prompt = params.get("system_prompt", "You are GRID, a quantitative trading intelligence system.")

    if not prompt:
        return {"error": "No prompt provided"}

    r = requests.post(
        "http://localhost:11434/api/generate",
        json={"model": model, "prompt": prompt, "system": system_prompt, "stream": False},
        timeout=300,
    )
    r.raise_for_status()
    data = r.json()

    return {
        "output": {
            "model": model,
            "response": data.get("response", ""),
            "done": data.get("done", False),
        },
        "metrics": {
            "total_duration_ns": data.get("total_duration", 0),
            "eval_count": data.get("eval_count", 0),
        },
    }


def run_backtest(params):
    """Placeholder for backtest execution."""
    return {"output": {"status": "backtest_not_yet_implemented"}, "metrics": {}}


def run_feature_compute(params):
    """Placeholder for feature computation."""
    return {"output": {"status": "feature_compute_not_yet_implemented"}, "metrics": {}}


def run_simulation(params):
    """Placeholder for Monte Carlo simulation."""
    return {"output": {"status": "simulation_not_yet_implemented"}, "metrics": {}}


def run_data_pull(params):
    """Execute a data pull script."""
    script = params.get("script", "")
    if not script:
        return {"error": "No script specified"}

    script_path = Path(__file__).parent / script
    if not script_path.exists():
        return {"error": f"Script not found: {script}"}

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True, text=True, timeout=3600,
    )
    return {
        "output": {
            "stdout": result.stdout[-5000:],  # last 5K chars
            "stderr": result.stderr[-2000:],
            "returncode": result.returncode,
        },
        "metrics": {},
    }


# ── Main Loop ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GRID Compute Worker")
    parser.add_argument("--coordinator", default=DEFAULT_COORDINATOR, help="Coordinator URL")
    parser.add_argument("--max-concurrent", type=int, default=2, help="Max concurrent jobs")
    args = parser.parse_args()

    coordinator = args.coordinator.rstrip("/")
    log.info("GRID Worker starting — coordinator: {url}", url=coordinator)

    # Detect hardware
    cpu_cores = detect_cpu_cores()
    ram_gb = detect_ram_gb()
    gpu_model, gpu_vram = detect_gpu()
    has_ollama = detect_ollama()
    has_docker = detect_docker()
    hostname = socket.gethostname()
    ts_ip = get_tailscale_ip()

    log.info("Hardware: {cores} cores, {ram}GB RAM, GPU={gpu}, Ollama={oll}, Docker={dock}",
             cores=cpu_cores, ram=ram_gb,
             gpu=f"{gpu_model} ({gpu_vram}GB)" if gpu_model else "none",
             oll=has_ollama, dock=has_docker)

    # Register with coordinator
    try:
        r = requests.post(f"{coordinator}/workers/register", json={
            "hostname": hostname,
            "tailscale_ip": ts_ip,
            "cpu_cores": cpu_cores,
            "ram_gb": ram_gb,
            "gpu_model": gpu_model,
            "gpu_vram_gb": gpu_vram,
            "has_ollama": has_ollama,
            "has_docker": has_docker,
            "max_concurrent": args.max_concurrent,
        }, timeout=10)
        r.raise_for_status()
        worker = r.json()
        worker_id = worker["id"]
        log.info("Registered as worker #{id}", id=worker_id)
    except Exception as e:
        log.error("Failed to register with coordinator: {e}", e=e)
        sys.exit(1)

    # Main loop
    last_heartbeat = time.time()

    while True:
        try:
            # Heartbeat
            if time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
                try:
                    requests.post(f"{coordinator}/workers/{worker_id}/heartbeat", timeout=5)
                    last_heartbeat = time.time()
                except Exception:
                    log.warning("Heartbeat failed")

            # Try to claim a job
            try:
                r = requests.post(f"{coordinator}/jobs/claim", params={
                    "worker_id": worker_id,
                    "gpu_available": gpu_model is not None,
                    "ollama_available": has_ollama,
                }, timeout=10)
                r.raise_for_status()
                job = r.json()
            except Exception as e:
                log.debug("Claim failed: {e}", e=e)
                time.sleep(POLL_INTERVAL)
                continue

            if job.get("status") == "no_jobs":
                time.sleep(POLL_INTERVAL)
                continue

            job_id = job["id"]

            # Mark job as started
            try:
                requests.post(f"{coordinator}/jobs/{job_id}/start",
                              params={"worker_id": worker_id}, timeout=10)
            except Exception:
                pass

            # Execute
            start_time = time.time()
            result = execute_job(job, coordinator)
            elapsed_ms = int((time.time() - start_time) * 1000)

            # Report result
            output = result.get("output", {})
            metrics = result.get("metrics", {})
            metrics["compute_time_ms"] = elapsed_ms
            error = result.get("error")

            try:
                requests.post(f"{coordinator}/jobs/{job_id}/complete", json={
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "output": output,
                    "metrics": metrics,
                    "error": error,
                }, timeout=30)
            except Exception as e:
                log.error("Failed to report result for job #{id}: {e}", id=job_id, e=e)

            if error:
                log.warning("Job #{id} failed: {e}", id=job_id, e=error)
            else:
                log.info("Job #{id} completed in {ms}ms", id=job_id, ms=elapsed_ms)

        except KeyboardInterrupt:
            log.info("Worker shutting down")
            break
        except Exception as e:
            log.error("Worker loop error: {e}", e=e)
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
