#!/usr/bin/env python3
"""GRID — Distributed Compute Worker.

Runs on any Tailscale node. Auto-detects hardware (CPU, RAM, GPU, Ollama, Docker).
Registers with the coordinator, polls for jobs, executes them, reports results.

Default coordinator: http://100.75.185.36:8100

Run: python3 worker.py
     python3 worker.py --coordinator http://10.254.111.80:8100
"""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from loguru import logger as log
except ImportError:  # pragma: no cover - exercised on lightweight edge nodes
    import logging

    _base_logger = logging.getLogger("grid-worker")
    if not _base_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        _base_logger.addHandler(handler)
    _base_logger.setLevel(logging.INFO)

    class _LoggerAdapter:
        def __init__(self, logger):
            self._logger = logger

        def _fmt(self, message, **kwargs):
            if kwargs:
                try:
                    return message.format(**kwargs)
                except Exception:
                    return f"{message} {kwargs}"
            return message

        def info(self, message, **kwargs):
            self._logger.info(self._fmt(message, **kwargs))

        def warning(self, message, **kwargs):
            self._logger.warning(self._fmt(message, **kwargs))

        def error(self, message, **kwargs):
            self._logger.error(self._fmt(message, **kwargs))

        def debug(self, message, **kwargs):
            self._logger.debug(self._fmt(message, **kwargs))

    log = _LoggerAdapter(_base_logger)

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
    except Exception as exc:
        log.warning("Failed to read /proc/meminfo: {e}", e=exc)
    try:
        result = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            bytes_total = int(result.stdout.strip())
            return round(bytes_total / 1e9, 1)
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        pass
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory",
            ],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            bytes_total = int(result.stdout.strip())
            return round(bytes_total / 1e9, 1)
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        pass
    # Fallback for non-Linux
    try:
        import psutil
        return round(psutil.virtual_memory().total / 1e9, 1)
    except ImportError:
        return 1.0


def detect_gpu():
    """Detect NVIDIA GPU(s) via nvidia-smi.

    Aggregates ALL GPUs on the host:
      - name: the LARGEST card (used by coordinator for capability filtering).
      - vram: SUM of every card's VRAM in GB (true total available capacity).
    Hosts with multiple GPUs (gridz4, panda, ocr-node, grid-svr) previously
    under-reported by registering only the first row.
    """
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            rows = []
            for line in result.stdout.strip().splitlines():
                parts = [part.strip() for part in line.split(",", 1)]
                if len(parts) != 2:
                    continue
                try:
                    rows.append((parts[0], float(parts[1]) / 1024))
                except ValueError:
                    continue
            if rows:
                # Largest card name for capability filtering by coordinator.
                largest_name, _ = max(rows, key=lambda row: row[1])
                # Sum all VRAM so multi-GPU hosts report true total capacity.
                total_vram = sum(vram for _, vram in rows)
                if len(rows) > 1:
                    largest_name = f"{largest_name} (+{len(rows)-1} more)"
                return largest_name, round(total_vram, 1)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None, None


def detect_ollama():
    """Check if Ollama can actually generate with an installed model."""
    try:
        model = _resolve_ollama_model(os.environ.get("GRID_WORKER_OLLAMA_PROBE_MODEL", "llama3.2"))
        if os.environ.get("GRID_WORKER_SKIP_OLLAMA_GENERATE_PROBE") == "1":
            return True
        return _ollama_generation_probe(model)
    except Exception as exc:
        log.warning("Ollama generate probe failed: {e}", e=exc)
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


def _ollama_model_catalog():
    """Return installed Ollama model names on the local node."""
    r = requests.get("http://localhost:11434/api/tags", timeout=10)
    r.raise_for_status()
    payload = r.json()
    models = []
    for model in payload.get("models", []):
        name = model.get("name") or model.get("model")
        if name:
            models.append(name)
    return models


def _ollama_generation_probe(model: str) -> bool:
    payload = {
        "model": model,
        "prompt": "OK",
        "stream": False,
        "options": {"num_predict": 1},
    }
    r = requests.post("http://localhost:11434/api/generate", json=payload, timeout=20)
    r.raise_for_status()
    data = r.json()
    return bool(data.get("done") or data.get("response") is not None)


def _resolve_ollama_model(requested_model: str) -> str:
    """Resolve a usable local Ollama model name for the requested alias."""
    models = _ollama_model_catalog()
    if not models:
        raise RuntimeError("No Ollama models installed on local node")

    if requested_model in models:
        return requested_model

    latest_alias = f"{requested_model}:latest"
    if latest_alias in models:
        return latest_alias

    requested_base = requested_model.split(":", 1)[0]
    for name in models:
        if name.split(":", 1)[0] == requested_base:
            return name

    for name in models:
        if "embed" not in name and "embedding" not in name:
            return name

    return models[0]


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


def available_claim_slots(max_concurrent: int, active_jobs: int) -> int:
    max_workers = max(int(max_concurrent or 1), 1)
    return max(max_workers - active_jobs, 0)


def drain_finished_futures(active_futures):
    remaining = set()
    for future in active_futures:
        if future.done():
            try:
                future.result()
            except Exception as exc:
                log.error("Worker job future failed: {e}", e=exc)
        else:
            remaining.add(future)
    return remaining


def send_heartbeat(coordinator, worker_id, active_jobs=0):
    try:
        r = requests.post(
            f"{coordinator}/workers/{worker_id}/heartbeat",
            params={"active_jobs": active_jobs},
            timeout=5,
        )
        r.raise_for_status()
        return True
    except Exception as exc:
        log.warning("Heartbeat failed: {e}", e=exc)
        return False


def claim_next_job(coordinator, worker_id, gpu_available, ollama_available, exclude_types):
    r = requests.post(f"{coordinator}/jobs/claim", params={
        "worker_id": worker_id,
        "gpu_available": gpu_available,
        "ollama_available": ollama_available,
        "exclude_types": exclude_types,
    }, timeout=10)
    r.raise_for_status()
    return r.json()


def run_claimed_job(job, coordinator, worker_id):
    job_id = job["id"]

    try:
        requests.post(f"{coordinator}/jobs/{job_id}/start",
                      params={"worker_id": worker_id}, timeout=10)
    except Exception as exc:
        log.warning("Failed to mark job #{j} as started: {e}", j=job_id, e=exc)

    start_time = time.time()
    result = execute_job(job, coordinator)
    elapsed_ms = int((time.time() - start_time) * 1000)

    output = result.get("output", {})
    metrics = result.get("metrics", {})
    metrics["compute_time_ms"] = elapsed_ms
    error = result.get("error")

    payload = {
        "job_id": job_id,
        "worker_id": worker_id,
        "output": output,
        "metrics": metrics,
        "error": error,
    }
    reported = False
    for attempt in range(1, 4):
        try:
            r = requests.post(f"{coordinator}/jobs/{job_id}/complete", json=payload, timeout=30)
            r.raise_for_status()
            reported = True
            break
        except Exception as exc:
            log.error("Failed to report result for job #{id} attempt {a}: {e}",
                      id=job_id, a=attempt, e=exc)
            time.sleep(min(attempt * 2, 5))

    if error:
        log.warning("Job #{id} failed: {e}", id=job_id, e=error)
    elif reported:
        log.info("Job #{id} completed in {ms}ms", id=job_id, ms=elapsed_ms)

    return reported


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

    fids = requested_feature_ids(params)
    if not fids:
        rows = execute_sql("SELECT id FROM feature_registry WHERE model_eligible=TRUE ORDER BY id LIMIT 50")
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


def requested_feature_ids(params):
    feature_ids = []
    seen = set()
    for value in params.get("feature_ids") or []:
        try:
            feature_id = int(value)
        except (TypeError, ValueError):
            continue
        if feature_id and feature_id not in seen:
            seen.add(feature_id)
            feature_ids.append(feature_id)
    return feature_ids


def run_llm_inference(params):
    """Run LLM inference via local Ollama."""
    requested_model = params.get("model", "llama3.2")
    prompt = params.get("prompt", "")
    system_prompt = params.get("system_prompt", (
        "You are GRID, an internal trading intelligence system. "
        "Cite only data present in the prompt — never invent numbers or sources. "
        "Output format: ANALYSIS (findings) → CONFIDENCE (high/medium/low with reasoning) → SOURCES (which input data supported each claim)."
    ))

    if not prompt:
        return {"error": "No prompt provided"}

    model = _resolve_ollama_model(requested_model)
    r = requests.post(
        "http://localhost:11434/api/generate",
        json={"model": model, "prompt": prompt, "system": system_prompt, "stream": False},
        timeout=300,
    )
    r.raise_for_status()
    data = r.json()
    response_text = data.get("response", "")
    try:
        from llm.feedback_loop import log_llm_call

        log_llm_call(
            module="grid_worker.llm_inference",
            tier="worker",
            system_prompt=system_prompt[:2000],
            user_prompt=prompt[:2000],
            output=response_text[:2000],
            context_tokens=data.get("prompt_eval_count", 0) or 0,
            output_tokens=data.get("eval_count", 0) or 0,
            latency_ms=int(data.get("total_duration", 0) / 1_000_000)
            if data.get("total_duration") else 0,
            model=model,
            provider="ollama",
            metadata={
                "endpoint": "http://localhost:11434/api/generate",
                "requested_model": requested_model,
            },
        )
    except Exception:
        pass

    return {
        "output": {
            "model": model,
            "requested_model": requested_model,
            "response": response_text,
            "done": data.get("done", False),
        },
        "metrics": {
            "total_duration_ns": data.get("total_duration", 0),
            "eval_count": data.get("eval_count", 0),
        },
    }


def run_backtest(params):
    """Walk-forward backtest. Branches on params['kind']."""
    kind = params.get("kind", "model_walkforward")
    if kind == "rotation_variant":
        return _run_rotation_variant(params)
    return _run_model_walkforward(params)


def _run_rotation_variant(params):
    from datetime import date as _date
    from db import get_engine
    from alpha_research.strategies.rotation_variant_backtest import (
        backtest_rotation_variant, RotationConfig,
    )
    tunables = (
        "TREND_WEEKS", "VIX_ZSCORE_THRESHOLD", "DRAWDOWN_THRESHOLD",
        "DRAWDOWN_WINDOW", "FAST_RISK_OFF_DURATION", "FAST_RISK_OFF_CASH_FLOOR",
        "RANKING_WEEKS", "ABSOLUTE_STOP", "TRAILING_STOP", "COOLDOWN_DAYS",
        "MAX_ACTIVE_GROUPS",
    )
    cfg_kwargs = {k: params[k] for k in tunables if k in params}
    cfg = RotationConfig(**cfg_kwargs)
    ws = _date.fromisoformat(params.get("window_start", "2024-01-01"))
    we = _date.fromisoformat(params.get("window_end", "2026-03-25"))
    metrics = backtest_rotation_variant(get_engine(), cfg, ws, we)
    if "error" in metrics:
        return {"error": metrics["error"]}
    return {"output": metrics, "metrics": {
        "total_return": metrics.get("total_return"),
        "sharpe": metrics.get("sharpe"),
        "max_drawdown": metrics.get("max_drawdown"),
        "alpha_vs_benchmark": metrics.get("alpha_vs_benchmark"),
        "rebalance_count": metrics.get("rebalance_count"),
        "config_id": metrics.get("config_id"),
        "backtest_run_id": metrics.get("backtest_run_id"),
    }}


def _run_model_walkforward(params):

    """Run walk-forward backtest for a model/hypothesis.

    Parameters (from params dict):
        model_id: Model registry ID to backtest.
        n_splits: Number of walk-forward splits (default 5).
        train_pct: Training set fraction (default 0.7).
    """
    from datetime import date, timedelta
    from db import get_engine
    from store.pit import PITStore
    import numpy as np

    model_id = params.get("model_id")
    n_splits = params.get("n_splits", 5)
    train_pct = params.get("train_pct", 0.7)

    engine = get_engine()
    pit = PITStore(engine)

    # Get model's feature set
    from sqlalchemy import text
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT feature_set, parameter_snapshot FROM model_registry WHERE id = :id"),
            {"id": model_id},
        ).fetchone()

    if row is None:
        return {"error": f"Model {model_id} not found"}

    feature_ids = row[0] or []
    if not feature_ids:
        return {"error": "Model has no feature set"}

    # Get feature matrix
    end = date.today()
    start = end - timedelta(days=365 * 5)
    matrix = pit.get_feature_matrix(feature_ids, start, end, end)
    matrix = matrix.ffill().bfill().dropna(axis=1, how="all").dropna()

    if matrix.shape[0] < 100:
        return {"error": f"Insufficient data: {matrix.shape[0]} rows (need 100+)"}

    # Walk-forward splits
    n = len(matrix)
    split_size = n // n_splits
    results = []

    for i in range(n_splits):
        split_start = i * split_size
        split_end = min((i + 1) * split_size, n)
        train_end = split_start + int((split_end - split_start) * train_pct)

        train = matrix.iloc[split_start:train_end]
        test = matrix.iloc[train_end:split_end]

        if len(test) < 5:
            continue

        # Simple mean-reversion signal as baseline
        train_mean = train.mean()
        train_std = train.std().replace(0, np.nan)
        test_zscore = (test - train_mean) / train_std

        # Score: average absolute z-score (higher = more extreme = more signal)
        avg_signal = float(test_zscore.abs().mean().mean())
        results.append({
            "split": i + 1,
            "train_rows": len(train),
            "test_rows": len(test),
            "avg_signal_strength": round(avg_signal, 4),
            "train_dates": [str(train.index[0].date()), str(train.index[-1].date())],
            "test_dates": [str(test.index[0].date()), str(test.index[-1].date())],
        })

    return {
        "output": {
            "model_id": model_id,
            "n_splits": n_splits,
            "n_features": matrix.shape[1],
            "total_observations": n,
            "splits": results,
            "avg_signal_across_splits": round(
                np.mean([r["avg_signal_strength"] for r in results]), 4
            ) if results else 0,
        },
        "metrics": {},
    }


def run_feature_compute(params):
    """Compute derived features for a set of base features.

    Parameters (from params dict):
        feature_ids: List of feature registry IDs.
        transformations: List of transforms to apply (default all).
        as_of_date: Date string (default today).
    """
    from datetime import date
    from db import get_engine
    from store.pit import PITStore
    from features.lab import zscore_normalize, rolling_slope, pct_change_lagged

    feature_ids = params.get("feature_ids", [])
    as_of_str = params.get("as_of_date")
    as_of = date.fromisoformat(as_of_str) if as_of_str else date.today()

    if not feature_ids:
        return {"error": "No feature_ids provided"}

    engine = get_engine()
    pit = PITStore(engine)

    from datetime import timedelta
    start = as_of - timedelta(days=365 * 3)
    matrix = pit.get_feature_matrix(feature_ids, start, as_of, as_of)
    matrix = matrix.ffill().bfill().dropna(axis=1, how="all").dropna()

    if matrix.empty:
        return {"error": "No data available for features"}

    computed: dict = {}
    for col in matrix.columns:
        series = matrix[col]
        computed[f"{col}_zscore"] = round(float(zscore_normalize(series).iloc[-1]), 4) if len(series) > 252 else None
        computed[f"{col}_slope"] = round(float(rolling_slope(series).iloc[-1]), 4) if len(series) > 63 else None
        computed[f"{col}_pct_21d"] = round(float(pct_change_lagged(series, 21).iloc[-1]), 4) if len(series) > 21 else None

    # Remove None values
    computed = {k: v for k, v in computed.items() if v is not None}

    return {
        "output": {
            "as_of_date": as_of.isoformat(),
            "n_base_features": matrix.shape[1],
            "n_derived_features": len(computed),
            "features": computed,
        },
        "metrics": {},
    }


def run_simulation(params):
    """Run Monte Carlo simulation of portfolio paths under current regime.

    Parameters (from params dict):
        n_paths: Number of simulation paths (default 1000).
        horizon_days: Forward horizon in trading days (default 63).
        feature_ids: Features to use for volatility estimation.
    """
    from datetime import date, timedelta
    from db import get_engine
    from store.pit import PITStore
    import numpy as np

    n_paths = params.get("n_paths", 1000)
    horizon = params.get("horizon_days", 63)
    feature_ids = params.get("feature_ids", [])

    engine = get_engine()

    # Get eligible features if none specified
    if not feature_ids:
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id FROM feature_registry WHERE model_eligible = TRUE ORDER BY id LIMIT 10")
            ).fetchall()
        feature_ids = [r[0] for r in rows]

    if not feature_ids:
        return {"error": "No features available for simulation"}

    pit = PITStore(engine)
    end = date.today()
    start = end - timedelta(days=365 * 2)
    matrix = pit.get_feature_matrix(feature_ids, start, end, end)
    matrix = matrix.ffill().bfill().dropna(axis=1, how="all").dropna()

    if matrix.shape[0] < 60:
        return {"error": f"Insufficient history: {matrix.shape[0]} rows (need 60+)"}

    # Estimate daily returns and volatility from feature changes
    returns = matrix.pct_change().dropna()
    avg_return = float(returns.mean().mean())
    avg_vol = float(returns.std().mean())

    # Monte Carlo paths
    np.random.seed(42)
    paths = np.zeros((n_paths, horizon))
    paths[:, 0] = 1.0  # Start at $1

    for t in range(1, horizon):
        daily_return = np.random.normal(avg_return, avg_vol, n_paths)
        paths[:, t] = paths[:, t - 1] * (1 + daily_return)

    # Statistics
    final_values = paths[:, -1]
    percentiles = {
        "p5": round(float(np.percentile(final_values, 5)), 4),
        "p25": round(float(np.percentile(final_values, 25)), 4),
        "p50": round(float(np.percentile(final_values, 50)), 4),
        "p75": round(float(np.percentile(final_values, 75)), 4),
        "p95": round(float(np.percentile(final_values, 95)), 4),
    }

    return {
        "output": {
            "n_paths": n_paths,
            "horizon_days": horizon,
            "n_features_used": matrix.shape[1],
            "estimated_daily_return": round(avg_return, 6),
            "estimated_daily_vol": round(avg_vol, 6),
            "annualized_vol": round(avg_vol * np.sqrt(252), 4),
            "terminal_value_percentiles": percentiles,
            "prob_loss": round(float((final_values < 1.0).mean()), 4),
            "expected_value": round(float(final_values.mean()), 4),
            "max_drawdown_median_path": round(float(
                1 - np.min(paths[n_paths // 2]) / np.max(paths[n_paths // 2])
            ), 4),
        },
        "metrics": {},
    }


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
    parser.add_argument(
        "--hostname",
        default=None,
        help="Override the hostname used for coordinator registration.",
    )
    parser.add_argument(
        "--heartbeat-only",
        action="store_true",
        help="Register and heartbeat without claiming jobs; useful for fresh node bootstrap.",
    )
    parser.add_argument(
        "--exclude-types",
        default="HUMAN_LLM_QUERY",
        help="Comma-separated job types to skip while claiming.",
    )
    args = parser.parse_args()

    coordinator = args.coordinator.rstrip("/")
    log.info("GRID Worker starting — coordinator: {url}", url=coordinator)

    # Detect hardware
    cpu_cores = detect_cpu_cores()
    ram_gb = detect_ram_gb()
    gpu_model, gpu_vram = detect_gpu()
    has_ollama = detect_ollama()
    has_docker = detect_docker()
    hostname = args.hostname or socket.gethostname()
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
    max_workers = max(int(args.max_concurrent or 1), 1)
    active_futures = set()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            try:
                active_futures = drain_finished_futures(active_futures)

                # Heartbeat from the main thread even while jobs are running.
                if time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
                    if send_heartbeat(coordinator, worker_id, active_jobs=len(active_futures)):
                        last_heartbeat = time.time()

                if args.heartbeat_only:
                    time.sleep(POLL_INTERVAL)
                    continue

                slots = available_claim_slots(max_workers, len(active_futures))
                for _ in range(slots):
                    try:
                        job = claim_next_job(
                            coordinator,
                            worker_id,
                            gpu_model is not None,
                            has_ollama,
                            args.exclude_types,
                        )
                    except Exception as e:
                        log.debug("Claim failed: {e}", e=e)
                        break

                    if job.get("status") in {"no_jobs", "no_capacity"}:
                        break

                    active_futures.add(executor.submit(run_claimed_job, job, coordinator, worker_id))

                time.sleep(1 if active_futures else POLL_INTERVAL)

            except KeyboardInterrupt:
                log.info("Worker shutting down")
                break
            except Exception as e:
                log.error("Worker loop error: {e}", e=e)
                time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
