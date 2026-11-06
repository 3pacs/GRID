#!/usr/bin/env python3
"""GRID — Distributed Compute Worker.

Runs on any Tailscale node. Auto-detects hardware (CPU, RAM, GPU, Ollama, Docker).
Registers with the coordinator, polls for jobs, executes them, reports results.

Default coordinator: http://100.75.185.36:8100

Run: python3 worker.py
     python3 worker.py --coordinator http://10.254.111.80:8100
"""

from __future__ import annotations

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


OLLAMA_URL = "http://localhost:11434"
OLLAMA_PROBE_MODEL_ENV = "GRID_WORKER_OLLAMA_PROBE_MODEL"
OLLAMA_GENERATE_PROBE_ENV = "GRID_WORKER_OLLAMA_GENERATE_PROBE"
OLLAMA_PROBE_TIMEOUT_ENV = "GRID_WORKER_OLLAMA_PROBE_TIMEOUT"
#: A cold load of a 27-32B model takes ~25 s (qwen3.8:27b on grid-svr,
#: 2026-11-06: load_duration 25.3 s, total 30.5 s). The old fixed 20 s
#: generate timeout therefore failed on every node, and has_ollama read
#: false fleet-wide while Ollama was serving.
DEFAULT_OLLAMA_GENERATE_TIMEOUT_S = 120.0


def _is_embedding_model(name: str) -> bool:
    lowered = name.lower()
    return "embed" in lowered


def _configured_probe_models() -> list[str]:
    """Models named in GRID_WORKER_OLLAMA_PROBE_MODEL (comma-separated)."""
    raw = os.environ.get(OLLAMA_PROBE_MODEL_ENV, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _match_installed_model(requested: str, installed: list[str]) -> str | None:
    """Exact name, then ``name:latest``, then the same base name with any tag."""
    if requested in installed:
        return requested
    if f"{requested}:latest" in installed:
        return f"{requested}:latest"
    base = requested.split(":", 1)[0]
    for name in installed:
        if name.split(":", 1)[0] == base:
            return name
    return None


def _ollama_probe_timeout() -> float:
    try:
        value = float(os.environ.get(OLLAMA_PROBE_TIMEOUT_ENV, ""))
    except ValueError:
        return DEFAULT_OLLAMA_GENERATE_TIMEOUT_S
    return value if value > 0 else DEFAULT_OLLAMA_GENERATE_TIMEOUT_S


def probe_ollama() -> dict:
    """Probe the local Ollama server; never raises.

    ``available`` is true when the server answers ``/api/tags`` and has a
    usable model: one of ``GRID_WORKER_OLLAMA_PROBE_MODEL`` (comma-separated)
    when that is set, otherwise any installed non-embedding model (embedding
    models cannot serve LLM_INFERENCE). A generation round-trip is opt-in
    (``GRID_WORKER_OLLAMA_GENERATE_PROBE=1``) because it loads the model into
    memory on every worker start; its timeout is
    ``GRID_WORKER_OLLAMA_PROBE_TIMEOUT`` seconds (default 120).
    """
    result = {
        "available": False,
        "reachable": False,
        "models": [],
        "probe_model": None,
        "reason": "",
    }
    try:
        models = _ollama_model_catalog()
    except (requests.RequestException, OSError, ValueError) as exc:
        result["reason"] = f"unreachable: {type(exc).__name__}"
        return result
    result["reachable"] = True
    result["models"] = models

    wanted = _configured_probe_models()
    if wanted:
        matches = [m for m in (_match_installed_model(w, models) for w in wanted) if m]
        probe_model = matches[0] if matches else None
        if probe_model is None:
            result["reason"] = f"none of {wanted} installed"
            return result
    else:
        chat_models = [m for m in models if not _is_embedding_model(m)]
        probe_model = chat_models[0] if chat_models else None
        if probe_model is None:
            result["reason"] = "no non-embedding model installed"
            return result
    result["probe_model"] = probe_model

    if os.environ.get(OLLAMA_GENERATE_PROBE_ENV) == "1":
        try:
            if not _ollama_generation_probe(probe_model, timeout=_ollama_probe_timeout()):
                result["reason"] = f"generate probe on {probe_model} returned no response"
                return result
        except (requests.RequestException, OSError, ValueError) as exc:
            result["reason"] = f"generate probe on {probe_model} failed: {type(exc).__name__}"
            return result

    result["available"] = True
    return result


def detect_ollama():
    """True when local Ollama is reachable and has a usable model."""
    probe = probe_ollama()
    if not probe["available"]:
        log.warning("Ollama unavailable: {r}", r=probe["reason"])
    return probe["available"]


def detect_docker():
    """Check if Docker is available."""
    return shutil.which("docker") is not None


TAILSCALE_IP_ENV = "GRID_WORKER_TAILSCALE_IP"


def _is_loopback(address: str) -> bool:
    return address.startswith("127.") or address in {"::1", "localhost", ""}


def _route_source_ip(coordinator_url: str | None) -> str | None:
    """Local address the kernel would use to reach the coordinator.

    ``connect`` on a UDP socket only selects a route; no packet is sent. When
    the coordinator is a tailnet address this is the worker's tailnet IP.
    """
    if not coordinator_url:
        return None
    from urllib.parse import urlparse

    parsed = urlparse(coordinator_url)
    host = parsed.hostname
    if not host or _is_loopback(host):
        return None
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((host, parsed.port or 80))
            address = sock.getsockname()[0]
    except OSError:
        return None
    return None if _is_loopback(address) else address


def get_tailscale_ip(coordinator_url: str | None = None) -> str:
    """Return a coordinator-reachable address for this node, never loopback.

    Order: ``GRID_WORKER_TAILSCALE_IP``, ``tailscale ip -4``, the route source
    address toward the coordinator, the hostname's resolved address, and
    finally the bare hostname (tailnet MagicDNS resolves it). Debian maps the
    hostname to 127.0.1.1, which is how forge registered as 127.0.1.1.
    """
    override = os.environ.get(TAILSCALE_IP_ENV, "").strip()
    if override:
        return override
    try:
        result = subprocess.run(
            ["tailscale", "ip", "-4"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            address = result.stdout.strip().splitlines()[0].strip()
            if not _is_loopback(address):
                return address
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass
    routed = _route_source_ip(coordinator_url)
    if routed:
        return routed
    hostname = socket.gethostname()
    try:
        address = socket.gethostbyname(hostname)
    except OSError:
        address = ""
    if not _is_loopback(address):
        return address
    return hostname


def _ollama_model_catalog():
    """Return installed Ollama model names on the local node."""
    r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=10)
    r.raise_for_status()
    payload = r.json()
    models = []
    for model in payload.get("models", []):
        name = model.get("name") or model.get("model")
        if name:
            models.append(name)
    return models


def _ollama_generation_probe(model: str, timeout: float = DEFAULT_OLLAMA_GENERATE_TIMEOUT_S) -> bool:
    payload = {
        "model": model,
        "prompt": "OK",
        "stream": False,
        "options": {"num_predict": 1},
    }
    r = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return bool(data.get("done") or data.get("response") is not None)


def _resolve_ollama_model(requested_model: str) -> str:
    """Resolve a usable local Ollama model name for the requested alias."""
    models = _ollama_model_catalog()
    if not models:
        raise RuntimeError("No Ollama models installed on local node")

    matched = _match_installed_model(requested_model, models)
    if matched:
        return matched

    for name in models:
        if not _is_embedding_model(name):
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
        elif job_type == "KILL_PREDICTOR_SCORE":
            return run_kill_predictor_score(params)
        elif job_type == "EMBEDDING_BATCH":
            return run_embedding_batch(params)
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


#: Seconds to wait for the coordinator to acknowledge a completion report.
#: 30 s produced a daily stream of "Read timed out" followed by 400s: the
#: coordinator had recorded the result, the retry then hit an invalid
#: COMPLETED -> COMPLETED transition.
COMPLETE_TIMEOUT_S = 90

#: Job states in which a completion report has already been recorded.
TERMINAL_JOB_STATES = frozenset({"COMPLETED", "FAILED", "VALID", "ASSIMILATED"})


def job_already_recorded(coordinator, job_id):
    """True when the coordinator already holds a terminal state for *job_id*.

    Used after a failed ``/complete`` call: a read timeout or a 400 on retry
    usually means the first report landed and the retry is redundant.
    """
    try:
        r = requests.get(f"{coordinator}/jobs/{job_id}", timeout=10)
        r.raise_for_status()
        state = str((r.json() or {}).get("state", "")).upper()
        return state in TERMINAL_JOB_STATES
    except Exception as exc:  # noqa: BLE001 — best-effort probe
        log.debug("Could not read job #{j} state: {e}", j=job_id, e=exc)
        return False


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
            r = requests.post(f"{coordinator}/jobs/{job_id}/complete", json=payload,
                              timeout=COMPLETE_TIMEOUT_S)
            r.raise_for_status()
            reported = True
            break
        except Exception as exc:
            if job_already_recorded(coordinator, job_id):
                log.info("Job #{id} result already recorded by coordinator (attempt {a}: {e})",
                         id=job_id, a=attempt, e=exc)
                reported = True
                break
            level = log.warning if attempt < 3 else log.error
            level("Failed to report result for job #{id} attempt {a}: {e}",
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


#: Directory of hash-receipted PIT window snapshots (``pit-window-<sha>.json.gz``,
#: built with ``python -m alpha_research.strategies.rotation_variant_backtest
#: --build-snapshot DIR``). Unset = off: every job reads its window from the
#: database. Set on remote workers so a rotation job reads no resolved_series
#: rows over the relay (W2-7). A job may pin one with ``pit_snapshot_sha256``.
PIT_SNAPSHOT_DIR_ENV = "GRID_PIT_SNAPSHOT_DIR"


def rotation_snapshot_for(params, window_start, window_end):
    """The verified snapshot to run *params* from, or None for the DB path."""
    snapshot_dir = os.environ.get(PIT_SNAPSHOT_DIR_ENV, "").strip()
    if not snapshot_dir:
        return None
    from alpha_research.strategies.rotation_variant_backtest import (
        find_rotation_snapshot,
    )

    sha = params.get("pit_snapshot_sha256") or None
    cache = find_rotation_snapshot(snapshot_dir, window_start, window_end, sha256=sha)
    if cache is None:
        log.warning(
            "No PIT snapshot in {d} covers {s}..{e} (sha={h}); reading the database",
            d=snapshot_dir, s=window_start, e=window_end, h=sha,
        )
    return cache


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
    pit = rotation_snapshot_for(params, ws, we)
    metrics = backtest_rotation_variant(get_engine(), cfg, ws, we, pit=pit)
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
        "data_source": (metrics.get("data_source") or {}).get("mode"),
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
    ollama_probe = probe_ollama()
    has_ollama = ollama_probe["available"]
    if not has_ollama:
        log.warning("Ollama unavailable: {r}", r=ollama_probe["reason"])
    has_docker = detect_docker()
    hostname = args.hostname or socket.gethostname()
    ts_ip = get_tailscale_ip(coordinator)

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
            "ollama_models": ollama_probe["models"],
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



# ── KILL_PREDICTOR_SCORE handler (task #50, 2026-05-16) ─────────────────
# Claims KILL_PREDICTOR_SCORE jobs, POSTs the thesis to the kill-predictor
# ASIC (default http://koala:8090/score), and UPSERTs the result into
# hypothesis_asic_decisions with source='backfill_v1' so we can distinguish
# coordinator-driven backfills from live hermes_operator calls.
KILL_PREDICTOR_URL = os.environ.get(
    "GRID_KILL_PREDICTOR_URL", "http://koala:8090/score"
)
KILL_PREDICTOR_TIMEOUT = float(os.environ.get("GRID_KILL_PREDICTOR_TIMEOUT", "10"))
KILL_PREDICTOR_THRESHOLD = float(os.environ.get("GRID_KILL_PREDICTOR_THRESHOLD", "0.5"))


def run_kill_predictor_score(params):
    """Score a hypothesis via the kill-predictor ASIC and persist the decision.

    Expected params:
        hypothesis_id (or hypothesis_uuid): text id of a row in
            discovered_hypotheses.
        source: optional override (default 'backfill_v1').
        predictor_version: optional override (default 'v1').
    """
    import psycopg2
    import psycopg2.extras

    hypothesis_id = (
        params.get("hypothesis_id")
        or params.get("hypothesis_uuid")
        or params.get("id")
    )
    if not hypothesis_id:
        return {"error": "missing hypothesis_id in params"}

    source = params.get("source") or "backfill_v1"
    predictor_version = params.get("predictor_version") or "v1"

    # Fetch thesis row.
    pg_dsn = (
        "host=" + os.environ.get("PG_HOST", "100.75.185.36")
        + " port=" + os.environ.get("PG_PORT", "5432")
        + " dbname=" + os.environ.get("PG_DATABASE", "griddb")
        + " user=" + os.environ.get("PG_USER", "grid")
        + " password=" + (os.environ.get("PG_PASSWORD") or os.environ["GRID_DB_PASSWORD"])
    )
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = True
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT id, thesis, pattern_type, evidence, invalidation, confidence, "
            "       status, times_tested, times_correct "
            "FROM discovered_hypotheses WHERE id=%s",
            (hypothesis_id,),
        )
        row = cur.fetchone()
        if not row:
            return {"error": f"hypothesis_id {hypothesis_id} not found"}

        evidence = row.get("evidence") or {}
        try:
            evidence_keys_count = len(evidence) if isinstance(evidence, dict) else 0
        except Exception:
            evidence_keys_count = 0

        payload = {
            "thesis": row["thesis"] or "",
            "pattern_type": row.get("pattern_type") or "unknown",
            "times_tested": int(row.get("times_tested") or 0),
            "times_correct": int(row.get("times_correct") or 0),
            "confidence": float(row.get("confidence") or 0.5),
            "evidence_keys_count": evidence_keys_count,
        }

        try:
            resp = requests.post(
                KILL_PREDICTOR_URL,
                json=payload,
                timeout=KILL_PREDICTOR_TIMEOUT,
            )
            resp.raise_for_status()
            scored = resp.json()
        except Exception as e:
            return {"error": f"kill-predictor POST failed: {e}"}

        kill_prob = scored.get("kill_probability")
        if kill_prob is None:
            return {"error": f"kill-predictor returned no kill_probability: {scored}"}
        kill_prob = float(kill_prob)
        decision = "KILL" if kill_prob >= KILL_PREDICTOR_THRESHOLD else "KEEP"
        # Honour a kill-predictor-supplied `decision` if present, else use threshold.
        if scored.get("decision"):
            decision = str(scored["decision"]).upper()

        cur.execute(
            """
            INSERT INTO hypothesis_asic_decisions
                (hypothesis_id, decided_at, asic_kill_prob, decision,
                 predictor_version, source)
            VALUES (%s, NOW(), %s, %s, %s, %s)
            ON CONFLICT (hypothesis_id) DO UPDATE
              SET decided_at = EXCLUDED.decided_at,
                  asic_kill_prob = EXCLUDED.asic_kill_prob,
                  decision = EXCLUDED.decision,
                  predictor_version = EXCLUDED.predictor_version,
                  source = EXCLUDED.source
            """,
            (hypothesis_id, kill_prob, decision, predictor_version, source),
        )
    finally:
        conn.close()

    return {
        "output": {
            "hypothesis_id": hypothesis_id,
            "kill_probability": kill_prob,
            "decision": decision,
            "predictor_version": predictor_version,
            "source": source,
            "model_version": scored.get("model_version"),
            "logit": scored.get("logit"),
            "latency_ms": scored.get("latency_ms"),
        },
        "metrics": {
            "compute_time_ms": 0,  # filled by caller
            "kill_predictor_latency_ms": scored.get("latency_ms"),
        },
    }




# ── EMBEDDING_BATCH handler (task #49, 2026-05-16) ──────────────────────
# Claims EMBEDDING_BATCH jobs from the coordinator and enqueues the
# requested (source_type, source_id) rows into embedding_queue. The four
# embed workers (grid-svr P1000, koala TITAN X x2, ocr-node 2070 Super)
# continue to drain embedding_queue directly; this handler is purely an
# additional way to ENQUEUE work, not consume it.
#
# Payload shapes (either form is valid):
#   {"items": [
#       {"source_type": "news_article", "source_id": "123"},
#       {"source_type": "actor", "source_id": "foo"}],
#    "priority": 10}
#
#   {"predicate": {
#       "source_type": "news_article",
#       "since": "2026-05-15T00:00:00Z",  # optional
#       "until": "2026-05-16T00:00:00Z",  # optional
#       "limit": 1000                     # required, capped at 100000
#    },
#    "priority": 10}
#
# Returns {"output": {"enqueued": N, "skipped_existing": M, ...}}.

EMBEDDING_BATCH_MAX_LIMIT = int(os.environ.get("GRID_EMBED_BATCH_MAX_LIMIT", "100000"))
EMBEDDING_BATCH_MAX_ITEMS = int(os.environ.get("GRID_EMBED_BATCH_MAX_ITEMS", "50000"))

# Map source_type -> (table_name, id_column, timestamp_column_or_None)
# Used for predicate-form queries. Add new source types here.
EMBEDDING_BATCH_PREDICATE_TABLES = {
    "news_article": ("news_articles", "id", "published_at"),
    "actor":        ("actors", "id", "updated_at"),
    "signal_data":  ("signal_data", "id", "created_at"),
    "sec_fact":     ("sec_material_facts", "id", "created_at"),
}


def _pg_connect_for_embedding_batch():
    import psycopg2
    pg_dsn = (
        "host=" + os.environ.get("PG_HOST", "100.75.185.36")
        + " port=" + os.environ.get("PG_PORT", "5432")
        + " dbname=" + os.environ.get("PG_DATABASE", "griddb")
        + " user=" + os.environ.get("PG_USER", "grid")
        + " password=" + (os.environ.get("PG_PASSWORD") or os.environ["GRID_DB_PASSWORD"])
    )
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = False
    return conn


def run_embedding_batch(params):
    """Enqueue a batch of items into embedding_queue.

    Either `items` or `predicate` must be provided. `priority` is optional
    (default 0, same as the timer-driven enqueuer).
    """
    import psycopg2  # noqa: F401  (ensure driver present)
    import psycopg2.extras

    items = params.get("items")
    predicate = params.get("predicate")
    priority = int(params.get("priority") or 0)

    if not items and not predicate:
        return {"error": "EMBEDDING_BATCH requires either 'items' or 'predicate' in params"}
    if items and predicate:
        return {"error": "EMBEDDING_BATCH accepts 'items' OR 'predicate', not both"}

    rows_to_insert = []
    predicate_meta = None

    conn = _pg_connect_for_embedding_batch()
    try:
        cur = conn.cursor()

        if items:
            if not isinstance(items, list):
                return {"error": "'items' must be a list of {source_type, source_id} objects"}
            if len(items) > EMBEDDING_BATCH_MAX_ITEMS:
                return {
                    "error": (
                        f"'items' length {len(items)} exceeds max "
                        f"{EMBEDDING_BATCH_MAX_ITEMS} (set GRID_EMBED_BATCH_MAX_ITEMS to override)"
                    )
                }
            for it in items:
                if not isinstance(it, dict):
                    return {"error": f"item is not an object: {it!r}"}
                st = it.get("source_type")
                sid = it.get("source_id")
                if not st or sid is None:
                    return {"error": f"item missing source_type/source_id: {it!r}"}
                rows_to_insert.append((str(st), str(sid)))

        else:  # predicate form
            if not isinstance(predicate, dict):
                return {"error": "'predicate' must be an object"}
            st = predicate.get("source_type")
            if st not in EMBEDDING_BATCH_PREDICATE_TABLES:
                return {
                    "error": (
                        f"predicate.source_type '{st}' not supported. "
                        f"Known: {sorted(EMBEDDING_BATCH_PREDICATE_TABLES)}"
                    )
                }
            limit = predicate.get("limit")
            if not isinstance(limit, int) or limit <= 0:
                return {"error": "predicate.limit must be a positive integer"}
            if limit > EMBEDDING_BATCH_MAX_LIMIT:
                limit = EMBEDDING_BATCH_MAX_LIMIT

            table, id_col, ts_col = EMBEDDING_BATCH_PREDICATE_TABLES[st]
            sql_parts = [f"SELECT {id_col}::text FROM {table}"]
            where = []
            sql_params = []
            since = predicate.get("since")
            until = predicate.get("until")
            if since and ts_col:
                where.append(f"{ts_col} >= %s")
                sql_params.append(since)
            if until and ts_col:
                where.append(f"{ts_col} < %s")
                sql_params.append(until)
            if where:
                sql_parts.append("WHERE " + " AND ".join(where))
            order_col = ts_col or id_col
            sql_parts.append(f"ORDER BY {order_col} DESC NULLS LAST LIMIT %s")
            sql_params.append(int(limit))
            cur.execute(" ".join(sql_parts), sql_params)
            for (sid,) in cur.fetchall():
                if sid is None:
                    continue
                rows_to_insert.append((st, sid))
            predicate_meta = {
                "source_type": st,
                "table": table,
                "matched": len(rows_to_insert),
                "limit_applied": limit,
                "since": since,
                "until": until,
            }

        if not rows_to_insert:
            conn.commit()
            return {
                "output": {
                    "enqueued": 0,
                    "skipped_existing": 0,
                    "candidates": 0,
                    "priority": priority,
                    "predicate": predicate_meta,
                }
            }

        # Batch INSERT ... ON CONFLICT DO NOTHING. RETURNING id tells us
        # how many rows were actually inserted (i.e. not already queued).
        psycopg2.extras.execute_values(
            cur,
            (
                "INSERT INTO embedding_queue (source_type, source_id, priority, status) "
                "VALUES %s "
                "ON CONFLICT (source_type, source_id) DO NOTHING "
                "RETURNING id"
            ),
            [(st, sid, priority, "pending") for (st, sid) in rows_to_insert],
            page_size=1000,
        )
        inserted = len(cur.fetchall())
        conn.commit()
    except Exception as exc:
        conn.rollback()
        return {"error": f"EMBEDDING_BATCH failed: {exc}"}
    finally:
        conn.close()

    candidates = len(rows_to_insert)
    skipped = max(candidates - inserted, 0)
    log.info(
        "EMBEDDING_BATCH: enqueued {n}/{c} (skipped_existing={s}, priority={p})",
        n=inserted, c=candidates, s=skipped, p=priority,
    )
    return {
        "output": {
            "enqueued": inserted,
            "skipped_existing": skipped,
            "candidates": candidates,
            "priority": priority,
            "predicate": predicate_meta,
        },
        "metrics": {
            "compute_time_ms": 0,  # filled by caller
            "rows_enqueued": inserted,
            "rows_skipped_existing": skipped,
        },
    }



if __name__ == "__main__":
    main()
