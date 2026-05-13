#!/usr/bin/env python3
"""Inventory and benchmark local GRID LLM runtimes.

This is report-only by default. It probes local llama.cpp-compatible and
Ollama endpoints, estimates throughput, and emits conservative worker profile
recommendations for CPU-only and GPU hosts without changing services.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import socket
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import requests


DEFAULT_PROMPT = "Reply with exactly one short sentence about market risk."
DEFAULT_LLAMA_URL = "http://127.0.0.1:8080"
DEFAULT_OLLAMA_URL = "http://127.0.0.1:11434"
CPU_ONLY_EXCLUDES = [
    "HUMAN_LLM_QUERY",
    "BACKTEST",
    "FEATURE_COMPUTE",
    "SIMULATION",
    "HYPOTHESIS_TEST",
    "REGIME_DETECT",
]
REMOTE_INVENTORY_PROBE = r"""
import json
import os
import socket
import subprocess
import urllib.error
import urllib.request


def ram_gb():
    try:
        with open("/proc/meminfo", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemTotal:"):
                    return round(int(line.split()[1]) / 1024 / 1024, 1)
    except OSError:
        pass
    return 0.0


def gpu():
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, None
    rows = []
    for line in result.stdout.strip().splitlines():
        parts = [part.strip() for part in line.split(",", 1)]
        if len(parts) != 2:
            continue
        try:
            rows.append((parts[0], float(parts[1]) / 1024))
        except ValueError:
            pass
    if not rows:
        return None, None
    largest, _ = max(rows, key=lambda row: row[1])
    return largest, round(sum(vram for _, vram in rows), 1)


def ollama_models():
    try:
        req = urllib.request.Request("http://127.0.0.1:11434/api/tags")
        with urllib.request.urlopen(req, timeout=3) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return []
    return [
        model.get("name") or model.get("model")
        for model in payload.get("models", [])
        if model.get("name") or model.get("model")
    ]


def choose_model(models):
    for size_hint in ("3b", "7b", "8b", "9b", "12b"):
        for model in models:
            lowered = model.lower()
            if size_hint in lowered and "embed" not in lowered and "embedding" not in lowered:
                return model
    for model in models:
        lowered = model.lower()
        if "embed" not in lowered and "embedding" not in lowered:
            return model
    return models[0] if models else None


def benchmark_ollama(model):
    prompt = "Reply with exactly one short sentence about market risk."
    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"num_predict": 48, "temperature": 0},
    }).encode("utf-8")
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:11434/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        started = subprocess.check_output(["date", "+%s%N"], text=True).strip()
        with urllib.request.urlopen(req, timeout=90) as response:
            raw = response.read().decode("utf-8")
        ended = subprocess.check_output(["date", "+%s%N"], text=True).strip()
        data = json.loads(raw)
    except (OSError, subprocess.CalledProcessError, urllib.error.URLError, json.JSONDecodeError) as exc:
        return {"provider": "ollama", "model": model or "unknown", "ok": False, "error": str(exc)}
    eval_count = int(data.get("eval_count") or len(str(data.get("response") or "").split()) or 1)
    eval_duration = float(data.get("eval_duration") or 0)
    if eval_duration:
        tokens_per_second = round(eval_count / (eval_duration / 1000000000), 3)
    else:
        elapsed = max((int(ended) - int(started)) / 1000000000, 0.001)
        tokens_per_second = round(eval_count / elapsed, 3)
    response_text = str(data.get("response") or "").strip()
    return {
        "provider": "ollama",
        "model": model or "unknown",
        "ok": True,
        "completion_tokens": eval_count,
        "tokens_per_second": tokens_per_second,
        "latency_seconds": round(float(data.get("total_duration") or 0) / 1000000000, 3),
        "quality_sanity_ok": bool(response_text) and len(response_text) <= 300,
    }


gpu_model, gpu_vram = gpu()
models = ollama_models()
out = {
    "hostname": socket.gethostname(),
    "cpu_cores": os.cpu_count() or 1,
    "ram_gb": ram_gb(),
    "gpu_model": gpu_model,
    "gpu_vram_gb": gpu_vram,
    "ollama_available": bool(models),
    "ollama_models": models,
}
if os.environ.get("GRID_LLM_BENCHMARK") == "1":
    out["benchmarks"] = []
    model = choose_model(models)
    if model:
        out["benchmarks"].append(benchmark_ollama(model))
print(json.dumps(out, sort_keys=True))
"""


@dataclass(frozen=True)
class BenchmarkMetrics:
    provider: str
    model: str
    completion_tokens: int
    latency_seconds: float
    tokens_per_second: float
    prompt_tokens: int = 0
    ok: bool = True
    error: str = ""


def _round(value: float) -> float:
    return round(float(value), 3)


def _estimate_tokens(text: str) -> int:
    return max(1, len((text or "").split()))


def ollama_metrics(payload: dict[str, Any]) -> BenchmarkMetrics:
    eval_count = int(payload.get("eval_count") or 0)
    eval_duration_ns = float(payload.get("eval_duration") or 0)
    total_duration_ns = float(payload.get("total_duration") or eval_duration_ns or 0)
    completion_tokens = eval_count or _estimate_tokens(str(payload.get("response") or ""))
    latency_seconds = total_duration_ns / 1_000_000_000 if total_duration_ns else 0.0
    eval_seconds = eval_duration_ns / 1_000_000_000 if eval_duration_ns else latency_seconds
    tokens_per_second = completion_tokens / eval_seconds if eval_seconds else 0.0
    return BenchmarkMetrics(
        provider="ollama",
        model=str(payload.get("model") or "unknown"),
        completion_tokens=completion_tokens,
        latency_seconds=_round(latency_seconds),
        tokens_per_second=_round(tokens_per_second),
        prompt_tokens=int(payload.get("prompt_eval_count") or 0),
    )


def llamacpp_metrics(
    model: str,
    payload: dict[str, Any],
    elapsed_seconds: float,
) -> BenchmarkMetrics:
    usage = payload.get("usage") or {}
    choices = payload.get("choices") or []
    text = ""
    if choices and isinstance(choices[0], dict):
        text = str(choices[0].get("text") or choices[0].get("message", {}).get("content") or "")
    completion_tokens = int(usage.get("completion_tokens") or _estimate_tokens(text))
    tokens_per_second = completion_tokens / elapsed_seconds if elapsed_seconds else 0.0
    return BenchmarkMetrics(
        provider="llamacpp",
        model=model,
        completion_tokens=completion_tokens,
        latency_seconds=_round(elapsed_seconds),
        tokens_per_second=_round(tokens_per_second),
        prompt_tokens=int(usage.get("prompt_tokens") or 0),
    )


def recommend_worker_profile(
    *,
    hostname: str,
    cpu_cores: int,
    ram_gb: float,
    gpu_vram_gb: float | None,
    has_ollama: bool,
) -> dict[str, Any]:
    if ram_gb and ram_gb < 8:
        return {
            "hostname": hostname,
            "max_concurrent": 1,
            "exclude_types": CPU_ONLY_EXCLUDES,
            "notes": f"Low-RAM host with {ram_gb:.1f}GB RAM; keep it on light jobs.",
        }
    if not gpu_vram_gb:
        return {
            "hostname": hostname,
            "max_concurrent": 1,
            "exclude_types": CPU_ONLY_EXCLUDES,
            "notes": f"CPU-only host with {ram_gb:.1f}GB RAM; keep it on light jobs.",
        }
    if cpu_cores <= 4 or ram_gb < 24:
        return {
            "hostname": hostname,
            "max_concurrent": 1,
            "exclude_types": [] if has_ollama else ["HUMAN_LLM_QUERY"],
            "notes": (
                f"Constrained host ({cpu_cores} cores, {ram_gb:.1f}GB RAM, "
                f"{gpu_vram_gb:.1f}GB VRAM); keep concurrency at 1."
            ),
        }
    if gpu_vram_gb < 12:
        return {
            "hostname": hostname,
            "max_concurrent": 1,
            "exclude_types": ["HUMAN_LLM_QUERY"] if not has_ollama else [],
            "notes": f"Small GPU ({gpu_vram_gb:.1f}GB VRAM); avoid broad LLM fanout.",
        }
    if gpu_vram_gb < 32:
        return {
            "hostname": hostname,
            "max_concurrent": 2,
            "exclude_types": [] if has_ollama else ["HUMAN_LLM_QUERY"],
            "notes": f"Mid-size GPU ({gpu_vram_gb:.1f}GB VRAM); moderate concurrency.",
        }
    return {
        "hostname": hostname,
        "max_concurrent": max(2, min(4, cpu_cores // 4 or 2)),
        "exclude_types": [] if has_ollama else ["HUMAN_LLM_QUERY"],
        "notes": f"Large GPU pool ({gpu_vram_gb:.1f}GB VRAM); eligible for heavier work.",
    }


def _detect_ram_gb() -> float:
    try:
        with open("/proc/meminfo", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemTotal:"):
                    return round(int(line.split()[1]) / 1024 / 1024, 1)
    except OSError:
        pass
    try:
        result = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            return round(int(result.stdout.strip()) / 1e9, 1)
    except (OSError, subprocess.TimeoutExpired, ValueError):
        pass
    return 0.0


def _detect_gpu_vram_gb() -> tuple[str | None, float | None]:
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, None
    if result.returncode != 0 or not result.stdout.strip():
        return None, None
    rows: list[tuple[str, float]] = []
    for line in result.stdout.strip().splitlines():
        parts = [part.strip() for part in line.split(",", 1)]
        if len(parts) != 2:
            continue
        try:
            rows.append((parts[0], float(parts[1]) / 1024))
        except ValueError:
            continue
    if not rows:
        return None, None
    largest_name, _ = max(rows, key=lambda row: row[1])
    return largest_name, round(sum(vram for _, vram in rows), 1)


def _ollama_models(base_url: str, timeout: float) -> list[str]:
    response = requests.get(f"{base_url.rstrip('/')}/api/tags", timeout=timeout)
    response.raise_for_status()
    return [
        model.get("name") or model.get("model")
        for model in response.json().get("models", [])
        if model.get("name") or model.get("model")
    ]


def _choose_ollama_model(models: list[str]) -> str | None:
    non_embedding = [
        model for model in models
        if "embed" not in model.lower() and "embedding" not in model.lower()
    ]
    for size_hint in ("3b", "7b", "8b", "9b", "12b"):
        for model in non_embedding:
            if size_hint in model.lower():
                return model
    if non_embedding:
        return non_embedding[0]
    return models[0] if models else None


def benchmark_ollama(base_url: str, prompt: str, timeout: float) -> BenchmarkMetrics:
    models = _ollama_models(base_url, timeout)
    model = _choose_ollama_model(models)
    if not model:
        raise RuntimeError("Ollama is reachable but no models are installed")
    response = requests.post(
        f"{base_url.rstrip('/')}/api/generate",
        json={
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"num_predict": 64, "temperature": 0},
        },
        timeout=max(timeout, 60),
    )
    response.raise_for_status()
    payload = response.json()
    payload.setdefault("model", model)
    return ollama_metrics(payload)


def _llamacpp_model(base_url: str, timeout: float) -> str:
    try:
        response = requests.get(f"{base_url.rstrip('/')}/v1/models", timeout=timeout)
        response.raise_for_status()
        data = response.json().get("data") or []
        if data and isinstance(data[0], dict):
            return str(data[0].get("id") or "local-model")
    except requests.RequestException:
        pass
    return "local-model"


def benchmark_llamacpp(base_url: str, prompt: str, timeout: float) -> BenchmarkMetrics:
    model = _llamacpp_model(base_url, timeout)
    started = time.perf_counter()
    response = requests.post(
        f"{base_url.rstrip('/')}/v1/completions",
        json={"model": model, "prompt": prompt, "max_tokens": 64, "temperature": 0},
        timeout=max(timeout, 60),
    )
    elapsed = time.perf_counter() - started
    response.raise_for_status()
    return llamacpp_metrics(model=model, payload=response.json(), elapsed_seconds=elapsed)


def inventory_host() -> dict[str, Any]:
    gpu_model, gpu_vram = _detect_gpu_vram_gb()
    ollama_available = False
    ollama_models: list[str] = []
    try:
        ollama_models = _ollama_models(DEFAULT_OLLAMA_URL, timeout=3)
        ollama_available = bool(ollama_models)
    except requests.RequestException:
        pass
    cpu_cores = os.cpu_count() or 1
    ram_gb = _detect_ram_gb()
    hostname = socket.gethostname()
    return {
        "hostname": hostname,
        "cpu_cores": cpu_cores,
        "ram_gb": ram_gb,
        "gpu_model": gpu_model,
        "gpu_vram_gb": gpu_vram,
        "ollama_available": ollama_available,
        "ollama_models": ollama_models,
        "recommendation": recommend_worker_profile(
            hostname=hostname,
            cpu_cores=cpu_cores,
            ram_gb=ram_gb,
            gpu_vram_gb=gpu_vram,
            has_ollama=ollama_available,
        ),
    }


def inventory_ssh_host(host: str, timeout: float, *, benchmark: bool = False) -> dict[str, Any]:
    env_prefix = "GRID_LLM_BENCHMARK=1 " if benchmark else ""
    result = subprocess.run(
        ["ssh", host, f"{env_prefix}python3 -c {shlex.quote(REMOTE_INVENTORY_PROBE)}"],
        capture_output=True,
        text=True,
        timeout=max(timeout, 120 if benchmark else 15),
        check=False,
    )
    if result.returncode != 0:
        return {
            "ssh_host": host,
            "ok": False,
            "error": (result.stderr or result.stdout or "").strip(),
        }
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return {
            "ssh_host": host,
            "ok": False,
            "error": f"invalid inventory JSON: {exc}",
        }
    data["ssh_host"] = host
    data["ok"] = True
    data["recommendation"] = recommend_worker_profile(
        hostname=str(data.get("hostname") or host),
        cpu_cores=int(data.get("cpu_cores") or 1),
        ram_gb=float(data.get("ram_gb") or 0),
        gpu_vram_gb=data.get("gpu_vram_gb"),
        has_ollama=bool(data.get("ollama_available")),
    )
    return data


def _metric_error(provider: str, error: Exception) -> BenchmarkMetrics:
    return BenchmarkMetrics(
        provider=provider,
        model="unknown",
        completion_tokens=0,
        latency_seconds=0.0,
        tokens_per_second=0.0,
        ok=False,
        error=str(error),
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    report: dict[str, Any] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "inventory": inventory_host(),
        "remote_inventory": [],
        "benchmarks": [],
    }
    for host in args.ssh_host or []:
        report["remote_inventory"].append(inventory_ssh_host(host, args.timeout, benchmark=args.benchmark))
    if args.benchmark:
        for provider, func, url in (
            ("llamacpp", benchmark_llamacpp, args.llamacpp_url),
            ("ollama", benchmark_ollama, args.ollama_url),
        ):
            try:
                metric = func(url, args.prompt, args.timeout)
            except Exception as exc:  # noqa: BLE001 - benchmark reports failures per provider.
                metric = _metric_error(provider, exc)
            report["benchmarks"].append(asdict(metric))
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="local_llm_autotune")
    parser.add_argument("--benchmark", action="store_true", help="Run short local LLM throughput probes.")
    parser.add_argument("--llamacpp-url", default=os.getenv("GRID_LLAMACPP_URL", DEFAULT_LLAMA_URL))
    parser.add_argument("--ollama-url", default=os.getenv("OLLAMA_HOST", DEFAULT_OLLAMA_URL))
    parser.add_argument("--prompt", default=DEFAULT_PROMPT)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--report", help="Optional JSON report path.")
    parser.add_argument(
        "--ssh-host",
        action="append",
        help="Remote host to inventory over SSH. Can be supplied more than once.",
    )
    args = parser.parse_args(argv)

    report = run(args)
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.report:
        path = Path(args.report).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
