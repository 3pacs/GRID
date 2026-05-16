#!/usr/bin/env python3
"""Read-only fleet audit for GRID compute and GPU hosts.

The audit combines coordinator state with SSH-level host probes, emits a
structured JSON/Markdown report, and can optionally persist snapshots into the
``fleet_state`` table. It does not mutate services; findings contain proposed
actions for a human or future apply-safe layer.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import requests


DEFAULT_COORDINATOR = os.getenv("GRID_COORDINATOR_URL", "http://100.75.185.36:8100")
DEFAULT_HOSTS = ("grid-svr", "gridz4", "koala", "redbox", "ocr-node", "p9d", "z400")
SERVICE_NEEDLES = ("grid-", "storymill-", "llama", "ollama", "comfyui", "topaz", "surya")
REMOTE_PROBE = r"""
import json
import os
import socket
import subprocess


def run(cmd, timeout=10):
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"ok": False, "stdout": "", "stderr": str(exc), "returncode": 124}
    return {
        "ok": result.returncode == 0,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
    }


gpu_query = run([
    "nvidia-smi",
    "--query-gpu=index,uuid,name,utilization.gpu,memory.used,memory.total",
    "--format=csv,noheader,nounits",
])
apps_query = run([
    "nvidia-smi",
    "--query-compute-apps=gpu_uuid,pid,process_name,used_memory",
    "--format=csv,noheader,nounits",
])
services = run([
    "systemctl",
    "list-units",
    "--type=service",
    "--state=running,failed",
    "--no-legend",
    "--plain",
], timeout=8)
tail_ip = run(["tailscale", "ip", "-4"], timeout=5)

print(json.dumps({
    "hostname": socket.gethostname(),
    "probe_host": os.environ.get("GRID_FLEET_PROBE_HOST") or socket.gethostname(),
    "gpus_csv": gpu_query,
    "compute_apps_csv": apps_query,
    "services_text": services,
    "tailscale_ip": tail_ip,
}, sort_keys=True))
"""


@dataclass(frozen=True)
class GPUState:
    index: int | None
    uuid: str
    name: str
    util_pct: int | None
    mem_used_mb: int | None
    mem_total_mb: int | None


@dataclass(frozen=True)
class ComputeProcess:
    gpu_uuid: str
    pid: int | None
    process_name: str
    used_memory_mb: int | None


@dataclass(frozen=True)
class ServiceState:
    name: str
    load_state: str
    active_state: str
    sub_state: str
    description: str


@dataclass
class HostSnapshot:
    host: str
    ok: bool
    hostname: str | None = None
    tailscale_ip: str | None = None
    gpus: list[GPUState] = field(default_factory=list)
    compute_apps: list[ComputeProcess] = field(default_factory=list)
    services: list[ServiceState] = field(default_factory=list)
    error: str | None = None


@dataclass(frozen=True)
class Finding:
    severity: str
    code: str
    host: str
    summary: str
    proposed_action: str
    apply_safe: bool = False
    details: dict[str, Any] = field(default_factory=dict)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip().replace("%", "").replace("MiB", "").replace("MB", "")
    if not text or text.upper() in {"N/A", "[N/A]"}:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _csv_rows(text: str) -> list[list[str]]:
    if not text.strip():
        return []
    reader = csv.reader(io.StringIO(text))
    return [[part.strip() for part in row] for row in reader if row]


def parse_gpu_csv(text: str) -> list[GPUState]:
    """Parse nvidia-smi GPU rows from either units or nounits output."""
    gpus: list[GPUState] = []
    for row in _csv_rows(text):
        if len(row) >= 6:
            index, uuid, name, util, used, total = row[:6]
        elif len(row) >= 4:
            index = str(len(gpus))
            uuid = ""
            name, util, used, total = row[:4]
        else:
            continue
        gpus.append(
            GPUState(
                index=_as_int(index),
                uuid=uuid,
                name=name,
                util_pct=_as_int(util),
                mem_used_mb=_as_int(used),
                mem_total_mb=_as_int(total),
            )
        )
    return gpus


def parse_compute_apps_csv(text: str) -> list[ComputeProcess]:
    apps: list[ComputeProcess] = []
    for row in _csv_rows(text):
        if len(row) < 4:
            continue
        gpu_uuid, pid, process_name, used_memory = row[:4]
        apps.append(
            ComputeProcess(
                gpu_uuid=gpu_uuid,
                pid=_as_int(pid),
                process_name=process_name,
                used_memory_mb=_as_int(used_memory),
            )
        )
    return apps


def parse_systemd_services(text: str) -> list[ServiceState]:
    services: list[ServiceState] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lowered = line.lower()
        if not any(needle in lowered for needle in SERVICE_NEEDLES):
            continue
        parts = line.split(None, 4)
        if len(parts) < 4:
            continue
        name, load_state, active_state, sub_state = parts[:4]
        description = parts[4] if len(parts) > 4 else ""
        services.append(ServiceState(name, load_state, active_state, sub_state, description))
    return services


def _first_line(text: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return None


def parse_remote_probe(host: str, payload: dict[str, Any]) -> HostSnapshot:
    gpu_result = payload.get("gpus_csv") or {}
    apps_result = payload.get("compute_apps_csv") or {}
    service_result = payload.get("services_text") or {}
    tail_result = payload.get("tailscale_ip") or {}
    errors = []
    for label, result in (("gpus", gpu_result), ("services", service_result)):
        if result and not result.get("ok"):
            errors.append(f"{label}: {result.get('stderr') or result.get('stdout') or result.get('returncode')}")
    return HostSnapshot(
        host=host,
        ok=not errors,
        hostname=payload.get("hostname"),
        tailscale_ip=_first_line(str(tail_result.get("stdout") or "")),
        gpus=parse_gpu_csv(str(gpu_result.get("stdout") or "")),
        compute_apps=parse_compute_apps_csv(str(apps_result.get("stdout") or "")),
        services=parse_systemd_services(str(service_result.get("stdout") or "")),
        error="; ".join(errors) if errors else None,
    )


def probe_ssh_host(host: str, timeout: float) -> HostSnapshot:
    command = f"GRID_FLEET_PROBE_HOST={shlex.quote(host)} python3 -c {shlex.quote(REMOTE_PROBE)}"
    try:
        result = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={int(timeout)}", host, command],
            capture_output=True,
            text=True,
            timeout=max(timeout + 20, 30),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return HostSnapshot(host=host, ok=False, error=str(exc))
    if result.returncode != 0:
        return HostSnapshot(host=host, ok=False, error=(result.stderr or result.stdout or "").strip())
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return HostSnapshot(host=host, ok=False, error=f"invalid probe JSON: {exc}")
    snapshot = parse_remote_probe(host, payload)
    if snapshot.error:
        return snapshot
    snapshot.ok = True
    return snapshot


def get_json(base_url: str, path: str, timeout: float) -> dict[str, Any] | list[dict[str, Any]]:
    response = requests.get(f"{base_url.rstrip('/')}{path}", timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_coordinator_state(coordinator: str, timeout: float) -> dict[str, Any]:
    try:
        workers = get_json(coordinator, "/workers", timeout)
        stats = get_json(coordinator, "/stats", timeout)
    except Exception as exc:  # noqa: BLE001 - report the coordinator failure.
        return {"ok": False, "error": str(exc), "workers": [], "stats": {}}
    return {"ok": True, "workers": workers, "stats": stats}


def queue_depths(stats: dict[str, Any]) -> dict[str, int]:
    states = stats.get("job_states") or {}
    return {str(key): int(value or 0) for key, value in states.items()}


def worker_by_hostname(workers: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(worker.get("hostname") or "").lower(): worker for worker in workers if worker.get("hostname")}


def open_worker_capacity(workers: list[dict[str, Any]]) -> int:
    capacity = 0
    for worker in workers:
        if worker.get("state") == "OFFLINE":
            continue
        try:
            max_concurrent = max(int(worker.get("max_concurrent") or 1), 1)
            active = max(int(worker.get("active_jobs") or 0), 0)
        except (TypeError, ValueError):
            continue
        capacity += max(max_concurrent - active, 0)
    return capacity


def _gpu_processes(snapshot: HostSnapshot, gpu: GPUState) -> list[ComputeProcess]:
    if not gpu.uuid:
        return []
    return [app for app in snapshot.compute_apps if app.gpu_uuid == gpu.uuid]


def _has_service(snapshot: HostSnapshot, needle: str) -> bool:
    lowered = needle.lower()
    return any(lowered in service.name.lower() for service in snapshot.services)


def build_findings(
    snapshots: list[HostSnapshot],
    coordinator_state: dict[str, Any],
    *,
    idle_util_threshold: int = 10,
) -> list[Finding]:
    findings: list[Finding] = []
    workers = list(coordinator_state.get("workers") or [])
    workers_by_host = worker_by_hostname(workers)
    depths = queue_depths(dict(coordinator_state.get("stats") or {}))
    queued = depths.get("QUEUED", 0)

    if not coordinator_state.get("ok"):
        findings.append(
            Finding(
                severity="critical",
                code="coordinator_unreachable",
                host="coordinator",
                summary="Compute coordinator state could not be read.",
                proposed_action="Restore coordinator/API visibility before applying fleet placement decisions.",
                details={"error": coordinator_state.get("error")},
            )
        )

    if queued == 0 and open_worker_capacity(workers) > 0:
        findings.append(
            Finding(
                severity="warning",
                code="queue_starvation",
                host="grid-svr",
                summary="Workers have open capacity but the coordinator queue is empty.",
                proposed_action=(
                    "Run compute_job_producer with a higher target/max-create and collect evidence before "
                    "changing producer timer cadence."
                ),
                details={"open_capacity": open_worker_capacity(workers), "queue_depths": depths},
            )
        )

    for snapshot in snapshots:
        if not snapshot.ok:
            findings.append(
                Finding(
                    severity="warning",
                    code="host_probe_failed",
                    host=snapshot.host,
                    summary=f"{snapshot.host} could not be fully probed over SSH.",
                    proposed_action="Check Tailscale/SSH reachability and do not infer service placement from stale dashboard data.",
                    details={"error": snapshot.error},
                )
            )
            continue

        worker = workers_by_host.get(snapshot.host.lower()) or workers_by_host.get((snapshot.hostname or "").lower())
        if snapshot.gpus and not worker:
            findings.append(
                Finding(
                    severity="warning",
                    code="gpu_host_missing_worker",
                    host=snapshot.host,
                    summary=f"{snapshot.host} has GPUs but no matching compute worker row.",
                    proposed_action="Register or revive scripts/worker.py for this host before routing queued compute jobs there.",
                    details={"gpu_count": len(snapshot.gpus), "hostname": snapshot.hostname},
                )
            )

        for service in snapshot.services:
            if service.active_state == "failed" or service.sub_state == "failed":
                findings.append(
                    Finding(
                        severity="critical",
                        code="service_failed",
                        host=snapshot.host,
                        summary=f"{service.name} is failed on {snapshot.host}.",
                        proposed_action=f"Inspect journalctl -u {service.name}; restart only after confirming the failure mode.",
                        apply_safe=False,
                        details=asdict(service),
                    )
                )

        for gpu in snapshot.gpus:
            util = gpu.util_pct if gpu.util_pct is not None else 0
            used = gpu.mem_used_mb if gpu.mem_used_mb is not None else 0
            total = gpu.mem_total_mb if gpu.mem_total_mb is not None else 0
            processes = _gpu_processes(snapshot, gpu)
            if queued > 0 and util < idle_util_threshold and used < max(1024, int(total * 0.2)):
                findings.append(
                    Finding(
                        severity="warning",
                        code="idle_gpu_with_queue",
                        host=snapshot.host,
                        summary=f"{snapshot.host} GPU {gpu.index} is mostly idle while jobs are queued.",
                        proposed_action="Route eligible work to this host or start the appropriate worker/service after checking role constraints.",
                        details={"gpu": asdict(gpu), "queue_depths": depths},
                    )
                )
            if len(processes) >= 2 and used > max(4096, int(total * 0.5)):
                findings.append(
                    Finding(
                        severity="warning",
                        code="possible_gpu_contention",
                        host=snapshot.host,
                        summary=f"{snapshot.host} GPU {gpu.index} has multiple compute processes and high memory use.",
                        proposed_action="Review CUDA_VISIBLE_DEVICES bindings; propose rebinds before changing services.",
                        details={"gpu": asdict(gpu), "processes": [asdict(process) for process in processes]},
                    )
                )

        busy = [gpu for gpu in snapshot.gpus if (gpu.util_pct or 0) >= 80]
        idle = [gpu for gpu in snapshot.gpus if (gpu.util_pct or 0) < idle_util_threshold]
        if busy and idle and len(snapshot.gpus) >= 2:
            findings.append(
                Finding(
                    severity="info",
                    code="unused_sibling_gpu",
                    host=snapshot.host,
                    summary=f"{snapshot.host} has one busy GPU and at least one idle sibling GPU.",
                    proposed_action="Consider a second narrow-lane model or embedding worker on the idle card if host role allows it.",
                    details={"busy": [asdict(gpu) for gpu in busy], "idle": [asdict(gpu) for gpu in idle]},
                )
            )

        if snapshot.host == "p9d" and snapshot.gpus and not _has_service(snapshot, "ollama"):
            findings.append(
                Finding(
                    severity="info",
                    code="p9d_no_ollama",
                    host=snapshot.host,
                    summary="p9d has GPUs but no Ollama service detected.",
                    proposed_action="Keep p9d as Graphics/ComfyUI unless it is explicitly promoted to an LLM lane.",
                    details={"gpu_count": len(snapshot.gpus)},
                )
            )

    return findings


def fleet_state_schema() -> str:
    schema_path = Path(__file__).resolve().parent / "fleet" / "fleet_state_init.sql"
    return schema_path.read_text(encoding="utf-8")


def _db_connect_info(db_url: str | None = None) -> str | dict[str, Any] | None:
    dsn = db_url or os.getenv("GRID_DB_URL") or os.getenv("DATABASE_URL")
    if dsn:
        return dsn.replace("postgresql+psycopg2://", "postgresql://", 1)
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    if host and name and user:
        return {
            "host": host,
            "port": int(os.getenv("DB_PORT", "5432")),
            "dbname": name,
            "user": user,
            "password": os.getenv("DB_PASSWORD", ""),
        }
    return None


def write_fleet_state(conninfo: str | dict[str, Any], snapshots: list[HostSnapshot], depths: dict[str, int]) -> int:
    import psycopg2
    import psycopg2.extras

    rows = 0
    if isinstance(conninfo, dict):
        conn = psycopg2.connect(**conninfo)
    else:
        conn = psycopg2.connect(conninfo)
    with conn:
        with conn.cursor() as cur:
            cur.execute(fleet_state_schema())
        with conn.cursor() as cur:
            for snapshot in snapshots:
                services = [asdict(service) for service in snapshot.services]
                if not snapshot.gpus:
                    cur.execute(
                        """
                        INSERT INTO fleet_state
                            (host, ok, error, gpu_index, gpu_name, gpu_uuid, util_pct,
                             mem_used_mb, mem_total_mb, procs, services_running, queue_depths)
                        VALUES (%s,%s,%s,NULL,NULL,NULL,NULL,NULL,NULL,%s,%s,%s)
                        """,
                        (
                            snapshot.host,
                            snapshot.ok,
                            snapshot.error,
                            psycopg2.extras.Json([]),
                            psycopg2.extras.Json(services),
                            psycopg2.extras.Json(depths),
                        ),
                    )
                    rows += 1
                    continue
                for gpu in snapshot.gpus:
                    cur.execute(
                        """
                        INSERT INTO fleet_state
                            (host, ok, error, gpu_index, gpu_name, gpu_uuid, util_pct,
                             mem_used_mb, mem_total_mb, procs, services_running, queue_depths)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            snapshot.host,
                            snapshot.ok,
                            snapshot.error,
                            gpu.index,
                            gpu.name,
                            gpu.uuid,
                            gpu.util_pct,
                            gpu.mem_used_mb,
                            gpu.mem_total_mb,
                            psycopg2.extras.Json([asdict(process) for process in _gpu_processes(snapshot, gpu)]),
                            psycopg2.extras.Json(services),
                            psycopg2.extras.Json(depths),
                        ),
                    )
                    rows += 1
    return rows


def report_payload(
    *,
    snapshots: list[HostSnapshot],
    coordinator_state: dict[str, Any],
    findings: list[Finding],
    wrote_rows: int | None = None,
) -> dict[str, Any]:
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mode": "read-only",
        "coordinator": coordinator_state,
        "queue_depths": queue_depths(dict(coordinator_state.get("stats") or {})),
        "snapshots": [asdict(snapshot) for snapshot in snapshots],
        "findings": [asdict(finding) for finding in findings],
        "wrote_fleet_state_rows": wrote_rows,
    }


def markdown_report(payload: dict[str, Any]) -> str:
    findings = payload.get("findings") or []
    snapshots = payload.get("snapshots") or []
    lines = [
        "# GRID Fleet-Hermes Audit",
        "",
        f"- Generated: `{payload.get('generated_at')}`",
        f"- Mode: `{payload.get('mode')}`",
        f"- Hosts probed: `{len(snapshots)}`",
        f"- Findings: `{len(findings)}`",
        "",
        "## Findings",
        "",
    ]
    if not findings:
        lines.append("- No findings.")
    else:
        for finding in findings:
            lines.append(
                f"- **{finding['severity']} / {finding['code']} / {finding['host']}**: "
                f"{finding['summary']} Action: {finding['proposed_action']}"
            )
    lines.extend(["", "## Host Summary", ""])
    for snapshot in snapshots:
        gpu_bits = []
        for gpu in snapshot.get("gpus") or []:
            gpu_bits.append(
                f"GPU {gpu.get('index')} {gpu.get('name')} "
                f"{gpu.get('util_pct')}% {gpu.get('mem_used_mb')}/{gpu.get('mem_total_mb')} MiB"
            )
        services = [service.get("name") for service in snapshot.get("services") or []]
        lines.append(
            f"- `{snapshot.get('host')}` ok={snapshot.get('ok')} "
            f"gpus={'; '.join(gpu_bits) or 'none'} services={', '.join(services) or 'none'}"
        )
    lines.extend(["", "## Guardrails", ""])
    lines.append("- This v0 audit is read-only. It does not restart, install, rebind, or retime services.")
    lines.append("- Producer cadence and OCR/topaz GPU bindings require separate evidence before mutation.")
    return "\n".join(lines) + "\n"


def _hosts_from_args(values: list[str] | None) -> list[str]:
    if values:
        hosts: list[str] = []
        for value in values:
            hosts.extend(part.strip() for part in value.split(",") if part.strip())
        return hosts
    env_hosts = os.getenv("GRID_FLEET_HOSTS")
    if env_hosts:
        return [part.strip() for part in env_hosts.split(",") if part.strip()]
    return list(DEFAULT_HOSTS)


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.from_json:
        data = json.loads(Path(args.from_json).read_text(encoding="utf-8"))
        snapshots = [
            HostSnapshot(
                host=item["host"],
                ok=bool(item["ok"]),
                hostname=item.get("hostname"),
                tailscale_ip=item.get("tailscale_ip"),
                gpus=[GPUState(**gpu) for gpu in item.get("gpus", [])],
                compute_apps=[ComputeProcess(**process) for process in item.get("compute_apps", [])],
                services=[ServiceState(**service) for service in item.get("services", [])],
                error=item.get("error"),
            )
            for item in data.get("snapshots", [])
        ]
        coordinator_state = data.get("coordinator") or {"ok": True, "workers": [], "stats": {}}
    else:
        hosts = _hosts_from_args(args.host)
        snapshots = [] if args.skip_ssh else [probe_ssh_host(host, args.timeout) for host in hosts]
        coordinator_state = fetch_coordinator_state(args.coordinator, args.timeout)

    findings = build_findings(snapshots, coordinator_state)
    wrote_rows = None
    if args.write_db:
        conninfo = _db_connect_info(args.db_url)
        if not conninfo:
            raise SystemExit("--write-db requires --db-url, GRID_DB_URL, DATABASE_URL, or DB_HOST/DB_NAME/DB_USER")
        wrote_rows = write_fleet_state(conninfo, snapshots, queue_depths(dict(coordinator_state.get("stats") or {})))
    return report_payload(
        snapshots=snapshots,
        coordinator_state=coordinator_state,
        findings=findings,
        wrote_rows=wrote_rows,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="fleet_audit")
    parser.add_argument("--host", action="append", help="SSH host to probe. Repeat or comma-separate.")
    parser.add_argument("--coordinator", default=DEFAULT_COORDINATOR, help="Compute coordinator base URL.")
    parser.add_argument("--timeout", type=float, default=8.0, help="Per-probe timeout in seconds.")
    parser.add_argument("--skip-ssh", action="store_true", help="Only read coordinator state.")
    parser.add_argument("--from-json", help="Load a previous JSON report or fixture instead of probing.")
    parser.add_argument("--output", help="Write JSON report to this path.")
    parser.add_argument("--markdown", help="Write Markdown report to this path.")
    parser.add_argument("--json-only", action="store_true", help="Print compact JSON only.")
    parser.add_argument("--write-db", action="store_true", help="Persist snapshots to fleet_state after probing.")
    parser.add_argument("--db-url", help="Postgres DSN for --write-db.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Reserved for a future apply-safe layer. v0 remains read-only.",
    )
    args = parser.parse_args(argv)
    if args.apply:
        raise SystemExit("--apply is reserved; fleet_audit v0 is read-only")

    payload = run(args)
    json_text = json.dumps(payload, indent=None if args.json_only else 2, sort_keys=True)
    if args.output:
        out = Path(args.output).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json_text + "\n", encoding="utf-8")
    if args.markdown:
        md = Path(args.markdown).expanduser()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(markdown_report(payload), encoding="utf-8")
    print(json_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
