#!/usr/bin/env python3
"""GRID — BOINC-inspired Distributed Compute Coordinator.

FastAPI service on :8100 that manages compute jobs across Tailscale workers.
Supports 7 job types: SIMULATION, BACKTEST, FEATURE_COMPUTE, HYPOTHESIS_TEST,
REGIME_DETECT, LLM_INFERENCE, DATA_PULL.

Job state machine:
  CREATED → QUEUED → DISPATCHED → IN_PROGRESS → COMPLETED → VALID → ASSIMILATED

Tables created:
  - compute_jobs: Job definitions and state
  - compute_workers: Registered worker nodes
  - compute_results: Job outputs
  - compute_state_log: Full state transition audit trail

Run: uvicorn scripts.compute_coordinator:app --host 0.0.0.0 --port 8100
  or: python3 compute_coordinator.py
"""

import os
import sys
import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from loguru import logger as log

app = FastAPI(title="GRID Compute Coordinator", version="1.0.0")


# ── Enums ──────────────────────────────────────────────────────

class JobType(str, Enum):
    SIMULATION = "SIMULATION"
    BACKTEST = "BACKTEST"
    FEATURE_COMPUTE = "FEATURE_COMPUTE"
    HYPOTHESIS_TEST = "HYPOTHESIS_TEST"
    REGIME_DETECT = "REGIME_DETECT"
    LLM_INFERENCE = "LLM_INFERENCE"
    DATA_PULL = "DATA_PULL"
    HUMAN_LLM_QUERY = "HUMAN_LLM_QUERY"


class JobState(str, Enum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    DISPATCHED = "DISPATCHED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    VALID = "VALID"
    ASSIMILATED = "ASSIMILATED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


VALID_TRANSITIONS = {
    JobState.CREATED: [JobState.QUEUED, JobState.CANCELLED],
    JobState.QUEUED: [JobState.DISPATCHED, JobState.CANCELLED],
    JobState.DISPATCHED: [JobState.IN_PROGRESS, JobState.QUEUED, JobState.FAILED],
    JobState.IN_PROGRESS: [JobState.COMPLETED, JobState.FAILED],
    JobState.COMPLETED: [JobState.VALID, JobState.FAILED],
    JobState.VALID: [JobState.ASSIMILATED],
    JobState.ASSIMILATED: [],
    JobState.FAILED: [JobState.QUEUED],
    JobState.CANCELLED: [],
}


# ── Models ─────────────────────────────────────────────────────

class JobCreate(BaseModel):
    job_type: JobType
    name: str
    description: str = ""
    params: dict = {}
    priority: int | dict[str, Any] = 5
    timeout_seconds: int = 3600
    requires_gpu: bool = False
    requires_ollama: bool = False
    tenant: Optional[str] = None
    labels: Optional[dict] = None
    workload: Optional[dict] = None
    yield_policy: Optional[dict] = None
    preemption: Optional[dict] = None
    kill_switch: Optional[dict] = None
    isolation: Optional[dict] = None
    audit: Optional[dict] = None


class WorkerRegister(BaseModel):
    hostname: str
    tailscale_ip: str
    cpu_cores: int = 1
    ram_gb: float = 1.0
    gpu_model: Optional[str] = None
    gpu_vram_gb: Optional[float] = None
    has_ollama: bool = False
    has_docker: bool = False
    max_concurrent: int = 2


class JobResult(BaseModel):
    job_id: int
    worker_id: int
    output: dict = {}
    metrics: dict = {}
    error: Optional[str] = None


# ── Database ───────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def init_tables():
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS compute_workers (
            id              SERIAL PRIMARY KEY,
            hostname        TEXT NOT NULL UNIQUE,
            tailscale_ip    TEXT NOT NULL,
            cpu_cores       INTEGER DEFAULT 1,
            ram_gb          DOUBLE PRECISION DEFAULT 1.0,
            gpu_model       TEXT,
            gpu_vram_gb     DOUBLE PRECISION,
            has_ollama      BOOLEAN DEFAULT FALSE,
            has_docker      BOOLEAN DEFAULT FALSE,
            max_concurrent  INTEGER DEFAULT 2,
            active_jobs     INTEGER DEFAULT 0,
            state           TEXT DEFAULT 'IDLE' CHECK (state IN ('IDLE','BUSY','OFFLINE')),
            last_heartbeat  TIMESTAMPTZ DEFAULT NOW(),
            registered_at   TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS compute_jobs (
            id              SERIAL PRIMARY KEY,
            job_type        TEXT NOT NULL,
            name            TEXT NOT NULL,
            description     TEXT DEFAULT '',
            params          JSONB DEFAULT '{}',
            state           TEXT NOT NULL DEFAULT 'CREATED',
            priority        INTEGER DEFAULT 5,
            timeout_seconds INTEGER DEFAULT 3600,
            requires_gpu    BOOLEAN DEFAULT FALSE,
            requires_ollama BOOLEAN DEFAULT FALSE,
            assigned_worker INTEGER REFERENCES compute_workers(id),
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            queued_at       TIMESTAMPTZ,
            dispatched_at   TIMESTAMPTZ,
            started_at      TIMESTAMPTZ,
            completed_at    TIMESTAMPTZ,
            error_message   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_compute_jobs_state ON compute_jobs (state);
        CREATE INDEX IF NOT EXISTS idx_compute_jobs_type ON compute_jobs (job_type);
        CREATE INDEX IF NOT EXISTS idx_compute_jobs_worker ON compute_jobs (assigned_worker);

        CREATE TABLE IF NOT EXISTS compute_results (
            id          SERIAL PRIMARY KEY,
            job_id      INTEGER NOT NULL REFERENCES compute_jobs(id),
            worker_id   INTEGER NOT NULL REFERENCES compute_workers(id),
            output      JSONB DEFAULT '{}',
            metrics     JSONB DEFAULT '{}',
            error       TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_compute_results_job ON compute_results (job_id);

        CREATE TABLE IF NOT EXISTS compute_state_log (
            id          BIGSERIAL PRIMARY KEY,
            job_id      INTEGER NOT NULL REFERENCES compute_jobs(id),
            from_state  TEXT,
            to_state    TEXT NOT NULL,
            reason      TEXT DEFAULT '',
            worker_id   INTEGER,
            logged_at   TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_compute_state_log_job ON compute_state_log (job_id);
    """)
    conn.close()
    log.info("Compute coordinator tables initialized")


# ── State Machine ──────────────────────────────────────────────

def transition_job(cur, job_id, new_state, reason="", worker_id=None):
    """Transition a job to a new state with validation."""
    cur.execute("SELECT state FROM compute_jobs WHERE id=%s FOR UPDATE", (job_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Job {job_id} not found")

    current = JobState(row["state"] if isinstance(row, dict) else row[0])
    target = JobState(new_state) if isinstance(new_state, str) else new_state

    if target not in VALID_TRANSITIONS.get(current, []):
        raise HTTPException(400, f"Invalid transition: {current} → {target}")

    # Update timestamp columns
    ts_col = {
        JobState.QUEUED: "queued_at",
        JobState.DISPATCHED: "dispatched_at",
        JobState.IN_PROGRESS: "started_at",
        JobState.COMPLETED: "completed_at",
        JobState.FAILED: "completed_at",
    }.get(target)

    # Pre-built queries keyed by validated column name — no f-string SQL
    _TS_QUERIES = {
        "queued_at": "UPDATE compute_jobs SET state=%s, queued_at=NOW() WHERE id=%s",
        "dispatched_at": "UPDATE compute_jobs SET state=%s, dispatched_at=NOW() WHERE id=%s",
        "started_at": "UPDATE compute_jobs SET state=%s, started_at=NOW() WHERE id=%s",
        "completed_at": "UPDATE compute_jobs SET state=%s, completed_at=NOW() WHERE id=%s",
    }
    if ts_col:
        query = _TS_QUERIES.get(ts_col)
        if query is None:
            raise ValueError(f"Invalid timestamp column: {ts_col}")
        cur.execute(query, (target.value, job_id))
    else:
        cur.execute("UPDATE compute_jobs SET state=%s WHERE id=%s", (target.value, job_id))

    if worker_id:
        cur.execute("UPDATE compute_jobs SET assigned_worker=%s WHERE id=%s", (worker_id, job_id))

    # Log transition
    cur.execute(
        "INSERT INTO compute_state_log (job_id,from_state,to_state,reason,worker_id) "
        "VALUES (%s,%s,%s,%s,%s)",
        (job_id, current.value, target.value, reason, worker_id),
    )


def worker_state_for_active_jobs(active_jobs: int) -> str:
    """Return the coordinator-visible worker state for an active job count."""
    return "BUSY" if active_jobs > 0 else "IDLE"


def worker_heartbeat_update_sql() -> str:
    return (
        "UPDATE compute_workers SET last_heartbeat=NOW(), "
        "state=CASE WHEN active_jobs > 0 THEN 'BUSY' ELSE 'IDLE' END "
        "WHERE id=%s"
    )


def worker_heartbeat_with_active_jobs_update_sql() -> str:
    return (
        "UPDATE compute_workers SET active_jobs=GREATEST(%s,0), last_heartbeat=NOW(), "
        "state=CASE WHEN GREATEST(%s,0) > 0 THEN 'BUSY' ELSE 'IDLE' END "
        "WHERE id=%s"
    )


def worker_claim_update_sql() -> str:
    return (
        "UPDATE compute_workers SET active_jobs=active_jobs+1, "
        "state='BUSY', last_heartbeat=NOW() WHERE id=%s"
    )


def worker_complete_update_sql() -> str:
    return (
        "UPDATE compute_workers SET active_jobs=GREATEST(active_jobs-1,0), "
        "state=CASE WHEN GREATEST(active_jobs-1,0) > 0 THEN 'BUSY' ELSE 'IDLE' END, "
        "last_heartbeat=NOW() WHERE id=%s"
    )


def clear_job_error_sql() -> str:
    return "UPDATE compute_jobs SET error_message=NULL WHERE id=%s"


BOOGERBOTS_ALLOWED_PRIORITY_CLASSES = {"boogerbots-low", "boogerbots-background"}
BOOGERBOTS_ALLOWED_WORKLOAD_TYPES = {
    "tts",
    "render",
    "video",
    "lora_training",
    "whisper_qc",
    "eval",
}
BOOGERBOTS_GPU_WORKLOADS = {"render", "video", "lora_training", "whisper_qc"}
BOOGERBOTS_REQUIRED_AUDIT_EVENTS = {
    "submitted",
    "leased",
    "yielded_or_preempted",
    "completed_or_failed",
}
BOOGERBOTS_PRIORITY_CEILING = 30
OCMRI_PRIORITY_FLOOR = BOOGERBOTS_PRIORITY_CEILING + 1
KILL_SWITCH_TRUTHY_VALUES = {"1", "true", "yes", "on", "active", "stop"}
KILL_SWITCH_FALSEY_VALUES = {"", "0", "false", "no", "off", "inactive"}


def _is_mapping(value: Any) -> bool:
    return isinstance(value, dict)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _priority_value(payload: dict[str, Any]) -> int | None:
    priority = payload.get("priority")
    if isinstance(priority, int):
        return priority
    if _is_mapping(priority) and isinstance(priority.get("value"), int):
        return int(priority["value"])
    return None


def db_priority_value(priority: int | dict[str, Any]) -> int:
    """Return an integer DB priority for non-Boogerbots live jobs."""

    if isinstance(priority, int):
        return priority
    if _is_mapping(priority) and isinstance(priority.get("value"), int):
        raise HTTPException(
            400,
            "object priority is reserved for Boogerbots /jobs/dry-run until "
            "live submission is explicitly enabled",
        )
    raise HTTPException(400, "priority must be an integer")


def _env_tripwire_active(env_name: str) -> bool:
    value = os.environ.get(env_name)
    if value is None:
        return False
    normalized = value.strip().lower()
    if normalized in KILL_SWITCH_FALSEY_VALUES:
        return False
    return normalized in KILL_SWITCH_TRUTHY_VALUES or bool(normalized)


def boogerbots_kill_switch_state(payload: dict[str, Any]) -> dict[str, Any]:
    """Inspect the declared Boogerbots kill switch without mutating work."""

    kill_switch = payload.get("kill_switch")
    if not _is_mapping(kill_switch):
        return {
            "configured": False,
            "active": False,
            "tripwires": [],
            "would_accept_new_work": False,
            "would_release_leases": False,
        }

    tripwires: list[dict[str, Any]] = []

    path = kill_switch.get("path")
    if isinstance(path, str) and path:
        active = Path(path).exists()
        tripwires.append({"type": "path", "value": path, "active": active})

    env_name = kill_switch.get("env")
    if isinstance(env_name, str) and env_name:
        active = _env_tripwire_active(env_name)
        tripwires.append({"type": "env", "value": env_name, "active": active})

    endpoint = kill_switch.get("endpoint")
    if isinstance(endpoint, str) and endpoint:
        # The coordinator does not call arbitrary endpoints from validation.
        # Path/env tripwires are the supported live proof mechanism.
        tripwires.append({
            "type": "endpoint",
            "value": endpoint,
            "active": False,
            "checked": False,
        })

    active = any(bool(tripwire.get("active")) for tripwire in tripwires)
    action_ok = kill_switch.get("action") == "stop_new_work_and_release_leases"
    return {
        "configured": bool(tripwires) and action_ok,
        "active": active,
        "tripwires": tripwires,
        "action": kill_switch.get("action"),
        "would_accept_new_work": not active,
        "would_release_leases": active and action_ok,
    }


def boogerbots_scheduler_proof(payload: dict[str, Any]) -> dict[str, Any]:
    priority_value = _priority_value(payload)
    yield_policy = payload.get("yield_policy")
    yield_targets = (
        set(_string_list(yield_policy.get("yield_to")))
        if _is_mapping(yield_policy)
        else set()
    )
    preemption = payload.get("preemption")
    preemption_enabled = _is_mapping(preemption) and preemption.get("enabled") is True
    priority_in_range = (
        isinstance(priority_value, int)
        and 0 <= priority_value <= BOOGERBOTS_PRIORITY_CEILING
    )
    ocmri_priority_wins = (
        priority_in_range
        and "ocmri" in yield_targets
        and preemption_enabled
        and priority_value < OCMRI_PRIORITY_FLOOR
    )
    return {
        "boogerbots_priority_value": priority_value,
        "boogerbots_priority_ceiling": BOOGERBOTS_PRIORITY_CEILING,
        "ocmri_priority_floor": OCMRI_PRIORITY_FLOOR,
        "ocmri_priority_wins": ocmri_priority_wins,
        "yield_to_ocmri": "ocmri" in yield_targets,
        "preemption_enabled": preemption_enabled,
        "claim_order_proof": [
            {"tenant": "ocmri", "priority": OCMRI_PRIORITY_FLOOR},
            {"tenant": "boogerbots", "priority": priority_value},
        ],
    }


def boogerbots_audit_sink_proof(payload: dict[str, Any]) -> dict[str, Any]:
    audit = payload.get("audit")
    if not _is_mapping(audit):
        return {
            "log_sink": "",
            "separate_from_ocmri_sentry": False,
            "required_events_present": False,
            "missing_events": sorted(BOOGERBOTS_REQUIRED_AUDIT_EVENTS),
            "would_record_events": [],
        }

    log_sink = str(audit.get("log_sink", ""))
    events = set(_string_list(audit.get("events")))
    missing_events = BOOGERBOTS_REQUIRED_AUDIT_EVENTS - events
    separate = bool(log_sink) and "ocmri" not in log_sink.lower() and "sentry" not in log_sink.lower()
    return {
        "log_sink": log_sink,
        "separate_from_ocmri_sentry": separate,
        "required_events_present": not missing_events,
        "missing_events": sorted(missing_events),
        "would_record_events": sorted(BOOGERBOTS_REQUIRED_AUDIT_EVENTS),
        "correlation_id": audit.get("correlation_id"),
    }


def boogerbots_w1_proof(payload: dict[str, Any]) -> dict[str, Any]:
    """Return non-mutating W1 scheduling, kill switch, and audit evidence."""

    scheduler = boogerbots_scheduler_proof(payload)
    kill_switch = boogerbots_kill_switch_state(payload)
    audit = boogerbots_audit_sink_proof(payload)
    ready = (
        scheduler["ocmri_priority_wins"]
        and kill_switch["configured"]
        and audit["separate_from_ocmri_sentry"]
        and audit["required_events_present"]
    )
    return {
        "ready": ready,
        "non_mutating": True,
        "scheduler": scheduler,
        "kill_switch": kill_switch,
        "audit": audit,
    }


def boogerbots_contract_errors(payload: dict[str, Any]) -> list[str]:
    """Validate the Storymill Boogerbots W1 contract without touching the DB."""

    errors: list[str] = []

    if payload.get("tenant") != "boogerbots":
        errors.append("tenant must be 'boogerbots'")

    labels = payload.get("labels")
    if not _is_mapping(labels):
        errors.append("labels must be an object")
    else:
        if labels.get("owner") != "boogerbots":
            errors.append("labels.owner must be 'boogerbots'")
        if labels.get("repo") != "3pacs/storymill":
            errors.append("labels.repo must be '3pacs/storymill'")

    workload = payload.get("workload")
    workload_type = workload.get("type") if _is_mapping(workload) else None
    if workload_type not in BOOGERBOTS_ALLOWED_WORKLOAD_TYPES:
        errors.append(
            "workload.type must be one of "
            f"{sorted(BOOGERBOTS_ALLOWED_WORKLOAD_TYPES)}"
        )

    priority = payload.get("priority")
    if not _is_mapping(priority):
        errors.append("priority must be an object")
    else:
        priority_class = priority.get("class")
        priority_value = priority.get("value")
        if priority_class not in BOOGERBOTS_ALLOWED_PRIORITY_CLASSES:
            errors.append(
                "priority.class must be 'boogerbots-low' or "
                "'boogerbots-background'"
            )
        if (
            not isinstance(priority_value, int)
            or priority_value < 0
            or priority_value > 30
        ):
            errors.append("priority.value must be an integer from 0 through 30")

    resources = payload.get("resources")
    resources = resources if _is_mapping(resources) else {}
    gpu = resources.get("gpu", {})
    if workload_type in BOOGERBOTS_GPU_WORKLOADS:
        if not _is_mapping(gpu):
            errors.append("resources.gpu must be an object for GPU workloads")
        elif gpu.get("required") is not True:
            errors.append("GPU workloads must set resources.gpu.required=true")
        if workload_type == "lora_training" and resources.get("off_hours_only") is not True:
            errors.append("lora_training jobs must set resources.off_hours_only=true")

    yield_policy = payload.get("yield_policy")
    if not _is_mapping(yield_policy):
        errors.append("yield_policy must be an object")
    else:
        if "ocmri" not in set(_string_list(yield_policy.get("yield_to"))):
            errors.append("yield_policy.yield_to must include 'ocmri'")
        check_interval = yield_policy.get("check_interval_seconds")
        if not isinstance(check_interval, int) or check_interval < 5 or check_interval > 60:
            errors.append("yield_policy.check_interval_seconds must be between 5 and 60")
        if (
            workload_type in BOOGERBOTS_GPU_WORKLOADS
            and yield_policy.get("idle_window_required") is not True
        ):
            errors.append("GPU workloads must set yield_policy.idle_window_required=true")
        if yield_policy.get("on_ocmri_demand") not in {
            "checkpoint_and_exit",
            "exit_without_start",
            "pause_and_resume",
        }:
            errors.append("yield_policy.on_ocmri_demand has an unsupported action")

    preemption = payload.get("preemption")
    if not _is_mapping(preemption):
        errors.append("preemption must be an object")
    else:
        if preemption.get("enabled") is not True:
            errors.append("preemption.enabled must be true")
        max_yield = preemption.get("max_seconds_to_yield")
        if not isinstance(max_yield, int) or max_yield < 1 or max_yield > 120:
            errors.append("preemption.max_seconds_to_yield must be between 1 and 120")
        if workload_type in {"lora_training", "video"} and not preemption.get(
            "checkpoint_path"
        ):
            errors.append(
                "lora_training and video jobs must declare "
                "preemption.checkpoint_path"
            )

    kill_switch = payload.get("kill_switch")
    if not _is_mapping(kill_switch):
        errors.append("kill_switch must be an object")
    else:
        has_tripwire = any(kill_switch.get(key) for key in ("path", "env", "endpoint"))
        if not has_tripwire:
            errors.append("kill_switch must declare path, env, or endpoint")
        if kill_switch.get("action") != "stop_new_work_and_release_leases":
            errors.append(
                "kill_switch.action must be "
                "'stop_new_work_and_release_leases'"
            )

    isolation = payload.get("isolation")
    if not _is_mapping(isolation):
        errors.append("isolation must be an object")
    else:
        if isolation.get("vm_user") != "boogerbots":
            errors.append("isolation.vm_user must be 'boogerbots'")
        if isolation.get("no_sudo") is not True:
            errors.append("isolation.no_sudo must be true")
        if isolation.get("phi_network_blocked") is not True:
            errors.append("isolation.phi_network_blocked must be true")
        if isolation.get("separate_log_sink") is not True:
            errors.append("isolation.separate_log_sink must be true")

    audit = payload.get("audit")
    if not _is_mapping(audit):
        errors.append("audit must be an object")
    else:
        log_sink = str(audit.get("log_sink", ""))
        if not log_sink:
            errors.append("audit.log_sink is required")
        if "ocmri" in log_sink.lower() or "sentry" in log_sink.lower():
            errors.append("audit.log_sink must be separate from OCMRI/Sentry")
        events = set(_string_list(audit.get("events")))
        missing_events = BOOGERBOTS_REQUIRED_AUDIT_EVENTS - events
        if missing_events:
            errors.append(f"audit.events missing {sorted(missing_events)}")
        if not audit.get("correlation_id"):
            errors.append("audit.correlation_id is required")

    return errors


# ── Endpoints ──────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    init_tables()


@app.get("/health")
async def health():
    return {"status": "ok", "service": "compute_coordinator", "timestamp": datetime.utcnow().isoformat()}


@app.post("/workers/register")
async def register_worker(w: WorkerRegister):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "INSERT INTO compute_workers (hostname,tailscale_ip,cpu_cores,ram_gb,gpu_model,"
        "gpu_vram_gb,has_ollama,has_docker,max_concurrent) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (hostname) DO UPDATE SET tailscale_ip=EXCLUDED.tailscale_ip, "
        "cpu_cores=EXCLUDED.cpu_cores, ram_gb=EXCLUDED.ram_gb, gpu_model=EXCLUDED.gpu_model, "
        "gpu_vram_gb=EXCLUDED.gpu_vram_gb, has_ollama=EXCLUDED.has_ollama, "
        "has_docker=EXCLUDED.has_docker, max_concurrent=EXCLUDED.max_concurrent, "
        "active_jobs=0, last_heartbeat=NOW(), state='IDLE' "
        "RETURNING *",
        (w.hostname, w.tailscale_ip, w.cpu_cores, w.ram_gb, w.gpu_model,
         w.gpu_vram_gb, w.has_ollama, w.has_docker, w.max_concurrent),
    )
    worker = dict(cur.fetchone())
    conn.close()
    log.info("Worker registered: {h} ({ip})", h=w.hostname, ip=w.tailscale_ip)
    return worker


@app.get("/workers")
async def list_workers():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM compute_workers ORDER BY id")
    workers = [dict(r) for r in cur.fetchall()]
    conn.close()
    return workers


@app.post("/workers/{worker_id}/heartbeat")
async def worker_heartbeat(worker_id: int, active_jobs: Optional[int] = None):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    if active_jobs is None:
        cur.execute(worker_heartbeat_update_sql(), (worker_id,))
    else:
        cur.execute(worker_heartbeat_with_active_jobs_update_sql(), (active_jobs, active_jobs, worker_id))
    conn.close()
    return {"status": "ok"}


@app.post("/jobs")
async def create_job(job: JobCreate):
    if job.tenant == "boogerbots":
        raise HTTPException(
            400,
            "Boogerbots contract payloads must pass /jobs/dry-run before live "
            "submission is enabled",
        )
    db_priority = db_priority_value(job.priority)
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "INSERT INTO compute_jobs (job_type,name,description,params,priority,"
        "timeout_seconds,requires_gpu,requires_ollama,state) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'CREATED') RETURNING *",
        (job.job_type.value, job.name, job.description, json.dumps(job.params),
         db_priority, job.timeout_seconds, job.requires_gpu, job.requires_ollama),
    )
    created = dict(cur.fetchone())

    # Auto-queue
    transition_job(cur, created["id"], JobState.QUEUED, "auto-queued on creation")
    cur.execute("SELECT * FROM compute_jobs WHERE id=%s", (created["id"],))
    result = dict(cur.fetchone())
    conn.close()
    log.info("Job created: #{id} {name} ({type})", id=result["id"], name=job.name, type=job.job_type.value)
    return result


@app.post("/jobs/dry-run")
async def dry_run_job(payload: dict):
    """Validate a proposed job without writing compute_jobs or claiming work."""

    errors = boogerbots_contract_errors(payload)
    w1_proof = boogerbots_w1_proof(payload)
    return {
        "status": "accepted" if not errors else "rejected",
        "dry_run": True,
        "would_enqueue": False,
        "would_accept_new_work": not errors and w1_proof["kill_switch"]["would_accept_new_work"],
        "would_release_leases": w1_proof["kill_switch"]["would_release_leases"],
        "mutating_actions_performed": [],
        "tenant": payload.get("tenant"),
        "errors": errors,
        "w1_proof": w1_proof,
    }


@app.get("/jobs")
async def list_jobs(state: Optional[str] = None, job_type: Optional[str] = None, limit: int = 50):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query = "SELECT * FROM compute_jobs WHERE TRUE"
    params = []
    if state:
        query += " AND state=%s"
        params.append(state)
    if job_type:
        query += " AND job_type=%s"
        params.append(job_type)
    query += " ORDER BY priority DESC, created_at ASC LIMIT %s"
    params.append(limit)
    cur.execute(query, params)
    jobs = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jobs


@app.get("/jobs/{job_id}")
async def get_job(job_id: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM compute_jobs WHERE id=%s", (job_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Job not found")
    return dict(row)


@app.get("/metadata/compute-inputs")
async def compute_inputs(model_limit: int = 16, feature_limit: int = 20):
    """Return coordinator-DB-valid IDs for external job producers."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "SELECT id FROM model_registry "
        "WHERE feature_set IS NOT NULL AND cardinality(feature_set) > 0 "
        "ORDER BY id LIMIT %s",
        (model_limit,),
    )
    model_ids = [int(row["id"]) for row in cur.fetchall()]
    cur.execute(
        "SELECT id FROM feature_registry "
        "WHERE model_eligible=TRUE ORDER BY id LIMIT %s",
        (feature_limit,),
    )
    feature_ids = [int(row["id"]) for row in cur.fetchall()]
    conn.close()
    return {"model_ids": model_ids, "feature_ids": feature_ids}


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: int):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    transition_job(cur, job_id, JobState.CANCELLED, "cancelled by operator")
    conn.close()
    return {"status": "cancelled", "job_id": job_id}


@app.post("/jobs/claim")
async def claim_job(
    worker_id: int,
    gpu_available: bool = False,
    ollama_available: bool = False,
    job_type: Optional[str] = None,
    exclude_types: Optional[str] = None,
):
    """Worker claims the next available job matching its capabilities.

    Args:
        job_type: Only claim jobs of this type (e.g. HUMAN_LLM_QUERY).
        exclude_types: Comma-separated job types to skip (e.g. HUMAN_LLM_QUERY).
    """
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        cur.execute(
            "SELECT active_jobs,max_concurrent FROM compute_workers WHERE id=%s FOR UPDATE",
            (worker_id,),
        )
        worker = cur.fetchone()
        if not worker:
            raise HTTPException(404, f"Worker {worker_id} not found")

        max_concurrent = max(int(worker["max_concurrent"] or 1), 1)
        if int(worker["active_jobs"] or 0) >= max_concurrent:
            cur.execute(worker_heartbeat_update_sql(), (worker_id,))
            conn.commit()
            return {"status": "no_capacity"}

        # Find best matching job
        query = "SELECT id FROM compute_jobs WHERE state='QUEUED'"
        conditions = []
        params_list = []
        if not gpu_available:
            conditions.append("requires_gpu = FALSE")
        if not ollama_available:
            conditions.append("requires_ollama = FALSE")
        if job_type:
            conditions.append("job_type = %s")
            params_list.append(job_type)
        if exclude_types:
            for et in exclude_types.split(","):
                et = et.strip()
                if et:
                    conditions.append("job_type != %s")
                    params_list.append(et)
        if conditions:
            query += " AND " + " AND ".join(conditions)
        query += " ORDER BY priority DESC, created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED"

        cur.execute(query, params_list if params_list else None)
        row = cur.fetchone()

        if not row:
            cur.execute(worker_heartbeat_update_sql(), (worker_id,))
            conn.commit()
            return {"status": "no_jobs"}

        job_id = row["id"]
        transition_job(cur, job_id, JobState.DISPATCHED, f"claimed by worker {worker_id}", worker_id)
        cur.execute(clear_job_error_sql(), (job_id,))

        # Update worker active count and heartbeat in the same transaction as the claim.
        cur.execute(worker_claim_update_sql(), (worker_id,))

        cur.execute("SELECT * FROM compute_jobs WHERE id=%s", (job_id,))
        job = dict(cur.fetchone())
        conn.commit()
        log.info("Job #{id} claimed by worker {w}", id=job_id, w=worker_id)
        return job
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.post("/jobs/{job_id}/start")
async def start_job(job_id: int, worker_id: int):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    transition_job(cur, job_id, JobState.IN_PROGRESS, "worker started execution", worker_id)
    cur.execute(worker_heartbeat_update_sql(), (worker_id,))
    conn.close()
    return {"status": "started", "job_id": job_id}


@app.post("/jobs/{job_id}/complete")
async def complete_job(job_id: int, result: JobResult):
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        if result.error:
            transition_job(cur, job_id, JobState.FAILED, result.error, result.worker_id)
            cur.execute("UPDATE compute_jobs SET error_message=%s WHERE id=%s", (result.error, job_id))
        else:
            transition_job(cur, job_id, JobState.COMPLETED, "worker reported completion", result.worker_id)
            cur.execute(clear_job_error_sql(), (job_id,))

        # Store result
        cur.execute(
            "INSERT INTO compute_results (job_id,worker_id,output,metrics,error) VALUES (%s,%s,%s,%s,%s)",
            (job_id, result.worker_id, json.dumps(result.output),
             json.dumps(result.metrics), result.error),
        )

        # Decrement worker active count and derive state atomically.
        cur.execute(worker_complete_update_sql(), (result.worker_id,))

        conn.commit()
        log.info("Job #{id} completed by worker {w}", id=job_id, w=result.worker_id)
        return {"status": "completed" if not result.error else "failed", "job_id": job_id}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.post("/jobs/{job_id}/validate")
async def validate_job(job_id: int):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    transition_job(cur, job_id, JobState.VALID, "validated by operator")
    conn.close()
    return {"status": "valid", "job_id": job_id}


@app.post("/jobs/{job_id}/assimilate")
async def assimilate_job(job_id: int):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    transition_job(cur, job_id, JobState.ASSIMILATED, "results assimilated into GRID")
    conn.close()
    return {"status": "assimilated", "job_id": job_id}


@app.get("/stats")
async def coordinator_stats():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT state, count(*) as count FROM compute_jobs GROUP BY state")
    job_states = {r["state"]: r["count"] for r in cur.fetchall()}

    cur.execute("SELECT count(*) as total, count(*) FILTER (WHERE state='IDLE') as idle, "
                "count(*) FILTER (WHERE state='BUSY') as busy FROM compute_workers")
    workers = dict(cur.fetchone())

    cur.execute("SELECT job_type, count(*) as count FROM compute_jobs GROUP BY job_type")
    by_type = {r["job_type"]: r["count"] for r in cur.fetchall()}

    conn.close()
    return {
        "job_states": job_states,
        "workers": workers,
        "jobs_by_type": by_type,
        "timestamp": datetime.utcnow().isoformat(),
    }


# ── CLI Entry Point ────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    log.info("Starting GRID Compute Coordinator on :8100")
    uvicorn.run(app, host="0.0.0.0", port=8100)
