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
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

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
    priority: int = 5
    timeout_seconds: int = 3600
    requires_gpu: bool = False
    requires_ollama: bool = False


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

    current = JobState(row[0])
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

    _ALLOWED_TS_COLS = {"queued_at", "dispatched_at", "started_at", "completed_at"}
    if ts_col:
        if ts_col not in _ALLOWED_TS_COLS:
            raise ValueError(f"Invalid timestamp column: {ts_col}")
        cur.execute(f"UPDATE compute_jobs SET state=%s, {ts_col}=NOW() WHERE id=%s",
                    (target.value, job_id))
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
        "last_heartbeat=NOW(), state='IDLE' RETURNING *",
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
async def worker_heartbeat(worker_id: int):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("UPDATE compute_workers SET last_heartbeat=NOW() WHERE id=%s", (worker_id,))
    conn.close()
    return {"status": "ok"}


@app.post("/jobs")
async def create_job(job: JobCreate):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "INSERT INTO compute_jobs (job_type,name,description,params,priority,"
        "timeout_seconds,requires_gpu,requires_ollama,state) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'CREATED') RETURNING *",
        (job.job_type.value, job.name, job.description, json.dumps(job.params),
         job.priority, job.timeout_seconds, job.requires_gpu, job.requires_ollama),
    )
    created = dict(cur.fetchone())

    # Auto-queue
    transition_job(cur, created["id"], JobState.QUEUED, "auto-queued on creation")
    cur.execute("SELECT * FROM compute_jobs WHERE id=%s", (created["id"],))
    result = dict(cur.fetchone())
    conn.close()
    log.info("Job created: #{id} {name} ({type})", id=result["id"], name=job.name, type=job.job_type.value)
    return result


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


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: int):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    transition_job(cur, job_id, JobState.CANCELLED, "cancelled by operator")
    conn.close()
    return {"status": "cancelled", "job_id": job_id}


@app.post("/jobs/claim")
async def claim_job(worker_id: int, gpu_available: bool = False, ollama_available: bool = False):
    """Worker claims the next available job matching its capabilities."""
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Find best matching job
    query = "SELECT id FROM compute_jobs WHERE state='QUEUED'"
    conditions = []
    if not gpu_available:
        conditions.append("requires_gpu = FALSE")
    if not ollama_available:
        conditions.append("requires_ollama = FALSE")
    if conditions:
        query += " AND " + " AND ".join(conditions)
    query += " ORDER BY priority DESC, created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED"

    cur.execute(query)
    row = cur.fetchone()

    if not row:
        conn.close()
        return {"status": "no_jobs"}

    job_id = row["id"]
    transition_job(cur, job_id, JobState.DISPATCHED, f"claimed by worker {worker_id}", worker_id)

    # Update worker active count
    cur.execute("UPDATE compute_workers SET active_jobs=active_jobs+1, state='BUSY' WHERE id=%s", (worker_id,))

    cur.execute("SELECT * FROM compute_jobs WHERE id=%s", (job_id,))
    job = dict(cur.fetchone())
    conn.close()
    log.info("Job #{id} claimed by worker {w}", id=job_id, w=worker_id)
    return job


@app.post("/jobs/{job_id}/start")
async def start_job(job_id: int, worker_id: int):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    transition_job(cur, job_id, JobState.IN_PROGRESS, "worker started execution", worker_id)
    conn.close()
    return {"status": "started", "job_id": job_id}


@app.post("/jobs/{job_id}/complete")
async def complete_job(job_id: int, result: JobResult):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    if result.error:
        transition_job(cur, job_id, JobState.FAILED, result.error, result.worker_id)
        cur.execute("UPDATE compute_jobs SET error_message=%s WHERE id=%s", (result.error, job_id))
    else:
        transition_job(cur, job_id, JobState.COMPLETED, "worker reported completion", result.worker_id)

    # Store result
    cur.execute(
        "INSERT INTO compute_results (job_id,worker_id,output,metrics,error) VALUES (%s,%s,%s,%s,%s)",
        (job_id, result.worker_id, json.dumps(result.output),
         json.dumps(result.metrics), result.error),
    )

    # Decrement worker active count
    cur.execute(
        "UPDATE compute_workers SET active_jobs=GREATEST(active_jobs-1,0) WHERE id=%s",
        (result.worker_id,),
    )
    cur.execute(
        "UPDATE compute_workers SET state='IDLE' WHERE id=%s AND active_jobs=0",
        (result.worker_id,),
    )

    conn.close()
    log.info("Job #{id} completed by worker {w}", id=job_id, w=result.worker_id)
    return {"status": "completed" if not result.error else "failed", "job_id": job_id}


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
