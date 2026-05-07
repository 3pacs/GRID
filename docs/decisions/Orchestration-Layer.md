---
source: /Users/anikdang/grid_obsidian/Architecture/Orchestration-Layer.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
# Orchestration Layer

7 files managing distributed compute, event coordination, and LLM task queuing.

## Event Bus (`orchestration/event_bus.py`)

[[PostgreSQL]]-backed event bus and task queue. Uses `LISTEN/NOTIFY` for real-time delivery with polling fallback. No Redis/Celery dependency.

```
bus = EventBus(engine)
bus.emit("gap_discovered", {"feature": "vix_spot", "days_stale": 3})

queue = TaskQueue(engine)
queue.enqueue("baseline_compute", {"signal": "vix_spot"}, node_target="gridz4")
task = queue.claim("gridz4")
queue.complete(task["id"], result={"baseline": {...}})
```

**Tables used**: `event_log`, `task_queue`

## LLM Task Queue (`orchestration/llm_taskqueue.py`)

Priority queue keeping the local Qwen 32B model working continuously.

**Priority levels:**
1. Real-time requests (trade reviews, user chat)
2. [[Hermes Scheduler|Hermes]] cycle tasks (scheduled work)
3. Background generators (feature interpretation, hypothesis generation)

Auto-fills with background work when queue is empty. Provides status introspection via API.

**API endpoints**: `GET /api/v1/system/llm-status`, `POST /api/v1/system/llm-task`

## Distributed Worker (`orchestration/grid_worker.py`)

Runs on gridz4 (or any compute node). Polls `task_queue` via Tailscale Postgres.

- Claims tasks, executes them, writes results back
- Heartbeat every 60s
- Deploy as systemd service
- Supports explicit node targeting

## Distributed Compute Engine (`subnet/distributed_compute.py`)

BOINC-style volunteer compute network:
- Edge contributors process GRID research tasks
- Earn API credits for quality responses
- API credits grant access to GRID intelligence
- Validates responses before crediting

**API endpoints**: `GET /api/v1/compute/task`, `POST /api/v1/compute/submit`

## Reconciliation (`orchestration/reconcile.py`)

Reconciles results from distributed compute — validates, deduplicates, and merges.

## Integration (`orchestration/integrate.py`)

Integrates orchestration components with the broader GRID system.

## Dispatch (`orchestration/dispatch.py`)

Task dispatch logic for routing work to appropriate compute nodes.

## Related

- [[Hermes-Operator]] — Primary orchestrator for automated cycles
- [[Agents-System]] — LLM agent orchestration
- [[Alerts-System]] — Email, push, Telegram alerts
- [[Modules/Alerts|Alerts Module]] — Multi-channel alert implementation
- [[Cron-Schedule]] — Scheduled task timing
- [[MCP-Server]] — Model Context Protocol + A2A
- [[Governance-Payments]] — x402 micropayments, workflows, journal
- [[Subnet-Compute]] — BOINC-style distributed compute
