# Workflows & Compute — Orchestration Layer

## Workflow Engine

GRID uses declarative workflows defined as Markdown files with YAML frontmatter.
This makes workflows human-readable, version-controlled, and self-documenting.

### Workflow Definition Format

```yaml
---
name: pull-ecb
group: ingestion
schedule: "daily 06:00"
secrets: []
depends_on: []
description: Pull ECB SDW data for Euro area rates and aggregates
---

## Steps
1. Connect to ECB SDW API
2. Pull configured series (M3, bank lending, TARGET2, yield curves)
3. Insert into raw_series with source tracking

## Output
- New rows in raw_series for ECB source

## Notes
- Respects ECB rate limits
- Handles partial failures gracefully
```

### Workflow Groups

| Group | Purpose | Examples |
|-------|---------|---------|
| ingestion | Pull raw data | pull-ecb, pull-bls, pull-annual-datasets |
| resolution | Resolve conflicts | resolve-conflicts |
| features | Compute features | compute-features |
| discovery | Run discovery | audit-orthogonality, run-clustering |
| validation | Backtest | validate-backtest, sweep-parameters |
| physics | Verification | verify-physics |

### Enable/Disable

Workflows live in `workflows/available/`. Enabled workflows are symlinked to
`workflows/enabled/`. This allows quick enable/disable without deleting files.

## Wave-Based Execution

Independent tasks are grouped into waves for parallel execution:

```
Wave 0: [pull-fred, pull-yfinance, pull-bls, pull-ecb, ...]  (parallel)
         ↓ all complete
Wave 1: [resolve-conflicts, compute-features]                 (parallel)
         ↓ all complete
Wave 2: [audit-orthogonality, run-clustering]                 (parallel)
         ↓ all complete
Wave 3: [validate-backtest, verify-physics]                   (parallel)
         ↓ all complete
Wave 4: [model-promotion]                                     (sequential)
```

### How It Works

1. **Dependency graph** — Each workflow declares `depends_on`
2. **Topological sort** — Tasks ordered by dependencies
3. **Wave grouping** — Same-depth tasks grouped into waves
4. **Parallel execution** — Tasks within a wave run via ThreadPoolExecutor
5. **Sequential waves** — Next wave starts only when all previous tasks complete
6. **Circular dependency detection** — Raises error if detected

### Task Tracking

Each task reports: status (pending|running|success|failed), error message,
duration_ms. Visible in the PWA Workflows page.

## Key Files

- `workflows/loader.py` — Load, enable, disable, validate workflows
- `workflows/available/*.md` — Workflow definitions
- `physics/waves.py` — WaveTask, build_execution_waves(), execute_waves()
- `api/routers/workflows.py` — REST endpoints
