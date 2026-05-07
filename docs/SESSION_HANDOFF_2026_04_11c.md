# Session Handoff — 2026-04-11c

**Merged:** PR [#37](https://github.com/3pacs/GRID/pull/37) → `1b67012a`
**Tag:** `phase-1-contracts`
**Scope:** Information flow Phase 1 (contracts infrastructure) + 25 pre-existing test/prod fixes
**Result:** 2563 non-contracts tests + 70 contracts tests all passing. Zero regressions.

---

## 1. What shipped

### Contracts infrastructure (new `contracts/` package)

Thin adapter layer over `events/bus.py` that formalises cross-module information flow through typed schemas, correlation-id lineage, dead-letter replay, and observability. **Empty `ROUTES` dict** — dispatcher runs but routes nothing yet. Phase 2 wires real handler bindings on top.

| File | Purpose |
|---|---|
| `contracts/schemas.py` | `BaseContract` + 13 frozen Pydantic models (`extra="forbid"`) |
| `contracts/channels.py` | `channel_for(cls)` → `grid_contracts_<snake>`; reverse lookup |
| `contracts/correlation.py` | `ContextVar`-based correlation scope |
| `contracts/emit.py` | `emit(contract)` + `pull_lifecycle()` context manager |
| `contracts/router.py` | Empty `ROUTES` + `resolve_handler()` + integrity test |
| `contracts/dispatcher.py` | Subscribe → validate → dispatch → dead-letter |
| `contracts/dead_letter.py` | CRUD + retry schedule (1min / 10min / 1hr) |
| `contracts/retry_scheduler.py` | Background daemon thread draining due retries |
| `contracts/observability.py` | Counters + histograms + Prometheus text export |
| `contracts/replay.py` | `python -m contracts.replay` CLI |
| `contracts/handlers/` | Empty package (Phase 2 target) |
| `api/routers/contracts.py` | `/metrics`, `/lineage/{cid}`, `/dead-letter/{id}/replay` |
| `tests/contracts/` | 70 tests (67 unit + 3 integration) |

Wired into `api/main.py` lifespan startup with graceful exception fallback.

### The 13 contracts (schemas + channels only — handlers in Phase 2)

`PostmortemCompleted`, `PredictionScored`, `BacktestGateVerdict`, `OptionsTradeOutcome`, `CrossReferenceAnomaly`, `LeverageRiskUpdate`, `RegimeTransition`, `SignalFired`, `HypothesisGenerated`, `ActorMaterialized`, `PullLifecycle`, `ForensicsTrace`, `InvestigationProgress`

### 25+ pre-existing fixes (load-bearing)

Full breakdown in commit messages, but the headline items:

1. **`features/lab.py::zscore_normalize`** — was returning NaN for constant-value series. Any feed that went flat silently broke downstream inference. Now returns 0.0 (mathematically correct — constant value is exactly at the rolling mean). Three old tests that enshrined the bug updated to match.

2. **`prevent_journal_delete` trigger** — was unconditionally blocking all DELETEs, so tests could never clean up. Replaced with a trigger that permits DELETE only when BOTH `app.journal_testing = 'on'` (session GUC, `SET LOCAL` only) AND `annotation = 'TEST_JOURNAL'`. Production immutability verified rigorously.

3. **`canvas_nodes` column rename** — 163 references across 6 canvas router files wrote `INSERT INTO canvas_nodes (node_id, ...)` but the live schema had the column as `id`. Every canvas write path was silently broken at runtime. Added idempotent `ALTER TABLE canvas_nodes RENAME COLUMN id TO node_id` migration + updated the one outlier router file + [[Alembic]] migration for fresh installs.

4. **`sys.modules['api.auth']` pollution** — five test files installed a stub via `setdefault`, breaking import for every subsequent test that needed `create_token`. Fixed all five to prefer the real import.

5. **4 collection-error crashes** — `outputs/llm_logger` mkdir on broken symlink, missing `aiohttp`/`networkx` deps, Python 3.10 union syntax without `from __future__ import annotations`, `nx.hits()` float roundoff.

6. **Hanging tests** — PMXT fixture didn't patch `time.sleep`, so real 1.5s-per-keyword rate limit burned 60s per test. Resolver integration tests took 140s each against real DB.

7. **Optional-dep import** — `foia_cables.py` crashed on unconditional `from playwright import`. Made optional.

8. **Stale FTS query** — `plainto_tsquery` was parsing `|` as conjunction (not OR). Switched to `websearch_to_tsquery` with `OR`.

9. **Schema drift** — test fixture INSERTs missing `signal_date` defaults. Stale router references to `/nodes`/`/edges` that moved to `canvas_graph.py`. Test assertion expecting 7 checks when production added an 8th.

10. **Vendor-dep tests** — `test_trending_news` required external `vendor/last30days-skill` clone. Mocked.

---

## 2. Migrations to apply (sequence matters)

**Already applied to dev DB.** For grid-svr / production:

```bash
psql $GRID_DATABASE_URL -f scripts/migrations/20260411_contracts_infrastructure.sql
psql $GRID_DATABASE_URL -f scripts/migrations/20260411_journal_test_cleanup.sql
psql $GRID_DATABASE_URL -f scripts/migrations/20260411_rename_canvas_nodes_id.sql
```

All three are idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE OR REPLACE FUNCTION`, and a DO-block that checks column existence before renaming).

**The canvas rename migration is load-bearing** — 6 canvas router files will stay silently broken in production until it runs. Apply it before the next canvas feature deploy.

---

## 3. Deploy to grid-svr checklist

```bash
# 1. Pull
ssh grid-svr
cd ~/grid_v4
git pull origin main

# 2. Apply migrations
psql $GRID_DATABASE_URL -f scripts/migrations/20260411_contracts_infrastructure.sql
psql $GRID_DATABASE_URL -f scripts/migrations/20260411_journal_test_cleanup.sql
psql $GRID_DATABASE_URL -f scripts/migrations/20260411_rename_canvas_nodes_id.sql

# 3. Restart API
sudo systemctl restart grid-api

# 4. Smoke-test contracts endpoints
curl -H "Authorization: Bearer $GRID_ADMIN_TOKEN" \
     https://grid.stepdad.finance/api/v1/contracts/metrics
# Expected: Prometheus text format with zero counts (no contracts emitted yet)

curl -H "Authorization: Bearer $GRID_ADMIN_TOKEN" \
     https://grid.stepdad.finance/api/v1/contracts/lineage/00000000-0000-0000-0000-000000000000
# Expected: {"events":[]}

# 5. Canvas smoke — after the column rename, any canvas_predict / canvas_expand /
#    canvas_investigate call that writes a node should now succeed instead of
#    raising "column node_id does not exist".
```

---

## 4. Known operational concerns

### Codex auto-commit collision

During this session, a parallel Codex session committed `f7c1c74b "Wire AstroGrid Guru ask flow"` directly onto `contracts-phase-1` because Codex commits to whatever branch `HEAD` happens to be on. The same content was committed as `4d091763` on main, and main then added two Guru-extension commits (`a91addae`, `536f1d4d`). Merge conflict was resolved by taking main's newer version via `--theirs`.

**Fix for future sessions:** Either put Claude in a git worktree (`git worktree add ../grid-contracts contracts-phase-1`) or constrain Codex to its own branch. A Codex worktree already exists at `/private/tmp/grid-guru-main` — presumably Codex's workspace.

### pytest-timeout not in requirements.txt

Installed `pytest-timeout>=2.4.0` in the venv during this session but didn't commit the requirement. Add it to `requirements.txt` to pin it for CI.

### Integration test marker warnings

`@pytest.mark.integration` is used throughout but not registered, producing `PytestUnknownMarkWarning`. Register in a `pytest.ini` / `pyproject.toml`:

```toml
[tool.pytest.ini_options]
markers = [
    "integration: tests that require a live PostgreSQL",
    "slow: tests that take more than 10 seconds",
]
```

### SQL injection warnings in `api/routers/intel.py`

23 f-string-in-SQL sites flagged by `tests/test_regression_20260329.py:443` as warnings (lines 1321, 1448, 1481, 1511, 1540 + others). Not failing, but they belong on a dedicated security PR.

---

## 5. What's next — Phase 2

Phase 1 delivered the infrastructure. Phase 2 wires the 13 real contracts into `ROUTES` so the feedback loops actually close.

### Phase 2 scope (per spec §4b — fully enumerated)

**New small public methods on existing modules:**
- `intelligence/trust_scorer.py` → `apply_failure_feedback`, `apply_success_feedback`, `set_regime_context`
- `oracle/engine.py` → `set_regime_context`, `get_risk_damper`
- `inference/live.py` → `get_risk_damper`
- `discovery/options_scanner.py` → `OptionsScanner.update_signal_weights`
- `trading/options_recommender.py` → `OptionsRecommender.set_regime`
- `intelligence/actor_network.py` → `upsert_actor`
- `intelligence/causation_graph.py` → `ingest_forensics_trace`
- `intelligence/sleuth.py` → `Sleuth.add_lead`, `Sleuth.ingest_trace`
- `normalization/entity_map.py` → `EntityMap.merge_actor`
- `governance/registry.py` → `ModelRegistry.create_candidate`
- `api/routers/canvas_investigate.py` → extract `seed_investigation()` library from the existing async endpoint
- `api/routers/canvas.py` → `emit_signal_to_live_boards`, `emit_actor_to_live_boards`, `emit_trace_to_live_boards`

**New tables:**
- `model_evolver_feedback_queue` — per-prediction outcome buffer between scheduled `evolve_cycle()` runs
- `oracle_signal_queue` — per-signal buffer between oracle cycles
- `inference_risk_state` — single-row durable risk damper
- `data_health_events` — pull lifecycle history

**New supporting modules:**
- `events/sse.py::broadcast(channel, payload)` — 15-LOC passthrough over `bus.emit_sync()`
- `intelligence/data_health.py` — `record_lifecycle`, `recent_failures`, `coverage_snapshot` (~120 LOC)
- `contracts/emit.py::pull_lifecycle` — **already exists**, Phase 2 just wires each puller with a one-line `with pull_lifecycle(engine, name) as rows:`

**Handlers (all in `contracts/handlers/`, ~20 LOC each):**
- `trust.on_postmortem_completed`, `trust.on_prediction_scored`, `trust.on_regime_transition`, `trust.on_options_trade_outcome`
- `model_evolver.on_postmortem_completed`, `model_evolver.on_prediction_scored`
- `journal.on_prediction_scored`, `journal.on_backtest_gate_verdict`
- `governance.on_backtest_gate_verdict`, `governance.on_hypothesis_generated`
- `alerts.on_postmortem_completed`, `alerts.on_backtest_gate_verdict`, `alerts.on_cross_reference_anomaly`, `alerts.on_leverage_risk_update`, `alerts.on_pull_lifecycle`
- `options_scanner.on_options_trade_outcome`
- `postmortem.on_prediction_scored`, `postmortem.on_options_trade_outcome`
- `inference.on_leverage_risk_update`
- `oracle.on_leverage_risk_update`, `oracle.on_regime_transition`, `oracle.on_signal_fired`
- `trading.on_regime_transition`
- `canvas.on_cross_reference_anomaly`, `canvas.on_signal_fired`, `canvas.on_actor_materialized`, `canvas.on_forensics_trace`
- `sleuth.on_cross_reference_anomaly`, `sleuth.on_forensics_trace`
- `hypothesis.on_hypothesis_generated`
- `validation.on_hypothesis_generated`
- `entity_map.on_actor_materialized`
- `actor_network.on_actor_materialized`
- `actor_bridge.on_signal_fired`
- `causation.on_forensics_trace`
- `data_health.on_pull_lifecycle`
- `sse.on_regime_transition`, `sse.on_signal_fired`, `sse.on_pull_lifecycle`, `sse.on_investigation_progress`

**~50 producer emit sites** — one-line `with pull_lifecycle(engine, "<name>") as rows:` per puller, plus single emit calls at each producer module ([[Postmortem|postmortem]], [[Oracle Engine|oracle engine]], gates, [[Options Tracker|options tracker]], cross_reference, leverage_network, clustering, hypothesis_engine, actor_discovery, [[Forensics|forensics]], canvas_investigate).

**Expected outcome after Phase 2:** feedback loops close — postmortem failures decay signal trust, oracle weights evolve with scored predictions, backtest verdicts land in the journal, regime transitions re-weight sources, options trade outcomes tune the 7-signal scanner weights.

---

## 6. Verification commands

```bash
cd /Users/anikdang/dev/GRID

# Contracts suite
.venv/bin/python -m pytest tests/contracts/ -v

# Full non-contracts suite
.venv/bin/python -m pytest tests/ --ignore=tests/contracts --timeout=300

# App import smoke
.venv/bin/python -c "from api.main import app; print('ok')"

# Contract routes register
.venv/bin/python -c "from api.main import app; print(sorted(r.path for r in app.routes if '/contracts' in getattr(r, 'path', '')))"

# DB state
.venv/bin/python -c "
from sqlalchemy import create_engine, text
e = create_engine('postgresql://grid_user:changeme@localhost:5432/grid')
with e.connect() as c:
    for t in ['contracts_audit', 'contracts_dead_letter']:
        r = c.execute(text(f'SELECT to_regclass(:t)').bindparams(t=t)).fetchone()
        print(f'{t}: {r[0]}')
    rows = c.execute(text(\"SELECT column_name FROM information_schema.columns WHERE table_name = 'canvas_nodes' ORDER BY ordinal_position\")).fetchall()
    print('canvas_nodes columns:', [r[0] for r in rows])
"
```

---

## 7. Artifacts

- **Spec:** `docs/superpowers/specs/2026-04-11-information-flow-optimization-design.md`
- **Plan:** `docs/superpowers/plans/2026-04-11-information-flow-phase-1.md`
- **PR:** https://github.com/3pacs/GRID/pull/37 (merged)
- **Merge commit:** `1b67012a`
- **Tag:** `phase-1-contracts`
- **Branch:** `contracts-phase-1` (kept alive, not deleted — Codex auto-commit in history)
