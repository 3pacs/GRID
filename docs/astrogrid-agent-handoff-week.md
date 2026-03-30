# AstroGrid Agent Handoff — Next Week

## Purpose

This document is the working handoff for the next agent continuing AstroGrid. It is not a product brief. It is an execution document: current state, what is stable, what is weak, what to run first, and what not to break.

## Current Repo State

- Repo: `/Users/anikdang/dev/GRID`
- Branch: `main`
- Current head when this handoff was written: `6f8dec0`
- Public app: [https://grid.stepdad.finance/astrogrid/](https://grid.stepdad.finance/astrogrid/)
- Server worktree: `/data/grid_v4/astrogrid_dedup`

## Current Local Dirty State

At handoff time, these files are dirty or untracked locally and are **not AstroGrid work owned by this handoff**:

- `/Users/anikdang/dev/GRID/.coordination.md`
- `/Users/anikdang/dev/GRID/api/main.py`
- `/Users/anikdang/dev/GRID/subnet/distributed_compute.py`
- `/Users/anikdang/dev/GRID/api/routers/intel.py`
- `/Users/anikdang/dev/GRID/scripts/baseline_predictions.py`

Do not fold those into AstroGrid commits unless you explicitly audit and intend to own them.

## AstroGrid State Summary

AstroGrid is operational across the full learning loop:

- prediction logging
- immediate postmortem persistence
- outcome scoring
- backtests
- review runs
- weight proposals

The main backend implementation is in:

- `/Users/anikdang/dev/GRID/store/astrogrid.py`
- `/Users/anikdang/dev/GRID/api/routers/astrogrid.py`
- `/Users/anikdang/dev/GRID/oracle/astrogrid_universe.py`
- `/Users/anikdang/dev/GRID/scripts/run_astrogrid_learning_loop.py`
- `/Users/anikdang/dev/GRID/scripts/seed_astrogrid_prediction_corpus.py`

Project docs already written:

- `/Users/anikdang/dev/GRID/docs/astrogrid-project.md`
- `/Users/anikdang/dev/GRID/docs/astrogrid-project-brief.md`

## What Is Working

### 1. Prediction / Postmortem / Score Loop

Canonical AstroGrid tables are in `astrogrid.*`:

- `weight_version`
- `prediction_run`
- `prediction_postmortem`
- `prediction_score`
- `backtest_run`
- `backtest_result`
- `review_run`
- `weight_proposal`
- `weight_proposal_decision`

Important implementation facts:

- `prediction_run` and `prediction_postmortem` are append-only
- current state is derived from `prediction_score`, not by mutating the original prediction row
- shared Oracle comparable writes must go through the explicit publish contract, not direct table inserts

### 2. Scoreable Universe

AstroGrid has a canonical scoreable-universe contract:

- `/Users/anikdang/dev/GRID/oracle/astrogrid_universe.py`
- route: `GET /api/v1/astrogrid/universe`

It distinguishes:

- `scoreable_now`
- `degraded`
- `unscored`

Prediction writes enforce this and downgrade degraded targets to `unscored_experimental` instead of polluting the liquid-market scored set.

### 3. Historical Mystical Features

Canonical ephemeris history is wired into seeded snapshots and mystical attribution:

- `ephemeris_hard_aspect_count`
- `ephemeris_soft_aspect_count`
- `ephemeris_lunar_age_days`
- `ephemeris_tithi_index`
- `ephemeris_phase_bucket`
- `ephemeris_nakshatra_pada`

Relevant code:

- `/Users/anikdang/dev/GRID/scripts/backfill_celestial_ephemeris.py`
- `/Users/anikdang/dev/GRID/store/astrogrid.py`
- `/Users/anikdang/dev/GRID/scripts/seed_astrogrid_prediction_corpus.py`

### 4. Regime Integration

Historical regime source now used by AstroGrid:

- `public.regime_history(obs_date, regime, confidence)`

Stable labels:

- `risk_on`
- `risk_off`
- `neutral`
- `transition`

Important implementation detail:

- fresh scoring uses historical regime lookup
- backtests also patch stale older score rows at read time when `prediction_score.regime_context` is missing
- because `prediction_score` is append-only, the backtest read-time fallback is the safe approach

Relevant code:

- `/Users/anikdang/dev/GRID/store/astrogrid.py`

### 5. Group-Aware Review

Review logic now uses backtest slices and emits:

- `best_variant_by_group`
- `group_conditionals`

It is also sample-gated:

- group guidance only appears once group slices clear the minimum sample threshold in `store/astrogrid.py`

Current behavior:

- mystical weights are reduced when crypto/equity slices show degradation
- mystical weights can still hold or rise where macro slices support them

## What Is Weak

### 1. Historical Regime Coverage

`regime_history` is still shallow relative to the full seeded prediction range.

Impact:

- regime slices are structurally correct
- but the current backtest window still tends to collapse into `neutral`
- long-range regime interpretation is not yet truly historical

### 2. Question Intent Is Still Inferred Late

Target groups and intent are still inferred from symbols and backtest grouping more than they should be.

What should exist but does not yet:

- explicit question intent classification at prediction write
- explicit persisted target group at prediction write

### 3. Review Logic Is Better But Still Conservative

Review is now group-aware and sample-gated, but it is still heuristic.

It should become more explicit about:

- minimum sample gates
- minimum alpha gap before preferring one variant over another
- whether a slice is merely non-negative or actually strong enough to move weights

### 4. Frontend Is Not The Priority

The Oracle/Atlas/Chamber frontend is usable, but the real remaining value is backend discipline:

- cleaner metadata persistence
- better review calibration
- better historical contracts

Do not spend the week on visual polish unless it is required to expose backend truth cleanly.

## Highest-Value Next Tasks

### Task 1: Persist Question Intent and Target Group at Write Time

Goal:

- stop inferring everything late from target symbols

Add explicit fields to prediction persistence or payloads for:

- `question_intent`
- `target_group`
- possibly `answer_mode`

Suggested first locations:

- `/Users/anikdang/dev/GRID/api/routers/astrogrid.py`
- `/Users/anikdang/dev/GRID/store/astrogrid.py`
- schema / migration if stored structurally

This is the single best next task.

### Task 2: Tighten Review Thresholds

Current review is sample-gated, but not yet alpha-gap-gated.

Add stronger rules such as:

- do not prefer a variant unless sample size >= threshold
- do not prefer a variant unless alpha gap >= threshold
- do not adjust weights if slice evidence is flat or contradictory

Relevant code:

- `/Users/anikdang/dev/GRID/store/astrogrid.py`

### Task 3: Improve Historical Regime Use

Do not change contracts casually. But if more historical regime coverage lands upstream, AstroGrid should use it immediately.

If `regime_history` gets extended:

- remove or reduce earliest-row fallback where appropriate
- verify `by_regime` meaningfully diversifies away from `neutral`

### Task 4: Extend Review Output for Operators

The backend now knows:

- best variant overall
- best variant by group
- regime labels

Expose that more directly through:

- review/latest route payload clarity
- project docs
- operator summaries

Do not overcomplicate the public UI. This can stay backend/API-first.

## What To Run First

### 1. Focused Tests

Run first:

```bash
cd /Users/anikdang/dev/GRID
.venv/bin/python -m pytest -q \
  tests/test_astrogrid_predictions.py \
  tests/test_astrogrid_routes.py \
  tests/test_astrogrid_seed_corpus.py
```

### 2. If Touching Frontend Runtime

Also run:

```bash
cd /Users/anikdang/dev/GRID
.venv/bin/python -m pytest -q tests/test_astrogrid_web_runtime.py
/tmp/astrogrid_smoke_venv/bin/python scripts/astrogrid_web_smoke.py
```

### 3. Server Learning Loop

When you need live confirmation:

```bash
ssh grid@grid-svr
cd /data/grid_v4/astrogrid_dedup
git fetch origin
git pull --ff-only origin main
python3 scripts/run_astrogrid_learning_loop.py \
  --as-of-date 2026-03-29 \
  --provider-mode deterministic
```

### 4. Inspect Latest Review Directly

```bash
ssh grid@grid-svr
export PGPASSWORD='gridmaster2026'
psql -h localhost -p 5432 -U grid -d griddb -At -F $'\t' -c \
  "SELECT review_key, review_payload::text FROM astrogrid.review_run ORDER BY created_at DESC LIMIT 1;"
```

## Server / DB Facts

Known good DB config:

- host: `localhost`
- port: `5432`
- db: `griddb`
- user: `grid`
- password: `gridmaster2026`

Do not use the wrong local DB (`grid`). Earlier failures came from hitting the wrong database.

## Key Files To Read Before Editing

- `/Users/anikdang/dev/GRID/store/astrogrid.py`
- `/Users/anikdang/dev/GRID/api/routers/astrogrid.py`
- `/Users/anikdang/dev/GRID/oracle/astrogrid_universe.py`
- `/Users/anikdang/dev/GRID/docs/astrogrid-project.md`
- `/Users/anikdang/dev/GRID/docs/SHARED-READ-CONTRACT.md`
- `/Users/anikdang/dev/GRID/.coordination.md`

## Do Not Regress

- no direct AstroGrid writes into shared Oracle tables
- no mutation-based design for append-only AstroGrid tables
- no fake scoreability for degraded assets
- no weak scoring thresholds that count tiny moves as wins
- no review changes that overfit tiny slices

## Current Practical Read on the System

AstroGrid is now doing the right kind of work:

- log predictions
- score them
- compare variants
- review outcomes
- move weights skeptically

The main remaining risk is not missing infrastructure. It is false confidence from thin slices, shallow historical regime labels, and over-inferred intent metadata.

So the next agent should optimize for:

- cleaner persistence
- stricter review logic
- better historical contracts

Do not optimize for vibes.
