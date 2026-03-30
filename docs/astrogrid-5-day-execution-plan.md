# AstroGrid 5-Day Execution Plan

## Purpose

This is the execution plan for the next 5 days of AstroGrid work. It is written for agents shipping at high speed against the current live system. It assumes the current backend loop already exists and is functional:

- prediction logging
- postmortem stub persistence
- scoring
- backtests
- review runs
- weight proposals

The target is not a design refresh. The target is a stronger decision engine with tighter metadata, better calibration, better historical evaluation, and cleaner operator outputs.

## Ground Rules

### 1. Do not waste the week on cosmetics

Frontend work is only allowed when it exposes backend truth more clearly. Do not spend time on visual polish, layout bikeshedding, or themed copy unless it directly unblocks:

- prediction capture
- postmortem clarity
- evidence quality display
- operator review

### 2. Keep AstroGrid writes in `astrogrid.*`

AstroGrid is one face on the same GRID system, but its canonical writes remain in `astrogrid.*`. Comparable Oracle writes must continue to go through the explicit publish contract. Do not insert directly into shared Oracle tables.

### 3. Preserve append-only semantics

These tables are append-only in practice and should stay that way:

- `astrogrid.prediction_run`
- `astrogrid.prediction_postmortem`
- `astrogrid.prediction_score`
- `astrogrid.backtest_run`
- `astrogrid.backtest_result`
- `astrogrid.review_run`
- `astrogrid.weight_proposal`
- `astrogrid.weight_proposal_decision`

If stale values need correction, prefer:

- read-time repair
- append-only decision rows
- new score/review/backtest records

Do not mutate prior rows casually.

### 4. Commit only owned files

The repo often has unrelated dirt. Before every commit, inspect `git status --short` and stage only the files you actually changed.

### 5. Tests are mandatory

Every backend change needs targeted pytest coverage. If the change affects runtime/web behavior, also run AstroGrid smoke.

## Current Baseline

Repo:

- `/Users/anikdang/dev/GRID`

Server:

- `grid@grid-svr`
- worktree: `/data/grid_v4/astrogrid_dedup`

Primary files:

- `/Users/anikdang/dev/GRID/store/astrogrid.py`
- `/Users/anikdang/dev/GRID/api/routers/astrogrid.py`
- `/Users/anikdang/dev/GRID/oracle/astrogrid_universe.py`
- `/Users/anikdang/dev/GRID/scripts/seed_astrogrid_prediction_corpus.py`
- `/Users/anikdang/dev/GRID/scripts/run_astrogrid_learning_loop.py`
- `/Users/anikdang/dev/GRID/tests/test_astrogrid_predictions.py`
- `/Users/anikdang/dev/GRID/tests/test_astrogrid_routes.py`
- `/Users/anikdang/dev/GRID/tests/test_astrogrid_seed_corpus.py`

Current known truths:

- canonical ephemeris history is using underscore names
- regime labels are coming from `public.regime_history`
- review is group-aware and sample-gated
- live question flow now persists `question_intent` and `target_group`

## Day 1: Metadata Discipline and Write-Path Truth

### Goal

Make the prediction record fully self-describing at write time so later review does not infer basics from downstream artifacts.

### Scope

1. Audit the live prediction payload shape end to end.
2. Confirm new writes persist:
   - `question_intent`
   - `target_group`
   - `target_symbols`
   - `scoring_class`
   - evidence quality / target status breakdown
3. Expose those fields cleanly through:
   - `GET /api/v1/astrogrid/predictions/latest`
   - `GET /api/v1/astrogrid/predictions/{prediction_id}`
   - `GET /api/v1/astrogrid/postmortems`
4. Make sure postmortem summaries include explicit:
   - what the question was asking
   - what asset group it hit
   - what would falsify the answer

### Files

- `/Users/anikdang/dev/GRID/api/routers/astrogrid.py`
- `/Users/anikdang/dev/GRID/store/astrogrid.py`
- `/Users/anikdang/dev/GRID/tests/test_astrogrid_predictions.py`
- `/Users/anikdang/dev/GRID/tests/test_astrogrid_routes.py`

### Deliverable

An operator reading the API output should not have to guess:

- what the user asked
- what class of answer was returned
- what market group the answer belongs to
- whether it is scoreable

### Success Criteria

- targeted tests pass
- a fresh seeded prediction clearly shows `question_intent` and `target_group` in API output
- no schema change unless absolutely necessary

### Run

```bash
cd /Users/anikdang/dev/GRID
.venv/bin/python -m pytest -q \
  tests/test_astrogrid_predictions.py \
  tests/test_astrogrid_routes.py
```

## Day 2: Review Calibration and Weight Proposal Discipline

### Goal

Make the review layer harder to fool. It should not recommend weight changes on weak evidence.

### Scope

1. Tighten deterministic review thresholds:
   - minimum sample size by group
   - minimum alpha gap to declare a winning variant
   - minimum consistency across recent windows before changing weights
2. Distinguish:
   - weak edge
   - flat edge
   - contradictory evidence
3. Add explicit hold/no-change outcomes when evidence is not good enough.
4. Ensure proposals reduce mystical weights when they add noise, but do not overreact to one narrow slice.

### Files

- `/Users/anikdang/dev/GRID/store/astrogrid.py`
- `/Users/anikdang/dev/GRID/tests/test_astrogrid_predictions.py`

### Deliverable

Review output that says one of:

- prefer `grid_only`
- prefer `grid_plus_mystical`
- hold current weights

And it should explain why in measured terms.

### Success Criteria

- tests cover threshold behavior
- review payload includes explicit rationale tied to sample and alpha gap
- proposals stop oscillating under thin evidence

### Run

```bash
cd /Users/anikdang/dev/GRID
.venv/bin/python -m pytest -q tests/test_astrogrid_predictions.py
python3 scripts/run_astrogrid_learning_loop.py --as-of-date 2026-03-29 --provider-mode deterministic
```

### Server Check

```bash
ssh grid@grid-svr 'cd /data/grid_v4/astrogrid_dedup && python3 scripts/run_astrogrid_learning_loop.py --as-of-date 2026-03-29 --provider-mode deterministic'
```

## Day 3: Historical Coverage and Slice Quality

### Goal

Make backtests and review slices more representative of reality by improving historical labeling and slice integrity.

### Scope

1. Audit `by_regime` and `by_group` summaries in the latest backtests.
2. Confirm where regime coverage is still collapsing to `neutral`.
3. If upstream `regime_history` coverage is still shallow:
   - keep the current fallback
   - document the actual coverage in the backtest summary
4. Add summary metadata:
   - date range actually covered
   - regime label coverage
   - unknown/fallback rate
5. Ensure backtest summary does not silently imply confidence when regime coverage is thin.

### Files

- `/Users/anikdang/dev/GRID/store/astrogrid.py`
- `/Users/anikdang/dev/GRID/tests/test_astrogrid_predictions.py`

### Deliverable

Backtest summaries that tell the truth about:

- how much data was actually scored
- how much regime context was truly historical vs fallback
- whether slice conclusions are robust

### Success Criteria

- `by_regime` summary includes coverage metadata
- tests verify fallback/coverage reporting
- latest server backtests expose that metadata

### Run

```bash
cd /Users/anikdang/dev/GRID
.venv/bin/python -m pytest -q tests/test_astrogrid_predictions.py
```

## Day 4: Operator Output and API Clarity

### Goal

Make the system legible for operators without bloating the public UI.

### Scope

1. Improve `GET /api/v1/astrogrid/review/latest` and related responses so they clearly show:
   - best overall variant
   - best variant by group
   - current evidence quality
   - reasons mystical weights are rising/falling
2. Add compact operator-facing summary fields:
   - `decision_state`
   - `evidence_strength`
   - `group_conditionals`
   - `regime_conditionals`
3. Keep raw payloads available, but make the top-level read easy to scan.
4. Update docs if the response contract becomes meaningfully clearer.

### Files

- `/Users/anikdang/dev/GRID/api/routers/astrogrid.py`
- `/Users/anikdang/dev/GRID/store/astrogrid.py`
- `/Users/anikdang/dev/GRID/docs/astrogrid-project.md`
- `/Users/anikdang/dev/GRID/tests/test_astrogrid_routes.py`
- `/Users/anikdang/dev/GRID/tests/test_astrogrid_predictions.py`

### Deliverable

An operator should be able to answer, from one response:

- what variant is winning
- where it is winning
- whether the evidence is weak or strong
- whether weights should move or hold

### Success Criteria

- route tests updated
- server response is readable without digging into nested raw payloads

## Day 5: Live Capture, Validation, and Deployment Discipline

### Goal

Close the week by validating the full loop against live writes and documenting exactly what shipped.

### Scope

1. Seed a fresh short historical slice post-patch.
2. Rerun the learning loop.
3. Confirm:
   - fresh predictions carry `question_intent`
   - fresh predictions carry `target_group`
   - review output reflects the tightened thresholds
   - backtest metadata includes coverage truth
4. Sync server worktree to latest `main`.
5. Write a fresh handoff note summarizing:
   - final live commit
   - open risks
   - real next step

### Files

- `/Users/anikdang/dev/GRID/docs/astrogrid-agent-handoff-week.md`
- `/Users/anikdang/dev/GRID/docs/astrogrid-project.md`

### Deliverable

A clean end-of-week state where:

- the learning loop is still green
- the metadata is explicit
- the review layer is harder to fool
- the next agent gets an honest status, not a vague summary

### Success Criteria

- focused tests pass
- server rerun succeeds
- latest prediction rows confirm new metadata
- latest review reflects the new calibration rules

### Run

Local:

```bash
cd /Users/anikdang/dev/GRID
.venv/bin/python -m pytest -q \
  tests/test_astrogrid_predictions.py \
  tests/test_astrogrid_routes.py \
  tests/test_astrogrid_seed_corpus.py
```

Server:

```bash
ssh grid@grid-svr '
  cd /data/grid_v4/astrogrid_dedup &&
  git fetch origin &&
  git pull --ff-only origin main &&
  python3 scripts/seed_astrogrid_prediction_corpus.py --start-date 2026-03-01 --end-date 2026-03-07 --step-days 7 &&
  python3 scripts/run_astrogrid_learning_loop.py --as-of-date 2026-03-29 --provider-mode deterministic
'
```

## Explicit Non-Goals For This 5-Day Window

Do not spend this week on:

- redesigning the AstroGrid frontend
- more Vault / riddle / mystery work
- new symbolic lore systems
- broadening to houses / real estate scoring
- broadening to every tradeable token/equity without a scoreable-universe contract
- replacing append-only persistence with mutable state

Those are later tasks. They are not the fastest path to a stronger engine this week.

## Daily Cadence

Every day should end with:

1. a clean commit or a deliberately parked WIP state
2. focused test output
3. one server rerun if the backend contract changed
4. one line added to the handoff doc if the direction changed materially

## If You Need Upstream Input

Only interrupt the coordinator for one of these:

- canonical historical contract is missing
- API route name/source is ambiguous
- server DB state does not match `main`
- feature is claimed materialized but missing from `feature_registry` / `resolved_series`

Do not escalate for ordinary implementation decisions that can be answered from the repo.

## Final Instruction To The Next Agent

Push backend truth forward, not AstroGrid aesthetics.

The highest-value sequence is:

1. write-path metadata truth
2. review calibration
3. historical coverage truth
4. operator summary clarity
5. fresh server validation

If you do those five well, the system becomes materially more useful without pretending the mystical layer has earned more than the data supports.
