# AstroGrid Project

## Project Metadata

- Project: AstroGrid
- Owner: GRID
- Status: Active
- Stage: Build / Calibration
- Priority: High
- Environment:
  - Local repo: `/Users/anikdang/dev/GRID`
  - Server worktree: `/data/grid_v4/astrogrid_dedup`
  - Public app: [https://grid.stepdad.finance/astrogrid/](https://grid.stepdad.finance/astrogrid/)
- Canonical branch: `main`
- Current repo head at time of writing: `49465d3`

## Summary

AstroGrid is a market-facing Oracle interface on top of GRID’s shared data and scoring infrastructure. It answers user questions such as:

- What crypto should I buy right now?
- Which stock is the best buy right now: Google, Apple, or Microsoft?
- Is NVDA worth buying?
- When should I buy META?

The public layer is mystical and concise. The backend is strict:

- every prediction is logged
- every prediction gets an immediate postmortem stub
- outcomes are scored later
- backtests compare `grid_only`, `grid_plus_mystical`, and `mystical_only`
- review runs propose weight changes without auto-applying them

The core question is not whether the mystical layer is interesting. The core question is whether it improves outcomes over GRID-only baselines, and if so, where.

## Product Goal

Build a question-driven Oracle that:

- gives clear, actionable answers with timeframe and invalidation
- logs the actual thesis behind the answer
- scores the answer with precise win / lose criteria
- runs postmortems after the fact
- backtests what helps and what does not
- adjusts weights through a review-gated loop

## Scope

### In Scope

- Oracle question flow
- Prediction logging in `astrogrid.*`
- Immediate postmortem persistence
- Outcome scoring
- Backtest variants
- Review runs
- Weight proposals
- Shared read contracts from GRID
- Mystical historical feature integration

### Out of Scope

- Broad visual redesign
- Asset classes without a scoring contract
- Auto-live weight mutation
- Direct writes from AstroGrid into shared Oracle tables

## Current State

### Working

- AstroGrid web app is live and booting
- Oracle flow exists
- Prediction logging exists
- Immediate postmortem persistence exists
- Scoreable-universe contract exists
- Outcome scoring exists
- Backtests exist
- Review runs exist
- Weight proposals exist
- Canonical historical ephemeris features are wired into seed snapshots and mystical attribution
- Historical regime labeling is wired into scoring and backtests
- Review layer is now group-aware and sample-gated

### Main Backend Surfaces

- Prediction routes:
  - `POST /api/v1/astrogrid/predictions`
  - `GET /api/v1/astrogrid/predictions/latest`
  - `GET /api/v1/astrogrid/predictions/{prediction_id}`
  - `GET /api/v1/astrogrid/postmortems`
- Scoring / backtest / review routes:
  - `POST /api/v1/astrogrid/predictions/score`
  - `GET /api/v1/astrogrid/predictions/scoreboard`
  - `POST /api/v1/astrogrid/backtest/run`
  - `GET /api/v1/astrogrid/backtest/summary`
  - `GET /api/v1/astrogrid/backtest/results`
  - `POST /api/v1/astrogrid/review/generate`
  - `GET /api/v1/astrogrid/review/latest`
  - `GET /api/v1/astrogrid/weights/current`
  - `GET /api/v1/astrogrid/weights/proposals`
  - `POST /api/v1/astrogrid/weights/proposals/{id}/approve`
  - `POST /api/v1/astrogrid/weights/proposals/{id}/reject`
  - `POST /api/v1/astrogrid/learning-loop/run`
  - `GET /api/v1/astrogrid/universe`

## Architecture

### Canonical AstroGrid Storage

AstroGrid writes only to `astrogrid.*`.

Key tables:

- `astrogrid.weight_version`
- `astrogrid.prediction_run`
- `astrogrid.prediction_postmortem`
- `astrogrid.prediction_score`
- `astrogrid.backtest_run`
- `astrogrid.backtest_result`
- `astrogrid.review_run`
- `astrogrid.weight_proposal`
- `astrogrid.weight_proposal_decision`

### Shared GRID Reads

AstroGrid reads from GRID through explicit contracts only.

Important upstream dependencies:

- `/api/v1/regime/current`
- `/api/v1/intelligence/thesis`
- `/api/v1/flows/money-map`
- `/api/v1/flows/aggregated`
- `/api/v1/flows/sectors`
- `/api/v1/flows/sectors/{sector_name}/detail`
- `/api/v1/signals/snapshot`
- `/api/v1/intelligence/patterns/active`
- `/api/v1/intelligence/cross-reference`

Important historical sources:

- `public.regime_history`
- `feature_registry`
- `resolved_series`

### Canonical Historical Features In Use

Price / market series:

- `btc_full`
- `eth_full`
- `sol_full`
- `spy_full`
- `qqq_full`
- `tlt_full`
- `dxy_index`
- `gld_full`
- `cl_close`
- `aapl_full`
- `msft_full`
- `googl_full`
- `nvda_full`
- `meta_full`

Ephemeris series:

- `ephemeris_hard_aspect_count`
- `ephemeris_soft_aspect_count`
- `ephemeris_lunar_age_days`
- `ephemeris_tithi_index`
- `ephemeris_phase_bucket`
- `ephemeris_nakshatra_pada`

Historical regime source:

- `regime_history(obs_date, regime, confidence)`

Allowed regime labels:

- `risk_on`
- `risk_off`
- `neutral`
- `transition`

## Scoring Rules

Directional calls require meaningful follow-through.

- `swing hit`: `>= 4%`
- `swing partial`: `>= 2%`
- `macro hit`: `>= 8%`
- `macro partial`: `>= 4%`
- `neutral hit`: `<= 1% absolute move`

The system is intentionally strict. Small moves do not count as wins unless the prediction was explicitly “no move”.

## Current Learning Loop Results

Latest server runs showed:

- regime slices now populate with canonical labels instead of collapsing to `unknown`
- current live backtest window is effectively `neutral` because `regime_history` coverage is still recent
- review output is group-aware
- review output is sample-gated

Recent qualitative findings from the live loop:

- mystical signal is not uniformly helpful
- its value is asset-group dependent
- current review logic is cutting mystical weights where they degrade decisions

Current review behavior:

- emits `best_variant_by_group`
- emits `group_conditionals`
- only does so when per-group sample size clears a minimum threshold

## Current Risks

### 1. Shallow Regime History

`regime_history` currently has limited backward coverage, so older windows collapse to the earliest available regime label or a recent neutral regime.

Impact:

- regime slices are now structurally correct
- but long lookbacks still do not represent real historical regime variation well

### 2. Review Overreaction Risk

Group-aware review now exists, but if the backtest window is too small, even sample-gated guidance can still be noisy.

Impact:

- weights may still move on weak evidence if not reviewed carefully

### 3. Asset Coverage Asymmetry

The scoreable-universe contract is real, but not all assets have equal history quality.

Impact:

- some questions are scoreable now
- others are degraded or experimental

### 4. Mystical Signal May Be Mostly Noise

This is an explicit design assumption, not a surprise.

Impact:

- mystical weights should remain skeptical by default
- only measured uplift should preserve them

## Current Priorities

### Priority 1: Strengthen Review Discipline

- widen review windows where needed
- add stronger minimum-sample constraints
- ensure group-specific guidance only appears when statistically defensible

### Priority 2: Persist More Intent Metadata

- attach target group explicitly at prediction creation time
- attach question intent classification
- avoid inferring everything later from target symbols

### Priority 3: Improve Historical Regime Coverage

- extend `regime_history` backward if possible
- make `by_regime` analytically meaningful over larger windows

### Priority 4: Expand and Classify Questions Cleanly

- continue answering broad user questions
- keep backend scoring-class labels honest:
  - `liquid_market`
  - `illiquid_real_asset`
  - `macro_narrative`
  - `unscored_experimental`

## Roadmap

### Phase A: Prediction Quality

- improve target inference
- persist target group and question intent explicitly
- keep the Oracle answer compact but structured:
  - call
  - timing
  - setup
  - invalidation
  - note

### Phase B: Scoring and Backtests

- maintain canonical feature-backed price lookup
- keep strict win/loss thresholds
- expand scoreable-universe coverage carefully
- continue validating `grid_only` vs `grid_plus_mystical` vs `mystical_only`

### Phase C: Review and Weight Governance

- keep weights append-only and versioned
- keep approval separate from proposal
- strengthen group-aware and regime-aware review logic
- do not allow automatic live mutation

### Phase D: Research Expansion

- expand mystical historical features where evidence exists
- broaden asset classes only with proper scoring contracts
- add new question classes without corrupting the evaluation loop

## Files That Matter

Core backend:

- `/Users/anikdang/dev/GRID/store/astrogrid.py`
- `/Users/anikdang/dev/GRID/api/routers/astrogrid.py`
- `/Users/anikdang/dev/GRID/oracle/astrogrid_universe.py`
- `/Users/anikdang/dev/GRID/schema.sql`

Key scripts:

- `/Users/anikdang/dev/GRID/scripts/seed_astrogrid_prediction_corpus.py`
- `/Users/anikdang/dev/GRID/scripts/run_astrogrid_learning_loop.py`
- `/Users/anikdang/dev/GRID/scripts/backfill_celestial_ephemeris.py`
- `/Users/anikdang/dev/GRID/scripts/astrogrid_web_smoke.py`

Reference docs:

- `/Users/anikdang/dev/GRID/docs/astrogrid-build.md`
- `/Users/anikdang/dev/GRID/docs/astrogrid-oracle-stance.md`
- `/Users/anikdang/dev/GRID/docs/astrogrid-schema.md`
- `/Users/anikdang/dev/GRID/docs/SHARED-READ-CONTRACT.md`
- `/Users/anikdang/dev/GRID/.coordination.md`

## Notion-Ready Database Fields

Recommended project properties:

- Name: `AstroGrid`
- Status: `Active`
- Stage: `Build / Calibration`
- Priority: `High`
- Owner: `GRID`
- Repo: `GRID`
- Branch: `main`
- Public URL: `https://grid.stepdad.finance/astrogrid/`
- Local Path: `/Users/anikdang/dev/GRID`
- Server Path: `/data/grid_v4/astrogrid_dedup`
- Current Focus: `Prediction quality, backtests, review governance`
- Current Risk: `Historical regime coverage is shallow`
- Next Milestone: `Persist question intent + target group, improve review discipline`

## Suggested Notion Sections

### Overview

Use the Summary section above.

### Current Metrics

Track:

- predictions logged
- predictions scored
- backtest variant metrics
- latest review confidence
- latest proposed mystical weights

### Decisions

Track decisions such as:

- AstroGrid writes only to `astrogrid.*`
- no direct AstroGrid writes into shared Oracle tables
- mystical weights start skeptical and must earn weight
- review is proposal-only, not auto-live

### Open Questions

- How far back should `regime_history` be extended?
- What is the minimum sample size required before group-aware weighting changes are actionable?
- Which additional asset groups are worth scoring next?
- When should question intent be first-class in the schema?
