# AstroGrid Project

## 2026-11-05 actor-flow follow-up

Live audit found no BTC/ETH/SOL `signal_sources` rows in seven days. The
`CryptoSignalBridge` had no production caller; Whale Alert had no configured
key and was a `skip_runtime` stub; CryptoQuant's recent pulls failed auth.
The crypto ETF puller reports price/volume proxies, not measured creations or
redemptions, and does not meet the actor-flow contract. These observations do
not authorize paid subscriptions or relabeling proxies as measured flow.

Anik explicitly selected a **separate 30-minute research contract** for a live
forward generation-and-scoring verification. The original 24h/1% contract stays
separate. The existing five-minute producer now:

1. Uses `BinancePuller.fetch_spot_flow_klines` and
   `CryptoSignalBridge.bridge_binance_order_flow` to capture completed UTC 5m
   global spot REST klines into `signal_sources`. First-seen receipts use the
   real DB availability clock; duplicate reads never overwrite them. Balanced
   bars are retained too, so price outcomes do not depend on a future flow gate.
2. Issues model `astrogrid-spot-flow-v1`, contract
   `binance_spot_taker_30m_v1`, only from the latest valid receipt per asset:
   at least 100,000 USDT quote turnover, 100 trades and 10% absolute signed
   taker imbalance, no more than 10 minutes old. The direction is measured
   taker-buy quote volume minus taker-sell quote volume, divided by total quote
   turnover. Actors are anonymous exchange buyer/seller cohorts, not named
   whales, institutions or proven new capital entering the market.
3. Freezes that receipt ID, content hash, exact spot venue/instrument, price,
   availability, issue time, 30m due time and 0.10% directional threshold. The
   outcome is the first recorded completed 5m close at/after due, within 15m.
   Fees are excluded. The hypothesis and thresholds are declared research rules,
   not calibrated odds or evidence of trading edge.
4. Scores only this model on the five-minute cadence. Missing, future,
   mismatched-asset/venue and altered-source outcomes remain unscored. Original
   forecast and postmortem logs remain unchanged; results derive from scores.

The public feed retains bounded pending and completed slices and separate
performance counters for the two contracts. It also exposes the measured
qualification gates, and Guru answers use the latest pending contract per
asset/cohort. Neither research cohort enters the legacy mystical backtests.

Primary source contract:
[Binance Spot klines fields](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market)
and [public market-data-only host](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md).
The global REST receipts are separate from the pre-existing observed trade
stream candles: no overwrite or cross-venue substitution.

Verification: focused gate/scorer/renderer regressions, opt-in production-schema
synthetic E2E and legacy compatibility, then a separately recorded live forward
forecast and its naturally matured real price outcome. Synthetic tests never
count as production performance. Deployment receipts belong in the fleet report.

## Project Metadata

- Project: [[AstroGrid]]
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

[[AstroGrid]] is a market-facing Oracle interface on top of GRID’s shared data and scoring infrastructure. It answers user questions such as:

- What crypto should I buy right now?
- Which stock is the best buy right now: Google, Apple, or Microsoft?
- Is NVDA worth buying?
- When should I buy META?

The public layer is mystical and concise. The backend is strict:

- every prediction is logged
- every prediction gets an immediate [[Postmortem|postmortem]] stub
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
- Immediate [[Postmortem|postmortem]] persistence
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
- Direct writes from [[AstroGrid]] into shared Oracle tables

## Current State

### Working

- [[AstroGrid]] web app is live and booting
- Oracle flow exists
- Prediction logging exists
- Immediate [[Postmortem|postmortem]] persistence exists
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

[[AstroGrid]] writes only to `astrogrid.*`.

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

[[AstroGrid]] reads from GRID through explicit contracts only.

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

- [[AstroGrid]] writes only to `astrogrid.*`
- no direct [[AstroGrid]] writes into shared Oracle tables
- mystical weights start skeptical and must earn weight
- review is proposal-only, not auto-live

### Open Questions

- How far back should `regime_history` be extended?
- What is the minimum sample size required before group-aware weighting changes are actionable?
- Which additional asset groups are worth scoring next?
- When should question intent be first-class in the schema?


## 2026-11-05 — evidence-backed insights

The scheduled `scripts/generate_crypto_predictions.py` producer now extends the
AstroGrid ledger with `astrogrid-evidence-v2` observations from recorded Binance
BTCUSDT/ETHUSDT/SOLUSDT 5m candles. PITStore bounds both market time and local
availability time; incomplete 24h windows and stale sources remain explicit.
Relative performance uses matching windows, not an inferred causal story.

A directional research forecast additionally requires a named actor, liquidity
valve, action, measured flow, unit and direction from a recent source record.
Absent that evidence the producer abstains. No calibrated probability or proven
market edge is claimed. One source event creates at most one forecast.

Forecasts freeze an observed entry, exact asset, 24h due time and declared 1%
directional threshold. Scoring verifies that entry and uses the first completed
same-asset bucket at/after the due time within 15 minutes. Missing outcomes stay
pending, never substitute SPY or another coin, and are retried independently of
the legacy skip budget. The cash-relative signed return excludes fees. Source
forecasts and postmortem logs are immutable; new scores are recorded and the
completed postmortem display is projected from those scores.
Legacy seven-/thirty-day contracts and the separate legacy rescore work retain
their existing rules. New contracts are excluded from incompatible legacy
strategy-comparison backtests and headline performance is separately scoped.

`GET /api/v1/astrogrid/insights` reads the published ledger without generating
forecasts. The public site loads this evidence automatically, flags stale data,
and keeps symbolic celestial readings separate. Guru answers use server-side
published evidence rather than caller-supplied overlays and do not create more
forecast rows. Uncovered requested assets produce an explicit abstention.

Focused verification:

- `python -m pytest tests/test_astrogrid_insights.py tests/test_astrogrid_predictions.py tests/test_generate_crypto_predictions.py tests/test_legacy_crypto_rescore.py`
- `node --test tests/js/astrogrid-insights.test.mjs`
- `ASTROGRID_INSIGHT_TEST_DB_URL=<disposable database URL> python -m pytest tests/test_astrogrid_insights_native.py`

The native test requires an empty production-schema clone whose database name
starts `astrogrid_insight_e2e_`. It inserts only synthetic fixtures and covers
producer → ledger → immature/missing/wrong-asset outcome → score → postmortem →
public API, idempotency, and original-forecast byte preservation. Its synthetic
outcomes demonstrate software correctness, not trading performance.

Install `server_setup/grid-predict-crypto.timer.d/zz-astrogrid-evidence.conf`
as the matching drop-in for the existing timer and reload/restart that timer
after release verification. Its five-minute calendar replaces the hourly
monotonic trigger; the existing service/user/environment remain unchanged.
Without this cadence the 20-minute stale limit would correctly mark evidence
stale for most of each hour even while fresh source candles were available.
Verify the loaded calendar, next deadline, and a subsequent scheduled run.

## Frozen spot-flow economic evaluation (2026-11-06)

The existing evidence panel now distinguishes prospective contract hit counts
from a fee-aware historical edge assessment. See
[results](research/astrogrid-spot-flow-edge-v1-results.md) and the
[pre-data protocol](research/astrogrid-spot-flow-edge-v1-protocol.md).
The rule shows no net predictive edge in 10,854 nonoverlapping August–October
2026 test events. Gross mean is essentially zero; current regular-user fee and
slippage assumptions produce a negative mean. Keep the model as unvalidated
research; never promote weights or claim profitable trading from target hits.
Historical replay and actual live forecasts remain separate and immutable.
