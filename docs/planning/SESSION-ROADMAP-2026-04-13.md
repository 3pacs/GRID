---
title: Session Roadmap — 2026-04-13
branch: claude/review-cryexc-app-T2icl
scope: cryexc review → signal stack planning → inference architecture discoveries
status: reference (read-first for future sessions working on signal stack or oracle)
supersedes: none
consumed_by: future Claude sessions working on oracle, intelligence layer, or signal additions
---

# Session Roadmap — 2026-04-13

**Purpose of this document:** Capture everything discovered in one working session so future sessions do not redo the discovery work. Started as a code review of jose-donato's `cryexc` app; ended as a full architectural audit that revealed **GRID has evidence abundance and inference poverty**, plus several hours of wasted potential because `CLAUDE.md` dramatically under-represents the real module surface.

**If you are a future Claude session:** read [Section 1: Session-Start Pre-Read](#1-session-start-pre-read-read-this-first) before proposing any new intelligence module, puller, oracle change, or options feature. Most of what sounds like a good idea has already been built but isn't wired in.

---

## Table of Contents

1. [Session-Start Pre-Read (read this first)](#1-session-start-pre-read-read-this-first)
2. [Cryexc + jose-donato repo lineage](#2-cryexc--jose-donato-repo-lineage)
3. [User's trade horizons + why cadence is fine](#3-users-trade-horizons--why-cadence-is-fine)
4. [Oracle architecture discoveries](#4-oracle-architecture-discoveries)
5. [Intelligence layer reality check (14 vs 104+)](#5-intelligence-layer-reality-check-14-vs-104)
6. [Options / vol surface / dealer gamma — what exists but is unused](#6-options--vol-surface--dealer-gamma--what-exists-but-is-unused)
7. [Confirmed genuine gaps (not duplicates)](#7-confirmed-genuine-gaps-not-duplicates)
8. [Tier A shortlist — 40 signals](#8-tier-a-shortlist--40-signals)
9. [Session artifacts + deferred items](#9-session-artifacts--deferred-items)
10. [Meta-principles of certainty (outside-the-box)](#10-meta-principles-of-certainty-outside-the-box)

---

## 1. Session-Start Pre-Read (read this first)

These are the ten things a new session must know **before** proposing any changes to the oracle, intelligence layer, options stack, or signal surface. Every one of them cost time in this session because they weren't captured in `CLAUDE.md` or the auto-loaded index.

1. **`CLAUDE.md` says "14 intelligence modules." Reality: ~104+.** The 14 listed are the *documented core* (CLAUDE.md lines 87-100). The real `intelligence/` directory contains roughly 90 more undocumented modules including `earnings_transcript_analyzer`, `hypothesis_engine`, `prediction_calibration`, `signal_registry`, `signal_backlinker`, `sentiment_scorer`, `news_intel`, `market_diary`, `news_momentum`, `deal_detector`, and specialized network mappers (banking, energy, pharma, defense, tech_monopoly, real_estate, commodities_agriculture, defi_protocols). **The canonical catalog is `docs/MODULE_CATALOG.md` — read it, not CLAUDE.md.**

2. **`flow_thesis.py` and `flow_aggregator.py` are in `analysis/`, not `intelligence/`.** CLAUDE.md lists them under the intelligence layer. They are not there. Check `analysis/` when referencing them.

3. **The oracle hardcodes a monthly 3rd-Friday expiry.** `oracle/engine.py:893` calls `self._next_monthly_expiry()` for every prediction. The `oracle_predictions` table (`oracle/engine.py:224-250`) has **no `horizon`, `as_of_date`, `resolution_date`, `catalyst_type`, or `catalyst_proximity` fields**. If a task involves multi-horizon, LEAPS, or catalyst-aware logic, this is a schema migration first.

4. **`timeseries_forecasts` already has a `horizon` field** (`oracle/engine.py:295`). The concept exists in the data pipeline but is not plumbed through to `oracle_predictions`. Don't reinvent — extend.

5. **`analysis/vol_surface.py` already implements SVI parameterization, per-expiry skew, and butterfly-arbitrage checks.** It is **not wired** into `discovery/options_scanner.py` or `trading/options_recommender.py`. Any "build a vol surface" task is actually a "wire the existing vol surface" task.

6. **`intelligence/earnings_transcript_analyzer.py` already splits transcripts into prepared remarks vs Q&A and scores tone, guidance, Q&A divergence, and risk phrases.** Any "build an earnings call analyzer" task is actually "extend the existing analyzer with quarter-over-quarter deltas or semantic embeddings."

7. **`intelligence/prediction_calibration.py` already exists.** Calibration tracking is not missing at the concept level — it is missing at the **persistence and per-horizon** level. `oracle/calibration.py` computes Brier/ECE on-demand but does not save results; there is no calibration drift tracking.

8. **Vanna and charm are computed in `physics/dealer_gamma.py:248-250` but are never used in any prediction or score.** They are "measured but not actionable." Wiring them into the options scanner or recommender is a cheap quick win.

9. **`physics/dealer_gamma.py` assumes dealers are net short every option** (`physics/dealer_gamma.py:19-30`). Per-ticker GEX is computed but the dealer positioning inference is crude. Any "improve dealer gamma" task is really "replace the net-short assumption with a flow-based inference."

10. **`features/importance.py` already tracks permutation importance, regime correlation, and rolling stability** — but **not per-horizon or per-regime**. Horizon-conditional feature importance is an extension, not a new build.

### Also critical (but less often hit)

- **`store/pit.py`** provides `PITStore.get_pit(feature_ids, as_of_date, vintage_policy)` at lines 43-132. `assert_no_lookahead()` is at line 129. This is the walk-forward backtest primitive — **do not reinvent PIT querying**.
- **`hermes_operator.py:972-986`** is where `oracle.run_cycle()` is invoked every 6h. That is the oracle entry point.
- **`scheduler.py` is authoritative; `scheduler_v2.py` is deprecated** (already noted in CLAUDE.md but worth repeating).
- **`intelligence/hypothesis_engine.py` exists** for LLM-driven hypothesis generation. If a task involves "generate trade ideas from data," it already has infrastructure.
- **`intelligence/signal_registry.py` + `signal_backlinker.py` + `signal_extractor.py` exist** for signal inventory. If a task involves "catalog signals," use these.
- **`intelligence/causation.py` is a facade** over `causation_core`, `causation_scoring`, and `causation_graph`. Single-hop primary implementation; graph walks are shallow.
- **`ingestion/altdata/earnings_calendar.py` uses yfinance** and lacks BMO/AMC time-of-day, whisper numbers, and any event importance score.
- **Calibration is not persisted.** `oracle/calibration.py` recomputes from scratch each call — no historical tracking.

### The "before you build X" checklist

Before proposing **any** new module, puller, or oracle feature:

1. Search `docs/MODULE_CATALOG.md` for keywords.
2. Run `ls intelligence/ analysis/ physics/ features/ discovery/ trading/ | grep -i <keyword>`.
3. Grep for the concept across `intelligence/`, `analysis/`, `physics/`, `features/`, `discovery/`, `trading/`, `oracle/`.
4. Read the top 50 lines of any match to confirm it's relevant.
5. If it exists, the task is almost always "extend and wire," not "build new."

---

## 2. Cryexc + jose-donato repo lineage

**User prompt:** "https://cryexc.josedonato.com/app check this out."

**What cryexc actually is:** a live crypto **market microstructure** scanner built by **Jose Donato** (founding engineer at OpenBB, 65.8k★ repo). It is **not** an options scanner — that was a misconception early in the session. Cryexc is the consolidation of four earlier repos into one shippable Python/FastAPI service.

### The four source repos (evolution path)

| Repo | Stars | Language | Role |
|---|---|---|---|
| [crypto-orderbook](https://github.com/jose-donato/crypto-orderbook) | 120 | Go | L2 order book visualization, goroutine-per-venue pattern |
| [crypto-futures-arbitrage-scanner](https://github.com/jose-donato/crypto-futures-arbitrage-scanner) | 124 | Go | Cross-venue mid-price arb (7 futures + 2 spot venues) |
| [binancef_l3_estimate_go](https://github.com/jose-donato/binancef_l3_estimate_go) | 27 | Go | L3 order inference from L2 |
| [flowsurface](https://github.com/jose-donato/flowsurface) | 12 | Rust (Iced) | Native desktop pro terminal — the "ideal form" |
| **[cryexc-backend](https://github.com/jose-donato/cryexc-backend)** | **58** | **Python (FastAPI + DuckDB)** | **The ship-it form — consolidated web service** |

### Architecture of cryexc-backend

- **FastAPI + WebSocket** server
- **DuckDB in-memory** for hot trades/aggregates
- **Binance Futures** as the primary venue (Hyperliquid noted as US-friendly alt)
- **Tree of Alpha WebSocket** for crypto news
- Six microstructure primitives streamed to frontend:
  1. **Footprint candles** — buy/sell volume bucketed by (price × time)
  2. **DOM** — depth-of-market combined with historical trade volume
  3. **Orderbook heatmap** — time-series "painted book" liquidity distribution
  4. **CVD** (Cumulative Volume Delta)
  5. **Book stats** — spread, mid, depth at 0.5% / 2% / 10%
  6. **Derivs stats** — funding rate, open interest, mark/index, basis

### Architecture of flowsurface (Rust, for reference only)

Workspace structure (clean separation GRID doesn't currently have):
```
flowsurface/
├── exchange/   # WS clients, order book models, trade streams
├── data/       # Aggregation, footprint/heatmap computation
└── src/        # Iced UI, canvas rendering
```

Notable crates: `iced 0.14-dev`, `rust_decimal 1.36` (fixed-precision prices), `ordered-float 5.0`, `palette 0.7` (OKLab perceptually-uniform colormaps for heatmap rendering), `rustc-hash`, `enum-map`, `tokio`, `wgpu`.

### Key architectural lessons from the jose-donato arc

1. **Second-system rewrite:** He built the ideal form in Rust, hit distribution friction for a native app, then re-did it as browser-accessible Python service covering ~70% of the functionality with ~10% of the code. **Implication for GRID: don't reach for Rust. Python + DuckDB + FastAPI is the right layer.**
2. **Goroutine-per-venue (Go) → one-task-per-venue (Python asyncio).** Clean separation of WS consumers from aggregation is correct at any language.
3. **`rust_decimal` for prices:** GRID should audit `trading/options_recommender.py` and `store/pit.py` for `float` drift; `decimal.Decimal` is the right type for money.
4. **Perceptually uniform colormaps (OKLab/LCH) for heatmaps:** Python equivalent is `colour-science`. Important for any future footprint/heatmap view.

### Why cryexc is NOT worth porting right now

Analyzed in depth — see [Section 3](#3-users-trade-horizons--why-cadence-is-fine) below. Short version: orderflow primitives (footprint, CVD, heatmap) are tick-level signals that decay in seconds-to-minutes. GRID's 6h oracle cadence (and the user's swing/quarterly/earnings/LEAPS horizons) are too slow to harvest that alpha. Expected Brier lift at current cadence: **0.1-0.5%**, below the 1% materiality bar. Parked in `SEED-001` with explicit trigger conditions.

---

## 3. User's trade horizons + why cadence is fine

**User quote:** "my goal is swing/quarterly/earnings/LEAPS until i have a real time data flow."

This framing is **critical for every future session**. The user does NOT want tick-level or intraday optimization right now. Their horizons are:

| Horizon | Duration | GRID's relevance |
|---|---|---|
| **Swing** | Days to weeks | Directly matched to 6h oracle cadence |
| **Quarterly** | 3-month macro rebalances | 6h is faster than needed |
| **Earnings** | Event-driven, 1-4 weeks around prints | Catalyst-aware scoring needed (see Section 4) |
| **LEAPS** | 1-2 years | Dominated by vega + term structure, not path |

**Conclusion: 6h cadence is close to optimal at these horizons.** Cadence is **not** the biggest drawback. The actual ranking of GRID's drawbacks (for these horizons):

| Rank | Drawback | Why it bites |
|---|---|---|
| **1** | **No horizon-conditional modeling** | Oracle produces one probability, not per-horizon. A 5d feature and a 365d feature fight each other in a single model. Biggest single lever available. |
| **2** | **No catalyst-aware scoring** | Earnings / FOMC / FDA / CPI are deterministic future events. Predictions within N days should be reweighted. GRID has `earnings_calendar.py` but doesn't condition. |
| **3** | **LEAPS-specific vol surface gap** | `analysis/vol_surface.py` exists (SVI) but is not wired into scanner/recommender. LEAPS P&L is ~60% vega. |
| **4** | **Missing positioning + flow stack** | 13F deltas, primary dealer positions, prime broker notes, sovereign wealth rebalances, MSCI/Russell — swing-horizon alpha lives here. |
| **5** | **China / Europe macro blind spots** | At LEAPS horizon, regional growth differentials compound into major theses. GRID is US-centric. |
| 6 | Earnings-specific feature stack | Pre-announcement drift, revision clusters, whisper, BMO/AMC, sector cascade. |
| 7 | Cadence | 6h is fine. Not a priority. |
| 8 | Tick-level orderflow | Decays before it's useful at these horizons. Parked in SEED-001. |

### Implication for future sessions

Do not propose **anything** that requires sub-hour data until the user explicitly enables an intraday tactical layer. All work should target swing-to-LEAPS horizons. `SEED-001` captures the deferred tick-level work with its trigger conditions.

---

## 4. Oracle architecture discoveries

### The 5-model architecture (+ TimesFM 6th)

`oracle/engine.py:162-206` defines `DEFAULT_MODELS` as a hardcoded dataclass. The five are:

| Model | Signal families | Initial weight |
|---|---|---|
| `flow_momentum` | Capital flow + price momentum | 1.0 |
| `regime_contrarian` | Regime state + mean reversion | 1.0 |
| `options_flow` | Options positioning + dark pool signals | 1.0 |
| `cross_asset` | Rates + FX + commodities + credit confirmation | 1.0 |
| `news_energy` | News sentiment momentum + coherence | 1.0 |

A 6th model, `timeseries_enhanced` (TimesFM foundation model), is routed separately at `engine.py:709-762`.

Each model is an `OracleModel` dataclass (`engine.py:135-158`) with: `name`, `version`, `signal_families[]`, `weight`.

### Weight evolution

Weights are **dynamic and learned**: `engine.py:1079` defines `evolve_weights()` with `MIN_WEIGHT=0.1`, `MAX_WEIGHT=3.0`, `LEARNING_RATE=0.1`. Hits / misses / partials feed back into weights each cycle.

Combined prediction = weighted vote on direction + z-score strength (`engine.py:826-875`):
```python
bull_score = sum(s.z_score * s.weight for ...)
confidence = bull_score normalized to 0-1
```

### The critical limitation: one prediction, one horizon, hardcoded

**Every oracle prediction has the same expiry:** `engine.py:893` calls `self._next_monthly_expiry()` which returns the next 3rd Friday. There is no concept of "predict this ticker at 5d, 30d, 90d, and 365d separately." All five horizons are collapsed into one monthly output.

This is **the single highest-leverage change available**. Splitting the oracle into horizon-conditional predictions is likely worth **2-4% Brier improvement oracle-wide** because features that are signal at 5d are noise at 365d, and vice versa. Currently they fight each other in a single model.

### `oracle_predictions` table schema (`engine.py:224-250`)

```sql
id, created_at, ticker, prediction_type, direction, target_price, entry_price,
expiry, confidence, expected_move_pct, signal_strength, coherence,
model_name, model_version,
signals (JSONB), anti_signals (JSONB), flow_context (JSONB), model_weights (JSONB),
verdict, actual_price, actual_move_pct, pnl_pct, scored_at, score_notes
```

**Missing fields (schema migration required for horizon-conditional work):**
- `horizon` (days)
- `as_of_date` (decision timestamp for walk-forward PIT)
- `resolution_date` (when the prediction will be scored)
- `catalyst_type` (earnings / FOMC / FDA / macro_release / none)
- `catalyst_proximity` (days to nearest relevant catalyst)

### Inputs consumed by the oracle

- **Features** from `resolved_series` table with 30-day rolling z-scores (`engine.py:340-348`)
- **Options daily signals** (PCR, IV, max pain) via `options_daily_signals`
- **TimesFM forecasts** from `timeseries_forecasts` (this table **already has** a `horizon` field at `engine.py:295` — the concept exists in the pipeline but is not plumbed into oracle predictions)
- **Actor trust signals** (`engine.py:773-813`)

### Calibration (`oracle/calibration.py`)

- Single global + optional per-model or per-ticker filters (`calibration.py:66-195`)
- Returns a `CalibrationReport` with `buckets[]`, `brier_score`, `calibration_error`, `sharpness`, `label`, `overall_accuracy`
- **Calibration is NOT persisted.** It is computed on-demand from the `oracle_predictions` table each call. This means there is no calibration drift tracking over time.
- No walk-forward calibration. No per-horizon calibration curves.

### Report (`oracle/report.py`)

Produces an email digest with:
- Scorecard: H/M/P counts + adjusted hit rate (`report.py:28-56`)
- Model leaderboard: weight, hit rate, P/L, Sharpe (`report.py:58-79`)
- Weight evolution (`report.py:81-100`)
- Top 8 predictions with ticker, direction, confidence, entry/target, expiry, signals, anti-signals, flow context (`report.py:102-185`)

Stateless — no per-horizon breakdown possible without schema changes.

### Oracle entry point

`hermes_operator.py:972-986` — `oracle.run_cycle()` called every 6h inside the Hermes cycle. Forecaster adapter (`forecaster_adapter.run_timesfm_forecast_cycle()`) runs in parallel in the same cycle.

CLI: `oracle/run_cycle.py` (standalone with `--tickers` and `--no-email` flags).

### PIT primitives for backtesting (already exist)

`store/pit.py:43-132`:
```python
PITStore.get_pit(feature_ids, as_of_date, vintage_policy="LATEST_AS_OF" | "FIRST_RELEASE")
```

Returns a DataFrame with `[feature_id, obs_date, value, release_date, vintage_date]`, enforcing `release_date <= as_of_date` and `obs_date <= as_of_date`.

`assert_no_lookahead(df, as_of_date)` at `pit.py:129` is the post-query safety net.

**This is what a walk-forward horizon-conditional backtest would use:** set `as_of_date = target_resolution_date - horizon_days`, re-run the oracle at that point, score on actual. **The framework exists — the oracle just doesn't use it yet.**

### Implementation foundation (good news)

The codebase is well-architected for the horizon + catalyst additions:
- `OraclePrediction` dataclass is extensible.
- `oracle_predictions` table has JSONB columns (`flow_context`, `model_weights`) that can absorb structured additions without migration.
- PIT engine already enforces lookahead safety.
- TimesFM adapter bridges probabilistic forecasts and **can be per-horizon today**.
- Model weights are mutable via `evolve_weights()` — can be made horizon-aware.

**Critical blockers: none architectural.** Just schema migration + new signal flows.

---

## 5. Intelligence layer reality check (14 vs 104+)

### The documentation gap

`CLAUDE.md` lines 87-100 lists **14 intelligence modules** totaling 14,402 lines. This is the section every new Claude session reads first and assumes is exhaustive. **It is not.** The real `intelligence/` directory has ~104 Python files, and `docs/MODULE_CATALOG.md` (generated 2026-03-30) already reports **46 modules in the intelligence layer and 405 modules total across the codebase**.

**The canonical catalog is `docs/MODULE_CATALOG.md`. Always read it before CLAUDE.md.** CLAUDE.md's intelligence list is a curated subset, not a complete inventory.

### The 14 "core documented" modules (per CLAUDE.md)

These are real and still accurate:

| # | Module | Lines | Purpose |
|---|---|---|---|
| 1 | `trust_scorer.py` | 1,100 | Bayesian trust scoring with 90-day recency decay |
| 2 | `lever_pullers.py` | 1,376 | Market-moving actor tracker (5 categories) |
| 3 | `actor_network.py` | 7,002 | 495-actor wealth flow graph |
| 4 | `cross_reference.py` | 1,435 | Lie detector — govt stats vs physical reality |
| 5 | `source_audit.py` | 939 | Source accuracy comparison + priority updates |
| 6 | `postmortem.py` | 1,344 | Automated failure analysis |
| 7 | `sleuth.py` | 1,228 | Investigative leads + rabbit-hole follower |
| 8 | `thesis_tracker.py` | 961 | Thesis versioning + post-mortem scoring |
| 9 | `dollar_flows.py` | 1,081 | USD normalization + capital flow quantification |
| 10 | `event_sequence.py` | 998 | Chronological timeline reconstruction |
| 11 | `forensics.py` | 927 | Price move reconstruction |
| 12 | `causation.py` | 2,387 | Root cause tracing (facade over causation_core/scoring/graph) |
| 13 | `flow_thesis.py` | 804 | **Actually in `analysis/`, not `intelligence/`** |
| 14 | `flow_aggregator.py` | 772 | **Actually in `analysis/`, not `intelligence/`** |

**Location bug:** CLAUDE.md says `flow_thesis.py` and `flow_aggregator.py` are in `intelligence/`. They are in `analysis/`. Update CLAUDE.md.

### The ~90 additional intelligence modules (not in CLAUDE.md)

Discovered this session via `ls intelligence/*.py` and exploration. Grouped by role:

**NLP + sentiment:**
- `earnings_transcript_analyzer.py` — tone scoring, Q&A vs prepared remarks split, guidance extraction, risk phrases (**extensible, don't rebuild**)
- `sentiment_scorer.py`
- `news_intel.py`

**LLM-backed reasoning:**
- `hypothesis_engine.py` — LLM-driven hypothesis generation with kill criteria (**extensible, don't rebuild**)
- `obsidian_agent.py`
- `deep_dive.py`
- `rag.py`

**Signal inventory + management:**
- `signal_registry.py` — canonical signal inventory (**reuse**)
- `signal_backlinker.py` — signal-to-source backlinking
- `signal_extractor.py` — signal extraction pipeline

**Calibration + prediction:**
- `prediction_calibration.py` — oracle Brier / reliability tracking (**extensible for per-horizon calibration**)

**Monitoring / tracking:**
- `market_diary.py`
- `news_momentum.py`
- `trend_tracker.py`
- `deal_detector.py`

**Specialized network mappers (NEW TO ME — these exist!):**
- `banking_network.py`
- `energy_network.py`
- `tech_monopoly_network.py`
- `pharma_network.py`
- `defense_contractors.py`
- `commodities_agriculture_network.py`
- `real_estate_network.py`
- `defi_protocols.py`

**Domain-specific intel:**
- `insider_intel.py`
- `earnings_intel.py`
- `legislative_intel.py`
- `gov_intel.py`

**Causation submodules (facade-delegated):**
- `causation_core/`, `causation_scoring/`, `causation_graph/`

**Micro-signal specialists:**
- `cds_tracker.py`
- `whale_fingerprinter.py`

### Implication

Every session that proposes to "build a sector network mapper" or "build an LLM-driven hypothesis engine" or "build a sentiment tracker" is almost certainly **duplicating** existing work. Check `docs/MODULE_CATALOG.md` and `ls intelligence/` first.

**Rule:** If a capability sounds obvious, assume it exists. Find it, read it, decide whether to extend it.

---

## 6. Options / vol surface / dealer gamma — what exists but is unused

This is the biggest cluster of "already built, not wired" infrastructure. Quick wins live here.

### `analysis/vol_surface.py` — EXISTS, NOT WIRED

SVI (Stochastic Volatility Inspired) parameterization, skew by expiry, butterfly arbitrage checks. **Not imported by** `discovery/options_scanner.py` or `trading/options_recommender.py`.

**Quick win:** wire it in. LEAPS-relevant features (1Y ATM IV, long-dated skew, term structure slope, div risk premium, ρ) can be computed from it immediately without building anything new.

### `discovery/options_scanner.py` — 7 signals, no LEAPS awareness

The 7 mispricing signals (lines 3-10):

1. Extreme IV skew dislocations (vol surface kinks)
2. Put/call ratio extremes
3. Max pain divergence (gamma squeeze potential)
4. IV term structure inversions (near-term event pricing)
5. OI concentration spikes
6. IV percentile rank (cheap options vs history)
7. Gamma exposure imbalance (dealer hedging flow)

**Output:** `MispricingOpportunity` dataclass (lines 30-48) — ticker, scan_date, score (0-10), estimated_payoff_multiple, direction, thesis, signals, strikes, expiry, spot_price, iv_atm, confidence.

**Gaps:**
- **No LEAPS differentiation.** Single `near_expiry` field. No DTE bucketing.
- **Point-estimate IV only** (`iv_atm`, `iv_25d_put`, `iv_25d_call`). No surface fitting even though `vol_surface.py` is right there.
- **Basic term structure slope as a scalar** (line 194). No multi-tenor bucketing.

### `trading/options_recommender.py` + `options_tracker.py`

Recommendation output includes strike, expiry, entry (bid/ask mid), target, stop, time stop, max risk, Kelly fraction, suggested contracts, confidence, thesis with lever/catalyst/conditions, dealer context (GEX/vanna/charm), invalidation threshold.

**Kelly sizing is partially horizon-aware:** `_EARNINGS_MIN_DTE = 3` (line 243) allows short earnings plays vs default `_MIN_DTE`. But Kelly is not dynamically adjusted for theta as expiry nears.

**Tracker feedback loop** (`options_tracker.py`):
- Scores expired recs as WIN / LOSS / EXPIRED (line 142)
- Computes realized vs expected edge **per scanner signal** (lines 183-274): win_rate, avg_return, contribution per signal
- Updates scanner weights dynamically (lines 428-508) with `LEARNING_RATE = 0.15`
- LLM improvement analysis (lines 693-764)

**Missing:** tracker does NOT track edge by **recommendation horizon** or **catalyst type**. That's a cheap extension.

### `physics/dealer_gamma.py` — per-ticker but crude positioning

- Computes **per-ticker** GEX, vanna, charm, and gamma walls (lines 375-383 read `options_snapshots`)
- **Assumes dealers are net short every option** (lines 19-30) — computes:
  - Dealer short calls → negative gamma
  - Dealer short puts → positive gamma
- No cross-name dealer net positioning inference
- No actual market-maker filing data (13F, Form 4, Form 606)
- **Vanna computed** at lines 248-249 (`bs_vanna × OI`, aggregated at line 179)
- **Charm computed** at line 250 (time decay delta sensitivity, line 180)
- **Both reported** at lines 205-206 in the output — **but never used** in predictions or scoring. **Free alpha on the floor.**

**Two quick wins:**
1. Wire vanna/charm into `options_scanner.py` as additional signals (or their own scoring dimension).
2. Replace the "net short everything" assumption with a flow-based dealer positioning inference (requires tracking customer flow direction).

### `ingestion/altdata/earnings_calendar.py` — flat schema, yfinance source

Source: `yfinance.Ticker.earnings_dates` and `.calendar` (lines 170, 231).

**Table schema** (lines 89-105):
```sql
ticker, earnings_date, fiscal_quarter,
eps_estimate, eps_actual, eps_surprise_pct,
revenue_estimate, revenue_actual, revenue_surprise_pct,
classification (beat/miss/inline), reported (BOOLEAN),
raw_payload (JSONB)
```

**Missing fields:**
- `earnings_time` (BMO / AMC / AH flag)
- Whisper consensus
- Historical surprise percentile
- **Event importance score** — all earnings treated equally

### Missing catalyst calendars (none exist today)

- FOMC meeting calendar — **MISSING**
- CPI / NFP / PCE / ISM macro release calendar — **MISSING**
- FDA AdCom / PDUFA calendar — **MISSING**
- Options expiry aggregation — **implicit in `options_daily_signals`**, no dedicated table
- Treasury auction calendar — **MISSING**
- Corporate actions (splits, dividends, buyback announcements) — **MISSING**

`intelligence/trace_evolver.py:327` has a comment: "Add event-risk filter (FOMC, earnings, geopolitical)" — **aspirational but not implemented**.

### Implication: catalyst_aggregator is a real gap

A unified `intelligence/catalyst_aggregator.py` that pulls earnings + FOMC + CPI + NFP + ISM + PMI + jobless claims + VIX term structure rollovers + SOFR fixings + Fed balance sheet actions + Treasury auctions + corporate actions into one calendar is **genuinely missing** and would unlock catalyst-aware scoring in the oracle.

---

## 7. Confirmed genuine gaps (not duplicates)

After verifying against `docs/MODULE_CATALOG.md` and live `intelligence/` / `analysis/` / `physics/` / `features/` listings, these capabilities are **genuinely missing** — they do not duplicate any existing module. Ordered by expected leverage.

### Inference architecture gaps (highest leverage — pure code, no new data)

1. **Horizon-conditional oracle** — `oracle_predictions` has no `horizon` field. All predictions collapse to one monthly expiry. Per-horizon models + per-horizon calibration curves. **Expected lift: 2-4% Brier oracle-wide.**

2. **Catalyst-aware scoring** — no unified catalyst calendar; no proximity-weighted reweighting of predictions near known future events. **Expected lift: ~1.5% on event-adjacent trades (~30% of volume).**

3. **Shapley-value attribution per prediction** — grep found no SHAP references anywhere. Every prediction should be decomposable as "X% from macro, Y% from positioning, Z% from flows." **Enables sizing down fragile predictions.**

4. **Ensemble disagreement as a meta-feature** — no inter-model correlation or disagreement tracking across the 5 oracle models. When models disagree that IS information (route to vol trade or size down).

5. **Market-implied probability comparator** — no module compares GRID's prediction probability to market-implied probabilities (options skew, yield curve, Polymarket, Kalshi). **GRID's edge = |GRID_p − market_p|.** Any signal that doesn't move GRID away from consensus is not alpha.

6. **Per-horizon feature importance** — `features/importance.py` has permutation importance + regime correlation + rolling stability **but not per-horizon or per-regime**. A feature high-signal at 5d may be noise at 365d.

7. **Calibration persistence + drift tracking** — `oracle/calibration.py` computes on-demand and never stores. Can't detect calibration drift over time.

### Analytics gaps

8. **Granger / transfer entropy / mutual information discovery engine** — confirmed no implementation exists. Unsupervised lead-lag discovery across features would auto-surface edges GRID isn't currently exploiting.

9. **HMM-style regime transition matrix** — `discovery/clustering.py` does regime discovery (PCA + GMM/KMeans/Agglomerative) but treats regimes as static labels. Transition probability `P(regime_next | regime_now, macro_state)` is missing.

10. **Narrative lifecycle tracker** — `earnings_transcript_analyzer.py` does tone snapshots per call but does not track narrative evolution across quarters, analyst consensus language drift, or paradigm shifts. Partial; needs extension.

11. **Causal DAG + do-calculus reasoning** — no explicit encoding of GRID's causal structure (Fed → rates → credit → equity). Relationships are implicit in feature correlations.

12. **Synthetic control event-study engine** — no "construct weighted non-affected basket to isolate event effect" infrastructure. Standard econometric causal inference for observational data.

13. **Pattern library with base rates** — no historical analog matcher ("current setup is 94% match to 2018 Q4; outcome was −15% then +20%"). Base-rate conditioning is missing.

14. **Adversarial LLM red-team loop** — `hypothesis_engine.py` generates hypotheses but nothing systematically red-teams each prediction with "smart bear" and "smart bull" LLM personas and measures rebuttal strength as a meta-feature.

### Data/positioning gaps (highest marginal value for swing/LEAPS)

15. **Liquidity regime classifier** — no module combines Fed balance sheet decomposition (TGA / RRP / reserves / SOMA) + primary dealer positions + RRP + SOFR dispersion + FX basis + bank reserves into a 5-state classifier. **Conditions every other prediction** → multiplicative value.

16. **Fed reaction function estimator** — `fed_speeches.py` ingests but no Bayesian model of the Fed loss function (what data surprise flips hawkish/dovish?).

17. **Structured-flow calendar engine** — no unified forward calendar of MSCI/Russell rebalances, index inclusions, options expiry, quarter-end, year-end, tax-loss harvesting with historical impact magnitudes.

18. **Primary dealer Treasury positioning** — H.4.1 + FR 2004 data not ingested at the positioning level.

19. **Prime broker client positioning notes** — GS/MS/JPM weekly client letters not scraped.

20. **TRACE corporate bond trade prints** — FINRA TRACE not ingested. Institutional rotation shows up in credit before equity.

21. **13F delta clustering** — `institutional_flows.py` ingests 13F but no quarter-over-quarter delta clustering across 500 funds.

22. **Insider cluster detector (3+ simultaneous)** — `insider_filings.py` ingests but no 3-or-more-C-suite-within-30-days cluster detection.

23. **Credit event probability machine** — no CDS term structure + bond price + equity vol + rating trajectory → P(distressed | 90d) per name.

24. **Cross-asset carry trade monitor** — no JPY/MXN/TRY/BRL carry stress + unwind probability tracker.

25. **China real-time electricity + rail freight** — US-centric macro blind spot; Li Keqiang Index logic missing.

26. **China LGFV + trust product default tracker** — same regional blind spot.

27. **European gas storage + TTF curve** — EU inflation/ECB/EUR/DAX blind spot.

28. **Japan MOF intervention + BOJ JGB operations** — carry trade condition tracker missing.

29. **Fed balance sheet decomposition** — GRID has `fed_liquidity.py` but probably not at the TGA / RRP / reserves / SOMA component level.

30. **FX swap basis + cross-currency basis** — dollar funding health proxy missing.

### Decomposition

Gaps 1-7 are **pure inference architecture** — no new data required. Estimated leverage: these alone are likely **4-6% Brier improvement** if implemented correctly.

Gaps 8-14 are **analytics engines** built on existing data. Estimated leverage: **1-3% each on relevant trade slices**.

Gaps 15-30 are **new data sources + aggregators**. Estimated leverage: **0.5-2% each**, with big multipliers for 15-16 because they condition everything else.

**Critical insight:** The order is intentional. Do 1-7 first. A flat combiner with rich signals loses to a smart combiner with fewer signals. GRID is currently the former.

---

## 8. Tier A shortlist — 40 signals

My highest-conviction picks from the session discussion. Each is estimated to deliver ≥1% certainty on the slice of trades it touches. Estimates are gut-level, not measured — treat as priors for a falsification test, not gospel.

### 20 new pullers (data sources)

**China blind spot (highest impact per unit effort):**
1. **China LGFV + trust product default tracker** (Wind/CSMAR scrape) — ~2-4% on commodities/EM/RMB trades
2. **China real-time electricity + rail freight** (State Grid / China Railway daily) — ~1.5% on anything China-sensitive
3. **European gas storage + TTF curve** (GIE + ICE) — ~2% on EUR/DAX/European credit
4. **Japan MOF intervention + BOJ JGB operations** (MOF weekly + BOJ daily) — ~3-5% on JPY crosses / carry trades
5. **BRICS+ central bank swap line activations** (PBoC + Fed H.4.1 + BIS) — ~2% on EM when triggered

**Mechanical flows (dwarf discretionary):**
6. **Sovereign wealth fund rebalancing calendar** (GIC/ADIA/Norway/Temasek disclosures + heuristics) — ~1-2% around quarter-ends
7. **Primary dealer Treasury positioning** (NY Fed H.4.1 + FR 2004) — ~2% on rates trades
8. **Prime broker net exposure notes** (GS/MS/JPM weekly letters, scraped) — ~2% on equity index + factor trades
9. **TRACE corporate bond trade prints** (FINRA TRACE) — ~1.5% on HY credit + cross-asset rotation
10. **Leveraged ETF daily rebalance demand** (TQQQ/SQQQ/TMF/SOXL AUM × daily returns) — ~1% on intraday/overnight index
11. **MSCI / Russell / S&P rebalance + inclusion calendar** (public schedules, not systematized) — ~2% on affected names

**Liquidity plumbing:**
12. **Fed balance sheet components decomposition** (H.4.1 granular: TGA, RRP, reserves, SOMA) — ~2% on risk-asset calls
13. **FX swap basis + cross-currency basis** (ICE, BIS) — ~2% on equity/USD/credit
14. **SOFR dispersion + repo volume** (NY Fed SOFR stats) — ~2-3% when triggered
15. **Bank deposit beta + reserves flight** (H.8 + FFIEC call reports) — ~1.5% on credit + regional bank + real estate

**Physical truth:**
16. **LME cancellation ratios + warehouse stocks** (LME + SHFE + COMEX daily) — ~2% on metals + miner equities
17. **Refined product crack spreads + refinery utilization** (EIA weekly detailed) — ~1.5% on energy
18. **Global port AIS-derived congestion** (MarineTraffic + VesselFinder + Port of LA) — ~1.5% on retail + logistics + inflation

**Structural tail risk:**
19. **Taiwan Strait OSINT** (ADS-B Exchange + MarineTraffic + PLA exercise tracking) — ~1% baseline, +10% when anomaly
20. **Insurance CAT risk + reinsurance rates** (Guy Carpenter + Artemis + Lloyd's) — ~1-2% on property/casualty + mortgage credit

### 20 new intelligence modules (analytics on existing + new data)

**Inference / dot connection (highest leverage):**
21. **Transfer entropy + Granger discovery engine** — unsupervised lead-lag across all features — ~2% oracle-wide
22. **Regime transition matrix (HMM + macro prior)** — extends `discovery/clustering.py` — ~2% on regime-conditional trades
23. **Narrative lifecycle tracker** — extends `earnings_transcript_analyzer.py` with QoQ delta + paradigm detection — ~2% on crowded trades
24. **Post-announcement drift scanner** — systematic catalog of (event × sector × mcap) → drift + half-life — ~1.5% on event-driven
25. **Structured-flow calendar engine** — unifies MSCI/Russell/expiry/quarter-end — ~1-2% in affected windows

**Causality engines:**
26. **Fed reaction function estimator** — Bayesian model over `fed_speeches.py` + FOMC votes + dot plots — ~3% on rates/risk around Fed events
27. **Dealer options surface (GEX/DEX/VEX/CEX) single-name** — extends `physics/dealer_gamma.py`; replaces crude net-short assumption — ~2% on single-name options
28. **Cross-source disagreement / lie detector (expanded)** — extends `cross_reference.py` to 3+ source consensus — ~2% on macro-release trades
29. **13F delta clustering across 500 funds** — extends `institutional_flows.py` — ~1.5% on factor + sector trades
30. **Earnings surprise cascade predictor** — leader → follower revisions via `earnings_calendar.py` + `analyst_ratings.py` — ~2% on earnings-season follow-the-leader

**Actor network extensions:**
31. **Director interlock + auditor graph** — extends `actor_network.py` with corporate governance overlay — ~2% on single-name credit/equity tail risk
32. **Credit event probability machine** — CDS + bond + equity vol + rating → P(distressed | 90d) — ~3% on credit + distressed
33. **Cross-asset carry trade monitor** — JPY/MXN/TRY/BRL stress + unwind probability — ~3% on FX and risk when building
34. **Central bank credibility tracker** — dynamic Bayesian ahead/behind-curve scoring — ~1.5% on FX + rates
35. **Insider cluster detector (3+ simultaneous)** — extends `insider_filings.py` — ~3% on rare triggers (~70% academic hit rate)

**Real-time state classifiers:**
36. **Liquidity regime classifier** — 5-state (gushing/ample/neutral/tightening/stressed), conditions everything — ~2-3% oracle-wide multiplicative
37. **Recession nowcast ensemble** — Sahm + yield curve + spreads + claims + ISM + confidence + permits — ~2% on cyclical vs defensive
38. **Financial conditions index (multi-factor)** — rates + spreads + vol + FX + credit + housing — ~1.5% oracle-wide

**NLP / novelty:**
39. **LLM-powered 10-K/10-Q risk factor novelty detector** — flags materially new language vs prior filing — ~2% on fundamental L/S
40. **Earnings call tone-shift detector** — extends `earnings_transcript_analyzer.py` with delta + CFO/CEO divergence — ~1.5% on post-earnings drift

### Monday-morning top 5 (if you can only build five things this quarter)

1. **Horizon-conditional oracle** (gap #1 — not on the Tier A list because it's inference architecture, not a signal) — multiplies the value of everything below
2. **Liquidity regime classifier** (#36) — conditions every other prediction
3. **Fed reaction function estimator** (#26) — every Fed event becomes a GRID opportunity
4. **Structured-flow calendar engine** (#25) — cheap to build, systematic alpha on known dates
5. **Transfer entropy discovery engine** (#21) — force-multiplier; finds signals in data GRID already has

---

## 9. Session artifacts + deferred items

### Commits on `claude/review-cryexc-app-T2icl`

| Commit | Hash | Artifact |
|---|---|---|
| Plant SEED-001 | `e0789d9` | `.planning/seeds/SEED-001-orderflow-primitives-cryexc-port.md` |
| Park 200-signal catalog TODO | `dd31534` | `.planning/signals/TODO-200-catalog.md` |
| Session roadmap + orientation fixes | *(this commit)* | `docs/planning/SESSION-ROADMAP-2026-04-13.md` + CLAUDE.md patches + skill updates |

### SEED-001 — Orderflow primitives port

**Location:** `.planning/seeds/SEED-001-orderflow-primitives-cryexc-port.md`
**Status:** dormant
**Trigger conditions (any one unlocks it):**
1. GRID adds intraday tactical entry timing (1-6h predictions instead of 6h-to-weeks regime calls) — expected Brier lift jumps from 0.1-0.5% to 2-5%
2. GRID begins trading crypto options directly via Deribit / Paradex / Lyra — book imbalance on the underlying becomes a genuine pricing input
3. GRID routes real orders and execution quality / slippage matters — CVD at execution saves hard dollars

**Scope:** Medium-Large (2-4 weeks full port, ~2 days for the falsification test)

**Cheap falsification test** (if uncertain whether to commit the full port later):
- Compute 1h-bar CVD for BTCUSDT only from Binance aggTrade data GRID already has
- Feed as single feature into one oracle model
- Hold out 30 days, measure Brier delta on BTC predictions
- Ship if Δ ≥ 0.2% on BTC; kill otherwise

**Auto-surfaces via:** `/gsd:new-milestone` when milestone scope matches any trigger.

### TODO-200-catalog — Full 200-signal brainstorm

**Location:** `.planning/signals/TODO-200-catalog.md`
**Status:** parked for next session
**Objective:** Brainstorm 200 candidate signals (~100 new pullers + ~100 new intelligence modules) each plausibly delivering ≥1% certainty gain. Expected hit rate after falsification testing: 10-25%, yielding 20-50 shipped signals.

**Partial output already delivered:** the 40 Tier A picks in [Section 8](#8-tier-a-shortlist--40-signals) above — this is the starting seed for tomorrow's full 200.

**Structure when built:**
```
.planning/signals/
├── CATALOG.md              (index, scoring rubric, tiering)
├── PULLERS.md              (~100 new data sources)
├── INTELLIGENCE.md         (~100 new analytics engines)
└── SHORTLIST-TIER-A.md     (highest-conviction subset, ship order)
```

### Required fields per catalog entry (for tomorrow's session)

1. ID, name, type (Puller / Intel / Hybrid)
2. Domain (macro / positioning / flows / sentiment / etc.)
3. One-line description
4. Why ≥1% (lever named, coverage estimate)
5. Lever vs Condition classification (per Prediction Causation Standard)
6. PIT feasibility (easy / tricky / hard — with reason)
7. Source (API / scrape / paid / free / OSINT)
8. Build cost (S / M / L)
9. Confidence tier (A = ship after quick test, B = needs falsification, C = speculative)
10. Overlap flag with existing GRID modules (check `docs/MODULE_CATALOG.md` first!)

---

## 10. Meta-principles of certainty (outside-the-box)

The user pushed for "think deeper and logically of what will bring about certainty" and "think outside the box." These are the principles that emerged from that thinking. **Every signal, module, and oracle change should be evaluated against this list** — not against "does it correlate with returns."

### The core reframe

> **GRID has evidence abundance and inference poverty.**

GRID has 104+ intelligence modules and 100+ pullers producing evidence. It has a **flat weighted-vote combiner** that turns evidence into predictions. The leverage is not in adding more evidence — it is in upgrading the combiner. A smarter inference layer on today's data will almost certainly beat a flat combiner on 2x the data.

### The 12 meta-principles

1. **Orthogonal stacking with known correlations.** 3 independent signals all pointing the same way >> 10 correlated signals pointing the same way. Measure and track feature correlations; Bayesian-update the posterior.

2. **Mechanism, not correlation.** Trust relationships that have a named actor and a named liquidity valve. Correlations break on regime shifts; mechanisms don't (until the mechanism itself changes). This IS GRID's Prediction Causation Standard — enforce it everywhere.

3. **Base-rate conditioning from historical analogs.** "This setup matches 2018 Q4 with 94% similarity; outcome was −15% then +20%" is a far better prior than "momentum is positive." Pattern library with historical outcomes.

4. **Market-implied priors as the benchmark.** GRID's edge = |GRID_p − market_p|. Any signal that doesn't move GRID's distribution **farther** from the market's current pricing is not alpha. Build a market-implied-probability comparator.

5. **Adversarial validation.** For every prediction, automatically generate the strongest counter-thesis a smart adversary would hold. If GRID can't rebut it, the certainty isn't real. LLM-powered red-team with a "smart bear" and "smart bull" persona, rebuttal strength as a meta-feature.

6. **Explicit uncertainty bounds.** Not `p = 0.65` but `p = 0.65 ± 0.08`. Size by the lower bound. Track calibration per confidence bucket.

7. **Horizon-matched evidence.** Measure each feature's predictive half-life and only use it at matching horizons. A 5d feature is noise at 365d.

8. **Regime-specific submodel routing.** Instead of one oracle, five sub-oracles trained on specific regimes (growth / neutral / fragile / crisis / recovery). Route predictions to the active regime's oracle. Smooth handoff via regime probability.

9. **Information-theoretic feature selection.** Prioritize signals by **bits of information** reduced about the outcome, not by correlation coefficient. Entropy reduction is the honest metric.

10. **Calibration history + drift tracking.** When GRID says p=0.7, it should BE right ~70% of the time. Measure and correct. Persist calibration curves; alert on drift.

11. **Tail robustness — counterfactual stress tests.** Every prediction should show its P(outcome | GFC-analog), P(outcome | dotcom-analog), P(outcome | SNB-style shock). Max adverse excursion bound. Kelly fraction capped by tail-aware adjustment.

12. **Pre-registration of theses.** Write lever + condition + invalidation BEFORE looking at recent data. Prevents post-hoc rationalization. GRID has decision journal infrastructure (`journal/log.py`) — enforce that every trade pre-registers.

### Outside-the-box ideas worth capturing

- **Ensemble disagreement → volatility trade.** When the 5 oracle models disagree on direction, don't predict direction — predict *vol* and route to long-straddle plays.
- **Shapley attribution per prediction.** Decompose every prediction into feature contributions. Size down when fragile features dominate.
- **Second-order signals.** Don't predict BTC; predict "probability Goldman ups its year-end target." Meta-signals about signal-makers.
- **Reflexivity modeling.** When a GRID prediction is acted upon, it changes the outcome. Model this for live-traded predictions.
- **Feature temperature / half-life tracking.** Every feature decays in weight if recent performance drifts. Dynamic downweighting.
- **Synthetic controls for events.** For any event, construct a weighted basket of non-affected assets that matches the pre-event path. Post-event difference = causal effect. Econometric causal inference for observational data.
- **Proof-of-work for predictions.** Each prediction must be derivable from at least 3 independent paths through the evidence. Single-path predictions get lower confidence automatically.
- **Consensus crowding as a negative signal.** When a signal aligns with crowd positioning (short interest low, fund positioning extreme, media volume high), discount it — the market has already priced it.
- **The "simplest counter-explanation" test.** Ask an LLM: "Given these signals, what's the simplest reason the market hasn't priced this yet?" If the counter-explanation is strong, size down.
- **Causal DAG with do-calculus.** Encode GRID's believed causal structure explicitly. Use do-calculus to answer "what does the signal predict if we INTERVENE on X?" — different from observational prediction.

### The bottom line

**Stop adding evidence sources. Start building the inference machine.** 40 good signals through a world-class inference engine with horizon separation, causal DAG, Shapley attribution, uncertainty propagation, and adversarial validation will outperform 200 signals in a bag-of-features oracle.

**This is the architectural message of the session.** Every future decision should weigh: "does this upgrade the combiner, or does this add more evidence?" Front-load combiner work.

---

## End of roadmap

**Next session start-up checklist:**
1. Read [Section 1: Session-Start Pre-Read](#1-session-start-pre-read-read-this-first) above.
2. Read `docs/MODULE_CATALOG.md` for the full module inventory (do not rely on CLAUDE.md).
3. Check `.planning/seeds/` for dormant seeds.
4. Check `.planning/signals/` for parked catalog work.
5. Run `/grid-check-exists <keyword>` before proposing to build anything new.
6. If in doubt, grep across `intelligence/`, `analysis/`, `physics/`, `features/`, `discovery/`, `trading/`, `oracle/`.

**Branch:** `claude/review-cryexc-app-T2icl`
**Date:** 2026-04-13
