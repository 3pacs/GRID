# Implementation Plan: GRID Alpha Research Engine

> **Sources**: QuantaAlpha (Saulius.io), RD-Agent-Quant (Microsoft/NeurIPS 2025), FinRL-X (AI4Finance), TradingAgents (TauricResearch), Factor Zoo (JPM 2024), FinRL-DeepSeek
> **Goal**: Absorb proven alpha research frameworks into GRID, leapfrogging ~1 year of methodology development. Prioritized by verified live results > OOS backtests > in-sample only.

---

## Task Type
- [x] Backend (Python — factor engine, DSL, LightGBM, backtesting)
- [ ] Frontend (deferred — leaderboard UI comes after engine works)
- [x] Fullstack (API endpoints + eventual visualization)

---

## What QuantaAlpha Gives Us (Baseline Findings)

### From Article 1: DSL Factor Mining
- **Evolutionary pipeline**: 5 directions x 3 calls/direction x 5 evolution rounds
- **DSL with 60+ operators**: constrains LLM to composable, interpretable factors
- **Results**: 80% of explored factors had positive OOS RankIC; best Sharpe 1.72 (38.7% return)
- **Key insight**: Crossover > mutation > random. Top 3 factors were all crossover products.
- **Key signal**: Vol-regime adaptive momentum (switches 5d/20d lookback by vol regime)

### From Article 2: LightGBM Layer
- **18 factors per individual**, trained via LightGBM to predict next-day returns
- **Population-based evolution**: 16 initial x 8 rounds = ~96 candidates per run
- **False Discovery Gauntlet**: 5 statistical tests (permutation, deflated Sharpe, subsample stability, decay, CV consistency)
- **Results**: Best model 23.2% net return, 0.97 net Sharpe (after 10bps/side costs) on true OOS
- **Key finding**: Validation RankIC does NOT predict OOS RankIC (R^2=0.00). But validation net Sharpe DOES predict OOS net Sharpe (R^2=0.42).
- **64% RankIC shrinkage** from validation to OOS — budget for it
- **Regime is the bottleneck**: No factor set consistently works across COVID/post-COVID/Ukraine

---

## What GRID Already Has (Our Advantages)

| Capability | GRID Status | QuantaAlpha Equivalent |
|------------|-------------|----------------------|
| **Feature registry** | 1,238 features, 96.4% with data | 18 DSL factors per individual |
| **PIT store** | Production-grade, lookahead-proof | Train/test split with embargo |
| **Oracle engine** | 5 competing models, weight evolution | LightGBM ensemble |
| **Backtesting** | Walk-forward, 874 winners found | Walk-forward with purged CV |
| **Alpha101** | 101 classical quant factors implemented | ~60 DSL operators |
| **Intelligence layer** | 14 modules, 495 actors, trust scoring | N/A (QuantaAlpha is pure quant) |
| **Signal registry** | 200+ signals with aggregation | N/A |
| **Data sources** | 46 active, 76.7M raw rows | 53 commodity futures OHLCV |
| **LLM inference** | Qwen 32B local, llama.cpp | Claude Code API calls |
| **Scheduler** | Hermes (hourly/4h/6h/daily cycles) | Bash scripts |

### GRID's Unique Edge Over QuantaAlpha
1. **Actor-level intelligence (THE edge)**: "Who moved the money" is not available to a pure price-based system. 495 named actors with trust scores, lever vs condition distinction. The vibes of the people pulling the levers — congressional dumps before committee votes, insider filing clusters, dark pool inversions — are the real signals. Price patterns are downstream effects.
2. **Derivatives mechanics as transmission layer**: `dealer_gamma.py`, `options_recommender.py`, GEX/vanna/charm. This explains HOW actor intent propagates into price: senator sells → dealer hedges → gamma flips → price waterfall. The mechanics of the lever pull.
3. **Fundamental + alternative data**: QuantaAlpha only uses OHLCV. GRID has congressional trades, insider filings, dark pool flows, Fed actions, social sentiment, prediction markets, macro data.
4. **The killer factor**: `WHERE(GT($lever_puller_activity, THRESHOLD), MULTIPLY($lever_direction, $gex_alignment), 0)` — when actors are active AND gamma mechanics align with their direction, trade. Otherwise stay flat. Nobody else can build this because nobody else has the data.
5. **Multi-asset coverage**: GRID covers equities, options, crypto, commodities. QuantaAlpha tested on 20-53 commodity futures only.
6. **Real-time intelligence loop**: Hermes runs hourly. QuantaAlpha is batch-only.

---

## Technical Solution

### Architecture: QuantaAlpha Engine as a New GRID Subsystem

```
grid_repo/
├── alpha_research/                    # NEW: QuantaAlpha-inspired subsystem
│   ├── __init__.py
│   ├── dsl/
│   │   ├── __init__.py
│   │   ├── parser.py                 # DSL expression parser
│   │   ├── operators.py              # 60+ operator implementations
│   │   ├── compiler.py               # DSL → vectorized pandas/numpy
│   │   └── validator.py              # Expression validation + complexity limits
│   │
│   ├── evolution/
│   │   ├── __init__.py
│   │   ├── population.py             # Individual (18 factors) + population management
│   │   ├── mutation.py               # LLM-guided mutation (feature importance → targeted replacement)
│   │   ├── crossover.py              # LLM-guided crossover (combine orthogonal signals)
│   │   ├── selection.py              # Fitness ranking + tournament selection
│   │   └── island.py                 # Multi-island populations (momentum, vol, mean-rev, fundamental)
│   │
│   ├── training/
│   │   ├── __init__.py
│   │   ├── lgbm_trainer.py           # LightGBM training with purged CV
│   │   ├── walk_forward.py           # Walk-forward with dynamic embargo
│   │   ├── feature_builder.py        # DSL factors → feature matrix (PIT-correct via GRID's store)
│   │   └── target_builder.py         # Forward returns at multiple horizons
│   │
│   ├── validation/
│   │   ├── __init__.py
│   │   ├── gauntlet.py               # False Discovery Gauntlet orchestrator
│   │   ├── permutation_test.py       # 1000-shuffle permutation test
│   │   ├── deflated_sharpe.py        # Bailey & Lopez de Prado (2014)
│   │   ├── subsample_stability.py    # Instrument subsample splits
│   │   ├── decay_analysis.py         # RankIC at horizons [1,2,5,10,20]
│   │   └── cv_consistency.py         # Cross-fold positive fraction
│   │
│   ├── portfolio/
│   │   ├── __init__.py
│   │   ├── constructor.py            # L/S portfolio from factor ranks
│   │   ├── cost_model.py             # Transaction cost modeling (configurable bps)
│   │   └── metrics.py                # Sharpe, RankIC, ICIR, Calmar, MaxDD, turnover
│   │
│   ├── orchestrator.py               # Full pipeline: direction → evolve → validate → report
│   ├── prompts/                       # LLM prompt templates for mutation/crossover
│   │   ├── hypothesis.md
│   │   ├── mutation.md
│   │   └── crossover.md
│   └── config.py                      # Hyperparameters, thresholds, universe definitions
│
├── api/routers/
│   └── alpha_research.py             # NEW: API endpoints for factor leaderboard, evolution status
│
└── tests/
    ├── test_dsl_parser.py
    ├── test_dsl_operators.py
    ├── test_evolution.py
    ├── test_gauntlet.py
    └── test_alpha_research_integration.py
```

### Key Design Decisions

1. **Use GRID's PIT store instead of raw DataFrames**: QuantaAlpha loads CSV. We load from `resolved_series` with full PIT correctness. This is strictly better — no lookahead bias by construction.

2. **Extend DSL with GRID's unique data**: Beyond OHLCV operators, add:
   - `$congressional_flow` — net congressional buying/selling
   - `$insider_flow` — insider transaction volumes
   - `$darkpool_flow` — dark pool activity
   - `$gex` — gamma exposure
   - `$fed_rate` — Fed funds rate
   - `$sentiment` — composite sentiment score
   - `$trust_convergence` — trust scorer convergence signal
   This is the key differentiator. QuantaAlpha's DSL only sees price. GRID's DSL sees the world.

3. **LLM-guided evolution via local Qwen 32B**: Instead of Claude API calls (expensive, rate-limited), use GRID's existing llama.cpp Qwen 32B for mutation/crossover prompts. Fall back to Claude for complex crossover if needed.

4. **Multi-island populations with thematic seeding**:
   - Island 1: Momentum + trend (price-based)
   - Island 2: Volatility + regime (vol-based)
   - Island 3: Flow + positioning (GRID's fundamental edge)
   - Island 4: Sentiment + alternative (GRID's alt-data edge)
   Migration of top factors between islands every 2 rounds.

5. **False Discovery Gauntlet is non-negotiable**: Every factor must pass the 5-test gauntlet before promotion. Verdict thresholds from Article 2:
   - ROBUST: CV >=75%, permutation p<0.05, subsample >50%, val net Sharpe >0.3
   - MARGINAL: CV >=50%, permutation p<0.10
   - UNSTABLE: everything else

6. **Transaction costs in fitness**: QuantaAlpha learned this the hard way — net Sharpe predicts OOS, raw RankIC doesn't. Include 10bps/side in all fitness evaluations.

7. **Integration with Oracle**: Validated factors feed into Oracle's signal assembly. This closes the loop:
   ```
   Alpha Research → validated factors → signal_registry → Oracle → predictions → scoring → feedback
   ```

---

## Implementation Steps

### Phase 1: DSL Engine (Foundation)
**Expected deliverable**: Parseable, executable DSL with 60+ operators running on GRID's PIT data

1. **Create `alpha_research/dsl/operators.py`** — Implement all 60+ operators from QuantaAlpha:
   - Time-series: `TS_MEAN`, `TS_STD`, `TS_ZSCORE`, `TS_RANK`, `TS_CORR`, `TS_SKEW`, `TS_KURT`, `EMA`, `DELAY`, `DIFF`, `TS_MAX`, `TS_MIN`, `TS_ARGMAX`, `TS_ARGMIN`, `TS_SUM`
   - Cross-sectional: `RANK`, `MEAN`, `STD`, `ZSCORE`
   - Conditional: `WHERE`, `GT`, `LT`, `GE`, `LE`, `AND`, `OR`, `NOT`
   - Arithmetic: `ADD`, `SUBTRACT`, `MULTIPLY`, `DIVIDE`, `ABS`, `LOG`, `SIGN`, `MAX`, `MIN`, `POWER`
   - GRID extensions: `$congressional_flow`, `$insider_flow`, `$darkpool_flow`, `$gex`, `$fed_rate`, `$sentiment`, `$trust_convergence`, `$dollar_flow`, `$actor_signal`

2. **Create `alpha_research/dsl/parser.py`** — S-expression parser: `RANK(TS_ZSCORE($return, 20))` → AST → executable

3. **Create `alpha_research/dsl/compiler.py`** — AST → vectorized numpy/pandas operations on GRID's PIT-indexed DataFrames

4. **Create `alpha_research/dsl/validator.py`** — Complexity limits (max depth 8, max operators 20), lookahead checks, NaN handling rules

5. **Tests**: Parser round-trips, operator correctness vs manual calculation, PIT compliance

### Phase 2: Evolution Engine
**Expected deliverable**: LLM-guided mutation and crossover producing new factor sets

6. **Create `alpha_research/evolution/population.py`** — Individual (18 DSL expressions + metadata), Population (16 individuals), serialization to/from JSON

7. **Create `alpha_research/prompts/hypothesis.md`** — Prompt template for initial factor generation from research direction

8. **Create `alpha_research/prompts/mutation.md`** — Prompt template: parent factors + feature importances + metrics → targeted mutations

9. **Create `alpha_research/prompts/crossover.md`** — Prompt template: two parents + importances → hybrid factor set

10. **Create `alpha_research/evolution/mutation.py`** — LLM-guided mutation: call Qwen 32B with parent analysis, parse DSL output, validate

11. **Create `alpha_research/evolution/crossover.py`** — LLM-guided crossover: merge top factors from two parents, fill gaps

12. **Create `alpha_research/evolution/selection.py`** — Tournament selection by validation RankIC, keep top-K

13. **Create `alpha_research/evolution/island.py`** — Multi-island with thematic seeding + migration

### Phase 3: Training Pipeline
**Expected deliverable**: LightGBM models trained on DSL factors with purged walk-forward CV

14. **Create `alpha_research/training/feature_builder.py`** — DSL factors → feature matrix via GRID's PIT store (no lookahead by construction)

15. **Create `alpha_research/training/target_builder.py`** — Forward returns at [1, 2, 5, 10, 20] day horizons

16. **Create `alpha_research/training/walk_forward.py`** — Purged time-series CV with dynamic embargo (max lookback window)

17. **Create `alpha_research/training/lgbm_trainer.py`** — LightGBM training: features → RankIC on validation fold

### Phase 4: False Discovery Gauntlet
**Expected deliverable**: 5-test statistical validation pipeline with ROBUST/MARGINAL/UNSTABLE verdicts

18. **Create `alpha_research/validation/permutation_test.py`** — 1000-shuffle null distribution, p-value

19. **Create `alpha_research/validation/deflated_sharpe.py`** — Bailey & Lopez de Prado adjustment for multiple testing

20. **Create `alpha_research/validation/subsample_stability.py`** — 20 random instrument splits, consistency check

21. **Create `alpha_research/validation/decay_analysis.py`** — RankIC at multiple horizons, smooth decay = real signal

22. **Create `alpha_research/validation/cv_consistency.py`** — Fraction of CV folds with positive validation RankIC

23. **Create `alpha_research/validation/gauntlet.py`** — Orchestrate all 5 tests, apply verdict thresholds, generate report

### Phase 5: Portfolio & Metrics
**Expected deliverable**: Long/short portfolio construction with realistic cost modeling

24. **Create `alpha_research/portfolio/constructor.py`** — Rank instruments by factor value, top-N long, bottom-N short

25. **Create `alpha_research/portfolio/cost_model.py`** — Configurable transaction costs (default 10bps/side), turnover tracking

26. **Create `alpha_research/portfolio/metrics.py`** — RankIC, ICIR, Sharpe, Calmar, MaxDD, annualized return, win rate, turnover

### Phase 6: Orchestrator & Integration
**Expected deliverable**: End-to-end pipeline: research direction → validated factors → Oracle integration

27. **Create `alpha_research/orchestrator.py`** — Full pipeline:
    ```
    research_direction → seed population → [evolve 8 rounds] → gauntlet → verdict → report
    ```

28. **Create `alpha_research/config.py`** — All hyperparameters:
    - Population size: 16
    - Evolution rounds: 8
    - Factors per individual: 18
    - Train/val/OOS splits
    - Embargo days
    - Cost assumptions (10bps/side)
    - Verdict thresholds
    - Universe definitions (tickers, asset classes)

29. **Wire into Hermes scheduler** — Weekly alpha research cycle (Sunday night)

30. **Wire into signal_registry** — ROBUST/MARGINAL factors auto-register as signals

31. **Create `api/routers/alpha_research.py`** — Endpoints:
    - `GET /alpha/leaderboard` — all factors ranked by OOS metrics
    - `GET /alpha/evolution/{run_id}` — evolution trajectory for a run
    - `GET /alpha/gauntlet/{factor_id}` — full statistical validation report
    - `GET /alpha/factors/{factor_id}/performance` — per-instrument breakdown
    - `POST /alpha/research` — trigger new research direction

32. **Integration tests** — End-to-end: seed → evolve → validate → score on historical data

---

## Key Files

| File | Operation | Description |
|------|-----------|-------------|
| `alpha_research/dsl/operators.py` | Create | 60+ DSL operators + GRID extensions |
| `alpha_research/dsl/parser.py` | Create | S-expression parser → AST |
| `alpha_research/dsl/compiler.py` | Create | AST → vectorized pandas/numpy |
| `alpha_research/evolution/population.py` | Create | Individual/population management |
| `alpha_research/evolution/mutation.py` | Create | LLM-guided factor mutation |
| `alpha_research/evolution/crossover.py` | Create | LLM-guided factor crossover |
| `alpha_research/evolution/island.py` | Create | Multi-island with migration |
| `alpha_research/training/lgbm_trainer.py` | Create | LightGBM with purged CV |
| `alpha_research/training/walk_forward.py` | Create | Walk-forward validation |
| `alpha_research/training/feature_builder.py` | Create | DSL → PIT-correct features |
| `alpha_research/validation/gauntlet.py` | Create | 5-test False Discovery Gauntlet |
| `alpha_research/validation/permutation_test.py` | Create | 1000-shuffle permutation |
| `alpha_research/validation/deflated_sharpe.py` | Create | Multiple testing adjustment |
| `alpha_research/portfolio/constructor.py` | Create | L/S portfolio from ranks |
| `alpha_research/portfolio/cost_model.py` | Create | Transaction cost modeling |
| `alpha_research/portfolio/metrics.py` | Create | Sharpe, RankIC, ICIR, Calmar |
| `alpha_research/orchestrator.py` | Create | Full pipeline orchestrator |
| `alpha_research/config.py` | Create | All hyperparameters |
| `api/routers/alpha_research.py` | Create | API endpoints |
| `intelligence/signal_registry.py` | Modify | Accept alpha_research factors |
| `oracle/signal_aggregator.py` | Modify | Consume validated alpha factors |
| `scheduler/hermes.py` | Modify | Weekly alpha research cycle |

---

## Risks and Mitigation

| Risk | Mitigation |
|------|------------|
| **Overfitting** (the real enemy per Saulius) | False Discovery Gauntlet is mandatory. 5 tests, pre-committed thresholds. No factor promoted without MARGINAL+ verdict. |
| **LLM cost for evolution** | Use Qwen 32B local (free, already running) for mutation/crossover. Claude API only for complex crossover if local fails. |
| **Compute time** (LightGBM training x 96 candidates x 8 rounds) | Parallelize across individuals. LightGBM is fast (~seconds per model on GRID's data). Whole run should complete overnight. |
| **64% RankIC shrinkage** (proven by Saulius) | Budget for it. Only promote factors with val net Sharpe > 1.4 (Article 2 threshold). |
| **Regime sensitivity** (CV consistency bottleneck) | Multi-island populations with regime-specific islands. GRID's intelligence layer provides regime signals (trust_scorer convergence, lever_pullers activity) as additional features. |
| **DSL complexity explosion** | Validator enforces depth <= 8, operators <= 20. Complexity penalty in fitness function. |
| **Integration with existing Oracle** | Alpha factors enter via signal_registry (existing interface). No Oracle refactoring needed. |

---

## What This Unlocks for GRID

1. **Systematic alpha generation** — instead of manual hypothesis writing, LLM explores the factor space autonomously
2. **Statistical rigor** — 5-test gauntlet prevents fool-yourself dynamics (GRID's current Oracle lacks this)
3. **Fundamental data edge** — QuantaAlpha only used OHLCV. GRID's DSL will include congressional, insider, dark pool, sentiment, macro signals. No one else has this in an evolutionary factor engine.
4. **Self-improving loop** — weekly research cycles accumulate validated factors. Oracle gets better every week without human intervention.
5. **Regime awareness** — GRID's intelligence layer provides regime context that QuantaAlpha lacks entirely
6. **The Flywheel**: More data → better factors → better predictions → better scoring → better data selection → more data

---

## QuantaAlpha Baselines to Beat

| Metric | QuantaAlpha (Article 1) | QuantaAlpha (Article 2) | GRID Target |
|--------|------------------------|------------------------|-------------|
| Universe | 53 commodities | 20 commodities | Multi-asset (equities, options, crypto, commodities) |
| Factors explored per run | 20 | 96 | 128+ (multi-island) |
| Best OOS Sharpe (gross) | 1.72 | 2.23 | >2.0 |
| Best OOS Sharpe (net) | N/A | 0.97 | >1.0 |
| Best OOS return (net) | 38.7% (gross) | 23.2% | >25% |
| RankIC shrinkage | N/A | 64% | <50% (fundamental data should help) |
| Statistical validation | Train/test split only | 5-test gauntlet | 5-test gauntlet + GRID intelligence cross-check |
| Data types | OHLCV only | OHLCV only | OHLCV + fundamental + alternative + actor signals |
| Evolution guidance | Blind LLM | Feature importance | Feature importance + intelligence layer context |

---

## Saulius Findings Absorbed as GRID Axioms

1. **Crossover > mutation > random** — weight crossover higher in selection
2. **Net Sharpe predicts OOS; raw RankIC doesn't** — always evaluate net of costs
3. **64% RankIC shrinkage is the baseline** — never trust validation numbers at face value
4. **Regime is the bottleneck** — invest in regime detection (GRID's Q3 goal accelerated)
5. **DSL constraint is the key** — don't let LLM write arbitrary Python. Composable primitives only.
6. **LLMs are better evolutionary operators than random mutation** — they understand solution structure
7. **Multi-island populations + migration > single pool** — diversity matters
8. **Transaction costs are a natural regularizer** — high-turnover models get penalized automatically
9. **Deflated Sharpe is essential** — adjust for multiple testing (96+ candidates per run)
10. **Validation net Sharpe > 1.4 minimum** — below this, expect negative OOS net Sharpe

---

## ADDENDUM: Findings from Cross-Agent Review

### A. Regime-Signal Routing (GRID GMM → Factor Assignment)

Map GRID's existing GMM regime states to specific factor activation:

| GMM State | Active Signals | Inactive/Inverted |
|-----------|---------------|-------------------|
| **GROWTH** | Vol Regime Adaptive (20d branch), Trend Volume Gate, Dual Horizon | Mean reversion OFF |
| **NEUTRAL** | Vol-Price Divergence Contrarian, Mean Reversion Z-score | Reduce momentum weight |
| **FRAGILE** | Vol Regime Adaptive (5d branch only), Divergence Contrarian | Long-term momentum OFF |
| **CRISIS** | All signals OFF except vol-of-vol scalar. Invert momentum. VIX-above-MA exposure scaling. | Everything else OFF |

**Key insight from QuantConnect postmortem (Baldisserri):** HMMs fail for bear market detection — too slow. Single-threshold VIX (VIX > 20) didn't improve Sharpe. **What worked: VIX above its moving average as a continuous exposure scalar** — not a binary switch. Reduce exposure proportionally as VIX/MA rises.

Implementation: Add `alpha_research/regime/exposure_scaler.py` — VIX/VIX_MA ratio as continuous [0, 1] position sizing multiplier. Wire into portfolio constructor.

### B. Anti-Patterns (What Explicitly Doesn't Work)

These are verified failures — do NOT implement:

1. **All-weather alphas** — QuantConnect proved it: top 5% still underperformed SPY. Structure: small regime-conditional factors, not monolithic strategies.
2. **HMMs for bear market detection** — too slow. Don't implement.
3. **High validation RankIC as selection criterion** — R² = 0.00 predicting OOS. Use net Sharpe instead.
4. **Single-agent LLM trading** — TradingAgents paper: multi-agent with debate consistently beats single-agent.
5. **Phantom portfolios (TradeTrap)** — LLM agents hallucinate retained positions they've liquidated. Must inject explicit portfolio state JSON every call.
6. **Self-improving loops without fresh data** — entropy decay: strategy space collapses. Must inject real data at each generation.
7. **Volume-weighted 10-day momentum (standalone)** — OOS Sharpe -1.24, return -40.2%. Worst performer in sweep.
8. **Dual regime breakout momentum without directional volume confirmation** — OOS Sharpe -1.47, return -41.7%.

### C. TradingAgents Architecture (Bull/Bear Debate Layer)

**Bull/Bear debate is non-optional.** TradingAgents paper: adversarial debate consistently outperformed single-agent baselines on Sharpe, cumulative returns, and max drawdown.

Agent roles and model tiers for GRID:

| Role | Model | Rationale |
|------|-------|-----------|
| Fund Manager / Orchestrator | Opus (or Qwen 32B local) | Portfolio allocation, multi-source synthesis |
| Risk Manager | Opus (or Qwen 32B local) | Tail risk, hard constraints — can't be wrong |
| Fundamental Analyst | Sonnet (or Qwen 32B local) | Interprets filings, domain reasoning |
| Sentiment Analyst | Sonnet (or Qwen 32B local) | Nuanced language |
| Technical Analyst | Haiku (or Qwen 7B) | Structured calculations, speed |
| News Analyst | Haiku (or Qwen 7B) | Classification, summarization |

**Tool isolation rules:**
- Risk Manager: exec + read only. No web. Operates only on data already in workspace.
- Execution Agent: sandboxed. Network access restricted to broker API only.
- No agent has access to everything.

**Implementation:** Add `alpha_research/debate/` with `bull_agent.py`, `bear_agent.py`, `synthesizer.py`. Wire into Oracle signal assembly as pre-prediction step.

### D. Critic-Author Separation in Mutation

Two-pass mutation (from Mind Evolution paper):
1. **Critique pass**: LLM analyzes parent's weaknesses — which factors are low-importance, which categories are over/under-represented, which time periods failed
2. **Generation pass**: LLM generates targeted replacements informed by the critique

This is NOT the same as single-pass mutation. The critique creates a structured analysis that constrains the generation. Implement as two sequential LLM calls in `mutation.py`.

### E. Data Gaps to Close

| Gap | Source | Priority | GRID Status |
|-----|--------|----------|-------------|
| **ALFRED vintage data** | FRED (free, separate endpoint) | HIGH | Have FRED, need ALFRED for macro surprise signals |
| **Intraday open prices** | Alpha Vantage / yfinance | MEDIUM | Need $open for overnight gap factor (top LightGBM feature) |
| **Earnings/fundamental data** | SEC EDGAR / Alpha Vantage | MEDIUM | GRID has EDGAR filings but not structured EPS/revenue |
| **CV consistency across COVID** | N/A (open problem) | KNOWN | GRID's 1947 history via FRED gives more regime samples than QuantaAlpha's 2016-2025 |

### F. Tactical Priority Ranking

Ranked by **(alpha x speed) / new infrastructure needed**:

| # | Action | New Code | Expected Impact | Timeline |
|---|--------|----------|----------------|----------|
| 1 | **Vol Regime Adaptive signal** | ~100 lines | Sharpe 1.72 OOS proven | Day 1 |
| 2 | **False Discovery Gauntlet** on existing backtest infra | ~400 lines | Changes how everything is evaluated | Day 1-2 |
| 3 | **Val net Sharpe > 1.4 filter** as go/no-go threshold | ~20 lines | Prevents fool-yourself dynamics | Day 1 |
| 4 | **Vol-Price Divergence Contrarian** | ~80 lines | Sharpe 1.03, MaxDD only -16.9% | Day 2 |
| 5 | **Trend-Volume Gate** | ~60 lines | Sharpe 1.11 | Day 2 |
| 6 | **VIX-as-MA-scalar exposure model** | ~50 lines | Replace binary regime switch | Day 2-3 |
| 7 | **LightGBM signal ensemble** on top factors | ~200 lines | 23.2% OOS return proven | Day 3-4 |
| 8 | **Bull/Bear debate layer** in signal generation | ~300 lines | Multi-agent > single-agent proven | Day 4-5 |
| 9 | **Full DSL engine** | ~800 lines | Enables evolutionary loop | Week 2 |
| 10 | **Multi-island evolutionary loop** | ~600 lines | Autonomous alpha generation | Week 2-3 |

**Key realization:** Items 1-6 require almost no new infrastructure — they use GRID's existing data, PIT store, and backtest engine. The full QuantaAlpha evolutionary engine (items 9-10) is the long game, but the individual signals and the gauntlet can ship immediately.

### G. Additional Verified Signal: Vol-Price Divergence Contrarian

Missed from Article 1:
```
# Price extends past 20-day SMA without volume confirmation → bet mean reversion
WHERE(GT(ABS(TS_ZSCORE($close - TS_MEAN($close, 20), 20)), 1.5),
     WHERE(LT(TS_ZSCORE($volume, 20), 0),
          MULTIPLY(SIGN(SUBTRACT(TS_MEAN($close, 20), $close)), 1),
          0),
     0)
```
OOS Sharpe: **1.03** | Return: **24.3%** | MaxDD: **-16.9%** (best risk-adjusted on drawdown)

---

## ADDENDUM 2: Extended Research Findings

### H. FinRL-X Adaptive Rotation — VERIFIED LIVE RESULTS

**This is the only system in the entire sweep with verified live paper trading results.**

Paper Trading (Oct 2025 – Mar 2026, 5 months live on Alpaca):

| Metric | Adaptive Rotation | SPY | QQQ |
|--------|------------------|-----|-----|
| Total Return | **+19.76%** | -2.51% | -4.79% |
| Annualized | **62.16%** | -6.60% | -12.32% |
| Sharpe | **1.96** | -0.55 | -0.73 |
| Max Drawdown | **-12.22%** | -5.35% | -7.88% |
| Win Rate | **64.89%** | 52.13% | 54.02% |

Historical Backtest (Jan 2018 – Oct 2025):
Adaptive Rotation: Sharpe 1.10, 22.32% annualized, MaxDD -21.46% vs QQQ Sharpe 0.81, MaxDD -35.12%

**How it works (directly implementable in GRID):**
- 3 asset groups: Growth Tech, Real Assets, Defensive
- Max 2 active groups at once
- Group selection: Information Ratio relative to QQQ benchmark
- Intra-group ranking: Residual momentum + robust Z-score
- Regime detection: Slow (26-week trend + VIX) + Fast Risk-Off (3-day shock)
- Risk controls: trailing stop, absolute stop, cooldown periods
- Rebalance: weekly full + daily monitoring

**Repo:** github.com/AI4Finance-Foundation/FinRL-Trading

**GRID implementation path:**
- GRID already has VIX via FRED, price data via yfinance, regime detection via GMM
- The 26-week trend + VIX dual regime is simpler and more proven than GRID's current approach
- 3-day shock detector maps to GRID's circuit_breaker.py pattern
- Asset group rotation maps to GRID's sector flow analysis (flow_aggregator.py)
- Trailing/absolute stops already in paper_engine.py

### I. TradingAgents v0.2.3 — Direct GRID Integration

Now supports Claude 4.6 natively AND Ollama — GRID's existing llama3.2 plugs straight in:

```python
config["llm_provider"] = "ollama"
config["deep_think_llm"] = "llama3.2"  # GRID's existing setup
```

Install: `pip install .` then `tradingagents` CLI.
Alpha Vantage key already in GRID's stack.

**This changes the TradingAgents integration from "build our own debate layer" to "pip install and configure."** The multi-agent debate architecture is already built — GRID just needs to plug in its data sources as tools.

### J. FinRL-DeepSeek — Regime-Conditional RL Finding

**Key finding:** Bull market → use PPO (standard RL). Bear market → use CPPO-DeepSeek (risk-sensitive RL with LLM sentiment signals).

Architecture:
- CPPO = Conditional Value-at-Risk PPO (CVaR objective, penalizes tail losses)
- LLM adds two signals: sentiment score + risk assessment from financial news
- Dataset: FNSPID (Nasdaq-100 news 2013–2023), 10-year span
- Evaluation: Information Ratio, CVaR, Rachev Ratio (not just Sharpe)
- Backtested 2019–2023

**GRID mapping:**
- GMM GROWTH/NEUTRAL → standard momentum signals (PPO-equivalent behavior)
- GMM FRAGILE/CRISIS → switch to CPPO-style risk-weighted position sizing
- LLM sentiment signals → GRID already has these (trust_scorer convergence, social sentiment, GDELT)
- CVaR as risk metric → add to gauntlet alongside Sharpe/MaxDD

**Implementation:** Not a separate RL system. The insight is the regime-conditional risk objective. When GRID detects FRAGILE/CRISIS, switch from "maximize return" to "minimize tail risk" as the optimization target. This is a config change in the portfolio constructor, not a new model.

### K. OpenClaw Operational Patterns

**HEARTBEAT.md pattern** — a checklist file read autonomously on a schedule:

```markdown
# GRID Heartbeat (every 30 minutes)
- Check GMM regime state — alert if transition detected
- Check VIX vs 20-day MA — alert if crosses above
- Check portfolio positions vs stop levels
- Scan FRED for new macro data releases
- Monitor GDELT for sector-relevant event spikes
- Check Hermes puller health — alert if >3 failures in 6h
- Verify PIT store freshness — alert if stale >24h
```

**GRID mapping:** This is a Hermes scheduler job. Add a `heartbeat` task that runs every 30 minutes, checks these conditions, and pushes alerts via existing Telegram/email infrastructure.

**Trade journal as agent memory** — log every signal decision with context. Agent recalls prior decisions to avoid repeating known-bad patterns. **GRID already has this:** decision_journal (append-only) + postmortem.py (failure categorization). The gap: the LLM doesn't currently READ the journal before making new predictions. Wire journal retrieval into Oracle's signal assembly step.

**Subagent parallel research** — spawn 4 agents simultaneously (fundamental, technical, sentiment, macro), synthesize in parent. **Maps directly to TradingAgents architecture** now available via pip install.

### L. RD-Agent (Microsoft) — 2x Returns, 70% Fewer Factors

**URL for full extraction:** https://saulius.io/blog/automated-quant-research-ai-agents-rdagent-2x-returns

**Headline finding:** Aggressive factor pruning with quality filtering produces better results than adding more factors. 2x returns with 70% fewer factors.

**GRID relevance (CRITICAL):** GRID has 1,238 registered features. If the RD-Agent finding holds, the optimal strategy is NOT "add more features to the DSL" but "ruthlessly prune to the ~370 that actually matter." This directly contradicts the instinct to maximize feature count.

**Action:** Fetch this post in next session. The factor pruning methodology could reshape GRID's entire feature engineering strategy. Feature importance tracking exists (features/importance.py) but may not be aggressive enough in culling.

### M. Revised Tactical Priority Ranking

Updated with FinRL-X findings:

| # | Action | New Code | Expected Impact | Timeline |
|---|--------|----------|----------------|----------|
| **1** | **Clone FinRL-X Adaptive Rotation** | ~300 lines | **Only system with verified live results.** Sharpe 1.96 live. | Day 1-2 |
| **2** | **False Discovery Gauntlet** | ~400 lines | Statistical honesty for everything | Day 1-2 |
| **3** | **Val net Sharpe > 1.4 filter** | ~20 lines | Go/no-go threshold | Day 1 |
| **4** | **Vol Regime Adaptive signal** | ~100 lines | Sharpe 1.72 OOS | Day 2 |
| **5** | **VIX-as-MA-scalar exposure** | ~50 lines | Continuous regime dial | Day 2 |
| **6** | **Vol-Price Divergence Contrarian** | ~80 lines | Sharpe 1.03, best MaxDD | Day 3 |
| **7** | **Trend-Volume Gate** | ~60 lines | Sharpe 1.11 | Day 3 |
| **8** | **pip install TradingAgents** + configure with Ollama | ~50 lines config | Multi-agent debate for free | Day 3-4 |
| **9** | **Wire decision_journal into Oracle** | ~100 lines | LLM reads past failures before predicting | Day 4 |
| **10** | **HEARTBEAT.md Hermes job** | ~150 lines | Autonomous monitoring | Day 4-5 |
| **11** | **LightGBM signal ensemble** | ~200 lines | 23.2% OOS return | Day 5 |
| **12** | **Fetch RD-Agent post + feature pruning** | Research | Could reshape feature strategy | Next session |
| **13** | **Full DSL engine** | ~800 lines | Enables evolutionary loop | Week 2 |
| **14** | **Multi-island evolutionary loop** | ~600 lines | Autonomous alpha generation | Week 2-3 |
| **15** | **CPPO-style risk objective for CRISIS regime** | ~200 lines | Tail risk protection | Week 3 |

### N. RD-Agent-Quant (Microsoft, NeurIPS 2025) — THE Architecture

**This is the most important finding.** Published NeurIPS 2025, openly available, directly answers "70% fewer factors, 2x returns."

**What it is:** Full-stack automated quant R&D pipeline that jointly optimizes factors AND models in a closed loop. Uses multi-armed bandit to decide whether next step improves factor set or model architecture.

**Architecture — 5 units across 2 phases:**
```
RESEARCH PHASE:
  Specification Unit → generates goal-aligned prompts from optimization targets
  Synthesis Unit → "knowledge forest" from prior outcomes → new factor/model hypotheses

DEVELOPMENT PHASE:
  Implementation Unit (Co-STEER) → chain-of-thought code gen with graph-based knowledge store
  Validation Unit → real-market backtests

ANALYSIS UNIT:
  Evaluates across 8 metrics: IC, ICIR, Rank IC, Rank ICIR, ARR, IR, -MDD, SR
  Multi-armed bandit (Thompson sampling): next step = factor or model?
  Bayesian linear model with Gaussian posteriors per action
```

**Results (OOS, CSI 500 + NASDAQ 100, 2024–June 2025):**
- Up to **2x higher annualized returns** than Alpha101
- Using **70% fewer factors**
- Outperforms deep time-series models under smaller compute budgets
- Top-ranked on both Chinese AND US markets

**Co-STEER (the key mechanism):** Graph-based knowledge store of ALL prior hypotheses, implementations, and results. Each new generation is conditioned on this accumulated context. **Prevents re-exploring dead ends.** This is what GRID's evolutionary loop needs that QuantaAlpha doesn't have.

**Bandit scheduler:** Ablation study shows bandit > LLM-based direction selection > random. The bandit achieves highest IC, ARR, and SOTA selection rate under fixed compute budget.

**Install:** `pip install rdagent` then `rdagent fin_quant`

**GRID additions needed:**
1. **Bandit scheduler** — Thompson sampling deciding factor vs model optimization at each loop iteration
2. **Co-STEER knowledge graph** — PostgreSQL-backed graph of all prior factor hypotheses + results, queried before each new generation
3. Both are implementable on GRID's existing PostgreSQL backend

### O. Factor Zoo Compression (JPM 2024 Best Paper + ML Factor Zoo)

**153 US equity factors → 15 that span the zoo** (Swade, Hanauer, Lohre, Blitz)

Key findings:
- 15 factors from **8 of 13 factor style clusters** — diversity > quantity
- The 10-20 optimal factors are NOT fixed — which representatives matter changes over time
- Factor rotation is continuous, not static

**From ML Factor Zoo (ScienceDirect 2024):**
- Only **two alternating subsets of 3-4 characteristics** dominate ML portfolio returns
- Timing aligns with the **US credit cycle**:
  - **Credit contraction** → arbitrage constraint chars: Ivol, max effect, min effect
  - **Credit expansion** → financial constraint chars: cash flow risk, external financing growth, gross profitability

**GRID mapping (CRITICAL):** GRID has FRED credit spreads + M2. Credit cycle state is directly computable from existing macro features. This gives a data-driven rotation between signal families — confirming what QuantConnect Alpha Streams v2 said: small regime-conditional factors, not monolithic strategies.

### P. Revised Final Priority Stack

Everything from all sessions. Ordered by (alpha x deployment speed / new infra required):

**TIER 1 — Build This Week (zero new data needed):**

| # | Action | Why | Evidence |
|---|--------|-----|----------|
| 1 | **FinRL-X Adaptive Rotation regime logic** | Only live-verified result in entire sweep | Sharpe 1.96, 5 months paper trading |
| 2 | **Vol Regime Adaptive Momentum** | Highest OOS Sharpe from backtests | Sharpe 1.72, 38.7% return |
| 3 | **Val net Sharpe > 1.4 filter** | Replace IC as primary metric | R²=0.42 predicting OOS (vs IC R²=0.00) |
| 4 | **Trend + Volume Gate** | Clean signal, low false positive | Sharpe 1.11 |
| 5 | **Dual Horizon Momentum** | Highest RankIC of all 20 factors | RankIC 0.024 |

**TIER 2 — Build This Month (minor new infra):**

| # | Action | Why | Evidence |
|---|--------|-----|----------|
| 6 | **Credit cycle detector** | Data-driven signal family rotation | ML Factor Zoo: 2 alternating subsets tied to credit cycle |
| 7 | **False Discovery Gauntlet** | Statistical honesty for everything | QuantaAlpha: 0/7 ROBUST without it |
| 8 | **VIX-above-MA continuous exposure scalar** | Replace binary regime switch | QuantConnect postmortem: only regime method that worked |
| 9 | **LightGBM ensemble** on top factor features | Combines orthogonal signals | 23.2% OOS net return |
| 10 | **TradingAgents via Ollama** | Multi-agent debate for free | `pip install tradingagents`, llm_provider="ollama" |
| 11 | **Wire decision_journal into Oracle** | LLM reads past failures before predicting | OpenClaw pattern |
| 12 | **HEARTBEAT.md Hermes job** | Autonomous 30-min monitoring | OpenClaw operational pattern |

**TIER 3 — Strategic (requires planning):**

| # | Action | Why | Evidence |
|---|--------|-----|----------|
| 13 | **RD-Agent bandit scheduler** | Most structurally novel addition | NeurIPS 2025: bandit > LLM > random for compute allocation |
| 14 | **Co-STEER knowledge graph** | Prevents redundant factor exploration | RD-Agent: 70% fewer factors, 2x returns |
| 15 | **Multi-island evolutionary loop** | Diverse factor discovery | QuantaAlpha: crossover products dominate top 3 |
| 16 | **CPPO risk-sensitive RL for CRISIS** | Tail risk protection | FinRL-DeepSeek: CVaR objective for bear markets |
| 17 | **Factor pruning** from 1,238 → ~370 | Quality over quantity | Factor Zoo: 15 factors span 153; RD-Agent: 70% fewer = 2x returns |

**ANTI-PATTERNS (verified failures — do NOT build):**
- All-weather alphas (QuantConnect: top 5% underperformed SPY)
- HMM bear detection (too slow)
- High validation IC as optimization target (R²=0.00 to OOS)
- Single-agent LLM trading (TradingAgents: debate consistently wins)
- Standalone volume-weighted momentum (OOS Sharpe -1.24)
- Phantom portfolios without explicit state injection (TradeTrap)
- More factors without pruning (Factor Zoo + RD-Agent: fewer = better)

---

## ADDENDUM 3: Codebase Reality Check (3-Agent Verification)

Three agents independently verified the plan against actual GRID code. **5 critical issues, 6 high issues** found. All must be addressed before implementation.

### CRITICAL FIXES (Blocking — Must Resolve First)

#### FIX-C1: LightGBM Not Installed

`requirements.txt` has scikit-learn, scipy, xgboost — but **no lightgbm**. It's a hard dependency for the training pipeline.

**Fix:** `pip install lightgbm>=4.0` and add to `requirements.txt`. CPU-only is fine (LightGBM trains in seconds on GRID's data volume).

#### FIX-C2: GRID-Unique DSL Variables Don't Exist as PIT-Correct Time Series

The plan's key differentiator (`$congressional_flow`, `$insider_flow`, `$darkpool_flow`, `$gex`, `$trust_convergence`) **do not exist in `feature_registry` or `resolved_series`**. The data lives in:

| Variable | Actual Location | Format | PIT-Ready? |
|----------|----------------|--------|------------|
| `$congressional_flow` | `raw_series` (CONGRESS:...) + `signal_sources` table | Raw signals, not time series | NO |
| `$insider_flow` | `raw_series` (INSIDER:...) + cluster detection | Raw signals | NO |
| `$darkpool_flow` | `raw_series` (DARKPOOL:...:volume/trades) | Weekly aggregates, 2-week lag | NO |
| `$gex` | `physics/dealer_gamma.py` — computed on-the-fly | Dict per ticker, not stored | NO |
| `$fed_rate` | `resolved_series` via FRED puller | Proper time series | YES |
| `$sentiment` | Multiple modules, no composite | Fragmented | NO |
| `$trust_convergence` | `trust_scorer.detect_convergence()` — function call | Not stored | NO |

**Fix:** Add `alpha_research/data/materializer.py` — a pre-computation step that materializes these signals into a `alpha_signals_materialized` table (or into `resolved_series` with an `alpha_` prefix). This runs BEFORE DSL compilation, not during. Estimated 200-300 lines. **This is Phase 0 — must come before DSL engine.**

For the tactical items (Day 1-3 signals), skip the DSL entirely and hand-code in Python using the actual data interfaces:
- Congressional: `SignalRegistry.query_by_source("congressional_trading", engine)`
- GEX: `DealerGammaEngine(engine).compute_gex_profile(ticker)`
- Trust: `TrustScorer.get_trusted_sources(engine)`

#### FIX-C3: `market_daily` Table May Not Exist

`Alpha101Engine._load_ohlcv()` queries `market_daily` — but **this table is NOT in `schema.sql`**. It may exist from a migration not captured in schema.sql, or Alpha101 may be dead code.

**Fix:** Verify against live DB: `SELECT 1 FROM information_schema.tables WHERE table_name = 'market_daily'`. If it doesn't exist, the DSL's OHLCV operators need to source data differently — likely from `resolved_series` where yfinance stores OHLCV data, or from a new materialized view.

#### FIX-C4: No Database Schema for Alpha Research Persistence

The plan creates API endpoints but never defines the backing tables.

**Fix:** Add to `schema.sql` (or migration):
```sql
CREATE TABLE alpha_evolution_runs (
    id              TEXT PRIMARY KEY,
    direction       TEXT NOT NULL,
    config          JSONB NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    best_individual TEXT,
    status          TEXT DEFAULT 'running',
    generations     INTEGER DEFAULT 0,
    population_size INTEGER
);

CREATE TABLE alpha_individuals (
    id              TEXT PRIMARY KEY,
    run_id          TEXT REFERENCES alpha_evolution_runs(id),
    generation      INTEGER NOT NULL,
    parent_ids      TEXT[],
    origin          TEXT NOT NULL,  -- 'seed', 'mutation', 'crossover'
    factors         JSONB NOT NULL, -- 18 DSL expressions
    metrics         JSONB,          -- RankIC, Sharpe, etc.
    feature_importances JSONB,
    verdict         TEXT,           -- ROBUST, MARGINAL, UNSTABLE, NULL
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE alpha_gauntlet_results (
    id              SERIAL PRIMARY KEY,
    individual_id   TEXT REFERENCES alpha_individuals(id),
    test_name       TEXT NOT NULL,  -- permutation, deflated_sharpe, etc.
    passed          BOOLEAN NOT NULL,
    detail          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alpha_ind_run ON alpha_individuals (run_id, generation);
CREATE INDEX idx_alpha_gauntlet ON alpha_gauntlet_results (individual_id);
```

#### FIX-C5: Oracle Signal Registry Path Is Gated Behind Env Var

`oracle/engine.py` line 41: `_USE_SIGNAL_REGISTRY = os.getenv("GRID_SIGNAL_REGISTRY", "0") == "1"`. Default is OFF.

**Fix:**
1. Set `GRID_SIGNAL_REGISTRY=1` in `.env`
2. Create `alpha_research/adapters/oracle_adapter.py` that converts gauntlet-passing factors into `RegisteredSignal` objects with proper `source_module="alpha_research"`, `signal_type`, `direction`, `confidence`, `valid_from`/`valid_until`
3. Oracle models must subscribe to the `alpha_research` source_module

### HIGH FIXES (Should Resolve)

#### FIX-H1: Regime States Are Rule-Based, Not GMM

GROWTH/NEUTRAL/FRAGILE/CRISIS labels come from `scripts/auto_regime.py` (rule-based stress index thresholds), NOT from GMM clustering in `discovery/clustering.py`. GMM produces numbered clusters (0, 1, 2...) with no semantic labels.

**Fix:** The regime-signal routing table in Addendum A should reference `auto_regime.py` output, not GMM clusters. The interface is `(regime_label: str, confidence: float)`. Verify: `SELECT DISTINCT regime FROM analytical_snapshots WHERE snapshot_type = 'regime'`.

#### FIX-H2: Existing Backtest Engine Cannot Be Reused for Gauntlet

`validation/backtest.py` (`WalkForwardBacktest`) has fundamental incompatibilities:
- Requires `hypothesis_id` (needs hypothesis_registry entry)
- No purged cross-validation (uses simple non-overlapping eras)
- No RankIC metric (only Sharpe, return, max drawdown)
- No multi-instrument evaluation (single feature matrix)

**Fix:** Tactical item #2 ("False Discovery Gauntlet on existing backtest infra") is misleading. The gauntlet needs its own backtester in `alpha_research/training/walk_forward.py`. The existing one is for model promotion, not factor validation. They serve different purposes.

#### FIX-H3: PIT Store Returns Feature-Indexed Data, Not Ticker-Panel Data

DSL operates on multi-ticker panel data (dates x tickers). But `PITStore.get_feature_matrix()` returns `(obs_date, feature_id)` — one time series per feature, NOT per ticker. Different data shape.

Alpha101 solves this by querying `market_daily` directly (bypassing PIT store).

**Fix:** `alpha_research/data/panel_builder.py` — builds `{field: DataFrame[dates x tickers]}` panel data from:
- `resolved_series` where yfinance stores per-ticker OHLCV (series_id pattern: `YFINANCE:{ticker}:close`)
- Apply PIT constraints manually (release_date <= as_of_date)
- This is the actual data layer the DSL compiler operates on

#### FIX-H4: Hermes 15-Min Timeout Won't Work for Overnight Alpha Runs

`CYCLE_TIMEOUT_SECONDS = 900` (15 min). A full evolution run (128 individuals x 8 rounds) takes hours.

**Fix:** Alpha research runs as a separate `systemd` service (`grid-alpha-research`), triggered by Hermes but not contained within it. Similar pattern to how `grid-worker` runs alongside `grid-hermes`.

#### FIX-H5: LLM Config Says "hermes" Model, Not Qwen 32B

`config.py` shows `LLAMACPP_CHAT_MODEL = "hermes"`. Context window is 8192 tokens. Crossover prompts (two parents x 18 factors + metrics + importances) may exceed context.

**Fix:** Verify actual loaded model: `curl http://localhost:8080/v1/models`. Design prompts to fit within 6K tokens (leave 2K for response). For crossover, summarize parent factors (name + importance only) instead of full DSL + metrics dumps.

#### FIX-H6: feature_registry.family CHECK Constraint Excludes Alpha Research

The CHECK constraint limits families to: `rates, credit, equity, vol, fx, commodity, sentiment, macro, crypto, alternative, flows, systemic, trade, breadth, earnings`.

**Fix:** Migration: `ALTER TABLE feature_registry DROP CONSTRAINT ...; ALTER TABLE feature_registry ADD CONSTRAINT ... CHECK (family IN (..., 'alpha'));`

### MEDIUM FIXES

#### FIX-M1: Tactical Items 1-6 Are Hand-Coded Python, NOT DSL-Dependent

The tactical priority list says "Day 1-3" for individual signals, but the DSL engine is "Week 2". Clarification: **items 1-7 are standalone Python functions** using actual GRID interfaces directly. They do NOT require the DSL engine. The DSL is only needed for the evolutionary loop (items 13-14).

#### FIX-M2: No Error Handling for LLM-Generated Invalid DSL

**Fix:** Add to `dsl/validator.py`:
- Syntax errors → return structured error dict, don't raise
- Undefined variables → list available variables in error message
- Type mismatches → infer expected types from operator signatures
- LLM timeout → skip individual, log, continue evolution
- Training failure → skip individual with NaN fitness, log reason
- Insufficient data → minimum 60 observations required per factor, else skip

#### FIX-M3: No Checkpoint/Resume for Failed Evolution Runs

**Fix:** Serialize population state to `alpha_individuals` table at each generation boundary. On resume, query `SELECT MAX(generation) FROM alpha_individuals WHERE run_id = ?` and restart from there.

### Verified Integration Points (Confirmed Working)

These plan assumptions ARE correct:

| Assumption | Status | Evidence |
|------------|--------|----------|
| SignalRegistry accepts external registrations | **CONFIRMED** | `SignalRegistry.register(signals, engine)` — any module can call it |
| Signal format: direction, value, confidence, valid_from | **CONFIRMED** | `RegisteredSignal` dataclass with all required fields |
| Trust scorer returns scored sources with win rates | **CONFIRMED** | `SourceScore` dataclass with trust_score, hit_count, miss_count, avg_return_on_hits |
| Congressional data ingested with member/ticker/amount | **CONFIRMED** | `CongressionalTradingPuller` stores CONGRESS:{chamber}:{member}:{ticker}:{txn_type} |
| Dark pool data with volume spike detection | **CONFIRMED** | 2x average threshold, FINRA ATS data, SPY/QQQ/mega-caps tracked |
| GEX computed with gamma_flip, gamma_wall, vanna, charm | **CONFIRMED** | `DealerGammaEngine.compute_gex_profile()` returns full profile dict |
| LLM client with graceful degradation | **CONFIRMED** | `LlamaCppClient.chat()` returns None if server unavailable |
| Existing tests: 1,282+ across 80+ files | **CONFIRMED** | pytest suite, conftest.py fixtures |

---

## ADDENDUM 4: Deep Thread Pulls (Implementation-Ready Details)

### Thread 1: FinRL-X Adaptive Rotation — Full Implementation Spec

**Source:** `github.com/AI4Finance-Foundation/FinRL-Trading` (complete source code + config extracted)

#### Asset Groups (exact tickers from config v1.2.1)
```yaml
Growth/Tech (max 2): AAPL, MSFT, NVDA, META, AMZN, GOOGL, TSLA
Real Assets (max 2): XOM, CVX, COP, FCX, BHP, GLD, SLV
Defensive (max 2):   TLT, IEF, XLU, XLV, IAU, SHY, UUP
```

#### Regime Detection (exact parameters)
- **Slow regime**: 26-week trend MA on S&P500, three states: risk-on / neutral / risk-off
  - Controls group allocation caps and cash floors per state
- **Fast Risk-Off overlay**: Triggers on EITHER:
  - 3-day drawdown < **-3%** on S&P500 or QQQ
  - VIX Z-score > **3.0**
  - When triggered: **30% group cap, 50% cash floor, holds for 10 days**

#### Intra-Group Ranking
- 12-week risk-adjusted returns
- Robust Z-score method (capped at 20.0)
- Top 3 assets selected per group
- Exception rules:
  - Original M/K rule: Z-score >= 2.5 for 4 weeks minimum, twice
  - Strong signal: single trigger at Z-score >= 3.5 with 1.5x QQQ return comparison

#### Risk Controls (exact thresholds)
- **Absolute stop-loss: 5%**
- **Trailing stop-loss: 10%**
- **Cooldown after stop: 20 days** (blocks re-entry)

#### Portfolio Construction
- Max 2 active groups simultaneously
- Equal weighting within groups
- Fallback universe: `[SPY, QQQ, IAU, XLU, XLV]`

#### Rebalancing
- Weekly full rebalance (Friday)
- Daily monitoring for Fast Risk-Off + stop-loss only
- **Key design**: Daily monitoring does NOT trigger full rebalance — only adjusts existing positions

#### GRID Implementation Path
```python
# Map to GRID's existing infrastructure:
# - VIX data: FRED puller (already active)
# - S&P500/QQQ prices: yfinance puller (already active)
# - Regime state: auto_regime.py (add 26-week trend + VIX overlay)
# - Stop-loss: paper_engine.py already has trailing stop logic
# - Group rotation: flow_aggregator.py pattern (sector-level allocation)
# - Scheduling: Hermes weekly rebalance job + daily heartbeat check
```

**Key files to create:**
- `alpha_research/strategies/adaptive_rotation.py` — core engine (~300 lines)
- `alpha_research/strategies/config.py` — YAML-driven configuration
- `alpha_research/strategies/risk_manager.py` — stops + cooldown (~150 lines)

---

### Thread 2: RD-Agent-Quant — Full Algorithm Details (NeurIPS 2025)

**Source:** arxiv.org/abs/2505.15155 (full HTML extraction)

#### Thompson Sampling Bandit (Exact Formulation)

**State vector** (8-dimensional):
```
x_t = [IC, ICIR, RankIC, RankICIR, ARR, IR, -MDD, SR]^T ∈ R^8
```

**Action space**: A = {factor, model}

**Prior**: mu^(a) = 0, P^(a) = tau^(-2) * I

**Posterior update** (after observing reward r_t for action a_t):
```
P^(a_t) ← P^(a_t) + (1/sigma^2) * x_t * x_t^T
mu^(a_t) ← (P^(a_t))^(-1) * (P^(a_t) * mu^(a_t) + (1/sigma^2) * r_t * x_t)
```

**Selection**: Sample theta_tilde^(a) ~ N(mu^(a), (P^(a))^(-1)), choose a_t = argmax_a (theta_tilde^(a)^T * x_t)

**GRID implementation**: ~100 lines Python. Store posterior parameters in `alpha_evolution_runs.config` JSONB column. Update after each loop iteration.

#### Co-STEER Knowledge Graph

**Triple structure**: `(task, code, feedback)` stored persistently

**Retrieval**: `argmax_{c_k in K} similarity(t_new, t_k) * c_k` with threshold theta filtering

**On failure**: Increase task complexity `alpha_j ← alpha_j + delta`, recompute task ordering

**GRID implementation**: Store in `alpha_individuals` table (already in schema from FIX-C4). Add `knowledge_embedding` FLOAT[] column for similarity search. Use pgvector if available, else cosine similarity in Python.

#### Results Tables (Complete)

**CSI 300 (primary test, Jan 2017 - Aug 2020):**

| Method | IC | ICIR | RankIC | RankICIR | ARR | IR | MDD | CR |
|--------|------|------|--------|----------|------|------|-------|------|
| Alpha 101 | 0.0308 | — | — | — | 5.12% | — | — | — |
| Alpha 158 | 0.0341 | — | — | — | 5.70% | 0.85 | -7.71% | — |
| Alpha 360 | 0.0420 | — | — | — | 4.38% | — | — | — |
| GRU | 0.0315 | — | — | — | 3.44% | — | -10.17% | — |
| LSTM | 0.0318 | — | — | — | 3.81% | — | -12.07% | — |
| Transformer | 0.0317 | — | — | — | 2.93% | — | -9.87% | — |
| TRA | 0.0404 | — | — | — | 6.49% | 1.01 | -8.60% | — |
| **RD-Agent(Q) o3-mini** | **0.0532** | **0.4278** | **0.0495** | **0.4091** | **14.21%** | **1.74** | **-7.42%** | **1.92** |

**Key**: RD-Agent(Q) achieves 2.5x the ARR of Alpha 158 with 70% fewer factors (~24 vs 158)

**Ablation (bandit vs LLM vs random):**

| Scheduler | IC | ARR | MDD | Total Loops | Valid Loops | SOTA Selections |
|-----------|------|------|-------|-------------|-------------|-----------------|
| **Bandit** | **0.0532** | **14.21%** | **-7.42%** | 44 | 24 | 8 |
| LLM-based | 0.0476 | 10.09% | -7.94% | 33 | 20 | 5 |
| Random | 0.0445 | 8.97% | -10.04% | 33 | 19 | 7 |

All completed in 12 hours. **Cost: under $10 per full run.**

**Factor generation dynamics:**
- Start: Alpha 20 (20 factors), IC ~0.035
- After 10 loops: IC ~0.045 (28% gain)
- Final (loop 44): IC ~0.053 (51% gain)
- R&D-Factor achieves Alpha 158-level IC using only ~55 factors
- 8 of 36 trials selected into SOTA set, spanning 5 of 6 hypothesis clusters

**De-duplication**: IC_max >= 0.99 vs existing SOTA factors → marked redundant, excluded

#### Data Specifications

| Market | Train | Validation | Test |
|--------|-------|------------|------|
| CSI 300 | 2008-2014 | 2015-2016 | 2017-Aug 2020 |
| CSI 500 | 2008-2021 | 2022-2023 | 2024-Jun 2025 |
| NASDAQ 100 | 2008-2021 | 2022-2023 | 2024-Jun 2025 |

**Preprocessing**: Cross-sectional robust Z-score (MAD-based) + forward-fill imputation
**Loss**: MSE on next-day returns
**Trading**: Daily long-short based on predicted return rankings

---

### Thread 3: TradingAgents — Full Architecture

**Source:** github.com/TradingAgents-AI/TradingAgents + paper metadata

#### Agent Graph (LangGraph-based)

```
┌─────────────────────────────────────────────────┐
│ ANALYST TEAM (parallel execution)               │
│ ├─ Market Analyst: get_stock_data, get_indicators│
│ ├─ Social Analyst: get_news (social sentiment)  │
│ ├─ News Analyst: get_news, get_global_news,     │
│ │                get_insider_transactions        │
│ └─ Fundamentals: get_fundamentals, balance_sheet,│
│                  cashflow, income                │
└─────────────────────────────────────────────────┘
              ↓ (analyst reports)
┌─────────────────────────────────────────────────┐
│ DEBATE TEAM (sequential rounds)                 │
│ ├─ Bull Researcher: optimistic assessment       │
│ ├─ Bear Researcher: pessimistic assessment      │
│ └─ Investment Judge: arbitrates debate          │
│                                                 │
│ max_debate_rounds configurable                  │
│ Tracks: bull_history, bear_history,             │
│         judge_decision                          │
└─────────────────────────────────────────────────┘
              ↓ (investment thesis)
┌─────────────────────────────────────────────────┐
│ RISK MANAGEMENT TEAM (parallel perspectives)    │
│ ├─ Aggressive: higher risk tolerance            │
│ ├─ Conservative: lower risk tolerance           │
│ └─ Neutral: balanced perspective                │
│                                                 │
│ max_risk_discuss_rounds configurable             │
│ Tracks: aggressive_history, conservative_history,│
│         neutral_history                          │
└─────────────────────────────────────────────────┘
              ↓ (risk-adjusted thesis)
┌─────────────────────────────────────────────────┐
│ EXECUTION                                       │
│ ├─ Trader Agent: final buy/sell/hold decision   │
│ └─ Portfolio Manager: portfolio-level oversight  │
└─────────────────────────────────────────────────┘
```

#### Configuration for GRID (Ollama + local LLM)
```python
from tradingagents.graph.trading_graph import TradingAgentsGraph

config = {
    "llm_provider": "ollama",
    "deep_think_llm": "qwen2.5:32b",  # GRID's existing model
    "quick_think_llm": "qwen2.5:7b",   # lighter model for analysts
    "max_debate_rounds": 3,
    "max_risk_discuss_rounds": 2,
    "project_dir": "/data/grid_v4/grid_repo/alpha_research/tradingagents_cache",
}

ta = TradingAgentsGraph(debug=True, config=config)
_, decision = ta.propagate("NVDA", "2026-04-01")
```

#### Memory System
5 independent memory instances: Bull, Bear, Trader, Judge, Portfolio Manager
- Updated via `reflect_and_remember()` based on trade outcomes
- Maps directly to GRID's `decision_journal` + `postmortem.py`

#### GRID Integration
- Replace GRID's single-pass Oracle signal assembly with TradingAgents debate flow
- Feed GRID's analyst data (congressional, insider, GEX) as custom tools
- Use `decision_journal` entries as agent memory initialization
- Run as pre-Oracle step: debate produces thesis → Oracle scores it → prediction generated

---

### Thread 4: Factor Zoo — Credit Cycle Rotation Details

**ML Factor Zoo finding (ScienceDirect 2024):**

Two alternating factor subsets, each with 3-4 characteristics:

**During credit CONTRACTION** (spreads widening, M2 growth declining):
- Idiosyncratic volatility (Ivol) — Ang et al. 2006
- Maximum daily return (max effect) — Bali et al. 2011
- Minimum daily return (min effect) — Bali et al. 2011
- These are **arbitrage constraint** characteristics

**During credit EXPANSION** (spreads tightening, M2 growth increasing):
- Cash flow risk — Da & Warachka
- Growth in external financing — Bradshaw et al.
- Sale of common/preferred stock — Pontiff & Woodgate
- Gross profitability — Novy-Marx 2013
- These are **financial constraint** characteristics

**GRID implementation:**
```python
# Credit cycle state from GRID's existing FRED data:
# - ICE BofA US Corporate Index OAS (BAMLC0A0CM)
# - M2 Money Stock (M2SL)
# - Federal Funds Rate (FEDFUNDS)

def get_credit_cycle_state(pit_store, as_of_date):
    """Returns 'contraction' or 'expansion' based on credit spread trend."""
    spreads = pit_store.get_pit([SPREAD_FEATURE_ID], as_of_date)
    m2 = pit_store.get_pit([M2_FEATURE_ID], as_of_date)

    # 6-month trend of credit spreads
    spread_trend = spreads.pct_change(126).iloc[-1]  # 6 months
    m2_trend = m2.pct_change(126).iloc[-1]

    if spread_trend > 0 or m2_trend < 0:
        return "contraction"  # Use Ivol, max/min effect signals
    else:
        return "expansion"    # Use profitability, financing signals
```

This is the data-driven answer to regime-conditional factor rotation. Not VIX (lagging indicator of vol), not GMM (abstract clusters), but the **credit cycle** (leading indicator of which risk premia are available).

---

### Thread 5: Implementation Sequence (Connecting All Threads)

**Week 1 (Tier 1 — proven signals, zero new infra):**

Day 1-2: Adaptive Rotation
- Implement regime detection (26-week trend + VIX + 3-day shock)
- Implement group rotation (3 groups, max 2 active, IR-based selection)
- Implement risk controls (5% abs stop, 10% trailing, 20-day cooldown)
- Wire into paper_engine.py for immediate paper trading
- Backtest against GRID's existing yfinance data

Day 2-3: QuantaAlpha Signals
- Vol Regime Adaptive Momentum (hand-coded Python, not DSL)
- Trend + Volume Gate
- Dual Horizon Momentum
- Vol-Price Divergence Contrarian
- Each: ~60-100 lines, operates on existing yfinance OHLCV data

Day 3: Validation Infrastructure
- Val net Sharpe > 1.4 filter (20 lines, apply to all backtest outputs)
- Permutation test (100 lines — 1000 shuffles, p-value computation)
- Deflated Sharpe (50 lines — Bailey & Lopez de Prado adjustment)

**Week 2 (Tier 2 — minor new infra):**

Day 1: Credit cycle detector + factor rotation
Day 2: VIX/MA continuous exposure scalar
Day 3: LightGBM ensemble on top factor features (install lightgbm first)
Day 4: TradingAgents pip install + Ollama config + test run
Day 5: Wire decision_journal into Oracle pre-prediction step

**Week 3+ (Tier 3 — strategic):**

- RD-Agent bandit scheduler (Thompson sampling, ~100 lines)
- Co-STEER knowledge graph (extend alpha_individuals table)
- Full DSL engine + evolutionary loop
- Multi-island populations
- Factor pruning from 1,238 → optimal subset

---

### External Repos to Clone/Reference

| Repo | What to Take | License |
|------|-------------|---------|
| `AI4Finance-Foundation/FinRL-Trading` | Adaptive Rotation config + engine pattern | MIT |
| `TradingAgents-AI/TradingAgents` | Multi-agent debate framework (pip install) | Apache 2.0 |
| `microsoft/RD-Agent` | Bandit scheduler + Co-STEER pattern (pip install) | MIT |
| QuantaAlpha (saulius.io) | DSL operators + evolution pattern | Blog (no license) |

---

## SESSION_ID (for /ccg:execute use)
- CODEX_SESSION: N/A (local planning — no external model calls made)
- GEMINI_SESSION: N/A (local planning — no external model calls made)
