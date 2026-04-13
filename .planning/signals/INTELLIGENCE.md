---
title: GRID Catalog — 100 New Intelligence Modules
parent: CATALOG.md
---

# INTELLIGENCE — 100 new analytics / inference modules

See `CATALOG.md` for scoring rubric. `docs/MODULE_CATALOG.md` lists the ~46 existing intelligence modules — many proposals below are **EXTEND** operations on those, not net-new builds.

**Format:** `[Type | Tier | Status] · L/C · Cost · Coverage · Overlap`

---

## K. Inference architecture (#101-110)

**Highest leverage in the entire catalog.** These are pure-code upgrades to how GRID combines evidence. No new data required. Each multiplies the value of every other signal GRID consumes.

### 101. Horizon-conditional oracle + per-horizon calibration `[I · Tier A · EXTEND]`
Split `oracle/engine.py` to produce separate predictions at **5d / 30d / 90d / 365d** horizons. Each horizon gets its own calibration curve, feature weights, and Brier tracking. `oracle_predictions` schema gains `horizon`, `as_of_date`, `resolution_date`.
**Why ≥1%:** Features that are signal at 5d are noise at 365d. Current single-horizon pooling collapses them into a flat average. Expected Brier lift: **2-4% oracle-wide**. Single highest-leverage change.
**L/C:** N/A (architecture). **Cost:** L (schema migration + model retraining + calibration split) · **Coverage:** 100% (multiplicative)
**Location:** extend `oracle/engine.py` + `oracle/calibration.py` + migration to `oracle_predictions` table; new helper `oracle/horizon_config.py`
**Overlap:** extends existing oracle stack; leverages `timeseries_forecasts` table which already has a `horizon` field.

### 102. Unified catalyst calendar + catalyst-aware scoring `[I · Tier A · NEW]`
Single aggregator that ingests earnings, FOMC, CPI, NFP, PCE, ISM, PMI, FDA AdCom/PDUFA, Treasury auctions, options expiry, index rebalances, corporate actions. Each prediction gets a `catalyst_proximity_score` and the oracle reweights features based on the active catalyst type.
**Why ≥1%:** GRID currently fights itself near earnings (macro momentum says one thing, guidance revision says another). Reconciling via catalyst type earns ~1.5% on event-adjacent trades (~30% of volume).
**L/C:** N/A (architecture). **Cost:** M · **Coverage:** ~30%
**Location:** new `intelligence/catalyst_aggregator.py` + extend `oracle/engine.py` with `catalyst_aware_reweight()`
**Overlap:** consumes `ingestion/altdata/earnings_calendar.py` + FOMC/CPI/FDA pullers (to be built from PULLERS.md).

### 103. Shapley-value feature attribution per prediction `[I · Tier A · NEW]`
For every prediction the oracle produces, compute Shapley values decomposing the prediction into contributions from each feature family (flow / regime / options / cross-asset / news + new signals). Store in `oracle_predictions.shapley_attribution` JSONB column.
**Why ≥1%:** Enables **fragility-aware sizing**: when one feature dominates, size down. Also drives feature-pruning over time. Estimated meta-feature lift: ~1% via better sizing.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** new `intelligence/shapley_attribution.py` + migration to `oracle_predictions`
**Overlap:** none (zero SHAP references in codebase per session discoveries).

### 104. Ensemble disagreement as a meta-feature `[I · Tier A · NEW]`
Compute inter-model correlation + disagreement among the 5 oracle models (flow_momentum / regime_contrarian / options_flow / cross_asset / news_energy) per prediction. High disagreement = low confidence AND a vol-trade signal.
**Why ≥1%:** When models disagree, that's information. Low-disagreement predictions are higher-confidence; high-disagreement predictions signal volatility trades (long straddle), not directional trades. Estimated: ~1.5%.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** new `oracle/disagreement.py` (helper called from `oracle/engine.py`)
**Overlap:** none.

### 105. Market-implied probability comparator `[I · Tier A · NEW]`
For each GRID prediction, compute the market-implied probability from: options skew + IV term structure, yield curve, forward rates, Polymarket, Kalshi, sell-side mean targets. GRID's edge = |GRID_p − market_p|.
**Why ≥1%:** Any signal that doesn't move GRID's distribution **farther** from market consensus is not alpha. Without this, GRID has no adversarial benchmark. Estimated: **2% oracle-wide** because it kills false-positive signals that agree with the crowd.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** new `intelligence/market_implied_prob.py` + integration in `oracle/engine.py`
**Overlap:** consumes existing `ingestion/altdata/prediction_odds.py`, `kalshi.py`, `cboe_indices.py`.

### 106. Uncertainty quantification + confidence bounds per prediction `[I · Tier A · NEW]`
Every prediction output becomes `p = 0.65 ± 0.08` (not just `0.65`). Sizing uses the lower bound. Bounds come from: feature Shapley variance + historical bucket calibration width + ensemble spread.
**Why ≥1%:** Kelly sizing with confidence bounds beats point-estimate Kelly. Avoids catastrophic sizing on fragile predictions. Estimated: ~1% via better sizing, not better predictions.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** new `oracle/uncertainty.py` + schema field `oracle_predictions.confidence_lower_bound`
**Overlap:** consumes #103 (Shapley) and #104 (disagreement).

### 107. Per-horizon feature importance (extend features/importance.py) `[I · Tier A · EXTEND]`
`features/importance.py` already tracks permutation importance, regime correlation, and rolling stability. Extend with **per-horizon** buckets (5d/30d/90d/365d) and **per-regime** buckets (growth/neutral/fragile/crisis/recovery).
**Why ≥1%:** Current global feature importance is a weighted average across incompatible horizons. Per-horizon separation reveals which features to use where. Estimated: enables #101.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** extend `features/importance.py` with `HorizonImportanceTracker` class
**Overlap:** extends existing `features/importance.py`.

### 108. Calibration persistence + drift alerts `[I · Tier A · EXTEND]`
`oracle/calibration.py` currently computes on-demand and never stores. Persist `calibration_reports` as a time series; alert when calibration drifts beyond 2σ for any horizon × confidence bucket. Extends existing `intelligence/prediction_calibration.py`.
**Why ≥1%:** Calibration drift is silent failure. When GRID says p=0.7 but the actual hit rate is 0.55, sizing is wrong. Detection prevents fragile periods from compounding.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** extend `oracle/calibration.py` to persist + `intelligence/prediction_calibration.py` with drift alerts
**Overlap:** extends `oracle/calibration.py` AND `intelligence/prediction_calibration.py`.

### 109. Per-regime submodel routing (5 sub-oracles, soft handoff) `[I · Tier A · NEW]`
Instead of one oracle with dynamic weights, train **5 sub-oracles** — one per regime (growth / neutral / fragile / crisis / recovery). Route each prediction to the active regime's oracle with a soft blend across regime probabilities (no hard switch).
**Why ≥1%:** Regime-specific models outperform universal models on regime-conditional metrics. Estimated: **1.5-3%** via better specialization.
**L/C:** N/A. **Cost:** L · **Coverage:** 100%
**Location:** new `oracle/regime_router.py` + retrain existing 5 models per regime; consumes `discovery/clustering.py` regime labels
**Overlap:** depends on existing `discovery/clustering.py`.

### 110. Kelly-with-error-bars + tail-adjusted sizing `[I · Tier A · NEW]`
Replace current Kelly fraction with: Kelly(lower_bound_p, payoff) × tail_adjustment_factor. Tail factor drawn from historical GFC-analog / dotcom-analog drawdown distribution for the active regime.
**Why ≥1%:** Kelly with point estimates over-sizes on fragile predictions. With lower bounds + tail caps, P&L variance falls without reducing expected return. Sharpe improvement ~0.2-0.3.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** extend `trading/options_recommender.py` Kelly computation (line 460 per session discoveries) + new `trading/kelly_uncertainty.py`
**Overlap:** extends `trading/options_recommender.py`.

---

## L. Causality + lead-lag (#111-120)

Granger, transfer entropy, mutual information, do-calculus. GRID has `backtest_scanner.py` (cross-asset lead/lag scanner) as partial foundation — several entries below **EXTEND** it.

### 111. Transfer entropy discovery engine (unsupervised lead-lag) `[I · Tier A · EXTEND]`
Unsupervised scan computing transfer entropy across all feature pairs (and feature → return pairs). Surfaces lead-lag relationships GRID isn't currently exploiting. Extends `analysis/backtest_scanner.py` which already does cross-asset lead/lag.
**Why ≥1%:** Auto-discovers edges in GRID's existing ~500 features. Force-multiplier across the entire stack. Estimated: **2% oracle-wide**.
**L/C:** N/A. **Cost:** L · **Coverage:** 100%
**Location:** extend `analysis/backtest_scanner.py` with transfer-entropy computation; new `analysis/lead_lag_discovery.py` for the full pipeline
**Overlap:** extends `analysis/backtest_scanner.py`.

### 112. Granger causality matrix (daily, rolling 252d) `[I · Tier B · NEW]`
Rolling Granger causality tests across feature families + persistent ranking of top-N Granger leaders per target. Feeds a "which signals are currently leading which" dashboard.
**Why ≥1%:** Granger is weaker than transfer entropy but cheaper and more interpretable. Combined, they cross-validate.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** new `analysis/granger_matrix.py`
**Overlap:** complements #111.

### 113. Mutual information feature selection + redundancy pruning `[I · Tier B · NEW]`
Use mutual information to rank feature value by bits-of-information-reduction about horizon-specific returns. Prune highly redundant features below a threshold.
**Why ≥1%:** Most "alpha" in ML feature-bag oracles is redundant. MI-based pruning reduces variance without reducing signal.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** new `analysis/mutual_info_selection.py` + extends `features/importance.py`
**Overlap:** extends `features/importance.py`.

### 114. Causal DAG encoding + do-calculus query engine `[I · Tier B · NEW]`
Explicit DAG encoding GRID's believed causal structure: Fed → rates → credit → equity; China → commodities → EM; etc. Do-calculus lets you reason about interventions ("if Fed cuts, what's P(equity up)?").
**Why ≥1%:** Causal inference is structurally different from observational prediction. Enables counterfactual reasoning and adversarial robustness.
**L/C:** N/A. **Cost:** L · **Coverage:** ~30% (interventional use cases)
**Location:** new `intelligence/causal_dag.py` (could extend `intelligence/influence_network.py` Crown Jewel)
**Overlap:** partial with existing `intelligence/influence_network.py` (influence loops + leverage points).

### 115. Cross-asset lead-lag backtest framework (extend backtest_scanner) `[I · Tier A · EXTEND]`
`analysis/backtest_scanner.py` exists as a "cross-asset lead/lag scanner." Extend it with proper walk-forward validation, PIT enforcement via `store/pit.py`, and lead-lag half-life estimation.
**Why ≥1%:** The scanner already exists but may not use PIT walk-forward. Hardening it enables trustworthy discovery.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** extend `analysis/backtest_scanner.py` with PIT walk-forward + half-life tracker
**Overlap:** extends existing.

### 116. Feature half-life / temperature tracker `[I · Tier B · NEW]`
Per-feature half-life estimator: how many days does a signal stay actionable after it fires? Features with decayed recent performance get downweighted automatically ("cold" features).
**Why ≥1%:** Feature decay is silent. Without tracking, GRID rides stale signals. Auto-downweight prevents the "once-worked" trap.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** new `features/temperature.py` + extends `features/importance.py`
**Overlap:** extends `features/importance.py`; partial with `intelligence/trust_scorer.py` (actor-level decay, not feature-level).

### 117. Synthetic control event-study engine `[I · Tier B · NEW]`
For any event (Fed decision, earnings, election, M&A), construct a weighted basket of non-affected assets that matches the pre-event price path. Difference after event = causal effect. Core econometric method for causal inference in observational data.
**Why ≥1%:** Replaces "event study" (correlational) with causal impact. Better training signal for event-conditional models.
**L/C:** N/A. **Cost:** M · **Coverage:** ~20% (event-driven)
**Location:** new `analysis/synthetic_control.py`
**Overlap:** none.

### 118. Information cascade classifier (narrative lifecycle stages) `[I · Tier C · NEW]`
Classify each identified narrative ("AI capex", "soft landing", "China reopen") into lifecycle stages: **forming → accelerating → peak → decaying → dead**. Each stage has different optimal positioning.
**Why ≥1%:** Narratives drive ~30% of multi-week moves. Timing entry/exit relative to narrative stage is valuable.
**L/C:** Condition — crowd dynamics. **Cost:** M · **Coverage:** ~15%
**Location:** new `intelligence/narrative_lifecycle.py` (extends `intelligence/news_intel.py` and `intelligence/market_diary.py`)
**Overlap:** partial with `intelligence/news_intel.py`.

### 119. Reflexivity modeling for published predictions `[I · Tier C · NEW]`
When a GRID prediction is acted upon (internal or, hypothetically, external publication), it changes the outcome. Model the feedback loop: prediction → position → price → realized outcome.
**Why ≥1%:** For small-caps and illiquid names, reflexivity is real. For macro trades it's negligible. Scoped to size-aware application.
**L/C:** Condition — feedback. **Cost:** M · **Coverage:** ~5% (small-cap + illiquid only)
**Location:** new `oracle/reflexivity.py` (impact-scaled confidence adjustment)
**Overlap:** none.

### 120. Proof-of-work predictions (multi-path derivation requirement) `[I · Tier B · NEW]`
Every prediction must be derivable from at least 3 independent paths through the evidence graph. Single-path predictions get automatic confidence downgrade. Multi-path = stronger consensus.
**Why ≥1%:** Forces explicit orthogonality check. Kills single-feature overfits.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** new `oracle/derivation_paths.py` + extends `intelligence/trust_scorer.py` convergence detection
**Overlap:** extends `intelligence/trust_scorer.py` (convergence detection).

---

## M. Regime + state classifiers (#121-130)

HMM transition probabilities, liquidity regime, recession nowcast, financial conditions. Extends `discovery/clustering.py` (regime labels, no transitions) and plugs into `intelligence/global_levers.py` (hierarchical world money flow model already exists).

### 121. HMM regime transition matrix `[I · Tier A · EXTEND]`
`discovery/clustering.py` currently treats regimes as static labels with no transition model. Add a Hidden Markov Model that tracks `P(regime_next | regime_now, macro_state)` with stickiness priors.
**Why ≥1%:** Regime transitions are the highest-leverage events in a multi-regime oracle. Probabilistic transitions let you anticipate them instead of reacting.
**L/C:** N/A. **Cost:** M · **Coverage:** 100% (conditions everything)
**Location:** extend `discovery/clustering.py` with `RegimeHMM` class; new `discovery/regime_transitions.py` for the persistence + alerting layer
**Overlap:** extends existing `discovery/clustering.py`.

### 122. Liquidity regime classifier (5-state) `[I · Tier A · NEW]`
5-state classifier over Fed balance sheet components + primary dealer positions + RRP + SOFR dispersion + FX basis + bank reserves: **gushing / ample / neutral / tightening / stressed**. Conditions every other prediction.
**Why ≥1%:** Liquidity regime is multiplicative across all prediction confidence. A tightening regime downweights risk-on signals; a gushing regime upweights them. Estimated: **2-3% oracle-wide**.
**L/C:** N/A (meta-state). **Cost:** M · **Coverage:** 100%
**Location:** new `intelligence/liquidity_regime.py`; consumes Pullers #21-30 + plugs into `intelligence/global_levers.py`
**Overlap:** plugs into existing `intelligence/global_levers.py` (hierarchical world money flow model).

### 123. Recession nowcast ensemble `[I · Tier A · NEW]`
Ensemble of: Sahm rule + yield curve (2s10s + 3m10y) + HY spreads + jobless claims + ISM + consumer confidence + building permits → unified P(recession next 6/12 months). Updates daily.
**Why ≥1%:** GRID has the components but no ensemble. Recession probability is a direct cyclical-vs-defensive rotation signal at LEAPS horizon.
**L/C:** Condition — macro state. **Cost:** M · **Coverage:** ~20% (LEAPS + cyclical rotation)
**Location:** new `intelligence/recession_nowcast.py`
**Overlap:** none.

### 124. Financial conditions index (multi-factor FCI) `[I · Tier A · NEW]`
Multi-factor composite: rates + credit spreads + equity vol + FX + housing + leverage. Rolling z-score. Leading indicator for growth impulse and Fed posture.
**Why ≥1%:** FCI is a clean single-number summary of the plumbing state. Moves 4-8 weeks ahead of headline economic data.
**L/C:** Condition — macro state. **Cost:** S · **Coverage:** ~50%
**Location:** new `intelligence/financial_conditions.py`
**Overlap:** none; complements #122.

### 125. Inflation regime classifier (demand-driven vs supply-driven) `[I · Tier B · NEW]`
Decompose inflation into demand-driven (wage growth, core services ex-housing) vs supply-driven (commodities, container rates, goods). Different regimes require different trades.
**Why ≥1%:** Demand-driven inflation is bad for equities (Fed hikes); supply-driven inflation is bad for bonds but mixed for equities. Classification changes the trade.
**L/C:** Condition — inflation type. **Cost:** M · **Coverage:** ~15%
**Location:** new `intelligence/inflation_regime.py`
**Overlap:** none.

### 126. Credit cycle phase classifier `[I · Tier B · NEW]`
Classify credit cycle into 4 phases: **repair / recovery / expansion / downturn**. Uses HY spreads, default rates, lending standards (SLOOS), and primary issuance.
**Why ≥1%:** Credit phase is the dominant factor for LEAPS trades on financials, credit-sensitive equities, and real estate.
**L/C:** Condition — credit state. **Cost:** M · **Coverage:** ~15%
**Location:** new `intelligence/credit_cycle.py`
**Overlap:** none.

### 127. Dollar regime tracker (DXY × real yields × risk) `[I · Tier B · NEW]`
3-factor classifier over DXY level, US real yields, and risk sentiment: **dollar smile** position (strong US growth / global risk-off / weak USD). Determines FX + commodity + EM exposure.
**Why ≥1%:** Dollar smile position is the right mental model for cross-asset trades. GRID probably doesn't have it explicit.
**L/C:** Condition — FX state. **Cost:** S · **Coverage:** ~12%
**Location:** new `intelligence/dollar_regime.py`
**Overlap:** none.

### 128. Global growth impulse (China + US + EU composite) `[I · Tier B · NEW]`
Composite index of US ISM + China electricity/rail/PMI + EU flash PMI + Korea exports. Single-number global growth nowcast.
**Why ≥1%:** Global growth impulse drives commodity super-cycles, EM, cyclicals, and industrials. 12-month smoothed version is an excellent LEAPS input.
**L/C:** Condition — global cycle. **Cost:** M · **Coverage:** ~20%
**Location:** new `intelligence/global_growth_impulse.py`; consumes Pullers #2 (China Li Keqiang), #5 (Korea exports), #14 (Eurostat flash), #15 (German IFO)
**Overlap:** none.

### 129. Monetary vs fiscal impulse tracker `[I · Tier B · NEW]`
Separate fiscal impulse (deficit × velocity × multiplier) from monetary impulse (shadow rate × balance sheet). When they diverge, asset allocation conclusions diverge.
**Why ≥1%:** The current environment has tight monetary + loose fiscal — a rare combination that changes equity/bond correlations.
**L/C:** Lever — Fed + Treasury actors. **Cost:** M · **Coverage:** ~10%
**Location:** new `intelligence/monetary_fiscal_impulse.py`
**Overlap:** none.

### 130. Risk-on/off state machine (continuous + discrete) `[I · Tier B · NEW]`
Continuous risk-on/off score from: VIX term structure + credit spreads + defensive-vs-cyclical ratio + gold + USD. Plus a discrete state machine with hysteresis to avoid whipsaws.
**Why ≥1%:** Risk-on/off is the cleanest meta-state for tactical trades. Discrete state with hysteresis prevents over-trading regime transitions.
**L/C:** Condition — meta-state. **Cost:** S · **Coverage:** ~15%
**Location:** new `intelligence/risk_on_off.py`
**Overlap:** complements #122, #124, #127.

---

## N. Positioning analytics (#131-140)

13F delta clustering, dealer gamma reconstruction, flow attribution, institutional rotation. Extends `intelligence/institutional_flows.py` and `intelligence/institutional_map.py`.

### 131. 13F delta clustering (500 funds, QoQ rotation detection) `[I · Tier A · EXTEND]`
Quarter-over-quarter 13F position delta clustered across 500 largest filers. Detects consensus rotations (e.g., "30 funds added NVDA, 15 dropped XOM") before they become sell-side narrative.
**Why ≥1%:** Surfaces crowded rotations 4-8 weeks before coverage catches up. Known technique, rarely systematized.
**L/C:** Lever — named fund actors. **Cost:** M · **Coverage:** ~12%
**Location:** new `intelligence/thirteen_f_delta.py` (extends existing `institutional_flows.py` + `institutional_map.py`)
**Overlap:** extends `institutional_map.py` (private credit, HFs, VCs, PE already mapped).

### 132. Dealer GEX + single-name gamma reconstruction (replace net-short assumption) `[I · Tier A · EXTEND]`
`physics/dealer_gamma.py` currently assumes dealers are net short every option (crude). Replace with flow-based inference: track customer aggression (lifts vs hits) per strike and infer dealer positioning from net customer flow.
**Why ≥1%:** Realistic dealer positioning is materially different from "net short everything." Changes gamma walls, pinning levels, and squeeze dynamics.
**L/C:** Lever — dealer hedge flow. **Cost:** L · **Coverage:** ~8% (single-name options)
**Location:** extend `physics/dealer_gamma.py` with `fetch_customer_aggression()` + `infer_dealer_position()`; wire vanna/charm into scoring
**Overlap:** extends existing `physics/dealer_gamma.py`.

### 133. Vanna + charm-to-scoring wiring (activate existing computation) `[I · Tier A · WIRE]`
`physics/dealer_gamma.py:248-250` computes vanna and charm but they are "measured but not actionable" (session discovery). Wire them into `discovery/options_scanner.py` as new signal dimensions.
**Why ≥1%:** Free alpha on the floor. Vanna especially matters for pinning + end-of-month flow.
**L/C:** Condition — dealer hedge flow. **Cost:** S · **Coverage:** ~8%
**Location:** wire `physics/dealer_gamma.py` vanna/charm output into `discovery/options_scanner.py` 7-signal detector (add as signals #8 and #9)
**Overlap:** wires existing unused computation.

### 134. Vol surface wiring (activate analysis/vol_surface.py) `[I · Tier A · WIRE]`
`analysis/vol_surface.py` has SVI parameterization, skew, butterfly arbitrage checks — NOT wired into scanner or recommender. Wire it in and add LEAPS-specific features (1Y ATM IV, long-dated skew, term structure slope, div risk premium, ρ).
**Why ≥1%:** LEAPS P&L is ~60% vega + 20% term structure. Currently invisible to the scanner. Estimated: **3-5% on LEAPS trades**.
**L/C:** Condition — volatility pricing. **Cost:** S-M · **Coverage:** ~20% (LEAPS + vol trades)
**Location:** wire `analysis/vol_surface.py` into `discovery/options_scanner.py` + `trading/options_recommender.py`; new helper `physics/leaps_surface.py` for long-dated features
**Overlap:** wires existing unused `analysis/vol_surface.py`.

### 135. ETF flow attribution (sector rotation via ETF flows) `[I · Tier B · EXTEND]`
Attribute sector rotation signals from SPDR (XLK/XLE/XLF/XLV/XLY/XLP/XLU/XLI/XLB/XLRE/XLC) + iShares sector ETF flows. Extends `institutional_flows.py`.
**Why ≥1%:** Sector rotation is 2-3% annualized edge in walk-forward. ETF flows reveal it near-real-time.
**L/C:** Lever — passive flow. **Cost:** M · **Coverage:** ~10%
**Location:** extend `ingestion/altdata/institutional_flows.py` analytics; new `intelligence/sector_rotation_etf.py`
**Overlap:** extends existing.

### 136. Smart money concentration detector (Druckenmiller/Tepper/Burry/Ackman/Einhorn) `[I · Tier B · NEW]`
Track 10-30 proven operators' disclosed positions + public commentary + documented trades. When 3+ overlap on a name, high-conviction signal.
**Why ≥1%:** Famous investors' track records ARE the signal. Overlap detection is higher-precision than any single operator.
**L/C:** Lever — named operator actors. **Cost:** M · **Coverage:** ~5%
**Location:** new `intelligence/smart_money_overlap.py` (extends `intelligence/actor_network.py`)
**Overlap:** extends existing `intelligence/actor_network.py` (475+ named actors).

### 137. Insider cluster detector (3+ C-suite within 30 days) `[I · Tier A · EXTEND]`
`ingestion/altdata/insider_filings.py` already has "cluster detection" per MODULE_CATALOG. Verify the logic is 3+ C-suite within 30 days (academic standard). Extend with sector-adjusted hit rates and false-positive filtering (quarterly automated buys).
**Why ≥1%:** Insider cluster buys have ~70% historical outperformance in academic samples. Rare but high-precision.
**L/C:** Lever — insider actors. **Cost:** S · **Coverage:** ~4%
**Location:** extend `ingestion/altdata/insider_filings.py` cluster logic + analytics in `intelligence/insider_cluster.py`
**Overlap:** extends existing `insider_filings.py` (already has some cluster detection per MODULE_CATALOG).

### 138. Short squeeze probability (short interest × borrow × float × call skew) `[I · Tier B · NEW]`
Composite short squeeze probability per name: short interest + borrow rate + float + call skew + rising call OI. Triggers tracked per name over time.
**Why ≥1%:** Squeeze events are ~20-50% single-name moves. Low base rate, high impact.
**L/C:** Condition — positioning imbalance. **Cost:** M · **Coverage:** ~3%
**Location:** new `intelligence/squeeze_detector.py`; consumes Puller #38 (short interest + borrow)
**Overlap:** none.

### 139. Cross-asset carry trade monitor (JPY / MXN / TRY / BRL / ZAR) `[I · Tier A · NEW]`
Track carry trade stress + unwind probability for major carry pairs. Composite of: vol expansion, skew flip, rate differential compression, correlation spike across EM currencies.
**Why ≥1%:** Carry unwinds are correlated global events (see August 2024 JPY). Early warning is worth ~3% on FX and risk trades when it triggers.
**L/C:** Condition — tail amplifier. **Cost:** M · **Coverage:** ~8%
**Location:** new `intelligence/carry_trade_monitor.py`
**Overlap:** none.

### 140. Primary dealer positioning analytics (extend Puller #24) `[I · Tier A · NEW]`
Analytics layer on top of Puller #24 (primary dealer Treasury positioning). Detect positioning extremes, short-squeeze setups in specific tenors, and dealer-forced flow around auctions.
**Why ≥1%:** Dealer extremes are mechanical — when they're short into an auction, the tail is reliable.
**L/C:** Lever — primary dealer actors. **Cost:** M · **Coverage:** ~15% (rates)
**Location:** new `intelligence/dealer_positioning.py`; consumes Puller #24 (`ingestion/altdata/primary_dealer.py`)
**Overlap:** none.

---

## O. Event + catalyst engines (#141-150)

Fed reaction function, earnings cascade predictor, post-announcement drift, catalyst-aware reweighting. Partial overlap with `intelligence/earnings_intel.py` and `intelligence/news_impact.py` — entries below EXTEND where possible.

### 141. Fed reaction function estimator `[I · Tier A · NEW]`
Bayesian model of the Fed's loss function derived from speeches + FOMC votes + dot plots + historical reactions to data surprises. Output: `P(hike | CPI surprise), P(cut | NFP surprise)`, etc.
**Why ≥1%:** Every Fed event becomes a GRID prediction opportunity. Estimated **3% on rates + risk trades around Fed meetings**.
**L/C:** Lever — FOMC members are named actors. **Cost:** L · **Coverage:** ~12%
**Location:** new `intelligence/fed_reaction_function.py`; consumes `ingestion/altdata/fed_speeches.py`
**Overlap:** extends `intelligence/legislative_intel.py` concept (existing — legislative trading detection).

### 142. Earnings surprise cascade predictor `[I · Tier A · EXTEND]`
Given a sector leader's guidance, predict which downstream tickers will revise and with what lag. Extends existing `intelligence/earnings_intel.py`.
**Why ≥1%:** Earnings cascades are predictable — NVDA guides → SMCI/ARM/AVGO follow. Timing the follow-through is worth 2% on earnings trades.
**L/C:** Lever — leader's guidance, follower's revision. **Cost:** M · **Coverage:** ~8% (earnings season)
**Location:** extend `intelligence/earnings_intel.py` with `cascade_predictor()` method
**Overlap:** extends existing `intelligence/earnings_intel.py`.

### 143. Post-announcement drift scanner by sector × market cap `[I · Tier A · NEW]`
Systematic catalog of (event_type × sector × market_cap × surprise_magnitude) → average drift + half-life. Purely actionable historical base rate.
**Why ≥1%:** Drift patterns are stable. Knowing "small-cap biotech beats drift +5% over 10 days" converts knowledge into sizing.
**L/C:** Condition — momentum after news. **Cost:** M · **Coverage:** ~10% (event-driven)
**Location:** new `intelligence/post_announcement_drift.py`
**Overlap:** partial with `intelligence/news_impact.py` (already tracks which news moves which markets).

### 144. Sell-side revision wave detector `[I · Tier A · NEW]`
Detect clusters of analyst revisions (3+ within 5 days, same direction) via `ingestion/altdata/analyst_ratings.py`. Wave detection beats single revisions.
**Why ≥1%:** Revision waves precede price moves with ~65% hit rate. Extending existing analyst data.
**L/C:** Lever — analyst actors. **Cost:** M · **Coverage:** ~8%
**Location:** new `intelligence/revision_wave.py`; consumes `ingestion/altdata/analyst_ratings.py`
**Overlap:** none; extends analyst data.

### 145. Earnings whisper + guidance tracker `[I · Tier B · NEW]`
Aggregate pre-announcement whisper numbers + management guidance revisions in the 30d window before earnings. Whisper vs street consensus gap is alpha.
**Why ≥1%:** Whisper numbers + guidance deviations predict surprise direction ~60% of the time.
**L/C:** Condition — pre-event positioning. **Cost:** M · **Coverage:** ~6%
**Location:** new `intelligence/earnings_whisper.py` (extends `intelligence/earnings_intel.py`)
**Overlap:** extends existing.

### 146. FDA AdCom + PDUFA date calendar + historical win rates `[I · Tier B · NEW]`
Forward calendar of FDA Advisory Committee meetings + PDUFA action dates with historical win rates by drug class, therapeutic area, and sponsor size. Biotech binary event alpha.
**Why ≥1%:** FDA binaries are 20-80% swings. Knowing base rates sizes the trade correctly.
**L/C:** Lever — FDA is named actor. **Cost:** M · **Coverage:** ~2% (but high precision)
**Location:** new `intelligence/fda_calendar.py` + `ingestion/altdata/fda_pdufa.py` for the data pull
**Overlap:** none.

### 147. Structured flow calendar aggregator (MSCI + Russell + expiry + quarter-end) `[I · Tier A · NEW]`
Unified forward calendar of all known mechanical flow events: MSCI/Russell rebalances, options expiry (monthly + quarterly), quarter-end rebalancing, tax-loss harvesting, MOMA index changes. Each event has historical impact magnitudes.
**Why ≥1%:** Mechanical flows are ~20% of short-to-medium horizon moves. Knowing the forward calendar turns them from noise into signal.
**L/C:** Lever — passive funds are collective actors. **Cost:** M · **Coverage:** ~10%
**Location:** new `intelligence/structured_flow_calendar.py`; consumes Puller #33 (index rebalance), Puller #40 (futures roll)
**Overlap:** none.

### 148. Options expiry pin strike forecaster `[I · Tier B · EXTEND]`
For each monthly expiry, forecast the pin strike for major indices (SPX/QQQ/IWM) and large single names using dealer GEX + OI distribution. Extends `physics/dealer_gamma.py`.
**Why ≥1%:** Pinning is a documented phenomenon worth 0.5-1% on expiry day. Pre-forecasting lets you position.
**L/C:** Lever — dealer hedge flow. **Cost:** M · **Coverage:** ~5%
**Location:** extend `physics/dealer_gamma.py` with `pin_strike_forecast()`
**Overlap:** extends existing.

### 149. Election + legislation calendar + outcome probability `[I · Tier B · EXTEND]`
Forward calendar of US elections, debates, and major legislation + market-implied outcome probabilities via Polymarket/Kalshi. Extends `intelligence/legislative_intel.py`.
**Why ≥1%:** Elections and legislation are binary tail risks that move sectors 5-20%. Systematized tracking is underbuilt.
**L/C:** Lever — elected/legislative actors. **Cost:** M · **Coverage:** ~5%
**Location:** extend `intelligence/legislative_intel.py` + consume `ingestion/altdata/prediction_odds.py` + `kalshi.py`
**Overlap:** extends existing `legislative_intel.py`.

### 150. Corporate action impact estimator (splits, dividends, buybacks, spinoffs) `[I · Tier C · NEW]`
Historical impact analysis for corporate actions: splits cause short-term outperformance (documented), spinoffs have persistent alpha, special dividends front-run, etc.
**Why ≥1%:** Corporate actions are systematic events with base rates. Spinoffs alone have 2-3% annualized excess return.
**L/C:** Lever — issuer actors. **Cost:** M · **Coverage:** ~4%
**Location:** new `intelligence/corporate_action_impact.py`
**Overlap:** none.

---

## P. NLP + narrative (#151-160)

Tone delta tracking, 10-K risk factor novelty, narrative lifecycle, analyst revision waves. EXTENDS `intelligence/earnings_transcript_analyzer.py` and `intelligence/news_intel.py`.

### 151. Earnings call tone shift detector (QoQ delta) `[I · Tier A · EXTEND]`
`intelligence/earnings_transcript_analyzer.py` already scores tone, Q&A split, guidance, risk phrases. Add quarter-over-quarter **tone delta** + word-level novelty + CEO-vs-CFO divergence detection.
**Why ≥1%:** Absolute tone is noise. Tone **delta** from prior quarter is signal, especially when CFO and CEO diverge.
**L/C:** Condition — management sentiment shift. **Cost:** S · **Coverage:** ~6%
**Location:** extend `intelligence/earnings_transcript_analyzer.py` with `compute_qoq_delta()` and `detect_exec_divergence()` methods
**Overlap:** extends existing.

### 152. 10-K / 10-Q risk factor novelty detector `[I · Tier A · NEW]`
NLP scan flagging language in Risk Factors (Item 1A) that is **materially new** vs the prior filing. Forensic accountants do this manually; systematize it.
**Why ≥1%:** New risk-factor language precedes material adverse events with ~50% hit rate. Underexploited.
**L/C:** Lever — issuer + counsel actors. **Cost:** M · **Coverage:** ~5% (but high precision)
**Location:** new `intelligence/risk_factor_novelty.py` (could share NLP pipeline with `intelligence/earnings_transcript_analyzer.py`)
**Overlap:** shares infra with `earnings_transcript_analyzer.py`.

### 153. Central bank speech hawkishness scoring `[I · Tier B · EXTEND]`
Word-embedding-based hawkish/dovish scoring of central bank speeches (Fed, ECB, BOJ, BOE). Detect **shifts** in posture before market repricing.
**Why ≥1%:** Central bank language shifts lead rates repricing by hours to days.
**L/C:** Lever — CB actors. **Cost:** M · **Coverage:** ~8%
**Location:** new `intelligence/cb_hawkishness.py` (extends `ingestion/altdata/fed_speeches.py`)
**Overlap:** extends existing `fed_speeches.py` consumer.

### 154. Geopolitical risk index (NLP on wires + GDELT) `[I · Tier B · EXTEND]`
NLP-based geopolitical risk index from news wires + GDELT event data. Extends existing `ingestion/altdata/gdelt.py` + `intelligence/news_intel.py`.
**Why ≥1%:** Geopolitical risk spikes drive defensive rotation + gold + oil + USD.
**L/C:** Lever — state actors. **Cost:** M · **Coverage:** ~5%
**Location:** new `intelligence/geopolitical_risk.py` (extends `ingestion/altdata/gdelt.py`)
**Overlap:** extends existing `gdelt.py` + `news_intel.py`.

### 155. Narrative lifecycle tracker (forming → peaking → dying) `[I · Tier B · NEW]`
Track major financial narratives ("AI capex", "soft landing", "China reopen") through lifecycle stages via NLP on analyst notes + financial media + Twitter. Each stage has different optimal positioning.
**Why ≥1%:** Narratives drive ~30% of multi-week moves. Lifecycle awareness converts ambient noise into timed entries.
**L/C:** Condition — crowd dynamics. **Cost:** L · **Coverage:** ~15%
**Location:** new `intelligence/narrative_lifecycle.py`
**Overlap:** complements #118 (information cascade classifier).

### 156. Analyst downgrade wave detector + credibility-weighted `[I · Tier B · EXTEND]`
Detect downgrade waves (3+ within 5 days) weighted by analyst historical accuracy. Extends `ingestion/altdata/analyst_ratings.py`.
**Why ≥1%:** Credibility-weighted waves beat raw counts. Top-decile accurate analysts drive most of the alpha.
**L/C:** Lever — analyst actors. **Cost:** M · **Coverage:** ~7%
**Location:** extend `ingestion/altdata/analyst_ratings.py` + new `intelligence/analyst_wave.py`
**Overlap:** extends `analyst_ratings.py`.

### 157. SEC enforcement language severity tracker `[I · Tier C · NEW]`
NLP scan of SEC enforcement actions, comment letters, and consent decrees. Severity of language correlates with actual enforcement probability.
**Why ≥1%:** Severe SEC language precedes material enforcement actions by 3-9 months.
**L/C:** Lever — SEC is named actor. **Cost:** M · **Coverage:** ~2%
**Location:** new `intelligence/sec_enforcement_nlp.py` (extends `ingestion/altdata/edgar_transcripts.py` if exists)
**Overlap:** partial with existing SEC edgar infra.

### 158. Court filing docket monitor (bankruptcy + antitrust + class action) `[I · Tier C · NEW]`
Real-time PACER docket monitor for major bankruptcy, antitrust, and class-action filings. Court events move specific names 5-20%.
**Why ≥1%:** Bankruptcy filings are mechanical price events. Class-action filings cluster before formal announcements.
**L/C:** Lever — court + litigant actors. **Cost:** M · **Coverage:** ~2%
**Location:** new `intelligence/court_docket_monitor.py` + `ingestion/altdata/pacer_docket.py` for data layer
**Overlap:** none.

### 159. Executive bio change tracker + LinkedIn exits `[I · Tier C · NEW]`
Monitor executive LinkedIn profile changes + SEC Form 4 + 8-K Item 5.02 for clustered exits. Exec exit clusters precede bad quarters.
**Why ≥1%:** Clustered C-suite exits without announced replacements are a high-precision negative signal.
**L/C:** Lever — exec actors. **Cost:** M · **Coverage:** ~2%
**Location:** new `intelligence/exec_exit_tracker.py`
**Overlap:** partial with `actor_network.py`.

### 160. Whistleblower + anonymous tipline credibility scoring `[I · Tier C · NEW]`
Track whistleblower filings (SEC OWB) + anonymous short-seller claims + Reddit-originated fraud allegations with historical accuracy scoring.
**Why ≥1%:** Anonymous tips are usually noise, but specific historical sources have real track records.
**L/C:** Lever — tipster actors. **Cost:** M · **Coverage:** ~1%
**Location:** new `intelligence/whistleblower_credibility.py` (extends `intelligence/trust_scorer.py`)
**Overlap:** extends `intelligence/trust_scorer.py`.

---

## Q. Network + graph (#161-170)

Director interlock graph, audit firm network, causal DAG, influence propagation. EXTENDS `intelligence/actor_network.py` (475+ actors, 7002 lines), `intelligence/influence_network.py` (Crown Jewel influence loops), and `intelligence/deep_graph.py`.

### 161. Director interlock graph + corporate governance overlay `[I · Tier B · EXTEND]`
Build a graph of shared board members, shared audit firms, shared law firms, and shared PR agencies across US listed companies. Network contagion when one domino falls.
**Why ≥1%:** Governance networks surface second-order risk: when a company has an enforcement action, connected names are often next.
**L/C:** Lever — governance network. **Cost:** L · **Coverage:** ~4%
**Location:** extend `intelligence/actor_network.py` with `DirectorInterlockGraph` class (ingests DEF 14A proxy filings)
**Overlap:** extends existing 475+ actor network.

### 162. Credit event probability machine (single name) `[I · Tier A · NEW]`
Per-name credit event probability: CDS term structure + bond spread + equity vol + rating trajectory → `P(default | 90d)`, `P(default | 1y)`. Updates daily.
**Why ≥1%:** Distressed single-name exposure is asymmetric. Credit event probability drives both shorts and avoiding longs.
**L/C:** Lever — issuer + creditor actors. **Cost:** M · **Coverage:** ~5% (credit-sensitive names)
**Location:** new `intelligence/credit_event_prob.py`
**Overlap:** none.

### 163. Central bank credibility tracker (dynamic Bayesian) `[I · Tier B · NEW]`
Dynamic Bayesian update on whether each major central bank is ahead or behind the curve. Measures: dots vs market, forecast vs realized, speech tone vs action.
**Why ≥1%:** Credibility shifts precede FX + yield curve repricing.
**L/C:** Condition — CB posture. **Cost:** M · **Coverage:** ~8%
**Location:** new `intelligence/cb_credibility.py`; consumes `ingestion/international/ecb.py`, `rbi.py`, `jquants.py`, `altdata/fed_speeches.py`
**Overlap:** none.

### 164. Supply chain BOM graph + disruption impact propagator `[I · Tier C · NEW]`
Bottom-up bill-of-materials graph from 10-K + supply chain disclosures + trade press. Propagate disruption events through the graph to affected tickers.
**Why ≥1%:** Supply chain is the unseen network. A chip shortage at one foundry cascades to 50+ equities.
**L/C:** Lever — supply chain actors. **Cost:** L · **Coverage:** ~5%
**Location:** new `intelligence/supply_chain_graph.py` (extends `ingestion/altdata/supply_chain.py`)
**Overlap:** extends existing `supply_chain.py`.

### 165. Sector network mapper wiring (activate existing 8 networks) `[I · Tier A · WIRE]`
`intelligence/` has 8 existing network mappers (banking, energy, pharma, defense, tech_monopoly, real_estate, commodities_agriculture, defi_protocols, media) per MODULE_CATALOG. Wire them into oracle scoring as structural position context.
**Why ≥1%:** These networks exist but are not consumed by the oracle. Wiring them in as scoring context is a cheap win.
**L/C:** Lever — named sector actors. **Cost:** S · **Coverage:** ~20%
**Location:** new `intelligence/sector_network_integrator.py` pulling from all 8 existing network mappers
**Overlap:** wires existing `banking_network.py`, `energy_network.py`, `pharma_network.py`, `defense_contractors.py`, `tech_monopoly_network.py`, `real_estate_network.py`, `commodities_agriculture_network.py`, `defi_protocols.py`, `media_network.py`.

### 166. Actor temporal decay (actor_network dynamic weights) `[I · Tier B · EXTEND]`
`intelligence/actor_network.py` currently assumes static network topology (session discovery: "no temporal decay of relationships"). Add decay for relationships that haven't been observed recently.
**Why ≥1%:** Stale relationships bias influence scoring. Temporal decay keeps the actor graph relevant.
**L/C:** N/A. **Cost:** M · **Coverage:** ~10%
**Location:** extend `intelligence/actor_network.py` with decay method
**Overlap:** extends existing.

### 167. Multi-hop causation tracer (extend causation.py) `[I · Tier B · EXTEND]`
`intelligence/causation.py` is "single-hop primary implementation; graph walks are shallow" (session discovery). Extend to multi-hop causal chain tracing with confidence decay per hop.
**Why ≥1%:** Multi-hop is how real alpha propagates (Fed → bank → REIT; OPEC → crude → airline). Current shallow walks miss half the chain.
**L/C:** Lever — multi-actor chains. **Cost:** L · **Coverage:** ~10%
**Location:** extend `intelligence/causation.py` with `MultiHopTracer` class (or `causation_core/graph.py` submodule)
**Overlap:** extends existing.

### 168. Influence propagation simulator (activate influence_network.py) `[I · Tier B · EXTEND]`
`intelligence/influence_network.py` is the "Crown Jewel: influence loops & leverage points." Add a propagation simulator: given an actor's action, which downstream actors are most likely to react?
**Why ≥1%:** Converts structural influence maps into **dynamic flow forecasts**.
**L/C:** Lever — chained actor flows. **Cost:** L · **Coverage:** ~8%
**Location:** extend `intelligence/influence_network.py` with `propagate_action()` method
**Overlap:** extends the Crown Jewel.

### 169. Network centrality ranking for sizing (tiered actor trust) `[I · Tier B · EXTEND]`
Rank actors by graph centrality (eigenvector + betweenness) and tier them for sizing. More central actors' signals get more weight. Extends `intelligence/trust_scorer.py` with structural context.
**Why ≥1%:** Currently trust is Bayesian hit-rate based only. Adding structural centrality as a prior improves accuracy for rare signals.
**L/C:** N/A. **Cost:** M · **Coverage:** ~15%
**Location:** extend `intelligence/trust_scorer.py` with centrality computation
**Overlap:** extends existing.

### 170. Bayesian network over actor signals (actor-conditional probabilities) `[I · Tier C · NEW]`
Full Bayesian network: `P(NVDA up | Burry short, Pelosi buy, Fed cut)`. Conditional probabilities learned from historical co-occurrence. Belief propagation on new evidence.
**Why ≥1%:** Structured probabilistic reasoning beats flat feature bags for rare conjunctions.
**L/C:** Lever — conjunction of named actors. **Cost:** L · **Coverage:** ~8%
**Location:** new `intelligence/actor_bayes_network.py`
**Overlap:** none.

---

## R. Calibration + uncertainty (#171-180)

Per-horizon Brier tracking, calibration drift alerts, Shapley attribution, Kelly with confidence bounds. EXTENDS `intelligence/prediction_calibration.py` and `oracle/calibration.py` (computes on-demand, doesn't persist).

### 171. Per-horizon Brier tracking + reliability curves `[I · Tier A · EXTEND]`
Extend `oracle/calibration.py` to compute + persist separate Brier scores and reliability diagrams for each horizon (5d/30d/90d/365d). Currently it's one global score.
**Why ≥1%:** Per-horizon calibration reveals which horizons GRID is good at. Essential for #101 (horizon-conditional oracle) to function.
**L/C:** N/A. **Cost:** S (if #101 lands first) · **Coverage:** 100%
**Location:** extend `oracle/calibration.py` + persist to new `calibration_history` table
**Overlap:** extends existing + complements #108.

### 172. Calibration drift alerts (2σ from baseline) `[I · Tier A · NEW]`
Alert when any confidence bucket's hit rate drifts more than 2σ from its historical baseline. Silent calibration failure is one of the most dangerous oracle pathologies.
**Why ≥1%:** Catches model decay before it compounds. Alerts drive manual intervention or auto-downweighting.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** new `oracle/calibration_drift.py`; consumes #171
**Overlap:** complements #108, #171.

### 173. Predictive entropy tracking per trade `[I · Tier B · NEW]`
Compute Shannon entropy of the prediction distribution. Low entropy = high conviction; high entropy = skip or small size. Use as a multiplier on Kelly fraction.
**Why ≥1%:** Entropy is the honest "how much does GRID actually know" metric. Sizing by entropy beats sizing by point estimate.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** new `oracle/entropy_sizing.py`
**Overlap:** complements #106 (uncertainty bounds) + #110 (Kelly with error bars).

### 174. Brier decomposition (reliability + resolution + uncertainty) `[I · Tier C · NEW]`
Decompose Brier score into its three components: reliability (calibration quality), resolution (discrimination), and uncertainty (base-rate entropy). Reveals whether errors come from miscalibration or poor discrimination.
**Why ≥1%:** Diagnostic — tells you whether to fix calibration or fix features.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** extend `oracle/calibration.py` with `brier_decomposition()` method
**Overlap:** extends existing.

### 175. Counterfactual stress test engine `[I · Tier B · NEW]`
For every live prediction, compute `P(outcome | GFC-analog)`, `P(outcome | dotcom-analog)`, `P(outcome | 2015-SNB-analog)`, `P(outcome | 2020-COVID-analog)`. Each prediction reports a stress-adjusted confidence.
**Why ≥1%:** Tail robustness check. Predictions that die under stress get smaller sizing.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** new `oracle/stress_scenarios.py`
**Overlap:** none.

### 176. Historical scenario library (pattern-matched regimes) `[I · Tier B · NEW]`
Database of historical market scenarios (2008 Oct, 2015 Aug, 2018 Dec, 2020 Mar, 2022 Jun) with pattern-matching similarity scoring. When current conditions match a historical scenario, surface the parallel.
**Why ≥1%:** Historical analogs are more informative than ML forecasts for rare conditions.
**L/C:** N/A. **Cost:** M · **Coverage:** ~15% (high-similarity periods)
**Location:** new `intelligence/historical_scenarios.py`; complements #175
**Overlap:** none.

### 177. Pattern library with base rates (analog matcher) `[I · Tier B · NEW]`
For each current setup (regime × catalyst × positioning), find the N closest historical analogs + outcome distribution. "Current setup is 87% match to 2018 Q4; outcome distribution: −15% median, +20% best-case, −25% worst-case."
**Why ≥1%:** Base-rate conditioning beats point-estimate ML for sparse-data situations.
**L/C:** N/A. **Cost:** M · **Coverage:** ~20%
**Location:** new `intelligence/pattern_library.py` (extends `intelligence/pattern_engine.py` which exists)
**Overlap:** extends existing `intelligence/pattern_engine.py`.

### 178. Bayesian evidence combiner (explicit prior updating) `[I · Tier A · NEW]`
Replace the current weighted-vote combiner with an explicit Bayesian evidence accumulator: `posterior ∝ prior × likelihood(signal_1) × likelihood(signal_2) × ...` with known correlation adjustments.
**Why ≥1%:** Proper Bayesian combination handles signal correlations correctly. Weighted-vote double-counts correlated signals.
**L/C:** N/A (architecture). **Cost:** L · **Coverage:** 100%
**Location:** new `oracle/bayes_combiner.py` (optional replacement for current `oracle/engine.py` combiner)
**Overlap:** major architectural change to `oracle/engine.py`.

### 179. Prediction pre-registration enforcement `[I · Tier C · NEW]`
Require each prediction to declare lever + condition + invalidation + horizon BEFORE the oracle runs on current data. Prevents post-hoc rationalization.
**Why ≥1%:** Pre-registration is the gold standard for scientific hypothesis testing. Prevents the hindsight-bias failure mode.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** new `oracle/pre_registration.py` + extends existing `journal/log.py` (decision journal already enforces immutability)
**Overlap:** complements existing decision journal.

### 180. Confidence bucket hit rate tracking (per-horizon, per-regime) `[I · Tier B · EXTEND]`
Track hit rates per (horizon × regime × confidence bucket). When GRID says `p=0.7` in growth regime at 30d horizon, actual hit rate should be ~0.70.
**Why ≥1%:** Granular calibration reveals which buckets are miscalibrated. Enables targeted fixes.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** extend `oracle/calibration.py` with granular bucket tracking
**Overlap:** extends existing.

---

## S. Adversarial + meta (#181-190)

LLM red-team per prediction, market-implied probability comparator, consensus crowdedness detector, ensemble disagreement as feature. Mostly NEW builds — GRID has `hypothesis_engine.py` but no systematic adversarial loop.

### 181. LLM red-team loop per prediction (smart bear + smart bull) `[I · Tier A · NEW]`
For each prediction, run two LLM personas: **Smart Bear** (strongest counter-thesis) and **Smart Bull** (strongest supporting thesis). Rebuttal strength becomes a meta-feature. If GRID can't rebut the smart bear, confidence downgrades.
**Why ≥1%:** Forces adversarial validation on every prediction. Kills overconfident fragile theses. Estimated: ~1.5% via confidence rationalization.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** new `oracle/red_team_loop.py` (extends `intelligence/hypothesis_engine.py` existing infra)
**Overlap:** extends `intelligence/hypothesis_engine.py`.

### 182. Consensus crowdedness detector `[I · Tier A · NEW]`
Detect when GRID's prediction aligns with crowd positioning (high short interest on the loser, high long exposure on the winner, high media volume). Discount crowded signals automatically — the market has already priced them.
**Why ≥1%:** Crowdedness is the single best tell for signal decay. A "great" signal aligning with positioning extremes has ~50% lower hit rate.
**L/C:** Condition — crowding. **Cost:** M · **Coverage:** ~20%
**Location:** new `intelligence/crowdedness_detector.py`; consumes CFTC COT, 13F flows, short interest
**Overlap:** none.

### 183. Prediction market arbitrage detector (Polymarket + Kalshi vs GRID) `[I · Tier B · EXTEND]`
Systematically compare GRID's probability to Polymarket/Kalshi prediction market prices. Divergences are opportunities (or warnings).
**Why ≥1%:** Binary prediction markets are the most honest consensus measure. Divergence > 15% signals either edge or error.
**L/C:** N/A (benchmarking). **Cost:** S · **Coverage:** ~10% (event-driven)
**Location:** extend `ingestion/altdata/prediction_odds.py` with divergence computation; new `intelligence/prediction_market_arb.py`
**Overlap:** extends existing `prediction_odds.py` + `kalshi.py` + `prediction_pmxt.py`.

### 184. Contra-indicator ensemble (signals that fire when crowd agrees) `[I · Tier B · NEW]`
Signals that **reduce** confidence when they agree with consensus positioning. Think: AAII sentiment > 65% bulls is a contra-indicator; IBD #1 rank is a contra-indicator.
**Why ≥1%:** Contrarian signals are their own asset class. Systematize them as negative weights.
**L/C:** Condition — crowd contrarian. **Cost:** S · **Coverage:** ~10%
**Location:** new `intelligence/contra_indicators.py`
**Overlap:** consumes existing `ingestion/altdata/aaii_sentiment.py`.

### 185. Simplest counter-explanation test (LLM) `[I · Tier C · NEW]`
For each prediction, prompt an LLM: "Given these signals, what is the SIMPLEST explanation for why the market has not already priced this?" Strong counter-explanations downgrade confidence.
**Why ≥1%:** Occam's razor forces a reality check. Catches overcomplicated theses.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** new `oracle/counter_explanation.py` (extends `red_team_loop.py`)
**Overlap:** complements #181.

### 186. Null hypothesis forecaster (baseline skeptic) `[I · Tier B · NEW]`
Every prediction must beat a "null hypothesis" forecast from: random walk, momentum, mean reversion, yield-curve model. If GRID doesn't beat the baselines, skip the trade.
**Why ≥1%:** Null benchmarks catch spurious complexity. Many GRID signals may not beat a simple momentum baseline after costs.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** new `oracle/null_baselines.py`
**Overlap:** partially complements existing `oracle/engine.py` + `oracle/calibration.py`.

### 187. Signal value in bits (entropy reduction measurement) `[I · Tier C · NEW]`
Measure each signal's value in **bits of information reduction** about outcomes, not correlation coefficient. Prioritize signals by bits, not by R².
**Why ≥1%:** Information-theoretic ranking beats correlation for rare high-impact signals that move posterior distributions.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** new `features/information_value.py` (extends `features/importance.py`)
**Overlap:** extends existing.

### 188. Exogenous shock classifier (normal-signals-fail detector) `[I · Tier B · NEW]`
Real-time classifier that detects when markets are in a state where normal signals stop working (GFC day 3, COVID March 2020, SNB January 2015). Automatically downgrades ALL predictions during these windows.
**Why ≥1%:** Shock regimes invalidate most models. Detection prevents catastrophic over-sizing.
**L/C:** N/A. **Cost:** M · **Coverage:** 100% (multiplicative)
**Location:** new `oracle/exogenous_shock.py`
**Overlap:** complements `discovery/clustering.py` regime detection.

### 189. Forensic journal of failed predictions (pattern mining) `[I · Tier B · EXTEND]`
`intelligence/postmortem.py` exists but "no temporal pattern mining (are failures clustering on certain days/regimes?)" per session discovery. Extend with pattern mining + auto-feedback into trust weights.
**Why ≥1%:** Failed trades reveal model weaknesses. Pattern mining surfaces them systematically.
**L/C:** N/A. **Cost:** M · **Coverage:** 100%
**Location:** extend `intelligence/postmortem.py` with `mine_failure_patterns()` method
**Overlap:** extends existing.

### 190. Automatic thesis invalidation monitor `[I · Tier A · EXTEND]`
For each active prediction, monitor the pre-registered invalidation condition. When it triggers, alert + auto-size-down. Extends `journal/log.py` decision journal.
**Why ≥1%:** Automated invalidation prevents holding losing theses past their stop. Discipline multiplier.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** new `oracle/invalidation_monitor.py`; consumes `journal/log.py` + #179 pre-registration
**Overlap:** depends on #179.

---

## T. Outside-the-box (#191-200)

Reflexivity modeling, second-order signals, pattern library with base rates, synthetic control event studies, proof-of-work predictions, information cascade classifier. The most speculative entries — high variance but uncorrelated with everything else in the catalog.

### 191. Second-order signals (predict the predictors) `[I · Tier C · NEW]`
Don't predict BTC price; predict "probability Goldman raises its year-end target." Meta-signals about signal-makers themselves. When sell-side is about to capitulate or upgrade, that IS the signal.
**Why ≥1%:** Sell-side target changes have measurable price impact. Predicting them gives a window into the narrative pivot.
**L/C:** Lever — sell-side actors. **Cost:** M · **Coverage:** ~5%
**Location:** new `intelligence/second_order_signals.py`
**Overlap:** consumes `analyst_ratings.py`.

### 192. Dynamic feature temperature (automatic decay) `[I · Tier B · NEW]`
Each feature has a "temperature" (weight multiplier) that decays automatically when recent performance drifts. Hot features stay hot; cold features cool naturally.
**Why ≥1%:** Automated feature weighting removes manual intervention and prevents model staleness.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** new `features/temperature.py` (complements #116)
**Overlap:** complements #116.

### 193. Meta-learning: "which signals work in which conditions" `[I · Tier B · NEW]`
A meta-model whose output is NOT a prediction, but **"in this regime, trust signals A/B/C, distrust D/E/F."** Used to dynamically reweight features per regime × catalyst × horizon.
**Why ≥1%:** Feature importance is not universal. Meta-learning routes the right signals to the right moments.
**L/C:** N/A. **Cost:** L · **Coverage:** 100%
**Location:** new `oracle/meta_learner.py`
**Overlap:** complements #107 (per-horizon importance) + #109 (regime routing).

### 194. Hedge-of-the-hedge computer (primary-risk hedging) `[I · Tier C · NEW]`
For each primary trade thesis, compute the best hedge against the **primary risk TO THE THESIS** (not against market risk). Changes "certain trade" from high P(direction) to high P(direction) AND low tail cost.
**Why ≥1%:** Thesis-level hedging reduces P&L variance without reducing expected return.
**L/C:** N/A. **Cost:** M · **Coverage:** ~20% (sized trades)
**Location:** new `trading/thesis_hedges.py`
**Overlap:** extends `trading/options_recommender.py`.

### 195. Autonomous earnings call analyst (real-time LLM) `[I · Tier C · EXTEND]`
Extend `intelligence/earnings_transcript_analyzer.py` with a real-time LLM that listens to earnings calls as they happen, scores guidance delta, and flags unusual management behavior instantly.
**Why ≥1%:** Live reaction beats post-call analysis when the call is moving the stock in real time.
**L/C:** Lever — management actors. **Cost:** M · **Coverage:** ~4%
**Location:** extend `intelligence/earnings_transcript_analyzer.py` with streaming LLM pipeline
**Overlap:** extends existing.

### 196. Patent citation network → tech leadership forecaster `[I · Tier C · NEW]`
Use USPTO patent citation graph (already in `ingestion/physical/patents.py`) to forecast technology leadership. Citation velocity predicts commercial leadership 3-5 years out.
**Why ≥1%:** Patent networks predict LEAPS-horizon winners in AI, semis, biotech, clean tech.
**L/C:** Condition — innovation leadership. **Cost:** M · **Coverage:** ~5% (LEAPS tech theses)
**Location:** new `intelligence/patent_leadership.py` (extends existing `ingestion/physical/patents.py`)
**Overlap:** extends existing `patents.py`.

### 197. Autonomous regulatory filing extractor + clusterer `[I · Tier C · NEW]`
LLM-driven extraction of material facts from 10-K/10-Q/8-K filings + clustering across tickers to surface thematic trends ("15 restaurants cited labor cost inflation this quarter").
**Why ≥1%:** Thematic clustering surfaces macro shifts from micro filings.
**L/C:** Condition — thematic emergence. **Cost:** L · **Coverage:** ~5%
**Location:** new `intelligence/filing_extractor_clusterer.py` (extends `intelligence/earnings_transcript_analyzer.py`)
**Overlap:** extends existing.

### 198. M&A rumor triangulation agent `[I · Tier C · NEW]`
Triangulate M&A rumors across: unusual options activity + news leaks + executive travel + antitrust scrutiny + investment banker movements. Multi-source confirmation beats single-source rumors.
**Why ≥1%:** Confirmed M&A = 20-40% single-day moves. Triangulation cuts false-positive rate.
**L/C:** Lever — named corporate actors. **Cost:** L · **Coverage:** ~2%
**Location:** new `intelligence/ma_rumor_triangulation.py` (extends `intelligence/actor_network.py`)
**Overlap:** extends existing actor network.

### 199. Tail-hedge allocation optimizer `[I · Tier C · NEW]`
Given a portfolio, compute optimal tail hedge allocation using put spreads, VIX calls, long-dated puts on credit ETFs (HYG). Explicitly budget for tail protection.
**Why ≥1%:** Tail hedging has a known cost. Optimized allocation finds the Sharpe-improving subset vs naive approaches.
**L/C:** N/A. **Cost:** M · **Coverage:** ~10% (sizing layer)
**Location:** new `trading/tail_hedge_optimizer.py`
**Overlap:** extends `trading/options_recommender.py`.

### 200. The "known unknowns" tracker (epistemic uncertainty catalog) `[I · Tier C · NEW]`
Explicit catalog of **what GRID doesn't know** — known data gaps, known model limitations, known regime blind spots. Each prediction carries a reference to which unknowns affect it.
**Why ≥1%:** Epistemic humility tracking is the last-mile check. Predictions affected by multiple known unknowns get smaller sizing automatically.
**L/C:** N/A. **Cost:** S · **Coverage:** 100%
**Location:** new `oracle/known_unknowns.py` + `docs/KNOWN_UNKNOWNS.md` human-maintained
**Overlap:** complements #103 (Shapley) + #106 (uncertainty bounds).

---

_End of INTELLIGENCE.md. 100 entries (#101-200). See SHORTLIST-TIER-A.md for the highest-conviction subset with ship order._
