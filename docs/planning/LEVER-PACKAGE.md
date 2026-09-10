# The Lever Package — what in GRID actually identifies the levers that move markets

> **Written:** 2026-09-10, against the tree at `19f4f11`. **Method:** eight read-only audits
> over every production directory, one liveness test throughout — *is the module reachable
> from a running entrypoint* (`grid-hermes`, `grid-api`, `grid-intelligence`,
> `grid-scheduler`, `grid-worker`, `grid-spider`, `grid-backlinker`, `grid-extractor`,
> `grid-breaking-news`, cron)? Every claim carries a `file:line`. Server state (DB row
> counts, logs) is unverifiable from this clone and is marked as such.
>
> **Companion:** `GRID-4-PRODUCT-PIVOT.md` (2026-05-16) is the operator's own audit of the
> same question. This document verifies it against the code five months later and
> finishes assembling what it started.

---

## 0. Bottom line

1. **There are 982 production Python modules, not 800.** Roughly 55 of them are the
   package (§3). The rest are support plumbing, product surface, narration, dormant
   scaffolds, research spikes, or duplicates (§8).

2. **You already wrote the verdict on May 16.** The GRID-4 pivot document concluded from a
   day of data analysis that the prediction firehose had *zero positive alpha vs SPY* and
   the 13-layer conviction stack *anti-correlated with reality at high confidence* (11.9%
   hit rate at HIGH), because every layer silently defaults to `1.0` when its upstream is
   missing. Its promotion list is the lever package. Five months later: the Trump-Proximity
   Score shipped (`intelligence/trump_proximity.py`, 691 LOC, with NULL-propagation done
   correctly); **the truth gate (`alpha_research/realized_alpha.py`) was never built**;
   nothing on the deletion list was deleted; `signal_provenance.compute_aggregate_conviction`
   still defaults every layer to `1.0` (`intelligence/signal_provenance.py:298-309`); the
   oracle still runs every six hours. A second pivot (stepdad.finance, May 30–June 1) built
   a Dad persona and a "10-Year Compounder" view instead of the pivot's six views.

3. **The lever-identification core is real and mostly live.** Fifteen `intelligence/`
   modules answer *who pulled what lever, how much, when, why, with what track record*
   (§3A), and the mechanism quantifiers under `analysis/` and `physics/` — money-flow
   engine, dealer gamma with vanna/charm, vol surface, chokepoint contagion — are scheduled
   and consumed (§3B). This is the part of GRID that works.

4. **The long-horizon "stratospheric tailwind" stack exists on disk and runs nowhere.**
   `market_edge_scanner` (explicit 2–6-month playbooks over 540-day windows),
   `pattern_library` (5-year base rates), `decision_gateway.should_i_trade(horizon_days=…)`,
   and the only PIT-correct walk-forward engine (`validation/backtest.py`) are all
   unscheduled. The oracle writes every prediction with a ~35-day expiry, so **GRID has
   never recorded a prediction longer than five weeks** and the 90-day calibration bucket
   is structurally empty. The company-level structural data (gov contracts, legislation,
   export controls, insider clusters, 13F) is ingested but cannot reach the PIT feature
   layer because `entity_map.SEED_MAPPINGS` is an exact-key dict (§5). The "10-Year
   Compounder" frontier board is a hand-typed 20-ticker list ranked by chart quality
   against QQQ (`strategy/ten_year_portfolio.py:25-46`); no tailwind data touches it.

5. **The hypothesis engine was starved by a timeout inversion, not frozen.** The 30-minute
   scorer carried a 600 s budget inside a 360 s step. Fixed on this branch with an
   invariant test (§6). The May "freeze" was documented but never enacted in code. The
   Discovery → Hypotheses screen reads the governance table `hypothesis_registry`, never
   the engine's `discovered_hypotheses` (`api/routers/discovery.py:175,271`), so the
   engine looks dead from the UI even when it produces.

6. **Empirically, nothing is proven yet.** The paper ledger is −$194.31 across 8 open
   trades; the gauntlet's own header says 0/7 signals reached ROBUST; no oracle Brier/ECE
   report has ever been run; the Sharpe 1.96 for Adaptive Rotation is a docstring with no
   artifact (§4). The pivot's §8 truth gates are the right design and remain unbuilt.

7. **There is no equity broker.** Not Alpaca, IBKR, Tradier, Schwab, or Robinhood — the
   only order-placing venues are crypto perps, Solana, and prediction markets. For stocks,
   GRID ends at a human-readable ticket with a 5% Kelly cap
   (`trading/trade_ticket_generator.py:257`). "Real money" means you type the order.

---

## 1. The May 16 pivot: what was decided, what happened

| Pivot item (`GRID-4-PRODUCT-PIVOT.md`) | Decision | State on 2026-09-10 | Evidence |
|---|---|---|---|
| Trump-Proximity Score (Phase 0) | Build in 24 h | **Built.** 691 LOC, five evidence layers, NULL-propagating, router + view + scheduler | `intelligence/trump_proximity.py:15-24`; `api/routers/tps.py`; `pwa/src/views/TPS.jsx` |
| `alpha_research/realized_alpha.py` — rolling realized alpha vs SPY at 5/10/20/60 d for every journal entry (Phase 1, "blocks every claim of positive alpha") | Build | **Not built.** File does not exist | `ls alpha_research/` |
| `intelligence/swing_signal.py` — ≤5 candidates/day from dollar_flows + cross_reference + regime (Phase 1) | Build | **Not built** | — |
| Rewrite `compute_aggregate_conviction` to propagate NULL ("the single most important amputation") | Phase 1 | **Not done.** Every multiplier is `float(x or 1.0)`; a missing upstream still reads as neutral | `intelligence/signal_provenance.py:298-309` |
| Delete the 10 conviction layers, keep `cross_reference` + TPS | Phase 2 | **Not done.** All 10 present and imported; a 14th (`money_flow_adapter`) was added since | `signal_provenance.py:41-72` |
| Freeze then delete the oracle firehose (`engine.py` 5-model path, `model_factory`, `model_evolver`, `run_cycle`, `psi_model`, `scoreboard`, `calibration`) | Freeze 05-17, delete 05-31 | **Not done.** All present; `oracle.run_cycle` runs every 6 h under a 4,000 s budget | `scripts/hermes_operator.py:90`, `:1704` |
| Drop `oracle_predictions` after parquet export | Phase 2 | **Not done** — and the hypothesis engine reads it for price evidence | `intelligence/hypothesis_engine.py:1385` |
| Freeze `hypothesis_engine.py` | Phase 2.3 | **Not enacted.** Hermes still calls it every 30 min and every 20 h | `hermes_operator.py:584`, `:811` |
| Delete `options_recommender`, `options_tracker`, `strategy151`, `contagion_to_ticket`, `prediction_markets`, `prediction_backtest` | Phase 2.3 | **Not done.** All present; options recommender/tracker are scheduled daily | `hermes_operator.py:657`, `:788` |
| Cut PWA to 6 views (`TPS`, `Trades`, `Core`, `Dossiers`, `Journal`, `Ops`), archive 38 | Phase 2 | **Not done.** 71 views, 61 routes. `Core/Trades/Dossiers/Ops.jsx` do not exist | `pwa/src/routes.js` |
| 100x Core v0 — 5–10 long-horizon theses with kill criteria (BTC, AI compute, rare earths, platforms, uranium) | Phase 2 | **Re-expressed** as the 10-Year Compounder frontier board: 20 hand-typed tickers with theme strings, ranked by 10-year chart quality vs QQQ. No kill criteria, no catalyst timeline, no thesis store | `strategy/ten_year_portfolio.py:25-48`, `:329 score_candidate` |
| Truth gates (§8): SPY benchmark at 5 bp/side, 60 d realized alpha, kill if 180 d alpha < 0, journaled-at-emission with frozen invalidation | Design | **Design only.** Depends on `realized_alpha.py` (not built). `thesis_invalidation_monitor.py` exists (421 LOC) and is scheduled hourly | `intelligence/scheduler.py:859` |

**Reading:** the pivot's *diagnosis* stands and nothing since contradicts it. Its *subtraction* was not carried out, its *measurement* layer was never built, and its *long-horizon product* was replaced by a curated list. The rest of this document is built on the pivot's promotion list, verified module by module.

---

## 2. The honest count

Production `.py` files, excluding `tests/` (434 files), `migrations/`, and `__init__.py`:

| Directory | Files | Note |
|---|---|---|
| `scripts/` | 198 | daemons, cron jobs, migrations, one-off spikes, operator tools |
| `ingestion/` | 197 | ~110 pullers registered in `ingestion/scheduler.py`; the rest dormant or helpers |
| `intelligence/` | 174 | 182 incl. `__init__`; 158 import-reachable, 11 orphaned, 12 narration-only |
| `api/` | 107 | ~90 routers |
| `trading/` | 32 | |
| `analysis/` | 31 | |
| `oracle/` | 29 | |
| `contracts/` | 21 | the live event backbone (see `V5-TRANSFORMATION.md` D1) |
| `alpha_research/` | 15 | |
| `physics/` | 14 | |
| `orchestration/` | 11 | Prefect flows with no importers |
| `subnet/`, `astrogrid_api/` | 10 each | Bittensor stubs; astrology app |
| everything else (`agents`, `llm`, `inference`, `alerts`, `ollama`, `features`, `store`, `hyperspace`, `gemma`, `autoagent`, `verification`, `valuation`, `events`, `discovery`, `agent_hub`, `validation`, `utils`, `timeseries`, `strategy`, `server_log`, `outputs`, `knowledge`, `gem_hunter`, `backtest`, `a2a`, top-level) | 118 | |
| **Total** | **982** | 1,055 with `__init__.py` |

Verdict distribution across the audited slices (per-module tables in Appendix A):

| Slice | CORE | SUPPORT | DORMANT | EXHAUST | DUPLICATE |
|---|---|---|---|---|---|
| `intelligence/` (182) | 33 | 114 | 11 | 12 | 12 (thin adapters) |
| `analysis/` `physics/` `features/` `knowledge/` `verification/` `agents/` `hyperspace/` + top-level (84) | 26 | 41 | 14 | 4 | 3 |
| decision core: `oracle/` `inference/` `governance/` `journal/` `strategy/` `trading/` `discovery/` `validation/` `backtest/` `alpha_research/` `derivatives/` (135) | 29 | 46 | 28 | 29 | 3 |
| data plane: 181 puller modules + `normalization/` `store/` | 37 reach a scorer | — | 91 registered but unmapped; 12 unregistered | 10 duplicates, 5 celestial | 9 prefix-mismatch bugs, 6 documented-empty |
| periphery: `api/` `alerts/` `llm/` `contracts/` `events/` `orchestration/` `subnet/` `astrogrid_api/` `scripts/` (416) | — | 118 support + 148 product surface | 31 + **43 dead scripts** | 75 | 12 (incl. the 9-file `astrogrid_api/` fork) |

Across all slices: roughly **55 modules are the package** (§3), ~350 are live support or
product surface, and **~400 modules (~90,000 LOC) are dormant, exhaust, duplicate, or
dead** — close to a 2:1 ratio between "in the tree" and "would break something if deleted
tonight."

---

## 3. The package

Organized by the Prediction Causation Standard (`docs/reference/PREDICTION_SOP.md`): every
prediction must name the **lever** (actor + valve), the **condition** (amplifier), the
**thesis**, and the **invalidation**. The package is the set of modules that produce each
of those, plus the modules that score whether they were right.

### 3A. Lever identification — *who pulled what, how much, when, why, with what record*

The fifteen `intelligence/` modules that together answer the question. All fifteen are
live unless marked.

| # | Module | Answers | Entry point | Writes | Runs |
|---|---|---|---|---|---|
| 1 | `intelligence/lever_pullers.py` (1,377) | **Who.** Named actor, influence score, position, motivation | `identify_lever_pullers(engine)` `:332`; `assess_motivation()` `:583`; `find_lever_convergence()` `:879` | `lever_pullers` | Hermes every 6 h (`:705`), weekly report (`:1173`); 3 routers |
| 2 | `intelligence/global_levers.py` (2,254) | **What lever.** Hierarchical world power map, `trace_lever_chain` | `get_lever_hierarchy(engine)` `:1584`; `trace_lever_chain(event)` `:2039` | in-memory | API only (`intelligence_deepdive.py:44`) — pivot said "freeze"; it is reachable but not scheduled |
| 3 | `intelligence/causation_scoring.py` (1,088) | **Why.** Attributes a move to contracts, legislation, hearings, earnings | `find_causes()` `:32`; `batch_find_causes()` `:101`; `get_suspicious_trades()` `:155` | `causal_links` | via hypothesis engine every 30 min |
| 4 | `intelligence/causation_graph.py` (1,177) | **The chain.** Multi-hop actor → price | `trace_causal_chain()` `:62`; `detect_chain_in_progress()` `:291` | `causal_chains` | API (`intelligence_causation.py`) |
| 5 | `intelligence/trust_scorer.py` (1,991) | **Track record of the source.** Bayesian, 90-day half-life; `detect_convergence` | `run_trust_cycle(engine)` `:1274`; `get_insider_edge()` `:851` | `source_trust`, `source_trust_scores_cached`, `signal_sources` | Hermes every 4 h (`:601`). Most-imported intelligence module (55 refs), 12 test files |
| 6 | `intelligence/source_audit.py` (979) | **Track record of the data.** Cross-source discrepancy → resolver priority | `run_full_audit(engine)` `:669`; `update_source_priorities()` `:786` | `source_accuracy`, `source_discrepancies`, `UPDATE source_catalog` | Hermes daily (`:762`) |
| 7 | `intelligence/postmortem.py` (2,258) | **Track record of the call.** Why a trade failed; adjusts supply-chain edge weights | `batch_postmortem(engine, limit)` `:433` | `trade_postmortems`, `supply_chain_edge_adjustments` | Hermes daily (`:778`) |
| 8 | `intelligence/signal_provenance.py` (829) | **Assembles the answer.** Per-ticker "why" report over 14 multipliers | `build_provenance_report()` `:513`; `compute_aggregate_conviction()` `:234` | read-only | via `decision_gateway` ← `api/routers/conviction.py`. **Defaults every missing layer to 1.0** (`:298-309`) — the pivot's root cause, unfixed |
| 9 | `intelligence/cross_reference.py` (1,977) | **Is the actor lying.** Government stat vs physical reality | `run_all_checks(engine)`; `get_cross_ref_for_ticker()` `:1065` | `cross_reference_checks` | Hermes 4 h (`:669`) + weekly (`:1161`). The one layer the pivot kept |
| 10 | `intelligence/influence_network.py` (922) | **The loop.** Lobbying → PAC → vote → contract → insider trade | `detect_circular_flows(engine)` `:578`; `vote_trade_hypocrisy()` `:757` | `influence_loops` | API (`intelligence_forensics.py`) |
| 11 | `intelligence/deep_graph.py` (1,771) | **Hidden hop.** Company → board → lobbyist → politician → committee → ticker | `deep_drill(engine, ticker)` `:940` | `graph_overlaps` | API (`intelligence_companies.py:182`), canvas |
| 12 | `intelligence/dollar_flows.py` (1,080) | **How much.** Every flow type normalized to $ per actor per ticker | `normalize_all_flows(engine, days)` `:707`; `get_biggest_movers()` `:1040` | `dollar_flows` | API (`intelligence_govflow.py`), `dollar_flows_adapter` |
| 13 | `intelligence/institutional_map.py` (1,508) | **Conflicted actor.** Pension → PE fee extraction, conflicts of interest | `find_conflicts_of_interest()` `:1180`; `trace_pension_dollars()` `:1099` | derivation | API (`intelligence_companies.py:305`) |
| 14 | `intelligence/actors/db.py` + `actors/graph.py` + `actors/seed_data.py` (322 + 417 + 5,618) | **The registry.** 475+ named actors, connections, wealth flows | upserts / traversal | `actors`, `actor_connections`, `wealth_flows`, `spider_queue` | `grid-spider` hourly (≤50 new edges/run), `actor_network` ← Hermes |
| 15 | `intelligence/signal_backlinker.py` (492) | **When, closing the loop.** Signal → actor attribution | `--interval 300 --trust-every 12` | `actors`, `actor_connections` | `grid-backlinker.service`, 5-min loop |

Runners-up that belong in the package's second ring: `forced_flow_monitor.py` (forced-seller
early warning, Hermes), `holder_deal_overlap.py` (pre-positioning before deals, Hermes),
`actor_trust_cog.py` (trustworthy source vs cog-in-machine, Sunday), `trump_proximity.py`,
`wealth_tracker.py` (Hermes 6 h), `supply_chokepoints.py`, `short_squeeze_composite.py`,
`shipping_fudge_detector.py` (4 h), `legislative_intel.py`, `icij_linker.py` (daily),
`pocket_lining.py`.

**Not in the package despite their size:** `actor_discovery.py` (3,530 LOC, **zero
callers** — `docs/reference/CODEBASE_MAP.md:47` still presents it as load-bearing),
`grand_orchestrator.py`, `self_learning_loop.py`, `forensic_journal.py` (superseded by
`postmortem.py`), `edge_signals.py` (reachable only from a script no timer runs),
`prediction_calibration.py` (zero importers; it is a prediction-market divergence checker,
not calibration), `power_mapper.py` (a Hermes comment says it was deleted; it was not).

### 3B. Mechanism quantifiers — *the valve, measured*

| Module | Mechanism | Live path | Consumed by |
|---|---|---|---|
| `analysis/money_flow_engine/` (10 files, ~2,800 LOC; `build_flow_map`) | Where money is, layer by layer: monetary, credit, market, institutional, corporate, retail, sovereign, crypto; `flow_inference.py` infers the edges | `intelligence/money_flow_adapter.py:38` (14th conviction multiplier), `deep_dive`, `audio_briefing`, `flows.py:1948`, `chat.py:699` | conviction stack, thesis scorer |
| `analysis/thesis_scorer.py` (2,745) | 28 sub-scorers banding named levers into one thesis verdict | Hermes `:637`; `intelligence_thesis.py:50` | `thesis_snapshots`, scored later by `thesis_tracker.py:249` |
| `analysis/flow_thesis_data.py` + `flow_thesis_scoring.py` (1,413 + 333) | 24 `_get_*_state` lever state-getters (fed liquidity, dealer gamma, institutional rotation, cross-reference…) | six consumers via the `flow_thesis.py` facade | briefings, thesis, `flow_thesis_adapter` |
| `physics/dealer_gamma.py` (493) + `physics/greeks/black_scholes.py` (565) | Dealer GEX, gamma flip, walls, **vanna and charm** (`:246-250`) | **Scheduled daily** via `ingestion/scheduler.py:1317` → `discovery/options_scanner.py:585`; `trading/options_recommender.py:738` uses the profile for strike, target, stop | scanner signal (vanna wired at `options_scanner.py:232-236`), recommender, `valuation/derivatives_support.py:514` |
| `analysis/vol_surface.py` (1,271) | SVI surface, skew, butterfly/calendar arbitrage | `options_scanner.py:501-525`, weight 2.0 | scanner. April's "unwired" note is stale |
| `intelligence/supply_chokepoints.py` + `chain_contagion.py` + `sector_networks/*.yaml` (560 + 804 + 10 YAMLs) | Curated supply-chain DAG, 4-factor chokepoint score, BFS shock propagator with per-edge margin math | `news_contagion_listener.py:635` ← Hermes `:1662`; `contagion_backtest` scores it (Hermes `:1119`) → `postmortem.apply_contagion_feedback` | **the only mechanism with a wired path to execution** (`trading/contagion_to_ticket.py`) |
| `physics/news_energy.py` + `physics/momentum.py` (591 + 426) | News shock as force vector; sentiment velocity | `api/routers/physics.py`, `flows.py:1631,1643` | physics card |
| `analysis/backtest_scanner.py` (503) | Pair lead-lag scan with an LLM mechanism gate; writes `hypothesis_registry` `:365` | Hermes `:770`, `:798` | governance registry |
| `features/lab.py`, `features/per_signal_brier.py`, `features/regime_conditional_brier.py` (1,193 + 458 + 800) | Transform primitives; per-signal per-horizon Brier scorecards; regime-conditional calibration | `grid-worker` (`worker.py:681`); `signal_provenance.py:41,44` | every scorer; the conviction dial |

Built and tested but wired to nothing: `analysis/transfer_entropy.py` (the canonical
information-theoretic lead-lag engine — zero production callers; the hypothesis engine
uses its own `scan_lead_lag`), `analysis/lead_lag_backtest.py`, `features/importance.py`
(1,108 LOC writing `feature_importance_log`, which nothing weights on; the only importance
that influences a model is LightGBM's internal gain at `alpha_research/ensemble.py:127`).

### 3C. Conditions — *the regime the lever acts in*

Three independent regime classifiers exist and do not reference each other:

| Module | Used by | Scheduled |
|---|---|---|
| `intelligence/liquidity_regime.py` (`classify_current_regime`, 13 call sites) | the oracle — **this is the one that matters** | via consumers |
| `intelligence/hmm_regime_transitions.py` | regime router | daily 04:00 |
| `intelligence/regime/` (state_vector, classifier, episode_matcher, forecast; 1,706 LOC, zero tests) | `api/routers/intelligence_regime.py` only | **never** — `regime_state_vectors` gains a row only when a human hits `/regime` |

Plus `intelligence/financial_conditions_index.py` (6 h), `catalyst_aggregator.py`
(`proximity_score`, 18 call sites — dampens confidence near catalysts), and
`consensus_crowdedness.py` (crowded-trade dampener). The package uses `liquidity_regime`
+ `catalyst_aggregator`; the other two classifiers are candidates for consolidation.

### 3D. Conviction, provenance, and the capstone call

```
oracle/engine.py:2364  predict(ticker, horizon)                # ensemble + horizon bucket + regime router
        │
intelligence/decision_gateway.py:323  should_i_trade(engine, ticker, account_size_usd, horizon_days, instrument)
        ├── _run_prediction → oracle
        ├── intelligence/llm_red_team.py:199          adversarial critique
        ├── intelligence/signal_provenance.py:513     build_provenance_report → 14 multipliers
        ├── intelligence/pattern_library.py:253       5-year analog base rates
        └── intelligence/counterfactual_stress.py:270 stress under GFC / dotcom / SNB analogs
        │
trading/trade_ticket_generator.py:257  kelly_size_from_report   # sized off confidence_lower, MAX_KELLY_PER_TICKET = 0.05
```

`should_i_trade` is the single call that assembles the whole package into a decision. It
is reachable only from `api/routers/conviction.py` and is **not scheduled anywhere**. Its
`horizon_days` parameter accepts any integer; the oracle snaps it to the nearest of
`("1d","7d","30d","90d")` (`oracle/engine.py:185`).

The operator's worked example is `scripts/call_a_trade.py` (LONG TSM, 7 d) and the
offline map of every multiplier is `scripts/audit_conviction_stack.py`.

**The defect the pivot named, still present:** `compute_aggregate_conviction` multiplies
fourteen factors, each read as `float(x or 1.0)` (`signal_provenance.py:298-309`). A layer
whose upstream table is empty contributes exactly 1.0 — indistinguishable from "fully
checked and neutral". `trump_proximity.py:15-24` shows the correct contract (return
`None`, report `coverage`, never default). Until `signal_provenance` adopts it, HIGH
confidence cannot be trusted, which is the pivot's 11.9% finding.

### 3E. Positioning — *from conviction to a position*

| Stage | Module | Status |
|---|---|---|
| Sizing | `trading/trade_ticket_generator.py:257 kelly_size_from_report` (cap 5%, sized off `confidence_lower`); `trading/paper_engine.py:328 kelly_position_size` (half-Kelly, `max_fraction=0.25`) | live |
| Pre-registered invalidation | `intelligence/thesis_invalidation_monitor.py` (price_level / event / signal_flip grammar, `:19-41`) | live, hourly (`intelligence/scheduler.py:859`) |
| Journal (immutable) | `journal/log.py:44 log_decision`, `:142 record_outcome` | live |
| Paper | `trading/paper_engine.py` (`register_all_passed` `:308`, `_check_kill` `:240`: dies below 40% win / >5% DD / <0.5 Sharpe after 20 trades) | live |
| Hourly executor | `trading/signal_executor.py:84` — **closes at `_DEFAULT_EXPECTED_LAG = 1` day** (`:28`) | live via `intelligence/scheduler.py:133`; unusable for any multi-week thesis as-is |
| Circuit breaker / wallet | `trading/circuit_breaker.py:96 should_execute`; `trading/wallet_manager.py:205 check_risk`, `:236 kill_wallet` | live |
| Governance | `governance/registry.py:52 transition` (CANDIDATE → SHADOW → STAGING → PRODUCTION); `validation/gates.py` | **manual only.** Every `transition()` call site is an authenticated `POST /api/v1/models/{id}/transition` (`api/routers/models.py:162`) or a test. The models that actually change in production do so through `oracle/model_evolver.py` (`engine.py:1993`) and `oracle/trace_evolver.py` (`:2005`) every 6 h, which consult neither the registry nor the gates. **Two model lifecycles; only the un-gated one runs** |
| Execution — crypto perps | `trading/hyperliquid.py:199 open_position` | live |
| Execution — prediction markets | `trading/prediction_markets.py:35 PolymarketTrader`, `:331 KalshiTrader`; `prediction_pmxt.py:44` | live |
| Execution — Solana | `trading/solana/executor.py` | live |
| **Execution — equities** | **none.** `grep -rniE "alpaca|ibkr|tradier|schwab|ib_insync|robinhood"` finds only ticker lookup tables, seed data, and a plan doc | **missing** |

### 3F. Validation — *proving it*

| Engine | PIT-correct | Horizon | Shape | Status |
|---|---|---|---|---|
| `validation/backtest.py:23 WalkForwardBacktest.run_validation(predict_fn, n_splits, split_days, cost_bps)` | **yes** (`store.pit`, `as_of_date=era_end` `:103`) | **unbounded** (`split_days` free) | era-based, feature matrix + `predict_fn` | live, unscheduled — **the only PIT-correct, horizon-free validator** |
| `backtest/engine.py:288 PitchBacktester` (half-Kelly `:87`, regime-adjusted size `:111`) | yes | full history | regime + portfolio sim | live-as-library |
| `alpha_research/validation/gauntlet.py:218 run_gauntlet` (permutation p, subsample stability, decay) | **no** (reads `resolved_series` directly via `panel_builder`) | decay caps at **20 d** (`:157`) | cross-sectional | live-as-library; its header: *"0/7 runs achieved ROBUST in 2024 OOS"* |
| `alpha_research/strategies/rotation_variant_backtest.py`, `adaptive_rotation.py` | no | 7-day rebalance | group rotation | worker fan-out / unscheduled |
| `analysis/lead_lag_backtest.py:183 run_walk_forward` | claims PIT, **imports no PITStore** | pairwise | pairwise | dormant |
| `validation/gates.py` + `governance/registry.py` | — | — | promotion gates | live |

**No module can answer "I bought TICKER on date D on thesis T and held nine months — what
happened, out of sample, with costs."** Everything in `alpha_research/` is cross-sectional
or rotation; `validation/backtest.py` is era-based over a feature matrix. A single-name
hold simulator on top of `validation/backtest.py` is the one genuinely missing validator.

---

## 4. What is empirically proven vs asserted

| Claim | Source | Kind of evidence | Date |
|---|---|---|---|
| Prediction firehose: zero positive alpha vs SPY; 13-layer stack anti-correlates at high confidence (11.9% hit at HIGH) | `GRID-4-PRODUCT-PIVOT.md:3, :42, :155` | operator's own data analysis (numbers not reproduced in repo) | 2026-05-16 |
| 31,793 `oracle_predictions`, 1,312 scored; `per_signal_brier=1` row (aggregate only); `regime_brier=0`, `meta_learning=0` — blocked because the oracle write path never populates `signal_contributions` | `docs/reference/CONVICTION_STACK.md:33-43` | server state snapshot | 2026-04-14 |
| Paper trading: 8 OPEN trades, combined unrealized **−$194.31** | `docs/planning/ROADMAP.md:340` | ledger | 2026-04 |
| Calibration report (Brier/ECE) never run | `ROADMAP.md:337` (open checkbox) | absence | — |
| Gauntlet: **0/7 runs achieved ROBUST** in 2024 OOS; "CV consistency is the real blocker" | `alpha_research/validation/gauntlet.py:10` | module header | — |
| Adaptive Rotation paper: Sharpe 1.96, +19.76% vs SPY −2.51% (Oct 2025–Mar 2026); backtest Sharpe 1.10, 22.32% ann. (2018–2025) | `alpha_research/strategies/adaptive_rotation.py:6-7` | **docstring claim, no stored artifact** | — |
| QuantaAlpha OOS Sharpes 1.72 / 1.18 / 1.11 / 1.03 | `alpha_research/signals/quanta_alpha.py:7-11` | docstring claim | — |
| Sharpe 1.92 (QQQ + planetary stress Q2 + VIX<18, 112-day window) | `scripts/sharpe_199_final.py:6` | **explicitly cherry-picked**; flagged for deletion `PUNCH-LIST-2026-05-13.md:180` | — |
| Congressional trades are directional noise: lift −0.05%, n=407 | `intelligence/trust_scorer.py:201` (`_DIRECTIONAL_NOISE_SOURCES`) | replay | 2026-04-28 |
| Options scanner 7-signal weights self-tune from outcomes | `trading/options_tracker.py` → `scanner_weights` | mechanism exists; results not reported in repo | — |

**Honest read:** the only positive number with a mechanism behind it is the congressional
*negative* result. Every positive Sharpe in the repo is a docstring or a cherry-pick. The
pivot's realized-alpha truth gate is the missing instrument, and building it is the first
item in §7.

### 4.1 Why the numbers cannot yet be trusted even where they exist

Three structural defects sit under every metric above:

1. **Three disjoint pipelines.** The only scheduled path that opens a paper trade is
   Hermes → `scripts/rotation_paper_trader.py` → `adaptive_rotation.run_rotation` →
   `paper_engine.open_trade` (daily 17:00 UTC, `hermes_operator.py:2008`). It touches no
   oracle prediction, no journal, no Kelly, no slippage. The hourly `signal_executor`
   (`intelligence/scheduler.py:275`) is Kelly-sized but trades `paper_strategies` seeded
   from passed hypotheses, not predictions. The documented path — conviction stack →
   ticket — is HTTP-only. *The one that trades doesn't score; the one that scores doesn't
   trade.*

2. **Ledgers never reconcile.** `oracle_predictions` is scored and evolves weights every
   6 h (closed loop). `options_recommendations` → `scanner_weights` is closed. But
   `paper_trades` P&L feeds only position sizing and strategy kill — **never** oracle
   weights or signal trust — and `decision_journal.record_outcome` has no scheduled
   caller (verdicts arrive from a manual backfill script). The prediction outcome and the
   trading outcome are two ledgers that do not meet.

3. **Point-in-time is enforced by ~18 modules and bypassed by ~55.** `store/pit.py` is
   the only read path that applies `release_date <= as_of`. `oracle/engine.py` has zero
   `release_date` references in 2,877 lines and reads `resolved_series` and `raw_series`
   directly (`:747`, `:2071`, `:2106`, `:2114`); `intelligence/sentiment_scorer.py` has 11
   such reads, `global_levers.py` 10, `regime/state_vector.py` 3, and `signal_extractor`
   (a systemd service) reads `raw_series` without vintage. Any backtest built on those
   paths can see the future. CLAUDE.md calls PIT non-negotiable; the code does not
   currently honour it on the live prediction path. Full list: Appendix A, data plane, Q6.

Plus two that are simply embarrassing:

- `alpha_research/conviction_scorer.py:171,214-216` adds +1 to the macro SETUP layer when
  `planetary_stress_index` is between 0.5 and 4.0. Astrology is a scoring input to a
  production market endpoint (`api/routers/signals.py:194`).
- AstroGrid predictions are written **into `oracle_predictions`** — `oracle/publish.py:52-102`
  builds an `astrogrid:` id, tags `flow_context.source = "astrogrid"`, synthesises signals
  named `astrogrid_grid` and `astrogrid_mystical`, enriches with the full conviction
  context, and `INSERT`s (`:102`). They are separable only if every downstream reader
  filters on the prefix; the scorer, evolver, and calibration paths need to be checked for
  that filter before any Brier number is quoted.

And one that blocks any LLM-backed item in §7: `GRID_ALLOW_PAID_LLM` is read as
`getattr(settings, "GRID_ALLOW_PAID_LLM", False)` (`llm/router.py:224`) but the field is
not declared on `Settings` and `config.py:593` sets `extra="ignore"`, so the attribute
never exists and the flag **cannot be turned on** for `llm/router.py` (it does work for the
Hermes bridge, which reads `os.getenv`). Every tier is local-only in practice, and the
BATCH tier's `LLAMACPP_BATCH_BASE_URL` (`config.py:297`, port 8082) collides with
`grid-micro-classifier.service` on the same port.

---

## 5. The long-horizon path — "tailwinds that push companies stratospherically"

### 5.1 What exists, what runs, what is missing

| Stage | Module | Horizon | PIT | Runs? | Verdict |
|---|---|---|---|---|---|
| Thesis generation | `intelligence/market_edge_scanner.py` (1,356) | explicit `"1-3 months"`, `"2-6 months"` playbooks; source windows to **540 d** (`:19-24`) | no | **never** — API only (`intelligence_edges.py:12`) | the highest-value dormant asset for this goal |
| Base rates | `intelligence/pattern_library.py` (848) | 5-year lookback (`lookback_days=1825`), per-horizon | no | via `decision_gateway` only | live-as-library |
| Company profile | `intelligence/company_analyzer.py` (1,078) | 90 d insider window | no | API only | on-demand |
| Structural credit | `alpha_research/signals/credit_cycle.py` | ~6 months (`:26`) | partial (`release_date` present) | not registered by `signal_adapter` | dormant |
| Capstone | `intelligence/decision_gateway.py:323 should_i_trade(engine, ticker, account_size_usd, horizon_days, instrument="equity")` | any int; oracle snaps to `{1,7,30,90}` d | no | API only (`conviction.py:60`) | **the call that assembles everything; unscheduled** |
| Oracle | `oracle/engine.py` | horizon buckets + per-horizon calibration + regime router landed (migrations 0042–0045), **but every prediction is written with `_next_monthly_expiry()`** (`:1251`, `:1581`, `:2126`) → max ~35 d; `oracle_predictions` has no `horizon` column (`:608`) | no | every 6 h | **90 d bucket structurally unfillable** |
| Validation | `validation/backtest.py:23 WalkForwardBacktest` | unbounded (`split_days`) | **yes** | unscheduled | only PIT-correct, horizon-free validator |
| False-discovery | `alpha_research/validation/gauntlet.py:218` | decay caps at 20 d | no | manual script only | noise test, not a horizon test |
| Invalidation | `intelligence/thesis_invalidation_monitor.py` | any | — | hourly | live |
| Thesis scoring | `intelligence/thesis_tracker.py:205` | **3 days vs SPY** | no | API | mismatched to the horizon |
| Executor | `trading/signal_executor.py:28 _DEFAULT_EXPECTED_LAG = 1` | **1 day** | — | hourly | would liquidate a multi-month thesis overnight |
| "100x Core" | `strategy/ten_year_portfolio.py` | 10-year chart quality vs QQQ | no | API (`ten_year_portfolio.py`) | 19 hand-typed core tickers + 20 hand-typed frontier tickers with theme strings (`:25-46`); `score_candidate` `:329` is chart metrics × profile weights; no tailwind data, no kill criteria |

**Genuinely missing** (no module does it; grep-verified):

- A **single-name multi-month hold simulator** — "bought TICKER on D on thesis T, held N
  months, out of sample, with costs." Everything in `alpha_research/` is cross-sectional
  or rotation; `validation/backtest.py` is era-based over a feature matrix.
- A **long-horizon prediction record.** Nothing longer than ~35 days has ever been written.
- **Fed reaction function estimator** (April gap #16, top-5 item), **structured-flow
  calendar** (#17), **insider cluster detector** (#22) — still absent.
- **A structural-growth score.** "Secular/tailwind/supercycle" appears in ~20 YAML
  annotation strings and one 30-day momentum check labelled `commodity_supercycle`
  (`analysis/flow_thesis_data.py:1252`). There is no CAGR-regime classifier, no capex-cycle
  phase detector, no multi-year trend-persistence feature.

### 5.2 The tailwind data — ingested, scored, unreachable

Nine of ten structural families are ingested and four have company-level scoring written.
Three defects keep them out of the decision path:

| Defect | Where | Effect | Fix size |
|---|---|---|---|
| **Exact-key entity map.** `EntityMap.get_feature_id` is `SEED_MAPPINGS.get(series_id)` — 520 literal keys, no prefix or pattern | `normalization/entity_map.py:884-894` | Every per-entity series (`GOV_CONTRACT:{agency}:{ticker}:{amount}`, `INSIDER:{ticker}:{name}:{type}`, `13F:{cik}:{ticker}:{action}`, `LEGISLATION:*`, `EXPORT_CONTROL:*`, `DARKPOOL:*`) can never become a `resolved_series` feature, so `PITStore`, `oracle/`, `alpha_research/`, and `features/alpha101.py` cannot see any company-level structural signal. 91 registered pullers write into this void | a pattern table in `entity_map` (medium) |
| **Rolling windows starve long-horizon consumers.** `gov_contracts.py` pulls 7 days (`:42`), `legislation.py` 7–30, `export_controls.py` 90, `insider_filings.py` 1 | pullers | `market_edge_scanner` looks back 540 days into tables filled a week at a time | one-time backfill (`pull_all(days_back=1095)`) — small |
| **Nine prefix-mismatch bugs** where a mapping exists but the puller writes a different key (EIA 16 features, weather 10, Binance 8, NY Fed SOMA 6, OECD 3, DeFiLlama 2, Nowcast 2, USPTO 2, AlphaVantage) | `entity_map.py` vs each puller | ~49 features from code that already runs, silently dropped | nine one-line edits |

Best five families by history × wiring for a company-level thesis (from the data audit):
**government contracts** (540-day scorer window, three consumers, but 7-day pulls),
**patents** (the only end-to-end PIT-correct family, 50 years — but CPC-class not
assignee), **legislation** (deepest consumer fan-out, multi-quarter lifecycles),
**export controls** (highest signal per event; the module's own docstring names the NVIDIA
thesis), **supply-chain chokepoints + contagion** (the only mechanism with a wired path to
a ticket). Also: `ingestion/flow_materializer.py` (740 LOC) has **zero callers** and alone
gates five empty tables including `dark_pool_weekly` and `etf_flows` — the #4 and #5
most-referenced sources in the scorers.

### 5.3 Shortest path with existing modules — one thesis, validated, sized, papered

1. `market_edge_scanner.PlaybookBlueprint` over `_target_universe()` — the only generator
   that emits explicit multi-month horizons.
2. `pattern_library.build_state_vector` → `query_historical_states(lookback_days=1825)` →
   `compute_base_rate` — "this setup, five years of analogs, per-horizon win%".
3. `decision_gateway.should_i_trade(engine, ticker, account_size_usd, horizon_days=180,
   instrument="equity")` — prediction, red-team, provenance (with the 1.0-default caveat),
   base rates, stress. Accept the 90 d calibration bucket.
4. `validation.backtest.WalkForwardBacktest(db_engine, PITStore(...)).run_validation(
   predict_fn, n_splits=6, split_days=180, cost_bps=…)` — the only PIT-correct,
   horizon-free check.
5. `gauntlet.run_gauntlet` as a noise test only (require `permutation_p < 0.05`,
   `subsample_stability > 0.5`); ignore its 20-day decay.
6. `trade_ticket_generator.kelly_size_from_report` (5% cap, `confidence_lower`).
7. Write the invalidation into the journal `metadata` **before** the position exists
   (`thesis_invalidation_monitor` grammar, `:19-41`).
8. `paper_engine.open_trade` behind `circuit_breaker.should_execute`.
9. `journal.log_decision` now; `record_outcome` at resolution.
10. **Do not** route through `signal_executor` until `_DEFAULT_EXPECTED_LAG` is
    per-strategy.

Two parameter edits make this path honest: an explicit expiry at `oracle/engine.py:1251`
and `:1581` instead of `_next_monthly_expiry()`, and a per-strategy lag in
`signal_executor.py:28`.

---

## 6. The hypothesis engine — diagnosis, fix, runbook

**What it is.** `intelligence/hypothesis_engine.py` (2,305 LOC): deterministic (numpy/
scipy + SQL, **no LLM anywhere** — the paid-LLM gate is not the cause). Four pattern types
(`lead_lag`, `convergence`, `volume_anomaly`, `actor_shift`), 14-name kill taxonomy (10
reachable), Beta-posterior scoring, thesis/antithesis pairs. Writes `discovered_hypotheses`,
`hypothesis_postmortems`, `hypothesis_boost_log` via its own `ensure_tables()` — not
Alembic. Reads `oracle_predictions` for price evidence (`:1385`) — the table the pivot
planned to drop; if that table goes, every convergence hypothesis silently degrades to
`inconclusive` (`:1394-1397`, no log).

**Why it looked dead — three causes, ranked.**

1. **Timeout inversion (mechanical, fixed on this branch, commit `13fccfc`).** The 30-min
   scorer ran first inside `run_intelligence_tasks()` with `max_runtime_s=600`; the step
   itself was capped at 360 s (`hermes_operator.py:97` vs `:102`), and `_run_intel_task`
   adds no timeout of its own (`scripts/hermes_fixers.py:1681-1714`). Twice an hour the
   step hit its cap, `_run_with_timeout` orphaned the worker (`:147-163`), the whole
   intelligence step was recorded as `{"timeout": True}`, and the 02:00 daily block —
   `auto_discover()` at `:809-823` — ran only inside orphaned threads whose results were
   never recorded. Fix: scorer budget 600 → 240 s, step budget 360 → 900 s, named
   `DAILY_INTEL_BATCH_OBSERVED_S`, and `tests/test_hermes_timeout_budgets.py` pins
   `scorer + daily <= step < cycle`. 17 tests pass locally.
2. **The UI reads the wrong table.** `api/routers/discovery.py` queries `hypothesis_registry`
   at `:175`, `:207`, `:271`, `:374`, `:423` — the model-governance table that `autoresearch`
   and `backtest_scanner` write. The engine's output is in `discovered_hypotheses`, served
   by `GET /api/v1/surfacer/candidates` and the canvas `/predict` and `/expand` routes.
3. **Documented freeze, never enacted.** `GRID-4-PRODUCT-PIVOT.md:47` says freeze;
   Hermes kept calling it. Doc/code divergence, not a gate.

**Runbook (on `grid-svr`).**

```bash
# 0. Ground truth — nothing here is destructive
systemctl status grid-hermes
grep -E "Running daily intelligence batch|Hypothesis discovery|intelligence_tasks' timed out" \
     /data/grid/logs/hermes-operator.log | tail -30
psql -U grid -d griddb -c "
  SELECT status, count(*), max(created_at) AS latest_created, max(last_tested) AS latest_tested
  FROM discovered_hypotheses GROUP BY status;
  SELECT to_regclass('oracle_predictions');"
#    latest_tested fresh but latest_created stale  → cause 1 confirmed
#    to_regclass NULL                              → cause 3's table drop happened; fix _check_ticker_move first

# 1. Deploy the timeout fix (never raw scp/rsync)
python3 scripts/deploy.py --snapshot --restart --smoke scripts/hermes_operator.py tests/test_hermes_timeout_budgets.py

# 2. Run the engine by hand — fastest proof, no daemon involved
cd /data/grid_v4/grid_repo && set -a && source .env && set +a
python3 intelligence/hypothesis_engine.py stats
python3 intelligence/hypothesis_engine.py discover      # generates; self-migrating via ensure_tables()
python3 intelligence/hypothesis_engine.py score-all
python3 intelligence/hypothesis_engine.py stats         # confirm the delta

# 3. Watch the scheduled path (next 02:00–02:10 UTC, or catch-up any hour after)
grep -E "Intel task 'active_hypo_scoring' completed|Hypothesis discovery: .* new" /data/grid/logs/hermes-operator.log | tail

# 4. Read results where they actually are
curl -s localhost:8000/api/v1/surfacer/candidates?limit=16 | jq .
python3 intelligence/hypothesis_engine.py postmortems
```

No env flags are required for the core engine beyond DB credentials. The optional LLM
second-opinion layer is `HERMES_HYPO_LLM_ENABLED=true` (`config.py:182`, default false);
`GRID_ALLOW_PAID_LLM` is read from the environment, not `Settings`
(`llm/router.py:224`).

**Horizons it supports vs what tailwinds need.**

| Pattern | Window today | Where |
|---|---|---|
| `lead_lag` | 1–30 d (`max_lag_days=30`) | `:251`, `:291` |
| `convergence` | 14 d hard-coded | `:1198` |
| `volume_anomaly` | 30 d hard-coded | `:1236` |
| `actor_shift` | 45 d hard-coded | `:1286` |
| expiry kill | 2 × window → max 90 d | `:945` |
| retention | purged 90 d after invalidation | `:1919` |
| `SCORING_WINDOW_DAYS = 90` | **declared, never referenced** | `:48` |

A multi-month tier needs: configurable `window_days` per pattern type with 90/180/365
values; lead-lag lags beyond 30 d; price evidence from `resolved_series` through
`PITStore` instead of `oracle_predictions`; retention past 90 d; non-punitive expiry
(a slow tailwind reads `inconclusive` for months by design); and the horizon vocabulary
`api/routers/surfacer.py:358-372` already defines (`swing ≤10 d / multi_week ≤45 d /
multi_month`). That is an extension of the engine, not a new module.

---

## 7. The ordered plan — prove it, then position

Every item extends an existing module unless marked **NEW**; `scripts/pre_create_check.py`
before any new file; `scripts/deploy.py --smoke` to ship. Days are one engineer or one
agent wave.

> **Sprint 1 status (2026-09-10, branch `claude/finance-visualization-stack-tpDSI`):**
> landed — T0.1 realized alpha (`alpha_research/realized_alpha.py`, migration `0057`,
> `GET /api/v1/alpha/realized`, daily 06:30), T0.3 NULL propagation with layer and
> evidence coverage gating HIGH, T0.5 astrology quarantine (PSI removed from the SETUP
> layer; `astrogrid:` rows excluded from calibration, model/trace evolution and
> postmortem selection — scoring left intact so AstroGrid's own scoreboard still works),
> T0.6 for the live predictor's feature gather (`OracleEngine._gather_signals` through
> `PITStore`, `LATEST_AS_OF`; price lookups unchanged), the V5 R1.1–R1.3/R1.6 live canvas
> (contracts `pg_notify` in the audit transaction → API listener → SSE → node pulse),
> and R3.4 canvas in the operator tab bar. Not yet: T0.2 journal-outcome scheduling, T0.4
> calibration read-out (needs the server), T0.7 paid-LLM flag, T1.x data reachability.
> Deploy is the operator's step (`scripts/deploy.py --snapshot --restart --smoke`).

### T0 — Truth gates first (3–4 days). Nothing else can be believed until these exist.

| # | Item | Module | Verification |
|---|---|---|---|
| T0.1 | **Realized alpha** — rolling 5/10/20/60/90/180 d alpha vs SPY at 5 bp/side for every `decision_journal` and `paper_trades` row; daily in `intelligence/scheduler.py`; one table, one endpoint. The pivot's §8 truth gate, never built | **NEW** `alpha_research/realized_alpha.py` (`pre_create_check "realized alpha"` first) | unit tests on synthetic paths; server backfill; the number goes on the front page |
| T0.2 | Close the journal loop on a schedule — `record_outcome` currently has no scheduled caller | wire `scripts/backfill_journal_verdicts.py` logic into the daily block | verdict coverage query |
| T0.3 | **NULL propagation** in `compute_aggregate_conviction` — return `coverage`; missing layer → `None`, never `1.0`; never surface HIGH below a coverage floor. Copy the contract from `trump_proximity.py:15-24` | `intelligence/signal_provenance.py:234-320` | extend the 5 existing test files; `scripts/call_a_trade.py` still runs |
| T0.4 | Read the calibration that already runs — `oracle/calibration.py` is scheduled 02:15 daily (`intelligence/scheduler.py:326`) and persists to `oracle_calibration_history` (migration 0043). Report per-horizon Brier/ECE; confirm whether `oracle/prediction_context.py` has closed the `CONVICTION_STACK.md` "known gap" | query + one-page report | numbers, dated |
| T0.5 | Quarantine astrology from market scoring: remove the PSI input from `conviction_scorer`; confirm every reader of `oracle_predictions` in `oracle/engine.py` (`score_expired_predictions`, `evolve_weights`), `oracle/calibration.py`, and `intelligence/postmortem.py` excludes `source = 'astrogrid'` / ids prefixed `astrogrid:` | `alpha_research/conviction_scorer.py:171,214-216`; `oracle/publish.py:52-102` readers | tests; a count query on the server showing the split |
| T0.7 | Declare `GRID_ALLOW_PAID_LLM: bool = False` on `Settings` so the flag is real; fix the 8082 port collision (`LLAMACPP_BATCH_BASE_URL` vs `grid-micro-classifier`) | `config.py`, `llm/router.py:224`, `config.py:297` | `tests/test_config.py`; router unit test |
| T0.6 | **PIT on the live predictor.** Route `oracle/engine.py:747,2071,2106,2114` through `PITStore` (or add `release_date <= as_of` predicates); then `sentiment_scorer`, `global_levers`, `regime/state_vector`, `signal_extractor` | `store/pit.py` is the canonical read path | `assert_no_lookahead()` on the inference path; `tests/test_pit.py` |

### T1 — Let the data reach the scorers (2–3 days, parallel with T0)

| # | Item | Module |
|---|---|---|
| T1.1 | Nine prefix fixes → ~49 features from pullers that already run | `normalization/entity_map.py` (or the nine pullers) |
| T1.2 | **Pattern mapping** in `EntityMap.get_feature_id` — exact key first, then a pattern table (`GOV_CONTRACT:*:{ticker}:*` → `gov_contract_{ticker}_usd_30d` etc.) so company-level structural series become PIT features | `entity_map.py:884-894`; add tests to `test_resolver*.py` |
| T1.3 | Backfill: `gov_contracts.pull_all(days_back=1095)`, legislation, export controls, Form 4 bulk (Harvard Dataverse, `DATA_SOURCES_CATALOG.md:126`) | pullers; one-off scripts under `scripts/` |
| T1.4 | Wire `ingestion/flow_materializer.py` (zero callers) into the daily block — it gates `dark_pool_weekly`, `etf_flows`, `insider_trades`, `congressional_trades`, `junction_point_readings` | `scripts/hermes_operator.py` daily block |
| T1.5 | Kill-or-wire `grid-realtime.service` (62 symbols → `realtime_candles` → one dormant reader, 90-day TTL) | decision; mask the unit or schedule `ingestion/crypto_signals.py` |

### T2 — Turn on the long-horizon engine (4–6 days)

| # | Item | Module |
|---|---|---|
| T2.1 | Explicit horizon on predictions: `horizon_days` column on `oracle_predictions` (migration with the `_TEMPLATE.sql` GRANT footer); `predict(horizon)` writes `expiry = as_of + horizon` instead of `_next_monthly_expiry()`; emit a weekly 90 d batch over a defined universe so the 90 d bucket fills | `oracle/engine.py:608,1251,1581,2126`; `migrations/` |
| T2.2 | Schedule the generators: weekly `market_edge_scanner` playbooks → `decision_gateway.should_i_trade(horizon_days=90|180)` → persist the provenance report | `intelligence/scheduler.py`; `intelligence/decision_gateway.py` |
| T2.3 | **Single-name hold validator** — `run_hold_validation(ticker, entry_dates, hold_days, cost_bps)` on top of `WalkForwardBacktest` using `PITStore` | `validation/backtest.py` (new function, same module) |
| T2.4 | Hypothesis engine multi-month tier — configurable windows, PIT price evidence, longer retention, non-punitive expiry (§6) | `intelligence/hypothesis_engine.py` |
| T2.5 | Per-strategy horizon in the executor — replace `_DEFAULT_EXPECTED_LAG = 1` with a `paper_strategies` column | `trading/signal_executor.py:28`, `trading/paper_engine.py` |
| T2.6 | Ship rule: every signal passes a 30-day walk-forward Brier holdout (Tier-A rule, kill below Δ 0.2%) and the gauntlet noise test before it can reach a ticket | `validation/backtest.py`, `gauntlet.py` |

### T3 — Paper with pre-registration (60–90 calendar days, runs itself)

- Every thesis journaled at emission with a frozen invalidation
  (`thesis_invalidation_monitor`, already hourly). No parameter retuning until 30 trades
  complete (pivot §8.1 anti-fool guard).
- Install the review job that already exists: `scripts/paper_trading_review.py` is defined
  in `grid_cron.sh:127` but `setup_cron.sh:41-47` installs only 7 of the cron entries and
  this is not one of them (nor are `bottom_detector_monitor`, `run_psi_oracle`, or the
  inline `sentiment`/`flows`/`forecast`/`thesis-snapshot` jobs).
- `realized_alpha` on the front page. Kill criteria from the pivot: pause and root-cause if
  60 d alpha after 90 days is under +1% annualized; kill the layer if 180 d alpha is
  negative.
- Reconcile the ledgers: paper P&L → per-signal Brier (`features/per_signal_brier.py`)
  and source trust, so the trading outcome finally trains the scorer.
- Wire `validation/execution_sim.py` (dormant, 542 LOC) into paper P&L so the numbers are
  not frictionless.

### T4 — Real money (operator decision, after T3 evidence)

- **Equities are manual.** There is no broker adapter. If automation is wanted, that is the
  one genuinely new build in this plan — an Alpaca/IBKR adapter behind the existing
  `circuit_breaker`, `wallet_manager`, and 5% Kelly cap. `pre_create_check` will confirm
  nothing exists.
- **Persist the global circuit breaker.** `oracle/risk.py:426 get_global_circuit_breaker`
  is a process singleton; an API restart clears the kill switch and cooldowns. Back it with
  a table before any live capital.
- **One model lifecycle.** Either route `model_evolver`/`trace_evolver` changes through
  `governance/registry.transition` + `validation/gates.py`, or retire the registry. Today
  the governed path is manual and the ungoverned path runs every 6 h.
- Keep: Kelly sized off `confidence_lower`, 5% per ticket, half-Kelly with 0.25 cap on
  paper, `HYPERLIQUID_TESTNET=True` and `SOLANA_LIVE_TRADING=False` defaults.

**Total:** T0+T1+T2 ≈ 2–3 weeks of build, then a 60–90-day paper window that produces the
first defensible number GRID has ever had.

---

## 8. What does not matter — the ~900

By slice, from the audits (full tables in Appendix A). "Dormant" means coherent code with
no live caller; "exhaust" means narration, output, scaffold, or research spike.

| Slice | Dormant | Exhaust / duplicate | Largest single items |
|---|---|---|---|
| `intelligence/` (182) | 11 (7,201 LOC) | 12 + 12 thin adapters | `actor_discovery.py` 3,530 (zero callers); `market_edge_scanner.py` 1,356 (UI-only); `forensic_journal.py` 672 (superseded); `self_learning_loop.py` 594; `edge_signals.py` 585; `llm_harness.py` 574; `grand_orchestrator.py` 528; `prediction_calibration.py` 521 (zero importers) |
| decision core (135) | 28 | 29 + 3 | **`trading/solana/` 17 of 21 modules, 5,240 LOC, fully tested, never ticked**; `trading/strategy151.py` 980; `trading/prediction_pmxt.py` 404; `inference/tuning.py` 479 (would optimize Kelly — zero importers); `validation/execution_sim.py` 542; `alpha_research/ensemble.py` (competing LightGBM stacker); `derivatives/` SPA (20 files, un-built, un-deployed); `backtest/paper_trade.py` (second paper-trade system writing a different table) |
| `analysis/` `physics/` `features/` … (84) | 14 | 4 + 3 | `mcp_server.py` 1,271 (no unit, no importer); `agents/` 7 of 9 (gated off by `AGENTS_ENABLED=False`); `analysis/money_flow.py` 1,770 (same-named duplicate of `money_flow_engine.build_flow_map`); `features/importance.py` 1,108 (writes a table nothing reads); `analysis/transfer_entropy.py` + `lead_lag_backtest.py` (canonical, tested, uncalled) |
| data plane (181 pullers) | 12 unregistered; 91 registered-but-unmapped | 10 duplicates (`ag_commodity_futures`, `fx_rates`, `h8_bank_balance`, `mmf_composition`, `nasa_firms_puller`, `kalshi`, `marketwatch_news`, `tiingo_news`, `wikipedia_pageviews_puller`, `polygon_puller`, `dbnomics`, `noaa_space_weather`); 5 celestial | `store/astrogrid.py` 2,802 (AstroGrid only); `world_news.py` 45 mapped features with zero scorer reads; two **paid** Tiingo pullers (`tiingo_news_pull`, `tiingo_fundamentals_pull`) whose outputs are unmapped; congressional trades — 34 consumer references, proven noise (`trust_scorer.py:200-202`) |
| periphery (416) | 31 + **43 dead scripts** (6,972 LOC; 23 are `run_*.py` wrappers around pullers that were never scheduled) | 75 + 12 | **`astrogrid_api/`** — 9 files, ~4,900 LOC, a diverged fork of `api/routers/astrogrid*` + `api/auth.py` (199 changed lines in `astrogrid_helpers` alone), zero tests, **live on :8010**; `subnet/` 5,381 LOC, zero tests, one import to the rest of the tree and it is from a dead file; 7 registered routers with zero frontend callers (1,330 LOC); `orchestration/event_bus.py` + `grid_worker.py` (superseded); `ollama/router.py` (superseded by `llm/router.py`); `gemma/training/` (no unit, no cron); 357 generated files / 37 MB committed under `outputs/`; a cgroup override for a `grid-tao-miner.service` that does not exist |

None of these need deleting to make the package work. They need to stop being counted.

---

## Appendix A — Per-directory audit summaries

Condensed from the eight audits; every per-module row with `file:line` is in the audit
transcripts. Verdicts: CORE (on the lever→position path and live), SUPPORT (live
infrastructure the core needs), DORMANT (coherent, unreachable), EXHAUST (output,
narration, scaffold, spike), DUPLICATE (canonical named).

### A.1 `intelligence/` — 182 files, 104,370 LOC

158 import-reachable from nine systemd roots; 11 orphaned; 12 narration-only. Lever core
(15) and second ring (11) in §3A. `regime/` (4 modules, 1,706 LOC) has zero tests and
writes `regime_state_vectors` only on API calls. `spider/` runs as an hourly oneshot
(≤10 rounds, ≤50 new edges). 14 registered adapters are consumed all-or-nothing via
`ALL_ADAPTERS`; 12 are ~80% boilerplate. `hermes_operator.py:730` claims `power_mapper`
was deleted; it was not. Three regime classifiers do not reference each other.

### A.2 `analysis/` `physics/` `features/` `knowledge/` `verification/` `agents/` `hyperspace/` + top-level — 84 files, 32,700 LOC

CORE 26 / SUPPORT 41 / DORMANT 14 / EXHAUST 4 / DUPLICATE 3. The options-lever stack
(vol surface → dealer gamma → vanna/charm → scanner → recommender) is scheduled daily via
`ingestion/scheduler.py:1317`. `features/registry.py` has zero importers (everything
queries `feature_registry` by SQL). `hyperspace/` is 4-of-5 live; `hyperspace/research_agent.py`
name-collides with the live `analysis/research_agent.py` (punch-list P1). `dashboard.py`
duplicates `api/main.py` on :8080 with no unit.

### A.3 Decision core — 135 files, 41,249 LOC

CORE 29 / SUPPORT 46 / DORMANT 28 / EXHAUST 29 / DUPLICATE 3. Eleven `oracle/` modules
are core and scheduled (engine, aggregator, calibration, evolver, trace_evolver,
regime_router, uncertainty, disagreement, prediction_context, hallucination_guard, risk).
`oracle/publish.py` is a second `oracle_predictions` writer slated for merge.
`trading/contagion_to_ticket.py` duplicates the recommender's Kelly math ("highest risk
duplicate", `MODULE_DEDUPE_PLAN.md:281`). 21 hard gates between a signal and an order are
inventoried; gates 5–11 guard Solana code nothing runs. Five conviction implementations;
the canonical one (`signal_provenance`) is the only one not on a schedule.

### A.4 Data plane — 181 pullers + `normalization/` `store/`

Funnel: 181 → 146 registered → 55 mapped → 37 consumed by a scorer. Top sources by scorer
references: insider filings (42), yfinance/Tiingo prices (38), congressional (34, noise),
institutional flows (26), dark pool (17, table empty — FINRA 400), lobbying (14), FRED
(25 total; `vix_spot` alone 13), Unusual Whales (13), smart money + trending news (10),
prediction odds (7). Documented-empty or broken: `dark_pool.py`, `flow_materializer.py`,
`margin_debt.py`, `company_profiles_puller.py`, four `skip_runtime` stubs
(`crypto_etf_flows`, `hyperliquid_puller`, `onchain_rpc`, `whale_alert`), `sec_xbrl_shares`
(silently unrun 04-12 → 05-17), `YF:ICLN:close` stale since 2017. `resolver.py` is the only
write path into `resolved_series` (three pullers bypass it); `store/pit.py` is the only
lookahead-safe read path and is bypassed by ~55 scorer modules (§4.1).

### A.5 Periphery — `api/` `alerts/` `llm/` `contracts/` `events/` `orchestration/` `subnet/` `astrogrid_api/` `scripts/` — 416 modules

SUPPORT 118 / SURFACE 148 / DORMANT 31 / EXHAUST 75 / DUPLICATE 12 / DEAD 43. 97 router
files: 74 mounted (71 via the loop at `api/main.py:347-419`, all `required=False` except
`astrogrid` — a failed import is a WARNING and the router silently vanishes), 23 mounted
by aggregators, `canvas_core.py` unmounted. Ten routers carry the decision core (`oracle`,
`signals`, `signal_registry`, `journal`, `trading`, `options`, `conviction`,
`intelligence_deepdive`, `regime` + `intelligence_regime`, `contagion` + `trade_tickets`).
LLM: `llm/router.py` has 43 importers; all four tiers resolve local-only because the paid
flag is unreachable (§4.1); `gemma` is in every chain but gated off; `ollama/` is still
live (20 importers); CLAUDE.md's "Nemotron" model names appear nowhere else in the repo —
`config.py` names Qwen3-32B / Qwen3.6-35B / qwen3-14b and `scripts/start_llamacpp.sh`
loads Hermes-3-8B. Alerts: the 100x digest (4 h, 765 LOC, zero tests), daily digest,
health, supply-chain (976 LOC, zero tests), and contract-driven pages on HIGH anomalies
and regime transitions are live; the iMessage price-alert path (`scripts/sd_imessage.py`
← `check_price_alerts.py`) is built and never scheduled; `alerts/scheduler.py`'s docstring
claims it is started from `api/main.py` and it is not. `contracts/` is fully live with 11
handlers (trust, oracle weights, calibration, journal mirror, anti-signals, regime, alerts,
edges, trade outcomes, pull lifecycle). `subnet/` is an isolated island. Scripts: 7
daemons, 36 scheduled, 58 one-offs, 14 research spikes, 41 operator tools, 43 dead.

## Appendix B — Documents this audit contradicts

| Document | Claim | Reality |
|---|---|---|
| `docs/reference/CODEBASE_MAP.md:47-48` | `actor_discovery.py` is load-bearing | zero callers |
| `.claude/CODEBASE_INDEX.md` (corrected in PR #395) | React Flow canvas, live AGE | Sigma.js; AGE dormant |
| `docs/reference/CONVICTION_STACK.md` "Known gap" | oracle write path lacks `signal_contributions` | `oracle/prediction_context.py` exists and is wired — verify on server (T0.4) |
| `docs/planning/ROADMAP.md` "Palantir Test: all 7 ✅" | levers, timing, causation all answered | code exists; the loop from answer → position → outcome → weight is not closed (§4.1) |
| `GRID-4-PRODUCT-PIVOT.md` "freeze `hypothesis_engine`" | frozen | still scheduled; was starved, now fixed |
| `docs/AGENT_PROMPT_TEMPLATE.md:73` | `tests/test_no_sql_fstrings.py` guards regressions | file does not exist |
| `scripts/hermes_operator.py:730` | `power_mapper` deleted | still on disk with a test |
| `DATA_SOURCES_CATALOG.md:89-122` | congressional trading is a MUST-FIND priority | `trust_scorer` blocklists it as directional noise (lift −0.05%, n=407) |
| `CLAUDE.md:74-75` | LLM stack is Nemotron-Cascade-2 30B (:8080) + Nemotron-3-Super-120B (:8081) | neither name exists in the repo; `config.py:265-286` names Qwen3-32B, Qwen3.6-35B, qwen3-14b; `scripts/start_llamacpp.sh:97` loads Hermes-3-8B |
| `llm/router.py:224` + `.env.example` | `GRID_ALLOW_PAID_LLM` enables paid providers | field undeclared on `Settings`; the getattr always returns `False` |
| `alerts/scheduler.py` docstring | started from `api/main.py` at boot | zero importers |
| `docs/SERVER-SERVICES.md:74-77`, `llm/autoresearch/registry.py:123-126` | micro-models are `gemma-4-e4b` | the `.service` files load `e2b` |
| `docs/MODULE_CATALOG.md:5` | 405 modules | 982 production modules (1,068 files) |
