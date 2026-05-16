# GRID 4-Product Pivot Plan

**Status:** Active plan. Authored 2026-05-16 after a day of data analysis exposed that the existing prediction firehose has zero positive alpha vs SPY and the 13-layer conviction stack anti-correlates with reality at high confidence.

---

## 1. Bottom Line Up Front

GRID stops being a 2.27M-prediction/day options firehose and becomes a **four-product trading intelligence platform** built on the parts of the codebase that already work: the actor network, deep-graph traversal, FARA/lobbying/congressional/campaign-finance pullers, and `signal_provenance`. The 117-variant oracle ensemble, the 13-layer conviction stack (which anti-correlates with reality at high confidence), and `derivatives/`-driven options recommendations are **frozen on 2026-05-17** and slated for deletion by 2026-05-31. **Phase 0 ships in 24 hours: a Trump-Proximity Score (TPS) per ticker**, computed from the existing FARA + lobbying + campaign_finance + congressional + actor_network graphs, surfaced as a single ranked daily watchlist view in the PWA. That single artifact proves the pivot is real, requires no new data ingestion, and gives the trading layer its first defensible signal.

## 2. Subtraction List — What Gets Killed or Frozen

Concrete components to **freeze on 2026-05-17** and **delete by 2026-05-31** unless explicitly promoted.

### 2.1 Prediction firehose — DELETE

- `oracle/engine.py` 5-model competition path — freeze, then delete after data export
- `oracle/model_factory.py`, `oracle/model_evolver.py`, `oracle/run_cycle.py`, `oracle/psi_model.py` — delete entire competing-model orchestration. The 117 variants produced zero positive alpha
- `oracle/scoreboard.py`, `oracle/calibration.py` — delete; replaced by a single P&L-tracked journal
- `scripts/generate_crypto_predictions.py`, `scripts/score_crypto_predictions.py`, `scripts/score_oracle_trades.py`, `scripts/baseline_predictions.py`, `scripts/seed_astrogrid_prediction_corpus.py`, `scripts/run_psi_oracle.py` — delete
- Cron/systemd: disable any timer invoking `run_cycle`, `psi_oracle`, `score_oracle_trades`, or `generate_crypto_predictions`. Audit on grid-svr: `systemctl list-timers | grep -i oracle`. Mask the units.
- `trading/prediction_backtest.py`, `trading/prediction_markets.py`, `trading/prediction_pmxt.py` — delete (paper-trading on broken predictions is anti-information)
- DB tables to drop after export: `oracle_predictions`, `oracle_scoring`, `oracle_model_variants`, anything under `psi_*`. Export to parquet first, then drop.

### 2.2 Broken conviction stack — AMPUTATE

The 13-layer stack in `intelligence/signal_provenance.py` is mathematically rotten because every layer defaults to `1.0` on failure, so a missing upstream silently neuters the layer.

- **Keep** `intelligence/signal_provenance.py` as the scaffold (correct abstraction — provenance per signal)
- **Delete** these conviction layers entirely:
  - `intelligence/confidence_bucket_tracker.py`
  - `intelligence/null_hypothesis_forecaster.py`
  - `intelligence/meta_learning_matrix.py`
  - `intelligence/contra_indicator_ensemble.py`
  - `intelligence/short_squeeze_composite.py`
  - `intelligence/prediction_market_arbitrage.py`
  - `intelligence/signal_convergence_scanner.py`
  - `intelligence/historical_scenario_library.py`
  - `intelligence/signal_cooccurrence.py`
  - `intelligence/llm_red_team.py`
- **Keep two layers only** (under new aggregator): `intelligence/cross_reference.py` (genuine lie-detector) and a new **TPS layer** built on `intelligence/actors/`
- Rewrite `compute_aggregate_conviction` to **propagate NULL on missing layers** rather than silently defaulting to `1.0`. **This is the single most important amputation** — it's the root cause of "11.9% hit rate at HIGH confidence." If a layer can't compute, the signal is not surfaced.

### 2.3 Fake-alpha and decorative layers — FREEZE then DELETE

- `intelligence/grand_orchestrator.py` — delete. Orchestration without measurable edge is theater.
- `intelligence/hypothesis_engine.py` (2,137 LOC) — freeze for now. Wrong abstraction for a solo operator generating hundreds of hypotheses.
- `intelligence/self_learning_loop.py` — delete. Was training on broken outcomes per PR #173/#175.
- `intelligence/pattern_engine.py`, `intelligence/pattern_library.py` — freeze; no demonstrated edge
- `intelligence/global_levers.py` (2,258 LOC) — freeze; folded into research track only if needed
- `derivatives/` — freeze entire directory
- `trading/options_recommender.py`, `trading/options_tracker.py`, `trading/strategy151.py`, `trading/contagion_to_ticket.py` — delete

### 2.4 PWA frontend bloat — DELETE from `routes.js`

Current PWA has 68 view files and 59 routes across 5 drawer groups. Cut to 6 top-level views (§5). Archive on 2026-05-24, delete on 2026-06-07.

Views to archive:
`AppArchitecture.jsx`, `Archive.jsx`, `Associations.jsx` (legacy), `AssociationsLegacy.jsx`, `AttentionRadar.jsx`, `Backtest.jsx`, `Briefings.jsx`, `CatalystTimeline.jsx`, `CorrelationMatrix.jsx`, `EarningsCalendar.jsx`, `EdgeScanner.jsx`, `GeoFlows.jsx`, `Globe.jsx`, `GlobeView.jsx`, `Heatmap.jsx`, `Hyperspace.jsx`, `InfluenceNetwork.jsx`, `IntelDashboard.jsx`, `IntelModeration.jsx`, `IntelSubmit.jsx`, `MilestoneTracker.jsx`, `Models.jsx`, `Operator.jsx`, `Options.jsx`, `Physics.jsx`, `PipelineHealth.jsx`, `Predictions.jsx`, `Regime.jsx`, `RegimeAnalog.jsx`, `RiskMap.jsx`, `RiskView.jsx`, `SectorDive.jsx`, `Signals.jsx`, `Snapshots.jsx`, `SpiderStats.jsx`, `Strategies.jsx`, `Strategy.jsx`, `Surfacer.jsx`, `SystemLogs.jsx`.

That's ~38 views cut.

## 3. Promotion List — What Gets Kept and Elevated

| Module / Path | Promote to | Product | Gap it fills |
|---|---|---|---|
| `intelligence/actors/` subpackage | **TPS core graph** | Trump-Proximity | Actor → admin proximity edges |
| `intelligence/actor_discovery.py` (3,533 LOC) | TPS feeder | Trump-Proximity | Auto-enrich new actors |
| `intelligence/deep_graph.py` (1,772 LOC) | Multi-hop scorer | TPS + 100x | n-degree traversal |
| `intelligence/cross_reference.py` (1,435 LOC) | **The one kept conviction layer** | Trading + Research | Lie detector — genuine edge |
| `intelligence/lever_pullers.py` (1,376 LOC) | Big-Questions feeder | Research | Actors who move policy |
| `intelligence/sector_networks/*.yaml` | Thematic indices | 100x | YAML-driven sector defs |
| `intelligence/thesis_tracker.py`, `thesis_invalidation_monitor.py` | **100x Core thesis store** | 100x | Thesis + kill criteria |
| `intelligence/milestone_tracker.py` | Catalyst tracker | 100x + Research | Expected catalysts |
| `intelligence/signal_provenance.py` | **2-layer aggregator** (rewritten) | Trading | NULL-propagating |
| `intelligence/postmortem.py`, `forensic_journal.py`, `journal/log.py` | **Single trade journal** | Trading | Immutable P&L |
| `intelligence/causation_*.py` | Research dossier engine | Research | Causal chains |
| `intelligence/trust_scorer.py` | Source-trust layer | All 4 | Bayesian decay |
| `intelligence/dollar_flows.py`, `analysis/flow_aggregator.py`, `analysis/flow_thesis.py` | Trading swing-signal | Trading | Dollar-flow → ticket |
| `intelligence/icij_linker.py`, `wealth_tracker.py`, `power_mapper.py` | TPS enrichment | TPS | Offshore + wealth overlays |
| `ingestion/altdata/fara.py` | TPS primary | TPS | Foreign-agent registrations |
| `ingestion/altdata/lobbying.py`, `opensecrets_puller.py` | TPS primary | TPS | Lobbying + PAC money |
| `ingestion/altdata/campaign_finance.py` | TPS primary | TPS | Donor → policy |
| `ingestion/altdata/congressional.py`, `quiverquant.py` | TPS + Trading | TPS + Trading | Congressional trades |
| `ingestion/altdata/insider_filings.py`, `sec_13f_live.py`, `unusual_whales.py` | Trading | Trading | Form 4 + 13F + UW |
| `ingestion/altdata/gov_contracts.py`, `usaspending_puller.py`, `export_controls.py` | TPS | TPS | Direct contract awards |
| `ingestion/altdata/foia_cables.py`, `gdelt.py`, `taiwan_strait_osint.py` | Research | Research | Geopolitical |
| `ingestion/altdata/offshore_leaks.py`, `icij_puller.py`, `littlesis_puller.py`, `opencorporates.py`, `wikidata_persons.py` | TPS enrichment | TPS | Actor graph backbone |
| `ingestion/altdata/uspto_puller.py` | 100x | 100x | Patent moats |
| `ingestion/altdata/cftc_cot.py`, `cot_extremes.py`, `baltic_dry.py`, `jodi_oil.py`, `lme_warehouse.py`, `iron_ore_ports.py` | 100x + Research | 100x + Research | Physical scarcity |
| `intelligence/regime/`, `hmm_regime_transitions.py` | Trading risk overlay | Trading | Risk-on/off gating |
| `intelligence/cross_lens.py` | Research | Research | Multi-source synthesis |

## 4. Four-Product Feature Map

|  | **A. 100x Core** | **B. Trading Layer** | **C. Big Questions** | **D. Trump-Proximity** |
|---|---|---|---|---|
| **Job-to-be-done** | Screen + monitor scarce-asset compounders | Generate swing trades with positive alpha | Week-long investigative dossiers | Daily ranked ticker watchlist |
| **Data sources** | sector_networks YAML, uspto, baltic_dry, lme_warehouse, iron_ore_ports, sec_xbrl_financials, fred, thesis_tracker, milestone_tracker | dollar_flows, insider_filings, sec_13f_live, unusual_whales, congressional, cross_reference, regime | actor_network, deep_graph, lever_pullers, cross_lens, foia_cables, gdelt, fara, lobbying, supply_chokepoints, taiwan_strait_osint | fara, lobbying, opensecrets, campaign_finance, congressional, gov_contracts, usaspending, export_controls, actors + deep_graph |
| **Primary output** | ≤30 monitored theses with kill criteria + catalyst timeline | ≤50 active swing tickets with entry/exit/invalidation | 1 dossier per 1–2 weeks, ≥30 cited sources, 1 testable claim | Daily top-25 watchlist with proximity score breakdown |
| **Success metric** | 3-yr CAGR vs hold-BTC + hold-cash, drawdown-adjusted | Realized P&L vs SPY over rolling 60/90/180d | Citation depth, testable-claim accuracy | Precision@10 vs SPY/sector ETF over 5/10/20d |
| **PWA view** | `Core.jsx` (new) | `Trades.jsx` (new) | `Dossiers.jsx` (new) | `TPS.jsx` (new, Phase 0) |
| **Cadence** | Weekly review, real-time alerts | Daily AM, intra-day on conviction event | Async — 1–2 weeks per | Daily 06:00 ET refresh |

## 5. Simplified PWA Information Architecture

Replace 59 routes with **6 top-level views**. Single tab bar, no drawer.

| View | Route | Component | Job-to-be-done | Visualizations |
|---|---|---|---|---|
| **TPS Watchlist** | `tps` | `views/TPS.jsx` (new, **default landing**) | Today's top-25 by Trump-Proximity score | Ranked table; per-ticker actor-network mini-graph; proximity-score breakdown |
| **Trades** | `trades` | `views/Trades.jsx` (new) | Active positions, entry/exit, realized vs SPY | P&L vs SPY; per-trade evidence card; alpha decomposition |
| **Core (100x)** | `core` | `views/Core.jsx` (new) | Long-horizon thesis monitor | Thesis cards w/ kill-criteria; catalyst timeline; scarce-asset heatmap |
| **Dossiers** | `dossiers` | `views/Dossiers.jsx` (new) | Big-question research | Dossier list w/ status; per-dossier deep_graph; citation map |
| **Journal** | `journal` | `views/Journal.jsx` (keep, refocus) | Immutable decision log | Existing UI + P&L attribution; remove prediction sub-routes |
| **Ops** | `ops` | `views/Ops.jsx` (new, merges Settings + PipelineHealth + SystemLogs) | Health, ingestion status, settings | Puller status; data-source freshness; error rollup |

**Visualization principle:** every screen carries at least one chart with axes labeled in plain English and a defensible benchmark line (SPY, BTC, hold-cash, or sector ETF).

## 6. Phased Rollout

### Phase 0 — next 24h (by 2026-05-17 EOD)

**Ship one thing: the Trump-Proximity Score (TPS) daily watchlist v0.**

1. New file `intelligence/trump_proximity.py` (~300 LOC). Inputs: `intelligence/actors/` graph, FARA + lobbying + campaign_finance + congressional + gov_contracts + usaspending tables. Output: per-ticker `tps_score` (0–100) plus evidence list.
2. Scoring formula v0:
   ```
   tps = w1*direct_contract_$ + w2*lobbying_$_to_admin_priorities + w3*congressional_buy_pressure_30d + w4*fara_edges_to_admin + w5*actor_network_hops_to_admin_inverse
   ```
   All weights = 1.0 initially; tune only after 30d of forward data.
3. New API route `api/routers/tps.py` returning today's top-25 with full evidence chain.
4. New view `pwa/src/views/TPS.jsx`. Ranked table + per-ticker drill-down. Reuse `ActorNetwork.jsx` graph component.
5. Add to `routes.js` as the **default landing route**.
6. Cron at 06:00 ET via `ingestion/scheduler.py` to refresh.

### Phase 1 — this week (2026-05-17 → 2026-05-23)

1. **Wire alpha measurement properly.** New `alpha_research/realized_alpha.py`. For every entry in `journal/log.py`, compute rolling realized alpha vs SPY at 5/10/20/60d.
2. **Ship swing-signal v0.** Compose `dollar_flows` + `cross_reference` (fudge gate) + regime gate → `intelligence/swing_signal.py` (~200 LOC) emitting ≤5 candidates/day with entry, target, invalidation. Surface in `Trades.jsx`.
3. **Start first big-question dossier.** Topic: "Uranium enrichment capacity in NATO post-Russia." Target: 30 cited sources, 1 testable claim with kill criteria.
4. **Freeze (don't delete yet) the firehose.** Mask oracle systemd timers. Stop new `oracle_predictions` writes.

### Phase 2 — this month (2026-05-24 → 2026-06-14)

1. **Retire the firehose for real.** Delete subtraction-list files. Drop oracle tables after parquet export.
2. **Rewrite `signal_provenance.compute_aggregate_conviction`** to two layers (cross_reference + TPS) with NULL propagation.
3. **Ship simplified frontend.** Cut routes to 6. Archive 38 views.
4. **100x Core v0.** Seed with 5–10 starting theses (BTC, AI compute capacity, rare earths, dominant platforms, uranium).
5. **TPS v1.** Tune weights using 30d of forward returns.
6. **Second dossier.**

## 7. Critical-Path Dependencies

The **single most important amputation:** rewrite `signal_provenance.compute_aggregate_conviction` to propagate NULL instead of defaulting to `1.0`. This is the root cause of "11.9% hit rate at HIGH confidence" — high confidence was being assigned to predictions whose layers had silently failed. Do this in Phase 1 even before broader stack deletion.

```
[Phase 0] TPS v0
   depends on: actors/, fara/lobbying/campaign_finance/congressional tables
   blocks: nothing — unblocking artifact
   risk: actor-graph entity resolution gaps → surface "low coverage" badge, never silently zero

[Phase 1] Realized-alpha measurement
   depends on: journal/log.py, price history
   blocks: every claim of "positive alpha"
   gap: prior journal entries may lack clean entry/exit pairs

[Phase 1] Swing-signal v0
   depends on: realized-alpha, cross_reference, dollar_flows
   blocks: Trades.jsx going live
   risk: cross_reference returns 1.0 silently → must change to NULL-propagation FIRST

[Phase 2] Firehose deletion
   depends on: 7d of Phase 0+1 without regression, oracle_predictions exported
   blocks: PWA simplification
   risk: hidden importers → grep audit before delete

[Phase 2] Frontend cut to 6 views
   depends on: 4 new views shipped behind feature flag for 5d
```

## 8. First-Principles Re-Test Plan

No fake metrics. Every product gets a benchmark and a runway.

### 8.1 Trading Layer (B)
- **Benchmark:** SPY total return, costs modeled at 5bp/side
- **Truth gate:** rolling 60d realized alpha in `alpha_research/realized_alpha.py`
- **Kill criterion:** if 60d alpha after 90 days is < +1% annualized, pause and root-cause. If 180d alpha < 0, kill the layer.
- **Anti-fool guard:** signals journaled at emission with frozen invalidation. No backfill, no parameter retuning until 30 trades complete.

### 8.2 100x Core (A)
- **Benchmark:** DCA-BTC + 5%-cash-yield hybrid
- **Truth gate:** 3-yr CAGR, drawdown-adjusted
- **Kill criterion:** any thesis whose kill criteria fires gets archived within 5 trading days
- **Anti-fool guard:** kill criteria pre-committed at thesis creation; no editing after position opened

### 8.3 Big Questions (C)
- **Benchmark:** none (research). Quality metrics instead.
- **Truth gate:** ≥30 cited sources, ≥1 testable claim with timestamp + kill criterion, 90d follow-up scoring
- **Kill criterion:** if first 3 dossiers fail testable-claim accuracy bar (<50% directional), reduce to one/month

### 8.4 Trump-Proximity (D)
- **Benchmark:** SPY for absolute, sector ETF for sector-neutral
- **Truth gate:** precision@10 on 5/10/20d forward returns vs sector ETF
- **Kill criterion:** if precision@10 on 10d horizon after 60d is <55%, rework weighting. Still <55% after another 60d, revert to "evidence-only" mode.
- **Anti-fool guard:** today's top-25 frozen and journaled at 06:00 ET. Forward-return scored using next day's open.

## Commitments (dated)

- **2026-05-17 EOD** — TPS v0 shipped, default landing view, firehose timers masked
- **2026-05-23** — Realized-alpha measurement live, swing-signal v0 emitting, first dossier started
- **2026-05-24** — `signal_provenance` rewritten with NULL propagation
- **2026-05-31** — Subtraction list deleted (firehose, options recommender, 11 conviction layers, 38 PWA views archived)
- **2026-06-07** — Archived views deleted from repo
- **2026-06-14** — 6-view PWA live; 100x Core seeded with 5+ theses; second dossier in flight

## Critical Files

- `intelligence/signal_provenance.py` — the amputation lives here (NULL-propagation rewrite)
- `intelligence/actors/` — TPS graph core; new `trump_proximity.py` sits next to this
- `pwa/src/routes.js` — the IA cut (59 routes → 6)
- `oracle/engine.py` — freeze-then-delete keystone of the firehose
- `journal/log.py` — must be wired to `alpha_research/realized_alpha.py`
