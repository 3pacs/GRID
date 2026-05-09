# SYNTHESIS WIRING PLAN — The Offensive Alpha

Generated: 2026-04-11
Task: #91 (THE OFFENSIVE ALPHA)
Status: **BLUEPRINT — no code touched**. Consumed by the SYNTH-N follow-up queue that populates `contracts/router.py::ROUTES`.

---

## 1. Intro — what "closed loop" means here

GRID now has 649 modules. Roughly 35 of those were built in recent sessions as **detectors** — they write rows to dedicated intelligence tables (`contagion_predictions`, `supply_shock_attributions`, `fundamental_divergence`, `sector_health_snapshots`, `chokepoint_scores`, `holder_deal_overlap`, `supply_chain_edge_adjustments`, `regulatory_events`, etc.) but **nothing downstream reads those rows**.

A "closed loop" in this plan means the detector's output is consumed by at least one of the canonical intelligence sinks:

| Sink | Module | Role |
|---|---|---|
| **Oracle ensemble** | `oracle/engine.py` | Turns signals into scoreable predictions (615 predictions locked; scoring Apr 17). The weight evolver (`_load_models` → `_update_model_weights` path) decays losing models and boosts winners. |
| **Trust scorer** | `intelligence/trust_scorer.py` | Bayesian Beta posterior per (source_type, source_id) over a 90-day recency half-life. 17 signal types registered. |
| **Oracle calibration** | `oracle/calibration.py` | Brier / ECE / reliability — consumes scored `oracle_predictions` rows. |
| **Decision journal** | `journal/log.py` | Immutable append-only log; anchor for every prediction's provenance chain. |
| **Event bus** | `contracts/dispatcher.py` + `contracts/router.py` | Phase-1 infra is live; `ROUTES = {}` is the empty switchboard this plan populates. |
| **Postmortem feedback** | `intelligence/postmortem.py::apply_contagion_feedback` | Decays `supply_chain_edges.pct_downstream_cogs` based on scored accuracy. Only wired for contagion today. |

A detector is **rotting** if its rows are consumed by none of the six above. A consumer is **starving** if it scores, ranks, or trades on fewer signal families than the producers already populate.

The dedupe pass (SYNTH-1..18 in `MODULE_DEDUPE_PLAN.md`) was defensive — delete/merge/rename. This plan is the offensive counterpart: every rotting detector becomes a subscription into the oracle's signal assembly or the [[Trust Scorer|trust scorer]]'s Bayesian update, or both. **Closing these loops is where the alpha lives.**

### Key architectural lever

The `contracts/` package (from handoff 2026-04-11c) already ships with:

- 13 frozen Pydantic contracts in `contracts/schemas.py` (PostmortemCompleted, PredictionScored, SignalFired, CrossReferenceAnomaly, …)
- A dispatcher with correlation IDs, dead-letter queue, retry scheduler, observability, replay
- Integrity tests that enforce `extra="forbid"` and route coverage

What it's missing is handlers and routes. `contracts/router.py::ROUTES` is literally empty:

```python
ROUTES: dict[type[BaseContract], list[str]] = {
    # Phase 2 additions will go here
}
```

**The entire synthesis is: populate ROUTES with the producer→consumer edges this document enumerates, one handler file per edge under `contracts/handlers/`.** No new infra, no new schemas (except the delta in §7) — just wiring.

---

## 2. Current graph (as-is)

Rows = producers. Columns = consumers. Cell legend:
`wired` = import chain proven; `partial` = some fields consumed, most ignored; `missing` = producer writes, consumer is blind; `n/a` = not applicable by design.

| Producer | oracle.engine | trust_scorer | oracle.calibration | journal.log | contracts bus | news_listener | alerts |
|---|---|---|---|---|---|---|---|
| `intelligence/chain_contagion.py` | missing | missing | n/a | missing | missing | wired (input) | missing |
| `intelligence/contagion_backtest.py` | missing | missing | missing | missing | missing | n/a | missing |
| `intelligence/postmortem.py` (contagion feedback) | missing | missing | n/a | missing | missing | n/a | missing |
| `intelligence/postmortem.py` (trade postmortems) | partial | missing | wired | wired | missing | n/a | missing |
| `intelligence/cross_lens.py` | missing | missing | n/a | missing | missing | n/a | missing |
| `intelligence/fundamental_divergence.py` | missing | missing | n/a | missing | missing | n/a | missing |
| `intelligence/sector_health.py` | missing | missing | n/a | missing | missing | n/a | missing |
| `intelligence/holder_deal_overlap.py` | missing | missing | n/a | missing | missing | n/a | missing |
| `intelligence/news_contagion_listener.py` | missing | missing | n/a | missing | missing | n/a | missing |
| `intelligence/supply_chain_edge_validator.py` | missing | missing | n/a | missing | missing | n/a | missing |
| `ingestion/altdata/regulatory_events.py` | missing | missing | n/a | missing | missing | n/a | missing |
| `intelligence/supply_chokepoints.py` | missing | **partial** (chokepoint_crossing delta) | n/a | missing | missing | n/a | missing |
| `trading/contagion_to_ticket.py` | missing | missing | n/a | **wired** | missing | n/a | missing |
| `oracle/engine.py` (predictions) | self | partial (via detect_convergence read-back only) | wired | wired | missing | n/a | missing |

### What "wired" actually covers today

- **Oracle → calibration / journal:** direct. `oracle/engine.py:996-1095` reads `oracle_predictions`, scores them, and upserts back. `journal/log.py` owns the immutable leg.
- **[[Trust Scorer|Trust scorer]] → oracle (read-only):** `oracle/engine.py::_get_convergence_for_ticker` calls `trust_scorer.detect_convergence` per ticker. One-way consumption only; oracle's verdict never feeds back into `signal_sources.outcome`.
- **Supply chokepoints → [[Trust Scorer|trust scorer]]:** `SIGNAL_TRUST_DELTA["chokepoint_crossing"] = -0.10` exists, but only chokepoint *crossings* are read — not the baseline `chokepoint_score` rows. Partial.
- **Contagion → options ticket → journal:** `trading/contagion_to_ticket.py` (SYNTH-13 canonical pricer) reads `contagion_predictions`, builds tickets, and logs them. The backward edge (journal verdict → contagion accuracy) exists via `contagion_backtest` but **does not flow through trust_scorer or oracle weight evolver**. The contagion model never earns or loses oracle weight from its trades.

### What "missing" looks like in practice

- `cross_lens` writes 186 `supply_shock_attributions` rows. `oracle.engine._gather_signals` queries `resolved_series` + `options_daily_signals`. It has never heard of supply shock attributions.
- `fundamental_divergence` tags 202 tickers LONG/SHORT. `trust_scorer` doesn't have a `fundamental_divergence` source_type. Oracle models don't subscribe to the `divergence` family.
- `sector_health` computes a 0-100 composite per sector. No consumer.
- `holder_deal_overlap` flags pre-positioning ahead of M&A announcements. This is prime insider-edge data. It does not appear in `trust_scorer.get_insider_edge` and it does not feed a BUY signal into oracle.
- `news_contagion_listener` already auto-fires contagion from news, but the news event itself (ticker, shock type, severity) is never emitted as a `SignalFired` contract.
- `regulatory_events` lands rows with severity {low, medium, high, critical} and projects them as `supply_chain_edges.relationship='regulatory_threat'`. Oracle has no notion of regulator-driven re-pricing.

---

## 3. Gap analysis

### Rotting signals (producers with zero wired consumers)

**Severity P0 — highest-alpha detectors stuck in the cellar:**

1. `intelligence/holder_deal_overlap.py` — pre-position detection. Filer holds both acquirer and target before announcement. This is the highest-asymmetry signal in the entire tree and nobody reads it.
2. `intelligence/fundamental_divergence.py` — 202 tickers with long/short classification. The oracle's equity models have no fundamental lens at all right now; this plugs that gap directly.
3. `intelligence/cross_lens.py` — supply shock attributions are the "explained by" layer. 186 rows waiting. Every [[Causation|causation]] narrative the system produces could cite these, and oracle's anti-signal engine could use them to downgrade conflicting equity signals.

**Severity P1 — loop-closing but lower immediate alpha:**

4. `intelligence/postmortem.py::apply_contagion_feedback` — already decays `pct_downstream_cogs`. The decay event must fire as a contract so trust_scorer can also downweight the `news_listener` source that triggered a missed prediction.
5. `intelligence/sector_health.py` — composite score is a strong regime flag. Plugs into oracle as a `sector` family signal and into `_get_credit_cycle_routing` style family multipliers.
6. `ingestion/altdata/regulatory_events.py` — severity-tagged enforcement events are hard anti-signals for long theses on the named ticker.
7. `intelligence/supply_chokepoints.py` — chokepoint baseline scores (not just crossings) should inform oracle's `supply` family weight.
8. `intelligence/news_contagion_listener.py` — already produces contagion rows. The listener run itself should emit `SignalFired` contracts.
9. `intelligence/supply_chain_edge_validator.py` — weak-edge flags should downweight any signal that cites the weak edge.
10. `intelligence/chain_contagion.py` — predictions are stored but never emitted as events; the follow-on `contagion_to_ticket` is the only consumer and it pulls by DB query.
11. `intelligence/contagion_backtest.py` — scored accuracy should feed oracle weight evolver for the `contagion` model AND trust_scorer for the news source that triggered it.

### Starving consumers (sinks with unread upstream volume)

- **Oracle weight evolver** (`oracle/engine.py::_update_model_weights`) only sees verdicts on predictions the oracle itself made. Its DEFAULT_MODELS list has no `contagion`, `fundamental_divergence`, `sector_health`, `regulatory`, `holder_overlap`, or `cross_lens` model entries. **Six missing model heads.**
- **[[Trust Scorer|Trust scorer]]** has 17 signal types but `SIGNAL_TRUST_DELTA` defines only 2 deltas (`sec_filing`, `chokepoint_crossing`). The Bayesian Beta is running but half the producers can't deposit outcomes into it.
- **[[Oracle Calibration|Oracle calibration]]** only reads `oracle_predictions`. Per-detector calibration would let us pause a detector whose ECE blows up without blowing up the whole oracle.
- **[[Decision Journal|Decision journal]]** is under-utilized. The only non-oracle writer is `contagion_to_ticket.py`. Every detector firing a scoreable prediction should journal.
- **Contracts bus** (`ROUTES`) is empty. `DLQ`, `retry_scheduler`, `replay`, `observability` are wired and tested but no events route.

---

## 4. Target graph (to-be)

Same axes as §2. **Bold** cells are additions. Existing wiring is preserved.

| Producer | oracle.engine | trust_scorer | oracle.calibration | journal.log | contracts bus | news_listener | alerts |
|---|---|---|---|---|---|---|---|
| `chain_contagion.py` | **wired** (via ContagionSimulated → SignalFired) | **wired** | n/a | **wired** | **wired** | wired | **wired** (critical shocks) |
| `contagion_backtest.py` | **wired** (feeds weight evolver for contagion model) | **wired** (source=news_listener bayes update) | **wired** (per-detector Brier) | **wired** | **wired** | n/a | missing |
| `postmortem.py` (contagion feedback) | **wired** (triggers model weight decay if accuracy low) | **wired** (decays source trust) | n/a | **wired** | **wired** (PostmortemCompleted) | n/a | missing |
| `postmortem.py` (trade postmortems) | partial → **wired** | **wired** (signals_used ref) | wired | wired | **wired** | n/a | missing |
| `cross_lens.py` | **wired** (supply_shock family signal) | **wired** (source=cross_lens) | n/a | missing | **wired** (CrossReferenceAnomaly) | n/a | missing |
| `fundamental_divergence.py` | **wired** (new fundamental model head) | **wired** | n/a | **wired** | **wired** (SignalFired) | n/a | missing |
| `sector_health.py` | **wired** (sector family routing + signal) | missing | n/a | missing | **wired** (RegimeTransition when crosses threshold) | n/a | **wired** (health < 30) |
| `holder_deal_overlap.py` | **wired** (new holder_overlap model head) | **wired** (source=filer_name per overlap) | n/a | **wired** | **wired** (SignalFired) | n/a | **wired** |
| `news_contagion_listener.py` | **wired** (via SignalFired fanout) | **wired** (source=news_listener) | n/a | missing | **wired** | n/a | missing |
| `supply_chain_edge_validator.py` | **wired** (downweight weak-edge contagion) | **wired** (decays cross_lens trust on weak edge) | n/a | missing | **wired** (EdgeValidated contract — NEW) | n/a | missing |
| `regulatory_events.py` | **wired** (severity → anti-signal for long theses) | **wired** (source=regulator_name) | n/a | missing | **wired** (CrossReferenceAnomaly variant) | n/a | **wired** (critical severity) |
| `supply_chokepoints.py` | **wired** (baseline supply family) | partial → **wired** (score in addition to crossing) | n/a | missing | **wired** (SignalFired when score>0.7) | n/a | missing |
| `contagion_to_ticket.py` | **wired** (outcome → weight evolver) | **wired** (trade result → source bayes) | **wired** | wired | **wired** (OptionsTradeOutcome) | n/a | missing |
| `oracle/engine.py` | self | **wired** (PredictionScored → trust update) | wired | wired | **wired** (PredictionScored emit) | n/a | missing |

**Feedback arrows added (these are the real alpha):**

- `oracle.engine` **emits** `PredictionScored` after each scoring cycle. Handler chain:
  1. `trust_scorer.on_prediction_scored` — walks `signals_used`, Bayes-updates each source.
  2. `oracle.weight_evolver.on_prediction_scored` — decays losing model heads.
  3. `calibration.on_prediction_scored` — updates running Brier / ECE per model.
- `postmortem.on_contagion_feedback_applied` **emits** `PostmortemCompleted`. Handler chain:
  1. `trust_scorer.on_postmortem_completed` — downweights `news_listener` source for misses.
  2. `oracle.weight_evolver.on_postmortem_completed` — nudges `contagion` model head weight.
- `contagion_to_ticket` **emits** `OptionsTradeOutcome` when a ticket expires. Handler chain:
  1. `oracle.weight_evolver` — Sharpe / hit-rate update.
  2. `calibration` — per-strategy reliability curve.

---

## 5. Per-edge SYNTH task queue

All tasks land **contracts/handlers/<domain>.py** (new files) and mutate `contracts/router.py::ROUTES` only. No producer code changes except a single `emit()` call at the end of each existing detector. Every handler is <100 LOC; heavy logic stays in the existing sinks.

| ID | Producer | Consumer + handler signature | Mechanism | Contract | Migration | Priority | LOC | Depends on |
|---|---|---|---|---|---|---|---|---|
| **SYNTH-19** | `oracle/engine.py` (score cycle end) | `contracts/handlers/trust.py::on_prediction_scored(evt: PredictionScored)` → fans `signals_used[*]` into `trust_scorer.score_pending_signals` | emit→dispatcher→handler | `PredictionScored` (exists) | none | **P0** | ~120 | — |
| **SYNTH-20** | same | `contracts/handlers/oracle_weights.py::on_prediction_scored` → calls new `oracle.engine.ModelRegistry.update_from_contract` | emit→dispatcher→handler | `PredictionScored` | none | **P0** | ~80 | SYNTH-19 |
| **SYNTH-21** | same | `contracts/handlers/calibration.py::on_prediction_scored` → delegates to `oracle/calibration.py::update_running_metrics` (new) | emit→dispatcher→handler | `PredictionScored` | `ALTER TABLE oracle_models ADD COLUMN running_brier DOUBLE PRECISION, running_ece DOUBLE PRECISION` | **P0** | ~100 | SYNTH-19 |
| **SYNTH-22** | `postmortem.py::apply_contagion_feedback` | `contracts/handlers/trust.py::on_postmortem_completed` → Bayes-update `news_listener` source | direct emit in existing function | `PostmortemCompleted` (exists) | none | **P0** | ~90 | — |
| **SYNTH-23** | same | `contracts/handlers/oracle_weights.py::on_postmortem_completed` → decay contagion model head | emit→dispatcher | `PostmortemCompleted` | none | **P0** | ~60 | SYNTH-20, SYNTH-22 |
| **SYNTH-24** | `intelligence/holder_deal_overlap.py` | `oracle/engine.py` adds `"holder_overlap"` model head (DEFAULT_MODELS entry) + `contracts/handlers/oracle_signals.py::on_signal_fired` routes `signal_type="holder_overlap"` into `_gather_signals_from_registry` | direct import of new oracle model + SignalFired emit | `SignalFired` (exists) | `INSERT INTO oracle_models (name, signal_families) VALUES ('holder_overlap', ARRAY['insider','flows'])` (seed) | **P0** | ~140 | — |
| **SYNTH-25** | same | `trust_scorer.score_pending_signals` — add `holder_overlap` source_type branch | direct (no bus) | n/a | `ALTER CHECK` on `signal_sources.source_type` if restricted; else none | **P0** | ~50 | SYNTH-24 |
| **SYNTH-26** | `intelligence/fundamental_divergence.py` | new `fundamental` model head in DEFAULT_MODELS + SignalFired emit | direct + bus | `SignalFired` | seed INSERT | **P0** | ~160 | SYNTH-24 (same pattern) |
| **SYNTH-27** | same | `trust_scorer` — add `fundamental_divergence` source_type, `SIGNAL_TRUST_DELTA["fundamental_divergence"] = +0.05` (prior) | direct | n/a | none | **P0** | ~40 | SYNTH-26 |
| **SYNTH-28** | `intelligence/cross_lens.py` | `oracle.engine._find_anti_signals` consults `supply_shock_attributions` for the ticker; downgrades any long signal with a confirmed upstream shock | direct DB join in existing anti-signal pass | `CrossReferenceAnomaly` (reused; statistic="supply_shock") | none | **P0** | ~120 | — |
| **SYNTH-29** | same | `trust_scorer` — `cross_lens` source_type Bayes updates on realized downstream moves | direct | n/a | none | **P1** | ~80 | SYNTH-28 |
| **SYNTH-30** | `intelligence/sector_health.py` | `oracle.engine._get_credit_cycle_routing`-style `_get_sector_health_routing()` multiplies family weights by health score | direct | `RegimeTransition` (reused, only when crossing 30/70 bands) | `sector_health_snapshots` exists | **P0** | ~100 | — |
| **SYNTH-31** | same | `alerts/` — new `sector_health_alert.py` consumer on `RegimeTransition`-type emission | bus→handler | same | none | **P1** | ~60 | SYNTH-30 |
| **SYNTH-32** | `ingestion/altdata/regulatory_events.py` | `oracle.engine._find_anti_signals` — for every ticker in `entities_mentioned` with severity >= MEDIUM, add AntiSignal with severity-scaled weight | direct DB read | `CrossReferenceAnomaly` (severity mapped) | none | **P0** | ~130 | — |
| **SYNTH-33** | same | `trust_scorer.SIGNAL_TRUST_DELTA["regulatory_threat"] = -0.15` + new source_type branch | direct | n/a | none | **P0** | ~40 | SYNTH-32 |
| **SYNTH-34** | `intelligence/supply_chokepoints.py` | `oracle.engine._gather_signals` — join on `supply_chain_edges.chokepoint_score >= 0.7` into the `supply` family | direct DB read | `SignalFired` on threshold cross (new emission point only) | none | **P1** | ~90 | — |
| **SYNTH-35** | `intelligence/chain_contagion.py` | emit `SignalFired(source="chain_contagion", signal_type="contagion_ranked_impact", ticker=<each ranked_impact>)` after each `simulate_contagion` call | direct emit | `SignalFired` | none | **P1** | ~70 | — |
| **SYNTH-36** | same | `oracle.engine` adds `"contagion"` model head subscribed to `contagion` family | direct + bus | `SignalFired` | seed INSERT | **P1** | ~90 | SYNTH-35, SYNTH-20 |
| **SYNTH-37** | `intelligence/contagion_backtest.py` | emit `PredictionScored(prediction_id=contagion_prediction_id, verdict=...)` per scored row | direct emit | `PredictionScored` (reused) | none | **P0** | ~100 | SYNTH-19 |
| **SYNTH-38** | `intelligence/news_contagion_listener.py` | emit `SignalFired(source="news_listener", signal_type="contagion_trigger", raw_row_ids=[news_id])` per triggered shock | direct emit | `SignalFired` | none | **P1** | ~60 | SYNTH-35 |
| **SYNTH-39** | `intelligence/supply_chain_edge_validator.py` | new `EdgeValidated` contract (see §7) — handler downgrades trust of `cross_lens` attributions citing a weak edge | bus→handler | **NEW contract** | none | **P1** | ~110 | §7 contract add |
| **SYNTH-40** | `trading/contagion_to_ticket.py` | emit `OptionsTradeOutcome` when ticket expires/closes; handler updates contagion model head Sharpe + strategy calibration | direct emit | `OptionsTradeOutcome` (exists) | none | **P0** | ~90 | SYNTH-20 |
| **SYNTH-41** | `postmortem.py` (trade postmortems, not contagion) | emit `PostmortemCompleted` with `signals_used` populated from journal | direct emit in existing function | `PostmortemCompleted` | none | **P0** | ~80 | SYNTH-22 |
| **SYNTH-42** | any producer above | `contracts/handlers/journal.py::on_signal_fired` — for every `SignalFired` with `strength > 0.7`, log a provisional journal entry for later outcome scoring | bus→handler | `SignalFired` | `ALTER TABLE decision_journal ADD COLUMN source_contract_id UUID NULL` | **P1** | ~120 | SYNTH-19..38 landed |
| **SYNTH-43** | `oracle/engine.py::_update_model_weights` | refactor to consume PredictionScored events instead of per-cycle DB scan | dispatcher-driven | `PredictionScored` | none | **P1** | ~180 | SYNTH-20, SYNTH-21 |
| **SYNTH-44** | all detectors landing new trust source_types | add router-integrity test asserting every `SIGNAL_TRUST_DELTA` key exists in `signal_sources` schema check constraint | test only | n/a | possible CHECK constraint widening | **P1** | ~60 | SYNTH-25, 27, 33 |
| **SYNTH-45** | `contracts/router.py` | populate `ROUTES` dict with every mapping above; router-integrity test must pass | none | n/a | none | **P0** (final seal) | ~80 | ALL above |

**Total LOC estimate:** ~2,360 across 27 files, all new handler modules plus one seed migration and one ALTER for `running_brier`/`running_ece`.

---

## 6. Execution order (topological)

Producers land before consumers, contracts before their handlers, bus-integrity tests last.

**Wave A — foundation (P0, unblocks the bus):**

1. SYNTH-19 — `trust.on_prediction_scored` handler
2. SYNTH-20 — `oracle_weights.on_prediction_scored` handler
3. SYNTH-21 — `calibration.on_prediction_scored` handler + migration
4. **Milestone:** oracle is now closed-loop with itself via the bus. Router has 1 contract type wired.

**Wave B — [[Postmortem|postmortem]] + contagion feedback (P0):**

5. SYNTH-22 — `trust.on_postmortem_completed`
6. SYNTH-23 — `oracle_weights.on_postmortem_completed`
7. SYNTH-37 — `contagion_backtest` emits `PredictionScored`
8. SYNTH-40 — `contagion_to_ticket` emits `OptionsTradeOutcome`
9. SYNTH-41 — trade postmortems emit `PostmortemCompleted`
10. **Milestone:** contagion loop is fully closed. [[Postmortem]] decays flow into both trust and weight evolver.

**Wave C — highest-alpha rotting detectors (P0):**

11. SYNTH-24, SYNTH-25 — `holder_deal_overlap` → oracle + trust
12. SYNTH-26, SYNTH-27 — `fundamental_divergence` → oracle + trust
13. SYNTH-28, SYNTH-29 — `cross_lens` → oracle anti-signals + trust
14. SYNTH-32, SYNTH-33 — `regulatory_events` → oracle anti-signals + trust
15. SYNTH-30 — `sector_health` → oracle family routing
16. **Milestone:** five new signal families flowing into oracle. This is the offensive alpha the task title refers to.

**Wave D — bus fanout + lower priority (P1):**

17. SYNTH-34 — chokepoint baseline
18. SYNTH-35, SYNTH-36 — chain_contagion SignalFired + contagion model head
19. SYNTH-38 — news_listener SignalFired
20. SYNTH-39 — EdgeValidated contract + handler (needs §7 delta)
21. SYNTH-31 — sector_health alerts
22. SYNTH-42 — provisional journal entries from high-strength SignalFired events

**Wave E — consolidation + governance (P1):**

23. SYNTH-43 — refactor weight_evolver to be bus-driven
24. SYNTH-44 — schema integrity tests
25. SYNTH-45 — populate `ROUTES` in full, make router-integrity test mandatory in CI

**Kill-switch rule:** if any handler raises, the dispatcher sends it to the DLQ. No handler can block the producer. This is already enforced by the Phase-1 dispatcher — we inherit it for free.

---

## 7. Contracts delta

`contracts/schemas.py` needs exactly **one** new contract for SYNTH-39. All other wiring reuses existing schemas.

### 7.1 New: `EdgeValidated`

```python
class EdgeValidated(BaseContract):
    edge_id: int
    upstream_id: int
    downstream_id: int
    relationship: str                          # 'cost_pass_through', 'supplier', ...
    validation_correlation: float              # last 180d Pearson
    weak_since: datetime | None
    relationship_weak: bool
    implied_pct_cogs: float | None             # optional empirical re-derivation
```

### 7.2 Extensions (no schema change, only field population discipline)

- `SignalFired.source` enum extended implicitly to include: `holder_overlap`, `fundamental_divergence`, `cross_lens`, `sector_health`, `regulatory_events`, `chain_contagion`, `news_listener`, `chokepoint`. Since `source` is typed `str`, no schema change; add a module-level `ALLOWED_SIGNAL_SOURCES` tuple in `contracts/schemas.py` plus a validator so drift is caught at construction time.
- `CrossReferenceAnomaly.statistic` used for two new values: `"supply_shock"` (from cross_lens) and `"regulatory_threat"` (from regulatory_events). Document the allowed set in the docstring.
- `PostmortemCompleted.root_cause` becomes a short enum-like string. Producers must populate one of: `contagion_decay`, `trade_loss`, `signal_source_downgrade`. Enforced by lint, not schema.

### 7.3 ROUTES after Wave E (canonical end state)

```python
ROUTES = {
    PredictionScored: [
        "contracts.handlers.trust.on_prediction_scored",
        "contracts.handlers.oracle_weights.on_prediction_scored",
        "contracts.handlers.calibration.on_prediction_scored",
        "contracts.handlers.journal.on_prediction_scored",
    ],
    PostmortemCompleted: [
        "contracts.handlers.trust.on_postmortem_completed",
        "contracts.handlers.oracle_weights.on_postmortem_completed",
    ],
    OptionsTradeOutcome: [
        "contracts.handlers.oracle_weights.on_options_trade_outcome",
        "contracts.handlers.calibration.on_options_trade_outcome",
    ],
    SignalFired: [
        "contracts.handlers.oracle_signals.on_signal_fired",
        "contracts.handlers.trust.on_signal_fired",
        "contracts.handlers.journal.on_signal_fired",
    ],
    CrossReferenceAnomaly: [
        "contracts.handlers.oracle_anti_signals.on_cross_reference_anomaly",
        "contracts.handlers.alerts.on_cross_reference_anomaly",
    ],
    EdgeValidated: [
        "contracts.handlers.trust.on_edge_validated",
        "contracts.handlers.cross_lens_downgrade.on_edge_validated",
    ],
    RegimeTransition: [
        "contracts.handlers.oracle_regime.on_regime_transition",
        "contracts.handlers.alerts.on_regime_transition",
    ],
    # LeverageRiskUpdate, BacktestGateVerdict, HypothesisGenerated,
    # ActorMaterialized, PullLifecycle, ForensicsTrace, InvestigationProgress
    # are governed by the V5/governance track and are out of scope for
    # this synthesis pass — they land in a later oracle-governance PR.
}
```

---

## 8. Open questions (non-blocking)

1. **Idempotency on replay.** If the dispatcher replays a `PredictionScored` event, the oracle weight evolver must not double-count. Current Phase-1 infra has a correlation_id — handlers must upsert-by-correlation. Flag this in SYNTH-20.
2. **Ordering.** `PredictionScored` must run `trust.update` before `oracle_weights.update` or the weight delta and the trust delta race on overlapping state. The dispatcher supports ordered handler chains per contract key; document the invariant in each handler docstring.
3. **Backpressure.** 615 locked predictions score on Apr 17. That is 615 `PredictionScored` events in a burst. Confirm dispatcher queue depth + retry budget before flipping the switch. Retry scheduler is live so this should be fine.
4. **Signal schema churn.** Several `signal_sources.source_type` CHECK constraints may need widening (SYNTH-44). Preferred fix: drop the CHECK, move validation to Python at insert time.

---

## 9. Success criteria

This plan is "done" when:

- [ ] `contracts/router.py::ROUTES` is populated per §7.3.
- [ ] Every producer in §2 has at least one `emit()` call or direct reader in a consumer.
- [ ] Every consumer in §2 reads from at least one new producer.
- [ ] Oracle `DEFAULT_MODELS` includes `holder_overlap`, `fundamental_divergence`, `contagion`, `sector_health_regime` (4 new heads).
- [ ] `SIGNAL_TRUST_DELTA` includes entries for every source_type the producers actually emit.
- [ ] Router integrity test asserts no contract in `ALL_CONTRACTS` has zero handlers except the 7 V5-scope ones listed in §7.3.
- [ ] DLQ observed empty after a full scoring cycle.

**When all green, the offensive loop is closed and the weight evolver is finally a real evolver — it has something to evolve on.**

## Appendix A — Wave completion status (as of 2026-04-13)

| Wave | Tasks | Status | Landing task |
|------|-------|--------|--------------|
| A — foundation | SYNTH-19..23 | shipped | #97 |
| B — contagion feedback | SYNTH-37 + bundled in C | shipped | #98 (37) + #100 (40/41) |
| C — rotting detectors | SYNTH-24..33 | shipped | #99 |
| D — bus fanout | SYNTH-34..36, 38, 39, 42 | shipped | #100 |
| E — consolidation | SYNTH-43, 44, 45 | shipped | #101 |

Synthesis plan closed. Remaining wiring deferred: SYNTH-31 (sector_health alerts, P1, not blocking).
