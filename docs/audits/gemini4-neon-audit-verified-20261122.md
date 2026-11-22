# Gemini 4 Neon audit of GRID, verified (2026-11-22)

On 2026-11-21 Gemini 4 Neon (in Antigravity) audited GRID at origin/main `56a5a86b`. Its claims were then checked line by line by four independent Claude reviewers at `586ba09b`. Every file the claims cite is unchanged between those two commits.

This ledger records each claim's verdict and fix owner. Line numbers refer to `586ba09b`.

**Verdicts:** CONFIRMED, PARTLY (some of the claim holds), WRONG, or UNCLEAR (needs a production read).

**Reach** says where the code is called from:
- **live**: a scheduled job or an always-on route.
- **API**: an on-demand route.
- **Hermes**: runs inside `grid-hermes`, which is **active**. It has run since at least the 11-15 boot, and every deploy restarts it (most recently 2026-11-22 03:12Z), so Hermes items are live.
- **CLI**: a manual script.
- **dead**: no non-test caller.

**Slice** is the fix branch (`claude/fp-<slice>-20261122`), or the owner if the fix lives elsewhere. Regime and state items go to the regime and state work (#1301).

## Scoreboard context

`docs/research/edge-report-forward-20261106.md` says GRID has no edge yet:

- Forward signal sources: N = 343,808, pooled hit 50.7%, 0 EDGE and 11 NO_EDGE.
- Oracle: 2,817 forward rows from only 7 creation dates, every model INSUFFICIENT.
- Since mid-May 2026 the Oracle has produced no official verdicts, only `pending` and `no_data`.

The defects below are reasons the system could not have found or used an edge even if one existed. Fixing them does not create one.

## Production reality (read-only survey of grid-svr, 2026-11-22 ~03:45Z)

- **Release:** `/data/grid_v4/grid_release` = `586ba09b`. grid-api, coordinator, hermes, goal-worker, spy-band-flags and gamma-time run it.
- **Older trees:**
  - grid-intelligence (options recommender 07:00, thesis monitor, calibration, weekly sweep): `12dfa011`, 18 behind. It missed the 03:12Z restart.
  - grid-scheduler (auto_regime): pinned to `235e9467`, about 300 behind.
  - grid-worker and five other units: `a3cae338`, about 289 behind.
  - grid-realtime: `4a1045ff`, about 768 behind.
  - grid-options-puller: `8ae1a2b5` on an old venv.
  - **A merged fix reaches a pinned unit only after that unit is repointed.**
- **Deploy-order hazard (from the regime and state work, 2026-11-22):**
  - grid-scheduler's `zz-release-worktree.conf` pins `WorkingDirectory` to `grid_release.releases/235e9467…` (repinned 11-18). Its `auto_regime.py` writes `regime_history` with `ON CONFLICT (obs_date) DO UPDATE` and gives no reason.
  - The unpushed state-1 change (#1302) installs a trigger that refuses an UPDATE with no reason. If it deploys while the scheduler is pinned, that day's 2nd–4th auto_regime writes are refused and the label freezes at the morning run.
  - Repoint the scheduler first, or archive and refuse nothing. Anik is deciding.
- **Oracle registry:** 305 active models. 301 are `evolver:hypothesis` and 4 `evolver:mutation`; all 9 human-made models are inactive. So the live hourly forward pass votes only with evolver-made models that were never shadow-tested (4.5).
- **Changepoints:** the detector writes 32,000–76,000 `regime_change` rows a day to `signal_registry` (1.08M in 30 days). These reach `analysis/thesis_scorer` → `thesis_snapshots`, which feed `/intelligence/thesis`, the market diary and the publication packet (2.7).
- **Forward pass:** the 354 forward-v2 Oracle rows are all QQQ, none SPY. The pass abstained on 11-20 (Cboe 403, no options capture).
- **Empty outputs:**
  - The thesis invalidation monitor scanned 0 predictions in all 145 runs (4.6).
  - `oracle_calibration_history` has 0 rows ever.
  - `per_signal_brier` was last written 05-16; `regime_conditional_brier` has 1 row.
  - `validation_results` was last written 09-25.
- **Dormant:** `signal_executor` (job not registered, `paper_trades` last created 04-17) and `oracle/risk` (`trade_tickets` last written 05-17).
- **Robinhood:** the legacy executor's mirror path was reachable only through an env flag; the fix branch refuses it unconditionally. No orders are logged. gex-trader is independent of GRID's executor.
- **JAX:** absent in all venvs, so the autobnn fallback runs (2.6).
- **Who reads what:**
  - Routes with traffic in the 7-day access log: `/regime/current` (242 hits, including the cache warmer) and `/intelligence/thesis` (149).
  - Oracle, conviction, trading, chat and astrogrid routes had 0 hits.
  - The live user surfaces are the Hermes Oracle email, thesis, diary, publication packet and regime.

## Oracle per-ticker path (`_oracle_one_ticker`; reach: **live**, the Hermes hourly SPY/QQQ forward pass, plus CLI)

| Claim | Verdict | Evidence | Slice |
|---|---|---|---|
| 1.1: `z>0.5` votes bullish for every family; rising VIX, spreads and unemployment vote bullish | CONFIRMED | `oracle/engine.py:996-1001`. No polarity column in `feature_registry` (`schema.sql:97-118`). Overrides only shrink weights (`signal_weight_overrides.py:92-101`), and the comment there records "vol 0r / 348w". | oracle-polarity |
| 1.2: `pcr` and `max_pain_gap` carry z=0 | CONFIRMED | `engine.py:1020,1026`. They still count in coherence (`1243`). | oracle-polarity |
| 1.3: `bull_score` has no `abs()` | PARTLY | `engine.py:1691`. Inert on the default path; it bites only with `GRID_SIGNAL_REGISTRY=1` (`1048`). | oracle-polarity |
| 3.1: confidence saturates and the expected move ignores vol and horizon | CONFIRMED | `engine.py:1782-1806` (`raw/5.0`, `min(30, strength*0.5)`). The 612-trade 0.950 bucket in `signal_provenance.py:507-516` is blamed there on six clamp sites; it is a May audit. | oracle-polarity |
| 3.5: credit-cycle routing is dead | CONFIRMED | `engine.py:1635-1638` reads a field that doesn't exist and rebinds the loop variable. It is also gated off at `1276`. | oracle-polarity |
| 3.6: anti-signals are called without `ticker` | CONFIRMED | `engine.py:1704`, and `1084-1086` only run `if ticker:`. | oracle-polarity |
| new: the anti-signal veto double-counts and works by count | CONFIRMED | `engine.py:1704-1745` | oracle-polarity |
| 4.4: scoring never updates weights or calibration | PARTLY | `evolve_weights(event_driven=True)` is read-only (`2255,2419`). No `PredictionScored` is emitted. **Wrong** that `ModelRegistry` is never updated: hits, misses and P&L are (`2181-2187`). The reliability rebuild has no scheduled caller. | oracle-polarity (notes) |

## Ensemble, uncertainty and ticket risk (`EnsemblePredictor.predict`; reach: API `/oracle/predict-live`, `/conviction/*`, weekly universe ranker)

| Claim | Verdict | Evidence | Slice |
|---|---|---|---|
| 3.2: one hit history is counted three times; direction-blind confidence; sign-blind FCI; fixed ±3% target | PARTLY | `engine.py:2903-2908,2962-2966,3045-3047,3092`. The disagreement penalty (`3014`) cuts the 5-vs-4 example to 0.754, not 0.90. Bucket weights rarely move (only `contagion_backtest` emits `PredictionScored`). | ensemble-risk |
| 3.7: zero-width CI on unsigned confidence feeds Kelly | CONFIRMED | `oracle/uncertainty.py:169-182`. 3 vs 3 at 0.8 gives (0.669, 0.669). | ensemble-risk |
| 6.3: the risk circuit breaker is never fed and passes confidence as price | CONFIRMED | `oracle/risk.py` has no production `record_outcome`, `current_price=confidence` (`299`), and fails open (`457-476`). | ensemble-risk |
| new: the `GRID_SIGNAL_REGISTRY` default differs (engine "0", `model_factory` "1") | CONFIRMED | `engine.py:44` vs `model_factory.py:34`. The server value is UNCLEAR. | ensemble-risk |

## Options recommender (reach: **live**, daily 07:00 scheduler, plus Hermes every 4 h and `POST /options-recommendations`; persisted and broadcast)

| Claim | Verdict | Evidence | Slice |
|---|---|---|---|
| 5.7: delta=1 target, stale mid with no fees, strike and expiry picked independently | CONFIRMED | `trading/options_recommender.py:1102-1108`. Spot 100, wall 110, entry 1.00 gives an 11× target and half-Kelly 0.21. Also `1044,1026,1598,1594,1037`. | options |
| 6.4: 25% Kelly, zero-Kelly recommendations, SKIP counted as PASS, no expiry filter, stop text reversed | CONFIRMED | `578,1180,1183,791,504,1259-1269,472`. The stop text has no callers. | options |

## Provenance stack and convergence (reach: API `/conviction/ticker`, weekly universe ranker)

| Claim | Verdict | Evidence | Slice |
|---|---|---|---|
| 3.3: 16 layers; disagreement and fragility applied twice; global shipping alerts; LLM red team scales conviction | PARTLY | `signal_provenance.py:255,386-414,650`, `decision_gateway.py:117`, `llm_red_team.py:174-224`. **Wrong** about convergence being applied twice on this path. | provenance-stack |
| 3.4: failed layers return 1.0, so the coverage gate never demotes | CONFIRMED | 11 adapters return a neutral 1.0. Coverage is 15/16 = 94%, not the ~81% Neon gave. | provenance-stack |
| new: scorecard keys probably never match (lookup by model name, written by signal or family) | UNCLEAR | `signal_provenance.py:606` vs `bootstrap_per_signal_brier.py:201-222` | provenance-stack |
| 1.8: the prediction-market sign is negated twice; neutral weight counts in the numerator; FINRA short ratio treated as bearish; 13F lag | PARTLY | `signal_convergence_scanner.py:1016,1093-1099,704`. The 13F lag holds. The congress stream is mostly empty, not stale. | convergence-scorer |
| 1.5: `INST_FLOW` exact match; `SEC_FORM4` and XBRL read series nobody writes | PARTLY | `alpha_research/conviction_scorer.py:376,363-370`. The real signed Form 4 series is `FORM4:{t}:OPEN_MKT_NET_VALUE` (`ingestion/edgar.py:458-463`). Both layers always score 0. | convergence-scorer |

## Calibration and scoring math

| Claim | Verdict | Evidence | Slice |
|---|---|---|---|
| 3.8: `running_ece` is MAE | CONFIRMED | `oracle/calibration.py:262,325,367`. Read by the drift monitor (`intelligence/scheduler.py:656`). | calibration-math |
| 3.9: Shapley-weighted Brier divided by the plain count | CONFIRMED | `features/per_signal_brier.py:228,274` and `regime_conditional_brier.py:335,384`. A weight of 0.05 on coin flips gives the maximum 1.5× boost. Writes come only from the manual bootstrap; reads are live. | calibration-math |
| 3.10: the chat publisher gate fails open | CONFIRMED | `oracle/publisher_gate.py:64-70,136-146,155-160` (`api/routers/chat.py:1949`) | calibration-math |

## Postmortem and feedback

| Claim | Verdict | Evidence | Slice |
|---|---|---|---|
| 4.1: underlying spot compared with option premium | CONFIRMED | `intelligence/postmortem.py:1312`. The entry price is the premium at the write site (`options_recommender.py:701`). Every losing call becomes `right_signal_wrong_timing`. | postmortem-feedback |
| 4.2: staleness measured against the future expiry | CONFIRMED | `postmortem.py:1379`. Losing puts become `bad_data`. | postmortem-feedback |
| 4.3: options that expire worthless become PARTIAL; producer is `intelligence.postmortem`; contagion uses `abs` | PARTLY | `677,664,751,2014`; `oracle_weights.py:42`; `engine.py:669-686`. `POSTMORTEM_WRITES_LESSONS=False` is **deliberate** (the 2026-11-07 rule). The contagion code is dead. | postmortem-feedback |
| 4.6: the thesis invalidation monitor queries columns that don't exist | CONFIRMED | `thesis_invalidation_monitor.py:283-295` vs `schema.sql:321-343`. **live**, hourly (`intelligence/scheduler.py:1338`). | postmortem-feedback |

## Evolution (reach: `run_cycle` only. Hermes stopped calling it at 9fe8b16b (11-10), and the only remaining live caller is `POST /oracle/evolve`. Its output is live: all 305 active Oracle models are evolver-made.)

| Claim | Verdict | Evidence | Slice |
|---|---|---|---|
| 4.5: mutants go live at weight 1.0; failure-only pattern counts; LLM prose substring-matched | CONFIRMED | `model_evolver.py:234`, `trace_evolver.py:519-523,309-327,451` | evolver-shadow |
| new: mutants vote identically to their parent | CONFIRMED | `_mutate` and `adjust_horizon` change fields `_load_models` never reads (`engine.py:906`) | evolver-shadow |

## Validation and research machinery

| Claim | Verdict | Evidence | Slice |
|---|---|---|---|
| 5.1: research stack has no callers; promotion by count | PARTLY | The research factories are offline **by design** (`CROSS_SECTION_FACTORY.md:4`, `EDGE_STREAM.md:4,11`, `FACTORY_PRESCREEN.md:25`), so there is no air-gap defect. The gate claim is CONFIRMED (`validation/gates.py:199-212`, operator-only route). | validation-gates |
| new: the journal pins provisional rows to the oldest row | CONFIRMED | `contracts/handlers/journal.py:103-106` | validation-gates |
| 5.2: flat cost per day; autoresearch backtests constant long | CONFIRMED | `validation/backtest.py:507`, `scripts/autoresearch.py:1035-1042`. In simulation ~33% of random series "beat" the baseline. Gated by `AUTORESEARCH_ENABLED=False`, but the CLI ignores the flag. | validation-gates |
| 5.3: the gauntlet null pays costs (71–83% false positives on noise); ROBUST ignores DSR | CONFIRMED | `alpha_research/validation/gauntlet.py:166,173,469-470`. The recommendations of `docs/research/null-calibration-farm-v1.md` are not applied. | validation-gates |
| 5.4: no embargo; early stopping on validation; double shift | CONFIRMED | `alpha_research/ensemble.py:98-121`, `metrics.py:71`. Manual scripts only. | validation-gates |
| 5.5: cross-section factory sector leak, dropped picks, no borrow, 1-day holds | CONFIRMED, documented | All listed as known limits in `docs/reference/CROSS_SECTION_FACTORY.md:222-232,334` | no change (documented) |
| 5.6: module factory live parity covers 4 features | CONFIRMED, documented | `analysis/module_factory.py:177-212`. Parity work is tracked (#1160, #1275, #1295, #1296). | Desk lane |
| 5.8: local proud-kepler scripts assume Black–Scholes on realized vol, IV uplift, flat spreads, no fees | PARTLY | Real, and already superseded by the `grid_eval_*` real-quote scripts. BV_Ignition survives in forward shadow (N=29, underpowered). | none (local scripts) |

## Legacy paper executor (reach: scheduler job only if `GRID_ENABLE_LEGACY_PAPER_TRADING_JOB`, off by default; `POST /trading/execute-signals`)

| Claim | Verdict | Evidence | Slice |
|---|---|---|---|
| 6.1: negative Kelly becomes a 10% bet | CONFIRMED | `trading/signal_executor.py:267-275`. Paper only; no portfolio cap. | signal-executor |
| 6.2: Robinhood mirror opens but never closes; breaker skips closes | PARTLY | `149-152,420,458,327`. The mirror is reachable only through `?venue=robinhood` plus live-trading env configuration. The breaker recovers after cooldown. | signal-executor |
| new: breaker table never created, so a missing table crashes the run | UNCLEAR (prod) | `circuit_breaker.py:54`, `migrations/RAW_SQL_LEDGER.md:95` | signal-executor |
| new: missing leader data and stale prices skip closes or re-fire entries | CONFIRMED | `signal_executor.py:338,342,43-53` | signal-executor |

## Regime and state (owner: regime and state work, #1301; findings sent 2026-11-22. Priority by reach: changepoints → thesis (2.7) and the live auto_regime index (1.4); clustering is off the daily timer since 11-12)

| Claim | Verdict | Notes |
|---|---|---|
| 1.4: the stress index has zeroed concepts | PARTLY | 56–59% of nominal weight is unbound (not ">60%"), with no vol and no curve input. A warning is logged, so it isn't silent. |
| 1.6: max-pain unit mix | PARTLY | The raw value is about 90%, but the 63-day z-score removes it. Negligible. |
| 1.7: clustering drops equity and macro series on a 7-day calendar | CONFIRMED | Taken off the daily analytics timer on 11-12; API only now |
| 2.1–2.3: `market_state_daily` unread; 27-year z-scores; classifier order and pro-cyclicality | CONFIRMED | `desk_brief_sources.py:90` now reads it for display |
| 2.4: slope blows up at zero crossings | PARTLY | Max about 36, not ±1e6 |
| 2.5: DIM_WEIGHTS squared; cosine scale-invariant | PARTLY | Uncentered dimensions inflate similarity (new) |
| 2.6: autobnn fallback cliff | CONFIRMED | JAX is in no requirements file, so the fallback runs |
| 2.7: changepoints with no vintages and sign-blind | CONFIRMED | Hermes only. New: old breaks are republished as fresh every cycle into the served thesis. |
| 2.8–2.10: live inference abs max, 0.0 fill, `date.today()` | CONFIRMED | Coverage guard is dead (new). Reach depends on which PRODUCTION model rows exist (UNCLEAR). |
