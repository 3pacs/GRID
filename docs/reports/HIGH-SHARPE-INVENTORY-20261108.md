# High-Sharpe inventory — what in GRID has a measurable return record (2026-11-08)

Author: Claude (Fable 5.1), session `claude/high-sharpe-ensemble-20261108`. Owner: Anik.
Scope: every strategy, signal or model in GRID whose returns, P&L or hit outcomes are persisted
anywhere (griddb, JSONL paper logs, `outputs/`, docs). The GEX 0DTE lane is out of scope and is
listed for existence only. Nothing here was changed; every number below was read on 2026-11-08
from grid-svr through a read-only session (`default_transaction_read_only=on`, 60 s statement
timeout, bounded queries) or from files on grid-svr, and the code was read at `origin/main`
`1d0a8491`.

**Headline: nothing in GRID has a real out-of-sample record with a positive, statistically
defensible Sharpe.** Three things have forward records large enough to mean something, and all
three say "no edge" (oracle predictions, signal-source feeds, options recommendations). The one
live paper strategy (adaptive rotation) has 23 closed trades, 19 of them at a fake flat price,
and has not traded since 2026-04-17. The one large backtest sweep (242 rotation variants) reports
Sharpe 4.5–4.7 and a +9,600 % return in 27 months on every parameter set, which is a data
artefact, not a record. Every other "Sharpe" in the repo is a docstring claim copied from a paper
or a vendor, with no stored artefact.

Conventions. *Record type*: live paper P&L (`paper_trades`), journal outcomes
(`decision_journal`), forward log (append-only JSONL, pre-registered), forward hit record (a
prediction table scored after the fact), backtest (a stored backtest output), claim (a number in
a docstring or doc with no artefact). *Sharpe* is annualised unless marked per-period; a CI is
given only where a per-period return series exists to bootstrap. *Verdict*: **real OOS record**
(forward, PIT-honest, n large enough to bound the mean), **in-sample only**, **no record**, or
**record but no edge**. "n too small" means the 95 % interval on the mean spans zero by a wide
margin or the sample is under ~100 independent observations.

## 1. Inventory

| # | Candidate (source module) | Record type and location | Period | n | Sharpe (CI) | Max DD | Turnover / cost | Data basis | Verdict |
|---|---|---|---|---|---|---|---|---|---|
| 1 | **Adaptive rotation, live paper** (`alpha_research/strategies/adaptive_rotation.py:301 run_rotation`; runner `scripts/rotation_paper_trader.py`; Hermes step 7g) | Live paper P&L: `paper_trades` strategy `adaptive_rotation_live`, wallet `grid_rotation_paper_7913a6f5` | 2026-04-01 → 2026-05-27 (last trade opened 2026-04-17; nothing since) | 23 closed trades, 0 open | per-trade mean +0.27 %, sd 0.83 % (not time-weighted; **19 of 23 trades have entry = exit to the penny**, the stale-price bug the script now guards against). `realized_alpha_daily` 180 d: mean alpha vs SPY **−1.72 % per trade** (n = 23). | wallet HWM = current, DD 0 (the flat trades hide it) | 20 % per position; no cost charged in the paper book | Signal PIT via `panel_builder`; execution price read directly from `resolved_series` (not PIT) | **no usable record** — n too small and corrupted; the "Sharpe 1.96 / 1.56" in the docstring and `paper_strategies.description` are FinRL-X's Alpaca numbers (`.claude/plan/quanta-alpha-integration.md:459`), not GRID's |
| 2 | **Adaptive rotation parameter sweep** (`alpha_research/strategies/rotation_variant_backtest.py`; fleet job `rotation_variant`, producer `/usr/local/bin/grid-rotation-producer.py` every 30 min) | Backtest: `astrogrid.backtest_run` / `backtest_result` (`run_key rotation_variant_*`) | window 2024-01-01 → 2026-03-25, 242 runs written 2026-05-08 → 05-19; the producer still queues jobs (50,707 COMPLETED, **111,256 FAILED** `compute_jobs`) | 117 weekly segments per run | **4.52 – 4.67 on all 242 configs** (mean 4.62); default config 4.61 | −8.4 % to −9.1 % | ~302 trades per run; no cost model | PIT window cache, `LATEST_AS_OF` vintages | **in-sample only, not credible**: mean total return +9,616 % vs QQQ +43.5 % in 27 months, and 240 parameter sets within 0.15 Sharpe of each other. §3 shows why: `*_full` vintages alternate between raw and dividend-adjusted closes, which the backtest reads as weekly jumps. Treat as a broken artefact, not a record. |
| 3 | **Pitch backtester, posture allocation** (`backtest/engine.py:302 PitchBacktester`, GMM regime → `POSTURE_ALLOCATIONS`) | Backtest JSON `/data/grid/outputs/backtest/backtest_results.json` (2026-03-24) | 2015-01-01 → 2026-03-22 (3,055 trading days) | 1 run | **0.163** vs SPY 0.441 and 60/40 0.281 (no CI stored; ~12 yrs of daily data would give roughly ±0.57) | −31.6 % (SPY −33.7 %) | 10 bp, 1-day rebalance lag | Regimes PIT; **asset prices from live `yfinance.download(auto_adjust=True)`**, not PIT | **in-sample only**, underperforms SPY on every metric |
| 4 | **Oracle predictions, all models** (`oracle/engine.py OracleEngine`, 9 seed models + factory mutants; scored by `scripts/score_oracle_trades.py`) | Forward hit record: `oracle_predictions` (`dedup_keep`), aggregates in `realized_alpha_daily`, `backtest_results` (nightly `walk_forward_profitability`), `docs/research/edge-report-forward-20261106.md` | 2026-03-26 → 2026-11-08 | 365 d dedup, non-AstroGrid: hit 1,873 / partial 1,162 / miss 3,042 / no_data 15,792 / pending 2,210 | **hit-only 30.8 %, hit+partial 49.9 %**; mean `pnl_pct` hit +11.9 %, partial +4.4 %, miss −9.2 % → **≈ −0.1 % per scored prediction**. Nightly stack check (365 d, n = 2,820): HIGH bucket mean −1.48 %, per-period Sharpe −0.15 → verdict **STACK BROKEN**. Edge report: forward N = 2,817, pooled hit 49.2 %, 0 EDGE, 105 INSUFFICIENT. | per-bucket cumulative DD ≈ −100 % (sum of signed pnl_pct) | n/a; `realized_alpha` charges 5 bp/side | signals PIT; entry price falls back to direct `resolved_series`; scoring on `raw_series` completed closes | **record but no edge** (the known 29 % dedup hit rate, zero alpha vs SPY). Excluded from the ensemble by instruction. |
| 5 | **Regime decisions / decision journal** (`scripts/auto_regime.py` → `journal/log.py`; model `GRID-REGIME-v1`, the only `model_registry` row in PRODUCTION) | Journal outcomes: `decision_journal` (2,450 rows, 2,438 with outcome) + append-only rescore `decision_journal_verdicts` | 2026-03-01 → 2026-11-07 | v1 verdicts: 25 HELPED / 37 HARMED / 3 NEUTRAL / 2,373 INSUFFICIENT_DATA; v2 rescore: 215 HELPED / 80 HARMED / 849 NEUTRAL / 1,228 INSUFFICIENT | not a return stream (posture vs SPY, outcome_value mean +1.7 % when HELPED, −1.5 % when HARMED); one decision per day with overlapping windows | n/a | n/a | SPY forward return, anchor-close scorer | **no rankable record** — a posture label, not a strategy; 50 % of rows have no data |
| 6 | **Sentiment SPY direction** (`intelligence/sentiment_scorer.py`; daily `_sentiment_outcomes_daily`) | Forward hit record: `sentiment_predictions` | 2026-04-02 → 2026-11-08 | 182 directional calls decided (+34 neutral, 5 pending) | **37.9 % correct** (69 / 182; Wilson 95 % 31–45 %); no signed-return series stored per row, so no Sharpe | n/a | n/a | SPY (`^GSPC`) 1-day realised return from `raw_series` | **record but no edge** (below coin flip); weight promotion correctly still gated off |
| 7 | **Signal-source feeds / trust scorer** (`intelligence/trust_scorer.py`; corrected eval `evaluation/signal_outcomes.py`, report `evaluation/edge_report.py`, PR #1056 Evidence page) | Forward hit record: `signal_sources` + append-only `signal_source_outcomes` (`sig-eval-4-sessions`) | through 2026-11-06 | forward N = 343,808 (CORRECT 175,177 / WRONG 170,198 / NO_MOVE 88,861 / INELIGIBLE 592,280) | **pooled hit 50.7 %**; by source type 11 × NO_EDGE, 0 × EDGE (clustered Wilson + BH); mean cost-adjusted return +3.76 % when CORRECT, −2.17 % when WRONG, i.e. ≈ +0.7 % per eligible signal gross of overlap, which the clustered test does not support | n/a | 10/30/100 bp by cap bucket in the eval | signal time from feed; outcome from completed closes | **record but no edge**. Not reconstructible as a PIT return stream without the raw feeds; not an ensemble candidate. |
| 8 | **Options recommendations** (`trading/options_recommender.py`, scored by `trading/options_tracker.py`) | Forward outcome record: `options_recommendations` | 2026-04-06 → 2026-10-03 | 762 scored (WIN 155, LOSS 96, EXPIRED 511) | mean `actual_return`: WIN +216 %, LOSS −54 %, EXPIRED −100 % → **≈ −30 % per recommendation** | n/a (every expiry is −100 %) | option premium; no commission model | `raw_series` YF close / live yfinance fallback | **record, strongly negative** |
| 9 | **Gem hunter outcomes** (`scripts/evaluate_gem_outcomes.py`) | Forward hit record: `gem_outcomes` | 2026-05-17 → 2026-06-25 (5 weeks, then stopped) | decided 7,301 (HIT 2,930 / WRONG_DIRECTION 2,250 / MISS 2,121); 14,474 INCONCLUSIVE | hit share **40.1 %** of decided; mean move +9.1 % (hit) / −10.6 % (wrong) / +0.2 % (miss) | n/a | n/a | yfinance closes | **record but no edge**, and dead since June |
| 10 | **Trial signals** (`intelligence/trial_outcomes.py`) | Forward: `trial_signals.fwd_return_30d` | 2026 | 537 scored of 1,186 | mean 30-day forward return **−10.95 %** (sd 14.1 %), 22 % positive | n/a | n/a | n/a | **record, negative** |
| 11 | **Hypothesis scoring v2** (`intelligence/hypothesis_trials.py`, ledger `grid-hypothesis-trials`) | Forward trial ledger (34 batches, hash-chained) + `discovered_hypotheses` | genesis 2026-11-07, 34 batches to 2026-11-08 | **4,750 declared trials: 0 confirmed**, 4,104 expired, 646 invalidated (BH q = 0.10). v1 "confirmed 133,089 / invalidated 71,476" scored GRID's own telemetry and is labelled unsound. | no return stream (one-look p-values) | n/a | n/a | PIT completed closes | **record but no edge** |
| 12 | **Hypothesis forward log v1** (`analysis/research_forward_log.py`, prereg 2026-09-26) | Forward log `/data/grid/paper_log/hypothesis_forward_v1/` | since 2026-09-29 | **0 candidates admitted from 0 scans** | — | — | — | known-at reads | **no record** |
| 13 | **trade_edge v2, large insider buys** (`paper_log/trade_edge/`, prereg 2026-10-07) | Forward log `/data/grid/paper_log/trade_edge_v2/` (LATEST.md 2026-11-06) | 2026-10-07 → | primary 30-session horizon: **0 closed** of 111 positions; 5-session interim: large n = 25 mean −1.99 % (28 % win), small n = 42 mean −2.06 %; SURVIVORSHIP_WARNING 5.4 % missing | interim only; first look at n = 100 | — | 10/30/100 bp round trip by cap | EDGAR acceptance time, completed closes | **no record yet** (n too small; v1's "+7.2 % mean excess, n = 153" is declared contaminated in its own prereg) |
| 14 | **VS1 insider-density panels v1–v8, sectors v2–v6** (`analysis/panel_insider_density*.py`) | Registrations only; `vs1-v7-stop-e0-scorecard.json` is a synthetic power scorecard | — | 0 (never opened; v6/v7 stopped on synthetic power < 0.50) | — | — | — | — | **no record** |
| 15 | **Legacy lead→follower paper strategies** (`trading/signal_executor.py`, `paper_engine.register_all_passed`) | Live paper: `paper_trades` strategies `h{id}_{leader}_{follower}` | opened 2026-03-26 | 10 strategies, **1 OPEN trade each, 0 closed**; job disabled (`GRID_ENABLE_LEGACY_PAPER_TRADING_JOB=False`) | — | — | — | direct `resolved_series`, no future-date bound (config comment) | **no record** (ROADMAP's "−$194.31 unrealised" is the whole history) |
| 16 | **Autoresearch / walk-forward validation** (`scripts/autoresearch.py` → `validation/backtest.py WalkForwardBacktest`) | Backtest: `validation_results` | 2026-03-22 → 2026-09-25 | 78 runs | **78 × FAIL** (latest: 134 days, Sharpe −0.15, DD −37.8 %, cost 10 bp) | — | 10 bp flat daily drag | PIT `FIRST_RELEASE` | **in-sample only, all failed** |
| 17 | **Contagion model** (`intelligence/chain_contagion.py`, scorer `intelligence/contagion_backtest.py`) | Forward hit record: `contagion_backtest_results` | 2026 | 7,647 rows | mean `accuracy_score` **0.05** | — | — | direct `raw_series` | **record but no edge** |
| 18 | **Thesis snapshots, SPY 5-day** (`analysis/thesis_scorer.py`) | `thesis_snapshots.outcome` | 2026 | 284 scored of 5,363 (57 correct / 211 partial / 16 wrong); scoring held (`THESIS_SCORING_HELD = True`) | — | — | — | — | **no rankable record** |
| 19 | **Baseline LLM predictions** (`scripts/baseline_predictions.py`) | `baseline_predictions` | 2026 | 36 (12 hit / 24 miss) | — | — | — | — | **n too small** |
| 20 | **AstroGrid models** (`astrogrid-seed-v1`, `guru-v1`, `oracle-v1`, `grid-crypto-v1`, `psi-oracle-v1`, `evidence-v2`, `spot-flow-v1`) | Forward hit record `astrogrid.prediction_score` (25,602 hit / 11,922 miss / 287 partial; mean alpha +0.38 % / −1.07 %) | 2026 | edge report: forward N = 2,329 pooled hit 80.3 % but **INSUFFICIENT** (3 models); seed-v1 30.8 % of 5,140; psi-oracle-v1 0 of 23 | — | — | — | mixed `resolved_series`/`raw_series` joins | **excluded**: separate product; **grid-crypto-v1 scores are corrupted** (char-list symbols, 31,329 rows graded vs SPY) and the rest is insufficient or negative |
| 21 | **Realized-alpha truth gate** (`alpha_research/realized_alpha.py`, daily 06:30) | Aggregate record `realized_alpha_daily` / `realized_alpha_trades` (upserted) | 2026-09-10 → 2026-11-08 | paper 180 d: 23 trades, mean alpha −1.72 %; oracle: 31 trades, mean alpha +6.6 % (hit 65 %) | n too small on both sources | — | 5 bp/side | SPY via `PITStore.get_feature_matrix` | **n too small**; this is the right instrument and it currently has almost nothing to measure |
| 22 | **QuantaAlpha factors** (`alpha_research/signals/quanta_alpha.py`: vol_regime, dual_horizon, trend_volume_gate, vol_price_divergence; published by `signal_adapter.py` to `signal_registry`, Hermes 7e) | No return record (`signal_registry` has no outcome column; 116k + 21k + 21k rows live) | — | — | docstring claims "OOS Sharpe 1.72 / 1.18 / 1.11 / 1.03" are the QuantaAlpha paper's numbers on commodity futures; `run-ensemble.md`: GRID val Sharpe 0.37 (113 tickers, 2026-03-31, log only); `gauntlet.py`: "0/7 runs achieved ROBUST in 2024 OOS" | — | 10 bp in `metrics.py` | PIT `panel_builder` | **claim only / no record** (trend_volume_gate and vol_price_divergence are dead without volume data) |
| 23 | **151 Trading Strategies engine** (`trading/strategy151.py`: mean reversion, pairs, 12-1 momentum, VRP, OU, sector rotation) | No record; output goes to LLM tasks only | — | — | — | — | — | reads `market_daily`, a table with **no DDL anywhere in the repo**; signal functions themselves are pure functions of a price panel | **no record** |
| 24 | **WorldQuant alpha101** (`features/alpha101.py`) | No record; same `market_daily` dependency | — | — | — | — | — | — | **no record** |
| 25 | **LightGBM factor ensemble** (`alpha_research/ensemble.py`, `scripts/run_ensemble.py`) | Log only | — | — | claim: "val Sharpe 0.37" (2026-03-31), "+0.15 Sharpe over best single factor" (QuantaAlpha paper) | — | — | PIT panel | **claim only** |
| 26 | **VIX/MA exposure scalar, credit cycle, macro regime signals** (`alpha_research/signals/exposure_scaler.py`, `credit_cycle.py`, `macro_regime.py`) | No record (`signal_registry` only) | — | — | — | — | — | PIT VIX feature; credit cycle reads `resolved_series` directly | **no record** |
| 27 | **PSI oracle / astro Sharpe hunts** (`oracle/psi_model.py`, `scripts/astro_sharpe_hunter.py`, `sharpe_199_*.py`) | psi-oracle-v1 forward: **0 hits of 23**; the rest log only | — | 23 | docstring claims "Sharpe 2.59 / 2.01 / 2.32 / 1.92"; `sharpe_199_research.py:222` literally cherry-picks the best contiguous window | — | — | direct `resolved_series` | **claim only, forward record negative**; slated for deletion in GRID-4 pivot |
| 28 | **Lead/lag backtest scanner** (`analysis/backtest_scanner.py` → `hypothesis_registry` PASSED rows) | In-sample Sharpe embedded in hypothesis text ("Sharpe 9.3 …") | — | 10,609 PASSED rows = 35 statements | annualisation bug (PUNCH-LIST #291); rows labelled `LEGACY_UNVALIDATED` | — | — | direct `resolved_series` | **in-sample only** |
| 29 | **Pitch paper-trade snapshots** (`backtest/paper_trade.py`) | 1 JSON snapshot 2026-03-22 + journal rows | — | 1 | — | — | — | yfinance | **no record** |
| 30 | **Solana autohedge, Hyperliquid live rotation, Robinhood mirror** (`trading/solana/`, `scripts/live_rotation_trader.py`, `trading/robinhood_*`) | `trading_order_log`: 0 trades; Hyperliquid journal table does not exist in griddb | — | 0 | — | — | — | — | **no record** (and stays OFF) |
| 31 | **E2 forward scoreboard** (`evals/e2`) | Scores other logs (S10, GEX) into `/data/grid/evals/e2_scoreboard` | — | derivative | — | — | — | — | not a strategy |
| — | GEX lane (`paper_log/gex_levels`, gen O/P/Q, trader Stage 0) | separate owner session | — | — | per the owner: no rule with positive expectancy as of 2026-11-07 | — | — | — | **out of scope** |

### Things with a Sharpe written next to them and nothing behind it

`adaptive_rotation.py:5-6` (1.96, 1.10), `trading-pipeline/SKILL.md:34` (1.56), `quanta_alpha.py:8-11`
(1.72, 1.18, 1.11, 1.03), `signal_adapter.py:219` (0.94 "on GRID data", no artefact),
`ensemble.py:7-8` (1.18 → 1.33), `psi_model.py:4-10` (2.59, 2.01, 2.32), `astro_market_timing.py:5`
(2.21), `sharpe_199_final.py:6` (1.92), `NEXT-AGENT-INSTRUCTIONS.md:79` ("BTC→SOL Sharpe 21"),
`HYPOTHESIS_SCORING_V2.md:149` ("Sharpe 9.3"). `paper_strategies.sharpe` and `oracle_models.sharpe`
are columns that no code ever writes. `docs/planning/LEVER-PACKAGE.md:272-290` already said most of
this in September; it is still true.

## 2. What can enter the ensemble (Part 2 universe)

A candidate for the walk-forward selector must be (a) a deterministic rule that exists in GRID
today, (b) computable from the point-in-time price/VIX panel alone at any past decision date,
(c) parameterised before this work (no fitting here), and (d) distinct from the oracle, AstroGrid
and hypothesis machinery, which the brief excludes and which §1 shows have no edge. That leaves
seven, all taken from §1 rows 1, 22, 23 and 26:

| id | rule as it exists in GRID | portfolio construction used here |
|---|---|---|
| `adaptive_rotation` | `alpha_research.strategies.adaptive_rotation.run_rotation`, default constants | the weights it returns (long only, cash remainder) |
| `s151_xs_momentum` | `trading.strategy151.Strategy151Engine.cross_sectional_momentum` (252/21, top 5) | LONG leg equal weight 0.5 gross, SHORT leg equal weight 0.5 gross |
| `s151_mean_reversion` | `Strategy151Engine.mean_reversion_scan` (20-day z, entry 2.0) | same long/short construction; no signal → cash |
| `s151_sector_rotation` | `Strategy151Engine.sector_rotation` (1/3/6/12-month risk-adjusted, top 3) on the sector ETFs | same long/short construction |
| `qa_dual_horizon_equity` | `alpha_research.signals.quanta_alpha.dual_horizon_equity` rank signal | long top quintile / short bottom quintile, equal weight, 0.5/0.5 gross |
| `qa_vol_regime_equity` | `alpha_research.signals.quanta_alpha.vol_regime_adaptive_equity` rank signal | same quintile construction |
| `vix_ma_exposure_spy` | `alpha_research.signals.exposure_scaler` VIX/MA(20) scalar | SPY weight = scalar in [0, 1], cash remainder |

Excluded and why: the pitch posture allocation (needs the GMM regime history; its own record says
Sharpe 0.16 vs SPY 0.44), `trend_volume_gate` and `vol_price_divergence` (need volume, dead),
pairs/OU/VRP from strategy151 (need per-rebalance cointegration fitting or implied vol; no record
either), credit cycle (needs HY spread and M2 features outside the price panel), sentiment,
signal feeds, trade_edge (not reconstructible from prices), oracle, AstroGrid, hypotheses (by
instruction and by record). The 240-combination rotation sweep is counted as trials already spent
on this data (it sets the deflated-Sharpe trial count), not as candidates.

The important honesty caveat: every candidate's parameters were chosen by people who had seen
some of this history (the rotation defaults come from FinRL-X's 2018–2025 backtest; strategy151
and QuantaAlpha parameters come from their papers). The selection layer is strictly walk-forward,
but the candidates themselves are not out of sample before 2026. The registration
(`docs/paper_log/high-sharpe-ensemble-v1-preregistration.md`) therefore reports the full window
as a walk-forward backtest, not a forward record, and names 2026-11-08 as the date after which
the ensemble's record becomes forward.

## 3. Data basis for the ensemble backtest — the `{ticker}_full` panel is unusable

Two read-only diagnostics on grid-svr (2026-11-08, 32 tickers, 2015-07-01 → 2026-11-06) decided
the price source for Part 2 and explain the rotation sweep's numbers.

**3.1 `resolved_series` `{ticker}_full` through `PITStore.get_feature_matrix` + the universal
split adjuster (what `panel_builder.build_price_panel` returns, i.e. what adaptive rotation, the
rotation sweep, QuantaAlpha and the realized-alpha gate read).** The `*_full` features are fed by
both `YF:{T}:adj_close` and `YF:{T}:close` (entity map, `normalization/entity_map.py:1056-1100`),
so successive vintages of the same observation date alternate between dividend-adjusted and raw
closes (SPY 2016-01-04: 201.02 on most vintages, 169.91 on 2026-04-24 and 04-29; NVDA
2021-07-19: 751.19 vs 18.74). Whichever vintage is latest wins per date, the series jumps, and
`adjust_panel` reads every drop beyond −40 % as a split and multiplies all earlier history.
First-to-last price ratio of the panel actually served, `LATEST_AS_OF` as of 2026-11-07:

| ticker | ratio served | plausible ratio | day jumps > 10 % |
|---|---|---|---|
| XLP | 8 × 10^201 | ≈ 2.5 | 101 |
| XLB | 9 × 10^134 | ≈ 2.6 | 98 |
| XLRE | 1 × 10^148 | ≈ 1.8 | 98 |
| XLI | 1.9 × 10^12 | ≈ 3.6 | 8 |
| GLD | 2,071 | 3.3 | 2 |
| XLK | 97 | 8.9 | 5 |
| XLV | 26 | 2.9 | 1 |
| NVDA | 366 | ≈ 60 (raw 2015 base) | 23 |
| USO | 96 | 1.0 | 16 |
| SPY | 3.5 | 4.2 | 2 |

`FIRST_RELEASE` is worse (SPY 296 jumps above 10 %, 233 of them reversing within five sessions;
IWM 329). With weights of 20 % per name and weekly segments clipped at ±50 %, a panel like this
hands any long-only rotation a few percent a week for free: the sweep's mean +9,616 % and its
parameter-insensitive Sharpe 4.6 are this artefact. The same panel sits under
`realized_alpha`'s SPY path and under every QuantaAlpha/LightGBM number logged in 2026. This is
the single most consequential finding of the inventory and is filed as a follow-up for the
resolver/entity-map owner (fix: one price field per `_full` feature, or a split-adjust that
refuses to adjust a reverting jump).

**3.2 `raw_series` `YF:{T}:adj_close` through `store.observations.read_window`.** With
`source='yfinance'`: clean for SPY (3.24× over the window), QQQ (5.58×), IWM, DIA, GLD, SLV, XLE,
TLT, XLU, XLV, XLF, XLK, XLY, XLC, IEF, SHY, HYG, LQD, USO (0–7 jumps above 10 %, 0–3 reversing);
junk for XLB, XLP, XLRE (≈ 200 jumps, ≈ 105 reversing, extra observation dates); mega caps only
from 2020-01-02 and stale since 2026-03-27 (AAPL 2026-05-13); no series at all for AMZN, TSLA,
XOM, CVX. The yfinance junk is a second writer stamping ≈ 0.6 values on market holidays
(2024-01-15, 2024-02-19, …, pulled 2026-06-25) into `YF:XLP:adj_close`.

**3.3 Tiingo writes the same `YF:{T}:adj_close` series ids under source `TIINGO`**
(`ingestion/tiingo_pull.py:262`), and that series is clean and complete for all 30 universe
tickers: exactly 2,856 observation dates each (the NYSE calendar 2015-07-01 → 2026-11-06), no
weekend dates, 0–7 jumps above 10 % on every name except TSLA (62, 16 reversing — genuine) and
NVDA (23), first-to-last ratios that match the market (SPY 4.24, QQQ 6.59, AAPL 12.1, MSFT 13.8,
NVDA 378, AMZN 11.9, GLD 3.33, TLT 0.95, USO 0.97). The registration (§3) therefore fixes a
mechanical source rule (Tiingo `adj_close` when it covers ≥ 95 % of SPY's dates, else yfinance)
and an exclusion screen (weekend dates, > 25 reversing jumps, < 90 % coverage, stale by > 10
sessions), and reports the exclusions with the look. Better data existed all along; the
`_full` features just never carried it cleanly.

## 4. Part 2 result — the registered look (2026-11-08T07:26Z, grid-svr, code `f5491b13`)

Record: `docs/reports/high-sharpe-ensemble-v1-look-20261108T0726Z.{json,md}` (registration
`docs/paper_log/high-sharpe-ensemble-v1-preregistration.md`, sha pinned and matched). Prices:
Tiingo `adj_close` for all 30 tickers (the §3 rule chose Tiingo everywhere; no exclusions, no
candidate failures, 0 of 16,950 segment returns missing). 513 weekly out-of-sample segments,
2017-01-06 → 2026-10-30, net of 10 bp per side.

**Verdict: FAIL.** The ensemble does not beat SPY buy-and-hold, and nothing in the candidate set
does either at the registered confidence.

| series | Sharpe | ann. return | max DD | total return | excess vs SPY, mean weekly | 95 % LB |
|---|---|---|---|---|---|---|
| ensemble (top-3 shrunk-Sharpe, inverse vol) | 0.78 | 9.1 % | −23.1 % | +137 % | −0.104 % | −0.249 % |
| SPY buy-and-hold | 0.87 | 14.3 % | −31.8 % | +275 % | — | — |
| 1/N over the 7 candidates | 0.13 | 0.6 % | −10.2 % | +6 % | | |
| best single in hindsight: `vix_ma_exposure_spy` | 0.94 | 14.0 % | −22.8 % | +264 % | | −0.080 % (α/7) |
| `adaptive_rotation` | 0.94 | 16.3 % | −24.7 % | | | −0.195 % (α/7) |
| `s151_xs_momentum` | 0.39 | 4.5 % | −26.5 % | | | −0.495 % |
| `s151_sector_rotation` | −0.31 | −2.5 % | −29.3 % | | | −0.627 % |
| `s151_mean_reversion` | −0.69 | −10.1 % | −70.0 % | | | −0.767 % |
| `qa_vol_regime_equity` | −1.36 | −12.6 % | −73.6 % | | | −0.852 % |
| `qa_dual_horizon_equity` | −1.40 | −13.0 % | −74.7 % | | | −0.863 % |

- Criterion 1 fails: mean weekly net excess over SPY −0.10 %, one-sided 95 % block-bootstrap lower
  bound −0.25 % (block 1, 10,000 resamples), P(mean ≤ 0) = 0.88.
- Criterion 2 fails: deflated Sharpe 0.00 at n_trials = 248 (observed per-week Sharpe 0.108 against
  an expected-maximum threshold of 0.393; the threshold is high because the seven candidates' Sharpes
  are widely dispersed, which is exactly what the DSR penalises).
- PBO over the candidate matrix is 0.00 (the in-sample best is also the out-of-sample best in every
  CSCV split) with P(OOS loss) 0.035: selection is *stable*, it just does not produce excess return.
  The selector picked `vix_ma_exposure_spy` 108 of 129 times and `adaptive_rotation` 90 times; the two
  are 0.61 correlated, both are ways of holding SPY-like beta with a volatility brake, and together
  they are a lower-beta SPY. The 1/N control shows the other five candidates are return drains.
- Ensemble turnover 20.6×/yr (adaptive rotation alone 31.9×/yr); the two QuantaAlpha rank signals
  turn over 83×/yr and lose 13 % a year after costs — at 10 bp per side that is their whole loss.

What has edge, honestly: nothing in GRID beats holding SPY after costs on 2017–2026 at the 95 %
level. Adaptive rotation (0.94) and the VIX/MA-scaled SPY (0.94) have the best risk-adjusted records
of the seven and both roughly match SPY's Sharpe with a smaller drawdown; neither has a positive
excess-return bound, and both are rules whose parameters were set with this history in view. Their
forward record starts now. The weekly `W-FRI` grid and 10 bp costs are declared choices; a FAIL
here is a FAIL of the registered design, not a proof that a daily or cheaper variant would also
fail — that would be v2, registered first.

Per §6 of the registration: no shadow book, no `model_registry` row, no flag flipped.
