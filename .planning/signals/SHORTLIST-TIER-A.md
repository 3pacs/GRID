---
title: GRID Tier A Shortlist — Top 40 Highest-Conviction Picks
parent: CATALOG.md
date: 2026-04-13
---

# Tier A Shortlist — Top 40 picks in ship order

Distilled from the 200-entry catalog (`PULLERS.md` + `INTELLIGENCE.md`). These are the highest-conviction picks where expected Brier lift × coverage × cost-to-build favors shipping soon.

**Build order is not arbitrary.** Phase 0 inference upgrades must land first because they multiply the value of everything else. Phase 1 is quick wins from wiring existing-but-unused code. Phase 2 is the signal stack additions GRID actually needs for swing/quarterly/earnings/LEAPS horizons.

---

## Phase 0: Inference architecture (do first — ~2-3 weeks)

**These are multiplicative across every prediction. No new data required.**

| # | Entry | Cost | Est. lift | Location |
|---|---|---|---|---|
| 1 | **#101 Horizon-conditional oracle + per-horizon calibration** | L | **2-4% oracle-wide** | `oracle/engine.py`, `oracle/calibration.py`, schema migration |
| 2 | **#107 Per-horizon feature importance** (extends features/importance.py) | M | enables #101 | `features/importance.py` |
| 3 | **#108 Calibration persistence + drift alerts** | S | prevents silent decay | `oracle/calibration.py`, `intelligence/prediction_calibration.py` |
| 4 | **#102 Unified catalyst calendar + catalyst-aware scoring** | M | **~1.5% on 30% of trades** | `intelligence/catalyst_aggregator.py` |
| 5 | **#105 Market-implied probability comparator** | M | **~2% oracle-wide** | `intelligence/market_implied_prob.py` |
| 6 | **#103 Shapley attribution per prediction** | M | enables fragility-aware sizing (~1%) | `intelligence/shapley_attribution.py` |
| 7 | **#104 Ensemble disagreement as meta-feature** | S | **~1.5%** | `oracle/disagreement.py` |
| 8 | **#106 Uncertainty bounds + confidence intervals** | M | better sizing (~1%) | `oracle/uncertainty.py` |
| 9 | **#110 Kelly-with-error-bars + tail adjustment** | M | Sharpe +0.2-0.3 | `trading/options_recommender.py` |
| 10 | **#109 Per-regime submodel routing** (5 sub-oracles) | L | **1.5-3%** | `oracle/regime_router.py` |

**Phase 0 total:** ~3 weeks. **Expected stacked Brier lift: 5-8%.** This is where the biggest gains live.

---

## Phase 1: Quick wins — wire existing-but-unused code (~1 week)

**Zero new data, zero new build. Pure plumbing of existing modules.**

| # | Entry | Cost | Est. lift | Location |
|---|---|---|---|---|
| 11 | **#134 Vol surface wiring** (activate analysis/vol_surface.py) | S-M | **3-5% on LEAPS** | wire `analysis/vol_surface.py` → `discovery/options_scanner.py` + `trading/options_recommender.py` |
| 12 | **#133 Vanna/charm wiring** (activate physics/dealer_gamma.py:248-250) | S | free alpha on the floor | wire into `discovery/options_scanner.py` as signals #8 and #9 |
| 13 | **#165 Sector network mapper wiring** (activate 9 existing network mappers) | S | ~1% on sector plays | new `intelligence/sector_network_integrator.py` |
| 14 | **#171 Per-horizon Brier tracking** | S | enables #101 validation | `oracle/calibration.py` |
| 15 | **#132 Dealer GEX reconstruction** (replace net-short assumption) | L | **~2% on single-name options** | `physics/dealer_gamma.py` |

**Phase 1 total:** ~1 week. **Quick wins that unlock LEAPS and improve single-name options.**

---

## Phase 2: Positioning + flow stack (~3 weeks)

**Swing-horizon alpha that matches user's target horizons.**

| # | Entry | Cost | Est. lift | Location |
|---|---|---|---|---|
| 16 | **#24 Primary dealer Treasury positioning (FR 2004)** | S | ~2% on rates | `ingestion/altdata/primary_dealer.py` |
| 17 | **#140 Primary dealer analytics** (extreme detector) | M | — | `intelligence/dealer_positioning.py` |
| 18 | **#31 Prime broker net exposure notes** | M | **~2% on equity factor + index** | `ingestion/altdata/prime_broker_exposure.py` |
| 19 | **#32 Sovereign wealth fund rebalancing** (extends swf_network.py) | M | ~1-2% around quarter-ends | extends `intelligence/swf_network.py` |
| 20 | **#33 MSCI / Russell / S&P rebalance calendar** | M | **~2% on affected names** | `ingestion/altdata/index_rebalance.py` |
| 21 | **#147 Structured flow calendar aggregator** | M | unifies 18-20 | `intelligence/structured_flow_calendar.py` |
| 22 | **#41 FINRA TRACE corporate bond prints** | M | **~1.5% on credit + rotation** | `ingestion/altdata/trace_bonds.py` |
| 23 | **#36 / #131 13F quarterly delta tracking + clustering** | M | ~1.5% on factor/sector | extends `institutional_flows.py` + new `intelligence/thirteen_f_delta.py` |
| 24 | **#137 Insider cluster detector** (extends existing) | S | **~3% on rare triggers** | extends `insider_filings.py` |
| 25 | **#144 Sell-side revision wave detector** | M | ~1.5% | `intelligence/revision_wave.py` |

**Phase 2 total:** ~3 weeks. **Direct alpha on user's actual horizons (swing/quarterly/earnings).**

---

## Phase 3: Liquidity + macro regime (~2 weeks)

**Conditions every other prediction. Multiplicative value via the liquidity regime classifier.**

| # | Entry | Cost | Est. lift | Location |
|---|---|---|---|---|
| 26 | **#21 Fed H.4.1 granular decomposition** | S | **~2% on risk calls** | extends `fed_liquidity.py` |
| 27 | **#22 FX swap basis + cross-currency basis** | M | **~2% on equity + USD + credit** | `ingestion/altdata/fx_swap_basis.py` |
| 28 | **#23 SOFR dispersion + Fed SRF usage** | S | **~2-3% when triggered** | extends `repo_market.py` |
| 29 | **#122 Liquidity regime classifier** (5-state) | M | **~2-3% oracle-wide multiplicative** | `intelligence/liquidity_regime.py` |
| 30 | **#141 Fed reaction function estimator** | L | **~3% on Fed event trades** | `intelligence/fed_reaction_function.py` |
| 31 | **#123 Recession nowcast ensemble** | M | ~2% on LEAPS cyclical vs defensive | `intelligence/recession_nowcast.py` |

**Phase 3 total:** ~2 weeks. **Conditions every prediction. Essential for LEAPS and regime-sensitive trades.**

---

## Phase 4: Regional blind spots (~3 weeks)

**China + Europe + Japan macro. LEAPS-horizon work.**

| # | Entry | Cost | Est. lift | Location |
|---|---|---|---|---|
| 32 | **#2 China real-time electricity + rail** (activate akshare_macro) | M | ~1.5% on China-sensitive | reactivate `ingestion/international/akshare_macro.py` |
| 33 | **#1 China LGFV + trust product defaults** | M | **~2-4% on commodities + EM + RMB** | `ingestion/international/china_lgfv.py` |
| 34 | **#11 European gas storage + TTF curve** | S | **~2% on EUR + DAX + European credit** | `ingestion/altdata/ttf_gas.py` |
| 35 | **#6 Japan MOF + BOJ operations** | S | **~3-5% on JPY + carry** | `ingestion/international/boj_mof.py` |
| 36 | **#139 Cross-asset carry trade monitor** | M | ~3% on FX + risk | `intelligence/carry_trade_monitor.py` |
| 37 | **#5 Korea 20-day export flash** (activate kosis) | S | ~1.5% on anything semi/cycle | reactivate `ingestion/international/kosis.py` |

**Phase 4 total:** ~3 weeks. **LEAPS-relevant. Closes the US-centric coverage gap.**

---

## Phase 5: Earnings-specific stack (~2 weeks)

**For the earnings horizon specifically.**

| # | Entry | Cost | Est. lift | Location |
|---|---|---|---|---|
| 38 | **#142 Earnings surprise cascade predictor** | M | **~2% on earnings season follow-the-leader** | extends `intelligence/earnings_intel.py` |
| 39 | **#143 Post-announcement drift scanner** (by sector × mcap) | M | ~1.5% event-driven | `intelligence/post_announcement_drift.py` |
| 40 | **#151 Earnings call tone shift detector** (QoQ delta) | S | ~1.5% post-earnings drift | extends `earnings_transcript_analyzer.py` |

**Phase 5 total:** ~2 weeks. **Directly on user's earnings horizon.**

---

## Summary: total time and expected Brier lift

| Phase | Cost | Expected Brier lift | Notes |
|---|---|---|---|
| Phase 0 — Inference architecture | ~3 weeks | **+5-8%** | Multiplicative. Do first. |
| Phase 1 — Quick wins (wire existing) | ~1 week | **+3-5% on LEAPS** | Pure plumbing. |
| Phase 2 — Positioning + flow stack | ~3 weeks | **+2-4% on swing** | |
| Phase 3 — Liquidity + macro regime | ~2 weeks | **+2-3% oracle-wide** | Multiplicative. |
| Phase 4 — Regional blind spots | ~3 weeks | **+2-3% on LEAPS** | |
| Phase 5 — Earnings-specific | ~2 weeks | **+1-2% earnings** | |
| **Total** | **~14 weeks** | **~10-15% aggregate after correlation discount** | |

**Rough expected outcome:** 10-15% aggregate Brier improvement is **transformative**. For context, beating buy-and-hold by 3-5% annualized is considered elite for systematic equity strategies. Every entry above has been sanity-checked against `docs/MODULE_CATALOG.md` to avoid duplication.

---

## Sequencing rationale

1. **Phase 0 must land first.** The oracle can't use any new signal at its full value until it's horizon-aware and catalyst-aware. Adding new data through a flat combiner wastes 50%+ of the signal.

2. **Phase 1 is trivial and unlocks LEAPS.** Wiring `vol_surface.py` and vanna/charm is ~1 week of work for 3-5% on LEAPS. Near-free.

3. **Phases 2-4 are independent of each other** and can run in parallel if you want to divide work. Phase 3 (liquidity regime) is the highest leverage among them because it conditions everything.

4. **Phase 5 is last** because earnings-specific signals need the catalyst-aware scoring layer (#102) from Phase 0 to function.

## Falsification test before each ship

Every entry above needs a **30-day walk-forward Brier holdout** before it's considered shipped. Kill criterion: **Δ Brier ≥ 0.2%** on the target slice. Below that, abandon and move to the next item.

## What's deliberately excluded

Tier B and C entries from the catalog are not in this shortlist. Their expected build cost vs expected lift didn't clear a "ship in Q1" bar. They remain in `PULLERS.md` and `INTELLIGENCE.md` as the option surface for later phases.
