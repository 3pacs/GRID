---
title: GRID 200-Signal Catalog
date: 2026-04-13
branch: claude/review-cryexc-app-T2icl
supersedes: .planning/signals/TODO-200-catalog.md
canonical_source: docs/MODULE_CATALOG.md
related: docs/planning/SESSION-ROADMAP-2026-04-13.md
---

# GRID 200-Signal Catalog

200 candidate signals — **~100 new pullers** and **~100 new intelligence modules** — each plausibly delivering ≥1% certainty gain on the slice of GRID trades it touches. Expected hit rate after 30-day falsification testing: **10-25%**, yielding 20-50 shipped signals after survival filters.

**This is a brainstorm surface, not a ship list.** Every entry needs a falsification test before it gets promoted. The purpose of enumerating 200 is to give the oracle upgrade program a rich option surface to draw from — not to commit to building all 200.

---

## Files in this catalog

| File | Purpose | Entries |
|---|---|---|
| `CATALOG.md` | **This file.** Scoring rubric, tiering framework, domain map, index. | — |
| `PULLERS.md` | 100 new data sources grouped by domain | #1-100 |
| `INTELLIGENCE.md` | 100 new analytics / inference modules | #101-200 |
| `SHORTLIST-TIER-A.md` | Top ~40 highest-conviction picks in ship order | subset |

---

## Scoring rubric

Each entry is scored on seven dimensions:

| Dimension | Values | Notes |
|---|---|---|
| **Type** | `P` (Puller) / `I` (Intelligence) / `H` (Hybrid) | H = new puller + dedicated analytics module |
| **Tier** | `A` / `B` / `C` | A = ship after quick test. B = needs falsification. C = speculative. |
| **Status** | `NEW` / `EXTEND` / `WIRE` / `ACTIVATE` | NEW = net-new. EXTEND = adds to existing module. WIRE = connects existing-but-unused. ACTIVATE = revives DORMANT module. |
| **PIT** | `easy` / `tricky` / `hard` | How hard to make PIT-correct. Hard = release lag > 30d or revision risk. |
| **Cost** | `S` (1-3d) / `M` (3-10d) / `L` (10-20d) | Build time estimate. |
| **L/C** | `L` (Lever) / `C` (Condition) / `L+C` (Both) | Per GRID Prediction Causation Standard. Pure C signals get lower confidence. |
| **Coverage** | `%` of oracle prediction surface | Narrow (1 sector) vs broad (all asset classes). |

Format per entry:

```
### NNN. Signal Name `[P | Tier A | NEW]`
What it is — one line. Why ≥1% — mechanism.
**L/C:** Lever: actor pulls which valve. **Source:** API/scrape/paid/OSINT · **Cost:** S/M/L · **PIT:** easy/tricky/hard · **Coverage:** N%
**Location:** `target/filepath.py` within GRID's existing structure
**Overlap:** none | extends/wires existing_module.py at path
```

### Location field — where each module lives in the repo

The **Location** field is critical for merge-time cleanliness. Every entry specifies the exact target filepath within GRID's existing directory structure so this catalog is directly executable without re-deciding layout later.

Directory conventions (from current repo):

| Directory | Purpose | Example existing modules |
|---|---|---|
| `ingestion/altdata/` | US + global alternative data pullers | `dark_pool.py`, `cftc_cot.py`, `fed_liquidity.py` |
| `ingestion/international/` | Country-specific central bank / stats agencies | `ecb.py`, `rbi.py`, `jquants.py` |
| `ingestion/physical/` | Physical-economy data (satellite, ag, energy infra) | `usda_nass.py`, `viirs.py`, `patents.py` |
| `ingestion/trade/` | Trade flows + economic complexity | `comtrade.py`, `cepii.py` |
| `intelligence/` | Analytics, inference, actor networks, NLP | `trust_scorer.py`, `actor_network.py` |
| `analysis/` | Research engines, lead-lag scanners, flow theses | `backtest_scanner.py`, `flow_thesis.py` |
| `physics/` | Dealer gamma, vol surfaces, Greeks | `dealer_gamma.py` |
| `features/` | Feature engineering + importance tracking | `importance.py` |
| `discovery/` | Regime clustering, options scanners | `clustering.py`, `options_scanner.py` |
| `oracle/` | Prediction engine, calibration, reporting | `engine.py`, `calibration.py` |
| `trading/` | Recommenders, trackers, executors | `options_recommender.py` |

**Rule:** When an entry extends an existing module, Location points to that module. When it adds a new module, Location picks the directory matching its purpose. When it wires existing-but-unused code, Location points at both the source and destination module.

---

## The 1% certainty bar — what actually counts

A signal only clears the bar if it satisfies **all five filters**:

1. **Orthogonal** to GRID's existing 405-module surface (checked via `/grid-check-exists` before entry).
2. **PIT-feasible** — available at decision time with no lookahead.
3. **Broad enough coverage** — a 5% lift on 2% of trades is worth ~0.1% oracle-wide.
4. **Names a lever** (actor + valve) or **amplifies one** (condition). Pure condition signals alone produce 50/50 noise per GRID's Prediction Causation Standard.
5. **Survives 30-day walk-forward Brier holdout** vs baseline.

Most entries here satisfy 1-4 on paper; filter 5 is measured after build. **Do not ship without filter 5.**

---

## Domain map

### Pullers (100 total)

| Domain | Range | Count | Notes |
|---|---|---|---|
| China / Asia macro | #1-10 | 10 | GRID's biggest regional blind spot |
| Europe macro | #11-20 | 10 | EUR / ECB / TTF / German equities |
| Liquidity plumbing | #21-30 | 10 | Fed balance sheet components, FX basis, SOFR |
| Positioning + flows | #31-40 | 10 | CFTC, 13F, dealer, prime broker |
| Credit markets | #41-50 | 10 | TRACE, CLO, LL, muni, sovereign CDS |
| Commodities physical | #51-60 | 10 | LME, port stocks, refining, energy |
| Corporate filings | #61-70 | 10 | 8-K, S-1, auditor changes, going concern |
| Labor + consumer | #71-80 | 10 | WARN, mobility, credit card, alt data |
| Industrial + logistics | #81-90 | 10 | Freight, container, rail, trucking |
| OSINT + tail risk | #91-100 | 10 | Taiwan Strait, shipping, satellite, weather |

### Intelligence modules (100 total)

| Category | Range | Count | Notes |
|---|---|---|---|
| Inference architecture | #101-110 | 10 | **Highest leverage** — horizon, catalyst, Shapley, uncertainty |
| Causality + lead-lag | #111-120 | 10 | Granger, transfer entropy, do-calculus, DAG |
| Regime + state classifiers | #121-130 | 10 | HMM transitions, liquidity, recession, financial conditions |
| Positioning analytics | #131-140 | 10 | 13F delta clusters, dealer reconstruction, flow attribution |
| Event + catalyst engines | #141-150 | 10 | Fed reaction function, earnings cascade, post-announcement drift |
| NLP + narrative | #151-160 | 10 | Tone delta, 10-K novelty, lifecycle, analyst revision waves |
| Network + graph | #161-170 | 10 | Director interlock, audit firm, causal DAG, influence propagation |
| Calibration + uncertainty | #171-180 | 10 | Per-horizon Brier, drift, Shapley, Kelly-with-error-bars |
| Adversarial + meta | #181-190 | 10 | Red-team LLM, market-implied comparator, crowdedness |
| Outside-the-box | #191-200 | 10 | Reflexivity, second-order, pattern library, synthetic controls |

---

## Expected ship rate by tier

After falsification testing, rough expected survival:

| Tier | Count | Expected ship rate | Expected shipped |
|---|---|---|---|
| A | ~40 | 60-80% | 24-32 |
| B | ~100 | 15-25% | 15-25 |
| C | ~60 | 5-10% | 3-6 |
| **Total** | **200** | **~20-30%** | **~42-63** |

**Realistic target:** 40-50 signals shipped over the quarter. That's a 2x expansion of GRID's active alpha surface.

---

## Reading order

1. This file (you're here).
2. `SHORTLIST-TIER-A.md` — ~40 highest-conviction picks with Monday ship order.
3. `PULLERS.md` — full 100 new data sources.
4. `INTELLIGENCE.md` — full 100 new analytics engines.

Before building anything from this catalog, run:
```
/grid-check-exists <keyword>
```
to confirm no duplication. `docs/MODULE_CATALOG.md` is the canonical module inventory.

---

## Final tally

| File | Entries | Tier A | Tier B | Tier C | Status: NEW | EXTEND | WIRE | ACTIVATE |
|---|---|---|---|---|---|---|---|---|
| `PULLERS.md` | 100 | ~22 | ~48 | ~30 | ~82 | ~10 | 0 | ~4 |
| `INTELLIGENCE.md` | 100 | ~20 | ~50 | ~30 | ~66 | ~30 | ~4 | 0 |
| **Total** | **200** | **~42** | **~98** | **~60** | **~148** | **~40** | **~4** | **~4** |

**Shortlist:** 40 picks in `SHORTLIST-TIER-A.md` — the intersection of "high expected lift" × "reasonable build cost" × "clears 1% bar on paper."

## Estimated expected outcome if Tier A ships

Realistic build: **~14 weeks** for the 40-entry Tier A shortlist, staged across 5 phases with Phase 0 (inference architecture) first.

| Phase | Scope | Est. Brier lift |
|---|---|---|
| Phase 0 — Inference architecture | 10 items (#101-110) | **+5-8%** multiplicative |
| Phase 1 — Wire existing code | 5 items | **+3-5% on LEAPS** |
| Phase 2 — Positioning + flow | 10 items | **+2-4% on swing** |
| Phase 3 — Liquidity + macro regime | 6 items | **+2-3% oracle-wide** |
| Phase 4 — Regional blind spots | 6 items | **+2-3% on LEAPS** |
| Phase 5 — Earnings-specific | 3 items | **+1-2% on earnings** |
| **Aggregate (after correlation discount)** | **40 items** | **~10-15% Brier improvement** |

10-15% aggregate Brier lift is transformative — the magnitude of moving GRID from "pretty good systematic strategy" to "elite." That's the prize.

## Ship discipline

**Before shipping any entry:**
1. Run `/grid-check-exists <keyword>` to re-verify orthogonality (MODULE_CATALOG may have changed).
2. Build the minimum viable version (no premature abstraction).
3. Run a **30-day walk-forward Brier holdout** on the target slice.
4. Ship only if **Δ Brier ≥ 0.2%** on that slice. Below → abandon and move on.
5. Log the outcome in `intelligence/prediction_calibration.py` and the decision journal.

This discipline is what converts a 200-entry brainstorm into a real shipping pipeline.
