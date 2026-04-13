---
id: SEED-001
status: dormant
planted: 2026-04-13
planted_during: review of jose-donato's cryexc/flowsurface projects (branch claude/review-cryexc-app-T2icl)
trigger_when: GRID adds intraday tactical entry timing OR crypto options trading via Deribit/Paradex/Lyra OR real order routing where slippage matters
scope: Medium-Large
---

# SEED-001: Port cryexc/flowsurface orderflow primitives (footprint, CVD, heatmap) into GRID

## Why This Matters

Jose Donato (OpenBB founding engineer) built a progression of crypto microstructure
tools culminating in **cryexc** (Python/FastAPI/DuckDB) and **flowsurface** (Rust/Iced).
These projects contain production-quality implementations of three primitives GRID
currently lacks:

1. **Footprint candles** — per-price-bucket buy vs sell volume within each candle,
   with imbalance + naked-POC studies. Reveals *aggression asymmetry* inside a bar:
   who was hitting vs. lifting. This is the gold standard for tape-reading.
2. **CVD (Cumulative Volume Delta)** — running net of buy volume minus sell volume.
   CVD divergences (price up, CVD flat/down) are a textbook condition-layer signal.
3. **Orderbook heatmap** — time-series L2 book snapshots rendered as a "painted book,"
   showing resting walls being eaten, spoofing, iceberg refills, and liquidity gaps.

**Why NOT now:** At GRID's current 6h oracle cadence predicting multi-day-to-week
regime moves, these tick-level microstructure signals decay in seconds-to-minutes.
The timescale mismatch is fatal — by the time a cycle rolls over, the CVD divergence
has already been priced in. Expected Brier improvement at current cadence: **0.1–0.5%**,
below the 1% materiality bar we've set for new signal work.

Additional reasons it doesn't clear the bar today:

- **Coverage is narrow.** Only applies to BTC/ETH/SOL — ~10% of GRID's prediction
  surface (oracle also spans equities, credit, commodities, FX). Weighted oracle-wide
  contribution is tiny.
- **Orthogonality is weaker than it looks.** CVD is highly correlated with funding
  rates (already in `ingestion/altdata/binance_puller.py`-adjacent pullers), open
  interest changes, institutional flows (`ingestion/altdata/institutional_flows.py`,
  `ingestion/altdata/dark_pool.py`), and `intelligence/dollar_flows.py`. Incremental
  orthogonal information << raw signal value.
- **Violates GRID's Prediction Causation Standard.** CVD/footprint are pure
  condition-layer (amplifiers), not levers. They can't name the actor pulling the
  liquidity valve — just evidence that *someone* was aggressive. Anonymous condition
  data doesn't move a lever-weighted oracle much.
- **Cost is non-trivial.** 2-4 weeks: new 24/7 WebSocket consumer tier, hot storage
  (DuckDB-in-memory or Redis Streams), aggregation service, PIT-compatible tick
  ingestion, PWA component, tests. Adds a real-time tier to an EOD-timescale platform.

**Why the seed matters:** When any trigger condition fires, these primitives jump
from "trader entertainment" to genuine oracle alpha. Having the research, source
repos, and cheap falsification test pre-captured means we can move in days, not
weeks of rediscovery.

## When to Surface

**Trigger:** Any one of these conditions unlocks this seed.

This seed should be presented during `/gsd:new-milestone` when the milestone scope
matches any of these conditions:

- **Intraday tactical entry layer** — GRID oracle starts producing 1-6h entries
  instead of 6h-to-weeks regime calls. Timescales now match. Expected Brier lift
  jumps to **2-5%** on crypto names.
- **Direct crypto options trading** — GRID begins quoting or trading options via
  Deribit, Paradex, Lyra, or similar. Book imbalance on the underlying becomes a
  genuine pricing input (feeds into `discovery/options_scanner.py` and
  `trading/options_recommender.py` for crypto names).
- **Real order routing / execution quality matters** — GRID routes live orders and
  slippage becomes a P&L item. CVD at the moment of execution saves hard dollars,
  separate from prediction accuracy.

Also surface if the new milestone touches: `trading/exchanges/`, `physics/dealer_gamma.py`
for crypto underlyings, or any new PWA view focused on crypto microstructure.

## Scope Estimate

**Medium-Large** — 2-4 weeks of focused work for the full port.

Breakdown:
- ~3 days: WebSocket consumer service (Python + aiohttp or similar; does NOT need
  to be Go/Rust for GRID's scope per the cryexc Python-vs-Go lesson)
- ~3 days: Hot storage tier (DuckDB-in-memory pattern from cryexc-backend) +
  PIT-compatible flush to Postgres/Timescale
- ~3 days: Aggregation engine — footprint bucketing `(round(price/tick)*tick,
  floor(ts/interval)*interval)`, CVD accumulator, heatmap snapshots
- ~2 days: `intelligence/orderflow.py` module wrapping computed signals for the
  oracle + Prediction Causation Standard-compliant output (clearly labeled as
  conditions, never standalone levers)
- ~3 days: PWA view (new route, D3 or lightweight-charts rendering)
- ~2 days: Tests (happy path + PIT correctness for tick-level `observation_date`
  vs `release_date`)
- ~2 days: Integration with oracle + Brier measurement harness

### Cheap Falsification Test (if uncertain whether to commit the full port)

**Budget:** ~2 days. **Purpose:** prove or kill the thesis before spending 2-4 weeks.

1. Compute **1h-bar CVD for BTCUSDT only** from Binance aggTrade data GRID already
   has access to via `ingestion/altdata/binance_puller.py`
2. Feed as a single feature into **one** oracle model (not all 5) in `oracle/engine.py`
3. Hold out 30 days, measure Brier delta vs baseline on BTC predictions only
4. **Ship if Δ ≥ 0.2% on BTC predictions alone.** Kill otherwise.

If the test passes, scale up: add ETH/SOL, add footprint + heatmap, build the PWA
view, wire into all 5 oracle models.

## Breadcrumbs

### Source repos (external — jose-donato)

- `https://github.com/jose-donato/cryexc-backend` — Python/FastAPI/DuckDB reference
  implementation. 58★. **Closest to GRID's stack** — port this first.
- `https://github.com/jose-donato/flowsurface` — Rust/Iced native desktop version.
  12★. Cleanest architecture (workspace: `exchange/` + `data/` + `src/`). Use for
  footprint/heatmap algorithm reference.
- `https://github.com/jose-donato/crypto-futures-arbitrage-scanner` — Go. 124★.
  Multi-venue spread pattern if we ever add cross-venue arb as a separate signal.
- `https://github.com/jose-donato/crypto-orderbook` — Go. 120★. L2 book consumer
  pattern reference.
- `https://github.com/jose-donato/binancef_l3_estimate_go` — Go. 27★. L3 order
  inference from L2 (advanced — probably overkill for GRID).

### Related GRID modules (will integrate with or overlap)

- `discovery/options_scanner.py` — existing 7-signal mispricing detector. Crypto
  options extension hooks here.
- `trading/options_recommender.py` — generates specific trade recommendations.
  Would consume CVD as an execution-timing input if triggers fire.
- `physics/dealer_gamma.py` — GEX/vanna/charm for equity options. Crypto analog
  (Deribit DVOL, Paradex GEX) would live alongside when crypto-options trigger
  fires.
- `intelligence/dollar_flows.py` — existing USD normalization and capital flow
  quantification. CVD is correlated — check orthogonality before claiming alpha.
- `intelligence/lever_pullers.py` — existing actor-action tracker. CVD/footprint
  are conditions, NOT levers. Must stay disciplined per the Prediction Causation
  Standard (lever + condition + invalidation).
- `ingestion/altdata/binance_puller.py` + `ingestion/altdata/hyperliquid_puller.py`
  + `ingestion/altdata/cryptoquant_puller.py` — existing crypto data pullers.
  aggTrade tick data for the falsification test comes from here.
- `ingestion/altdata/dark_pool.py`, `ingestion/altdata/institutional_flows.py`,
  `ingestion/altdata/unusual_whales.py` — existing flow-adjacent signals. Check
  correlation with CVD before committing to full port.
- `store/pit.py` — tick data must pass `assert_no_lookahead()` and carry both
  `observation_date` and `release_date`. Non-trivial for sub-second data.
- `oracle/engine.py` — where the new feature gets wired in. 5 competing models
  with dynamic weight evolution.

### Related GRID decisions / patterns

- **Prediction Causation Standard** (CLAUDE.md) — the doctrine that made this a
  seed rather than a phase. CVD/footprint are conditions, not levers. Must be
  labeled as such wherever consumed.
- **Hermes scheduler** (`scheduler.py`, NOT `scheduler_v2.py`) — existing 48 cron
  pullers. Real-time WS consumers do NOT fit Hermes; they'd be a new service tier
  (new systemd unit alongside grid-api, grid-hermes, etc.).
- **1% materiality bar** — established in this session. New signal work needs
  ≥1% expected Brier improvement to justify phase-level effort. Seeds can be
  smaller bets.

## Notes

### Session context

Seed planted during code review of `https://cryexc.josedonato.com/app` (branch
`claude/review-cryexc-app-T2icl`). User asked whether porting cryexc primitives
would give ≥1% trade certainty improvement. Analysis concluded no — expected
0.1-0.5% at current 6h oracle cadence due to timescale mismatch. User chose to
park it as a conditional seed rather than reject outright.

### Lessons from jose-donato's project progression

Donato built this arc over 4 repos:
```
crypto-orderbook (Go, book viz)
    ↓
crypto-futures-arbitrage-scanner (Go, cross-venue spread)
    ↓
binancef_l3_estimate_go (Go, L3 inference from L2)
    ↓
flowsurface (Rust, native pro terminal)  ← the "ideal form"
    ↓
cryexc (Python+DuckDB, FastAPI)           ← the "ship-it form"
```

He built the ideal form in Rust, realized distribution is hard for a native app,
then re-did it as a browser-accessible Python service covering 70% of functionality
with 10% of the code. **Classic second-system lesson — don't rewrite in Rust for
GRID.** Port the three primitives to Python and ship them in the existing PWA.

### Also worth capturing separately (not in this seed)

- **Tree of Alpha news WebSocket** — free, fast crypto news feed used by cryexc.
  Could be wired into `ingestion/altdata/` alongside `tiingo_news.py` /
  `world_news.py`. Unrelated to the orderflow port — plant a separate seed/todo
  if interesting.
- **`Decimal` for all price/volume math** — flowsurface uses `rust_decimal` for
  fixed-precision price arithmetic. GRID likely has `float` drift in
  `trading/options_recommender.py` and `store/pit.py`. Audit is separate work,
  not part of this seed.
- **Perceptually uniform colormaps** — flowsurface uses `palette` crate (OKLab/LCH)
  for footprint imbalance rendering. Python equivalent is `colour-science`. Matters
  for any future heatmap view in the PWA.
