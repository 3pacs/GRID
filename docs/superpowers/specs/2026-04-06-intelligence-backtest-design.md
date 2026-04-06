# Intelligence Backtest Engine + Crypto Signal Pipeline

**Date:** 2026-04-06
**Status:** Approved
**Goal:** Validate intelligence boost multipliers against real market data, fix crypto data blindness, and produce self-tuning calibration data.

## Problem

1. Intelligence boost multipliers (lever_pullers, trust_scorer, causation, forensics, cross_reference) use static defaults. No evidence they improve predictions.
2. Crypto has near-zero signal_sources coverage (4 entries for all of BTC/ETH/SOL combined vs 500+ per equity ticker). The intelligence layer is blind to crypto despite having pullers that collect raw data.
3. No mechanism to discover which information sources actually predict price moves vs which are noise.

## Solution

Four-phase system: fix crypto data gap, measure raw signal edge, replay full intelligence pipeline, output calibration + forensic reports.

---

## Phase 0: Crypto Signal Pipeline

### Problem
Existing crypto pullers (CoinGecko, CryptoQuant, DeFi Llama, DEX Scanner) write to raw_series/resolved_series but NOT to signal_sources. The intelligence layer (trust_scorer, lever_pullers, hypothesis_engine) only reads signal_sources. Result: crypto is invisible to intelligence.

### New Module: `ingestion/crypto_signals.py`

Bridge that reads existing crypto raw data and emits standardized signal_sources entries.

#### Signal Generators from Existing Data

**1. CoinGecko Price Signals** (from resolved_series)
- `crypto_price_breakout`: Price crosses 20d SMA by >5%
- `crypto_volume_spike`: 24h volume > 3x 7d average
- Tickers: All 21 in CRYPTO_MAP
- Source type: `coingecko`

**2. Binance Realtime Signals** (from realtime_candles)
- `crypto_momentum`: 5-min candle volume > 5x recent average
- `crypto_price_break`: Price breaks 1h high/low by >2%
- Tickers: 31 Binance streams (BTCUSDT→BTC, ETHUSDT→ETH, SOLUSDT→SOL, etc. — strip USDT suffix for normalization)
- Source type: `binance_rt`

**3. DeFi Llama TVL Signals** (from raw_series)
- `tvl_crash`: Protocol TVL drops >20% in 24h (already detected, just not written to signal_sources)
- `tvl_surge`: Protocol TVL jumps >30% in 24h
- `stablecoin_supply_shift`: USDT/USDC supply changes >1% in 7d
- Source type: `defi_llama`

**4. CryptoQuant Anomaly Signals** (from raw_series)
- `exchange_netflow_spike`: Exchange net flow > 3 sigma (already detected in anomalies list, currently discarded)
- `funding_rate_extreme`: Funding rate > 0.1% or < -0.05%
- `leverage_spike`: Estimated leverage ratio > 2 sigma
- Source type: `cryptoquant`

#### New Free-API Pullers

**5. Hyperliquid Puller** (`ingestion/altdata/hyperliquid_puller.py`)
- API: `https://api.hyperliquid.xyz/info` (public, no auth)
- Signals:
  - `hl_oi_spike`: Open interest change >10% in 4h
  - `hl_funding_extreme`: Funding rate > 0.05% per 8h (annualized >200%)
  - `hl_liquidation_cascade`: Liquidation volume > $10M in 1h
  - `hl_whale_position`: Single position > $5M opened/closed
- Tickers: BTC, ETH, SOL, + top 20 by OI
- Source type: `hyperliquid`
- Rate limit: None documented, use 1 req/sec

**6. Crypto ETF Flow Puller** (`ingestion/altdata/crypto_etf_flows.py`)
- Data sources: 
  - Yahoo Finance (IBIT, ETHA, GBTC, FBTC, ARKB, BITB volume/price)
  - SoSoValue API (free, aggregated ETF flow data)
- Signals:
  - `etf_inflow_spike`: Daily inflow > 2x 20d average
  - `etf_outflow_spike`: Daily outflow > 2x 20d average
  - `etf_premium_discount`: NAV premium/discount > 1%
- Tickers: BTC (via IBIT/GBTC/FBTC/ARKB/BITB), ETH (via ETHA)
- Source type: `crypto_etf`

**7. Fear & Greed Index** (`ingestion/altdata/fear_greed.py`)
- API: `https://api.alternative.me/fng/` (free, no auth)
- Signals:
  - `sentiment_extreme_fear`: Index < 20 (extreme fear = contrarian buy signal)
  - `sentiment_extreme_greed`: Index > 80 (extreme greed = contrarian sell signal)
  - `sentiment_shift`: Index changes >20 points in 7 days
- Tickers: BTC (market-wide proxy)
- Source type: `fear_greed`

**8. Whale Alert Signals** (`ingestion/altdata/whale_alert.py`)
- API: `https://api.whale-alert.io/v1/transactions` (free tier: 10 req/min, last 1h)
- Signals:
  - `whale_transfer_to_exchange`: >$10M moved to known exchange (sell pressure)
  - `whale_transfer_from_exchange`: >$10M moved from exchange (accumulation)
  - `whale_large_transfer`: >$50M moved between unknown wallets
- Tickers: BTC, ETH, SOL, XRP, DOGE (by blockchain)
- Source type: `whale_alert`
- Requires: Free API key (sign up at whale-alert.io)

### signal_sources Entry Format

All crypto signals write entries matching the equity pattern:
```sql
INSERT INTO signal_sources (
    source_type,    -- 'hyperliquid', 'crypto_etf', 'fear_greed', etc.
    source_id,      -- specific identifier (e.g., 'IBIT', 'binance:BTCUSDT')
    ticker,         -- normalized: 'BTC', 'ETH', 'SOL'
    signal_type,    -- 'BUY' or 'SELL' (directional) or signal name
    signal_date,    -- when the signal occurred
    signal_value,   -- JSON with details
    trust_score     -- initialized to 0.5 (neutral prior, trust_scorer will calibrate)
)
```

### Scheduler Integration

Add to `ingestion/scheduler.py` 24/7 block (crypto runs every day):
- `crypto_signals.bridge()` — every pull cycle (4x daily)
- `hyperliquid_puller.pull()` — every pull cycle
- `crypto_etf_flows.pull()` — weekday post-close only (ETFs are equity-traded)
- `fear_greed.pull()` — once daily
- `whale_alert.pull()` — every pull cycle

---

## Phase 1: Signal Co-occurrence Analysis (Edge Table)

### What It Does

For every scored oracle prediction (1,312 events across 8 tickers), look back N days and catalog which signal sources were active on that ticker before the prediction was made.

### Data Flow

```
oracle_predictions (WHERE scored_at IS NOT NULL AND actual_move_pct IS NOT NULL)
  × signal_sources (WHERE signal_date BETWEEN pred.created_at - lookback AND pred.created_at)
  → per-source hit/miss matrix
```

### Lookback Windows (per source type)

Match trust_scorer evaluation windows:
| Source Type | Window |
|-------------|--------|
| congressional | 30d |
| insider | 14d |
| options_flow | 7d |
| darkpool | 5d |
| lobbying | 45d |
| hyperliquid | 3d |
| crypto_etf | 7d |
| fear_greed | 7d |
| whale_alert | 3d |
| coingecko | 7d |
| binance_rt | 1d |
| defi_llama | 7d |
| cryptoquant | 7d |

### Output: Edge Table

Columns per row (source_type × ticker × signal_direction):
- `n_events`: How many predictions had this signal active beforehand
- `n_absent`: Predictions without this signal
- `hit_rate_present`: % correct when signal was present
- `hit_rate_absent`: % correct when signal was absent
- `avg_return_present`: Average actual_move_pct when signal present
- `avg_return_absent`: Average actual_move_pct when signal absent
- `information_coefficient`: Pearson correlation between signal presence (0/1) and outcome correctness (0/1)
- `p_value`: Statistical significance (t-test)
- `verdict`: EDGE (p < 0.05 and IC > 0.1), WEAK_EDGE (p < 0.10), NOISE (p >= 0.10), INSUFFICIENT (n < 10)

### Script

```
PYTHONPATH=. python3 scripts/backtest_intelligence.py edge-table
```

Outputs:
- `outputs/backtest/edge_table.csv`
- `outputs/backtest/edge_table.md` (formatted for LLM consumption)
- Prints summary to stdout

---

## Phase 2: Full Module Replay

### Scope

Top 5 tickers by signal density: NVDA, META, GOOGL, AAPL, MSFT (762+ equity predictions) plus BTC, ETH, SOL once Phase 0 fills crypto signals.

### What It Does

For each scored prediction:
1. Reconstruct signal state at `created_at` (query signal_sources with date filter)
2. Call each intelligence module with date-bounded queries:
   - `lever_pullers.get_lever_context_for_ticker(ticker)` — filter signals to pre-prediction
   - `trust_scorer.get_trusted_sources()` — compute trust from signals before prediction date
   - `causation_scoring.find_causes()` — check causation for actors active pre-prediction
   - `forensics.find_significant_moves(ticker, days=30)` — price moves before prediction
   - `cross_reference.run_all_checks(skip_narrative=True)` — macro reality at prediction time
3. Compute boost for each module
4. Record: `(prediction_id, module, boost_value, raw_confidence, adjusted_confidence, actual_outcome)`

### Accuracy Comparison

For each module:
- Raw accuracy: % correct at raw confidence
- Boosted accuracy: % correct at adjusted confidence
- Lift: boosted - raw (positive = module helps, negative = module hurts)
- Brier score improvement: raw Brier vs boosted Brier

### Script

```
PYTHONPATH=. python3 scripts/backtest_intelligence.py replay --tickers NVDA,META,GOOGL,AAPL,MSFT
PYTHONPATH=. python3 scripts/backtest_intelligence.py replay --tickers BTC,ETH,SOL  # after Phase 0
```

Output: `outputs/backtest/replay_results.csv`

---

## Phase 3: Outputs

### A. Edge Table (from Phase 1)

Already described above. Markdown + CSV.

### B. Calibration JSON

Computed from Phase 2 replay data. For each module × pattern_type:

```json
{
  "calibration_version": "2026-04-06",
  "generated_from": "762 predictions, 5 tickers",
  "modules": {
    "lever_pullers": {
      "convergence": {
        "boost": 1.22,
        "penalty": 0.68,
        "n_boost_events": 89,
        "n_penalty_events": 34,
        "boost_hit_rate": 0.71,
        "penalty_correct_rate": 0.82,
        "confidence": 0.87
      }
    },
    "trust_scorer": { "...": "..." },
    "causation": { "...": "..." },
    "forensics": { "...": "..." },
    "cross_reference": { "...": "..." }
  },
  "per_ticker": {
    "NVDA": {
      "lever_pullers": { "boost": 1.35, "penalty": 0.60, "n": 47 }
    }
  }
}
```

Saved to `outputs/backtest/calibration.json`. Can be loaded by the scoring engine to replace defaults.

Script: `PYTHONPATH=. python3 scripts/backtest_intelligence.py calibrate`

### C. Forensic Narrative

Per-ticker LLM report. Feeds Hermes the edge table + replay results for that ticker and asks it to explain:
- Which information sources actually predicted moves
- Which were noise or harmful
- What the optimal multiplier strategy is
- Specific examples: "On 2026-03-15, Pelosi bought NVDA. 14 days later, NVDA was down 7.3%. Congressional BUY on NVDA = contrarian sell signal."

Stored in `outputs/backtest/forensic_{ticker}.md`.

Script: `PYTHONPATH=. python3 scripts/backtest_intelligence.py report`

### Full Run

```
PYTHONPATH=. python3 scripts/backtest_intelligence.py full
```

Runs Phase 1 → Phase 2 → Phase 3A + 3B + 3C sequentially.

---

## File Structure

```
ingestion/
  crypto_signals.py              # Bridge: existing crypto data → signal_sources
  altdata/
    hyperliquid_puller.py        # NEW: OI, funding, liquidations, whale positions
    crypto_etf_flows.py          # NEW: IBIT/ETHA/GBTC flows
    fear_greed.py                # NEW: Fear & Greed Index
    whale_alert.py               # NEW: On-chain whale transfers

scripts/
  backtest_intelligence.py       # Main backtest CLI

outputs/backtest/
  edge_table.csv                 # Phase 1 output
  edge_table.md                  # Phase 1 formatted
  replay_results.csv             # Phase 2 output
  calibration.json               # Phase 3B output
  forensic_NVDA.md               # Phase 3C per-ticker
  forensic_META.md
  ...
```

## Dependencies

- No new paid APIs. All free tier or no-auth.
- Whale Alert requires free API key signup (optional, skip if not configured)
- CryptoQuant already has puller, just needs signal_sources bridge (requires existing CRYPTOQUANT_API_KEY)
- Hyperliquid, Fear & Greed, DeFi Llama, SoSoValue: fully public, no keys

## Success Criteria

1. Crypto tickers (BTC, ETH, SOL) have >50 signal_sources entries each within 7 days of deployment
2. Edge table identifies at least 2 signal sources with statistically significant edge (p < 0.05)
3. At least 1 intelligence module shows positive lift in the replay backtest
4. Calibration JSON produces multipliers that differ meaningfully from defaults (proving data-driven tuning works)
5. Forensic reports name specific actors, dates, and outcomes — not generic observations
