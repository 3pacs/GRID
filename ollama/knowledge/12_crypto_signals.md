# Crypto-Native Signals — DexScreener & Pump.fun

## Why Crypto Data Matters for GRID

Crypto-native metrics are orthogonal to GRID's traditional macro/rates/credit
features. They capture **speculative risk appetite** in real-time — when retail
traders are minting memecoins at record pace, that tells you something about
market psychology that VIX and credit spreads cannot.

Key insight: crypto speculation often leads traditional risk-on/risk-off
transitions by days or weeks because the participants act faster and with
less friction.

## Data Sources

### DexScreener (api.dexscreener.com)
- **What**: Aggregates on-chain DEX trading data across all major chains
- **Auth**: None required (free tier: 300 req/min)
- **Quality**: MED trust — data is real on-chain activity, but aggregation
  methodology is proprietary
- **Latency**: REALTIME — updated every block

### Pump.fun (frontend-api-v3.pump.fun)
- **What**: Solana memecoin launchpad — tokens created via bonding curves
- **Auth**: Undocumented frontend API (no official API docs)
- **Quality**: LOW trust — endpoints may change without notice
- **Latency**: REALTIME

## Feature Descriptions

### DexScreener Signals (family: crypto)

| Feature | What It Measures | Economic Mechanism |
|---------|-----------------|-------------------|
| `dex_sol_volume_24h` | Total Solana DEX volume (USD) | Speculative activity level — spikes during mania, collapses during fear |
| `dex_sol_liquidity` | Total liquidity depth (USD) | Market maker confidence — withdrawal signals risk-off |
| `dex_sol_buy_sell_ratio` | Buy/sell transaction ratio | Directional pressure — >1.2 = aggressive buying, <0.8 = panic selling |
| `dex_sol_momentum_24h` | Avg 24h price change | Momentum breadth — when everything pumps, froth is high |
| `dex_sol_txn_count_24h` | Total transactions | Raw activity — more txns = more speculation |
| `dex_sol_boosted_tokens` | Paid boost count | Token teams paying for visibility = peak speculation |

### Pump.fun Signals (family: crypto)

| Feature | What It Measures | Economic Mechanism |
|---------|-----------------|-------------------|
| `pump_new_tokens_count` | New token launches | Memecoin mania gauge — 1000+ launches/day = euphoria |
| `pump_koth_mcap` | King-of-the-hill market cap | Peak speculative ticket size |
| `pump_graduated_count` | Bonding curve completions | Tokens reaching DEX listing — sustained vs flash interest |
| `pump_graduated_avg_mcap` | Avg graduated market cap | Quality of speculation — higher = real interest |
| `pump_latest_avg_mcap` | Avg new token market cap | Entry-level speculation willingness |

## Hypotheses to Test

These signals are most useful for:

1. **Regime transition leading indicators** — Crypto froth metrics (volume,
   new token rate) may lead VIX moves by 3-7 days
2. **Risk-on confirmation** — When traditional signals say GROWTH and crypto
   signals confirm (high volume, positive momentum), conviction is higher
3. **Divergence warnings** — When equities rally but crypto speculation
   collapses (falling volume, negative momentum), it may signal fragile
   growth that reverses
4. **Speculative froth ceiling** — Extreme readings (buy/sell >2.0, thousands
   of new tokens/day) historically precede broad risk-off events

## Caveats

- **No PIT correctness** — These are real-time snapshots stored daily. There
  is no revision history or vintage tracking.
- **Regime-dependent** — Crypto markets themselves undergo regime shifts
  (bull/bear cycles) that may decouple from traditional markets.
- **Short history** — DexScreener Solana data starts ~2023, Pump.fun ~2024.
  Walk-forward validation will have limited eras.
- **API fragility** — Pump.fun endpoints are undocumented and may break.
  DexScreener is more stable but still no SLA.
