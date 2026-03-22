# Signal Taxonomy — 10 Domains x 72 Subtypes

## Overview

GRID organizes all features into a canonical taxonomy of 10 financial domains,
each with multiple subtypes. This classification drives discovery, reporting,
and ensures comprehensive cross-asset coverage.

## Domain Map

### 1. RATES (8 subtypes)
Yield curves, Fed policy, treasury auctions, real rates, SOFR, term premium,
breakeven inflation, interest rate swaps.

### 2. CREDIT (6 subtypes)
HY spreads, IG spreads, credit default swaps, leveraged loans, credit flows,
municipal bonds.

### 3. EQUITY (10 subtypes)
Index prices, sector ETFs, market breadth, momentum, value factors, earnings,
insider transactions, analyst revisions, fund flows, market structure.

### 4. VOLATILITY (6 subtypes)
VIX level, term structure, realized volatility, implied volatility, skew,
vol-of-vol.

### 5. FX (5 subtypes)
Dollar index, major currency pairs, EM currencies, FX volatility, carry trade.

### 6. COMMODITY (9 subtypes)
Crude oil, natural gas, gold/silver, copper, agriculture, shipping/Baltic Dry,
energy equities, refinery margins, electricity.

### 7. SENTIMENT (8 subtypes)
Fear & greed index, Reddit/social, news volume, Wikipedia trends, prediction
markets, options flow, CFTC COT (Commitments of Traders), consumer surveys.

### 8. MACRO (8 subtypes)
GDP, employment/NFP, inflation/CPI, housing, manufacturing/ISM, PMI, consumer
confidence, banking/lending.

### 9. CRYPTO (8 subtypes)
Bitcoin on-chain, altcoin metrics, DeFi TVL, staking rates, DEX volume,
memecoin activity, exchange flows, supply dynamics.

### 10. ALTERNATIVE (variable)
Weather/satellite (VIIRS), patents (USPTO), geopolitical events (GDELT),
FDA approvals, congressional trading, SEC filings (EDGAR).

## Current Feature Count

- **36 core features** in feature_registry (model-eligible)
- **464+ total features** including derived signals and computed indicators
- Each feature maps to exactly one domain and one subtype
- Features tagged with: family, transformation, normalization, lag_days,
  eligible_from_date, model_eligible flag

## How Taxonomy Is Used

1. **Discovery**: Ensure hypotheses span multiple domains (orthogonality)
2. **Briefings**: Walk through each domain systematically
3. **Contradiction scan**: Flag disagreements between domains
4. **Feature selection**: Ensure model inputs are diversified across domains
5. **Reporting**: Organized signal snapshots in the PWA

## Key Files

- `scripts/signal_taxonomy.py` — Domain/subtype definitions and DB updater
- `features/registry.py` — FeatureRegistry query interface
- `schema.sql` — feature_registry table with family CHECK constraint
