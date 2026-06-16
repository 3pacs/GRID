# Prediction Causation Standard (SOP) (reference)

> Load when: generating predictions, building signal/thesis logic, or evaluating whether
> a prediction is well-formed. This is mandatory methodology, not optional guidance.

## Levers vs Conditions

Every prediction MUST separate **levers** (causes) from **conditions** (amplifiers).

**Levers** = specific actions by identifiable actors that open/close liquidity valves:
- "Fed raised rates 25bp" → credit valve closes → risk assets reprice
- "Tether minted $1B USDT" → crypto liquidity valve opens → BTC bid
- "Whale moved 10K BTC to Binance" → sell-side valve opening → price pressure
- "SEC approved spot ETH ETF" → institutional flow valve opens → ETH bid

**Conditions** = environmental features that amplify or dampen lever effects:
- Weekend low volume → amplifies any move (NOT a cause)
- Options expiry week → pins or accelerates (NOT a cause)
- High funding rates → enables a squeeze (NOT a cause)
- Q-end rebalancing window → creates flow (NOT a cause)

**The rule:** If you cannot name the valve, the flow direction, and the actor pulling it,
do not generate the prediction. Conditions alone produce 50/50 noise.

## Required prediction structure

```
LEVER:     [Who] did [what] affecting [which liquidity valve]
CONDITION: [Environmental factor] that amplifies/dampens the lever
THESIS:    Lever + condition → expected [direction] [magnitude] [timeframe]
INVALIDATION: [Specific condition] that proves the lever thesis wrong
```

**Wrong:** "BTC bearish because weekend low volume"
**Right:** "Whale X moved Y BTC to Binance (lever) in thin weekend book (condition) →
expect 5-8% drawdown within 12h. Invalidated if BTC reclaims $71K."

- Post-mortems are mandatory for every failed trade.
- Source accuracy auto-updates resolver priorities.

## Signal Source Types (trust_scorer evaluation windows)

- `congressional` (30d), `insider` (14d), `darkpool` (5d), `social` (5d), `scanner` (7d)
- `foreign_lobbying` (45d) — [[FARA]]-registered foreign agents influencing US policy
- `geopolitical` (7d) — [[GDELT]] tension spikes between country pairs
- `diplomatic_cable` (30d) — declassified [[FOIA]] cables revealing hidden motivations
- `lobbying` (30d) — domestic lobbying disclosure (Senate LDA + OpenSecrets)
- `campaign_finance` (60d) — PAC contributions mapped to policy outcomes
- `offshore_leak` (14d) — ICIJ Panama/Pandora Papers exposure

## Key Principles

- Every data point has a confidence label: confirmed/derived/estimated/rumored/inferred.
- Trust scores use Bayesian updating with 90-day recency half-life.
