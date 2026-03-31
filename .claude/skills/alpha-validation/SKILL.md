# alpha-validation

Validates GRID predictions against the Prediction Causation Standard (SOP). Ensures every prediction is mechanically sound, identifies the specific levers (actors + actions) that move markets, and rejects predictions built on conditions alone.

## When to Use This Skill

- Before logging any prediction to the immutable journal (`journal/log.py`)
- When reviewing model outputs for inference quality
- When debugging why a backtest prediction failed
- When analyzing cross-reference intelligence to translate into market theses
- When conducting post-mortems on failed trades

## Core Validation Rules

### 1. Prediction Structure Check

Every prediction MUST contain four required sections:

```
LEVER:     [Who] did [what] affecting [which liquidity valve]
CONDITION: [Environmental factor] that amplifies/dampens the lever
THESIS:    Lever + condition → expected [direction] [magnitude] [timeframe]
INVALIDATION: [Specific condition] that proves the lever thesis wrong
```

**Bad Examples (will fail validation):**
- "BTC bearish because weekend low volume" — uses only condition, no lever
- "Rates up 25bp" — missing direction, magnitude, timeframe; no liquidity valve named
- "Options expiry week creating vol" — condition alone, no actor/action

**Good Example (will pass):**
- "Fed raised rates 25bp (lever) in a risk-off environment (condition) → expect 5-8% drawdown in equities within 72h. Invalidated if SPX reclaims 4850."

### 2. Lever Validation

A lever must answer: **Who pulled the valve, and which valve did they pull?**

Valid levers name:
1. **Specific actor** — Fed, Tether founder, Whale X, SEC, specific institution (not "market makers" or "smart money")
2. **Specific action** — "raised rates 25bp", "minted $1B USDT", "moved 10K BTC to exchange", "approved spot ETF", "banned product"
3. **Specific valve** — credit, liquidity, institutional flow, retail flow, sell-side, funding-driven squeeze, regulatory approval, margin available

Common valve categories:
- **Credit valve** — Fed funds rate, reverse repos, TLF, credit spreads
- **Crypto liquidity valve** — exchange inflows, Tether mints, staking yields, bridge flows
- **Equity flow valve** — ETF flows, 401k window, Q-end rebalancing, buyback windows
- **Funding valve** — options funding rates, margin costs, borrow rates
- **Regulatory valve** — SEC approvals, enforcement actions, tax rule changes
- **Central bank valve** — QE/QT, intervention, guidance shifts

**Lever validation checklist:**
- [ ] Actor is named specifically (not "the market" or "sentiment")
- [ ] Action is quantified (% change, dollar amount, specific date/event)
- [ ] Valve name is concrete (not "pressure", "momentum", "risk")
- [ ] Causal link from action to valve is mechanical (not speculative)

### 3. Condition vs Cause Discrimination

**CONDITIONS amplify or dampen, they do not cause.** The #1 error pattern in GRID predictions.

Valid conditions (these amplify/dampen but do NOT cause):
- Weekend/holiday low volume → amplifies any move
- Options expiry pinning → can accelerate moves in either direction
- High funding rates → enables (but does not force) liquidation cascades
- Q-end rebalancing window → creates time-sensitive flows
- Low VIX → complacent risk positioning (can amplify shocks)
- Fed funds rate high → tighter financial conditions (dampens leverage)

Invalid conditions (these are vague causes, not conditions):
- "Sentiment is negative" — sentiment is not environmental, it is the sum of all causes
- "Macro backdrop is weak" — backdrop is not a condition, it is collection of causes
- "Volatility is high" — this is a symptom, not a cause
- "Market structure has changed" — this is meaningless without specificity

**Condition validation checklist:**
- [ ] Condition is environmental, not behavioral ("low volume" yes, "fear" no)
- [ ] Condition amplifies or dampens the lever effect
- [ ] Condition is measurable (VIX, volume, day-of-week, calendar event, rate level)
- [ ] Prediction still works if condition is NOT present (but with lower magnitude)

### 4. Thesis Validation

The thesis must specify:
1. **Direction** — bullish, bearish, range-bound, rotation
2. **Magnitude** — percentage move, duration, affected instruments
3. **Timeframe** — hours, days, weeks (be specific: "within 48h", "by quarter-end", not "eventually")

**Thesis validation checklist:**
- [ ] Direction is unambiguous (bullish/bearish, not "will move")
- [ ] Magnitude is quantified in basis points or percent (not "significant" or "strong")
- [ ] Timeframe has clear boundary (not "ongoing" or "medium-term")
- [ ] Expected move is proportional to the lever size (Fed 25bp move ≠ 20% equity move)

### 5. Invalidation Condition Validation

For every prediction, define the specific observation that proves the thesis wrong.

**Good invalidation conditions:**
- "Invalidated if SPX closes above 4850 by Friday close"
- "Invalidated if Fed suddenly cuts rates instead"
- "Invalidated if Bitcoin moves above whale's reported cost basis"
- "Invalidated if alternative leverage source opens (USDC, stablecoin supply spike)"

**Bad invalidation conditions:**
- "Invalidated if sentiment changes" — sentiment is not observable
- "Invalidated if macro improves" — too vague
- "Invalidated if thesis is wrong" — circular

**Invalidation validation checklist:**
- [ ] Condition is observable (price level, data point, event)
- [ ] Condition is specific (includes number/date where applicable)
- [ ] Condition is mutually exclusive from the thesis (thesis true XOR invalidation true)
- [ ] Condition is binary (not a spectrum)

### 6. Confidence Label Validation

Every prediction must have a confidence label from the intelligence/trust_scorer.py standard set:
- `confirmed` — multiple independent sources, direct observation
- `derived` — calculated from confirmed sources
- `estimated` — informed model output
- `rumored` — single source, not independently verified
- `inferred` — logical deduction from secondary signals

**Confidence validation checklist:**
- [ ] Label is one of: confirmed/derived/estimated/rumored/inferred
- [ ] Label is consistent with source count (confirmed needs ≥2 sources)
- [ ] Label acknowledges source type (rumored should cite source)

### 7. Probability and Uncertainty Validation

Predictions may include explicit probability. If present, validate:
- **Value must be in [0, 1]** — not NaN, infinity, or >1
- **Avoid false precision** — use round numbers (0.6, 0.75) not (0.6234)
- **Source confidence drives probability** — rumored source ≤ 0.6, confirmed source ≥ 0.7

**Probability validation checklist:**
- [ ] Value is finite number in [0, 1]
- [ ] Value is not NaN or infinity
- [ ] Value matches confidence label (confirmed → 0.7+, rumored → 0.6 or less)
- [ ] Probability reflects base rate for similar predictions

## Source Type Evaluation Windows

When citing intelligence sources for levers, verify freshness per `intelligence/trust_scorer.py`:

| Signal Type | Eval Window | Module | Confidence Impact |
|---|---|---|---|
| congressional | 30 days | `ingestion/altdata/congressional.py` | confirmed (direct disclosure) |
| insider | 14 days | `ingestion/altdata/insider_filings.py` | confirmed (Form 4 filed) |
| darkpool | 5 days | `ingestion/altdata/dark_pool.py` | estimated (aggregate data) |
| social | 5 days | `ingestion/altdata/smart_money.py` | rumored (unverified) |
| scanner | 7 days | `discovery/options_scanner.py` | derived (model output) |
| foreign_lobbying | 45 days | `ingestion/altdata/fara.py` | confirmed (DOJ registered) |
| geopolitical | 7 days | `ingestion/altdata/gdelt.py` | estimated (event signals) |
| diplomatic_cable | 30 days | `ingestion/altdata/foia_cables.py` | confirmed (declassified) |
| lobbying | 30 days | `ingestion/altdata/lobbying.py` | confirmed (disclosure) |
| campaign_finance | 60 days | `ingestion/altdata/campaign_finance.py` | derived (PAC data) |
| offshore_leak | 14 days | `ingestion/altdata/icij_papers.py` | confirmed (ICIJ verified) |

**Source window validation:**
- [ ] Source cited is within its eval window
- [ ] Source type matches the actor/action claimed (congressional = insider, not just sentiment)
- [ ] If multiple sources cite same lever, confidence can upgrade from rumored → estimated

## Validation Workflow

### Step 1: Parse Prediction
Extract the four required sections (LEVER, CONDITION, THESIS, INVALIDATION).
```python
required_sections = ["LEVER", "CONDITION", "THESIS", "INVALIDATION"]
# If any missing, FAIL immediately
```

### Step 2: Validate Lever Naming
Check that lever names:
- A specific person/entity (not "the market")
- A specific action with quantity
- A specific valve name

**FAIL if:** Lever uses vague words like "momentum", "pressure", "risk", "sentiment"

### Step 3: Validate Condition
Confirm condition is environmental, not behavioral.

**FAIL if:** Condition is sentiment, macro mood, or behavioral

### Step 4: Check Lever ≠ Condition
Confirm lever and condition are different.

**FAIL if:** "Fed raised rates (lever) in a rising rate environment (condition)" — these are the same thing

### Step 5: Validate Thesis
Check direction, magnitude, timeframe are all specified.

**FAIL if:** Any of the three are missing or vague

### Step 6: Validate Invalidation
Confirm invalidation is observable and mutually exclusive from thesis.

**FAIL if:** Invalidation is vague or is just restating the thesis in negative form

### Step 7: Check Confidence and Probability
Verify label matches [confirmed/derived/estimated/rumored/inferred] and probability (if present) is in [0, 1].

**FAIL if:** Confidence label is undefined or probability is NaN/infinity

### Step 8: Freshness Check (if using intelligence sources)
If lever cites congressional, insider, darkpool, or other alt-data sources, verify:
- Source is within its eval window (see table above)
- Confidence label is consistent with source age

**WARN if:** Source is at edge of eval window (e.g., congressional data 28 days old — only 2 days left)

## Examples: Validation Passing and Failing

### Example 1: PASS
```
LEVER:     Fed raised the discount rate 50bp on 2026-03-30 (confirmed source: Federal Reserve announcement)
           → affects credit valve (margin cost for financial institutions rises)

CONDITION: Financial conditions index at +1.5 standard deviations (low stress), enabling leverage unwind
           → market is positioned long, this valve closure amplifies selloff

THESIS:    Expect 3-5% drawdown in financial sector (XLF) within 72h
           → lever (cost of borrowing up) in condition (leverage present) = forced selling

INVALIDATION: Invalidated if Fed signals rate cut within 24h OR if XLF closes above $50

CONFIDENCE: confirmed (Fed press release is primary source)
PROBABILITY: 0.75
```
VALIDATION: PASS — lever is specific actor + action + valve, condition is measurable and different from lever, thesis has direction/magnitude/timeframe, invalidation is observable.

### Example 2: FAIL (No Lever)
```
LEVER:     Bearish sentiment in credit markets
CONDITION: Credit spreads widening
THESIS:    Expect equity selloff
INVALIDATION: Invalidated if spreads tighten
```
VALIDATION: FAIL — "bearish sentiment" is not a lever, it's a symptom. No actor, no action, no valve. Rewrite as: "BlackRock upgraded risk parity positioning (lever) amid spread widening (condition) → expect 2-3% rotation into fixed income."

### Example 3: FAIL (Condition Only)
```
LEVER:     Options expiry is Friday
CONDITION: High gamma positioning expected
THESIS:    Expect volatility spike 2-3%
INVALIDATION: Invalidated if markets close flat
```
VALIDATION: FAIL — lever is "options expiry", which is a calendar condition, not an actor action. Who is driving gamma? What's the directional bias? Rewrite as: "Large call holders need delta hedging (lever: forced selling if spot drops) + high gamma (condition: small move cascades) → expect 2-3% whipsaw down then up within 4h if spot breaks $X."

### Example 4: FAIL (Probability Out of Bounds)
```
LEVER:     Tether minted $500M USDT on 2026-03-29
CONDITION: Bitcoin at support level with low volume
THESIS:    Expect 2-3% rally in BTC within 6h
INVALIDATION: Invalidated if new mint is not confirmed within 24h
CONFIDENCE: estimated
PROBABILITY: 2.5
```
VALIDATION: FAIL — probability is 2.5, which is invalid (must be in [0, 1]). If meant 0.25, validation would PASS.

### Example 5: FAIL (Vague Invalidation)
```
LEVER:     SEC approved spot ETH ETF on 2026-03-25
CONDITION: Institutional week-long settlement window
THESIS:    Expect 5-8% move in ETH upward within 7 days
INVALIDATION: Invalidated if sentiment turns negative
CONFIDENCE: confirmed
PROBABILITY: 0.8
```
VALIDATION: FAIL — invalidation "sentiment turns negative" is not observable. Rewrite as: "Invalidated if ETH closes below $3,000 OR if institutional flows reverse (negative weekly ETF flows) by 2026-03-31."

## Integration with GRID Modules

### Journal Logging
Before logging any prediction to `journal/log.py`:
```python
from validation.prediction_validator import validate_prediction

prediction_dict = {
    "lever": "...",
    "condition": "...",
    "thesis": "...",
    "invalidation": "...",
    "confidence": "confirmed",  # or derived/estimated/rumored/inferred
    "probability": 0.75  # optional; must be in [0, 1] if present
}

is_valid, errors = validate_prediction(prediction_dict)
if not is_valid:
    log.error(f"Prediction validation failed: {errors}")
    return  # Do not log to journal
```

### Post-Mortems
When analyzing a failed prediction in `intelligence/postmortem.py`, re-validate the original prediction:
- Was the lever correctly identified?
- Did the lever actually occur, or was the source wrong?
- Was the condition present at the time of prediction?
- Did the thesis magnitude and timeframe match reality?
- What should have invalidated the thesis?

### Options Recommender
Before generating trade recommendations from `trading/options_recommender.py`, validate that each recommendation has a clear lever and measurable timeframe.

## Common Failure Modes

| Error | How to Spot | How to Fix |
|---|---|---|
| Condition presented as cause | Lever uses "low volume", "pinning", "funding rate" | Name the actor pulling the valve; these are amplifiers, not causes |
| Vague actor | "Whales", "smart money", "the market", "institutions" | Cite specific entity — institution name, Form 13F filer, whale wallet address |
| Missing timeframe | "BTC will go up", "rates will fall", "spreads will widen" | Add specific duration: "within 24h", "by Q2 earnings", "before NFP" |
| Circular invalidation | "Invalidated if thesis is wrong" | Define the condition that would prove thesis wrong; must be observable |
| Over-confidence | Rumored source + confidence "confirmed" | Match confidence to source type; rumored sources → estimated or rumored confidence only |
| NaN/infinity in probability | Probability = None, NaN, float('inf') | Reject and require finite number in [0, 1] or omit probability entirely |

## Automation Integration

This skill can be called by:
- `inference/live.py` — before committing inference results
- `trading/options_recommender.py` — before suggesting trades
- `journal/log.py` — before persisting decision journal entries
- Post-mortem analysis — to identify prediction structure failures

See `validation/prediction_validator.py` for the validation function signature.
