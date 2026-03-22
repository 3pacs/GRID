# Risk Management Framework

## Position Sizing Principles

GRID's regime detection informs position sizing, not just direction:

| Regime | Risk Budget | Position Size | Hedge Ratio |
|--------|-------------|---------------|-------------|
| Risk-On / Expansion | Full | 100% of target | Minimal |
| Transition / Inflection | Reduced | 50-75% of target | Moderate |
| Risk-Off / Contraction | Minimal | 25-50% of target | Maximum |
| Crisis / Dislocation | Capital preservation | <25% or flat | Full hedge |
| Recovery / Early Cycle | Aggressive | 100-125% of target | Reducing |

## Signal Confidence Framework

How confident should you be in a regime classification?

### HIGH Confidence (>80%)
- 4+ feature families confirming the same regime
- No contradictions between major signal groups
- Regime has persisted for >2 weeks
- Historical analogy is clear
- **Action**: Trade at full size with conviction

### MEDIUM Confidence (50-80%)
- 2-3 feature families confirming
- 1-2 minor contradictions that can be explained
- Regime change may be in progress
- **Action**: Trade at reduced size, tighter stops

### LOW Confidence (<50%)
- Signals are mixed across families
- Multiple unresolved contradictions
- Possible regime transition underway
- **Action**: Reduce exposure, wait for clarity

## Key Risk Metrics to Monitor

### Tail Risk Indicators
- VIX term structure inversion (spot > 3M)
- HY-IG spread divergence
- Dollar surge velocity (>2% in a week)
- Cross-asset correlation spike
- OFR stress index components all rising

### Liquidity Risk Indicators
- Bid-ask spread widening in credit markets
- Treasury market depth declining
- Repo rate spikes
- Money market fund flows
- Fed reverse repo facility usage

### Concentration Risk Indicators
- Narrow breadth (few stocks driving index)
- Sector ETF correlation structure
- Factor crowding measures
- Geographic concentration in EM flows

## Scenario Analysis Template

For each market briefing, consider three scenarios:

### Base Case (50-60% probability)
- What the current regime classification implies
- Expected trajectory for the next 1-3 months
- Key features to monitor for confirmation

### Bull Case (20-25% probability)
- What would need to improve
- Which signals would confirm the upgrade
- Positioning implications

### Bear Case (20-25% probability)
- What could go wrong
- Which signals would confirm the downgrade
- Risk management actions

### Tail Risk (<5% probability)
- Black swan scenarios
- What would break the model's assumptions
- Circuit breakers and stop-loss levels

## Decision Journal Protocol

Every GRID recommendation includes:
- **Inferred State**: Current regime classification
- **State Confidence**: Numerical confidence (0-1)
- **Transition Probability**: Likelihood of regime change
- **Contradiction Flags**: Which signals disagree
- **GRID Recommendation**: What the system suggests
- **Baseline Recommendation**: What a simple model would suggest
- **Counterfactual**: What happens if we're wrong

All of this is logged immutably. Outcomes are recorded later for honest self-assessment.
