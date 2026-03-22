---
name: validate-backtest
group: validation
schedule: "weekly sunday 08:00"
secrets: []
depends_on: ["run-clustering"]
description: Run walk-forward backtesting on candidate and shadow models
---

## Steps

1. Query `model_registry` for models in CANDIDATE or SHADOW state
2. For each model:
   a. Load feature set and parameter snapshot
   b. Run walk-forward backtest with PIT-correct data
   c. Compute era-level and full-period metrics
   d. Compare against baseline (buy-and-hold, random)
3. Store results in `validation_results`
4. Update model state if gate criteria are met

## Output

- `validation_results` rows per (model, vintage_policy)
- Walk-forward split results
- Baseline comparison metrics

## Notes

- Uses FIRST_RELEASE vintage policy for realistic backtesting
- Gate checks enforced before any state transition
- Failed models get KILLED in hypothesis_registry
