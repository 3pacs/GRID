---
name: check-regime
group: discovery
schedule: "daily 22:00 weekdays"
secrets: []
depends_on: ["compute-features"]
description: Run auto-regime detection and update decision journal with current market regime
---

## Steps

1. Run GMM clustering with n_components=4 on latest feature vector
2. Map cluster IDs to regime labels (GROWTH / NEUTRAL / FRAGILE / CRISIS)
3. Compute assignment confidence and transition probability
4. Flag contradictions (e.g., low-confidence GROWTH with high stress indicators)
5. Update `decision_journal` with regime assessment

## Output

- Current regime label with confidence score
- Transition probability from current regime
- Contradiction flags if any

## Notes

- Runs daily for tactical awareness
- Full clustering (run-clustering) runs weekly for model refit
- Uses scripts/auto_regime.py logic
