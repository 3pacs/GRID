---
name: verify-physics
group: physics
schedule: "daily 22:30 weekdays"
secrets: []
depends_on: ["compute-features", "check-regime"]
description: Run market physics verification checks (conservation, limiting cases, stationarity)
---

## Steps

1. Initialize MarketPhysicsVerifier
2. Run full verification suite:
   a. Conservation — capital flow balance check
   b. Limiting cases — extreme value behavior
   c. Dimensional consistency — unit validation across features
   d. Regime boundaries — plausible transition paths
   e. Stationarity — ADF tests on clustering inputs
   f. Numerical stability — NaN/inf/outlier scan
3. Log results and flag warnings

## Output

- Per-check results: {passed, score, details, warnings}
- Summary: total passed/failed, average score
- Warnings logged for operator review

## Notes

- Adapted from Get Physics Done's verify-work framework
- Convention locking ensures consistent units across all checks
- Non-stationary features in clustering → automatic warning
