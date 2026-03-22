---
name: compute-features
group: features
schedule: "daily 21:30 weekdays"
secrets: []
depends_on: ["resolve-conflicts"]
description: Compute derived features from resolved series using FeatureLab
---

## Steps

1. Initialize FeatureLab with PITStore
2. Call `compute_derived_features(as_of_date=today)` for standard features:
   - fed_funds_3m_chg, hy_spread_3m_chg
   - copper_gold_ratio, copper_gold_slope
   - sp500_mom_12_1, sp500_mom_3m
   - real_ffr, vix_3m_ratio
3. Compute physics-derived features:
   - sp500_kinetic_energy, sp500_potential_energy
   - market_temperature, regime_entropy
4. Store results in `resolved_series`

## Output

- All derived feature values updated for current date
- Physics energy/temperature metrics refreshed

## Notes

- Depends on resolve-conflicts completing first
- Uses 504-day lookback for feature computation
- Z-score normalization uses 252-day window
