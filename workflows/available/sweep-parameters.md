---
name: sweep-parameters
group: physics
schedule: "manual"
secrets: []
depends_on: ["run-clustering"]
description: Systematic parameter sweep for clustering and feature engineering hyperparameters
---

## Steps

1. Define sweep grid:
   - PCA components: [3, 5, 7, 10]
   - Clustering k: [2, 3, 4, 5, 6]
   - Feature lookback windows: [126, 252, 504]
   - Normalization: [ZSCORE, RANK, MINMAX]
2. Run each combination using wave-based parallel execution
3. Evaluate: silhouette, persistence, transition entropy, BIC
4. Rank combinations by composite score
5. Save results to `outputs/sweeps/`

## Output

- Parameter sweep results CSV
- Best combination recommendation
- Sensitivity analysis (which params matter most)

## Notes

- MANUAL schedule — computationally expensive
- Use wave-based execution for parallelism within each sweep tier
- Consider running on distributed workers (scripts/worker.py)
