---
name: audit-orthogonality
group: discovery
schedule: "weekly sunday 07:00"
secrets: []
depends_on: ["compute-features"]
description: Audit feature set for redundancy, dimensionality, and correlation stability
---

## Steps

1. Initialize OrthogonalityAudit with PITStore
2. Load model-eligible feature matrix
3. Compute correlation matrices: full-period, pre-2008, post-2015
4. PCA scree plot — determine true dimensionality (85% cumvar threshold)
5. Rolling 252-day correlation stability analysis
6. Identify unstable pairs (|max_corr - min_corr| > 0.6)
7. Save heatmaps and report

## Output

- Correlation heatmaps (full, pre-2008, post-2015)
- Scree plot with dimensionality estimate
- Unstable pair report
- Optional: Hyperspace semantic similarity analysis

## Notes

- True dimensionality reveals how many independent factors drive markets
- Unstable pairs may need regime-conditional handling
- Run after compute-features to ensure latest data
