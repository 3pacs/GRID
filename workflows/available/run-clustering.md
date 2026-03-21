---
name: run-clustering
group: discovery
schedule: "weekly sunday 06:00"
secrets: []
depends_on: ["compute-features"]
description: Run unsupervised regime discovery using PCA + GMM/KMeans/Agglomerative clustering
---

## Steps

1. Initialize ClusterDiscovery with PITStore
2. Load model-eligible feature matrix
3. PCA reduction to 5 components
4. Test k ∈ {2, 3, 4, 5, 6} across three algorithms
5. Select best k by silhouette score
6. Generate cluster assignments with confidence scores
7. Compute transition matrix and persistence metrics
8. Save outputs to `outputs/clustering/`

## Output

- `cluster_metrics_{timestamp}.csv` — per-k evaluation
- `cluster_assignments_{timestamp}.csv` — date, cluster_id, confidence
- `transition_matrix_{timestamp}.csv` — k×k transition probabilities
- `cluster_summary_{timestamp}.png` — metric comparison plots

## Notes

- Weekly cadence balances freshness with computational cost
- Transition entropy measures disorder in state dynamics
- Persistence = average consecutive days in same regime
