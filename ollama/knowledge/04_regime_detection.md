# GRID Regime Detection

## What Is a Market Regime?

A regime is a persistent market state characterized by distinct statistical properties. Regimes are not defined by price levels but by the *relationships between features*. The same VIX level can mean different things depending on what credit spreads, the yield curve, and breadth are doing.

## Discovery Process

GRID uses unsupervised learning to discover regimes:

1. **Feature Selection**: Choose orthogonal features (low correlation to each other) that span different economic dimensions
2. **Dimensionality Reduction**: PCA to find the true number of independent dimensions
3. **Clustering**: KMeans, GMM (Gaussian Mixture Models), and Agglomerative clustering
4. **Validation**: Silhouette scores, persistence scores, and out-of-sample stability

## Key Metrics

- **Persistence Score**: Average regime duration / total periods. Higher = more stable regimes. Random assignment gives ~0.25
- **Silhouette Score**: Cluster separation quality. >0.3 is decent, >0.5 is strong
- **Transition Recall**: How well do detected transitions correspond to known market events?
- **Out-of-Sample Stability**: Do regimes discovered in-sample generalize to unseen periods?

## Common Regime Archetypes

Based on historical analysis, markets tend to cluster into these states:

### Risk-On / Expansion
- Yield curve: normal (positive slope)
- Credit spreads: tight and narrowing
- VIX: low (<18), contango term structure
- Breadth: expanding (>60% above 200MA)
- Cu/Au ratio: rising
- PMI: >50 and rising
- **Implication**: Stay long risk assets, use pullbacks to add

### Risk-Off / Contraction
- Yield curve: flat or inverted
- Credit spreads: wide and widening
- VIX: elevated (>25), backwardated
- Breadth: contracting (<40% above 200MA)
- Cu/Au ratio: falling
- PMI: <50 and falling
- **Implication**: Defensive positioning, raise cash, consider hedges

### Transition / Inflection
- Mixed signals across families
- Correlation structure breaking down
- VIX elevated but contango maintained
- Credit and equity diverging
- **Implication**: Reduce position sizes, wait for clarity

### Crisis / Dislocation
- Everything correlated (correlation = 1 in panic)
- VIX >35, deeply backwardated
- Credit spreads blowing out
- Dollar surging (funding squeeze)
- Breadth collapsing
- **Implication**: Capital preservation, cash is king, prepare shopping list

### Recovery / Early Cycle
- Yield curve steepening from inversion
- Credit stabilizing then tightening
- VIX declining from elevated levels
- Breadth improving from lows
- PMI bottoming and turning up
- **Implication**: Most aggressive risk-on opportunity; early movers rewarded

## What Makes a Good Regime Signal

1. **Economic mechanism**: There must be a plausible causal story
2. **Persistence**: Regimes should last weeks/months, not days
3. **Distinctiveness**: Different regimes should have meaningfully different return distributions
4. **Tradability**: Transitions should be detectable in time to act
5. **Robustness**: Should work across different time periods, not just one era

## Contradictions to Flag

- VIX low but credit spreads widening → credit market sees risk equity doesn't
- PMI rising but LEI slope negative → manufacturing vs leading indicators disagree
- Strong price momentum but declining breadth → narrow rally, vulnerable
- Yield curve normalizing from inversion during rate cuts → recession arriving, not ending
- Dollar strengthening while commodities rally → one of them is wrong
