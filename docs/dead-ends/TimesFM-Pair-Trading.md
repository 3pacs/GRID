---
source: /Users/anikdang/grid_obsidian/Dead-Ends/TimesFM-Pair-Trading.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
---
tags: [dead-end, timesfm, ml]
---
# Dead End: TimesFM for Pair Trading

**Date:** 2026-04-03
**Model:** TimesFM 2.5 (200M params)

## Results
- 16K runs on PYPL→XLK: **49.9%** directional accuracy (random)
- 16K runs across 71 passed hypotheses: **49.9%** (random)
- Context 128/256/512: no improvement (49.4%, 50.1%, 50.3%)
- SPY single-asset: 56-58% (marginally above coin flip)

## Don't Retry
TimesFM is useless for cross-asset directional prediction. Use analog engine as primary, TimesFM as comparison signal only.

## Still Useful For
- Single-asset [[Walk-Forward Backtesting|walk-forward]] as a weak signal (~56%)
- Batch forecasting on gridz4 (CPU, 0.42s/window)
- Comparison/sanity check against other models

## Related
- [[Decisions/LLM-Model-Selection]]
