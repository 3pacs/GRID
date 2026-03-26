# Implementation Research — 2026-03-25

## Timeframe Comparison SQL Pattern
SELECT fr.name,
  (SELECT value FROM resolved_series WHERE feature_id = fr.id ORDER BY obs_date DESC LIMIT 1) as current,
  (SELECT value FROM resolved_series WHERE feature_id = fr.id AND obs_date <= CURRENT_DATE - N ORDER BY obs_date DESC LIMIT 1) as period_start
FROM feature_registry fr WHERE fr.name = :feature

## Watchlist Enrichment Joins
- feature_registry (name, family) for sector context
- sector_map.py SECTOR_MAP for influence weight
- options_daily_signals for PCR/IV
- resolved_series for z-scores via PITStore

## Orthogonal Feature Selection
1. Compute correlation matrix for all features with z-scores
2. Sort features by |z-score| descending
3. Greedily select: take top feature, remove all features with |corr| > 0.7
4. Repeat until K features selected or all processed
5. Result: maximum information density per token for LLM prompts
