# Self-Learning Prompt Pruning — Implementation Plan

## Overview

Closed-loop system that tracks LLM feature citations, scores utility, and prunes low-value signals.

## 5 Phases

### Phase 1: Schema + Citation Extraction
- `migrations/add_prompt_pruning.sql` — `prompt_feedback` + `feature_utility` tables
- `oracle/pruning_config.py` — anchors, thresholds, constants
- `oracle/citation_extractor.py` — parse LLM output for feature refs (exact + fuzzy + alias)

### Phase 2: Instrument Chat + Briefing
- `oracle/feedback_recorder.py` — shared recording logic
- Modify `api/routers/chat.py` — track features in `_build_context_block`, extract citations after response
- Modify `ollama/market_briefing.py` — same instrumentation

### Phase 3: Utility Scorer
- `oracle/utility_scorer.py` — daily recompute:
  - citation_rate (30%), hit_correlation (40%), information_gain (15%), recency (15%)
- `scripts/compute_feature_utility.py` — daily cron (9:00 AM, after oracle scoring at 8:30)

### Phase 4: Prompt Optimizer
- `oracle/prompt_pruner.py` — select top-N features by utility, always include anchors
- Integrate into chat context builder + market briefing
- Enhance `analysis/prompt_optimizer.py` with utility × z-score weighting
- A/B test: 80% pruned, 20% full (control)

### Phase 5: Continuous Learning
- Incremental updates after each scoring cycle
- Regime shift detection (pruned feature z > 2.5 → re-include)
- Time decay (90-day half-life)
- `/api/v1/system/pruning-stats` endpoint for A/B comparison

## Anchor Features (never pruned)
vix_spot, vix_3m_ratio, spy, spy_full, qqq, qqq_full, yld_curve_2s10s, fed_funds_rate, dxy_index, hy_spread_proxy

## Crisis Protection
- systemic + credit families always protected
- z-score > 2.5 on pruned feature → auto re-include
- Regime-aware: volatile market → relax pruning (max 80 features vs 40 in calm)

## Cold Start
- < 50 prompt_feedback rows → no pruning, track citations only
- Existing orthogonality optimizer provides baseline selection
- ~2-3 weeks to accumulate enough data

## New Files
```
oracle/pruning_config.py
oracle/citation_extractor.py
oracle/feedback_recorder.py
oracle/utility_scorer.py
oracle/prompt_pruner.py
oracle/pruning_alerts.py
scripts/compute_feature_utility.py
migrations/add_prompt_pruning.sql
```

## Modified Files
```
api/routers/chat.py
ollama/market_briefing.py
analysis/prompt_optimizer.py
scripts/score_oracle_trades.py
oracle/run_cycle.py
api/routers/system.py
```

## Success Criteria
- Pruned prompts 30-60% fewer features
- A/B: pruned >= full hit rate
- Chat latency < 200ms regression
- Regime shift alerts fire when needed
- 80%+ test coverage on new code

See full plan: ~/.claude/skills/self-learning-prompt-pruning/SKILL.md
