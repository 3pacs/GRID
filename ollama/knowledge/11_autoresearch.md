# Autoresearch — Autonomous Hypothesis Discovery

## Overview

GRID's autoresearch engine is a closed-loop system that uses Ollama to
autonomously generate, test, and refine market hypotheses. It runs walk-forward
backtests against GRID's PIT-correct data and iteratively improves hypotheses
based on quantitative results and LLM-generated critiques.

## Loop Architecture

```
┌─────────────────────────────────────────────────────┐
│  1. GENERATE                                         │
│     Ollama produces a hypothesis from:              │
│     - Available features (feature_registry)         │
│     - Current market snapshot (resolved_series)     │
│     - Prior failed attempts + critiques             │
│     Output: JSON with statement, feature_ids, lags  │
├─────────────────────────────────────────────────────┤
│  2. REGISTER                                         │
│     Insert into hypothesis_registry (state=TESTING) │
├─────────────────────────────────────────────────────┤
│  3. BACKTEST                                         │
│     WalkForwardBacktest.run_validation()             │
│     - PIT-correct data via PITStore                 │
│     - Era-based evaluation (default 5 splits)       │
│     - Cost-adjusted returns (default 10 bps)        │
│     - Baseline comparison (buy-and-hold)            │
├─────────────────────────────────────────────────────┤
│  4. EVALUATE                                         │
│     Verdict: PASS / FAIL / CONDITIONAL              │
│     - Must beat baseline Sharpe                     │
│     - Must have positive returns in >60% of eras   │
│     - PASS requires positive in ALL eras            │
├─────────────────────────────────────────────────────┤
│  5. CRITIQUE (on FAIL)                               │
│     OllamaReasoner.critique_backtest_result()       │
│     Identifies: overfitting, regime dependence,     │
│     survivorship bias, mechanism plausibility       │
├─────────────────────────────────────────────────────┤
│  6. REFINE                                           │
│     Ollama generates improved hypothesis using:     │
│     - Failed result + critique                      │
│     - Full history of all prior attempts            │
│     - Current market context                        │
│     Loop back to step 2                             │
└─────────────────────────────────────────────────────┘
```

## Key Design Principles

1. **PIT correctness** — All backtests use point-in-time data. No lookahead.
2. **Economic mechanism** — Hypotheses must have causal reasoning, not just
   pattern-matching. The LLM is prompted to distinguish correlation from
   causation.
3. **Overfitting resistance** — Walk-forward splits, era consistency checks,
   and baseline comparisons all guard against curve-fitting.
4. **Cumulative learning** — Each iteration sees the full history of prior
   failures and their critiques, preventing the LLM from repeating mistakes.
5. **Feature grounding** — The LLM can only reference features that actually
   exist in feature_registry with model_eligible=TRUE.

## Usage

```bash
# Default: 5 iterations, REGIME layer
python scripts/autoresearch.py

# More iterations with a seed idea
python scripts/autoresearch.py --max-iter 10 --seed "VIX term structure inversion predicts regime shifts"

# Different layer and date range
python scripts/autoresearch.py --layer TACTICAL --start 2024-01-01 --end 2025-12-31
```

## Hypothesis JSON Format

```json
{
  "statement": "When 2s10s yield curve inverts while VIX term structure is in backwardation, a CRISIS regime follows within 60 days",
  "feature_ids": [1, 11, 12],
  "lag_structure": {"1": 0, "11": 0, "12": 60},
  "layer": "REGIME",
  "proposed_metric": "sharpe",
  "proposed_threshold": 0.5
}
```

## Integration with GRID Lifecycle

Hypotheses that PASS autoresearch can proceed through the standard GRID
governance pipeline:

1. `hypothesis_registry.state` set to PASSED
2. Model created in `model_registry` (state=CANDIDATE)
3. Gate checks via `GateChecker`
4. Promotion through SHADOW → STAGING → PRODUCTION
