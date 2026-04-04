# GRID AutoAgent — Meta-Agent Directives

## Mission

Improve `agent.py` so that the agent produces higher-quality EOG trading signals.
The score (0.0–1.0) is a weighted composite of Sharpe ratio, hit rate, drawdown,
information coefficient, and parsimony. **Maximize the composite score.**

## Philosophy: LLM Reasoning > Dumb Quant

The background data (prices, vol, macro) gives context. But the real edge is
the LLM's ability to **reason about what the data means** — interpret conflicting
signals, weigh regime implications, and make judgment calls no sklearn model can.

The strategy is **vol-regime-gated, single-sector (energy)**. The regime filter
is the primary alpha source: avoid CRISIS periods entirely, lean in during RISK_ON.
The LLM's job is to get the regime classification and signal logic right.

## Scoring Weights (for reference — defined in test_state.py)

| Metric              | Weight | What Earns 1.0                         |
|---------------------|--------|----------------------------------------|
| Sharpe ratio        | 40%    | Signal-gated Sharpe ≥ 1.5              |
| Hit rate            | 25%    | ≥ 70% of BUY signals correct           |
| Max drawdown        | 15%    | Worst drawdown ≥ -10%                   |
| Information coeff.  | 10%    | IC ≥ 0.15                               |
| Parsimony           | 10%    | ≤ 5 features used                       |

## GRID Context

The agent operates on real financial data from a PostgreSQL database containing:
- **1,237 features** across 15 families (rates, credit, equity, vol, macro, etc.)
- **EOG Resources** price history (13,672 rows since 1989)
- Features include Treasury curves, VIX, credit spreads, oil prices, and esoteric
  signals (planetary aspects, lunar phases, satellite nightlight data)

The bridge module (`grid_bridge.py`) provides all database access. The agent
should use it exclusively — no raw SQL.

## High-Leverage Improvement Areas

0. **Supply chain news reasoning** — THE BIGGEST LEVER. The LLM reads energy
   news headlines and reasons about supply chain implications. "OPEC cuts
   production" → "tighter supply" → "Permian producers like EOG benefit."
   This is what no sklearn model can do. The bridge provides:
   - `get_energy_news_context()` — OPEC, pipeline, sanctions, shipping, Permian
   - `get_news_headlines()` — general + ticker-specific news with LLM sentiment
   - `get_supply_chain_data()` — freight rates, manufacturing, trade balance
   - `get_gdelt_tone()` — global event sentiment and conflict data
   The meta-agent should ensure the agent uses LLM reasoning on news, not just
   pre-computed sentiment scores.

1. **Feature selection** — the quant context layer. Energy stocks are driven by:
   - Oil prices (cl_close, uso_full)
   - Credit conditions (hyg_full/lqd_full ratio, ofr_financial_stress)
   - Macro regime (vix_spot, tlt_full for rate expectations)
   - Sector momentum (xle_full vs. SPY-like proxies)
   - Parsimony is scored — fewer features that matter > many that noise

2. **Feature engineering** — transform raw features into predictive signals:
   - Rolling z-scores (mean reversion detection)
   - Rate-of-change / momentum indicators
   - Cross-asset ratios and spreads
   - Regime classification (risk-on vs. risk-off)

3. **Model selection** — gradient boosting (LightGBM/XGBoost) typically dominates
   for tabular financial data. But logistic regression with good features can win
   on parsimony.

4. **Walk-forward discipline** — the agent MUST use expanding-window walk-forward.
   Any look-ahead bias will be caught by the test suite's date validation.

## Experiment Protocol

1. Run baseline first — record score
2. Diagnose which component is weakest (Sharpe? Hit rate? IC?)
3. Make ONE focused change per iteration targeting the weakest component
4. Keep changes that improve composite score; revert those that don't
5. Log all runs to `results.tsv`
6. Iterate continuously

## Anti-Patterns

- **Overfitting**: Using too many features, too-short training windows
- **Curve fitting**: Optimizing for in-sample metrics instead of walk-forward
- **Look-ahead bias**: Using any future information at prediction time
- **Complexity without payoff**: Adding ensemble methods that don't beat simple models
- **Task-specific hacks**: Hardcoding signals for specific date ranges

## Constraints

- Model: Use whatever model the current agent.py specifies
- Only edit code above the "FIXED ADAPTER BOUNDARY" in agent.py
- The agent writes a Python script — it does NOT directly execute trades
- Database is read-only. The bridge module handles all DB interaction.
