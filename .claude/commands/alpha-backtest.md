# Alpha Backtest

Run all alpha research signals against real GRID data and report metrics.

## Instructions

1. **Run the backtest script:**
   ```bash
   cd /data/grid_v4/grid_repo && python3 scripts/run_alpha_backtest.py
   ```

2. **Interpret results:**
   - Focus on TEST metrics (not TRAIN)
   - **Net Sharpe > 1.4** = passes QuantaAlpha threshold for OOS confidence
   - **64% RankIC shrinkage** from train to test is normal — budget for it
   - Negative Sharpe on momentum signals in equity universe is expected (designed for commodities)

3. **Run the gauntlet on any signal with positive test Sharpe:**
   - Instantiate gauntlet from `alpha_research/validation/gauntlet.py`
   - Verdict must be MARGINAL or ROBUST before wiring into Oracle
   - UNSTABLE signals: investigate but don't promote

4. **Report format:**
   - Signal name, train/test split metrics
   - Gauntlet verdict if run
   - Recommendation: wire / investigate / reject

## Key Thresholds
- Val net Sharpe > 1.4 → expect positive OOS
- Permutation p < 0.05 → signal is real (not noise)
- Subsample stability > 50% → signal is broad-based
- CV consistency > 75% → ROBUST (hardest bar)
