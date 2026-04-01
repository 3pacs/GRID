# Run Ensemble

Train the LightGBM ensemble on all QuantaAlpha factor signals and report results.

## Instructions

1. **Run the ensemble script:**
   ```bash
   cd /data/grid_v4/grid_repo && python3 scripts/run_ensemble.py
   ```

2. **Interpret results:**
   - Val Sharpe > 0.5 = marginal signal (worth monitoring)
   - Val Sharpe > 1.0 = tradeable signal
   - Val Sharpe > 1.4 = strong (QuantaAlpha v2 threshold for OOS profitability)
   - Train >> Val = overfit (need more tickers or regularization)
   - Early stopping at round 1 = model can't learn (features are noise)

3. **Feature importance tells you:**
   - Which factors carry real signal vs noise
   - 0% importance = factor is dead (likely constant output — check volume data)
   - If one factor dominates >80%, the ensemble adds little over the single factor

4. **Current state (2026-03-31):**
   - 113 tickers, val Sharpe 0.37
   - dual_horizon_equity 72%, vol_regime_equity 28%
   - trend_volume_gate and vol_price_divergence dead (no volume data)
   - Needs volume data + more factors to push above 1.0

5. **To improve:**
   - Add volume data to resolved_series (needs `_avg_volume` features)
   - Expand ticker universe via `/tiingo-backfill`
   - Add macro factors (credit spreads, yield curve, VIX) as additional signals
   - Try different forward return horizons (currently 5-day)

## Source files
- `scripts/run_ensemble.py`
- `alpha_research/ensemble.py`
- `alpha_research/signals/quanta_alpha.py`
- `alpha_research/data/panel_builder.py`
