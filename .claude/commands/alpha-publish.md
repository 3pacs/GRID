# Alpha Publish

Run all alpha research signals and publish to GRID's SignalRegistry for Oracle consumption.

## Instructions

1. **Publish signals:**
   ```python
   from sqlalchemy import create_engine
   from config import settings
   from alpha_research.adapters.signal_adapter import publish_all_alpha_signals

   engine = create_engine(settings.DB_URL)
   results = publish_all_alpha_signals(engine)
   print(results)
   ```

2. **Verify in database:**
   ```sql
   SELECT source_module, COUNT(*), MIN(valid_from), MAX(valid_until)
   FROM signal_registry
   WHERE source_module LIKE 'alpha_research:%'
   GROUP BY source_module;
   ```

3. **Current signals published:**
   - `alpha_research:vol_price_divergence` — 81 per-ticker directional signals (contrarian mean-reversion)
   - `alpha_research:vix_exposure` — 1 regime signal (calm/elevated/stressed)
   - `alpha_research:credit_cycle` — 1 regime signal (contraction/expansion)

4. **To consume in Oracle:** Set `GRID_SIGNAL_REGISTRY=1` in `.env`

## Notes
- Signals have 24-hour validity (valid_until = now + 24h)
- Re-running overwrites (upsert on signal_id + valid_from)
- Regime signals are ticker-agnostic (ticker=NULL)
