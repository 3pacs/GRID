# Alpha Heartbeat

Run the autonomous monitoring heartbeat and report any alerts.

## Instructions

1. **Run heartbeat:**
   ```python
   from sqlalchemy import create_engine
   from config import settings
   from alpha_research.heartbeat import run_heartbeat, format_alerts

   engine = create_engine(settings.DB_URL)
   alerts = run_heartbeat(engine)
   print(format_alerts(alerts))
   ```

2. **Checks performed:**
   - **Regime transition**: VIX exposure scalar state change → CRITICAL if stressed
   - **VIX MA cross**: VIX crossing above 20-day MA → INFO
   - **Puller health**: >3 failures in 6 hours for any source → WARNING
   - **PIT freshness**: resolved_series data stale >24h → WARNING

3. **Alert levels:**
   - CRITICAL: Immediate attention (regime stressed, fast risk-off triggered)
   - WARNING: Investigate soon (puller failures, stale data)
   - INFO: FYI (VIX near MA crossover)

4. **Integration:**
   - Add to Hermes as 30-minute recurring job
   - Pipe alerts through existing `alerts/` email infrastructure
   - Log to `server_log/` for operator review
