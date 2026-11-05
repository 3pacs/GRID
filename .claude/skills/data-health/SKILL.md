---
name: data-health
description: Monitor GRID data-source freshness thresholds, API-key availability, and NaN quality across sources; for scheduler cycle structure, time gates, or why a puller step did not run use hermes-operations.
---

# data-health

Monitors GRID data source freshness, detects ingestion anomalies, and validates API key availability. Provides health dashboards for all 37+ data sources and flags stale data before it affects inference or trading decisions.

## When to Use This Skill

- On startup or after scheduled restarts to verify all sources are pulling
- Before running inference — check that critical data is fresh
- When investigating unexpected model behavior — data staleness can cause cascading failures
- As part of daily monitoring alerts (`alerts/` system)
- During debugging sessions when source data quality is suspect
- When adding new ingestion sources or modifying scheduling

## Data Sources (37+ Total)

GRID ingests from multiple categories:

### Macroeconomic (US Federal Reserve, Bureau of Labor Statistics, etc.)
- `ingestion/fred.py` — Federal Reserve Economic Data (FRED API; pulls macro series including retail sales/Census via RETAILSMNSA, initial claims/DOL via ICSA, and PCE inflation via PCEPI/PCEPILFE)
- `ingestion/bls.py` — Bureau of Labor Statistics (CPI, unemployment, payroll)
- `ingestion/altdata/wage_tracker.py` — Atlanta Fed Wage Growth Tracker
- `ingestion/altdata/h8_bank_balance.py` — Fed H.8 commercial bank assets and liabilities

### International (`ingestion/international/`, `ingestion/trade/`)
- `ingestion/international/ecb.py` — European Central Bank (rates, FX, economic indicators)
- `ingestion/international/jquants.py` — Japan Exchange Group (Japanese equities and market structure)
- `ingestion/international/edinet.py` — Japanese EDINET corporate disclosures
- `ingestion/international/kosis.py` — Korean Statistics (K-economy data)
- `ingestion/international/bis.py` — Bank for International Settlements (effective exchange rates, credit)
- `ingestion/trade/comtrade.py` — UN Comtrade (bilateral trade flows)

### Commodity and Physical Data
- `ingestion/altdata/eia_puller.py` — EIA energy data (WTI crude oil spot, Brent crude spot, Cushing storage)
- `ingestion/altdata/jodi_oil.py` — JODI world crude oil production, closing stocks, refinery intake
- `ingestion/altdata/refinery_cracks.py` — 3-2-1 refinery crack spread from WTI and Gulf Coast product spot
- `ingestion/altdata/sge_premium.py` — Shanghai Gold Exchange (SGE) au9999 spot minus London PM fixing basis
- `ingestion/altdata/lme_warehouse.py` — London Metal Exchange warehouse inventory (copper, aluminum, zinc, nickel)
- `ingestion/altdata/ag_commodity_futures.py` — Agricultural commodity futures (corn, soybeans, wheat)
- `ingestion/physical/usda_nass.py` — USDA National Agricultural Statistics Service
- `ingestion/altdata/baltic_dry.py` — Baltic Dry Index and container freight rates
- `ingestion/physical/viirs.py` — VIIRS satellite night-lights physical activity proxy

### Financial Market Data
- `ingestion/tiingo_pull.py` — Tiingo equities daily EOD prices (OHLCV)
- `ingestion/yfinance_pull.py` — Yahoo Finance market fallbacks and ETF proxies
- `ingestion/altdata/cboe_indices.py` — CBOE volatility indices (VIX, VVIX, SKEW)
- `ingestion/altdata/yield_curve_full.py` — Full Treasury yield curve and term spread models
- `ingestion/crypto_signals.py` — Crypto market data and technical signals
- `ingestion/coingecko.py` — CoinGecko cryptocurrency prices and market cap
- `ingestion/realtime/feeds/yahoo.py` & `binance.py` — Real-time price websocket listeners

### Alternative Data
- `ingestion/altdata/congressional.py` — Congressional trading disclosures (House.gov, Senate.gov)
- `ingestion/altdata/insider_filings.py` — SEC Form 4 insider trading (SEC EDGAR)
- `ingestion/altdata/dark_pool.py` — FINRA dark pool data (weekly)
- `ingestion/altdata/unusual_whales.py` — Whale options flow, unusual activity (API)
- `ingestion/altdata/prediction_odds.py` — Polymarket prediction contracts (API)
- `ingestion/altdata/smart_money.py` — Reddit posts, Finviz mentions, trust-scored social signals
- `ingestion/altdata/supply_chain.py` — ISM manufacturing, supply chain indices
- `ingestion/altdata/fed_liquidity.py` — Fed balance sheet, net liquidity equation
- `ingestion/altdata/institutional_flows.py` — ETF flows, 13F holdings (SEC)
- `ingestion/altdata/fara.py` — DOJ FARA foreign agent lobbying disclosures
- `ingestion/altdata/foia_cables.py` — State Dept, NSA Archive declassified diplomatic cables
- `ingestion/altdata/gdelt.py` — GDELT geopolitical event signals, tone scoring

## Source Health Monitoring

### Freshness Thresholds by Source Type

| Source Type | Schedule | Stale Alert | Examples |
|---|---|---|---|
| Real-time market data | Continuous/5min | >1 hour | VIX, equity prices, crypto |
| Daily economic data | Daily EOD | >36 hours | FRED data, Treasury yields |
| Weekly releases | Wednesday/Friday | >8 days | Dark pool, jobless claims |
| Monthly releases | Mid-month | >35 days | CPI, employment, ISM |
| Congressional disclosures | Quarterly | >100 days | Form 4 filings, 13F filings |
| Geopolitical signals | Daily | >2 days | GDELT, diplomatic cables |
| High-frequency alt-data | Hourly/4h | >6 hours | Unusual whales, Polymarket |

### How to Check Source Health

#### Option 1: Query Database

```sql
SELECT
    source_name,
    MAX(observation_date) as last_obs_date,
    MAX(release_date) as last_release_date,
    COUNT(*) as record_count,
    (CURRENT_TIMESTAMP - MAX(observation_date)) as staleness_duration
FROM resolved_series
WHERE source_id IN (
    SELECT id FROM source_catalog
    WHERE source_name IN (
        'FRED', 'BLS', 'ECB', 'dark_pool', 'congressional'
    )
)
GROUP BY source_name
ORDER BY staleness_duration DESC;
```

Expected output format:
```
source_name     | last_obs_date | staleness_duration | alert_level
FRED            | 2026-03-28    | 2 days             | OK
BLS             | 2026-03-27    | 3 days             | WARNING
dark_pool       | 2026-03-23    | 7 days             | CRITICAL
congressional   | 2025-12-31    | 90 days            | CRITICAL
crypto          | 2026-03-30    | 0 minutes          | OK
```

#### Option 2: Use Health Check Endpoints

For unauthenticated system and database health:
```bash
curl https://grid.stepdad.finance/api/v1/system/health
```

Should return JSON:
```json
{
  "status": "ok",
  "checks": {
    "database": true,
    "features_registered": true,
    "recent_data": true,
    "pool_healthy": true,
    "api_keys_configured": 31,
    "api_keys_total": 37,
    "disk_percent": 45.2,
    "llm_available": true
  },
  "degraded_reasons": []
}
```

For authenticated comprehensive per-source pipeline health:
```bash
curl -H "Authorization: Bearer <jwt>" https://grid.stepdad.finance/api/v1/system/pipeline-health
# Per-family GREEN/YELLOW/RED plus stale_sources (same auth):
curl -H "Authorization: Bearer <jwt>" https://grid.stepdad.finance/api/v1/system/freshness
```

Should return JSON:
```json
{
  "sources": [
    {
      "name": "FRED",
      "status": "healthy",
      "freshness": "green",
      "last_pull": "2026-03-28T16:00:00Z",
      "rows_last_pull": 150,
      "next_scheduled": "2026-03-29T16:00:00Z"
    },
    {
      "name": "DarkPool",
      "status": "stale",
      "freshness": "yellow",
      "last_pull": "2026-03-23T17:00:00Z",
      "rows_last_pull": 0,
      "next_scheduled": null
    }
  ],
  "summary": {
    "total_sources": 44,
    "healthy": 40,
    "stale": 3,
    "broken": 1
  }
}
```

## API Key Validation

Only `FRED_API_KEY` and `DB_PASSWORD` are validated at startup (field validators in `config.py`). Other sources may fail silently.

### Validated at Startup
- `FRED_API_KEY` — Required for FRED data. Checked by the `_check_fred_key` field validator in `config.py` (empty is allowed only when `ENVIRONMENT=development`); `DB_PASSWORD` gets the same treatment

### NOT Validated (Will Fail Silently)
- `KOSIS_API_KEY` — Korean statistics (if missing, HTTP 401 returns empty results)
- `COMTRADE_API_KEY` — UN trade data (fails at query time)
- `JQUANTS_EMAIL` / `JQUANTS_PASSWORD` — Japanese market data (returns 401)
- `USDA_NASS_API_KEY` — Agriculture data (rate limit errors)
- `NOAA_TOKEN` — Weather/climate data
- `EIA_API_KEY` — Energy Information Administration (oil, gas, renewables)

These are the names `config.py` declares; `GET /api/v1/system/api-keys` (JWT required) reports which are set.

### How to Check API Key Status

```python
from config import settings
from loguru import log

# Startup checks (config.py already raises for FRED_API_KEY outside development)
if not settings.FRED_API_KEY:
    log.critical("FRED_API_KEY is missing — system cannot operate")
    exit(1)

# Silent failures — log warnings
api_keys = {
    "KOSIS": settings.KOSIS_API_KEY,
    "COMTRADE": settings.COMTRADE_API_KEY,
    "JQUANTS": settings.JQUANTS_EMAIL and settings.JQUANTS_PASSWORD,
    "USDA_NASS": settings.USDA_NASS_API_KEY,
    "NOAA": settings.NOAA_TOKEN,
    "EIA": settings.EIA_API_KEY,
}

for key_name, key_value in api_keys.items():
    if not key_value:
        log.warning(f"{key_name}_API_KEY is missing — {key_name} data will not ingest")
```

**Action:** Add validation for all API keys in startup routine, or add monitoring that detects failed API calls and alerts operators.

## NaN and Data Quality Issues

### Silent NaN Conversion

Every ingestion module uses `pd.to_numeric(errors="coerce")` to handle bad data:

```python
df['value'] = pd.to_numeric(df['value'], errors="coerce")
```

This converts unparseable values to NaN **without logging**. Can silently drop entire days of data.

**How to detect:** Count NaN values after ingestion.

```python
def check_nans_in_series(series_name: str, days_back: int = 7):
    """Flag if NaN percentage exceeds threshold."""
    from store.pit import PIT

    pit = PIT(as_of=datetime.now())
    data = pit.fetch(series_name, start_date=datetime.now() - timedelta(days=days_back))

    nan_count = data['value'].isna().sum()
    total_count = len(data)
    nan_pct = (nan_count / total_count) * 100 if total_count > 0 else 0

    if nan_pct > 10:  # More than 10% NaN is suspicious
        log.warning(f"{series_name}: {nan_pct:.1f}% NaN rate (last {days_back}d)")
        return False
    return True
```

**Action:** When modifying ingestion modules, add logging when coercion occurs:

```python
df['value'] = pd.to_numeric(df['value'], errors="coerce")
coerced_count = df['value'].isna().sum()
if coerced_count > 0:
    log.warning(f"Coerced {coerced_count} non-numeric values to NaN for {series_name}")
```

### Inconsistent NaN Handling

Different modules use different strategies for NaN:
- `discovery/orthogonality.py:156` — `ffill(limit=5)`
- `discovery/clustering.py:114` — `ffill().dropna()`
- `features/lab.py` — varies by transformation

**Rule:** When modifying a module, follow that module's existing NaN strategy. Do not introduce a new approach without updating all related modules.

### How to Audit NaN Handling

Check each feature-engineering module:

```bash
grep -n "ffill\|dropna\|fillna" features/*.py
grep -n "ffill\|dropna\|fillna" discovery/*.py
grep -n "ffill\|dropna\|fillna" inference/*.py
```

Expected patterns (document existing strategy):
```
discovery/orthogonality.py:156  → ffill(limit=5)
discovery/clustering.py:114     → ffill().dropna()
features/lab.py:234             → (varies by feature)
inference/live.py:456           → dropna() with logging
```

## Graceful Degradation Pattern

Per CLAUDE.md, when API keys are missing or services are offline, the system should operate with graceful degradation:

```python
def fetch_hyperspace_embedding(market_signal: str) -> Optional[np.ndarray]:
    """Returns None if Hyperspace node is offline."""
    try:
        # Try to connect to local P2P node
        response = hyperspace_client.embed(market_signal)
        return response.embedding
    except ConnectionError:
        log.warning(f"Hyperspace offline; skipping embedding for '{market_signal}'")
        return None  # Continue without embedding
    except Exception as e:
        log.error(f"Hyperspace error: {e}")
        return None  # Fail gracefully
```

Similarly for Ollama:

```python
def get_market_briefing() -> Optional[str]:
    """Tries Ollama, falls back to None if unavailable."""
    try:
        response = ollama_client.generate(prompt)
        return response.text
    except ConnectionError:
        log.warning("Ollama offline; skipping market briefing generation")
        return None
```

**Key principle:** Missing data sources and offline services should WARN, not CRASH. The system degrades gracefully.

## Health Monitoring Dashboard

### Daily Health Report Format

```
=== GRID DATA HEALTH REPORT ===
Generated: 2026-03-30 09:00:00 UTC

CRITICAL (Immediate Action Required)
- dark_pool: last obs 2026-03-23 (7 days stale) ⚠ Check FINRA API
- congressional: last obs 2025-12-31 (90 days stale) ⚠ Check House.gov scraper

WARNING (Monitor)
- BLS: last obs 2026-03-27 (3 days old) — normal, monthly release lag
- COMTRADE: last obs 2026-03-20 (10 days old) — lagged by design

OK (Healthy)
- FRED: 37 series, latest 2026-03-28 ✓
- crypto: 8 series, latest 2026-03-30 09:15:00 ✓
- VIX: latest 2026-03-30 16:00:00 ✓

API KEY STATUS
- FRED_API_KEY: ✓ Validated
- KOSIS_API_KEY: ✗ Missing
- COMTRADE_API_KEY: ✗ Missing
- JQUANTS_EMAIL/JQUANTS_PASSWORD: ✓ Present
- USDA_NASS_API_KEY: ✓ Present
- NOAA_TOKEN: ✗ Missing
- EIA_API_KEY: ✓ Present

INGESTION SCHEDULER STATUS
- scheduler.py: Running (44 active jobs)
- 2 jobs failed in last 24h (congressional, dark_pool)
- Last successful run: 2026-03-30 08:30:00

DATA QUALITY
- NaN rates >10%: None flagged
- Conflict resolution rate: 2.3% (normal)
- PIT lookahead violations: 0

RECOMMENDATION
Monitor dark_pool and congressional ingestion — both are overdue. Check logs for API errors. All other sources healthy.
```

## Scheduler Pattern (ingestion/scheduler.py)

GRID uses `ingestion/scheduler.py` to manage ingestion cadence (the old
`scheduler_v2.py` no longer exists). There is no `INGESTION_JOBS` list: pullers are
grouped (`_get_pullers_for_group("daily")`, `"crypto"`, ...) and run through
`run_pull_group()`, with `_should_run(key, period)` gating repeats. Hermes step 3
runs only the due or stale pullers through `ingestion/smart_scheduler.py`
(SmartScheduler), which tracks per-source frequency and cooldowns.

### Checking Scheduler Status

The live view is the pipeline-health route, which lists every source with its
`status` (healthy/stale/broken), `last_pull`, `rows_last_pull` and `next_scheduled`:

```bash
curl -H "Authorization: Bearer <jwt>" http://localhost:8000/api/v1/system/pipeline-health
```

Whether the Hermes daemon itself ran a given step is `hermes-operations`.

## Debugging Common Issues

### Issue: Source Marked Stale But API is Working

1. Check if API key is present but invalid:
   ```python
   # Test API connectivity
   import requests
   response = requests.get(f"https://api.example.com/data",
                          headers={"X-API-Key": API_KEY})
   print(f"Status: {response.status_code}")  # 401 = invalid key, 200 = valid
   ```

2. Check if scheduler job is actually running:
   ```python
   from apscheduler.schedulers.background import BackgroundScheduler
   scheduler = BackgroundScheduler()
   for job in scheduler.get_jobs():
       print(f"{job.name}: next_run={job.next_run_time}, func={job.func.__name__}")
   ```

3. Check logs for errors:
   ```bash
   grep -i "error\|failed" grid/logs/ingestion_*.log | tail -50
   ```

### Issue: API Key is Set But Still Getting 401 Errors

1. Verify environment variable is loaded:
   ```python
   import os
   print(f"FRED_API_KEY loaded: {bool(os.environ.get('FRED_API_KEY'))}")
   ```

2. Check config module actually reads it:
   ```python
   from config import FRED_API_KEY
   print(f"FRED_API_KEY in config: {bool(FRED_API_KEY)}")
   ```

3. Test API directly:
   ```bash
   curl "https://api.stlouisfed.org/fred/series/GDP?api_key=$FRED_API_KEY&file_type=json"
   ```

### Issue: NaN Spike in Specific Series

1. Check raw ingestion data for parsing errors:
   ```bash
   # Last raw import before NaN spike
   SELECT * FROM raw_data WHERE series_id = 123
   ORDER BY created_at DESC LIMIT 20;
   ```

2. Check for data type mismatches:
   ```python
   df = pd.read_csv("raw_import.csv")
   print(df['value'].dtype)  # Should be numeric
   print(df['value'].head(20))  # Inspect for unparseable values
   ```

3. Check coercion logging:
   ```bash
   grep "Coerced.*NaN" grid/logs/ingestion_*.log
   ```

## Integration Checklist

Before deploying new or modified sources:

- [ ] Source is registered in `source_catalog` table
- [ ] Source has observation_date and release_date fields (PIT requirement)
- [ ] Scheduler job is added for the source
- [ ] API key (if needed) is in `.env.example`
- [ ] API key validation added to startup (if critical source)
- [ ] NaN coercion is logged
- [ ] Staleness threshold is documented
- [ ] Health check endpoint includes the source
- [ ] Post-mortem tracking captures source failures

## See Also

- `CLAUDE.md` — Architecture overview
- `ATTENTION.md` — now a stub; the numbered audit items this skill used to cite no longer exist there
- `ingestion/scheduler.py` — Authoritative scheduler (not scheduler_v2.py)
- `normalization/resolver.py` — Multi-source conflict resolution
- `store/pit.py` — Point-in-time data queries
