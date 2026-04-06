# FILL EMPTY TABLES — Agent Execution Plan

**Date:** 2026-04-05
**Server:** grid@100.75.185.36 (Tailscale)
**Repo:** ~/grid_v4/grid_repo (server) or ~/dev/GRID (local)
**DB:** PostgreSQL — griddb, user=grid
**LLM:** Gemma 4 31B on localhost:8080 via llama.cpp

---

## SETUP — Run Before Every Task

```bash
ssh grid@100.75.185.36
cd ~/grid_v4/grid_repo && set -a && source .env 2>/dev/null && set +a
```

Verify DB is up: `psql -U grid -d griddb -c "SELECT 1;"`
Verify LLM is up: `curl -s http://localhost:8080/health | head -5`

---

## DEPENDENCY ORDER

Tasks must be executed in this order because later tasks depend on earlier ones:

```
Phase 1 (independent — run in parallel):
  Task 1: dark_pool_weekly
  Task 2: margin_debt_monthly
  Task 3: earnings_predictions

Phase 2 (depends on existing data being fresh):
  Task 4: company_profiles
  Task 5: discovered_hypotheses

Phase 3 (depends on Phase 1-2 data):
  Task 6: options_recommendations

Phase 4 (depends on time-series data + forecasting model):
  Task 7: timeseries_forecasts

Bonus:
  Task 8: decision_journal cleanup
  Task 9: flow_materializer sync
```

---

## TASK 1: dark_pool_weekly — 0 rows

### Root Cause

The `DarkPoolPuller` stores data into `raw_series` (as `DARKPOOL:{ticker}:volume` and `DARKPOOL:{ticker}:trades`), NOT directly into `dark_pool_weekly`. The `dark_pool_weekly` table is a **materialized query-friendly table** populated by `ingestion/flow_materializer.py:sync_all()`.

There are two things that may be wrong:
1. The puller itself may not have been run (FINRA API 400 error reported)
2. Even if raw_series has dark pool data, the materializer may not have been run

### Step 1: Check if raw_series has dark pool data

```bash
psql -U grid -d griddb -c "
  SELECT COUNT(*), MAX(observation_date)
  FROM raw_series
  WHERE series_id LIKE 'DARKPOOL:%';
"
```

### Step 2A: If raw_series has data → run the materializer

```bash
python3 -c "
from ingestion.flow_materializer import sync_all
from db import get_engine
result = sync_all(get_engine())
print(result)
"
```

Verify: `psql -U grid -d griddb -c "SELECT COUNT(*) FROM dark_pool_weekly;"`

### Step 2B: If raw_series has NO dark pool data → fix the puller

```bash
# Test the FINRA API directly
curl -s -X POST "https://api.finra.org/data/group/otcMarket/name/weeklySummary" \
  -H "Content-Type: application/json" \
  -d '{"fields":["weekStartDate","totalWeeklyShareQuantity","totalWeeklyTradeCount","issueSymbolIdentifier"],"dateRangeFilters":[{"fieldName":"weekStartDate","startDate":"2026-03-01","endDate":"2026-04-05"}],"domainFilters":[{"fieldName":"issueSymbolIdentifier","values":["SPY","QQQ","AAPL"]}],"limit":100,"offset":0}' | python3 -m json.tool | head -30
```

If the API returns 400:
- Check https://otctransparency.finra.org/otctransparency/api for updated endpoints
- The URL may have changed — FINRA sometimes renames from `weeklySummary` to `weeklyReport`
- Try without `domainFilters` to see if the filter format changed
- Check if FINRA now requires auth headers (API key or Accept header)

**Backup source if FINRA API is permanently broken:**
Create a scraper for https://otctransparency.finra.org/otctransparency/OtcIssueData using requests + BeautifulSoup. The web interface still shows the data. Follow the existing puller pattern in `ingestion/altdata/congressional.py`.

Once raw_series has data, re-run the materializer (Step 2A).

### Step 3: Run the puller

```bash
python3 -c "
from ingestion.altdata.dark_pool import DarkPoolPuller
from db import get_engine
p = DarkPoolPuller(get_engine())
p.pull_weekly(weeks_back=12)
"
```

### Verify

```bash
psql -U grid -d griddb -c "SELECT ticker, report_date, short_volume, total_volume FROM dark_pool_weekly ORDER BY report_date DESC LIMIT 10;"
```

### Files to Modify

| File | What | Why |
|------|------|-----|
| `ingestion/altdata/dark_pool.py` | Fix FINRA API URL/params if 400 | API may have changed |
| `ingestion/flow_materializer.py` | Possibly nothing | May just need to be run |

### Success Criteria

- `dark_pool_weekly` has 100+ rows across multiple tickers
- Data spans at least 4 weeks
- `short_pct` values are between 0 and 1

---

## TASK 2: margin_debt_monthly — 0 rows

### Root Cause

No puller exists for this table. The table schema is defined in `migrations/versions/f1a2b3c4d5e6_capital_flow_tables.py` (lines 346-363) but nothing writes to it. Multiple downstream consumers exist:
- `analysis/money_flow_engine/layer_retail.py:46` — `_build_margin_debt_node()`
- `analysis/flow_thesis_data.py:1023` — `_get_margin_debt_leverage_state()`
- `analysis/sector_map.py:1163` — margin_debt entry

### Step 1: Add FRED margin debt series to the FRED puller

The best approach is to add the margin-related FRED series to the existing FRED puller and then create a small materializer to transform raw_series → margin_debt_monthly.

**FRED series for margin debt:**
- `BOGZ1FL663067003Q` — Security brokers/dealers margin accounts (quarterly, level)
- Already in the pull list for cross-border flows

But this is **quarterly**. The table is `margin_debt_monthly`. For monthly granularity:

**Primary: FINRA Margin Statistics (monthly, free, no key)**
- URL: https://www.finra.org/investors/learn-to-invest/advanced-investing/margin-statistics
- Published monthly with ~6-week lag
- Contains: debit balances, free credit cash, free credit margin accounts

### Step 2: Create the puller

Create `ingestion/altdata/margin_debt.py` following the `congressional.py` pattern:

```python
"""
GRID — FINRA margin debt monthly data puller.

Scrapes FINRA margin statistics page for monthly margin debt data.
Backup: FRED series BOGZ1FL663067003Q (quarterly).
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller


class MarginDebtPuller(BasePuller):
    SOURCE_NAME: str = "FINRA_MARGIN"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.finra.org/investors/learn-to-invest/advanced-investing/margin-statistics",
        "cost_tier": "FREE",
        "latency_class": "MONTHLY",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 38,
    }

    def pull(self) -> dict[str, Any]:
        """Pull margin debt data from FINRA statistics page."""
        # 1. Try FINRA page scrape
        result = self._pull_finra()
        if result["rows_inserted"] > 0:
            return result
        
        # 2. Fallback: FRED quarterly series
        log.warning("FINRA margin scrape failed, falling back to FRED quarterly")
        return self._pull_fred_fallback()

    def _pull_finra(self) -> dict[str, Any]:
        # Scrape the FINRA margin statistics page
        # Parse the table for monthly margin debt values
        # Store in margin_debt_monthly
        ...

    def _pull_fred_fallback(self) -> dict[str, Any]:
        # Query raw_series for BOGZ1FL663067003Q
        # Transform quarterly → margin_debt_monthly with source='FRED'
        ...

    def _store_row(self, conn, obs_date: date, margin_debt: float,
                   free_credit_cash: float | None, free_credit_margin: float | None,
                   source: str = "FINRA") -> bool:
        conn.execute(text("""
            INSERT INTO margin_debt_monthly (obs_date, margin_debt, free_credit_cash,
                free_credit_margin, net_margin, source)
            VALUES (:d, :md, :fcc, :fcm, :nm, :src)
            ON CONFLICT (obs_date) DO NOTHING
        """), {
            "d": obs_date,
            "md": margin_debt,
            "fcc": free_credit_cash,
            "fcm": free_credit_margin,
            "nm": (margin_debt - (free_credit_cash or 0) - (free_credit_margin or 0)),
            "src": source,
        })
        return True
```

**IMPORTANT: The FINRA margin stats page may serve data as a downloadable CSV or embedded table. Inspect the page before coding the scraper.**

### Step 3: Alternative — direct FRED approach (simpler, quarterly)

If FINRA scraping is too fragile, just use FRED:

1. Add `BOGZ1FL663067003Q` to `FRED_SERIES_LIST` in `ingestion/fred.py` (if not already there — check first, it may be listed under cross-border)
2. Create a simple materializer function:

```python
def materialize_margin_debt_from_fred(engine: Engine) -> int:
    """Transform FRED quarterly margin debt series into margin_debt_monthly."""
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT observation_date, value
            FROM raw_series
            WHERE series_id = 'FRED:BOGZ1FL663067003Q'
              AND value IS NOT NULL
            ORDER BY observation_date
        """)).fetchall()
        
        inserted = 0
        for obs_date, value in rows:
            conn.execute(text("""
                INSERT INTO margin_debt_monthly (obs_date, margin_debt, source)
                VALUES (:d, :v, 'FRED')
                ON CONFLICT (obs_date) DO NOTHING
            """), {"d": obs_date, "v": float(value)})
            inserted += 1
        return inserted
```

### Step 4: Register in Hermes

Add to `scripts/hermes_operator.py` `_SOURCE_REGISTRY`:

```python
"margin_debt": {
    "mod": "ingestion.altdata.margin_debt",
    "cls": "MarginDebtPuller",
    "interval": "monthly",
},
```

### Verify

```bash
psql -U grid -d griddb -c "SELECT obs_date, margin_debt, source FROM margin_debt_monthly ORDER BY obs_date DESC LIMIT 10;"
```

### Files to Create/Modify

| File | Action | Details |
|------|--------|---------|
| `ingestion/altdata/margin_debt.py` | CREATE | New puller module |
| `ingestion/fred.py` | MODIFY (maybe) | Ensure BOGZ1FL663067003Q is in pull list |
| `scripts/hermes_operator.py` | MODIFY | Register new puller |

### Success Criteria

- `margin_debt_monthly` has 20+ rows (ideally 10+ years of quarterly data from FRED)
- `_build_margin_debt_node()` in `layer_retail.py` returns a valid FlowNode (test this)
- `change_mom` and `change_yoy` are populated (may need post-processing)

---

## TASK 3: earnings_predictions — 0 rows

### Root Cause

The module exists and is complete (`intelligence/earnings_intel.py`). The `run_earnings_cycle()` function generates predictions for upcoming earnings within 14 days. It was likely never called, OR `earnings_calendar` has no unreported upcoming entries.

### Step 1: Check prerequisite data

```bash
# Check earnings_calendar
psql -U grid -d griddb -c "
  SELECT COUNT(*) as total,
         COUNT(*) FILTER (WHERE reported = FALSE AND earnings_date >= CURRENT_DATE) as upcoming,
         COUNT(*) FILTER (WHERE reported = FALSE AND earnings_date <= CURRENT_DATE + 14) as in_window
  FROM earnings_calendar;
"
```

### Step 2A: If earnings_calendar has upcoming unreported entries → run the cycle

```bash
python3 -c "
from intelligence.earnings_intel import run_earnings_cycle
from db import get_engine
result = run_earnings_cycle(get_engine())
print(result)
"
```

### Step 2B: If earnings_calendar is stale → refresh it first

```bash
# Run the earnings calendar puller
python3 -c "
from ingestion.altdata.earnings_calendar import EarningsCalendarPuller
from db import get_engine
p = EarningsCalendarPuller(get_engine())
p.pull()
"
```

If no `EarningsCalendarPuller` class exists, check for the correct module:
```bash
grep -rn "earnings_calendar" ingestion/ --include="*.py" -l
```

Then run `run_earnings_cycle()` again.

### Step 2C: If earnings_calendar has entries but ALL have `earnings_date < CURRENT_DATE`

The 14-day lookahead window (`CURRENT_DATE + 14`) may miss all entries. Temporarily widen it:

In `intelligence/earnings_intel.py` line 575, the query filters for `ec.earnings_date <= CURRENT_DATE + 14`. If all upcoming earnings are >14 days out, increase to 30:

```python
# Temporary: widen window to 30 days to populate initial data
AND ec.earnings_date <= CURRENT_DATE + 30
```

OR just call `predict_earnings_reaction()` directly for known tickers:

```bash
python3 -c "
from intelligence.earnings_intel import predict_earnings_reaction
from db import get_engine
engine = get_engine()
for t in ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA']:
    try:
        result = predict_earnings_reaction(engine, t)
        print(f'{t}: {result.get(\"predicted_direction\", \"N/A\")} ({result.get(\"confidence\", 0):.2f})')
    except Exception as e:
        print(f'{t}: FAILED - {e}')
"
```

### Verify

```bash
psql -U grid -d griddb -c "
  SELECT ticker, earnings_date, predicted_direction, predicted_move_pct, confidence, verdict
  FROM earnings_predictions
  ORDER BY earnings_date DESC LIMIT 10;
"
```

### Files to Modify

| File | What | Why |
|------|------|-----|
| `intelligence/earnings_intel.py` | Maybe widen 14-day window | To catch more upcoming earnings |
| None (if it just needs running) | — | — |

### Success Criteria

- `earnings_predictions` has 5+ rows
- Each row has non-null `predicted_direction`, `predicted_move_pct`, `confidence`
- `verdict` is `'pending'` for future earnings, `'hit'`/`'miss'` for past ones

---

## TASK 4: company_profiles — 0 rows

### Root Cause

The module exists (`intelligence/company_analyzer.py`) with `analyze_company()` and `run_analysis_queue()`. It was never run. The analysis is LLM-heavy — it queries every intelligence module (gov contracts, lobbying, insider edge, export controls, actor network) and then generates an LLM narrative.

### Step 1: Check if intelligence data exists for analysis

```bash
psql -U grid -d griddb -c "
  SELECT 'signal_sources' as tbl, COUNT(*) FROM signal_sources
  UNION ALL SELECT 'actors', COUNT(*) FROM actors
  UNION ALL SELECT 'insider_trades', COUNT(*) FROM insider_trades
  UNION ALL SELECT 'congressional_trades', COUNT(*) FROM congressional_trades
  UNION ALL SELECT 'gov_contracts', COUNT(*) FROM gov_contracts
;
"
```

If these tables have data, the analyzer can run.

### Step 2: Run analysis for priority tickers

```bash
python3 -c "
from intelligence.company_analyzer import analyze_company
from db import get_engine
engine = get_engine()

# Start with mega-caps that have the most intelligence data
tickers = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA', 'JPM', 'BAC', 'GS']
for t in tickers:
    try:
        profile = analyze_company(engine, t)
        print(f'{t}: suspicion={profile.suspicion_score:.3f}, confidence={profile.confidence:.2f}')
    except Exception as e:
        print(f'{t}: FAILED - {e}')
"
```

### Step 3: Run the batch queue

```bash
python3 -c "
from intelligence.company_analyzer import run_analysis_queue
from db import get_engine
result = run_analysis_queue(get_engine(), batch_size=10)
print(result)
"
```

### Backup: If LLM is down, populate basic fundamentals without narrative

If the LLM at localhost:8080 is unavailable, the `_generate_narrative()` function should fall back gracefully (check). If it crashes, the analyzer still stores the profile — you may need to catch the LLM exception and store with `narrative=""`.

For basic market cap / sector / industry data without the full analysis pipeline:

```bash
python3 << 'PYEOF'
"""Seed company_profiles with yfinance fundamentals as baseline."""
import yfinance as yf
from sqlalchemy import text
from db import get_engine
import json

engine = get_engine()
tickers = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
    "JPM", "BAC", "GS", "NFLX", "AMD", "PLTR", "COIN", "SOFI",
    "SPY", "QQQ", "IWM", "XLF", "XLE", "XLK",
    "SNOW", "DDOG", "CRWD", "NET", "ABNB", "UBER", "SQ",
]

for t in tickers:
    try:
        info = yf.Ticker(t).info
        profile = {
            "market_cap": info.get("marketCap"),
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
            "revenue": info.get("totalRevenue"),
            "cash": info.get("totalCash"),
            "shares_outstanding": info.get("sharesOutstanding"),
            "short_interest_pct": info.get("shortPercentOfFloat"),
            "pe_ratio": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
            "dividend_yield": info.get("dividendYield"),
            "52w_high": info.get("fiftyTwoWeekHigh"),
            "52w_low": info.get("fiftyTwoWeekLow"),
            "beta": info.get("beta"),
        }
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO company_profiles (ticker, name, sector, profile, suspicion_score, last_analyzed)
                VALUES (:t, :n, :s, :p, 0, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    profile = :p, last_analyzed = NOW()
            """), {
                "t": t,
                "n": info.get("shortName", t),
                "s": info.get("sector", "Other"),
                "p": json.dumps(profile),
            })
        print(f"{t}: OK — mcap={profile['market_cap']}")
    except Exception as e:
        print(f"{t}: FAILED — {e}")
PYEOF
```

### Step 4: Backfill market_cap_mm in trial_signals

After company_profiles has data, update trial_signals:

```bash
psql -U grid -d griddb -c "
  UPDATE trial_signals ts
  SET market_cap_mm = (cp.profile->>'market_cap')::numeric / 1e6
  FROM company_profiles cp
  WHERE ts.ticker = cp.ticker
    AND ts.market_cap_mm IS NULL
    AND cp.profile->>'market_cap' IS NOT NULL;
"
```

### Verify

```bash
psql -U grid -d griddb -c "
  SELECT ticker, name, sector, suspicion_score,
         (profile->>'market_cap')::bigint / 1e9 as mcap_b,
         last_analyzed
  FROM company_profiles
  ORDER BY (profile->>'market_cap')::bigint DESC NULLS LAST
  LIMIT 15;
"
```

### Files to Modify

| File | What | Why |
|------|------|-----|
| `intelligence/company_analyzer.py` | Possibly nothing | May just need to be run |
| None (if backup script is used) | One-shot script | Seed basic data |

### Success Criteria

- `company_profiles` has 25+ tickers
- Each has non-null `sector` and `profile` JSONB with `market_cap`
- `trial_signals.market_cap_mm` is no longer NULL for matched tickers

---

## TASK 5: discovered_hypotheses — 0 rows

### Important Note

The handoff document says `hypothesis_registry` is empty, but that's the **model governance** table (schema.sql:142). The hypothesis discovery engine writes to `discovered_hypotheses` (intelligence/hypothesis_engine.py:95). Both may be empty.

The `discovered_hypotheses` table is what the auto-discovery pipeline populates. The `hypothesis_registry` is populated when a discovered hypothesis is promoted to a formal testable hypothesis in the model governance pipeline.

**Focus on `discovered_hypotheses` first** — that feeds the governance pipeline.

### Step 1: Check prerequisites

The hypothesis engine needs data in `raw_series` and `signal_sources` to find patterns:

```bash
psql -U grid -d griddb -c "
  SELECT 'raw_series' as tbl, COUNT(*) FROM raw_series
  UNION ALL SELECT 'signal_sources', COUNT(*) FROM signal_sources
  UNION ALL SELECT 'feature_registry', COUNT(*) FROM feature_registry
;
"
```

Need: raw_series > 10K rows, signal_sources > 100 rows, feature_registry > 100 rows.

### Step 2: Run auto-discovery

```bash
python3 -c "
from intelligence.hypothesis_engine import HypothesisEngine
from db import get_engine
engine = get_engine()
h = HypothesisEngine(engine)
results = h.auto_discover()
print(f'Generated {len(results)} hypotheses')
for r in results[:5]:
    print(f'  - {r[\"thesis\"][:100]}... (confidence={r[\"confidence\"]:.3f})')
"
```

### Step 3: If auto_discover finds 0 patterns

The engine needs sufficient time-series overlap to detect lead-lag relationships (MIN_OBSERVATIONS = 5). If there isn't enough data:

1. Make sure FRED puller has been run: `python3 -c "from ingestion.fred import FREDPuller; from db import get_engine; FREDPuller(get_engine()).pull_all()"`
2. Ensure options/earnings/congressional data is fresh
3. Try lowering `MIN_OBSERVATIONS` from 5 to 3 temporarily (line 42)
4. Try lowering `SIGNIFICANCE_THRESHOLD` from 0.05 to 0.10 temporarily (line 43)

### Verify

```bash
psql -U grid -d griddb -c "
  SELECT id, LEFT(thesis, 80) as thesis, pattern_type, confidence, status
  FROM discovered_hypotheses
  ORDER BY confidence DESC LIMIT 10;
"
```

### Files to Modify

| File | What | Why |
|------|------|-----|
| `intelligence/hypothesis_engine.py` | Maybe lower thresholds temporarily | Bootstrap initial hypotheses |

### Success Criteria

- `discovered_hypotheses` has 5+ rows
- Mix of `lead_lag`, `anomaly_convergence`, `actor_shift` pattern types
- Confidence values between 0.1 and 0.9

---

## TASK 6: options_recommendations — 0 rows

### Root Cause

The `OptionsRecommender` (trading/options_recommender.py) calls `OptionsScanner.scan_all(min_score=self.min_score)` where `self.min_score` defaults to 6.0. The scanner (discovery/options_scanner.py) has its own default of 5.0. The scanner's CLI uses 4.0 (line 772).

Zero recommendations means either:
1. Input data (`options_snapshots` / `options_daily_signals`) is stale
2. The threshold is too high for current market conditions

### Step 1: Check input data freshness

```bash
psql -U grid -d griddb -c "
  SELECT 'options_snapshots' as tbl, COUNT(*), MAX(snapshot_date) as latest
  FROM options_snapshots
  UNION ALL
  SELECT 'options_daily_signals', COUNT(*), MAX(signal_date)
  FROM options_daily_signals;
"
```

If data is older than 3 days, refresh first:

```bash
# Find and run the options data puller
grep -rn "options_snapshots\|options_daily" ingestion/ --include="*.py" -l
# Then run the appropriate puller
```

### Step 2: Test scanner at lower threshold

```bash
python3 -c "
from discovery.options_scanner import OptionsScanner
from db import get_engine
scanner = OptionsScanner(db_engine=get_engine())
opps = scanner.scan_all(min_score=3.0)
print(f'Found {len(opps)} opportunities at min_score=3.0')
for o in opps[:5]:
    print(f'  {o.ticker}: score={o.score:.1f}, direction={o.direction}')
"
```

### Step 3: Generate recommendations at adjusted threshold

```bash
python3 -c "
from trading.options_recommender import OptionsRecommender
from db import get_engine
engine = get_engine()
r = OptionsRecommender(engine, min_score=4.0)
recs = r.generate_recommendations()
print(f'Generated {len(recs)} recommendations')
for rec in recs:
    print(rec.to_trade_ticket() if hasattr(rec, 'to_trade_ticket') else str(rec)[:200])
"
```

### Step 4: If still 0 — check individual signal scores

```bash
python3 -c "
from discovery.options_scanner import OptionsScanner
from db import get_engine
scanner = OptionsScanner(db_engine=get_engine())
# Get raw signals for a single liquid ticker
result = scanner._scan_ticker('SPY')
if result:
    print(f'SPY score: {result.score}')
    for sig_name, sig_data in result.signals.items():
        print(f'  {sig_name}: {sig_data}')
else:
    print('SPY: no result (likely missing data)')
"
```

If SPY returns None, the issue is missing options data, not threshold.

### Step 5: Persist threshold change

If lowering to 4.0 produces good results, make it configurable:

In `trading/options_recommender.py`, the constructor already accepts `min_score`:
```python
def __init__(self, db_engine, min_score=6.0, ...):
```

Change the default to 4.0:
```python
def __init__(self, db_engine, min_score=4.0, ...):
```

### Verify

```bash
psql -U grid -d griddb -c "
  SELECT ticker, direction, strike, expiry, entry_price, target_price,
         stop_loss, kelly_fraction, confidence
  FROM options_recommendations
  ORDER BY created_at DESC LIMIT 10;
"
```

### Files to Modify

| File | What | Why |
|------|------|-----|
| `trading/options_recommender.py` | Lower default min_score from 6.0 to 4.0 | Bootstrap initial recommendations |
| `discovery/options_scanner.py` | Possibly nothing | Scanner default is already 5.0 |

### Success Criteria

- `options_recommendations` has 3+ rows
- Each has: ticker, direction, strike, expiry, entry_price, target_price, stop_loss, kelly_fraction
- thesis field is non-null and explains the trade

---

## TASK 7: timeseries_forecasts — 0 rows

### Root Cause

The table is created by `oracle/engine.py:290-303`. It's populated by the `oracle/forecaster_adapter.py` which wraps TimesFM forecasts. TimesFM is a Google time-series foundation model that requires either:
1. A local installation (`timesfm` pip package + model weights)
2. A Hugging Face Inference API call

### Step 1: Check if TimesFM is available

```bash
python3 -c "import timesfm; print('TimesFM available')" 2>&1
```

### Step 2A: If TimesFM is installed → run forecasts

```bash
# Find the forecaster entry point
grep -rn "def forecast\|def run_forecast\|class.*Forecaster" oracle/ --include="*.py"
```

Then run it:

```bash
python3 -c "
from oracle.engine import OracleEngine
from db import get_engine
engine = get_engine()
oracle = OracleEngine(engine)
# Run prediction cycle which includes forecasts
oracle.run_prediction_cycle()
"
```

### Step 2B: If TimesFM is NOT installed → use statistical fallback

Create a simple statistical forecaster that writes to the same table format:

```python
"""Statistical fallback forecaster for timeseries_forecasts table."""
import numpy as np
from datetime import date, timedelta
from sqlalchemy import text
from db import get_engine

def generate_statistical_forecasts(engine, tickers=None, horizons=(5, 10, 21)):
    """Generate simple statistical forecasts using exponential smoothing."""
    if tickers is None:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT split_part(series_id, ':', 2) as ticker
                FROM raw_series
                WHERE series_id LIKE 'TIINGO:%:adjclose'
                   OR series_id LIKE 'YFINANCE:%:adjclose'
                ORDER BY ticker LIMIT 50
            """)).fetchall()
            tickers = [r[0] for r in rows]

    inserted = 0
    for ticker in tickers:
        with engine.connect() as conn:
            prices = conn.execute(text("""
                SELECT observation_date, value FROM raw_series
                WHERE (series_id = :s1 OR series_id = :s2)
                  AND value IS NOT NULL
                ORDER BY observation_date DESC LIMIT 252
            """), {
                "s1": f"TIINGO:{ticker}:adjclose",
                "s2": f"YFINANCE:{ticker}:adjclose",
            }).fetchall()

        if len(prices) < 60:
            continue

        values = np.array([float(r[1]) for r in reversed(prices)])
        returns = np.diff(np.log(values))

        for h in horizons:
            mu = returns[-60:].mean() * h
            sigma = returns[-60:].std() * np.sqrt(h)
            last_price = values[-1]

            pred = last_price * np.exp(mu)
            lower = last_price * np.exp(mu - 1.96 * sigma)
            upper = last_price * np.exp(mu + 1.96 * sigma)

            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO timeseries_forecasts
                        (ticker, forecast_date, horizon, predictions, lower_bound,
                         upper_bound, forecast_std, model_version)
                    VALUES (:t, :fd, :h, :p, :lb, :ub, :fs, :mv)
                    ON CONFLICT (ticker, forecast_date, horizon) DO UPDATE SET
                        predictions = :p, lower_bound = :lb, upper_bound = :ub,
                        forecast_std = :fs, model_version = :mv, created_at = NOW()
                """), {
                    "t": ticker, "fd": date.today(), "h": h,
                    "p": str(round(pred, 2)),
                    "lb": str(round(lower, 2)),
                    "ub": str(round(upper, 2)),
                    "fs": str(round(last_price * sigma, 2)),
                    "mv": "statistical_v1",
                })
                inserted += 1

    return {"tickers": len(tickers), "forecasts_inserted": inserted}
```

Run it:

```bash
python3 -c "
exec(open('scripts/statistical_forecaster.py').read())
from db import get_engine
result = generate_statistical_forecasts(get_engine())
print(result)
"
```

### Verify

```bash
psql -U grid -d griddb -c "
  SELECT ticker, forecast_date, horizon, predictions, lower_bound, upper_bound, model_version
  FROM timeseries_forecasts
  ORDER BY created_at DESC LIMIT 15;
"
```

### Files to Create

| File | Action | Details |
|------|--------|---------|
| `scripts/statistical_forecaster.py` | CREATE (if TimesFM unavailable) | Statistical fallback |

### Success Criteria

- `timeseries_forecasts` has 50+ rows (multiple tickers x multiple horizons)
- Each has numeric predictions, lower_bound, upper_bound
- `model_version` is set (`timesfm_v1` or `statistical_v1`)

---

## TASK 8: decision_journal cleanup (BONUS)

### Problem

The `decision_journal` entries have NULL in key fields (ticker, direction, thesis, verdict). The table is append-only (immutability trigger prevents modification of decision fields). Only `annotation`, `outcome_value`, `outcome_recorded_at`, and `verdict` can be updated.

### Step 1: Diagnose

```bash
psql -U grid -d griddb -c "
  SELECT COUNT(*) as total,
         COUNT(ticker) FILTER (WHERE ticker IS NOT NULL) as has_ticker,
         COUNT(direction) FILTER (WHERE direction IS NOT NULL) as has_direction,
         COUNT(thesis) FILTER (WHERE thesis IS NOT NULL) as has_thesis,
         COUNT(verdict) as has_verdict
  FROM decision_journal;
"
```

### Step 2: Check the journal writer

```bash
grep -n "INSERT INTO decision_journal" journal/log.py
```

Read the INSERT statement to see which fields it populates. The NULL fields suggest the writer isn't passing all required data.

### Step 3: Fix journal writer

The fix depends on what the INSERT looks like. The writer likely needs to populate `ticker` and `direction` from the recommendation that triggered the journal entry.

**DO NOT try to UPDATE existing NULL rows in decision fields** — the immutability trigger will block it. Only `verdict` and `annotation` are updatable.

### Success Criteria

- New journal entries have non-null ticker, direction, thesis
- `verdict` field is populated for entries with outcomes

---

## TASK 9: flow_materializer sync (BONUS)

### Why

The flows API reads from `insider_trades`, `congressional_trades`, `dark_pool_weekly`, `etf_flows`, and `junction_point_readings`. These are materialized from `signal_sources` and `raw_series` by the flow materializer.

### Run

```bash
python3 -c "
from ingestion.flow_materializer import sync_all
from db import get_engine
result = sync_all(get_engine())
for table, info in result.items():
    print(f'{table}: {info}')
"
```

### Verify

```bash
psql -U grid -d griddb -c "
  SELECT 'insider_trades' as tbl, COUNT(*) FROM insider_trades
  UNION ALL SELECT 'congressional_trades', COUNT(*) FROM congressional_trades
  UNION ALL SELECT 'dark_pool_weekly', COUNT(*) FROM dark_pool_weekly
  UNION ALL SELECT 'etf_flows', COUNT(*) FROM etf_flows
  UNION ALL SELECT 'junction_point_readings', COUNT(*) FROM junction_point_readings
;
"
```

---

## GLOBAL CONSTRAINTS

1. **No paid API calls** unless the key already exists in `.env`. Check with `grep -c "KEY" .env` before using any API.
2. **All SQL must be parameterized** — never use f-strings or `.format()` for queries.
3. **Follow the existing puller pattern** in `ingestion/altdata/congressional.py` for any new modules.
4. **Test against the actual server DB** before committing.
5. **Do NOT modify:** LLM feedback loop, RAG orthogonality selection, or the 27 audited prompts.
6. **Confidence labels required** on all stored data: confirmed / derived / estimated / rumored / inferred.

## VERIFICATION CHECKLIST

After all tasks are complete, run this final check:

```bash
psql -U grid -d griddb -c "
  SELECT 'dark_pool_weekly' as tbl, COUNT(*) FROM dark_pool_weekly
  UNION ALL SELECT 'margin_debt_monthly', COUNT(*) FROM margin_debt_monthly
  UNION ALL SELECT 'earnings_predictions', COUNT(*) FROM earnings_predictions
  UNION ALL SELECT 'company_profiles', COUNT(*) FROM company_profiles
  UNION ALL SELECT 'discovered_hypotheses', COUNT(*) FROM discovered_hypotheses
  UNION ALL SELECT 'options_recommendations', COUNT(*) FROM options_recommendations
  UNION ALL SELECT 'timeseries_forecasts', COUNT(*) FROM timeseries_forecasts
  UNION ALL SELECT 'decision_journal', COUNT(*) FROM decision_journal
;
"
```

**Target state: ALL rows > 0.**

## DELIVERABLES WHEN DONE

Report back with:
1. Branch name and changed files
2. Row counts for each table (before/after)
3. Any migrations run
4. Any new puller modules created
5. Tests added or run
6. Unresolved issues or blockers
