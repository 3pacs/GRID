# Tiingo Backfill

Bulk download historical price + news data from Tiingo Pro for all tracked tickers.

## Arguments

- `$TICKER_LIST` — comma-separated tickers (optional, defaults to full universe)
- `$START_DATE` — start date YYYY-MM-DD (optional, defaults to 5 years ago)

## Instructions

1. **Load environment:**
   ```bash
   cd /data/grid_v4/grid_repo
   ```

2. **Run price backfill:**
   ```python
   from ingestion.tiingo_pull import TiingoPuller
   from db import get_engine
   engine = get_engine()
   puller = TiingoPuller(engine)
   results = puller.pull_all(ticker_list=TICKERS, start_date=START_DATE)
   ```

3. **Run news backfill:**
   ```python
   from ingestion.tiingo_news_pull import TiingoNewsPuller
   tnp = TiingoNewsPuller(engine)
   news = tnp.pull_bulk_history(ticker_list=TICKERS, days_back=90)
   ```

4. **Resolve into resolved_series** (use direct SQL, NOT the Python resolver):
   ```sql
   INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, source_priority_used, conflict_flag)
   SELECT fr.id, rs.obs_date, rs.pull_timestamp::date, rs.pull_timestamp::date, rs.value, rs.source_id, FALSE
   FROM raw_series rs
   JOIN feature_registry fr ON fr.name = LOWER(SPLIT_PART(rs.series_id, ':', 2)) || '_full'
   WHERE rs.series_id LIKE 'YF:%:close' AND rs.pull_status = 'SUCCESS' AND rs.obs_date >= '2021-01-01'
   ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING;
   ```

5. **Create feature_registry entries** for any new tickers:
   ```sql
   INSERT INTO feature_registry (name, family, description, transformation, normalization, missing_data_policy, eligible_from_date)
   VALUES ('ticker_full', 'equity', 'Close TICKER', 'RAW', 'ZSCORE', 'FORWARD_FILL', '2020-01-01')
   ON CONFLICT (name) DO NOTHING;
   ```

6. **Report:**
   - Tickers pulled / failed
   - Total rows inserted
   - Bandwidth estimate (each ticker ~5KB/year of daily data)
   - Tiingo Pro: 40GB/month budget

## Key constraints
- Rate limit: 0.2s between calls (Pro tier)
- Always use batch inserts (not row-by-row)
- Direct SQL resolution is 1000x faster than the Python resolver for bulk loads
- Tiingo uses plain symbols (no ^, =F, =X prefixes)
