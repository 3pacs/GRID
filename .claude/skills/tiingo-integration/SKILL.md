# tiingo-integration

Patterns for using Tiingo Pro API for price data, news, and fundamentals. Covers rate limits, data formats, resolver integration, and bandwidth management.

## When to Use This Skill

- Adding new tickers to the GRID universe
- Backfilling historical data for new features
- Setting up news sentiment signals
- Debugging Tiingo API issues
- Planning bandwidth usage (40GB/month Pro tier)

## API Endpoints

### Daily Prices
```
GET https://api.tiingo.com/tiingo/daily/{ticker}/prices
Headers: Authorization: Token {TIINGO_API_KEY}
Params: startDate, endDate, format=json
```

Returns: date, close, high, low, open, volume, adjClose, adjHigh, adjLow, adjOpen, adjVolume, divCash, splitFactor

### News
```
GET https://api.tiingo.com/tiingo/news
Params: tickers={csv}, startDate, limit, sortBy=date
```

Returns: id, title, url, source, publishedDate, tags[], tickers[], description

## GRID Integration

### Price Puller
- File: `ingestion/tiingo_pull.py`
- Writes to raw_series as `YF:{TICKER}:{field}` (same format as yfinance)
- Batch inserts for performance
- 0.2s rate limit between calls (Pro tier)

### News Puller
- File: `ingestion/tiingo_news_pull.py`
- Stores per-article data + daily article count aggregate
- `TIINGO_NEWS:{TICKER}:{url_hash}` for articles
- `TIINGO_NEWS:{TICKER}:daily_count` for aggregates

### Resolver Integration
After pulling, resolve into resolved_series via direct SQL:
```sql
INSERT INTO resolved_series (feature_id, obs_date, ...)
SELECT fr.id, rs.obs_date, ...
FROM raw_series rs
JOIN feature_registry fr ON fr.name = LOWER(SPLIT_PART(rs.series_id, ':', 2)) || '_full'
WHERE rs.series_id LIKE 'YF:%:close' ...
ON CONFLICT DO NOTHING;
```

### Hermes Schedule
Step 7h: overnight pull at 02:00-06:00 UTC daily. Pulls last 5 days of prices + 3 days of news.

## Bandwidth Budget

| Operation | Size/ticker | Monthly for 500 tickers |
|-----------|------------|------------------------|
| Daily price update | ~500B | ~7.5MB |
| 5-year backfill | ~5KB | ~2.5MB |
| News (50 articles) | ~25KB | ~12.5MB |
| **Total monthly** | | **~25MB** (well under 40GB) |

## Ticker Format
- Tiingo uses plain symbols: SPY, AAPL, BRK-B
- No ^ prefix (yfinance uses ^GSPC, ^VIX)
- No =F suffix (yfinance uses CL=F for futures)
- No =X suffix (yfinance uses EURUSD=X for forex)

## Common Issues
- 404 = ticker not on Tiingo (check spelling, may be delisted)
- `adjClose` vs `close`: use `close` for GRID (we don't adjust for splits in raw_series)
- Pro tier has no hard rate limit but be respectful (~5 req/sec)
