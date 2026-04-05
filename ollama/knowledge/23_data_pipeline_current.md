# Current Data Pipeline State (2026-04-05)

## Database
- 1.4M+ raw_series rows locally, server has full production data
- 9,185 actors locally, 1.6M on server
- 50+ data sources registered

## Active Data Sources (Daily)
- AlphaVantage Pro (150/min): prices, earnings, financials, sentiment
- DeFi Llama: TVL, stablecoins, bridges for 4000+ protocols
- Etherscan V2: ETH price/gas, 15 whale wallets, 8 token supplies
- Wikipedia: Pageview anomaly detection for 40+ entities
- Polygon.io: Stock quotes, options reference
- NASA FIRMS: Satellite fire detection near 10 critical infrastructure sites

## Active Data Sources (Weekly)
- Redfin: Housing data for 20 metros
- Indeed: Job postings index + sectors
- LittleSis: Power-mapping relationships
- Wikidata: Board seats, subsidiaries, ownership

## Active Data Sources (Monthly)
- ICIJ: 814K entity re-download + actor discovery
- World Bank: 30 countries × 11 indicators
- EDGAR Transcripts: 8-K filings + guidance extraction

## Bulk Historical Data
- AV daily prices: 218 tickers × 20yr OHLCV (1.1M rows)
- AV financials: 120 tickers × income/balance/cash flow back to 1996 (168K rows)
- QuiverQuant: 20K insiders, 20K lobbying, 1K congressional trades

## Milestone Tracker
- 118 companies scored on execution quality (beat/miss rate, trend, streaks)
- Top: SNOW (A, 80.5%), DDOG (B+, 73%), PLTR (B+, 67%, 11-streak)
- Worst: COST (D-, 28.5%), HON (D-, 31.4%), WFC (D-, 34.3%)

## API Keys Active
Etherscan, Polygon, AlphaVantage Pro, NASA Earthdata, WorldNews, Congress.gov,
FRED, Tiingo, QuiverQuant Pro, BLS, EIA, NOAA, CoinGecko, TwelveData,
HF, OpenAI, OpenRouter, Groq, Gemini, Perplexity

## LLM Stack
- Gemma 4 31B-IT (Q4_K_M) on RTX PRO 4000 Blackwell (24GB)
- 128K context with Q4 KV cache quantization
- Port 8080, ~27 tok/s
