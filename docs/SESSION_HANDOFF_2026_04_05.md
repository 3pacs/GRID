# Session Handoff — 2026-04-05

## What Was Built

### New Data Pullers (20+, all tested, all scheduler-registered)
```
ingestion/altdata/defi_llama_puller.py      — DeFi protocol TVL, stablecoins, bridges (no key)
ingestion/altdata/etherscan_puller.py       — ETH whale wallets, gas, token supplies (V2 API)
ingestion/altdata/fmp_puller.py             — Financial Modeling Prep (free tier limited, falls back)
ingestion/altdata/earnings_puller.py        — yfinance EPS/surprises for 96 tickers
ingestion/altdata/polygon_puller.py         — Options reference, stock quotes (free tier)
ingestion/altdata/cryptoquant_puller.py     — BTC/ETH exchange flows, SOPR, NUPL (key needs paid plan)
ingestion/altdata/wikipedia_puller.py       — Pageview anomaly detection, Z-score > 3
ingestion/altdata/google_trends_puller.py   — Breakout detection via pytrends
ingestion/altdata/cloudflare_radar_puller.py — Internet traffic (403s now, needs auth)
ingestion/altdata/redfin_puller.py          — Housing data, 20 metros (CSV)
ingestion/altdata/indeed_hiring_puller.py   — Job postings index + sectors (CSV)
ingestion/altdata/nasa_firms_puller.py      — Satellite fire detection near infrastructure
ingestion/altdata/edgar_transcripts.py      — 8-K filing text + LLM milestone extraction
ingestion/altdata/icij_puller.py            — 814K ICIJ offshore entities
ingestion/altdata/littlesis_puller.py       — Power-mapping relationships
ingestion/altdata/wikidata_puller.py        — Board seats, subsidiaries, ownership (SPARQL)
ingestion/altdata/opensecrets_puller.py     — Political donations/lobbying
ingestion/altdata/findkg_puller.py          — Financial knowledge graph
ingestion/international/world_bank_puller.py — 30 countries × 11 indicators
```

### Intelligence Modules
```
intelligence/actor_ingest.py          — Universal actor discovery from ALL data sources
intelligence/actor_researcher.py      — LLM-powered continuous enrichment with rabbit holes
intelligence/milestone_tracker.py     — Execution scorecard (beat/miss rate, trend, streaks)
intelligence/attention_anomaly.py     — Wikipedia + Trends combined scoring
intelligence/power_mapper.py          — Unified influence graph from all power-mapping sources
intelligence/icij_linker.py           — Fuzzy match actors against offshore entities
intelligence/icij_actor_discovery.py  — Auto-discover high-connection offshore players
```

### Infrastructure
```
rag/                              — pgvector RAG pipeline (chunker, indexer, retriever, pipeline)
inference/kv_cache_manager.py     — TurboQuant KV cache compression for live inference
migrations/add_pgvector_rag.sql   — pgvector, ICIJ tables, actors, attention_anomaly tables
scripts/av_bulk_download.py       — AV Pro bulk: earnings + financials for 120 tickers
scripts/av_bulk_prices.py         — AV Pro bulk: 218 tickers × 20yr daily OHLCV
scripts/quiver_bulk_download.py   — QuiverQuant bulk: congress, insiders, lobbying, flights, WSB
scripts/actor_research_daemon.sh  — 24/7 daemon for actor enrichment (on server)
```

### Frontend
```
pwa/src/views/MilestoneTracker.jsx  — Execution scorecard + timeline drill-down (809 lines)
pwa/src/views/AttentionRadar.jsx    — Radar control room for attention anomalies (767 lines)
```

### API Endpoints (new)
```
GET /api/intel/milestones/scorecard      — All companies ranked by execution grade
GET /api/intel/milestones/{ticker}       — Milestone timeline for one ticker
GET /api/intel/attention/alerts          — Attention spike alerts
GET /api/intel/actor-network/db          — DB-backed graph (5M+ connections)
GET /api/intel/actor/{id}/profile        — Enriched actor profile with LLM analysis
```

## Database State

### Local Docker DB
- 1,419,628 [[Raw Series Table|raw_series]] rows
- 13,745 actors
- 11,101 actor_connections
- 559 MB
- pgvector: 320 chunks indexed (knowledge + Obsidian)

### Server Production DB
- 1,611,640 actors (1.6M from ICIJ + 443 seed + discoveries)
- 5,044,613 actor_connections
- 8,309 ICIJ fuzzy matches linking seed actors to offshore entities
- 30+ actors with LLM-enriched profiles
- 40 new actors auto-discovered via rabbit holes

## What's Running on Server

| Process | Screen/Service | Status |
|---------|---------------|--------|
| Gemma 4 31B-IT | Direct (PID) | 128K context, Q4 KV cache, port 8080, 27 tok/s |
| Actor researcher daemon | `screen -r actor_daemon` | 24/7, enriching actors, following rabbit holes |
| Hermes operator | Direct (PID) | Running, all new pullers registered |
| API (uvicorn) | systemd `grid-api` | Port 8000, serving new endpoints |
| Ollama | systemd | Port 11434, nomic-embed-text for RAG |
| Cloudflare tunnel | systemd | https://grid.stepdad.finance |

## API Keys Configured (.env on server)

**Working:** [[FRED]], Tiingo, QuiverQuant Pro, [[BLS]], [[EIA]], [[NOAA]], [[CoinGecko]], AlphaVantage Pro (150/min), TwelveData, Etherscan, Polygon.io, NASA Earthdata, WorldNews, Congress.gov, HF, OpenAI, OpenRouter, Groq, Gemini, Perplexity, Newsapi

**Dead/Limited:** FMP (403 all endpoints — key invalid), CryptoQuant (free tier = no API), Cloudflare Radar (now requires auth)

## Hermes Intelligence Cycle

### Every 4 hours
- [[Trust Scorer|Trust scorer]] cycle
- TimesFM signal forecasts
- Options recommendations
- [[Cross Reference|Cross-reference]] checks

### Every 6 hours
- Actor wealth migration tracking
- **Power mapping** ([[Institutional Flows|13F]] + ICIJ + QuiverQuant influence graph)

### Daily at 2:00 AM
- [[Source Audit|Source audit]], backtest scan, postmortems
- **Actor researcher** (20 actors enriched per cycle)
- **ICIJ cross-reference** (fuzzy matching)
- **Milestone scoring** (118+ companies)
- **Attention anomaly detection**
- **[[EDGAR]] transcript extraction** (with LLM milestones)
- RAG index refresh
- Hypothesis discovery

### Weekly (Sunday 3:00 AM)
- Full cross-reference with LLM narrative
- Lever report, trust report, actor report

## Key Findings

### Milestone Tracker (118 companies scored)
- **Top executors:** SNOW (A, 80.5%), DDOG (B+, 73%), PLTR (B+, 67%, 11-streak), NVDA (B+, 62.5%)
- **Worst:** COST (D-, 28.5%), HON (D-, 31.4%), WFC (D-, 34.3%), MMM (D-, 35%)

### ICIJ Fuzzy Matches (verified offshore connections)
- David E. Shaw, David Solomon (GS CEO), David Siegel (Two Sigma) — exact ICIJ matches
- Carl Icahn, Christine Lagarde ([[ECB]]), Nelson Peltz — high-confidence fuzzy
- All major SWFs (CIC, KIA, QIA, ADIA) — matched to offshore entities
- Michael Saylor — matched to "SAYLOR MICHAEL J."

### Actor Research (LLM-enriched profiles)
- Profiles include: title, key relationships, trading patterns, offshore connections, risk flags
- Every claim cites DB evidence, confidence labeled (confirmed/derived/inferred)
- Rabbit holes auto-spawn new actors (40 discovered so far)

## Schema Changes Applied (both local + server)
- `embeddings` table with pgvector HNSW index
- `icij_entities`, `icij_officers`, `icij_intermediaries`, `icij_addresses`, `icij_relationships` tables
- `icij_actor_matches` table
- `attention_anomaly` table
- `actor_connections` table (local only — already existed on server)
- `actors` table columns: `source`, `degree`, `icij_node_id` added locally
- `source_catalog` constraints expanded: latency_class (+INTRADAY, BATCH), revision_behavior (NEVER/RARE/FREQUENT)

## Known Issues
- FMP API key is dead — either get new key or upgrade to paid ($19/mo)
- CryptoQuant free tier has no API access — need paid plan ($29/mo)
- Cloudflare Radar now requires auth token
- [[GDELT]] BigQuery times out frequently
- [[FOIA]] State Dept API returning 404s (API may have changed)
- NYFed GDP nowcast Excel parser broken (format changed)
- Some seed actor connections were generic (industry_peer from mass-connect) — cleaned up 50, more may need review
- Gemma 4 thinking mode puts reasoning in `reasoning_content` not `content` — router uses llamacpp path which works fine
- The .env synced from local overwrote server DB creds — FIXED (server uses grid/gridmaster2026/griddb, local uses grid_user/changeme/grid)

## Next Steps (not done yet)
1. [[Actor Network|Actor network]] D3 visualization needs updating to use the new `/actor-network/db` endpoint (existing view reads from Python, not DB)
2. More VRAM next week — bump context or enable parallel slots
3. Build the cross-reference view: congress member trades stock of ICIJ-linked company = signal
4. Fine-tune Gemma on best actor research outputs (self-improving)
5. Run ICIJ full CSV download on server (the new icij_puller.py, not the old offshore_leaks.py)
6. Get working FMP key for full earnings/financial statement data

## Git Commits This Session
```
fd2f8a2  feat: massive data infrastructure buildout — 20+ pullers, 1.4M rows, actor network
518e0e0  feat: LLM milestone extraction, rabbit hole following, cross-reference wiring
39250e0  feat: API endpoints for milestone tracker, attention anomalies, DB-backed actor network
f439326  feat: milestone tracker + attention radar frontend views
2cc2c68  feat: wire 13F mining, actor research, milestones, attention into Hermes daily cycle
```

55+ files changed, ~16,000 lines added.
