# Integrations

## Database

- **PostgreSQL 15 + TimescaleDB** via Docker (`grid/docker-compose.yml`)
- **Connection:** `postgresql://{user}:{password}@{host}:{port}/{dbname}` constructed in `grid/config.py:114-119`
- **Defaults:** `localhost:5432/grid`, user `grid_user`, password `changeme`
- **Driver:** psycopg2-binary (raw) + SQLAlchemy 2.0 (engine/pool)
- **Pool:** size=5, max_overflow=10, timeout=30s, pre_ping=True (`grid/db.py:44-49`)
- **Schema:** `grid/schema.sql` applied via `grid/db.py:apply_schema()`
- **PIT queries:** `grid/store/pit.py` — uses PostgreSQL-specific `DISTINCT ON` (incompatible with SQLite/MySQL)

## External APIs

### US Economic Data

| Source | Module | Auth Method | Key Config |
|--------|--------|-------------|------------|
| **FRED** (Federal Reserve Economic Data) | `grid/ingestion/fred.py` | API key via `fedfred` library | `FRED_API_KEY` — validated at startup, required in non-dev |
| **BLS** (Bureau of Labor Statistics) | `grid/ingestion/bls.py` | Optional API key (higher rate limits with key) | Registration key in POST payload; no env var — passed at init |
| **SEC EDGAR** (13F, Form 4, 8-K) | `grid/ingestion/edgar.py` | User-agent identity string (SEC compliance) | `set_identity("GRID Trading System grid@localhost")` — no API key |
| **SEC Velocity** (8-K filing velocity) | `grid/ingestion/sec_velocity.py` | User-agent identity string | Same as EDGAR — `set_identity()` |
| **Yahoo Finance** | `grid/ingestion/yfinance_pull.py` | None (public API via `yfinance` library) | No auth needed |
| **EIA** (Energy Information Admin) | Referenced in `grid/config.py` | API key | `EIA_API_KEY` |

### International Macro Data

| Source | Module | Auth Method | Key Config |
|--------|--------|-------------|------------|
| **ECB SDW** (Euro Central Bank) | `grid/ingestion/international/ecb.py` | None (public SDMX REST API) | No auth — `https://sdw-wsrest.ecb.europa.eu/service/data` |
| **IMF IFS/WEO** | `grid/ingestion/international/imf.py` | None (via `imfdatapy` library) | No auth |
| **Eurostat** | `grid/ingestion/international/eurostat.py` | None (via `eurostat` library) | No auth |
| **KOSIS** (Korea Statistics) | `grid/ingestion/international/kosis.py` | API key in query params | `KOSIS_API_KEY` |
| **J-Quants** (Japan Exchange) | `grid/ingestion/international/jquants.py` | Email/password -> refresh token -> Bearer token | `JQUANTS_EMAIL`, `JQUANTS_PASSWORD` |
| **AKShare** (China macro) | `grid/ingestion/international/akshare_macro.py` | None (via `akshare` library) | No auth |
| **BIS** (Bank for Intl Settlements) | `grid/ingestion/international/bis.py` | None (public data) | No auth |
| **OECD** | `grid/ingestion/international/oecd.py` | None (public SDMX API) | No auth |
| **MAS** (Monetary Authority Singapore) | `grid/ingestion/international/mas.py` | None (public API) | No auth |
| **BCB** (Brazil Central Bank) | `grid/ingestion/international/bcb.py` | None (public API) | No auth |
| **RBI** (Reserve Bank India) | `grid/ingestion/international/rbi.py` | None (public data) | No auth |
| **ABS** (Australian Bureau of Statistics) | `grid/ingestion/international/abs_au.py` | None (public API) | No auth |
| **EDINET** (Japan financial disclosures) | `grid/ingestion/international/edinet.py` | None (public API) | No auth |
| **DBnomics** | `grid/ingestion/physical/dbnomics.py` | None (via `dbnomics` library) | No auth |

### Trade Data

| Source | Module | Auth Method | Key Config |
|--------|--------|-------------|------------|
| **UN Comtrade** | `grid/ingestion/trade/comtrade.py` | Optional API key (`subscription-key` param) | `COMTRADE_API_KEY` |
| **WIOD** (World Input-Output) | `grid/ingestion/trade/wiod.py` | None (public datasets) | No auth |
| **Atlas ECI** (Economic Complexity) | `grid/ingestion/trade/atlas_eci.py` | None (public API) | No auth |
| **CEPII** (trade gravity data) | `grid/ingestion/trade/cepii.py` | None (public data) | No auth |

### Physical Economy / Alt Data

| Source | Module | Auth Method | Key Config |
|--------|--------|-------------|------------|
| **USDA NASS** (agriculture) | `grid/ingestion/physical/usda_nass.py` | API key in query params | `USDA_NASS_API_KEY` |
| **USPTO PatentsView** | `grid/ingestion/physical/patents.py` | None (public API via `patent-client`) | No auth |
| **NASA VIIRS** (nighttime lights) | `grid/ingestion/physical/viirs.py` | None / AWS S3 (via `boto3`) | No specific key config — uses default AWS credentials if needed |
| **EU KLEMS** (productivity) | `grid/ingestion/physical/euklems.py` | None (public data) | No auth |
| **OFR** (Office of Financial Research) | `grid/ingestion/physical/ofr.py` | None (public API) | No auth |
| **NOAA AIS** (vessel traffic) | `grid/ingestion/altdata/noaa_ais.py` | Token | `NOAA_TOKEN` |
| **GDELT** (news events) | `grid/ingestion/altdata/gdelt.py` | API key | `GDELT_API_KEY` |
| **Opportunity Insights** | `grid/ingestion/altdata/opportunity.py` | None (public GitHub CSV data) | No auth |

### Crypto Data

| Source | Module | Auth Method | Key Config |
|--------|--------|-------------|------------|
| **DexScreener** | `grid/ingestion/dexscreener.py` | None (public API, 300 req/min) | No auth |
| **Pump.fun** | `grid/ingestion/pumpfun.py` | None (reverse-engineered frontend API) | No auth — endpoints may break |

## LLM Services

### llama.cpp (Primary — replaces Ollama)
- **Module:** `grid/llamacpp/client.py`
- **Connection:** OpenAI-compatible REST API at `LLAMACPP_BASE_URL` (default `http://localhost:8080`)
- **Endpoints:** `/v1/chat/completions`, `/v1/embeddings`, `/v1/models`
- **Config:** `LLAMACPP_ENABLED` (default True), `LLAMACPP_CHAT_MODEL` (default "hermes"), `LLAMACPP_EMBED_MODEL`, `LLAMACPP_TIMEOUT_SECONDS` (120s)
- **Pattern:** Singleton client, all methods return `None` on failure (graceful degradation)

### Ollama (Deprecated)
- **Module:** `grid/ollama/client.py`
- **Connection:** Native Ollama API at `OLLAMA_BASE_URL` (default `http://localhost:11434`)
- **Endpoints:** `/api/chat`, `/api/generate`, `/api/embeddings`, `/api/tags`, `/api/pull`
- **Config:** `OLLAMA_ENABLED` (default False), `OLLAMA_CHAT_MODEL` (default "llama3.1:8b"), `OLLAMA_EMBED_MODEL` (default "nomic-embed-text")
- **Note:** `grid/ollama/client.py:get_client()` dispatches to LlamaCppClient when `LLAMACPP_ENABLED=True`
- **Knowledge system:** Markdown docs loaded from `grid/ollama/knowledge/` directory, injected into system prompts

### Hyperspace (P2P Local Inference)
- **Module:** `grid/hyperspace/client.py`
- **Connection:** OpenAI-compatible API at `HYPERSPACE_BASE_URL` (default `http://localhost:8080/v1`)
- **Endpoints:** `/chat/completions`, `/embeddings`, `/models`
- **Config:** `HYPERSPACE_ENABLED` (default True), `HYPERSPACE_CHAT_MODEL` (default "auto"), `HYPERSPACE_EMBED_MODEL` (default "all-MiniLM-L6-v2")
- **Privacy:** Explicit privacy boundary — no GRID signal logic, features, or cluster data sent to network
- **Additional modules:** `grid/hyperspace/embeddings.py`, `grid/hyperspace/monitor.py`, `grid/hyperspace/reasoner.py`, `grid/hyperspace/research_agent.py`

### TradingAgents (Multi-Agent Framework)
- **Module:** `grid/agents/` directory
- **Config:** `grid/agents/config.py` — builds provider config from GRID settings
- **Providers:** llamacpp (default), hyperspace, openai, anthropic
- **OpenAI:** Uses `AGENTS_OPENAI_API_KEY`, models `gpt-4o` / `gpt-4o-mini`
- **Anthropic:** Uses `AGENTS_ANTHROPIC_API_KEY`, models `claude-sonnet-4-6` / `claude-haiku-4-5-20251001`
- **Local:** llama.cpp and Hyperspace use OpenAI-compatible API with dummy key `"not-needed"`
- **Framework:** `tradingagents` + `langgraph` for multi-agent deliberation with configurable debate rounds
- **Scheduler:** `grid/agents/scheduler.py` — optional cron-based automated runs
- **Runner:** `grid/agents/runner.py` — fetches GRID regime context, runs agents, logs to decision journal

## Authentication

- **Method:** Single-operator JWT authentication (no user management)
- **Module:** `grid/api/auth.py`
- **Password:** bcrypt-hashed master password stored in `GRID_MASTER_PASSWORD_HASH` env var
- **Token:** HS256 JWT with `sub: "grid-operator"`, configurable expiry (`GRID_JWT_EXPIRE_HOURS`, default 168h/7 days)
- **Endpoints:**
  - `POST /api/v1/auth/login` — password -> JWT
  - `POST /api/v1/auth/logout` — no-op (token expiry handles invalidation)
  - `GET /api/v1/auth/verify` — check token validity
- **Token delivery:** `Authorization: Bearer <token>` header or `?token=` query param
- **Rate limiting:** In-memory dict, 5 attempts per 60s per IP — resets on restart, no multi-instance support (`grid/api/auth.py:118-128`)
- **WebSocket auth:** Token via query parameter `?token=` — leaks to logs/proxies (`grid/api/main.py:202-209`)
- **Dependencies:** `python-jose` (JWT), `passlib` (bcrypt)

## Internal Services

### Ingestion Schedulers
- **v1 (authoritative):** `grid/ingestion/scheduler.py` — FRED, yfinance, BLS, EDGAR on cron schedules via `schedule` library
- **v2:** `grid/ingestion/scheduler_v2.py` — international, trade, physical, altdata sources
- **Both run as daemon threads** started on FastAPI startup (`grid/api/main.py:178-197`)

### Domain Modules
- **PIT Store:** `grid/store/pit.py` — point-in-time query engine (critical path)
- **Conflict Resolution:** `grid/normalization/resolver.py` — multi-source conflict resolution
- **Entity Mapping:** `grid/normalization/entity_map.py` — entity disambiguation across sources
- **Feature Engineering:** `grid/features/lab.py` — z-score, slopes, ratios
- **Regime Discovery:** `grid/discovery/clustering.py` — unsupervised regime clustering
- **Orthogonality Audit:** `grid/discovery/orthogonality.py` — feature independence checks
- **Validation Gates:** `grid/validation/gates.py` — walk-forward backtesting promotion gates
- **Model Governance:** `grid/governance/registry.py` — CANDIDATE -> SHADOW -> STAGING -> PRODUCTION lifecycle
- **Live Inference:** `grid/inference/live.py` — model scoring engine
- **Decision Journal:** `grid/journal/log.py` — immutable recommendation log with full provenance
