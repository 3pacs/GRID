---
source: /Users/anikdang/grid_obsidian/Architecture/API-Layer.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
# API Layer

[[FastAPI]] application serving at `/api/v1/*` with WebSocket at `/ws`. Entry point: `api/main.py`.

## Architecture

- **Auth**: JWT-based (`api/auth.py`), role-based access via `require_role`
- **Lazy loading**: Routers loaded via `_load_router()` — optional modules don't block boot
- **Middleware stack**: SecurityHeaders → RateLimit → X402Payment → CORS
- **WebSocket**: Real-time updates broadcast loop for agent progress, market data

## Router Registry (40 routers)

All routers registered in `api/main.py` via a declarative list. See [[API-Endpoints-Master]] for complete endpoint listing.

### Core System
| Router | Prefix | Endpoints | Description |
|--------|--------|-----------|-------------|
| `system` | `/api/v1/system` | 20 | Health, status, freshness, logs, alerts, settings, services |
| `config` | `/api/v1/config` | 6 | Feature/source configuration CRUD |
| `search` | `/api/v1/search` | 1 | Global search |
| `notifications` | `/api/v1/notifications` | 6 | Push notification subscriptions |
| `a2a` | `/api/v1/a2a` | 5 | Agent-to-Agent protocol (Google A2A) |

### Market Intelligence
| Router | Prefix | Endpoints | Description |
|--------|--------|-----------|-------------|
| `intel` | `/api/v1/intel` | 11 | Search, entity lookup, briefings, predictions |
| `intelligence_*` | `/api/v1/intelligence` | ~60 | Companies, actors, forensics, gov, news, regime, risk, thesis, deep-dive |
| `intel_cross_reference` | `/api/v1/intel` | 4 | Cross-reference lie detector |
| `intel_source_audit` | `/api/v1/intel` | 5 | Source reliability audit |

### Data & Signals
| Router | Prefix | Endpoints | Description |
|--------|--------|-----------|-------------|
| `signals` | `/api/v1/signals` | 5 | Signal snapshots, crucix bridge, timeseries |
| `signal_registry` | `/api/v1/signals/registry` | 7 | Signal taxonomy + oracle factory |
| `regime` | `/api/v1/regime` | 8 | Regime detection, weights, simulation |
| `flows` | `/api/v1/flows` | 28 | Capital flows, money map, CDS, briefings |
| `earnings` | `/api/v1/earnings` | 7 | Earnings calendar, surprises, predictions |

### Trading & Options
| Router | Prefix | Endpoints | Description |
|--------|--------|-----------|-------------|
| `trading` | `/api/v1/trading` | 29 | Paper trading, Hyperliquid, Polymarket, Kalshi, wallets |
| `options` | `/api/v1/options` | 7 | Options recommendations, signals, 100x scanner |
| `derivatives` | `/api/v1/derivatives` | 15 | GEX, vanna-charm, vol surface, skew, term structure |
| `strategy` | `/api/v1/strategy` | 3 | Active strategies, regime-based assignment |

### Analysis & Discovery
| Router | Prefix | Endpoints | Description |
|--------|--------|-----------|-------------|
| `discovery` | `/api/v1/discovery` | 13 | Orthogonality, clustering, hypothesis testing |
| `associations` | `/api/v1/associations` | 5 | Correlation matrix, lag analysis, anomalies |
| `backtest` | `/api/v1/backtest` | 9 | Backtest engine, paper trades, charts |
| `physics` | `/api/v1/physics` | 9 | OU processes, Hurst, energy, momentum |

### LLM & Knowledge
| Router | Prefix | Endpoints | Description |
|--------|--------|-----------|-------------|
| `chat` | `/api/v1/chat` | 1 | LLM chat interface |
| `ollama` | `/api/v1/ollama` | 10 | Local LLM briefings, reasoning |
| `knowledge` | `/api/v1/knowledge` | 4 | Knowledge base CRUD |
| `briefing` | `/api/v1/briefing` | 6 | Sentiment briefings with history |

### AstroGrid
| Router | Prefix | Endpoints | Description |
|--------|--------|-----------|-------------|
| `astrogrid_core` | `/api/v1/astrogrid` | 5 | Overview, snapshot, scorecard |
| `astrogrid_celestial` | `/api/v1/astrogrid/celestial` | 10 | Ephemeris, correlations, retrograde |
| `astrogrid_predictions` | `/api/v1/astrogrid/predictions` | 16 | Predictions, scoring, learning loop |

### Visualization & UI
| Router | Prefix | Endpoints | Description |
|--------|--------|-----------|-------------|
| `viz` | `/api/v1/viz` | 9 | Chart specs (capital flows, regime, energy particle) |
| `watchlist_*` | `/api/v1/watchlist` | 12 | Watchlist CRUD, prices, enrichment |
| `journal` | `/api/v1/journal` | 5 | Decision journal entries |

### Infrastructure
| Router | Prefix | Endpoints | Description |
|--------|--------|-----------|-------------|
| `models` | `/api/v1/models` | 7 | ML model lifecycle management |
| `model_comparison` | `/api/v1/models` | 3 | Shadow vs production comparison |
| `workflows` | `/api/v1/workflows` | 8 | Declarative workflow system |
| `agents` | `/api/v1/agents` | 10 | LLM agent orchestration |
| `forecasts` | `/api/v1/forecasts` | 3 | TimesFM/AutoBNN forecast generation |
| `mcp_export` | `/api/v1/mcp` | 8 | MCP protocol export |
| `oracle` | `/api/v1/oracle` | 4 | Oracle predictions + scoreboard |
| `tradingview` | `/api/v1/tradingview` | 2 | TradingView webhook + signals |
| `snapshots` | `/api/v1/snapshots` | 5 | Historical analytical snapshots |

## Schemas (`api/schemas/`)

Pydantic models for request/response validation:
- `auth.py` — Login, token, user models
- `journal.py` — Journal entry models
- `models.py` — ML [[Model Governance|model lifecycle]] models
- `regime.py` — Regime state/history models
- `system.py` — Health, status, freshness models
- `watchlist.py` — Watchlist item models

## Middleware

- **SecurityHeaders**: X-Content-Type-Options, X-Frame-Options, CSP, HSTS
- **RateLimit**: 30 req/min per IP on expensive endpoints (deep-dive, network, briefing, globe)
- **X402Payment**: Agent micropayments (disabled by default, `X402_ENABLED=true`)
- **CORS**: Explicit allowlist (dev: localhost:5173/8000, prod: grid.stepdad.finance)

## Dependencies

- [[Database-Schema]] — all routers read from [[PostgreSQL]]
- [[Intelligence-Layer]] — intelligence routers delegate to intelligence modules
- [[Trading-Layer]] — trading router delegates to trading modules
- [[Orchestration-Layer]] — LLM task queue router, compute router

## Router Notes (code-verified)

- [[API-Chat]]
- [[API-Intelligence]]
- [[API-Market-Data]]
- [[API-System]]
- [[API-Trading]]
- [[API-Analytics]]
