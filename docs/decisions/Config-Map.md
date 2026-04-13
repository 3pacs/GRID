---
source: /Users/anikdang/grid_obsidian/Architecture/Config-Map.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
---
tags: [config, environment, settings, api-keys]
created: 2026-04-04
---

# Config Map — Settings & Environment

`config.py` (372 lines) uses pydantic-settings. All values from `.env` file.

Related: [[Database-Schema]], [[Frontend-Views]], [[All-Scripts]]

---

## Database
| Key | Default | Purpose |
|-----|---------|---------|
| `DB_HOST` | localhost | PostgreSQL host |
| `DB_PORT` | 5432 | PostgreSQL port |
| `DB_NAME` | grid | Database name |
| `DB_USER` | grid_user | Database user |
| `DB_PASSWORD` | "" | Database password (validated non-default in prod) |
| `ASTROGRID_DB_SCHEMA` | astrogrid | AstroGrid schema namespace |

## Core API Keys
| Key | Purpose | Used By |
|-----|---------|---------|
| `FRED_API_KEY` | Federal Reserve data | `ingestion/fred.py` |
| `BLS_API_KEY` | Bureau of Labor Statistics | `ingestion/bls.py` |
| `TIINGO_API_KEY` | Tiingo market data (PAID, primary) | `ingestion/tiingo_*.py` |
| `QUIVERQUANT_API_KEY` | QuiverQuant congressional/insider data (PAID) | `ingestion/altdata/congressional.py` |
| `COINGECKO_API_KEY` | CoinGecko crypto data (Pro) | `ingestion/coingecko.py` |
| `EIA_API_KEY` | Energy Information Administration | `ingestion/altdata/`, `scripts/parse_eia.py` |
| `NOAA_TOKEN` | NOAA weather/solar data | `ingestion/celestial/solar.py` |

## International Data Keys
| Key | Purpose |
|-----|---------|
| `KOSIS_API_KEY` | Korea Statistical Info Service |
| `COMTRADE_API_KEY` | UN Comtrade trade data |
| `JQUANTS_EMAIL` / `JQUANTS_PASSWORD` | J-Quants (Japan) |
| `USDA_NASS_API_KEY` | USDA agriculture data |
| `GDELT_API_KEY` | GDELT geopolitical events |
| `WORLDNEWS_API_KEY` | World news headlines |

## Backup/Fallback Data Keys
| Key | Purpose |
|-----|---------|
| `ALPHAVANTAGE_API_KEY` | Free: 25 req/day |
| `TWELVEDATA_API_KEY` | Free: 800 req/day |

## LLM Infrastructure (Multi-Provider)

### Local LLM — llama.cpp (Primary)
| Key | Default | Purpose |
|-----|---------|---------|
| `LLAMACPP_BASE_URL` | http://localhost:8080 | Nemotron-Super-49B v1.5 Q5_K_M, GPU+CPU split |
| `LLAMACPP_ENABLED` | true | |
| `LLAMACPP_TIMEOUT_SECONDS` | 300 | |
| `LLAMACPP_CHAT_MODEL` | nvidia_Llama-3_3-Nemotron-Super-49B-v1_5-Q5_K_M | |
| `LLAMACPP_ORACLE_BASE_URL` | http://localhost:8081 | 120B model (DISABLED, too slow) |
| `LLAMACPP_ORACLE_ENABLED` | false | |

### Local LLM — Gemma 3
| Key | Default | Purpose |
|-----|---------|---------|
| `GEMMA_BASE_URL` | http://localhost:8081 | 27B QAT on RTX 3090, 128K context |
| `GEMMA_ENABLED` | true | |
| `GEMMA_MICRO_CLASSIFIER_URL` | http://localhost:8082 | 270M fine-tuned classifier |
| `GEMMA_MICRO_NARRATOR_URL` | http://localhost:8083 | 270M fine-tuned narrator |
| `GEMMA_MICRO_EXTRACTOR_URL` | http://localhost:8084 | 270M fine-tuned extractor |

### Local LLM — Ollama
| Key | Default | Purpose |
|-----|---------|---------|
| `OLLAMA_BASE_URL` | http://localhost:11434 | Qwen 2.5 7B |
| `OLLAMA_ENABLED` | true | |
| `OLLAMA_CHAT_MODEL` | qwen2.5:7b | |
| `OLLAMA_EMBED_MODEL` | nomic-embed-text | |

### Cloud LLM — HuggingFace
| Key | Default | Purpose |
|-----|---------|---------|
| `HF_API_KEY` | "" | HuggingFace Inference API |
| `HF_BASE_URL` | https://router.huggingface.co/together/v1 | Via Together |
| `HF_CHAT_MODEL` | meta-llama/Llama-3.3-70B-Instruct-Turbo | |

### Cloud LLM — Anthropic
| Key | Default | Purpose |
|-----|---------|---------|
| `ANTHROPIC_API_KEY` | "" | Claude API |
| `ANTHROPIC_CHAT_MODEL` | claude-sonnet-4-6 | |

### Cloud LLM — OpenAI
| Key | Default | Purpose |
|-----|---------|---------|
| `OPENAI_API_KEY` | "" | Fallback cloud LLM |
| `OPENAI_CHAT_MODEL` | gpt-4o | |
| `OPENAI_EMBED_MODEL` | text-embedding-3-small | |

### Cloud LLM — OpenRouter (Primary Cloud)
| Key | Default | Purpose |
|-----|---------|---------|
| `OPENROUTER_API_KEY` | "" | Claude Sonnet via OpenAI-compatible API |
| `OPENROUTER_CHAT_MODEL` | anthropic/claude-sonnet-4 | |

### LLM Router
| Key | Default | Purpose |
|-----|---------|---------|
| `LLM_ROUTER_ENABLED` | true | 3-tier routing enabled |
| `LLM_LOCAL_PROVIDER` | llamacpp | LOCAL tier |
| `LLM_REASON_PROVIDER` | llamacpp | REASON tier |
| `LLM_ORACLE_PROVIDER` | llamacpp | ORACLE tier (OpenRouter fallback) |

## ML Models
| Key | Default | Purpose |
|-----|---------|---------|
| `TIMESFM_ENABLED` | true | Google TimesFM 2.0 |
| `TIMESFM_MODEL_NAME` | google/timesfm-2.0-200m-pytorch | |
| `TIMESFM_BACKEND` | gpu | gpu/cpu/tpu |
| `TIMESFM_CONTEXT_LENGTH` | 512 | Max historical steps |
| `TIMESFM_HORIZON` | 7 | Default forecast days |
| `AUTOBNN_ENABLED` | true | Google AutoBNN |
| `AUTOBNN_NUM_SAMPLES` | 200 | |

## Auth & Security
| Key | Purpose |
|-----|---------|
| `GRID_MASTER_PASSWORD_HASH` | Admin password (bcrypt) |
| `GRID_JWT_SECRET` | JWT signing secret |
| `GRID_JWT_EXPIRE_HOURS` | 168 (7 days) |
| `GRID_ALLOWED_ORIGINS` | CORS origins (empty = env-aware defaults) |

## Trading & Prediction Markets
| Key | Purpose |
|-----|---------|
| `POLYMARKET_API_KEY` / `POLYMARKET_PRIVATE_KEY` | Polymarket integration |
| `KALSHI_EMAIL` / `KALSHI_PASSWORD` | Kalshi prediction market |
| `HYPERLIQUID_PRIVATE_KEY` | Hyperliquid perp trading |
| `HYPERLIQUID_TESTNET` | true (safety default) |
| `HYPERLIQUID_MAX_POSITION_USD` | 100.0 |
| `HYPERLIQUID_MAX_DRAWDOWN_PCT` | 0.20 |

## TradingAgents (Multi-Agent System)
| Key | Default | Purpose |
|-----|---------|---------|
| `AGENTS_ENABLED` | false | Multi-agent deliberation |
| `AGENTS_LLM_PROVIDER` | openai | Agent LLM backend |
| `AGENTS_DEBATE_ROUNDS` | 1 | Bull/bear debate rounds |
| `AGENTS_DEFAULT_TICKER` | SPY | Default analysis ticker |
| `AGENTS_SCHEDULE_CRON` | 0 17 * * 1-5 | Weekdays 5 PM |
| `AGENTS_PERSONA` | balanced | Agent personality |
| `AGENTS_BACKTEST_MAX_DAYS` | 365 | |

## Protocol & Monetization
| Key | Default | Purpose |
|-----|---------|---------|
| `A2A_ENABLED` | true | Agent-to-agent protocol |
| `A2A_BASE_URL` | https://grid.stepdad.finance | |
| `X402_ENABLED` | false | Micropayments (USDC on Base L2) |
| `X402_PRICE_FORECAST` | 0.01 | USD per forecast call |
| `X402_PRICE_PREDICTION` | 0.02 | USD per oracle prediction |

## Alerts & Notifications
| Key | Purpose |
|-----|---------|
| `VAPID_PRIVATE_KEY` / `VAPID_PUBLIC_KEY` | Web Push VAPID keys |
| `ALERT_EMAIL_ENABLED` | Email alerts toggle |
| `ALERT_EMAIL_TO` | stepdadfinance@gmail.com |
| `ALERT_SMTP_HOST` | localhost:25 |

## Social Scanners
| Key | Purpose |
|-----|---------|
| `TELEGRAM_API_ID` / `TELEGRAM_API_HASH` / `TELEGRAM_PHONE` | Telegram monitoring |
| `DISCORD_USER_TOKEN` | Discord monitoring (user token, not bot) |

## Schedules (Cron Format)
| Key | Default | Purpose |
|-----|---------|---------|
| `PULL_SCHEDULE_FRED` | 0 18 * * 1-5 | FRED data weekdays 6 PM |
| `PULL_SCHEDULE_YFINANCE` | 30 18 * * 1-5 | yfinance weekdays 6:30 PM |
| `PULL_SCHEDULE_BLS` | 0 9 * * * | BLS daily 9 AM |
| `BRIEFING_CRON_DAILY` | 0 6 * * 1-5 | Daily briefing 6 AM |
| `BRIEFING_CRON_WEEKLY` | 0 7 * * 1 | Weekly briefing Monday 7 AM |
| `AUTORESEARCH_CRON` | 0 2 * * 1-5 | Autoresearch weekdays 2 AM |

## .env Keys (Production)
56 keys configured in `.env` (actual values redacted). Includes additional keys not in config.py defaults:
`GEMINI_API_KEY`, `GRID_INTEL_KEY`, `GROQ_API_KEY`, `HERMES_EMAIL_ALLOWLIST`, `MASSIVE_API_KEY`, `NEWSAPI_KEY`, `PERPLEXITY_API_KEY`, `LLAMACPP_CTX`, `LLAMACPP_MODEL`, `LLAMACPP_PARALLEL`, `LLAMACPP_SLOT_SAVE_PATH`, `TRADINGVIEW_WEBHOOK_SECRET`
