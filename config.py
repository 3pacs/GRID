"""
GRID configuration module.

Loads all settings from environment variables with sensible defaults.
Exposes a single ``Settings`` object that is imported everywhere in the project.
Raises a clear error at import time if FRED_API_KEY is missing in
non-development environments.
"""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
from loguru import logger as log
from pydantic import ConfigDict, field_validator
from pydantic_settings import BaseSettings

# Load .env from the project root (same directory as this file)
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))


class Settings(BaseSettings):
    """Central configuration object for the GRID system.

    All values are read from environment variables. A ``.env`` file placed next
    to ``config.py`` is loaded automatically via *python-dotenv*.

    Attributes:
        DB_HOST: PostgreSQL hostname.
        DB_PORT: PostgreSQL port.
        DB_NAME: Database name.
        DB_USER: Database user.
        DB_PASSWORD: Database password.
        DB_URL: Fully-formed database URL (constructed automatically).
        FRED_API_KEY: API key for the FRED data service.
        LOG_LEVEL: Logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        ENVIRONMENT: Runtime environment (development, staging, production).
        PULL_SCHEDULE_FRED: Cron expression for FRED pull schedule.
        PULL_SCHEDULE_YFINANCE: Cron expression for yfinance pull schedule.
        PULL_SCHEDULE_BLS: Cron expression for BLS pull schedule.
    """

    # Database
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "grid"
    DB_USER: str = "grid_user"
    DB_PASSWORD: str = ""
    ASTROGRID_DB_SCHEMA: str = "astrogrid"

    # API Keys — core
    FRED_API_KEY: str = ""
    BLS_API_KEY: str = ""
    TIINGO_API_KEY: str = ""

    # TradingView webhook
    TRADINGVIEW_WEBHOOK_SECRET: str = ""

    # API Keys — international / trade / physical
    KOSIS_API_KEY: str = ""
    COMTRADE_API_KEY: str = ""
    JQUANTS_EMAIL: str = ""
    JQUANTS_PASSWORD: str = ""
    USDA_NASS_API_KEY: str = ""
    NOAA_TOKEN: str = ""
    EIA_API_KEY: str = ""
    GDELT_API_KEY: str = ""
    WORLDNEWS_API_KEY: str = ""
    OPENSECRETS_API_KEY: str = ""

    # Backup data source API keys
    COINGECKO_API_KEY: str = ""          # Free: 30 req/min, Pro: unlimited
    ALPHAVANTAGE_API_KEY: str = ""       # Free: 25 req/day
    TWELVEDATA_API_KEY: str = ""         # Free: 800 req/day

    # Financial Modeling Prep (earnings, financials, transcripts)
    FMP_API_KEY: str = ""                # Free: 250 req/day

    # Etherscan (Ethereum on-chain data)
    ETHERSCAN_API_KEY: str = ""          # Free: 5 req/sec

    # CryptoQuant (exchange flows, miner flows, on-chain metrics)
    CRYPTOQUANT_API_KEY: str = ""        # Free tier available

    # Dune Analytics (SQL over decoded Ethereum/Solana/Base data)
    DUNE_API_KEY: str = ""               # Free tier available
    DUNE_QUERY_SMART_MONEY: int = 0      # saved query: top wallets by realized PnL
    DUNE_QUERY_CEX_FLOW: int = 0         # saved query: net CEX inflows/outflows
    DUNE_QUERY_NARRATIVE_HEAT: int = 0   # saved query: w/w new-holder growth

    # Polygon.io (stocks, options with Greeks, crypto, forex)
    POLYGON_API_KEY: str = ""            # Free: 5 req/min, Paid: unlimited

    # NASA Earthdata (FIRMS fire data, VIIRS, satellite imagery)
    NASA_EARTHDATA_TOKEN: str = ""       # JWT token from earthdata.nasa.gov

    # Reference hallucination guard (arxiv 2604.03173)
    REF_CHECK_ENABLED: bool = True
    REF_CHECK_TIMEOUT_S: float = 5.0
    REF_CHECK_MAX_CONCURRENT: int = 10
    REF_CHECK_RATE_LIMIT: float = 10.0
    REF_CHECK_CACHE_TTL_S: int = 3600
    REF_CHECK_WAYBACK_ENABLED: bool = True
    REF_CHECK_REJECT_THRESHOLD: float = 0.4

    # Logging / Environment
    LOG_LEVEL: str = "INFO"
    ENVIRONMENT: str = "development"

    # Pull schedules (cron format)
    PULL_SCHEDULE_FRED: str = "0 18 * * 1-5"
    PULL_SCHEDULE_YFINANCE: str = "30 18 * * 1-5"
    PULL_SCHEDULE_BLS: str = "0 9 * * *"

    # Hyperspace integration
    HYPERSPACE_BASE_URL: str = "http://localhost:8080/v1"
    HYPERSPACE_ENABLED: bool = True
    HYPERSPACE_TIMEOUT_SECONDS: int = 30
    HYPERSPACE_EMBED_MODEL: str = "all-MiniLM-L6-v2"
    HYPERSPACE_CHAT_MODEL: str = "auto"

    # HuggingFace Inference API (primary cloud LLM)
    HF_API_KEY: str = ""
    HF_BASE_URL: str = "https://router.huggingface.co/together/v1"
    HF_TIMEOUT_SECONDS: int = 120
    HF_CHAT_MODEL: str = "meta-llama/Llama-3.3-70B-Instruct-Turbo"
    HF_DEEP_MODEL: str = "meta-llama/Llama-3.3-70B-Instruct-Turbo"

    # Anthropic / Claude (cloud LLM)
    ANTHROPIC_API_KEY: str = ""
    ANTHROPIC_BASE_URL: str = "https://api.anthropic.com"
    ANTHROPIC_TIMEOUT_SECONDS: int = 120
    ANTHROPIC_CHAT_MODEL: str = "claude-sonnet-4-6"
    ANTHROPIC_DEEP_MODEL: str = "claude-sonnet-4-6"

    # OpenAI integration (fallback cloud LLM)
    OPENAI_API_KEY: str = ""
    OPENAI_BASE_URL: str = "https://api.openai.com/v1"
    OPENAI_TIMEOUT_SECONDS: int = 120
    OPENAI_CHAT_MODEL: str = "gpt-4o"

    # OpenRouter (primary cloud LLM — Claude Sonnet via OpenAI-compatible API)
    OPENROUTER_API_KEY: str = ""
    OPENROUTER_BASE_URL: str = "https://openrouter.ai/api/v1"
    OPENROUTER_TIMEOUT_SECONDS: int = 120
    OPENROUTER_CHAT_MODEL: str = "anthropic/claude-sonnet-4"
    OPENAI_EMBED_MODEL: str = "text-embedding-3-small"

    # Ollama (local lightweight LLM — Qwen 7B)
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_ENABLED: bool = True
    OLLAMA_TIMEOUT_SECONDS: int = 120
    OLLAMA_CHAT_MODEL: str = "qwen3:8b"
    OLLAMA_EMBED_MODEL: str = "nomic-embed-text"

    # Remote Ollama nodes — added 2026-05-09. Each has its own URL +
    # default model so the LLM router can fan out across the cluster.
    # Brought online by the operator: panda (2× P100 16GB Pascal hosting
    # qwen2.5:32b) and ocr-node (2× 8GB Ampere hosting smaller models +
    # vision). Each becomes a provider name `ollama_panda` / `ollama_ocr`
    # in llm/router.py.
    OLLAMA_PANDA_BASE_URL: str = "http://panda:11434"
    OLLAMA_PANDA_ENABLED: bool = True
    OLLAMA_PANDA_TIMEOUT_SECONDS: int = 240
    # Pascal P100 (sm_60) cannot use bf16 native, mxfp8 (sm_89+), or
    # nvfp4 (sm_120). Q4_K_M is the highest-quality quant available for
    # qwen3.6 on Pascal — the coding variant only ships as bf16/mxfp8/
    # nvfp4 so we use the general 27b model for both general + coding.
    OLLAMA_PANDA_CHAT_MODEL: str = "qwen3.6:27b-q4_K_M"

    OLLAMA_OCR_BASE_URL: str = "http://ocr-node:11434"
    OLLAMA_OCR_ENABLED: bool = True
    OLLAMA_OCR_TIMEOUT_SECONDS: int = 120
    # ocr-node has 2× 8GB Ampere — gemma3:12b-it-q4_K_M (~7GB) replaces
    # gemma2:9b after the 2026-05-09 cluster refresh. Vision models on
    # ocr-node (qwen2.5vl:7b, minicpm-v:8b) are not in the standard chain.
    OLLAMA_OCR_CHAT_MODEL: str = "gemma3:12b-it-q4_K_M"

    # koala — 2× GTX TITAN X Maxwell 12 GB. Card 0 hosts Ollama
    # (gemma2:9b chat + nomic-embed-text embeddings); card 1 is reserved
    # for Whisper transcription + Kokoro TTS. Maxwell sm_52 cannot run
    # flash-attn or NVFP4, but excels at small dense inference and
    # embedding generation.
    OLLAMA_KOALA_BASE_URL: str = "http://koala:11434"
    OLLAMA_KOALA_ENABLED: bool = True
    OLLAMA_KOALA_TIMEOUT_SECONDS: int = 120
    OLLAMA_KOALA_CHAT_MODEL: str = "gemma3:12b-it-q4_K_M"
    OLLAMA_KOALA_EMBED_MODEL: str = "nomic-embed-text"

    # z400 — DECOMMISSIONED as an LLM host (2026-05-26). Lost its GPU and is
    # now the OCMRI app docker host (ocmri-frontend/backend/postgres); Ollama
    # was removed entirely, so this endpoint is dead. Kept (disabled) for
    # history; re-enable only if z400 ever serves inference again.
    OLLAMA_Z400_BASE_URL: str = "http://z400:11434"
    OLLAMA_Z400_ENABLED: bool = False
    OLLAMA_Z400_TIMEOUT_SECONDS: int = 120
    OLLAMA_Z400_CHAT_MODEL: str = "qwen2.5:7b-instruct-q4_K_M"
    OLLAMA_Z400_EMBED_MODEL: str = "nomic-embed-text"

    # koala card 1 — Kokoro TTS server (CPU inference, FastAPI on :8091).
    # OpenAI-compatible /v1/audio/speech endpoint. 54 voices, 24kHz mono WAV.
    # Useful as a local replacement for OpenAI TTS in audio_briefing.py.
    KOKORO_TTS_BASE_URL: str = "http://koala:8091"
    KOKORO_TTS_ENABLED: bool = True
    KOKORO_TTS_VOICE: str = "af_sarah"

    # koala card 1 — whisper.cpp Vulkan transcription server (port 8092).
    # whisper-large-v3-turbo model, runs on GTX TITAN X via Vulkan backend.
    WHISPER_BASE_URL: str = "http://koala:8092"
    WHISPER_ENABLED: bool = True

    # llama.cpp server on grid-svr Blackwell (Qwen3.6 27B GPU + mmproj, port 8081)
    # Timeout MUST be < HERMES cycle timeout (600s in scripts/hermes_operator.py)
    # so that when Hermes blacklists a slow cycle, the in-flight HTTP call
    # also unwinds and the thread exits — otherwise we leak one stuck
    # thread per timed-out cycle, each holding a parallel slot on the LLM
    # server. Diagnosed 2026-05-07: 80+ leaked threads after 14h, server
    # bombarded, every fresh cycle blocked. 300s is plenty for normal
    # generation; queue depth >300s means the LLM is overloaded and
    # backing off is the right call.
    LLAMACPP_BASE_URL: str = "http://localhost:8081"
    LLAMACPP_ENABLED: bool = True
    LLAMACPP_TIMEOUT_SECONDS: int = 300
    LLAMACPP_CHAT_MODEL: str = "Qwen3-32B-Q4_K_M"
    LLAMACPP_EMBED_MODEL: str = "Qwen3-32B-Q4_K_M"

    # llama.cpp ORACLE server on grid-svr Blackwell.
    LLAMACPP_ORACLE_BASE_URL: str = "http://localhost:8081"
    LLAMACPP_ORACLE_ENABLED: bool = True
    LLAMACPP_ORACLE_TIMEOUT_SECONDS: int = 300
    LLAMACPP_ORACLE_CHAT_MODEL: str = "Qwen3-32B-Q4_K_M"
    LLAMACPP_ORACLE_NUM_PREDICT: int = 15000
    LLAMACPP_ORACLE_MIN_NUM_PREDICT: int = 15000

    # llama.cpp QUICK-tier remote server (redbox node — Qwen3-14B, Tailscale-reachable)
    LLAMACPP_QUICK_BASE_URL: str = "http://100.126.129.45:8080"
    LLAMACPP_QUICK_ENABLED: bool = True
    LLAMACPP_QUICK_TIMEOUT_SECONDS: int = 120
    LLAMACPP_QUICK_CHAT_MODEL: str = "qwen3-14b"

    # llama.cpp REASON-tier remote server (gridz4 node — Qwen3.6 35B A3B, Tailscale-reachable)
    LLAMACPP_Z4_BASE_URL: str = "http://gridz4:8080"
    LLAMACPP_Z4_ENABLED: bool = True
    LLAMACPP_Z4_TIMEOUT_SECONDS: int = 180
    LLAMACPP_Z4_CHAT_MODEL: str = "Qwen3.6-35B-A3B-UD-Q4_K_M.gguf"
    LLAMACPP_Z4_NUM_PREDICT: int = 512
    LLAMACPP_Z4_MIN_NUM_PREDICT: int = 0
    LLAMACPP_Z4_REASONING_HEADROOM: int = 0

    # llama.cpp BATCH-tier CPU server on grid-svr (DeepSeek-V4-Flash 158B Q4_K_M, port 8082).
    # Non-interactive heavy reasoning only — slow (~5 tok/sec) but free + powerful.
    # Opt-in: only Tier.BATCH callers reach this; never auto-added to interactive
    # tier fallbacks (would block deep_dive / audio briefing / regression eval otherwise).
    # The legacy `.env` var ``LLM_BATCH_BASE_URL`` was renamed to ``LLAMACPP_BATCH_BASE_URL``
    # to match the established LLAMACPP_* convention used by oracle/quick/z4 tiers.
    LLAMACPP_BATCH_BASE_URL: str = "http://localhost:8082"
    LLAMACPP_BATCH_ENABLED: bool = False
    LLAMACPP_BATCH_TIMEOUT_SECONDS: int = 600
    LLAMACPP_BATCH_CHAT_MODEL: str = "DeepSeekV4-Flash-158B-Q4_K_M"

    # Auth
    GRID_MASTER_PASSWORD_HASH: str = ""
    GRID_JWT_SECRET: str = ""
    GRID_JWT_EXPIRE_HOURS: int = 168
    GRID_ALLOWED_ORIGINS: str = ""  # Empty = use api/main.py env-aware defaults. NEVER "*" with credentials.

    # Prediction markets
    POLYMARKET_API_KEY: str = ""
    POLYMARKET_PRIVATE_KEY: str = ""
    KALSHI_EMAIL: str = ""
    KALSHI_PASSWORD: str = ""

    # TradingAgents integration
    AGENTS_ENABLED: bool = False
    AGENTS_LLM_PROVIDER: str = "openai"  # openai | llamacpp | hyperspace | anthropic
    AGENTS_LLM_MODEL: str = "auto"
    AGENTS_OPENAI_API_KEY: str = ""
    AGENTS_ANTHROPIC_API_KEY: str = ""
    AGENTS_DEBATE_ROUNDS: int = 1
    AGENTS_DEFAULT_TICKER: str = "SPY"
    AGENTS_SCHEDULE_ENABLED: bool = False
    AGENTS_SCHEDULE_CRON: str = "0 17 * * 1-5"  # weekdays at 5 PM
    AGENTS_BACKTEST_MAX_DAYS: int = 365
    AGENTS_MIN_DEBATE_ROUNDS: int = 1
    AGENTS_MAX_DEBATE_ROUNDS: int = 5
    AGENTS_DEBATE_SCALE_THRESHOLD: float = 0.2  # position size at which max rounds kick in
    AGENTS_PERSONA: str = "balanced"

    # AI-Trader integration (HKUDS/AI-Trader multi-agent signal marketplace)
    AI_TRADER_ENABLED: bool = False
    AI_TRADER_BASE_URL: str = ""               # e.g. http://localhost:8000
    AI_TRADER_API_KEY: str = ""                 # Bearer token for AI-Trader API
    AI_TRADER_TOP_AGENTS: int = 10              # Number of top agents to follow
    AI_TRADER_MAX_SIGNALS: int = 200            # Max signals per feed poll
    AI_TRADER_MARKET_FILTER: str = "stocks"     # stocks | crypto | forex | options | ""

    # Circuit breaker (signal executor)
    CIRCUIT_BREAKER_THRESHOLD: int = 3       # consecutive failures before halting
    CIRCUIT_BREAKER_COOLDOWN_HOURS: int = 24  # hours before probation

    # Gemma 4 main server is disabled until a live port-8080 Gemma service is restored.
    # The Gemma micro endpoints below remain separate and active.
    GEMMA_BASE_URL: str = "http://localhost:8080"
    GEMMA_ENABLED: bool = False
    GEMMA_PRIMARY: bool = False
    GEMMA_TIMEOUT_SECONDS: int = 180
    GEMMA_CHAT_MODEL: str = "gemma-4-31B-it-Q4_K_M"
    GEMMA_EMBED_MODEL: str = "gemma-4-31B-it-Q4_K_M"

    # Gemma 3 270M micro models (CPU — task-specific fine-tuned)
    GEMMA_MICRO_CLASSIFIER_URL: str = "http://localhost:8082"
    GEMMA_MICRO_NARRATOR_URL: str = "http://localhost:8083"
    GEMMA_MICRO_EXTRACTOR_URL: str = "http://localhost:8084"
    GEMMA_MICRO_MAPPER_URL: str = "http://localhost:8085"

    # TimesFM (Google time-series foundation model)
    TIMESFM_ENABLED: bool = True
    TIMESFM_MODEL_NAME: str = "google/timesfm-2.0-200m-pytorch"
    TIMESFM_BACKEND: str = "gpu"             # gpu | cpu | tpu
    TIMESFM_CONTEXT_LENGTH: int = 512        # max historical steps
    TIMESFM_HORIZON: int = 7                 # default forecast days

    # AutoBNN (Google — interpretable signal decomposition)
    AUTOBNN_ENABLED: bool = True
    AUTOBNN_NUM_SAMPLES: int = 200
    AUTOBNN_NUM_CHAINS: int = 2
    AUTOBNN_SEED: int = 42

    # A2A Protocol (agent-to-agent communication)
    A2A_ENABLED: bool = True
    A2A_BASE_URL: str = "https://grid.stepdad.finance"

    # x402 Agent Micropayments (AP2 + Coinbase on Base L2)
    X402_ENABLED: bool = False
    X402_NETWORK: str = "base"               # base | base-sepolia
    X402_TOKEN: str = "USDC"
    X402_RECEIVER_ADDRESS: str = ""           # GRID's USDC address on Base
    X402_PRICE_FORECAST: float = 0.01         # USD per forecast call
    X402_PRICE_PREDICTION: float = 0.02       # USD per oracle prediction
    X402_PRICE_SIGNAL: float = 0.01           # USD per signal query
    X402_PRICE_REGIME: float = 0.005          # USD per regime check
    X402_PRICE_ACTOR: float = 0.02            # USD per actor query
    X402_PRICE_OPTIONS: float = 0.02          # USD per options flow query

    # BitNet (1-bit ternary LLM — ultra-fast CPU inference, disabled by default)
    BITNET_BASE_URL: str = "http://localhost:8090"
    BITNET_ENABLED: bool = False
    BITNET_TIMEOUT_SECONDS: int = 120
    BITNET_CHAT_MODEL: str = "bitnet-b1.58-2B-4T"
    BITNET_EMBED_MODEL: str = "bitnet-b1.58-2B-4T"

    # LLM task router — providers: openai | huggingface | anthropic | ollama | llamacpp | llamacpp_quick | llamacpp_z4 | openrouter | bitnet
    LLM_ROUTER_ENABLED: bool = True
    LLM_LOCAL_PROVIDER: str = "llamacpp_quick"  # LOCAL tier — redbox Qwen3-14B
    LLM_REASON_PROVIDER: str = "llamacpp_quick"  # REASON tier — redbox until z4 is tuned
    LLM_ORACLE_PROVIDER: str = "llamacpp_oracle"  # ORACLE tier — heavier oracle path
    # Legacy keys — kept so old .env files don't break get_llm() fallback logic
    LLM_DEFAULT_PROVIDER: str = "llamacpp"
    LLM_QUICK_PROVIDER: str = "llamacpp"
    LLM_DEEP_PROVIDER: str = "llamacpp"

    # pmxt prediction market integration
    PMXT_ENABLED: bool = False
    PMXT_POLYMARKET_PRIVATE_KEY: str = ""
    PMXT_KALSHI_API_KEY: str = ""
    PMXT_KALSHI_PRIVATE_KEY_PATH: str = ""

    # Obsidian Vault (bidirectional knowledge layer)
    OBSIDIAN_VAULT_PATH: str = os.path.expanduser("~/Documents/Obsidian Vault")
    OBSIDIAN_SYNC_ENABLED: bool = True

    # Bookmark Intelligence Pipeline
    GROQ_API_KEY: str = ""
    GEMINI_API_KEY: str = ""
    BOOKMARKS_DB_PATH: str = os.path.expanduser("~/.ft-bookmarks/bookmarks.db")
    BOOKMARKS_OBSIDIAN_PATH: str = os.path.expanduser("~/Documents/Obsidian Vault")
    BOOKMARKS_SYNC_ENABLED: bool = True
    BOOKMARKS_SYNC_CRON: str = "23 7 * * *"  # daily 7:23 AM
    HERMES_Z4_URL: str = "http://gridz4:8080"  # gridz4 llama.cpp node, GTX 1080/P1000

    # Autoresearch (self-improvement loop)
    AUTORESEARCH_ENABLED: bool = True
    AUTORESEARCH_CRON: str = "0 2 * * 1-5"   # weekdays 2 AM
    AUTORESEARCH_MAX_ITER: int = 5
    AUTORESEARCH_LAYER: str = "REGIME"

    # Hyperliquid perp trading
    HYPERLIQUID_PRIVATE_KEY: str = ""
    HYPERLIQUID_TESTNET: bool = True
    HYPERLIQUID_MAX_POSITION_USD: float = 100.0
    HYPERLIQUID_MAX_DRAWDOWN_PCT: float = 0.20

    # Solana trading (AutoHedge-derived 4-agent pipeline)
    JUPITER_API_KEY: str = ""              # Unlocks Jupiter rate limits
    SOLANA_PRIVATE_KEY: str = ""           # Base58 wallet key; required for live
    SOLANA_RPC_URL: str = "https://api.mainnet-beta.solana.com"
    SOLANA_LIVE_TRADING: bool = False      # Must be True to enable live swaps
    SOLANA_MAX_POSITION_USD: float = 50.0
    SOLANA_MAX_DRAWDOWN_PCT: float = 0.20

    # Solana safety rails — thresholds for trading/solana/safety.py
    SOLANA_REQUIRE_MINT_RENOUNCED: bool = True   # Block mints with live mint auth
    SOLANA_REQUIRE_FREEZE_RENOUNCED: bool = True # Block mints with live freeze auth
    SOLANA_MAX_TOP10_HOLDER_PCT: float = 25.0    # Top-10 holders can't own > 25%
    SOLANA_MAX_PRICE_IMPACT_PCT: float = 5.0     # Max slippage on sim sell
    SOLANA_CAPITAL_PER_TRADE_USD: float = 50.0   # Upper bound per trade
    SOLANA_MAX_DAILY_USD: float = 200.0          # Daily notional cap
    SOLANA_MAX_DAILY_TRADES: int = 20            # Daily trade count cap
    SOLANA_MAX_PER_MINT_DAILY_USD: float = 75.0  # Per-mint daily cap

    # Hard blocklist — comma-separated Solana mint addresses the
    # operator has a beneficial interest in. GRID will never trade any
    # token listed here, regardless of signal. Use this for CTO coins
    # you market, bags you promote, or anything with a conflict of
    # interest. Enforced by SolanaSafetyChecker as a hard block.
    SOLANA_MINT_BLOCKLIST: str = ""

    # Top-volume universe snapshotter (ingestion/solana/top_volume.py)
    SOLANA_UNIVERSE_LIMIT: int = 250
    SOLANA_UNIVERSE_CRON: str = "0 */4 * * *"          # every 4 hours
    SOLANA_UNIVERSE_ENRICH_ON_INSERT: bool = True
    SOLANA_UNIVERSE_JUPITER_URL: str = "https://token.jup.ag/strict"
    SOLANA_UNIVERSE_DEX_BATCH: int = 30

    # Solana real-time ingest + cross-reference
    HELIUS_API_KEY: str = ""                       # Helius Enhanced Transactions + webhooks
    HELIUS_BASE_URL: str = "https://api.helius.xyz"
    SOLANA_FAST_ENTRY_MIN_SCORE: float = 0.40      # Gate for FastEntryPath
    SOLANA_FAST_ENTRY_BASE_SIZE: float = 0.60      # Portfolio fraction at composite=1
    SOLANA_FAST_ENTRY_REQUIRE_DEPLOYER: bool = False
    SOLANA_DEPLOYER_LOOKBACK_DAYS: int = 180
    SOLANA_DEPLOYER_GRADUATION_USD: float = 100_000.0
    SOLANA_ENRICH_WINDOW_SECONDS: int = 60

    # Telegram scanner (Solana memecoin monitoring)
    TELEGRAM_API_ID: str = ""              # From my.telegram.org
    TELEGRAM_API_HASH: str = ""            # From my.telegram.org
    TELEGRAM_PHONE: str = ""               # Phone with country code (+1...)
    TELEGRAM_CHANNELS: str = ""            # Comma-separated channel usernames
    TELEGRAM_USERS: str = ""               # JSON array for multi-user mode

    # Discord scanner (Solana memecoin monitoring)
    DISCORD_USER_TOKEN: str = ""           # Discord user token (not bot)
    DISCORD_GUILD_IDS: str = ""            # Comma-separated guild IDs
    DISCORD_CHANNEL_IDS: str = ""          # Comma-separated channel IDs
    DISCORD_USERS: str = ""                # JSON array for multi-user mode

    # Push notifications (VAPID for Web Push)
    VAPID_PRIVATE_KEY: str = ""
    VAPID_PUBLIC_KEY: str = ""
    VAPID_CLAIMS_EMAIL: str = "mailto:stepdadfinance@gmail.com"

    # Email alerts
    ALERT_EMAIL_ENABLED: bool = True
    ALERT_EMAIL_TO: str = "stepdadfinance@gmail.com"
    ALERT_EMAIL_FROM: str = "grid-alerts@grid-svr"
    ALERT_SMTP_HOST: str = "localhost"
    ALERT_SMTP_PORT: int = 25
    ALERT_SMTP_USER: str = ""
    ALERT_SMTP_PASSWORD: str = ""
    ALERT_SMTP_USE_TLS: bool = False

    # Market briefing schedules
    BRIEFING_CRON_DAILY: str = "0 6 * * 1-5"  # weekdays 6 AM
    BRIEFING_CRON_WEEKLY: str = "0 7 * * 1"   # Monday 7 AM

    # KV Cache Quantization (TurboQuant — arXiv:2504.19874)
    TURBOQUANT_ENABLED: bool = True
    TURBOQUANT_BITS: int = 3
    TURBOQUANT_MODE: str = "mse"

    # Prefect workflow orchestration
    PREFECT_API_URL: str = "http://localhost:4200/api"
    PREFECT_ENABLED: bool = True

    # Redpanda / Kafka event stream
    REDPANDA_BROKER: str = "localhost:19092"
    REDPANDA_ENABLED: bool = True

    # MinIO / S3 blob store
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "gridminio"
    MINIO_SECRET_KEY: str = "gridminio2026"
    MINIO_SECURE: bool = False
    MINIO_REGION: str = "us-east-1"

    @property
    def DB_URL(self) -> str:
        """Construct the full PostgreSQL connection URL."""
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    @field_validator("FRED_API_KEY")
    @classmethod
    def _check_fred_key(cls, v: str) -> str:
        """Allow empty key only in development; raise otherwise."""
        env = os.getenv("ENVIRONMENT", "development")
        if env != "development" and not v:
            raise ValueError(
                "FRED_API_KEY must be set in non-development environments. "
                "Set the FRED_API_KEY environment variable or add it to .env."
            )
        return v

    @field_validator("DB_PASSWORD")
    @classmethod
    def _check_db_password(cls, v: str) -> str:
        """Reject missing or default password in non-development environments."""
        env = os.getenv("ENVIRONMENT", "development")
        if env != "development" and v == "changeme":
            raise ValueError(
                "DB_PASSWORD must be set in non-development environments. "
                "Set DB_PASSWORD in .env."
            )
        return v

    @field_validator("GRID_JWT_SECRET")
    @classmethod
    def _check_jwt_secret(cls, v: str) -> str:
        """Require a real JWT secret in production."""
        env = os.getenv("ENVIRONMENT", "development")
        if env == "production" and (not v or v == "dev-secret-change-me"):
            raise ValueError(
                "GRID_JWT_SECRET must be set in production. Generate one with: "
                "python -c \"import secrets; print(secrets.token_urlsafe(64))\""
            )
        return v

    def audit_api_keys(self) -> dict[str, bool]:
        """Check which optional API keys are configured.

        Returns a dict of key_name -> is_set for operator awareness.
        """
        keys = {
            "FRED_API_KEY": self.FRED_API_KEY,
            "KOSIS_API_KEY": self.KOSIS_API_KEY,
            "COMTRADE_API_KEY": self.COMTRADE_API_KEY,
            "JQUANTS_EMAIL": self.JQUANTS_EMAIL,
            "USDA_NASS_API_KEY": self.USDA_NASS_API_KEY,
            "NOAA_TOKEN": self.NOAA_TOKEN,
            "EIA_API_KEY": self.EIA_API_KEY,
            "GDELT_API_KEY": self.GDELT_API_KEY,
            "WORLDNEWS_API_KEY": self.WORLDNEWS_API_KEY,
        }
        return {k: bool(v) for k, v in keys.items()}

    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


# ---------------------------------------------------------------------------
# Singleton settings instance used throughout the project
# ---------------------------------------------------------------------------
settings = Settings()

# Configure loguru
log.remove()  # Remove default handler
log.add(
    sys.stderr,
    level=settings.LOG_LEVEL,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> — "
        "<level>{message}</level>"
    ),
)

# Git-backed error sink. Writes ERROR+ records to .server-logs/errors.jsonl
# locally; the background push-to-remote is env-gated off by default
# (see GIT_SINK_PUSH_ENABLED in server_log/git_sink.py) so there's no
# exfiltration risk from just attaching the sink. The sink was lost in
# the 709bab4 refactor — without it, .server-logs/errors.jsonl goes
# stale and the triage dashboards read empty data. Gate behind an env
# var in case a script or test wants to disable it entirely.
if os.getenv("GRID_GIT_SINK_DISABLED", "").lower() not in ("1", "true", "yes"):
    try:
        from server_log.git_sink import GitSink

        _git_sink = GitSink()
        log.add(_git_sink.write, level="ERROR")
        _git_sink.start()
        log.info(
            "GitSink attached — errors land in .server-logs/errors.jsonl "
            "(push_enabled={push})",
            push=bool(os.getenv("GIT_SINK_PUSH_ENABLED", "").lower()
                      in ("1", "true", "yes")),
        )
    except Exception as _gs_exc:
        # Never let logging setup crash the app
        log.warning("GitSink could not be attached: {e}", e=str(_gs_exc))

log.info(
    "GRID config loaded — environment={env}, db={db}",
    env=settings.ENVIRONMENT,
    db=f"{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}",
)


if __name__ == "__main__":
    print(f"DB_URL:              {settings.DB_URL}")
    print(f"FRED_API_KEY:        {'***' if settings.FRED_API_KEY else '(not set)'}")
    print(f"BLS_API_KEY:         {'***' if settings.BLS_API_KEY else '(not set)'}")
    print(f"ENVIRONMENT:         {settings.ENVIRONMENT}")
    print(f"LOG_LEVEL:           {settings.LOG_LEVEL}")
    print(f"PULL_SCHEDULE_FRED:  {settings.PULL_SCHEDULE_FRED}")
