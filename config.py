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

    # Backup data source API keys
    COINGECKO_API_KEY: str = ""          # Free: 30 req/min, Pro: unlimited
    ALPHAVANTAGE_API_KEY: str = ""       # Free: 25 req/day
    TWELVEDATA_API_KEY: str = ""         # Free: 800 req/day

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
    OLLAMA_CHAT_MODEL: str = "qwen2.5:7b"
    OLLAMA_EMBED_MODEL: str = "nomic-embed-text"

    # llama.cpp server (ALL tiers — Nemotron-Super-49B v1.5 Q5_K_M, GPU+CPU split, port 8080)
    LLAMACPP_BASE_URL: str = "http://localhost:8080"
    LLAMACPP_ENABLED: bool = True
    LLAMACPP_TIMEOUT_SECONDS: int = 300
    LLAMACPP_CHAT_MODEL: str = "nvidia_Llama-3_3-Nemotron-Super-49B-v1_5-Q5_K_M"
    LLAMACPP_EMBED_MODEL: str = "nvidia_Llama-3_3-Nemotron-Super-49B-v1_5-Q5_K_M"

    # llama.cpp CPU server (disabled — 120B too slow, kept for future use)
    LLAMACPP_ORACLE_BASE_URL: str = "http://localhost:8081"
    LLAMACPP_ORACLE_ENABLED: bool = False
    LLAMACPP_ORACLE_TIMEOUT_SECONDS: int = 300
    LLAMACPP_ORACLE_CHAT_MODEL: str = "nvidia_Nemotron-3-Super-120B-A12B-Q6_K"

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

    # Circuit breaker (signal executor)
    CIRCUIT_BREAKER_THRESHOLD: int = 3       # consecutive failures before halting
    CIRCUIT_BREAKER_COOLDOWN_HOURS: int = 24  # hours before probation

    # Gemma 3 (local GPU — 27B QAT on RTX 3090, 128K context)
    GEMMA_BASE_URL: str = "http://localhost:8081"
    GEMMA_ENABLED: bool = True
    GEMMA_TIMEOUT_SECONDS: int = 180
    GEMMA_CHAT_MODEL: str = "gemma-3-27b-it"
    GEMMA_EMBED_MODEL: str = "gemma-3-27b-it"

    # Gemma 3 270M micro models (CPU — task-specific fine-tuned)
    GEMMA_MICRO_CLASSIFIER_URL: str = "http://localhost:8082"
    GEMMA_MICRO_NARRATOR_URL: str = "http://localhost:8083"
    GEMMA_MICRO_EXTRACTOR_URL: str = "http://localhost:8084"

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

    # LLM task router — providers: openai | huggingface | anthropic | ollama | llamacpp | openrouter
    LLM_ROUTER_ENABLED: bool = True
    LLM_LOCAL_PROVIDER: str = "llamacpp"       # LOCAL tier — Nemotron-Super-49B local
    LLM_REASON_PROVIDER: str = "llamacpp"      # REASON tier — Nemotron-Super-49B local
    LLM_ORACLE_PROVIDER: str = "llamacpp"      # ORACLE tier — Nemotron-Super-49B local (OpenRouter fallback)
    # Legacy keys — kept so old .env files don't break get_llm() fallback logic
    LLM_DEFAULT_PROVIDER: str = "llamacpp"
    LLM_QUICK_PROVIDER: str = "llamacpp"
    LLM_DEEP_PROVIDER: str = "llamacpp"

    # pmxt prediction market integration
    PMXT_ENABLED: bool = False
    PMXT_POLYMARKET_PRIVATE_KEY: str = ""
    PMXT_KALSHI_API_KEY: str = ""
    PMXT_KALSHI_PRIVATE_KEY_PATH: str = ""

    # Bookmark Intelligence Pipeline
    GROQ_API_KEY: str = ""
    GEMINI_API_KEY: str = ""
    BOOKMARKS_DB_PATH: str = os.path.expanduser("~/.ft-bookmarks/bookmarks.db")
    BOOKMARKS_OBSIDIAN_PATH: str = os.path.expanduser("~/Documents/Obsidian Vault")
    BOOKMARKS_SYNC_ENABLED: bool = True
    BOOKMARKS_SYNC_CRON: str = "23 7 * * *"  # daily 7:23 AM

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
