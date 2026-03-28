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
    DB_PASSWORD: str = "changeme"
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

    # OpenAI integration (preferred primary cloud LLM)
    OPENAI_API_KEY: str = ""
    OPENAI_BASE_URL: str = "https://api.openai.com/v1"
    OPENAI_TIMEOUT_SECONDS: int = 120
    OPENAI_CHAT_MODEL: str = "gpt-4o-mini"
    OPENAI_EMBED_MODEL: str = "text-embedding-3-small"

    # Ollama integration (deprecated — use llama.cpp)
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_ENABLED: bool = False
    OLLAMA_TIMEOUT_SECONDS: int = 120
    OLLAMA_CHAT_MODEL: str = "llama3.1:8b"
    OLLAMA_EMBED_MODEL: str = "nomic-embed-text"

    # llama.cpp server (replaces Ollama — direct GPU inference)
    LLAMACPP_BASE_URL: str = "http://localhost:8080"
    LLAMACPP_ENABLED: bool = True
    LLAMACPP_TIMEOUT_SECONDS: int = 120
    LLAMACPP_CHAT_MODEL: str = "hermes"
    LLAMACPP_EMBED_MODEL: str = "hermes"

    # Auth
    GRID_MASTER_PASSWORD_HASH: str = ""
    GRID_JWT_SECRET: str = ""
    GRID_JWT_EXPIRE_HOURS: int = 168
    GRID_ALLOWED_ORIGINS: str = "*"

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
        """Reject default password in non-development environments."""
        env = os.getenv("ENVIRONMENT", "development")
        if env != "development" and v == "changeme":
            raise ValueError(
                "DB_PASSWORD must be changed from the default in non-development "
                "environments. Set DB_PASSWORD in .env."
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
