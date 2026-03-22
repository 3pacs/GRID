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
from pydantic import field_validator
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

    # API Keys
    FRED_API_KEY: str = ""

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

    # TradingAgents integration
    AGENTS_ENABLED: bool = False
    AGENTS_LLM_PROVIDER: str = "llamacpp"  # llamacpp | hyperspace | openai | anthropic
    AGENTS_LLM_MODEL: str = "auto"
    AGENTS_OPENAI_API_KEY: str = ""
    AGENTS_ANTHROPIC_API_KEY: str = ""
    AGENTS_DEBATE_ROUNDS: int = 1
    AGENTS_DEFAULT_TICKER: str = "SPY"
    AGENTS_SCHEDULE_ENABLED: bool = False
    AGENTS_SCHEDULE_CRON: str = "0 17 * * 1-5"  # weekdays at 5 PM
    AGENTS_BACKTEST_MAX_DAYS: int = 365

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
        # Validation happens after model construction, so we inspect the
        # ENVIRONMENT variable directly from the environment here.
        env = os.getenv("ENVIRONMENT", "development")
        if env != "development" and not v:
            raise ValueError(
                "FRED_API_KEY must be set in non-development environments. "
                "Set the FRED_API_KEY environment variable or add it to .env."
            )
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


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
    print(f"ENVIRONMENT:         {settings.ENVIRONMENT}")
    print(f"LOG_LEVEL:           {settings.LOG_LEVEL}")
    print(f"PULL_SCHEDULE_FRED:  {settings.PULL_SCHEDULE_FRED}")
