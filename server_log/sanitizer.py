"""Sanitize log messages by scrubbing secrets and sensitive values.

The sanitizer operates in two layers:

1. **Value-based** — any config value marked as sensitive is replaced with
   ``[REDACTED]`` wherever it appears in the message text.
2. **Pattern-based** — regex patterns catch common secret shapes (JWTs,
   API keys, connection strings, Bearer tokens) even if the value is not
   in config.
"""

from __future__ import annotations

import re
from typing import Sequence


# ---------------------------------------------------------------------------
# Regex patterns for common secret shapes
# ---------------------------------------------------------------------------
_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # JWT tokens (header.payload.signature)
    (re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"), "[REDACTED_JWT]"),
    # Bearer tokens in headers
    (re.compile(r"(?i)(bearer\s+)[^\s,;\"']+"), r"\1[REDACTED_TOKEN]"),
    # PostgreSQL connection strings with password
    (re.compile(r"postgresql://[^:]+:([^@]+)@"), _replace_pg_password := "postgresql://[user]:[REDACTED]@"),
    # Generic password= in connection strings or logs
    (re.compile(r"(?i)(password\s*[=:]\s*)[^\s,;\"'}\]]+"), r"\1[REDACTED]"),
    # API key patterns (long alphanumeric strings preceded by key/token/secret)
    (re.compile(r"(?i)((?:api[_-]?key|token|secret)\s*[=:]\s*)[^\s,;\"'}\]]+"), r"\1[REDACTED]"),
    # Generic hex/base64 secrets (32+ chars that look like keys)
    (re.compile(r"(?<=[=: ])[A-Za-z0-9+/]{40,}={0,2}(?=[\s,;\"'}\]]|$)"), "[REDACTED_KEY]"),
]

# Fix the pg password pattern — use a proper callable
_PATTERNS[2] = (
    re.compile(r"postgresql://([^:]+):([^@]+)@"),
    lambda m: f"postgresql://{m.group(1)}:[REDACTED]@",
)


class Sanitizer:
    """Scrub sensitive values and patterns from text.

    Parameters
    ----------
    secret_values:
        Exact string values to replace (e.g., actual API keys from config).
        Empty strings and very short values (< 4 chars) are ignored to
        avoid false-positive replacements.
    """

    def __init__(self, secret_values: Sequence[str] = ()) -> None:
        # Only register values long enough to be meaningful
        self._secrets: list[str] = sorted(
            (v for v in secret_values if v and len(v) >= 4),
            key=len,
            reverse=True,  # longest first to avoid partial matches
        )

    def scrub(self, text: str) -> str:
        """Return *text* with all sensitive material replaced."""
        # Layer 1: exact value replacement
        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, "[REDACTED]")

        # Layer 2: pattern-based replacement
        for pattern, replacement in _PATTERNS:
            if callable(replacement):
                text = pattern.sub(replacement, text)
            else:
                text = pattern.sub(replacement, text)

        return text


def build_sanitizer_from_settings() -> Sanitizer:
    """Construct a Sanitizer pre-loaded with all sensitive config values.

    Imports ``config.settings`` lazily to avoid circular imports.
    """
    from config import settings

    # Include every API key we know about — upstream error messages
    # regularly quote the caller's key back verbatim (observed with
    # Alpha Vantage), which would leak credentials to the error log.
    # Adding a field here is always safe: values < 4 chars / empty are
    # dropped by Sanitizer, and unknown settings raise AttributeError
    # that we swallow rather than failing boot.
    candidate_attrs = (
        "DB_PASSWORD",
        "FRED_API_KEY",
        "KOSIS_API_KEY",
        "COMTRADE_API_KEY",
        "JQUANTS_EMAIL",
        "JQUANTS_PASSWORD",
        "USDA_NASS_API_KEY",
        "NOAA_TOKEN",
        "EIA_API_KEY",
        "GDELT_API_KEY",
        "GRID_JWT_SECRET",
        "GRID_MASTER_PASSWORD_HASH",
        "AGENTS_OPENAI_API_KEY",
        "AGENTS_ANTHROPIC_API_KEY",
        "ALPHAVANTAGE_API_KEY",
        "POLYGON_API_KEY",
        "OPENROUTER_API_KEY",
        "CONGRESS_API_KEY",
        "FEC_API_KEY",
        "UNUSUAL_WHALES_API_KEY",
        "CRYPTOQUANT_API_KEY",
        "FINNHUB_API_KEY",
        "TIINGO_API_KEY",
        "DUNE_API_KEY",
        "CLOUDFLARE_API_TOKEN",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
    )
    secret_values = [
        getattr(settings, name, "") for name in candidate_attrs
    ]

    return Sanitizer(secret_values)
