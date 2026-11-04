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
    (re.compile(r"(?i)((?:api[_-]?key|token|secret)\s*[=:]\s*)[^\s,;\"'}\]&#]+"), r"\1[REDACTED]"),
    # Credentials in URL query strings (?apikey=..., &key=..., &access_token=...): a requests HTTPError message
    # carries the full request URL, so any puller that passes its key as a query parameter would otherwise log it.
    (re.compile(r"(?i)([?&](?:api[_-]?key|apikey|key|token|access[_-]?key|access[_-]?token|auth[_-]?token|"
                r"client[_-]?secret|secret|password|sig|signature)=)[^&\s#\"'<>]+"), r"\1[REDACTED]"),
]

# Generic hex/base64 secrets (40+ chars that look like keys). Kept separate because it also matches git SHAs and
# content hashes, so it only runs where those don't matter (the error-file sink), not on every log line.
_GENERIC_PATTERN = (re.compile(r"(?<=[=: ])[A-Za-z0-9+/]{40,}={0,2}(?=[\s,;\"'}\]]|$)"), "[REDACTED_KEY]")

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
    generic:
        Also replace any 40+ character key-shaped token. True for the error-file sink; False for every-line log
        scrubbing, where it would also erase git SHAs and content hashes.
    min_secret_len:
        Shortest exact value replaced. Every-line scrubbing uses 8: a short credential-named setting (X402_TOKEN
        defaults to a currency symbol) would otherwise erase an ordinary word from every log line.
    """

    def __init__(self, secret_values: Sequence[str] = (), generic: bool = True, min_secret_len: int = 4) -> None:
        self._patterns = _PATTERNS + [_GENERIC_PATTERN] if generic else list(_PATTERNS)
        # Only register values long enough to be meaningful
        self._secrets: list[str] = sorted(
            (v for v in secret_values if v and len(v) >= min_secret_len),
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
        for pattern, replacement in self._patterns:
            if callable(replacement):
                text = pattern.sub(replacement, text)
            else:
                text = pattern.sub(replacement, text)

        return text


# Settings fields whose names look secret but whose values are not: file paths and public keys.
_NOT_SECRET_SUFFIXES = ("_FILE", "_PATH", "_PUBLIC_KEY", "_DIR", "_URL")
_SECRET_NAME = re.compile(r"(KEY|TOKEN|SECRET|PASSWORD|CREDENTIALS)")


def secret_values_from_settings(settings: object) -> list[str]:
    """Every string setting whose field name looks like a credential (…_KEY, …_TOKEN, …_SECRET, …_PASSWORD).

    Collected by name so a newly added API key is scrubbed without editing a list here.
    """
    fields = None
    dump = getattr(settings, "model_dump", None)  # pydantic settings
    if callable(dump):
        try:
            fields = dump()
        except Exception:
            fields = None
    if not isinstance(fields, dict):  # plain objects and test doubles
        fields = {k: getattr(settings, k, None) for k in dir(settings) if k.isupper()}
    values = [
        v for k, v in fields.items()
        if isinstance(v, str) and _SECRET_NAME.search(k) and not k.endswith(_NOT_SECRET_SUFFIXES)
    ]
    # Not credential-named, but sensitive.
    values.append(getattr(settings, "JQUANTS_EMAIL", "") or "")
    return values


def build_sanitizer_from_settings(generic: bool = True, min_secret_len: int = 4) -> Sanitizer:
    """Construct a Sanitizer pre-loaded with all sensitive config values.

    Imports ``config.settings`` lazily to avoid circular imports.
    """
    from config import settings

    return Sanitizer(secret_values_from_settings(settings), generic=generic, min_secret_len=min_secret_len)


_default_scrubber: Sanitizer | None = None


def scrub_secrets(text: str) -> str:
    """Scrub credentials from text that is about to be logged or stored (no generic 40+ char layer).

    Never raises: if settings cannot be loaded, only the pattern layer runs.
    """
    global _default_scrubber
    if not text:
        return text
    if _default_scrubber is None:
        try:
            _default_scrubber = build_sanitizer_from_settings(generic=False, min_secret_len=8)
        except Exception:
            _default_scrubber = Sanitizer(generic=False)
    return _default_scrubber.scrub(text)
