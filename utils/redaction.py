"""Credential redaction for text that leaves the process.

Shared by ingestion logging (``ingestion/base.py``) and Hermes health
reasons (``scripts/hermes_health.py``), which reach journald, alert emails
and the Hermes LLM prompt. 2026-11-04: the Polygon key was found in 60
grid-scheduler journal lines in 7 days via the retry wrapper's ``str(exc)``.
"""

from __future__ import annotations

import re

# Query strings of any scheme are dropped whole (to whitespace or "#").
_URL_QUERY_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://[^\s?#'\"<>]+)\?[^\s#'\"<>]*")
# Userinfo of any scheme, greedy to the LAST "@" that is followed by a host,
# so passwords containing "@" or "/" are covered (over-redacts a path "@").
_URL_USERINFO_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^\s'\"<>]*@(?=[^\s@/'\"<>]+)")
# Authorization headers: everything to end of line / closing quote.
_AUTH_HEADER_RE = re.compile(r"(?i)\b((?:proxy-)?authorization['\"]?\s*[:=]\s*['\"]?)[^\r\n'\"]+")
# Credential schemes outside a header (Bearer/Basic/Digest + value).
_AUTH_SCHEME_RE = re.compile(r"(?i)\b(bearer|basic|digest)\s+[A-Za-z0-9._~+/=-]{8,}")
# JSON Web Tokens.
_JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}")
# A 32+ char hex path segment is a key (NASA FIRMS /api/area/csv/<MAP_KEY>/).
_HEX_PATH_KEY_RE = re.compile(r"(?i)(/)[0-9a-f]{32,}(?=/|\b)")
# key=value, key: value, "key": "value", 'key'='value' -- the key may be
# quoted, and a quoted value is redacted whole (spaces included). An
# unterminated quote (pull_log truncates messages at 500 chars) is redacted
# to the end of the text.
_SECRET_ASSIGN_RE = re.compile(
    r"(?i)\b([a-z0-9_-]*(?:api[_-]?key|apikey|access[_-]?key|private[_-]?key|client[_-]?secret"
    r"|credential|token|secret|password|passwd|auth)[a-z0-9_-]*['\"]?\s*[=:]\s*)"
    r"(?:\"[^\"]*(?:\"|$)|'[^']*(?:'|$)|[^\s,&'\"}]+)"
)


def redact_secrets(reason: str | None) -> str | None:
    """Strip credentials from error text before it is logged or sent.

    Upstream errors embed request URLs (``?api_key=...``, Polygon's
    ``?apiKey=...``, FIRMS' key as a path segment) and occasionally
    ``token=...`` text. Drops URL query strings and userinfo entirely, masks
    long hex path segments, bearer tokens and key/token/secret/password
    assignments. Non-string input is stringified; ``None`` stays ``None``.
    """
    if reason is None:
        return None
    # Callable replacements, not "\\1" templates: re parses templates via a
    # lazy import, which breaks logging wherever builtins.__import__ is
    # patched (and this runs inside the global log patcher).
    text_ = _URL_USERINFO_RE.sub(lambda m: m.group(1) + "[REDACTED]@", str(reason))
    text_ = _URL_QUERY_RE.sub(lambda m: m.group(1) + "?[REDACTED]", text_)
    text_ = _HEX_PATH_KEY_RE.sub(lambda m: m.group(1) + "[REDACTED]", text_)
    text_ = _JWT_RE.sub(lambda m: "[REDACTED-JWT]", text_)
    text_ = _AUTH_HEADER_RE.sub(lambda m: m.group(1) + "[REDACTED]", text_)
    text_ = _AUTH_SCHEME_RE.sub(lambda m: m.group(1) + " [REDACTED]", text_)
    return _SECRET_ASSIGN_RE.sub(lambda m: m.group(1) + "[REDACTED]", text_)
