"""Credential redaction for text that leaves the process.

Shared by ingestion logging (``ingestion/base.py``) and Hermes health
reasons (``scripts/hermes_health.py``), which reach journald, alert emails
and the Hermes LLM prompt. 2026-11-04: the Polygon key was found in 60
grid-scheduler journal lines in 7 days via the retry wrapper's ``str(exc)``.
"""

from __future__ import annotations

import re

_URL_QUERY_RE = re.compile(r"(https?://[^\s?#'\"<>]+)\?[^\s'\"<>)]*")
# Any scheme (postgresql://, redis://, ...); greedy up to the last "@"
# before the host so passwords containing "@" are covered too.
_URL_USERINFO_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^/\s'\"<>]*@")
# key=value, key: value, "key": "value", 'key'='value' -- the key may be
# quoted, and a quoted value is redacted whole (spaces included). An
# unterminated quote (pull_log truncates messages at 500 chars) is redacted
# to the end of the text.
_SECRET_ASSIGN_RE = re.compile(
    r"(?i)\b([a-z0-9_]*(?:api[_-]?key|apikey|token|secret|password|passwd|auth)[a-z0-9_]*['\"]?\s*[=:]\s*)"
    r"(?:\"[^\"]*(?:\"|$)|'[^']*(?:'|$)|[^\s,&'\"}]+)"
)
# A 32+ char hex path segment is a key (NASA FIRMS /api/area/csv/<MAP_KEY>/).
_HEX_PATH_KEY_RE = re.compile(r"(?i)(/)[0-9a-f]{32,}(?=/|\b)")
_BEARER_RE = re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}")


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
    text_ = _URL_USERINFO_RE.sub(r"\1[REDACTED]@", str(reason))
    text_ = _URL_QUERY_RE.sub(r"\1?[REDACTED]", text_)
    text_ = _HEX_PATH_KEY_RE.sub(r"\1[REDACTED]", text_)
    text_ = _BEARER_RE.sub(r"\1 [REDACTED]", text_)
    return _SECRET_ASSIGN_RE.sub(r"\1[REDACTED]", text_)
