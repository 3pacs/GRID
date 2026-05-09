"""HTTP status-code severity classifier shared by ingestion modules.

Determines whether an upstream HTTP failure should be downgraded to a
WARNING or surfaced at ERROR. CLAUDE.md reserves ERROR for unhandled
application bugs; transient upstream sickness (5xx, rate limits) and
permanent client-side rejections (4xx) are both ``not actionable in our
codebase`` and should not pollute errors.jsonl.

This module has no third-party deps so it can be imported by the
ingestion modules and their tests without dragging in fedfred / requests.
"""

from __future__ import annotations

# Permanent client-side rejections — retrying never helps, log at WARNING
# and skip writing a FAILED row.
PERMANENT_4XX: frozenset[int] = frozenset({400, 401, 403, 404, 405, 410, 422, 429, 451})

# Transient upstream errors — retry pipeline already exhausted by the
# time we see them, but they will resolve on the next cycle. Log at
# WARNING and skip writing a FAILED row to keep errors.jsonl signal-rich.
TRANSIENT_5XX: frozenset[int] = frozenset({500, 502, 503, 504})


def is_transient_http(status: int | None) -> bool:
    """Return True if the status is a known transient upstream error."""
    return status in TRANSIENT_5XX


def is_permanent_http(status: int | None) -> bool:
    """Return True if the status is a permanent client-side rejection."""
    return status in PERMANENT_4XX


def is_warning_worthy(status: int | None) -> bool:
    """Return True if the status should be logged at WARNING (not ERROR)."""
    return is_transient_http(status) or is_permanent_http(status)
