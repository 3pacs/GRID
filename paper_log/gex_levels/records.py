"""Record-building helpers shared by ``preopen.py`` and ``postclose.py``.

Keeps the envelope fields (``kind``, ``run_at``, ``session_date``,
``code_sha``, ``excluded``, ``exclusion_reason``) spelled identically in
both places, and trims the intermediate computation dataclasses down to
exactly the fields the pre-registration asks to be recorded — most
importantly, dropping ``LevelsResult.raw`` (the full engine profile,
including per-strike and curve data meant for the API/PWA, not for a lean
audit log) rather than dumping it into every line.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from typing import Any

from paper_log.gex_levels.engine_adapter import LevelsResult
from paper_log.gex_levels.market_data import PricePoint


def envelope(
    *,
    kind: str,
    run_at: datetime,
    session_date: date,
    code_sha: str,
    excluded: bool,
    exclusion_reason: str | None,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "run_at": run_at,
        "session_date": session_date,
        "code_sha": code_sha,
        "excluded": excluded,
        "exclusion_reason": exclusion_reason,
    }


def levels_result_to_dict(lr: LevelsResult) -> dict[str, Any]:
    """LevelsResult, minus the full engine `raw` payload."""
    d = asdict(lr)
    d.pop("raw", None)
    return d


def price_point_to_dict(pp: PricePoint | None) -> dict[str, Any] | None:
    return None if pp is None else asdict(pp)
