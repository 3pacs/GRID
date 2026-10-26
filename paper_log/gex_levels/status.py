"""``status`` — activity and data quality only.

Pre-registration (Schedule): "Until then [60 valid sessions] the job
reports only activity and data quality (sessions logged, sessions excluded
and why, touches, trades), never returns or hit rates."

This module is written so that guarantee holds *by construction*, not by
convention: it never reads a record's `return_pct`, `pnl_usd`, or `held`
field, anywhere. It counts reach/trade *events*, never their outcomes.
Nothing here changes once 60 valid sessions is reached — `status` stays
outcome-free for the life of v1; only `evaluate` (run explicitly, once) is
allowed to look at results.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from paper_log.gex_levels.config import (
    ALL_EXCLUSION_CODES,
    EXCL_MARKET_CLOSED,
    STOP_REVIEW_EXCLUSION_PCT,
    STOP_REVIEW_SESSION_COUNT,
)
from paper_log.gex_levels.reading import load_session_pairs
from paper_log.gex_levels.storage import PaperLogStore


@dataclass(frozen=True)
class StatusReport:
    sessions_preopen: int
    sessions_postclose: int
    valid_sessions: int
    excluded_by_reason: dict[str, int] = field(default_factory=dict)
    real_reaches: int = 0
    placebo_reaches: int = 0
    real_trades: int = 0
    placebo_trades: int = 0
    stop_review_advisory: str | None = None


def _count_reach_events(reaches_arm: dict[str, Any] | None) -> int:
    """How many levels in one arm ("real" or "placebo") were reached —
    a pure activity count, never *which* held/broke."""
    if not reaches_arm:
        return 0
    return sum(1 for outcome in reaches_arm.values() if outcome and outcome.get("status") == "reached")


def compute_status(log_dir: Path) -> StatusReport:
    store = PaperLogStore(Path(log_dir))
    records = store.read_all()

    preopen = [r for r in records if r.get("kind") == "preopen"]
    postclose = [r for r in records if r.get("kind") == "postclose"]

    excluded_by_reason: dict[str, int] = {code: 0 for code in ALL_EXCLUSION_CODES}
    for r in preopen + postclose:
        if r.get("excluded"):
            reason = r.get("exclusion_reason") or "unknown"
            excluded_by_reason[reason] = excluded_by_reason.get(reason, 0) + 1
    excluded_by_reason = {k: v for k, v in excluded_by_reason.items() if v > 0}

    valid_sessions = sum(1 for p in load_session_pairs(log_dir).values() if p.valid)

    real_reaches = placebo_reaches = real_trades = placebo_trades = 0
    for r in postclose:
        if r.get("excluded"):
            continue
        reaches = r.get("reaches") or {}
        real_reaches += _count_reach_events(reaches.get("real"))
        placebo_reaches += _count_reach_events(reaches.get("placebo"))
        h3 = r.get("h3_trade") or {}
        if (h3.get("real") or {}).get("triggered"):
            real_trades += 1
        if (h3.get("placebo") or {}).get("triggered"):
            placebo_trades += 1

    stop_review_advisory = _stop_review_advisory(preopen)

    return StatusReport(
        sessions_preopen=len(preopen),
        sessions_postclose=len(postclose),
        valid_sessions=valid_sessions,
        excluded_by_reason=excluded_by_reason,
        real_reaches=real_reaches,
        placebo_reaches=placebo_reaches,
        real_trades=real_trades,
        placebo_trades=placebo_trades,
        stop_review_advisory=stop_review_advisory,
    )


def _stop_review_advisory(preopen: list[dict[str, Any]]) -> str | None:
    """"If more than 10% of sessions are excluded (not counting
    market_closed) by the 30th session, v1 stops and the problem is fixed
    in a v2."

    Informational only — nothing in this codebase halts the job over this;
    it is a data-quality count (how many sessions, how many excluded and
    why), so it belongs in `status` same as the rest of this module.
    `market_closed` days are not real sessions (by definition — "no
    regular session that day"), so they are excluded from both the count
    of sessions and the count of exclusions here, not just the numerator.
    """
    real_sessions = [r for r in preopen if r.get("exclusion_reason") != EXCL_MARKET_CLOSED]
    n = len(real_sessions)
    if n < STOP_REVIEW_SESSION_COUNT:
        return None

    n_excluded = sum(1 for r in real_sessions if r.get("excluded"))
    pct = n_excluded / n
    if pct <= STOP_REVIEW_EXCLUSION_PCT:
        return None

    return (
        f"{n_excluded}/{n} sessions ({pct:.1%}) excluded, excluding "
        f"market_closed, at or past session {STOP_REVIEW_SESSION_COUNT} — "
        "pre-registration says v1 should stop and be fixed in a v2."
    )


def format_status(report: StatusReport) -> str:
    lines = [
        "paper_log gex_levels v1 — status (activity and data quality only)",
        f"  preopen records:   {report.sessions_preopen}",
        f"  postclose records: {report.sessions_postclose}",
        f"  valid sessions:    {report.valid_sessions}",
        "  excluded by reason:",
    ]
    if report.excluded_by_reason:
        for reason, count in sorted(report.excluded_by_reason.items()):
            lines.append(f"    {reason}: {count}")
    else:
        lines.append("    (none)")
    lines.extend([
        f"  real-level reaches:    {report.real_reaches}",
        f"  placebo-level reaches: {report.placebo_reaches}",
        f"  real H3 trades:        {report.real_trades}",
        f"  placebo H3 trades:     {report.placebo_trades}",
    ])
    if report.stop_review_advisory:
        lines.append(f"  ADVISORY: {report.stop_review_advisory}")
    return "\n".join(lines)
