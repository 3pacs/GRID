"""Pure #1191 planning policy. Not connected to a writer, timer or provider.

Adapters must validate immutable receipt bytes before constructing evidence.
Calendar sessions and source-specific expected observation dates are explicit;
this module deliberately does not guess publication lags or infer PIT lineage.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import re
from typing import Mapping, Sequence


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-aware timestamp required")
    return value.astimezone(timezone.utc)


def cutoff(target: date) -> datetime:
    """Existing market-state convention: exclusive next-day UTC midnight."""
    return datetime.combine(target + timedelta(days=1), time(), timezone.utc)


@dataclass(frozen=True)
class SourceRequirement:
    source: str
    expected_observation: date
    # Source schedule/vintage selection, not same-day freshness for every series.
    policy_id: str


@dataclass(frozen=True)
class SourceEvidence:
    source: str
    observation: date
    published_at: datetime
    captured_at: datetime
    receipt_sha256: str
    # Set only by adapter after checking actual bytes, source identity and selection.
    receipt_verified: bool = False
    vintage_verified: bool = False


def assess_source(requirement: SourceRequirement, evidence: SourceEvidence | None,
                  target: date, now: datetime) -> dict:
    """Separate selected-vintage replay readiness from contemporaneous capture.

    An expected lagged weekly/monthly observation is legitimate. A newly revised
    old observation, a timestamp or a hash string alone does not prove PIT.
    """
    now = _utc(now)
    end = cutoff(target)
    base = {"source": requirement.source, "policy_id": requirement.policy_id,
            "expected_observation": requirement.expected_observation.isoformat(),
            "ready": False, "captured_before_cutoff": False}
    if not requirement.source or not requirement.policy_id or requirement.expected_observation > target:
        raise ValueError("invalid source requirement")
    if now < end:
        return {**base, "reason": "session_cutoff_not_reached"}
    if evidence is None:
        return {**base, "reason": "missing_receipt"}
    if evidence.source != requirement.source:
        return {**base, "reason": "source_mismatch"}
    if evidence.observation != requirement.expected_observation:
        return {**base, "reason": "wrong_observation"}
    if not evidence.receipt_verified or not re.fullmatch(r"[0-9a-f]{64}", evidence.receipt_sha256):
        return {**base, "reason": "unverified_receipt"}
    if not evidence.vintage_verified:
        return {**base, "reason": "unverified_vintage"}
    published, captured = _utc(evidence.published_at), _utc(evidence.captured_at)
    if published >= end:
        return {**base, "reason": "published_after_cutoff"}
    if published > captured or captured > now:
        return {**base, "reason": "inconsistent_capture_time"}
    return {**base, "ready": True, "reason": "verified_selected_vintage",
            "captured_before_cutoff": captured < end,
            "observation": evidence.observation.isoformat(),
            "published_at": published.isoformat(), "captured_at": captured.isoformat(),
            "receipt_sha256": evidence.receipt_sha256}


def plan_sessions(*, now: datetime, start: date, end: date,
                  calendar_sessions: Sequence[date], calendar_sha256: str,
                  calendar_verified: bool, calendar_start: date, calendar_end: date,
                  existing: set[date], requirements: Mapping[date, Sequence[SourceRequirement]],
                  evidence: Mapping[date, Mapping[str, SourceEvidence]], limit: int = 5) -> list[dict]:
    """Bounded insert-only catch-up plan over an explicitly covered calendar.

    Every session in the requested range gets an outcome. Holes never advance a
    watermark. Existing rows are preserved and never credited to this run.
    This returns a plan only; a DB writer must still use ON CONFLICT DO NOTHING.
    """
    now = _utc(now)
    if (not calendar_verified or not re.fullmatch(r"[0-9a-f]{64}", calendar_sha256)
            or calendar_start > start or calendar_end < end):
        raise ValueError("verified calendar covering requested range required")
    if start > end or (end - start).days > 366 or not 1 <= limit <= 20:
        raise ValueError("invalid or unbounded planning range/limit")
    days = list(calendar_sessions)
    if days != sorted(set(days)) or any(d < calendar_start or d > calendar_end for d in days):
        raise ValueError("calendar must be unique, ordered and inside declared coverage")
    rows, selected = [], 0
    for target in (d for d in days if start <= d <= end):
        row = {"target_session": target.isoformat(), "cutoff_exclusive": cutoff(target).isoformat(),
               "calendar_sha256": calendar_sha256, "forward_qualified": False}
        if target in existing:
            row["outcome"] = "already_present"
        elif cutoff(target) > now:
            row["outcome"] = "not_due"
        else:
            reqs = requirements.get(target, ())
            names = [r.source for r in reqs]
            if not reqs or len(names) != len(set(names)):
                row["outcome"] = "held_missing_or_duplicate_policy"
            else:
                checks = [assess_source(r, evidence.get(target, {}).get(r.source), target, now) for r in reqs]
                row["sources"] = checks
                if not all(c["ready"] for c in checks):
                    row["outcome"] = "held_source_readiness"
                elif selected == limit:
                    row["outcome"] = "deferred_batch_limit"
                else:
                    row["outcome"] = "eligible_insert"
                    row["capture_class"] = ("contemporaneous_inputs" if all(c["captured_before_cutoff"] for c in checks)
                                            else "historical_replay")
                    selected += 1
        rows.append(row)
    return rows
