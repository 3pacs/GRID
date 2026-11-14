"""Offline desk evidence labels; no promotion, data reads or scheduling.

Adapters must supply receipt identifiers and actual availability clocks. A label
is not verification of a receipt. Unknown clocks stay unknown; accumulated sample
count alone never changes an unproven candidate into a validated signal.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum


class EvidenceKind(str, Enum):
    FORWARD = "forward"
    DEVELOPMENT = "development"
    LEAD = "lead"


@dataclass(frozen=True)
class DeskEvidence:
    item_id: str
    kind: EvidenceKind
    receipt_id: str
    observed_at: datetime | None
    available_at: datetime | None
    samples: int
    required_samples: int = 100
    declared_test: str | None = None


def _clock(value: datetime | None) -> None:
    if value is not None and (
        not isinstance(value, datetime) or value.utcoffset() is None
    ):
        raise ValueError("clocks must be timezone-aware")


def evidence_card(item: DeskEvidence, *, as_of: datetime, max_age: timedelta) -> dict:
    """Return machine-readable conservative labels at an explicit clock.

    max_age is the caller's declared source-specific policy, not a global implied
    cadence. Future/unclocked/stale receipts cannot supply current signal evidence.
    Source payload or arbitrary narrative never becomes a headline here.
    """
    _clock(as_of)
    if as_of is None or not isinstance(max_age, timedelta) or max_age <= timedelta(0):
        raise ValueError("as_of and positive freshness policy required")
    if not isinstance(item.kind, EvidenceKind):
        raise ValueError("unknown evidence kind")
    for value in (item.item_id, item.receipt_id):
        if not isinstance(value, str) or not value.strip() or len(value) > 512:
            raise ValueError("bounded nonempty identity required")
    for value in (item.samples, item.required_samples):
        if type(value) is not int or value < 0:
            raise ValueError("sample counts must be nonnegative integers")
    if item.required_samples == 0:
        raise ValueError("positive sample requirement required")
    if item.declared_test is not None and (
        not isinstance(item.declared_test, str)
        or not item.declared_test.strip()
        or len(item.declared_test) > 2048
    ):
        raise ValueError("declared test must be a bounded nonempty identifier or text")
    _clock(item.observed_at)
    _clock(item.available_at)
    reasons = []
    if item.observed_at is None or item.available_at is None:
        reasons.append("missing_clock")
    else:
        if item.available_at < item.observed_at:
            reasons.append("availability_before_observation")
        if max(item.observed_at, item.available_at) > as_of:
            reasons.append("future_evidence")
        if as_of - item.observed_at > max_age:
            reasons.append("stale_observation")
    if item.kind == EvidenceKind.LEAD and not item.declared_test:
        reasons.append("undeclared_test")
    if item.kind == EvidenceKind.FORWARD:
        label = f"unproven, {item.samples} of {item.required_samples}"
        if item.samples >= item.required_samples:
            label += "; sample requirement met, validation pending"
    elif item.kind == EvidenceKind.DEVELOPMENT:
        label = "development-only; not forward evidence"
    else:
        label = "research lead; not a finding"
    return {
        "schema": "desk-evidence-card-v1",
        "item_id": item.item_id,
        "receipt_id": item.receipt_id,
        "kind": item.kind.value,
        "as_of": as_of.isoformat(),
        "observed_at": item.observed_at.isoformat() if item.observed_at else None,
        "available_at": item.available_at.isoformat() if item.available_at else None,
        "max_age_seconds": max_age.total_seconds(),
        "label": label,
        "samples": item.samples,
        "required_samples": item.required_samples,
        "declared_test": item.declared_test,
        "quality": "withheld" if reasons else "clock_eligible",
        "reasons": reasons,
        "promotion_allowed": False,
    }
