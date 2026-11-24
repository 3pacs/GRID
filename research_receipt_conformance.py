"""Fixture-only D3/D4 receipt semantics; not admission, real custody or a scorer."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime


def _time(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("an aware timestamp is required")
    return value.astimezone(UTC)


def _digest(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _id(value):
    return isinstance(value, str) and bool(value.strip())


@dataclass(frozen=True)
class FrozenRun:
    """Pre-outcome identity, selected elsewhere under M2 custody rules."""

    trial_id: str
    manifest_sha256: str
    run_id: int
    review_sha256: str
    frozen_at: datetime


@dataclass(frozen=True)
class RunReceipt:
    trial_id: str
    manifest_sha256: str
    run_id: int
    review_sha256: str
    witnessed_at: datetime


def check_frozen_run(pin, receipts):
    """Check preselected identity; never choose latest, fall back, or inspect payloads."""
    frozen_at = _time(pin.frozen_at)
    if (not _id(pin.trial_id) or type(pin.run_id) is not int or pin.run_id <= 0
            or not _digest(pin.manifest_sha256) or not _digest(pin.review_sha256)):
        raise ValueError("invalid frozen run identity")
    for receipt in receipts:
        if (receipt.trial_id == pin.trial_id and type(receipt.run_id) is int and receipt.run_id == pin.run_id
                and receipt.review_sha256 == pin.review_sha256
                and receipt.manifest_sha256 == pin.manifest_sha256):
            try:
                if _time(receipt.witnessed_at) <= frozen_at:
                    return "FIXTURE_IDENTITY_MATCH"
            except (TypeError, ValueError, OverflowError):
                continue
    return "HOLD_NO_FROZEN_RUN_RECEIPT"


@dataclass(frozen=True)
class Opportunity:
    """Expected opportunity from a separately frozen calendar/cohort."""

    opportunity_id: str
    decision_cutoff: datetime


@dataclass(frozen=True)
class OpportunityReceipt:
    """Pre-outcome receipt metadata from the existing custody owner."""

    opportunity_id: str
    manifest_sha256: str
    disposition: str
    receipt_sha256: str
    decision_at: datetime
    witnessed_at: datetime


def reconcile_opportunities(expected, receipts, *, manifest_sha256, frozen_at, audit_at):
    """Count every expected ID exactly once, without selecting rows by result.

    Witnessed_at must be at or before the individual cutoff, not merely the audit.
    An early receipt is provisional until the cutoff, never final coverage.
    Before the first cutoff the caller must already have frozen the opportunity
    universe and manifest. Neither witness authenticity nor roster completeness
    against an exchange calendar can be established by this fixture consumer.
    """
    if not _digest(manifest_sha256):
        raise ValueError("invalid frozen manifest digest")
    frozen, audit = _time(frozen_at), _time(audit_at)
    if audit < frozen:
        raise ValueError("audit predates freeze")
    cutoffs = {}
    for item in expected:
        cutoff = _time(item.decision_cutoff)
        if not _id(item.opportunity_id) or item.opportunity_id in cutoffs or cutoff <= frozen:
            raise ValueError("invalid, duplicate, or not prospectively frozen opportunity")
        cutoffs[item.opportunity_id] = cutoff
    if not cutoffs:
        return {"status": "HOLD_EMPTY_ROSTER", "opportunities": {}, "ignored": {}}

    reconciled, conflicts, ignored = {}, set(), {}

    def ignore(reason):
        ignored[reason] = ignored.get(reason, 0) + 1

    for receipt in receipts:
        try:
            witness = _time(receipt.witnessed_at)
        except (TypeError, ValueError, OverflowError):
            ignore("invalid_clock")
            continue
        if witness > audit:
            # Later rows may not change any audit-visible count.
            continue
        if receipt.opportunity_id not in cutoffs:
            ignore("unexpected_opportunity")
            continue
        if receipt.manifest_sha256 != manifest_sha256:
            ignore("manifest_mismatch")
            continue
        if receipt.disposition not in {"DECISION", "NONTRADE", "UNAVAILABLE"} or not _digest(receipt.receipt_sha256):
            ignore("invalid_receipt")
            continue
        try:
            decision = _time(receipt.decision_at)
        except (TypeError, ValueError, OverflowError):
            ignore("invalid_clock")
            continue
        if not frozen < decision <= witness <= cutoffs[receipt.opportunity_id]:
            ignore("not_visible_by_decision_cutoff")
            continue
        # Same immutable receipt replay is idempotent; differing receipt or
        # metadata for a single opportunity may not be silently selected.
        value = (receipt.receipt_sha256, receipt.disposition, decision)
        prior = reconciled.setdefault(receipt.opportunity_id, value)
        if prior != value:
            conflicts.add(receipt.opportunity_id)

    states = {}
    for opportunity_id, cutoff in sorted(cutoffs.items()):
        if opportunity_id in conflicts:
            states[opportunity_id] = "CONFLICT"
        elif opportunity_id in reconciled:
            states[opportunity_id] = "PROVISIONAL" if audit < cutoff else reconciled[opportunity_id][1]
        else:
            states[opportunity_id] = "PENDING" if audit < cutoff else "MISSING"
    return {"status": "HOLD_INCOMPLETE" if ignored or {"MISSING", "CONFLICT", "PENDING", "PROVISIONAL"} & set(states.values())
            else "FIXTURE_RECONCILED", "opportunities": states, "ignored": dict(sorted(ignored.items()))}
