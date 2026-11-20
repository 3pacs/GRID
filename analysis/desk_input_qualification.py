"""Pure factory-3 eligibility prerequisite; no reads, signals or runtime wiring.

Availability means actual capture/ingestion, not a modeled publication lag.
A calendar with zero events is not proof of a flow-only day. Coverage is always
source-specific and cannot establish absence of unscheduled news.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import isfinite


@dataclass(frozen=True)
class Eligibility:
    eligible: bool
    reasons: tuple[str, ...]
    source: str
    evidence_id: str


def _aware(value):
    return isinstance(value, datetime) and value.utcoffset() is not None


def _numeric(value):
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return isfinite(value)
    except OverflowError:
        return False


def _base(source, evidence_id, available_at, decision_at):
    reasons = []
    if not isinstance(source, str) or not source.strip():
        reasons.append("source_missing")
    if not isinstance(evidence_id, str) or not evidence_id.strip():
        reasons.append("evidence_missing")
    if not _aware(decision_at):
        raise ValueError("decision_at must be timezone aware")
    if not _aware(available_at):
        reasons.append("availability_unknown")
    elif available_at > decision_at:
        reasons.append("not_yet_available")
    return reasons


def qualify_tape(*, source, evidence_id, available_at, decision_at,
                 bar_start, bar_end, bid, ask, volume, volume_kind,
                 max_age: timedelta) -> Eligibility:
    """Require a completed fresh bar and interval volume; no aggressor inference.

    bid/ask are aligned observations of the same supplied interval, not execution
    guarantees. Callers must enforce symbol/session identity and contiguous window
    coverage before calculating any rolling feature.
    """
    if not isinstance(max_age, timedelta) or max_age < timedelta(0):
        raise ValueError("max_age must be a nonnegative timedelta")
    reasons = _base(source, evidence_id, available_at, decision_at)
    if not _aware(bar_start) or not _aware(bar_end):
        reasons.append("bar_clock_unknown")
    elif bar_end <= bar_start:
        reasons.append("invalid_bar_interval")
    else:
        if bar_end > decision_at:
            reasons.append("bar_incomplete")
        elif decision_at - bar_end > max_age:
            reasons.append("bar_stale")
        if _aware(available_at) and available_at < bar_end:
            reasons.append("capture_before_completion")
    if not _numeric(bid) or not _numeric(ask) or bid <= 0 or ask < bid:
        reasons.append("invalid_quote")
    if not _numeric(volume) or volume < 0:
        reasons.append("invalid_volume")
    if volume_kind != "interval":
        reasons.append("volume_semantics_unqualified")
    return Eligibility(not reasons, tuple(reasons), source, evidence_id)


def qualify_calendar(*, source, evidence_id, available_at, decision_at,
                     coverage_start, coverage_end, window_start, window_end,
                     coverage_complete, version, event_times,
                     max_age: timedelta) -> Eligibility:
    """Qualify one immutable schedule snapshot for a requested half-open window.

    Event times may be future: the schedule must already be available at decision.
    Empty event_times is meaningful only for this source's declared complete
    coverage. No inference about quiet/flow-only market conditions is made.
    """
    if not isinstance(max_age, timedelta) or max_age < timedelta(0):
        raise ValueError("max_age must be a nonnegative timedelta")
    reasons = _base(source, evidence_id, available_at, decision_at)
    if not isinstance(version, str) or not version.strip():
        reasons.append("version_missing")
    if coverage_complete is not True:
        reasons.append("coverage_incomplete")
    clocks = (coverage_start, coverage_end, window_start, window_end)
    if not all(_aware(t) for t in clocks):
        reasons.append("coverage_clock_unknown")
    elif coverage_end <= coverage_start or window_end <= window_start:
        reasons.append("invalid_coverage_interval")
    elif coverage_start > window_start or coverage_end < window_end:
        reasons.append("window_not_covered")
    if _aware(available_at) and available_at <= decision_at and decision_at - available_at > max_age:
        reasons.append("calendar_stale")
    if not isinstance(event_times, (list, tuple)):
        reasons.append("events_unknown")
    elif not all(_aware(t) for t in event_times):
        reasons.append("event_clock_unknown")
    elif all(_aware(t) for t in (coverage_start, coverage_end)) and any(
            t < coverage_start or t >= coverage_end for t in event_times):
        reasons.append("event_outside_coverage")
    return Eligibility(not reasons, tuple(reasons), source, evidence_id)
