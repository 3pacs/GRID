"""Dormant supplied-row temporal diagnostics. No readers, clocks or I/O.

Payload/provenance are opaque original bytes. The module neither authenticates
them nor calculates scores. See docs/temporal_feature_eligibility_contract.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

__all__ = (
    "Status",
    "SourceKind",
    "Missing",
    "FeatureKey",
    "TemporalPolicy",
    "Candidate",
    "Component",
    "Lineage",
    "Classification",
    "SelectionPolicy",
    "SelectionResult",
    "classify",
    "select_latest_as_of",
)


class Status(str, Enum):
    CURRENT = "current"
    STALE = "stale"
    KNOWN_FUTURE = "known_future"
    UNKNOWN = "unknown"


class SourceKind(str, Enum):
    OBSERVATION = "observation"
    PRECOMPUTED_EPHEMERIS = "precomputed_ephemeris"
    DERIVED_SCORE = "derived_score"


class Missing(Enum):
    RETRACTION_KNOWLEDGE = "missing_retraction_knowledge"


@dataclass(frozen=True)
class FeatureKey:
    feature_id: str
    source_id: str
    observation_id: str


@dataclass(frozen=True)
class TemporalPolicy:
    version: str
    feature_id: str
    source_id: str
    max_age: timedelta
    clock_roles: tuple[str, ...]
    retraction_semantics: str
    permit_precomputed_ephemeris: bool
    future_horizon: timedelta | None


@dataclass(frozen=True)
class Candidate:
    row_id: str
    key: FeatureKey
    source_version: str
    kind: SourceKind
    observation: datetime | None
    release: datetime | None
    available_at: datetime | None
    vintage: datetime | None
    retracted_at: datetime | None | Missing
    payload_json: bytes
    provenance_json: bytes
    computed_at: datetime | None = None
    lineage: Lineage | None = None


@dataclass(frozen=True)
class Component:
    component_id: str
    row: Candidate
    policy: TemporalPolicy | None


@dataclass(frozen=True)
class Lineage:
    model_id: str
    model_version: str
    declared_component_ids: tuple[str, ...]
    components: tuple[Component, ...]


@dataclass(frozen=True)
class Classification:
    row: Candidate
    status: Status
    reason: str
    cutoff: datetime
    policy_version: str | None
    component_diagnostics: tuple[Classification, ...] = ()


@dataclass(frozen=True)
class SelectionPolicy:
    version: str
    mode: str


@dataclass(frozen=True)
class SelectionResult:
    key: FeatureKey
    status: Status
    reason: str
    current: Candidate | None
    known_future: tuple[Candidate, ...]
    diagnostics: tuple[Classification, ...]
    history: tuple[Candidate, ...]
    cutoff: datetime
    selection_policy_version: str | None


_CLOCK_ROLES = ("observation", "release", "available_at", "vintage")


def _text(value: object) -> bool:
    return type(value) is str and bool(value.strip())


def _instant(value: object) -> datetime | None:
    # UTC conversion is essential: subtraction with the same ZoneInfo object
    # otherwise uses wall time and ignores fold transitions.
    if not isinstance(value, datetime):
        return None
    try:
        if value.utcoffset() is None:
            return None
        return value.astimezone(timezone.utc)
    except (ValueError, OverflowError, TypeError):
        return None


def _duration(value: object) -> bool:
    # bool, floats (including NaN/inf), days and strings are not durations.
    return type(value) is timedelta and value >= timedelta(0)


def _key_valid(key: object) -> bool:
    return type(key) is FeatureKey and all(
        _text(v) for v in (key.feature_id, key.source_id, key.observation_id)
    )


def _policy_valid(policy: object, key: FeatureKey) -> bool:
    return (
        type(policy) is TemporalPolicy
        and _text(policy.version)
        and _text(policy.feature_id)
        and _text(policy.source_id)
        and policy.feature_id == key.feature_id
        and policy.source_id == key.source_id
        and _duration(policy.max_age)
        and type(policy.clock_roles) is tuple
        and policy.clock_roles == _CLOCK_ROLES
        and policy.retraction_semantics == "knowledge_time"
        and type(policy.permit_precomputed_ephemeris) is bool
        and (policy.future_horizon is None or _duration(policy.future_horizon))
    )


def classify(
    row: Candidate, *, cutoff: datetime, policy: TemporalPolicy | None
) -> Classification:
    """Classify one immutable supplied row using explicit injected policy/time.

    Malformed fields produce unknown. Objects outside the Candidate API raise
    TypeError. Explicit None retraction means no known retraction; Missing does
    not. Future retraction knowledge is retained without rewriting history.
    """
    if type(row) is not Candidate:
        raise TypeError("row must be a Candidate")
    version = (
        policy.version
        if type(policy) is TemporalPolicy and _text(policy.version)
        else None
    )

    def result(
        status: Status, reason: str, components: tuple[Classification, ...] = ()
    ) -> Classification:
        return Classification(row, status, reason, cutoff, version, components)

    decision = _instant(cutoff)
    if decision is None:
        return result(Status.UNKNOWN, "invalid_cutoff")
    if (
        not _key_valid(row.key)
        or not _text(row.row_id)
        or not _text(row.source_version)
    ):
        return result(Status.UNKNOWN, "invalid_identity")
    if type(row.payload_json) is not bytes or type(row.provenance_json) is not bytes:
        return result(Status.UNKNOWN, "nonimmutable_original_bytes")
    if not _policy_valid(policy, row.key):
        return result(Status.UNKNOWN, "missing_or_invalid_policy")
    if type(row.kind) is not SourceKind:
        return result(Status.UNKNOWN, "undeclared_source_kind")
    observation = _instant(row.observation)
    if observation is None:
        return result(Status.UNKNOWN, "invalid_observation")
    # Do not replace one clock role by another, or assume chronological order.
    for role in ("release", "available_at", "vintage"):
        instant = _instant(getattr(row, role))
        if instant is None:
            return result(Status.UNKNOWN, "invalid_" + role)
        if instant > decision:
            return result(Status.UNKNOWN, "future_" + role)
    if row.retracted_at is not None:
        retraction = _instant(row.retracted_at)
        if retraction is None:
            return result(Status.UNKNOWN, "missing_or_invalid_retraction_knowledge")
        if retraction <= decision:
            return result(Status.UNKNOWN, "retracted_at_cutoff")

    if row.kind is SourceKind.PRECOMPUTED_EPHEMERIS:
        computed = _instant(row.computed_at)
        if computed is None or computed > decision:
            return result(Status.UNKNOWN, "missing_or_future_computation")
        if not row.provenance_json or not policy.permit_precomputed_ephemeris:
            return result(
                Status.UNKNOWN, "ephemeris_not_permitted_or_missing_provenance"
            )
        if not _duration(policy.future_horizon):
            return result(Status.UNKNOWN, "missing_future_horizon")
        if observation > decision:
            if observation - decision > policy.future_horizon:
                return result(Status.UNKNOWN, "outside_future_horizon")
            return result(Status.KNOWN_FUTURE, "precomputed_event_known_at_cutoff")

    if observation > decision:
        return result(Status.UNKNOWN, "future_observation")

    components: tuple[Classification, ...] = ()
    if row.kind is SourceKind.DERIVED_SCORE:
        lineage = row.lineage
        computed = _instant(row.computed_at)
        if (
            type(lineage) is not Lineage
            or not _text(lineage.model_id)
            or not _text(lineage.model_version)
            or type(lineage.declared_component_ids) is not tuple
            or not lineage.declared_component_ids
            or not all(_text(v) for v in lineage.declared_component_ids)
            or len(set(lineage.declared_component_ids))
            != len(lineage.declared_component_ids)
            or type(lineage.components) is not tuple
            or not all(
                type(v) is Component and _text(v.component_id)
                for v in lineage.components
            )
            or computed is None
            or computed > decision
            or not row.provenance_json
        ):
            return result(Status.UNKNOWN, "incomplete_score_lineage")
        ids = tuple(v.component_id for v in lineage.components)
        if len(set(ids)) != len(ids) or set(ids) != set(lineage.declared_component_ids):
            return result(Status.UNKNOWN, "incomplete_score_components")
        # This bounded API admits direct leaf support only; nested scores would
        # need an explicit separate graph contract, never implicit recursion.
        if any(
            type(v.row) is not Candidate
            or not _text(v.row.row_id)
            or v.row.kind is SourceKind.DERIVED_SCORE
            for v in lineage.components
        ):
            return result(Status.UNKNOWN, "nonleaf_score_component")
        if len({v.row.row_id for v in lineage.components}) != len(lineage.components):
            return result(Status.UNKNOWN, "duplicate_score_component_row")
        components = tuple(
            classify(v.row, cutoff=cutoff, policy=v.policy)
            for v in sorted(lineage.components, key=lambda v: v.component_id)
        )
        if any(v.status is not Status.CURRENT for v in components):
            return result(Status.UNKNOWN, "noncurrent_score_support", components)
        for v in lineage.components:
            # Declared support must also have been known when the score was
            # computed. Future-known support cannot retrospectively justify it.
            support_clocks = ("release", "available_at", "vintage")
            if v.row.kind is SourceKind.PRECOMPUTED_EPHEMERIS:
                support_clocks += ("computed_at",)
            if any(
                _instant(getattr(v.row, role)) > computed for role in support_clocks
            ):
                return result(
                    Status.UNKNOWN, "support_unknown_at_computation", components
                )

    # timedelta comparisons preserve microseconds even at very large ages.
    if decision - observation > policy.max_age:
        return result(Status.STALE, "exceeds_explicit_max_age", components)
    return result(Status.CURRENT, "within_explicit_max_age", components)


def select_latest_as_of(
    rows: tuple[Candidate, ...],
    *,
    key: FeatureKey,
    cutoff: datetime,
    policy: TemporalPolicy | None,
    selection_policy: SelectionPolicy | None,
) -> SelectionResult:
    """Filter every revision before selecting latest vintage within one key.

    Duplicate row IDs and tied eligible vintages are unknown. Input history
    retains original order; diagnostics and the future lane are deterministic.
    """
    if type(rows) is not tuple or not all(type(v) is Candidate for v in rows):
        raise TypeError("rows must be an immutable tuple of Candidates")
    # Preserve all candidate diagnostics, including losing and retracted rows.
    diagnostics = tuple(
        sorted(
            (classify(v, cutoff=cutoff, policy=policy) for v in rows),
            key=lambda v: v.row.row_id if _text(v.row.row_id) else "",
        )
    )
    version = (
        selection_policy.version
        if type(selection_policy) is SelectionPolicy and _text(selection_policy.version)
        else None
    )

    def result(
        reason: str,
        current: Candidate | None = None,
        future: tuple[Candidate, ...] = (),
    ) -> SelectionResult:
        return SelectionResult(
            key,
            Status.CURRENT if current is not None else Status.UNKNOWN,
            reason,
            current,
            future,
            diagnostics,
            rows,
            cutoff,
            version,
        )

    if not _key_valid(key) or _instant(cutoff) is None:
        return result("invalid_selection_key_or_cutoff")
    if (
        type(selection_policy) is not SelectionPolicy
        or version is None
        or selection_policy.mode != "LATEST_AS_OF"
    ):
        return result("missing_or_invalid_selection_policy")
    if any(v.key != key or not _text(v.row_id) for v in rows):
        return result("foreign_or_invalid_identity")
    ids = tuple(v.row_id for v in rows)
    if len(set(ids)) != len(ids):
        return result("duplicate_row_identity")
    observations = {
        _instant(v.observation) for v in rows if _instant(v.observation) is not None
    }
    if len(observations) > 1:
        return result("conflicting_observation_identity")
    eligible = tuple(v.row for v in diagnostics if v.status is Status.CURRENT)
    future = tuple(
        sorted(
            (v.row for v in diagnostics if v.status is Status.KNOWN_FUTURE),
            key=lambda v: (_instant(v.observation), _instant(v.vintage), v.row_id),
        )
    )
    if not eligible:
        return result("no_current_candidate", future=future)
    latest = max(_instant(v.vintage) for v in eligible)
    winners = tuple(v for v in eligible if _instant(v.vintage) == latest)
    if len(winners) != 1:
        return result("conflicting_latest_vintage", future=future)
    return result("latest_eligible_vintage", winners[0], future)
