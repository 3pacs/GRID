"""Pure synthetic ETF observation quality. No feed collection or admission authority."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from math import isfinite
from types import MappingProxyType
from typing import Mapping
from zoneinfo import ZoneInfo


INSTRUMENTS = ("TEST:SPY", "TEST:QQQ")
PIN_KEYS = (
    "code_sha256",
    "config_sha256",
    "calendar_sha256",
    "outcome_definition_sha256",
)


class State(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    UNKNOWN = "unknown"
    NOT_REQUIRED = "not_required"


class Presence(str, Enum):
    PRESENT = "present"
    MISSING = "missing"
    UNAVAILABLE = "unavailable"


class Finality(str, Enum):
    FINAL = "final"
    PROVISIONAL = "provisional"
    INCOMPLETE = "incomplete"
    UNKNOWN = "unknown"


class Mode(str, Enum):
    RESEARCH = "research"
    CURRENT = "current"
    ZERO_DTE = "zero_dte"


class Check(str, Enum):
    POLICY = "policy"
    PRESENCE = "presence"
    IDENTITY = "identity"
    EVIDENCE = "evidence"
    CUTOFF = "cutoff"
    CLOCK_ORDER = "clock_order"
    FALLBACK = "fallback"
    EVENT_AGE = "event_age"
    RECEIPT_AGE = "receipt_age"
    DELAY = "delay"
    FINALITY = "finality"
    INTERVAL = "interval"
    COVERAGE = "coverage"
    CONTINUITY = "continuity"
    LIFECYCLE = "lifecycle"
    SESSION = "session"
    OI_VINTAGE = "oi_vintage"
    EXPIRY_SETTLEMENT = "expiry_settlement"
    GREEKS = "greeks"
    PUBLICATION_PINS = "publication_pins"
    ZERO_DTE_HOLD = "zero_dte_hold"


MANDATORY = frozenset(
    (
        Check.POLICY,
        Check.PRESENCE,
        Check.IDENTITY,
        Check.EVIDENCE,
        Check.CUTOFF,
        Check.CLOCK_ORDER,
        Check.FALLBACK,
    )
)


def _text(value: object, *, nullable: bool = True) -> None:
    if value is None and nullable:
        return
    if (
        type(value) is not str
        or not 1 <= len(value) <= 128
        or any(
            c
            not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:._-/"
            for c in value
        )
    ):
        raise ValueError("invalid_identity")


def _hash(value: object) -> None:
    if value is not None and (
        type(value) is not str
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValueError("invalid_hash_reference")


def _clock(value: object, *, nullable: bool = True) -> None:
    if value is None and nullable:
        return
    if type(value) is not datetime:
        raise ValueError("invalid_clock")
    if type(value.tzinfo) not in (timezone, ZoneInfo):
        raise ValueError("invalid_clock")
    try:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("invalid_clock")
        value.astimezone(timezone.utc)
    except (ValueError, OverflowError, TypeError):
        raise ValueError("invalid_clock") from None


def _utc(value: datetime) -> datetime:
    # UTC normalization also avoids same-tz DST fold/wall-time arithmetic.
    return value.astimezone(timezone.utc)


def _number(value: object, *, fraction: bool = False, positive: bool = False) -> None:
    if value is None:
        return
    if (
        type(value) not in (int, float)
        or (type(value) is float and not isfinite(value))
        or value < 0
        or (fraction and value > 1)
        or (positive and value == 0)
    ):
        raise ValueError("invalid_numeric_fact")


def _boolean(value: object) -> None:
    if value is not None and type(value) is not bool:
        raise ValueError("invalid_boolean_fact")


def _typed(value: object, cls: type, *, nullable: bool = True) -> None:
    if value is None and nullable:
        return
    if type(value) is not cls:
        raise ValueError("invalid_fact_type")


@dataclass(frozen=True)
class SourceIdentity:
    instrument: str
    source: str | None
    tool: str | None
    field: str | None
    adapter_version: str | None

    def __post_init__(self) -> None:
        _text(self.instrument, nullable=False)
        if self.instrument not in INSTRUMENTS:
            raise ValueError("synthetic_instrument_required")
        for value in (self.source, self.tool, self.field, self.adapter_version):
            _text(value)


@dataclass(frozen=True)
class FactReference:
    """Opaque provenance and its own knowledge clocks; no payload/authentication."""

    evidence_id: str | None
    sha256: str | None
    available_at: datetime | None
    recorded_at: datetime | None

    def __post_init__(self) -> None:
        _text(self.evidence_id)
        _hash(self.sha256)
        _clock(self.available_at)
        _clock(self.recorded_at)


@dataclass(frozen=True)
class DelayEvidence:
    scope: SourceIdentity
    seconds: int | float | None
    reference: FactReference | None

    def __post_init__(self) -> None:
        _typed(self.scope, SourceIdentity, nullable=False)
        _number(self.seconds)
        _typed(self.reference, FactReference)


@dataclass(frozen=True)
class LifecycleFacts:
    connected: bool | None
    observation_generation: str | None
    current_generation: str | None
    reference: FactReference | None

    def __post_init__(self) -> None:
        _boolean(self.connected)
        _text(self.observation_generation)
        _text(self.current_generation)
        _typed(self.reference, FactReference)


@dataclass(frozen=True)
class OptionsFacts:
    oi_vintage_at: datetime | None
    expiry_settlement_verified: bool | None
    greeks_verified: bool | None
    reference: FactReference | None

    def __post_init__(self) -> None:
        _clock(self.oi_vintage_at)
        _boolean(self.expiry_settlement_verified)
        _boolean(self.greeks_verified)
        _typed(self.reference, FactReference)


@dataclass(frozen=True)
class SessionFacts:
    instrument: str
    state: str | None
    reference: FactReference | None

    def __post_init__(self) -> None:
        if type(self.instrument) is not str or self.instrument not in INSTRUMENTS:
            raise ValueError("synthetic_instrument_required")
        _text(self.state)
        _typed(self.reference, FactReference)


@dataclass(frozen=True)
class Observation:
    identity: SourceIdentity
    evidence_id: str | None
    evidence_sha256: str | None
    event_at: datetime | None
    receipt_at: datetime | None
    as_of_at: datetime | None
    available_at: datetime | None
    recorded_at: datetime | None
    presence: Presence
    delay: DelayEvidence | None = None
    interval_seconds: int | float | None = None
    finality: Finality = Finality.UNKNOWN
    revision_id: str | None = None
    revises_evidence_id: str | None = None
    coverage_fraction: int | float | None = None
    continuous: bool | None = None
    lifecycle: LifecycleFacts | None = None
    options: OptionsFacts | None = None
    is_fallback: bool | None = None
    fallback_reference: FactReference | None = None

    def __post_init__(self) -> None:
        _typed(self.identity, SourceIdentity, nullable=False)
        _text(self.evidence_id)
        _hash(self.evidence_sha256)
        for stamp in (
            self.event_at,
            self.receipt_at,
            self.as_of_at,
            self.available_at,
            self.recorded_at,
        ):
            _clock(stamp)
        _typed(self.presence, Presence, nullable=False)
        _typed(self.finality, Finality, nullable=False)
        _typed(self.delay, DelayEvidence)
        _number(self.interval_seconds, positive=True)
        _text(self.revision_id)
        _text(self.revises_evidence_id)
        _number(self.coverage_fraction, fraction=True)
        _boolean(self.continuous)
        _typed(self.lifecycle, LifecycleFacts)
        _typed(self.options, OptionsFacts)
        _boolean(self.is_fallback)
        _typed(self.fallback_reference, FactReference)


@dataclass(frozen=True)
class CapabilityPolicy:
    name: str
    expected_identity: SourceIdentity
    evidence_id: str | None
    mode: Mode
    required_checks: tuple[Check, ...] | None
    expected_evidence_sha256: str | None = None
    max_event_age_seconds: int | float | None = None
    max_receipt_age_seconds: int | float | None = None
    max_future_skew_seconds: int | float | None = None
    max_delay_seconds: int | float | None = None
    required_interval_seconds: int | float | None = None
    min_coverage_fraction: int | float | None = None
    max_oi_age_seconds: int | float | None = None
    allowed_session_states: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        _text(self.name, nullable=False)
        _typed(self.expected_identity, SourceIdentity, nullable=False)
        _text(self.evidence_id)
        _hash(self.expected_evidence_sha256)
        _typed(self.mode, Mode, nullable=False)
        if self.required_checks is not None:
            if (
                type(self.required_checks) is not tuple
                or any(type(c) is not Check for c in self.required_checks)
                or len(set(self.required_checks)) != len(self.required_checks)
            ):
                raise ValueError("invalid_required_checks")
        for value in (
            self.max_event_age_seconds,
            self.max_receipt_age_seconds,
            self.max_future_skew_seconds,
            self.max_delay_seconds,
            self.max_oi_age_seconds,
        ):
            _number(value)
        _number(self.required_interval_seconds, positive=True)
        _number(self.min_coverage_fraction, fraction=True)
        if self.allowed_session_states is not None:
            if type(self.allowed_session_states) is not tuple:
                raise ValueError("invalid_session_policy")
            for value in self.allowed_session_states:
                _text(value, nullable=False)


def _freeze_pins(value: object) -> Mapping[str, str | None] | None:
    if value is None:
        return None
    if type(value) not in (dict, MappingProxyType):
        raise ValueError("invalid_policy_provenance")
    for key, pin in value.items():
        if type(key) is not str or key not in PIN_KEYS:
            raise ValueError("invalid_policy_provenance")
        _hash(pin)
    return MappingProxyType(dict(value))


@dataclass(frozen=True)
class EvaluationPolicy:
    version: str | None
    provenance: Mapping[str, str | None] | None
    capabilities: tuple[CapabilityPolicy, ...]
    frozen_publication_provenance: Mapping[str, str | None] | None = None

    def __post_init__(self) -> None:
        _text(self.version)
        object.__setattr__(self, "provenance", _freeze_pins(self.provenance))
        object.__setattr__(
            self,
            "frozen_publication_provenance",
            _freeze_pins(self.frozen_publication_provenance),
        )
        if type(self.capabilities) is not tuple or not self.capabilities:
            raise ValueError("invalid_capabilities")
        for cap in self.capabilities:
            _typed(cap, CapabilityPolicy, nullable=False)
        keys = [(c.expected_identity.instrument, c.name) for c in self.capabilities]
        if len(set(keys)) != len(keys):
            raise ValueError("duplicate_capability")


@dataclass(frozen=True)
class CheckResult:
    check: Check
    state: State
    reason: str


@dataclass(frozen=True)
class DescriptiveContext:
    observation: Observation
    visible: bool
    knowledge_state: State
    delay_label: str
    event_age_seconds: float | None
    receipt_age_seconds: float | None
    decision_event_age_seconds: float | None
    decision_receipt_age_seconds: float | None


@dataclass(frozen=True)
class CapabilityResult:
    name: str
    mode: Mode
    evidence_id: str | None
    evidence_sha256: str | None
    state: State
    eligible: bool
    checks: tuple[CheckResult, ...]


@dataclass(frozen=True)
class InstrumentResult:
    instrument: str
    context: tuple[DescriptiveContext, ...]
    capabilities: tuple[CapabilityResult, ...]


@dataclass(frozen=True)
class EvaluationResult:
    policy_version: str | None
    evaluated_at: datetime
    decision_at: datetime
    instruments: tuple[InstrumentResult, ...]


def _age(stamp: datetime | None, clock: datetime) -> float | None:
    return None if stamp is None else (_utc(clock) - _utc(stamp)).total_seconds()


def _elapsed_microseconds(stamp: datetime, clock: datetime) -> int:
    delta = _utc(clock) - _utc(stamp)
    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds


def _exceeds_seconds(microseconds: int, seconds: int | float) -> bool:
    # Preserve the exact admitted policy value without rounding a timedelta
    # or constructing one from arbitrarily large numeric policy durations.
    numerator, denominator = (
        seconds.as_integer_ratio() if type(seconds) is float else (seconds, 1)
    )
    return microseconds * denominator > numerator * 1000000


def _reference(ref: FactReference | None, cutoff: datetime) -> tuple[State, str]:
    if ref is None or ref.evidence_id is None or ref.sha256 is None:
        return State.UNKNOWN, "missing_fact_provenance"
    if ref.available_at is None or ref.recorded_at is None:
        return State.UNKNOWN, "missing_fact_knowledge_clock"
    if _utc(ref.available_at) > _utc(ref.recorded_at):
        return State.FAIL, "fact_clock_order"
    if _utc(ref.available_at) > cutoff or _utc(ref.recorded_at) > cutoff:
        return State.FAIL, "fact_after_decision"
    return State.PASS, "supplied_fact_reference"


def _pins_complete(
    pins: Mapping[str, str | None] | None, keys: tuple[str, ...]
) -> bool:
    return pins is not None and all(pins.get(k) is not None for k in keys)


def _source_complete(identity: SourceIdentity) -> bool:
    return all(
        v is not None
        for v in (
            identity.source,
            identity.tool,
            identity.field,
            identity.adapter_version,
        )
    )


def _age_check(
    stamp: datetime | None,
    clock: datetime,
    maximum: int | float | None,
    skew: int | float | None,
) -> tuple[State, str]:
    if maximum is None or skew is None:
        return State.UNKNOWN, "unconfigured_age_or_skew_policy"
    if stamp is None:
        return State.UNKNOWN, "missing_economic_or_receipt_clock"
    age = _elapsed_microseconds(stamp, clock)
    if _exceeds_seconds(-age, skew):
        return State.FAIL, "future_clock_beyond_skew"
    if _exceeds_seconds(age, maximum):
        return State.FAIL, "age_exceeds_policy"
    return State.PASS, "within_supplied_age_policy"


def _check(
    check: Check,
    obs: Observation | None,
    cap: CapabilityPolicy,
    policy: EvaluationPolicy,
    now: datetime,
    decision: datetime,
    session: SessionFacts | None,
    ambiguous: bool,
) -> tuple[State, str]:
    if check is Check.POLICY:
        if policy.version is None or not _pins_complete(
            policy.provenance, PIN_KEYS[:2]
        ):
            return State.UNKNOWN, "missing_policy_version_or_provenance"
        if cap.required_checks is None or not cap.required_checks:
            return State.UNKNOWN, "unconfigured_required_checks"
        if cap.evidence_id is None:
            return State.UNKNOWN, "unconfigured_evidence_selection"
        return State.PASS, "explicit_research_policy"
    if check is Check.ZERO_DTE_HOLD:
        return State.FAIL, "zero_dte_held"
    if check is Check.PUBLICATION_PINS:
        if not (
            _pins_complete(policy.provenance, PIN_KEYS)
            and _pins_complete(policy.frozen_publication_provenance, PIN_KEYS)
        ):
            return State.UNKNOWN, "missing_literal_publication_pins"
        if policy.provenance != policy.frozen_publication_provenance:
            return State.FAIL, "changed_publication_pins"
        return State.PASS, "matching_supplied_publication_pins"
    if ambiguous:
        return State.FAIL, "ambiguous_evidence_identity"
    if obs is None:
        return State.UNKNOWN, "missing_selected_observation"
    if check is Check.PRESENCE:
        if obs.presence is Presence.UNAVAILABLE:
            return State.FAIL, "observation_unavailable"
        if obs.presence is Presence.MISSING:
            return State.UNKNOWN, "observation_missing"
        return State.PASS, "observation_present"
    if check is Check.IDENTITY:
        if not _source_complete(obs.identity) or not _source_complete(
            cap.expected_identity
        ):
            return State.UNKNOWN, "missing_source_identity"
        if obs.identity != cap.expected_identity:
            return State.FAIL, "source_identity_mismatch"
        return State.PASS, "matching_supplied_source_identity"
    if check is Check.EVIDENCE:
        if obs.evidence_id is None or obs.evidence_sha256 is None:
            return State.UNKNOWN, "missing_evidence_provenance"
        if cap.expected_evidence_sha256 is None:
            return State.UNKNOWN, "unconfigured_evidence_hash_pin"
        if obs.evidence_sha256 != cap.expected_evidence_sha256:
            return State.FAIL, "evidence_hash_pin_mismatch"
        return State.PASS, "opaque_evidence_reference"
    if check is Check.CUTOFF:
        if any(s is None for s in (obs.receipt_at, obs.available_at, obs.recorded_at)):
            return State.UNKNOWN, "missing_knowledge_clock"
        if any(
            _utc(s) > decision
            for s in (
                obs.event_at,
                obs.as_of_at,
                obs.available_at,
                obs.receipt_at,
                obs.recorded_at,
            )
            if s is not None
        ):
            return State.FAIL, "observation_after_decision"
        return State.PASS, "known_by_original_decision"
    if check is Check.CLOCK_ORDER:
        if any(s is None for s in (obs.available_at, obs.receipt_at, obs.recorded_at)):
            return State.UNKNOWN, "missing_knowledge_clock"
        available, receipt, recorded = map(
            _utc, (obs.available_at, obs.receipt_at, obs.recorded_at)
        )
        if not available <= receipt <= recorded:
            return State.FAIL, "knowledge_clock_order"
        if (
            obs.event_at is not None
            and obs.as_of_at is not None
            and _utc(obs.event_at) > _utc(obs.as_of_at)
        ):
            return State.FAIL, "event_as_of_order"
        ahead = max(
            _elapsed_microseconds(available, s)
            for s in (obs.event_at, obs.as_of_at, obs.available_at)
            if s is not None
        )
        if ahead > 0:
            if cap.max_future_skew_seconds is None:
                return State.UNKNOWN, "unconfigured_clock_skew_policy"
            if _exceeds_seconds(ahead, cap.max_future_skew_seconds):
                return State.FAIL, "source_clock_order_beyond_skew"
            return State.PASS, "supplied_source_skew_tolerance"
        return State.PASS, "ordered_supplied_clocks"
    if check is Check.FALLBACK:
        if obs.is_fallback is None:
            return State.UNKNOWN, "unknown_fallback_status"
        if obs.is_fallback:
            return _reference(obs.fallback_reference, decision)
        return State.PASS, "supplied_primary_status"
    clock = now if cap.mode is Mode.CURRENT else decision
    if check is Check.EVENT_AGE:
        return _age_check(
            obs.event_at, clock, cap.max_event_age_seconds, cap.max_future_skew_seconds
        )
    if check is Check.RECEIPT_AGE:
        return _age_check(
            obs.receipt_at,
            clock,
            cap.max_receipt_age_seconds,
            cap.max_future_skew_seconds,
        )
    if check is Check.DELAY:
        if cap.max_delay_seconds is None:
            return State.UNKNOWN, "unconfigured_delay_policy"
        if obs.delay is None or obs.delay.seconds is None:
            return State.UNKNOWN, "unknown_source_delay"
        if obs.delay.scope != obs.identity:
            return State.FAIL, "delay_scope_mismatch"
        state, reason = _reference(obs.delay.reference, decision)
        if state is not State.PASS:
            return state, reason
        if obs.delay.seconds > cap.max_delay_seconds:
            return State.FAIL, "declared_delay_exceeds_policy"
        return State.PASS, "within_scoped_delay_policy"
    if check is Check.FINALITY:
        if obs.finality is Finality.UNKNOWN:
            return State.UNKNOWN, "unknown_provider_finality"
        return (
            (State.PASS, "supplied_final")
            if obs.finality is Finality.FINAL
            else (State.FAIL, "provisional_or_incomplete")
        )
    if check is Check.INTERVAL:
        if cap.required_interval_seconds is None:
            return State.UNKNOWN, "unconfigured_interval_policy"
        if obs.interval_seconds is None:
            return State.UNKNOWN, "unknown_interval"
        return (
            (State.PASS, "matching_interval")
            if obs.interval_seconds == cap.required_interval_seconds
            else (State.FAIL, "interval_mismatch")
        )
    if check is Check.COVERAGE:
        if cap.min_coverage_fraction is None:
            return State.UNKNOWN, "unconfigured_coverage_policy"
        if obs.coverage_fraction is None:
            return State.UNKNOWN, "unknown_coverage"
        return (
            (State.PASS, "sufficient_supplied_coverage")
            if obs.coverage_fraction >= cap.min_coverage_fraction
            else (State.FAIL, "insufficient_coverage")
        )
    if check is Check.CONTINUITY:
        if obs.continuous is None:
            return State.UNKNOWN, "unknown_continuity"
        return (
            (State.PASS, "supplied_continuity")
            if obs.continuous
            else (State.FAIL, "discontinuous")
        )
    if check is Check.LIFECYCLE:
        life = obs.lifecycle
        if life is None or life.connected is None:
            return State.UNKNOWN, "unverified_feed_health"
        if not life.connected:
            return State.FAIL, "disconnected_retained_evidence"
        if life.observation_generation is None or life.current_generation is None:
            return State.UNKNOWN, "unknown_connection_generation"
        if life.observation_generation != life.current_generation:
            return State.FAIL, "retained_connection_generation"
        return _reference(life.reference, decision)
    if check is Check.SESSION:
        if cap.allowed_session_states is None or not cap.allowed_session_states:
            return State.UNKNOWN, "unconfigured_session_policy"
        if session is None or session.state is None:
            return State.UNKNOWN, "unknown_supplied_session"
        state, reason = _reference(session.reference, decision)
        if state is not State.PASS:
            return state, reason
        return (
            (State.PASS, "allowed_supplied_session")
            if session.state in cap.allowed_session_states
            else (State.FAIL, "session_not_allowed")
        )
    options = obs.options
    if options is None:
        return State.UNKNOWN, "missing_options_facts"
    state, reason = _reference(options.reference, decision)
    if state is not State.PASS:
        return state, reason
    if check is Check.OI_VINTAGE:
        if options.oi_vintage_at is not None and _utc(options.oi_vintage_at) > decision:
            return State.FAIL, "oi_vintage_after_decision"
        if options.oi_vintage_at is not None and _utc(options.oi_vintage_at) > _utc(
            options.reference.available_at
        ):
            return State.FAIL, "oi_vintage_after_availability"
        return _age_check(
            options.oi_vintage_at,
            clock,
            cap.max_oi_age_seconds,
            cap.max_future_skew_seconds,
        )
    verified = (
        options.expiry_settlement_verified
        if check is Check.EXPIRY_SETTLEMENT
        else options.greeks_verified
    )
    if verified is None:
        return State.UNKNOWN, "unverified_options_mapping_or_greeks"
    return (
        (State.PASS, "supplied_options_verification")
        if verified
        else (State.FAIL, "options_verification_failed")
    )


def evaluate_feed_quality(
    observations: tuple[Observation, ...],
    policy: EvaluationPolicy,
    *,
    now: datetime,
    decision_at: datetime,
    sessions: tuple[SessionFacts, ...],
) -> EvaluationResult:
    """Evaluate exact selected evidence, keeping every input observation in order.

    PASS is research quality under caller facts/policy; it never proves a live
    provider, payload authenticity, journal publication, execution or trading.
    """
    _typed(policy, EvaluationPolicy, nullable=False)
    _clock(now, nullable=False)
    _clock(decision_at, nullable=False)
    now, decision = _utc(now), _utc(decision_at)
    if decision > now:
        raise ValueError("decision_after_evaluation")
    if type(observations) is not tuple or type(sessions) is not tuple:
        raise ValueError("immutable_tuple_required")
    for obs in observations:
        _typed(obs, Observation, nullable=False)
    for session in sessions:
        _typed(session, SessionFacts, nullable=False)
    if len({s.instrument for s in sessions}) != len(sessions):
        raise ValueError("ambiguous_session_facts")
    instruments = []
    for instrument in INSTRUMENTS:
        retained = tuple(o for o in observations if o.identity.instrument == instrument)
        contexts = []
        for obs in retained:
            delay = obs.delay
            if delay is None or delay.seconds is None:
                label = "unknown_delay"
            elif delay.scope != obs.identity:
                label = "unverified_delay_scope"
            elif _reference(delay.reference, now)[0] is not State.PASS:
                label = "unverified_delay_provenance"
            else:
                label = (
                    "declared_delayed" if delay.seconds > 0 else "declared_zero_delay"
                )
            knowledge = (
                State.UNKNOWN
                if any(
                    s is None
                    for s in (obs.available_at, obs.receipt_at, obs.recorded_at)
                )
                else State.FAIL
                if any(
                    _utc(s) > now
                    for s in (
                        obs.event_at,
                        obs.as_of_at,
                        obs.available_at,
                        obs.receipt_at,
                        obs.recorded_at,
                    )
                    if s is not None
                )
                else State.PASS
            )
            contexts.append(
                DescriptiveContext(
                    obs,
                    obs.presence is Presence.PRESENT and knowledge is State.PASS,
                    knowledge,
                    label,
                    _age(obs.event_at, now),
                    _age(obs.receipt_at, now),
                    _age(obs.event_at, decision),
                    _age(obs.receipt_at, decision),
                )
            )
        results = []
        session = next((s for s in sessions if s.instrument == instrument), None)
        for cap in policy.capabilities:
            if cap.expected_identity.instrument != instrument:
                continue
            selected = tuple(
                o
                for o in retained
                if cap.evidence_id is not None and o.evidence_id == cap.evidence_id
            )
            obs = selected[0] if len(selected) == 1 else None
            required = MANDATORY | frozenset(cap.required_checks or ())
            if cap.mode is Mode.CURRENT:
                required |= {
                    Check.EVENT_AGE,
                    Check.RECEIPT_AGE,
                    Check.DELAY,
                    Check.LIFECYCLE,
                }
            if cap.mode is Mode.ZERO_DTE:
                required |= {Check.ZERO_DTE_HOLD}
            checks = tuple(
                CheckResult(
                    c,
                    *_check(
                        c, obs, cap, policy, now, decision, session, len(selected) > 1
                    ),
                )
                if c in required
                else CheckResult(c, State.NOT_REQUIRED, "not_required_by_capability")
                for c in Check
            )
            states = {c.state for c in checks if c.check in required}
            state = (
                State.FAIL
                if State.FAIL in states
                else State.UNKNOWN
                if State.UNKNOWN in states
                else State.PASS
            )
            results.append(
                CapabilityResult(
                    cap.name,
                    cap.mode,
                    cap.evidence_id,
                    obs.evidence_sha256 if obs else None,
                    state,
                    state is State.PASS,
                    checks,
                )
            )
        instruments.append(
            InstrumentResult(instrument, tuple(contexts), tuple(results))
        )
    return EvaluationResult(policy.version, now, decision, tuple(instruments))
