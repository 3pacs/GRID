"""Synthetic numeric/session contracts and exact caller-qualified OR/VWAP."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from fractions import Fraction
from zoneinfo import ZoneInfo

from analysis.feed_reliability_projection import FactReference, Observation, State
from analysis.feed_reliability_projection import (
    Check,
    EvaluationPolicy,
    SessionFacts,
    evaluate_feed_quality,
)

__all__ = (
    "Adjustment",
    "NotionalBasis",
    "TimestampConvention",
    "SessionConvention",
    "BarInterval",
    "PriceBasis",
    "SessionWindow",
    "NumericBar",
    "ExpectedBarGrid",
    "StructuralResult",
    "compare_price_basis",
    "validate_numeric_bar",
    "validate_bar_grid",
)


class Adjustment(str, Enum):
    RAW = "raw"
    SPLIT = "split"
    TOTAL_RETURN = "total_return"
    UNKNOWN = "unknown"


class NotionalBasis(str, Enum):
    TRADE = "trade"
    OHLCV_PROXY = "ohlcv_proxy"
    UNKNOWN = "unknown"


class TimestampConvention(str, Enum):
    START = "start"
    END = "end"
    UNKNOWN = "unknown"


class SessionConvention(str, Enum):
    REGULAR = "regular"
    EXTENDED = "extended"
    AUCTION = "auction"
    UNKNOWN = "unknown"


def _text(value: object) -> None:
    if value is None:
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
        raise ValueError("invalid_contract_identifier")


def _typed(value: object, cls: type, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if type(value) is not cls:
        raise ValueError("invalid_contract_type")


def _utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc)


def _clock(value: object) -> None:
    if type(value) is not datetime or type(value.tzinfo) not in (timezone, ZoneInfo):
        raise ValueError("invalid_interval_clock")
    try:
        _utc(value)
    except (ValueError, OverflowError, TypeError):
        raise ValueError("invalid_interval_clock") from None


def _interval(start: datetime, end: datetime) -> None:
    _clock(start)
    _clock(end)
    if _utc(start) >= _utc(end):
        raise ValueError("nonpositive_interval")


def _decimal(value: object, *, price: bool = False) -> None:
    if value is None:
        return
    if type(value) is not Decimal or not value.is_finite():
        raise ValueError("invalid_decimal_fact")
    if price and value <= 0:
        raise ValueError("nonpositive_price")


@dataclass(frozen=True)
class BarInterval:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        _interval(self.start, self.end)


@dataclass(frozen=True)
class PriceBasis:
    price_currency: str | None
    volume_unit: str | None
    notional_unit: str | None
    adjustment: Adjustment
    basis_id: str | None
    basis_version: str | None
    adjustment_reference: FactReference | None
    notional_basis: NotionalBasis

    def __post_init__(self) -> None:
        for value in (
            self.price_currency,
            self.volume_unit,
            self.notional_unit,
            self.basis_id,
            self.basis_version,
        ):
            _text(value)
        _typed(self.adjustment, Adjustment)
        _typed(self.adjustment_reference, FactReference, nullable=True)
        _typed(self.notional_basis, NotionalBasis)


@dataclass(frozen=True)
class SessionWindow:
    instrument: str
    exchange: str | None
    session_id: str | None
    timezone_id: str | None
    open_at: datetime
    close_at: datetime
    calendar_id: str | None
    calendar_version: str | None
    calendar_reference: FactReference | None
    convention: SessionConvention
    segments: tuple[BarInterval, ...] | None

    def __post_init__(self) -> None:
        if type(self.instrument) is not str or self.instrument not in (
            "TEST:SPY",
            "TEST:QQQ",
        ):
            raise ValueError("synthetic_instrument_required")
        for value in (
            self.exchange,
            self.session_id,
            self.timezone_id,
            self.calendar_id,
            self.calendar_version,
        ):
            _text(value)
        _interval(self.open_at, self.close_at)
        _typed(self.calendar_reference, FactReference, nullable=True)
        _typed(self.convention, SessionConvention)
        if self.segments is not None:
            if type(self.segments) is not tuple:
                raise ValueError("immutable_segments_required")
            previous = _utc(self.open_at)
            for segment in self.segments:
                _typed(segment, BarInterval)
                if _utc(segment.start) < previous or _utc(segment.end) > _utc(
                    self.close_at
                ):
                    raise ValueError("invalid_session_segments")
                previous = _utc(segment.end)


@dataclass(frozen=True)
class NumericBar:
    observation: Observation
    interval_start: datetime
    interval_end: datetime
    timestamp_convention: TimestampConvention
    session: SessionWindow
    basis: PriceBasis
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: int | None
    trade_notional: Decimal | None
    notional_reference: FactReference | None

    def __post_init__(self) -> None:
        _typed(self.observation, Observation)
        _interval(self.interval_start, self.interval_end)
        _typed(self.timestamp_convention, TimestampConvention)
        _typed(self.session, SessionWindow)
        _typed(self.basis, PriceBasis)
        for value in (self.open, self.high, self.low, self.close):
            _decimal(value, price=True)
        if self.volume is not None and (
            type(self.volume) is not int or self.volume < 0
        ):
            raise ValueError("invalid_volume")
        _decimal(self.trade_notional)
        _typed(self.notional_reference, FactReference, nullable=True)
        if self.high is not None and self.low is not None and self.low > self.high:
            raise ValueError("inverted_price_range")
        for value in (self.open, self.close):
            if value is not None and (
                (self.low is not None and value < self.low)
                or (self.high is not None and value > self.high)
            ):
                raise ValueError("price_outside_range")


@dataclass(frozen=True)
class ExpectedBarGrid:
    interval_start: datetime
    interval_end: datetime
    session: SessionWindow
    basis: PriceBasis
    intervals: tuple[BarInterval, ...] | None
    reference: FactReference | None

    def __post_init__(self) -> None:
        _interval(self.interval_start, self.interval_end)
        _typed(self.session, SessionWindow)
        _typed(self.basis, PriceBasis)
        _typed(self.reference, FactReference, nullable=True)
        if self.intervals is not None:
            if type(self.intervals) is not tuple:
                raise ValueError("immutable_grid_required")
            for interval in self.intervals:
                _typed(interval, BarInterval)


@dataclass(frozen=True)
class StructuralResult:
    state: State
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        _typed(self.state, State)
        if type(self.reasons) is not tuple or any(
            type(r) is not str for r in self.reasons
        ):
            raise ValueError("invalid_structural_reasons")


def _result(failures: list[str], unknowns: list[str]) -> StructuralResult:
    reasons = tuple(dict.fromkeys(failures + unknowns))
    return StructuralResult(
        State.FAIL if failures else State.UNKNOWN if unknowns else State.PASS,
        reasons or ("structural_match",),
    )


def _reference_known(reference: FactReference | None) -> bool:
    # Presence only. These clocks are never adjudicated against a decision here.
    return reference is not None and all(
        value is not None
        for value in (
            reference.evidence_id,
            reference.sha256,
            reference.available_at,
            reference.recorded_at,
        )
    )


def _clock_key(value: datetime | None) -> tuple | None:
    if value is None:
        return None
    # Include the original clock representation AND its UTC instant. Python's
    # same-zone datetime equality alone can conflate distinct DST folds.
    return (_utc(value), value.isoformat(), str(value.tzinfo), value.fold)


def _reference_key(value: FactReference | None) -> tuple | None:
    if value is None:
        return None
    return (
        value.evidence_id,
        value.sha256,
        _clock_key(value.available_at),
        _clock_key(value.recorded_at),
    )


def _basis_unknown(basis: PriceBasis) -> bool:
    return (
        any(
            x is None
            for x in (
                basis.price_currency,
                basis.volume_unit,
                basis.notional_unit,
                basis.basis_id,
                basis.basis_version,
            )
        )
        or basis.adjustment is Adjustment.UNKNOWN
        or basis.notional_basis is NotionalBasis.UNKNOWN
        or not _reference_known(basis.adjustment_reference)
    )


def compare_price_basis(left: PriceBasis, right: PriceBasis) -> StructuralResult:
    """Compare complete declared bases, including every adjustment reference field."""
    _typed(left, PriceBasis)
    _typed(right, PriceBasis)
    failures, unknowns = [], []
    for field in (
        "price_currency",
        "volume_unit",
        "notional_unit",
        "adjustment",
        "basis_id",
        "basis_version",
        "notional_basis",
    ):
        a, b = getattr(left, field), getattr(right, field)
        if (
            a is None
            or b is None
            or a is Adjustment.UNKNOWN
            or b is Adjustment.UNKNOWN
            or a is NotionalBasis.UNKNOWN
            or b is NotionalBasis.UNKNOWN
        ):
            unknowns.append("unknown_price_basis")
        elif a != b:
            failures.append("incompatible_" + field)
    if not (
        _reference_known(left.adjustment_reference)
        and _reference_known(right.adjustment_reference)
    ):
        unknowns.append("unknown_adjustment_reference")
    elif _reference_key(left.adjustment_reference) != _reference_key(
        right.adjustment_reference
    ):
        failures.append("incompatible_adjustment_reference")
    return _result(failures, unknowns)


def _session_key(session: SessionWindow) -> tuple:
    return (
        session.instrument,
        session.exchange,
        session.session_id,
        session.timezone_id,
        _clock_key(session.open_at),
        _clock_key(session.close_at),
        session.calendar_id,
        session.calendar_version,
        _reference_key(session.calendar_reference),
        session.convention,
        None
        if session.segments is None
        else tuple((_clock_key(s.start), _clock_key(s.end)) for s in session.segments),
    )


def _session_unknown(session: SessionWindow) -> bool:
    return (
        any(
            x is None
            for x in (
                session.exchange,
                session.session_id,
                session.timezone_id,
                session.calendar_id,
                session.calendar_version,
            )
        )
        or not _reference_known(session.calendar_reference)
        or session.convention is SessionConvention.UNKNOWN
        or session.segments is None
    )


def _inside(start: datetime, end: datetime, session: SessionWindow) -> bool | None:
    a, b = _utc(start), _utc(end)
    if a < _utc(session.open_at) or b > _utc(session.close_at):
        return False
    if session.segments is None:
        return None
    return any(_utc(s.start) <= a and b <= _utc(s.end) for s in session.segments)


def _observation_duration(bar: NumericBar) -> State:
    """Bind the direct Observation declaration to geometry without rounding."""
    seconds = bar.observation.interval_seconds
    if seconds is None:
        return State.UNKNOWN
    elapsed = _utc(bar.interval_end) - _utc(bar.interval_start)
    microseconds = (
        elapsed.days * 86400 + elapsed.seconds
    ) * 1000000 + elapsed.microseconds
    if type(seconds) is int:
        matches = microseconds == seconds * 1000000
    else:
        numerator, denominator = seconds.as_integer_ratio()
        matches = microseconds * denominator == numerator * 1000000
    return State.PASS if matches else State.FAIL


def validate_numeric_bar(bar: NumericBar) -> StructuralResult:
    """Describe numeric/interval structure; never temporal eligibility or finality."""
    _typed(bar, NumericBar)
    failures, unknowns = [], []
    duration = _observation_duration(bar)
    if duration is State.FAIL:
        failures.append("observation_duration_mismatch")
    elif duration is State.UNKNOWN:
        unknowns.append("unknown_observation_duration")
    if bar.observation.identity.instrument != bar.session.instrument:
        failures.append("bar_instrument_mismatch")
    inside = _inside(bar.interval_start, bar.interval_end, bar.session)
    if inside is False:
        failures.append("bar_outside_trading_segment")
    elif inside is None:
        unknowns.append("unknown_trading_segments")
    if _session_unknown(bar.session):
        unknowns.append("unknown_session")
    if _basis_unknown(bar.basis):
        unknowns.append("unknown_price_basis")
    if bar.timestamp_convention is TimestampConvention.UNKNOWN:
        unknowns.append("unknown_timestamp_convention")
    elif bar.observation.event_at is None:
        unknowns.append("unknown_event_timestamp")
    elif _utc(bar.observation.event_at) != _utc(
        bar.interval_start
        if bar.timestamp_convention is TimestampConvention.START
        else bar.interval_end
    ):
        failures.append("event_timestamp_interval_mismatch")
    if any(x is None for x in (bar.open, bar.high, bar.low, bar.close, bar.volume)):
        unknowns.append("unknown_numeric_component")
    if bar.trade_notional is None:
        unknowns.append("unknown_trade_notional")
    if not _reference_known(bar.notional_reference):
        unknowns.append("unknown_notional_reference")
    return _result(failures, unknowns)


def _interval_key(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    return _utc(start), _utc(end)


def _sequence_checks(keys: tuple, prefix: str, failures: list[str]) -> None:
    for index, key in enumerate(keys):
        if key in keys[:index]:
            failures.append(prefix + "duplicate_interval")
        if index and key[0] < keys[index - 1][0]:
            failures.append(prefix + "out_of_order")
        if any(key[0] < old[1] and old[0] < key[1] for old in keys[:index]):
            failures.append(prefix + "overlap")


def validate_bar_grid(
    bars: tuple[NumericBar, ...], grid: ExpectedBarGrid
) -> StructuralResult:
    """Exact supplied coverage and declared reference compatibility, without repairs.

    PASS says the expected grid tiles the allowed analysis segments and bars
    match it. It says nothing about missing prices/notional, known-at cutoffs,
    authenticity, connectivity, provider finality or trigger admission.
    """
    if type(bars) is not tuple:
        raise ValueError("immutable_bars_required")
    for bar in bars:
        _typed(bar, NumericBar)
    _typed(grid, ExpectedBarGrid)
    failures, unknowns = [], []
    if _session_unknown(grid.session):
        unknowns.append("unknown_session")
    if _basis_unknown(grid.basis):
        unknowns.append("unknown_price_basis")
    if not _reference_known(grid.reference):
        unknowns.append("unknown_grid_reference")
    start, end = _interval_key(grid.interval_start, grid.interval_end)
    if start < _utc(grid.session.open_at) or end > _utc(grid.session.close_at):
        failures.append("analysis_outside_session")
    if grid.session.segments:
        if not any(
            _utc(s.start) <= start < _utc(s.end) for s in grid.session.segments
        ) or not any(_utc(s.start) < end <= _utc(s.end) for s in grid.session.segments):
            failures.append("analysis_endpoint_outside_trading_segment")
    keys = tuple(_interval_key(b.interval_start, b.interval_end) for b in bars)
    _sequence_checks(keys, "observed_", failures)
    for i, bar in enumerate(bars):
        if _session_key(bar.session) != _session_key(grid.session):
            failures.append("incompatible_session")
        if bar.observation.identity.instrument != grid.session.instrument:
            failures.append("bar_instrument_mismatch")
        basis = compare_price_basis(bar.basis, grid.basis)
        if basis.state is State.FAIL:
            failures.extend(basis.reasons)
        elif basis.state is State.UNKNOWN:
            unknowns.extend(basis.reasons)
        if not (start <= keys[i][0] and keys[i][1] <= end):
            failures.append("bar_outside_analysis")
        if _inside(bar.interval_start, bar.interval_end, grid.session) is False:
            failures.append("bar_outside_trading_segment")
        if bar.timestamp_convention is TimestampConvention.UNKNOWN:
            unknowns.append("unknown_timestamp_convention")
        if bar.observation.event_at is None:
            unknowns.append("unknown_event_timestamp")
        if any(
            x is None
            for x in (
                bar.observation.evidence_id,
                bar.observation.evidence_sha256,
                bar.observation.revision_id,
            )
        ):
            unknowns.append("unknown_observation_identifier")
        check = validate_numeric_bar(bar)
        for reason in check.reasons:
            if reason in (
                "event_timestamp_interval_mismatch",
                "bar_instrument_mismatch",
                "observation_duration_mismatch",
            ):
                failures.append(reason)
            elif reason == "unknown_observation_duration":
                unknowns.append(reason)
        for old in bars[:i]:
            if (
                bar.observation.evidence_id is not None
                and bar.observation.evidence_id == old.observation.evidence_id
            ):
                failures.append("duplicate_evidence_id")
            if (
                bar.observation.identity == old.observation.identity
                and bar.observation.revision_id is not None
                and bar.observation.revision_id == old.observation.revision_id
            ):
                failures.append("duplicate_revision_id")
            if keys[i] == _interval_key(old.interval_start, old.interval_end):
                if (
                    bar.observation.revision_id != old.observation.revision_id
                    or bar.observation.evidence_sha256
                    != old.observation.evidence_sha256
                    or bar.observation.identity != old.observation.identity
                ):
                    failures.append("conflicting_interval_revision")
    if grid.intervals is None:
        unknowns.append("unknown_expected_grid")
    else:
        expected = tuple(_interval_key(s.start, s.end) for s in grid.intervals)
        _sequence_checks(expected, "expected_", failures)
        for a, b in expected:
            if not (start <= a and b <= end):
                failures.append("expected_outside_analysis")
            if _inside(a, b, grid.session) is False:
                failures.append("expected_outside_trading_segment")
        if grid.session.segments is not None:
            cursor = 0
            for segment in grid.session.segments:
                a, b = max(start, _utc(segment.start)), min(end, _utc(segment.end))
                if a >= b:
                    continue
                edge = a
                while (
                    cursor < len(expected)
                    and expected[cursor][0] == edge
                    and expected[cursor][1] <= b
                ):
                    edge = expected[cursor][1]
                    cursor += 1
                if edge != b:
                    failures.append("expected_grid_gap")
            if cursor != len(expected):
                failures.append("expected_grid_not_segment_tiling")
        if any(key not in keys for key in expected):
            failures.append("missing_expected_interval")
        if any(key not in expected for key in keys):
            failures.append("unexpected_observed_interval")
    return _result(failures, unknowns)


# Separately admitted mathematics; accepted definitions/validators stay unchanged.
class PriceFeatureMethod(str, Enum):
    OPENING_RANGE = "opening_range"
    TRADE_NOTIONAL_VOLUME_VWAP = "trade_notional_volume_vwap"
    OHLCV_HLC3_VOLUME_PROXY = "ohlcv_hlc3_volume_proxy"


@dataclass(frozen=True)
class PriceFeatureResult:
    method: PriceFeatureMethod
    inputs: tuple[NumericBar, ...]
    grid: ExpectedBarGrid
    cutoff: datetime
    quality_policy: EvaluationPolicy
    evaluation_at: datetime
    decision_at: datetime
    session_facts: tuple[SessionFacts, ...]
    opening_minutes: int | None
    numeric: StructuralResult
    coverage: StructuralResult
    basis: StructuralResult
    temporal: StructuralResult
    high: Decimal | None
    low: Decimal | None
    vwap: Fraction | None

    def __post_init__(self) -> None:
        _typed(self.method, PriceFeatureMethod)
        _feature_arguments(
            self.inputs,
            self.grid,
            self.cutoff,
            self.quality_policy,
            self.evaluation_at,
            self.decision_at,
            self.session_facts,
        )
        for result in (self.numeric, self.coverage, self.basis, self.temporal):
            _typed(result, StructuralResult)
        if self.opening_minutes is not None and (
            type(self.opening_minutes) is not int
            or self.opening_minutes not in (5, 15, 30)
        ):
            raise ValueError("invalid_opening_minutes")
        _decimal(self.high, price=True)
        _decimal(self.low, price=True)
        _typed(self.vwap, Fraction, nullable=True)


def _feature_arguments(
    bars: tuple[NumericBar, ...],
    grid: ExpectedBarGrid,
    cutoff: datetime,
    policy: EvaluationPolicy,
    evaluation_at: datetime,
    decision_at: datetime,
    sessions: tuple[SessionFacts, ...],
) -> None:
    if type(bars) is not tuple or type(sessions) is not tuple:
        raise ValueError("immutable_feature_inputs_required")
    for bar in bars:
        _typed(bar, NumericBar)
    _typed(grid, ExpectedBarGrid)
    _typed(policy, EvaluationPolicy)
    for clock in (cutoff, evaluation_at, decision_at):
        _clock(clock)
    for facts in sessions:
        _typed(facts, SessionFacts)


def _feature_basis(
    bars: tuple[NumericBar, ...], grid: ExpectedBarGrid, method: PriceFeatureMethod
) -> StructuralResult:
    failures, unknowns = [], []
    for basis in (grid.basis,) + tuple(bar.basis for bar in bars):
        comparison = compare_price_basis(basis, grid.basis)
        if comparison.state is State.FAIL:
            failures.extend(comparison.reasons)
        elif comparison.state is State.UNKNOWN:
            unknowns.extend(comparison.reasons)
        if method is not PriceFeatureMethod.OPENING_RANGE:
            required = (
                NotionalBasis.TRADE
                if method is PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP
                else NotionalBasis.OHLCV_PROXY
            )
            if basis.notional_basis is NotionalBasis.UNKNOWN:
                unknowns.append("unknown_notional_method")
            elif basis.notional_basis is not required:
                failures.append("incompatible_notional_method")
            if basis.volume_unit is None:
                unknowns.append("unknown_economic_volume_unit")
            elif basis.volume_unit != "shares":
                failures.append("unsupported_economic_volume_unit")
            if basis.price_currency is None or basis.notional_unit is None:
                unknowns.append("unknown_economic_currency")
            elif basis.price_currency != basis.notional_unit:
                failures.append("incompatible_economic_currency")
    return _result(failures, unknowns)


def _ancillary_cutoff(
    label: str,
    reference: FactReference | None,
    cutoff: datetime,
    failures: list[str],
    unknowns: list[str],
) -> None:
    # This is the explicitly admitted binding of these numeric references, not
    # an alternate observation-quality/finality policy or evidence registry.
    if reference is None:
        unknowns.append(label + "_reference_unknown")
        return
    if any(
        value is None
        for value in (
            reference.evidence_id,
            reference.sha256,
            reference.available_at,
            reference.recorded_at,
        )
    ):
        unknowns.append(label + "_reference_unknown")
    a, r = reference.available_at, reference.recorded_at
    if a is not None and _utc(a) > _utc(cutoff):
        failures.append(label + "_available_after_cutoff")
    if r is not None and _utc(r) > _utc(cutoff):
        failures.append(label + "_recorded_after_cutoff")
    if a is not None and r is not None and _utc(a) > _utc(r):
        failures.append(label + "_reference_clock_order")


def _feature_temporal(
    bars: tuple[NumericBar, ...],
    grid: ExpectedBarGrid,
    method: PriceFeatureMethod,
    cutoff: datetime,
    policy: EvaluationPolicy,
    evaluation_at: datetime,
    decision_at: datetime,
    sessions: tuple[SessionFacts, ...],
) -> StructuralResult:
    failures, unknowns = [], []
    if _utc(decision_at) != _utc(cutoff):
        failures.append("decision_cutoff_mismatch")
    if _utc(grid.interval_end) > _utc(cutoff):
        failures.append("window_not_closed_at_cutoff")
    _ancillary_cutoff(
        "calendar", grid.session.calendar_reference, cutoff, failures, unknowns
    )
    _ancillary_cutoff("grid", grid.reference, cutoff, failures, unknowns)
    _ancillary_cutoff(
        "adjustment", grid.basis.adjustment_reference, cutoff, failures, unknowns
    )
    quality = evaluate_feed_quality(
        tuple(bar.observation for bar in bars),
        policy,
        now=evaluation_at,
        decision_at=decision_at,
        sessions=sessions,
    )
    results = {
        (row.instrument, result.name): result
        for row in quality.instruments
        for result in row.capabilities
    }
    for bar in bars:
        _ancillary_cutoff(
            "calendar", bar.session.calendar_reference, cutoff, failures, unknowns
        )
        _ancillary_cutoff(
            "adjustment", bar.basis.adjustment_reference, cutoff, failures, unknowns
        )
        if method is PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP:
            _ancillary_cutoff(
                "notional", bar.notional_reference, cutoff, failures, unknowns
            )
        selected = tuple(
            cap
            for cap in policy.capabilities
            if cap.expected_identity.instrument == bar.observation.identity.instrument
            and cap.evidence_id == bar.observation.evidence_id
        )
        if not selected:
            unknowns.append("bar_quality_policy_unconfigured")
        for cap in selected:
            result = results[(cap.expected_identity.instrument, cap.name)]
            if result.state is State.FAIL:
                failures.extend(
                    "quality_" + check.reason
                    for check in result.checks
                    if check.state is State.FAIL
                )
            elif result.state is State.UNKNOWN:
                unknowns.extend(
                    "quality_" + check.reason
                    for check in result.checks
                    if check.state is State.UNKNOWN
                )
            finality = next(
                check for check in result.checks if check.check is Check.FINALITY
            )
            if finality.state is State.NOT_REQUIRED:
                unknowns.append("explicit_finality_check_required")
    return _result(failures, unknowns)


def _feature_numeric(
    bars: tuple[NumericBar, ...], method: PriceFeatureMethod
) -> StructuralResult:
    failures, unknowns = [], []
    if not bars:
        unknowns.append("no_numeric_observations")
    for bar in bars:
        if method is PriceFeatureMethod.OPENING_RANGE:
            if bar.high is None or bar.low is None:
                unknowns.append("missing_range_price")
        else:
            if bar.volume is None:
                unknowns.append("missing_feature_volume")
            if method is PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP:
                if bar.trade_notional is None:
                    unknowns.append("missing_trade_notional")
                elif bar.trade_notional < 0:
                    failures.append("negative_economic_notional")
                elif bar.volume == 0 and bar.trade_notional != 0:
                    failures.append("zero_volume_nonzero_notional")
            elif any(value is None for value in (bar.high, bar.low, bar.close)):
                unknowns.append("missing_proxy_price")
    if (
        method is not PriceFeatureMethod.OPENING_RANGE
        and bars
        and all(bar.volume is not None for bar in bars)
        and sum(bar.volume for bar in bars) == 0
    ):
        failures.append("zero_total_volume")
    return _result(failures, unknowns)


def _feature_result(
    bars: tuple[NumericBar, ...],
    grid: ExpectedBarGrid,
    method: PriceFeatureMethod,
    cutoff: datetime,
    policy: EvaluationPolicy,
    evaluation_at: datetime,
    decision_at: datetime,
    sessions: tuple[SessionFacts, ...],
    minutes: int | None,
) -> PriceFeatureResult:
    _feature_arguments(bars, grid, cutoff, policy, evaluation_at, decision_at, sessions)
    numeric = _feature_numeric(bars, method)
    coverage = validate_bar_grid(bars, grid)
    if minutes is not None:
        failures, unknowns = [], []
        if coverage.state is State.FAIL:
            failures.extend(coverage.reasons)
        elif coverage.state is State.UNKNOWN:
            unknowns.extend(coverage.reasons)
        if _utc(grid.interval_start) != _utc(grid.session.open_at):
            failures.append("opening_window_not_anchored")
        elapsed = _utc(grid.interval_end) - _utc(grid.interval_start)
        microseconds = (
            elapsed.days * 86400 + elapsed.seconds
        ) * 1000000 + elapsed.microseconds
        if microseconds != minutes * 60 * 1000000:
            failures.append("opening_window_duration_mismatch")
        coverage = _result(failures, unknowns)
    basis = _feature_basis(bars, grid, method)
    temporal = _feature_temporal(
        bars, grid, method, cutoff, policy, evaluation_at, decision_at, sessions
    )
    high = low = vwap = None
    if all(
        result.state is State.PASS for result in (numeric, coverage, basis, temporal)
    ):
        if method is PriceFeatureMethod.OPENING_RANGE:
            high = max(bar.high for bar in bars)
            low = min(bar.low for bar in bars)
        else:
            volume = sum(bar.volume for bar in bars)
            if method is PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP:
                notional = sum(
                    (Fraction(bar.trade_notional) for bar in bars), Fraction(0)
                )
            else:
                notional = sum(
                    (
                        (Fraction(bar.high) + Fraction(bar.low) + Fraction(bar.close))
                        * bar.volume
                        / 3
                        for bar in bars
                    ),
                    Fraction(0),
                )
            vwap = notional / volume
    return PriceFeatureResult(
        method,
        bars,
        grid,
        cutoff,
        policy,
        evaluation_at,
        decision_at,
        sessions,
        minutes,
        numeric,
        coverage,
        basis,
        temporal,
        high,
        low,
        vwap,
    )


def opening_range(
    bars: tuple[NumericBar, ...],
    grid: ExpectedBarGrid,
    *,
    minutes: int,
    cutoff: datetime,
    quality_policy: EvaluationPolicy,
    evaluation_at: datetime,
    decision_at: datetime,
    session_facts: tuple[SessionFacts, ...],
) -> PriceFeatureResult:
    """Qualified 5/15/30 minute range from exact supplied final coverage."""
    if type(minutes) is not int or minutes not in (5, 15, 30):
        raise ValueError("invalid_opening_minutes")
    return _feature_result(
        bars,
        grid,
        PriceFeatureMethod.OPENING_RANGE,
        cutoff,
        quality_policy,
        evaluation_at,
        decision_at,
        session_facts,
        minutes,
    )


def trade_vwap(
    bars: tuple[NumericBar, ...],
    grid: ExpectedBarGrid,
    *,
    cutoff: datetime,
    quality_policy: EvaluationPolicy,
    evaluation_at: datetime,
    decision_at: datetime,
    session_facts: tuple[SessionFacts, ...],
) -> PriceFeatureResult:
    """Exact rational ratio of declared compatible trade notional to shares."""
    return _feature_result(
        bars,
        grid,
        PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP,
        cutoff,
        quality_policy,
        evaluation_at,
        decision_at,
        session_facts,
        None,
    )


def ohlcv_hlc3_volume_proxy(
    bars: tuple[NumericBar, ...],
    grid: ExpectedBarGrid,
    *,
    cutoff: datetime,
    quality_policy: EvaluationPolicy,
    evaluation_at: datetime,
    decision_at: datetime,
    session_facts: tuple[SessionFacts, ...],
) -> PriceFeatureResult:
    """Explicit rational HLC3-volume approximation; never true trade VWAP."""
    return _feature_result(
        bars,
        grid,
        PriceFeatureMethod.OHLCV_HLC3_VOLUME_PROXY,
        cutoff,
        quality_policy,
        evaluation_at,
        decision_at,
        session_facts,
        None,
    )


__all__ += (
    "PriceFeatureMethod",
    "PriceFeatureResult",
    "opening_range",
    "trade_vwap",
    "ohlcv_hlc3_volume_proxy",
)
