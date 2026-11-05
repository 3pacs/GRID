"""Offline exact-72h evaluation; no provider, database, scheduler or trade I/O.

Explicit venue/cadence/basis evidence is required. This deliberately does not
adapt date-only NYSE price rows or infer elapsed time from TimesFM step counts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite, sqrt
from typing import Any, Sequence

VERSION = "continuous-72h-v1"
HORIZON = timedelta(hours=72)


class ContractError(ValueError):
    """Input evidence cannot support this evaluation contract."""


def utc(value: datetime) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ContractError("timezone-aware timestamp required")
    return value.astimezone(timezone.utc)


def number(value: float, *, positive: bool = False) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (float, int))
        or not isfinite(value)
    ):
        raise ContractError("finite numeric value required")
    if positive and value <= 0:
        raise ContractError("positive price required")
    return float(value)


def reference(value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ContractError("nonempty evidence reference required")


@dataclass(frozen=True)
class VenueContract:
    venue: str
    instrument: str
    market: str  # spot or perpetual
    basis: str  # raw_trade_close or mark_close; never interchangeable
    cadence_seconds: int
    calendar: str
    evidence_ref: str  # operator-reviewed venue/calendar/basis evidence
    synthetic: bool  # explicit; never inferred from source names

    def validate(self) -> None:
        for value in (self.venue, self.instrument, self.evidence_ref):
            reference(value)
        if self.calendar != "24/7":
            raise ContractError("explicit 24/7 calendar required")
        if self.market not in ("spot", "perpetual"):
            raise ContractError("unsupported market")
        if self.basis not in ("raw_trade_close", "mark_close"):
            raise ContractError("unverified price basis")
        if self.market == "spot" and self.basis != "raw_trade_close":
            raise ContractError("spot requires raw trade close")
        if type(self.synthetic) is not bool:
            raise ContractError("explicit synthetic flag required")
        if (
            type(self.cadence_seconds) is not int
            or self.cadence_seconds <= 0
            or 259200 % self.cadence_seconds
            or self.cadence_seconds > 86400
        ):
            raise ContractError("cadence must divide 72h exactly and be at most 24h")

    @property
    def steps(self) -> int:
        return 259200 // self.cadence_seconds


@dataclass(frozen=True)
class Bar:
    closed_at: datetime
    available_at: datetime
    price: float
    venue: str
    instrument: str
    basis: str
    source_ref: str  # includes immutable vintage/row identity


@dataclass(frozen=True)
class Forecast:
    origin: datetime
    issued_at: datetime
    input_as_of: datetime  # latest input availability, not merely observation time
    model_ref: str  # model/version + frozen configuration
    input_ref: str  # immutable input snapshot
    targets: tuple[datetime, ...]
    point: tuple[float, ...]
    q10: tuple[float, ...]
    q50: tuple[float, ...]
    q90: tuple[float, ...]


@dataclass(frozen=True)
class CostScenario:
    fee_bps_per_side: float
    slippage_bps_per_side: float
    funding_bps_long: float  # signed cumulative debit for a unit-notional long
    funding_start: datetime
    funding_end: datetime
    available_at: datetime
    evidence_ref: str
    estimated: bool  # True for assumed scenarios, False for verified realized costs


def _bars(bars: Sequence[Bar], contract: VenueContract, cutoff: datetime) -> list[Bar]:
    result = list(bars)
    if not result:
        raise ContractError("missing bars")
    times = []
    for bar in result:
        closed, available = utc(bar.closed_at), utc(bar.available_at)
        reference(bar.source_ref)
        number(bar.price, positive=True)
        if (bar.venue, bar.instrument, bar.basis) != (
            contract.venue,
            contract.instrument,
            contract.basis,
        ):
            raise ContractError("venue/instrument/price basis mismatch")
        if available < closed or available > cutoff:
            raise ContractError("bar availability outside cutoff")
        times.append(closed)
    if times != sorted(set(times)):
        raise ContractError("duplicate or unordered bars")
    cadence = timedelta(seconds=contract.cadence_seconds)
    if any(b - a != cadence for a, b in zip(times, times[1:])):
        raise ContractError("missing or irregular bar")
    return result


def preflight(
    contract: VenueContract, forecast: Forecast, history: Sequence[Bar]
) -> dict[str, Any]:
    """Bounded readiness for frozen input only; never authorizes activation."""
    contract.validate()
    origin = utc(forecast.origin)
    if utc(forecast.issued_at) > origin or utc(forecast.input_as_of) > utc(
        forecast.issued_at
    ):
        raise ContractError("forecast/input availability leaks beyond origin")
    reference(forecast.model_ref)
    reference(forecast.input_ref)
    bars = _bars(history, contract, utc(forecast.input_as_of))
    if (
        len(bars) < 2
        or not origin - timedelta(seconds=contract.cadence_seconds)
        <= utc(bars[-1].closed_at)
        < origin
    ):
        raise ContractError(
            "history requires two bars and a last close within one cadence before origin"
        )
    targets = tuple(
        origin + timedelta(seconds=contract.cadence_seconds * i)
        for i in range(1, contract.steps + 1)
    )
    if tuple(utc(t) for t in forecast.targets) != targets:
        raise ContractError("forecast must cover exact 72h timestamp grid")
    arrays = (forecast.point, forecast.q10, forecast.q50, forecast.q90)
    if any(len(a) != contract.steps for a in arrays):
        raise ContractError("forecast length differs from required horizon")
    for point, lo, median, hi in zip(*arrays):
        for value in (point, lo, median, hi):
            number(value, positive=True)
        if not lo <= median <= hi:
            raise ContractError("crossing quantiles")
    return {
        "input_ready": True,
        "activation_authorized": False,
        "version": VERSION,
        "steps": contract.steps,
        "origin": origin.isoformat(),
        "target_end": targets[-1].isoformat(),
        "venue_evidence": contract.evidence_ref,
    }


def evaluate(
    contract: VenueContract,
    forecast: Forecast,
    history: Sequence[Bar],
    outcomes: Sequence[Bar],
    costs: CostScenario,
    *,
    as_of: datetime,
    entry_bar: Bar | None = None,
) -> dict[str, Any]:
    """Score one frozen forecast; missing evidence raises, immaturity is unavailable.

    Return errors are percentage points; price errors and pinball use quote units.
    Net return is a fixed-unit-notional long scenario, not an execution backtest.
    """
    ready = preflight(contract, forecast, history)
    origin, cutoff = utc(forecast.origin), utc(as_of)
    if cutoff < origin:
        raise ContractError("evaluation precedes forecast origin")
    if cutoff < origin + HORIZON:
        return {
            "available": False,
            "status": "unavailable",
            "reason": "immature_72h",
            "as_of": None,
            "metrics": None,
            "version": VERSION,
        }
    bars = _bars(outcomes, contract, cutoff)
    if tuple(utc(b.closed_at) for b in bars) != tuple(utc(t) for t in forecast.targets):
        raise ContractError("outcomes must match every exact forecast target")
    reference(costs.evidence_ref)
    if type(costs.estimated) is not bool:
        raise ContractError("explicit cost assumption flag required")
    if utc(costs.funding_start) != origin or utc(costs.funding_end) != origin + HORIZON:
        raise ContractError("funding coverage must equal entire holding interval")
    if utc(costs.available_at) > cutoff or utc(costs.available_at) < utc(
        costs.funding_end
    ):
        raise ContractError("cost evidence availability outside evaluation window")
    fee, slip, funding = [
        number(v)
        for v in (
            costs.fee_bps_per_side,
            costs.slippage_bps_per_side,
            costs.funding_bps_long,
        )
    ]
    if not 0 <= fee <= 10000 or not 0 <= slip <= 10000 or abs(funding) > 10000:
        raise ContractError("cost basis points outside bounds")
    if contract.market == "spot" and funding != 0:
        raise ContractError("spot has no perpetual funding")
    if entry_bar is None:
        raise ContractError("exact origin entry bar required")
    _bars([entry_bar], contract, cutoff)
    if utc(entry_bar.closed_at) != origin:
        raise ContractError("entry bar must close exactly at origin")
    actual = [b.price for b in bars]
    entry = entry_bar.price
    last_input = history[-1].price
    drift_per_step = (last_input - history[0].price) / (len(history) - 1)
    baselines = {
        "persistence": [last_input] * contract.steps,
        "linear_drift": [
            last_input
            + drift_per_step
            * (
                (utc(t) - utc(history[-1].closed_at)).total_seconds()
                / contract.cadence_seconds
            )
            for t in forecast.targets
        ],
    }

    # A linear extrapolation may go negative. Report its error honestly; it is
    # a price benchmark, never an executable quote or silently clipped series.
    def errors(predicted: Sequence[float]) -> dict[str, float]:
        diffs = [p - y for p, y in zip(predicted, actual)]
        return {
            "mae_quote": sum(abs(d) for d in diffs) / len(diffs),
            "rmse_quote": sqrt(sum(d * d for d in diffs) / len(diffs)),
            "terminal_return_error_pp": 100 * (predicted[-1] - actual[-1]) / entry,
        }

    pinball = {}
    for q, values in ((0.1, forecast.q10), (0.5, forecast.q50), (0.9, forecast.q90)):
        pinball[str(q)] = sum(
            max(q * (y - p), (q - 1) * (y - p)) for y, p in zip(actual, values)
        ) / len(actual)
    gross = 100 * (actual[-1] / entry - 1)
    metrics = {
        "model": errors(forecast.point),
        "baselines": {name: errors(values) for name, values in baselines.items()},
        "pinball_quote": pinball,
        "interval_80_coverage": sum(
            lo <= y <= hi for y, lo, hi in zip(actual, forecast.q10, forecast.q90)
        )
        / len(actual),
        "interval_80_mean_width_quote": sum(
            hi - lo for lo, hi in zip(forecast.q10, forecast.q90)
        )
        / len(actual),
        "gross_long_return_pct": gross,
        "scenario_net_long_return_pct": gross - (2 * (fee + slip) + funding) / 100,
    }

    def finite_metrics(value: Any) -> None:
        if isinstance(value, dict):
            for nested in value.values():
                finite_metrics(nested)
        else:
            number(value)

    finite_metrics(metrics)
    return {
        "available": True,
        "status": "ok",
        "version": VERSION,
        "as_of": bars[-1].closed_at.isoformat(),
        "evaluated_as_of": cutoff.isoformat(),
        "origin": origin.isoformat(),
        "horizon_hours": 72,
        "n_bars": len(bars),
        "venue": contract.venue,
        "instrument": contract.instrument,
        "market": contract.market,
        "price_basis": contract.basis,
        "metrics": metrics,
        "preflight": ready,
        "provenance": {
            "synthetic": contract.synthetic,
            "source": contract.evidence_ref,
            "model": forecast.model_ref,
            "model_estimated": True,
            "inputs": forecast.input_ref,
            "history_rows": [b.source_ref for b in history],
            "outcome_rows": [b.source_ref for b in bars],
            "entry_row": entry_bar.source_ref,
            "evidence_available_at": max(
                utc(b.available_at) for b in [*history, entry_bar, *bars]
            ).isoformat(),
            "costs_available_at": utc(costs.available_at).isoformat(),
            "costs": costs.evidence_ref,
            "costs_estimated": costs.estimated,
            "cost_basis_bps": {
                "fee_per_side": fee,
                "slippage_per_side": slip,
                "funding_long_72h": funding,
            },
        },
        "calibration_status": "descriptive_single_path_not_calibration_proof",
        "execution_basis": "unit_notional_long_cost_scenario_not_fills",
    }
