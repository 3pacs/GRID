"""Pure display projection of a supplied synthetic PriceFeatureResult."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from fractions import Fraction

from analysis.etf_price_features import PriceFeatureMethod, PriceFeatureResult
from analysis.feed_reliability_projection import State

__all__ = ("project_price_feature",)

_LABELS = {
    PriceFeatureMethod.OPENING_RANGE: "Opening range",
    PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP: "Trade-notional / volume VWAP",
    PriceFeatureMethod.OHLCV_HLC3_VOLUME_PROXY: "HLC3-volume proxy (not trade VWAP)",
}


def _metadata(value: object) -> object:
    """Copy declared metadata into JSON types; keep original clock representation."""
    if isinstance(value, Enum):
        return value.value
    if type(value) is datetime:
        return {
            "iso": value.isoformat(),
            "utc": value.astimezone(timezone.utc).isoformat(),
            "zone": getattr(value.tzinfo, "key", str(value.tzinfo)),
            "fold": value.fold,
        }
    if type(value) is int:
        return _integer(value)
    if value is None or type(value) in (str, bool, float):
        return value
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _metadata(getattr(value, field.name)) for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {key: _metadata(value[key]) for key in sorted(value)}
    if type(value) is tuple:
        return [_metadata(item) for item in value]
    raise TypeError("unsupported_display_metadata")


def _integer(value: int) -> str:
    """Exact base-10 digits without Python int-string limits or context rounding."""
    return format(Decimal(value), "f")


def _decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _ratio(value: Fraction | None) -> dict[str, str] | None:
    if value is None:
        return None
    return {
        "numerator": _integer(value.numerator),
        "denominator": _integer(value.denominator),
    }


def project_price_feature(result: PriceFeatureResult) -> dict:
    """Return independent display values/diagnostics without recalculating quality.

    Only all four supplied states PASS permits method-specific values. Provenance
    is declared rather than authenticated; no current-readiness or trading vote
    follows from this display. Raw input prices/volume/notional are never emitted.
    """
    if type(result) is not PriceFeatureResult:
        raise TypeError("exact_price_feature_result_required")
    checks = (result.numeric, result.coverage, result.basis, result.temporal)
    show = all(check.state is State.PASS for check in checks)
    opening = result.method is PriceFeatureMethod.OPENING_RANGE
    return {
        "schema": "market-feature-display-v2",
        "method": result.method.value,
        "method_label": _LABELS[result.method],
        "opening_minutes": result.opening_minutes,
        "values": {
            "high": _decimal(result.high) if show and opening else None,
            "low": _decimal(result.low) if show and opening else None,
            "vwap": _ratio(result.vwap) if show and not opening else None,
        },
        "diagnostics": {
            name: _metadata(getattr(result, name))
            for name in ("numeric", "coverage", "basis", "temporal")
        },
        "clocks": {
            "decision_at": _metadata(result.decision_at),
            "evaluation_at": _metadata(result.evaluation_at),
            "cutoff": _metadata(result.cutoff),
        },
        "provenance": {
            "semantics": "declared_not_authenticated",
            "grid": _metadata(result.grid),
            "quality_policy": _metadata(result.quality_policy),
            "session_facts": _metadata(result.session_facts),
            "input_metadata": [
                {
                    "observation": _metadata(bar.observation),
                    "interval_start": _metadata(bar.interval_start),
                    "interval_end": _metadata(bar.interval_end),
                    "timestamp_convention": bar.timestamp_convention.value,
                    "session": _metadata(bar.session),
                    "basis": _metadata(bar.basis),
                    "notional_reference": _metadata(bar.notional_reference),
                }
                for bar in result.inputs
            ],
        },
    }
