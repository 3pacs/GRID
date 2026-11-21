"""Versioned descriptive wing IV proxies, never measurements of 25-delta IV.

One expiry and one observed spot per invocation. No delta model, extrapolation,
ATM fallback or historical-method inference. IVs use annualized decimal units.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Any, Iterable, Mapping

WING_METHOD = "fixed_moneyness_90_110_band3_mean_v1"
PUT_WING = "iv_m90_put_v1"
CALL_WING = "iv_m110_call_v1"
WING_SPREAD = "iv_m90_m110_spread_v1"


def positive_finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _day(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def fixed_moneyness_wings(
    calls: Iterable[Mapping[str, Any]], puts: Iterable[Mapping[str, Any]],
    spot: Any, *, expiry: Any, snap_date: Any, source: str | None,
    capture_batch_id: str | None = None, spot_observed_at: str | None = None,
) -> dict[str, Any]:
    """Equal-weight valid IVs at K/S in [0.87,0.93] puts / [1.07,1.13] calls.

    Inputs must already be scoped to the stated expiry/capture. A single valid
    contract is a sparse but defined proxy; the sample count and exact strikes
    are retained. Missing sides remain missing. No liquidity/execution claim.
    """
    price = positive_finite(spot)
    exp, snap = _day(expiry), _day(snap_date)
    dte = (exp - snap).days if exp and snap else None

    def side(rows, lower, upper):
        selected = []
        if price is not None and dte is not None and dte > 0:
            for row in rows:
                strike, iv = positive_finite(row.get("strike")), positive_finite(row.get("iv"))
                if strike is not None and iv is not None and lower <= strike / price <= upper:
                    selected.append((strike, iv))
        selected.sort()
        # Scale before adding to keep a large finite sample mean finite.
        value = math.fsum(iv / len(selected) for _, iv in selected) if selected else None
        return value, {"count": len(selected), "strikes": [s for s, _ in selected]}

    put, put_sample = side(puts, .87, .93)
    call, call_sample = side(calls, 1.07, 1.13)
    return {
        PUT_WING: put, CALL_WING: call,
        "wing_iv_metadata": {
            "method": WING_METHOD, "units": "annualized_decimal",
            "expiry": exp.isoformat() if exp else None,
            "snap_date": snap.isoformat() if snap else None, "dte": dte,
            "source": source, "capture_batch_id": capture_batch_id,
            "spot": price, "spot_basis": "provider_regular_market_price",
            "spot_observed_at": spot_observed_at,
            "put": put_sample, "call": call_sample,
        },
    }


def wing_comparison_key(metadata: Any) -> tuple | None:
    """Only compare known methods, provider, spot basis and exact calendar DTE.

    Different expiry dates are expected across days; different tenors are not.
    Never infer a method for old rows from their dates or values.
    """
    if not isinstance(metadata, dict) or metadata.get("method") != WING_METHOD:
        return None
    if (metadata.get("units") != "annualized_decimal"
            or metadata.get("spot_basis") != "provider_regular_market_price"
            or positive_finite(metadata.get("spot")) is None):
        return None
    exp, snap = _day(metadata.get("expiry")), _day(metadata.get("snap_date"))
    dte, source = metadata.get("dte"), metadata.get("source")
    if (type(dte) is not int or dte <= 0 or not exp or not snap
            or (exp - snap).days != dte or not isinstance(source, str) or not source.strip()):
        return None
    return WING_METHOD, source, dte


def public_wing_fields(put: Any, call: Any, metadata: Any, legacy_put: Any = None, legacy_call: Any = None) -> dict:
    """Keep deprecated API keys nullable; expose legacy numbers as unknown."""
    known = wing_comparison_key(metadata) is not None
    return {
        "iv_25d_put": None, "iv_25d_call": None,
        PUT_WING: positive_finite(put) if known else None,
        CALL_WING: positive_finite(call) if known else None,
        "wing_iv_metadata": metadata,
        "legacy_wing_iv": {
            "method": "unknown_legacy", "iv_25d_put": positive_finite(legacy_put),
            "iv_25d_call": positive_finite(legacy_call),
        },
    }
