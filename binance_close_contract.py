"""Evidence contract for completed Binance spot UTC daily closes."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

CONTRACT_VERSION = "binance_utc_close_v1"
PUBLIC_DATA_HOST = "data-api.binance.vision"
DAY_MS = 86_400_000
CANONICAL_CLOSE_SERIES = frozenset({
    "binance.BTCUSDT.close", "binance.ETHUSDT.close",
    "BINANCE:BTCUSDT:close", "BINANCE:ETHUSDT:close",
})


def completed_kline_payload(kline: list[Any], captured_at: datetime) -> tuple[date, dict[str, Any]] | None:
    """Return a closed UTC 1d bar's date/evidence, or None for the open bar.

    Malformed or non-UTC bars fail the pull rather than entering raw_series.
    Binance's daily kline has open time at index 0 and inclusive close time
    at index 6. The returned close time alone is not proof the bar has ended.
    """
    if captured_at.tzinfo is None or len(kline) < 7:
        raise ValueError("Binance kline lacks timezone or required fields")
    open_ms, close_ms = int(kline[0]), int(kline[6])
    if open_ms % DAY_MS or close_ms != open_ms + DAY_MS - 1:
        raise ValueError("Binance kline is not a UTC 1d bar")
    captured_utc = captured_at.astimezone(timezone.utc)
    if int(captured_utc.timestamp() * 1000) <= close_ms:
        return None
    obs_date = datetime.fromtimestamp(open_ms / 1000, timezone.utc).date()
    return obs_date, {
        "close_contract": CONTRACT_VERSION,
        "endpoint_host": PUBLIC_DATA_HOST,
        "interval": "1d",
        "time_zone": "UTC",
        "open_time_ms": open_ms,
        "close_time_ms": close_ms,
        "captured_at_utc": captured_utc.isoformat(),
    }


def is_completed_canonical_close(payload: Any, obs_date: date, pulled_at: datetime) -> bool:
    """Require source timestamps and database availability after the UTC close."""
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return False
    if (not isinstance(payload, dict)
            or payload.get("close_contract") != CONTRACT_VERSION
            or payload.get("endpoint_host") != PUBLIC_DATA_HOST
            or payload.get("interval") != "1d"
            or payload.get("time_zone") != "UTC"):
        return False
    try:
        open_ms = int(payload["open_time_ms"])
        close_ms = int(payload["close_time_ms"])
        captured_at = datetime.fromisoformat(payload["captured_at_utc"])
        open_day = datetime.fromtimestamp(open_ms / 1000, timezone.utc).date()
        close_end = datetime.fromtimestamp((close_ms + 1) / 1000, timezone.utc)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
    if (open_ms % DAY_MS or close_ms != open_ms + DAY_MS - 1
            or open_day != obs_date):
        return False
    if captured_at.tzinfo is None or pulled_at.tzinfo is None:
        return False
    return captured_at >= close_end and pulled_at >= close_end
