"""Evidence contract for completed Binance spot UTC daily closes.

Crypto daily close convention (``binance_utc_close_v1``)
--------------------------------------------------------
Crypto trades 24/7, so GRID's daily crypto "close" is defined by the clock,
not by an exchange session:

* **Bar**: Binance spot ``{COIN}USDT`` 1d kline requested with
  ``timeZone=0``, i.e. the bar covering ``[obs_date 00:00:00.000 UTC,
  obs_date 23:59:59.999 UTC]``. The close is that bar's last trade price.
* **obs_date**: the UTC calendar day the bar covers (its open time).
* **available_at**: ``obs_date + 1 day 00:00 UTC`` (``bar_available_at``).
  A bar is never stored, resolved or read before that instant; the open
  bar of the current UTC day is rejected.
* **release / vintage**: ``raw_series.pull_timestamp`` (the capture time,
  always ``>= available_at``) and, after resolution, its UTC date as
  ``resolved_series.release_date``. A bar captured late (catch-up or
  backfill) is released when GRID captured it, never back-dated, so a
  point-in-time read as of an earlier date does not see it.
* **Basis**: USDT, not USD. The BTC/ETH/SOL canonical features
  (``btc_full``/``eth_full``/``sol_full``) resolve from these series.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

CONTRACT_VERSION = "binance_utc_close_v1"
PUBLIC_DATA_HOST = "data-api.binance.vision"
DAY_MS = 86_400_000
CANONICAL_CLOSE_SERIES = frozenset({
    "binance.BTCUSDT.close", "binance.ETHUSDT.close", "binance.SOLUSDT.close",
    "BINANCE:BTCUSDT:close", "BINANCE:ETHUSDT:close", "BINANCE:SOLUSDT:close",
})


def bar_available_at(obs_date: date) -> datetime:
    """The first instant the UTC daily bar for ``obs_date`` is complete."""
    return datetime.combine(obs_date + timedelta(days=1), time.min, timezone.utc)


def last_completed_obs_date(now: datetime) -> date:
    """The latest UTC day whose daily bar has closed at ``now``."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    return now.astimezone(timezone.utc).date() - timedelta(days=1)


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
