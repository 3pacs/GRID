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
SPOT_FLOW_MEASUREMENT = "binance_spot_taker_5m_v1"
FIVE_MINUTES_MS = 300_000
DAY_MS = 86_400_000
CANONICAL_CLOSE_SERIES = frozenset({
    "binance.BTCUSDT.close", "binance.ETHUSDT.close", "binance.SOLUSDT.close",
    "BINANCE:BTCUSDT:close", "BINANCE:ETHUSDT:close", "BINANCE:SOLUSDT:close",
})


def completed_spot_flow_payload(kline: list[Any], instrument: str,
                                captured_at: datetime) -> dict[str, Any] | None:
    """Validate a provider 5m kline, retaining measured taker-side volumes.

    These are anonymous aggressor cohorts, not identified whales or fund flows.
    Field 7 is total quote volume; field 10 is taker-buy quote volume. Their
    difference is taker-sell quote volume. Never derive direction from price.
    See Binance's Spot REST market-data klines schema (fields 0..11).
    """
    import math

    if (captured_at.tzinfo is None or instrument not in {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
            or not isinstance(kline, list) or len(kline) != 12):
        raise ValueError("Invalid spot flow instrument, clock or kline")
    if any(type(kline[i]) is not int for i in (0, 6, 8)):
        raise ValueError("Spot flow timestamps and trade count must be integers")
    opened, closed, count = kline[0], kline[6], kline[8]
    if opened < 0 or opened % FIVE_MINUTES_MS or closed != opened + FIVE_MINUTES_MS - 1 or count < 0:
        raise ValueError("Not a complete UTC 5m kline")
    if int(captured_at.timestamp() * 1000) <= closed:
        return None
    values = {}
    for name, i in {"open": 1, "high": 2, "low": 3, "close": 4,
                    "base_volume": 5, "quote_volume": 7,
                    "taker_buy_base": 9, "taker_buy_quote": 10}.items():
        if isinstance(kline[i], bool) or not isinstance(kline[i], (str, int, float)):
            raise ValueError("Invalid spot flow number")
        values[name] = float(kline[i])
    if (not all(math.isfinite(v) and v >= 0 for v in values.values())
            or min(values[k] for k in ("open", "high", "low", "close")) <= 0
            or not values["low"] <= min(values["open"], values["close"]) <= max(values["open"], values["close"]) <= values["high"]
            or values["taker_buy_base"] > values["base_volume"]
            or values["taker_buy_quote"] > values["quote_volume"]):
        raise ValueError("Inconsistent spot flow volumes or OHLC")
    for base, quote in ((values["base_volume"], values["quote_volume"]),
                        (values["taker_buy_base"], values["taker_buy_quote"])):
        tolerance = 1e-6 * max(quote, 1)
        if not values["low"] * base - tolerance <= quote <= values["high"] * base + tolerance:
            raise ValueError("Quote flow is inconsistent with traded base units")
    if (count == 0) != (values["base_volume"] == 0):
        raise ValueError("Trade count is inconsistent with volume")
    return {"measurement": SPOT_FLOW_MEASUREMENT, "instrument": instrument,
            "endpoint_host": PUBLIC_DATA_HOST, "venue": "Binance global spot",
            "interval": "5m", "kline": kline,
            "captured_at": captured_at.astimezone(timezone.utc).isoformat(),
            "bar_end": datetime.fromtimestamp((closed + 1) / 1000, timezone.utc).isoformat(),
            "trade_count": count, **values,
            "taker_sell_quote": values["quote_volume"] - values["taker_buy_quote"]}


def verified_spot_flow_payload(payload: Any, available_at: datetime,
                              cutoff: datetime) -> dict[str, Any] | None:
    """Recompute fields from the stored source kline and bound availability."""
    try:
        if (not isinstance(payload, dict) or payload.get("measurement") != SPOT_FLOW_MEASUREMENT
                or payload.get("endpoint_host") != PUBLIC_DATA_HOST
                or available_at.tzinfo is None or cutoff.tzinfo is None or available_at > cutoff):
            return None
        captured = datetime.fromisoformat(payload["captured_at"])
        if captured.tzinfo is None or captured > available_at:
            return None
        recomputed = completed_spot_flow_payload(payload["kline"], payload["instrument"], captured)
        return recomputed if recomputed is not None and recomputed == payload else None
    except (KeyError, ValueError, TypeError, OverflowError, AttributeError):
        return None


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
