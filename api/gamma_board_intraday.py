"""Completed one-minute cash-session candles for the private Gamma Board."""

from __future__ import annotations

from collections import deque
from datetime import datetime, time, timedelta, timezone
import logging
import math
from threading import Lock
from time import monotonic
from zoneinfo import ZoneInfo

import requests

from api.gamma_board_symbols import provider_symbol
from ingestion.market_calendar import (
    is_market_open,
    last_trading_day,
    market_close_time,
)
from ingestion.schwab_market import SchwabMarketClient
from utils.single_flight import single_flight
from utils.ttl_cache import TTLCache

NY = ZoneInfo("America/New_York")
UTC = timezone.utc
MINUTE = timedelta(minutes=1)
_CACHE = TTLCache(ttl=300, max_size=96)
_ERRORS = TTLCache(ttl=15, max_size=96)
_LIMIT_PER_MINUTE = 30
_STARTS = deque()
_BUDGET_LOCK = Lock()
log = logging.getLogger(__name__)


def _clock():
    return datetime.now(UTC)


def _session(now):
    local = now.astimezone(NY)
    day = local.date()
    if local.time() < time(9, 30):
        day -= timedelta(days=1)
    day = last_trading_day(day)
    start = datetime.combine(day, time(9, 30), NY).astimezone(UTC)
    end = datetime.combine(day, market_close_time(day), NY).astimezone(UTC)
    cutoff = min(now.replace(second=0, microsecond=0), end)
    return start, end, cutoff


def _admit():
    with _BUDGET_LOCK:
        tick = monotonic()
        while _STARTS and _STARTS[0] <= tick - 60:
            _STARTS.popleft()
        if len(_STARTS) >= _LIMIT_PER_MINUTE:
            return False
        _STARTS.append(tick)
        return True


def _history(symbol, start, cutoff):
    with requests.Session() as http:
        reader = SchwabMarketClient(session=http, timeout=8, max_retries=0)
        return reader.price_history(
            provider_symbol(symbol),
            periodType="day",
            frequencyType="minute",
            frequency=1,
            startDate=int(start.timestamp() * 1000),
            endDate=int(cutoff.timestamp() * 1000) - 1,
            needExtendedHoursData="false",
            needPreviousClose="false",
        )


def _bars(symbol, body, start, cutoff):
    if not isinstance(body, dict) or body.get("symbol") != provider_symbol(symbol):
        raise ValueError("Unexpected underlying")
    rows = body.get("candles")
    if not isinstance(rows, list) or len(rows) > 1500:
        raise ValueError("Unexpected candle window")
    by_time = {}
    conflicts = set()
    first = int(start.timestamp() * 1000)
    until = int(cutoff.timestamp() * 1000)
    for row in rows:
        if not isinstance(row, dict):
            continue
        stamp = row.get("datetime")
        if type(stamp) is not int or not first <= stamp < until or stamp % 60000:
            continue
        values = [row.get(k) for k in ("open", "high", "low", "close")]
        if any(
            type(v) not in (int, float) or not math.isfinite(v) or v <= 0
            for v in values
        ):
            continue
        opening, high, low, close = values
        if low > min(opening, close) or high < max(opening, close):
            continue
        volume = row.get("volume")
        if symbol == "SPX" or type(volume) is not int or volume < 0:
            volume = None
        bar = dict(
            time=datetime.fromtimestamp(stamp / 1000, UTC).isoformat(),
            open=opening,
            high=high,
            low=low,
            close=close,
            volume=volume,
        )
        if stamp in by_time and by_time[stamp] != bar:
            conflicts.add(stamp)
        by_time[stamp] = bar
    return [by_time[stamp] for stamp in sorted(by_time.keys() - conflicts)]


@single_flight
def _read(symbol):
    now = _clock()
    start, end, cutoff = _session(now)
    expected = int((cutoff - start) / MINUTE)
    base = dict(
        symbol=symbol,
        source_label="Schwab price history",
        interval_minutes=1,
        session_date=start.astimezone(NY).date().isoformat(),
        session_open=start <= now < end,
        session_start=start.isoformat(),
        session_end=end.isoformat(),
        expected_bars=expected,
        received_at=None,
        as_of=None,
        bars=[],
        missing_bars=expected,
    )
    if not expected:
        return {
            **base,
            "status": "unavailable",
            "reason": "Waiting for the first completed minute.",
        }
    key = symbol + "|" + cutoff.isoformat()
    cached = _CACHE.get(key) or _ERRORS.get(key)
    if cached is not None:
        # Same minute-key can span 16:00, when the cash-session badge changes.
        return {**cached, "session_open": base["session_open"]}
    if not _admit():
        return {
            **base,
            "status": "unavailable",
            "reason": "Chart request limit reached. Retry in one minute.",
        }
    try:
        bars = _bars(symbol, _history(symbol, start, cutoff), start, cutoff)
    except Exception as exc:
        log.warning(
            "Intraday source unavailable for %s (%s)", symbol, type(exc).__name__
        )
        result = {
            **base,
            "status": "unavailable",
            "reason": "Price history unavailable.",
        }
        _ERRORS.set(key, result)
        return result
    base["received_at"] = _clock().isoformat()
    if not bars:
        result = {
            **base,
            "status": "unavailable",
            "reason": "No completed one-minute bars returned for this session.",
        }
        _ERRORS.set(key, result)
        return result
    result = {
        **base,
        "bars": bars,
        "missing_bars": expected - len(bars),
        "status": "available",
        "as_of": (datetime.fromisoformat(bars[-1]["time"]) + MINUTE).isoformat(),
    }
    _CACHE.set(key, result)
    return result


def read_intraday(symbol, enabled):
    provider_symbol(symbol)
    if not enabled:
        return {
            "symbol": symbol,
            "status": "unavailable",
            "reason": "Private intraday source is disabled.",
            "bars": [],
        }
    return _read(symbol)
