"""In-memory OHLCV candle aggregator.

Receives ticks from multiple feeds, builds 5-minute candles, and queues
completed candles for batch DB flush. Thread-safe via asyncio (single-threaded
event loop, no lock needed).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from loguru import logger as log


INTERVAL_SECONDS = 300  # 5 minutes


@dataclass
class CandleState:
    """State of a single in-progress candle."""

    symbol: str
    asset_class: str
    source: str
    interval: str
    ts_bucket: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap_numerator: float   # sum(price * volume)
    vwap_denominator: float  # sum(volume)
    trade_count: int

    @property
    def vwap(self) -> float | None:
        if self.vwap_denominator == 0:
            return None
        return self.vwap_numerator / self.vwap_denominator


class CandleBuilder:
    """Aggregates ticks into 5-minute OHLCV candles."""

    def __init__(self, interval_seconds: int = INTERVAL_SECONDS) -> None:
        self.interval_seconds = interval_seconds
        self.interval_label = f"{interval_seconds // 60}m"
        self.candles: dict[tuple[str, str], CandleState] = {}
        self.flush_queue: list[CandleState] = []

    def _bucket_floor(self, ts: datetime) -> datetime:
        """Floor a timestamp to the nearest interval boundary."""
        epoch = int(ts.timestamp())
        floored = epoch - (epoch % self.interval_seconds)
        return datetime.fromtimestamp(floored, tz=timezone.utc)

    def ingest(self, symbol, price, volume, ts, asset_class, source) -> None:
        """Ingest a single tick. If new bucket, flush old candle to queue."""
        bucket = self._bucket_floor(ts)
        key = (symbol, self.interval_label)
        existing = self.candles.get(key)

        if existing is not None and existing.ts_bucket != bucket:
            self.flush_queue.append(existing)
            existing = None

        if existing is None:
            self.candles[key] = CandleState(
                symbol=symbol, asset_class=asset_class, source=source,
                interval=self.interval_label, ts_bucket=bucket,
                open=price, high=price, low=price, close=price,
                volume=volume, vwap_numerator=price * volume,
                vwap_denominator=volume, trade_count=1,
            )
        else:
            existing.high = max(existing.high, price)
            existing.low = min(existing.low, price)
            existing.close = price
            existing.volume += volume
            existing.vwap_numerator += price * volume
            existing.vwap_denominator += volume
            existing.trade_count += 1

    def drain(self) -> list[CandleState]:
        """Return and clear all completed candles from flush queue."""
        result = list(self.flush_queue)
        self.flush_queue.clear()
        return result

    def flush_all(self) -> None:
        """Move all active candles to flush queue (for graceful shutdown)."""
        for candle in self.candles.values():
            self.flush_queue.append(candle)
        self.candles.clear()

    @property
    def active_symbols(self) -> int:
        return len(self.candles)

    @property
    def pending_flush(self) -> int:
        return len(self.flush_queue)
