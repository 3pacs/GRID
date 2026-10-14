"""yfinance daily closes and market caps (pre-registration §5, §6).

Both legs (issuer and SPY) of every return come from one ``download`` call
on identical session dates. ``auto_adjust`` is always passed explicitly
(repository convention, ``tests/test_yfinance_auto_adjust_explicit.py``).
The seams tests replace are :meth:`YFinancePrices._download` and
:meth:`YFinancePrices._market_cap`; no test touches the network.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable

from loguru import logger as log

Closes = dict[str, dict[date, float]]


class YFinancePrices:
    def __init__(self, attempts: int = 2) -> None:
        self.attempts = attempts
        self.failures: list[str] = []

    # ── seams ────────────────────────────────────────────────────────────
    def _download(self, symbols: list[str], start: date, end: date, adjusted: bool):
        import yfinance as yf

        return yf.download(
            symbols,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=adjusted,
            actions=False,
            group_by="ticker",
            progress=False,
            threads=False,
        )

    def _market_cap(self, symbol: str) -> float | None:
        import yfinance as yf

        value = yf.Ticker(symbol).fast_info["marketCap"]
        return float(value) if value else None

    # ── API ──────────────────────────────────────────────────────────────
    def closes(self, symbols: Iterable[str], start: date, end: date, *, adjusted: bool) -> Closes:
        """symbol -> {session date: close}; a symbol with no data maps to {}."""
        symbols = sorted(set(symbols))
        out: Closes = {s: {} for s in symbols}
        if not symbols:
            return out
        frame = None
        for attempt in range(self.attempts):
            try:
                frame = self._download(symbols, start, end, adjusted)
                break
            except Exception as exc:  # noqa: BLE001 - surfaced in the run record
                log.warning("trade_edge: yfinance download failed (attempt {a}): {e}", a=attempt + 1, e=exc)
                self.failures.append(f"download:{type(exc).__name__}")
        if frame is None or getattr(frame, "empty", True):
            return out
        for s in symbols:
            try:
                sub = frame[s] if s in frame.columns.get_level_values(0) else None
            except (AttributeError, KeyError):
                sub = None
            if sub is None and len(symbols) == 1 and "Close" in frame.columns:
                sub = frame
            if sub is None or "Close" not in sub:
                continue
            series = sub["Close"].dropna()
            out[s] = {ts.date(): float(v) for ts, v in series.items() if float(v) > 0}
        return out

    def market_cap(self, symbol: str) -> float | None:
        try:
            return self._market_cap(symbol)
        except Exception as exc:  # noqa: BLE001 - bucket becomes "unknown"
            self.failures.append(f"market_cap:{symbol}:{type(exc).__name__}")
            return None
