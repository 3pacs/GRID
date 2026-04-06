"""Yahoo Finance HTTP poller for traditional market data.

Polls yfinance every 60 seconds for metals, energy, grains, index futures,
forex, and bond yields. Feeds last price into CandleBuilder.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder

POLL_INTERVAL = 60

SYMBOLS: dict[str, str] = {
    # Metals
    "GC=F": "metal", "SI=F": "metal", "PL=F": "metal",
    "PA=F": "metal", "HG=F": "metal",
    # Energy
    "CL=F": "energy", "BZ=F": "energy", "NG=F": "energy", "HO=F": "energy",
    # Grains / Softs
    "ZC=F": "grain", "ZS=F": "grain", "ZW=F": "grain",
    "KC=F": "grain", "SB=F": "grain", "CT=F": "grain",
    # Index Futures
    "ES=F": "index", "NQ=F": "index", "YM=F": "index",
    "RTY=F": "index", "NKD=F": "index",
    # Forex
    "EURUSD=X": "forex", "GBPUSD=X": "forex", "USDJPY=X": "forex",
    "USDCHF=X": "forex", "AUDUSD=X": "forex", "USDCAD=X": "forex",
    "NZDUSD=X": "forex", "USDCNH=X": "forex",
    # Bond yields
    "^TNX": "bond", "^TYX": "bond", "^FVX": "bond",
}


async def run_yahoo_feed(builder: CandleBuilder) -> None:
    """Poll Yahoo Finance every 60s and feed prices into CandleBuilder."""
    consecutive_failures = 0

    while True:
        try:
            await asyncio.sleep(POLL_INTERVAL)

            prices = await asyncio.get_event_loop().run_in_executor(None, _fetch_prices)

            now = datetime.now(tz=timezone.utc)
            ingested = 0
            for symbol, (price, volume) in prices.items():
                asset_class = SYMBOLS.get(symbol, "other")
                builder.ingest(symbol, price, volume, now, asset_class, "yahoo")
                ingested += 1

            if ingested > 0:
                log.debug("Yahoo poll — {n}/{t} symbols updated", n=ingested, t=len(SYMBOLS))
                consecutive_failures = 0

        except asyncio.CancelledError:
            log.info("Yahoo feed cancelled — shutting down")
            return
        except Exception as exc:
            consecutive_failures += 1
            log.warning("Yahoo poll failed ({n}): {err}", n=consecutive_failures, err=str(exc))
            if consecutive_failures >= 5:
                log.error("Yahoo feed: 5 consecutive failures, backing off to 120s")
                await asyncio.sleep(120)
                consecutive_failures = 0


def _fetch_prices() -> dict[str, tuple[float, float]]:
    """Synchronous yfinance fetch. Returns {symbol: (price, volume)}."""
    import yfinance as yf

    tickers = list(SYMBOLS.keys())
    result: dict[str, tuple[float, float]] = {}

    try:
        data = yf.download(tickers, period="1d", interval="1m", progress=False, threads=True)
        if data.empty:
            return result

        for symbol in tickers:
            try:
                if len(tickers) == 1:
                    close_col = data["Close"]
                    vol_col = data["Volume"]
                else:
                    close_col = data["Close"][symbol]
                    vol_col = data["Volume"][symbol]

                last_close = close_col.dropna().iloc[-1]
                last_vol = vol_col.dropna().iloc[-1] if not vol_col.dropna().empty else 0.0

                if last_close > 0:
                    result[symbol] = (float(last_close), float(last_vol))
            except (KeyError, IndexError):
                continue

    except Exception as exc:
        log.debug("yfinance bulk download error: {e}", e=str(exc))

    return result
