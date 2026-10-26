"""Yahoo Finance HTTP poller for traditional market data.

Polls yfinance every 60 seconds for SPY and selected traditional markets.
Feeds the provider's bar time into CandleBuilder, not the poll time.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder

POLL_INTERVAL = 60

SYMBOLS: dict[str, str] = {
    # Intraday SPY observation is separate from the completed daily close used
    # for scoring. Yahoo may deliver this exchange bar with a delay.
    "SPY": "equity",
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
    # Bond yields (indices — stale on weekends but canonical)
    "^TNX": "bond", "^TYX": "bond", "^FVX": "bond",
    # Treasury futures (trade 24/5 — fresh data on weekends)
    "ZN=F": "bond", "ZB=F": "bond", "ZF=F": "bond",
}


async def run_yahoo_feed(builder: CandleBuilder) -> None:
    """Poll Yahoo Finance every 60s and feed prices into CandleBuilder."""
    consecutive_failures = 0
    last_seen: dict[str, datetime] = {}

    while True:
        try:
            await asyncio.sleep(POLL_INTERVAL)

            prices = await asyncio.get_event_loop().run_in_executor(None, _fetch_prices)

            ingested = 0
            for symbol, (price, volume, observed_at) in prices.items():
                if observed_at <= last_seen.get(symbol, datetime.min.replace(tzinfo=timezone.utc)):
                    continue
                asset_class = SYMBOLS.get(symbol, "other")
                builder.ingest(symbol, price, volume, observed_at, asset_class, "yahoo")
                last_seen[symbol] = observed_at
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


def _fetch_prices() -> dict[str, tuple[float, float, datetime]]:
    """Return price, volume and the actual timestamp of the provider's last bar."""
    import yfinance as yf

    tickers = list(SYMBOLS.keys())
    result: dict[str, tuple[float, float, datetime]] = {}

    try:
        # period='5d' ensures data on weekends (period='1d' returns empty for some symbols)
        # auto_adjust=False: this is the live tape feeding the candle
        # builder, so it must carry actual traded prices rather than a
        # back-adjusted series. yfinance's default flipped to True in 0.2.x.
        data = yf.download(
            tickers, period="5d", interval="1m", progress=False, threads=True,
            auto_adjust=False,
        )
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

                closes = close_col.dropna()
                last_close = closes.iloc[-1]
                last_vol = vol_col.dropna().iloc[-1] if not vol_col.dropna().empty else 0.0
                observed_at = closes.index[-1].to_pydatetime()
                if observed_at.tzinfo is None:
                    # An unzoned bar cannot substantiate an intraday freshness
                    # claim. Do not stamp it with the poll clock.
                    continue
                observed_at = observed_at.astimezone(timezone.utc)

                if last_close > 0:
                    result[symbol] = (float(last_close), float(last_vol), observed_at)
            except (KeyError, IndexError):
                continue

    except Exception as exc:
        log.debug("yfinance bulk download error: {e}", e=str(exc))

    return result
