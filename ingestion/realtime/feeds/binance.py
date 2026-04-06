"""Binance combined-stream WebSocket client for real-time crypto trades.

Connects to Binance.US WebSocket (US-compliant) and subscribes to
@trade streams for all configured symbols. Parses trade messages and
feeds them into the CandleBuilder.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder

CRYPTO_SYMBOLS = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
    "taousdt", "dogeusdt", "adausdt", "avaxusdt", "linkusdt",
    "dotusdt", "maticusdt", "uniusdt", "aaveusdt", "mkrusdt",
    "snxusdt", "crvusdt", "shibusdt", "ltcusdt", "atomusdt",
    "nearusdt", "pepeusdt", "wifusdt", "arbusdt", "opusdt",
    "suiusdt", "aptusdt", "seiusdt", "fetusdt", "renderusdt",
    "injusdt",
]

# Binance.US endpoint — global binance.com returns HTTP 451 from US IPs
BINANCE_ENDPOINTS = [
    "wss://stream.binance.us:9443/stream",
    "wss://stream.binance.com:9443/stream",
]
MAX_BACKOFF = 60


async def run_binance_feed(builder: CandleBuilder) -> None:
    """Connect to Binance combined stream and ingest trades forever."""
    import websockets

    streams = "/".join(f"{s}@trade" for s in CRYPTO_SYMBOLS)
    endpoint_idx = 0
    backoff = 1

    while True:
        base_url = BINANCE_ENDPOINTS[endpoint_idx % len(BINANCE_ENDPOINTS)]
        url = f"{base_url}?streams={streams}"
        try:
            log.info("Binance WS connecting to {url} — {n} streams", url=base_url, n=len(CRYPTO_SYMBOLS))
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1
                log.info("Binance WS connected")

                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                        data = msg.get("data", {})
                        if data.get("e") != "trade":
                            continue

                        symbol = data["s"]  # "BTCUSDT"
                        price = float(data["p"])
                        qty = float(data["q"])
                        trade_time = datetime.fromtimestamp(
                            data["T"] / 1000, tz=timezone.utc
                        )

                        builder.ingest(symbol, price, qty, trade_time, "crypto", "binance")

                    except (KeyError, ValueError, TypeError) as exc:
                        log.debug("Binance parse error: {e}", e=str(exc))

        except asyncio.CancelledError:
            log.info("Binance WS feed cancelled — shutting down")
            return
        except Exception as exc:
            log.warning(
                "Binance WS disconnected: {err} — reconnecting in {s}s",
                err=str(exc), s=backoff,
            )
            # On 451 (geo-block), try next endpoint immediately
            if "451" in str(exc):
                endpoint_idx += 1
                log.info("Trying next Binance endpoint...")
                continue
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
