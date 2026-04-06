"""DEX token scanner — GeckoTerminal + DexScreener liquidity spike detection.

Polls trending pools on Solana and Ethereum every 60 seconds. Detects
volume spikes, new high-liquidity pools, and price surges. Writes
signals to signal_data and feeds hot token prices into CandleBuilder.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder

POLL_INTERVAL = 60
WATCHED_TOKEN_TTL = 86400  # 24 hours

VOLUME_SPIKE_MULTIPLIER = 3.0
NEW_POOL_MIN_LIQUIDITY = 50_000.0
NEW_POOL_MAX_AGE_HOURS = 24.0
PRICE_SURGE_THRESHOLD = 20.0  # percent


@dataclass
class PoolData:
    symbol: str
    chain: str
    dex: str
    pool_address: str
    price: float
    volume_24h: float
    volume_avg_24h: float
    liquidity: float
    price_change_1h: float
    pool_age_hours: float


def detect_spikes(pools: list[PoolData]) -> list[dict]:
    """Apply spike detection rules. Returns list of signal dicts."""
    signals: list[dict] = []
    for p in pools:
        if p.volume_avg_24h > 0 and p.volume_24h / p.volume_avg_24h >= VOLUME_SPIKE_MULTIPLIER:
            signals.append({
                "signal_type": "dex_liquidity_spike",
                "ticker": p.symbol,
                "direction": "spike_volume",
                "magnitude": round(p.volume_24h / p.volume_avg_24h, 1),
                "data": _pool_metadata(p),
            })
            continue
        if p.pool_age_hours < NEW_POOL_MAX_AGE_HOURS and p.liquidity >= NEW_POOL_MIN_LIQUIDITY:
            signals.append({
                "signal_type": "dex_liquidity_spike",
                "ticker": p.symbol,
                "direction": "new_pool",
                "magnitude": p.liquidity,
                "data": _pool_metadata(p),
            })
            continue
        if abs(p.price_change_1h) >= PRICE_SURGE_THRESHOLD:
            signals.append({
                "signal_type": "dex_liquidity_spike",
                "ticker": p.symbol,
                "direction": "price_surge",
                "magnitude": round(p.price_change_1h, 1),
                "data": _pool_metadata(p),
            })
    return signals


def _pool_metadata(p: PoolData) -> dict:
    return {
        "chain": p.chain, "dex": p.dex, "pool_address": p.pool_address,
        "volume_24h": p.volume_24h, "liquidity": p.liquidity,
        "price_change_1h": p.price_change_1h, "pool_age_hours": p.pool_age_hours,
        "price": p.price,
    }


async def run_dex_scanner(builder: CandleBuilder) -> None:
    """Poll DEX APIs every 60s, detect spikes, write signals, feed prices."""
    import aiohttp

    watched_tokens: dict[str, float] = {}

    while True:
        try:
            await asyncio.sleep(POLL_INTERVAL)

            async with aiohttp.ClientSession() as session:
                pools = await _fetch_all_pools(session)

            if not pools:
                continue

            spikes = detect_spikes(pools)

            if spikes:
                _write_signals(spikes)
                for s in spikes:
                    watched_tokens[s["ticker"]] = (
                        datetime.now(tz=timezone.utc).timestamp() + WATCHED_TOKEN_TTL
                    )
                log.info("DEX scanner — {n} spikes detected", n=len(spikes))

            now = datetime.now(tz=timezone.utc)
            _expire_watched(watched_tokens, now)

            for p in pools:
                if p.symbol in watched_tokens or p.volume_24h > 100_000:
                    builder.ingest(p.symbol, p.price, p.volume_24h, now, "dex_token", "dex")

        except asyncio.CancelledError:
            log.info("DEX scanner cancelled — shutting down")
            return
        except Exception as exc:
            log.warning("DEX scanner error: {err}", err=str(exc))


def _expire_watched(watched: dict[str, float], now: datetime) -> None:
    cutoff = now.timestamp()
    expired = [k for k, v in watched.items() if v < cutoff]
    for k in expired:
        del watched[k]


async def _fetch_all_pools(session) -> list[PoolData]:
    """Fetch trending pools from GeckoTerminal + DexScreener."""
    import aiohttp
    pools: list[PoolData] = []

    for network in ["solana", "eth"]:
        chain = "solana" if network == "solana" else "ethereum"
        try:
            url = f"https://api.geckoterminal.com/api/v2/networks/{network}/trending_pools"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data.get("data", []):
                        pool = _parse_geckoterminal(item, chain)
                        if pool:
                            pools.append(pool)
        except Exception as exc:
            log.debug("GeckoTerminal {net} error: {e}", net=network, e=str(exc))

    try:
        url = "https://api.dexscreener.com/token-boosts/latest/v1"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                for item in data if isinstance(data, list) else []:
                    pool = _parse_dexscreener_boost(item)
                    if pool:
                        pools.append(pool)
    except Exception as exc:
        log.debug("DexScreener error: {e}", e=str(exc))

    return pools


def _parse_geckoterminal(item: dict, chain: str) -> PoolData | None:
    try:
        attrs = item.get("attributes", {})
        name = attrs.get("name", "")
        base_token = name.split("/")[0].strip() if "/" in name else name
        symbol = f"{chain[:3].upper()}:{base_token}"
        return PoolData(
            symbol=symbol, chain=chain,
            dex=attrs.get("dex_id", "unknown"),
            pool_address=attrs.get("address", ""),
            price=float(attrs.get("base_token_price_usd") or 0),
            volume_24h=float(attrs.get("volume_usd", {}).get("h24") or 0),
            volume_avg_24h=float(attrs.get("volume_usd", {}).get("h24") or 0) / 1.5,
            liquidity=float(attrs.get("reserve_in_usd") or 0),
            price_change_1h=float(attrs.get("price_change_percentage", {}).get("h1") or 0),
            pool_age_hours=_pool_age_hours(attrs.get("pool_created_at")),
        )
    except Exception:
        return None


def _parse_dexscreener_boost(item: dict) -> PoolData | None:
    try:
        chain_id = item.get("chainId", "")
        if chain_id not in ("solana", "ethereum"):
            return None
        chain = chain_id
        symbol = f"{chain[:3].upper()}:{item.get('tokenAddress', '')[:8]}"
        return PoolData(
            symbol=symbol, chain=chain, dex="dexscreener",
            pool_address=item.get("tokenAddress", ""),
            price=0.0, volume_24h=0.0, volume_avg_24h=0.0,
            liquidity=0.0, price_change_1h=0.0, pool_age_hours=0.0,
        )
    except Exception:
        return None


def _pool_age_hours(created_at: str | None) -> float:
    if not created_at:
        return 9999.0
    try:
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        delta = datetime.now(tz=timezone.utc) - created
        return delta.total_seconds() / 3600
    except Exception:
        return 9999.0


def _write_signals(signals: list[dict]) -> None:
    try:
        from db import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                for s in signals:
                    cur.execute(
                        "INSERT INTO signal_data (signal_type, signal_date, ticker, direction, magnitude, data, confidence) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (s["signal_type"], date.today(), s["ticker"], s["direction"], s["magnitude"], json.dumps(s["data"]), "derived"),
                    )
        log.debug("Wrote {n} DEX signals to signal_data", n=len(signals))
    except Exception as exc:
        log.warning("Failed to write DEX signals: {e}", e=str(exc))
