"""
GRID DexScreener data ingestion module.

Pulls on-chain DEX pair data from the DexScreener public API and stores
aggregate crypto market signals in ``raw_series``.  No API key required.

Tracked signals:
- Total 24h volume across top Solana pairs
- New pair creation rate (pairs created in last 24h)
- Top token buy/sell ratio (speculative pressure)
- Liquidity depth of top pairs
- 24h price change distribution (momentum breadth)

These feed into GRID as crypto-native sentiment/risk-appetite indicators.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

BASE_URL = "https://api.dexscreener.com"

# Solana token addresses for direct lookup (returns highest-volume pairs)
# Using token address endpoint instead of text search for accurate data
SOLANA_TOKEN_ADDRESSES = [
    "So11111111111111111111111111111111111111112",   # Native SOL (wrapped)
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
    "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",  # WIF
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",    # JUP
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",   # RAY
]

# Legacy text search queries (fallback only)
SEARCH_QUERIES = [
    "SOL USDC",
    "BONK SOL",
    "WIF SOL",
    "JUP SOL",
    "RAY SOL",
]

# Rate limit: 300 req/min — we use ~10 per pull, well within limits
_REQUEST_DELAY = 0.25  # seconds between requests


class DexScreenerPuller:
    """Pulls aggregate crypto market signals from DexScreener.

    Produces daily aggregate features (not individual token prices) that
    serve as risk-appetite and speculative-froth indicators for GRID.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The source_catalog.id for DexScreener.
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("DexScreenerPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        """Look up or create source_catalog entry for DexScreener."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "DexScreener"},
            ).fetchone()

        if row is not None:
            return row[0]

        # Auto-register source
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO source_catalog "
                    "(name, base_url, cost_tier, latency_class, pit_available, "
                    "revision_behavior, trust_score, priority_rank, active) "
                    "VALUES (:name, :url, 'FREE', 'EOD', FALSE, 'NEVER', 'MED', 20, TRUE) "
                    "RETURNING id"
                ),
                {"name": "DexScreener", "url": BASE_URL},
            ).fetchone()

        log.info("Registered DexScreener in source_catalog — id={id}", id=row[0])
        return row[0]

    def _get(self, path: str) -> dict[str, Any] | None:
        """Make a GET request to the DexScreener API."""
        url = f"{BASE_URL}{path}"
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            time.sleep(_REQUEST_DELAY)
            return resp.json()
        except Exception as exc:
            log.warning("DexScreener request failed: {p} — {e}", p=path, e=str(exc))
            return None

    def pull_aggregate_signals(self) -> dict[str, Any]:
        """Pull aggregate crypto market signals and store in raw_series.

        Computes:
        - dex_sol_volume_24h: Total 24h USD volume across tracked pairs
        - dex_sol_liquidity: Total USD liquidity across tracked pairs
        - dex_sol_buy_sell_ratio: Aggregate buy/sell transaction ratio (24h)
        - dex_sol_new_pairs_momentum: Avg 24h price change across pairs
        - dex_sol_txn_count_24h: Total 24h transaction count

        Returns:
            dict: Summary with signal values and insert counts.
        """
        log.info("Pulling DexScreener aggregate signals")
        today = date.today()

        all_pairs: list[dict] = []

        # Primary: query by token address (returns top 30 pairs by volume)
        for addr in SOLANA_TOKEN_ADDRESSES:
            data = self._get(f"/latest/dex/tokens/{addr}")
            if data and isinstance(data, dict) and "pairs" in data:
                sol_pairs = [p for p in data["pairs"] if p.get("chainId") == "solana"]
                all_pairs.extend(sol_pairs[:30])

        # Fallback: text search if token lookup returned nothing
        if len(all_pairs) < 10:
            log.warning("Token address lookup returned few pairs, falling back to text search")
            for query in SEARCH_QUERIES:
                data = self._get(f"/latest/dex/search?q={query}")
                if data and "pairs" in data:
                    sol_pairs = [p for p in data["pairs"] if p.get("chainId") == "solana"]
                    all_pairs.extend(sol_pairs[:50])

        if not all_pairs:
            log.warning("No pairs returned from DexScreener")
            return {"status": "FAILED", "error": "No data", "rows_inserted": 0}

        # Also pull latest boosted tokens (speculative interest indicator)
        boost_data = self._get("/token-boosts/top/v1")
        n_boosted = len(boost_data) if isinstance(boost_data, list) else 0

        # Deduplicate by pair address
        seen = set()
        unique_pairs = []
        for p in all_pairs:
            addr = p.get("pairAddress", "")
            if addr not in seen:
                seen.add(addr)
                unique_pairs.append(p)

        log.info("Processing {n} unique Solana pairs", n=len(unique_pairs))

        # Compute aggregate signals
        total_volume_24h = 0.0
        total_liquidity = 0.0
        total_buys_24h = 0
        total_sells_24h = 0
        total_txns_24h = 0
        price_changes_24h: list[float] = []

        for p in unique_pairs:
            vol = p.get("volume", {})
            liq = p.get("liquidity", {})
            txns = p.get("txns", {})
            pchg = p.get("priceChange", {})

            total_volume_24h += vol.get("h24", 0) or 0
            total_liquidity += liq.get("usd", 0) or 0

            h24_txns = txns.get("h24", {})
            buys = h24_txns.get("buys", 0) or 0
            sells = h24_txns.get("sells", 0) or 0
            total_buys_24h += buys
            total_sells_24h += sells
            total_txns_24h += buys + sells

            chg24 = pchg.get("h24")
            if chg24 is not None:
                price_changes_24h.append(float(chg24))

        buy_sell_ratio = (
            total_buys_24h / total_sells_24h
            if total_sells_24h > 0
            else 0.0
        )

        avg_price_change = (
            sum(price_changes_24h) / len(price_changes_24h)
            if price_changes_24h
            else 0.0
        )

        signals = {
            "dex_sol_volume_24h": total_volume_24h,
            "dex_sol_liquidity": total_liquidity,
            "dex_sol_buy_sell_ratio": round(buy_sell_ratio, 4),
            "dex_sol_momentum_24h": round(avg_price_change, 4),
            "dex_sol_txn_count_24h": float(total_txns_24h),
            "dex_sol_boosted_tokens": float(n_boosted),
        }

        # Store in raw_series
        inserted = 0
        with self.engine.begin() as conn:
            for series_name, value in signals.items():
                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, pull_status) "
                        "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                        "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) "
                        "DO NOTHING"
                    ),
                    {
                        "sid": f"DEXSCR:{series_name}",
                        "src": self.source_id,
                        "od": today,
                        "val": value,
                    },
                )
                inserted += 1

        log.info(
            "DexScreener signals stored: vol=${v:,.0f}, liq=${l:,.0f}, "
            "buy/sell={bs:.2f}, momentum={m:.1f}%, txns={t:,}",
            v=total_volume_24h,
            l=total_liquidity,
            bs=buy_sell_ratio,
            m=avg_price_change,
            t=total_txns_24h,
        )

        for name, val in signals.items():
            log.debug("  {name}: {val}", name=name, val=val)

        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "signals": signals,
            "pairs_analyzed": len(unique_pairs),
        }


if __name__ == "__main__":
    from db import get_engine

    puller = DexScreenerPuller(db_engine=get_engine())
    result = puller.pull_aggregate_signals()
    print(f"\nStatus: {result['status']} | Rows: {result['rows_inserted']}")
