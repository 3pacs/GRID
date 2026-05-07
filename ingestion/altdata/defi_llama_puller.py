"""
GRID DeFi Llama data ingestion module.

Pulls decentralised finance metrics from the DeFi Llama API (no key required):
1. Protocol TVL — top 500 protocols by total value locked
2. Chain TVL — historical TVL for major L1/L2 chains
3. Stablecoin market caps — USDT, USDC, DAI, etc.
4. Bridge volumes — cross-chain capital flow data
5. TVL anomaly detection — flags >20% 24h drops (exploit/rug signal)

Data source: https://defillama.com/docs/api

Series stored:
- defillama.protocol_tvl.<slug>       — per-protocol TVL (top 500)
- defillama.chain_tvl.<chain>         — per-chain historical TVL
- defillama.stablecoin_mcap.<symbol>  — stablecoin circulating supply
- defillama.bridge_volume.<name>      — bridge 24h volume
- defillama.tvl_anomaly               — protocols with >20% 24h TVL drop
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure
from intelligence.actor_ingest import ingest_actors_batch

# ---- API URLs ----
_BASE_URL: str = "https://api.llama.fi"
_PROTOCOLS_URL: str = f"{_BASE_URL}/v2/protocols"
_CHAINS_URL: str = f"{_BASE_URL}/v2/chains"
_HISTORICAL_CHAIN_TVL_URL: str = f"{_BASE_URL}/v2/historicalChainTvl"
_STABLECOINS_URL: str = f"{_BASE_URL}/stablecoins"
_BRIDGES_URL: str = f"{_BASE_URL}/bridges"
_YIELDS_URL: str = f"{_BASE_URL}/yields/pools"

# Series ID prefix
_SERIES_PREFIX: str = "defillama"

# Major chains to track historical TVL
MAJOR_CHAINS: list[str] = [
    "Ethereum", "Solana", "Arbitrum", "Base",
    "BSC", "Polygon", "Avalanche", "Optimism",
]

# TVL anomaly threshold (20% drop in 24h)
_TVL_DROP_THRESHOLD: float = 0.20

# Top N protocols by TVL to store
_TOP_PROTOCOLS: int = 500

# HTTP config
_REQUEST_TIMEOUT: int = 45
_RATE_LIMIT_DELAY: float = 0.5

# Feature definitions
DEFILLAMA_FEATURES: dict[str, str] = {
    "protocol_tvl": "DeFi protocol total value locked (USD)",
    "chain_tvl": "Blockchain chain total value locked (USD)",
    "stablecoin_mcap": "Stablecoin circulating market cap (USD)",
    "bridge_volume": "Bridge 24h volume (USD)",
    "tvl_anomaly": "Protocols flagged for >20% TVL drop in 24h",
}


class DefiLlamaPuller(BasePuller):
    """Pulls DeFi metrics from DeFi Llama into ``raw_series``.

    DeFi Llama is the largest TVL aggregator across 200+ chains. This
    puller captures protocol-level TVL, chain TVL, stablecoin flows,
    and bridge volumes to track on-chain capital allocation.

    TVL anomaly detection flags protocols with sudden drops that may
    indicate exploits, rug pulls, or mass withdrawals.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for DeFi_Llama.
    """

    SOURCE_NAME: str = "DeFi_Llama"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.llama.fi",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the DeFi Llama puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "DefiLlamaPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    def _series_id(self, category: str, name: str) -> str:
        """Build the full series_id for a feature.

        Parameters:
            category: Feature category (e.g., 'protocol_tvl').
            name: Specific entity name (e.g., 'aave').

        Returns:
            Full series_id (e.g., 'defillama.protocol_tvl.aave').
        """
        # Sanitise name for series_id: lowercase, replace spaces/special chars
        safe_name = (
            name.lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
            .replace("/", "_")
        )
        return f"{_SERIES_PREFIX}.{category}.{safe_name}"

    # ------------------------------------------------------------------ #
    # HTTP helpers
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_json(self, url: str) -> Any:
        """Fetch a JSON endpoint from DeFi Llama.

        Parameters:
            url: Full URL to fetch.

        Returns:
            Parsed JSON response.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }
        resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    # 1. Protocol TVL (top 500)
    # ------------------------------------------------------------------ #

    def pull_protocols(self) -> dict[str, Any]:
        """Pull all protocols and store top 500 by TVL.

        Fetches the full protocol list, sorts by TVL descending,
        stores the top 500, and ingests protocol names as actors.

        Returns:
            dict with status, rows_inserted, protocols_count, anomalies.
        """
        try:
            protocols = self._fetch_json(_PROTOCOLS_URL)
        except Exception as exc:
            log.error("DeFi Llama protocols pull failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if not protocols or not isinstance(protocols, list):
            log.warning("DeFi Llama: no protocol data returned")
            return {"status": "SUCCESS", "rows_inserted": 0}

        # Sort by TVL descending, take top N
        valid_protocols = [
            p for p in protocols
            if isinstance(p.get("tvl"), (int, float)) and p["tvl"] > 0
        ]
        valid_protocols.sort(key=lambda p: p["tvl"], reverse=True)
        top_protocols = valid_protocols[:_TOP_PROTOCOLS]

        today = date.today()
        inserted = 0
        anomalies: list[dict[str, Any]] = []

        # Collect actors for batch ingestion
        actor_batch: list[tuple[str, str]] = []

        with self.engine.begin() as conn:
            for proto in top_protocols:
                slug = proto.get("slug", proto.get("name", "unknown"))
                name = proto.get("name", slug)
                tvl = float(proto["tvl"])

                sid = self._series_id("protocol_tvl", slug)

                # Dedup check
                if self._row_exists(sid, today, conn):
                    continue

                # Detect TVL anomaly: >20% drop in 24h
                change_1d = proto.get("change_1d")
                is_anomaly = False
                if change_1d is not None:
                    try:
                        pct_change = float(change_1d) / 100.0
                        if pct_change < -_TVL_DROP_THRESHOLD:
                            is_anomaly = True
                            anomalies.append({
                                "protocol": name,
                                "slug": slug,
                                "tvl": tvl,
                                "change_1d_pct": float(change_1d),
                                "category": proto.get("category"),
                                "chains": proto.get("chains", []),
                            })
                    except (TypeError, ValueError):
                        pass

                payload = {
                    "name": name,
                    "slug": slug,
                    "category": proto.get("category"),
                    "chains": proto.get("chains", []),
                    "change_1h": proto.get("change_1h"),
                    "change_1d": proto.get("change_1d"),
                    "change_7d": proto.get("change_7d"),
                    "mcap": proto.get("mcap"),
                    "fdv": proto.get("fdv"),
                    "is_anomaly": is_anomaly,
                    "source_url": _PROTOCOLS_URL,
                }

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=today,
                    value=tvl,
                    raw_payload=payload,
                )
                inserted += 1

                # Queue actor for ingestion
                actor_batch.append((name, "company"))

        # Store anomaly summary as its own series
        if anomalies:
            self._store_anomalies(anomalies, today)

        # Ingest protocol names as actors
        if actor_batch:
            added = ingest_actors_batch(
                self.engine, actor_batch, source="defillama"
            )
            log.info(
                "DeFi Llama: ingested {n} new actors from protocols",
                n=added,
            )

        log.info(
            "DeFi Llama protocols: {n} rows inserted, {a} anomalies detected",
            n=inserted,
            a=len(anomalies),
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "protocols_count": len(top_protocols),
            "anomalies": anomalies,
        }

    def _store_anomalies(
        self, anomalies: list[dict[str, Any]], obs_date: date
    ) -> None:
        """Store TVL anomaly flags as a summary series row.

        Parameters:
            anomalies: List of anomaly detail dicts.
            obs_date: Observation date.
        """
        sid = f"{_SERIES_PREFIX}.tvl_anomaly"
        with self.engine.begin() as conn:
            if self._row_exists(sid, obs_date, conn):
                return
            self._insert_raw(
                conn=conn,
                series_id=sid,
                obs_date=obs_date,
                value=float(len(anomalies)),
                raw_payload={
                    "anomaly_count": len(anomalies),
                    "protocols": anomalies,
                    "threshold_pct": _TVL_DROP_THRESHOLD * 100,
                },
            )

    # ------------------------------------------------------------------ #
    # 2. Chain TVL (historical for major chains)
    # ------------------------------------------------------------------ #

    def pull_chain_tvl(
        self,
        chains: list[str] | None = None,
        days_back: int = 90,
    ) -> dict[str, Any]:
        """Pull historical TVL for major chains.

        Parameters:
            chains: List of chain names. Defaults to MAJOR_CHAINS.
            days_back: Number of historical days to store.

        Returns:
            dict with status, rows_inserted, per_chain counts.
        """
        if chains is None:
            chains = MAJOR_CHAINS

        cutoff = date.today() - timedelta(days=days_back)
        total_inserted = 0
        per_chain: dict[str, int] = {}

        for chain in chains:
            url = f"{_HISTORICAL_CHAIN_TVL_URL}/{chain}"
            try:
                data = self._fetch_json(url)
            except Exception as exc:
                log.warning(
                    "DeFi Llama chain TVL failed for {c}: {e}",
                    c=chain,
                    e=str(exc),
                )
                per_chain[chain] = 0
                time.sleep(_RATE_LIMIT_DELAY)
                continue

            if not data or not isinstance(data, list):
                per_chain[chain] = 0
                time.sleep(_RATE_LIMIT_DELAY)
                continue

            sid = self._series_id("chain_tvl", chain)
            chain_inserted = 0

            with self.engine.begin() as conn:
                existing = self._get_existing_dates(sid, conn)

                for point in data:
                    ts = point.get("date")
                    tvl = point.get("tvl")
                    if ts is None or tvl is None:
                        continue

                    try:
                        obs_date = datetime.fromtimestamp(
                            int(ts), tz=timezone.utc
                        ).date()
                    except (ValueError, OverflowError, OSError):
                        continue

                    if obs_date < cutoff or obs_date in existing:
                        continue

                    self._insert_raw(
                        conn=conn,
                        series_id=sid,
                        obs_date=obs_date,
                        value=float(tvl),
                        raw_payload={
                            "chain": chain,
                            "timestamp_unix": ts,
                            "source_url": url,
                        },
                    )
                    chain_inserted += 1

            per_chain[chain] = chain_inserted
            total_inserted += chain_inserted
            log.info(
                "DeFi Llama chain {c}: {n} rows inserted",
                c=chain,
                n=chain_inserted,
            )
            time.sleep(_RATE_LIMIT_DELAY)

        # Ingest chain names as actors
        chain_actors = [(c, "entity") for c in chains]
        ingest_actors_batch(self.engine, chain_actors, source="defillama")

        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "per_chain": per_chain,
        }

    # ------------------------------------------------------------------ #
    # 3. Stablecoin market caps
    # ------------------------------------------------------------------ #

    def pull_stablecoins(self) -> dict[str, Any]:
        """Pull stablecoin market caps and circulating supply.

        Returns:
            dict with status, rows_inserted, stablecoins_count.
        """
        try:
            data = self._fetch_json(_STABLECOINS_URL)
        except Exception as exc:
            log.error("DeFi Llama stablecoins pull failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        pegged_assets = data.get("peggedAssets", [])
        if not pegged_assets:
            log.warning("DeFi Llama: no stablecoin data returned")
            return {"status": "SUCCESS", "rows_inserted": 0}

        today = date.today()
        inserted = 0
        actor_batch: list[tuple[str, str]] = []

        with self.engine.begin() as conn:
            for stable in pegged_assets:
                name = stable.get("name", "")
                symbol = stable.get("symbol", "")
                if not symbol:
                    continue

                # Extract circulating supply (peggedUSD is the main metric)
                circulating = stable.get("circulating", {})
                pegged_usd = circulating.get("peggedUSD")
                if pegged_usd is None or not isinstance(pegged_usd, (int, float)):
                    continue
                if pegged_usd <= 0:
                    continue

                sid = self._series_id("stablecoin_mcap", symbol)
                if self._row_exists(sid, today, conn):
                    continue

                payload = {
                    "name": name,
                    "symbol": symbol,
                    "peg_type": stable.get("pegType"),
                    "peg_mechanism": stable.get("pegMechanism"),
                    "chains": stable.get("chains", []),
                    "price": stable.get("price"),
                    "source_url": _STABLECOINS_URL,
                }

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=today,
                    value=float(pegged_usd),
                    raw_payload=payload,
                )
                inserted += 1

                if name:
                    actor_batch.append((name, "company"))

        # Ingest stablecoin issuers as actors
        if actor_batch:
            ingest_actors_batch(
                self.engine, actor_batch, source="defillama"
            )

        log.info(
            "DeFi Llama stablecoins: {n} rows inserted", n=inserted,
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "stablecoins_count": len(pegged_assets),
        }

    # ------------------------------------------------------------------ #
    # 4. Bridge volumes
    # ------------------------------------------------------------------ #

    def pull_bridges(self) -> dict[str, Any]:
        """Pull bridge volumes for cross-chain capital flow tracking.

        Returns:
            dict with status, rows_inserted, bridges_count.
        """
        try:
            data = self._fetch_json(_BRIDGES_URL)
        except Exception as exc:
            log.error("DeFi Llama bridges pull failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        bridges = data.get("bridges", [])
        if not bridges:
            log.warning("DeFi Llama: no bridge data returned")
            return {"status": "SUCCESS", "rows_inserted": 0}

        today = date.today()
        inserted = 0
        actor_batch: list[tuple[str, str]] = []

        with self.engine.begin() as conn:
            for bridge in bridges:
                name = bridge.get("displayName") or bridge.get("name", "")
                if not name:
                    continue

                # Use lastDayVolume or currentDayVolume
                volume = bridge.get("lastDayVolume")
                if volume is None:
                    volume = bridge.get("currentDayVolume")
                if volume is None or not isinstance(volume, (int, float)):
                    continue
                if volume <= 0:
                    continue

                sid = self._series_id("bridge_volume", name)
                if self._row_exists(sid, today, conn):
                    continue

                payload = {
                    "name": name,
                    "chains": bridge.get("chains", []),
                    "destination_chain": bridge.get("destinationChain"),
                    "last_day_volume": bridge.get("lastDayVolume"),
                    "current_day_volume": bridge.get("currentDayVolume"),
                    "last_day_net_flow": bridge.get("dayBeforeLastVolume"),
                    "source_url": _BRIDGES_URL,
                }

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=today,
                    value=float(volume),
                    raw_payload=payload,
                )
                inserted += 1

                actor_batch.append((name, "company"))

        # Ingest bridge names as actors
        if actor_batch:
            ingest_actors_batch(
                self.engine, actor_batch, source="defillama"
            )

        log.info(
            "DeFi Llama bridges: {n} rows inserted", n=inserted,
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "bridges_count": len(bridges),
        }

    # ------------------------------------------------------------------ #
    # Combined pull
    # ------------------------------------------------------------------ #

    def pull_all(
        self,
        chains: list[str] | None = None,
        days_back: int = 90,
    ) -> list[dict[str, Any]]:
        """Pull all DeFi Llama data sources.

        Parameters:
            chains: List of chain names for TVL history. Defaults to MAJOR_CHAINS.
            days_back: Days of chain TVL history to fetch.

        Returns:
            List of result dicts (one per data source).
        """
        results: list[dict[str, Any]] = []

        log.info("DeFi Llama pull_all starting")

        # 1. Protocols (includes anomaly detection)
        proto_result = self.pull_protocols()
        results.append({"source": "protocols", **proto_result})
        time.sleep(_RATE_LIMIT_DELAY)

        # 2. Chain TVL history
        chain_result = self.pull_chain_tvl(chains=chains, days_back=days_back)
        results.append({"source": "chain_tvl", **chain_result})
        time.sleep(_RATE_LIMIT_DELAY)

        # 3. Stablecoins
        stable_result = self.pull_stablecoins()
        results.append({"source": "stablecoins", **stable_result})
        time.sleep(_RATE_LIMIT_DELAY)

        # 4. Bridges
        bridge_result = self.pull_bridges()
        results.append({"source": "bridges", **bridge_result})

        succeeded = sum(1 for r in results if r.get("status") == "SUCCESS")
        total_rows = sum(r.get("rows_inserted", 0) for r in results)
        log.info(
            "DeFi Llama pull_all complete -- {ok}/{total} sources, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results


if __name__ == "__main__":
    from config import settings  # noqa: F401
    from db import get_engine

    puller = DefiLlamaPuller(db_engine=get_engine())
    results = puller.pull_all()
    for r in results:
        print(
            f"  {r.get('source', '?')}: {r.get('status', '?')} "
            f"({r.get('rows_inserted', 0)} rows)"
        )
