"""
Etherscan puller — Ethereum on-chain intelligence.

Tracks whale wallets, large transfers, token flows, gas prices,
and smart contract activity. Fills the critical crypto on-chain gap.

API: https://docs.etherscan.io/
Free tier: 5 calls/sec, 100K calls/day.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

ETHERSCAN_API = "https://api.etherscan.io/v2/api"

# Whale wallets to track (known large holders / exchanges / DeFi)
WHALE_WALLETS: dict[str, str] = {
    # Exchanges
    "Binance Hot": "0x28C6c06298d514Db089934071355E5743bf21d60",
    "Binance Cold": "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8",
    "Coinbase Prime": "0xA9D1e08C7793af67e9d92fe308d5697FB81d3E43",
    "Kraken": "0x2910543Af39abA0Cd09dBb2D50200b3E800A63D2",
    "Bitfinex": "0x876EabF441B2EE5B5b0554Fd502a8E0600950cFa",
    # DeFi
    "Aave V3": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
    "Lido": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
    "Uniswap V3 Router": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
    "Maker DSR": "0x197E90f9FAD81970bA7976f33CbD77088E5D7cf7",
    # Whales
    "Justin Sun": "0x3DdfA8eC3052539b6C9549F12cEA2C295cfF5296",
    "Ethereum Foundation": "0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe",
    "Vitalik": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
    # Stablecoins
    "Tether Treasury": "0x5754284f345afc66a98fbB0a0Afe71e0F007B949",
    "Circle (USDC)": "0x55FE002aefF02F77364de339a1292923A15844B8",
}

# Key tokens to track supply/transfers
TOKENS: dict[str, str] = {
    "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
    "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    "LINK": "0x514910771AF9Ca656af840dff83E8264EcF986CA",
    "UNI": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
    "stETH": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
}


class EtherscanPuller(BasePuller):
    """Pull Ethereum on-chain data from Etherscan."""

    SOURCE_NAME = "etherscan"
    SOURCE_CONFIG = {
        "base_url": ETHERSCAN_API,
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 18,
    }

    def __init__(self, db_engine: Engine, api_key: str | None = None) -> None:
        super().__init__(db_engine)
        if api_key:
            self.api_key = api_key
        else:
            from config import settings
            self.api_key = getattr(settings, "ETHERSCAN_API_KEY", "")
        if not self.api_key:
            log.warning("ETHERSCAN_API_KEY not set — Etherscan puller disabled")

    @retry_on_failure(max_attempts=3, retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.exceptions.RequestException))
    def _api_get(self, params: dict[str, Any]) -> dict[str, Any]:
        """Make an Etherscan API call.

        Args:
            params: Query parameters (module, action, etc.)

        Returns:
            Response JSON.
        """
        params["apikey"] = self.api_key
        params.setdefault("chainid", 1)  # Ethereum mainnet
        resp = requests.get(ETHERSCAN_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "0" and data.get("message") != "No transactions found":
            log.debug("Etherscan API error: {m}", m=data.get("result", ""))
        return data

    # ------------------------------------------------------------------
    # ETH price and gas
    # ------------------------------------------------------------------

    def pull_eth_price(self) -> dict[str, float] | None:
        """Get current ETH price in USD and BTC."""
        data = self._api_get({"module": "stats", "action": "ethprice"})
        result = data.get("result", {})
        if not result:
            return None
        return {
            "eth_usd": float(result.get("ethusd", 0)),
            "eth_btc": float(result.get("ethbtc", 0)),
            "timestamp": int(result.get("ethusd_timestamp", 0)),
        }

    def pull_gas_oracle(self) -> dict[str, Any] | None:
        """Get current gas prices (low, average, high, base fee)."""
        data = self._api_get({"module": "gastracker", "action": "gasoracle"})
        result = data.get("result", {})
        if not result:
            return None
        return {
            "low": float(result.get("SafeGasPrice", 0)),
            "average": float(result.get("ProposeGasPrice", 0)),
            "high": float(result.get("FastGasPrice", 0)),
            "base_fee": float(result.get("suggestBaseFee", 0)),
        }

    # ------------------------------------------------------------------
    # Whale tracking
    # ------------------------------------------------------------------

    def pull_wallet_balance(self, address: str) -> float | None:
        """Get ETH balance of a wallet in Ether."""
        data = self._api_get({
            "module": "account",
            "action": "balance",
            "address": address,
            "tag": "latest",
        })
        result = data.get("result")
        if result and result != "0":
            return float(result) / 1e18  # Wei to ETH
        return 0.0

    def pull_wallet_token_balance(self, wallet: str, token_contract: str) -> float | None:
        """Get ERC-20 token balance for a wallet."""
        data = self._api_get({
            "module": "account",
            "action": "tokenbalance",
            "contractaddress": token_contract,
            "address": wallet,
            "tag": "latest",
        })
        result = data.get("result")
        if result:
            # Most tokens use 6 or 18 decimals
            return float(result)
        return None

    def pull_recent_transactions(self, address: str, limit: int = 20) -> list[dict[str, Any]]:
        """Get recent transactions for a wallet."""
        data = self._api_get({
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": limit,
            "sort": "desc",
        })
        result = data.get("result", [])
        if not isinstance(result, list):
            return []
        return result

    # ------------------------------------------------------------------
    # Token supply
    # ------------------------------------------------------------------

    def pull_token_supply(self, contract: str) -> float | None:
        """Get total supply of an ERC-20 token."""
        data = self._api_get({
            "module": "stats",
            "action": "tokensupply",
            "contractaddress": contract,
        })
        result = data.get("result")
        if result:
            return float(result)
        return None

    # ------------------------------------------------------------------
    # ETH supply and burn
    # ------------------------------------------------------------------

    def pull_eth_supply(self) -> dict[str, float] | None:
        """Get ETH supply stats (total, staked, burned)."""
        data = self._api_get({"module": "stats", "action": "ethsupply2"})
        result = data.get("result", {})
        if not result:
            return None
        return {
            "total_supply": float(result.get("EthSupply", 0)) / 1e18,
            "staked": float(result.get("Eth2Staking", 0)) / 1e18,
            "burned": float(result.get("BurntFees", 0)) / 1e18,
            "withdrawn": float(result.get("WithdrawnTotal", 0)) / 1e18,
        }

    # ------------------------------------------------------------------
    # Main pull
    # ------------------------------------------------------------------

    def pull(self) -> dict[str, Any]:
        """Pull all on-chain data: prices, gas, whale balances, token supplies.

        Returns:
            Summary with counts and anomalies.
        """
        if not self.api_key:
            return {"error": "ETHERSCAN_API_KEY not configured"}

        today = date.today()
        api_calls = 0
        anomalies: list[dict[str, Any]] = []

        # 1. ETH price
        try:
            price = self.pull_eth_price()
            api_calls += 1
            time.sleep(0.25)
            if price:
                with self.engine.begin() as conn:
                    self._insert_raw(conn, "eth:price_usd", today, price["eth_usd"],
                                     raw_payload=price)
                    self._insert_raw(conn, "eth:price_btc", today, price["eth_btc"])
        except Exception as exc:
            log.debug("ETH price failed: {e}", e=str(exc))

        # 2. Gas prices
        try:
            gas = self.pull_gas_oracle()
            api_calls += 1
            time.sleep(0.25)
            if gas:
                with self.engine.begin() as conn:
                    self._insert_raw(conn, "eth:gas_low", today, gas["low"], raw_payload=gas)
                    self._insert_raw(conn, "eth:gas_avg", today, gas["average"])
                    self._insert_raw(conn, "eth:gas_high", today, gas["high"])
                    self._insert_raw(conn, "eth:gas_base_fee", today, gas["base_fee"])
        except Exception as exc:
            log.debug("Gas oracle failed: {e}", e=str(exc))

        # 3. ETH supply
        try:
            supply = self.pull_eth_supply()
            api_calls += 1
            time.sleep(0.25)
            if supply:
                with self.engine.begin() as conn:
                    self._insert_raw(conn, "eth:total_supply", today, supply["total_supply"],
                                     raw_payload=supply)
                    self._insert_raw(conn, "eth:staked", today, supply["staked"])
                    self._insert_raw(conn, "eth:burned", today, supply["burned"])
        except Exception as exc:
            log.debug("ETH supply failed: {e}", e=str(exc))

        # 4. Whale wallet balances
        whale_balances: dict[str, float] = {}
        for name, address in WHALE_WALLETS.items():
            try:
                balance = self.pull_wallet_balance(address)
                api_calls += 1
                time.sleep(0.25)
                if balance is not None:
                    safe_name = name.lower().replace(" ", "_").replace("(", "").replace(")", "")
                    with self.engine.begin() as conn:
                        self._insert_raw(conn,
                            series_id=f"eth:whale:{safe_name}",
                            obs_date=today,
                            value=balance,
                            raw_payload={"wallet": name, "address": address},
                        )
                    whale_balances[name] = balance

                    # Auto-discover actors
                    try:
                        from intelligence.actor_ingest import ingest_actor
                        ingest_actor(self.engine, name,
                                    "company" if any(x in name.lower() for x in ["binance", "coinbase", "kraken", "bitfinex", "aave", "lido", "uniswap", "maker", "circle", "tether"]) else "person",
                                    source="etherscan",
                                    metadata={"eth_balance": balance, "address": address})
                    except Exception:
                        pass
            except Exception as exc:
                log.debug("Whale balance failed for {n}: {e}", n=name, e=str(exc))

        # 5. Stablecoin supplies (USDT, USDC, DAI)
        for token_name, contract in TOKENS.items():
            try:
                supply = self.pull_token_supply(contract)
                api_calls += 1
                time.sleep(0.25)
                if supply is not None:
                    with self.engine.begin() as conn:
                        self._insert_raw(conn,
                            series_id=f"eth:token_supply:{token_name.lower()}",
                            obs_date=today,
                            value=supply,
                            raw_payload={"token": token_name, "contract": contract},
                        )
            except Exception as exc:
                log.debug("Token supply failed for {t}: {e}", t=token_name, e=str(exc))

        # 6. Check for large recent transfers on exchange wallets
        for name, address in list(WHALE_WALLETS.items())[:5]:  # Top 5 wallets only (rate limit)
            try:
                txns = self.pull_recent_transactions(address, limit=10)
                api_calls += 1
                time.sleep(0.25)
                for tx in txns:
                    value_eth = float(tx.get("value", 0)) / 1e18
                    if value_eth > 1000:  # >1000 ETH = whale transfer
                        anomalies.append({
                            "type": "whale_transfer",
                            "wallet": name,
                            "value_eth": round(value_eth, 2),
                            "from": tx.get("from", ""),
                            "to": tx.get("to", ""),
                            "hash": tx.get("hash", ""),
                            "timestamp": tx.get("timeStamp", ""),
                        })
            except Exception as exc:
                log.debug("Transaction fetch failed for {n}: {e}", n=name, e=str(exc))

        log.info("Etherscan pull: {api} API calls, {w} whale balances, {a} large transfers",
                 api=api_calls, w=len(whale_balances), a=len(anomalies))

        return {
            "api_calls": api_calls,
            "whale_balances": len(whale_balances),
            "anomalies": anomalies,
            "whale_data": whale_balances,
        }
