# ingestion/altdata/onchain_rpc.py
"""On-chain RPC poller — direct blockchain queries for price and activity.

Always free. No API keys. Ground truth from the chain itself.
- Solana: public RPC (mainnet-beta)
- Ethereum: free public RPCs (llamarpc, ankr)
- Bitcoin: Blockstream REST API
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

SOL_RPC = "https://api.mainnet-beta.solana.com"
ETH_RPC = "https://eth.llamarpc.com"
ETH_RPC_FALLBACK = "https://rpc.ankr.com/eth"
BTC_API = "https://blockstream.info/api"


class OnChainRPCPoller:
    """Poll blockchains directly for price and activity signals."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GRID/4.0"})

    def pull(self) -> dict[str, Any]:
        """Pull on-chain data and emit signals."""
        emitted = 0
        results = {}

        # ETH gas price (mempool pressure indicator)
        try:
            gas_result = self._check_eth_gas()
            results["eth_gas"] = gas_result
            emitted += gas_result.get("signals_emitted", 0)
        except Exception as exc:
            log.debug("ETH gas check failed: {}", exc)

        # SOL slot/block info
        try:
            sol_result = self._check_sol_activity()
            results["sol_activity"] = sol_result
            emitted += sol_result.get("signals_emitted", 0)
        except Exception as exc:
            log.debug("SOL activity check failed: {}", exc)

        # BTC mempool
        try:
            btc_result = self._check_btc_mempool()
            results["btc_mempool"] = btc_result
            emitted += btc_result.get("signals_emitted", 0)
        except Exception as exc:
            log.debug("BTC mempool check failed: {}", exc)

        log.info("OnChainRPC: {} total signals emitted", emitted)
        results["total_emitted"] = emitted
        return results

    def _check_eth_gas(self) -> dict[str, Any]:
        """Check ETH gas price for mempool pressure signals."""
        payload = {"jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id": 1}
        try:
            resp = self._session.post(ETH_RPC, json=payload, timeout=10)
            resp.raise_for_status()
        except Exception:
            resp = self._session.post(ETH_RPC_FALLBACK, json=payload, timeout=10)
            resp.raise_for_status()

        gas_hex = resp.json().get("result", "0x0")
        gas_wei = int(gas_hex, 16)
        gas_gwei = gas_wei / 1e9

        emitted = 0
        # High gas = network congestion = high activity
        if gas_gwei > 50:
            with self.engine.begin() as conn:
                try:
                    conn.execute(
                        text(
                            "INSERT INTO signal_sources "
                            "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                            "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                            "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                            "DO NOTHING"
                        ),
                        {
                            "stype": "onchain_rpc",
                            "sid": "eth_gas",
                            "ticker": "ETH",
                            "sdate": date.today(),
                            "sigtype": "onchain_mempool_pressure",
                            "sval": json.dumps({"gas_gwei": round(gas_gwei, 2), "gas_wei": gas_wei}),
                            "trust": 0.5,
                        },
                    )
                    emitted = 1
                except Exception:
                    pass

        return {"gas_gwei": round(gas_gwei, 2), "signals_emitted": emitted}

    def _check_sol_activity(self) -> dict[str, Any]:
        """Check Solana recent performance samples for TPS signals."""
        payload = {
            "jsonrpc": "2.0",
            "method": "getRecentPerformanceSamples",
            "params": [4],
            "id": 1,
        }
        resp = self._session.post(SOL_RPC, json=payload, timeout=10)
        resp.raise_for_status()
        samples = resp.json().get("result", [])

        if not samples:
            return {"signals_emitted": 0}

        avg_tps = sum(s.get("numTransactions", 0) / max(s.get("samplePeriodSecs", 60), 1)
                      for s in samples) / len(samples)

        emitted = 0
        # Unusually high TPS = network surge
        if avg_tps > 4000:
            with self.engine.begin() as conn:
                try:
                    conn.execute(
                        text(
                            "INSERT INTO signal_sources "
                            "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                            "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                            "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                            "DO NOTHING"
                        ),
                        {
                            "stype": "onchain_rpc",
                            "sid": "sol_tps",
                            "ticker": "SOL",
                            "sdate": date.today(),
                            "sigtype": "onchain_program_activity",
                            "sval": json.dumps({"avg_tps": round(avg_tps, 1)}),
                            "trust": 0.5,
                        },
                    )
                    emitted = 1
                except Exception:
                    pass

        return {"avg_tps": round(avg_tps, 1), "signals_emitted": emitted}

    def _check_btc_mempool(self) -> dict[str, Any]:
        """Check BTC mempool size via Blockstream API."""
        resp = self._session.get(f"{BTC_API}/mempool", timeout=10)
        resp.raise_for_status()
        data = resp.json()

        count = data.get("count", 0)
        vsize = data.get("vsize", 0)
        total_fee = data.get("total_fee", 0)

        emitted = 0
        # Large mempool = congestion = high demand
        if count > 100000:
            with self.engine.begin() as conn:
                try:
                    conn.execute(
                        text(
                            "INSERT INTO signal_sources "
                            "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                            "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                            "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                            "DO NOTHING"
                        ),
                        {
                            "stype": "onchain_rpc",
                            "sid": "btc_mempool",
                            "ticker": "BTC",
                            "sdate": date.today(),
                            "sigtype": "onchain_mempool_pressure",
                            "sval": json.dumps({
                                "tx_count": count,
                                "vsize_bytes": vsize,
                                "total_fee_sats": total_fee,
                            }),
                            "trust": 0.5,
                        },
                    )
                    emitted = 1
                except Exception:
                    pass

        return {"mempool_tx_count": count, "signals_emitted": emitted}
