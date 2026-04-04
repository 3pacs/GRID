"""
GRID Pump.fun data ingestion module.

Pulls memecoin launch/graduation metrics from Pump.fun's frontend API
and stores aggregate signals in ``raw_series``.

Pump.fun does NOT have an official public API — these are reverse-engineered
frontend endpoints (documented at github.com/BankkRoll/pumpfun-apis).
Endpoints may break without notice.

Tracked signals:
- New token launch rate (speculative mania gauge)
- King-of-the-hill market cap (peak memecoin sentiment)
- Graduation rate (tokens hitting bonding curve completion)
- Currently live token count
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Pump.fun frontend API (v3, reverse-engineered — no official docs)
BASE_URL = "https://frontend-api-v3.pump.fun"

# Also use the older v2 endpoint as fallback
FALLBACK_URL = "https://frontend-api-v2.pump.fun"

_REQUEST_DELAY = 0.5  # Be polite — no published rate limits


class PumpFunPuller:
    """Pulls aggregate memecoin launch metrics from Pump.fun.

    Produces daily aggregate features (not individual token prices) that
    serve as crypto-speculative-mania indicators for GRID.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The source_catalog.id for Pump.fun.
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("PumpFunPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        """Look up or create source_catalog entry for Pump.fun."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "PumpFun"},
            ).fetchone()

        if row is not None:
            return row[0]

        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO source_catalog "
                    "(name, base_url, cost_tier, latency_class, pit_available, "
                    "revision_behavior, trust_score, priority_rank, active) "
                    "VALUES (:name, :url, 'FREE', 'REALTIME', FALSE, 'NEVER', 'LOW', 21, TRUE) "
                    "RETURNING id"
                ),
                {"name": "PumpFun", "url": BASE_URL},
            ).fetchone()

        log.info("Registered PumpFun in source_catalog — id={id}", id=row[0])
        return row[0]

    def _get(self, path: str, base: str | None = None) -> Any | None:
        """Make a GET request to the Pump.fun API."""
        url = f"{base or BASE_URL}{path}"
        headers = {
            "User-Agent": "GRID/4.0 (market-research)",
            "Accept": "application/json",
        }
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            time.sleep(_REQUEST_DELAY)
            return resp.json()
        except Exception as exc:
            log.warning("PumpFun request failed: {p} — {e}", p=path, e=str(exc))
            # Try fallback
            if base is None:
                return self._get(path, base=FALLBACK_URL)
            return None

    def pull_aggregate_signals(self) -> dict[str, Any]:
        """Pull aggregate Pump.fun metrics and store in raw_series.

        Computes:
        - pump_new_tokens_count: Number of recently launched tokens
        - pump_koth_mcap: King-of-the-hill market cap (USD)
        - pump_live_token_count: Currently active token count
        - pump_graduated_count: Tokens that completed bonding curve

        Returns:
            dict: Summary with signal values and insert counts.
        """
        log.info("Pulling Pump.fun aggregate signals")
        today = date.today()
        signals: dict[str, float] = {}

        # 1. Latest coins (launch rate proxy)
        latest = self._get("/coins/latest?limit=50&offset=0&includeNsfw=false")
        if isinstance(latest, list):
            signals["pump_new_tokens_count"] = float(len(latest))
            # Extract aggregate market cap if available
            mcaps = [c.get("usd_market_cap", 0) or 0 for c in latest if isinstance(c, dict)]
            signals["pump_latest_avg_mcap"] = (
                sum(mcaps) / len(mcaps) if mcaps else 0.0
            )
        elif isinstance(latest, dict) and "coins" in latest:
            coins = latest["coins"]
            signals["pump_new_tokens_count"] = float(len(coins))
            mcaps = [c.get("usd_market_cap", 0) or 0 for c in coins if isinstance(c, dict)]
            signals["pump_latest_avg_mcap"] = (
                sum(mcaps) / len(mcaps) if mcaps else 0.0
            )
        else:
            log.warning("Could not parse latest coins response")
            signals["pump_new_tokens_count"] = 0.0
            signals["pump_latest_avg_mcap"] = 0.0

        # 2. King of the hill (peak speculative sentiment)
        koth = self._get("/coins/king-of-the-hill?includeNsfw=false")
        if isinstance(koth, dict):
            signals["pump_koth_mcap"] = float(koth.get("usd_market_cap", 0) or 0)
            signals["pump_koth_reply_count"] = float(koth.get("reply_count", 0) or 0)
        elif isinstance(koth, list) and koth:
            top = koth[0]
            signals["pump_koth_mcap"] = float(top.get("usd_market_cap", 0) or 0)
            signals["pump_koth_reply_count"] = float(top.get("reply_count", 0) or 0)
        else:
            signals["pump_koth_mcap"] = 0.0
            signals["pump_koth_reply_count"] = 0.0

        # 3. Currently live tokens
        live = self._get("/coins/currently-live?offset=0&limit=1&includeNsfw=false")
        if isinstance(live, list):
            # The API returns paginated results; count from response
            signals["pump_live_token_count"] = float(len(live))
        elif isinstance(live, dict):
            signals["pump_live_token_count"] = float(live.get("total", live.get("count", 0)))
        else:
            signals["pump_live_token_count"] = 0.0

        # 4. Graduated tokens (completed bonding curve)
        graduated = self._get("/coins?limit=50&offset=0&complete=true&includeNsfw=false&order=DESC&sort=last_trade_timestamp")
        if isinstance(graduated, list):
            signals["pump_graduated_count"] = float(len(graduated))
            grad_mcaps = [c.get("usd_market_cap", 0) or 0 for c in graduated if isinstance(c, dict)]
            signals["pump_graduated_avg_mcap"] = (
                sum(grad_mcaps) / len(grad_mcaps) if grad_mcaps else 0.0
            )
        elif isinstance(graduated, dict) and "coins" in graduated:
            coins = graduated["coins"]
            signals["pump_graduated_count"] = float(len(coins))
            grad_mcaps = [c.get("usd_market_cap", 0) or 0 for c in coins if isinstance(c, dict)]
            signals["pump_graduated_avg_mcap"] = (
                sum(grad_mcaps) / len(grad_mcaps) if grad_mcaps else 0.0
            )
        else:
            signals["pump_graduated_count"] = 0.0
            signals["pump_graduated_avg_mcap"] = 0.0

        # 5. Compute graduation rate (graduated / launched)
        new_count = signals.get("pump_new_tokens_count", 0)
        grad_count = signals.get("pump_graduated_count", 0)
        signals["pump_grad_rate"] = (
            grad_count / new_count if new_count > 0 else 0.0
        )

        if not any(v > 0 for v in signals.values()):
            log.warning("All Pump.fun signals are zero — API may be blocked or changed")
            return {"status": "FAILED", "error": "No data", "rows_inserted": 0, "signals": signals}

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
                        "sid": f"PUMP:{series_name}",
                        "src": self.source_id,
                        "od": today,
                        "val": value,
                    },
                )
                inserted += 1

        log.info(
            "Pump.fun signals stored: new_tokens={nt}, koth_mcap=${km:,.0f}, "
            "graduated={gc}",
            nt=signals.get("pump_new_tokens_count", 0),
            km=signals.get("pump_koth_mcap", 0),
            gc=signals.get("pump_graduated_count", 0),
        )

        for name, val in signals.items():
            log.debug("  {name}: {val}", name=name, val=val)

        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "signals": signals,
        }


if __name__ == "__main__":
    from db import get_engine

    puller = PumpFunPuller(db_engine=get_engine())
    result = puller.pull_aggregate_signals()
    print(f"\nStatus: {result['status']} | Rows: {result['rows_inserted']}")
