"""
GRID Prediction Market Historical Data Sync.

Ingests the Jon Becker prediction-market-analysis dataset (Parquet files)
into GRID's raw_series and a new prediction_market_history table for
full trade-level microstructure analysis.

Dataset: https://github.com/Jon-Becker/prediction-market-analysis
- 7.68M markets, 72.1M trades across Polymarket and Kalshi
- Parquet format, ~36GB compressed

Data directory layout:
  data/prediction_markets/kalshi/markets/*.parquet
  data/prediction_markets/kalshi/trades/*.parquet
  data/prediction_markets/polymarket/markets/*.parquet
  data/prediction_markets/polymarket/trades/*.parquet
  data/prediction_markets/polymarket/blocks/*.parquet

Series stored:
- PMHIST:kalshi:{ticker}:probability  — daily snapshots of market prices
- PMHIST:kalshi:{ticker}:volume       — daily trade volume
- PMHIST:poly:{condition_id}:probability — daily snapshots
- PMHIST:poly:{condition_id}:volume      — daily trade volume
"""

from __future__ import annotations

import json
import math
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ── Paths ───────────────────────────────────────────────────────────
_DATA_ROOT = Path(os.environ.get(
    "PM_HISTORY_DATA_DIR",
    Path(__file__).resolve().parents[2] / "data" / "prediction_markets",
))

# ── Batch sizes ─────────────────────────────────────────────────────
_MARKET_BATCH = 5000
_TRADE_BATCH = 10000


class PredictionMarketHistoryPuller(BasePuller):
    """Ingests historical prediction market data from Parquet files."""

    SOURCE_NAME: str = "PM_HISTORY"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://github.com/Jon-Becker/prediction-market-analysis",
        "cost_tier": "FREE",
        "latency_class": "HISTORICAL",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 8,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._ensure_tables()

    # ── Schema ──────────────────────────────────────────────────────

    def _ensure_tables(self) -> None:
        """Create prediction market history tables if they don't exist."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS prediction_market_markets (
                    id SERIAL PRIMARY KEY,
                    platform TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    ticker TEXT,
                    title TEXT,
                    category TEXT,
                    status TEXT,
                    outcomes JSONB,
                    outcome_prices JSONB,
                    volume NUMERIC,
                    open_interest NUMERIC,
                    yes_bid NUMERIC,
                    yes_ask NUMERIC,
                    no_bid NUMERIC,
                    no_ask NUMERIC,
                    created_at TIMESTAMPTZ,
                    closed_at TIMESTAMPTZ,
                    raw_payload JSONB,
                    ingested_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(platform, market_id)
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS prediction_market_trades (
                    id BIGSERIAL PRIMARY KEY,
                    platform TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    trade_id TEXT,
                    trade_timestamp TIMESTAMPTZ NOT NULL,
                    price NUMERIC NOT NULL,
                    size NUMERIC,
                    side TEXT,
                    taker_side TEXT,
                    maker_address TEXT,
                    taker_address TEXT,
                    fee NUMERIC,
                    tx_hash TEXT,
                    block_number BIGINT,
                    raw_payload JSONB,
                    ingested_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            # Indexes for efficient querying
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_pmt_platform_market
                ON prediction_market_trades (platform, market_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_pmt_timestamp
                ON prediction_market_trades (trade_timestamp)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_pmm_platform
                ON prediction_market_markets (platform)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_pmm_status
                ON prediction_market_markets (status)
            """))
        log.info("prediction_market_markets/trades tables ready")

    # ── Main entry point ────────────────────────────────────────────

    def pull_all(self, **kwargs) -> list[dict[str, Any]]:
        """Ingest all Parquet data from the prediction market dataset."""
        results: list[dict[str, Any]] = []

        if not _DATA_ROOT.exists():
            log.warning(
                "PM history data dir not found at {p}. "
                "Run scripts/download_prediction_market_data.sh first.",
                p=_DATA_ROOT,
            )
            return [{"status": "SKIP", "reason": "data_dir_missing"}]

        # Ingest each platform
        for platform in ["kalshi", "polymarket"]:
            platform_dir = _DATA_ROOT / platform
            if not platform_dir.exists():
                log.warning("No {p} data directory found", p=platform)
                continue

            market_result = self._ingest_markets(platform, platform_dir)
            results.append(market_result)

            trade_result = self._ingest_trades(platform, platform_dir)
            results.append(trade_result)

        # Build daily aggregates into raw_series for PIT consumption
        agg_result = self._build_daily_aggregates()
        results.append(agg_result)

        return results

    # ── Market ingestion ────────────────────────────────────────────

    def _ingest_markets(
        self, platform: str, platform_dir: Path,
    ) -> dict[str, Any]:
        """Ingest market metadata from Parquet files."""
        markets_dir = platform_dir / "markets"
        if not markets_dir.exists():
            return {"platform": platform, "type": "markets", "status": "SKIP"}

        parquet_files = sorted(markets_dir.glob("*.parquet"))
        if not parquet_files:
            return {"platform": platform, "type": "markets", "status": "SKIP"}

        total_inserted = 0
        total_skipped = 0

        for pf in parquet_files:
            try:
                df = pd.read_parquet(pf)
            except Exception as exc:
                log.warning("Failed to read {f}: {e}", f=pf, e=exc)
                continue

            log.info(
                "Processing {p} markets from {f} ({n} rows)",
                p=platform, f=pf.name, n=len(df),
            )

            inserted, skipped = self._upsert_markets(platform, df)
            total_inserted += inserted
            total_skipped += skipped

        log.info(
            "{p} markets: {i} inserted, {s} skipped",
            p=platform, i=total_inserted, s=total_skipped,
        )
        return {
            "platform": platform,
            "type": "markets",
            "status": "SUCCESS",
            "inserted": total_inserted,
            "skipped": total_skipped,
        }

    def _upsert_markets(
        self, platform: str, df: pd.DataFrame,
    ) -> tuple[int, int]:
        """Upsert market rows into prediction_market_markets."""
        inserted = 0
        skipped = 0

        with self.engine.begin() as conn:
            for start in range(0, len(df), _MARKET_BATCH):
                batch = df.iloc[start:start + _MARKET_BATCH]
                for _, row in batch.iterrows():
                    record = self._normalize_market(platform, row)
                    if record is None:
                        skipped += 1
                        continue

                    result = conn.execute(
                        text("""
                            INSERT INTO prediction_market_markets
                            (platform, market_id, ticker, title, category,
                             status, outcomes, outcome_prices, volume,
                             open_interest, yes_bid, yes_ask, no_bid, no_ask,
                             created_at, closed_at, raw_payload)
                            VALUES
                            (:platform, :market_id, :ticker, :title, :category,
                             :status, :outcomes, :outcome_prices, :volume,
                             :open_interest, :yes_bid, :yes_ask, :no_bid, :no_ask,
                             :created_at, :closed_at, :raw_payload)
                            ON CONFLICT (platform, market_id) DO UPDATE SET
                                status = EXCLUDED.status,
                                outcome_prices = EXCLUDED.outcome_prices,
                                volume = EXCLUDED.volume,
                                open_interest = EXCLUDED.open_interest,
                                yes_bid = EXCLUDED.yes_bid,
                                yes_ask = EXCLUDED.yes_ask,
                                no_bid = EXCLUDED.no_bid,
                                no_ask = EXCLUDED.no_ask,
                                closed_at = EXCLUDED.closed_at,
                                raw_payload = EXCLUDED.raw_payload,
                                ingested_at = NOW()
                        """),
                        record,
                    )
                    inserted += 1

        return inserted, skipped

    def _normalize_market(
        self, platform: str, row: pd.Series,
    ) -> dict[str, Any] | None:
        """Normalize a market row to a common schema."""
        try:
            if platform == "kalshi":
                return self._normalize_kalshi_market(row)
            elif platform == "polymarket":
                return self._normalize_polymarket_market(row)
        except Exception as exc:
            log.debug("Market normalization failed: {e}", e=exc)
            return None
        return None

    def _normalize_kalshi_market(self, row: pd.Series) -> dict[str, Any]:
        """Normalize Kalshi market row."""
        market_id = str(row.get("ticker", row.get("id", "")))
        return {
            "platform": "kalshi",
            "market_id": market_id,
            "ticker": str(row.get("ticker", "")),
            "title": str(row.get("title", row.get("event_title", ""))),
            "category": str(row.get("category", "")),
            "status": str(row.get("status", "")),
            "outcomes": json.dumps(["Yes", "No"]),
            "outcome_prices": json.dumps({
                "yes": _safe_numeric(row.get("yes_bid", row.get("yes_price"))),
                "no": _safe_numeric(row.get("no_bid", row.get("no_price"))),
            }),
            "volume": _safe_numeric(row.get("volume", 0)),
            "open_interest": _safe_numeric(row.get("open_interest", 0)),
            "yes_bid": _safe_numeric(row.get("yes_bid")),
            "yes_ask": _safe_numeric(row.get("yes_ask")),
            "no_bid": _safe_numeric(row.get("no_bid")),
            "no_ask": _safe_numeric(row.get("no_ask")),
            "created_at": _safe_timestamp(row.get("created_time")),
            "closed_at": _safe_timestamp(row.get("close_time")),
            "raw_payload": json.dumps(
                {k: _json_safe(v) for k, v in row.to_dict().items()},
                default=str,
            ),
        }

    def _normalize_polymarket_market(self, row: pd.Series) -> dict[str, Any]:
        """Normalize Polymarket market row."""
        market_id = str(row.get("id", row.get("condition_id", "")))
        outcomes = row.get("outcomes", "[]")
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except (json.JSONDecodeError, TypeError):
                outcomes = []

        outcome_prices = row.get("outcome_prices", row.get("outcomePrices", "[]"))
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except (json.JSONDecodeError, TypeError):
                outcome_prices = []

        return {
            "platform": "polymarket",
            "market_id": market_id,
            "ticker": None,
            "title": str(row.get("question", row.get("title", ""))),
            "category": str(row.get("category", row.get("group_slug", ""))),
            "status": _polymarket_status(row),
            "outcomes": json.dumps(outcomes),
            "outcome_prices": json.dumps(outcome_prices),
            "volume": _safe_numeric(row.get("volume", row.get("volumeNum", 0))),
            "open_interest": _safe_numeric(row.get("liquidity", 0)),
            "yes_bid": None,
            "yes_ask": None,
            "no_bid": None,
            "no_ask": None,
            "created_at": _safe_timestamp(
                row.get("created_at", row.get("startDate")),
            ),
            "closed_at": _safe_timestamp(row.get("end_date_iso", row.get("endDate"))),
            "raw_payload": json.dumps(
                {k: _json_safe(v) for k, v in row.to_dict().items()},
                default=str,
            ),
        }

    # ── Trade ingestion ─────────────────────────────────────────────

    def _ingest_trades(
        self, platform: str, platform_dir: Path,
    ) -> dict[str, Any]:
        """Ingest trade history from Parquet files."""
        trades_dir = platform_dir / "trades"
        if not trades_dir.exists():
            return {"platform": platform, "type": "trades", "status": "SKIP"}

        parquet_files = sorted(trades_dir.glob("*.parquet"))
        if not parquet_files:
            return {"platform": platform, "type": "trades", "status": "SKIP"}

        # Check what we already have
        with self.engine.connect() as conn:
            existing = conn.execute(
                text(
                    "SELECT COUNT(*) FROM prediction_market_trades "
                    "WHERE platform = :p"
                ),
                {"p": platform},
            ).scalar() or 0

        if existing > 0:
            log.info(
                "{p} already has {n} trades — doing incremental sync",
                p=platform, n=existing,
            )

        total_inserted = 0

        for pf in parquet_files:
            try:
                df = pd.read_parquet(pf)
            except Exception as exc:
                log.warning("Failed to read {f}: {e}", f=pf, e=exc)
                continue

            log.info(
                "Processing {p} trades from {f} ({n} rows)",
                p=platform, f=pf.name, n=len(df),
            )

            count = self._insert_trades(platform, df)
            total_inserted += count

        log.info(
            "{p} trades: {i} inserted (total in DB: {t})",
            p=platform, i=total_inserted, t=existing + total_inserted,
        )
        return {
            "platform": platform,
            "type": "trades",
            "status": "SUCCESS",
            "inserted": total_inserted,
        }

    def _insert_trades(
        self, platform: str, df: pd.DataFrame,
    ) -> int:
        """Bulk insert trades."""
        inserted = 0

        with self.engine.begin() as conn:
            for start in range(0, len(df), _TRADE_BATCH):
                batch = df.iloc[start:start + _TRADE_BATCH]
                records = []

                for _, row in batch.iterrows():
                    record = self._normalize_trade(platform, row)
                    if record is not None:
                        records.append(record)

                if not records:
                    continue

                # Use executemany for bulk insert
                conn.execute(
                    text("""
                        INSERT INTO prediction_market_trades
                        (platform, market_id, trade_id, trade_timestamp,
                         price, size, side, taker_side,
                         maker_address, taker_address, fee,
                         tx_hash, block_number, raw_payload)
                        VALUES
                        (:platform, :market_id, :trade_id, :trade_timestamp,
                         :price, :size, :side, :taker_side,
                         :maker_address, :taker_address, :fee,
                         :tx_hash, :block_number, :raw_payload)
                    """),
                    records,
                )
                inserted += len(records)

        return inserted

    def _normalize_trade(
        self, platform: str, row: pd.Series,
    ) -> dict[str, Any] | None:
        """Normalize a trade row."""
        try:
            if platform == "kalshi":
                return self._normalize_kalshi_trade(row)
            elif platform == "polymarket":
                return self._normalize_polymarket_trade(row)
        except Exception as exc:
            log.debug("Trade normalization failed: {e}", e=exc)
            return None
        return None

    def _normalize_kalshi_trade(self, row: pd.Series) -> dict[str, Any]:
        """Normalize Kalshi trade."""
        # Kalshi prices are in cents (1-99), convert to 0-1
        yes_price = _safe_numeric(row.get("yes_price", 0))
        price = yes_price / 100.0 if yes_price and yes_price > 1 else yes_price

        return {
            "platform": "kalshi",
            "market_id": str(row.get("ticker", row.get("market_ticker", ""))),
            "trade_id": str(row.get("trade_id", "")),
            "trade_timestamp": _safe_timestamp(
                row.get("created_time", row.get("ts")),
            ),
            "price": price,
            "size": _safe_numeric(row.get("count", row.get("size", 1))),
            "side": str(row.get("taker_side", "")),
            "taker_side": str(row.get("taker_side", "")),
            "maker_address": None,
            "taker_address": None,
            "fee": None,
            "tx_hash": None,
            "block_number": None,
            "raw_payload": json.dumps(
                {k: _json_safe(v) for k, v in row.to_dict().items()},
                default=str,
            ),
        }

    def _normalize_polymarket_trade(self, row: pd.Series) -> dict[str, Any]:
        """Normalize Polymarket trade (OrderFilled events from Polygon)."""
        # Polymarket amounts use 6 decimals (USDC)
        maker_amount = _safe_numeric(row.get("makerAssetAmount", 0))
        taker_amount = _safe_numeric(row.get("takerAssetAmount", 0))

        # Price = taker_amount / maker_amount for buy fills
        price = None
        if maker_amount and taker_amount and maker_amount > 0:
            price = taker_amount / maker_amount

        # Try to get price from direct field if available
        if price is None or not (0 <= (price or 0) <= 1):
            price = _safe_numeric(row.get("price", 0.5))

        return {
            "platform": "polymarket",
            "market_id": str(
                row.get("market", row.get("condition_id", row.get("asset_id", ""))),
            ),
            "trade_id": str(row.get("id", row.get("trade_id", ""))),
            "trade_timestamp": _safe_timestamp(
                row.get("timestamp", row.get("block_timestamp")),
            ),
            "price": price,
            "size": _safe_numeric(
                row.get("size", row.get("amount", taker_amount)),
            ),
            "side": str(row.get("side", "")),
            "taker_side": str(row.get("side", row.get("taker_side", ""))),
            "maker_address": str(row.get("maker", row.get("maker_address", "")))[:42] or None,
            "taker_address": str(row.get("taker", row.get("taker_address", "")))[:42] or None,
            "fee": _safe_numeric(row.get("fee", row.get("takerFee"))),
            "tx_hash": str(row.get("transactionHash", row.get("tx_hash", "")))[:66] or None,
            "block_number": _safe_int(row.get("blockNumber", row.get("block_number"))),
            "raw_payload": json.dumps(
                {k: _json_safe(v) for k, v in row.to_dict().items()},
                default=str,
            ),
        }

    # ── Daily aggregates for PIT ────────────────────────────────────

    def _build_daily_aggregates(self) -> dict[str, Any]:
        """Build daily OHLCV-style aggregates from trades into raw_series.

        This allows Oracle/PIT queries to access prediction market history
        using the standard store/pit.py interface.
        """
        log.info("Building daily aggregates from prediction_market_trades...")

        inserted = 0
        with self.engine.begin() as conn:
            # Daily VWAP + volume per market
            rows = conn.execute(text("""
                SELECT
                    platform,
                    market_id,
                    DATE(trade_timestamp) AS obs_date,
                    AVG(price) AS avg_price,
                    SUM(size) AS total_volume,
                    COUNT(*) AS trade_count,
                    MIN(price) AS low_price,
                    MAX(price) AS high_price
                FROM prediction_market_trades
                WHERE price IS NOT NULL
                  AND price BETWEEN 0 AND 1
                GROUP BY platform, market_id, DATE(trade_timestamp)
                ORDER BY obs_date
            """)).fetchall()

            for row in rows:
                platform_prefix = "kalshi" if row[0] == "kalshi" else "poly"
                market_id = row[1][:64]  # Truncate long IDs
                obs = row[2]

                # Store probability (VWAP)
                prob_series = f"PMHIST:{platform_prefix}:{market_id}:probability"
                self._safe_insert_raw(
                    conn, prob_series, obs, row[3],
                    {"high": float(row[7] or 0), "low": float(row[6] or 0),
                     "trades": int(row[5])},
                )
                inserted += 1

                # Store volume
                vol_series = f"PMHIST:{platform_prefix}:{market_id}:volume"
                self._safe_insert_raw(
                    conn, vol_series, obs, row[4],
                    {"trade_count": int(row[5])},
                )
                inserted += 1

        log.info("Daily aggregates: {n} series-day records inserted", n=inserted)
        return {
            "type": "daily_aggregates",
            "status": "SUCCESS",
            "records": inserted,
        }

    def _safe_insert_raw(
        self,
        conn: Any,
        series_id: str,
        obs_date: date,
        value: float | None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Insert into raw_series with ON CONFLICT guard."""
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return

        conn.execute(
            text("""
                INSERT INTO raw_series
                (series_id, source_id, obs_date, value, raw_payload, pull_status)
                VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')
                ON CONFLICT (series_id, source_id, obs_date, pull_timestamp)
                DO NOTHING
            """),
            {
                "sid": series_id,
                "src": self.source_id,
                "od": obs_date,
                "val": float(value),
                "payload": json.dumps(payload, default=str) if payload else None,
            },
        )


# ── Utility functions ───────────────────────────────────────────────


def _safe_numeric(val: Any) -> float | None:
    """Convert value to float, returning None for bad values."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> int | None:
    """Convert value to int, returning None for bad values."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_timestamp(val: Any) -> datetime | None:
    """Convert value to datetime, handling various formats."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    try:
        s = str(val)
        # Try ISO format first
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        # Try epoch seconds
        try:
            epoch = float(s)
            if epoch > 1e12:  # milliseconds
                epoch /= 1000
            return datetime.fromtimestamp(epoch, tz=timezone.utc)
        except ValueError:
            pass
        # Try date string
        return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc,
        )
    except Exception:
        return None


def _polymarket_status(row: pd.Series) -> str:
    """Derive Polymarket market status from available fields."""
    if row.get("closed") or row.get("resolved"):
        return "closed"
    if row.get("active"):
        return "active"
    return str(row.get("status", "unknown"))


def _json_safe(val: Any) -> Any:
    """Make a value JSON-serializable."""
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.isoformat()
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    if isinstance(val, bytes):
        return val.hex()
    return val
