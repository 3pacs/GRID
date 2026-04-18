"""Regression tests for raw_series duplicate guards in selected ingestion pullers."""

from __future__ import annotations

import hashlib
from datetime import date
from unittest.mock import MagicMock, patch

from ingestion.altdata.polymarket import PolymarketPuller
from ingestion.altdata.stocktwits import StockTwitsPuller
from ingestion.dexscreener import DexScreenerPuller
from ingestion.pumpfun import PumpFunPuller


def _make_engine(existing_rows: set[tuple[str, int, date]] | None = None, source_id: int = 7):
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = ctx
    engine.begin.return_value = ctx

    existing_rows = existing_rows or set()
    inserted_rows: list[tuple[str, dict[str, object]]] = []

    def execute(stmt, params=None):
        sql = str(stmt)
        result = MagicMock()
        if "SELECT id FROM source_catalog" in sql:
            result.fetchone.return_value = (source_id,)
        elif "SELECT 1 FROM raw_series" in sql:
            key = (params["sid"], params["src"], params["od"])
            result.fetchone.return_value = (1,) if key in existing_rows else None
        elif "INSERT INTO raw_series" in sql:
            inserted_rows.append((sql, dict(params or {})))
            result.fetchone.return_value = None
        else:  # pragma: no cover - keeps the fixture honest
            raise AssertionError(f"Unexpected SQL: {sql}")
        return result

    conn.execute.side_effect = execute
    return engine, conn, inserted_rows


def test_dexscreener_skips_existing_success_row():
    today = date.today()
    engine, conn, inserted_rows = _make_engine(
        existing_rows={("DEXSCR:dex_sol_volume_24h", 7, today)}
    )

    pair_a = {
        "chainId": "solana",
        "pairAddress": "pair-a",
        "volume": {"h24": 100},
        "liquidity": {"usd": 50},
        "txns": {"h24": {"buys": 4, "sells": 2}},
        "priceChange": {"h24": 5.0},
    }
    pair_b = {
        "chainId": "solana",
        "pairAddress": "pair-b",
        "volume": {"h24": 100},
        "liquidity": {"usd": 50},
        "txns": {"h24": {"buys": 4, "sells": 2}},
        "priceChange": {"h24": 5.0},
    }

    puller = DexScreenerPuller(engine)
    with patch.object(
        puller,
        "_get",
        side_effect=lambda path: (
            {"pairs": [pair_a, pair_b]}
            if path.startswith("/latest/dex/tokens/")
            else []
            if path == "/token-boosts/top/v1"
            else None
        ),
    ):
        result = puller.pull_aggregate_signals()

    assert result["rows_inserted"] == 5
    assert len(inserted_rows) == 5
    assert all("ON CONFLICT" not in sql for sql, _ in inserted_rows)


def test_pumpfun_skips_existing_success_row():
    today = date.today()
    engine, conn, inserted_rows = _make_engine(
        existing_rows={("PUMP:pump_koth_mcap", 7, today)}
    )

    responses = {
        "/coins/latest?limit=50&offset=0&includeNsfw=false": [
            {"usd_market_cap": 100},
            {"usd_market_cap": 200},
        ],
        "/coins/king-of-the-hill?includeNsfw=false": {
            "usd_market_cap": 300,
            "reply_count": 7,
        },
        "/coins/currently-live?offset=0&limit=1&includeNsfw=false": [1, 2, 3],
        "/coins?limit=50&offset=0&complete=true&includeNsfw=false&order=DESC&sort=last_trade_timestamp": [
            {"usd_market_cap": 400},
        ],
    }

    puller = PumpFunPuller(engine)
    with patch.object(puller, "_get", side_effect=lambda path, base=None: responses.get(path)):
        result = puller.pull_aggregate_signals()

    assert result["rows_inserted"] == 7
    assert len(inserted_rows) == 7
    assert all("ON CONFLICT" not in sql for sql, _ in inserted_rows)


def test_polymarket_skips_existing_success_row():
    today = date.today()
    first_slug = "fed-cut"
    first_series_id = f"POLY:{hashlib.md5(first_slug.encode()).hexdigest()[:12]}"
    engine, conn, inserted_rows = _make_engine(existing_rows={(first_series_id, 7, today)})

    markets = [
        {
            "question": "Will the Fed cut rates in 2026?",
            "outcomePrices": "[0.70, 0.30]",
            "slug": first_slug,
        },
        {
            "question": "Will Bitcoin hit 100k?",
            "outcomePrices": "[0.60, 0.40]",
            "slug": "bitcoin-100k",
        },
    ]

    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = markets

    with patch("ingestion.altdata.polymarket.requests.get", return_value=resp):
        puller = PolymarketPuller(engine)
        result = puller.pull_all()

    summary = result[0]
    assert summary["rows_inserted"] == 2
    assert len(inserted_rows) == 2
    assert all("ON CONFLICT" not in sql for sql, _ in inserted_rows)


def test_stocktwits_skips_existing_success_row():
    today = date.today()
    engine, conn, inserted_rows = _make_engine(
        existing_rows={("ST:AAPL:sentiment", 7, today)}
    )

    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status.return_value = None
    resp.json.return_value = {
        "messages": [
            {"entities": {"sentiment": {"basic": "Bullish"}}},
            {"entities": {"sentiment": {"basic": "Bearish"}}},
        ]
    }

    with patch("ingestion.altdata.stocktwits.requests.get", return_value=resp):
        puller = StockTwitsPuller(engine)
        result = puller._pull_ticker("AAPL")

    assert result["rows_inserted"] == 1
    assert len(inserted_rows) == 1
    assert all("ON CONFLICT" not in sql for sql, _ in inserted_rows)
