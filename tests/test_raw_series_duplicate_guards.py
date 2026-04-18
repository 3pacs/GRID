"""Regression tests for raw_series duplicate guards in selected ingestion pullers."""

from __future__ import annotations

import hashlib
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd

from ingestion.altdata import cftc_cot, nyfed_gscpi, pmxt_archive, taiwan_exports
from ingestion.altdata.cftc_cot import CFTCCOTPuller
from ingestion.altdata.nyfed_gscpi import NYFedGSCPIPuller
from ingestion.altdata.pmxt_archive import PmxtArchivePuller
from ingestion.altdata.polymarket import PolymarketPuller
from ingestion.altdata.stocktwits import StockTwitsPuller
from ingestion.altdata.taiwan_exports import TaiwanExportsPuller, TaiwanExportSnapshot
from ingestion.dexscreener import DexScreenerPuller
from ingestion.pumpfun import PumpFunPuller


def _make_engine(
    existing_rows: set[tuple[str, int, date]] | None = None,
    source_id: int = 7,
    trade_rows: list[tuple] | None = None,
):
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
        elif "FROM prediction_market_trades" in sql:
            result.fetchall.return_value = trade_rows or []
        elif "SELECT 1 FROM raw_series" in sql:
            key = (params["sid"], params["src"], params["od"])
            result.fetchone.return_value = (1,) if key in existing_rows else None
        elif "SELECT DISTINCT obs_date FROM raw_series" in sql:
            sid = params["sid"]
            src = params["src"]
            dates = sorted(
                {od for row_sid, row_src, od in existing_rows if row_sid == sid and row_src == src}
            )
            result.fetchall.return_value = [(od,) for od in dates]
        elif "SELECT MAX(obs_date) FROM raw_series" in sql:
            sid = params["sid"]
            src = params["src"]
            dates = [od for row_sid, row_src, od in existing_rows if row_sid == sid and row_src == src]
            result.fetchone.return_value = (max(dates),) if dates else (None,)
        elif "INSERT INTO raw_series" in sql:
            inserted_rows.append((sql, dict(params or {})))
            result.fetchone.return_value = None
            result.rowcount = 1
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


def test_cftc_cot_skips_duplicate_success_rows():
    engine, conn, inserted_rows = _make_engine()
    puller = CFTCCOTPuller(engine)

    record = {
        "report_date_as_yyyy_mm_dd": "2026-04-07",
        "market_and_exchange_names": "S&P 500 - CHICAGO MERCANTILE EXCHANGE",
        "comm_positions_long_all": "10",
        "comm_positions_short_all": "4",
        "noncomm_positions_long_all": "7",
        "noncomm_positions_short_all": "3",
        "open_interest_all": "100",
    }

    with patch.object(cftc_cot, "_RATE_LIMIT_DELAY", 0.0), patch.object(
        puller, "_fetch_cot_data", return_value=[record, record]
    ):
        result = puller.pull_contract("SP500", start_date="2026-01-01")

    assert result["rows_inserted"] == 6
    assert len(inserted_rows) == 6
    assert all("ON CONFLICT" not in sql for sql, _ in inserted_rows)


def test_nyfed_gscpi_skips_existing_success_row():
    today = date.today()
    engine, conn, inserted_rows = _make_engine(
        existing_rows={("NYFED:gscpi", 7, today)}
    )
    puller = NYFedGSCPIPuller(engine)

    df = pd.DataFrame(
        {
            "Date": [today, today + timedelta(days=1)],
            "GSCPI": [1.0, 2.0],
        }
    )
    fake_resp = MagicMock()
    fake_resp.raise_for_status.return_value = None
    fake_resp.headers = {
        "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }
    fake_resp.content = b"excel"

    with patch.object(nyfed_gscpi.requests, "get", return_value=fake_resp), patch.object(
        nyfed_gscpi.pd, "read_excel", return_value=df
    ):
        result = puller.pull_all()

    summary = result[0]
    assert summary["rows_inserted"] == 1
    assert len(inserted_rows) == 1
    assert all("ON CONFLICT" not in sql for sql, _ in inserted_rows)


def test_pmxt_archive_skips_existing_success_row():
    today = date.today()
    first_question = "Will the Fed cut rates in 2026?"
    first_series_id = f"PMXT:{hashlib.md5(first_question.encode()).hexdigest()[:12]}"
    engine, conn, inserted_rows = _make_engine(
        existing_rows={(first_series_id, 7, today)}
    )
    with patch.object(pmxt_archive.Path, "mkdir", return_value=None):
        puller = PmxtArchivePuller(engine)

    df = pd.DataFrame(
        {
            "question": [first_question, "Will Bitcoin hit 100k?"],
            "price": [0.55, 0.61],
            "date": [today, today],
        }
    )

    inserted = puller._load_parquet(df)

    assert inserted == 1
    assert len(inserted_rows) == 1
    assert all("ON CONFLICT" not in sql for sql, _ in inserted_rows)


def test_taiwan_exports_skips_existing_success_row():
    today = date.today()
    engine, conn, inserted_rows = _make_engine(
        existing_rows={("taiwan:export_orders_usd_bn", 7, today)}
    )
    puller = TaiwanExportsPuller(engine, fred_api_key="FAKE")

    snapshots = [
        TaiwanExportSnapshot(
            month_end=today,
            orders_usd_bn=62.4,
            semiconductor_orders_usd_bn=None,
            yoy_pct=None,
        ),
        TaiwanExportSnapshot(
            month_end=today + timedelta(days=31),
            orders_usd_bn=64.0,
            semiconductor_orders_usd_bn=None,
            yoy_pct=None,
        ),
    ]

    with patch.object(taiwan_exports, "HISTORICAL_FOUNDRY_UTILIZATION", {}):
        inserted = puller.save_to_db(snapshots)

    assert inserted == 1
    assert len(inserted_rows) == 1
    assert all("ON CONFLICT" not in sql for sql, _ in inserted_rows)
