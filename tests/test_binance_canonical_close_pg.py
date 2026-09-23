"""Disposable PostgreSQL proof: completed Binance bar -> canonical BTC/ETH."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from binance_close_contract import (
    DAY_MS, completed_kline_payload, is_completed_canonical_close,
)
from ingestion import scheduler
from ingestion.altdata import binance_puller
from ingestion.altdata.binance_puller import BinanceAccessBlocked, BinancePuller
from normalization.resolver import Resolver


@pytest.fixture
def crypto_pg_engine() -> Engine:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("Binance proof requires a local disposable test database")

    schema = "crypto_close_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema},public"})
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE source_catalog (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE,
                    priority_rank INTEGER NOT NULL, last_pull_at TIMESTAMPTZ)
            """))
            conn.execute(text("""
                CREATE TABLE feature_registry (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, family TEXT NOT NULL)
            """))
            conn.execute(text("""
                CREATE TABLE raw_series (
                    id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL,
                    source_id INTEGER NOT NULL REFERENCES source_catalog(id),
                    obs_date DATE NOT NULL, pull_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    value DOUBLE PRECISION NOT NULL, raw_payload JSONB,
                    pull_status TEXT NOT NULL)
            """))
            conn.execute(text("""
                CREATE TABLE resolved_series (
                    id BIGSERIAL PRIMARY KEY,
                    feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
                    obs_date DATE NOT NULL, release_date DATE NOT NULL,
                    vintage_date DATE NOT NULL, value DOUBLE PRECISION NOT NULL,
                    source_priority_used INTEGER NOT NULL REFERENCES source_catalog(id),
                    conflict_flag BOOLEAN NOT NULL DEFAULT FALSE,
                    conflict_detail TEXT,
                    UNIQUE(feature_id, obs_date, vintage_date))
            """))
            conn.execute(text("""
                CREATE TABLE pull_log (
                    id BIGSERIAL PRIMARY KEY, puller_name TEXT NOT NULL,
                    source_id INTEGER, started_at TIMESTAMPTZ NOT NULL,
                    completed_at TIMESTAMPTZ, status TEXT NOT NULL,
                    node_name TEXT, rows_inserted INTEGER, rows_expected INTEGER,
                    error_message TEXT, features_affected INTEGER[])
            """))
            conn.execute(text("INSERT INTO source_catalog VALUES (184, 'binance', 25, NULL)"))
            conn.execute(text("""
                INSERT INTO feature_registry VALUES
                    (101, 'btc_full', 'crypto'), (102, 'eth_full', 'crypto')
            """))
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        admin.dispose()


def _kline(day, close: float) -> list:
    open_ms = int(datetime.combine(day, time.min, timezone.utc).timestamp() * 1000)
    return [open_ms, str(close * 0.99), str(close * 1.02), str(close * 0.98),
            str(close), "10", open_ms + DAY_MS - 1,
            "0", 1, "0", "0", "0"]


def test_utc_daily_evidence_rejects_open_or_non_utc_bar() -> None:
    day = date(2026, 9, 22)
    captured = datetime(2026, 9, 23, 0, 5, tzinfo=timezone.utc)
    completed = completed_kline_payload(_kline(day, 65000), captured)
    assert completed is not None
    assert completed[0] == day
    assert is_completed_canonical_close(completed[1], day, captured)
    assert completed_kline_payload(_kline(day + timedelta(days=1), 65010), captured) is None
    shifted = _kline(day, 65000)
    shifted[0] += 3_600_000
    with pytest.raises(ValueError, match="UTC 1d"):
        completed_kline_payload(shifted, captured)
    assert not is_completed_canonical_close(completed[1], day, captured - timedelta(days=1))


def test_fetch_uses_binance_public_spot_market_host(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    class Response:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> list:
            return []

    def fake_get(url: str, **kwargs):
        calls.append((url, kwargs["params"]))
        return Response()

    monkeypatch.setattr(binance_puller.requests, "get", fake_get)
    puller = object.__new__(BinancePuller)
    assert puller._fetch_klines("BTCUSDT") == []
    assert calls == [(
        "https://data-api.binance.vision/api/v3/klines",
        {"symbol": "BTCUSDT", "interval": "1d", "timeZone": "0", "limit": 2},
    )]


@pytest.mark.parametrize("symbol,feature,price", [
    ("BTCUSDT", "btc_full", 65000.0),
    ("ETHUSDT", "eth_full", 3200.0),
])
def test_completed_raw_to_canonical_without_open_bar_or_provisional_promotion(
    crypto_pg_engine: Engine, monkeypatch: pytest.MonkeyPatch,
    symbol: str, feature: str, price: float,
) -> None:
    engine = crypto_pg_engine
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    today = yesterday + timedelta(days=1)
    sid = f"binance.{symbol}.close"
    # This legacy unmarked row must neither resolve nor suppress completion.
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO raw_series (series_id,source_id,obs_date,value,raw_payload,pull_status)
            VALUES (:sid,184,:day,1.0,CAST(:payload AS jsonb),'SUCCESS')
        """), {"sid": sid, "day": yesterday,
               "payload": json.dumps({"symbol": symbol, "field": "close"})})

    resolver = Resolver(engine)
    first = resolver.resolve_pending(workers=1, since=datetime.now(timezone.utc) - timedelta(hours=1))
    assert first["resolved"] == 0

    puller = BinancePuller(engine)
    monkeypatch.setattr(puller, "_fetch_klines", lambda _symbol: [
        _kline(yesterday, price), _kline(today, price + 10),
    ])
    assert puller._pull_klines(symbol) == 5
    assert puller._pull_klines(symbol) == 0  # completed marker is idempotent
    second = resolver.resolve_pending(workers=1, since=datetime.now(timezone.utc) - timedelta(hours=1))
    assert second["errors"] == 0
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT rs.obs_date,rs.value,sc.name
            FROM resolved_series rs
            JOIN feature_registry fr ON fr.id=rs.feature_id
            JOIN source_catalog sc ON sc.id=rs.source_priority_used
            WHERE fr.name=:feature
        """), {"feature": feature}).fetchall()
        assert [(row[0], row[1], row[2]) for row in rows] == [
            (yesterday, price, "binance")
        ]
        raw = conn.execute(text("""
            SELECT obs_date,raw_payload FROM raw_series
            WHERE series_id=:sid ORDER BY id
        """), {"sid": sid}).fetchall()
        assert len(raw) == 2
        assert raw[1][1]["close_contract"] == "binance_utc_close_v1"
        assert raw[1][1]["close_time_ms"] < int(datetime.now(timezone.utc).timestamp() * 1000)
        assert all(row[0] != today for row in raw)
    print(f"BINANCE_CANONICAL_E2E symbol={symbol} feature={feature} "
          "unmarked_rejected=1 completed_raw=1 open_bar_rejected=1 resolved=1")


def test_access_failure_is_not_recorded_as_success_or_watermark(
    crypto_pg_engine: Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = crypto_pg_engine
    puller = BinancePuller(engine)

    def blocked(_symbol: str) -> list:
        raise BinanceAccessBlocked("Binance market-data host returned access status 451")

    monkeypatch.setattr(puller, "_fetch_klines", blocked)
    monkeypatch.setattr(scheduler, "_get_pullers_for_group", lambda *_args: [
        ("Binance_Crypto", puller, "pull", {})
    ])
    summary = scheduler.run_pull_group("daily", engine, config={})
    assert summary["failure_count"] == 1
    assert summary["success_count"] == 0
    with engine.connect() as conn:
        status = conn.execute(text("""
            SELECT status,rows_inserted FROM pull_log
            WHERE puller_name='Binance_Crypto'
        """)).one()
        watermark = conn.execute(text("""
            SELECT last_pull_at FROM source_catalog WHERE name='binance'
        """)).scalar_one()
    assert status == ("FAILED", 0)
    assert watermark is None
    print("BINANCE_FAILURE_ACCOUNTING pull_log_failed=1 watermark_unchanged=1")
