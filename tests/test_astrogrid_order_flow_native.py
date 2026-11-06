"""Opt-in synthetic full-schema E2E. Real prospective verification runs separately.

Refuses production. Source prices, actors and future test clocks here are
synthetic and must never be inserted into the production database.
"""
import math
import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from binance_close_contract import completed_spot_flow_payload
from ingestion.crypto_signals import CryptoSignalBridge
from oracle import astrogrid_order_flow as flow
from store.astrogrid import AstroGridStore


@pytest.mark.integration
def test_native_receipts_forecast_automatic_scoring_and_separate_public_cohorts(monkeypatch):
    url = os.getenv("ASTROGRID_FLOW_TEST_DB_URL")
    if not url:
        pytest.skip("Opt-in disposable PostgreSQL URL required")
    if not make_url(url).database.startswith("astrogrid_insight_e2e_flow_"):
        pytest.fail("Refusing a non-disposable database")
    engine = create_engine(url)
    now = datetime.now(timezone.utc)
    ended = now.replace(second=0, microsecond=0) - timedelta(minutes=now.minute % 5)
    def raw(coin="BTC", end=ended, price=100.0):
        ms = int(end.timestamp() * 1000)
        buy = 140_000 if coin == "BTC" else 100_000
        return [ms-300_000, str(price), str(price), str(price), str(price), str(200_000/price),
                ms-1, "200000", 2000, str(buy/price), str(buy), "0"]
    monkeypatch.setattr("ingestion.altdata.binance_puller.BinancePuller.fetch_spot_flow_klines",
                        lambda symbol, **kw: [raw(symbol.removesuffix("USDT"))])
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM astrogrid.prediction_run")).scalar() == 0
    bridge = CryptoSignalBridge(engine)
    assert bridge.bridge_binance_order_flow() == {"receipts_inserted": 3, "unchanged": 0, "errors": []}
    assert bridge.bridge_binance_order_flow() == {"receipts_inserted": 0, "unchanged": 3, "errors": []}
    store = AstroGridStore(engine)
    generated = flow.produce_forecasts(engine, store)
    assert generated["errors"] == [] and generated["persisted"] == 1
    assert generated["qualification"]["BTC"]["qualified"]
    assert generated["qualification"]["ETH"]["reason"] == "balanced_taker_flow"
    assert flow.produce_forecasts(engine, store)["unchanged"] == 1
    with engine.connect() as conn:
        original = conn.execute(text("""
            SELECT prediction_id, as_of_ts, row_to_json(pr)::text,
                   (SELECT row_to_json(pp)::text FROM astrogrid.prediction_postmortem pp
                    WHERE pp.prediction_run_id=pr.id)
            FROM astrogrid.prediction_run pr
        """)).one()
    assert store.score_predictions(model_versions=[flow.MODEL_VERSION])["scored"] == 0
    due = original[1] + flow.HORIZON
    outcome_end = datetime.fromtimestamp(math.ceil(due.timestamp()/300)*300, timezone.utc)
    cutoff = outcome_end + timedelta(seconds=5)
    monkeypatch.setattr("store.astrogrid._utc_now", lambda: cutoff)
    def insert_outcome(symbol, price):
        p = completed_spot_flow_payload(raw(symbol, outcome_end, price), symbol+"USDT", cutoff-timedelta(seconds=2))
        import json
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO signal_sources(source_type,source_id,ticker,signal_date,signal_type,signal_value,created_at)
                VALUES ('exchange_flow',:sid,:symbol,:day,'spot_taker_flow',CAST(:p AS jsonb),:available)
            """), {"sid": f"binance_spot:5m:{symbol}:{p['kline'][0]}", "symbol":symbol,
                   "day":outcome_end.date(), "p":json.dumps(p), "available":cutoff-timedelta(seconds=1)})
    insert_outcome("ETH", 999)
    scored = store.score_predictions(as_of_date=cutoff.date(), model_versions=[flow.MODEL_VERSION])
    assert scored["scored"] == 0
    assert scored["unscored"][0]["reason"] == "missing_order_flow_outcome"
    insert_outcome("BTC", 99.95)  # A genuine miss in this synthetic positive-flow fixture.
    scored = store.score_predictions(as_of_date=cutoff.date(), model_versions=[flow.MODEL_VERSION])
    assert scored["scored"] == 1 and scored["verdicts"]["miss"] == 1
    assert store.score_predictions(as_of_date=cutoff.date(), model_versions=[flow.MODEL_VERSION])["scored"] == 0
    with engine.connect() as conn:
        unchanged = conn.execute(text("""
            SELECT row_to_json(pr)::text,
                   (SELECT row_to_json(pp)::text FROM astrogrid.prediction_postmortem pp WHERE pp.prediction_run_id=pr.id)
            FROM astrogrid.prediction_run pr WHERE prediction_id=:pid
        """), {"pid":original[0]}).one()
    assert unchanged == (original[2], original[3])
    import astrogrid_api.astrogrid_predictions as routes
    from astrogrid_api.main import app
    monkeypatch.setattr(routes, "get_db_engine", lambda: engine)
    monkeypatch.setattr(routes, "get_astrogrid_store", lambda: store)
    with TestClient(app) as client:
        feed = client.get("/api/v1/astrogrid/insights").json()
        assert feed["performance"]["scored"] == 0
        short = feed["performance_by_contract"][flow.CONTRACT_VERSION]
        assert short["scored"] == 1 and short["accuracy"] == 0
        card = next(i for i in feed["items"] if i["prediction_id"] == original[0])
        assert card["status"] == "miss" and card["score"]["outcome"]["instrument"] == "BTCUSDT"
        detail = client.get(f"/api/v1/astrogrid/predictions/{original[0]}").json()
        assert detail["status"] == "miss" and detail["postmortem"]["projection"].startswith("derived_from_score")
        assert detail["target_symbols"] == ["BTC"] and detail["postmortem"]["original_summary"]
    engine.dispose()
