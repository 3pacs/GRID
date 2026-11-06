"""Opt-in end-to-end test on an empty, disposable PostgreSQL database.

Requires the deployed schema and ASTROGRID_INSIGHT_TEST_DB_URL. The database
name must start astrogrid_insight_e2e_; production cannot be selected. All
prices, actors and timestamps written here are synthetic fixtures.
"""
import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from oracle import astrogrid_insights as insights
from store.astrogrid import AstroGridStore


@pytest.mark.integration
def test_native_forecast_scoring_api_and_immutable_history(monkeypatch):
    url = os.getenv("ASTROGRID_INSIGHT_TEST_DB_URL")
    if not url:
        pytest.skip("Opt-in disposable PostgreSQL URL required")
    if not make_url(url).database.startswith("astrogrid_insight_e2e_"):
        pytest.fail("Refusing a non-disposable database")
    engine = create_engine(url)
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    now -= timedelta(minutes=now.minute % 5)
    with engine.begin() as conn:
        assert conn.execute(text("SELECT count(*) FROM astrogrid.prediction_run")).scalar() == 0
        source = conn.execute(text("""
            INSERT INTO signal_sources (source_type, source_id, ticker, signal_date, signal_type,
                                        signal_value, created_at)
            VALUES ('institutional', 'synthetic-e2e', 'BTC', :date, 'BUY',
                    CAST(:value AS jsonb), :now) RETURNING id
        """), {"date": now.date(), "now": now, "value": '''{
            "actor":"Synthetic allocator", "action":"acquired inventory",
            "liquidity_valve":"spot demand", "flow_direction":"buy",
            "flow_amount":12, "flow_unit":"BTC"}'''}).scalar()
        for symbol, base in [("BTC", 100), ("ETH", 50), ("SOL", 20)]:
            conn.execute(text("""
                INSERT INTO realtime_candles (symbol, asset_class, interval, ts, open, high, low,
                    close, volume, source, created_at)
                VALUES (:symbol, 'crypto', '5m', :ts, :price, :price, :price,
                        :price, 10, 'binance', :available)
            """), [{"symbol": symbol + "USDT", "ts": now - timedelta(minutes=5 * (290-i)),
                     "price": base + i / 100, "available": now - timedelta(minutes=5 * (289-i))}
                    for i in range(290)])
    store = AstroGridStore(engine)
    result = insights.produce_insights(engine, store, as_of=now)
    assert result["persisted"] == 4
    assert result["errors"] == []
    with engine.connect() as conn:
        forecast = conn.execute(text("""
            SELECT prediction_id, row_to_json(pr)::text,
                   (SELECT row_to_json(pp)::text FROM astrogrid.prediction_postmortem pp
                    WHERE pp.prediction_run_id=pr.id)
            FROM astrogrid.prediction_run pr
            WHERE scoring_class = 'liquid_market'
        """)).one()
    monkeypatch.setattr("store.astrogrid._utc_now", lambda: now)
    assert store.score_predictions(prediction_ids=[forecast[0]])["scored"] == 0
    cutoff = now + insights.HORIZON
    monkeypatch.setattr("store.astrogrid._utc_now", lambda: cutoff)
    # An available price for another coin must not fill a missing BTC outcome.
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO realtime_candles (symbol, asset_class, interval, ts, open, high, low,
                close, volume, source, created_at)
            VALUES ('ETHUSDT', 'crypto', '5m', :ts, 500, 500, 500, 500, 10, 'binance', :available)
        """), {"ts": cutoff - insights.BAR, "available": cutoff})
    result = store.score_predictions(as_of_date=cutoff.date(), prediction_ids=[forecast[0]])
    assert result["scored"] == 0
    assert result["unscored"][0]["reason"] == "missing_insight_outcome"
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO realtime_candles (symbol, asset_class, interval, ts, open, high, low,
                close, volume, source, created_at)
            VALUES ('BTCUSDT', 'crypto', '5m', :ts, 110, 110, 110, 110, 10, 'binance', :available)
        """), {"ts": cutoff - insights.BAR, "available": cutoff})
    result = store.score_predictions(as_of_date=cutoff.date(), prediction_ids=[forecast[0]])
    assert result["scored"] == 1
    assert result["verdicts"]["hit"] == 1
    assert store.score_predictions(as_of_date=cutoff.date(), prediction_ids=[forecast[0]])["scored"] == 0
    with engine.connect() as conn:
        unchanged = conn.execute(text("""
            SELECT row_to_json(pr)::text FROM astrogrid.prediction_run pr WHERE prediction_id=:pid
        """), {"pid": forecast[0]}).scalar()
    assert unchanged == forecast[1]
    with engine.connect() as conn:
        original_postmortem = conn.execute(text("""
            SELECT row_to_json(pp)::text FROM astrogrid.prediction_postmortem pp
            JOIN astrogrid.prediction_run pr ON pr.id=pp.prediction_run_id
            WHERE pr.prediction_id=:pid
        """), {"pid": forecast[0]}).scalar()
    assert original_postmortem == forecast[2]
    from astrogrid_api.main import app
    import astrogrid_api.astrogrid_predictions as routes
    monkeypatch.setattr(routes, "get_db_engine", lambda: engine)
    monkeypatch.setattr(routes, "get_astrogrid_store", lambda: store)
    with TestClient(app) as client:
        feed = client.get("/api/v1/astrogrid/insights")
        assert feed.status_code == 200
        data = feed.json()
        assert len(data["items"]) == 4
        assert data["performance"]["scored"] == 1
        forecast_card = next(i for i in data["items"] if i["prediction_id"] == forecast[0])
        assert forecast_card["status"] == "hit"
        assert forecast_card["score"]["symbols"] == ["BTC"]
        assert forecast_card["evidence"]["trigger"]["id"] == source
        detail = client.get(f"/api/v1/astrogrid/predictions/{forecast[0]}").json()
        assert detail["status"] == "hit"
        assert detail["target_symbols"] == ["BTC"]
        assert detail["postmortem"]["state"] == "scored"
        assert "signed return" in detail["postmortem"]["summary"]
        answer = client.post("/api/v1/astrogrid/guru/ask", json={
            "question": "What crypto should I buy?",
            "seer": {"prediction": "buy SOL", "confidence": 1},
        }).json()
        assert answer["prediction"] is None
        assert "buy SOL" not in answer["answer"]["call"]
        assert answer["persistence_status"] == "not_attempted"
        assert len(store.list_predictions(limit=20)) == 4
    engine.dispose()
