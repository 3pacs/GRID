"""Measured-flow gates and prospective grading, never a trading-edge claim."""
import copy
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from binance_close_contract import (
    completed_spot_flow_payload,
    verified_spot_flow_payload,
)
from ingestion.crypto_signals import CryptoSignalBridge
from oracle import astrogrid_order_flow as flow
from store.astrogrid import AstroGridStore

NOW = datetime(2026, 11, 6, 5, 0, 10, tzinfo=timezone.utc)


def kline(ended=None, price=100, quote=200_000, buy=140_000, trades=2000):
    end = ended or NOW.replace(second=0)
    ms = int(end.timestamp() * 1000)
    return [ms - 300_000, str(price), str(price), str(price), str(price), str(quote / price),
            ms - 1, str(quote), trades, str(buy / price), str(buy), "0"]


def receipt(ident=11, symbol="BTC", ended=None, price=100, buy=140_000, available=None):
    ended = ended or NOW.replace(second=0)
    available = available or ended + timedelta(seconds=6)
    p = completed_spot_flow_payload(kline(ended, price, buy=buy), symbol + "USDT",
                                    available - timedelta(seconds=1))
    return {"id": ident, "symbol": symbol, "payload": p, "available_at": available,
            "source_id": f"binance_spot:5m:{symbol}:{p['kline'][0]}"}


def source_tuple(row):
    return tuple(row[k] for k in ("id", "symbol", "payload", "available_at", "source_id"))


def engine_for(entry, outcomes):
    engine = MagicMock()
    first, second = MagicMock(), MagicMock()
    first.fetchone.return_value = source_tuple(entry) if entry else None
    second.fetchall.return_value = [source_tuple(r) for r in outcomes]
    engine.connect.return_value.__enter__.return_value.execute.side_effect = [first, second]
    return engine


def score(engine, payload, cutoff=None, **kwargs):
    return flow.score_contract(engine, symbols=kwargs.pop("symbols", ["BTC"]),
                               evidence=payload["grid_feature_payload"]["insight"], issued_at=NOW,
                               created_at=kwargs.pop("created_at", NOW),
                               cutoff=cutoff or NOW + flow.HORIZON + timedelta(minutes=6), **kwargs)


def test_receipt_preserves_measured_taker_volumes_and_true_availability():
    row = receipt()
    p = row["payload"]
    assert p["taker_sell_quote"] == 60_000
    assert verified_spot_flow_payload(p, row["available_at"], NOW) == p
    assert verified_spot_flow_payload(p, row["available_at"], NOW - timedelta(seconds=9)) is None
    altered = {**p, "taker_buy_quote": 199_999}
    assert verified_spot_flow_payload(altered, row["available_at"], NOW) is None


@pytest.mark.parametrize("index,value", [(0, True), (6, 2), (8, True), (8, -1),
                                       (4, "NaN"), (7, "inf"), (10, "200001"),
                                       (9, "2001"), (3, "101"), (1, False)])
def test_malformed_provider_kline_rejected(index, value):
    raw = kline()
    raw[index] = value
    with pytest.raises((ValueError, TypeError)):
        completed_spot_flow_payload(raw, "BTCUSDT", NOW)


def test_open_bar_never_enters_receipts():
    assert completed_spot_flow_payload(kline(NOW.replace(second=0) + timedelta(minutes=5)), "BTCUSDT", NOW) is None


@pytest.mark.parametrize("change,reason", [
    ({"buy": 100_000}, "balanced_taker_flow"),
    ({"quote": 50_000, "buy": 40_000}, "insufficient_quote_turnover"),
    ({"trades": 99}, "insufficient_trades"),
])
def test_gates_are_actual_flow_not_price_or_total_volume(change, reason):
    row = receipt()
    row["payload"] = completed_spot_flow_payload(kline(**change), "BTCUSDT", NOW - timedelta(seconds=5))
    assert flow.qualification(row, NOW)["reason"] == reason
    with pytest.raises(ValueError):
        flow.build_forecast(row, NOW)


def test_stale_source_abstains_and_balanced_latest_does_not_pick_older_buy(monkeypatch):
    old = receipt(ended=NOW.replace(second=0) - timedelta(minutes=15))
    assert flow.qualification(old, NOW)["reason"] == "stale_spot_flow"
    latest, older = receipt(buy=100_000), receipt(ident=10, ended=NOW.replace(second=0)-timedelta(minutes=5))
    monkeypatch.setattr("store.pit.PITStore.get_spot_order_flow_receipts", lambda *a, **k: [older, latest])
    rows = flow.latest_receipts(MagicMock(), NOW)
    assert rows["BTC"]["id"] == latest["id"]
    assert not flow.qualification(rows["BTC"], NOW)["qualified"]


@pytest.mark.parametrize("buy,direction", [(140_000, "buy"), (60_000, "sell")])
def test_contract_declares_separate_horizon_threshold_and_anonymous_actor(buy, direction):
    p = flow.build_forecast(receipt(buy=buy), NOW)
    e = p["grid_feature_payload"]["insight"]
    c = e["forecast"]
    assert p["target_symbols"] == ["BTC"]
    assert p["model_version"] == flow.MODEL_VERSION
    assert c["due_at"] == (NOW + timedelta(minutes=30)).isoformat()
    assert c["threshold"] == .001
    assert c["direction"] == direction
    assert "anonymous" in e["trigger"]["details"]["actor"]
    assert c["entry"]["source"] == "binance_spot_rest"


@pytest.mark.parametrize("buy,price,verdict", [(140_000, 100.1, "hit"), (140_000, 100.09, "miss"),
                                            (60_000, 99.9, "hit"), (60_000, 100.2, "miss")])
def test_realized_same_venue_same_coin_return_grades_hit_or_miss(buy, price, verdict):
    entry = receipt(buy=buy)
    outcome = receipt(ident=12, ended=NOW.replace(second=0)+timedelta(minutes=35), price=price)
    p = flow.build_forecast(entry, NOW)
    engine = engine_for(entry, [outcome])
    result = score(engine, p)
    assert result["verdict"] == verdict
    assert result["raw_payload"]["contract_version"] == flow.CONTRACT_VERSION
    assert result["raw_payload"]["outcome"]["receipt_id"] == 12
    queries = engine.connect.return_value.__enter__.return_value.execute.call_args_list
    assert "realtime_candles" not in " ".join(str(c.args[0]) for c in queries)
    assert queries[1].args[1]["symbol"] == "BTC"
    assert all("BTC:" in sid for sid in queries[1].args[1]["ids"])


def test_immature_and_backdated_forecasts_never_score():
    p = flow.build_forecast(receipt(), NOW)
    engine = MagicMock()
    assert score(engine, p, cutoff=NOW)["not_mature"]
    engine.connect.assert_not_called()
    assert "invalid" in score(engine, p, created_at=NOW+timedelta(hours=1))["unscored"]["reason"]


@pytest.mark.parametrize("bad", ["wrong_coin", "wrong_venue", "late_available", "past_due_grace", "missing"])
def test_other_coin_venue_late_or_absent_outcome_stays_unscored(bad):
    entry = receipt()
    outcome = receipt(ident=12, ended=NOW.replace(second=0)+timedelta(minutes=35))
    if bad == "wrong_coin":
        outcome = receipt(ident=12, symbol="ETH", ended=NOW.replace(second=0)+timedelta(minutes=35))
    elif bad == "wrong_venue":
        outcome["payload"]["endpoint_host"] = "stream.binance.us"
    elif bad == "late_available":
        outcome["available_at"] = NOW + timedelta(days=1)
    elif bad == "past_due_grace":
        outcome = receipt(ident=12, ended=NOW.replace(second=0)+timedelta(minutes=50))
    result = score(engine_for(entry, [] if bad == "missing" else [outcome]), flow.build_forecast(entry, NOW))
    assert result["unscored"]["reason"] == "missing_order_flow_outcome"


@pytest.mark.parametrize("field,value", [("direction", "sell"), ("threshold", .00001),
                                       ("horizon_minutes", 5), ("target_price", 1000)])
def test_contract_tampering_cannot_change_the_frozen_rule(field, value):
    entry = receipt()
    p = flow.build_forecast(entry, NOW)
    p["grid_feature_payload"]["insight"]["forecast"][field] = value
    assert "unscored" in score(engine_for(entry, []), p)


def test_changed_source_and_asset_identity_are_rejected():
    entry = receipt()
    p = flow.build_forecast(entry, NOW)
    changed = copy.deepcopy(entry)
    changed["payload"]["captured_at"] = NOW.isoformat()
    assert "unscored" in score(engine_for(changed, []), p)
    assert "unscored" in score(MagicMock(), p, symbols=["ETH"])


def test_receipt_bridge_counts_actual_insertions_and_never_updates(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    monkeypatch.setattr("ingestion.crypto_signals.datetime", Clock)
    monkeypatch.setattr("ingestion.altdata.binance_puller.BinancePuller.fetch_spot_flow_klines",
                        lambda *a, **k: [kline()])
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    conn.execute.return_value.scalar.side_effect = [1, None, 3]
    result = CryptoSignalBridge(engine).bridge_binance_order_flow()
    assert result == {"receipts_inserted": 2, "unchanged": 1, "errors": []}
    assert all("DO NOTHING" in str(c.args[0]) and "UPDATE" not in str(c.args[0]) for c in conn.execute.call_args_list)


def test_bridge_errors_are_visible_and_bound_to_asset(monkeypatch):
    def fail(*a, **k):
        raise ConnectionError("private credential must not be returned")
    monkeypatch.setattr("ingestion.altdata.binance_puller.BinancePuller.fetch_spot_flow_klines", fail)
    result = CryptoSignalBridge(MagicMock()).bridge_binance_order_flow()
    assert len(result["errors"]) == 3
    assert all(e["reason"] == "ConnectionError" for e in result["errors"])
    assert "credential" not in str(result)


def test_scheduled_scoring_is_scoped_and_30m_is_mature_in_sql():
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = []
    AstroGridStore(engine).score_predictions(model_versions=[flow.MODEL_VERSION])
    statement, params = conn.execute.call_args.args
    assert "INTERVAL '30 minutes'" in str(statement)
    assert params["model_versions"] == [flow.MODEL_VERSION]
    assert "pr.model_version = ANY(:model_versions)" in str(statement)
