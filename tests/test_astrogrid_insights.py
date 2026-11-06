"""Forecast/data integrity regressions, separate from any claim of trading skill."""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from oracle import astrogrid_insights as insights
from store.astrogrid import AstroGridStore

NOW = datetime(2026, 11, 6, 1, 0, tzinfo=timezone.utc)


def candles(symbol="BTC", price=100.0):
    return [{"ts": NOW - timedelta(minutes=5 * (290 - i)), "close": price + i / 100,
             "volume": 10.0, "available_at": NOW - timedelta(minutes=5 * (289 - i))}
            for i in range(290)]


def trigger(symbol="BTC", direction="buy"):
    return {"id": 11, "symbol": symbol, "source": "institutional", "source_id": "fixture",
            "event_date": NOW.date().isoformat(), "available_at": NOW.isoformat(),
            "details": {"actor": "Synthetic allocator", "action": "acquired inventory",
                        "liquidity_valve": "spot demand", "flow_direction": direction,
                        "flow_amount": 12.0, "flow_unit": symbol}}


def payload(direction="buy"):
    obs = insights.observe("BTC", candles(), NOW)
    return insights.build_payload(obs, trigger(direction=direction), NOW)


def score_engine(p, outcome_price):
    engine = MagicMock()
    c = p["grid_feature_payload"]["insight"]["forecast"]
    entry = c["entry"]
    result1, result2 = MagicMock(), MagicMock()
    result1.fetchone.return_value = (entry["price"], datetime.fromisoformat(entry["available_at"]))
    result2.fetchone.return_value = (NOW + insights.HORIZON - insights.BAR,
                                    outcome_price, NOW + insights.HORIZON)
    engine.connect.return_value.__enter__.return_value.execute.side_effect = [result1, result2]
    return engine


def score(engine, p, **overrides):
    args = {"symbols": ["BTC"], "evidence": p["grid_feature_payload"]["insight"],
            "issued_at": NOW, "created_at": NOW, "cutoff": NOW + insights.HORIZON}
    args.update(overrides)
    return insights.score_contract(engine, **args)


def test_observation_has_real_window_and_no_invented_prediction():
    obs = insights.observe("BTC", candles(), NOW)
    assert obs["as_of"] == NOW.isoformat()
    assert obs["change_24h"] == pytest.approx(102.89 / 100.01 - 1)
    assert obs["recorded_volume_24h"] == 2880
    assert "volume_24h" not in obs
    assert obs["coverage"] == 1
    p = insights.build_payload(obs, None, NOW)
    assert p["scoring_class"] == "unscored_experimental"
    assert p["grid_feature_payload"]["insight"]["forecast"] is None
    assert "No verified actor" in p["setup"]


@pytest.mark.parametrize("mutate", [
    lambda cs: [],
    lambda cs: [{**c, "close": float("nan")} for c in cs],
    lambda cs: [{**c, "ts": c["ts"] - timedelta(hours=1)} for c in cs],
    lambda cs: [{**c, "available_at": NOW + timedelta(seconds=1)} for c in cs],
])
def test_absent_invalid_stale_or_future_data_never_becomes_live(mutate):
    obs = insights.observe("BTC", mutate(candles()), NOW)
    assert obs["available"] is False
    assert obs["price"] is None
    assert obs["change_24h"] is None


def test_incomplete_history_is_not_a_zero_return_or_directional_forecast():
    obs = insights.observe("BTC", candles()[-10:], NOW)
    assert obs["available"]
    assert obs["change_24h"] is None
    p = insights.build_payload(obs, trigger(), NOW)
    assert p["grid_feature_payload"]["insight"]["kind"] == "observation"


@pytest.mark.parametrize("field,value", [("actor", ""), ("flow_amount", 0),
    ("flow_amount", float("nan")), ("flow_amount", True), ("flow_direction", "BUY"), ("liquidity_valve", None)])
def test_conditions_or_malformed_levers_cannot_create_forecast(field, value):
    row = trigger()
    row["details"][field] = value
    assert insights.causal_trigger([row], "BTC") is None


def test_trigger_is_asset_specific_and_only_selected_evidence_is_published():
    row = trigger("ETH")
    assert insights.causal_trigger([row], "BTC") is None
    row = trigger()
    row["details"]["unrelated_payload"] = "must not publish"
    assert "unrelated_payload" not in insights.causal_trigger([row], "BTC")["details"]


def test_forecast_cannot_be_repeated_for_each_new_price_bar():
    obs = insights.observe("BTC", candles(), NOW)
    p1 = insights.build_payload(obs, trigger(), NOW)
    obs["as_of"] = (NOW + insights.BAR).isoformat()
    p2 = insights.build_payload(obs, trigger(), NOW + insights.BAR)
    assert p1["prediction_id"] == p2["prediction_id"]


@pytest.mark.parametrize("direction,price,verdict", [
    ("buy", 110, "hit"), ("buy", 100, "miss"),
    ("sell", 95, "hit"), ("sell", 110, "miss"),
])
def test_bull_and_bear_grade_their_declared_asset_threshold(direction, price, verdict):
    p = payload(direction)
    engine = score_engine(p, price)
    result = score(engine, p)
    assert result["verdict"] == verdict
    assert result["raw_payload"]["symbols"] == ["BTC"]
    assert result["benchmark_symbol"] == "cash"
    calls = engine.connect.return_value.__enter__.return_value.execute.call_args_list
    assert all(c.args[1]["instrument"] == "BTCUSDT" for c in calls)


def test_missing_outcome_is_pending_even_if_spy_or_other_coin_is_present():
    p = payload()
    engine = score_engine(p, 110)
    result = MagicMock()
    result.fetchone.return_value = None
    first = MagicMock()
    entry = p["grid_feature_payload"]["insight"]["forecast"]["entry"]
    first.fetchone.return_value = (entry["price"], NOW)
    engine.connect.return_value.__enter__.return_value.execute.side_effect = [first, result]
    assert score(engine, p)["unscored"]["reason"] == "missing_insight_outcome"


def test_future_forecast_cannot_be_scored_or_backdated_to_make_it_mature():
    p = payload()
    engine = MagicMock()
    assert score(engine, p, cutoff=NOW)["not_mature"]
    engine.connect.assert_not_called()
    result = score(engine, p, created_at=NOW + timedelta(days=1))
    assert result["unscored"]["reason"] == "invalid_insight_contract"


@pytest.mark.parametrize("mutation", ["symbol", "source", "direction", "price", "horizon"])
def test_corrupt_contract_never_grades(mutation):
    p = payload()
    contract = p["grid_feature_payload"]["insight"]["forecast"]
    if mutation == "symbol":
        contract["symbol"] = "ETH"
    elif mutation == "source":
        contract["entry"]["source"] = "yahoo"
    elif mutation == "direction":
        contract["direction"] = "neutral"
    elif mutation == "price":
        contract["entry"]["price"] = 0
    else:
        contract["due_at"] = (NOW + timedelta(hours=1)).isoformat()
    assert score(MagicMock(), p)["unscored"]["reason"] == "invalid_insight_contract"


def test_entry_is_verified_against_original_source_row():
    p = payload()
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (7, NOW)
    assert score(engine, p)["unscored"]["reason"] == "unverified_insight_entry"


def test_store_routes_new_contract_before_legacy_seven_day_maturity(monkeypatch):
    p = payload()
    engine = score_engine(p, 110)
    store = AstroGridStore(engine)
    row = [1, p["prediction_id"], NOW, "swing", "liquid_market", ["BTC"],
           p["call"], p["setup"], p["invalidation"], {}, {}, p["grid_feature_payload"],
           p["question"], None, NOW, insights.MODEL_VERSION]
    result = store._score_candidate_row(None, row, evaluation_date=NOW.date() + timedelta(days=1),
                                        score_cutoff=NOW + insights.HORIZON)
    assert result["state"] == "scored"
    assert result["score"]["raw_payload"]["symbols"] == ["BTC"]


def test_retries_are_idempotent_and_observation_always_published(monkeypatch):
    monkeypatch.setattr(insights, "read_triggers", lambda *args: [trigger()])
    monkeypatch.setattr(insights, "read_candles", lambda *args: candles())
    store = MagicMock()
    known = {}
    store.get_prediction.side_effect = lambda pid: known.get(pid)
    def save(p):
        known[p["prediction_id"]] = p
        return p
    store.save_prediction.side_effect = save
    result = insights.produce_insights(MagicMock(), store, as_of=NOW)
    assert result["persisted"] == 4  # Three observations and one causal forecast.
    result = insights.produce_insights(MagicMock(), store, as_of=NOW)
    assert result["persisted"] == 0
    assert result["unchanged"] == 4


def test_answer_abstains_for_uncovered_asset_and_ignores_old_winners():
    feed = {"items": [{"prediction_id": "x", "call": "BTC +2%", "status": "observation",
                       "stale": False, "evidence": {"symbol": "BTC"}}]}
    answer = insights.answer_from_insights(feed, ["NVDA"], now=NOW)
    assert answer["target_symbols"] == []
    assert answer["call"] == "No qualified directional forecast"
    assert answer["confidence"] is None


def test_pit_queries_bind_availability_and_exact_source_before_inference():
    from store.pit import PITStore
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
    PITStore(engine).get_closed_crypto_candles("ETH", as_of=NOW)
    statement, params = engine.connect.return_value.__enter__.return_value.execute.call_args.args
    assert "created_at <= :as_of" in str(statement)
    assert "INTERVAL '5 minutes' <= :as_of" in str(statement)
    assert params["instrument"] == "ETHUSDT"
    assert params["source"] == "binance"
    with pytest.raises(ValueError):
        PITStore(engine).get_closed_crypto_candles("SPY", as_of=NOW)
    with pytest.raises(ValueError):
        PITStore(engine).get_crypto_actor_actions(as_of=NOW.replace(tzinfo=None))
