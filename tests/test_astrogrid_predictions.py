from __future__ import annotations

import os
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
_TEST_PASSWORD = "testpassword123"
_TEST_HASH = _pwd_ctx.hash(_TEST_PASSWORD)
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _TEST_HASH)

from api.auth import create_token
from api.routers.astrogrid import AstrogridPredictionRequest, _infer_target_symbols
from api.main import app
from store.astrogrid import AstroGridStore

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    token = create_token(expires_hours=1)
    return {"Authorization": f"Bearer {token}"}


def test_infer_target_symbols_from_question_aliases() -> None:
    req = AstrogridPredictionRequest(
        question="Which stock is the best buy right now: Google, Apple, or Microsoft?",
        call="wait",
        timing="now",
        setup="relative strength",
        invalidation="break if regime flips",
    )
    assert _infer_target_symbols(req) == ["AAPL", "MSFT", "GOOGL"]


def test_infer_target_symbols_from_group_cue_without_explicit_symbols() -> None:
    req = AstrogridPredictionRequest(
        question="What crypto should I buy right now?",
        call="press leader",
        timing="now",
        setup="relative strength",
        invalidation="break if regime flips",
    )
    assert _infer_target_symbols(req)[:3] == ["BTC", "ETH", "SOL"]


def test_score_predictions_prefers_mature_as_of_dates(mock_engine) -> None:
    store = AstroGridStore(mock_engine)
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    captured: dict[str, object] = {}

    select_row = (
        1,
        "pred-mature",
        datetime(2026, 2, 1, tzinfo=timezone.utc),
        "swing",
        "liquid_market",
        '["BTC"]',
        "buy BTC",
        "leader rotation",
        "break if regime flips",
        "{}",
        "{}",
        "{}",
        "What crypto should I buy right now?",
        None,
    )
    insert_result = MagicMock()
    insert_result.fetchone.return_value = (99,)

    def _execute_side_effect(statement, params=None):
        sql_text = str(statement)
        if "FROM astrogrid.prediction_run pr" in sql_text:
            captured["sql"] = sql_text
            captured["params"] = dict(params or {})
            result = MagicMock()
            result.fetchall.return_value = [select_row]
            return result
        if "INSERT INTO astrogrid.prediction_score" in sql_text:
            return insert_result
        raise AssertionError(f"Unexpected SQL executed: {sql_text}")

    mock_conn.execute.side_effect = _execute_side_effect
    store._get_symbol_price_at_date = MagicMock(side_effect=[100.0, 105.0, 100.0, 101.0])
    store._load_price_path = MagicMock(return_value=[(date(2026, 2, 1), 100.0), (date(2026, 3, 29), 105.0)])

    summary = store.score_predictions(as_of_date=date(2026, 3, 29), limit=200)

    assert summary["candidates"] == 1
    assert summary["scored"] == 1
    assert summary["skipped_not_mature"] == 0
    assert summary["prediction_ids"] == ["pred-mature"]
    assert "pr.as_of_ts::date" in str(captured["sql"])
    assert "THEN 30" in str(captured["sql"])
    assert "ELSE 7" in str(captured["sql"])
    assert "ORDER BY pr.as_of_ts ASC, pr.created_at ASC" in str(captured["sql"])
    assert captured["params"]["evaluation_date"] == date(2026, 3, 29)


def test_score_predictions_uses_historical_regime_context(mock_engine) -> None:
    store = AstroGridStore(mock_engine)
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    captured: dict[str, object] = {}

    select_row = (
        1,
        "pred-regime",
        datetime(2026, 2, 1, tzinfo=timezone.utc),
        "swing",
        "liquid_market",
        '["BTC"]',
        "buy BTC",
        "leader rotation",
        "break if regime flips",
        '{"regime":{"state":"neutral","confidence":0.3},"thesis":{"stance":"steady"}}',
        "{}",
        "{}",
        "What crypto should I buy right now?",
        None,
    )
    insert_result = MagicMock()
    insert_result.fetchone.return_value = (100,)

    def _execute_side_effect(statement, params=None):
        sql_text = str(statement)
        result = MagicMock()
        if "FROM astrogrid.prediction_run pr" in sql_text:
            result.fetchall.return_value = [select_row]
            return result
        if "SELECT regime, confidence" in sql_text and "FROM regime_history" in sql_text:
            result.fetchone.return_value = ("risk_off", 0.82)
            return result
        if "INSERT INTO astrogrid.prediction_score" in sql_text:
            captured["insert_params"] = dict(params or {})
            return insert_result
        raise AssertionError(f"Unexpected SQL executed: {sql_text}")

    mock_conn.execute.side_effect = _execute_side_effect
    store._get_symbol_price_at_date = MagicMock(side_effect=[100.0, 105.0, 100.0, 101.0])
    store._load_price_path = MagicMock(return_value=[(date(2026, 2, 1), 100.0), (date(2026, 3, 29), 105.0)])

    summary = store.score_predictions(as_of_date=date(2026, 3, 29), limit=200)

    regime_context = captured["insert_params"]["regime_context"]
    assert summary["scored"] == 1
    assert '"regime": "risk_off"' in regime_context
    assert '"confidence": 0.82' in regime_context
    assert '"source": "regime_history"' in regime_context
    assert '"thesis": "steady"' in regime_context


def test_score_predictions_falls_back_to_earliest_regime_history(mock_engine) -> None:
    store = AstroGridStore(mock_engine)
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    captured: dict[str, object] = {}

    select_row = (
        1,
        "pred-old-regime",
        datetime(2024, 2, 1, tzinfo=timezone.utc),
        "swing",
        "liquid_market",
        '["BTC"]',
        "buy BTC",
        "leader rotation",
        "break if regime flips",
        "{}",
        "{}",
        "{}",
        "What crypto should I buy right now?",
        None,
    )
    insert_result = MagicMock()
    insert_result.fetchone.return_value = (101,)

    def _execute_side_effect(statement, params=None):
        sql_text = str(statement)
        result = MagicMock()
        if "FROM astrogrid.prediction_run pr" in sql_text:
            result.fetchall.return_value = [select_row]
            return result
        if "WHERE obs_date <= :target_date" in sql_text and "FROM regime_history" in sql_text:
            result.fetchone.return_value = None
            return result
        if "ORDER BY obs_date ASC" in sql_text and "FROM regime_history" in sql_text:
            result.fetchone.return_value = ("risk_on", 0.61)
            return result
        if "INSERT INTO astrogrid.prediction_score" in sql_text:
            captured["insert_params"] = dict(params or {})
            return insert_result
        raise AssertionError(f"Unexpected SQL executed: {sql_text}")

    mock_conn.execute.side_effect = _execute_side_effect
    store._get_symbol_price_at_date = MagicMock(side_effect=[100.0, 105.0, 100.0, 101.0])
    store._load_price_path = MagicMock(return_value=[(date(2024, 2, 1), 100.0), (date(2024, 3, 1), 105.0)])

    summary = store.score_predictions(as_of_date=date(2026, 3, 29), limit=200)

    regime_context = captured["insert_params"]["regime_context"]
    assert summary["scored"] == 1
    assert '"regime": "risk_on"' in regime_context
    assert '"confidence": 0.61' in regime_context
    assert '"source": "regime_history_earliest"' in regime_context


def test_get_symbol_price_at_date_prefers_canonical_feature(mock_engine) -> None:
    store = AstroGridStore(mock_engine)
    mock_conn = mock_engine.connect.return_value.__enter__.return_value

    def _execute_side_effect(statement, params=None):
        sql_text = str(statement)
        result = MagicMock()
        if "FROM feature_registry fr" in sql_text and "JOIN resolved_series rs" in sql_text:
            result.fetchone.return_value = (123.45,)
            return result
        raise AssertionError(f"Unexpected SQL executed: {sql_text}")

    mock_conn.execute.side_effect = _execute_side_effect

    price = store._get_symbol_price_at_date("BTC", date(2026, 3, 29))

    assert price == 123.45


def test_run_learning_loop_retries_backtest_with_scored_date_range() -> None:
    store = AstroGridStore(MagicMock())
    store.score_predictions = MagicMock(return_value={"scored": 3})
    store.run_backtests = MagicMock(
        side_effect=[
            {"count": 3, "runs": [{"strategy_variant": "grid_only", "summary": {"total_predictions": 0}}]},
            {"count": 3, "runs": [{"strategy_variant": "grid_only", "summary": {"total_predictions": 3}}]},
        ]
    )
    store._scored_prediction_date_range = MagicMock(return_value=(date(2024, 1, 1), date(2024, 5, 13)))
    store.generate_review_run = MagicMock(return_value={"review_key": "review-1"})

    result = store.run_learning_loop(as_of_date=date(2026, 3, 29))

    assert store.run_backtests.call_count == 2
    first_call = store.run_backtests.call_args_list[0].kwargs
    second_call = store.run_backtests.call_args_list[1].kwargs
    assert first_call["window_start"] == date(2025, 9, 30)
    assert first_call["window_end"] == date(2026, 3, 29)
    assert second_call["window_start"] == date(2024, 1, 1)
    assert second_call["window_end"] == date(2024, 5, 13)
    assert result["backtest"]["runs"][0]["summary"]["total_predictions"] == 3


def test_summarize_backtest_metrics_includes_regime_and_group_slices(mock_engine) -> None:
    store = AstroGridStore(mock_engine)

    summary = store._summarize_backtest_metrics(
        [
            {"verdict": "hit", "signed_return": 0.08, "signed_alpha": 0.03, "regime": "risk_on", "target_group": "crypto"},
            {"verdict": "miss", "signed_return": -0.04, "signed_alpha": -0.01, "regime": "risk_on", "target_group": "crypto"},
            {"verdict": "partial", "signed_return": 0.02, "signed_alpha": 0.01, "regime": "neutral", "target_group": "equity"},
        ]
    )

    assert summary["total_predictions"] == 3
    assert summary["by_regime"]["risk_on"]["total_predictions"] == 2
    assert summary["by_regime"]["neutral"]["partials"] == 1
    assert summary["by_group"]["crypto"]["hits"] == 1
    assert summary["by_group"]["equity"]["accuracy"] == 0.5
    assert summary["dominant_regime"] == "risk_on"
    assert summary["dominant_group"] == "crypto"


def test_attribution_mystical_uses_available_snapshot_signals(mock_engine) -> None:
    store = AstroGridStore(mock_engine)

    labels = store._attribution_mystical(
        {
            "seer": {"prediction": "press the move"},
            "snapshot": {
                "lunar": {"phase_name": "Full Moon"},
                "nakshatra": {"nakshatra_name": "Magha", "pada": 3},
                "signals": {
                    "planetaryStress": 4,
                    "retrogradeCount": 2,
                    "solarGeomagneticStatus": "storm watch",
                    "nakshatraQuality": "sharp",
                },
                "void_of_course": {"is_void": True},
                "signal_field": [
                    {"key": "planetary_stress"},
                    {"key": "retrograde_pressure"},
                ],
                "retrograde_planets": [
                    {"name": "Mercury"},
                    {"name": "Saturn"},
                ],
                "canonical_ephemeris": {
                    "ephemeris_phase_bucket": 3,
                    "ephemeris_tithi_index": 12,
                    "ephemeris_hard_aspect_count": 4,
                    "ephemeris_soft_aspect_count": 1,
                },
            },
        }
    )

    assert "seer:bullish" in labels
    assert "moon:Full Moon" in labels
    assert "nakshatra:Magha" in labels
    assert "pada:3" in labels
    assert "void:active" in labels
    assert "stress:4" in labels
    assert "retrograde:2" in labels
    assert "phase_bucket:3" in labels
    assert "tithi:12" in labels
    assert "hard_aspects:4" in labels


@patch("api.routers.astrogrid.publish_astrogrid_prediction")
@patch("api.routers.astrogrid._classify_prediction_scoreability")
@patch("api.routers.astrogrid.get_astrogrid_store")
def test_create_prediction_persists_and_returns_postmortem(
    mock_store_factory,
    mock_classify_scoreability,
    mock_publish,
) -> None:
    mock_store = MagicMock()
    mock_classify_scoreability.return_value = (
        "liquid_market",
        [{"symbol": "BTC", "status": "scoreable_now", "scoreable_now": True, "reason_if_not": None}],
    )
    mock_store.save_prediction.return_value = {
        "prediction_id": "pred-1",
        "call": "press BTC",
        "scoring_class": "liquid_market",
        "timing": "now / ingress",
        "setup": "leader rotation",
        "invalidation": "break if regime flips",
        "status": "pending",
        "postmortem": {
            "state": "pending",
            "summary": "Pending swing read on BTC: press BTC. Break if regime flips.",
            "dominant_grid_drivers": ["regime:risk_on"],
            "dominant_mystical_drivers": ["moon:Full Moon"],
            "invalidation_rule": "break if regime flips",
            "feature_family_summary": {"grid": ["regime:risk_on"], "mystical": ["moon:Full Moon"]},
        },
    }
    mock_store_factory.return_value = mock_store
    mock_publish.return_value = {
        "status": "published",
        "oracle_prediction_id": "astrogrid:pred-1",
        "contract": "oracle.publish.v1",
    }

    response = client.post(
        "/api/v1/astrogrid/predictions",
        headers=_auth_header(),
        json={
            "question": "what crypto should i buy right now?",
            "call": "press BTC",
            "timing": "now / ingress",
            "setup": "leader rotation",
            "invalidation": "break if regime flips",
            "as_of_ts": "2025-01-15T12:00:00+00:00",
            "note": "keep the leash short",
            "seer": {"confidence": 0.72, "horizon": "days"},
            "snapshot": {
                "date": "2026-03-28",
                "lunar": {"phase_name": "Full Moon"},
                "nakshatra": {"nakshatra_name": "Magha", "pada": 2},
                "signals": {"planetaryStress": 3, "retrogradeCount": 1},
                "signal_field": [{"key": "planetary_stress", "name": "Planetary Stress"}],
                "void_of_course": {"is_void": True},
                "retrograde_planets": [{"name": "Mercury"}],
                "events": [{"type": "nakshatra"}],
                "canonical_ephemeris": {"ephemeris_phase_bucket": 3, "ephemeris_tithi_index": 12},
                "grid": {"solar": {"geomagnetic_kp_index": 4.2}},
            },
            "market_overlay_snapshot": {"regime": {"state": "risk_on"}},
            "engine_outputs": [{"engine_id": "western"}],
            "scoring_class": "liquid_market",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["prediction_id"] == "pred-1"
    assert data["scoring_class"] == "liquid_market"
    assert data["postmortem"]["state"] == "pending"
    assert "summary" in data["postmortem"]
    saved_payload = mock_store.save_prediction.call_args.args[0]
    assert saved_payload["as_of_ts"] == "2025-01-15T12:00:00+00:00"
    assert saved_payload["mystical_feature_payload"]["snapshot"]["signals"]["planetaryStress"] == 3
    assert saved_payload["mystical_feature_payload"]["snapshot"]["void_of_course"]["is_void"] is True
    assert saved_payload["mystical_feature_payload"]["snapshot"]["retrograde_planets"][0]["name"] == "Mercury"
    assert saved_payload["mystical_feature_payload"]["snapshot"]["canonical_ephemeris"]["ephemeris_phase_bucket"] == 3
    assert saved_payload["mystical_feature_payload"]["snapshot"]["grid"]["solar"]["geomagnetic_kp_index"] == 4.2
    mock_publish.assert_called_once()
    mock_store.save_prediction.assert_called_once()


@patch("api.routers.astrogrid.publish_astrogrid_prediction")
@patch("api.routers.astrogrid._classify_prediction_scoreability")
@patch("api.routers.astrogrid.get_astrogrid_store")
def test_create_prediction_downgrades_degraded_targets(
    mock_store_factory,
    mock_classify_scoreability,
    mock_publish,
) -> None:
    mock_store = MagicMock()
    mock_classify_scoreability.return_value = (
        "unscored_experimental",
        [{"symbol": "QQQ", "status": "degraded", "scoreable_now": False, "reason_if_not": "needs longer history"}],
    )
    mock_store.save_prediction.return_value = {
        "prediction_id": "pred-2",
        "call": "buy QQQ",
        "scoring_class": "unscored_experimental",
        "timing": "now / ingress",
        "setup": "relative strength",
        "invalidation": "break if regime flips",
        "status": "pending",
        "postmortem": {"state": "pending", "summary": "Pending review."},
    }
    mock_store_factory.return_value = mock_store
    mock_publish.return_value = {"status": "published", "oracle_prediction_id": "astrogrid:pred-2"}

    response = client.post(
        "/api/v1/astrogrid/predictions",
        headers=_auth_header(),
        json={
            "question": "Should I buy QQQ over the next month?",
            "call": "buy QQQ",
            "timing": "now / ingress",
            "setup": "relative strength",
            "invalidation": "break if regime flips",
            "market_overlay_snapshot": {},
            "target_symbols": ["QQQ"],
            "scoring_class": "liquid_market",
        },
    )

    assert response.status_code == 200
    assert response.json()["scoring_class"] == "unscored_experimental"
    saved_payload = mock_store.save_prediction.call_args.args[0]
    assert saved_payload["scoring_class"] == "unscored_experimental"
    assert saved_payload["market_overlay_snapshot"]["scorecard"]["target_statuses"][0]["status"] == "degraded"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_latest_predictions_returns_store_payload(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.list_predictions.return_value = [{"prediction_id": "pred-1"}]
    mock_store_factory.return_value = mock_store

    response = client.get("/api/v1/astrogrid/predictions/latest", headers=_auth_header())
    assert response.status_code == 200
    data = response.json()
    assert data["predictions"] == [{"prediction_id": "pred-1"}]


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_postmortems_returns_store_payload(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.list_postmortems.return_value = [{"prediction_id": "pred-1", "postmortem": {"state": "pending"}}]
    mock_store_factory.return_value = mock_store

    response = client.get("/api/v1/astrogrid/postmortems", headers=_auth_header())
    assert response.status_code == 200
    data = response.json()
    assert data["postmortems"][0]["postmortem"]["state"] == "pending"


@patch("api.routers.oracle.publish_astrogrid_prediction")
def test_oracle_publish_contract_route(mock_publish) -> None:
    mock_publish.return_value = {
        "status": "published",
        "oracle_prediction_id": "astrogrid:pred-1",
        "contract": "oracle.publish.v1",
    }
    response = client.post(
        "/api/v1/oracle/publish",
        headers=_auth_header(),
        json={
            "prediction_id": "pred-1",
            "question": "what crypto should i buy right now?",
            "call": "press BTC",
            "timing": "now / ingress",
            "invalidation": "break if regime flips",
        },
    )
    assert response.status_code == 200
    assert response.json()["contract"] == "oracle.publish.v1"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_score_predictions_route_returns_store_summary(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.score_predictions.return_value = {
        "evaluation_date": "2026-03-28",
        "candidates": 2,
        "scored": 1,
        "skipped_not_mature": 1,
        "skipped_no_price": 0,
        "verdicts": {"hit": 1, "miss": 0, "partial": 0, "invalidated": 0, "expired": 0},
        "prediction_ids": ["pred-1"],
    }
    mock_store_factory.return_value = mock_store

    response = client.post(
        "/api/v1/astrogrid/predictions/score",
        headers=_auth_header(),
        json={"as_of_date": "2026-03-28", "limit": 25},
    )
    assert response.status_code == 200
    assert response.json()["scored"] == 1
    mock_store.score_predictions.assert_called_once()


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_prediction_scoreboard_route_returns_scoreboard_and_weights(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.build_prediction_scoreboard.return_value = {"overall": {"scored": 3}}
    mock_store.ensure_active_weight_version.return_value = {"version_key": "astrogrid-v1", "status": "active"}
    mock_store_factory.return_value = mock_store

    response = client.get("/api/v1/astrogrid/predictions/scoreboard", headers=_auth_header())
    assert response.status_code == 200
    data = response.json()
    assert data["scoreboard"]["overall"]["scored"] == 3
    assert data["weights"]["version_key"] == "astrogrid-v1"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_backtest_run_route_returns_store_payload(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.run_backtests.return_value = {"runs": [{"strategy_variant": "grid_only"}], "count": 1}
    mock_store_factory.return_value = mock_store

    response = client.post(
        "/api/v1/astrogrid/backtest/run",
        headers=_auth_header(),
        json={"strategy_variants": ["grid_only"], "window_start": "2026-01-01", "window_end": "2026-03-01"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["runs"][0]["strategy_variant"] == "grid_only"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_backtest_summary_and_results_routes_use_store(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.get_backtest_summary.return_value = {"latest_by_variant": {"grid_only": {"summary": {}}}, "history": []}
    mock_store.list_backtest_results.return_value = [{"strategy_variant": "grid_only", "metrics": {"verdict": "hit"}}]
    mock_store_factory.return_value = mock_store

    summary_response = client.get("/api/v1/astrogrid/backtest/summary", headers=_auth_header())
    assert summary_response.status_code == 200
    assert "grid_only" in summary_response.json()["latest_by_variant"]

    results_response = client.get(
        "/api/v1/astrogrid/backtest/results",
        headers=_auth_header(),
        params={"strategy_variant": "grid_only"},
    )
    assert results_response.status_code == 200
    assert results_response.json()["results"][0]["strategy_variant"] == "grid_only"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_current_weights_route_returns_active_version(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.ensure_active_weight_version.return_value = {
        "version_key": "astrogrid-v1",
        "status": "active",
        "grid_weights": {"regime": 0.9},
        "mystical_weights": {"seer": 0.2},
    }
    mock_store_factory.return_value = mock_store

    response = client.get("/api/v1/astrogrid/weights/current", headers=_auth_header())
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "active"
    assert data["grid_weights"]["regime"] == 0.9


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_generate_review_route_returns_review_and_proposal(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.generate_review_run.return_value = {
        "review_key": "review-1",
        "provider_mode": "deterministic",
        "review": {"what_worked": ["GRID drivers held."]},
        "proposal": {"weight_proposal_id": "proposal-1", "status": "pending_review"},
    }
    mock_store_factory.return_value = mock_store

    response = client.post(
        "/api/v1/astrogrid/review/generate",
        headers=_auth_header(),
        json={"provider_mode": "deterministic", "prediction_limit": 50, "backtest_limit": 6},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["review_key"] == "review-1"
    assert data["proposal"]["weight_proposal_id"] == "proposal-1"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_latest_review_and_weight_proposals_routes_use_store(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.get_latest_review.return_value = {
        "review_key": "review-1",
        "review": {"reasoning_summary": "Keep mystical light."},
        "proposal": {"weight_proposal_id": "proposal-1"},
    }
    mock_store.list_weight_proposals.return_value = [{"weight_proposal_id": "proposal-1", "status": "pending_review"}]
    mock_store_factory.return_value = mock_store

    latest_response = client.get("/api/v1/astrogrid/review/latest", headers=_auth_header())
    assert latest_response.status_code == 200
    assert latest_response.json()["review_key"] == "review-1"

    proposals_response = client.get("/api/v1/astrogrid/weights/proposals", headers=_auth_header())
    assert proposals_response.status_code == 200
    assert proposals_response.json()["proposals"][0]["weight_proposal_id"] == "proposal-1"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_weight_proposal_decision_routes_use_store(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.approve_weight_proposal.return_value = {
        "weight_proposal_id": "proposal-1",
        "status": "approved",
        "approved_weight_version_key": "astrogrid-v2",
    }
    mock_store.reject_weight_proposal.return_value = {
        "weight_proposal_id": "proposal-2",
        "status": "rejected",
    }
    mock_store_factory.return_value = mock_store

    approve_response = client.post(
        "/api/v1/astrogrid/weights/proposals/proposal-1/approve",
        headers=_auth_header(),
        json={"decided_by": "operator", "notes": "Ship it."},
    )
    assert approve_response.status_code == 200
    assert approve_response.json()["status"] == "approved"

    reject_response = client.post(
        "/api/v1/astrogrid/weights/proposals/proposal-2/reject",
        headers=_auth_header(),
        json={"decided_by": "operator", "notes": "Too noisy."},
    )
    assert reject_response.status_code == 200
    assert reject_response.json()["status"] == "rejected"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_learning_loop_route_uses_store(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.run_learning_loop.return_value = {
        "evaluation_date": "2026-03-29",
        "score": {"scored": 3},
        "backtest": {"count": 3},
        "review": {"review_key": "review-1"},
    }
    mock_store_factory.return_value = mock_store

    response = client.post(
        "/api/v1/astrogrid/learning-loop/run",
        headers=_auth_header(),
        json={"as_of_date": "2026-03-29", "provider_mode": "deterministic"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["score"]["scored"] == 3
    assert data["review"]["review_key"] == "review-1"
