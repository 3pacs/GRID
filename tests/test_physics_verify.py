"""Direct tests for GRID market physics verifier checks."""

from __future__ import annotations

import sys
from datetime import date, timedelta
from types import ModuleType
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from physics.verify import MarketPhysicsVerifier


AS_OF = date(2024, 6, 30)


def _sql_result(
    *, rows: list[tuple[object, ...]] | None = None, scalar: object | None = None
) -> MagicMock:
    result = MagicMock()
    result.fetchall.return_value = rows or []
    result.scalar.return_value = scalar
    return result


def _queue_sql_results(mock_engine: MagicMock, *results: MagicMock) -> MagicMock:
    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.side_effect = list(results)
    return conn


def _set_feature_registry_rows(
    mock_engine: MagicMock, rows: list[tuple[object, ...]]
) -> None:
    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = _sql_result(rows=rows)


def _install_fake_adfuller(
    monkeypatch: pytest.MonkeyPatch, adfuller: object
) -> None:
    statsmodels_module = ModuleType("statsmodels")
    tsa_module = ModuleType("statsmodels.tsa")
    stattools_module = ModuleType("statsmodels.tsa.stattools")
    stattools_module.adfuller = adfuller
    tsa_module.stattools = stattools_module
    statsmodels_module.tsa = tsa_module

    monkeypatch.setitem(sys.modules, "statsmodels", statsmodels_module)
    monkeypatch.setitem(sys.modules, "statsmodels.tsa", tsa_module)
    monkeypatch.setitem(sys.modules, "statsmodels.tsa.stattools", stattools_module)


def test_dimensional_consistency_passes_clean_feature_values(
    mock_engine: MagicMock, mock_pit_store: MagicMock
) -> None:
    verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
    _set_feature_registry_rows(
        mock_engine,
        [
            ("macro_zscore", "macro", "ZSCORE", None),
            ("sentiment_rank", "sentiment", "RANK", None),
            ("fed_funds_rate", "rates", None, None),
        ],
    )
    values = {
        "macro_zscore": 1.25,
        "sentiment_rank": 0.72,
        "fed_funds_rate": 5.25,
    }

    with patch.object(
        verifier,
        "_get_latest_value",
        side_effect=lambda feature_name, _as_of_date: values[feature_name],
    ):
        result = verifier.check_dimensional_consistency(AS_OF)

    assert result.check_name == "dimensional_consistency"
    assert result.passed is True
    assert result.score == 1.0
    assert result.details == {
        "total_features_checked": 3,
        "issues_found": 0,
    }
    assert result.warnings == []


def test_dimensional_consistency_reports_normalization_and_convention_warnings(
    mock_engine: MagicMock, mock_pit_store: MagicMock
) -> None:
    verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
    _set_feature_registry_rows(
        mock_engine,
        [
            ("extreme_macro_zscore", "macro", "ZSCORE", None),
            ("bad_sentiment_rank", "sentiment", "RANK", None),
            ("ten_year_rate", "rates", None, None),
            ("tiny_credit_spread", "spreads", None, None),
        ],
    )
    values = {
        "extreme_macro_zscore": 7.5,
        "bad_sentiment_rank": 1.2,
        "ten_year_rate": 125.0,
        "tiny_credit_spread": 0.25,
    }

    with patch.object(
        verifier,
        "_get_latest_value",
        side_effect=lambda feature_name, _as_of_date: values[feature_name],
    ):
        result = verifier.check_dimensional_consistency(AS_OF)

    assert result.check_name == "dimensional_consistency"
    assert result.passed is False
    assert result.score == 0.0
    assert result.details == {
        "total_features_checked": 4,
        "issues_found": 5,
    }
    assert len(result.warnings) == 5
    assert any("z-score=7.50" in warning for warning in result.warnings)
    assert any("rank=1.2000" in warning for warning in result.warnings)
    assert any("basis points" in warning for warning in result.warnings)
    assert any("tiny_credit_spread" in warning for warning in result.warnings)


def test_regime_boundaries_passes_persistent_ordered_transitions(
    mock_engine: MagicMock, mock_pit_store: MagicMock
) -> None:
    verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
    regimes = ["GROWTH"] * 10 + ["NEUTRAL"] * 10 + ["FRAGILE"] * 10 + ["CRISIS"] * 10
    rows = [
        (AS_OF - timedelta(days=len(regimes) - index), regime, 0.9)
        for index, regime in enumerate(regimes)
    ]
    _queue_sql_results(
        mock_engine,
        _sql_result(scalar=True),
        _sql_result(rows=rows),
    )

    result = verifier.check_regime_boundaries(AS_OF)

    assert result.check_name == "regime_boundaries"
    assert result.passed is True
    assert result.score == 1.0
    assert result.details["regime_count"] == 40
    assert result.details["direct_jumps"] == 0
    assert result.details["avg_persistence_days"] == 10.0
    assert result.details["low_confidence_pct"] == 0.0
    assert set(result.details["unique_regimes"]) == {
        "GROWTH",
        "NEUTRAL",
        "FRAGILE",
        "CRISIS",
    }
    assert result.warnings == []


def test_regime_boundaries_flags_direct_jumps_low_persistence_and_confidence(
    mock_engine: MagicMock, mock_pit_store: MagicMock
) -> None:
    verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
    regimes = ["GROWTH", "CRISIS", "GROWTH", "CRISIS", "GROWTH", "CRISIS"]
    rows = [
        (AS_OF - timedelta(days=len(regimes) - index), regime, 0.4)
        for index, regime in enumerate(regimes)
    ]
    _queue_sql_results(
        mock_engine,
        _sql_result(scalar=True),
        _sql_result(rows=rows),
    )

    result = verifier.check_regime_boundaries(AS_OF)

    assert result.check_name == "regime_boundaries"
    assert result.passed is False
    assert result.score == 0.0
    assert result.details["regime_count"] == 6
    assert result.details["direct_jumps"] == 5
    assert result.details["avg_persistence_days"] == 1.0
    assert result.details["low_confidence_pct"] == 100.0
    assert sum(
        "Direct regime jump" in warning for warning in result.warnings
    ) == 5
    assert any("Low regime persistence" in warning for warning in result.warnings)
    assert any("confidence < 50%" in warning for warning in result.warnings)


def test_stationarity_passes_when_all_tested_features_are_stationary(
    monkeypatch: pytest.MonkeyPatch,
    mock_engine: MagicMock,
    mock_pit_store: MagicMock,
) -> None:
    verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
    rows = [
        (1, "feature_one", "ZSCORE"),
        (2, "feature_two", "RANK"),
        (3, "feature_three", "LEVEL"),
    ]
    _set_feature_registry_rows(mock_engine, rows)
    mock_pit_store.get_feature_matrix.return_value = pd.DataFrame(
        {
            1: range(40),
            2: range(100, 140),
            3: range(200, 240),
        }
    )

    _install_fake_adfuller(
        monkeypatch,
        lambda _series, autolag: (None, 0.01),
    )

    result = verifier.check_stationarity(AS_OF)

    assert result.check_name == "stationarity"
    assert result.passed is True
    assert result.score == 1.0
    assert result.details == {
        "tested": 3,
        "stationary": 3,
        "non_stationary": 0,
    }
    assert result.warnings == []


def test_stationarity_fails_when_too_many_features_are_non_stationary(
    monkeypatch: pytest.MonkeyPatch,
    mock_engine: MagicMock,
    mock_pit_store: MagicMock,
) -> None:
    verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
    rows = [
        (1, "feature_one", "LEVEL"),
        (2, "feature_two", "LEVEL"),
        (3, "feature_three", "LEVEL"),
        (4, "feature_four", "ZSCORE"),
        (5, "feature_five", "RANK"),
    ]
    _set_feature_registry_rows(mock_engine, rows)
    mock_pit_store.get_feature_matrix.return_value = pd.DataFrame(
        {
            feature_id: range(feature_id * 100, feature_id * 100 + 40)
            for feature_id in range(1, 6)
        }
    )
    p_values = iter([0.2, 0.5, 0.8, 0.01, 0.02])

    def fake_adfuller(_series: pd.Series, autolag: str) -> tuple[None, float]:
        assert autolag == "AIC"
        return None, next(p_values)

    _install_fake_adfuller(monkeypatch, fake_adfuller)

    result = verifier.check_stationarity(AS_OF)

    assert result.check_name == "stationarity"
    assert result.passed is False
    assert result.score == pytest.approx(0.4)
    assert result.details == {
        "tested": 5,
        "stationary": 2,
        "non_stationary": 3,
    }
    assert len(result.warnings) == 3
    assert result.warnings[0].startswith("feature_one: ADF p=0.2000")
    assert all("Consider differencing" in warning for warning in result.warnings)
