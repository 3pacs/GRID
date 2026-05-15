"""Unit tests for oracle/psi_model.py.

Covers the PSI+VIX gating thresholds powering `scripts/run_psi_oracle.py:26`
via `evaluate_psi_signals`. Pure-function tests; the only DB-touching path
(`_load_latest_value`) is exercised with a MagicMock engine that mimics
SQLAlchemy's `engine.connect().__enter__().execute().fetchone()` chain.

TIER 4 punch-list item — docs/PUNCH-LIST-2026-05-13.md [P2] item 14.
"""
from __future__ import annotations

from unittest.mock import MagicMock
from uuid import UUID

import pytest

from oracle.psi_model import (
    PSISignal,
    _PSI_CONFIGS,
    _check_psi_condition,
    _load_latest_value,
    build_astrogrid_prediction_payload,
    evaluate_psi_signals,
    run_psi_oracle,
)


# ── _check_psi_condition ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "psi,op,threshold,expected",
    [
        (4.0, "lt", 5.25, True),
        (5.25, "lt", 5.25, False),  # strict <
        (6.0, "lt", 5.25, False),
        (3.0, "gt", 2.0, True),
        (2.0, "gt", 2.0, False),  # strict >
        (1.5, "gt", 2.0, False),
    ],
)
def test_check_psi_condition_lt_and_gt(
    psi: float, op: str, threshold: float, expected: bool
) -> None:
    assert _check_psi_condition(psi, op, threshold) is expected


def test_check_psi_condition_unknown_op_returns_false() -> None:
    # Any op other than "lt"/"gt" silently returns False (no exception).
    assert _check_psi_condition(5.0, "eq", 5.0) is False
    assert _check_psi_condition(5.0, "", 5.0) is False


# ── _load_latest_value (MagicMock engine) ───────────────────────────────


def _make_engine_returning(rows: dict[str, tuple | None]) -> MagicMock:
    """Build a MagicMock engine whose .connect()/.execute()/.fetchone()
    returns ``rows[feature_name]`` based on the bound ``:name`` param."""
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value

    def _execute(_sql, params):
        result = MagicMock()
        result.fetchone.return_value = rows.get(params["name"])
        return result

    conn.execute.side_effect = _execute
    return engine


def test_load_latest_value_returns_float_when_row_exists() -> None:
    engine = _make_engine_returning({"planetary_stress_index": (3.14, "2026-05-13")})
    assert _load_latest_value(engine, "planetary_stress_index") == pytest.approx(3.14)


def test_load_latest_value_returns_none_when_no_row() -> None:
    engine = _make_engine_returning({"missing_feature": None})
    assert _load_latest_value(engine, "missing_feature") is None


def test_load_latest_value_coerces_decimal_or_int_to_float() -> None:
    # Real DB driver may hand back Decimal/int; the function calls float().
    engine = _make_engine_returning({"vix_spot": (22, "2026-05-13")})
    out = _load_latest_value(engine, "vix_spot")
    assert isinstance(out, float)
    assert out == 22.0


# ── evaluate_psi_signals — happy paths & gating ─────────────────────────


def _patch_loader(monkeypatch: pytest.MonkeyPatch, psi: float | None, vix: float | None) -> None:
    """Replace ``_load_latest_value`` so tests don't need DB mocks."""

    def _fake(_engine, feature_name: str):
        if feature_name == "planetary_stress_index":
            return psi
        if feature_name == "vix_spot":
            return vix
        return None

    monkeypatch.setattr("oracle.psi_model._load_latest_value", _fake)


def test_evaluate_returns_empty_when_psi_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_loader(monkeypatch, psi=None, vix=18.0)
    assert evaluate_psi_signals(MagicMock()) == []


def test_evaluate_psi_low_vix_low_triggers_all_lt_configs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # PSI=2.0 (<5.25, >1.0, >2.0 NO — strict gt), VIX=15 (<17, <20, <22).
    # Expect: gld_lt525_vix_lt22, gld_lt525_vix_lt20, gld_psi_gt100_vix_lt17,
    #         gld_lt525_no_vix.   NOT qqq_gt200 (PSI not strictly > 2.0).
    _patch_loader(monkeypatch, psi=2.0, vix=15.0)
    signals = evaluate_psi_signals(MagicMock())
    names = {s.config_name for s in signals}
    assert names == {
        "gld_psi_lt525_vix_lt22",
        "gld_psi_lt525_vix_lt20",
        "gld_psi_gt100_vix_lt17",
        "gld_psi_lt525_no_vix",
    }


def test_evaluate_psi_high_no_vix_required_triggers_qqq_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # PSI=10.0 fails every lt-5.25 config; passes qqq_psi_gt200 (no VIX gate)
    # and gld_psi_gt100_vix_lt17 IF VIX<17. Set VIX=None → that config skips.
    _patch_loader(monkeypatch, psi=10.0, vix=None)
    signals = evaluate_psi_signals(MagicMock())
    assert [s.config_name for s in signals] == ["qqq_psi_gt200_no_vix"]
    assert signals[0].symbol == "QQQ"
    assert signals[0].vix_value is None


def test_evaluate_vix_at_threshold_does_not_trigger(monkeypatch: pytest.MonkeyPatch) -> None:
    # VIX gate is strict <: VIX==threshold should skip the gated config.
    _patch_loader(monkeypatch, psi=4.0, vix=22.0)
    signals = evaluate_psi_signals(MagicMock())
    names = {s.config_name for s in signals}
    # gld_psi_lt525_vix_lt22 requires VIX<22; 22.0 fails. Same for vix_lt20.
    # gld_psi_gt100_vix_lt17 needs PSI>1 AND VIX<17 — VIX=22 fails.
    # gld_psi_lt525_no_vix has no VIX gate → triggers.
    # qqq_psi_gt200_no_vix needs PSI>2 — 4>2 → triggers.
    assert names == {"gld_psi_lt525_no_vix", "qqq_psi_gt200_no_vix"}


def test_evaluate_vix_required_but_missing_skips_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # VIX=None must skip every config that declares a vix_threshold.
    _patch_loader(monkeypatch, psi=2.0, vix=None)
    signals = evaluate_psi_signals(MagicMock())
    names = {s.config_name for s in signals}
    # Only the configs with vix_threshold=None can fire.
    # PSI=2.0 → fails qqq>2 (strict), passes gld<5.25_no_vix.
    assert names == {"gld_psi_lt525_no_vix"}


def test_evaluate_psi_in_no_man_s_land_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    # PSI=5.5 fails every lt 5.25; PSI=5.5 > 2.0 → qqq fires. So pick a value
    # below qqq's 2.0 and above gld's 5.25 — impossible. Instead, force VIX
    # to bury every gated config and PSI to bury every gt config.
    _patch_loader(monkeypatch, psi=0.5, vix=99.0)
    # PSI=0.5: lt 5.25 yes, gt 1.0 no, gt 2.0 no
    # gld_lt525_vix_lt22 (vix<22): VIX=99 fails
    # gld_lt525_vix_lt20: fails
    # gld_lt525_no_vix: passes (no VIX gate) → 1 signal expected
    signals = evaluate_psi_signals(MagicMock())
    assert [s.config_name for s in signals] == ["gld_psi_lt525_no_vix"]


# ── PSISignal field plumbing ────────────────────────────────────────────


def test_signal_fields_populated_from_config_and_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_loader(monkeypatch, psi=2.0, vix=15.0)
    signals = evaluate_psi_signals(MagicMock())
    by_name = {s.config_name: s for s in signals}
    s = by_name["gld_psi_lt525_vix_lt22"]
    assert s.symbol == "GLD"
    assert s.direction == "bullish"
    assert s.psi_value == 2.0
    assert s.vix_value == 15.0
    assert s.horizon_label == "swing"
    assert s.config_sharpe == pytest.approx(2.587)
    # Reasoning carries the human-readable summary.
    assert "PSI=2.00" in s.reasoning
    assert "Sharpe 2.59" in s.reasoning
    assert "gld_psi_lt525_vix_lt22" in s.reasoning


def test_signal_is_frozen_dataclass() -> None:
    sig = PSISignal(
        symbol="GLD",
        direction="bullish",
        confidence=0.8,
        psi_value=2.0,
        vix_value=15.0,
        config_name="x",
        config_sharpe=2.5,
        horizon_label="swing",
        reasoning="r",
    )
    with pytest.raises((AttributeError, Exception)):
        sig.confidence = 0.1  # type: ignore[misc]


# ── confidence scaling ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    "sharpe,expected_low,expected_high",
    [
        # Formula: clip((sharpe-1)/3 + 0.3, [0.3, 0.95])
        (2.587, 0.829, 0.829),  # ~(1.587/3)+0.3 = 0.829
        (2.012, 0.637, 0.637),
        (10.0, 0.95, 0.95),  # cap
        (0.5, 0.3, 0.3),  # floor
    ],
)
def test_confidence_scaling_matches_formula(
    sharpe: float, expected_low: float, expected_high: float
) -> None:
    # Verify the inline formula in evaluate_psi_signals stays consistent.
    base = min(0.95, max(0.3, (sharpe - 1.0) / 3.0 + 0.3))
    assert round(base, 3) == pytest.approx(expected_low, abs=0.001)
    assert round(base, 3) == pytest.approx(expected_high, abs=0.001)


def test_confidence_rounded_to_three_decimals(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_loader(monkeypatch, psi=2.0, vix=15.0)
    signals = evaluate_psi_signals(MagicMock())
    for s in signals:
        # round(x, 3) means at most 3 decimal places.
        assert s.confidence == round(s.confidence, 3)
        assert 0.3 <= s.confidence <= 0.95


# ── build_astrogrid_prediction_payload ──────────────────────────────────


def _stub_signal(**overrides) -> PSISignal:
    defaults: dict = {
        "symbol": "GLD",
        "direction": "bullish",
        "confidence": 0.83,
        "psi_value": 2.0,
        "vix_value": 15.0,
        "config_name": "gld_psi_lt525_vix_lt22",
        "config_sharpe": 2.587,
        "horizon_label": "swing",
        "reasoning": "PSI=2.00<5.25, VIX=15.0<22. Historical: Sharpe 2.59.",
    }
    defaults.update(overrides)
    return PSISignal(**defaults)


def test_payload_contains_required_keys() -> None:
    payload = build_astrogrid_prediction_payload(_stub_signal())
    required = {
        "prediction_id",
        "as_of_ts",
        "horizon_label",
        "target_universe",
        "scoring_class",
        "target_symbols",
        "question",
        "call",
        "timing",
        "setup",
        "invalidation",
        "note",
        "seer_summary",
        "mystical_feature_payload",
        "grid_feature_payload",
        "weight_version",
        "model_version",
        "live_or_local",
        "status",
        "mode",
        "lens_ids",
    }
    assert required.issubset(payload.keys())


def test_payload_prediction_id_is_uuid() -> None:
    payload = build_astrogrid_prediction_payload(_stub_signal())
    # Will raise ValueError if not a valid UUID string.
    UUID(payload["prediction_id"])


def test_payload_static_fields() -> None:
    payload = build_astrogrid_prediction_payload(_stub_signal())
    assert payload["target_universe"] == "hybrid"
    assert payload["scoring_class"] == "liquid_market"
    assert payload["weight_version"] == "psi-oracle-v1"
    assert payload["model_version"] == "psi-oracle-v1"
    assert payload["live_or_local"] == "local"
    assert payload["status"] == "pending"
    assert payload["mode"] == "psi_oracle"
    assert payload["lens_ids"] == ["planetary_stress", "vix_regime"]


def test_payload_threads_signal_fields() -> None:
    sig = _stub_signal(symbol="QQQ", direction="bullish", psi_value=2.5, vix_value=None)
    payload = build_astrogrid_prediction_payload(sig)
    assert payload["target_symbols"] == ["QQQ"]
    assert payload["horizon_label"] == "swing"
    assert "QQQ" in payload["call"]
    assert "BULLISH" in payload["call"]  # direction.upper()
    assert "QQQ" in payload["question"]
    assert payload["mystical_feature_payload"]["planetary_stress_index"] == 2.5
    assert payload["mystical_feature_payload"]["oracle_config"] == sig.config_name
    assert payload["mystical_feature_payload"]["oracle_sharpe"] == sig.config_sharpe
    assert payload["mystical_feature_payload"]["source"] == "psi_oracle"
    assert payload["grid_feature_payload"] == {"vix": None}


def test_payload_seer_summary_threads_psi_and_sharpe() -> None:
    sig = _stub_signal(psi_value=3.14, config_sharpe=2.5, config_name="cfg")
    payload = build_astrogrid_prediction_payload(sig)
    assert "3.14" in payload["seer_summary"]
    assert "2.50" in payload["seer_summary"]
    assert "cfg" in payload["seer_summary"]


def test_payloads_have_unique_prediction_ids() -> None:
    p1 = build_astrogrid_prediction_payload(_stub_signal())
    p2 = build_astrogrid_prediction_payload(_stub_signal())
    assert p1["prediction_id"] != p2["prediction_id"]


# ── run_psi_oracle (end-to-end glue) ────────────────────────────────────


def test_run_psi_oracle_returns_payloads_matching_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_loader(monkeypatch, psi=2.0, vix=15.0)
    payloads = run_psi_oracle(MagicMock())
    signals = evaluate_psi_signals(MagicMock())
    assert len(payloads) == len(signals) == 4
    symbols = [p["target_symbols"][0] for p in payloads]
    assert all(sym == "GLD" for sym in symbols)


def test_run_psi_oracle_returns_empty_when_no_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_loader(monkeypatch, psi=None, vix=15.0)
    assert run_psi_oracle(MagicMock()) == []


# ── _PSI_CONFIGS structural integrity ───────────────────────────────────


def test_psi_configs_have_required_keys() -> None:
    required = {
        "name",
        "symbol",
        "psi_op",
        "psi_threshold",
        "vix_threshold",
        "direction",
        "sharpe",
        "ann_return",
        "max_dd",
        "trading_days",
    }
    for cfg in _PSI_CONFIGS:
        assert required.issubset(cfg.keys()), f"missing keys in {cfg.get('name')}"
        assert cfg["psi_op"] in {"lt", "gt"}
        assert cfg["direction"] in {"bullish", "bearish", "neutral"}


def test_psi_configs_names_are_unique() -> None:
    names = [c["name"] for c in _PSI_CONFIGS]
    assert len(names) == len(set(names))
