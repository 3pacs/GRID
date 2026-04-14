"""Tests for ``scripts/bootstrap_per_signal_brier.py``.

Uses mocked engines and patches ``record_scored_prediction`` so nothing
ever touches a real database. Covers the verdict→outcome mapping, all
three contribution-extraction fallback layers, summary composition, and
the replay control flow (dry-run / append / limit / errors).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from scripts import bootstrap_per_signal_brier as boot


# ── verdict_to_outcome ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "verdict, expected",
    [
        ("hit", 1.0),
        ("partial", 0.5),
        ("miss", 0.0),
        ("pending", 0.0),
        ("garbage", 0.0),
        ("", 0.0),
    ],
)
def test_verdict_to_outcome_maps_known_and_unknown(verdict, expected):
    assert boot.verdict_to_outcome(verdict) == expected


# ── extract_signal_contributions ──────────────────────────────────────────


def test_extract_layer1_direct_breakdown_passes_through():
    row = {
        "id": "p1",
        "signal_contributions": {"jodi_oil": 0.6, "fed_liquidity": 0.4},
        "model_name": "ignored",
    }
    out = boot.extract_signal_contributions(
        row, oracle_models_lookup={"ignored": ["foo", "bar"]}
    )
    assert set(out) == {"jodi_oil", "fed_liquidity"}
    assert pytest.approx(sum(out.values()), abs=1e-9) == 1.0
    assert out["jodi_oil"] > out["fed_liquidity"]


def test_extract_layer1_accepts_json_encoded_string():
    row = {
        "signal_contributions": json.dumps({"a": 1.0, "b": 1.0}),
        "model_name": None,
    }
    out = boot.extract_signal_contributions(row, oracle_models_lookup={})
    assert out == {"a": 0.5, "b": 0.5}


def test_extract_layer2_uniform_split_across_model_families():
    row = {
        "id": "p2",
        "signal_contributions": None,
        "model_name": "MOMENTUM_HUNTER",
    }
    out = boot.extract_signal_contributions(
        row,
        oracle_models_lookup={
            "MOMENTUM_HUNTER": ["equity", "flows", "breadth", "vol"],
        },
    )
    assert set(out) == {"equity", "flows", "breadth", "vol"}
    assert all(pytest.approx(v, abs=1e-9) == 0.25 for v in out.values())


def test_extract_layer3_aggregate_fallback_when_nothing_present():
    row = {"signal_contributions": None, "model_name": None}
    out = boot.extract_signal_contributions(row, oracle_models_lookup={})
    assert out == {boot.ORACLE_AGGREGATE_SOURCE: 1.0}


def test_extract_malformed_jsonb_falls_through_to_layer2():
    row = {
        "signal_contributions": "{not json",
        "model_name": "MACRO_MAESTRO",
    }
    out = boot.extract_signal_contributions(
        row,
        oracle_models_lookup={"MACRO_MAESTRO": ["rates", "credit"]},
    )
    assert out == {"rates": 0.5, "credit": 0.5}


def test_extract_empty_model_name_falls_through_to_layer3():
    row = {"signal_contributions": None, "model_name": "   "}
    out = boot.extract_signal_contributions(row, oracle_models_lookup={})
    assert out == {boot.ORACLE_AGGREGATE_SOURCE: 1.0}


def test_extract_unknown_model_name_falls_through_to_layer3():
    row = {"signal_contributions": None, "model_name": "GHOST_MODEL"}
    out = boot.extract_signal_contributions(
        row, oracle_models_lookup={"OTHER": ["rates"]}
    )
    assert out == {boot.ORACLE_AGGREGATE_SOURCE: 1.0}


def test_extract_layer1_dict_with_zero_weights_falls_through():
    row = {
        "signal_contributions": {"a": 0.0, "b": 0.0},
        "model_name": "M",
    }
    out = boot.extract_signal_contributions(
        row, oracle_models_lookup={"M": ["x", "y"]}
    )
    assert out == {"x": 0.5, "y": 0.5}


# ── _compose_summary ──────────────────────────────────────────────────────


def test_compose_summary_top10_sorted_by_count_desc():
    counts = {f"src_{i}": (20 - i) for i in range(15)}
    out = boot._compose_summary(replayed=200, signal_counts=counts)
    assert out["replayed_count"] == 200
    assert out["seeded_signals"] == 15
    assert len(out["per_signal_after_replay"]) == 10
    # Sorted descending by count
    sample_counts = [r["sample_count"] for r in out["per_signal_after_replay"]]
    assert sample_counts == sorted(sample_counts, reverse=True)
    assert out["per_signal_after_replay"][0]["signal_source"] == "src_0"


def test_compose_summary_returns_all_when_fewer_than_ten():
    counts = {"a": 3, "b": 1, "c": 2}
    out = boot._compose_summary(replayed=6, signal_counts=counts)
    assert len(out["per_signal_after_replay"]) == 3
    assert [r["signal_source"] for r in out["per_signal_after_replay"]] == [
        "a",
        "c",
        "b",
    ]


def test_compose_summary_empty_signal_counts():
    out = boot._compose_summary(replayed=0, signal_counts={})
    assert out["replayed_count"] == 0
    assert out["seeded_signals"] == 0
    assert out["per_signal_after_replay"] == []


# ── _coerce_horizon_days / _coerce_confidence ─────────────────────────────


def test_coerce_horizon_days_handles_null_and_negative():
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert boot._coerce_horizon_days({"created_at": None, "expiry": None}) == 7
    assert (
        boot._coerce_horizon_days(
            {"created_at": base, "expiry": base + timedelta(days=7)}
        )
        == 7
    )
    # Negative or zero collapses to 1.
    assert (
        boot._coerce_horizon_days(
            {"created_at": base, "expiry": base - timedelta(days=2)}
        )
        == 1
    )


def test_coerce_confidence_handles_scientific_notation_and_clamping():
    assert boot._coerce_confidence("7.5e-1") == 0.75
    assert boot._coerce_confidence(1.7) == 1.0
    assert boot._coerce_confidence(-0.2) == 0.0
    assert boot._coerce_confidence(None) is None
    assert boot._coerce_confidence("not a number") is None
    assert boot._coerce_confidence(float("nan")) is None


# ── replay_predictions (mocked I/O) ───────────────────────────────────────


def _fake_engine() -> MagicMock:
    """Build a minimal mock engine for replay_predictions tests. The
    helpers we patch (_fetch_scored_predictions etc.) bypass the real
    engine, so this only needs to be a sentinel.
    """
    return MagicMock(name="engine")


def _sample_rows() -> list[dict]:
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "id": "p1",
            "ticker": "SPY",
            "created_at": base,
            "expiry": base + timedelta(days=7),
            "confidence": 0.7,
            "verdict": "hit",
            "model_name": "MOMENTUM",
            "signals": None,
            "signal_contributions": {"alpha": 0.5, "beta": 0.5},
            "model_weights": None,
        },
        {
            "id": "p2",
            "ticker": "QQQ",
            "created_at": base,
            "expiry": base + timedelta(days=30),
            "confidence": 0.4,
            "verdict": "miss",
            "model_name": "MACRO",
            "signals": None,
            "signal_contributions": None,
            "model_weights": None,
        },
        {
            "id": "p3",
            "ticker": "IWM",
            "created_at": base,
            "expiry": base + timedelta(days=1),
            "confidence": 0.55,
            "verdict": "partial",
            "model_name": None,
            "signals": None,
            "signal_contributions": None,
            "model_weights": None,
        },
    ]


def test_replay_happy_path_calls_record_for_each_row():
    engine = _fake_engine()
    rows = _sample_rows()
    with patch.object(boot, "_fetch_scored_predictions", return_value=rows), patch.object(
        boot, "_load_oracle_models_lookup", return_value={"MACRO": ["rates", "credit"]}
    ), patch.object(boot, "_truncate_per_signal_brier_history") as trunc, patch.object(
        boot, "record_scored_prediction"
    ) as rec:
        summary = boot.replay_predictions(engine, days=30)

    assert rec.call_count == 3
    assert trunc.call_count == 1
    # Verify horizons snapped from row data
    horizons = [c.kwargs["horizon_days"] for c in rec.call_args_list]
    assert horizons == [7, 30, 1]
    # Verify outcomes
    outcomes = [c.kwargs["outcome"] for c in rec.call_args_list]
    assert outcomes == [1.0, 0.0, 0.5]
    # Layer 2 row uses the model lookup
    assert set(rec.call_args_list[1].kwargs["signal_contributions"]) == {
        "rates",
        "credit",
    }
    # Layer 3 fallback for the no-model row
    assert rec.call_args_list[2].kwargs["signal_contributions"] == {
        boot.ORACLE_AGGREGATE_SOURCE: 1.0
    }
    assert summary["replayed_count"] == 3
    assert summary["seeded_signals"] >= 3


def test_replay_dry_run_never_writes():
    engine = _fake_engine()
    rows = _sample_rows()
    with patch.object(boot, "_fetch_scored_predictions", return_value=rows), patch.object(
        boot, "_load_oracle_models_lookup", return_value={}
    ), patch.object(boot, "_truncate_per_signal_brier_history") as trunc, patch.object(
        boot, "record_scored_prediction"
    ) as rec:
        summary = boot.replay_predictions(engine, dry_run=True)

    assert rec.call_count == 0
    assert trunc.call_count == 0  # dry-run skips truncate too
    assert summary["replayed_count"] == 3
    assert summary["dry_run"] is True


def test_replay_limit_stops_after_n_rows():
    engine = _fake_engine()
    rows = _sample_rows()
    with patch.object(boot, "_fetch_scored_predictions", return_value=rows), patch.object(
        boot, "_load_oracle_models_lookup", return_value={}
    ), patch.object(boot, "_truncate_per_signal_brier_history"), patch.object(
        boot, "record_scored_prediction"
    ) as rec:
        summary = boot.replay_predictions(engine, limit=2)

    assert rec.call_count == 2
    assert summary["replayed_count"] == 2


def test_replay_default_truncates_first():
    engine = _fake_engine()
    with patch.object(boot, "_fetch_scored_predictions", return_value=[]), patch.object(
        boot, "_load_oracle_models_lookup", return_value={}
    ), patch.object(boot, "_truncate_per_signal_brier_history") as trunc, patch.object(
        boot, "record_scored_prediction"
    ):
        boot.replay_predictions(engine, append=False)

    trunc.assert_called_once_with(engine)


def test_replay_append_does_not_truncate():
    engine = _fake_engine()
    with patch.object(boot, "_fetch_scored_predictions", return_value=[]), patch.object(
        boot, "_load_oracle_models_lookup", return_value={}
    ), patch.object(boot, "_truncate_per_signal_brier_history") as trunc, patch.object(
        boot, "record_scored_prediction"
    ):
        boot.replay_predictions(engine, append=True)

    trunc.assert_not_called()


def test_truncate_swallows_db_error():
    engine = MagicMock()
    engine.begin.side_effect = RuntimeError("db down")
    # Should not raise — log only.
    boot._truncate_per_signal_brier_history(engine)


def test_replay_skips_malformed_row_continues_processing():
    engine = _fake_engine()
    rows = _sample_rows()
    # Inject a poison row in the middle whose contribution extraction throws.
    rows.insert(
        1,
        {
            "id": "POISON",
            "ticker": "BAD",
            "created_at": None,
            "expiry": None,
            "confidence": "not-a-number",
            "verdict": "hit",
            "model_name": None,
            "signals": None,
            "signal_contributions": None,
            "model_weights": None,
        },
    )
    with patch.object(boot, "_fetch_scored_predictions", return_value=rows), patch.object(
        boot, "_load_oracle_models_lookup", return_value={}
    ), patch.object(boot, "_truncate_per_signal_brier_history"), patch.object(
        boot, "record_scored_prediction"
    ) as rec:
        summary = boot.replay_predictions(engine)

    # 3 valid + 1 skipped (bad confidence)
    assert rec.call_count == 3
    assert summary["replayed_count"] == 3
    assert summary["skipped_count"] >= 1


def test_replay_skips_pending_verdict_rows():
    engine = _fake_engine()
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [
        {
            "id": "pending1",
            "ticker": "A",
            "created_at": base,
            "expiry": base + timedelta(days=7),
            "confidence": 0.6,
            "verdict": "pending",
            "model_name": None,
            "signals": None,
            "signal_contributions": None,
            "model_weights": None,
        },
    ]
    with patch.object(boot, "_fetch_scored_predictions", return_value=rows), patch.object(
        boot, "_load_oracle_models_lookup", return_value={}
    ), patch.object(boot, "_truncate_per_signal_brier_history"), patch.object(
        boot, "record_scored_prediction"
    ) as rec:
        summary = boot.replay_predictions(engine)

    assert rec.call_count == 0
    assert summary["replayed_count"] == 0
    assert summary["skipped_count"] == 1
