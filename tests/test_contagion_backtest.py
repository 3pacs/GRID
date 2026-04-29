"""Tests for intelligence/contagion_backtest.py.

Scoring logic is pure (``compute_accuracy``) so it's unit-tested directly.
The DB-touching paths (``score_predictions``, ``_fetch_close_price``, the
upsert) use a FakeEngine that intercepts the parameterised SQL calls.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from intelligence.contagion_backtest import (
    SCORE_WINDOWS,
    compute_accuracy,
    score_all_windows,
    score_predictions,
)


# ── Pure math tests ─────────────────────────────────────────────────────────

def test_score_correct_direction_close_magnitude():
    # predicted -2%, actual -1.8% (ratio 0.9, in band)
    acc = compute_accuracy(-0.02, -0.018)
    assert acc > 0.8
    assert acc <= 1.0


def test_score_exact_match_is_one():
    acc = compute_accuracy(-0.05, -0.05)
    assert acc == pytest.approx(1.0)


def test_score_wrong_direction_is_zero():
    # predicted -2%, actual +3%
    assert compute_accuracy(-0.02, 0.03) == 0.0


def test_score_wrong_direction_positive_predicted():
    assert compute_accuracy(0.05, -0.04) == 0.0


def test_score_right_direction_wrong_magnitude_low():
    # predicted -5%, actual -0.5% (ratio 0.1, outside 0.5-1.5 band)
    acc = compute_accuracy(-0.05, -0.005)
    assert acc == pytest.approx(0.5)


def test_score_right_direction_wrong_magnitude_high():
    # predicted -1%, actual -10% (ratio 10, outside band, right sign)
    acc = compute_accuracy(-0.01, -0.10)
    assert acc == pytest.approx(0.5)


def test_score_within_band_interpolates():
    # ratio = 0.5 → 0.5, ratio = 1.0 → 1.0, ratio = 1.5 → 0.5
    assert compute_accuracy(-0.02, -0.01) == pytest.approx(0.5)     # ratio 0.5
    assert compute_accuracy(-0.02, -0.02) == pytest.approx(1.0)     # ratio 1.0
    assert compute_accuracy(-0.02, -0.03) == pytest.approx(0.5)     # ratio 1.5


def test_score_zero_predicted_returns_zero():
    # Skip ~zero predictions entirely.
    assert compute_accuracy(0.0, -0.05) == 0.0
    assert compute_accuracy(1e-8, -0.05) == 0.0


def test_score_zero_actual_returns_zero():
    # Actual zero move when we predicted a hit = wrong.
    assert compute_accuracy(-0.02, 0.0) == 0.0


# ── Fake engine plumbing for DB-touching tests ──────────────────────────────

@dataclass
class _Row:
    values: tuple

    def __getitem__(self, idx):
        return self.values[idx]

    def __iter__(self):
        return iter(self.values)


class _Result:
    def __init__(self, rows: list[_Row]) -> None:
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


@dataclass
class _FakePrice:
    ticker: str
    obs_date: Any
    value: float


@dataclass
class _FakeDB:
    """Shared mutable state between a FakeEngine and its FakeConns."""

    predictions: list[dict[str, Any]] = field(default_factory=list)
    prices: list[_FakePrice] = field(default_factory=list)
    upserts: list[dict[str, Any]] = field(default_factory=list)


class _FakeConn:
    def __init__(self, db: _FakeDB) -> None:
        self._db = db

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, params=None):
        sql = str(stmt).strip().lower()
        p: dict[str, Any] = dict(params or {})

        # Prediction fetch
        if "from contagion_predictions" in sql:
            days = int(p.get("days", 7))
            limit = int(p.get("limit", 500))
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(days=days)
            rows = []
            for pred in self._db.predictions:
                sim_at = pred["simulated_at"]
                already_scored = any(
                    u["pid"] == pred["id"] and u["days"] == days
                    for u in self._db.upserts
                )
                if sim_at <= cutoff and not already_scored:
                    ranked = pred.get("ranked_impact") or []
                    if not isinstance(ranked, (list, str)):
                        ranked = []
                    rows.append(_Row((pred["id"], sim_at, ranked)))
            rows.sort(key=lambda row: (row[1], row[0]))
            rows = rows[:limit]
            return _Result(rows)

        # Price fetch
        if "from raw_series" in sql:
            sid = p.get("sid", "")
            d = p.get("d")
            # sid looks like "YF:TICKER:close"
            parts = sid.split(":")
            ticker = parts[1] if len(parts) >= 2 else ""
            matches = [
                fp for fp in self._db.prices
                if fp.ticker == ticker and fp.obs_date <= d
            ]
            matches.sort(key=lambda fp: fp.obs_date, reverse=True)
            if matches:
                m = matches[0]
                return _Result([_Row((m.value, m.obs_date))])
            return _Result([])

        # Upsert into contagion_backtest_results
        if "insert into contagion_backtest_results" in sql:
            # Record params, then dedupe on (pid, ticker, days) keeping latest.
            pid = p.get("pid")
            tk = p.get("ticker")
            days = p.get("days")
            self._db.upserts = [
                u for u in self._db.upserts
                if not (u["pid"] == pid and u["ticker"] == tk and u["days"] == days)
            ]
            self._db.upserts.append(dict(p))
            return _Result([])

        return _Result([])


class _FakeEngine:
    def __init__(self, db: _FakeDB) -> None:
        self._db = db

    def connect(self):
        return _FakeConn(self._db)

    def begin(self):
        return _FakeConn(self._db)


def _make_prediction(
    pid: int,
    days_ago: int,
    ranked_impact: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "id": pid,
        "simulated_at": datetime.now(timezone.utc) - timedelta(days=days_ago),
        "ranked_impact": ranked_impact,
    }


# ── DB-touching tests ───────────────────────────────────────────────────────

def test_score_predictions_writes_rows_for_window():
    db = _FakeDB()
    db.predictions.append(
        _make_prediction(
            1,
            days_ago=7,
            ranked_impact=[
                {"id": "HSY", "margin_impact_pct": -0.04, "revenue_at_risk_usd": 1e6},
            ],
        )
    )
    start_day = (datetime.now(timezone.utc) - timedelta(days=7)).date()
    end_day = datetime.now(timezone.utc).date()
    db.prices.extend(
        [
            _FakePrice("HSY", start_day, 100.0),
            _FakePrice("HSY", end_day, 96.0),  # -4% actual, predicted -4% → 1.0
        ]
    )

    n = score_predictions(_FakeEngine(db), as_of_days_ago=7)
    assert n == 1
    assert len(db.upserts) == 1
    row = db.upserts[0]
    assert row["pid"] == 1
    assert row["ticker"] == "HSY"
    assert row["days"] == 7
    assert row["acc"] == pytest.approx(1.0)
    assert row["actual"] == pytest.approx(-0.04)


def test_score_upsert_is_idempotent():
    db = _FakeDB()
    db.predictions.append(
        _make_prediction(
            2,
            days_ago=14,
            ranked_impact=[
                {"id": "NVDA", "margin_impact_pct": -0.02},
            ],
        )
    )
    start_day = (datetime.now(timezone.utc) - timedelta(days=14)).date()
    end_day = datetime.now(timezone.utc).date()
    db.prices.extend(
        [
            _FakePrice("NVDA", start_day, 500.0),
            _FakePrice("NVDA", end_day, 490.0),
        ]
    )

    engine = _FakeEngine(db)
    score_predictions(engine, as_of_days_ago=14)
    first = list(db.upserts)
    second_count = score_predictions(engine, as_of_days_ago=14)
    second = list(db.upserts)

    # Same key, only one row total. The smarter fetch uses the result table
    # as its cursor, so the second pass has nothing new to score.
    keys = {(u["pid"], u["ticker"], u["days"]) for u in second}
    assert len(keys) == 1
    assert len(first) == len(second) == 1
    assert second_count == 0


def test_score_predictions_catches_up_older_unscored_rows():
    db = _FakeDB()
    db.predictions.append(
        _make_prediction(
            22,
            days_ago=21,
            ranked_impact=[{"id": "HSY", "margin_impact_pct": -0.02}],
        )
    )

    n = score_predictions(_FakeEngine(db), as_of_days_ago=7)

    assert n == 1
    assert db.upserts[0]["pid"] == 22
    assert db.upserts[0]["days"] == 7


def test_price_lookup_from_raw_series_uses_most_recent_before():
    db = _FakeDB()
    db.predictions.append(
        _make_prediction(
            3,
            days_ago=7,
            ranked_impact=[{"id": "AAPL", "margin_impact_pct": -0.03}],
        )
    )
    # Only a price 2 days before the simulation window — lookup should
    # find the "most recent before" row, not fail.
    old_date = (datetime.now(timezone.utc) - timedelta(days=9)).date()
    end_old_date = (datetime.now(timezone.utc) - timedelta(days=2)).date()
    db.prices.extend(
        [
            _FakePrice("AAPL", old_date, 200.0),
            _FakePrice("AAPL", end_old_date, 194.0),
        ]
    )

    n = score_predictions(_FakeEngine(db), as_of_days_ago=7)
    assert n == 1
    row = db.upserts[0]
    assert row["ps"] == 200.0
    assert row["pe"] == 194.0
    # actual move = -0.03, predicted -0.03 → 1.0
    assert row["acc"] == pytest.approx(1.0)


def test_missing_prices_writes_null_accuracy():
    db = _FakeDB()
    db.predictions.append(
        _make_prediction(
            4,
            days_ago=7,
            ranked_impact=[{"id": "TSLA", "margin_impact_pct": -0.05}],
        )
    )
    # No prices at all — scorer should still upsert a row with null values.
    n = score_predictions(_FakeEngine(db), as_of_days_ago=7)
    assert n == 1
    row = db.upserts[0]
    assert row["acc"] is None
    assert row["actual"] is None


def test_score_all_windows_runs_each_horizon():
    db = _FakeDB()
    db.predictions.extend(
        [
            _make_prediction(
                10,
                days_ago=7,
                ranked_impact=[{"id": "HSY", "margin_impact_pct": -0.02}],
            ),
            _make_prediction(
                11,
                days_ago=14,
                ranked_impact=[{"id": "HSY", "margin_impact_pct": -0.03}],
            ),
            _make_prediction(
                12,
                days_ago=30,
                ranked_impact=[{"id": "HSY", "margin_impact_pct": -0.04}],
            ),
        ]
    )
    # No price rows → accuracy NULL but rows still written.
    result = score_all_windows(_FakeEngine(db))
    assert set(result.keys()) == set(SCORE_WINDOWS)
    assert result[7] >= 1
    assert result[14] >= 1
    assert result[30] >= 1


def test_score_filters_non_ticker_nodes():
    db = _FakeDB()
    db.predictions.append(
        _make_prediction(
            20,
            days_ago=7,
            ranked_impact=[
                # Commodity seeds should be filtered out (contain underscore).
                {"id": "cocoa_beans", "margin_impact_pct": -0.10},
                # Long ids filtered
                {"id": "SOMEREALLYLONGNODE", "margin_impact_pct": -0.10},
                # Valid ticker
                {"id": "HSY", "margin_impact_pct": -0.04},
            ],
        )
    )
    n = score_predictions(_FakeEngine(db), as_of_days_ago=7)
    # Only HSY should be scored.
    assert n == 1
    assert db.upserts[0]["ticker"] == "HSY"


def test_score_handles_json_string_ranked_impact():
    """Postgres returns JSONB as dict, but some drivers emit string.
    score_predictions should handle either."""
    db = _FakeDB()
    db.predictions.append(
        {
            "id": 30,
            "simulated_at": datetime.now(timezone.utc) - timedelta(days=7),
            "ranked_impact": json.dumps(
                [{"id": "HSY", "margin_impact_pct": -0.02}]
            ),
        }
    )
    n = score_predictions(_FakeEngine(db), as_of_days_ago=7)
    assert n == 1
    assert db.upserts[0]["ticker"] == "HSY"


def test_invalid_days_raises():
    with pytest.raises(ValueError):
        score_predictions(_FakeEngine(_FakeDB()), as_of_days_ago=0)


# ── SYNTH-37: PredictionScored emission tests ───────────────────────────────

def test_score_predictions_emits_prediction_scored(monkeypatch):
    """Every scored row should emit exactly one PredictionScored contract.

    We monkeypatch ``intelligence.contagion_backtest.emit`` (the name the
    module imported) and capture each contract instance so we can assert
    on the fields relayed to downstream handlers.
    """
    from contracts.schemas import PredictionScored as _PS
    import intelligence.contagion_backtest as cbt

    captured: list[_PS] = []

    def _fake_emit(contract):
        captured.append(contract)
        return contract.event_id

    monkeypatch.setattr(cbt, "emit", _fake_emit)

    db = _FakeDB()
    db.predictions.append(
        _make_prediction(
            101,
            days_ago=7,
            ranked_impact=[
                {"id": "HSY", "margin_impact_pct": -0.04, "revenue_at_risk_usd": 1e6},
                {"id": "NVDA", "margin_impact_pct": 0.03},
            ],
        )
    )
    start_day = (datetime.now(timezone.utc) - timedelta(days=7)).date()
    end_day = datetime.now(timezone.utc).date()
    db.prices.extend(
        [
            _FakePrice("HSY", start_day, 100.0),
            _FakePrice("HSY", end_day, 96.0),   # -4% actual, -4% predicted → HIT
            _FakePrice("NVDA", start_day, 500.0),
            _FakePrice("NVDA", end_day, 515.0),  # +3% actual, +3% predicted → HIT
        ]
    )

    n = cbt.score_predictions(_FakeEngine(db), as_of_days_ago=7)
    assert n == 2
    assert len(captured) == 2, "one PredictionScored per scored row"

    # Schema envelope: every contract is a PredictionScored instance.
    for c in captured:
        assert isinstance(c, _PS)
        assert c.producer_module == "intelligence.contagion_backtest"
        assert c.decision_id == 101
        assert c.signals_used == []
        assert c.model_weights_at_prediction == {}

    by_ticker = {c.ticker: c for c in captured}
    assert set(by_ticker.keys()) == {"HSY", "NVDA"}

    hsy = by_ticker["HSY"]
    assert hsy.verdict == "HIT"
    assert hsy.expected_direction == "DOWN"
    assert hsy.realized_direction == "DOWN"
    assert hsy.confidence == pytest.approx(1.0)
    assert hsy.brier_component == pytest.approx(0.0)

    nvda = by_ticker["NVDA"]
    assert nvda.verdict == "HIT"
    assert nvda.expected_direction == "UP"
    assert nvda.realized_direction == "UP"
    assert nvda.confidence == pytest.approx(1.0)


def test_score_predictions_emit_failure_is_non_fatal(monkeypatch):
    """If the event bus / audit write raises, the scoring loop must still
    commit the backtest row and return a non-zero count. The SYNTH-37
    contract is an intelligence feedhorn, not a gate on the primary DB
    write path."""
    import intelligence.contagion_backtest as cbt

    call_count = {"n": 0}

    def _boom(contract):
        call_count["n"] += 1
        raise RuntimeError("bus is offline")

    monkeypatch.setattr(cbt, "emit", _boom)

    db = _FakeDB()
    db.predictions.append(
        _make_prediction(
            202,
            days_ago=7,
            ranked_impact=[{"id": "HSY", "margin_impact_pct": -0.02}],
        )
    )
    start_day = (datetime.now(timezone.utc) - timedelta(days=7)).date()
    end_day = datetime.now(timezone.utc).date()
    db.prices.extend(
        [
            _FakePrice("HSY", start_day, 100.0),
            _FakePrice("HSY", end_day, 98.0),
        ]
    )

    # Should NOT raise — emit failures are swallowed.
    n = cbt.score_predictions(_FakeEngine(db), as_of_days_ago=7)

    # Core scoring side-effects still land.
    assert n == 1
    assert len(db.upserts) == 1
    assert db.upserts[0]["pid"] == 202
    assert db.upserts[0]["ticker"] == "HSY"
    # Emit was actually attempted.
    assert call_count["n"] == 1
