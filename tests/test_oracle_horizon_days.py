"""Oracle predictions carry an explicit forecast horizon (LEVER-PACKAGE §5.1).

Until 2026-09-10 every oracle prediction expired at the next monthly options
expiry (35 d at most), so the 90 d / 180 d calibration buckets could never
fill and "long-horizon tailwinds" were unprovable. These tests pin:

* ``_expiry_for_horizon`` — explicit horizon → ``today + N``; ``None`` keeps
  the legacy monthly expiry.
* ``_horizon_days_for`` — the recorded horizon is the request, else derived
  from the expiry.
* ``run_cycle(horizon_days=...)`` hands the horizon to
  ``generate_predictions``.
* The ``oracle_predictions`` INSERT persists ``horizon_days`` and the schema
  guard adds the column.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from pathlib import Path

from oracle import engine as oe

ROOT = Path(__file__).resolve().parents[1]
SRC = (ROOT / "oracle" / "engine.py").read_text(encoding="utf-8")


def _bare_oracle() -> oe.OracleEngine:
    return oe.OracleEngine.__new__(oe.OracleEngine)


class TestExpiryForHorizon:
    def test_explicit_horizon_is_calendar_days_from_now(self) -> None:
        now = datetime(2026, 9, 10, 14, 30)
        o = _bare_oracle()
        assert o._expiry_for_horizon(now, 90) == date(2026, 12, 9)
        assert o._expiry_for_horizon(now, 180) == date(2027, 3, 9)

    def test_none_or_non_positive_falls_back_to_monthly_expiry(self, monkeypatch) -> None:
        o = _bare_oracle()
        monkeypatch.setattr(o, "_next_monthly_expiry", lambda: date(2026, 10, 16))
        now = datetime(2026, 9, 10)
        assert o._expiry_for_horizon(now, None) == date(2026, 10, 16)
        assert o._expiry_for_horizon(now, 0) == date(2026, 10, 16)
        assert o._expiry_for_horizon(now, -5) == date(2026, 10, 16)


class TestHorizonDaysFor:
    def test_explicit_request_wins(self) -> None:
        now = datetime(2026, 9, 10)
        assert oe.OracleEngine._horizon_days_for(now, date(2026, 10, 16), 90) == 90

    def test_derived_from_expiry_when_not_requested(self) -> None:
        now = datetime(2026, 9, 10)
        assert oe.OracleEngine._horizon_days_for(now, date(2026, 10, 16), None) == 36
        # Never below one day, even for a same-day expiry.
        assert oe.OracleEngine._horizon_days_for(now, now.date(), None) == 1


class TestRunCyclePassesHorizon:
    def test_horizon_reaches_generate_predictions(self, monkeypatch) -> None:
        o = _bare_oracle()
        o.engine = object()
        seen: dict = {}

        def fake_generate(tickers=None, horizon_days=None):
            seen["tickers"] = tickers
            seen["horizon_days"] = horizon_days
            return []

        monkeypatch.setattr(o, "score_expired_predictions", lambda: {"scored": 0})
        monkeypatch.setattr(o, "evolve_weights", lambda: {})
        monkeypatch.setattr(o, "generate_predictions", fake_generate)
        monkeypatch.setattr(o, "_get_leaderboard", lambda: [])

        result = o.run_cycle(["NVDA", "AMD"], horizon_days=90)

        assert seen == {"tickers": ["NVDA", "AMD"], "horizon_days": 90}
        assert result["new_predictions"] == 0

    def test_default_keeps_legacy_none(self, monkeypatch) -> None:
        o = _bare_oracle()
        o.engine = object()
        seen: dict = {}
        monkeypatch.setattr(o, "score_expired_predictions", lambda: {})
        monkeypatch.setattr(o, "evolve_weights", lambda: {})
        monkeypatch.setattr(
            o, "generate_predictions",
            lambda tickers=None, horizon_days=None: seen.setdefault("h", horizon_days) or [],
        )
        monkeypatch.setattr(o, "_get_leaderboard", lambda: [])
        o.run_cycle()
        assert seen["h"] is None


class TestPersistence:
    def test_prediction_dataclass_carries_horizon(self) -> None:
        p = oe.OraclePrediction(
            id="x", timestamp=datetime(2026, 9, 10), ticker="NVDA",
            prediction_type=oe.PredictionType.DIRECTION, direction="LONG",
            target_price=None, current_price=100.0,
            expiry=date(2026, 12, 9), confidence=0.6, expected_move_pct=5.0,
            horizon_days=90,
        )
        assert p.horizon_days == 90
        # Legacy construction (no horizon) still works and is explicit None.
        assert oe.OraclePrediction(
            id="y", timestamp=datetime(2026, 9, 10), ticker="NVDA",
            prediction_type=oe.PredictionType.DIRECTION, direction="LONG",
            target_price=None, current_price=100.0,
            expiry=date(2026, 10, 16), confidence=0.6, expected_move_pct=5.0,
        ).horizon_days is None

    def test_insert_persists_horizon_days(self) -> None:
        insert = SRC[SRC.index("INSERT INTO oracle_predictions"):]
        insert = insert[: insert.index("ON CONFLICT")]
        cols = re.search(r"\((.*?)\)\s*VALUES\s*\((.*?)\)", insert, re.S)
        assert cols is not None
        column_names = [c.strip() for c in cols.group(1).split(",")]
        placeholders = [v.strip() for v in cols.group(2).split(",")]
        assert "horizon_days" in column_names
        assert ":hd" in placeholders
        assert column_names.index("horizon_days") == placeholders.index(":hd")
        assert '"hd": int(p.horizon_days) if p.horizon_days is not None else None' in SRC

    def test_schema_guard_adds_column(self) -> None:
        assert "ADD COLUMN IF NOT EXISTS horizon_days INTEGER" in SRC
