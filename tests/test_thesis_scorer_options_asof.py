"""Point-in-time cutoff, age-guard, and proxy-label tests for options-derived
models in ``analysis/thesis_scorer.py`` (``_score_dealer_gamma`` and
``_score_vanna_charm``).

All inputs are small synthetic fixtures with a fake DB connection that records
executed SQL and bound parameters (no database, no network).
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from analysis import thesis_scorer as ts


_DECISION_TS = datetime(2026, 11, 25, 15, 0, tzinfo=timezone.utc)
_DECISION_DATE = _DECISION_TS.date()


def _freeze_clock(monkeypatch: pytest.MonkeyPatch, fixed_dt: datetime) -> None:
    """Freeze thesis_scorer.datetime.now() and thesis_scorer.date.today()."""

    class _FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_dt if tz is not None else fixed_dt.replace(tzinfo=None)

    class _FakeDate(date):
        @classmethod
        def today(cls):
            return fixed_dt.date()

    monkeypatch.setattr(ts, "datetime", _FakeDatetime)
    monkeypatch.setattr(ts, "date", _FakeDate)


def _extract_params(stmt: Any, params: dict[str, Any] | None) -> dict[str, Any]:
    """Collect both execution-level params and statement-bound params."""
    merged: dict[str, Any] = {}
    bindparams = getattr(stmt, "_bindparams", None)
    if bindparams:
        for k, bp in bindparams.items():
            merged[k] = bp.value
    if params:
        merged.update(params)
    return merged


def _asof_options_engine(
    candidates: list[tuple[float, float, date, datetime | None]],
):
    """Fake Engine that records SQL + bound params and applies only the cutoffs
    actually present in the SQL statement.

    Each candidate is ``(primary_metric, spot_price, signal_date, created_at)``.
    If the query omits ``signal_date <= :<param>`` or ``created_at <= :<param>``,
    that cutoff is NOT applied — so an unconstrained query returns future or
    late-captured rows just like a real database would.
    """
    recorded: list[tuple[str, dict[str, Any]]] = []

    def execute(stmt: Any, params: dict[str, Any] | None = None):
        sql = getattr(stmt, "text", str(stmt))
        bound = _extract_params(stmt, params)
        recorded.append((sql, bound))

        eligible = list(candidates)

        date_match = re.search(r"signal_date\s*<=\s*:(\w+)", sql)
        if date_match:
            cutoff_date = bound[date_match.group(1)]
            eligible = [r for r in eligible if r[2] is not None and r[2] <= cutoff_date]

        ts_match = re.search(r"created_at\s*<=\s*:(\w+)", sql)
        if ts_match:
            cutoff_ts = bound[ts_match.group(1)]
            eligible = [
                r for r in eligible
                if r[3] is not None and r[3] <= cutoff_ts
            ]

        eligible.sort(
            key=lambda r: (
                r[2] or date.min,
                r[3] or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=True,
        )

        res = MagicMock()
        res.fetchone.return_value = eligible[0] if eligible else None
        return res

    conn = MagicMock()
    conn.execute.side_effect = execute
    engine = MagicMock()

    @contextmanager
    def _ctx():
        yield conn

    engine.connect.side_effect = _ctx
    return engine, recorded


def _bypass_engine(row: tuple[Any, ...] | None):
    """Fake Engine that ignores SQL filters and returns ``row`` verbatim.

    Used to prove Python-side age/availability guards fail closed even if a
    row bypasses SQL filtering.
    """
    recorded: list[tuple[str, dict[str, Any]]] = []

    def execute(stmt: Any, params: dict[str, Any] | None = None):
        sql = getattr(stmt, "text", str(stmt))
        recorded.append((sql, _extract_params(stmt, params)))
        res = MagicMock()
        res.fetchone.return_value = row
        return res

    conn = MagicMock()
    conn.execute.side_effect = execute
    engine = MagicMock()

    @contextmanager
    def _ctx():
        yield conn

    engine.connect.side_effect = _ctx
    return engine, recorded


# ── 1. Selection cutoff: future row, same-day late fetch, earlier fallback ─


def test_dealer_gamma_ignores_future_and_same_day_late_fetch_and_uses_earlier_fallback(monkeypatch):
    _freeze_clock(monkeypatch, _DECISION_TS)
    candidates = [
        # Future signal_date (poison: extreme complacency PCR -> bearish -30)
        (0.40, 590.0, date(2026, 11, 26), datetime(2026, 11, 26, 20, 0, tzinfo=timezone.utc)),
        # Same-day late fetch after 15:00Z decision clock (poison: bearish -30)
        (0.45, 585.0, date(2026, 11, 25), datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc)),
        # Eligible earlier fallback: Tue 2026-11-24 captured before clock (bullish PCR 1.25)
        (1.25, 580.0, date(2026, 11, 24), datetime(2026, 11, 24, 21, 0, tzinfo=timezone.utc)),
        # Older eligible row (should not beat 2026-11-24)
        (0.50, 575.0, date(2026, 11, 23), datetime(2026, 11, 23, 21, 0, tzinfo=timezone.utc)),
    ]
    engine, recorded = _asof_options_engine(candidates)

    verdict = ts._score_dealer_gamma(engine, accuracy=0.5)

    assert len(recorded) == 1
    sql, bound = recorded[0]
    assert re.search(r"signal_date\s*<=\s*:\w+", sql), "query must bind signal_date upper cutoff"
    assert re.search(r"created_at\s*<=\s*:\w+", sql), "query must bind created_at availability cutoff"
    assert str(_DECISION_DATE) not in sql, "decision date must be a bound parameter, not interpolated"
    assert _DECISION_DATE in bound.values()
    assert _DECISION_TS in bound.values()

    assert verdict["status"] == "active"
    assert verdict["direction"] == "bullish"
    assert verdict["score"] > 0
    assert "1.25" in verdict["data_point"]
    assert verdict["data_age_hours"] is not None and verdict["data_age_hours"] >= 0


def test_vanna_charm_ignores_future_and_same_day_late_fetch_and_uses_earlier_fallback(monkeypatch):
    _freeze_clock(monkeypatch, _DECISION_TS)
    candidates = [
        # Future signal_date (poison: spot well below max pain -> bullish)
        (610.0, 580.0, date(2026, 11, 26), datetime(2026, 11, 26, 20, 0, tzinfo=timezone.utc)),
        # Same-day late fetch after 15:00Z decision clock (poison: bullish)
        (605.0, 580.0, date(2026, 11, 25), datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc)),
        # Eligible earlier fallback: spot 580 vs max pain 560 (+3.4% above -> bearish)
        (560.0, 580.0, date(2026, 11, 24), datetime(2026, 11, 24, 21, 0, tzinfo=timezone.utc)),
        # Older eligible row (should not beat 2026-11-24)
        (600.0, 580.0, date(2026, 11, 23), datetime(2026, 11, 23, 21, 0, tzinfo=timezone.utc)),
    ]
    engine, recorded = _asof_options_engine(candidates)

    verdict = ts._score_vanna_charm(engine, accuracy=0.5)

    assert len(recorded) == 1
    sql, bound = recorded[0]
    assert re.search(r"signal_date\s*<=\s*:\w+", sql), "query must bind signal_date upper cutoff"
    assert re.search(r"created_at\s*<=\s*:\w+", sql), "query must bind created_at availability cutoff"
    assert str(_DECISION_DATE) not in sql, "decision date must be a bound parameter, not interpolated"
    assert _DECISION_DATE in bound.values()
    assert _DECISION_TS in bound.values()

    assert verdict["status"] == "active"
    assert verdict["direction"] == "bearish"
    assert verdict["score"] < 0
    assert "$560" in verdict["data_point"]
    assert verdict["data_age_hours"] is not None and verdict["data_age_hours"] >= 0


def test_only_future_or_late_rows_return_no_data(monkeypatch):
    _freeze_clock(monkeypatch, _DECISION_TS)
    ineligible = [
        (1.45, 580.0, date(2026, 11, 26), datetime(2026, 11, 26, 16, 0, tzinfo=timezone.utc)),
        (1.40, 580.0, date(2026, 11, 25), datetime(2026, 11, 25, 16, 0, tzinfo=timezone.utc)),
        # Missing created_at (unverifiable known-at time) must also be excluded
        (1.35, 580.0, date(2026, 11, 24), None),
    ]

    dg_engine, _ = _asof_options_engine(ineligible)
    dg = ts._score_dealer_gamma(dg_engine, accuracy=0.5)
    assert dg["status"] == "no_data"
    assert dg["score"] == 0
    assert dg["confidence"] == 0

    vc_ineligible = [
        (560.0, 580.0, date(2026, 11, 26), datetime(2026, 11, 26, 16, 0, tzinfo=timezone.utc)),
        (560.0, 580.0, date(2026, 11, 25), datetime(2026, 11, 25, 16, 0, tzinfo=timezone.utc)),
        (560.0, 580.0, date(2026, 11, 24), None),
    ]
    vc_engine, _ = _asof_options_engine(vc_ineligible)
    vc = ts._score_vanna_charm(vc_engine, accuracy=0.5)
    assert vc["status"] == "no_data"
    assert vc["score"] == 0
    assert vc["confidence"] == 0


def test_same_day_row_fetched_before_decision_clock_is_eligible(monkeypatch):
    _freeze_clock(monkeypatch, _DECISION_TS)
    dg_engine, _ = _asof_options_engine([
        (1.30, 580.0, date(2026, 11, 25), datetime(2026, 11, 25, 14, 0, tzinfo=timezone.utc)),
        (0.50, 580.0, date(2026, 11, 24), datetime(2026, 11, 24, 21, 0, tzinfo=timezone.utc)),
    ])
    dg = ts._score_dealer_gamma(dg_engine, accuracy=0.5)
    assert dg["status"] == "active"
    assert "1.30" in dg["data_point"]
    assert dg["data_age_hours"] == pytest.approx(1.0, abs=0.1)

    vc_engine, _ = _asof_options_engine([
        (560.0, 580.0, date(2026, 11, 25), datetime(2026, 11, 25, 14, 0, tzinfo=timezone.utc)),
        (600.0, 580.0, date(2026, 11, 24), datetime(2026, 11, 24, 21, 0, tzinfo=timezone.utc)),
    ])
    vc = ts._score_vanna_charm(vc_engine, accuracy=0.5)
    assert vc["status"] == "active"
    assert "$560" in vc["data_point"]
    assert vc["data_age_hours"] == pytest.approx(1.0, abs=0.1)


# ── 2. Age guard: _trading_days_age and Python-side negative/future defense ─


def test_trading_days_age_rejects_future_signal_date():
    with pytest.raises(ValueError):
        ts._trading_days_age(date(2026, 11, 26), date(2026, 11, 25))
    assert ts._trading_days_age(date(2026, 11, 25), date(2026, 11, 25)) == 0


@pytest.mark.parametrize(
    "bad_row_dg,bad_row_vc",
    [
        # Future signal_date (4-tuple)
        (
            (1.45, 580.0, date(2026, 11, 26), datetime(2026, 11, 26, 12, 0, tzinfo=timezone.utc)),
            (560.0, 580.0, date(2026, 11, 26), datetime(2026, 11, 26, 12, 0, tzinfo=timezone.utc)),
        ),
        # Future signal_date even with past created_at
        (
            (1.45, 580.0, date(2026, 11, 26), datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)),
            (560.0, 580.0, date(2026, 11, 26), datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)),
        ),
        # Future signal_date (legacy 3-tuple shape)
        (
            (1.45, 580.0, date(2026, 11, 26)),
            (560.0, 580.0, date(2026, 11, 26)),
        ),
        # Same-day row with created_at after decision clock (negative wall-clock age)
        (
            (1.45, 580.0, date(2026, 11, 25), datetime(2026, 11, 25, 18, 0, tzinfo=timezone.utc)),
            (560.0, 580.0, date(2026, 11, 25), datetime(2026, 11, 25, 18, 0, tzinfo=timezone.utc)),
        ),
        # Past signal_date with NULL created_at (unverifiable known-at)
        (
            (1.45, 580.0, date(2026, 11, 24), None),
            (560.0, 580.0, date(2026, 11, 24), None),
        ),
    ],
)
def test_scorers_fail_closed_if_future_or_negative_age_row_bypasses_sql(
    monkeypatch, bad_row_dg, bad_row_vc
):
    _freeze_clock(monkeypatch, _DECISION_TS)

    dg_eng, _ = _bypass_engine(bad_row_dg)
    dg = ts._score_dealer_gamma(dg_eng, accuracy=0.5)
    assert dg["status"] == "no_data"
    assert dg["score"] == 0
    assert dg["confidence"] == 0
    assert dg["data_age_hours"] is None
    assert "missing" not in dg["reasoning"].lower() or "created_at" in dg["reasoning"].lower()

    vc_eng, _ = _bypass_engine(bad_row_vc)
    vc = ts._score_vanna_charm(vc_eng, accuracy=0.5)
    assert vc["status"] == "no_data"
    assert vc["score"] == 0
    assert vc["confidence"] == 0
    assert vc["data_age_hours"] is None
    assert "missing" not in vc["reasoning"].lower() or "created_at" in vc["reasoning"].lower()


def test_earlier_fallback_older_than_2_trading_days_is_marked_stale(monkeypatch):
    # Wed 2026-11-25 15:00Z; future (Nov 26) and late same-day (Nov 25 21:00Z) skipped;
    # newest eligible fallback is Fri 2026-11-20 = 3 trading days old -> stale.
    _freeze_clock(monkeypatch, _DECISION_TS)
    dg_engine, _ = _asof_options_engine([
        (1.45, 580.0, date(2026, 11, 26), datetime(2026, 11, 26, 20, 0, tzinfo=timezone.utc)),
        (1.45, 580.0, date(2026, 11, 25), datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc)),
        (1.45, 580.0, date(2026, 11, 20), datetime(2026, 11, 20, 21, 0, tzinfo=timezone.utc)),
    ])
    dg = ts._score_dealer_gamma(dg_engine, accuracy=0.5)
    assert dg["status"] == "stale"
    assert dg["score"] == 0
    assert dg["confidence"] == 0
    assert dg["data_age_hours"] >= 0
    assert dg["measurement"] == "proxy:put_call_ratio"

    vc_engine, _ = _asof_options_engine([
        (560.0, 580.0, date(2026, 11, 26), datetime(2026, 11, 26, 20, 0, tzinfo=timezone.utc)),
        (560.0, 580.0, date(2026, 11, 25), datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc)),
        (560.0, 580.0, date(2026, 11, 20), datetime(2026, 11, 20, 21, 0, tzinfo=timezone.utc)),
    ])
    vc = ts._score_vanna_charm(vc_engine, accuracy=0.5)
    assert vc["status"] == "stale"
    assert vc["score"] == 0
    assert vc["confidence"] == 0
    assert vc["data_age_hours"] >= 0
    assert vc["measurement"] == "proxy:max_pain_distance"


# ── 3. Proxy labelling: stable keys + explicit proxy metadata & display name ─


def test_dealer_gamma_and_vanna_charm_label_proxies_explicitly(monkeypatch):
    _freeze_clock(monkeypatch, _DECISION_TS)
    eligible_dg = [(1.35, 580.0, date(2026, 11, 24), datetime(2026, 11, 24, 21, 0, tzinfo=timezone.utc))]
    eligible_vc = [(560.0, 580.0, date(2026, 11, 24), datetime(2026, 11, 24, 21, 0, tzinfo=timezone.utc))]

    dg_active = ts._score_dealer_gamma(_asof_options_engine(eligible_dg)[0], accuracy=0.5)
    vc_active = ts._score_vanna_charm(_asof_options_engine(eligible_vc)[0], accuracy=0.5)
    dg_empty = ts._score_dealer_gamma(_asof_options_engine([])[0], accuracy=0.5)
    vc_empty = ts._score_vanna_charm(_asof_options_engine([])[0], accuracy=0.5)

    for v in (dg_active, dg_empty):
        assert v["key"] == "dealer_gamma"
        assert "proxy" in v["name"].lower()
        assert "pcr" in v["name"].lower()
        assert v.get("measurement") == "proxy:put_call_ratio"
    assert "proxy" in dg_active["reasoning"].lower()

    for v in (vc_active, vc_empty):
        assert v["key"] == "vanna_charm"
        assert "proxy" in v["name"].lower()
        assert "max-pain" in v["name"].lower() or "max pain" in v["name"].lower()
        assert v.get("measurement") == "proxy:max_pain_distance"
    assert "proxy" in vc_active["reasoning"].lower()
