"""Point-in-time cutoff, age-guard, and proxy-label tests for options-derived
models in ``analysis/thesis_scorer.py`` (``_score_dealer_gamma`` and
``_score_vanna_charm``).

All inputs are small synthetic fixtures with a fake DB connection that records
executed SQL and bound parameters (no database, no network).
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
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


_POISON_DG = [
    # Future signal_date (poison: extreme complacency PCR -> bearish -30)
    (0.40, 590.0, date(2026, 11, 26), datetime(2026, 11, 26, 20, 0, tzinfo=timezone.utc)),
    # Same-session row last written after the 15:00Z decision clock (poison)
    (0.45, 585.0, date(2026, 11, 25), datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc)),
    # Earlier session captured before the clock: must NOT be substituted
    (1.25, 580.0, date(2026, 11, 24), datetime(2026, 11, 24, 21, 0, tzinfo=timezone.utc)),
]
_POISON_VC = [
    (610.0, 580.0, date(2026, 11, 26), datetime(2026, 11, 26, 20, 0, tzinfo=timezone.utc)),
    (605.0, 580.0, date(2026, 11, 25), datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc)),
    (560.0, 580.0, date(2026, 11, 24), datetime(2026, 11, 24, 21, 0, tzinfo=timezone.utc)),
]


@pytest.mark.parametrize("scorer,rows,poison", [
    ("_score_dealer_gamma", _POISON_DG, ("0.40", "0.45", "1.25")),
    ("_score_vanna_charm", _POISON_VC, ("$610", "$605", "$560")),
])
def test_session_row_written_after_the_clock_is_unavailable_not_replaced(monkeypatch, scorer, rows, poison):
    """Codex HOLD on #1382 (a5bfc4ac): the newest session at or before the
    decision date was written after the clock, so its as-of value cannot be
    reproduced. The scorer says so; it does not fall back to 2026-11-24."""
    _freeze_clock(monkeypatch, _DECISION_TS)
    engine, recorded = _asof_options_engine(rows)

    verdict = getattr(ts, scorer)(engine, accuracy=0.5)

    assert len(recorded) == 1
    sql, bound = recorded[0]
    assert re.search(r"signal_date\s*<=\s*:\w+", sql), "query must bind signal_date upper cutoff"
    assert not re.search(r"created_at\s*<=", sql), "SQL must not silently fall through to an earlier session"
    assert str(_DECISION_DATE) not in sql, "decision date must be a bound parameter, not interpolated"
    assert bound["as_of_date"] == _DECISION_DATE
    assert _DECISION_TS not in bound.values()

    assert verdict["status"] == "no_data"
    assert (verdict["score"], verdict["confidence"], verdict["data_age_hours"]) == (0, 0, None)
    assert verdict["evidence_availability"] == ts.OPTIONS_WRITTEN_AFTER_CLOCK
    assert "no earlier session substituted" in verdict["reasoning"]
    for value in poison:
        assert value not in verdict["data_point"]


@pytest.mark.parametrize("scorer,rows,expect", [
    ("_score_dealer_gamma", _POISON_DG[:1] + _POISON_DG[2:], ("bullish", "1.25")),
    ("_score_vanna_charm", _POISON_VC[:1] + _POISON_VC[2:], ("bearish", "$560")),
])
def test_an_earlier_session_used_live_is_labelled_as_such(monkeypatch, scorer, rows, expect):
    """No 2026-11-25 row exists at the 15:00Z clock (EOD data): the live score
    uses 2026-11-24 and names it as an earlier session."""
    _freeze_clock(monkeypatch, _DECISION_TS)
    verdict = getattr(ts, scorer)(_asof_options_engine(rows)[0], accuracy=0.5)
    assert verdict["status"] == "active"
    assert verdict["direction"] == expect[0]
    assert expect[1] in verdict["data_point"]
    assert verdict["evidence_basis"] == "prior_session"
    assert verdict["evidence_session"] == "2026-11-24"
    assert verdict["reasoning"].startswith("Uses an earlier session's row (2026-11-24)")
    assert verdict["data_age_hours"] is not None and verdict["data_age_hours"] >= 0


@pytest.mark.parametrize("scorer", ["_score_dealer_gamma", "_score_vanna_charm"])
@pytest.mark.parametrize("offset", [timedelta(days=-30), timedelta(hours=-1), timedelta(seconds=-1),
                                    timedelta(0), timedelta(hours=2)])
def test_any_explicit_decision_clock_is_refused_as_a_replay(monkeypatch, scorer, offset):
    """Values as of an explicit clock cannot be reproduced from one mutable row
    per session, even a second ago: refused, and the table is not read."""
    _freeze_clock(monkeypatch, _DECISION_TS)
    engine, recorded = _asof_options_engine(_POISON_DG)
    verdict = getattr(ts, scorer)(engine, accuracy=0.5, as_of_ts=_DECISION_TS + offset)
    assert recorded == []
    assert verdict["status"] == "no_data"
    assert verdict["evidence_availability"] == ts.OPTIONS_REPLAY_UNAVAILABLE
    assert verdict["score"] == 0 and verdict["confidence"] == 0


def test_decision_clock_helper():
    clock, replay = ts._options_decision_clock(None)
    assert replay is False and clock.tzinfo is not None
    naive = datetime(2026, 11, 25, 15, 0)
    clock, replay = ts._options_decision_clock(naive)
    assert replay is True and clock == _DECISION_TS


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
    # Wed 2026-11-25 15:00Z; the future (Nov 26) row is skipped and no Nov 25 row
    # exists yet; the newest session is Fri 2026-11-20 = 3 trading days old -> stale.
    _freeze_clock(monkeypatch, _DECISION_TS)
    dg_engine, _ = _asof_options_engine([
        (1.45, 580.0, date(2026, 11, 26), datetime(2026, 11, 26, 20, 0, tzinfo=timezone.utc)),
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
