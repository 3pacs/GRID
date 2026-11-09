"""gov_contracts rows the 2026-11-07 fiscal re-date could not move.

``scripts/qq_gov_contracts_redate.py`` moved 22,693 ``quiverquant:gov_contracts``
rows from calendar to fiscal quarter ends and left the ones that collide on the
unique key: stale copies of a quarter whose fiscal row exists (FY26Q3 at
2026-09-30 beside its 2026-06-30 row; FY26Q4 at 2026-12-31; FY27Q1 at 2027-03-31)
and quarters whose fiscal slot another row holds. On 2026-11-09 there were 148,
67 of them future-dated. ``is_calendar_dated_leftover`` names them;
``trust_scorer`` neither scores them nor counts their old outcomes, and the
money-flow corporate layer does not sum them.

Sleuth's intersection leads never see any ``quiverquant:gov_contracts`` row:
signal_type ``gov_contracts`` has no direction there. The test below pins that,
so a change to it has to face these rows.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pytest

from ingestion.altdata.quiverquant_identity import (
    calendar_dated_leftover_sql,
    is_calendar_dated_leftover,
)
from intelligence import sleuth_intersections as si
from intelligence import trust_scorer
from tests.test_trust_scorer_options_scoring import (  # noqa: F401
    _engine_for,
    _row,
    _ScorerConn,
    prices,
)

QQ = "quiverquant:gov_contracts"


def _p(year: int, qtr: int, amount: float = 1e6) -> dict:
    return {"Ticker": "AZTA", "Year": year, "Qtr": qtr, "Amount": amount}


# (signal_date, payload, leftover?) — the live classes of 2026-11-09 and their neighbours.
CASES = [
    (date(2026, 9, 30), _p(2026, 3), True),     # stale FY26Q3 copy; FY26Q3's row is at 06-30
    (date(2026, 6, 30), _p(2026, 3), False),    # FY26Q3 at its fiscal end
    (date(2026, 9, 30), _p(2026, 4), False),    # FY26Q4 at its fiscal end
    (date(2026, 12, 31), _p(2026, 4), True),    # FY26Q4 one quarter late, in the future
    (date(2026, 12, 31), _p(2027, 1), False),   # the running FY27Q1, at its fiscal end
    (date(2027, 3, 31), _p(2027, 1), True),     # stale FY27Q1 copy
    (date(2026, 3, 31), _p(2026, 1), True),     # Q1: calendar end 03-31, fiscal end the previous 12-31
    (date(2025, 12, 31), _p(2026, 1), False),
    (date(2026, 8, 14), _p(2026, 4), False),    # pre-#694 daily snapshot, dated the pull day
    (date(2026, 9, 30), {"Year": 2026}, False),  # no quarter
    (date(2026, 9, 30), {"Year": "x", "Qtr": 3}, False),
    (date(2026, 9, 30), {"Year": 2026, "Qtr": 5}, False),
    (date(2026, 9, 30), {}, False),
]


@pytest.mark.parametrize("signal_date,payload,expected", CASES)
def test_leftover_is_the_calendar_end_that_is_not_the_fiscal_end(signal_date, payload, expected):
    assert is_calendar_dated_leftover(signal_date, payload) is expected
    assert is_calendar_dated_leftover(signal_date, json.dumps(payload)) is expected


def test_no_date_or_payload_is_not_a_leftover():
    assert is_calendar_dated_leftover(None, _p(2026, 3)) is False
    assert is_calendar_dated_leftover(date(2026, 9, 30), None) is False


def test_sql_fragment_has_no_bind_or_percent():
    sql = calendar_dated_leftover_sql()
    assert ":" not in sql.replace("::", "") and "%" not in sql


# ── Sleuth intersection leads: QuiverQuant gov_contracts never votes ───────


@pytest.mark.parametrize("signal_date,payload,_", CASES)
def test_sleuth_gives_no_quiverquant_gov_row_a_vote(signal_date, payload, _):
    assert si.classify_signal(QQ, "gov_contracts", payload) is None


class _SleuthConn:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, stmt, params):
        class R:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows
        return R(self.rows)


def test_sleuth_window_keeps_usaspending_awards_and_drops_every_quiverquant_row():
    day = date(2026, 12, 31)
    rows = [(1, QQ, "qq_gov_contracts", "AZTA", day, "gov_contracts", _p(2026, 4)),
            (2, QQ, "qq_gov_contracts", "DGX", day, "gov_contracts", _p(2027, 1)),
            (3, "gov_contract", "usaspending", "BAH", day, "CONTRACT_AWARD", {"amount": 1e6})]
    obs = si.collect_signal_observations(_SleuthConn(rows), day - timedelta(days=7), day, None)
    assert [(o.ticker, o.family, o.direction) for o in obs] == [("BAH", "gov_contract", "up")]


# ── trust_scorer: never scores a leftover, never counts its old outcome ─────


TODAY = date(2026, 11, 8)


class _Today(date):
    """``trust_scorer``'s clock pinned to ``TODAY`` (rows carry datetimes)."""

    @classmethod
    def today(cls):
        return cls(TODAY.year, TODAY.month, TODAY.day)


def _gov_row(sig_id: int, ticker: str, signal_date: date, payload: dict) -> tuple:
    return _row(sig_id, QQ, "gov_contracts", payload, ticker=ticker, source_id="qq_gov_contracts",
                signal_date=datetime.combine(signal_date, datetime.min.time()))


def test_score_pending_skips_leftovers_and_still_scores_fiscal_rows(prices, monkeypatch):  # noqa: F811
    monkeypatch.setattr(trust_scorer, "date", _Today)
    window = trust_scorer.EVALUATION_WINDOWS.get(QQ, 7)
    day = date(2026, 9, 30)
    for ticker in ("AZTA", "DGX"):
        prices.table[(ticker, day)] = 100.0
        prices.table[(ticker, day + timedelta(days=window))] = 110.0
    rows = [
        _gov_row(1, "AZTA", day, _p(2026, 3)),               # stale FY26Q3 copy: would score CORRECT
        _gov_row(2, "DGX", day, _p(2026, 4)),                # FY26Q4 at its fiscal end: scored
        _gov_row(3, "AZTA", date(2026, 3, 31), _p(2026, 1)),  # > 90 days: would be EXPIRED
        _gov_row(4, "AZTA", date(2026, 12, 31), _p(2026, 4)),  # future leftover
    ]
    conn = _ScorerConn(rows)
    summary = trust_scorer.score_pending_signals(_engine_for(conn))
    assert summary["skipped_qq_gov_leftover"] == 3
    assert [(u["id"], u["outcome"]) for u in conn.outcome_updates()] == [(2, "CORRECT")]
    assert conn.expiries() == []


def test_trust_inputs_exclude_leftover_outcomes():
    for sql in (trust_scorer._LEGACY_SCORED_ROWS_SQL, trust_scorer._CORRECTED_SCORED_ROWS_SQL,
                trust_scorer._CORRECTED_NEUTRAL_COUNTS_SQL):
        assert f"source_type = '{QQ}'" in sql and "make_date" in sql
