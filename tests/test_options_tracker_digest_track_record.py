"""Track-record scoring of emailed digest calls (``trading/options_tracker.py``).

``score_contract_outcome`` is pure; ``load_digest_track_record`` reads the
``options_digest_calls`` ledger plus prices, all bounded above by ``as_of``
(PIT: a record built for date D never reads a price dated after D). The DB is
a scripted fake that answers by table name and records every bound param.
"""

from __future__ import annotations

import types
from datetime import date, datetime, timezone

import pytest

from trading import options_tracker as ot

AS_OF = date(2026, 11, 3)


# ── score_contract_outcome ──────────────────────────────────────────────────

def test_settled_put_in_the_money():
    out = ot.score_contract_outcome("PUT", 35.0, 0.79, expiry=date(2026, 10, 2), as_of=AS_OF,
                                    settle_spot=29.94)
    assert out["status"] == "SETTLED"
    assert out["value"] == pytest.approx(5.06)
    assert out["return"] == pytest.approx(5.06 / 0.79 - 1)


def test_settled_call_worthless_is_minus_one():
    out = ot.score_contract_outcome("CALL", 66.0, 0.04, expiry=date(2026, 9, 11), as_of=AS_OF,
                                    settle_spot=60.0)
    assert out == {**out, "status": "SETTLED", "return": -1.0, "value": 0.0}


def test_open_contract_marks_at_mid():
    out = ot.score_contract_outcome("PUT", 27.0, 5.10, expiry=date(2026, 12, 18), as_of=AS_OF,
                                    mark_mid=2.96)
    assert out["status"] == "OPEN" and out["return"] == pytest.approx(2.96 / 5.10 - 1)


def test_expiry_day_itself_is_still_open():
    out = ot.score_contract_outcome("PUT", 27.0, 5.10, expiry=AS_OF, as_of=AS_OF, mark_mid=1.0,
                                    settle_spot=1.0)
    assert out["status"] == "OPEN"


@pytest.mark.parametrize("kw", [
    {"expiry": date(2026, 10, 2), "settle_spot": None},
    {"expiry": date(2026, 12, 18), "mark_mid": None},
    {"expiry": date(2026, 12, 18), "mark_mid": 1.0, "entry": 0.0},
])
def test_missing_prices_are_unpriced_not_invented(kw):
    entry = kw.pop("entry", 1.0)
    out = ot.score_contract_outcome("PUT", 30.0, entry, as_of=AS_OF, **kw)
    assert out["status"] == "UNPRICED" and out["return"] is None


# ── load_digest_track_record ────────────────────────────────────────────────

def _ts(d: str) -> datetime:
    return datetime.fromisoformat(d + "T15:00:00+00:00").astimezone(timezone.utc)


class _Engine:
    """Answers the three reads by table name; records SQL + params."""

    def __init__(self, ledger, spots, marks):
        self.ledger, self.spots, self.marks = ledger, spots, marks
        self.calls: list[tuple[str, dict]] = []

    def connect(self):
        eng = self

        class _C:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params):
                sql = str(stmt)
                eng.calls.append((sql, params))
                if "FROM options_digest_calls" in sql:
                    rows = eng.ledger
                elif "FROM options_daily_signals" in sql:
                    rows = [eng.spots[params["t"]]] if params["t"] in eng.spots else []
                elif "FROM options_snapshots" in sql:
                    rows = [eng.marks[params["t"]]] if params["t"] in eng.marks else []
                else:
                    raise AssertionError(sql)
                return types.SimpleNamespace(fetchall=lambda: rows,
                                             fetchone=lambda: rows[0] if rows else None)
        return _C()


def test_track_record_dedupes_scores_and_stays_point_in_time():
    ledger = [  # newest first, as the SQL orders it
        ("BEAM", "PUT", date(2026, 12, 18), 27.0, 5.10, _ts("2026-11-02"), 5.5),
        ("VKTX", "PUT", date(2026, 10, 2), 35.0, 0.95, _ts("2026-09-24"), 6.8),  # re-email
        ("VKTX", "PUT", date(2026, 10, 2), 35.0, 0.79, _ts("2026-09-23"), 6.8),
        ("CMCSA", "CALL", date(2026, 9, 11), 28.0, 0.07, _ts("2026-09-04"), 6.6),
        ("CLDX", "CALL", date(2026, 10, 16), 35.0, 0.50, _ts("2026-10-01"), 6.1),
    ]
    spots = {"VKTX": (29.94, date(2026, 10, 2)), "CMCSA": (27.10, date(2026, 9, 11))}
    marks = {"BEAM": (2.0, 3.92, date(2026, 11, 3))}
    eng = _Engine(ledger, spots, marks)

    rec = ot.load_digest_track_record(eng, as_of=AS_OF)

    by = {c["ticker"]: c for c in rec["calls"]}
    assert [c["ticker"] for c in rec["calls"]] == ["BEAM", "CLDX", "VKTX", "CMCSA"]
    assert by["VKTX"]["entry"] == 0.79                       # first emailed quote wins
    assert by["VKTX"]["return"] == pytest.approx(5.06 / 0.79 - 1)
    assert by["CMCSA"]["return"] == -1.0
    assert by["BEAM"]["status"] == "OPEN" and by["BEAM"]["return"] == pytest.approx(2.96 / 5.10 - 1)
    assert by["CLDX"]["status"] == "UNPRICED"                # no spot near expiry
    assert rec["settled"] == 2 and rec["settled_wins"] == 1 and rec["settled_hit_rate"] == 0.5
    assert rec["open"] == 1 and rec["unpriced"] == 1

    # PIT: every price read is bounded above by as_of; settlement spots are
    # also bounded below so a dead ticker cannot settle on an old price.
    for sql, params in eng.calls:
        if "options_daily_signals" in sql:
            assert params["hi"] <= AS_OF
            assert (params["hi"] - params["lo"]).days <= ot._SETTLE_MAX_GAP_DAYS
        if "options_snapshots" in sql:
            assert params["as_of"] == AS_OF and params["o"] == "put"
        if "options_digest_calls" in sql:
            assert params["before"] == datetime(2026, 11, 4, tzinfo=timezone.utc)
        assert "UPDATE" not in sql and "INSERT" not in sql


def test_track_record_window_limits_unique_contracts():
    ledger = [(f"T{i}", "PUT", date(2026, 12, 18), 10.0, 1.0, _ts("2026-11-02"), 5.0)
              for i in range(8)]
    rec = ot.load_digest_track_record(_Engine(ledger, {}, {}), as_of=AS_OF, window=3)
    assert len(rec["calls"]) == 3
    assert rec["settled_hit_rate"] is None and rec["unpriced"] == 3
