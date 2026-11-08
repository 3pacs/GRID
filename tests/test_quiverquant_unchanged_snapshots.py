"""QuiverQuant: an undated snapshot identical to the previous one is not re-dated.

QuiverQuant's /live/wallstreetbets returned one snapshot (307 records) on all
133 pulls from 2026-04-03 to 2026-11-07, and /live/politicalbeta one (12
records) on every pull in the last 30 days. Their records carry no date, so
each pull stamped the same content with that day's date, and Sleuth read the
April ticker list as fresh retail sentiment. ``pull_endpoint`` now stores
nothing when an undated pull equals the previous date's rows, and reports
``unchanged_since``.

The engine is local SQLite; the API is mocked.
"""

from __future__ import annotations

import json
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from ingestion.altdata import quiverquant as qq

TODAY = date(2026, 11, 8)
WSB = qq.ENDPOINTS["wsb"]["source_type"]
SNAPSHOT = [{"Ticker": "AAON", "Count": 1, "Sentiment": 0.5719},
            {"Ticker": "gme", "Count": 12, "Sentiment": -0.2}]


def _engine(rows: list[tuple[str, str, date, dict]]):
    eng = create_engine("sqlite://", poolclass=StaticPool,
                        connect_args={"check_same_thread": False})
    with eng.begin() as c:
        c.execute(text("CREATE TABLE signal_sources (source_type TEXT, ticker TEXT, "
                       "signal_date DATE, signal_value TEXT)"))
        for st, ticker, d, value in rows:
            c.execute(text("INSERT INTO signal_sources VALUES (:s, :t, :d, :v)"),
                      {"s": st, "t": ticker, "d": d, "v": json.dumps(value)})
    return eng


def _stored(snapshot, d, source_type=WSB):
    return [(source_type, r["Ticker"].upper(), d, qq._signal_value(r)) for r in snapshot]


def test_identical_undated_snapshot_reports_its_previous_date():
    eng = _engine(_stored(SNAPSHOT, date(2026, 11, 6)) + _stored(SNAPSHOT, date(2026, 11, 7)))
    assert qq.unchanged_snapshot_since(eng, SNAPSHOT, "wsb", WSB, TODAY) == date(2026, 11, 7)


@pytest.mark.parametrize("pull", [
    [dict(SNAPSHOT[0], Sentiment=0.6), SNAPSHOT[1]],          # one value changed
    SNAPSHOT[:1],                                             # one record dropped
    SNAPSHOT + [{"Ticker": "AMC", "Count": 3, "Sentiment": 0.1}],  # one added
    SNAPSHOT + SNAPSHOT[:1],                                  # a duplicate is content too
])
def test_any_change_is_stored(pull):
    eng = _engine(_stored(SNAPSHOT, date(2026, 11, 7)))
    assert qq.unchanged_snapshot_since(eng, pull, "wsb", WSB, TODAY) is None


def test_records_with_their_own_date_are_never_suppressed():
    dated = [dict(r, Date="2026-11-07") for r in SNAPSHOT]
    eng = _engine(_stored(SNAPSHOT, date(2026, 11, 7)))
    assert qq.unchanged_snapshot_since(eng, dated, "wsb", WSB, TODAY) is None


def test_first_pull_and_other_sources_are_not_compared():
    eng = _engine(_stored(SNAPSHOT, date(2026, 11, 7), source_type="quiverquant:political_beta"))
    assert qq.unchanged_snapshot_since(eng, SNAPSHOT, "wsb", WSB, TODAY) is None
    assert qq.unchanged_snapshot_since(eng, [], "wsb", WSB, TODAY) is None


def test_todays_own_rows_are_not_the_previous_snapshot():
    # A second pull on the same day compares with the day before, not with itself.
    eng = _engine(_stored(SNAPSHOT, TODAY))
    assert qq.unchanged_snapshot_since(eng, SNAPSHOT, "wsb", WSB, TODAY) is None


def test_pull_endpoint_stores_nothing_for_an_unchanged_snapshot(monkeypatch):
    eng = _engine(_stored(SNAPSHOT, date(2026, 11, 7)))
    monkeypatch.setattr(qq, "transition_guard_blocks", lambda _k: False)
    monkeypatch.setattr(qq, "_get_api_key", lambda: "test-key")
    monkeypatch.setattr(qq, "_fetch_endpoint", lambda _p, _k: [dict(r) for r in SNAPSHOT])
    monkeypatch.setattr(qq.time, "sleep", lambda _s: None)
    monkeypatch.setattr(qq, "date", type("D", (date,), {"today": classmethod(lambda cls: TODAY)}))
    calls = []
    monkeypatch.setattr(qq, "_store_signals", lambda *a, **k: calls.append(a) or 99)

    result = qq.pull_endpoint(eng, "wsb")
    assert result == {"endpoint": "wsb", "status": "SUCCESS", "fetched": 2, "stored": 0,
                      "unchanged_since": "2026-11-07"}
    assert calls == []

    monkeypatch.setattr(qq, "_fetch_endpoint",
                        lambda _p, _k: [dict(SNAPSHOT[0], Sentiment=0.6), SNAPSHOT[1]])
    assert qq.pull_endpoint(eng, "wsb")["stored"] == 99
    assert len(calls) == 1


def test_a_failed_comparison_stores_as_before(monkeypatch):
    monkeypatch.setattr(qq, "transition_guard_blocks", lambda _k: False)
    monkeypatch.setattr(qq, "_get_api_key", lambda: "test-key")
    monkeypatch.setattr(qq, "_fetch_endpoint", lambda _p, _k: [dict(r) for r in SNAPSHOT])
    monkeypatch.setattr(qq.time, "sleep", lambda _s: None)
    monkeypatch.setattr(qq, "_store_signals", lambda *a, **k: 2)
    result = qq.pull_endpoint(object(), "wsb")  # no connect(): the check raises
    assert result["status"] == "SUCCESS" and result["stored"] == 2
    assert "unchanged_since" not in result
