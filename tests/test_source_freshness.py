"""Unit tests for ingestion/source_freshness.py (pure classification + SQLite fetch)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

from ingestion import source_freshness as sf
from ingestion.source_freshness import (
    FRESH,
    NEVER,
    PARTIAL,
    STALE,
    UNKNOWN,
    PullRow,
    assess,
    assess_source,
    error_class,
    rows_written,
)

NOW = datetime(2026, 10, 14, 22, 0, tzinfo=timezone.utc)


def row(name, hours_ago, status="SUCCESS", rows=None, err=None, sid=None):
    t = NOW - timedelta(hours=hours_ago)
    return PullRow(name, sid, t, t + timedelta(minutes=1), status, rows, err)


def entry(name="fred", freq_h=12):
    return {"name": name, "freq_h": freq_h}


pytestmark = pytest.mark.unit


def test_fresh_within_two_cadences_min_48h():
    s = assess_source(entry(freq_h=4), {}, [row("smart:fred", 40, rows=5)], NOW)
    assert s.verdict == FRESH  # 40 h < max(8, 48)


def test_stale_when_last_success_is_old_and_nothing_landed():
    rows = [row("smart:fred", 100, rows=5), row("smart:fred", 10, "FAILED", err="TIMEOUT: Exceeded 300s timeout")]
    s = assess_source(entry(), {}, rows, NOW)
    assert (s.verdict, s.attempts_since_success, s.last_error_class) == (STALE, 1, "TIMEOUT")


def test_contract_failure_with_unreported_rows_is_unknown_not_stale():
    rows = [row("smart:fred", 300, rows=5),
            row("smart:fred", 5, "FAILED", err="puller did not report committed write count")]
    s = assess_source(entry(), {}, rows, NOW)
    assert s.verdict == UNKNOWN
    assert s.rows_reported_since_success is None


def test_contract_partial_that_wrote_rows_is_partial():
    msg = "29 of 400 items failed or partial, 0 skipped, 23360 rows written: unrecognized puller status NO_DATA"
    s = assess_source(entry("sec_xbrl_shares", 24), {}, [row("smart:sec_xbrl_shares", 30, "PARTIAL", err=msg)], NOW)
    assert s.verdict == PARTIAL
    assert s.rows_reported_since_success == 23360


def test_never_when_no_rows():
    assert assess_source(entry(), {}, [], NOW).verdict == NEVER


def test_matches_catalog_name_and_source_id_not_other_sources(monkeypatch):
    monkeypatch.setattr(sf, "catalog_name_for", lambda n: "FRED")
    rows = [row("FRED", 3, rows=1), row("ECB", 1, rows=1, sid=99), row("legacy_name", 2, rows=1, sid=7)]
    s = assess_source(entry(), {"fred": 7}, rows, NOW)
    assert s.verdict == FRESH and s.last_attempt == rows[2].started_at  # sid 7 matched; ECB ignored


def test_running_rows_are_ignored():
    s = assess_source(entry(), {}, [row("smart:fred", 1, "RUNNING")], NOW)
    assert s.verdict == NEVER


def test_rows_written_parses_largest_count_and_none_means_unknown():
    assert rows_written(row("x", 1, "PARTIAL", err="1 of 2 ... 21 rows written: 3 of 4 ... 5 rows written")) == 21
    assert rows_written(row("x", 1, "PARTIAL", rows=7)) == 7
    assert rows_written(row("x", 1, "FAILED", err="puller did not report committed write count")) is None


@pytest.mark.parametrize("msg,cls", [
    ("unrecognized puller status NO_CIK", "CONTRACT"),
    ("source coverage incomplete or row count unknown", "CONTRACT"),
    ("TIMEOUT: Exceeded 60s timeout", "TIMEOUT"),
    ("1 of 2 items failed or partial: reddit not configured", "CONFIG"),
    ("PullerReportedFailure: 0 of 5 items completed", "PARTIAL"),
    ("'str' object has no attribute 'connect'", "OTHER"),
    (None, None),
])
def test_error_class(msg, cls):
    assert error_class(msg) == cls


def test_assess_orders_worst_first():
    reg = [entry("a"), entry("b"), entry("c")]
    rows = [row("smart:a", 1, rows=1), row("smart:b", 500, rows=1)]
    assert [s.verdict for s in assess(reg, {}, rows, NOW)] == [STALE, NEVER, FRESH]


def test_entries_sharing_a_catalog_row_do_not_absorb_each_other(monkeypatch):
    # fred, fed_liquidity and yield_curve_full all write under source_catalog "FRED" (id 7):
    # each must see only its own smart: rows, and the shared grid-scheduler name "FRED"
    # must not be attributed to any of them.
    monkeypatch.setattr(sf, "catalog_name_for", lambda n: "FRED")
    reg = [entry("fred"), entry("fed_liquidity")]
    rows = [
        row("smart:fred", 2, rows=5, sid=7),
        row("smart:fed_liquidity", 300, rows=5, sid=7),
        row("smart:fed_liquidity", 1, "PARTIAL", err="21 rows written: unrecognized puller status NO_DATA", sid=7),
        row("FRED", 1, "FAILED", err="TIMEOUT: Exceeded 300s timeout", sid=7),
    ]
    by_name = {s.name: s for s in assess(reg, {"fred": 7}, rows, NOW)}
    assert by_name["fred"].verdict == FRESH
    assert by_name["fed_liquidity"].verdict == PARTIAL
    assert by_name["fed_liquidity"].rows_reported_since_success == 21
    assert by_name["fed_liquidity"].attempts_since_success == 1


def test_fetch_pull_history_reads_window_on_sqlite():
    eng = create_engine("sqlite://")
    recent = datetime.now(timezone.utc) - timedelta(days=1)
    old = datetime.now(timezone.utc) - timedelta(days=90)
    with eng.begin() as c:
        c.execute(text("CREATE TABLE source_catalog (id INTEGER, name TEXT)"))
        c.execute(text("CREATE TABLE pull_log (puller_name TEXT, source_id INTEGER, started_at TIMESTAMP, "
                       "completed_at TIMESTAMP, status TEXT, rows_inserted INTEGER, error_message TEXT)"))
        c.execute(text("INSERT INTO source_catalog VALUES (7, 'FRED')"))
        for t in (recent, old):
            c.execute(text("INSERT INTO pull_log VALUES ('smart:fred', 7, :t, :t, 'SUCCESS', 3, NULL)"), {"t": t})
    catalog, rows = sf.fetch_pull_history(eng, lookback_days=45)
    assert catalog == {"fred": 7}
    assert len(rows) == 1 and rows[0].puller_name == "smart:fred"
