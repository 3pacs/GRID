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


# ── Catalog-level pipeline status (pipeline-health) ────────────────────


def summary(*, run_h=None, rows_h=None, status="SUCCESS", rows=0, err=None, runs_7d=1):
    return sf.CatalogRunSummary(
        last_run_at=NOW - timedelta(hours=run_h) if run_h is not None else None,
        last_success_with_rows_at=NOW - timedelta(hours=rows_h) if rows_h is not None else None,
        last_status=status,
        last_rows_inserted=rows,
        last_error=err,
        runs_7d=runs_7d,
    )


def classify(run=None, write_h=None, fresh_h=48.0):
    write = NOW - timedelta(hours=write_h) if write_h is not None else None
    return sf.classify_catalog_source(now=NOW, fresh_hours=fresh_h, last_write_at=write, run=run)


@pytest.mark.parametrize(
    ("status", "rows", "err", "expected"),
    [
        ("SUCCESS", 12, None, sf.RUN_ROWS),
        ("PARTIAL", 5, "source coverage incomplete or row count unknown", sf.RUN_ROWS),
        ("SUCCESS", 0, "NO_NEW_DATA: run completed, 0 rows written", sf.RUN_NO_NEW_DATA),
        ("SUCCESS", 0, None, sf.RUN_NO_NEW_DATA),
        ("SUCCESS", 0, "SKIPPED: JQUANTS_API_KEY not configured", sf.RUN_SKIPPED),
        ("FAILED", 0, "PullerReportedFailure: no_api_key", sf.RUN_SKIPPED),
        ("FAILED", 0, "PullerReportedFailure: auth_failed", sf.RUN_SKIPPED),
        ("PARTIAL", 0, "1 of 2 items failed or partial, 0 skipped, 0 rows written: reddit not configured", sf.RUN_SKIPPED),
        ("FAILED", 0, "TIMEOUT: Exceeded 60s timeout", sf.RUN_FAILED),
        ("FAILED", 0, "PullerReportedFailure: puller did not report committed write count", sf.RUN_FAILED),
        ("PARTIAL", 0, "1 of 3 items failed or partial, 0 skipped, 0 rows written", sf.RUN_FAILED),
        ("FAILED", 0, None, sf.RUN_FAILED),
    ],
)
def test_run_outcome(status, rows, err, expected):
    assert sf.run_outcome(status, rows, err) == expected


def test_healthy_from_pull_log_rows_without_raw_series():
    # yfinance_options-style: rows land in the options tables, not raw_series.
    assert classify(summary(run_h=2, rows_h=2, rows=75994)) == sf.PIPELINE_HEALTHY


def test_healthy_from_raw_series_without_pull_log():
    assert classify(None, write_h=3) == sf.PIPELINE_HEALTHY


def test_quiet_when_latest_recent_run_found_no_new_data():
    run = summary(run_h=1, rows_h=60, err="NO_NEW_DATA: run completed, 0 rows written")
    assert classify(run) == sf.PIPELINE_QUIET


def test_quiet_without_any_rows_on_record():
    assert classify(summary(run_h=1, err="NO_NEW_DATA: x")) == sf.PIPELINE_QUIET


def test_skipped_config():
    run = summary(run_h=5, err="SKIPPED: ['no CF Radar token configured']")
    assert classify(run) == sf.PIPELINE_SKIPPED_CONFIG


def test_failing_when_latest_recent_run_failed():
    run = summary(run_h=1, rows_h=70, status="FAILED", err="TIMEOUT: Exceeded 60s timeout")
    assert classify(run) == sf.PIPELINE_FAILING


def test_failing_even_with_no_rows_ever():
    assert classify(summary(run_h=1, status="FAILED", err="boom")) == sf.PIPELINE_FAILING


def test_stopped_when_nothing_within_twice_the_window():
    assert classify(summary(run_h=200, rows_h=200, rows=4)) == sf.PIPELINE_STOPPED
    assert classify(None, write_h=200) == sf.PIPELINE_STOPPED
    # A failure long ago is "stopped", not "failing": nothing is running now.
    assert classify(summary(run_h=500, status="FAILED", err="boom")) == sf.PIPELINE_STOPPED


def test_stale_band_between_one_and_two_windows():
    assert classify(None, write_h=72) == sf.PIPELINE_STALE
    assert classify(summary(run_h=72, rows_h=72, rows=4)) == sf.PIPELINE_STALE
    # A raw write in the band with only an old run of record is still stale.
    assert classify(summary(run_h=300, status="FAILED", err="x"), write_h=72) == sf.PIPELINE_STALE


def test_inactive_when_never_ran_and_never_wrote():
    assert classify(None) == sf.PIPELINE_INACTIVE


def test_window_scales_with_cadence():
    run = summary(run_h=700, rows_h=700, rows=3)
    assert classify(run, fresh_h=1080) == sf.PIPELINE_HEALTHY  # monthly
    assert classify(run, fresh_h=168) == sf.PIPELINE_STOPPED   # weekly


def test_merge_catalog_runs_attributes_and_folds():
    catalog = {"darkpool": 3, "fred": 7, "asset_registries": 9}

    def t(hours):
        return NOW - timedelta(hours=hours)

    rows = [
        # Two pullers under one catalog row: the newer run supplies status.
        (7, "smart:fed_liquidity", t(1), None, 2, "FAILED", 0, "TIMEOUT: x"),
        (7, "FRED", t(5), t(5), 3, "SUCCESS", 40, None),
        # No source_id: smart:<entry> through the registry's catalog mapping.
        (None, "smart:dark_pool", t(2), t(2), 1, "SUCCESS", 9, None),
        # No source_id: plain name, case-insensitive.
        (None, "Asset_Registries", t(4), None, 1, "SUCCESS", 0, "NO_NEW_DATA: x"),
        # No source_id and no catalog row of that name: dropped.
        (None, "YFinance_Daily", t(1), t(1), 1, "SUCCESS", 5, None),
    ]
    merged = sf.merge_catalog_runs(rows, catalog)
    assert set(merged) == {3, 7, 9}
    fred = merged[7]
    assert (fred.last_status, fred.last_error, fred.runs_7d) == ("FAILED", "TIMEOUT: x", 5)
    assert fred.last_run_at == t(1)
    assert fred.last_success_with_rows_at == t(5)
    assert merged[3].last_rows_inserted == 9
    assert merged[9].last_status == "SUCCESS"


def test_catalog_freshness_hours():
    explicit = {"FRED": ("daily", 48)}
    assert sf.catalog_freshness_hours("FRED", explicit) == 48.0
    # dark_pool runs weekly (freq_h 168) under DARKPOOL: 2 cadences.
    assert sf.catalog_freshness_hours("DARKPOOL", explicit) == 336.0
    assert sf.catalog_freshness_hours("no_such_source", explicit) == 168.0


def test_safe_last_error_redacts_then_truncates():
    msg = "TIMEOUT: GET https://api.example.com/q?apikey=SECRET123&x=1 token=abc123 " + "y" * 900
    out = sf.safe_last_error(msg)
    assert "SECRET123" not in out and "abc123" not in out
    assert len(out) <= sf.LAST_ERROR_MAX_CHARS
    assert out.endswith("…")
    assert sf.safe_last_error(None) is None
    assert sf.safe_last_error("SKIPPED: short") == "SKIPPED: short"
