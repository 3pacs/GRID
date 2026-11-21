"""#1051 follow-up: pre-#1051 AV transcript rows that a verified fiscal row supersedes.

Readers drop the superseded legacy copy (AV_TRANSCRIPT_NOT_SUPERSEDED_SQL); the
reconciliation script reports them and, with --apply, only appends supersession records.
The SQL runs for real on SQLite (>= 3.38 has ``->>``), with JSON text payloads.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, text

from ingestion import av_transcript_plan as plan
from scripts import reconcile_av_transcripts as recon

sqlite3.register_adapter(date, lambda d: d.isoformat())

LONG = "Operator: welcome to the call. " * 20

DDL = """
CREATE TABLE earnings_events (
    id INTEGER PRIMARY KEY, ticker TEXT NOT NULL, cik TEXT, event_type TEXT NOT NULL,
    filing_date DATE NOT NULL, period_end DATE, fiscal_quarter TEXT, accession TEXT,
    url TEXT, title TEXT, summary TEXT, full_text TEXT, sentiment TEXT, confidence REAL,
    raw_payload TEXT, source TEXT NOT NULL DEFAULT 'SEC_EDGAR',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ticker, event_type, filing_date, fiscal_quarter))
"""


def _legacy(quarter: str) -> str:
    return json.dumps({"symbol": "AAPL", "quarter": quarter, "entry_count": 40,
                       "sentiment_source": "alphavantage_average"})


def _verified(quarter: str, **over) -> str:
    p = {"symbol": "AAPL", "quarter": quarter, "entry_count": 40,
         "fiscal_period_source": "SEC_EDGAR_Fundamentals",
         "fiscal_period_accession": "0000320193-26-000077",
         "filing_date_basis": "matched_sec_filing_not_call_date",
         "observed_at": "2026-11-09T05:00:00+00:00", "sentiment_source": "alphavantage_average"}
    p.update(over)
    return json.dumps({k: v for k, v in p.items() if v is not None})


# id, ticker, event_type, filing_date, period_end, fiscal_quarter, full_text, sentiment, raw_payload, source
ROWS = [
    # AAPL fiscal 2026Q3: legacy (calendar end 09-30) + verified twin (fiscal end 06-27)
    (1, "AAPL", "EARNINGS_CALL", "2026-09-30", "2026-09-30", "2026Q3", LONG, "BULL", _legacy("2026Q3"), "ALPHAVANTAGE"),
    (2, "AAPL", "EARNINGS_CALL", "2026-08-01", "2026-06-27", "2026Q3", LONG, "BULL", _verified("2026Q3"), "ALPHAVANTAGE"),
    # AAPL 2026Q4: legacy only -> kept (still the only copy)
    (3, "AAPL", "EARNINGS_CALL", "2026-12-31", "2026-12-31", "2026Q4", LONG, "BULL", _legacy("2026Q4"), "ALPHAVANTAGE"),
    # AAPL 10-Q for fiscal Q3
    (4, "AAPL", "10-Q", "2026-08-01", "2026-06-27", "2026Q2", None, None, None, "SEC_EDGAR"),
    # MSFT 2026Q1: legacy + a twin WITHOUT accession -> not verified, legacy kept
    (5, "MSFT", "EARNINGS_CALL", "2026-09-30", "2026-09-30", "2026Q1", LONG, "BEAR", _legacy("2026Q1"), "ALPHAVANTAGE"),
    (6, "MSFT", "EARNINGS_CALL", "2026-10-29", "2026-09-30", "2026Q1", LONG, "BEAR",
     _verified("2026Q1", fiscal_period_accession=None), "ALPHAVANTAGE"),
    # NVDA 2026Q3: legacy + verified twin, but a different (EDGAR) source on the legacy -> not AV, kept
    (7, "NVDA", "EARNINGS_CALL", "2026-09-30", "2026-09-30", "2026Q3", LONG, None, _legacy("2026Q3"), "SEC_EDGAR"),
    (8, "NVDA", "EARNINGS_CALL", "2026-08-27", "2026-07-27", "2026Q3", LONG, None, _verified("2026Q3"), "ALPHAVANTAGE"),
]


@pytest.fixture
def engine():
    eng = create_engine("sqlite://")
    with eng.begin() as conn:
        conn.execute(text(DDL))
        for r in ROWS:
            conn.execute(text(
                "INSERT INTO earnings_events (id, ticker, event_type, filing_date, period_end, fiscal_quarter, "
                "full_text, sentiment, raw_payload, source, confidence) "
                "VALUES (:id, :t, :et, :fd, :pe, :fq, :ft, :s, :rp, :src, 0.8)"),
                dict(zip(("id", "t", "et", "fd", "pe", "fq", "ft", "s", "rp", "src"), r)))
    yield eng
    eng.dispose()


def _snapshot(eng):
    with eng.connect() as conn:
        return conn.execute(text("SELECT * FROM earnings_events ORDER BY id")).fetchall()


def test_fragments_are_fixed_sql_text():
    for frag in (plan.AV_VERIFIED_PROVENANCE_SQL, plan.AV_LEGACY_ROW_SQL, plan.AV_TWIN_JOIN_SQL,
                 plan.AV_TRANSCRIPT_SUPERSEDED_SQL, plan.AV_TRANSCRIPT_NOT_SUPERSEDED_SQL):
        assert "%" not in frag and "{" not in frag and ":" not in frag   # safe in psycopg %s and text() queries


def test_reader_predicate_drops_only_superseded_legacy_rows(engine):
    with engine.connect() as conn:
        kept = [r[0] for r in conn.execute(text(
            "SELECT e.id FROM earnings_events e WHERE 1 = 1" + plan.AV_TRANSCRIPT_NOT_SUPERSEDED_SQL + "ORDER BY e.id"))]
    assert kept == [2, 3, 4, 5, 6, 7, 8]          # only legacy AAPL 2026Q3 (id 1) goes


def test_catalyst_aggregator_counts_the_quarter_once(engine):
    from intelligence import catalyst_aggregator as ca
    events = ca._read_earnings_events(engine, start=date(2026, 6, 1), end=date(2026, 12, 31))
    aapl = sorted(str(e.event_date) for e in events if e.ticker == "AAPL")
    # 06-27 (10-Q / verified call, deduped on ticker+date) and the legacy-only Q4; not legacy Q3's 09-30
    assert aapl == ["2026-06-27", "2026-12-31"]


class _PsycopgLikeCursor:
    """sqlite3 cursor that accepts psycopg's %s placeholders."""

    def __init__(self, eng):
        self.raw = eng.raw_connection()
        self.cur = self.raw.cursor()
        self.sql: list[str] = []

    def execute(self, sql, params=()):
        self.sql.append(sql)
        self.cur.execute(sql.replace("%s", "?"), tuple(params))

    def fetchall(self):
        return self.cur.fetchall()


def test_cross_class_filings_lane_counts_one_call_per_quarter(engine):
    from gem_hunter import cross_class as cc
    cur = _PsycopgLikeCursor(engine)
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    end = datetime(2026, 12, 31, tzinfo=timezone.utc)
    _, components = cc._score_filings(cur, "AAPL", start, end)
    assert components["n_call"] == 2                # verified Q3 + legacy-only Q4 (was 3)
    assert plan.AV_TRANSCRIPT_NOT_SUPERSEDED_SQL in cur.sql[0]


def test_gem_rules_recent_filings_carries_the_predicate():
    from gem_hunter import gem_rules as gr

    class Cur:
        def __init__(self):
            self.sql = []

        def execute(self, sql, params=None):
            self.sql.append(sql)

        def fetchall(self):
            return []

    cur = Cur()
    assert gr.earnings_proximity_gems(cur) == []
    recent = cur.sql[0].split("insider_window AS")[0]
    assert "FROM earnings_events e" in recent and plan.AV_TRANSCRIPT_NOT_SUPERSEDED_SQL in recent
    assert recent.index(plan.AV_TRANSCRIPT_NOT_SUPERSEDED_SQL) < recent.index("GROUP BY ticker")


def test_dry_run_reports_and_writes_nothing(engine):
    before = _snapshot(engine)
    report = recon.reconcile(engine)
    assert report["mode"] == "dry_run" and report["written"] == 0 and report["count"] == 1
    [entry] = report["superseded"]
    assert entry["legacy_event_id"] == 1 and entry["superseded_by_event_id"] == 2
    assert entry["legacy_period_end"] == "2026-09-30" and entry["verified_period_end"] == "2026-06-27"
    assert entry["same_text"] is True and entry["fiscal_period_accession"] == "0000320193-26-000077"
    assert _snapshot(engine) == before
    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT count(*) FROM sqlite_master WHERE name = 'earnings_event_supersessions'")).scalar() == 0


def test_apply_only_appends_supersession_records_and_is_idempotent(engine):
    before = _snapshot(engine)
    first = recon.reconcile(engine, apply=True, run_tag="av-recon-20261121")
    assert first["written"] == 1 and first["already_recorded"] == 0
    again = recon.reconcile(engine, apply=True, run_tag="av-recon-rerun")
    assert again["written"] == 0 and again["already_recorded"] == 1
    assert _snapshot(engine) == before                 # earnings_events untouched
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT legacy_event_id, superseded_by_event_id, ticker, fiscal_quarter, run_tag "
            "FROM earnings_event_supersessions")).fetchall()
    assert rows == [(1, 2, "AAPL", "2026Q3", "av-recon-20261121")]   # first record kept, never rewritten


def test_ticker_filter_and_input_validation(engine):
    assert recon.reconcile(engine, ticker="MSFT")["count"] == 0
    assert recon.reconcile(engine, ticker="AAPL")["count"] == 1
    with pytest.raises(ValueError):
        recon.reconcile(engine, ticker="aapl; drop table x")
    with pytest.raises(ValueError):
        recon.reconcile(engine, apply=True)               # no run tag
    with pytest.raises(ValueError):
        recon.reconcile(engine, apply=True, run_tag="bad tag with spaces")


def test_two_verified_twins_pick_the_earliest_and_count_both(engine):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO earnings_events (id, ticker, event_type, filing_date, period_end, fiscal_quarter, "
            "full_text, raw_payload, source) VALUES (9, 'AAPL', 'EARNINGS_CALL', '2026-08-15', '2026-06-27', "
            "'2026Q3', :ft, :rp, 'ALPHAVANTAGE')"), {"ft": LONG + "amended", "rp": _verified("2026Q3")})
    [entry] = recon.reconcile(engine)["superseded"]
    assert entry["superseded_by_event_id"] == 2 and entry["verified_twins"] == 2
