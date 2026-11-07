"""Real PostgreSQL contracts, restricted to an explicitly disposable local cluster.

Run --noconftest with GRID_AV_TEST_PG_ROOT=/tmp/grid-av-qualification.<suffix>.
The cluster must expose only a private Unix socket on port 55439 and contain
the separate av_coverage_test database. Never accepts a DB_URL or TCP endpoint.
"""
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from ingestion.av_transcript_plan import TranscriptPeriod
from ingestion.earnings_events_puller import EarningsEventsPuller


pytestmark = pytest.mark.integration
ROOT = os.environ.get("GRID_AV_TEST_PG_ROOT")


@pytest.fixture(scope="module")
def engine():
    if not ROOT:
        pytest.skip("explicit disposable GRID_AV_TEST_PG_ROOT required")
    root = Path(ROOT).resolve()
    if root.parent != Path("/tmp") or not root.name.startswith("grid-av-qualification."):
        pytest.fail("Refusing non-disposable PostgreSQL test root")
    socket_dir = root / "socket"
    if not (root / "pgdata/PG_VERSION").is_file() or not (socket_dir / ".s.PGSQL.55439").is_socket():
        pytest.fail("Disposable cluster markers/private socket missing")
    eng = create_engine(URL.create("postgresql+psycopg2", database="av_coverage_test",
                                  query={"host": str(socket_dir), "port": "55439"}))
    with eng.begin() as conn:
        assert conn.execute(text("SELECT current_database(), inet_server_addr() IS NULL")).one() == ("av_coverage_test", True)
        assert conn.execute(text("SELECT current_setting('data_directory')")).scalar_one() == str(root / "pgdata")
        assert conn.execute(text("SELECT current_setting('listen_addresses')")).scalar_one() == ""
        # Relevant schema mirrored from production information_schema/pg_constraint
        # read-only inspection, 2026-11-07. Only this disposable DB is modified.
        conn.execute(text("""
            DROP TABLE IF EXISTS raw_series, earnings_events, source_catalog;
            CREATE TABLE IF NOT EXISTS source_catalog (
                id serial PRIMARY KEY, name text NOT NULL UNIQUE,
                active boolean NOT NULL DEFAULT true, last_pull_at timestamptz);
            CREATE TABLE IF NOT EXISTS raw_series (
                id bigserial PRIMARY KEY,
                source_id integer NOT NULL REFERENCES source_catalog(id),
                series_id text NOT NULL, obs_date date NOT NULL,
                pull_timestamp timestamptz NOT NULL DEFAULT now(),
                value double precision NOT NULL,
                pull_status text NOT NULL CHECK (pull_status IN ('SUCCESS','PARTIAL','FAILED','QUARANTINED')),
                raw_payload jsonb,
                UNIQUE(series_id,source_id,obs_date,pull_timestamp));
            CREATE TABLE IF NOT EXISTS earnings_events (
                id bigserial PRIMARY KEY, ticker text NOT NULL, cik text,
                event_type text NOT NULL CHECK (event_type IN (
                    '10-K','10-Q','8-K','EARNINGS_CALL','GUIDANCE','20-F','40-F',
                    'S-1','S-3','S-4','DEF 14A','EARNINGS_BEAT','EARNINGS_MISS')),
                filing_date date NOT NULL, period_end date, fiscal_quarter text,
                accession text, url text, title text, summary text, full_text text,
                sentiment text, confidence real, raw_payload jsonb,
                source text NOT NULL DEFAULT 'SEC_EDGAR',
                created_at timestamptz NOT NULL DEFAULT now(),
                UNIQUE (ticker,event_type,filing_date,fiscal_quarter));
        """))
    yield eng
    eng.dispose()


@pytest.fixture(autouse=True)
def no_provider_calls(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("Provider/network calls forbidden")
    monkeypatch.setattr("requests.sessions.Session.request", denied)
    monkeypatch.setattr("socket.create_connection", denied)
    monkeypatch.setattr("ingestion.earnings_events_puller.time.sleep", lambda _: None)


@pytest.fixture
def puller(engine, tmp_path, monkeypatch):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE raw_series, earnings_events, source_catalog RESTART IDENTITY"))
        conn.execute(text("INSERT INTO source_catalog (id,name) VALUES "
                          "(1,'earnings_events'),(2,'SEC_EDGAR_Fundamentals'),(3,'other')"))
    p = EarningsEventsPuller(engine, av_api_key="synthetic-test-key", av_max_calls=1,
                             av_state_path=tmp_path / "state.db")
    monkeypatch.setattr(p, "_classify_transcript_sentiment", lambda _: (None, None))
    return p


def period():
    return TranscriptPeriod("2026Q3", date(2026, 6, 27), date(2026, 7, 31), "0000000001-26-000001")


def seed_fact(engine, *, source=2, status="SUCCESS", future=False):
    p = period()
    payload = {"fiscal_year": 2026, "fiscal_period": "Q3", "period_end": p.period_end.isoformat(),
               "filed": p.filed.isoformat(), "accession": p.accession}
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO raw_series "
                          "(source_id,series_id,obs_date,pull_status,pull_timestamp,value,raw_payload) "
                          "VALUES (:source,:series,:end,:status,:observed,1.0,CAST(:payload AS jsonb))"),
                     {"source": source, "series": "sec_filed_fundamentals.AAPL.total_assets",
                      "end": p.period_end, "status": status,
                      "observed": datetime(2099, 1, 1, tzinfo=timezone.utc) if future else datetime.now(timezone.utc),
                      "payload": json.dumps(payload)})
    return p


def test_real_any_jsonb_source_status_and_time_filter(puller):
    p = seed_fact(puller.engine, source=3)
    seed_fact(puller.engine, status="FAILED")
    seed_fact(puller.engine, future=True)
    filings = [{"norm_form": "10-Q", "period": p.period_end, "filed": p.filed, "accession": p.accession}]
    assert puller._load_verified_periods("AAPL", filings) == []
    seed_fact(puller.engine)
    assert puller._load_verified_periods("AAPL", filings) == [p]
    assert puller._load_verified_periods("MSFT", filings) == []
    assert puller._load_verified_periods("AAPL", [{**filings[0], "accession": "comparison"}]) == []


def test_real_upsert_idempotency_and_verified_metadata(puller, monkeypatch):
    p = period()
    monkeypatch.setattr(puller, "fetch_av_transcript", lambda *a: {
        "symbol": "AAPL", "quarter": p.quarter, "transcript": [{"content": "synthetic transcript"}]})
    plans = {"AAPL": ("AAPL", p)}
    counts = {"AAPL": {"transcripts_inserted": 0}}
    assert puller._pull_planned_transcripts(plans, counts, {}) == (1, 1)
    assert puller._pull_planned_transcripts(plans, counts, {}) == (1, 0)
    with puller.engine.connect() as conn:
        row = conn.execute(text("SELECT count(*),min(period_end),min(filing_date) FROM earnings_events")).one()
    assert row == (1, p.period_end, p.filed)
    assert puller._stored_transcript_periods("AAPL", [p]) == ({(p.quarter, p.period_end)}, set())


def configure_pull(puller, monkeypatch):
    p = period()
    monkeypatch.setattr(puller, "_load_universe", lambda *a, **kw: ["AAPL"])
    monkeypatch.setattr(puller, "load_cik_map", lambda: {"AAPL": "0000000001"})
    monkeypatch.setattr(puller, "fetch_submissions", lambda *a: {"filings": {"recent": {
        "form": ["10-Q"], "filingDate": [p.filed.isoformat()], "reportDate": [p.period_end.isoformat()],
        "accessionNumber": [p.accession], "primaryDocument": ["synthetic.htm"]}}})
    return p


def test_full_pull_real_metadata_storage_and_second_run_skip(puller, monkeypatch):
    p = configure_pull(puller, monkeypatch)
    seed_fact(puller.engine)
    calls = []
    def fetch(ticker, quarter):
        calls.append((ticker, quarter))
        return {"symbol": ticker, "quarter": quarter, "transcript": [{"content": "synthetic text"}]}
    monkeypatch.setattr(puller, "fetch_av_transcript", fetch)
    first = puller.pull(days_back=550)
    second = puller.pull(days_back=550)
    assert first["filings_inserted"] == first["transcripts_inserted"] == 1
    assert second["filings_inserted"] == second["transcripts_inserted"] == second["av_calls"] == 0
    assert second["av_skipped_stored_periods"] == 1
    assert calls == [("AAPL", p.quarter)]
    with puller.engine.connect() as conn:
        assert conn.execute(text("SELECT last_pull_at IS NOT NULL FROM source_catalog WHERE id=1")).scalar_one()


def test_legacy_date_is_blocked_not_duplicated(puller, monkeypatch):
    p = configure_pull(puller, monkeypatch)
    seed_fact(puller.engine)
    with puller.engine.begin() as conn:
        conn.execute(text("INSERT INTO earnings_events "
                          "(ticker,event_type,filing_date,period_end,fiscal_quarter,full_text,source) "
                          "VALUES ('AAPL','EARNINGS_CALL',:end,:end,:quarter,'legacy text','ALPHAVANTAGE')"),
                     {"end": p.period_end, "quarter": p.quarter})
    result = puller.pull(days_back=550)
    assert result["av_calls"] == result["av_skipped_stored_periods"] == 0
    assert result["av_legacy_unverified_periods"] == 1
    with puller.engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM earnings_events WHERE event_type='EARNINGS_CALL'")).scalar_one() == 1


def test_empty_attempts_resume_across_real_pull_instances(puller, monkeypatch):
    p = configure_pull(puller, monkeypatch)
    seed_fact(puller.engine)
    calls = []
    resumed = EarningsEventsPuller(puller.engine, av_api_key="synthetic-test-key",
                                  av_max_calls=1, av_state_path=puller.av_state_path)
    configure_pull(resumed, monkeypatch)
    for instance in (puller, resumed):
        monkeypatch.setattr(instance, "_load_universe", lambda *a, **kw: ["AAPL", "ZZZ"])
        monkeypatch.setattr(instance, "_load_verified_periods", lambda *a: [p])
        monkeypatch.setattr(instance, "fetch_av_transcript", lambda t, q: calls.append((t, q)))
    assert puller.pull(days_back=550)["av_calls"] == 1
    assert resumed.pull(days_back=550)["av_calls"] == 1
    assert [t for t, _ in calls] == ["AAPL", "ZZZ"]
