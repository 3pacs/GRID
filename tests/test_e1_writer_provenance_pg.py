"""Real-PostgreSQL proof for the E1-V3/V4/V6 writer fixes.

Each writer the E1 gates flagged runs against the production DDL taken
verbatim from ``schema.sql`` (``source_catalog`` and ``feature_registry`` with
their CHECK constraints, ``raw_series`` with NOT NULL ``pull_status`` and no
default, ``uq_raw_series_composite``, ``resolved_series``, and
``signal_sources`` in production's shape) and must:

* write ``source_id`` + ``pull_status`` and get ``pull_timestamp`` from the
  schema default (the old inserts omitted ``pull_status`` and so never wrote a
  row on production);
* append only: a second run for the same observation writes nothing and
  leaves the stored value untouched, even when the provider value changed;
* (coingecko, E1-V6) write ``raw_series`` under its own ``coingecko`` catalog
  row and never ``resolved_series``; the value reaches ``resolved_series``
  only through ``normalization.resolver`` with ``release_date = vintage_date =``
  the pull date, not the observation date.

Throwaway schema per test, dropped afterwards. Skips without
``GRID_TEST_DB_URL``; the CI step "E1 writer provenance PostgreSQL contract"
fails on any skip.
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

import ingestion.smart_scheduler as ss

REPO = Path(__file__).resolve().parents[1]


def _schema_sql_ddl() -> list[str]:
    """The production DDL these writers touch, verbatim from schema.sql."""
    schema = (REPO / "schema.sql").read_text(encoding="utf-8")
    out = []
    for table in ("source_catalog", "raw_series", "feature_registry", "resolved_series", "signal_sources"):
        m = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \(.*?\n\);", schema, re.S)
        assert m, f"schema.sql has no {table} table"
        out.append(m.group(0))
    for index in ("uq_raw_series_composite", "uq_resolved_series_composite"):
        m = re.search(rf"CREATE UNIQUE INDEX IF NOT EXISTS {index}\s+ON [^;]+;", schema)
        assert m, f"schema.sql has no {index}"
        out.append(m.group(0))
    return out


@pytest.fixture
def pg_engine() -> Engine:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for the disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("E1 writer proof requires a local disposable test database")

    schema = "e1_writers_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    try:
        with engine.begin() as conn:
            for ddl in _schema_sql_ddl():
                conn.execute(text(ddl))
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()


def _rows(engine: Engine, series_prefix: str) -> list[Any]:
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT rs.series_id, rs.obs_date, rs.value, rs.pull_status, rs.pull_timestamp, sc.name "
            "FROM raw_series rs JOIN source_catalog sc ON sc.id = rs.source_id "
            "WHERE rs.series_id LIKE :p ORDER BY rs.series_id, rs.obs_date"
        ), {"p": series_prefix + "%"}).fetchall()


def _outcome(out: Any) -> str:
    return ss._classify_outcome(out)[0]


class _Response:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._payload


# ── E1-V3: bls ─────────────────────────────────────────────────────────


def test_bls_registry_entry_pulls_and_appends_once(pg_engine, monkeypatch) -> None:
    from ingestion import bls

    with pg_engine.begin() as conn:  # production has this row (id 3); the puller has no SOURCE_CONFIG
        conn.execute(text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, pit_available, "
            "revision_behavior, trust_score, priority_rank) "
            "VALUES ('BLS', 'https://api.bls.gov', 'FREE', 'MONTHLY', FALSE, 'FREQUENT', 'HIGH', 3)"
        ))

    requests_seen: list[dict] = []
    value = {"M08": "4.3"}

    def post(url, json=None, headers=None, timeout=None):
        requests_seen.append(json)
        return _Response({"status": "REQUEST_SUCCEEDED", "Results": {"series": [{
            "seriesID": "LNS14000000",
            "data": [{"year": "2026", "period": "M08", "value": value["M08"]},
                     {"year": "2026", "period": "M07", "value": "4.2"}],
        }]}})

    monkeypatch.setattr(bls.requests, "post", post)
    entry = next(e for e in ss.PULLER_REGISTRY if e["name"] == "bls")
    sched = ss.SmartScheduler.__new__(ss.SmartScheduler)
    sched.engine = pg_engine
    puller = sched._build_puller_instance(entry, bls.BLSPuller, {"BLS_API_KEY": "k-test"})
    assert puller.engine is pg_engine and puller.api_key == "k-test"

    out = getattr(puller, entry["method"])()
    assert out["rows_inserted"] == 2 and _outcome(out) == ss.OUTCOME_SUCCESS
    this_year = date.today().year
    assert len(requests_seen) == 1
    assert requests_seen[0]["startyear"] == str(this_year - 2)
    assert requests_seen[0]["endyear"] == str(this_year)
    assert requests_seen[0]["registrationkey"] == "k-test"

    value["M08"] = "4.4"  # a revision must not rewrite the stored vintage
    again = puller.pull_all()
    assert again["rows_inserted"] == 0 and _outcome(again) == ss.OUTCOME_NO_NEW_DATA
    rows = _rows(pg_engine, "LNS14000000")
    assert [(r.obs_date, r.value, r.pull_status, r.name) for r in rows] == [
        (date(2026, 7, 1), 4.2, "SUCCESS", "BLS"),
        (date(2026, 8, 1), 4.3, "SUCCESS", "BLS"),
    ]
    assert all(r.pull_timestamp is not None for r in rows)


# ── E1-V3 + E1-V4: wiki_history ────────────────────────────────────────


def test_wiki_history_pull_all_writes_one_row_per_day(pg_engine, monkeypatch) -> None:
    from ingestion.wiki_history import WikiHistoryPuller

    puller = WikiHistoryPuller(db_engine=pg_engine)
    events = [{"year": 1929, "text": "Stock market crash", "type": "event"}]
    monkeypatch.setattr(puller, "_wiki_on_this_day", lambda m, d: list(events))
    monkeypatch.setattr(puller, "_wiki_selected_anniversaries", lambda m, d: [])
    monkeypatch.setattr(puller, "_parse_rss", lambda url: [])

    day = date(2026, 9, 30)
    out = puller.pull_all(target_date=day)
    assert out["rows_inserted"] == 1 and _outcome(out) == ss.OUTCOME_SUCCESS
    events.append({"year": 1987, "text": "Black Monday", "type": "event"})
    again = puller.pull_all(target_date=day)
    assert again["rows_inserted"] == 0 and _outcome(again) == ss.OUTCOME_NO_NEW_DATA
    assert puller.save_to_db(puller.pull_today(day)) is True  # the intelligence-loop path

    rows = _rows(pg_engine, "wiki_today_")
    assert [(r.series_id, r.obs_date, r.value, r.pull_status, r.name) for r in rows] == [
        ("wiki_today_2026-09-30", day, 1.0, "SUCCESS", "WikiHistory"),
    ]

    events.clear()  # Wikipedia down: not an observation of "0 events"
    empty = puller.pull_all(target_date=date(2026, 10, 1))
    assert empty["rows_inserted"] == 0 and _outcome(empty) == ss.OUTCOME_FAILED
    assert len(_rows(pg_engine, "wiki_today_")) == 1


# ── E1-V4: social_sentiment ────────────────────────────────────────────


def test_social_sentiment_save_appends_once_per_day(pg_engine) -> None:
    from ingestion.social_sentiment import SocialSentimentPuller

    puller = SocialSentimentPuller(db_engine=pg_engine)
    scan = {"date": "2026-09-30", "reddit": {"stocks": [{"title": "NVDA"}]}, "bluesky": [],
            "ticker_sentiment": {"NVDA": {"mentions": 3}}}
    assert puller.save_to_db(scan) is True
    later = dict(scan, ticker_sentiment={"NVDA": {"mentions": 9}, "AMD": {"mentions": 1}})
    assert puller.save_to_db(later) is True
    assert puller.save_to_db({"date": "2026-10-01", "reddit": {}, "bluesky": [],
                              "ticker_sentiment": {}}) is False  # every source failed

    rows = _rows(pg_engine, "social_sentiment_")
    assert [(r.series_id, r.value, r.pull_status, r.name) for r in rows] == [
        ("social_sentiment_2026-09-30", 1.0, "SUCCESS", "SocialSentiment"),
    ]


# ── E1-V4: offshore_leaks ──────────────────────────────────────────────


def test_offshore_matches_land_and_are_not_restored(pg_engine, tmp_path) -> None:
    from ingestion.altdata.offshore_leaks import OffshoreLeaksPuller

    # Auto-creates ICIJ_OFFSHORE under source_catalog's CHECK constraints.
    puller = OffshoreLeaksPuller(pg_engine, data_dir=str(tmp_path))
    match = {
        "actor_name": "Example Person", "actor_id": "ACT001", "actor_tier": "tier_2",
        "officer_name": "EXAMPLE PERSON", "officer_node_id": "n1",
        "officer_jurisdiction": "VGB", "match_type": "exact", "officer_source_id": "panama",
        "connected_entities": [{"entity_name": "Example Holdings Ltd", "entity_jurisdiction": "VGB",
                                "entity_status": "Active", "incorporation_date": "2001-01-01",
                                "rel_type": "officer_of", "entity_source": "panama"}],
    }
    first = puller.store_matches([match])
    # signal_sources has production's shape (no metadata column), so that insert
    # fails -- inside its own savepoint, without rolling back the raw row.
    assert first["raw_series_inserted"] == 1
    assert puller.store_matches([match])["raw_series_inserted"] == 0

    rows = _rows(pg_engine, "OFFSHORE:")
    assert [(r.series_id, r.value, r.pull_status, r.name) for r in rows] == [
        ("OFFSHORE:Example_Person:Example_Holdings_Ltd:VGB", 1.0, "SUCCESS", "ICIJ_OFFSHORE"),
    ]


# ── E1-V4: scripts/full_universe_pull ──────────────────────────────────


def test_full_universe_fundamentals_append_once(pg_engine, monkeypatch) -> None:
    monkeypatch.chdir(os.getcwd())  # the script chdirs on import; restore afterwards
    from scripts import full_universe_pull as fup

    fup._store_fundamentals(pg_engine, "AAPL", {"SharesOutstanding": "15000000000", "PERatio": "30.5"})
    fup._store_fundamentals(pg_engine, "AAPL", {"SharesOutstanding": "15000000000", "PERatio": "31.0"})

    rows = _rows(pg_engine, "AV_FUND:AAPL:")
    assert [(r.series_id, r.value, r.pull_status, r.name) for r in rows] == [
        ("AV_FUND:AAPL:pe_ratio_av", 30.5, "SUCCESS", "ALPHAVANTAGE_FUND"),
        ("AV_FUND:AAPL:shares_outstanding", 15000000000.0, "SUCCESS", "ALPHAVANTAGE_FUND"),
    ]


# ── E1-V6: coingecko ───────────────────────────────────────────────────


def _feature(engine: Engine, name: str) -> int:
    with engine.begin() as conn:
        return conn.execute(text(
            "INSERT INTO feature_registry (name, family, description, transformation, normalization, "
            "missing_data_policy, eligible_from_date) "
            "VALUES (:n, 'crypto', 'fixture', 'RAW', 'RAW', 'FORWARD_FILL', '2020-01-01') RETURNING id"
        ), {"n": name}).scalar_one()


def test_coingecko_writes_raw_series_and_resolves_through_the_resolver(pg_engine, monkeypatch) -> None:
    from ingestion.coingecko import CoinGeckoPuller
    from normalization.resolver import Resolver

    xrp, btc = _feature(pg_engine, "xrp_usd_full"), _feature(pg_engine, "btc_usd_full")
    puller = CoinGeckoPuller(pg_engine)  # auto-creates the coingecko catalog row
    quoted_at = datetime.now(timezone.utc) - timedelta(days=1)
    quote = {"ripple": {"usd": 0.525, "usd_market_cap": 3.0e10, "usd_24h_vol": 1.2e9,
                        "last_updated_at": int(quoted_at.timestamp())},
             "bitcoin": {"usd": 60000.0, "last_updated_at": int(quoted_at.timestamp())}}
    calls: list[dict] = []

    def get(url, params=None, timeout=None):
        calls.append({"url": url, **(params or {})})
        return _Response(quote)

    monkeypatch.setattr(puller._session, "get", get)

    out = puller.pull_all(tickers=["XRP", "BTC"])
    assert out["rows_inserted"] == 2 and _outcome(out) == ss.OUTCOME_SUCCESS
    assert len(calls) == 1 and calls[0]["url"].endswith("/simple/price")
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM resolved_series")).scalar_one() == 0
        cg_id = conn.execute(text("SELECT id FROM source_catalog WHERE name = 'coingecko'")).scalar_one()

    rows = _rows(pg_engine, "CG:")
    assert [(r.series_id, r.obs_date, r.value, r.pull_status, r.name) for r in rows] == [
        ("CG:bitcoin:usd", quoted_at.date(), 60000.0, "SUCCESS", "coingecko"),
        ("CG:ripple:usd", quoted_at.date(), 0.525, "SUCCESS", "coingecko"),
    ]
    pulled_on = {r.pull_timestamp.astimezone(timezone.utc).date() for r in rows}
    assert pulled_on == {datetime.now(timezone.utc).date()}  # honest pull time, not the quote date

    quote["ripple"]["usd"] = 9.99  # same quote day, new price: not rewritten
    again = puller.pull_all(tickers=["XRP", "BTC"])
    assert again["rows_inserted"] == 0 and _outcome(again) == ss.OUTCOME_NO_NEW_DATA
    assert [r.value for r in _rows(pg_engine, "CG:ripple:usd")] == [0.525]

    quote.pop("bitcoin")
    partial = puller.pull_all(tickers=["XRP", "BTC"])
    assert _outcome(partial) == ss.OUTCOME_PARTIAL and "BTC" in partial["error"]

    summary = Resolver(pg_engine).resolve_pending(workers=1, lookback_days=3)
    assert summary["errors"] == 0
    with pg_engine.connect() as conn:
        resolved = conn.execute(text(
            "SELECT feature_id, obs_date, release_date, vintage_date, value, source_priority_used "
            "FROM resolved_series ORDER BY feature_id"
        )).fetchall()
    (pull_day,) = pulled_on
    # XRP resolves under coingecko's own source id, dated when GRID learned it.
    # BTC stays unmapped: btc_usd_full belongs to the yfinance daily close.
    assert [tuple(r) for r in resolved] == [(xrp, quoted_at.date(), pull_day, pull_day, 0.525, cg_id)]
    assert btc not in {r.feature_id for r in resolved}


def test_coingecko_provider_outage_writes_nothing(pg_engine, monkeypatch) -> None:
    from ingestion.coingecko import CoinGeckoPuller

    puller = CoinGeckoPuller(pg_engine)

    def down(url, params=None, timeout=None):
        raise RuntimeError("mock provider outage")

    monkeypatch.setattr(puller._session, "get", down)
    out = puller.pull_all()
    assert _outcome(out) == ss.OUTCOME_FAILED and out["rows_inserted"] == 0
    assert _rows(pg_engine, "CG:") == []
