"""Real-PostgreSQL proof that E2's price resolution is point-in-time and read-only.

Runs in a throwaway schema (random name, dropped afterwards) with minimal
``source_catalog`` and ``raw_series`` tables, read through
``store.observations.read_window_known_at`` by ``evals.e2.resolve.KnownAtCloseSource``.
Proves, against real SQL:

* a close is not observable before it was pulled, nor before its session closed;
* a revision pulled after the run instant is never used (the earlier vintage is);
* a second source writing the same series id is ignored when one source is named;
* a full board run resolves a generic stream's direction call only once the
  exit close is observable, and the receipt names the source and pull vintage;
* ``read_only_connection`` really is read-only (a write is refused by the server).

Skips without a reachable PostgreSQL (``GRID_TEST_DB_URL``); the CI step fails on a skip.
"""

from __future__ import annotations

from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from evals.e2 import board
from evals.e2.resolve import KnownAtCloseSource, read_only_connection
from tests import e2_support as S

DDL = (
    "CREATE TABLE source_catalog (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    "CREATE TABLE raw_series (id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL, source_id INTEGER NOT NULL "
    "REFERENCES source_catalog(id), obs_date DATE NOT NULL, value DOUBLE PRECISION, pull_status TEXT NOT NULL, "
    "pull_timestamp TIMESTAMPTZ NOT NULL)",
)
ROWS = [  # (source, obs_date, value, pulled)
    ("tiingo", date(2026, 10, 1), 100.0, S.utc(2026, 10, 1, 21, 5)),
    ("tiingo", date(2026, 10, 2), 110.0, S.utc(2026, 10, 2, 21, 5)),
    ("tiingo", date(2026, 10, 2), 111.0, S.utc(2026, 10, 3, 2, 0)),   # a revision pulled later
    ("yfinance", date(2026, 10, 2), 109.0, S.utc(2026, 10, 2, 21, 0)),  # another source, same series id
    ("tiingo", date(2026, 10, 5), 120.0, S.utc(2026, 10, 5, 18, 0)),  # "pulled" before the 20:00Z close
]


@pytest.fixture()
def scratch(pg_engine):
    schema = f"e2_prices_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            for ddl in DDL:
                conn.execute(text(ddl))
            ids = {name: conn.execute(text("INSERT INTO source_catalog (name) VALUES (:n) RETURNING id"),
                                      {"n": name}).scalar_one() for name in ("tiingo", "yfinance")}
            for source, d, value, pulled in ROWS:
                conn.execute(text("INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status, "
                                  "pull_timestamp) VALUES ('YF:AAA:close', :s, :d, :v, 'SUCCESS', :p)"),
                             {"s": ids[source], "d": d, "v": value, "p": pulled})
        reader = create_engine(pg_engine.url, connect_args={
            "options": f"-csearch_path={schema} -c default_transaction_read_only=on"})
        with reader.connect() as conn:
            yield conn
        reader.dispose()
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))


def test_close_is_not_observable_before_its_pull_or_its_session_close(scratch):
    src = KnownAtCloseSource(scratch, source="tiingo")
    assert src.close("AAA", date(2026, 10, 2), S.utc(2026, 10, 2, 21, 0)) is None   # pulled 21:05Z
    obs = src.close("AAA", date(2026, 10, 2), S.utc(2026, 10, 2, 21, 30))
    assert (obs.value, obs.vintage, obs.source) == (110.0, "2026-10-02T21:05:00+00:00", "raw_series:tiingo")
    assert obs.available_at == S.utc(2026, 10, 2, 21, 5)
    # a row pulled before the session closed is still only observable at the close (20:00Z)
    early = src.close("AAA", date(2026, 10, 5), S.utc(2026, 10, 5, 19, 0))
    assert early is None
    assert src.close("AAA", date(2026, 10, 5), S.utc(2026, 10, 5, 20, 30)).available_at == S.utc(2026, 10, 5, 20)


def test_revision_pulled_after_the_run_instant_is_never_used(scratch):
    src = KnownAtCloseSource(scratch, source="tiingo")
    # 01:00Z on 10-03: the 111 revision (pulled 02:00Z) is not yet known; the 110 vintage is
    assert src.close("AAA", date(2026, 10, 2), S.utc(2026, 10, 3, 1, 0)).value == 110.0
    assert src.close("AAA", date(2026, 10, 2), S.utc(2026, 10, 3, 3, 0)).value == 111.0


def test_named_source_ignores_the_other_puller(scratch):
    assert KnownAtCloseSource(scratch, source="yfinance").close(
        "AAA", date(2026, 10, 2), S.utc(2026, 10, 2, 21, 30)).value == 109.0
    assert KnownAtCloseSource(scratch, source="tiingo").close(
        "AAA", date(2026, 10, 2), S.utc(2026, 10, 2, 21, 30)).value == 110.0


def test_board_resolves_only_once_the_exit_close_is_observable(scratch, tmp_path):
    rules = S.rules()
    records = [r for r in S.stream_records() if r.get("kind") == "header" or r["prediction_id"].endswith(":d1")]
    log = tmp_path / "stream.jsonl"
    S.write_chain(log, records)
    prices = KnownAtCloseSource(scratch, source="tiingo")

    def run(now):
        return board.run(tmp_path / "board", [S.stream_adapter(log, rules, prices)], now, rules=rules,
                         cost_model=S.cost_model(), manifest_info=S.MANIFEST_INFO, code_sha=S.CODE_SHA)["snapshot"]

    assert run(S.utc(2026, 10, 2, 21, 0))["counts"]["resolutions"] == 0
    snap = run(S.utc(2026, 10, 2, 21, 30))
    assert snap["counts"]["scores"] == 1
    from evals.e2.chain import Ledger

    res = [r for r in Ledger(tmp_path / "board", "e2-v1").read_all() if r["kind"] == "resolution"][0]
    assert res["outcome"]["return"] == pytest.approx(0.10)
    assert res["receipt"]["exit"]["vintage"] == "2026-10-02T21:05:00+00:00"
    assert res["receipt"]["price_source"] == "raw_series:tiingo"


def test_read_only_connection_refuses_writes(pg_engine):
    with read_only_connection(str(pg_engine.url.render_as_string(hide_password=False))) as conn:
        with pytest.raises(DBAPIError, match="read-only"):
            conn.execute(text("CREATE TEMP TABLE e2_should_fail (x int)"))
