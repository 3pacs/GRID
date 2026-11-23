"""PostgreSQL contract: 13F holdings observations, writer -> served graph.

The SQLite suite (``tests/test_13f_holdings_observations.py``) covers the
rules; this file runs the same path on PostgreSQL types (JSONB payloads,
TIMESTAMPTZ receipts, the window-function vintage read) with nothing
patched in the graph: ``deep_graph.ensure_table`` runs its real DDL.

Runs against a fresh schema on ``GRID_TEST_DB_URL`` (local, disposable
database only). Skips without it unless ``REQUIRE_13F_HOLDINGS_PG=1``, which
the dedicated CI step sets so the contract cannot pass by skipping.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

from intelligence import deep_graph, dollar_flows
from intelligence import holdings_observations_13f as h13f
from tests import thirteenf_holdings_support as s

pytestmark = pytest.mark.xdist_group("postgres")

_REQUIRE_ENV = "REQUIRE_13F_HOLDINGS_PG"


@pytest.fixture()
def pg_engine():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required for the 13F holdings PG contract ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("13F holdings PG contract requires a local disposable test database")

    schema = "h13f_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        for ddl in s.DDL_PG:
            conn.execute(text(ddl))
    s.seed_base(engine)
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    h13f.clear_snapshot_cache()
    monkeypatch.setattr(dollar_flows, "_get_cusip_ticker_map", lambda: None)
    yield
    h13f.clear_snapshot_cache()


def _route(monkeypatch, engine):
    from api.routers import intelligence_companies as companies

    monkeypatch.setattr(companies, "get_db_engine", lambda: engine)
    companies._deep_graph_cache.clear()
    return companies


def test_pg_writer_to_served_graph_keeps_filing_and_receipt_dates(pg_engine, monkeypatch):
    s.run_writer(pg_engine, monkeypatch, s.two_quarter_scenario())
    campaign = s.seed_upstream_layers(pg_engine, s.ROOT, [s.PEER_A, s.PEER_B])
    _report, filed = s.quarter_dates(0)
    received = datetime.combine(filed + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).replace(hour=6)
    assert s.set_receipt(pg_engine, filed, received) == 6

    companies = _route(monkeypatch, pg_engine)
    try:
        response = json.loads(json.dumps(companies.get_deep_graph(ticker=s.ROOT, depth=10, _token="t")))
    finally:
        companies._deep_graph_cache.clear()
    drill = response["drill"]
    assert drill is not None, response
    layer9 = next(layer for layer in drill["layers"] if layer["depth"] == 9)
    assert [a["label"] for a in layer9["actors"]] == [s.ALPHA]
    shown = {h["ticker"]: h for h in layer9["actors"][0]["holdings"]}
    assert shown[s.ROOT]["filing_date"] == filed.isoformat()
    assert shown[s.ROOT]["received_at"] == received.isoformat()
    assert shown[s.ROOT]["reported_value_usd"] == 260_000.0
    assert layer9["dollar_flows"] == 0.0 and layer9["observed_holdings_value_usd"] == 380_000.0
    assert drill["total_dollar_flow"] == pytest.approx(campaign)
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('graph_overlaps') IS NOT NULL")).scalar() is True

    before = h13f.read_13f_holdings_snapshot(pg_engine, as_of_ts=received - timedelta(seconds=1))
    assert before.holdings == ()


def test_pg_latest_received_vintage_wins(pg_engine, monkeypatch):
    edgar = s.two_quarter_scenario()
    s.run_writer(pg_engine, monkeypatch, edgar, {s.CIK_ALPHA: s.ALPHA})
    _report, filed = s.quarter_dates(0)
    first = datetime.combine(filed + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).replace(hour=6)
    s.set_receipt(pg_engine, filed, first)
    edgar.filings[s.CIK_ALPHA][1].holdings[s.ROOT_CUSIP] = ("ROOT HOLDINGS INC", 280_000, 2_000)
    s.run_writer(pg_engine, monkeypatch, edgar, {s.CIK_ALPHA: s.ALPHA})

    def root_value(**kw):
        snap = h13f.read_13f_holdings_snapshot(pg_engine, **kw)
        return {h.ticker: h.reported_value_usd for h in snap.holdings}[s.ROOT]

    assert root_value() == 280_000.0
    assert root_value(as_of_ts=first + timedelta(hours=1)) == 260_000.0


def test_pg_shown_observation_is_absent_from_directional_readers(pg_engine, monkeypatch):
    from analysis import flow_aggregator
    from intelligence import sentiment_scorer

    s.run_writer(pg_engine, monkeypatch, s.two_quarter_scenario())
    _report, filed = s.quarter_dates(0)
    dollar_flows.normalize_all_flows(pg_engine, days=90)
    with pg_engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT ticker, amount_usd, direction, evidence FROM dollar_flows WHERE source_type = '13f'"
        )).fetchall()
    accession = "9000000001-26-000002"
    same = [r for r in rows if r[0] == s.ROOT and r[3]["filing_accession"] == accession]
    assert len(same) == 1 and float(same[0][1]) == 260_000.0 and same[0][2] == "observed"

    layer = deep_graph._layer9_cross_holding_funds(pg_engine, s.ROOT, [s.PEER_A])
    holding = {h["ticker"]: h for h in layer.actors[0]["holdings"]}[s.ROOT]
    assert holding["filing_accession"] == accession

    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dollar_flows (source_type, actor_name, ticker, amount_usd, direction, "
                "confidence, evidence, flow_date) VALUES ('insider', 'Jane Fixture', :t, 1000000, "
                "'inflow', 'confirmed', '{}', :d)"
            ),
            {"t": s.ROOT, "d": filed},
        )
    control = [("insider", 1_000_000.0)]
    assert [(r["source_type"], r["amount_usd"]) for r in dollar_flows.get_flows_by_ticker(pg_engine, s.ROOT)] == control
    assert set(dollar_flows.get_aggregate_flows(pg_engine, days=30)["by_source_type"]) == {"insider"}
    flow_aggregator._flow_cache.clear()
    try:
        fetched = flow_aggregator._fetch_flows(pg_engine, 30)
    finally:
        flow_aggregator._flow_cache.clear()
    assert [(f["source_type"], f["amount_usd"]) for f in fetched] == control
    assert sentiment_scorer._score_flows(pg_engine, days=30).raw_value == pytest.approx(1.0)
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM signal_sources")).scalar() == 0


def _layer9_live(monkeypatch, engine):
    companies = _route(monkeypatch, engine)
    h13f.clear_snapshot_cache()
    try:
        response = json.loads(json.dumps(companies.get_deep_graph(ticker=s.ROOT, depth=10, _token="t")))
    finally:
        companies._deep_graph_cache.clear()
    return next(layer for layer in response["drill"]["layers"] if layer["depth"] == 9)


def test_pg_no_delta_restatement_and_unreadable_filing_are_negative_controls(pg_engine, monkeypatch):
    """Codex HOLDs 6016240483 / 6016410247 on PostgreSQL, real submissions
    shape (the writer classifies the amendment from its cover page): a
    no-delta restatement withdraws the original's holdings, and an
    unreadable later filing closes nothing and leaves nothing claimed, both
    through the served route."""
    edgar = s.FakeEdgar()
    q1_report, q1_filed = s.quarter_dates(1)
    q0_report, q0_filed = s.quarter_dates(0)
    y_unchanged = {s.PEER_A_CUSIP: ("PEER A CORP", 1_000, 10)}
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", q1_filed, q1_report, dict(y_unchanged)))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0", q0_filed, q0_report, {
        s.PEER_A_CUSIP: ("PEER A CORP", 9_000, 90), s.ROOT_CUSIP: ("ROOT HOLDINGS INC", 40_000, 400)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0R", q0_filed + timedelta(days=3), q0_report, dict(y_unchanged),
                                    form="13F-HR/A"))

    s.seed_upstream_layers(pg_engine, s.ROOT, [s.PEER_A, s.PEER_B])
    filers = {s.CIK_ALPHA: s.ALPHA}

    edgar.today = q0_filed
    s.run_writer(pg_engine, monkeypatch, edgar, filers)
    assert [a["label"] for a in _layer9_live(monkeypatch, pg_engine)["actors"]] == [s.ALPHA]
    edgar.today = date.today()
    s.run_writer(pg_engine, monkeypatch, edgar, filers)
    after = _layer9_live(monkeypatch, pg_engine)
    assert after["actors"] == [] and after["holdings_observation"]["excluded"]["superseded_filing"] == 2

    # A later filing whose information table is missing: withheld, no CLOSED.
    later = s.Filing("A-N", date.today() - timedelta(days=1), date.today() - timedelta(days=10),
                     dict(y_unchanged), infotable="missing")
    edgar.add(s.CIK_ALPHA, later)
    results = s.run_writer(pg_engine, monkeypatch, edgar, filers)
    assert results[0]["status"] == "QUARANTINED" and results[0]["curr_snapshot"] == "missing_infotable"
    assert not [sid for sid, _p in s.series_rows(pg_engine, "13F:%") if sid.endswith(":CLOSED")]
    final = _layer9_live(monkeypatch, pg_engine)
    assert final["actors"] == []
    assert final["holdings_observation"]["excluded"]["managers_latest_filing_unverified"] == 1


def test_pg_known_newer_filing_that_fails_to_read_is_recorded_until_recovery(pg_engine, monkeypatch):
    """A 503 on the newer filing's index.json after its accession is known:
    a dated withheld receipt stands for it (nothing shown as current), the
    older holdings show as of before the failure, and a later run recovers."""
    edgar = s.FakeEdgar()
    dates = {q: s.quarter_dates(q) for q in range(3)}
    edgar.add(s.CIK_ALPHA, s.Filing("A-2", dates[2][1], dates[2][0], {s.PEER_A_CUSIP: ("PEER A CORP", 1_000, 10)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", dates[1][1], dates[1][0], {
        s.PEER_A_CUSIP: ("PEER A CORP", 9_000, 90), s.ROOT_CUSIP: ("ROOT HOLDINGS INC", 40_000, 400)}))
    newer = s.Filing("A-0", dates[0][1], dates[0][0], {
        s.PEER_A_CUSIP: ("PEER A CORP", 30_000, 300), s.ROOT_CUSIP: ("ROOT HOLDINGS INC", 44_000, 400)},
        fail="503")
    edgar.add(s.CIK_ALPHA, newer)
    s.seed_upstream_layers(pg_engine, s.ROOT, [s.PEER_A, s.PEER_B])
    filers = {s.CIK_ALPHA: s.ALPHA}

    edgar.today = dates[1][1]
    s.run_writer(pg_engine, monkeypatch, edgar, filers)
    s.set_receipt(pg_engine, dates[1][1],
                  datetime.combine(dates[1][1], datetime.min.time(), tzinfo=timezone.utc).replace(hour=6))
    edgar.today = date.today()
    results = s.run_writer(pg_engine, monkeypatch, edgar, filers)
    assert results[0]["curr_snapshot"] == "fetch_failed:http_503"
    failed_at = datetime.combine(dates[0][1] + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    s.set_receipt(pg_engine, dates[0][1], failed_at)
    assert _layer9_live(monkeypatch, pg_engine)["actors"] == []
    before = h13f.read_13f_holdings_snapshot(pg_engine, as_of_ts=failed_at - timedelta(seconds=1))
    assert {h.ticker for h in before.holdings} == {s.ROOT, s.PEER_A}

    newer.fail = None
    assert s.run_writer(pg_engine, monkeypatch, edgar, filers)[0]["status"] == "SUCCESS"
    recovered = _layer9_live(monkeypatch, pg_engine)
    assert recovered["actors"][0]["observation"]["held_through_report_date"] == dates[0][0].isoformat()
