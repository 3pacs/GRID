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


def test_pg_receipts_and_change_rows_come_from_one_snapshot(pg_engine, monkeypatch):
    """Codex HOLD 6016542599 (2): a writer whose transaction began before the
    read commits a new comparison (Q0 against Q-1's restatement) right after
    the reader's first raw_series statement. Under READ COMMITTED a second
    statement would see the new change rows against the old receipts and
    drop them as stale (a state that never existed). The reader must show
    one consistent state: the one before the commit, then the new one."""
    from sqlalchemy import event

    edgar = s.FakeEdgar()
    q1_report, q1_filed = s.quarter_dates(1)
    q0_report, q0_filed = s.quarter_dates(0)
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", q1_filed, q1_report, {s.PEER_A_CUSIP: ("PEER A CORP", 1_000, 10)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0", q0_filed, q0_report, {
        s.PEER_A_CUSIP: ("PEER A CORP", 9_000, 90), s.ROOT_CUSIP: ("ROOT HOLDINGS INC", 40_000, 400)}))
    filers = {s.CIK_ALPHA: s.ALPHA}
    earlier = datetime.combine(q0_filed, datetime.min.time(), tzinfo=timezone.utc).replace(hour=6)

    s.run_writer(pg_engine, monkeypatch, edgar, filers)
    s.set_receipt(pg_engine, q0_filed, earlier)
    # The real writer's next run (after Q-1 is restated), captured on a
    # scratch engine so its rows can be committed concurrently on PG.
    scratch = s.make_sqlite_engine()
    s.run_writer(scratch, monkeypatch, edgar, filers)
    s.set_receipt(scratch, q0_filed, earlier.replace(tzinfo=None))
    with scratch.connect() as conn:
        first_run_max = conn.execute(text("SELECT MAX(id) FROM raw_series")).scalar()
    edgar.add(s.CIK_ALPHA, s.Filing("A-1R", q0_filed + timedelta(days=1), q1_report, {
        s.PEER_A_CUSIP: ("PEER A CORP", 1_000, 10), s.ROOT_CUSIP: ("ROOT HOLDINGS INC", 40_000, 400)},
        form="13F-HR/A"))
    s.run_writer(scratch, monkeypatch, edgar, filers)
    with scratch.connect() as conn:
        second_run = conn.execute(
            text("SELECT series_id, obs_date, value, raw_payload FROM raw_series WHERE id > :m ORDER BY id"),
            {"m": first_run_max},
        ).fetchall()
    assert any(r[0].startswith("13F-FILING:") for r in second_run)
    assert any(r[0].endswith(":INCREASED") for r in second_run)

    writer_conn = pg_engine.connect()
    writer_tx = writer_conn.begin()  # NOW() = this transaction's start, before the read
    for series_id, obs_date, value, payload in second_run:
        writer_conn.execute(
            text("INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status) "
                 "VALUES (:sid, 7, :d, :v, :p, 'SUCCESS')"),
            {"sid": series_id, "d": obs_date, "v": value, "p": payload},
        )
    fired: list[str] = []

    def commit_mid_read(conn, cursor, statement, params, context, executemany):
        if "FROM raw_series" in statement and "vintage_rank" in statement and not fired:
            fired.append(statement)
            writer_tx.commit()

    event.listen(pg_engine, "after_cursor_execute", commit_mid_read)
    try:
        during = h13f.read_13f_holdings_snapshot(pg_engine)
    finally:
        event.remove(pg_engine, "after_cursor_execute", commit_mid_read)
        writer_conn.close()
    assert len(fired) == 1
    assert {h.ticker for h in during.holdings} == {s.ROOT, s.PEER_A}  # the state before the commit
    assert "stale_comparison" not in during.counts

    h13f.clear_snapshot_cache()
    after = h13f.read_13f_holdings_snapshot(pg_engine)
    assert {h.ticker for h in after.holdings} == {s.PEER_A}  # the committed state
    assert after.counts["stale_comparison"] == 1


def test_a_rerun_writes_no_change_row_and_a_changed_filing_writes_one(pg_engine, monkeypatch):
    """JSONB payloads compare equal after their round trip, so a re-run of the
    same filings a day later adds no change row (grid-svr 2026-11-23: every run
    more than an hour after the last appended all 41,762 again). A changed
    filing is still a new vintage. See tests/test_13f_rerun_writes_nothing_new.py."""

    def change_rows() -> int:
        with pg_engine.connect() as conn:
            return conn.execute(text("SELECT COUNT(*) FROM raw_series WHERE series_id LIKE '13F:%'")).scalar_one()

    edgar = s.two_quarter_scenario()
    s.run_writer(pg_engine, monkeypatch, edgar)
    stored = change_rows()
    assert stored > 0
    _report, filed = s.quarter_dates(0)
    assert s.set_receipt(pg_engine, filed, datetime.now(timezone.utc) - timedelta(days=1)) == stored

    second = s.run_writer(pg_engine, monkeypatch, edgar)

    assert change_rows() == stored
    assert sum(r["rows_inserted"] for r in second) == 0
    assert sum(r["rows_unchanged"] for r in second) == stored

    edgar.filings[s.CIK_ALPHA][1].holdings[s.ROOT_CUSIP] = ("ROOT HOLDINGS INC", 280_000, 2_000)
    third = s.run_writer(pg_engine, monkeypatch, edgar)

    assert sum(r["rows_inserted"] for r in third) == 1
    assert change_rows() == stored + 1


def test_two_overlapping_writers_add_each_change_row_once(pg_engine, monkeypatch):
    """Two connections write the same manager's filing at once (grid-svr
    2026-11-23: a timed-out run's orphaned thread and the repair worker's
    retry). The first is held open inside its transaction, after its inserts
    and before its commit. Without the advisory lock the second looks up,
    sees nothing committed and inserts every row again. With it the second
    waits, then finds the first writer's rows and adds none. Both receipts
    survive, and a changed filing is still a new vintage afterwards."""
    import threading

    from ingestion.altdata import institutional_flows as inst

    filers = {s.CIK_ALPHA: s.ALPHA}
    edgar = s.two_quarter_scenario()
    monkeypatch.setattr(inst.requests, "get", edgar.get)
    monkeypatch.setattr(inst.time, "sleep", lambda *_a: None)
    monkeypatch.setattr(inst, "TOP_13F_FILERS", dict(filers))
    inst._DEAD_CIKS.clear()

    first_inside = threading.Event()
    second_at_the_gate = threading.Event()
    arrivals: list[str] = []
    gate = threading.Lock()
    original_receipt = inst.InstitutionalFlowsPuller._insert_receipt

    def second_is_waiting_on_the_lock() -> bool:
        with pg_engine.connect() as probe:
            return bool(probe.execute(text(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted "
                "AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
            )).scalar_one())

    def held_receipt(self, conn, series_id, obs_date, payload, curr_snap):
        with gate:
            arrivals.append(threading.current_thread().name)
            first = len(arrivals) == 1
        if first:
            first_inside.set()
            deadline = datetime.now(timezone.utc) + timedelta(seconds=20)
            while datetime.now(timezone.utc) < deadline:
                if second_at_the_gate.is_set() or second_is_waiting_on_the_lock():
                    break
                threading.Event().wait(0.05)
        else:
            second_at_the_gate.set()      # reached only if nothing made it wait
        return original_receipt(self, conn, series_id, obs_date, payload, curr_snap)

    monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_insert_receipt", held_receipt)

    results: dict[str, list[dict]] = {}
    errors: list[BaseException] = []

    def write(name: str) -> None:
        try:
            results[name] = inst.InstitutionalFlowsPuller(pg_engine).pull_13f_only()
        except BaseException as exc:  # noqa: BLE001 -- surfaced below
            errors.append(exc)

    one = threading.Thread(target=write, args=("one",), name="writer-one")
    two = threading.Thread(target=write, args=("two",), name="writer-two")
    one.start()
    assert first_inside.wait(20), "the first writer never reached its receipt"
    two.start()
    one.join(60)
    two.join(60)
    assert not one.is_alive() and not two.is_alive() and not errors, errors

    def count(pattern: str) -> int:
        with pg_engine.connect() as conn:
            return conn.execute(text("SELECT COUNT(*) FROM raw_series WHERE series_id LIKE :p"),
                                {"p": pattern}).scalar_one()

    inserted = {name: sum(r["rows_inserted"] for r in rows) for name, rows in results.items()}
    unchanged = {name: sum(r["rows_unchanged"] for r in rows) for name, rows in results.items()}
    stored = count("13F:%")
    assert stored > 0
    assert sorted(inserted.values()) == [0, stored]            # one writer wrote them, the other none
    assert sorted(unchanged.values()) == [0, stored]
    with pg_engine.connect() as conn:
        copies = conn.execute(text(
            "SELECT MAX(n) FROM (SELECT COUNT(*) AS n FROM raw_series WHERE series_id LIKE '13F:%' "
            "GROUP BY series_id, obs_date) per_key")).scalar_one()
    assert copies == 1
    assert count("13F-FILING:%") == 2                           # each run's receipt survives

    monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_insert_receipt", original_receipt)
    edgar.filings[s.CIK_ALPHA][1].holdings[s.ROOT_CUSIP] = ("ROOT HOLDINGS INC", 280_000, 2_000)
    third = inst.InstitutionalFlowsPuller(pg_engine).pull_13f_only()
    assert sum(r["rows_inserted"] for r in third) == 1          # a different filing state is still stored
    assert count("13F:%") == stored + 1
    assert count("13F-FILING:%") == 3


def _patch_writer(monkeypatch, get):
    from ingestion.altdata import institutional_flows as inst

    monkeypatch.setattr(inst.requests, "get", get)
    monkeypatch.setattr(inst.time, "sleep", lambda *_a: None)
    monkeypatch.setattr(inst, "TOP_13F_FILERS", {s.CIK_ALPHA: s.ALPHA})
    inst._DEAD_CIKS.clear()
    return inst


def _count(pg_engine, pattern: str) -> int:
    with pg_engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM raw_series WHERE series_id LIKE :p"),
                            {"p": pattern}).scalar_one()


def test_a_writer_does_not_queue_behind_a_held_lock(pg_engine, monkeypatch):
    """Another connection holds the manager's lock for the whole run (an
    orphaned writer that never finishes). The writer waits its bound, defers
    the manager and returns: no change row, no receipt, no error. With the
    run's budget already spent it does not wait at all."""
    import time as _time

    from ingestion.base import _stable_lock_key

    edgar = s.two_quarter_scenario()
    inst = _patch_writer(monkeypatch, edgar.get)
    monkeypatch.setattr(inst, "_CHANGE_ROW_LOCK_WAIT_S", 0.3)
    _report, filed = s.quarter_dates(0)
    key = _stable_lock_key(inst.InstitutionalFlowsPuller.SOURCE_NAME, "13F-change-rows", s.CIK_ALPHA,
                           filed.isoformat())

    holder = pg_engine.connect()
    held = holder.begin()
    holder.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": key})
    try:
        started = _time.monotonic()
        waited = inst.InstitutionalFlowsPuller(pg_engine).pull_13f_only()
        took = _time.monotonic() - started
        assert [(r["status"], r.get("reason"), r["rows_inserted"]) for r in waited] == [
            ("DEFERRED", "change_rows_held_by_another_writer", 0)]
        assert 0.25 <= took < 10
        assert _count(pg_engine, "13F:%") == 0 and _count(pg_engine, "13F-FILING:%") == 0

        monkeypatch.setattr(inst, "_CHANGE_ROW_LOCK_BUDGET_S", 0.0)       # nothing left to wait with
        started = _time.monotonic()
        tried = inst.InstitutionalFlowsPuller(pg_engine).pull_13f_only()
        assert tried[0]["status"] == "DEFERRED" and _time.monotonic() - started < 5
    finally:
        held.rollback()
        holder.close()

    monkeypatch.setattr(inst, "_CHANGE_ROW_LOCK_BUDGET_S", 20.0)
    after = inst.InstitutionalFlowsPuller(pg_engine).pull_13f_only()     # the lock is free again
    assert after[0]["status"] == "SUCCESS" and after[0]["rows_inserted"] == _count(pg_engine, "13F:%") > 0
    assert _count(pg_engine, "13F-FILING:%") == 1


def test_overlapping_writers_with_different_payloads_store_both_in_commit_order(pg_engine, monkeypatch):
    """What the lock does not claim. Each writer fetched its filing before the
    lock; they hold different states of one holding. Both states are stored,
    and the newest vintage is the one written last (the DB stamps the write),
    whichever fetch was newer. Vintage order is write order, not fetch order."""
    import threading

    first_state = s.two_quarter_scenario()
    second_state = s.two_quarter_scenario()
    second_state.filings[s.CIK_ALPHA][1].holdings[s.ROOT_CUSIP] = ("ROOT HOLDINGS INC", 280_000, 2_000)

    def get(url, headers=None, timeout=None):
        edgar = second_state if threading.current_thread().name == "writer-two" else first_state
        return edgar.get(url, headers, timeout)

    inst = _patch_writer(monkeypatch, get)
    first_inside = threading.Event()
    arrivals: list[str] = []
    gate = threading.Lock()
    original_receipt = inst.InstitutionalFlowsPuller._insert_receipt

    def second_is_waiting_on_the_lock() -> bool:
        with pg_engine.connect() as probe:
            return bool(probe.execute(text(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted "
                "AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
            )).scalar_one())

    def held_receipt(self, conn, series_id, obs_date, payload, curr_snap):
        with gate:
            arrivals.append(threading.current_thread().name)
            first = len(arrivals) == 1
        if first:
            first_inside.set()
            deadline = datetime.now(timezone.utc) + timedelta(seconds=3)
            while datetime.now(timezone.utc) < deadline and not second_is_waiting_on_the_lock():
                threading.Event().wait(0.05)
        return original_receipt(self, conn, series_id, obs_date, payload, curr_snap)

    monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_insert_receipt", held_receipt)
    results: dict[str, list[dict]] = {}
    errors: list[BaseException] = []

    def write(name: str) -> None:
        try:
            results[name] = inst.InstitutionalFlowsPuller(pg_engine).pull_13f_only()
        except BaseException as exc:  # noqa: BLE001 -- surfaced below
            errors.append(exc)

    one = threading.Thread(target=write, args=("one",), name="writer-one")
    two = threading.Thread(target=write, args=("two",), name="writer-two")
    one.start()
    assert first_inside.wait(20)
    two.start()
    one.join(60)
    two.join(60)
    assert not errors and arrivals[0] == "writer-one", (errors, arrivals)
    assert results["two"][0]["status"] == "SUCCESS" and results["two"][0]["rows_inserted"] == 1

    with pg_engine.connect() as conn:
        root = conn.execute(text(
            "SELECT raw_payload->>'curr_value_usd' FROM raw_series WHERE series_id LIKE :p "
            "ORDER BY pull_timestamp, id"), {"p": f"13F:{s.CIK_ALPHA}:{s.ROOT_CUSIP}:%"}).fetchall()
        copies = conn.execute(text(
            "SELECT MAX(n) FROM (SELECT COUNT(*) AS n FROM raw_series WHERE series_id LIKE '13F:%' "
            "AND series_id NOT LIKE :root GROUP BY series_id, obs_date) per_key"),
            {"root": f"13F:{s.CIK_ALPHA}:{s.ROOT_CUSIP}:%"}).scalar_one()
    assert [float(v) for (v,) in root] == [260_000.0, 280_000.0]      # both kept; the later commit is newest
    assert copies == 1                                                # every unchanged series once
    assert _count(pg_engine, "13F-FILING:%") == 2
