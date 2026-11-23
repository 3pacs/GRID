"""13F holdings observations: writer -> reader -> holdings graph -> served route.

Owner decision 2026-11-23: 13F is non-directional until CUSIP identity and
split coverage exist. Codex (review 6016000946 on #1369) asked for an
explicit served holdings-observation reader and a writer-to-display fixture
that keeps filing and receipt dates and proves the same observation never
enters a directional consumer.

Every test here starts from the real writer
(``InstitutionalFlowsPuller.pull_13f_only``) with only the EDGAR network
replaced (``tests/thirteenf_holdings_support.FakeEdgar``), except the
amendment and legacy-row tests, which say why. Offline: in-memory SQLite.
The PostgreSQL contract is ``tests/test_13f_holdings_graph_pg.py``.
"""

from __future__ import annotations

import json
import pathlib
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

from intelligence import deep_graph, dollar_flows
from intelligence import holdings_observations_13f as h13f
from tests import thirteenf_holdings_support as s

REPO = pathlib.Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """No FTD-corpus fallback (fixture CUSIPs are synthetic) and no cache bleed."""
    h13f.clear_snapshot_cache()
    monkeypatch.setattr(dollar_flows, "_get_cusip_ticker_map", lambda: None)
    yield
    h13f.clear_snapshot_cache()


@pytest.fixture()
def engine():
    return s.make_sqlite_engine()


def _by_key(snapshot):
    return {(h.manager_cik, h.ticker): h for h in snapshot.holdings}


def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc)


# ── Labels and the writer agree ────────────────────────────────────────────

def test_reader_labels_are_the_writers_labels():
    from ingestion.altdata import institutional_flows

    assert h13f.WRITER_OBSERVATION_MEASURE == institutional_flows.OBSERVATION_MEASURE
    puller = institutional_flows.InstitutionalFlowsPuller.__new__(institutional_flows.InstitutionalFlowsPuller)

    def holding(value, shares):
        return {"value_usd": value, "shares": shares, "value_units": "usd", "value_multiplier": 1,
                "share_type": "SH", "name": "X"}

    changes = puller._compare_holdings(
        {"C1": holding(100, 10), "C2": holding(100, 10), "C3": holding(100, 10)},
        {"C1": holding(300, 30), "C2": holding(10, 1), "C4": holding(100, 10)},
    )
    kinds = {c["action"]: c["observation_kind"] for c in changes}
    assert kinds == dict(h13f._KIND_FOR_ACTION)
    assert all(c["qualified_for_directional_flow"] is False for c in changes)


# ── Writer -> reader: filing and receipt dates kept ────────────────────────

def test_writer_to_reader_keeps_filing_and_receipt_dates(engine, monkeypatch):
    results = s.run_writer(engine, monkeypatch, s.two_quarter_scenario())
    assert [r["status"] for r in results] == ["SUCCESS", "SUCCESS"]
    curr_report, curr_filed = s.quarter_dates(0)
    received = datetime.combine(curr_filed + timedelta(days=1), datetime.min.time()).replace(hour=6)
    assert s.set_receipt(engine, curr_filed, received) == 6

    snap = h13f.read_13f_holdings_snapshot(engine)
    held = _by_key(snap)
    assert set(held) == {(s.CIK_ALPHA, s.ROOT), (s.CIK_ALPHA, s.PEER_A), (s.CIK_BETA, s.ROOT)}

    alpha_root = held[(s.CIK_ALPHA, s.ROOT)]
    assert alpha_root.reported_value_usd == 260_000.0 and alpha_root.shares == 2_000
    assert alpha_root.filing_date == curr_filed
    assert alpha_root.value_report_date == curr_report == alpha_root.held_through_report_date
    assert alpha_root.received_at == _utc(received)
    assert alpha_root.filing_accession == "9000000001-26-000002"
    assert alpha_root.observation_kind == "filing_appearance" and alpha_root.carried_forward is False
    assert held[(s.CIK_ALPHA, s.PEER_A)].reported_value_usd == 120_000.0
    assert held[(s.CIK_BETA, s.ROOT)].reported_value_usd == 75_000.0

    shown = alpha_root.to_dict()
    assert shown["filing_date"] == curr_filed.isoformat()
    assert shown["received_at"] == _utc(received).isoformat()
    assert shown["measure"] == h13f.HOLDING_MEASURE and shown["directional"] is False
    assert "action" not in shown

    # Alpha's PEER_B and Beta's PEER_A were CLOSED; the unmapped CUSIP (issuer
    # "APPLE INC") is counted, never guessed from the issuer name.
    assert snap.counts["closed"] == 2
    assert snap.counts["unmapped_cusip"] == 1
    assert all(h.ticker != "AAPL" for h in snap.holdings)
    assert snap.read_ok is True


def test_holding_is_visible_only_from_its_filing_date_and_receipt(engine, monkeypatch):
    s.run_writer(engine, monkeypatch, s.two_quarter_scenario())
    _report, filed = s.quarter_dates(0)
    received = datetime.combine(filed + timedelta(days=2), datetime.min.time()).replace(hour=6)
    s.set_receipt(engine, filed, received)

    assert h13f.read_13f_holdings_snapshot(engine, as_of=filed - timedelta(days=1)).holdings == ()
    # Filed but not yet received by GRID.
    on_filing_day = h13f.read_13f_holdings_snapshot(engine, as_of=filed)
    assert on_filing_day.holdings == () and on_filing_day.counts.get("rows_read", 0) == 0
    assert h13f.read_13f_holdings_snapshot(engine, as_of=filed + timedelta(days=2)).holdings != ()
    before = h13f.read_13f_holdings_snapshot(engine, as_of_ts=_utc(received) - timedelta(seconds=1))
    at = h13f.read_13f_holdings_snapshot(engine, as_of_ts=_utc(received))
    assert before.holdings == () and len(at.holdings) == 3


def test_closed_ends_a_holding_and_the_latest_filing_wins(engine, monkeypatch):
    """X is held at Q-1; Q0's filing drops it (CLOSED). As of Q-1's filing
    it is held; as of now it is not, and it is counted as closed."""
    edgar = s.FakeEdgar()
    q2_report, q2_filed = s.quarter_dates(2)
    q1_report, q1_filed = s.quarter_dates(1)
    q0_report, q0_filed = s.quarter_dates(0)
    edgar.add(s.CIK_ALPHA, s.Filing("A-2", q2_filed, q2_report, {s.ROOT_CUSIP: ("ROOT", 10_000, 100),
                                                                  s.PEER_A_CUSIP: ("PA", 5_000, 50)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", q1_filed, q1_report, {s.ROOT_CUSIP: ("ROOT", 30_000, 300),
                                                                  s.PEER_A_CUSIP: ("PA", 5_000, 50)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0", q0_filed, q0_report, {s.PEER_A_CUSIP: ("PA", 20_000, 200)}))
    filers = {s.CIK_ALPHA: s.ALPHA}

    edgar.today = q1_filed
    s.run_writer(engine, monkeypatch, edgar, filers)
    s.set_receipt(engine, q1_filed, datetime.combine(q1_filed, datetime.min.time()).replace(hour=6))
    edgar.today = date.today()
    s.run_writer(engine, monkeypatch, edgar, filers)

    then = _by_key(h13f.read_13f_holdings_snapshot(engine, as_of=q1_filed))
    assert then[(s.CIK_ALPHA, s.ROOT)].reported_value_usd == 30_000.0
    assert then[(s.CIK_ALPHA, s.ROOT)].value_report_date == q1_report

    now = h13f.read_13f_holdings_snapshot(engine)
    assert (s.CIK_ALPHA, s.ROOT) not in _by_key(now)
    assert now.counts["closed"] == 1
    assert _by_key(now)[(s.CIK_ALPHA, s.PEER_A)].value_report_date == q0_report


def test_unchanged_position_carries_forward_through_compared_filings(engine, monkeypatch):
    """ROOT is NEW at Q-1 and within 20 % at Q0 (no Q0 row). Both filing
    pairs were compared, so it is held through Q0, valued at Q-1."""
    edgar = s.FakeEdgar()
    q2_report, q2_filed = s.quarter_dates(2)
    q1_report, q1_filed = s.quarter_dates(1)
    q0_report, q0_filed = s.quarter_dates(0)
    edgar.add(s.CIK_ALPHA, s.Filing("A-2", q2_filed, q2_report, {s.PEER_A_CUSIP: ("PA", 1_000, 10)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", q1_filed, q1_report, {s.ROOT_CUSIP: ("ROOT", 50_000, 500),
                                                                  s.PEER_A_CUSIP: ("PA", 1_000, 10)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0", q0_filed, q0_report, {s.ROOT_CUSIP: ("ROOT", 66_000, 550),
                                                                  s.PEER_A_CUSIP: ("PA", 9_000, 90)}))
    filers = {s.CIK_ALPHA: s.ALPHA}
    edgar.today = q1_filed
    s.run_writer(engine, monkeypatch, edgar, filers)
    edgar.today = date.today()
    s.run_writer(engine, monkeypatch, edgar, filers)

    held = _by_key(h13f.read_13f_holdings_snapshot(engine))
    root = held[(s.CIK_ALPHA, s.ROOT)]
    assert root.carried_forward is True
    assert root.value_report_date == q1_report and root.held_through_report_date == q0_report
    assert root.reported_value_usd == 50_000.0
    peer = held[(s.CIK_ALPHA, s.PEER_A)]
    assert peer.carried_forward is False and peer.value_report_date == q0_report


def test_a_gap_in_compared_filings_leaves_older_positions_out(engine, monkeypatch):
    """The writer compared Q-3 -> Q-2, then nothing until Q-1 -> Q0. ROOT,
    last seen at Q-2, could have closed at Q-1 unseen: left out, counted."""
    edgar = s.FakeEdgar()
    dates = {q: s.quarter_dates(q) for q in range(4)}
    holdings = {
        3: {s.PEER_A_CUSIP: ("PA", 1_000, 10)},
        2: {s.ROOT_CUSIP: ("ROOT", 50_000, 500), s.PEER_A_CUSIP: ("PA", 1_000, 10)},
        1: {s.ROOT_CUSIP: ("ROOT", 50_000, 500), s.PEER_A_CUSIP: ("PA", 1_000, 10)},
        0: {s.ROOT_CUSIP: ("ROOT", 52_000, 510), s.PEER_A_CUSIP: ("PA", 9_000, 90)},
    }
    for q in (3, 2):
        edgar.add(s.CIK_ALPHA, s.Filing(f"A-{q}", dates[q][1], dates[q][0], holdings[q]))
    filers = {s.CIK_ALPHA: s.ALPHA}
    edgar.today = dates[2][1]
    s.run_writer(engine, monkeypatch, edgar, filers)
    for q in (1, 0):
        edgar.add(s.CIK_ALPHA, s.Filing(f"A-{q}", dates[q][1], dates[q][0], holdings[q]))
    edgar.today = date.today()
    s.run_writer(engine, monkeypatch, edgar, filers)

    snap = h13f.read_13f_holdings_snapshot(engine)
    assert set(_by_key(snap)) == {(s.CIK_ALPHA, s.PEER_A)}
    assert snap.counts["unverified_continuity"] == 1


def test_a_later_vintage_of_the_same_filing_wins_and_receipt_picks_the_vintage(engine, monkeypatch):
    """The writer re-pulls the same filing pair (a new vintage per series and
    filing date). The newest vintage received by the as-of instant wins."""
    edgar = s.two_quarter_scenario()
    s.run_writer(engine, monkeypatch, edgar, {s.CIK_ALPHA: s.ALPHA})
    _report, filed = s.quarter_dates(0)
    first = datetime.combine(filed + timedelta(days=1), datetime.min.time()).replace(hour=6)
    s.set_receipt(engine, filed, first)

    curr = edgar.filings[s.CIK_ALPHA][1]
    curr.holdings[s.ROOT_CUSIP] = ("ROOT HOLDINGS INC", 280_000, 2_000)
    s.run_writer(engine, monkeypatch, edgar, {s.CIK_ALPHA: s.ALPHA})

    live = _by_key(h13f.read_13f_holdings_snapshot(engine))
    assert live[(s.CIK_ALPHA, s.ROOT)].reported_value_usd == 280_000.0
    older = _by_key(h13f.read_13f_holdings_snapshot(engine, as_of_ts=_utc(first) + timedelta(hours=1)))
    assert older[(s.CIK_ALPHA, s.ROOT)].reported_value_usd == 260_000.0


def _index_with_amendment_type(monkeypatch, edgar):
    """EDGAR's submissions index carries no amendment type, so the writer
    skips amendments it cannot verify. Supply the index's claim (RESTATEMENT)
    through the writer's own index hook; the cover page is still checked."""
    from ingestion.altdata import institutional_flows

    def typed(self, cik, filing_type="13F-HR", count=4):
        raw = []
        for f in edgar._visible(cik):
            entry = {"accession": f.accession, "filing_date": f.filing_date.isoformat(),
                     "report_date": f.report_date.isoformat(), "form": f.form,
                     "is_amendment": f.form.endswith("/A")}
            if entry["is_amendment"]:
                entry["amendment_type"] = "RESTATEMENT"
            raw.append(entry)
        return institutional_flows._select_latest_filings_by_report_date(raw, count=count)

    monkeypatch.setattr(institutional_flows.InstitutionalFlowsPuller, "_fetch_13f_index", typed)


def test_a_restatement_replaces_the_original_filing(engine, monkeypatch):
    """A verified 13F-HR/A RESTATEMENT replaces the original for its quarter.
    EDGAR's submissions index carries no amendment type, so the writer
    skips amendments it cannot verify; this test supplies the type through
    the writer's own index hook and keeps everything else real."""
    edgar = s.FakeEdgar()
    q1_report, q1_filed = s.quarter_dates(1)
    q0_report, q0_filed = s.quarter_dates(0)
    restated_filed = q0_filed + timedelta(days=3)
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", q1_filed, q1_report, {s.PEER_A_CUSIP: ("PA", 1_000, 10)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0", q0_filed, q0_report, {s.PEER_A_CUSIP: ("PA", 9_000, 90),
                                                                  s.ROOT_CUSIP: ("ROOT", 40_000, 400)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0R", restated_filed, q0_report, {s.PEER_A_CUSIP: ("PA", 9_000, 90)},
                                    form="13F-HR/A"))
    _index_with_amendment_type(monkeypatch, edgar)
    filers = {s.CIK_ALPHA: s.ALPHA}
    edgar.today = q0_filed
    s.run_writer(engine, monkeypatch, edgar, filers)
    s.set_receipt(engine, q0_filed, datetime.combine(q0_filed, datetime.min.time()).replace(hour=6))
    edgar.today = date.today()
    s.run_writer(engine, monkeypatch, edgar, filers)

    before = _by_key(h13f.read_13f_holdings_snapshot(engine, as_of=q0_filed))
    assert (s.CIK_ALPHA, s.ROOT) in before
    after = h13f.read_13f_holdings_snapshot(engine)
    assert (s.CIK_ALPHA, s.ROOT) not in _by_key(after)
    assert _by_key(after)[(s.CIK_ALPHA, s.PEER_A)].filing_accession == "A-0R"
    assert after.counts["superseded_filing"] >= 1


def _insert_row(engine, series_id, obs_date, payload, *, received=None):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status) "
                "VALUES (:sid, 7, :d, :ts, 1, :p, 'SUCCESS')"
            ),
            {"sid": series_id, "d": obs_date, "ts": received or datetime(2026, 1, 1),
             "p": json.dumps(payload)},
        )


def test_unlabelled_and_directional_looking_rows_are_left_out_and_counted(engine, monkeypatch):
    """Rows not written by the labelled writer (the pre-#1369 payload, a row
    claiming qualified_for_directional_flow=True, a 3-part summary key) are
    inserted directly because the current writer cannot produce them."""
    s.run_writer(engine, monkeypatch, s.two_quarter_scenario())
    _report, filed = s.quarter_dates(0)
    received = datetime.combine(filed, datetime.min.time()).replace(hour=7)
    cik = "9000000003"
    _insert_row(engine, f"13F:{cik}:{s.ROOT_CUSIP}:NEW", filed, {
        "manager_cik": cik, "manager_name": "Legacy Fund", "cusip": s.ROOT_CUSIP, "action": "NEW",
        "pct_change": None, "shares": "100", "filing_accession": "L-1",
    }, received=received)
    forged = {
        "manager_cik": cik, "manager_name": "Forged Fund", "cusip": s.PEER_A_CUSIP, "action": "NEW",
        "measure": h13f.WRITER_OBSERVATION_MEASURE, "observation_kind": "filing_appearance",
        "qualified_for_directional_flow": True, "filing_date": filed.isoformat(),
        "report_date": (filed - timedelta(days=45)).isoformat(), "filing_accession": "F-1",
        "curr_value_usd": 9e9, "curr_shares": 1, "value_units": "usd", "value_multiplier": 1,
    }
    _insert_row(engine, f"13F:{cik}:{s.PEER_A_CUSIP}:NEW", filed, forged, received=received)
    _insert_row(engine, f"13F:{cik}:TOTAL_VALUE", filed, {"value": 1}, received=received)

    snap = h13f.read_13f_holdings_snapshot(engine)
    assert {h.manager_cik for h in snap.holdings} == {s.CIK_ALPHA, s.CIK_BETA}
    assert snap.counts["not_observation_labelled"] == 2
    assert snap.counts["not_position_series"] == 1


def test_a_manager_whose_latest_filing_is_stale_is_left_out(engine, monkeypatch):
    edgar = s.FakeEdgar()
    old_prev, old_prev_filed = s.quarter_dates(4)
    old_report, old_filed = s.quarter_dates(3)
    edgar.add(s.CIK_ALPHA, s.Filing("A-4", old_prev_filed, old_prev, {s.PEER_A_CUSIP: ("PA", 1_000, 10)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-3", old_filed, old_report, {s.ROOT_CUSIP: ("ROOT", 5_000, 50),
                                                                    s.PEER_A_CUSIP: ("PA", 1_000, 10)}))
    s.run_writer(engine, monkeypatch, edgar, {s.CIK_ALPHA: s.ALPHA})
    assert (date.today() - old_report).days > h13f.MAX_REPORT_AGE_DAYS

    snap = h13f.read_13f_holdings_snapshot(engine)
    assert snap.holdings == ()
    assert snap.counts["managers_stale_filing"] == 1


def test_the_shared_mapping_and_its_fallback_are_used(engine, monkeypatch):
    s.run_writer(engine, monkeypatch, s.two_quarter_scenario())

    class _Map:
        def lookup(self, cusip):
            return {s.UNMAPPED_CUSIP: "UNMPX"}.get(cusip)

    without = h13f.read_13f_holdings_snapshot(engine, cusip_map=None)
    assert without.counts["unmapped_cusip"] == 1
    with_map = h13f.read_13f_holdings_snapshot(engine, cusip_map=_Map())
    assert (s.CIK_ALPHA, "UNMPX") in _by_key(with_map)
    assert "unmapped_cusip" not in with_map.counts


def test_a_failed_read_shows_nothing():
    snap = h13f.read_13f_holdings_snapshot(create_engine("sqlite://"))
    assert snap.read_ok is False and snap.holdings == ()
    layer = deep_graph._layer9_cross_holding_funds(create_engine("sqlite://"), s.ROOT, [s.PEER_A])
    assert layer.actors == [] and layer.observation_meta["read_ok"] is False


# ── Holdings graph and served routes ───────────────────────────────────────

def test_layer9_shows_funds_listing_root_and_a_connected_company(engine, monkeypatch):
    s.run_writer(engine, monkeypatch, s.two_quarter_scenario())
    layer = deep_graph._layer9_cross_holding_funds(engine, s.ROOT, [s.PEER_A, s.PEER_B])

    assert [a["label"] for a in layer.actors] == [s.ALPHA]  # Beta's PEER_A was CLOSED
    fund = layer.actors[0]
    assert fund["shared_tickers"] == [s.PEER_A, s.ROOT] and fund["manager_cik"] == s.CIK_ALPHA
    assert fund["observation"]["directional"] is False
    assert fund["observation"]["observed_holdings_value_usd"] == 380_000.0
    assert "total_value" not in fund and "dollar_amount" not in fund
    assert layer.dollar_flows == 0.0 and layer.observed_holdings_value_usd == 380_000.0
    edges = {c["to"]: c for c in layer.connections}
    assert set(edges) == {f"company:{s.PEER_A}", f"company:{s.ROOT}"}
    assert edges[f"company:{s.ROOT}"]["observation"]["observed_holding_value_usd"] == 260_000.0
    assert "amount" not in edges[f"company:{s.ROOT}"]


@pytest.fixture()
def served(engine, monkeypatch):
    """The real graph and route on the fixture engine. Only ensure_table is a
    no-op here: graph_overlaps' DDL is PostgreSQL-only (the PG contract runs
    it for real)."""
    from api.routers import intelligence_companies as companies

    monkeypatch.setattr(companies, "get_db_engine", lambda: engine)
    monkeypatch.setattr(deep_graph, "ensure_table", lambda _e: None)
    companies._deep_graph_cache.clear()
    yield companies
    companies._deep_graph_cache.clear()


def test_served_deep_graph_and_map_display_the_writer_observation(engine, monkeypatch, served):
    s.run_writer(engine, monkeypatch, s.two_quarter_scenario())
    campaign = s.seed_upstream_layers(engine, s.ROOT, [s.PEER_A, s.PEER_B])
    _report, filed = s.quarter_dates(0)

    response = json.loads(json.dumps(served.get_deep_graph(ticker=s.ROOT, depth=10, _token="t")))
    drill = response["drill"]
    layer9 = next(layer for layer in drill["layers"] if layer["depth"] == 9)
    assert [a["label"] for a in layer9["actors"]] == [s.ALPHA]
    holdings = {h["ticker"]: h for h in layer9["actors"][0]["holdings"]}
    assert holdings[s.ROOT]["filing_date"] == filed.isoformat()
    assert holdings[s.ROOT]["received_at"]
    assert holdings[s.ROOT]["filing_accession"] == "9000000001-26-000002"
    assert holdings[s.ROOT]["reported_value_usd"] == 260_000.0
    assert layer9["dollar_flows"] == 0.0 and layer9["observed_holdings_value_usd"] == 380_000.0
    assert layer9["holdings_observation"]["excluded"]["closed"] == 2
    assert layer9["holdings_observation"]["excluded"]["unmapped_cusip"] == 1
    # The only dollar flow on this graph is the campaign contribution.
    assert drill["total_dollar_flow"] == pytest.approx(campaign)

    graph_map = json.loads(json.dumps(served.get_connection_map(ticker=s.ROOT, depth=10, _token="t")))
    fund_node = next(n for n in graph_map["nodes"] if n["id"] == "fund:fixture_capital_alpha_lp")
    assert fund_node["dollar_amount"] == 0 and fund_node["observation"]["directional"] is False
    links = [link for link in graph_map["links"] if link["type"] == "holds_position"]
    assert {link["target"] for link in links} == {f"company:{s.PEER_A}", f"company:{s.ROOT}"}
    assert all(link["amount"] == 0 and link["observation"]["measure"] == h13f.HOLDING_MEASURE for link in links)


# ── The same observation never reaches a directional consumer ──────────────

_DIRECTIONAL_MODULES = (
    "intelligence/dollar_flows.py",
    "analysis/flow_aggregator.py",
    "intelligence/sentiment_scorer.py",
    "intelligence/signal_convergence_scanner.py",
    "intelligence/wealth_tracker.py",
    "intelligence/pocket_lining.py",
    "intelligence/trust_scorer.py",
    "analysis/money_flow_engine/layer_institutional.py",
)


def test_no_directional_consumer_imports_the_holdings_reader():
    for rel in _DIRECTIONAL_MODULES:
        src = (REPO / rel).read_text()
        assert "holdings_observations_13f" not in src, rel
        assert "_layer9_cross_holding_funds" not in src, rel


def _insert_control_flow(engine, ticker, flow_date):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dollar_flows (source_type, actor_name, ticker, amount_usd, direction, "
                "confidence, evidence, flow_date) VALUES ('insider', 'Jane Fixture', :t, 1000000, "
                "'inflow', 'confirmed', '{}', :d)"
            ),
            {"t": ticker, "d": flow_date},
        )


def test_the_shown_observation_never_reaches_a_directional_consumer(engine, monkeypatch, served):
    from analysis import flow_aggregator
    from analysis.money_flow_engine import layer_institutional
    from intelligence import (
        pocket_lining,
        sentiment_scorer,
        signal_convergence_scanner,
        wealth_tracker,
    )

    s.run_writer(engine, monkeypatch, s.two_quarter_scenario())
    s.seed_upstream_layers(engine, s.ROOT, [s.PEER_A, s.PEER_B])
    _report, filed = s.quarter_dates(0)

    # #1369's normalizer stores the same NEW filing appearance as an
    # 'observed' dollar_flows row (never inflow/outflow).
    dollar_flows.normalize_all_flows(engine, days=90)
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT ticker, amount_usd, direction, evidence FROM dollar_flows WHERE source_type = '13f'"
        )).fetchall()
    observed = [r for r in rows if r[0] == s.ROOT and json.loads(r[3])["filing_accession"] == "9000000001-26-000002"]
    assert len(observed) == 1 and observed[0][1] == 260_000.0 and observed[0][2] == "observed"
    assert {r[2] for r in rows} == {"observed"}

    # The graph shows that same observation.
    response = served.get_deep_graph(ticker=s.ROOT, depth=10, _token="t")
    layer9 = next(layer for layer in response["drill"]["layers"] if layer["depth"] == 9)
    shown = {h["ticker"]: h for h in layer9["actors"][0]["holdings"]}[s.ROOT]
    assert (shown["filing_accession"], shown["reported_value_usd"]) == ("9000000001-26-000002", 260_000.0)

    # A non-13F control row on the same ticker: each reader still reads it,
    # so each exclusion below is the 13F rule and not an empty table. The
    # synthetic tickers get a sector so the sector readers have rows to read.
    _insert_control_flow(engine, s.ROOT, filed)
    control = [("insider", 1_000_000.0)]
    sector = "Fixture Sector"
    fixture_sectors = {s.ROOT: sector, s.PEER_A: sector, s.PEER_B: sector}
    real_lookup = dollar_flows._build_sector_lookup
    monkeypatch.setattr(dollar_flows, "_build_sector_lookup", lambda: {**real_lookup(), **fixture_sectors})
    monkeypatch.setattr(dollar_flows, "_SECTOR_LOOKUP", None)
    monkeypatch.setattr(flow_aggregator, "_SECTOR_LOOKUP", {**flow_aggregator._build_sector_lookup(),
                                                            **fixture_sectors})

    agg = dollar_flows.get_aggregate_flows(engine, days=30)
    assert set(agg["by_source_type"]) == {"insider"} and agg["quarantined_13f"] >= 1
    assert agg["total_inflow"] == pytest.approx(1_000_000.0) and agg["total_outflow"] == 0.0
    for got in (
        dollar_flows.get_flows_by_ticker(engine, s.ROOT, days=30),
        dollar_flows.get_flows_by_sector(engine, sector, days=30),
        dollar_flows.get_biggest_movers(engine, days=30),
    ):
        assert [(r["source_type"], r["amount_usd"]) for r in got] == control

    flow_aggregator._flow_cache.clear()
    try:
        fetched = flow_aggregator._fetch_flows(engine, 30)
        tiers = flow_aggregator.aggregate_by_actor_tier(engine, days=30)
        sectors = flow_aggregator.aggregate_by_sector(engine, days=30)
        momentum = flow_aggregator.compute_flow_momentum(engine, s.ROOT)
    finally:
        flow_aggregator._flow_cache.clear()
    assert [(f["source_type"], f["amount_usd"]) for f in fetched] == control
    assert sum(t["inflow"] + t["outflow"] for t in tiers.values()) == pytest.approx(1_000_000.0)
    assert sectors[sector]["inflow"] == pytest.approx(1_000_000.0) and sectors[sector]["outflow"] == 0.0
    assert momentum["flow_count"] == 1

    component = sentiment_scorer._score_flows(engine, days=30)
    assert component.raw_value == pytest.approx(1.0)

    assert layer_institutional._build_13f_aggregate(engine, date.today()).value is None
    assert signal_convergence_scanner._scan_institutional(engine, s.ROOT, date.today(), 120, {}) is None

    # Nothing on this path wrote signal_sources (the trust scorer's,
    # wealth tracker's and pocket-lining check's only input): the one row
    # there is the seeded campaign contribution.
    with engine.connect() as conn:
        types = [r[0] for r in conn.execute(text("SELECT source_type FROM signal_sources")).fetchall()]
    assert types == ["campaign_finance"]
    monkeypatch.setattr("intelligence.actor_network._ensure_tables", lambda _e: None)
    assert wealth_tracker.track_wealth_migration(engine, days=120) == []
    flags = pocket_lining.assess_pocket_lining(engine)
    assert not any(s.ALPHA in json.dumps(f) or s.ROOT in json.dumps(f) for f in flags)


def test_fund_concentration_narrative_claims_no_direction(monkeypatch):
    overlaps = [
        deep_graph.Overlap(actor_a=a, actor_b=b, connection_point=s.ALPHA, shared_funds=[s.ALPHA])
        for a, b in (("AAA", "BBB"), ("BBB", "CCC"))
    ]
    monkeypatch.setattr(deep_graph, "ensure_table", lambda _e: None)
    monkeypatch.setattr(deep_graph, "_get_watchlist_tickers", lambda _e: ["AAA", "BBB", "CCC"])
    monkeypatch.setattr(deep_graph, "find_all_overlaps", lambda _e, _t: overlaps)
    found = [d for d in deep_graph.discover_hidden_influence(create_engine("sqlite://"))
             if d["type"] == "fund_concentration"]
    assert len(found) == 1
    narrative = found[0]["narrative"].lower()
    for word in ("selling pressure", "buying pressure", "holds significant", "redemption"):
        assert word not in narrative
    assert "says nothing about buying or selling" in narrative


# ── Codex HOLD 6016240483: negative controls, real writer -> served graph ──

def _served_layer9(companies):
    """Layer 9 of the served deep-graph route, caches cleared first."""
    h13f.clear_snapshot_cache()
    companies._deep_graph_cache.clear()
    response = json.loads(json.dumps(companies.get_deep_graph(ticker=s.ROOT, depth=10, _token="t")))
    return next(layer for layer in response["drill"]["layers"] if layer["depth"] == 9)


def _receipt(engine, accession):
    rows = [p for sid, p in s.series_rows(engine, "13F-FILING:%") if sid.endswith(f":{accession}")]
    assert rows, f"no receipt for {accession}"
    return rows[-1]


def _change_rows(engine, filing_date):
    return [(sid, p) for sid, p in s.series_rows(engine, "13F:%") if p.get("filing_date") == filing_date.isoformat()]


def test_a_no_delta_restatement_withdraws_the_original_through_the_served_graph(engine, monkeypatch, served):
    """Codex's case: Q1 lists Y; Q2's original adds X; Q2's restatement goes
    back to only Y, unchanged, so comparing it emits no change row. Its
    receipt still names it the quarter's filing, so X is no longer shown."""
    edgar = s.FakeEdgar()
    q1_report, q1_filed = s.quarter_dates(1)
    q0_report, q0_filed = s.quarter_dates(0)
    restated_filed = q0_filed + timedelta(days=3)
    y_unchanged = {s.PEER_A_CUSIP: ("PEER A CORP", 1_000, 10)}
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", q1_filed, q1_report, dict(y_unchanged)))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0", q0_filed, q0_report, {
        s.PEER_A_CUSIP: ("PEER A CORP", 9_000, 90), s.ROOT_CUSIP: ("ROOT HOLDINGS INC", 40_000, 400)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0R", restated_filed, q0_report, dict(y_unchanged), form="13F-HR/A"))
    _index_with_amendment_type(monkeypatch, edgar)
    s.seed_upstream_layers(engine, s.ROOT, [s.PEER_A, s.PEER_B])
    filers = {s.CIK_ALPHA: s.ALPHA}

    edgar.today = q0_filed
    s.run_writer(engine, monkeypatch, edgar, filers)
    before = _served_layer9(served)
    assert [a["label"] for a in before["actors"]] == [s.ALPHA]
    assert s.ROOT in before["actors"][0]["shared_tickers"]

    edgar.today = date.today()
    results = s.run_writer(engine, monkeypatch, edgar, filers)
    after = _served_layer9(served)
    assert after["actors"] == [] and after["connections"] == []  # X is no longer claimed

    assert results[0]["status"] == "SUCCESS" and results[0]["changes_detected"] == 0
    assert _change_rows(engine, restated_filed) == []
    receipt = _receipt(engine, "A-0R")
    assert receipt["comparison_status"] == "compared" and receipt["changes_detected"] == 0
    assert receipt["curr_snapshot"]["status"] == "complete" and receipt["replaces_accession"] == "A-0"
    assert after["holdings_observation"]["excluded"]["superseded_filing"] == 2


_DEFECT_STATUS = {
    "missing": "missing_infotable",
    "malformed": "unparseable_infotable",
    "truncated": "entry_count_mismatch",
    "cover_missing": "missing_cover_page",
    "cover_malformed": "unparseable_cover_page",
}


def _defective(filing: s.Filing, defect: str) -> s.Filing:
    if defect.startswith("cover_"):
        filing.cover = defect.split("_", 1)[1]
    else:
        filing.infotable = defect
    return filing


@pytest.mark.parametrize("defect", list(_DEFECT_STATUS))
def test_an_unreadable_current_filing_closes_nothing_and_claims_nothing(engine, monkeypatch, served, defect):
    """Alpha's Q-1 holdings are verified. Q0's filing cannot be read in full:
    no CLOSED row is written (an unreadable table is not an empty one), the
    pair is withheld with a receipt naming why, and Q-1's holdings are not
    claimed as current."""
    edgar = s.FakeEdgar()
    dates = {q: s.quarter_dates(q) for q in range(3)}
    held = {s.PEER_A_CUSIP: ("PEER A CORP", 9_000, 90), s.ROOT_CUSIP: ("ROOT HOLDINGS INC", 40_000, 400)}
    edgar.add(s.CIK_ALPHA, s.Filing("A-2", dates[2][1], dates[2][0], {s.PEER_A_CUSIP: ("PEER A CORP", 1_000, 10)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", dates[1][1], dates[1][0], dict(held)))
    edgar.add(s.CIK_ALPHA, _defective(s.Filing("A-0", dates[0][1], dates[0][0], dict(held)), defect))
    s.seed_upstream_layers(engine, s.ROOT, [s.PEER_A, s.PEER_B])
    filers = {s.CIK_ALPHA: s.ALPHA}

    edgar.today = dates[1][1]
    s.run_writer(engine, monkeypatch, edgar, filers)
    assert [a["label"] for a in _served_layer9(served)["actors"]] == [s.ALPHA]

    edgar.today = date.today()
    results = s.run_writer(engine, monkeypatch, edgar, filers)
    assert not [sid for sid, _p in s.series_rows(engine, "13F:%") if sid.endswith(":CLOSED")]  # no false CLOSED
    assert _change_rows(engine, dates[0][1]) == []
    assert results[0]["status"] == "QUARANTINED" and results[0]["curr_snapshot"] == _DEFECT_STATUS[defect]
    receipt = _receipt(engine, "A-0")
    assert receipt["comparison_status"] == "withheld_incomplete"
    assert receipt["curr_snapshot"]["status"] == _DEFECT_STATUS[defect]
    assert receipt["prev_snapshot"]["status"] == "complete"

    snap = h13f.read_13f_holdings_snapshot(engine)
    assert snap.holdings == () and snap.counts["managers_latest_filing_unverified"] == 1
    layer9 = _served_layer9(served)
    assert layer9["actors"] == []
    assert layer9["holdings_observation"]["excluded"]["managers_latest_filing_unverified"] == 1


@pytest.mark.parametrize("defect", list(_DEFECT_STATUS))
def test_an_unreadable_prior_filing_opens_nothing_and_claims_nothing(engine, monkeypatch, served, defect):
    """The prior filing cannot be read in full: no NEW row is written for the
    current filing's holdings, and nothing is shown."""
    edgar = s.FakeEdgar()
    q1_report, q1_filed = s.quarter_dates(1)
    q0_report, q0_filed = s.quarter_dates(0)
    edgar.add(s.CIK_ALPHA, _defective(s.Filing("A-1", q1_filed, q1_report, {
        s.PEER_B_CUSIP: ("PEER B INC", 3_000, 30), s.PEER_A_CUSIP: ("PEER A CORP", 1_000, 10)}), defect))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0", q0_filed, q0_report, {
        s.PEER_A_CUSIP: ("PEER A CORP", 9_000, 90), s.ROOT_CUSIP: ("ROOT HOLDINGS INC", 40_000, 400)}))
    s.seed_upstream_layers(engine, s.ROOT, [s.PEER_A, s.PEER_B])

    results = s.run_writer(engine, monkeypatch, edgar, {s.CIK_ALPHA: s.ALPHA})
    assert s.series_rows(engine, "13F:%") == []  # no false NEW (or anything else)
    assert results[0]["status"] == "QUARANTINED" and results[0]["prev_snapshot"] == _DEFECT_STATUS[defect]
    receipt = _receipt(engine, "A-0")
    assert receipt["comparison_status"] == "withheld_incomplete"
    assert receipt["prev_snapshot"]["status"] == _DEFECT_STATUS[defect]

    snap = h13f.read_13f_holdings_snapshot(engine)
    assert snap.holdings == () and snap.counts["managers_latest_filing_unverified"] == 1
    assert _served_layer9(served)["actors"] == []


def test_a_verified_empty_filing_closes_every_position(engine, monkeypatch, served):
    """A cover page counting zero entries, with no information table, is a
    verified empty snapshot: the CLOSED rows are real and nothing is held."""
    edgar = s.FakeEdgar()
    q1_report, q1_filed = s.quarter_dates(1)
    q0_report, q0_filed = s.quarter_dates(0)
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", q1_filed, q1_report, {
        s.PEER_A_CUSIP: ("PEER A CORP", 9_000, 90), s.ROOT_CUSIP: ("ROOT HOLDINGS INC", 40_000, 400)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0", q0_filed, q0_report, {}, infotable="missing"))

    results = s.run_writer(engine, monkeypatch, edgar, {s.CIK_ALPHA: s.ALPHA})
    assert results[0]["status"] == "SUCCESS"
    assert sorted(sid.rsplit(":", 1)[1] for sid, _p in _change_rows(engine, q0_filed)) == ["CLOSED", "CLOSED"]
    receipt = _receipt(engine, "A-0")
    assert receipt["comparison_status"] == "compared" and receipt["curr_snapshot"]["status"] == "verified_empty"

    snap = h13f.read_13f_holdings_snapshot(engine)
    assert snap.holdings == () and snap.counts["closed"] == 2
    assert "managers_latest_filing_unverified" not in snap.counts


def test_an_amendment_the_cover_page_calls_new_holdings_is_withheld(engine, monkeypatch):
    """The index claims a restatement but the cover page says NEW HOLDINGS
    (a partial list): comparing it would close every omitted position."""
    edgar = s.FakeEdgar()
    q1_report, q1_filed = s.quarter_dates(1)
    q0_report, q0_filed = s.quarter_dates(0)
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", q1_filed, q1_report, {s.PEER_A_CUSIP: ("PA", 1_000, 10)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0", q0_filed, q0_report, {
        s.PEER_A_CUSIP: ("PA", 9_000, 90), s.ROOT_CUSIP: ("ROOT", 40_000, 400)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0N", q0_filed + timedelta(days=2), q0_report,
                                    {s.PEER_B_CUSIP: ("PB", 5_000, 50)}, form="13F-HR/A",
                                    amendment_type="NEW HOLDINGS"))
    _index_with_amendment_type(monkeypatch, edgar)

    results = s.run_writer(engine, monkeypatch, edgar, {s.CIK_ALPHA: s.ALPHA})
    assert results[0]["curr_snapshot"] == "amendment_not_verified_restatement"
    assert s.series_rows(engine, "13F:%") == []
    assert h13f.read_13f_holdings_snapshot(engine).holdings == ()


def test_change_rows_without_a_filing_receipt_are_withheld(engine, monkeypatch):
    """Rows the writer stored before it wrote receipts carry no evidence that
    their filing was read in full or still stands: none is shown."""
    s.run_writer(engine, monkeypatch, s.two_quarter_scenario())
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM raw_series WHERE series_id LIKE '13F-FILING:%'"))
    snap = h13f.read_13f_holdings_snapshot(engine)
    assert snap.holdings == () and snap.counts["no_filing_receipt"] == 6


def test_a_restated_prior_filing_retires_rows_from_the_old_comparison(engine, monkeypatch):
    """Q0 was first compared against Q-1's original, then against Q-1's
    restatement. A row only the old comparison produced (ROOT NEW: the
    restatement already listed ROOT) is no longer shown."""
    edgar = s.FakeEdgar()
    q1_report, q1_filed = s.quarter_dates(1)
    q0_report, q0_filed = s.quarter_dates(0)
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", q1_filed, q1_report, {s.PEER_A_CUSIP: ("PA", 1_000, 10)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0", q0_filed, q0_report, {
        s.PEER_A_CUSIP: ("PA", 9_000, 90), s.ROOT_CUSIP: ("ROOT", 40_000, 400)}))
    _index_with_amendment_type(monkeypatch, edgar)
    filers = {s.CIK_ALPHA: s.ALPHA}
    s.run_writer(engine, monkeypatch, edgar, filers)
    s.set_receipt(engine, q0_filed, datetime.combine(q0_filed, datetime.min.time()).replace(hour=6))
    assert (s.CIK_ALPHA, s.ROOT) in _by_key(h13f.read_13f_holdings_snapshot(engine))

    edgar.add(s.CIK_ALPHA, s.Filing("A-1R", q0_filed + timedelta(days=1), q1_report, {
        s.PEER_A_CUSIP: ("PA", 1_000, 10), s.ROOT_CUSIP: ("ROOT", 40_000, 400)}, form="13F-HR/A"))
    s.run_writer(engine, monkeypatch, edgar, filers)
    assert _receipt(engine, "A-0")["prev_filing_accession"] == "A-1R"

    snap = h13f.read_13f_holdings_snapshot(engine)
    assert set(_by_key(snap)) == {(s.CIK_ALPHA, s.PEER_A)}
    assert snap.counts["stale_comparison"] == 1


def test_the_writer_reads_every_information_table_namespace():
    from ingestion.altdata import institutional_flows

    entry = ("<infoTable><nameOfIssuer>X CORP</nameOfIssuer><cusip>FXROOT001</cusip><value>1000</value>"
             "<shrsOrPrnAmt><sshPrnamt>10</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt></infoTable>")
    for ns in (s.INFOTABLE_NS, "http://www.sec.gov/document/thirteenf", ""):
        xmlns = f' xmlns="{ns}"' if ns else ""
        holdings, entries, total = institutional_flows._parse_13f_infotable(
            f"<informationTable{xmlns}>{entry}{entry}</informationTable>".encode(), 1)
        assert (len(holdings), entries, total) == (2, 2, 2_000.0), ns
        assert holdings[0]["value_usd"] == 1_000 and holdings[0]["cusip"] == "FXROOT001"
    with pytest.raises(institutional_flows.ET.ParseError):
        institutional_flows._parse_13f_infotable(b"<informationTable><infoTable>", 1)
