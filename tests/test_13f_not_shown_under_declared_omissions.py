"""A position absent from a filing that declares confidential omissions is NOT_SHOWN, not CLOSED.

A 13F cover page can say confidential positions are omitted
(``isConfidentialOmitted``). A position of the prior quarter that such a
filing does not list may have been sold, or may be one of the omitted ones:
the filing cannot tell which. The comparison used to ignore the flag and write
CLOSED (a ``filing_disappearance`` with a flow estimate).

Production, read-only, 2026-11-27: 5 of 72 current filings declare omissions.
Three of them are compared today and carry 14 CLOSED rows that rest on absence
alone (Icahn 2, MSD Capital 1, Senator Investment Group 11).

Now such an absence is written as NOT_SHOWN: no disappearance is claimed and
no flow is estimated. The holdings reader still does not show the position as
held, and counts it apart from a closure; the dollar-flow normalizer emits
nothing for it, and lets it replace an older CLOSED of the same filing pair.

The real writer and readers run against the fake EDGAR of
``tests/thirteenf_holdings_support.py`` (network replaced only).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from ingestion.altdata import institutional_flows as inst
from ingestion.smart_scheduler import _classify_outcome
from intelligence import dollar_flows
from intelligence import holdings_observations_13f as h13f
from tests import thirteenf_holdings_support as s

ALPHA_ONLY = {s.CIK_ALPHA: s.ALPHA}


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    h13f.clear_snapshot_cache()
    monkeypatch.setattr(dollar_flows, "_get_cusip_ticker_map", lambda: None)
    yield
    h13f.clear_snapshot_cache()


@pytest.fixture()
def engine():
    return s.make_sqlite_engine()


def _three_quarters(current_declares_omissions: bool, prior_declares_omissions: bool = False) -> s.FakeEdgar:
    """Alpha: Q-2 holds PEER_A; Q-1 adds PEER_B (so PEER_B is an observed
    holding); Q0 lists PEER_A (increased) and ROOT (new) and not PEER_B."""
    edgar = s.FakeEdgar()
    dates = {q: s.quarter_dates(q) for q in range(3)}
    edgar.add(s.CIK_ALPHA, s.Filing("A-2", dates[2][1], dates[2][0], {
        s.PEER_A_CUSIP: ("PEER A CORP", 50_000, 500)}))
    edgar.add(s.CIK_ALPHA, s.Filing("A-1", dates[1][1], dates[1][0], {
        s.PEER_A_CUSIP: ("PEER A CORP", 50_000, 500),
        s.PEER_B_CUSIP: ("PEER B INC", 30_000, 300)}, confidential_omitted=prior_declares_omissions))
    edgar.add(s.CIK_ALPHA, s.Filing("A-0", dates[0][1], dates[0][0], {
        s.PEER_A_CUSIP: ("PEER A CORP", 120_000, 1_000),
        s.ROOT_CUSIP: ("ROOT HOLDINGS INC", 260_000, 2_000)}, confidential_omitted=current_declares_omissions))
    return edgar


def _run_both_quarters(engine, monkeypatch, edgar) -> list[dict]:
    """Q-2 -> Q-1 on Q-1's filing day, then Q-1 -> Q0 today. Returns the second run."""
    edgar.today = s.quarter_dates(1)[1]
    s.run_writer(engine, monkeypatch, edgar, ALPHA_ONLY)
    _age_rows(engine)
    edgar.today = date.today()
    return s.run_writer(engine, monkeypatch, edgar, ALPHA_ONLY)


def _age_rows(engine, days: int = 2) -> None:
    """Make stored 13F rows clearly older than the next run's (SQLite's
    CURRENT_TIMESTAMP has one-second resolution)."""
    base = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0) - timedelta(days=days)
    with engine.begin() as conn:
        ids = [r[0] for r in conn.execute(text(
            "SELECT id FROM raw_series WHERE series_id LIKE '13F%' AND pull_timestamp > :b ORDER BY id"),
            {"b": base})]
        for n, row_id in enumerate(ids):
            conn.execute(text("UPDATE raw_series SET pull_timestamp = :ts WHERE id = :id"),
                         {"ts": base + timedelta(seconds=n), "id": row_id})


def _q0_rows(engine) -> dict[str, dict]:
    filed = s.quarter_dates(0)[1].isoformat()
    return {sid: p for sid, p in s.series_rows(engine, "13F:%") if p.get("filing_date") == filed}


def _receipt(engine, accession: str) -> dict:
    return [p for sid, p in s.series_rows(engine, "13F-FILING:%") if sid.endswith(f":{accession}")][-1]


# ── The writer ─────────────────────────────────────────────────────────────


def test_absence_from_a_filing_that_declares_omissions_is_not_shown_and_claims_no_flow(engine, monkeypatch):
    results = _run_both_quarters(engine, monkeypatch, _three_quarters(current_declares_omissions=True))

    rows = _q0_rows(engine)
    actions = sorted(sid.rsplit(":", 1)[1] for sid in rows)
    assert actions == ["INCREASED", "NEW", "NOT_SHOWN"]                      # and no CLOSED
    row = rows[f"13F:{s.CIK_ALPHA}:{s.PEER_B_CUSIP}:NOT_SHOWN"]
    assert row["action"] == "NOT_SHOWN" and row["observation_kind"] == "absent_under_declared_omissions"
    assert row["unqualified_reason"] == "current_filing_declares_omissions"
    assert row["qualified_for_directional_flow"] is False
    assert row["estimated_flow_usd"] is None and row["holdings_value_delta_usd"] is None
    assert row["curr_value_usd"] is None and row["curr_shares"] is None and row["delta_shares"] is None
    assert row["prev_value_usd"] == 30_000.0 and row["prev_shares"] == 300    # what was last reported
    assert row["filing_accession"] == "A-0" and row["prev_filing_accession"] == "A-1"

    (alpha,) = results
    assert alpha["status"] == "SUCCESS" and alpha["current_declares_omissions"] is True and alpha["not_shown"] == 1
    receipt = _receipt(engine, "A-0")
    assert receipt["comparison_status"] == "compared"
    assert receipt["current_declares_omissions"] is True and receipt["not_shown_under_declared_omissions"] == 1
    assert _classify_outcome(results)[0] == "SUCCESS"


def test_control_without_declared_omissions_the_same_absence_is_closed(engine, monkeypatch):
    results = _run_both_quarters(engine, monkeypatch, _three_quarters(current_declares_omissions=False))

    rows = _q0_rows(engine)
    assert sorted(sid.rsplit(":", 1)[1] for sid in rows) == ["CLOSED", "INCREASED", "NEW"]
    closed = rows[f"13F:{s.CIK_ALPHA}:{s.PEER_B_CUSIP}:CLOSED"]
    assert closed["observation_kind"] == "filing_disappearance" and closed["estimated_flow_usd"] == 30_000.0
    assert results[0]["current_declares_omissions"] is False and results[0]["not_shown"] == 0
    assert _receipt(engine, "A-0")["current_declares_omissions"] is False


def test_only_the_current_filings_declaration_counts(engine, monkeypatch):
    """Omissions declared by the prior filing say nothing about what the
    current one lists."""
    _run_both_quarters(engine, monkeypatch,
                       _three_quarters(current_declares_omissions=False, prior_declares_omissions=True))

    assert f"13F:{s.CIK_ALPHA}:{s.PEER_B_CUSIP}:CLOSED" in _q0_rows(engine)


def test_a_rerun_writes_no_second_not_shown_row(engine, monkeypatch):
    edgar = _three_quarters(current_declares_omissions=True)
    _run_both_quarters(engine, monkeypatch, edgar)
    stored = len(s.series_rows(engine, "13F:%"))
    _age_rows(engine)

    again = s.run_writer(engine, monkeypatch, edgar, ALPHA_ONLY)

    assert len(s.series_rows(engine, "13F:%")) == stored
    assert again[0]["rows_inserted"] == 0 and again[0]["rows_unchanged"] == 3 and again[0]["not_shown"] == 1


def _by_cusip(cusip: str, value: float, shares: int) -> dict:
    return {cusip: {"cusip": cusip, "name": cusip, "value_usd": value, "shares": shares,
                    "share_type": "SH", "value_units": "usd"}}


def test_compare_holdings_marks_only_the_absent_side_and_leaves_the_rest():
    puller = inst.InstitutionalFlowsPuller.__new__(inst.InstitutionalFlowsPuller)
    prev = {**_by_cusip("GONE00001", 1_000.0, 10), **_by_cusip("KEPT00001", 2_000.0, 20)}
    curr = {**_by_cusip("KEPT00001", 3_000.0, 30), **_by_cusip("FRESH0001", 4_000.0, 40)}

    declared = {c["cusip"]: c for c in puller._compare_holdings(prev, curr, current_declares_omissions=True)}
    plain = {c["cusip"]: c for c in puller._compare_holdings(prev, curr)}

    assert declared["GONE00001"]["action"] == "NOT_SHOWN" and plain["GONE00001"]["action"] == "CLOSED"
    assert declared["GONE00001"]["value_usd"] == 1_000.0
    for cusip in ("KEPT00001", "FRESH0001"):
        assert declared[cusip] == plain[cusip]


# ── The holdings reader ────────────────────────────────────────────────────


def _snapshot(engine) -> h13f.HoldingsSnapshot:
    h13f.clear_snapshot_cache()
    return h13f.read_13f_holdings_snapshot(engine)


def _held(snapshot: h13f.HoldingsSnapshot) -> set[str]:
    return {h.ticker for h in snapshot.holdings if h.manager_cik == s.CIK_ALPHA}


def test_the_reader_does_not_show_a_not_shown_position_and_counts_it_apart_from_a_closure(engine, monkeypatch):
    edgar = _three_quarters(current_declares_omissions=True)
    edgar.today = s.quarter_dates(1)[1]
    s.run_writer(engine, monkeypatch, edgar, ALPHA_ONLY)
    _age_rows(engine)
    edgar.today = date.today()
    s.run_writer(engine, monkeypatch, edgar, ALPHA_ONLY)

    snapshot = _snapshot(engine)

    assert s.PEER_B not in _held(snapshot)                                   # hidden, not invented
    assert {s.ROOT, s.PEER_A} <= _held(snapshot)
    assert snapshot.counts.get("not_shown_omissions_declared") == 1
    assert snapshot.counts.get("closed", 0) == 0                             # no closure is claimed


def test_control_the_reader_counts_a_real_disappearance_as_closed(engine, monkeypatch):
    _run_both_quarters(engine, monkeypatch, _three_quarters(current_declares_omissions=False))

    snapshot = _snapshot(engine)

    assert s.PEER_B not in _held(snapshot)
    assert snapshot.counts.get("closed") == 1 and snapshot.counts.get("not_shown_omissions_declared", 0) == 0


def test_the_position_was_shown_before_the_filing_that_omits_it(engine, monkeypatch):
    """Control for the two tests above: PEER_B is an observed holding after Q-1."""
    edgar = _three_quarters(current_declares_omissions=True)
    edgar.today = s.quarter_dates(1)[1]
    s.run_writer(engine, monkeypatch, edgar, ALPHA_ONLY)

    assert s.PEER_B in _held(_snapshot(engine))


# ── Rows written before this change said CLOSED ────────────────────────────


def _closed_then_not_shown(engine, monkeypatch) -> None:
    """The stored state production has today: the old writer ignored the cover
    page and wrote CLOSED; the new writer then reads the same filings."""
    edgar = _three_quarters(current_declares_omissions=True)
    real = inst.InstitutionalFlowsPuller._compare_holdings

    def ignoring_the_cover(self, prev, curr, *, quarantine_counts=None, current_declares_omissions=False):
        return real(self, prev, curr, quarantine_counts=quarantine_counts)

    with monkeypatch.context() as old_writer:
        old_writer.setattr(inst.InstitutionalFlowsPuller, "_compare_holdings", ignoring_the_cover)
        _run_both_quarters(engine, old_writer, edgar)
    assert f"13F:{s.CIK_ALPHA}:{s.PEER_B_CUSIP}:CLOSED" in _q0_rows(engine)
    _age_rows(engine)
    s.run_writer(engine, monkeypatch, edgar, ALPHA_ONLY)


def test_a_later_not_shown_outranks_the_closed_row_the_old_writer_left_in_the_reader(engine, monkeypatch):
    _closed_then_not_shown(engine, monkeypatch)

    rows = _q0_rows(engine)
    assert f"13F:{s.CIK_ALPHA}:{s.PEER_B_CUSIP}:CLOSED" in rows              # kept: nothing is rewritten
    assert f"13F:{s.CIK_ALPHA}:{s.PEER_B_CUSIP}:NOT_SHOWN" in rows
    snapshot = _snapshot(engine)
    assert snapshot.counts.get("not_shown_omissions_declared") == 1 and snapshot.counts.get("closed", 0) == 0
    assert s.PEER_B not in _held(snapshot)


# ── The dollar-flow normalizer ─────────────────────────────────────────────


def _flow_actions(engine) -> tuple[dict[str, str], dict[str, int]]:
    skipped: dict[str, int] = {}
    flows = dollar_flows._normalize_13f_flows(engine, days=400, skipped_counts=skipped)
    return {f["ticker"]: f["evidence"]["action"] for f in flows if f["evidence"].get("filing_accession") == "A-0"}, skipped


def test_a_not_shown_position_is_not_a_dollar_flow(engine, monkeypatch):
    _run_both_quarters(engine, monkeypatch, _three_quarters(current_declares_omissions=True))

    actions, skipped = _flow_actions(engine)

    assert s.PEER_B not in actions and skipped["not_shown_13f"] == 1
    assert actions[s.ROOT] == "NEW"                                          # the rest is unchanged


def test_control_a_real_disappearance_is_still_an_observed_dollar_flow(engine, monkeypatch):
    _run_both_quarters(engine, monkeypatch, _three_quarters(current_declares_omissions=False))

    actions, skipped = _flow_actions(engine)

    assert actions[s.PEER_B] == "CLOSED" and skipped["not_shown_13f"] == 0


def test_the_old_closed_row_no_longer_reaches_dollar_flows_once_not_shown_is_written(engine, monkeypatch):
    _closed_then_not_shown(engine, monkeypatch)

    actions, skipped = _flow_actions(engine)

    assert s.PEER_B not in actions and skipped["not_shown_13f"] == 1
