"""A symbol without a CIK is a checked leaf, not an item failure; a lost CIK is a failure.

2026-11-26 on grid-svr: ``smart:sec_xbrl_financials`` was PARTIAL on every
run for "unrecognized puller status NO_CIK". The 12:27Z run: 49 of 200 items
not clean, 46 of them symbols without a CIK (crypto, futures, FX, ETFs,
foreign ADRs, renamed or acquired names), 2 the old duration refusals. The
SEC ticker file (10,400 tickers) lists none of the 146 NO_CIK symbols that
were listed from the journal, so the leaf is accurate: there was nothing to
fetch. Each such symbol is recorded as attempted, so it came back on every
sweep of the ~1,090-symbol universe.

Offline: scripted CIK map, fetch and writer; no request, no database.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from unittest.mock import MagicMock


from ingestion.altdata import sec_xbrl_financials as sec
from ingestion.altdata.sec_financial_snapshots import NORMALIZER_VERSION
from ingestion.smart_scheduler import (
    OUTCOME_FAILED, OUTCOME_NO_NEW_DATA, OUTCOME_PARTIAL, OUTCOME_SUCCESS, _classify_outcome,
)
from store import sec_financial_snapshots as store


def _response(cik=1, value=100):
    data = dict(cik=cik, entityName="X", facts={"us-gaap": {"Revenues": {"units": {"USD": [
        dict(start="2026-04-27", end="2026-07-26", val=value, form="10-Q", fp="Q2",
             accn="0001045810-26-000075", filed="2026-08-26")]}}}})
    now = datetime.now(timezone.utc)
    return json.dumps(data).encode(), now, now


def _puller(monkeypatch, universe, cik_map, *, covered=frozenset(), state=None, write_rows=1, real_universe=False):
    """``covered=None`` scripts an unreadable heads table (unknown, not empty)."""
    state = {} if state is None else state
    if not real_universe:
        monkeypatch.setattr(sec, "_sector_map_tickers", lambda: list(universe))
    monkeypatch.setattr(sec, "_load_fx_rates", lambda _: {"USD": 1})
    monkeypatch.setattr(sec, "_fetch_ticker_to_cik_map", lambda: dict(cik_map))
    monkeypatch.setattr(sec, "_load_checkpoint", lambda: dict(state))

    def save(completed, last_ticker, total_rows, attempted, deferred=None):
        state.update(normalizer_version=NORMALIZER_VERSION, completed=completed,
                     attempted=attempted, deferred=deferred or [], total_rows=total_rows)

    monkeypatch.setattr(sec, "_save_checkpoint", save)
    monkeypatch.setattr(sec.time, "sleep", lambda _: None)
    monkeypatch.setattr(sec, "_stored_snapshot_actors",
                        lambda _e: covered if covered is None else frozenset(covered))
    monkeypatch.setattr(sec, "_fetch_company_facts_response",
                        MagicMock(side_effect=lambda cik: _response(int(cik))))
    write = MagicMock(return_value=dict(status="applied", rows_written=write_rows,
                                        snapshot_id="s", rows_removed=0))
    monkeypatch.setattr(store, "write_snapshot", write)
    puller = sec.SECXBRLFinancialsPuller(MagicMock())
    puller.write, puller.state = write, state
    return puller


# ── the run's outcome as the smart scheduler logs it ─────────────────────────

def test_a_run_whose_only_misses_are_symbols_without_a_cik_is_success(monkeypatch):
    universe = ["AAA", "BBB", "IBB", "WBA"]
    puller = _puller(monkeypatch, universe, {"AAA": "0000000001", "BBB": "0000000002"})
    out = puller.pull_all()
    statuses = {r.get("ticker"): r["status"] for r in out if r.get("ticker")}
    assert statuses == {"AAA": "SUCCESS", "BBB": "SUCCESS", "IBB": "NO_DATA", "WBA": "NO_DATA"}
    assert all(r["reason"] == "NO_CIK" for r in out if r.get("status") == "NO_DATA")
    summary = out[-1]
    assert summary["no_cik"] == 2 and summary["no_cik_tickers"] == ["IBB", "WBA"] and summary["cik_lost"] == 0
    outcome, rows, note = _classify_outcome(out)
    assert (outcome, rows, note) == (OUTCOME_SUCCESS, 2, None)


def test_the_2026_11_26_1227z_shape_is_partial_only_for_the_real_failures():
    """200 items: 46 NO_CIK, 2 refusals, 152 written -> 2 failed, not 49."""
    items = [{"ticker": f"S{i}", "status": "SUCCESS", "rows": 3} for i in range(152)]
    items += [{"ticker": f"N{i}", "status": "NO_DATA", "reason": "NO_CIK", "rows": 0} for i in range(46)]
    items += [{"ticker": f"F{i}", "status": "FAILED", "rows": 0,
               "error": "Invalid mapped financial-flow duration"} for i in range(2)]
    items.append({"status": "SUMMARY", "tickers_processed": 200, "rows_written": 456})
    outcome, rows, note = _classify_outcome(items)
    assert outcome == OUTCOME_PARTIAL and rows == 456
    assert note.startswith("2 of 200 items failed or partial, 0 skipped, 456 rows written")
    assert "NO_CIK" not in note and "unrecognized" not in note


def test_an_all_no_cik_run_is_checked_nothing_not_a_failure():
    items = [{"ticker": t, "status": "NO_DATA", "reason": "NO_CIK", "rows": 0} for t in ("A", "B")]
    items.append({"status": "SUMMARY", "tickers_processed": 2, "rows_written": 0})
    assert _classify_outcome(items)[0] == OUTCOME_NO_NEW_DATA


def test_the_old_no_cik_status_would_still_be_an_item_failure():
    """The classifier is untouched (its source is pinned by tests/test_hermes_sec_only.py):
    the fix is the puller no longer emitting a status of its own for this leaf."""
    outcome, _, note = _classify_outcome({"status": "NO_CIK", "rows": 0})
    assert outcome == OUTCOME_FAILED and "unrecognized puller status NO_CIK" in note


# ── a covered issuer that loses its CIK is a failure ─────────────────────────

def test_a_covered_issuer_missing_from_the_sec_file_is_a_failed_item(monkeypatch):
    puller = _puller(monkeypatch, ["AAA", "AVB"], {"AAA": "0000000001"}, covered={"avb", "aaa"})
    out = puller.pull_all()
    avb = next(r for r in out if r.get("ticker") == "AVB")
    assert avb["status"] == "FAILED" and avb["error"].startswith("CIK lost")
    assert out[-1]["cik_lost"] == 1 and out[-1]["no_cik"] == 0
    outcome, _, note = _classify_outcome(out)
    assert outcome == OUTCOME_PARTIAL and "CIK lost" in note
    assert puller.state["attempted"] == ["AAA", "AVB"]            # a known loss is a settled result


def test_a_successful_empty_heads_read_means_the_symbol_never_filed(monkeypatch):
    puller = _puller(monkeypatch, ["AAA", "IBB"], {"AAA": "0000000001"}, covered=frozenset())
    out = puller.pull_all()
    assert next(r for r in out if r.get("ticker") == "IBB")["status"] == "NO_DATA"
    assert _classify_outcome(out)[0] == OUTCOME_SUCCESS
    assert puller.state["attempted"] == ["AAA", "IBB"]


def test_an_unreadable_heads_table_is_unknown_not_a_checked_leaf(monkeypatch):
    """Codex 6079823206: the same inputs must not be PARTIAL with a working
    lookup and SUCCESS when it raises, and the symbol must be retried."""
    state: dict = {}
    puller = _puller(monkeypatch, ["AAA", "AVB", "BBB"],
                     {"AAA": "0000000001", "BBB": "0000000002"}, covered=None, state=state)
    out = puller.pull_all()
    avb = next(r for r in out if r.get("ticker") == "AVB")
    assert avb["status"] == "FAILED" and "coverage check unavailable" in avb["error"]
    assert out[-1]["coverage_check_unavailable"] == 1 and out[-1]["no_cik"] == 0
    outcome, rows, note = _classify_outcome(out)
    assert outcome == OUTCOME_PARTIAL and rows == 2 and "coverage check unavailable" in note
    # valid-CIK neighbours were written and completed; the unresolved symbol was not attempted
    assert state["completed"] == ["AAA", "BBB"] and state["attempted"] == ["AAA", "BBB"]
    assert state["deferred"] == ["AVB"]                                  # held back, not attempted
    assert puller.write.call_count == 2

    # the DB recovers: the next run sees it again, and AVB (covered) is now a known loss
    puller2 = _puller(monkeypatch, ["AAA", "AVB", "BBB"],
                      {"AAA": "0000000001", "BBB": "0000000002"}, covered={"avb"}, state=state)
    out2 = puller2.pull_all()
    assert [r["ticker"] for r in out2 if r.get("ticker")] == ["AVB"]       # only the unattempted one
    assert out2[0]["status"] == "FAILED" and out2[0]["error"].startswith("CIK lost")


def test_the_heads_reader_returns_unknown_on_a_read_failure():
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("db down")
    assert sec._stored_snapshot_actors(engine) is None


def test_the_heads_reader_returns_the_actor_ids():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [("avb",), ("nvda",), (None,)]
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    assert sec._stored_snapshot_actors(engine) == frozenset({"avb", "nvda"})


# ── the universe is the sector map, whatever a price provider thinks of a symbol ──

def _sector_map(monkeypatch, tickers, status=None):
    import analysis.sector_map as sm

    monkeypatch.setattr(sm, "SECTOR_MAP", {"s": {"subsectors": {"u": {"actors": [{"ticker": t} for t in tickers]}}}})
    monkeypatch.setattr(sm, "TICKER_STATUS", status or {})


def test_the_universe_is_not_filtered_before_the_authoritative_lookups(monkeypatch):
    """Codex 6080007104: a price-provider symbol heuristic (Tiingo hardcodes bare BTC, ETH, SOL,
    DXY) must not override a valid CIK or a stored head, and sidecar-marked tickers stay too."""
    _sector_map(monkeypatch, ["AAPL", "ACQ", "FOR", "SOL", "BTC-USD", "CL=F", "MOG.A", "AAPL", ""],
                {"ACQ": {"status": "acquired"}, "FOR": {"status": "foreign_listing"}})
    assert sec._sector_map_tickers() == ["AAPL", "ACQ", "FOR", "SOL", "BTC-USD", "CL=F", "MOG.A"]


def test_a_sol_style_collision_with_a_valid_cik_and_a_stored_head_is_pulled_on_the_scheduled_path(monkeypatch):
    """An issuer whose ticker collides with a crypto symbol is visited by the full sweep (no
    ``tickers`` argument); symbols with no CIK and no head are the checked NO_DATA leaf."""
    _sector_map(monkeypatch, ["AAA", "SOL", "BTC-USD", "CL=F", "IBB"])
    puller = _puller(monkeypatch, [], {"AAA": "0000000001", "SOL": "0000000009"},
                     covered={"aaa", "sol"}, real_universe=True)
    out = puller.pull_all()
    by = {r["ticker"]: r["status"] for r in out if r.get("ticker")}
    assert by == {"AAA": "SUCCESS", "SOL": "SUCCESS", "BTC-USD": "NO_DATA", "CL=F": "NO_DATA", "IBB": "NO_DATA"}
    assert _classify_outcome(out)[0] == OUTCOME_SUCCESS
    assert sorted(c.args[1].actor_id for c in puller.write.call_args_list) == ["aaa", "sol"]


def test_a_foreign_tagged_issuer_with_a_valid_cik_is_still_pulled_and_written(monkeypatch):
    """The explicitly targeted repair of a sidecar-marked issuer: it reaches the writer."""
    puller = _puller(monkeypatch, ["AAA", "FOR"], {"AAA": "0000000001", "FOR": "0000000007"},
                     covered={"for"})
    out = puller.pull_all(tickers=["FOR"])
    assert [(r["ticker"], r["status"]) for r in out if r.get("ticker")] == [("FOR", "SUCCESS")]
    assert puller.write.call_count == 1 and puller.write.call_args.args[1].actor_id == "for"
    assert puller.state["completed"] == ["FOR"]


# ── a degraded heads read cannot starve later issuers in bounded runs ────────

def test_unresolved_symbols_do_not_starve_a_valid_issuer_behind_them_and_recover_later(monkeypatch):
    """Codex 6080007104: the scheduler runs with a limit; unresolved symbols consume it. With the
    heads unreadable and a window of unresolved symbols ahead of a valid issuer, consecutive capped
    runs must still reach the valid issuer, and the unknowns must be settled once the read recovers."""
    state: dict = {}
    universe = ["U1", "U2", "U3", "V1"]
    cik = {"V1": "0000000005"}

    run1 = _puller(monkeypatch, universe, cik, covered=None, state=state).pull_all(limit=3)
    assert [r["status"] for r in run1 if r.get("ticker")] == ["FAILED"] * 3
    assert state["deferred"] == ["U1", "U2", "U3"] and state["attempted"] == []

    p2 = _puller(monkeypatch, universe, cik, covered=None, state=state)
    run2 = p2.pull_all(limit=3)
    assert [(r["ticker"], r["status"]) for r in run2 if r.get("ticker")] == [("V1", "SUCCESS")]    # forward progress
    assert state["completed"] == ["V1"] and state["attempted"] == ["V1"]

    # the heads read recovers: only the deferred symbols remain, so they are retried and settled
    run3 = _puller(monkeypatch, universe, cik, covered=frozenset(), state=state).pull_all(limit=3)
    assert [(r["ticker"], r["status"], r["reason"]) for r in run3 if r.get("ticker")] == [
        ("U1", "NO_DATA", "NO_CIK"), ("U2", "NO_DATA", "NO_CIK"), ("U3", "NO_DATA", "NO_CIK")]
    assert sorted(state["attempted"]) == ["U1", "U2", "U3", "V1"] and state["deferred"] == []
    assert _classify_outcome(run3)[0] == OUTCOME_NO_NEW_DATA

    # the sweep is complete: the next run starts a new one
    run4 = _puller(monkeypatch, universe, cik, covered=frozenset(), state=state).pull_all(limit=1)
    assert [r["ticker"] for r in run4 if r.get("ticker")] == ["U1"]


def test_still_degraded_the_deferred_symbols_are_retried_when_nothing_else_is_left(monkeypatch):
    state = {"normalizer_version": NORMALIZER_VERSION, "completed": ["V1"], "attempted": ["V1"],
             "deferred": ["U1", "U2"]}
    run = _puller(monkeypatch, ["U1", "U2", "V1"], {"V1": "0000000005"}, covered=None, state=state).pull_all(limit=1)
    assert [(r["ticker"], r["status"]) for r in run if r.get("ticker")] == [("U1", "FAILED")]
    assert state["deferred"] == ["U1"]            # U2 was not visited yet, so it is simply pending
    assert "U1" not in state["attempted"]
