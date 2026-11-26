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

import pytest

from ingestion.altdata import sec_xbrl_financials as sec
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


def _puller(monkeypatch, universe, cik_map, *, covered=frozenset(), write_rows=1):
    monkeypatch.setattr(sec, "_sector_map_tickers", lambda: list(universe))
    monkeypatch.setattr(sec, "_load_fx_rates", lambda _: {"USD": 1})
    monkeypatch.setattr(sec, "_fetch_ticker_to_cik_map", lambda: dict(cik_map))
    monkeypatch.setattr(sec, "_load_checkpoint", lambda: {})
    monkeypatch.setattr(sec, "_save_checkpoint", lambda *a: None)
    monkeypatch.setattr(sec.time, "sleep", lambda _: None)
    monkeypatch.setattr(sec, "_stored_snapshot_actors", lambda _e: frozenset(covered))
    monkeypatch.setattr(sec, "_fetch_company_facts_response",
                        MagicMock(side_effect=lambda cik: _response(int(cik))))
    monkeypatch.setattr(store, "write_snapshot", MagicMock(return_value=dict(
        status="applied", rows_written=write_rows, snapshot_id="s", rows_removed=0)))
    return sec.SECXBRLFinancialsPuller(MagicMock())


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


def test_the_lost_cik_check_failing_to_read_never_blocks_the_pull(monkeypatch):
    _puller(monkeypatch, ["AAA", "AVB"], {"AAA": "0000000001"})
    monkeypatch.undo()  # drop the scripted reader; use the real one on a broken engine
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("db down")
    assert sec._stored_snapshot_actors(engine) == frozenset()


# ── the universe ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("ticker", ["BTC-USD", "ETH-USD", "CL=F", "ZC=F", "BRL=X", "EURUSD=X",
                                    "ATD.TO", "DX-Y.NYB", "^GSPC", "DXY"])
def test_structural_non_filers_are_recognised(ticker):
    assert sec._is_structural_non_filer(ticker)


@pytest.mark.parametrize("ticker", ["AAPL", "BRK-B", "MOG.A", "KELY.A", "IBB", "ADRNY"])
def test_listed_symbols_are_not_dropped_by_the_symbol_rule(ticker):
    assert not sec._is_structural_non_filer(ticker)


def test_universe_drops_non_filers_and_sidecar_acquired_delisted_foreign_but_keeps_renamed(monkeypatch):
    import analysis.sector_map as sm

    status = {
        "ACQ": {"status": "acquired"}, "DEL": {"status": "delisted"},
        "FOR": {"status": "foreign_listing"}, "OLD": {"status": "renamed", "successor": "NEW"},
    }
    sector_map = {"s": {"subsectors": {"u": {"actors": [
        {"ticker": t} for t in ("AAPL", "ACQ", "DEL", "FOR", "OLD", "BTC-USD", "CL=F", "MOG.A", "AAPL", "")
    ]}}}}
    monkeypatch.setattr(sm, "SECTOR_MAP", sector_map)
    monkeypatch.setattr(sm, "TICKER_STATUS", status)
    assert sec._sector_map_tickers() == ["AAPL", "OLD", "MOG.A"]
