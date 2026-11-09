"""Sleuth lead construction v2 (pre-registered 2026-11-08 22:22Z).

The first night's ten intersection leads showed three ways a lead could be
built from evidence that was not independent or not a trade:

1. A price move counted as a second family when it was the market's reaction
   to the first (ADI, MSFT, CACI). It is now context: it neither makes nor
   blocks a lead.
2. QuiverQuant's ``insider_buy`` covers any acquisition: the insider "buys"
   behind BAH and KBR were $0 stock grants (code A), ADI's an option exercise
   (code M). Only open-market purchases (P) and sales (S) vote, as in the SEC
   feed.
3. One trade was several rows: TSM's one purchase was in both feeds, and Ray
   Stata's Nov 4 ADI sale was one SEC row and 20 QuiverQuant fills. One vote
   per (insider, day, direction).

Replaying the first night: ADI, CACI, BAH, KBR and LHX no longer form.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from intelligence import sleuth_intersections as si

AS_OF = datetime(2026, 11, 8, 2, 0, tzinfo=timezone.utc)
END = AS_OF.date()
START = date(2026, 11, 1)
DAY = date(2026, 11, 4)


def _obs(family, direction="up", ticker="ADI", **cite):
    return si.Observation(family=family, ticker=ticker, direction=direction,
                          obs_date=END, citation={"table": "signal_sources", **cite})


def _qq(name, code, acq="A"):
    return {"Name": name, "TransactionCode": code, "AcquiredDisposedCode": acq}


def _collect(rows):
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = rows
    return si.collect_signal_observations(conn, START, END, AS_OF)


# ── rule 1: price is context ────────────────────────────────────────────────


def test_a_price_move_cannot_be_the_second_family():
    # ADI and CACI on 2026-11-08: insider selling plus the price drop.
    assert si.find_intersections([_obs("insider", "down"), _obs("price_move", "down")], END) == []


def test_a_price_move_rides_along_as_context():
    obs = [_obs("options_flow"), _obs("retail_sentiment"), _obs("price_move", magnitude=None)]
    (lead,) = si.find_intersections(obs, END)
    assert lead.families == ["options_flow", "retail_sentiment"]
    ev = lead.evidence()
    assert [e["family"] for e in ev if e["role"] == "context"] == ["price_move"]
    assert {e["family"] for e in ev if e["role"] == "cites"} == {
        "options_flow", "retail_sentiment", "price_move"}
    assert si.evidence_direction(ev) == ("ADI", "up")


def test_a_price_move_against_the_lead_does_not_block_it():
    obs = [_obs("insider"), _obs("gov_contract"),
           _obs("options_flow", "down"), _obs("price_move", "down")]
    (lead,) = si.find_intersections(obs, END)
    assert [v.family for v in lead.dissent] == ["options_flow"]
    assert [v.family for v in lead.context] == ["price_move"]
    assert lead.priority == pytest.approx(0.55)  # context does not move priority


def test_every_lead_records_its_construction():
    (lead,) = si.find_intersections([_obs("insider"), _obs("options_flow")], END)
    assert lead.evidence()[0] == {"role": "construction", "kind": "construction",
                                  "version": "v2"}


# ── rule 2: open-market trades only ─────────────────────────────────────────


@pytest.mark.parametrize(("stype", "value", "expected"), [
    ("insider_buy", _qq("TAN LIP BU", "P"), ("insider", "up")),
    ("insider_sell", _qq("STATA RAY", "S", "D"), ("insider", "down")),
    ("insider_buy", _qq("Metzfield Dennis", "A"), None),       # BAH: $0 stock grant
    ("insider_buy", _qq("ROCHE VINCENT", "M"), None),          # ADI: option exercise
    ("insider_sell", _qq("X", "F", "D"), None),                # tax withholding
    ("insider_sell", _qq("X", "G", "D"), None),                # gift
    ("insider_buy", {"Name": "X"}, None),                      # no code: no vote
    ("insider_buy", '{"Name": "X", "TransactionCode": "p"}', ("insider", "up")),
])
def test_quiverquant_insider_votes_only_on_open_market_trades(stype, value, expected):
    assert si.classify_signal("quiverquant:insider", stype, value) == expected


def test_a_grant_cannot_complete_a_contract_lead():
    # BAH and KBR on 2026-11-08: a contract award plus a QuiverQuant grant.
    rows = [(1, "gov_contract", "usaspending", "BAH", DAY, "CONTRACT_AWARD", {}),
            (2, "quiverquant:insider", "qq_insider_trading:metzfield dennis|a|122|0", "BAH", DAY,
             "insider_buy", _qq("Metzfield Dennis", "A"))]
    assert si.find_intersections(_collect(rows), END) == []


# ── rule 3: one vote per insider, day and direction ─────────────────────────


def test_one_trade_in_both_feeds_is_one_vote_citing_the_sec_row():
    rows = [(9, "quiverquant:insider", "qq_insider_trading:yuan lipen|p|1000|71.49", "TSM", DAY,
             "insider_buy", _qq("Yuan Lipen", "P")),
            (3, "insider", "Yuan Lipen", "TSM", DAY, "BUY", {})]
    (o,) = _collect(rows)
    assert (o.citation["id"], o.citation["source_type"], o.actor) == (3, "insider", "yuan lipen")


def test_many_fills_of_one_sale_are_one_vote():
    rows = [(1, "insider", "STATA RAY", "ADI", DAY, "SELL", {})]
    rows += [(100 + i, "quiverquant:insider", f"qq_insider_trading:stata ray|s|{i}|400", "ADI", DAY,
              "insider_sell", _qq("STATA RAY", "S", "D")) for i in range(20)]
    (o,) = _collect(rows)
    assert o.citation["id"] == 1
    (vote,) = si.family_votes([o])
    assert (vote.up, vote.down) == (0, 1)


def test_quiverquant_only_fills_keep_the_lowest_id():
    rows = [(7, "quiverquant:insider", "k7", "ADI", DAY, "insider_sell", _qq("Stata, Ray.", "S", "D")),
            (5, "quiverquant:insider", "k5", "ADI", DAY, "insider_sell", _qq("STATA RAY", "S", "D"))]
    assert [o.citation["id"] for o in _collect(rows)] == [5]


def test_different_insiders_days_and_directions_stay_separate():
    rows = [(1, "insider", "STATA RAY", "ADI", DAY, "SELL", {}),
            (2, "insider", "ROCHE VINCENT", "ADI", DAY, "SELL", {}),
            (3, "insider", "STATA RAY", "ADI", date(2026, 11, 5), "SELL", {}),
            (4, "insider", "STATA RAY", "ADI", DAY, "BUY", {}),
            (5, "insider", "STATA RAY", "MSFT", DAY, "SELL", {})]
    assert [o.citation["id"] for o in _collect(rows)] == [1, 2, 3, 4, 5]


def test_other_families_and_nameless_rows_are_not_deduplicated():
    rows = [(1, "options_flow", "flow", "ADI", DAY, "UNUSUAL_OPTIONS", {"direction": "CALL"}),
            (2, "options_flow", "flow", "ADI", DAY, "UNUSUAL_OPTIONS", {"direction": "CALL"}),
            (3, "insider", None, "ADI", DAY, "BUY", {}),
            (4, "insider", "", "ADI", DAY, "BUY", {})]
    assert [o.citation["id"] for o in _collect(rows)] == [1, 2, 3, 4]


@pytest.mark.parametrize(("source", "source_id", "value", "expected"), [
    ("insider", "DISBROW LISA S", {}, "disbrow lisa s"),
    ("quiverquant:insider", "qq_insider_trading:disbrow lisa s|s|210|678.38",
     {"Name": "Disbrow,  Lisa S."}, "disbrow lisa s"),
    ("quiverquant:insider", "k", '{"Name": "TAN LIP BU"}', "tan lip bu"),
    ("quiverquant:insider", "k", {}, None),
    ("insider", None, {}, None),
])
def test_insider_actor_normalises_both_feeds(source, source_id, value, expected):
    assert si.insider_actor(source, source_id, value) == expected


def test_a_cluster_summary_row_adds_no_vote():
    # One buy, one sale and the SEC puller's cluster row: still a split vote.
    rows = [(1, "insider", "LOEB DANIEL S", "ADI", DAY, "BUY", {}),
            (2, "insider", "STATA RAY", "ADI", DAY, "SELL", {}),
            (3, "insider", "cluster_adi", "ADI", DAY, "CLUSTER_BUY", {"insiders": 2})]
    obs = _collect(rows)
    assert [o.citation["id"] for o in obs] == [1, 2]
    (vote,) = si.family_votes(obs)
    assert vote.direction is None
