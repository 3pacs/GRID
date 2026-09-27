"""Pure-logic tests for intelligence.causal_links (slice N2).

Pins the honesty rules: no event disclosed on/after the trade day is linked,
known_at is recorded per trade / event / edge, one Form 4 act seen through two
channels is one action, and edge keys are stable (idempotent upserts).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from intelligence import causal_links as cl

UTC = timezone.utc
AS_OF = datetime(2026, 9, 27, 8, 0, tzinfo=UTC)


def _sig(id_, source_type, source_id, ticker, signal_type, signal_date, created_at, value=None):
    return {
        "id": id_, "source_type": source_type, "source_id": source_id, "ticker": ticker,
        "signal_type": signal_type, "signal_date": signal_date, "created_at": created_at,
        "signal_value": value or {},
    }


def _earn(ticker, d, reported=True, eps_actual=1.1, eps_estimate=1.0):
    return {
        "ticker": ticker, "earnings_date": d, "fiscal_quarter": "Q3", "eps_estimate": eps_estimate,
        "eps_actual": eps_actual, "eps_surprise_pct": 10.0, "reported": reported,
    }


def _form4(trade_day=date(2026, 9, 15), created=datetime(2026, 9, 17, 3, 0, tzinfo=UTC)):
    return _sig(1, "insider", "Cutt Timothy J.", "gpor", "SELL", trade_day, created)


# ── time helpers ──────────────────────────────────────────────────────────


def test_parse_sec_filetime_is_eastern_wall_clock():
    # 22:06 EDT on 2026-09-25 is 02:06 UTC on 2026-09-26.
    assert cl.parse_sec_filetime("2026-09-25T22:06:32.000") == datetime(2026, 9, 26, 2, 6, 32, tzinfo=UTC)
    assert cl.parse_sec_filetime(None) is None
    assert cl.parse_sec_filetime("garbage") is None


def test_end_of_day_rounds_up():
    assert cl.end_of_day(date(2026, 9, 10)) == datetime(2026, 9, 11, tzinfo=UTC)


def test_recency_score_is_bounded_heuristic():
    assert cl.recency_score(0, 30) == 0.7
    assert cl.recency_score(30, 30) == 0.3
    assert cl.recency_score(300, 30) == 0.3


def test_normalize_actor_is_order_and_initial_insensitive():
    assert cl.normalize_actor("Cutt Timothy J.") == cl.normalize_actor("Timothy Cutt")
    assert cl.normalize_actor("") == ""


# ── canonical actions ─────────────────────────────────────────────────────


def test_form4_seen_in_two_channels_is_one_action_with_earliest_known_at():
    rows = [
        _form4(),
        _sig(2, "quiverquant:insider", "qq_insider_trading", "GPOR", "insider_sell", date(2026, 9, 15),
             datetime(2026, 9, 16, 5, 0, tzinfo=UTC),
             {"Name": "Timothy Cutt", "fileDate": "2026-09-15T18:00:00.000"}),
    ]
    actions = cl.canonical_actions(rows, AS_OF)
    assert len(actions) == 1
    a = actions[0]
    assert a.source_types == ("insider", "quiverquant:insider")
    assert a.source_refs == (1, 2)
    assert a.known_at == datetime(2026, 9, 15, 22, 0, tzinfo=UTC)  # 18:00 EDT filing
    assert a.known_at_basis == "filing"
    assert a.actor == "Timothy Cutt"  # the feed name 'qq_insider_trading' is never the actor


def test_unusual_types_map_to_direction_and_cluster_rows_are_skipped():
    created = datetime(2026, 9, 17, tzinfo=UTC)
    rows = [
        _sig(1, "insider", "A Person", "XYZ", "UNUSUAL_SELL", date(2026, 9, 15), created),
        _sig(2, "insider", "B Person", "XYZ", "CLUSTER_BUY", date(2026, 9, 15), created),
    ]
    actions = cl.canonical_actions(rows, AS_OF)
    assert [(a.actor, a.direction) for a in actions] == [("A Person", "SELL")]


def test_congress_known_at_is_statutory_bound_not_fake_disclosure_date():
    trade = date(2026, 8, 1)
    row = _sig(9, "congressional", "Pete Sessions", "AAPL", "SELL", trade,
               datetime(2026, 8, 2, tzinfo=UTC), {"disclosure_date": "2026-08-01"})
    (a,) = cl.canonical_actions([row], AS_OF)
    assert a.channel == "congress"
    assert a.known_at == cl.end_of_day(trade + timedelta(days=45))
    assert a.known_at_basis == "statutory_bound"


def test_rows_not_yet_public_or_seen_after_as_of_are_dropped():
    recent_congress = _sig(1, "congressional", "Rep A", "AAA", "BUY", date(2026, 9, 20),
                           datetime(2026, 9, 21, tzinfo=UTC))
    seen_later = _sig(2, "insider", "B Person", "AAA", "BUY", date(2026, 9, 20),
                      datetime(2026, 9, 28, tzinfo=UTC))
    assert cl.canonical_actions([recent_congress, seen_later], AS_OF) == []


# ── events ────────────────────────────────────────────────────────────────


def test_unreported_earnings_calendar_rows_are_not_events():
    evs = cl.earnings_events([_earn("GPOR", date(2026, 9, 1), reported=False, eps_actual=None)])
    assert evs == []


def test_contract_is_timed_by_first_seen_not_start_date():
    row = {"id": 5, "source_id": "DoD", "ticker": "LMT", "signal_date": date(2026, 9, 10),
           "created_at": datetime(2026, 9, 20, 6, 0, tzinfo=UTC),
           "signal_value": {"award_id": "W1", "amount": 1_000_000}}
    dup = dict(row, id=6, created_at=datetime(2026, 9, 25, tzinfo=UTC))
    (ev,) = cl.contract_events([row, dup])
    assert ev.known_at == datetime(2026, 9, 20, 6, 0, tzinfo=UTC)
    assert ev.event_date == date(2026, 9, 20)
    assert ev.evidence["award_start_date"] == "2026-09-10"
    assert ev.evidence["first_seen_minus_start_days"] == 10


def test_backfilled_contracts_are_not_events():
    stale = {"id": 5, "source_id": "DoD", "ticker": "LMT", "signal_date": date(2024, 12, 20),
             "created_at": datetime(2026, 9, 26, tzinfo=UTC), "signal_value": {"award_id": "OLD"}}
    far_future = dict(stale, signal_date=date(2028, 1, 1), signal_value={"award_id": "FUT"})
    assert cl.contract_events([stale, far_future]) == []


# ── edges ─────────────────────────────────────────────────────────────────


def test_events_disclosed_on_or_after_the_trade_day_are_never_linked():
    (action,) = cl.canonical_actions([_form4()], AS_OF)  # trade 2026-09-15
    events = cl.earnings_events([
        _earn("GPOR", date(2026, 9, 10)),   # public end of 09-10: linked
        _earn("GPOR", date(2026, 9, 14)),   # public 00:00 09-15 = trade start: linked
        _earn("GPOR", date(2026, 9, 15)),   # same day: timing unknown -> not linked
        _earn("GPOR", date(2026, 9, 20)),   # after the trade -> never a cause
        _earn("GPOR", date(2026, 7, 1)),    # outside the 30-day window
    ])
    edges = cl.build_edges([action], events)
    assert sorted(e.event.event_date for e in edges) == [date(2026, 9, 10), date(2026, 9, 14)]
    for e in edges:
        assert e.event.known_at <= cl.day_start(action.action_date)
        assert e.known_at == max(action.known_at, e.event.known_at)
        assert 0.3 <= e.score <= 0.7


def test_contract_first_seen_after_trade_is_not_linked_even_if_start_date_before():
    (action,) = cl.canonical_actions([_sig(1, "insider", "Xavier Young", "LMT", "BUY", date(2026, 9, 15),
                                           datetime(2026, 9, 16, tzinfo=UTC))], AS_OF)
    late = {"id": 5, "source_id": "DoD", "ticker": "LMT", "signal_date": date(2026, 9, 1),
            "created_at": datetime(2026, 9, 18, tzinfo=UTC), "signal_value": {"award_id": "W1"}}
    early = dict(late, id=6, created_at=datetime(2026, 9, 1, tzinfo=UTC), signal_value={"award_id": "W2"})
    edges = cl.build_edges([action], cl.contract_events([late, early]))
    assert [e.event.key for e in edges] == ["contract:LMT:W2"]


def test_edge_keys_are_stable_and_distinct_per_event():
    (action,) = cl.canonical_actions([_form4()], AS_OF)
    events = cl.earnings_events([_earn("GPOR", date(2026, 9, 10)), _earn("GPOR", date(2026, 9, 1))])
    first = cl.build_edges([action], events)
    again = cl.build_edges([action], events + events)
    assert [e.edge_key for e in first] == [e.edge_key for e in again]
    assert len({e.edge_key for e in first}) == 2


def test_edge_row_records_claim_and_score_method():
    (action,) = cl.canonical_actions([_form4()], AS_OF)
    (edge,) = cl.build_edges([action], cl.earnings_events([_earn("GPOR", date(2026, 9, 10))]))
    row = edge.to_row()
    assert row["score_method"] == cl.SCORE_METHOD
    assert row["cause_type"] == "earnings"
    assert "not proof of cause" in row["evidence"]
    assert row["action_known_at"] == action.known_at


@pytest.mark.parametrize("st,expected", [("BUY", "BUY"), ("insider_buy", "BUY"),
                                         ("UNUSUAL_SELL", "SELL"), ("CLUSTER_BUY", None),
                                         ("CONTRACT_AWARD", None)])
def test_direction_of(st, expected):
    assert cl.direction_of(st) == expected


def test_resolve_code_sha_prefers_env(monkeypatch):
    monkeypatch.setenv("GRID_CODE_SHA", "deadbeef")
    assert cl.resolve_code_sha() == "deadbeef"
