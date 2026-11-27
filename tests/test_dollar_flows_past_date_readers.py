"""Readers of ``dollar_flows`` asked about a past date.

``dollar_flows`` is a current view. Each normalization deletes the rows of the
dates it covers and writes them again (``intelligence.dollar_flows``, "Clear
stale rows for this date range"), so ``created_at`` is the time of the last
rebuild and a row corrected or dropped since leaves no trace. The table cannot
say what it held on a past date.

Three readers took a date and read the table anyway. Each case below stores
what was known on 20 November, replaces it on the 24th the way a normalization
does, and reads on the 27th:

* ``get_aggregate_flows(as_of=...)`` returned the 24 November rows for
  ``as_of`` = 20 November. A past date now gets no totals and a reason.
* the flow map's ``prediction_markets`` node did the same. A past date now has
  no value.
* the signal adapter's seven-day window on ``created_at`` had no upper bound,
  so a row written after ``now`` was read. The window is now closed at ``now``.

Today's reads are unchanged, and each case shows that next to the past read.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from analysis import money_flow_engine
from analysis.money_flow_engine import etf_flow_evidence, layer_retail
from intelligence import dollar_flows
from intelligence.adapters.dollar_flows_adapter import DollarFlowsAdapter
from tests import thirteenf_holdings_support as s

NOV10, NOV19, NOV20, NOV24, NOV27 = (date(2026, 11, d) for d in (10, 19, 20, 24, 27))
_REQUIRE_ENV = "REQUIRE_DOLLAR_FLOWS_PG"


@pytest.fixture()
def engine():
    return s.make_sqlite_engine()


@pytest.fixture()
def today_is(monkeypatch):
    """Set today's UTC date for both the table's readers and the flow map."""
    def _set(day: date) -> None:
        monkeypatch.setattr(dollar_flows, "_utc_today", lambda: day)
        monkeypatch.setattr(etf_flow_evidence, "_utc_today", lambda: day)
    return _set


def _utc(day: int, hour: int = 12) -> datetime:
    return datetime(2026, 11, day, hour, 0, tzinfo=timezone.utc)


def _write(engine, source_type: str, amount: float, flow_date: date, written: datetime,
           *, direction: str = "inflow", ticker: str | None = "AAPL", actor: str = "A") -> None:
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO dollar_flows (source_type, actor_name, ticker, amount_usd, direction, "
            "confidence, flow_date, created_at) "
            "VALUES (:st, :actor, :ticker, :amt, :dir, 'derived', :fd, :ca)"),
            {"st": source_type, "actor": actor, "ticker": ticker, "amt": amount, "dir": direction,
             "fd": flow_date, "ca": written})


def _rebuild(engine, source_type: str, amount: float, flow_date: date, written: datetime, **kw) -> None:
    """Replace a date's rows the way ``normalize_all_flows`` does."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dollar_flows WHERE flow_date = :fd"), {"fd": flow_date})
    _write(engine, source_type, amount, flow_date, written, **kw)


def _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(engine, source_type: str) -> None:
    _write(engine, source_type, 403.0, NOV20, datetime(2026, 11, 20, 12, 0))
    _rebuild(engine, source_type, 490.0, NOV20, datetime(2026, 11, 24, 12, 0))


# ── get_aggregate_flows ────────────────────────────────────────────────────


def test_a_past_as_of_does_not_get_rows_written_after_it(engine, today_is):
    _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(engine, "insider")
    today_is(NOV27)

    past = dollar_flows.get_aggregate_flows(engine, days=30, as_of=NOV20)

    # Before: total_inflow 490.0, the value written four days after as_of.
    assert past["withheld_reason"] == dollar_flows.CURRENT_VIEW_WITHHELD_REASON
    assert past["as_of"] == "2026-11-20"
    assert past["total_inflow"] is None and past["total_outflow"] is None and past["net_flow"] is None
    assert past["flow_count"] is None and past["quarantined_13f"] is None
    assert past["by_sector"] == {} and past["by_source_type"] == {} and past["by_actor"] == {}
    assert past["days"] == 30


def test_a_past_as_of_reads_no_row_at_all(today_is):
    """The past answer never touches the table: there is nothing in it to trust."""
    class _NoDatabase:
        def connect(self):
            raise AssertionError("a past as_of must not read dollar_flows")
        begin = connect

    today_is(NOV27)
    assert dollar_flows.get_aggregate_flows(_NoDatabase(), as_of=NOV20)["withheld_reason"] is not None


def test_a_rebuilt_row_whose_value_did_not_change_is_still_not_a_past_answer(engine, today_is):
    """No partial totals: a row left alone since the 20th is not served either,
    because the rows that were beside it then may have been replaced."""
    _write(engine, "insider", 1_000.0, NOV19, datetime(2026, 11, 19, 12, 0))
    _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(engine, "insider")
    today_is(NOV27)

    assert dollar_flows.get_aggregate_flows(engine, days=30, as_of=NOV20)["total_inflow"] is None


@pytest.mark.parametrize("as_of", [None, NOV27], ids=["default", "today_named"])
def test_todays_totals_are_unchanged(engine, today_is, monkeypatch, as_of):
    _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(engine, "insider")
    _write(engine, "congressional", 90.0, NOV24, datetime(2026, 11, 24, 12, 0), direction="outflow")
    today_is(NOV27)
    if as_of is None:
        monkeypatch.setattr(dollar_flows, "date", type("_D", (date,), {"today": classmethod(lambda cls: NOV27)}))

    agg = dollar_flows.get_aggregate_flows(engine, days=30, as_of=as_of)

    assert agg["withheld_reason"] is None and agg["as_of"] == "2026-11-27"
    assert agg["total_inflow"] == 490.0 and agg["total_outflow"] == 90.0 and agg["net_flow"] == 400.0
    assert agg["flow_count"] == 2


def test_a_future_as_of_is_not_a_past_date(engine, today_is):
    """Only an ended UTC date is withheld; the bound on flow_date still applies."""
    _write(engine, "insider", 5.0, NOV24, datetime(2026, 11, 24, 12, 0))
    today_is(NOV20)
    assert dollar_flows.is_past_as_of(NOV19) is True
    assert dollar_flows.is_past_as_of(NOV20) is False
    assert dollar_flows.is_past_as_of(NOV24) is False
    assert dollar_flows.is_past_as_of(None) is False
    assert dollar_flows.get_aggregate_flows(engine, days=30, as_of=NOV24)["total_inflow"] == 5.0


# ── Flow map: prediction markets node ──────────────────────────────────────


def test_the_prediction_markets_node_has_no_value_for_a_past_date(engine, today_is):
    _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(engine, "prediction_market")
    today_is(NOV27)

    past = layer_retail._build_prediction_markets_node(engine, NOV20)

    # Before: value 490.0, confidence "derived".
    assert past.value is None
    assert past.confidence == "estimated"
    assert past.metadata["current_views_withheld"] is True
    assert past.metadata["evidence"] == "withheld"
    assert past.metadata["withheld_reason"] == dollar_flows.CURRENT_VIEW_WITHHELD_REASON


def test_a_past_date_does_not_get_the_fixed_size_estimate_either(engine, today_is):
    """With no rows today's node falls back to a fixed estimate of the market's
    size. That is not something known about a past date."""
    today_is(NOV27)
    past = layer_retail._build_prediction_markets_node(engine, NOV20)
    assert past.value is None and past.source == "dollar_flows:prediction_market"


def test_todays_prediction_markets_node_is_unchanged(engine, today_is):
    _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(engine, "prediction_market")
    today_is(NOV27)

    current = layer_retail._build_prediction_markets_node(engine, NOV27)
    assert current.value == 490.0 and current.confidence == "derived"
    assert current.source == "dollar_flows:prediction_market"
    assert current.metadata == {"lookback_days": 30, "evidence": "current_view"}

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dollar_flows"))
    empty = layer_retail._build_prediction_markets_node(engine, NOV27)
    assert empty.value == 2_000_000_000 and empty.source == "estimate"
    assert empty.metadata == {"lookback_days": 30, "evidence": "fixed_estimate"}


@pytest.fixture()
def other_retail_nodes(monkeypatch):
    """The layer's other four nodes read PostgreSQL-only SQL; give them fixed values."""
    def _fixed(node_id: str, value: float):
        return lambda engine, as_of: layer_retail.FlowNode(
            id=node_id, label=node_id, layer="retail", value=value, unit="USD", source="fixture")
    monkeypatch.setattr(layer_retail, "_build_margin_debt_node", _fixed("margin_debt", 100.0))
    monkeypatch.setattr(layer_retail, "_build_consumer_sentiment_node", _fixed("consumer_sentiment", 10.0))
    monkeypatch.setattr(layer_retail, "_build_retail_fund_flows_node", _fixed("retail_fund_flows", 1.0))
    monkeypatch.setattr(layer_retail, "_build_qq_wsb_sentiment", _fixed("qq_wsb", 0.5))


def test_a_past_retail_layer_total_leaves_the_node_out(engine, today_is, other_retail_nodes):
    _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(engine, "prediction_market")
    today_is(NOV27)

    past = layer_retail.build_retail_layer(engine, NOV20)
    current = layer_retail.build_retail_layer(engine, NOV27)

    assert {n.id: n.value for n in past.nodes}["prediction_markets"] is None
    assert past.total_value_usd == 111.5                                  # before: 601.5
    assert {n.id: n.value for n in current.nodes}["prediction_markets"] == 490.0
    assert current.total_value_usd == 601.5


def test_the_whole_flow_map_for_a_past_date_carries_no_prediction_market_value(
    engine, today_is, other_retail_nodes
):
    _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(engine, "prediction_market")
    today_is(NOV27)

    def _node(as_of: date) -> dict:
        flow_map = money_flow_engine.build_flow_map(engine, as_of=as_of).to_dict()
        return next(n for layer in flow_map["layers"] for n in layer["nodes"] if n["id"] == "prediction_markets")

    assert _node(NOV20)["value"] is None
    assert _node(NOV20)["metadata"]["withheld_reason"] == dollar_flows.CURRENT_VIEW_WITHHELD_REASON
    assert _node(NOV27)["value"] == 490.0


# ── Signal adapter ─────────────────────────────────────────────────────────


def _signals(engine, now: datetime) -> dict[str, float]:
    return {sig.ticker: sig.value for sig in DollarFlowsAdapter()._build_signals(engine, now)}


def test_the_adapter_does_not_read_a_row_written_after_now(engine):
    _write(engine, "insider", 400_000.0, NOV19, _utc(19), ticker="AAPL")
    _write(engine, "insider", 900_000.0, NOV24, _utc(24), ticker="MSFT")

    # Before: MSFT 900000.0 was returned for now = 20 November.
    assert _signals(engine, _utc(20)) == {"AAPL": 400_000.0}
    assert _signals(engine, _utc(25)) == {"AAPL": 400_000.0, "MSFT": 900_000.0}


def test_the_adapters_window_still_opens_seven_days_back_and_includes_now(engine):
    _write(engine, "insider", 300_000.0, NOV10, _utc(10), ticker="OLD")
    _write(engine, "insider", 500_000.0, NOV20, _utc(20), ticker="EDGE")

    assert _signals(engine, _utc(20)) == {"EDGE": 500_000.0}
    assert _signals(engine, _utc(17)) == {"OLD": 300_000.0}


def test_the_adapter_stamps_its_signals_with_the_same_now(engine):
    _write(engine, "insider", 400_000.0, NOV19, _utc(19))
    now = _utc(20)

    (signal,) = DollarFlowsAdapter()._build_signals(engine, now)

    assert signal.valid_from == now and signal.valid_until == now + timedelta(hours=4)
    assert signal.direction == "neutral"


# ── PostgreSQL: the same bounds on TIMESTAMPTZ and NUMERIC ─────────────────


@pytest.fixture()
def pg_engine():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required for the dollar-flow reader PG contract ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("dollar-flow reader PG contract requires a local disposable test database")

    schema = "dflow_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        for ddl in s.DDL_PG:
            conn.execute(text(ddl))
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def test_real_types_adapter_window_is_closed_at_now(pg_engine):
    _write(pg_engine, "insider", 400_000.0, NOV19, _utc(19), ticker="AAPL")
    _write(pg_engine, "insider", 900_000.0, NOV24, _utc(24), ticker="MSFT")

    assert _signals(pg_engine, _utc(20)) == {"AAPL": 400_000.0}
    assert _signals(pg_engine, _utc(19)) == {"AAPL": 400_000.0}          # written at exactly now
    assert _signals(pg_engine, _utc(19, hour=11)) == {}
    assert _signals(pg_engine, _utc(25)) == {"AAPL": 400_000.0, "MSFT": 900_000.0}


def test_real_types_past_and_current_reads(pg_engine, today_is):
    _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(pg_engine, "prediction_market")
    today_is(NOV27)

    assert dollar_flows.get_aggregate_flows(pg_engine, days=30, as_of=NOV20)["total_inflow"] is None
    assert dollar_flows.get_aggregate_flows(pg_engine, days=30, as_of=NOV27)["total_inflow"] == 490.0
    assert layer_retail._build_prediction_markets_node(pg_engine, NOV20).value is None
    assert layer_retail._build_prediction_markets_node(pg_engine, NOV27).value == 490.0
