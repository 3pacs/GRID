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

The adapter also built signals from 13F rows, which are reported positions and
not flows; the last section covers that.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from analysis import money_flow_engine
from analysis.money_flow_engine import etf_flow_evidence, layer_crypto, layer_market, layer_retail
from intelligence import dollar_flows
from intelligence.adapters.base import sid
from intelligence.adapters.dollar_flows_adapter import DollarFlowsAdapter
from intelligence.signal_registry import RegisteredSignal, SignalRegistry, SignalType
from oracle.model_factory import ModelFactory, ModelSpec
from oracle.signal_aggregator import SignalAggregator
from tests import thirteenf_holdings_support as s

NOV10, NOV19, NOV20, NOV24, NOV26, NOV27 = (date(2026, 11, d) for d in (10, 19, 20, 24, 26, 27))
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
def test_todays_totals_are_unchanged(engine, today_is, as_of):
    _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(engine, "insider")
    _write(engine, "congressional", 90.0, NOV24, datetime(2026, 11, 24, 12, 0), direction="outflow")
    today_is(NOV27)

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


# ── One clock: the default date and "is it past" agree on any host ─────────
#
# Codex review 6085725060: the default was date.today() (the host's local
# date) while the classification used the UTC date. On a host behind UTC the
# two differ every evening: the default named a day the same code calls past.


@pytest.fixture()
def host_clock_is_a_day_behind_utc(monkeypatch, today_is):
    """UTC says 27 November; the host's local date is still the 26th."""
    local = type("_LocalDate", (date,), {"today": classmethod(lambda cls: NOV26)})
    for module in (dollar_flows, money_flow_engine, layer_retail, layer_market, layer_crypto):
        monkeypatch.setattr(module, "date", local)
    today_is(NOV27)


def test_the_default_as_of_is_the_utc_date_the_classification_uses(engine, host_clock_is_a_day_behind_utc):
    _write(engine, "insider", 490.0, NOV27, datetime(2026, 11, 27, 1, 0))

    default = dollar_flows.get_aggregate_flows(engine, days=30)

    # Before: as_of "2026-11-26" (the local date), and the 27 November row left out.
    assert default["as_of"] == "2026-11-27" and default["withheld_reason"] is None
    assert default["total_inflow"] == 490.0
    # The date the default reports is one the same function answers when named.
    assert dollar_flows.get_aggregate_flows(engine, days=30, as_of=NOV27) == default
    assert dollar_flows.is_past_as_of(date.fromisoformat(default["as_of"])) is False


def test_the_default_flow_map_is_todays_view_on_a_host_behind_utc(
    engine, host_clock_is_a_day_behind_utc, other_retail_nodes
):
    _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(engine, "prediction_market")

    def _prediction_markets(flow_map) -> dict:
        return next(n for layer in flow_map.to_dict()["layers"] for n in layer["nodes"]
                    if n["id"] == "prediction_markets")

    # Before: the default as_of was the local 26th, classified as past, so
    # today's own flow map withheld the node.
    assert _prediction_markets(money_flow_engine.build_flow_map(engine))["value"] == 490.0
    by_default = {n.id: n.value for n in layer_retail.build_retail_layer(engine).nodes}
    assert by_default["prediction_markets"] == 490.0


class _Reached(Exception):
    pass


@pytest.mark.parametrize("module, node_builder, layer_builder", [
    (layer_market, "_build_etf_flows", "build_market_layer"),
    (layer_crypto, "_build_btc_etf_flows_node", "build_crypto_layer"),
], ids=["market", "crypto"])
def test_the_other_classified_layers_default_to_the_same_clock(
    engine, host_clock_is_a_day_behind_utc, monkeypatch, module, node_builder, layer_builder
):
    """The market and crypto layers withhold their ETF current views for a
    past date (#1496). Their default date is today's, on that same clock."""
    def _stop_at_the_etf_node(engine_, as_of):
        raise _Reached(as_of)

    for name, value in vars(module).items():              # earlier nodes read PostgreSQL-only SQL
        if name.startswith("_build_") and callable(value) and name != node_builder:
            monkeypatch.setattr(module, name, lambda engine_, as_of, *a, **k: None)
    monkeypatch.setattr(module, node_builder, _stop_at_the_etf_node)

    with pytest.raises(_Reached) as reached:
        getattr(module, layer_builder)(engine)

    # Before: 26 November, which is_historical calls past.
    assert reached.value.args == (NOV27,)
    assert etf_flow_evidence.is_historical(reached.value.args[0]) is False


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


# ── Signal adapter: a 13F row is a reported position, not a flow ───────────
#
# Production, 2026-11-27 08:16Z (read-only): dollar_flows held 369 rows with
# source_type '13f'. 49 are legacy rows written 13 to 20 November: direction
# "inflow", evidence action UNKNOWN, no ticker, confidence "confirmed",
# 14,139,039,067,794 USD in all; the 28 written on the 20th made one
# no-ticker MAGNITUDE signal of 7,022,705,633,749. 320 were written on the
# 26th by the daily normalization: direction "observed", actions CLOSED and
# NEW, 291 with a ticker and 29 without. Of the adapter's 200 groups per run,
# 100 were 13F (two with no ticker) while 405 of 428 insider groups over the
# threshold were cut.


def _write_13f(engine, amount: float, written: datetime, *, direction: str, action: str,
               ticker: str | None, confidence: str, actor: str = "Filer") -> None:
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO dollar_flows (source_type, actor_name, ticker, amount_usd, direction, "
            "confidence, evidence, flow_date, created_at) "
            "VALUES ('13f', :actor, :ticker, :amt, :dir, :conf, :ev, :fd, :ca)"),
            {"actor": actor, "ticker": ticker, "amt": amount, "dir": direction, "conf": confidence,
             "ev": json.dumps({"cusip": "000000000", "action": action, "shares": 1, "pct_change": None,
                               "filing_accession": "0000000000-26-000001"}),
             "fd": written.date() - timedelta(days=7), "ca": written})


def _rows_shaped_like_production(engine) -> None:
    for n in range(28):                                   # the legacy portfolio totals
        _write_13f(engine, 250_000_000_000.0, _utc(20), direction="inflow", action="UNKNOWN",
                   ticker=None, confidence="confirmed", actor=f"Legacy {n}")
    _write_13f(engine, 2_283_782_911.0, _utc(26), direction="observed", action="NEW",
               ticker="XOM", confidence="derived")
    _write_13f(engine, 274_332_786.0, _utc(26), direction="observed", action="CLOSED",
               ticker="FLS", confidence="derived")
    _write_13f(engine, 3_847_160_143.0, _utc(26), direction="observed", action="NEW",
               ticker=None, confidence="derived")


def test_the_adapter_builds_no_signal_from_13f_rows(engine):
    _rows_shaped_like_production(engine)

    # Before: {None: 7003847160143.0, "XOM": 2283782911.0, "FLS": 274332786.0}
    # (None twice: the "inflow" and the "observed" totals, one signal each).
    assert DollarFlowsAdapter()._build_signals(engine, _utc(27, hour=8)) == []


def test_a_real_flow_beside_13f_rows_still_makes_its_signal(engine):
    _rows_shaped_like_production(engine)
    _write(engine, "insider", 400_000.0, NOV24, _utc(24), ticker="XOM")

    signals = DollarFlowsAdapter()._build_signals(engine, _utc(27, hour=8))

    assert [(sig.ticker, sig.value, sig.metadata["source_type"]) for sig in signals] == [
        ("XOM", 400_000.0, "insider")]
    assert all(sig.ticker for sig in signals)             # nothing addressed to every ticker


def test_13f_totals_no_longer_push_real_flows_out_of_the_200(engine):
    """The groups are cut to the 200 largest. 13F position values are far
    larger than an insider trade, so 200 of them used to take every place."""
    for n in range(200):
        _write_13f(engine, 5_000_000_000.0 + n, _utc(26), direction="observed", action="NEW",
                   ticker=f"T{n:03d}", confidence="derived")
    _write(engine, "insider", 400_000.0, NOV24, _utc(24), ticker="AAPL")

    # Before: 200 13F signals and no AAPL.
    assert _signals(engine, _utc(27, hour=8)) == {"AAPL": 400_000.0}


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


def test_real_types_adapter_leaves_13f_rows_out(pg_engine):
    _rows_shaped_like_production(pg_engine)
    _write(pg_engine, "insider", 400_000.0, NOV24, _utc(24), ticker="XOM")

    assert _signals(pg_engine, _utc(27, hour=8)) == {"XOM": 400_000.0}


# ── Changeover: 13F signals registered before the change ───────────────────
#
# Codex review 6085725060: the registry appends and never invalidates, so
# 13F signals registered by the previous adapter stay active until their
# four-hour valid_until, whatever the corrected adapter does. The case below
# is that changeover with synthetic evidence, through the real registry
# writer, the real model read and the real aggregator.

_MODEL_TABLES = (
    "CREATE TABLE oracle_models (name TEXT PRIMARY KEY, active BOOLEAN DEFAULT TRUE)",
    """CREATE TABLE signal_registry (
        id BIGSERIAL PRIMARY KEY, signal_id TEXT NOT NULL, source_module TEXT NOT NULL,
        signal_type TEXT NOT NULL, ticker TEXT, direction TEXT NOT NULL,
        value DOUBLE PRECISION NOT NULL, z_score DOUBLE PRECISION,
        confidence DOUBLE PRECISION NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        valid_from TIMESTAMPTZ NOT NULL, valid_until TIMESTAMPTZ,
        freshness_hours DOUBLE PRECISION, metadata JSONB, provenance TEXT,
        UNIQUE (signal_id, valid_from))""",
)


def _as_the_previous_adapter_registered(ticker: str | None, usd: float, at: datetime) -> RegisteredSignal:
    return RegisteredSignal(
        signal_id=sid("dollar_flows", "magnitude", ticker or "market", "13f", str(at.date())),
        source_module="dollar_flows", signal_type=SignalType.MAGNITUDE, ticker=ticker,
        direction="neutral", value=usd, z_score=None, confidence=0.5,
        valid_from=at, valid_until=at + timedelta(hours=4), freshness_hours=0.0,
        metadata={"source_type": "13f", "flow_count": 1, "amount_usd": usd},
        provenance=f"dollar_flows:13f:{ticker or ''}",
    )


def test_real_types_13f_signals_registered_before_the_change_last_four_hours_and_no_longer(
    pg_engine, monkeypatch
):
    monkeypatch.setenv("GRID_SIGNAL_REGISTRY", "1")
    with pg_engine.begin() as conn:
        for ddl in _MODEL_TABLES:
            conn.execute(text(ddl))
        conn.execute(text("INSERT INTO oracle_models (name, active) VALUES ('m', TRUE)"))
    spec = ModelSpec(name="m", signal_sources=["dollar_flows", "feature:equity"], min_signals=3)
    factory = ModelFactory.__new__(ModelFactory)
    factory.engine = pg_engine
    factory.get_model_spec = lambda name: spec

    last_old_run = _utc(27, hour=8)
    SignalRegistry.register([
        _as_the_previous_adapter_registered(None, 7_022_705_633_749.0, last_old_run),
        _as_the_previous_adapter_registered("AAPL", 2_283_782_911.0, last_old_run),
        RegisteredSignal(
            signal_id=sid("feature:equity", "AAPL"), source_module="feature:equity",
            signal_type=SignalType.DIRECTIONAL, ticker="AAPL", direction="bullish", value=0.6,
            z_score=0.6, confidence=0.9, valid_from=last_old_run,
            valid_until=last_old_run + timedelta(days=1)),
    ], pg_engine)

    # The corrected adapter runs an hour later over the same stored 13F rows.
    _rows_shaped_like_production(pg_engine)
    first_new_run = last_old_run + timedelta(hours=1)
    corrected = DollarFlowsAdapter()._build_signals(pg_engine, first_new_run)
    assert corrected == [] and SignalRegistry.register(corrected, pg_engine) == 0

    def _vote(as_of: datetime):
        signals = factory.get_signals_for_model("m", as_of, ticker="AAPL")
        return signals, SignalAggregator().aggregate(signals, spec.weight_config, as_of)

    # Carryover: until the old rows' valid_until the model still receives them.
    signals, vote = _vote(last_old_run + timedelta(hours=3, minutes=59))
    assert sorted((sig["source_module"], sig["ticker"] or "", sig["direction"]) for sig in signals) == [
        ("dollar_flows", "", "neutral"), ("dollar_flows", "AAPL", "neutral"),
        ("feature:equity", "AAPL", "bullish")]
    assert len(signals) >= spec.min_signals                       # the model votes on AAPL
    assert (vote.signal_count, vote.neutral_count, vote.direction) == (3, 2, "bullish")
    assert vote.strength == pytest.approx(0.2)                    # 0.6 diluted over three entries
    assert vote.confidence == pytest.approx((0.5 + 0.5 + 0.9) / 3, abs=1e-4)

    # Four hours after the last run of the previous adapter they are gone, and
    # nothing the corrected adapter wrote replaces them.
    signals, vote = _vote(last_old_run + timedelta(hours=4))
    assert [sig["source_module"] for sig in signals] == ["feature:equity"]
    assert len(signals) < spec.min_signals                        # OracleEngine.predict skips the model here
    assert (vote.signal_count, vote.neutral_count) == (1, 0)
    assert vote.strength == pytest.approx(0.6) and vote.confidence == pytest.approx(0.9)

    with pg_engine.connect() as conn:
        still_valid = conn.execute(text(
            "SELECT count(*) FROM signal_registry WHERE source_module = 'dollar_flows' "
            "AND provenance LIKE 'dollar_flows:13f:%' AND valid_until > :t"),
            {"t": last_old_run + timedelta(hours=4)}).scalar()
    assert still_valid == 0                                       # the changeover check, as a query


def test_real_types_past_and_current_reads(pg_engine, today_is):
    _known_403_on_the_20th_then_rebuilt_as_490_on_the_24th(pg_engine, "prediction_market")
    today_is(NOV27)

    assert dollar_flows.get_aggregate_flows(pg_engine, days=30, as_of=NOV20)["total_inflow"] is None
    assert dollar_flows.get_aggregate_flows(pg_engine, days=30, as_of=NOV27)["total_inflow"] == 490.0
    assert layer_retail._build_prediction_markets_node(pg_engine, NOV20).value is None
    assert layer_retail._build_prediction_markets_node(pg_engine, NOV27).value == 490.0
