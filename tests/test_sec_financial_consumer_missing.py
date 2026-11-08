"""Missing current SEC metrics stay unavailable in downstream consumers.

Pure functions and mocked connections only. Run with --noconftest through
run_sec_financial_period_checks.py to block sockets and PostgreSQL.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from api.routers import capital_flow as api
from features import lab
from intelligence import fundamental_divergence as fd
from intelligence import sector_health as sh


COMPLETE = {
    "revenue": 100.0, "cogs": 40.0, "opex": 20.0, "capex": 10.0,
    "tax": 5.0, "dividends": 2.0, "buybacks": 3.0, "r_and_d": 4.0,
    "debt_issuance": 0.0, "acquisitions": 0.0,
}
REQUIRED = {
    "gross_margin": ("revenue", "cogs"),
    "dividend_payout": ("dividends", "revenue", "cogs", "opex", "tax"),
    "fcf_conversion": ("revenue", "cogs", "opex", "capex"),
    "shareholder_yield": ("revenue", "dividends", "buybacks"),
    "reinvestment_ratio": ("revenue", "capex", "r_and_d"),
}


@pytest.mark.parametrize("calculate", [api._compute_ratios, lab._compute_ratios_from_amounts])
@pytest.mark.parametrize("ratio,missing", [
    (ratio, metric) for ratio, metrics in REQUIRED.items() for metric in metrics
])
@pytest.mark.parametrize("explicit_none", [False, True])
def test_composite_ratio_requires_every_reported_term(calculate, ratio, missing, explicit_none):
    amounts = dict(COMPLETE)
    if explicit_none:
        amounts[missing] = None
    else:
        del amounts[missing]
    assert calculate(amounts)[ratio] is None


@pytest.mark.parametrize("calculate", [api._compute_ratios, lab._compute_ratios_from_amounts])
def test_reported_zero_is_not_missing(calculate):
    amounts = {metric: 0.0 for metric in COMPLETE}
    amounts["revenue"] = 100.0
    result = calculate(amounts)
    assert result["gross_margin"] == 1.0
    assert result["fcf_conversion"] == 1.0
    assert result["dividend_payout"] == 0.0
    assert result["shareholder_yield"] == 0.0
    assert result["reinvestment_ratio"] == 0.0
    assert result["net_debt_issuance_intensity"] == 0.0


def test_complete_api_and_percentile_ratios_agree():
    result = api._compute_ratios(COMPLETE)
    peer = lab._compute_ratios_from_amounts(COMPLETE)
    for ratio in REQUIRED:
        assert result[ratio] == pytest.approx(peer[ratio], abs=0.000005)


def flow(flow_type, amount, direction):
    return dict(flow_type=flow_type, amount_usd=amount, direction=direction,
                currency="USD", source_filing="10-K", confidence="confirmed")


def test_period_with_no_reported_outflow_has_no_zero_outflow_or_net():
    period = api._build_period(date(2025, 12, 31), [flow("revenue", 100, "in")], "annual")
    assert period["totals"] == {"inflow_usd": 100.0, "outflow_usd": None, "net_usd": None}
    assert period["ratios"]["fcf_conversion"] is None


def test_reported_zero_outflow_is_retained_in_totals():
    rows = [flow("revenue", 100, "in"), flow("cogs", 0, "out")]
    period = api._build_period(date(2025, 12, 31), rows, "annual")
    assert period["totals"] == {"inflow_usd": 100.0, "outflow_usd": 0.0, "net_usd": 100.0}


def test_empty_period_does_not_create_zero_totals():
    period = api._build_period(date(2025, 12, 31), [], "annual")
    assert all(value is None for value in period["totals"].values())


def summary_period(amounts):
    return {"_amounts": amounts, "ratios": api._compute_ratios(amounts),
            "inflows": [], "outflows": []}


@pytest.mark.parametrize("missing", ["dividends", "buybacks"])
def test_shareholder_total_does_not_sum_partial_components(missing):
    partial = dict(COMPLETE)
    del partial[missing]
    result = api._compute_summary([summary_period(COMPLETE), summary_period(partial)])
    assert result["shareholder_return_3y_total_usd"] is None


def test_shareholder_total_retains_reported_zero():
    result = api._compute_summary([summary_period({"revenue": 100, "dividends": 0, "buybacks": 0})])
    assert result["shareholder_return_3y_total_usd"] == 0
    assert result["shareholder_yield_3y_avg"] == 0


def period_map():
    return {date(year, 12, 31): dict(COMPLETE) for year in range(2022, 2026)}


def mock_connection(rows):
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = rows
    return conn


@pytest.mark.parametrize("year", [2022, 2025])
def test_fundamental_margin_requires_original_endpoints(year):
    periods = period_map()
    del periods[date(year, 12, 31)]["cogs"]
    result = fd._fundamentals_from_period_map(periods)
    assert result["margin_trend"] is None


@pytest.mark.parametrize("missing", ["dividends", "buybacks"])
def test_fundamental_yield_does_not_impute_missing_component(missing):
    periods = period_map()
    for amounts in periods.values():
        del amounts[missing]
    result = fd._fundamentals_from_period_map(periods)
    assert result["shareholder_yield"] is None
    assert fd._build_fundamental_score(result, [0.0], [0.0]) is None


def test_fundamental_reported_zero_yield_and_flat_margin_are_valid():
    periods = period_map()
    for amounts in periods.values():
        amounts.update(dividends=0, buybacks=0)
    result = fd._fundamentals_from_period_map(periods)
    assert result["shareholder_yield"] == 0
    assert result["margin_trend"] == "flat"
    assert fd._build_fundamental_score(result, [0.0], [0.0]) == 50


@pytest.mark.parametrize("missing", ["revenue_cagr", "margin_trend", "shareholder_yield"])
def test_fundamental_score_does_not_impute_neutral_component(missing):
    result = {"revenue_cagr": 0.2, "margin_trend": "expanding", "shareholder_yield": 0.05}
    result[missing] = None
    assert fd._build_fundamental_score(result, [0.1, 0.2], [0.01, 0.05]) is None


def test_single_and_batch_loaders_share_missing_value_semantics(monkeypatch):
    monkeypatch.setattr(fd, "_table_exists", lambda *args: True)
    rows = [(fp, "revenue", 100.0) for fp in period_map()]
    rows += [(fp, "cogs", None) for fp in period_map()]
    single_conn = mock_connection(rows)
    batch_conn = mock_connection([("NVDA", *row) for row in rows])
    single = fd._load_ticker_fundamentals(single_conn, "NVDA")
    batch = fd._load_batch_fundamentals(batch_conn, ["NVDA"])["NVDA"]
    assert single == batch
    assert single["margin_trend"] is None
    assert single["shareholder_yield"] is None
    assert "capital_flows_current" in str(single_conn.execute.call_args.args[0])
    assert "capital_flows_current" in str(batch_conn.execute.call_args.args[0])


def test_incomplete_fundamentals_do_not_publish_trading_verdict(monkeypatch):
    monkeypatch.setattr(fd, "_load_universe", lambda: [fd.SectorTicker("AAA", "Tech")])
    monkeypatch.setattr(fd, "_load_batch_fundamentals", lambda *args: {
        "AAA": {"revenue_cagr": 0.5, "margin_trend": None, "shareholder_yield": 0.1},
    })
    monkeypatch.setattr(fd, "_load_batch_price_cagrs", lambda *args: {"AAA": -0.5})
    monkeypatch.setattr(fd, "get_financial_generation", lambda conn: "a" * 32)
    assert fd.compute_divergence(MagicMock(), as_of=date(2026, 11, 8)) == []


def sector_rows(amounts, year=2025):
    return [(date(year, 12, 31), metric, value) for metric, value in amounts.items()]


@pytest.mark.parametrize("missing", sorted(COMPLETE.keys() - {"tax", "r_and_d", "acquisitions"}))
def test_sector_capital_score_requires_all_components(monkeypatch, missing):
    monkeypatch.setattr(sh, "_table_exists", lambda *args: True)
    amounts = dict(COMPLETE)
    del amounts[missing]
    conn = mock_connection(sector_rows(amounts))
    assert sh._capital_allocation_score(conn, ["NVDA"]) is None


def test_sector_reported_zero_components_remain_valid(monkeypatch):
    monkeypatch.setattr(sh, "_table_exists", lambda *args: True)
    amounts = {metric: 0.0 for metric in COMPLETE}
    amounts["revenue"] = 100.0
    conn = mock_connection(sector_rows(amounts))
    assert sh._capital_allocation_score(conn, ["NVDA"]) == pytest.approx(2 / 3)


@pytest.mark.parametrize("missing_year", [2022, 2025])
def test_sector_margin_does_not_substitute_older_endpoint(monkeypatch, missing_year):
    monkeypatch.setattr(sh, "_table_exists", lambda *args: True)
    rows = []
    for year in range(2022, 2026):
        amounts = {"revenue": 100.0, "cogs": 40.0}
        if year == missing_year:
            del amounts["cogs"]
        rows.extend(sector_rows(amounts, year))
    assert sh._margin_score(mock_connection(rows), ["NVDA"]) is None


def test_sector_revenue_only_is_not_a_margin_or_capital_score(monkeypatch):
    monkeypatch.setattr(sh, "_table_exists", lambda *args: True)
    rows = sector_rows({"revenue": 100}, 2025) + sector_rows({"revenue": 90}, 2024)
    assert sh._margin_score(mock_connection(rows), ["NVDA"]) is None
    assert sh._capital_allocation_score(mock_connection(rows), ["NVDA"]) is None
