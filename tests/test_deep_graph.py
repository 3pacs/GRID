"""Tests for ``intelligence.deep_graph``."""

from __future__ import annotations

import pytest

from intelligence import deep_graph


def _drill_result(ticker: str, actors: list[dict]) -> dict:
    return {
        "ticker": ticker,
        "layers": [
            {
                "depth": 1,
                "layer_name": "Fixture",
                "actors": actors,
                "connections": [],
                "dollar_flows": 0.0,
                "count": len(actors),
            }
        ],
        "total_actors": len(actors),
        "total_connections": 0,
        "total_dollar_flow": 0.0,
        "capped": False,
    }


def test_find_overlaps_uses_fixture_graphs_and_sorts_by_significance(monkeypatch):
    engine = object()
    calls: list[str] = []
    ensured: list[object] = []
    stored: dict[str, object] = {}

    fixture_graphs = {
        "AAPL": _drill_result(
            "AAPL",
            [
                {
                    "id": "company:AAPL",
                    "label": "Apple",
                    "type": "company",
                    "ticker": "AAPL",
                },
                {
                    "id": "person:jane_smith",
                    "label": "Jane Smith",
                    "type": "person",
                    "connected_via": "board",
                    "dollar_amount": 1_500_000,
                },
                {
                    "id": "company:PLTR",
                    "label": "Palantir",
                    "type": "company",
                    "ticker": "PLTR",
                    "connected_via": "other board seat",
                },
                {
                    "id": "committee:armed_services",
                    "label": "Armed Services",
                    "type": "committee",
                    "connected_via": "PAC recipients",
                },
                {"id": "fund:titan", "label": "Titan Capital", "type": "fund"},
                {"id": "politician:a", "label": "Rep. A", "type": "politician"},
            ],
        ),
        "MSFT": _drill_result(
            "MSFT",
            [
                {
                    "id": "company:MSFT",
                    "label": "Microsoft",
                    "type": "company",
                    "ticker": "MSFT",
                },
                {
                    "id": "person:jane_smith",
                    "label": "Jane Smith",
                    "type": "person",
                    "connected_via": "strategic advisor",
                    "dollar_amount": 2_750_000,
                },
                {
                    "id": "company:PLTR",
                    "label": "Palantir",
                    "type": "company",
                    "ticker": "PLTR",
                    "connected_via": "vendor ecosystem",
                },
                {
                    "id": "committee:armed_services",
                    "label": "Armed Services",
                    "type": "committee",
                    "connected_via": "PAC recipients",
                },
                {"id": "fund:titan", "label": "Titan Capital", "type": "fund"},
                {"id": "politician:b", "label": "Rep. B", "type": "politician"},
            ],
        ),
    }

    def fake_deep_drill(received_engine, ticker):
        assert received_engine is engine
        calls.append(ticker)
        return fixture_graphs[ticker]

    def fake_store_overlaps(received_engine, overlaps, ticker_a, ticker_b):
        stored["engine"] = received_engine
        stored["overlaps"] = overlaps
        stored["tickers"] = (ticker_a, ticker_b)
        return len(overlaps)

    monkeypatch.setattr(
        deep_graph,
        "ensure_table",
        lambda received_engine: ensured.append(received_engine),
    )
    monkeypatch.setattr(deep_graph, "deep_drill", fake_deep_drill)
    monkeypatch.setattr(deep_graph, "_store_overlaps", fake_store_overlaps)

    overlaps = deep_graph.find_overlaps(engine, " aapl ", "msft")

    assert ensured == [engine]
    assert calls == ["AAPL", "MSFT"]
    assert stored == {
        "engine": engine,
        "overlaps": overlaps,
        "tickers": ("AAPL", "MSFT"),
    }
    assert [overlap.significance for overlap in overlaps] == sorted(
        [overlap.significance for overlap in overlaps],
        reverse=True,
    )

    connection_points = {overlap.connection_point for overlap in overlaps}
    assert "Apple" not in connection_points
    assert "Microsoft" not in connection_points
    assert "Jane Smith" in connection_points
    assert "Committee: Armed Services" in connection_points

    jane_overlap = next(overlap for overlap in overlaps if overlap.connection_point == "Jane Smith")
    assert jane_overlap is overlaps[0]
    assert jane_overlap.actor_a == "AAPL"
    assert jane_overlap.actor_b == "MSFT"
    assert jane_overlap.path_a == ["AAPL", "board", "Jane Smith"]
    assert jane_overlap.path_b == ["MSFT", "strategic advisor", "Jane Smith"]
    assert jane_overlap.shared_tickers == ["PLTR"]
    assert jane_overlap.shared_committees == ["Armed Services"]
    assert jane_overlap.shared_funds == ["Titan Capital"]
    assert jane_overlap.total_dollar_flow == pytest.approx(4_250_000)
    assert jane_overlap.significance == pytest.approx(0.47)
