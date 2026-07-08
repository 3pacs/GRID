from __future__ import annotations

import copy
from typing import Any

import pytest

from intelligence import global_levers


def _hierarchy_fixture() -> dict[str, dict[str, Any]]:
    return {
        "monetary_policy": {
            "label": "Money",
            "actors": {
                "tier_1": {
                    "fed": {
                        "name": "Federal Reserve",
                        "influence": 1.0,
                        "cross_domain": {
                            "information": "fed_communications",
                        },
                    },
                },
                "tier_2": {
                    "banks": {
                        "name": "Commercial Banks",
                        "influence": 0.6,
                        "cross_domain": {},
                    },
                },
            },
            "transmission": "rates -> credit -> demand",
        },
        "information": {
            "label": "Information",
            "actors": {
                "tier_1": {
                    "fed": {
                        "name": "Federal Reserve",
                        "influence": 0.8,
                        "cross_domain": {
                            "monetary_policy": "policy_guidance_moves_rates",
                        },
                    },
                },
            },
            "transmission": "guidance -> expectations -> positioning",
        },
    }


def test_get_lever_hierarchy_enriches_summary_without_mutating_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_hierarchy = _hierarchy_fixture()
    original = copy.deepcopy(source_hierarchy)
    engine = object()
    injected_engines: list[object] = []
    live_data = {
        "monetary_policy": {
            "metrics": [{"name": "fed_funds", "value": 5.25}],
            "status": "rates restrictive",
            "signals": [{"actor": "fed", "action": "holds policy steady"}],
        },
    }

    def fake_inject_dynamic_actors(received_engine: object, hierarchy: dict) -> None:
        injected_engines.append(received_engine)
        hierarchy["monetary_policy"]["actors"]["tier_3"] = {
            "credit_funds": {
                "name": "Credit Funds",
                "influence": 0.4,
                "cross_domain": {},
            },
        }

    monkeypatch.setattr(global_levers, "LEVER_HIERARCHY", source_hierarchy)
    monkeypatch.setattr(global_levers, "_fetch_live_lever_data", lambda received: live_data)
    monkeypatch.setattr(global_levers, "_inject_dynamic_actors", fake_inject_dynamic_actors)

    result = global_levers.get_lever_hierarchy(engine)

    monetary = result["hierarchy"]["monetary_policy"]
    assert injected_engines == [engine]
    assert monetary["live_metrics"] == live_data["monetary_policy"]["metrics"]
    assert monetary["status_summary"] == "rates restrictive"
    assert monetary["active_signals"] == live_data["monetary_policy"]["signals"]
    assert monetary["actors"]["tier_3"]["credit_funds"]["name"] == "Credit Funds"
    assert result["summary"]["monetary_policy"] == {
        "label": "Money",
        "actor_count": 3,
        "tiers": {"tier_1": 1, "tier_2": 1, "tier_3": 1},
        "transmission": "rates -> credit -> demand",
        "has_live_data": True,
    }
    assert result["summary"]["information"]["has_live_data"] is False
    assert result["total_domains"] == 2
    assert result["total_actors"] == 4
    assert source_hierarchy == original


def test_get_lever_domain_adds_other_appearances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(global_levers, "LEVER_HIERARCHY", _hierarchy_fixture())
    monkeypatch.setattr(
        global_levers,
        "_ACTOR_INDEX",
        {
            "fed": [
                {
                    "domain": "monetary_policy",
                    "tier": "tier_1",
                    "name": "Federal Reserve",
                    "influence": 1.0,
                },
                {
                    "domain": "information",
                    "tier": "tier_1",
                    "name": "Federal Reserve",
                    "influence": 0.8,
                },
            ],
        },
    )

    result = global_levers.get_lever_domain("monetary_policy")

    assert result["domain"] == "monetary_policy"
    assert result["label"] == "Money"
    assert result["transmission"] == "rates -> credit -> demand"
    fed = result["actors"]["tier_1"]["fed"]
    assert fed["name"] == "Federal Reserve"
    assert fed["also_appears_in"] == [
        {
            "domain": "information",
            "tier": "tier_1",
            "name": "Federal Reserve",
            "influence": 0.8,
        },
    ]


def test_get_lever_domain_unknown_domain_lists_available_domains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(global_levers, "LEVER_HIERARCHY", _hierarchy_fixture())

    result = global_levers.get_lever_domain("unknown")

    assert result == {
        "error": "Unknown domain 'unknown'",
        "available_domains": ["monetary_policy", "information"],
    }


def test_trace_lever_chain_exact_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain = [
        {"actor": "fed", "domain": "monetary_policy", "action": "raises rates"},
        {"actor": "banks", "domain": "monetary_policy", "action": "tighten credit"},
    ]
    monkeypatch.setattr(global_levers, "LEVER_CHAINS", {"rate_hike": chain})

    assert global_levers.trace_lever_chain("rate_hike") == chain


def test_trace_lever_chain_fuzzy_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain = [
        {"actor": "opec_plus", "domain": "energy", "action": "cuts supply"},
        {"actor": "consumer", "domain": "fiscal_policy", "action": "pays more"},
    ]
    monkeypatch.setattr(global_levers, "LEVER_CHAINS", {"oil_supply_cut": chain})

    result = global_levers.trace_lever_chain("oil supply shock")

    assert result == [{"_matched_event": "oil_supply_cut"}, *chain]


def test_trace_lever_chain_unknown_event_lists_available_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(global_levers, "LEVER_CHAINS", {"tariff_war": []})

    result = global_levers.trace_lever_chain("asteroid mining boom")

    assert result == [
        {
            "actor": "unknown",
            "domain": "unknown",
            "action": "No pre-built chain for 'asteroid mining boom'",
        },
        {
            "note": "Available events",
            "events": ["tariff_war"],
        },
    ]


def test_find_cross_domain_actors_returns_sorted_public_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(global_levers, "LEVER_HIERARCHY", _hierarchy_fixture())
    monkeypatch.setattr(
        global_levers,
        "_ACTOR_INDEX",
        {
            "fed": [
                {
                    "domain": "monetary_policy",
                    "tier": "tier_1",
                    "name": "Federal Reserve",
                    "influence": 1.0,
                },
                {
                    "domain": "information",
                    "tier": "tier_1",
                    "name": "Federal Reserve",
                    "influence": 0.8,
                },
            ],
            "banks": [
                {
                    "domain": "monetary_policy",
                    "tier": "tier_2",
                    "name": "Commercial Banks",
                    "influence": 0.6,
                },
            ],
            "analyst_research": [
                {
                    "domain": "information",
                    "tier": "tier_2",
                    "name": "Analyst Research",
                    "influence": 0.7,
                },
                {
                    "domain": "capital_allocation",
                    "tier": "tier_3",
                    "name": "Analyst Research",
                    "influence": 0.3,
                },
            ],
        },
    )

    result = global_levers.find_cross_domain_actors()

    assert [actor["actor_id"] for actor in result] == ["fed", "analyst_research"]
    fed = result[0]
    assert fed["name"] == "Federal Reserve"
    assert fed["domains"] == ["monetary_policy", "information"]
    assert fed["domain_count"] == 2
    assert fed["total_influence"] == 1.8
    assert fed["max_influence"] == 1.0
    assert fed["best_tier"] == "tier_1"
    assert fed["cross_domain_links"] == [
        {
            "from_domain": "monetary_policy",
            "to_domain": "information",
            "description": "fed_communications",
        },
        {
            "from_domain": "information",
            "to_domain": "monetary_policy",
            "description": "policy_guidance_moves_rates",
        },
    ]
