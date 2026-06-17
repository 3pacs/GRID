from __future__ import annotations

import pytest

from intelligence import global_levers


def test_get_lever_hierarchy_enriches_small_fixture_with_live_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hierarchy_fixture = {
        "monetary_policy": {
            "label": "Money",
            "actors": {
                "tier_1": {
                    "fed": {
                        "name": "Federal Reserve",
                        "influence": 1.0,
                    }
                },
                "tier_2": {
                    "banks": {
                        "name": "Commercial Banks",
                        "influence": 0.5,
                    }
                },
            },
            "transmission": "rates -> credit -> demand",
        }
    }
    live_fixture = {
        "monetary_policy": {
            "metrics": [{"name": "fed_funds", "value": 5.25}],
            "status": "rates restrictive",
            "signals": [{"actor": "fed", "action": "holds policy steady"}],
        }
    }
    engine = object()
    injected: list[object] = []

    def fake_inject_dynamic_actors(received_engine: object, hierarchy: dict) -> None:
        injected.append(received_engine)
        hierarchy["monetary_policy"]["actors"]["tier_3"] = {
            "credit_funds": {
                "name": "Credit Funds",
                "influence": 0.4,
            }
        }

    monkeypatch.setattr(global_levers, "LEVER_HIERARCHY", hierarchy_fixture)
    monkeypatch.setattr(
        global_levers,
        "_fetch_live_lever_data",
        lambda received_engine: live_fixture if received_engine is engine else {},
    )
    monkeypatch.setattr(
        global_levers,
        "_inject_dynamic_actors",
        fake_inject_dynamic_actors,
    )

    result = global_levers.get_lever_hierarchy(engine)

    domain = result["hierarchy"]["monetary_policy"]
    assert injected == [engine]
    assert domain["live_metrics"] == live_fixture["monetary_policy"]["metrics"]
    assert domain["status_summary"] == "rates restrictive"
    assert domain["active_signals"] == live_fixture["monetary_policy"]["signals"]
    assert domain["actors"]["tier_3"]["credit_funds"]["name"] == "Credit Funds"
    assert result["summary"]["monetary_policy"] == {
        "label": "Money",
        "actor_count": 3,
        "tiers": {"tier_1": 1, "tier_2": 1, "tier_3": 1},
        "transmission": "rates -> credit -> demand",
        "has_live_data": True,
    }
    assert result["total_domains"] == 1
    assert result["total_actors"] == 3
    assert "live_metrics" not in hierarchy_fixture["monetary_policy"]


def test_trace_lever_chain_returns_exact_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain = [
        {"actor": "fed", "domain": "monetary_policy", "action": "raises rates"},
        {"actor": "banks", "domain": "monetary_policy", "action": "tighten credit"},
    ]
    monkeypatch.setattr(global_levers, "LEVER_CHAINS", {"rate_hike": chain})

    result = global_levers.trace_lever_chain("rate_hike")

    assert result == chain


def test_trace_lever_chain_fuzzy_matches_event_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain = [
        {"actor": "opec_plus", "domain": "energy", "action": "cuts supply"},
        {"actor": "consumer", "domain": "fiscal_policy", "action": "pays more"},
    ]
    monkeypatch.setattr(global_levers, "LEVER_CHAINS", {"oil_supply_cut": chain})

    result = global_levers.trace_lever_chain("oil supply shock")

    assert result == [{"_matched_event": "oil_supply_cut"}, *chain]
