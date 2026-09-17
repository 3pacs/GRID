"""Curated reference maps must label themselves as curated.

Batch 4b of the fake-data remediation:

* ``intelligence/global_levers.py``  -- A-M13 (hand-assigned influence
  weights and personnel rosters labelled ``"hard_data"``, no roster vintage)
* ``intelligence/export_intel.py``   -- A-H12 (FY2024 revenue literals scaled
  by an undisclosed heuristic multiplier, served in 2026)
* ``api/routers/flows.py``          -- B-H11 / B-M25 (hardcoded activist,
  supply-chain, regulator and GLP-1 edges shipped with invented numeric
  strength and ``confidence: "confirmed"``, the only edges so labelled)
* ``a2a/agent_card.py``             -- D-M25 (entity counts frozen in a
  machine-readable capability document)
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from a2a.agent_card import build_grid_agent_card
from intelligence import export_intel, global_levers

REPO_ROOT = Path(__file__).resolve().parents[1]


# ══════════════════════════════════════════════════════════════════════════
# Criterion 3 (levers half) + 6 — global_levers
# ══════════════════════════════════════════════════════════════════════════


@pytest.mark.unit
def test_no_hand_assigned_score_is_labelled_hard_data() -> None:
    src = (REPO_ROOT / "intelligence" / "global_levers.py").read_text(
        encoding="utf-8", errors="replace",
    )
    assert src.count('"confidence": "hard_data"') == 0
    assert src.count('"confidence": "curated_estimate"') > 0


def _all_actors(hierarchy: dict) -> list[dict]:
    return [
        actor
        for domain in hierarchy.values()
        for tier in domain.get("actors", {}).values()
        for actor in tier.values()
    ]


@pytest.mark.unit
def test_every_key_personnel_block_carries_roster_as_of() -> None:
    result = global_levers.get_lever_hierarchy(None)
    actors = _all_actors(result["hierarchy"])
    assert actors

    rostered = [a for a in actors if "key_personnel" in a]
    assert rostered, "hierarchy has no key_personnel blocks at all"
    for actor in rostered:
        assert "roster_as_of" in actor, actor.get("name")
    assert any(
        a["roster_as_of"] == global_levers.ROSTER_AS_OF
        for a in rostered
        if a["key_personnel"]
    )


@pytest.mark.unit
def test_curated_label_and_roster_reach_the_lever_payload() -> None:
    hierarchy = global_levers.get_lever_hierarchy(None)["hierarchy"]
    fed = hierarchy["monetary_policy"]["actors"]["tier_1"]["fed"]

    assert fed["confidence"] == "curated_estimate"
    assert fed["roster_as_of"] == global_levers.ROSTER_AS_OF
    assert any(p["name"] == "Jerome Powell" for p in fed["key_personnel"])


@pytest.mark.unit
def test_lever_domain_also_stamps_the_roster() -> None:
    domain = global_levers.get_lever_domain("monetary_policy")
    for actor in domain["actors"]["tier_1"].values():
        if "key_personnel" in actor:
            assert "roster_as_of" in actor


@pytest.mark.unit
def test_stamping_never_mutates_the_module_constant() -> None:
    global_levers.get_lever_hierarchy(None)
    global_levers.get_lever_domain("monetary_policy")

    raw = global_levers.LEVER_HIERARCHY["monetary_policy"]["actors"]["tier_1"]["fed"]
    assert "roster_as_of" not in raw


@pytest.mark.unit
def test_dynamic_actor_gets_a_null_roster_not_a_curation_date() -> None:
    hierarchy = {
        "monetary_policy": {
            "label": "x",
            "actors": {
                "tier_3": {
                    "discovered": {
                        "name": "Discovered",
                        "key_personnel": [],
                        "dynamic": True,
                    },
                },
            },
        },
    }
    global_levers._stamp_roster_vintage(hierarchy)
    actor = hierarchy["monetary_policy"]["actors"]["tier_3"]["discovered"]
    assert actor["roster_as_of"] is None


# ══════════════════════════════════════════════════════════════════════════
# Criterion 7 — export_intel
# ══════════════════════════════════════════════════════════════════════════


def _no_controls_engine() -> MagicMock:
    """An engine whose every query returns no rows."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = lambda s: conn
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = []
    return engine


@pytest.mark.unit
def test_tracked_ticker_impact_carries_as_of_and_basis() -> None:
    impact = export_intel.assess_revenue_impact(_no_controls_engine(), "NVDA")

    assert impact["as_of"] == "FY2024"
    assert impact["basis"] == export_intel.REVENUE_AT_RISK_BASIS
    assert impact["data_source"] == "curated_filing_estimate"
    assert isinstance(impact["estimated_revenue_at_risk_pct"], float)


@pytest.mark.unit
def test_ticker_without_a_fiscal_period_falls_back_to_the_table_vintage() -> None:
    impact = export_intel.assess_revenue_impact(_no_controls_engine(), "AMD")
    assert impact["as_of"] == export_intel.CHINA_REVENUE_TABLE_AS_OF
    assert impact["basis"] == export_intel.REVENUE_AT_RISK_BASIS


@pytest.mark.unit
def test_untracked_ticker_is_unknown_with_no_number() -> None:
    impact = export_intel.assess_revenue_impact(_no_controls_engine(), "ZZZZ")

    assert impact["risk_level"] == "UNKNOWN"
    assert impact["estimated_revenue_at_risk_pct"] is None
    assert impact["china_revenue_pct"] is None
    assert impact["as_of"] is None
    assert impact["basis"] == "no_filed_china_revenue_baseline"


@pytest.mark.unit
def test_every_impact_shape_has_as_of_and_basis() -> None:
    engine = _no_controls_engine()
    for ticker in ("NVDA", "ASML", "ZZZZ"):
        impact = export_intel.assess_revenue_impact(engine, ticker)
        assert "as_of" in impact and "basis" in impact, ticker


# ══════════════════════════════════════════════════════════════════════════
# Criterion 8 — flows sector-detail edges
# ══════════════════════════════════════════════════════════════════════════


def _sector_payload(sector_name: str) -> dict:
    """Build a full sector connection payload with no working DB.

    Every DB section of ``_build_sector_connections`` is wrapped in
    try/except, so a raising engine exercises exactly the curated-static
    half plus the node/lineage assembly -- which is the half under audit.
    """
    from analysis.sector_map import SECTOR_MAP
    from api.routers import flows

    flows._sector_connections_cache.clear()

    broken = MagicMock()
    broken.connect.side_effect = RuntimeError("no database in this test")

    sector = SECTOR_MAP[sector_name]
    tickers = [
        actor["ticker"]
        for sub in sector.get("subsectors", {}).values()
        for actor in (sub.get("actors", []) if isinstance(sub, dict) else [])
        if actor.get("ticker")
    ]
    return flows._build_sector_connections(
        broken, sector_name, tickers, sector.get("subsectors", {}),
    )


@pytest.fixture(scope="module")
def staples_payload() -> dict:
    return _sector_payload("Consumer Staples")


@pytest.mark.unit
def test_every_edge_declares_its_provenance(staples_payload: dict) -> None:
    edges = staples_payload["edges"]
    assert edges, "no edges built; the static maps did not match this sector"
    for e in edges:
        assert e.get("provenance") in {"curated_static", "db_derived"}, e


@pytest.mark.unit
def test_no_curated_edge_claims_to_be_confirmed(staples_payload: dict) -> None:
    """The implication under audit, asserted over the whole payload."""
    for e in staples_payload["edges"]:
        if e["provenance"] == "curated_static":
            assert e["confidence"] != "confirmed", e


@pytest.mark.unit
def test_curated_edges_ship_no_invented_strength(staples_payload: dict) -> None:
    curated = [
        e for e in staples_payload["edges"] if e["provenance"] == "curated_static"
    ]
    assert curated, "expected the hardcoded maps to produce edges here"
    for e in curated:
        assert e["strength"] is None, e
        assert e["strength_basis"] == "editorial"
        assert e["as_of"] == flows_as_of(), e


def flows_as_of() -> str:
    from api.routers import flows

    return flows.CURATED_MAPS_AS_OF


@pytest.mark.unit
def test_curated_lineage_is_labelled(staples_payload: dict) -> None:
    assert staples_payload["lineage_provenance"] == "curated_static"
    assert staples_payload["lineage_as_of"] == flows_as_of()


@pytest.mark.unit
def test_curated_edges_cover_all_four_static_maps(staples_payload: dict) -> None:
    """Every static map that fires in this sector produces labelled edges."""
    kinds = {
        e["type"]
        for e in staples_payload["edges"]
        if e["provenance"] == "curated_static"
    }
    # activist_holder / private_control come from _ACTIVIST_HOLDERS.
    assert kinds & {"activist_holder", "private_control"}
    assert "supply_chain" in kinds        # _SUPPLY_CHAIN
    assert "regulatory_threat" in kinds   # _REGULATOR_THREATS
    assert "demand_destruction" in kinds  # _GLP1_PRESSURE


@pytest.mark.unit
def test_sector_alias_route_is_not_awaiting_a_dict() -> None:
    """/flows/sector/{s} used to `await` a sync handler and raise."""
    src = (REPO_ROOT / "api" / "routers" / "flows.py").read_text(
        encoding="utf-8", errors="replace",
    )
    assert "return await get_sector_detail(" not in src


# ══════════════════════════════════════════════════════════════════════════
# Criterion 9 — a2a agent card
# ══════════════════════════════════════════════════════════════════════════


@pytest.mark.unit
def test_agent_card_source_has_no_hardcoded_entity_counts() -> None:
    src = (REPO_ROOT / "a2a" / "agent_card.py").read_text(
        encoding="utf-8", errors="replace",
    )
    assert re.search(r"495 named actors|464\+ data sources", src) is None


@pytest.mark.unit
def test_served_agent_card_advertises_no_entity_count() -> None:
    card = build_grid_agent_card("https://grid.example.com").to_dict()

    blob = card["description"] + " ".join(s["description"] for s in card["skills"])
    assert re.search(r"\b\d{2,}\+?\s+(named actors|data sources|macro)", blob) is None
    assert card["skills"], "card lost its skills"
