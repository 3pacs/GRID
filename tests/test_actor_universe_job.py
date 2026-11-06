"""intelligence/actors/universe.py: identity resolution, graph assembly and
metrics on a synthetic universe (no DB). The PostgreSQL round trip (COPY,
versioned write, reads) is in tests/test_actor_universe_pg.py.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from intelligence.actors import universe as uni

igraph = pytest.importorskip("igraph")


def _actors() -> pd.DataFrame:
    rows = [
        # id, name, category, title, influence, icij_node_id, merged_into, source
        ("fed", "Federal Reserve", "central_bank", "", 0.95, None, None, None),
        ("ins_huang", "Jensen Huang", "insider", "CEO, NVIDIA", 0.8, None, None, None),
        ("ins_alias", "J. Huang", "insider", "", 0.5, None, "ins_huang", None),
        ("corp_NVDA", "NVIDIA Corp", "institution", "Semiconductors — NVDA", 0.7, None, None, None),
        ("ins_craig", "Craig Smith", "insider", "Director", 0.4, None, None, None),
        ("icij_entity_151138", "Shell Co Ltd", "icij_entity", "Offshore Entity (BVI)", 0.15, "151138", None, "icij"),
        ("icij_officer_77", "Some Officer", "icij_officer", "", 0.25, "77", None, "icij"),
        ("pep_1", "A Minister", "pep", "", 0.5, None, None, None),
        ("donor_1", "A Donor", "donor", "", 0.5, None, None, None),
        ("lonely", "Isolated Actor", "individual", "", 0.5, None, None, None),
    ]
    df = pd.DataFrame(rows, columns=["id", "name", "category", "title", "influence",
                                     "icij_node_id", "merged_into", "source"])
    df["positions_text"] = None
    df["board_text"] = None
    return df


def _edges() -> pd.DataFrame:
    rows = [
        ("ins_huang", "corp_NVDA", 0.9, "insider_trade", "actor_connections"),
        ("corp_NVDA", "ins_huang", 0.5, "insider_trade", "actor_connections"),   # reverse dup
        ("ins_huang", "corp_NVDA", 0.4, "insider_trade", "actor_connections"),   # weaker dup
        ("ins_alias", "fed", 0.6, "signal_linked", "actor_connections"),         # merged alias
        ("ins_craig", "corp_NVDA", 0.6, "wealth_flow", "wealth_flows"),
        ("fed", "fed", 0.9, "self", "actor_connections"),                        # self loop
        ("77", "151138", 0.5, "officer_of", "entity_relationships"),             # raw ICIJ ids
        ("icij_151138", "addr_9", 0.5, "registered_address", "entity_relationships"),
        ("NVDA", "corp_NVDA", 0.3, "signal_linked", "actor_connections"),        # ticker string
        ("old_alias", "pep_1", 0.5, "connected", "actor_connections"),           # actor_id_map alias
        ("donor_1", "pep_1", 0.5, "connected", "actor_connections"),
    ]
    df = pd.DataFrame(rows, columns=["src", "dst", "weight", "relationship", "table"])
    df["relationship"] = df["relationship"].astype("category")
    return df


def _universe() -> uni.LoadedUniverse:
    aliases = pd.DataFrame({"alias_id": ["old_alias"], "canonical_id": ["donor_1"]})
    return uni.LoadedUniverse(actors=_actors(), edges=_edges(), aliases=aliases)


def test_category_groups() -> None:
    assert uni.category_group("icij_entity") == "icij"
    assert uni.category_group("institution", "icij") == "icij"
    assert uni.category_group("pep") == "pep"
    assert uni.category_group("ofac_sanctioned") == "sanctioned"
    assert uni.category_group("donor") == "donor"
    assert uni.category_group("person_of_interest") == "watchlist"
    assert uni.category_group("insider") == "market"
    assert uni.category_group("corporation_quarantined_20260928") == "quarantined"
    assert "quarantined" in uni.BULK_GROUPS
    assert uni.category_group(float("nan"), float("nan")) == "market"


def test_endpoint_resolution_alias_icij_and_merge() -> None:
    u = _universe()
    raw = ["fed", "ins_alias", "77", "icij_151138", "151138", "old_alias", "addr_9", "NVDA"]
    out = uni.resolve_endpoint_ids(pd.Index(raw), u.actors, u.aliases).tolist()
    assert out == ["fed", "ins_huang", "icij_officer_77", "icij_entity_151138",
                   "icij_entity_151138", "donor_1", "addr_9", "NVDA"]


def test_assemble_dedupes_drops_self_loops_and_keeps_externals() -> None:
    g = uni.assemble_graph(_universe())
    ids = g.node_ids.tolist()
    # merged alias is not a node; actors come first, externals after
    assert "ins_alias" not in ids
    assert g.n_actors == 9
    assert set(ids[g.n_actors:]) == {"addr_9", "NVDA"}
    assert g.self_loops == 1
    pairs = {(ids[a], ids[b]) for a, b in zip(g.und_src, g.und_dst)}
    norm = {tuple(sorted(p)) for p in pairs}
    assert ("corp_NVDA", "ins_huang") in norm
    assert len(pairs) == len(norm), "undirected edges are unique"
    # the three huang<->nvda rows collapse to one undirected edge with the max weight
    i, j = ids.index("ins_huang"), ids.index("corp_NVDA")
    k = [n for n, (a, b) in enumerate(zip(g.und_src, g.und_dst)) if {a, b} == {i, j}]
    assert len(k) == 1 and g.und_w[k[0]] == pytest.approx(0.9)
    # directed keeps both directions
    dir_pairs = {(ids[a], ids[b]) for a, b in zip(g.dir_src, g.dir_dst)}
    assert ("ins_huang", "corp_NVDA") in dir_pairs and ("corp_NVDA", "ins_huang") in dir_pairs
    # the alias edge was redirected to the canonical actor
    assert ("ins_huang", "fed") in dir_pairs
    assert ("icij_officer_77", "icij_entity_151138") in dir_pairs


def test_metrics_tables_and_stats() -> None:
    u = _universe()
    table, communities, edges, stats = uni.compute_universe(u, betweenness_samples=4)
    assert stats["actors"] == 9 and stats["external_nodes"] == 2
    assert stats["isolated_actors"] == 1
    assert set(table["actor_id"]) == set(_actors()["id"]) - {"ins_alias"}
    row = table.set_index("actor_id")
    # sectors: title prefix, keyword, category; Craig is not AI (neighbour vote
    # from corp_NVDA gives him Semiconductors instead of the substring bug)
    assert row.loc["corp_NVDA", "sector"] == "Semiconductors"
    assert row.loc["ins_huang", "sector"] == "Semiconductors"
    assert row.loc["ins_craig", "sector"] == "Semiconductors"
    assert row.loc["ins_craig", "sector_basis"] == "neighbor"
    assert row.loc["icij_entity_151138", "sector"] == "Offshore"
    assert row.loc["lonely", "sector"] == "Other"
    # metrics
    assert row.loc["lonely", "degree"] == 0
    assert pd.isna(row.loc["lonely", "community_id"])
    assert row.loc["corp_NVDA", "degree"] == 3  # huang, craig, NVDA ticker node
    assert row.loc["corp_NVDA", "pagerank"] > row.loc["lonely", "pagerank"]
    assert ((table["importance"] >= 0) & (table["importance"] <= 1)).all()
    assert row.loc["pep_1", "category_group"] == "pep"
    assert row.loc["icij_officer_77", "category_group"] == "icij"
    # overview pool: all market actors + top of each bulk group (all here)
    assert table["in_overview_pool"].all()
    assert {"src", "dst", "weight", "relationship"} <= set(edges.columns)
    assert len(edges) >= 3
    pool_ids = set(table["actor_id"])
    assert set(edges["src"]) | set(edges["dst"]) <= pool_ids, "pool edges join actors only"
    # communities of >= 3 nodes, with top actors drawn from members
    assert len(communities) >= 1
    assert all(len(c) >= 1 for c in communities["top_actor_ids"])
    assert stats["timings"]["leiden_s"] >= 0


def test_pool_respects_bulk_group_cap(monkeypatch) -> None:
    monkeypatch.setattr(uni, "POOL_PER_BULK_GROUP", 0)
    table, _c, edges, _s = uni.compute_universe(_universe(), betweenness_samples=0)
    in_pool = table.set_index("actor_id")["in_overview_pool"]
    assert in_pool["fed"] and in_pool["ins_huang"]          # market: always
    assert not in_pool["pep_1"] and not in_pool["icij_entity_151138"]
    assert not (set(edges["src"]) | set(edges["dst"])) & {"pep_1", "donor_1"}


def test_betweenness_scales_sampled_sources() -> None:
    g = uni.assemble_graph(_universe())
    m = uni.compute_metrics(g, betweenness_samples=3)
    assert m.betweenness_samples == 3
    assert np.all(m.betweenness >= 0)
    m0 = uni.compute_metrics(g, betweenness_samples=0)
    assert m0.betweenness_samples == 0 and not m0.betweenness.any()


def test_maybe_launch_respects_flag_and_freshness(monkeypatch) -> None:
    launched: list[bool] = []
    monkeypatch.setattr(uni, "launch_job_subprocess", lambda **kw: launched.append(True) or True)

    monkeypatch.setenv("GRID_ACTOR_UNIVERSE_JOB", "0")
    assert uni.maybe_launch(engine=object()) is False

    monkeypatch.setenv("GRID_ACTOR_UNIVERSE_JOB", "1")
    monkeypatch.setattr(uni, "latest_complete_age_hours", lambda engine: 3.0)
    assert uni.maybe_launch(engine=object(), max_age_hours=20) is False
    monkeypatch.setattr(uni, "latest_complete_age_hours", lambda engine: 30.0)
    assert uni.maybe_launch(engine=object(), max_age_hours=20) is True
    monkeypatch.setattr(uni, "latest_complete_age_hours", lambda engine: None)
    assert uni.maybe_launch(engine=object()) is True
    assert launched == [True, True]


def test_launch_runs_a_niced_child_and_never_stacks(monkeypatch) -> None:
    calls: list[list[str]] = []

    class _Proc:
        pid = 4242

        def __init__(self, argv, **kwargs):  # noqa: ANN001
            calls.append(argv)
            self.alive = True

        def poll(self):  # noqa: ANN202
            return None if self.alive else 0

    import subprocess

    monkeypatch.setattr(subprocess, "Popen", _Proc)
    monkeypatch.setattr(uni, "_child", None)
    assert uni.launch_job_subprocess() is True
    assert calls[0][-2:] == ["-m", "intelligence.actors.universe"]
    assert uni.launch_job_subprocess() is False, "a running child is not relaunched"
    uni._child.alive = False
    assert uni.launch_job_subprocess() is True
    monkeypatch.setattr(uni, "_child", None)
