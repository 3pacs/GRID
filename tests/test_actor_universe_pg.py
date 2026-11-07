"""Actor universe on real PostgreSQL: migration, the offline job's COPY load +
versioned write, and every request-time read (overview, ego, search,
communities, /actor-network, ticker context).

Runs in a throwaway schema (``search_path = <scratch>, public``) so it never
touches shared tables. Skips when PostgreSQL is unavailable.
"""

from __future__ import annotations

import importlib
import json
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

pytest.importorskip("igraph")

_MIGRATION = "migrations.versions.actor_universe_20261106"

_BASE_DDL = [
    """
    CREATE TABLE actors (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, tier TEXT NOT NULL DEFAULT 'individual',
        category TEXT NOT NULL, title TEXT, net_worth_estimate NUMERIC, aum NUMERIC,
        influence_score NUMERIC DEFAULT 0.5, trust_score NUMERIC DEFAULT 0.5,
        motivation_model TEXT DEFAULT 'unknown', connections JSONB DEFAULT '[]',
        known_positions JSONB DEFAULT '[]', board_seats JSONB DEFAULT '[]',
        political_affiliations JSONB DEFAULT '[]', data_sources JSONB DEFAULT '[]',
        credibility TEXT DEFAULT 'inferred', metadata JSONB DEFAULT '{}',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), source TEXT, degree INT DEFAULT 0,
        icij_node_id TEXT, merged_into TEXT, provenance TEXT NOT NULL DEFAULT 'unknown',
        provenance_as_of DATE
    )
    """,
    "CREATE INDEX idx_actors_name_lower ON actors (lower(name))",
    """
    CREATE TABLE actor_connections (
        id SERIAL PRIMARY KEY, actor_a TEXT, actor_b TEXT, relationship TEXT,
        strength NUMERIC, evidence JSONB, discovered_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (actor_a, actor_b, relationship)
    )
    """,
    "CREATE INDEX ix_ac_a_strength ON actor_connections (actor_a, strength DESC)",
    "CREATE INDEX ix_ac_b_strength ON actor_connections (actor_b, strength DESC)",
    """
    CREATE TABLE entity_relationships (
        id BIGSERIAL PRIMARY KEY, actor_a TEXT, actor_b TEXT, relationship TEXT,
        strength DOUBLE PRECISION, source_id TEXT, metadata JSONB, created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE wealth_flows (
        id SERIAL PRIMARY KEY, from_actor TEXT, to_entity TEXT, amount_estimate NUMERIC,
        confidence TEXT, evidence JSONB, flow_date DATE, implication TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE actor_id_map (
        alias_id TEXT PRIMARY KEY, canonical_id TEXT, match_method TEXT,
        confidence REAL, created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE signal_sources (
        id SERIAL PRIMARY KEY, source_type TEXT, source_id TEXT, ticker TEXT,
        signal_type TEXT, signal_date DATE, signal_value JSONB, trust_score NUMERIC
    )
    """,
]

_ACTORS = [
    # id, name, category, title, influence, icij_node_id, merged_into, source, provenance
    ("fed", "Federal Reserve", "central_bank", "", 0.95, None, None, None, "curated_seed"),
    ("ins_huang", "Jensen Huang", "insider", "CEO, NVIDIA", 0.85, None, None, None, "observed"),
    ("ins_alias", "J Huang", "insider", "", 0.5, None, "ins_huang", None, "observed"),
    ("corp_NVDA", "NVIDIA Corp", "institution", "Semiconductors — NVDA", 0.7, None, None, None, "observed"),
    ("ins_craig", "Craig Smith", "insider", "Director", 0.4, None, None, None, "observed"),
    ("pelosi", "Nancy Pelosi", "politician", "Speaker emerita", 0.9, None, None, None, "observed"),
    ("icij_entity_151138", "Shell Co Ltd", "icij_entity", "Offshore Entity (BVI)", 0.15, "151138", None, "icij", "unknown"),
    ("icij_officer_77", "Some Officer", "icij_officer", "", 0.25, "77", None, "icij", "unknown"),
    ("pep_1", "A Minister", "pep", "", 0.5, None, None, None, "unknown"),
    ("pep_2", "Another Minister", "pep", "", 0.5, None, None, None, "unknown"),
    ("donor_1", "A Donor", "donor", "", 0.5, None, None, None, "unknown"),
    # junk row from an operator quarantine: opt-in, never in the market default
    ("junk_1", "Junk Holder", "corporation_quarantined_20260928", "", 0.6, None, None, None, "unknown"),
]


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"actor_universe_{uuid4().hex[:10]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema},public"})
    try:
        with engine.begin() as conn:
            for stmt in _BASE_DDL:
                conn.execute(text(stmt))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _migrate(engine: Engine, fn: str = "upgrade") -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION)
    with engine.connect() as conn:
        trans = conn.begin()
        real_op = migration.op
        migration.op = Operations(MigrationContext.configure(conn))
        try:
            getattr(migration, fn)()
            trans.commit()
        except Exception:
            trans.rollback()
            raise
        finally:
            migration.op = real_op


def _seed(engine: Engine) -> None:
    with engine.begin() as conn:
        for a in _ACTORS:
            conn.execute(text(
                "INSERT INTO actors (id, name, category, title, influence_score, icij_node_id, "
                "merged_into, source, provenance) VALUES (:id, :n, :c, :t, :i, :icij, :m, :s, :p)"
            ), dict(zip(["id", "n", "c", "t", "i", "icij", "m", "s", "p"], a)))
        conn.execute(text(
            "INSERT INTO actor_connections (actor_a, actor_b, relationship, strength) VALUES "
            "('ins_huang', 'corp_NVDA', 'insider_trade', 0.9), "
            "('ins_alias', 'fed', 'signal_linked', 0.6), "
            "('pelosi', 'corp_NVDA', 'co_traded_congress', 0.8), "
            "('pelosi', 'fed', 'signal_linked', 0.5), "
            "('NVDA', 'corp_NVDA', 'signal_linked', 0.3), "
            "('pep_1', 'pep_2', 'connected', 0.5), "
            "('donor_1', 'pep_1', 'connected', 0.5), "
            "('icij_77', 'icij_entity_151138', 'icij_officer_of', 0.5), "
            "('junk_1', 'fed', 'connected', 0.9)"
        ))
        conn.execute(text(
            "INSERT INTO entity_relationships (actor_a, actor_b, relationship, strength, source_id) VALUES "
            "('77', '151138', 'officer_of', 0.5, 'Panama Papers'), "
            "('151138', 'addr_9', 'registered_address', 0.5, 'Panama Papers')"
        ))
        conn.execute(text(
            "INSERT INTO wealth_flows (from_actor, to_entity, amount_estimate, confidence, flow_date) VALUES "
            "('ins_craig', 'corp_NVDA', 1000000, 'confirmed', CURRENT_DATE), "
            "('ins_huang', 'corp_NVDA', 5000000, 'confirmed', CURRENT_DATE)"
        ))


@pytest.fixture()
def universe(scratch: Engine):
    from intelligence.actors.universe import run_actor_universe_job

    _migrate(scratch)
    _seed(scratch)
    stats = run_actor_universe_job(scratch, betweenness_samples=4)
    return scratch, stats


def test_migration_is_idempotent_and_reversible(scratch: Engine) -> None:
    _migrate(scratch)
    _migrate(scratch)  # second run is a no-op
    with scratch.connect() as conn:
        for t in ("actor_graph_runs", "actor_graph_metrics", "actor_graph_communities", "actor_graph_edges"):
            assert conn.execute(text("SELECT to_regclass(:t)"), {"t": t}).scalar(), t
        assert conn.execute(text("SELECT to_regclass('idx_wealth_flows_to_entity')")).scalar()
    _migrate(scratch, "downgrade")
    with scratch.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('actor_graph_metrics')")).scalar() is None
        assert conn.execute(text("SELECT to_regclass('idx_wealth_flows_to_entity')")).scalar() is None


def test_job_writes_one_complete_version(universe) -> None:
    engine, stats = universe
    assert stats["actors"] == len(_ACTORS) - 1  # merged alias folded into ins_huang
    assert stats["external_nodes"] == 2         # NVDA ticker string + ICIJ address
    with engine.connect() as conn:
        run = conn.execute(text(
            "SELECT status, node_count, actor_count, computed_at, peak_rss_mb FROM actor_graph_runs"
        )).fetchall()
        assert len(run) == 1 and run[0][0] == "complete" and run[0][3] is not None
        n = conn.execute(text("SELECT count(*) FROM actor_graph_metrics")).scalar()
        assert n == stats["actors"]
        craig = conn.execute(text(
            "SELECT sector, sector_basis FROM actor_graph_metrics WHERE actor_id = 'ins_craig'"
        )).fetchone()
        assert tuple(craig) == ("Semiconductors", "neighbor")
        assert conn.execute(text(
            "SELECT category_group FROM actor_graph_metrics WHERE actor_id = 'icij_officer_77'"
        )).scalar() == "icij"


def test_second_run_supersedes_and_keeps_two_versions(universe) -> None:
    import time

    from intelligence.actors import universe as uni

    engine, _ = universe
    for _ in range(2):
        time.sleep(1.1)  # versions are second-resolution timestamps
        uni.run_actor_universe_job(engine, betweenness_samples=0)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM actor_graph_runs WHERE status='complete'")).scalar() == 3
        kept = conn.execute(text("SELECT count(DISTINCT graph_version) FROM actor_graph_metrics")).scalar()
        assert kept == uni.KEEP_VERSIONS


def test_dry_run_writes_nothing(scratch: Engine) -> None:
    from intelligence.actors.universe import run_actor_universe_job

    _migrate(scratch)
    _seed(scratch)
    stats = run_actor_universe_job(scratch, dry_run=True, betweenness_samples=0)
    assert stats["dry_run"] is True and stats["actors"] == len(_ACTORS) - 1
    with scratch.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM actor_graph_runs")).scalar() == 0
        assert conn.execute(text("SELECT count(*) FROM actor_graph_metrics")).scalar() == 0


def test_overview_defaults_to_market_and_bulk_groups_are_opt_in(universe) -> None:
    from intelligence.actors.universe_store import overview

    engine, _ = universe
    res = overview(engine, n=50)
    ids = {n["id"] for n in res["nodes"]}
    assert ids == {"fed", "ins_huang", "corp_NVDA", "ins_craig", "pelosi"}
    assert res["metadata"]["computed_at"] and res["metadata"]["graph_version"]
    assert any({l["source"], l["target"]} == {"ins_huang", "corp_NVDA"} for l in res["links"])
    assert all(l["source"] in ids and l["target"] in ids for l in res["links"])
    huang = next(n for n in res["nodes"] if n["id"] == "ins_huang")
    assert huang["label"] == "Jensen Huang" and huang["source"]  # provenance stamped

    with_pep = overview(engine, n=50, groups="market,pep")
    assert {"pep_1", "pep_2"} <= {n["id"] for n in with_pep["nodes"]}
    everything = overview(engine, n=50, groups="all")
    assert "icij_entity_151138" in {n["id"] for n in everything["nodes"]}
    assert "junk_1" in {n["id"] for n in everything["nodes"]}
    quarantined = overview(engine, n=50, groups="market,quarantined")
    assert "junk_1" in {n["id"] for n in quarantined["nodes"]}

    top2 = overview(engine, n=2)
    assert len(top2["nodes"]) == 2
    semis = overview(engine, n=50, sector="Semiconductors")
    assert {n["id"] for n in semis["nodes"]} == {"ins_huang", "corp_NVDA", "ins_craig"}
    cats = overview(engine, n=50, categories="insider")
    assert {n["id"] for n in cats["nodes"]} == {"ins_huang", "ins_craig"}


def test_overview_without_a_run_is_pending(scratch: Engine) -> None:
    from intelligence.actors.universe_store import overview

    _migrate(scratch)
    res = overview(scratch, n=10)
    assert res["status"] == "pending_first_run" and res["nodes"] == []


def test_ego_network_resolves_icij_ids_and_bounds_fanout(universe) -> None:
    from intelligence.actors.universe_store import ego_network

    engine, _ = universe
    res = ego_network(engine, "corp_NVDA", depth=1, fanout=10)
    by_id = {n["id"]: n for n in res["nodes"]}
    assert by_id["corp_NVDA"]["ring"] == 0
    assert {"ins_huang", "pelosi", "ins_craig"} <= set(by_id)
    assert by_id["NVDA"]["external"] is True
    rels = {(e["source"], e["target"], e["relationship"]) for e in res["edges"]}
    assert ("corp_NVDA", "ins_craig", "wealth_flow") in rels
    assert res["computed_at"]

    narrow = ego_network(engine, "corp_NVDA", depth=1, fanout=1)
    assert len([n for n in narrow["nodes"] if n["ring"] == 1]) == 1

    # raw ICIJ node ids ("77", "icij_77") resolve to the actor rows
    off = ego_network(engine, "icij_entity_151138", depth=1)
    ids = {n["id"] for n in off["nodes"]}
    assert "icij_officer_77" in ids and "addr_9" in ids
    no_off = ego_network(engine, "icij_entity_151138", depth=1, include_offshore=False)
    assert "icij_officer_77" not in {n["id"] for n in no_off["nodes"]}

    two = ego_network(engine, "ins_huang", depth=2, max_nodes=3)
    assert len(two["nodes"]) <= 3 and two["truncated"] is True

    missing = ego_network(engine, "nope", depth=1)
    assert missing["nodes"] == [] and "error" in missing


def test_search_is_paginated_and_ranked(universe) -> None:
    from intelligence.actors.universe_store import search_actors

    engine, _ = universe
    res = search_actors(engine, "minister", limit=1)
    assert res["total"] == 2 and res["has_more"] is True and len(res["entries"]) == 1
    page2 = search_actors(engine, "minister", limit=1, offset=1)
    assert page2["has_more"] is False
    assert {res["entries"][0]["id"], page2["entries"][0]["id"]} == {"pep_1", "pep_2"}
    assert search_actors(engine, "minister", groups="market")["total"] == 0
    # LIKE wildcards in the query are literal
    assert search_actors(engine, "%%%")["total"] == 0
    exact = search_actors(engine, "Nancy Pelosi")
    assert exact["entries"][0]["id"] == "pelosi" and exact["entries"][0]["importance"] is not None


def test_top_actors_default_to_market_and_bulk_groups_are_opt_in(universe) -> None:
    from intelligence.actors.universe_store import top_actors

    engine, _ = universe
    res = top_actors(engine, metric="pagerank", limit=50)
    assert res["groups"] == ["market"] and res["graph_version"]
    assert {e["category_group"] for e in res["entries"]} == {"market"}
    ids = [e["actor_id"] for e in res["entries"]]
    assert "junk_1" not in ids and "icij_entity_151138" not in ids
    scores = [e["score"] for e in res["entries"]]
    assert scores == sorted(scores, reverse=True)
    assert next(e for e in res["entries"] if e["actor_id"] == "pelosi")["name"] == "Nancy Pelosi"

    everything = top_actors(engine, metric="degree_centrality", limit=50, groups="all")
    assert {"junk_1", "icij_entity_151138", "pep_1"} <= {e["actor_id"] for e in everything["entries"]}
    assert len(top_actors(engine, limit=2)["entries"]) == 2
    with pytest.raises(ValueError):
        top_actors(engine, metric="pagerank; DROP TABLE actors")


def test_top_actors_without_a_run_is_pending(scratch: Engine) -> None:
    from intelligence.actors.universe_store import top_actors

    _migrate(scratch)
    res = top_actors(scratch)
    assert res["status"] == "pending_first_run" and res["entries"] == []


def test_community_summaries_and_members(universe) -> None:
    from intelligence.actors.universe_store import community_members, community_summaries

    engine, _ = universe
    res = community_summaries(engine, limit=10)
    assert res["total"] >= 1 and res["entries"]
    first = res["entries"][0]
    assert first["top_actors"] and first["top_actors"][0]["name"]
    members = community_members(engine, first["community_id"], limit=50)
    assert members["total"] >= 1
    assert {m["id"] for m in members["entries"]} >= {a["id"] for a in first["top_actors"]}


def test_actor_network_endpoint_reads_the_precomputed_universe(universe, monkeypatch) -> None:
    from api.routers import intelligence_actors as router

    engine, _ = universe
    monkeypatch.setattr(router, "get_db_engine", lambda: engine)
    monkeypatch.setattr(router, "get_flow_extras", lambda wait=False: {"flows": [{"from": "x"}]})
    built: list[bool] = []
    monkeypatch.setattr(router, "start_actor_graph_rebuild", lambda: built.append(True) or True)
    router._universe_run_cache.clear()
    try:
        res = router.get_actor_network(limit=500, sector=None, groups=None, community=None,
                                       categories=None, _token="t")
        assert res["metadata"]["source"] == "actor_universe"
        assert res["metadata"]["computed_at"]
        assert "pelosi" in {n["id"] for n in res["nodes"]}
        assert res["flows"] == [{"from": "x"}]
        assert built == [], "no legacy graph build when a universe run exists"
        comm = router.get_communities_endpoint(scope="full", limit=10, _token="t")
        assert comm["source"] == "actor_universe" and comm["communities"]
        top = router.get_top_actors_endpoint(metric="pagerank", limit=20, scope="full",
                                             groups=None, _token="t")
        assert top["source"] == "actor_universe" and top["groups"] == ["market"]
        assert top["actors"] and "junk_1" not in {a["actor_id"] for a in top["actors"]}
        ego = router.get_actor_universe_ego(actor_id="corp_NVDA", depth=1, fanout=25, max_nodes=50,
                                            include_offshore=True, include_external=True, _token="t")
        assert ego["center"] == "corp_NVDA" and len(ego["nodes"]) > 1
        legacy_ego = router.get_ego_graph(actor_id="corp_NVDA", depth=1, max_nodes=80, _token="t")
        assert "NVDA" not in {n["id"] for n in legacy_ego["nodes"]}  # no external leaves there
        assert all("color" in e for e in legacy_ego["edges"])
    finally:
        router._universe_run_cache.clear()


def test_ticker_context_uses_indexed_lookups(universe, monkeypatch) -> None:
    from intelligence.actors import analysis

    engine, _ = universe
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO signal_sources (source_type, source_id, ticker, signal_type, signal_date, "
            "signal_value, trust_score) VALUES "
            "('congressional', 'Hon. Nancy Pelosi (D-CA)', 'NVDA', 'BUY', CURRENT_DATE, :v, 0.7), "
            "('insider', 'Craig', 'NVDA', 'SELL', CURRENT_DATE, :v, 0.5)"
        ), {"v": json.dumps({})})

    def _boom(*a, **k):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("must not load the actor table")

    monkeypatch.setattr(analysis, "_load_actors_from_db", _boom)
    ctx = analysis.get_actor_context_for_ticker(engine, "NVDA")
    ids = {a["id"] for a in ctx["actors"]}
    assert "pelosi" in ids                    # "Nancy Pelosi" inside the source name
    assert {"ins_huang", "ins_craig"} <= ids  # wealth_flows into corp_NVDA
    assert "fed" not in ids
    assert len(ctx["recent_actions"]) == 2
