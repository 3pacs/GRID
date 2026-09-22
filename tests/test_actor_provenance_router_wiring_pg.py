"""Real-PostgreSQL proof that the 8 deferred actor-provenance reader call sites
across api/routers/intel.py and api/routers/intelligence_actors.py (plus the
shared intelligence/actors/graph.py::build_actor_graph / db.py::_load_actors_from_db
pair) now read the REAL stored ``actors.provenance``/``provenance_as_of`` columns
and expose the correct wire "source"/"source_as_of" through the actual API
response, instead of guessing from the static seed list -- part of the #596
remediation reconciliation
(00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md).

Scope, and why it's 8 sites, not the 15 raw SQL statements that touch
``actors``: this covers every call site that builds an actor GRAPH NODE (the
shape ``stamp_actor_node`` is designed for) across both routers, plus the one
shared node-building helper. It deliberately excludes intel.py's four FLAT
dossier/search-result sites (``intel_search``, ``intel_entity_profile``,
``intel_actor_dossier``, ``intel_ticker``) and its own ``"confidence"``
literal wiring -- those are a separate, already-tracked axis (see
``tests/test_confidence_policy.py::TestSourceClassLabels``, xfail-gated on a
prior, still-unmerged extraction) and touching them here would be exactly the
kind of broader audit this reconciliation has repeatedly stayed out of.

The 8:
  1. ``intelligence/actors/graph.py::build_actor_graph`` (shared helper)
  2. ``get_actor_network_db``      (``GET /actor-network/db``)
  3. ``get_actor_enriched_profile`` (``GET /actor/{id}/profile``)
  4. ``get_sector_power_map``      (``GET /power-map/{sector_name}``, both its
     normal path and its zero-DB-match fallback -- see below)
  5. ``ego_graph_search``          (``GET /ego-graph/search``)
  6. ``get_ego_graph``             (``GET /ego-graph/{actor_id}``)
  7. ``get_grand_power_map``       (``GET /grand-power-map``)
  8. intel.py's network-graph actor-traversal branch (``GET /api/v1/intel/network/{entity}``)

Transitively (not separately enumerated, since it builds no node dicts of its
own): ``GET /actor-network`` (the async, cache-fronted handler
``get_actor_network``) calls ``intelligence.actor_network.build_actor_graph``,
and that module is a thin re-export facade for the exact same function wired
as site 1 -- see ``test_get_actor_network_cached_endpoint_stamps_source_and_survives_cache_hit``
below, which proves stamped provenance survives this endpoint's 30-minute
``TTLCache`` (``_actor_graph_cache``). ``PowerMap.jsx`` and
``ActorNetwork.jsx`` are real PWA consumers of this and several of the 8
sites above (``power-map``, ``ego-graph``, ``grand-power-map``) -- checked
directly for any read of a node-level ``source``/``synthetic`` field; none
exists, every ``.source`` reference in those files is the unrelated D3
force-graph *edge* convention (``link.source``), so the new field is purely
additive there.

A pre-existing, unrelated collision was found and resolved while wiring sites
2 and 3: ``actors.source`` is a DIFFERENT, legacy ingestion-origin column
(``intelligence/actor_discovery.py``, e.g. ``"spider"``), and those two sites
already exposed it under the JSON key ``"source"``. That raw value now ships
under ``"ingestion_source"`` instead, freeing ``"source"``/``"source_as_of"``
to consistently mean the provenance module's wire label at all 8 sites.
Checked directly: no PWA code calls either of these two endpoints at all
(``get_actor_network_db``/``get_actor_enriched_profile`` have no
``pwa/src/api.js`` wrapper), so nothing could have been reading the old
ingestion-origin value under the ``"source"`` key in the first place.

Also found and fixed, as a precondition for site 8 to be reachable at all:
intel.py's network-graph actor-traversal query selected non-existent columns
(``actor_id``, ``sector`` instead of the real ``id``, ``category``). Every
version of the ``actors`` table's DDL, from its first commit onward
(``git log -S"CREATE TABLE IF NOT EXISTS actors"``), has used ``id``/
``category`` -- this query could never have executed successfully against
this table's schema at any point in its history. What that history does NOT
establish: whether this code path was ever actually invoked by production
traffic, or what (if anything) reached error monitoring -- the exception is
caught and logged at ``DEBUG`` level by that block's own broad
``except Exception: log.debug(...)``, which is silent by design regardless
of whether it fired. Production error logs were not checked (out of scope
for this task). Fixed to the real column names -- without this the query
never runs, so provenance could never be wired at all.

Fixing this query is a behavior change beyond provenance labeling: for any
entity search that matches a real ``actors`` row, this endpoint now (a)
returns an additional graph node for that actor (previously never added --
the searched entity's own pre-seeded placeholder node was, and remains, the
only thing this branch could ever contribute), (b) emits up to 20
``"connected"`` edges per matched actor per hop, parsed from that actor's
``connections`` JSONB column (previously never parsed, since the query
never returned rows to parse), and (c) for the first time actually expands
`next_frontier` through real actor-relationship data, meaning the ``depth``
parameter (1-5) is now effective for multi-hop traversal through actors --
previously only ICIJ relationships (a disjoint data source/table) could ever
expand the frontier past hop 0. No in-repo caller of this endpoint was
found (no PWA usage, no other backend module) -- it is documented in
``.coordination.md`` as a standalone, ``PRO``-tier-gated public API surface,
so its real audience is external API clients outside this repository, which
cannot be verified from source alone.

Uses the shared ``pg_engine`` fixture (tests/conftest.py) -- skips cleanly if
no PostgreSQL is reachable. Router functions are called directly (matching
the existing convention in tests/test_intel_search_pagination.py and
tests/test_intelligence_actors_cache.py), with ``get_db_engine`` monkeypatched
to the disposable engine -- this exercises the exact same code that builds
the real HTTP response body, without the ASGI/auth-dependency machinery.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.actors.provenance import (
    PROVENANCE_OBSERVED,
    PROVENANCE_SEED,
    PROVENANCE_UNCONFIRMED,
    SEED_VINTAGE,
    SOURCE_CURATED_SEED,
    SOURCE_OBSERVED,
    SOURCE_UNCONFIRMED,
    SOURCE_UNKNOWN,
)

_ACTORS_DDL = """
CREATE TABLE IF NOT EXISTS actors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tier TEXT NOT NULL,
    category TEXT NOT NULL,
    title TEXT,
    net_worth_estimate DOUBLE PRECISION,
    aum DOUBLE PRECISION,
    influence_score DOUBLE PRECISION,
    trust_score DOUBLE PRECISION,
    motivation_model TEXT,
    connections JSONB,
    known_positions JSONB,
    board_seats JSONB,
    political_affiliations JSONB,
    data_sources JSONB,
    credibility TEXT,
    degree INT DEFAULT 0,
    source TEXT DEFAULT 'unknown',
    metadata JSONB DEFAULT '{}',
    provenance TEXT NOT NULL DEFAULT 'unknown',
    provenance_as_of DATE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_ACTOR_CONNECTIONS_DDL = """
CREATE TABLE IF NOT EXISTS actor_connections (
    actor_a TEXT NOT NULL,
    actor_b TEXT NOT NULL,
    relationship TEXT NOT NULL,
    strength DOUBLE PRECISION,
    evidence JSONB,
    PRIMARY KEY (actor_a, actor_b, relationship)
)
"""

_WEALTH_FLOWS_DDL = """
CREATE TABLE IF NOT EXISTS wealth_flows (
    id SERIAL PRIMARY KEY,
    from_actor TEXT,
    to_entity TEXT,
    amount_estimate DOUBLE PRECISION,
    confidence TEXT,
    flow_date DATE,
    implication TEXT
)
"""

# intel.py's network-graph traversal queries this table (ICIJ relationships)
# unconditionally, in its own try/except, BEFORE the actor-lookup query runs
# on the same connection -- a real, pre-existing fragility: if that query
# throws (e.g. because the table doesn't exist), the connection's transaction
# is left aborted, and the actor-lookup query on the SAME connection then
# ALSO fails (also silently swallowed by its own broad except). Empty is
# fine; it only needs to exist so that query succeeds (trivially, with zero
# rows) rather than poisoning the transaction for the query this file
# actually tests.
_ICIJ_RELATIONSHIPS_DDL = """
CREATE TABLE IF NOT EXISTS icij_relationships (
    entity_name TEXT,
    linked_to TEXT,
    relationship_type TEXT,
    jurisdiction TEXT,
    source_dataset TEXT
)
"""


@pytest.fixture(autouse=True)
def _schema(pg_engine: Engine):
    with pg_engine.begin() as conn:
        conn.execute(text(_ACTORS_DDL))
        conn.execute(text(_ACTOR_CONNECTIONS_DDL))
        conn.execute(text(_WEALTH_FLOWS_DDL))
        conn.execute(text(_ICIJ_RELATIONSHIPS_DDL))
        for stmt in (
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance TEXT NOT NULL DEFAULT 'unknown'",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance_as_of DATE",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS degree INT DEFAULT 0",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'unknown'",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'",
        ):
            conn.execute(text(stmt))
    yield


@pytest.fixture
def test_ids() -> list[str]:
    return []


@pytest.fixture(autouse=True)
def _cleanup(pg_engine: Engine, test_ids: list[str]):
    yield
    if not test_ids:
        return
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM actor_connections WHERE actor_a = ANY(:ids) OR actor_b = ANY(:ids)").bindparams(ids=test_ids))
        conn.execute(text("DELETE FROM actors WHERE id = ANY(:ids)").bindparams(ids=test_ids))


def _insert_actor(
    conn, *, actor_id: str, name: str, provenance: str, provenance_as_of=None,
    tier: str = "institutional", category: str = "corporation", degree: int = 5,
    influence_score: float = 0.6, ingestion_source: str = "spider",
) -> None:
    conn.execute(text(
        "INSERT INTO actors (id, name, tier, category, degree, influence_score, "
        "trust_score, title, source, provenance, provenance_as_of) "
        "VALUES (:id, :name, :tier, :category, :degree, :inf, 0.5, 'Test Title', "
        ":isrc, :prov, :vintage)"
    ).bindparams(
        id=actor_id, name=name, tier=tier, category=category, degree=degree,
        inf=influence_score, isrc=ingestion_source, prov=provenance, vintage=provenance_as_of,
    ))


# ── Site 1: shared helper (build_actor_graph / _load_actors_from_db) ───────

def test_shared_graph_helper_stamps_source_for_all_states(pg_engine: Engine, test_ids):
    """Feeds GET /actor-network. Seeds one actor per state; asserts each node
    in the built graph carries the correct source/source_as_of."""
    from intelligence.actors.graph import build_actor_graph

    ids = {
        "seed": f"prov_router_pg_{uuid.uuid4().hex[:10]}_seed",
        "observed": f"prov_router_pg_{uuid.uuid4().hex[:10]}_obs",
        "unconfirmed": f"prov_router_pg_{uuid.uuid4().hex[:10]}_unc",
        "unknown": f"prov_router_pg_{uuid.uuid4().hex[:10]}_unk",
    }
    test_ids.extend(ids.values())

    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=ids["seed"], name="Seed Actor", provenance=PROVENANCE_SEED, provenance_as_of=SEED_VINTAGE)
        _insert_actor(conn, actor_id=ids["observed"], name="Observed Actor", provenance=PROVENANCE_OBSERVED)
        _insert_actor(conn, actor_id=ids["unconfirmed"], name="Unconfirmed Actor", provenance=PROVENANCE_UNCONFIRMED)
        _insert_actor(conn, actor_id=ids["unknown"], name="Unknown Actor", provenance="unknown")

    graph = build_actor_graph(pg_engine)
    nodes_by_id = {n["id"]: n for n in graph["nodes"]}

    assert nodes_by_id[ids["seed"]]["source"] == SOURCE_CURATED_SEED
    assert nodes_by_id[ids["seed"]]["source_as_of"] == SEED_VINTAGE
    assert nodes_by_id[ids["observed"]]["source"] == SOURCE_OBSERVED
    assert nodes_by_id[ids["observed"]]["source_as_of"] is None
    assert nodes_by_id[ids["unconfirmed"]]["source"] == SOURCE_UNCONFIRMED
    assert nodes_by_id[ids["unknown"]]["source"] == SOURCE_UNKNOWN, "unknown must never be guessed as observed"


# ── Site 2: get_actor_network_db ────────────────────────────────────────────

def test_get_actor_network_db_stamps_source_and_keeps_legacy_ingestion_source(pg_engine: Engine, test_ids, monkeypatch):
    from api.routers import intelligence_actors as router

    monkeypatch.setattr(router, "get_db_engine", lambda: pg_engine)

    aid = f"prov_router_pg_{uuid.uuid4().hex[:10]}_net"
    test_ids.append(aid)
    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=aid, name="Network DB Actor", provenance=PROVENANCE_OBSERVED, ingestion_source="wikidata")

    result = router.get_actor_network_db(limit=50, min_degree=0, include_icij=True, _token="t")
    node = next(n for n in result["nodes"] if n["id"] == aid)

    assert node["source"] == SOURCE_OBSERVED
    assert node["ingestion_source"] == "wikidata", "the legacy ingestion-origin column must survive under its own key"


# ── Site 3: get_actor_enriched_profile ──────────────────────────────────────

def test_get_actor_enriched_profile_stamps_source_and_keeps_legacy_ingestion_source(pg_engine: Engine, test_ids, monkeypatch):
    from api.routers import intelligence_actors as router

    monkeypatch.setattr(router, "get_db_engine", lambda: pg_engine)

    aid = f"prov_router_pg_{uuid.uuid4().hex[:10]}_prof"
    test_ids.append(aid)
    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=aid, name="Profile Actor", provenance=PROVENANCE_UNCONFIRMED, ingestion_source="operator")

    result = router.get_actor_enriched_profile(actor_id=aid, _token="t")

    assert result["actor"]["source"] == SOURCE_UNCONFIRMED
    assert result["actor"]["ingestion_source"] == "operator"


def test_get_actor_enriched_profile_missing_actor_returns_error_not_a_crash(pg_engine: Engine, monkeypatch):
    from api.routers import intelligence_actors as router

    monkeypatch.setattr(router, "get_db_engine", lambda: pg_engine)

    result = router.get_actor_enriched_profile(actor_id="definitely_does_not_exist_xyz", _token="t")
    assert "error" in result


# ── Site 4: get_sector_power_map ────────────────────────────────────────────

def test_get_sector_power_map_stamps_source_on_db_matched_actor(pg_engine: Engine, test_ids, monkeypatch):
    from api.routers import intelligence_actors as router

    monkeypatch.setattr(router, "get_db_engine", lambda: pg_engine)

    aid = f"prov_router_pg_{uuid.uuid4().hex[:10]}_sector"
    test_ids.append(aid)
    with pg_engine.begin() as conn:
        # Name must match the sector_map's own registered name (case-insensitive)
        # so the endpoint's pre-existing ticker-inheritance step (required to
        # survive its final node filter) resolves a ticker for this row.
        _insert_actor(conn, actor_id=aid, name="NVIDIA", provenance=PROVENANCE_SEED, provenance_as_of=SEED_VINTAGE)

    result = router.get_sector_power_map(sector_name="Technology", _token="t")
    matches = [n for n in result["nodes"] if n["id"] == aid]
    assert matches, "the ticker-match branch must find the seeded actor"
    node = matches[0]
    assert node["source"] == SOURCE_CURATED_SEED
    assert node["source_as_of"] == SEED_VINTAGE
    assert node.get("synthetic") is not True


def test_get_sector_power_map_synthetic_node_carries_sector_map_source(pg_engine: Engine, monkeypatch):
    """A sector_map-only entity with no DB row at all must be tagged
    SOURCE_SECTOR_MAP, not left unstamped or defaulted to observed."""
    from api.routers import intelligence_actors as router
    from intelligence.actors.provenance import SOURCE_SECTOR_MAP

    monkeypatch.setattr(router, "get_db_engine", lambda: pg_engine)

    result = router.get_sector_power_map(sector_name="Technology", _token="t")
    synthetic_nodes = [n for n in result["nodes"] if n.get("synthetic")]
    assert synthetic_nodes, "fixture check: at least one sector_map-only actor with no DB match is expected for AI"
    for n in synthetic_nodes:
        assert n["source"] == SOURCE_SECTOR_MAP


# ── Site 5: ego_graph_search ────────────────────────────────────────────────

def test_ego_graph_search_stamps_source(pg_engine: Engine, test_ids, monkeypatch):
    from api.routers import intelligence_actors as router

    monkeypatch.setattr(router, "get_db_engine", lambda: pg_engine)

    aid = f"prov_router_pg_{uuid.uuid4().hex[:10]}_egosearch"
    test_ids.append(aid)
    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=aid, name="EgoSearchTarget Actor", provenance=PROVENANCE_OBSERVED)

    result = router.ego_graph_search(q="EgoSearchTarget", limit=20, _token="t")
    match = next(r for r in result["results"] if r["id"] == aid)
    assert match["source"] == SOURCE_OBSERVED


# ── Site 6: get_ego_graph ────────────────────────────────────────────────────

def test_get_ego_graph_stamps_source_on_center_and_expansion(pg_engine: Engine, test_ids, monkeypatch):
    from api.routers import intelligence_actors as router

    monkeypatch.setattr(router, "get_db_engine", lambda: pg_engine)

    center_id = f"prov_router_pg_{uuid.uuid4().hex[:10]}_center"
    ring1_id = f"prov_router_pg_{uuid.uuid4().hex[:10]}_ring1"
    test_ids.extend([center_id, ring1_id])
    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=center_id, name="Center Actor", provenance=PROVENANCE_SEED, provenance_as_of=SEED_VINTAGE)
        _insert_actor(conn, actor_id=ring1_id, name="Ring1 Actor", provenance=PROVENANCE_UNCONFIRMED)
        conn.execute(text(
            "INSERT INTO actor_connections (actor_a, actor_b, relationship, strength) "
            "VALUES (:a, :b, 'business_partner', 0.9)"
        ).bindparams(a=center_id, b=ring1_id))

    result = router.get_ego_graph(actor_id=center_id, depth=1, max_nodes=80, _token="t")
    nodes_by_id = {n["id"]: n for n in result["nodes"]}

    assert nodes_by_id[center_id]["source"] == SOURCE_CURATED_SEED
    assert nodes_by_id[center_id]["source_as_of"] == SEED_VINTAGE
    assert nodes_by_id[ring1_id]["source"] == SOURCE_UNCONFIRMED


def test_get_ego_graph_missing_actor_returns_empty_not_a_crash(pg_engine: Engine, monkeypatch):
    from api.routers import intelligence_actors as router

    monkeypatch.setattr(router, "get_db_engine", lambda: pg_engine)

    result = router.get_ego_graph(actor_id="definitely_does_not_exist_xyz", depth=1, max_nodes=80, _token="t")
    assert result["nodes"] == []
    assert "error" in result


# ── Site 7: get_grand_power_map ─────────────────────────────────────────────

def test_get_grand_power_map_stamps_source_on_top_actor_and_bridge_actor(pg_engine: Engine, test_ids, monkeypatch):
    from api.routers import intelligence_actors as router

    monkeypatch.setattr(router, "get_db_engine", lambda: pg_engine)

    top_a = f"prov_router_pg_{uuid.uuid4().hex[:10]}_topa"
    top_b = f"prov_router_pg_{uuid.uuid4().hex[:10]}_topb"
    bridge = f"prov_router_pg_{uuid.uuid4().hex[:10]}_bridge"
    test_ids.extend([top_a, top_b, bridge])
    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=top_a, name="Top A", category="corporation", provenance=PROVENANCE_OBSERVED)
        _insert_actor(conn, actor_id=top_b, name="Top B", category="politician", provenance="unknown")
        _insert_actor(conn, actor_id=bridge, name="Bridge Actor", category="fund", provenance=PROVENANCE_UNCONFIRMED)
        # 3+ connections each, required by the conn_counts HAVING clause.
        for i in range(3):
            other = f"{top_a}_filler{i}"
            test_ids.append(other)
            _insert_actor(conn, actor_id=other, name=f"Filler {i}", provenance="unknown")
            conn.execute(text(
                "INSERT INTO actor_connections (actor_a, actor_b, relationship, strength) "
                "VALUES (:a, :b, 'co_investor', 0.8)"
            ).bindparams(a=top_a, b=other))
            other2 = f"{top_b}_filler{i}"
            test_ids.append(other2)
            _insert_actor(conn, actor_id=other2, name=f"Filler2 {i}", provenance="unknown")
            conn.execute(text(
                "INSERT INTO actor_connections (actor_a, actor_b, relationship, strength) "
                "VALUES (:a, :b, 'co_investor', 0.8)"
            ).bindparams(a=top_b, b=other2))
        # A path top_a -> bridge -> top_b so the bridge-expansion query finds `bridge`.
        conn.execute(text(
            "INSERT INTO actor_connections (actor_a, actor_b, relationship, strength) "
            "VALUES (:a, :b, 'co_investor', 0.9)"
        ).bindparams(a=top_a, b=bridge))
        conn.execute(text(
            "INSERT INTO actor_connections (actor_a, actor_b, relationship, strength) "
            "VALUES (:a, :b, 'co_investor', 0.9)"
        ).bindparams(a=top_b, b=bridge))

    result = router.get_grand_power_map(limit=50, _token="t")
    nodes_by_id = {n["id"]: n for n in result["nodes"]}

    assert nodes_by_id[top_a]["source"] == SOURCE_OBSERVED
    assert nodes_by_id[top_b]["source"] == SOURCE_UNKNOWN
    if bridge in nodes_by_id:
        assert nodes_by_id[bridge]["source"] == SOURCE_UNCONFIRMED


# ── Site 8: intel.py network-graph traversal ────────────────────────────────

def test_intel_network_graph_stamps_source_and_the_column_name_fix_works(pg_engine: Engine, test_ids, monkeypatch):
    """Also proves the actor_id/sector -> id/category column-name fix: before
    it, this query always failed silently and no actor node was ever added.

    intel_network() always pre-seeds a generic type="unknown" placeholder for
    the searched frontier key itself (the entity/id you search BY), before the
    actor-specific lookup runs -- and that lookup only inserts a NEW dict
    under `row.name.upper()`, skipping the update if that key is already
    present. To actually reach the actor-specific (type="actor", now
    provenance-stamped) branch, the row's `name` column must differ from the
    searched key -- searching by id with a distinct name does that.
    """
    import api.routers.intel as intel_router

    monkeypatch.setattr(intel_router, "get_db_engine", lambda: pg_engine)

    aid = f"prov_router_pg_{uuid.uuid4().hex[:10]}_intel"
    actor_name = f"Distinct Intel Actor {uuid.uuid4().hex[:6]}"
    test_ids.append(aid)
    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=aid, name=actor_name, provenance=PROVENANCE_OBSERVED)

    result = intel_router.intel_network(entity=aid, depth=1, _token="t")

    nodes_by_id = {n["id"]: n for n in result["data"]["nodes"]}
    node = nodes_by_id[actor_name.upper()]
    assert node["type"] == "actor"
    assert node["source"] == SOURCE_OBSERVED, "the query must have actually run (not silently failed) and stamped provenance"


# ── Cache-hit compatibility: GET /actor-network (30-minute TTLCache) ───────
#
# Not one of the 8 sites above -- it was never separately enumerated because
# it doesn't build its own node dicts. But `get_actor_network` (the async
# handler behind `GET /actor-network`, a real, PWA-consumed endpoint per
# pwa/src/components/PowerMap.jsx and pwa/src/views/ActorNetwork.jsx) calls
# `intelligence.actor_network.build_actor_graph`, and that module is a thin
# re-export facade (see its own module docstring) for the EXACT same
# `intelligence.actors.graph.build_actor_graph` wired as site 1 -- so this
# endpoint is transitively, not directly, affected. Its result is cached for
# 30 minutes in an in-process `TTLCache` (`_actor_graph_cache`, already
# covered mechanically by tests/test_intelligence_actors_cache.py). No
# existing test exercises the endpoint function itself or proves stamped
# provenance actually survives a cache hit -- that's the concrete gap this
# closes.

def test_get_actor_network_cached_endpoint_stamps_source_and_survives_cache_hit(pg_engine: Engine, test_ids, monkeypatch):
    import asyncio

    from api.routers import intelligence_actors as router

    monkeypatch.setattr(router, "get_db_engine", lambda: pg_engine)
    router._actor_graph_cache.clear()

    aid = f"prov_router_pg_{uuid.uuid4().hex[:10]}_cachedgraph"
    test_ids.append(aid)
    with pg_engine.begin() as conn:
        # influence_score >= 0.7 so the node survives the endpoint's own
        # pre-existing default-view filter without needing a sector match.
        _insert_actor(conn, actor_id=aid, name="Cached Graph Actor", provenance=PROVENANCE_OBSERVED, influence_score=0.9)

    try:
        result1 = asyncio.run(router.get_actor_network(limit=500, sector=None, _token="t"))
        node1 = next(n for n in result1["nodes"] if n["id"] == aid)
        assert node1["source"] == SOURCE_OBSERVED

        # Second call within the 30-minute TTL window is a cache hit, not a
        # rebuild -- the cached payload must still carry the same stamped
        # provenance, not something stale or missing the field entirely.
        result2 = asyncio.run(router.get_actor_network(limit=500, sector=None, _token="t"))
        node2 = next(n for n in result2["nodes"] if n["id"] == aid)
        assert node2["source"] == SOURCE_OBSERVED
    finally:
        router._actor_graph_cache.clear()
