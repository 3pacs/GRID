"""Real-PostgreSQL proof that intel_network's ICIJ traversal query matches
the ACTUAL production icij_relationships/icij_entities/icij_officers/
icij_intermediaries schema, and that a failure in the ICIJ block no longer
poisons the actor query (or any later hop) on the same connection.

Found during #602's post-deploy acceptance checks (2026-09-22): the
previously-deployed ICIJ query selected entity_name/linked_to/
relationship_type/jurisdiction directly from icij_relationships. Checked
directly against production: none of those columns exist there.
icij_relationships' real schema is (id, from_node, to_node, rel_type,
source_dataset, start_date, end_date, created_at) -- from_node/to_node are
integer node_id references, not name strings, and can point into any of
THREE separate name-bearing tables (icij_entities, icij_officers,
icij_intermediaries), confirmed directly from
ingestion/altdata/icij_puller.py's own INSERT statements (the only place
in this codebase that writes these tables). Only icij_entities carries a
jurisdiction column.

Because the old query always threw (UndefinedColumn), and PostgreSQL aborts
the whole connection's transaction on a failed statement, this ALSO
silently broke every later statement on the same connection -- including
the (separately fixed, in #602) actor-traversal query right after it, on
every single hop, for the lifetime of this endpoint. This file proves four
independent things: (1) the corrected ICIJ query works against the real
schema, matching via the from_node/n1 side; (2) it also correctly matches
via the to_node/n2 side, across all three name-bearing tables chained
together (entity -> officer -> intermediary), not just entity+officer;
(3) the query's own LIMIT 200 actually bounds row processing for a single
well-connected ("hub") name, which can legitimately have far more
relationships than the actor side's connections[:20] slice would ever
see; and (4) a genuine ICIJ-side failure no longer poisons anything that
runs after it on the same connection.

Development-only. No production changes, no deployment. DDL below mirrors
the real production schema exactly -- confirmed via
`information_schema.columns` for icij_relationships, and via
ingestion/altdata/icij_puller.py's own column lists for
icij_entities/icij_officers/icij_intermediaries (that puller is the only
writer of these tables in this codebase).
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

_ICIJ_ENTITIES_DDL = """
CREATE TABLE IF NOT EXISTS icij_entities (
    node_id INTEGER PRIMARY KEY,
    name TEXT,
    jurisdiction TEXT,
    country_codes TEXT,
    incorporation_date TEXT,
    inactivation_date TEXT,
    status TEXT,
    source_dataset TEXT,
    service_provider TEXT,
    address TEXT,
    note TEXT
)
"""

_ICIJ_OFFICERS_DDL = """
CREATE TABLE IF NOT EXISTS icij_officers (
    node_id INTEGER PRIMARY KEY,
    name TEXT,
    country_codes TEXT,
    source_dataset TEXT,
    valid_until TEXT,
    note TEXT
)
"""

_ICIJ_INTERMEDIARIES_DDL = """
CREATE TABLE IF NOT EXISTS icij_intermediaries (
    node_id INTEGER PRIMARY KEY,
    name TEXT,
    country_codes TEXT,
    source_dataset TEXT,
    status TEXT,
    address TEXT
)
"""

# Column names/types exactly as confirmed via information_schema.columns
# against production (2026-09-22): id bigint, from_node bigint, to_node
# bigint, rel_type text, source_dataset text, start_date text, end_date
# text, created_at timestamptz.
_ICIJ_RELATIONSHIPS_DDL = """
CREATE TABLE IF NOT EXISTS icij_relationships (
    id BIGSERIAL PRIMARY KEY,
    from_node BIGINT,
    to_node BIGINT,
    rel_type TEXT,
    source_dataset TEXT,
    start_date TEXT,
    end_date TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

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


@pytest.fixture(autouse=True)
def _schema(pg_engine: Engine):
    with pg_engine.begin() as conn:
        conn.execute(text(_ICIJ_ENTITIES_DDL))
        conn.execute(text(_ICIJ_OFFICERS_DDL))
        conn.execute(text(_ICIJ_INTERMEDIARIES_DDL))
        conn.execute(text(_ICIJ_RELATIONSHIPS_DDL))
        conn.execute(text(_ACTORS_DDL))
    yield


@pytest.fixture
def cleanup_ids() -> dict[str, list]:
    return {"node_ids": [], "actor_ids": [], "rel_ids": []}


@pytest.fixture(autouse=True)
def _cleanup(pg_engine: Engine, cleanup_ids: dict[str, list]):
    yield
    with pg_engine.begin() as conn:
        # Recreate any table a failure-isolation test dropped, so later
        # tests (or a later run against the same disposable DB) still see
        # the full real schema.
        conn.execute(text(_ICIJ_ENTITIES_DDL))
        conn.execute(text(_ICIJ_OFFICERS_DDL))
        conn.execute(text(_ICIJ_INTERMEDIARIES_DDL))
        if cleanup_ids["node_ids"]:
            conn.execute(text("DELETE FROM icij_relationships WHERE from_node = ANY(:ids) OR to_node = ANY(:ids)").bindparams(ids=cleanup_ids["node_ids"]))
            conn.execute(text("DELETE FROM icij_entities WHERE node_id = ANY(:ids)").bindparams(ids=cleanup_ids["node_ids"]))
            conn.execute(text("DELETE FROM icij_officers WHERE node_id = ANY(:ids)").bindparams(ids=cleanup_ids["node_ids"]))
            conn.execute(text("DELETE FROM icij_intermediaries WHERE node_id = ANY(:ids)").bindparams(ids=cleanup_ids["node_ids"]))
        if cleanup_ids["actor_ids"]:
            conn.execute(text("DELETE FROM actors WHERE id = ANY(:ids)").bindparams(ids=cleanup_ids["actor_ids"]))


def _node_id() -> int:
    # Deterministic-enough, collision-avoiding synthetic node_id for tests.
    return uuid.uuid4().int % 1_000_000_000


def test_intel_network_icij_traversal_works_against_the_real_schema(pg_engine: Engine, cleanup_ids: dict[str, list], monkeypatch):
    """Proves the corrected query -- joining icij_relationships to a union
    of icij_entities/icij_officers/icij_intermediaries by node_id -- finds
    a real relationship and stamps the right jurisdiction, dataset and
    relationship type. This exact scenario always threw against the old
    (entity_name/linked_to/relationship_type/jurisdiction-on-relationships)
    query."""
    import api.routers.intel as intel_router

    monkeypatch.setattr(intel_router, "get_db_engine", lambda: pg_engine)

    entity_node = _node_id()
    officer_node = _node_id()
    cleanup_ids["node_ids"].extend([entity_node, officer_node])

    entity_name = f"Offshore Corp {uuid.uuid4().hex[:8]}"
    officer_name = f"John Doe {uuid.uuid4().hex[:8]}"

    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO icij_entities (node_id, name, jurisdiction, source_dataset) "
            "VALUES (:nid, :name, 'BVI', 'Test Leaks')"
        ).bindparams(nid=entity_node, name=entity_name))
        conn.execute(text(
            "INSERT INTO icij_officers (node_id, name, source_dataset) "
            "VALUES (:nid, :name, 'Test Leaks')"
        ).bindparams(nid=officer_node, name=officer_name))
        conn.execute(text(
            "INSERT INTO icij_relationships (from_node, to_node, rel_type, source_dataset) "
            "VALUES (:f, :t, 'officer_of', 'Test Leaks')"
        ).bindparams(f=entity_node, t=officer_node))

    result = intel_router.intel_network(entity=entity_name, depth=1, _token="t")

    nodes_by_id = {n["id"]: n for n in result["data"]["nodes"]}
    assert officer_name.upper() in nodes_by_id, "the officer node reached via the relationship must be present"
    officer_node_dict = nodes_by_id[officer_name.upper()]
    assert officer_node_dict["type"] == "entity"

    edge = next(e for e in result["data"]["edges"] if e["target"] == officer_name.upper() or e["source"] == officer_name.upper())
    assert edge["relationship"] == "officer_of"
    assert edge["jurisdiction"] == "BVI", "jurisdiction must be resolved from icij_entities, the only table that carries it"
    assert edge["dataset"] == "Test Leaks"


def test_intel_network_icij_matches_via_linked_to_side_across_all_three_node_tables(pg_engine: Engine, cleanup_ids: dict[str, list], monkeypatch):
    """The first traversal test only searches by the from_node/n1 side
    (the entity). This proves the OTHER branch of the WHERE clause --
    matching by the to_node/n2 side -- also works, and exercises all three
    name-bearing tables in one chain (entity -> officer -> intermediary),
    not just entity+officer."""
    import api.routers.intel as intel_router

    monkeypatch.setattr(intel_router, "get_db_engine", lambda: pg_engine)

    entity_node = _node_id()
    officer_node = _node_id()
    intermediary_node = _node_id()
    cleanup_ids["node_ids"].extend([entity_node, officer_node, intermediary_node])

    entity_name = f"Chain Entity {uuid.uuid4().hex[:8]}"
    officer_name = f"Chain Officer {uuid.uuid4().hex[:8]}"
    intermediary_name = f"Chain Intermediary {uuid.uuid4().hex[:8]}"

    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO icij_entities (node_id, name, jurisdiction, source_dataset) "
            "VALUES (:nid, :name, 'BVI', 'Test Leaks')"
        ).bindparams(nid=entity_node, name=entity_name))
        conn.execute(text(
            "INSERT INTO icij_officers (node_id, name, source_dataset) "
            "VALUES (:nid, :name, 'Test Leaks')"
        ).bindparams(nid=officer_node, name=officer_name))
        conn.execute(text(
            "INSERT INTO icij_intermediaries (node_id, name, source_dataset) "
            "VALUES (:nid, :name, 'Test Leaks')"
        ).bindparams(nid=intermediary_node, name=intermediary_name))
        conn.execute(text(
            "INSERT INTO icij_relationships (from_node, to_node, rel_type, source_dataset) "
            "VALUES (:f, :t, 'introduced_by', 'Test Leaks')"
        ).bindparams(f=officer_node, t=intermediary_node))

    # Search by the intermediary's name -- it is the to_node/n2 side of the
    # relationship (officer -> intermediary), never exercised by the other
    # traversal test, which only searches by the from_node/n1 side.
    result = intel_router.intel_network(entity=intermediary_name, depth=1, _token="t")

    nodes_by_id = {n["id"]: n for n in result["data"]["nodes"]}
    assert officer_name.upper() in nodes_by_id, "matching via the to_node/n2 side of the JOIN must still reach the other end (the officer)"
    edge = next(e for e in result["data"]["edges"] if officer_name.upper() in (e["source"], e["target"]))
    assert edge["relationship"] == "introduced_by"
    assert edge["jurisdiction"] is None, "neither officers nor intermediaries carry jurisdiction"


def test_intel_network_icij_query_limit_bounds_a_single_hub_names_row_count(pg_engine: Engine, cleanup_ids: dict[str, list], monkeypatch):
    """A single well-connected ICIJ entity can legitimately have far more
    relationships than the actor side's connections[:20] slice would ever
    see -- the ICIJ query has no equivalent cap of its own except its new
    LIMIT 200. Proves that LIMIT actually bounds the row count processed
    for one hub name, not just that the clause exists in source."""
    import api.routers.intel as intel_router

    monkeypatch.setattr(intel_router, "get_db_engine", lambda: pg_engine)

    hub_node = _node_id()
    cleanup_ids["node_ids"].append(hub_node)
    hub_name = f"Hub Registered Agent {uuid.uuid4().hex[:8]}"

    leaf_nodes = [_node_id() for _ in range(250)]
    cleanup_ids["node_ids"].extend(leaf_nodes)

    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO icij_entities (node_id, name, jurisdiction, source_dataset) "
            "VALUES (:nid, :name, 'BVI', 'Test Leaks')"
        ).bindparams(nid=hub_node, name=hub_name))
        for i, leaf_id in enumerate(leaf_nodes):
            conn.execute(text(
                "INSERT INTO icij_entities (node_id, name, jurisdiction, source_dataset) "
                "VALUES (:nid, :name, 'BVI', 'Test Leaks')"
            ).bindparams(nid=leaf_id, name=f"Hub Leaf {i} {uuid.uuid4().hex[:6]}"))
            conn.execute(text(
                "INSERT INTO icij_relationships (from_node, to_node, rel_type, source_dataset) "
                "VALUES (:f, :t, 'registered_by', 'Test Leaks')"
            ).bindparams(f=hub_node, t=leaf_id))

    result = intel_router.intel_network(entity=hub_name, depth=1, _token="t")

    hub_edges = [e for e in result["data"]["edges"] if e["source"] == hub_name.upper()]
    assert len(hub_edges) <= 200, "a single hub name's ICIJ query must respect its LIMIT 200, not return all 250 relationships"


def test_intel_network_icij_failure_does_not_poison_the_actor_query(pg_engine: Engine, cleanup_ids: dict[str, list], monkeypatch):
    """Deliberately breaks the ICIJ block (drops a table its query
    references, mid-test) to force a real failure, then proves the actor
    query -- on the SAME connection, right after -- still succeeds. Without
    the conn.rollback() fix, PostgreSQL leaves the connection's transaction
    aborted after any failed statement, and every later statement on that
    connection (including this one) fails too -- this is exactly the
    production symptom that made #602's own actor-traversal fix invisible
    end-to-end despite being correct in isolation."""
    import api.routers.intel as intel_router

    monkeypatch.setattr(intel_router, "get_db_engine", lambda: pg_engine)

    from intelligence.actors.provenance import PROVENANCE_OBSERVED, SOURCE_OBSERVED

    actor_id = f"icijfix_test_{uuid.uuid4().hex[:10]}"
    actor_name = f"Isolation Test Actor {uuid.uuid4().hex[:8]}"
    cleanup_ids["actor_ids"].append(actor_id)

    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO actors (id, name, tier, category, provenance) "
            "VALUES (:id, :name, 'institutional', 'corporation', :prov)"
        ).bindparams(id=actor_id, name=actor_name, prov=PROVENANCE_OBSERVED))
        # Force the ICIJ query to genuinely fail: drop a table its UNION ALL
        # references. The autouse cleanup fixture recreates it afterward.
        conn.execute(text("DROP TABLE icij_intermediaries"))

    result = intel_router.intel_network(entity=actor_id, depth=1, _token="t")

    nodes_by_id = {n["id"]: n for n in result["data"]["nodes"]}
    assert actor_name.upper() in nodes_by_id, (
        "the actor node must still be added even though the ICIJ query on "
        "the same connection, immediately before it, genuinely failed"
    )
    actor_node = nodes_by_id[actor_name.upper()]
    assert actor_node["type"] == "actor"
    assert actor_node["source"] == SOURCE_OBSERVED, "the actor query's own fix must still take effect despite the prior ICIJ failure"
