"""
Wikidata SPARQL person-connection ingestion module for GRID.

Wikidata is the free, structured knowledge base maintained by the Wikimedia
Foundation.  It contains machine-readable facts about millions of real-world
entities -- people, organisations, political offices, corporate board seats,
family relationships, and more -- all linked via stable Q-identifiers and
property codes (e.g. P39 = "position held", P3320 = "board member of").

Why this matters for GRID's trading intelligence:

    1. **Actor enrichment** -- GRID tracks 500+ named financial/political actors.
       Wikidata fills in connections that no single financial data source
       provides: board memberships, prior government roles, political party
       affiliations, family ties, and corporate hierarchies.

    2. **Hidden-link discovery** -- a Fed governor who previously sat on a bank
       board, or a senator whose spouse runs a defence contractor, creates
       information asymmetries that move markets.  Wikidata surfaces these
       links automatically.

    3. **Graph visualisation** -- the enriched nodes and edges feed directly
       into GRID's D3 force-graph on the ActorNetwork frontend view,
       revealing the true topology of financial power.

Endpoint: https://query.wikidata.org/sparql  (public, no API key required)
Rate limit policy: respectful 1 req/sec with proper User-Agent.

Key Wikidata properties queried:
    P39   position held            P108  employer
    P3320 board member of          P127  owned by
    P749  parent organisation      P463  member of
    P102  member of political party
    P26   spouse   P22   father   P25   mother
    P1037 director/manager of      P169  chief executive officer
    P159  headquarters location
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SPARQL_URL: str = "https://query.wikidata.org/sparql"
_RATE_LIMIT_SECONDS: float = 1.0
_REQUEST_TIMEOUT: int = 60

_HEADERS: dict[str, str] = {
    "Accept": "application/sparql-results+json",
    "User-Agent": "GRID-Trading-Intelligence/1.0 (https://grid.stepdad.finance)",
}

# Wikidata property codes we care about, grouped by relationship semantics.
_PERSON_PROPERTIES: dict[str, str] = {
    "P39": "position_held",
    "P108": "employer",
    "P3320": "board_member_of",
    "P127": "owned_by",
    "P749": "parent_organisation",
    "P463": "member_of",
    "P102": "political_party",
    "P26": "spouse",
    "P22": "father",
    "P25": "mother",
    "P1037": "director_of",
    "P169": "ceo_of",
    "P159": "headquarters_location",
}

# Which properties point to other *persons* (vs organisations/places).
_PERSON_LINK_PROPS: frozenset[str] = frozenset({
    "P26", "P22", "P25",  # family
})

# Properties whose targets are typically organisations -- worth recursing.
_ORG_LINK_PROPS: frozenset[str] = frozenset({
    "P39", "P108", "P3320", "P463", "P1037", "P169",
})

# Maximum depth for recursive connection discovery.
_MAX_RECURSE_DEPTH: int = 1


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

class _RateLimiter:
    """Enforces a minimum interval between outbound HTTP requests."""

    def __init__(self, min_interval: float = _RATE_LIMIT_SECONDS) -> None:
        self._min_interval = min_interval
        self._last_call: float = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _qid(uri: str) -> str:
    """Extract Q-identifier from a full Wikidata URI."""
    return uri.rsplit("/", 1)[-1] if "/" in uri else uri


def _safe_val(binding: dict[str, Any], key: str) -> str:
    """Safely extract the 'value' from a SPARQL result binding."""
    node = binding.get(key)
    if node is None:
        return ""
    return node.get("value", "")


def _slugify(name: str) -> str:
    """Create a filesystem-safe slug from a name."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


# ---------------------------------------------------------------------------
# Puller
# ---------------------------------------------------------------------------

class WikidataPersonPuller(BasePuller):
    """Enriches GRID's actor network with Wikidata person connections.

    Workflow:
        1. For each GRID actor, search Wikidata by name to find the Q-id.
        2. For each matched Q-id, query all connection properties.
        3. For discovered connected entities, recurse one level deeper.
        4. Store results in ``wikidata_persons`` and ``wikidata_connections``.
        5. Provide ``get_graph_data()`` for the frontend D3 visualisation.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for this puller.
    """

    SOURCE_NAME: str = "wikidata_persons"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _SPARQL_URL,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 38,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._limiter = _RateLimiter()
        self._ensure_tables()
        log.info(
            "WikidataPersonPuller ready -- source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------
    # Table creation
    # ------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        """Create the wikidata_persons and wikidata_connections tables
        if they do not already exist."""
        with self.engine.begin() as conn:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS wikidata_persons ("
                "  wikidata_id TEXT PRIMARY KEY,"
                "  label TEXT NOT NULL,"
                "  description TEXT,"
                "  grid_actor_id TEXT,"
                "  category TEXT,"
                "  birth_date TEXT,"
                "  death_date TEXT,"
                "  nationality TEXT,"
                "  occupations JSONB DEFAULT '[]'::jsonb,"
                "  positions_held JSONB DEFAULT '[]'::jsonb,"
                "  image_url TEXT,"
                "  fetched_at TIMESTAMPTZ DEFAULT NOW(),"
                "  raw_data JSONB DEFAULT '{}'::jsonb"
                ")"
            ))
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS wikidata_connections ("
                "  id SERIAL PRIMARY KEY,"
                "  source_qid TEXT NOT NULL REFERENCES wikidata_persons(wikidata_id),"
                "  target_qid TEXT NOT NULL REFERENCES wikidata_persons(wikidata_id),"
                "  relationship TEXT NOT NULL,"
                "  property_id TEXT NOT NULL,"
                "  qualifier TEXT,"
                "  start_date TEXT,"
                "  end_date TEXT,"
                "  fetched_at TIMESTAMPTZ DEFAULT NOW(),"
                "  UNIQUE(source_qid, target_qid, property_id, qualifier)"
                ")"
            ))
        log.debug("wikidata_persons / wikidata_connections tables ensured")

    # ------------------------------------------------------------------
    # SPARQL helpers
    # ------------------------------------------------------------------

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(
            ConnectionError, TimeoutError, OSError, requests.RequestException,
        ),
    )
    def _sparql(self, query: str) -> list[dict[str, Any]]:
        """Execute a SPARQL query against the Wikidata endpoint.

        Parameters:
            query: SPARQL query string.

        Returns:
            List of result bindings (dicts).
        """
        self._limiter.wait()
        resp = requests.get(
            _SPARQL_URL,
            params={"query": query, "format": "json"},
            headers=_HEADERS,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("results", {}).get("bindings", [])

    # ------------------------------------------------------------------
    # Search by name
    # ------------------------------------------------------------------

    def search_person(self, name: str) -> list[dict[str, Any]]:
        """Search Wikidata for a person by name.

        Parameters:
            name: Full name to search (e.g. "Jerome Powell").

        Returns:
            List of candidate dicts with keys: wikidata_id, label,
            description, birth_date, death_date, image_url.
        """
        if not name or len(name.strip()) < 2:
            return []

        sanitised = name.strip().replace('"', '\\"')
        query = (
            "SELECT ?person ?personLabel ?personDescription "
            "?birth ?death ?image WHERE { "
            '  ?person wdt:P31 wd:Q5 . '
            '  ?person rdfs:label ?name . '
            '  FILTER(LANG(?name) = "en") '
            f'  FILTER(CONTAINS(LCASE(?name), LCASE("{sanitised}"))) '
            '  OPTIONAL {{ ?person wdt:P569 ?birth }} '
            '  OPTIONAL {{ ?person wdt:P570 ?death }} '
            '  OPTIONAL {{ ?person wdt:P18 ?image }} '
            '  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }} '
            "} LIMIT 5"
        )

        try:
            bindings = self._sparql(query)
        except Exception as exc:
            log.warning(
                "Wikidata person search failed for '{name}': {e}",
                name=name, e=exc,
            )
            return []

        results: list[dict[str, Any]] = []
        seen_qids: set[str] = set()
        for b in bindings:
            qid = _qid(_safe_val(b, "person"))
            if not qid or qid in seen_qids:
                continue
            seen_qids.add(qid)
            results.append({
                "wikidata_id": qid,
                "label": _safe_val(b, "personLabel"),
                "description": _safe_val(b, "personDescription"),
                "birth_date": _safe_val(b, "birth"),
                "death_date": _safe_val(b, "death"),
                "image_url": _safe_val(b, "image"),
            })

        log.info(
            "Wikidata search '{name}': {n} candidates",
            name=name, n=len(results),
        )
        return results

    # ------------------------------------------------------------------
    # Fetch connections for a Q-id
    # ------------------------------------------------------------------

    def _fetch_connections(self, qid: str) -> list[dict[str, Any]]:
        """Fetch all relevant connections for a Wikidata person Q-id.

        Queries each property in ``_PERSON_PROPERTIES`` individually to
        keep SPARQL queries simple and reliable.

        Parameters:
            qid: Wikidata Q-identifier (e.g. "Q313566").

        Returns:
            List of connection dicts with keys: property_id, relationship,
            target_qid, target_label, target_description, start_date,
            end_date.
        """
        connections: list[dict[str, Any]] = []

        for prop_id, rel_name in _PERSON_PROPERTIES.items():
            query = (
                f"SELECT ?target ?targetLabel ?targetDescription "
                f"?start ?end WHERE {{ "
                f"  wd:{qid} p:{prop_id} ?stmt . "
                f"  ?stmt ps:{prop_id} ?target . "
                f"  OPTIONAL {{ ?stmt pq:P580 ?start }} "
                f"  OPTIONAL {{ ?stmt pq:P582 ?end }} "
                f'  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }} '
                f"}} LIMIT 50"
            )

            try:
                bindings = self._sparql(query)
            except Exception as exc:
                log.debug(
                    "Wikidata {prop} query failed for {qid}: {e}",
                    prop=prop_id, qid=qid, e=exc,
                )
                continue

            for b in bindings:
                target_uri = _safe_val(b, "target")
                target_qid = _qid(target_uri)
                if not target_qid or not target_qid.startswith("Q"):
                    continue

                connections.append({
                    "property_id": prop_id,
                    "relationship": rel_name,
                    "target_qid": target_qid,
                    "target_label": _safe_val(b, "targetLabel"),
                    "target_description": _safe_val(b, "targetDescription"),
                    "start_date": _safe_val(b, "start"),
                    "end_date": _safe_val(b, "end"),
                })

        log.info(
            "Wikidata connections for {qid}: {n} links across {p} properties",
            qid=qid, n=len(connections), p=len(_PERSON_PROPERTIES),
        )
        return connections

    # ------------------------------------------------------------------
    # Upsert helpers
    # ------------------------------------------------------------------

    def _upsert_person(
        self,
        conn: Any,
        wikidata_id: str,
        label: str,
        description: str = "",
        grid_actor_id: str | None = None,
        category: str | None = None,
        birth_date: str = "",
        death_date: str = "",
        image_url: str = "",
        raw_data: dict[str, Any] | None = None,
    ) -> None:
        """Insert or update a row in wikidata_persons.

        Parameters:
            conn: Active DB connection (inside a transaction).
            wikidata_id: Wikidata Q-id.
            label: Person's display name.
            description: Short Wikidata description.
            grid_actor_id: GRID actor ID if this person matches a known actor.
            category: Actor category (central_bank, government, etc.).
            birth_date: ISO date string or empty.
            death_date: ISO date string or empty.
            image_url: Wikimedia Commons image URL.
            raw_data: Full SPARQL result for archival.
        """
        conn.execute(
            text(
                "INSERT INTO wikidata_persons "
                "(wikidata_id, label, description, grid_actor_id, category, "
                " birth_date, death_date, image_url, fetched_at, raw_data) "
                "VALUES (:wid, :label, :desc, :gaid, :cat, "
                " :birth, :death, :img, :now, :raw) "
                "ON CONFLICT (wikidata_id) DO UPDATE SET "
                "  label = EXCLUDED.label, "
                "  description = EXCLUDED.description, "
                "  grid_actor_id = COALESCE(EXCLUDED.grid_actor_id, "
                "    wikidata_persons.grid_actor_id), "
                "  category = COALESCE(EXCLUDED.category, "
                "    wikidata_persons.category), "
                "  birth_date = EXCLUDED.birth_date, "
                "  death_date = EXCLUDED.death_date, "
                "  image_url = EXCLUDED.image_url, "
                "  fetched_at = EXCLUDED.fetched_at, "
                "  raw_data = EXCLUDED.raw_data"
            ),
            {
                "wid": wikidata_id,
                "label": label,
                "desc": description or "",
                "gaid": grid_actor_id,
                "cat": category,
                "birth": birth_date or "",
                "death": death_date or "",
                "img": image_url or "",
                "now": datetime.now(timezone.utc),
                "raw": json.dumps(raw_data or {}),
            },
        )

    def _upsert_connection(
        self,
        conn: Any,
        source_qid: str,
        target_qid: str,
        relationship: str,
        property_id: str,
        qualifier: str = "",
        start_date: str = "",
        end_date: str = "",
    ) -> None:
        """Insert or update a row in wikidata_connections.

        Parameters:
            conn: Active DB connection (inside a transaction).
            source_qid: Source person Q-id.
            target_qid: Target entity Q-id.
            relationship: Human-readable relationship name.
            property_id: Wikidata property code (e.g. "P39").
            qualifier: Qualifier label (position name, org name, etc.).
            start_date: ISO date or empty.
            end_date: ISO date or empty.
        """
        conn.execute(
            text(
                "INSERT INTO wikidata_connections "
                "(source_qid, target_qid, relationship, property_id, "
                " qualifier, start_date, end_date, fetched_at) "
                "VALUES (:src, :tgt, :rel, :pid, :qual, :sd, :ed, :now) "
                "ON CONFLICT (source_qid, target_qid, property_id, qualifier) "
                "DO UPDATE SET "
                "  relationship = EXCLUDED.relationship, "
                "  start_date = EXCLUDED.start_date, "
                "  end_date = EXCLUDED.end_date, "
                "  fetched_at = EXCLUDED.fetched_at"
            ),
            {
                "src": source_qid,
                "tgt": target_qid,
                "rel": relationship,
                "pid": property_id,
                "qual": qualifier or "",
                "sd": start_date or "",
                "ed": end_date or "",
                "now": datetime.now(timezone.utc),
            },
        )

    # ------------------------------------------------------------------
    # Core enrichment pipeline
    # ------------------------------------------------------------------

    def _enrich_actor(
        self,
        grid_actor_id: str,
        actor_name: str,
        actor_category: str | None = None,
        depth: int = 0,
        visited: set[str] | None = None,
    ) -> dict[str, int]:
        """Search Wikidata for a GRID actor and store connections.

        Parameters:
            grid_actor_id: GRID internal actor ID (e.g. "fed_powell").
            actor_name: Human name to search.
            actor_category: Actor category for tagging.
            depth: Current recursion depth.
            visited: Set of already-visited Q-ids to avoid cycles.

        Returns:
            Dict with persons_stored and connections_stored counts.
        """
        if visited is None:
            visited = set()

        stats = {"persons_stored": 0, "connections_stored": 0}

        # Step 1 -- find the person on Wikidata
        candidates = self.search_person(actor_name)
        if not candidates:
            log.debug(
                "No Wikidata match for actor '{name}'",
                name=actor_name,
            )
            return stats

        # Take the best candidate (first result).
        best = candidates[0]
        qid = best["wikidata_id"]

        if qid in visited:
            return stats
        visited.add(qid)

        # Step 2 -- store the person
        with self.engine.begin() as conn:
            self._upsert_person(
                conn,
                wikidata_id=qid,
                label=best["label"],
                description=best.get("description", ""),
                grid_actor_id=grid_actor_id if depth == 0 else None,
                category=actor_category,
                birth_date=best.get("birth_date", ""),
                death_date=best.get("death_date", ""),
                image_url=best.get("image_url", ""),
                raw_data=best,
            )
            stats["persons_stored"] += 1

        # Step 3 -- fetch connections
        connections = self._fetch_connections(qid)

        with self.engine.begin() as conn:
            for link in connections:
                target_qid = link["target_qid"]
                target_label = link["target_label"]
                relationship = link["relationship"]
                property_id = link["property_id"]

                # Ensure target person/entity exists in wikidata_persons
                self._upsert_person(
                    conn,
                    wikidata_id=target_qid,
                    label=target_label,
                    description=link.get("target_description", ""),
                )
                stats["persons_stored"] += 1

                # Store the connection
                self._upsert_connection(
                    conn,
                    source_qid=qid,
                    target_qid=target_qid,
                    relationship=relationship,
                    property_id=property_id,
                    qualifier=target_label,
                    start_date=link.get("start_date", ""),
                    end_date=link.get("end_date", ""),
                )
                stats["connections_stored"] += 1

        # Step 4 -- recurse one level for connected persons/orgs
        if depth < _MAX_RECURSE_DEPTH:
            for link in connections:
                target_qid = link["target_qid"]
                prop_id = link["property_id"]

                # Only recurse into person-linked or org-linked properties
                if prop_id not in (_PERSON_LINK_PROPS | _ORG_LINK_PROPS):
                    continue
                if target_qid in visited:
                    continue

                sub_stats = self._enrich_actor(
                    grid_actor_id="",
                    actor_name=link["target_label"],
                    actor_category=None,
                    depth=depth + 1,
                    visited=visited,
                )
                stats["persons_stored"] += sub_stats["persons_stored"]
                stats["connections_stored"] += sub_stats["connections_stored"]

        return stats

    # ------------------------------------------------------------------
    # pull_all -- main entry point
    # ------------------------------------------------------------------

    def pull_all(
        self,
        actors: dict[str, dict[str, Any]] | None = None,
        max_actors: int = 50,
    ) -> dict[str, Any]:
        """Run the full Wikidata person enrichment pipeline.

        Parameters:
            actors: Dict of GRID actors keyed by actor_id.  If None,
                imports from ``intelligence.actors.seed_data``.
            max_actors: Maximum number of actors to process per run
                (to stay within Wikidata rate-limit budget).

        Returns:
            Summary dict with status, actors_searched, actors_matched,
            total_persons_stored, total_connections_stored.
        """
        if actors is None:
            try:
                from intelligence.actors.seed_data import _KNOWN_ACTORS
                actors = _KNOWN_ACTORS
            except ImportError:
                log.error(
                    "Cannot import _KNOWN_ACTORS from intelligence.actors.seed_data"
                )
                return {"status": "FAILED", "error": "seed_data import failed"}

        log.info(
            "WikidataPersonPuller.pull_all -- {n} actors available, "
            "processing up to {m}",
            n=len(actors), m=max_actors,
        )

        total_persons = 0
        total_connections = 0
        actors_searched = 0
        actors_matched = 0
        visited: set[str] = set()

        for actor_id, actor_data in list(actors.items())[:max_actors]:
            name = actor_data.get("name", "")
            category = actor_data.get("category")
            if not name:
                continue

            actors_searched += 1

            try:
                stats = self._enrich_actor(
                    grid_actor_id=actor_id,
                    actor_name=name,
                    actor_category=category,
                    depth=0,
                    visited=visited,
                )
            except Exception as exc:
                log.warning(
                    "Failed to enrich actor '{name}' ({aid}): {e}",
                    name=name, aid=actor_id, e=exc,
                )
                continue

            if stats["persons_stored"] > 0:
                actors_matched += 1

            total_persons += stats["persons_stored"]
            total_connections += stats["connections_stored"]

            log.info(
                "Actor '{name}': {p} persons, {c} connections",
                name=name,
                p=stats["persons_stored"],
                c=stats["connections_stored"],
            )

        summary = {
            "status": "SUCCESS",
            "actors_searched": actors_searched,
            "actors_matched": actors_matched,
            "total_persons_stored": total_persons,
            "total_connections_stored": total_connections,
        }

        log.info(
            "WikidataPersonPuller.pull_all complete: "
            "{searched} searched, {matched} matched, "
            "{persons} persons, {conns} connections stored",
            searched=actors_searched,
            matched=actors_matched,
            persons=total_persons,
            conns=total_connections,
        )
        return summary

    # ------------------------------------------------------------------
    # Graph data for frontend visualisation
    # ------------------------------------------------------------------

    def get_graph_data(
        self,
        grid_only: bool = False,
        limit: int = 500,
    ) -> dict[str, Any]:
        """Return nodes and edges for the frontend graph visualisation.

        Parameters:
            grid_only: If True, only return nodes linked to a GRID actor.
            limit: Maximum number of nodes to return.

        Returns:
            Dict with ``nodes`` (list of node dicts) and ``edges``
            (list of edge dicts) ready for D3 rendering.
        """
        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []

        # -- Fetch persons --
        with self.engine.connect() as conn:
            if grid_only:
                rows = conn.execute(
                    text(
                        "SELECT wikidata_id, label, description, "
                        "  grid_actor_id, category, image_url "
                        "FROM wikidata_persons "
                        "WHERE grid_actor_id IS NOT NULL "
                        "ORDER BY fetched_at DESC "
                        "LIMIT :lim"
                    ),
                    {"lim": limit},
                ).fetchall()
            else:
                rows = conn.execute(
                    text(
                        "SELECT wikidata_id, label, description, "
                        "  grid_actor_id, category, image_url "
                        "FROM wikidata_persons "
                        "ORDER BY fetched_at DESC "
                        "LIMIT :lim"
                    ),
                    {"lim": limit},
                ).fetchall()

        node_ids: set[str] = set()
        for row in rows:
            qid = row[0]
            node_ids.add(qid)
            nodes.append({
                "id": qid,
                "label": row[1],
                "description": row[2] or "",
                "grid_actor_id": row[3],
                "category": row[4] or "unknown",
                "image_url": row[5] or "",
                "is_grid_actor": row[3] is not None,
            })

        if not node_ids:
            return {"nodes": [], "edges": []}

        # -- Fetch connections between known nodes --
        with self.engine.connect() as conn:
            conn_rows = conn.execute(
                text(
                    "SELECT source_qid, target_qid, relationship, "
                    "  property_id, qualifier, start_date, end_date "
                    "FROM wikidata_connections "
                    "ORDER BY fetched_at DESC "
                    "LIMIT :lim"
                ),
                {"lim": limit * 3},
            ).fetchall()

        for row in conn_rows:
            src, tgt = row[0], row[1]
            # Only include edges where both endpoints are in our node set
            if src in node_ids and tgt in node_ids:
                edges.append({
                    "source": src,
                    "target": tgt,
                    "relationship": row[2],
                    "property_id": row[3],
                    "qualifier": row[4] or "",
                    "start_date": row[5] or "",
                    "end_date": row[6] or "",
                })

        log.info(
            "Graph data: {n} nodes, {e} edges",
            n=len(nodes), e=len(edges),
        )
        return {"nodes": nodes, "edges": edges}

    # ------------------------------------------------------------------
    # Convenience: get connections for a single person
    # ------------------------------------------------------------------

    def get_person_connections(self, wikidata_id: str) -> dict[str, Any]:
        """Return stored connections for a single person.

        Parameters:
            wikidata_id: Wikidata Q-id (e.g. "Q313566").

        Returns:
            Dict with person info and list of connections.
        """
        person: dict[str, Any] | None = None
        connections: list[dict[str, Any]] = []

        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT wikidata_id, label, description, "
                    "  grid_actor_id, category, birth_date, "
                    "  death_date, image_url "
                    "FROM wikidata_persons "
                    "WHERE wikidata_id = :qid"
                ),
                {"qid": wikidata_id},
            ).fetchone()

            if row:
                person = {
                    "wikidata_id": row[0],
                    "label": row[1],
                    "description": row[2] or "",
                    "grid_actor_id": row[3],
                    "category": row[4] or "",
                    "birth_date": row[5] or "",
                    "death_date": row[6] or "",
                    "image_url": row[7] or "",
                }

            conn_rows = conn.execute(
                text(
                    "SELECT c.target_qid, p.label, c.relationship, "
                    "  c.property_id, c.qualifier, c.start_date, c.end_date "
                    "FROM wikidata_connections c "
                    "LEFT JOIN wikidata_persons p ON p.wikidata_id = c.target_qid "
                    "WHERE c.source_qid = :qid "
                    "ORDER BY c.property_id"
                ),
                {"qid": wikidata_id},
            ).fetchall()

            for crow in conn_rows:
                connections.append({
                    "target_qid": crow[0],
                    "target_label": crow[1] or "",
                    "relationship": crow[2],
                    "property_id": crow[3],
                    "qualifier": crow[4] or "",
                    "start_date": crow[5] or "",
                    "end_date": crow[6] or "",
                })

        return {"person": person, "connections": connections}

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_stats(self) -> dict[str, Any]:
        """Return summary statistics about the stored Wikidata data.

        Returns:
            Dict with total_persons, grid_linked_persons,
            total_connections, connections_by_type.
        """
        with self.engine.connect() as conn:
            total_persons = conn.execute(
                text("SELECT COUNT(*) FROM wikidata_persons")
            ).scalar() or 0

            grid_linked = conn.execute(
                text(
                    "SELECT COUNT(*) FROM wikidata_persons "
                    "WHERE grid_actor_id IS NOT NULL"
                )
            ).scalar() or 0

            total_connections = conn.execute(
                text("SELECT COUNT(*) FROM wikidata_connections")
            ).scalar() or 0

            by_type_rows = conn.execute(
                text(
                    "SELECT relationship, COUNT(*) AS cnt "
                    "FROM wikidata_connections "
                    "GROUP BY relationship "
                    "ORDER BY cnt DESC"
                )
            ).fetchall()

        connections_by_type = {
            row[0]: row[1] for row in by_type_rows
        }

        return {
            "total_persons": total_persons,
            "grid_linked_persons": grid_linked,
            "total_connections": total_connections,
            "connections_by_type": connections_by_type,
        }
