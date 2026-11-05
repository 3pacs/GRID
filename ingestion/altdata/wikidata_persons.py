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
_SEARCH_URL: str = "https://www.wikidata.org/w/api.php"
_RATE_LIMIT_SECONDS: float = 1.0
_REQUEST_TIMEOUT: int = 60

# 429 handling (2026-11-04: 437 "429 Too Many Requests" warnings in 24h, the
# entity search ran unthrottled with a generic User-Agent and the batch kept
# going). A 429/503 waits the server's Retry-After (or a short backoff) and
# retries; a Retry-After above the cap, or this many 429s in a row, stops the
# whole batch, which then reports PARTIAL with the rows already committed.
_MAX_CONSECUTIVE_429: int = 3
_MAX_RETRY_AFTER_SECONDS: float = 60.0
_DEFAULT_429_BACKOFF_SECONDS: float = 5.0

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

# Hops of family-link discovery beyond a matched actor. The old value (6),
# combined with re-*searching* every connection's label as if it were a
# person's name ('delegate', 'Chair of the Federal Reserve', 'Farmers and
# Drovers Bank'), made each run fan out into hundreds of pointless searches.
# Recursion now follows the connection's Q-id directly, one hop deep.
_MAX_RECURSE_DEPTH: int = 1


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

class WikidataRateLimited(RuntimeError):
    """Wikidata keeps refusing with 429; stop the batch instead of hammering."""


def _retry_after_seconds(value: str | None) -> float | None:
    """Parse a Retry-After header (delta-seconds or HTTP-date)."""
    if not value:
        return None
    value = value.strip()
    try:
        return max(float(value), 0.0)
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime

        when = parsedate_to_datetime(value)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return max((when - datetime.now(timezone.utc)).total_seconds(), 0.0)
    except (TypeError, ValueError, IndexError):
        return None


def _clean_actor_name(name: str) -> str:
    """Person part of a seed actor name.

    Seed names carry context, e.g. 'Jim Simons (Estate / Renaissance)' or
    'George Soros / Soros Fund Management'; the entity search wants the
    person's name only.
    """
    cleaned = re.sub(r"\s*\([^)]*\)", "", name or "")
    cleaned = cleaned.split(" / ")[0]
    return " ".join(cleaned.split())


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

def _written(result: Any) -> int:
    """Rows an upsert wrote (inserted or refreshed); unknown fails closed."""
    affected = getattr(result, "rowcount", None)
    if type(affected) is not int or affected < 0:
        raise ValueError("Unknown affected row count")
    return affected


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

    def _http_get(self, url: str, params: dict[str, Any], headers: dict[str, str],
                  timeout: int) -> requests.Response:
        """Rate-limited GET that honours 429/503 Retry-After.

        Raises:
            WikidataRateLimited: Retry-After above the cap, or
                ``_MAX_CONSECUTIVE_429`` refusals in a row.
            requests.HTTPError: any other non-2xx response.
        """
        while True:
            self._limiter.wait()
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code not in (429, 503):
                self._consecutive_429 = 0
                resp.raise_for_status()
                return resp
            self._consecutive_429 = getattr(self, "_consecutive_429", 0) + 1
            wait = _retry_after_seconds(resp.headers.get("Retry-After"))
            if wait is None:
                wait = _DEFAULT_429_BACKOFF_SECONDS * self._consecutive_429
            if wait > _MAX_RETRY_AFTER_SECONDS or self._consecutive_429 >= _MAX_CONSECUTIVE_429:
                raise WikidataRateLimited(
                    f"Wikidata HTTP {resp.status_code} x{self._consecutive_429} "
                    f"(Retry-After {wait:.0f}s); stopping this batch"
                )
            log.info("Wikidata HTTP {s}; waiting {w:.1f}s (Retry-After)", s=resp.status_code, w=wait)
            time.sleep(wait)

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
        resp = self._http_get(
            _SPARQL_URL,
            params={"query": query, "format": "json"},
            headers=_HEADERS,
            timeout=_REQUEST_TIMEOUT,
        )
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

        # Use mwapi EntitySearch for fast name lookup, then enrich via SPARQL.
        # The old CONTAINS(LCASE()) approach scanned all Q5 entities — too slow.
        # Same limiter and descriptive User-Agent as SPARQL (Wikimedia UA policy).
        search_params = {
            "action": "wbsearchentities",
            "search": name.strip(),
            "language": "en",
            "type": "item",
            "limit": "5",
            "format": "json",
        }
        try:
            resp = self._http_get(
                _SEARCH_URL, params=search_params,
                headers={"User-Agent": _HEADERS["User-Agent"]}, timeout=15,
            )
            candidates = resp.json().get("search", [])
        except WikidataRateLimited:
            raise
        except Exception as exc:
            self._request_errors = getattr(self, "_request_errors", 0) + 1
            log.warning(
                "Wikidata entity search failed for '{name}': {e}",
                name=name, e=exc,
            )
            return []

        if not candidates:
            return []

        # Filter to humans (Q5) and fetch birth/death/image via SPARQL
        qid_values = " ".join(f"wd:{c['id']}" for c in candidates)
        query = (
            "SELECT ?person ?personLabel ?personDescription "
            "?birth ?death ?image WHERE { "
            f"  VALUES ?person {{ {qid_values} }} "
            "  ?person wdt:P31 wd:Q5 . "
            "  OPTIONAL { ?person wdt:P569 ?birth } "
            "  OPTIONAL { ?person wdt:P570 ?death } "
            "  OPTIONAL { ?person wdt:P18 ?image } "
            '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" } '
            "} LIMIT 5"
        )

        try:
            bindings = self._sparql(query)
        except WikidataRateLimited:
            raise
        except Exception as exc:
            self._request_errors = getattr(self, "_request_errors", 0) + 1
            log.warning(
                "Wikidata SPARQL enrichment failed for '{name}': {e}",
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

        One SPARQL query covers every property in ``_PERSON_PROPERTIES``
        (``VALUES (?p ?ps)``; shape probed 2026-11-04) instead of one query
        per property: 13x fewer requests against a rate-limited endpoint.
        Each property keeps the old 50-statement cap.

        Parameters:
            qid: Wikidata Q-identifier (e.g. "Q313566").

        Returns:
            List of connection dicts with keys: property_id, relationship,
            target_qid, target_label, target_description, start_date,
            end_date.
        """
        if not re.fullmatch(r"Q\d+", qid or ""):
            return []
        values = " ".join(f"(p:{pid} ps:{pid})" for pid in _PERSON_PROPERTIES)
        query = (
            "SELECT ?p ?target ?targetLabel ?targetDescription ?start ?end WHERE { "
            f"  VALUES (?p ?ps) {{ {values} }} "
            f"  wd:{qid} ?p ?stmt . "
            "  ?stmt ?ps ?target . "
            "  OPTIONAL { ?stmt pq:P580 ?start } "
            "  OPTIONAL { ?stmt pq:P582 ?end } "
            '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" } '
            f"}} LIMIT {50 * len(_PERSON_PROPERTIES)}"
        )

        try:
            bindings = self._sparql(query)
        except WikidataRateLimited:
            raise
        except Exception as exc:
            self._request_errors = getattr(self, "_request_errors", 0) + 1
            log.debug("Wikidata connections query failed for {qid}: {e}", qid=qid, e=exc)
            return []

        connections: list[dict[str, Any]] = []
        per_prop: dict[str, int] = {}
        for b in bindings:
            prop_id = _qid(_safe_val(b, "p"))
            rel_name = _PERSON_PROPERTIES.get(prop_id)
            if rel_name is None or per_prop.get(prop_id, 0) >= 50:
                continue
            target_qid = _qid(_safe_val(b, "target"))
            if not target_qid or not target_qid.startswith("Q"):
                continue
            per_prop[prop_id] = per_prop.get(prop_id, 0) + 1
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
            qid=qid, n=len(connections), p=len(per_prop),
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
    ) -> int:
        """Insert or update a row in wikidata_persons (returns rows written).

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
        return _written(conn.execute(
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
        ))

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
    ) -> int:
        """Insert or update a row in wikidata_connections (returns rows written).

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
        return _written(conn.execute(
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
        ))

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
            Dict with persons_stored, connections_stored and rows_written
            (committed upserts) counts.
        """
        if visited is None:
            visited = set()

        stats = {"persons_stored": 0, "connections_stored": 0, "rows_written": 0}

        # Step 1 -- find the person on Wikidata (humans only, Q5).
        candidates = self.search_person(_clean_actor_name(actor_name))
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
            written = self._upsert_person(
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
        stats["rows_written"] += written

        self._enrich_qid(qid, depth=depth, visited=visited, stats=stats)
        return stats

    def _enrich_qid(self, qid: str, *, depth: int, visited: set[str], stats: dict[str, int]) -> None:
        """Store ``qid``'s connections, then follow them by Q-id.

        Recursion uses the connection's own Q-id; it never re-searches a
        connection's label (an office, an employer, a party) as a name.
        """
        connections = self._fetch_connections(qid)

        persons = connections_n = written = 0
        with self.engine.begin() as conn:
            for link in connections:
                target_qid = link["target_qid"]
                target_label = link["target_label"]

                # Ensure target person/entity exists in wikidata_persons
                written += self._upsert_person(
                    conn,
                    wikidata_id=target_qid,
                    label=target_label,
                    description=link.get("target_description", ""),
                )
                persons += 1

                # Store the connection
                written += self._upsert_connection(
                    conn,
                    source_qid=qid,
                    target_qid=target_qid,
                    relationship=link["relationship"],
                    property_id=link["property_id"],
                    qualifier=target_label,
                    start_date=link.get("start_date", ""),
                    end_date=link.get("end_date", ""),
                )
                connections_n += 1
        # Counted only once the transaction committed.
        stats["persons_stored"] += persons
        stats["connections_stored"] += connections_n
        stats["rows_written"] += written

        if depth >= _MAX_RECURSE_DEPTH:
            return
        for link in connections:
            target_qid = link["target_qid"]
            # Family links are persons. Organisation targets were never
            # enriched in practice (their labels failed the Q5 human filter
            # of the old label search), so following them would only add
            # load, not data, to a person-connection puller.
            if link["property_id"] not in _PERSON_LINK_PROPS:
                continue
            if target_qid in visited:
                continue
            visited.add(target_qid)
            self._enrich_qid(target_qid, depth=depth + 1, visited=visited, stats=stats)

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
                return {"status": "FAILED", "rows_upserted": 0, "error": "seed_data import failed"}

        log.info(
            "WikidataPersonPuller.pull_all -- {n} actors available, "
            "processing up to {m}",
            n=len(actors), m=max_actors,
        )

        total_persons = 0
        total_connections = 0
        rows_written = 0
        actors_searched = 0
        actors_matched = 0
        actors_failed = 0
        visited: set[str] = set()
        stopped: str | None = None
        self._request_errors = 0
        self._consecutive_429 = 0

        batch = [(aid, data) for aid, data in list(actors.items())[:max_actors] if data.get("name")]
        for actor_id, actor_data in batch:
            name = actor_data.get("name", "")
            category = actor_data.get("category")
            errors_before = self._request_errors

            try:
                stats = self._enrich_actor(
                    grid_actor_id=actor_id,
                    actor_name=name,
                    actor_category=category,
                    depth=0,
                    visited=visited,
                )
            except WikidataRateLimited as exc:
                stopped = str(exc)
                log.warning("Wikidata rate limit: {e} after {n} actors", e=stopped, n=actors_searched)
                break
            except Exception as exc:
                actors_failed += 1
                actors_searched += 1
                log.warning(
                    "Failed to enrich actor '{name}' ({aid}): {e}",
                    name=name, aid=actor_id, e=exc,
                )
                continue

            actors_searched += 1
            if self._request_errors > errors_before:
                actors_failed += 1
            if stats["persons_stored"] > 0:
                actors_matched += 1

            total_persons += stats["persons_stored"]
            total_connections += stats["connections_stored"]
            rows_written += stats["rows_written"]

            log.info(
                "Actor '{name}': {p} persons, {c} connections",
                name=name,
                p=stats["persons_stored"],
                c=stats["connections_stored"],
            )

        total = len(batch)
        if stopped or actors_failed:
            # Rows committed so far are kept; coverage is incomplete.
            status = "PARTIAL"
        else:
            status = "SUCCESS"
        summary: dict[str, Any] = {
            "status": status,
            "rows_upserted": rows_written,
            "succeeded": actors_searched - actors_failed,
            "total": total,
            "actors_searched": actors_searched,
            "actors_matched": actors_matched,
            "total_persons_stored": total_persons,
            "total_connections_stored": total_connections,
        }
        if stopped:
            summary["error"] = stopped
            summary["stopped_by_rate_limit"] = True
        elif actors_failed:
            summary["error"] = f"{actors_failed} of {total} actors had Wikidata request failures"

        log.info(
            "WikidataPersonPuller.pull_all complete: "
            "{searched} searched, {matched} matched, "
            "{persons} persons, {conns} connections stored ({rows} rows, {st})",
            searched=actors_searched,
            matched=actors_matched,
            persons=total_persons,
            conns=total_connections,
            rows=rows_written,
            st=status,
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
