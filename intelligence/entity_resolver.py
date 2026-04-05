"""
GRID Intelligence — Entity Resolution Engine.

The same person or company appears across multiple datasets with different
name formats. Senate trades say "David A Perdue , Jr", ICIJ says "DAVID PERDUE",
FEC says "PERDUE, DAVID A", the actors table says "David Perdue", and
OpenSanctions says "Perdue, David Alfred". These are all the same person.

This module resolves them into a single canonical entity, builds a resolution
index across all data sources, and identifies bridge entities — people who
appear across multiple intelligence domains (senate trades + ICIJ + FEC +
sanctions). Bridges are the highest-value intelligence.

Key entry points:
    normalize_name          — canonical form from any input variant
    EntityResolver.resolve  — find all records matching an entity
    EntityResolver.build_resolution_index — scan all sources, cluster matches
    EntityResolver.find_connections — multi-hop graph traversal
    EntityResolver.discover_bridges — find cross-domain bridge entities
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════

# Similarity thresholds
DEFINITE_MATCH_THRESHOLD: float = 0.95
LIKELY_MATCH_THRESHOLD: float = 0.85

# Titles and suffixes to strip from person names
_PERSON_TITLES = re.compile(
    r"\b(Mr|Mrs|Ms|Miss|Dr|Prof|Rev|Hon|Sen|Rep|Gov|Pres|Sgt|Cpl|Pvt|Lt|Capt|Maj|Col|Gen|Adm|Sir|Dame|Lord|Lady)\b\.?",
    re.IGNORECASE,
)
_PERSON_SUFFIXES = re.compile(
    r",?\s*\b(Jr|Sr|I{1,3}|IV|V|VI{0,3}|VIII|IX|X|Esq|PhD|MD|DDS|CPA|RN|JD|MBA)\b\.?",
    re.IGNORECASE,
)

# Company suffixes to normalize
_COMPANY_SUFFIXES = re.compile(
    r"\b(Inc|Corp|Corporation|Ltd|Limited|LLC|LLP|LP|PLC|AG|GmbH|SA|S\.A\.|S\.A|"
    r"S\.L\.|SL|NV|N\.V\.|BV|B\.V\.|SE|AB|Pty|Co|Company|Group|Holdings|"
    r"Holding|International|Intl|Enterprises|Partners|Fund|Trust|Association|"
    r"Assoc|Foundation)\.?\s*$",
    re.IGNORECASE,
)

# Data source domains — each represents a distinct intelligence channel
SOURCE_DOMAINS: dict[str, str] = {
    "senate_trade": "congressional",
    "house_trade": "congressional",
    "congressional": "congressional",
    "form4": "insider",
    "insider": "insider",
    "13f": "institutional",
    "icij": "offshore",
    "panama": "offshore",
    "pandora": "offshore",
    "offshore": "offshore",
    "fec": "campaign_finance",
    "campaign_finance": "campaign_finance",
    "fara": "foreign_lobbying",
    "sanctions": "sanctions",
    "opensanctions": "sanctions",
    "dark_pool": "dark_pool",
    "gdelt": "geopolitical",
    "lobbying": "lobbying",
}

# Domain weights for bridge scoring — rarer domains worth more
DOMAIN_WEIGHTS: dict[str, float] = {
    "congressional": 1.5,
    "insider": 1.0,
    "institutional": 1.0,
    "offshore": 3.0,    # ICIJ Panama/Pandora — very high signal
    "campaign_finance": 1.5,
    "foreign_lobbying": 2.5,
    "sanctions": 3.0,   # sanctioned + trading = huge red flag
    "dark_pool": 1.0,
    "geopolitical": 1.0,
    "lobbying": 1.5,
}


# ══════════════════════════════════════════════════════════════════════════
# PHONETIC ENCODING — Double Metaphone (simplified)
# ══════════════════════════════════════════════════════════════════════════

def _soundex(name: str) -> str:
    """American Soundex encoding for a single name component."""
    name = name.upper().strip()
    if not name:
        return ""

    # Soundex coding table
    _TABLE = {
        "B": "1", "F": "1", "P": "1", "V": "1",
        "C": "2", "G": "2", "J": "2", "K": "2", "Q": "2", "S": "2",
        "X": "2", "Z": "2",
        "D": "3", "T": "3",
        "L": "4",
        "M": "5", "N": "5",
        "R": "6",
    }

    result = [name[0]]
    prev = _TABLE.get(name[0], "0")

    for ch in name[1:]:
        code = _TABLE.get(ch, "0")
        if code != "0" and code != prev:
            result.append(code)
        prev = code if code != "0" else prev

    return "".join(result).ljust(4, "0")[:4]


def phonetic_key(name: str) -> str:
    """Generate a phonetic key for fuzzy matching.

    Uses Soundex on each name component, joined with dashes.
    """
    parts = name.split()
    if not parts:
        return ""
    return "-".join(_soundex(p) for p in parts if len(p) > 1)


# ══════════════════════════════════════════════════════════════════════════
# STRING SIMILARITY
# ══════════════════════════════════════════════════════════════════════════

def levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if not s2:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            cost = 0 if c1 == c2 else 1
            curr_row.append(min(
                curr_row[j] + 1,           # insert
                prev_row[j + 1] + 1,       # delete
                prev_row[j] + cost,        # substitute
            ))
        prev_row = curr_row

    return prev_row[-1]


def jaro_similarity(s1: str, s2: str) -> float:
    """Compute Jaro similarity between two strings (0.0 to 1.0)."""
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    len1, len2 = len(s1), len(s2)
    match_window = max(len1, len2) // 2 - 1
    if match_window < 0:
        match_window = 0

    s1_matches = [False] * len1
    s2_matches = [False] * len2

    matches = 0
    transpositions = 0

    for i in range(len1):
        start = max(0, i - match_window)
        end = min(i + match_window + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    return (matches / len1 + matches / len2 +
            (matches - transpositions / 2) / matches) / 3


def jaro_winkler_similarity(s1: str, s2: str, prefix_weight: float = 0.1) -> float:
    """Jaro-Winkler similarity — boosts score for matching prefixes."""
    jaro = jaro_similarity(s1, s2)

    # Common prefix (max 4 chars)
    prefix_len = 0
    for i in range(min(4, len(s1), len(s2))):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break

    return jaro + prefix_len * prefix_weight * (1 - jaro)


def name_similarity(name1: str, name2: str) -> float:
    """Combined similarity score optimized for name matching.

    Normalizes both names first (strip titles, reorder LAST/FIRST, etc.),
    then uses Jaro-Winkler + Levenshtein. Also checks canonical key
    equality as a strong match signal.
    """
    # Canonical key match = definite match
    k1 = canonical_key(name1)
    k2 = canonical_key(name2)
    if k1 and k2 and k1 == k2:
        return 1.0

    # Normalize before comparing
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)

    # Compare normalized forms
    jw = jaro_winkler_similarity(n1, n2)
    max_len = max(len(n1), len(n2))
    if max_len == 0:
        return 1.0
    lev_norm = 1.0 - levenshtein_distance(n1, n2) / max_len

    # Also compare raw forms (catches cases where normalization hurts)
    jw_raw = jaro_winkler_similarity(name1, name2)

    return max(jw, lev_norm, jw_raw)


# ══════════════════════════════════════════════════════════════════════════
# NAME NORMALIZATION
# ══════════════════════════════════════════════════════════════════════════

def strip_accents(s: str) -> str:
    """Remove unicode accents: e.g. 'Müller' -> 'Muller'."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_name(raw: str, entity_type: str = "person") -> str:
    """Normalize a raw name string to canonical form.

    Handles:
        - Unicode/accent normalization
        - "LAST, FIRST MIDDLE" -> "First Middle Last" reordering
        - Title stripping (Mr., Dr., Sen., etc.)
        - Suffix stripping (Jr., Sr., III, etc.)
        - Extra whitespace cleanup
        - Company suffix normalization

    Args:
        raw: Raw name from any source.
        entity_type: "person" or "company".

    Returns:
        Canonical name in Title Case with no titles/suffixes.
    """
    if not raw or not raw.strip():
        return ""

    name = raw.strip()

    # Strip unicode accents
    name = strip_accents(name)

    # Remove non-alphanumeric except spaces, commas, hyphens, apostrophes, periods
    name = re.sub(r"[^\w\s,\-'.&]", " ", name)

    if entity_type == "person":
        # Strip titles
        name = _PERSON_TITLES.sub("", name)
        # Strip suffixes
        name = _PERSON_SUFFIXES.sub("", name)

        # Handle "LAST, FIRST [MIDDLE]" format
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            if len(parts) == 2 and parts[0] and parts[1]:
                last = parts[0]
                first_middle = parts[1]
                # Only reorder if the first part looks like a last name
                # (all caps or single word before comma)
                name = f"{first_middle} {last}"

    elif entity_type == "company":
        name = _COMPANY_SUFFIXES.sub("", name)

    # Title case
    name = name.strip().title()

    # Fix possessives and particles
    name = re.sub(r"\bMc(\w)", lambda m: f"Mc{m.group(1).upper()}", name)
    name = re.sub(r"\bO'(\w)", lambda m: f"O'{m.group(1).upper()}", name)
    name = re.sub(r"\bMac(\w)", lambda m: f"Mac{m.group(1).upper()}", name)

    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()

    return name


def canonical_key(name: str) -> str:
    """Generate a deterministic canonical key for an entity name.

    Lowercase, alphabetically sorted tokens, no punctuation.
    "David A Perdue" and "Perdue David A" -> "a david perdue"
    """
    cleaned = re.sub(r"[^\w\s]", "", name.lower())
    tokens = sorted(cleaned.split())
    return " ".join(tokens)


def entity_id(name: str, entity_type: str = "person") -> str:
    """Generate a stable ID for a canonical entity."""
    normalized = normalize_name(name, entity_type)
    key = canonical_key(normalized)
    hash_val = hashlib.sha256(f"{entity_type}:{key}".encode()).hexdigest()[:12]
    slug = re.sub(r"[^a-z0-9]", "_", key)[:40]
    return f"er_{slug}_{hash_val}"


# ══════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class ResolvedEntity:
    """A resolved entity with all known aliases and sources."""

    canonical_id: str
    canonical_name: str
    entity_type: str = "person"
    aliases: list[str] = field(default_factory=list)
    sources: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    bridge_score: float = 0.0
    first_seen: datetime | None = None
    last_seen: datetime | None = None

    @property
    def domain_count(self) -> int:
        """Number of distinct intelligence domains this entity spans."""
        domains = set()
        for source_key in self.sources:
            domain = SOURCE_DOMAINS.get(source_key, source_key)
            domains.add(domain)
        return len(domains)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["domain_count"] = self.domain_count
        if d["first_seen"]:
            d["first_seen"] = d["first_seen"].isoformat()
        if d["last_seen"]:
            d["last_seen"] = d["last_seen"].isoformat()
        return d


# ══════════════════════════════════════════════════════════════════════════
# ENTITY RESOLVER
# ══════════════════════════════════════════════════════════════════════════

class EntityResolver:
    """Cross-source entity resolution engine.

    Scans actors, analytical_snapshots, oracle_predictions, signal_data,
    entity_relationships, and wealth_flows to find the same entity
    appearing under different names, then clusters them into a single
    canonical record.
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self._ensure_tables()

    # ── Schema ────────────────────────────────────────────────────────

    def _ensure_tables(self) -> None:
        """Create the entity_resolution table if it does not exist."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS entity_resolution (
                    canonical_id    TEXT PRIMARY KEY,
                    canonical_name  TEXT NOT NULL,
                    entity_type     TEXT DEFAULT 'person',
                    aliases         JSONB DEFAULT '[]',
                    sources         JSONB DEFAULT '{}',
                    first_seen      TIMESTAMPTZ,
                    last_seen       TIMESTAMPTZ,
                    bridge_score    DOUBLE PRECISION DEFAULT 0,
                    created_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_er_canonical_name
                    ON entity_resolution (canonical_name)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_er_bridge_score
                    ON entity_resolution (bridge_score DESC)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_er_entity_type
                    ON entity_resolution (entity_type)
            """))
        log.info("entity_resolution table ensured")

    # ── Resolve a single name ────────────────────────────────────────

    def resolve(self, name: str, entity_type: str = "person") -> ResolvedEntity:
        """Find all records across all tables matching this entity.

        1. Normalize the input name
        2. Check entity_resolution cache
        3. If not cached, search all source tables with fuzzy matching
        4. Return a ResolvedEntity with all aliases and source records

        Args:
            name: Raw name string (any format).
            entity_type: "person" or "company".

        Returns:
            ResolvedEntity with canonical_name, all_aliases, sources, confidence.
        """
        normalized = normalize_name(name, entity_type)
        can_key = canonical_key(normalized)
        eid = entity_id(name, entity_type)

        # Check cache first
        cached = self._load_cached(eid)
        if cached:
            return cached

        # Search all source tables
        aliases: set[str] = {name.strip(), normalized}
        sources: dict[str, list[dict[str, Any]]] = {}
        first_seen: datetime | None = None
        last_seen: datetime | None = None

        # Search each source table
        for table_name, search_fn in self._source_searchers():
            try:
                hits = search_fn(normalized, can_key, entity_type)
                if hits:
                    sources[table_name] = hits
                    for hit in hits:
                        if hit.get("raw_name"):
                            aliases.add(hit["raw_name"])
                        ts = hit.get("timestamp")
                        if ts:
                            if first_seen is None or ts < first_seen:
                                first_seen = ts
                            if last_seen is None or ts > last_seen:
                                last_seen = ts
            except Exception as e:
                log.warning("Error searching {t}: {e}", t=table_name, e=e)

        bridge = self._compute_bridge_score(sources)

        entity = ResolvedEntity(
            canonical_id=eid,
            canonical_name=normalized,
            entity_type=entity_type,
            aliases=sorted(aliases),
            sources=sources,
            bridge_score=bridge,
            first_seen=first_seen,
            last_seen=last_seen,
        )

        # Persist to cache
        self._persist_entity(entity)

        return entity

    def _source_searchers(self) -> list[tuple[str, Any]]:
        """Return list of (source_name, search_function) tuples."""
        return [
            ("actors", self._search_actors),
            ("analytical_snapshots", self._search_snapshots),
            ("signal_data", self._search_signals),
            ("oracle_predictions", self._search_predictions),
            ("entity_relationships", self._search_relationships),
            ("wealth_flows", self._search_wealth_flows),
        ]

    def _search_actors(
        self, normalized: str, can_key: str, entity_type: str
    ) -> list[dict[str, Any]]:
        """Search actors table for matching entities."""
        results = []
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, name, tier, category, metadata,
                       updated_at, data_sources
                FROM actors
                WHERE lower(name) = lower(:name)
                   OR lower(name) LIKE :pattern
            """), {
                "name": normalized,
                "pattern": f"%{can_key.split()[0] if can_key.split() else ''}%",
            }).fetchall()

            for row in rows:
                row_name = row[1] if row[1] else ""
                sim = name_similarity(
                    canonical_key(normalize_name(row_name)),
                    can_key,
                )
                if sim >= LIKELY_MATCH_THRESHOLD:
                    results.append({
                        "raw_name": row_name,
                        "actor_id": row[0],
                        "tier": row[2],
                        "category": row[3],
                        "similarity": round(sim, 3),
                        "timestamp": row[5] if row[5] else None,
                        "source": "actors",
                    })
        return results

    def _search_snapshots(
        self, normalized: str, can_key: str, entity_type: str
    ) -> list[dict[str, Any]]:
        """Search analytical_snapshots for matching actor names."""
        results = []
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, category, actor, snapshot_date, title,
                       source_id, created_at
                FROM analytical_snapshots
                WHERE actor IS NOT NULL
                  AND (lower(actor) = lower(:name)
                       OR lower(actor) LIKE :pattern)
                LIMIT 200
            """), {
                "name": normalized,
                "pattern": f"%{can_key.split()[0] if can_key.split() else ''}%",
            }).fetchall()

            for row in rows:
                actor_name = row[2] if row[2] else ""
                sim = name_similarity(
                    canonical_key(normalize_name(actor_name)),
                    can_key,
                )
                if sim >= LIKELY_MATCH_THRESHOLD:
                    source_id = row[5] or ""
                    domain = _guess_domain_from_source_id(source_id, row[1])
                    results.append({
                        "raw_name": actor_name,
                        "snapshot_id": row[0],
                        "category": row[1],
                        "snapshot_date": str(row[3]) if row[3] else None,
                        "title": row[4],
                        "similarity": round(sim, 3),
                        "timestamp": row[6] if row[6] else None,
                        "source": domain,
                    })
        return results

    def _search_signals(
        self, normalized: str, can_key: str, entity_type: str
    ) -> list[dict[str, Any]]:
        """Search signal_data for matching actor names."""
        results = []
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, signal_type, signal_date, actor, ticker,
                       direction, magnitude, source_id, created_at
                FROM signal_data
                WHERE actor IS NOT NULL
                  AND (lower(actor) = lower(:name)
                       OR lower(actor) LIKE :pattern)
                LIMIT 200
            """), {
                "name": normalized,
                "pattern": f"%{can_key.split()[0] if can_key.split() else ''}%",
            }).fetchall()

            for row in rows:
                actor_name = row[3] if row[3] else ""
                sim = name_similarity(
                    canonical_key(normalize_name(actor_name)),
                    can_key,
                )
                if sim >= LIKELY_MATCH_THRESHOLD:
                    results.append({
                        "raw_name": actor_name,
                        "signal_id": row[0],
                        "signal_type": row[1],
                        "signal_date": str(row[2]) if row[2] else None,
                        "ticker": row[4],
                        "direction": row[5],
                        "magnitude": row[6],
                        "similarity": round(sim, 3),
                        "timestamp": row[8] if row[8] else None,
                        "source": SOURCE_DOMAINS.get(row[1] or "", row[1] or "unknown"),
                    })
        return results

    def _search_predictions(
        self, normalized: str, can_key: str, entity_type: str
    ) -> list[dict[str, Any]]:
        """Search oracle_predictions for matching signals that reference this entity."""
        results = []
        with self.engine.connect() as conn:
            # oracle_predictions stores signals as JSONB — search within
            try:
                rows = conn.execute(text("""
                    SELECT id, ticker, prediction_type, direction,
                           confidence, created_at, signals
                    FROM oracle_predictions
                    WHERE signals::text ILIKE :pattern
                    LIMIT 100
                """), {
                    "pattern": f"%{normalized}%",
                }).fetchall()

                for row in rows:
                    results.append({
                        "raw_name": normalized,
                        "prediction_id": row[0],
                        "ticker": row[1],
                        "prediction_type": row[2],
                        "direction": row[3],
                        "confidence": row[4],
                        "similarity": 1.0,
                        "timestamp": row[5] if row[5] else None,
                        "source": "oracle",
                    })
            except Exception:
                pass  # Table may not exist yet
        return results

    def _search_relationships(
        self, normalized: str, can_key: str, entity_type: str
    ) -> list[dict[str, Any]]:
        """Search entity_relationships for this entity on either side."""
        results = []
        with self.engine.connect() as conn:
            try:
                rows = conn.execute(text("""
                    SELECT id, actor_a, actor_b, relationship,
                           strength, source_id, created_at
                    FROM entity_relationships
                    WHERE lower(actor_a) = lower(:name)
                       OR lower(actor_b) = lower(:name)
                       OR lower(actor_a) LIKE :pattern
                       OR lower(actor_b) LIKE :pattern
                    LIMIT 200
                """), {
                    "name": normalized,
                    "pattern": f"%{can_key.split()[0] if can_key.split() else ''}%",
                }).fetchall()

                for row in rows:
                    # Check which side matches
                    a_name = row[1] or ""
                    b_name = row[2] or ""
                    sim_a = name_similarity(canonical_key(normalize_name(a_name)), can_key)
                    sim_b = name_similarity(canonical_key(normalize_name(b_name)), can_key)
                    best_sim = max(sim_a, sim_b)

                    if best_sim >= LIKELY_MATCH_THRESHOLD:
                        matched_name = a_name if sim_a >= sim_b else b_name
                        connected_to = b_name if sim_a >= sim_b else a_name
                        results.append({
                            "raw_name": matched_name,
                            "connected_to": connected_to,
                            "relationship": row[3],
                            "strength": row[4],
                            "similarity": round(best_sim, 3),
                            "timestamp": row[6] if row[6] else None,
                            "source": "entity_relationships",
                        })
            except Exception:
                pass  # Table may not exist
        return results

    def _search_wealth_flows(
        self, normalized: str, can_key: str, entity_type: str
    ) -> list[dict[str, Any]]:
        """Search wealth_flows for matching from_actor."""
        results = []
        with self.engine.connect() as conn:
            try:
                rows = conn.execute(text("""
                    SELECT wf.id, wf.from_actor, wf.to_entity,
                           wf.amount_estimate, wf.confidence,
                           wf.flow_date, wf.created_at,
                           a.name
                    FROM wealth_flows wf
                    LEFT JOIN actors a ON wf.from_actor = a.id
                    WHERE lower(a.name) = lower(:name)
                       OR lower(wf.to_entity) = lower(:name)
                       OR lower(a.name) LIKE :pattern
                       OR lower(wf.to_entity) LIKE :pattern
                    LIMIT 200
                """), {
                    "name": normalized,
                    "pattern": f"%{can_key.split()[0] if can_key.split() else ''}%",
                }).fetchall()

                for row in rows:
                    actor_name = row[7] or row[1] or ""
                    sim = name_similarity(
                        canonical_key(normalize_name(actor_name)),
                        can_key,
                    )
                    if sim >= LIKELY_MATCH_THRESHOLD:
                        results.append({
                            "raw_name": actor_name,
                            "to_entity": row[2],
                            "amount_estimate": float(row[3]) if row[3] else None,
                            "flow_date": str(row[5]) if row[5] else None,
                            "similarity": round(sim, 3),
                            "timestamp": row[6] if row[6] else None,
                            "source": "wealth_flows",
                        })
            except Exception:
                pass  # Table may not exist
        return results

    # ── Bridge scoring ───────────────────────────────────────────────

    def _compute_bridge_score(self, sources: dict[str, list]) -> float:
        """Compute bridge score based on how many distinct domains an entity spans.

        Higher scores mean the entity bridges more intelligence domains.
        An entity in both senate trades AND ICIJ scores much higher than
        one that only appears in dark pool data.
        """
        domains_seen: dict[str, int] = defaultdict(int)

        for source_key, records in sources.items():
            for record in records:
                domain = SOURCE_DOMAINS.get(
                    record.get("source", source_key),
                    record.get("source", source_key),
                )
                domains_seen[domain] += 1

        if not domains_seen:
            return 0.0

        # Base score: weighted sum of domains
        score = 0.0
        for domain, count in domains_seen.items():
            weight = DOMAIN_WEIGHTS.get(domain, 1.0)
            # Diminishing returns for more records in same domain
            score += weight * (1 + 0.1 * min(count - 1, 10))

        # Multiplicative bonus for spanning multiple domains
        num_domains = len(domains_seen)
        if num_domains >= 2:
            score *= 1.0 + 0.5 * (num_domains - 1)

        return round(score, 2)

    # ── Cache / persistence ──────────────────────────────────────────

    def _load_cached(self, canonical_id: str) -> ResolvedEntity | None:
        """Load a resolved entity from the entity_resolution table."""
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT canonical_id, canonical_name, entity_type,
                       aliases, sources, first_seen, last_seen,
                       bridge_score
                FROM entity_resolution
                WHERE canonical_id = :cid
            """), {"cid": canonical_id}).fetchone()

        if not row:
            return None

        return ResolvedEntity(
            canonical_id=row[0],
            canonical_name=row[1],
            entity_type=row[2],
            aliases=row[3] if isinstance(row[3], list) else json.loads(row[3] or "[]"),
            sources=row[4] if isinstance(row[4], dict) else json.loads(row[4] or "{}"),
            bridge_score=float(row[7]) if row[7] else 0.0,
            first_seen=row[5],
            last_seen=row[6],
        )

    def _persist_entity(self, entity: ResolvedEntity) -> None:
        """Upsert a resolved entity into the entity_resolution table."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO entity_resolution
                    (canonical_id, canonical_name, entity_type,
                     aliases, sources, first_seen, last_seen,
                     bridge_score, created_at)
                VALUES
                    (:cid, :cname, :etype,
                     :aliases::jsonb, :sources::jsonb,
                     :first_seen, :last_seen,
                     :bridge, NOW())
                ON CONFLICT (canonical_id) DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name,
                    aliases = EXCLUDED.aliases,
                    sources = EXCLUDED.sources,
                    first_seen = EXCLUDED.first_seen,
                    last_seen = EXCLUDED.last_seen,
                    bridge_score = EXCLUDED.bridge_score
            """), {
                "cid": entity.canonical_id,
                "cname": entity.canonical_name,
                "etype": entity.entity_type,
                "aliases": json.dumps(entity.aliases),
                "sources": json.dumps(entity.sources, default=str),
                "first_seen": entity.first_seen,
                "last_seen": entity.last_seen,
                "bridge": entity.bridge_score,
            })

    # ── Build resolution index ───────────────────────────────────────

    def build_resolution_index(self) -> dict[str, Any]:
        """Scan all data sources and build the full resolution table.

        For each unique name across all tables:
            1. Normalize the name
            2. Generate canonical key
            3. Cluster near-matches using fuzzy similarity
            4. Persist to entity_resolution table

        Returns:
            Summary statistics of the index build.
        """
        log.info("Building entity resolution index...")
        all_names: dict[str, set[str]] = defaultdict(set)  # source -> {names}

        with self.engine.connect() as conn:
            # Actors
            try:
                rows = conn.execute(text(
                    "SELECT DISTINCT name FROM actors WHERE name IS NOT NULL"
                )).fetchall()
                for row in rows:
                    all_names["actors"].add(row[0])
                log.info("Actors: {n} unique names", n=len(all_names["actors"]))
            except Exception as e:
                log.warning("Could not scan actors: {e}", e=e)

            # Analytical snapshots
            try:
                rows = conn.execute(text(
                    "SELECT DISTINCT actor FROM analytical_snapshots "
                    "WHERE actor IS NOT NULL"
                )).fetchall()
                for row in rows:
                    all_names["analytical_snapshots"].add(row[0])
                log.info("Snapshots: {n} unique actors",
                         n=len(all_names["analytical_snapshots"]))
            except Exception as e:
                log.warning("Could not scan snapshots: {e}", e=e)

            # Signal data
            try:
                rows = conn.execute(text(
                    "SELECT DISTINCT actor FROM signal_data "
                    "WHERE actor IS NOT NULL"
                )).fetchall()
                for row in rows:
                    all_names["signal_data"].add(row[0])
                log.info("Signals: {n} unique actors",
                         n=len(all_names["signal_data"]))
            except Exception as e:
                log.warning("Could not scan signals: {e}", e=e)

            # Entity relationships (both sides)
            try:
                rows = conn.execute(text(
                    "SELECT DISTINCT actor_a FROM entity_relationships "
                    "WHERE actor_a IS NOT NULL "
                    "UNION "
                    "SELECT DISTINCT actor_b FROM entity_relationships "
                    "WHERE actor_b IS NOT NULL"
                )).fetchall()
                for row in rows:
                    all_names["entity_relationships"].add(row[0])
                log.info("Relationships: {n} unique entities",
                         n=len(all_names["entity_relationships"]))
            except Exception as e:
                log.warning("Could not scan relationships: {e}", e=e)

        # Flatten and cluster
        name_to_sources: dict[str, set[str]] = defaultdict(set)
        for source, names in all_names.items():
            for n in names:
                name_to_sources[n].add(source)

        total_names = len(name_to_sources)
        log.info("Total unique raw names: {n}", n=total_names)

        # Build canonical clusters
        # Key: canonical_key -> list of (raw_name, sources)
        clusters: dict[str, list[tuple[str, set[str]]]] = defaultdict(list)
        for raw_name, srcs in name_to_sources.items():
            normalized = normalize_name(raw_name)
            key = canonical_key(normalized)
            clusters[key].append((raw_name, srcs))

        # Merge clusters with similar canonical keys using phonetic matching
        merged = self._merge_clusters_phonetic(clusters)

        # Persist each cluster
        resolved_count = 0
        bridge_count = 0
        for key, members in merged.items():
            # Pick the best canonical name (most common normalized form)
            name_counts: dict[str, int] = defaultdict(int)
            all_aliases: set[str] = set()
            all_sources: set[str] = set()
            for raw_name, srcs in members:
                norm = normalize_name(raw_name)
                name_counts[norm] += 1
                all_aliases.add(raw_name)
                all_aliases.add(norm)
                all_sources.update(srcs)

            best_name = max(name_counts, key=name_counts.get)  # type: ignore[arg-type]
            eid = entity_id(best_name)

            # Build sources dict for bridge scoring
            source_dict: dict[str, list[dict[str, Any]]] = {}
            for src in all_sources:
                source_dict[src] = [{"source": src, "raw_name": raw_name}
                                    for raw_name, srcs in members if src in srcs]

            bridge = self._compute_bridge_score(source_dict)

            entity = ResolvedEntity(
                canonical_id=eid,
                canonical_name=best_name,
                entity_type="person",
                aliases=sorted(all_aliases),
                sources=source_dict,
                bridge_score=bridge,
            )

            self._persist_entity(entity)
            resolved_count += 1
            if bridge > 0 and len(all_sources) > 1:
                bridge_count += 1

        stats = {
            "total_raw_names": total_names,
            "canonical_clusters": len(merged),
            "resolved_entities": resolved_count,
            "bridge_entities": bridge_count,
        }
        log.info("Resolution index built: {s}", s=stats)
        return stats

    def _merge_clusters_phonetic(
        self, clusters: dict[str, list[tuple[str, set[str]]]]
    ) -> dict[str, list[tuple[str, set[str]]]]:
        """Merge clusters whose canonical keys are phonetically similar.

        This catches cases like "John Smith" vs "Jon Smith" that have
        different canonical keys but sound the same.
        """
        keys = list(clusters.keys())
        merged: dict[str, list[tuple[str, set[str]]]] = {}
        absorbed: set[str] = set()

        # Build phonetic index
        phonetic_index: dict[str, list[str]] = defaultdict(list)
        for key in keys:
            # Use the first name from the cluster for phonetic key
            first_name = clusters[key][0][0]
            pkey = phonetic_key(normalize_name(first_name))
            phonetic_index[pkey].append(key)

        # Merge clusters with the same phonetic key and high similarity
        for pkey, group_keys in phonetic_index.items():
            if len(group_keys) <= 1:
                for k in group_keys:
                    if k not in absorbed:
                        merged[k] = clusters[k]
                continue

            # Check pairwise similarity within phonetic group
            primary = group_keys[0]
            if primary in absorbed:
                continue
            merged_members = list(clusters[primary])

            for other in group_keys[1:]:
                if other in absorbed:
                    continue
                sim = name_similarity(primary, other)
                if sim >= LIKELY_MATCH_THRESHOLD:
                    merged_members.extend(clusters[other])
                    absorbed.add(other)
                else:
                    merged[other] = clusters[other]

            merged[primary] = merged_members

        # Add any remaining unabsorbed
        for key in keys:
            if key not in absorbed and key not in merged:
                merged[key] = clusters[key]

        return merged

    # ── Multi-hop connections ────────────────────────────────────────

    def find_connections(self, canonical_id: str, depth: int = 2) -> dict[str, Any]:
        """Multi-hop graph traversal from a resolved entity.

        Hop 1: Direct connections (same person in different datasets,
               entity_relationships edges).
        Hop 2+: Entities connected to those (shared board seats,
                shared offshore entities, co-investors).

        Args:
            canonical_id: ID from entity_resolution table.
            depth: Number of hops (default 2, max 3).

        Returns:
            Graph structure with nodes and edges for visualization.
        """
        depth = min(depth, 3)  # Safety cap

        # Load the seed entity
        seed = self._load_cached(canonical_id)
        if not seed:
            return {"error": f"Entity {canonical_id} not found", "nodes": [], "edges": []}

        nodes: dict[str, dict[str, Any]] = {
            canonical_id: {
                "id": canonical_id,
                "name": seed.canonical_name,
                "type": seed.entity_type,
                "bridge_score": seed.bridge_score,
                "depth": 0,
            }
        }
        edges: list[dict[str, Any]] = []
        visited: set[str] = {canonical_id}
        frontier: list[str] = [canonical_id]

        for hop in range(1, depth + 1):
            next_frontier: list[str] = []

            for current_id in frontier:
                current = self._load_cached(current_id)
                if not current:
                    continue

                # Find connections from entity_relationships
                connected = self._get_direct_connections(current.canonical_name)
                for conn_name, rel_type, strength in connected:
                    conn_entity = self.resolve(conn_name)
                    if conn_entity.canonical_id in visited:
                        # Still add the edge
                        edges.append({
                            "source": current_id,
                            "target": conn_entity.canonical_id,
                            "relationship": rel_type,
                            "strength": strength,
                        })
                        continue

                    visited.add(conn_entity.canonical_id)
                    nodes[conn_entity.canonical_id] = {
                        "id": conn_entity.canonical_id,
                        "name": conn_entity.canonical_name,
                        "type": conn_entity.entity_type,
                        "bridge_score": conn_entity.bridge_score,
                        "depth": hop,
                    }
                    edges.append({
                        "source": current_id,
                        "target": conn_entity.canonical_id,
                        "relationship": rel_type,
                        "strength": strength,
                    })
                    next_frontier.append(conn_entity.canonical_id)

            frontier = next_frontier

        return {
            "seed": seed.canonical_name,
            "depth": depth,
            "nodes": list(nodes.values()),
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges),
        }

    def _get_direct_connections(
        self, name: str
    ) -> list[tuple[str, str, float]]:
        """Get direct connections for a name from entity_relationships."""
        results: list[tuple[str, str, float]] = []
        normalized = normalize_name(name)

        with self.engine.connect() as conn:
            try:
                rows = conn.execute(text("""
                    SELECT actor_a, actor_b, relationship, strength
                    FROM entity_relationships
                    WHERE lower(actor_a) = lower(:name)
                       OR lower(actor_b) = lower(:name)
                    LIMIT 100
                """), {"name": normalized}).fetchall()

                for row in rows:
                    a, b = row[0], row[1]
                    # The connected entity is the other side
                    connected = b if normalize_name(a).lower() == normalized.lower() else a
                    results.append((connected, row[2], float(row[3] or 0.5)))
            except Exception as exc:
                log.warning("Entity connection query failed: {e}", e=exc)

        return results

    # ── Bridge discovery ─────────────────────────────────────────────

    def discover_bridges(self, min_score: float = 2.0, limit: int = 100) -> list[dict[str, Any]]:
        """Find entities that bridge multiple intelligence domains.

        A bridge entity appears in multiple distinct data domains:
        - Person in BOTH senate trades AND ICIJ = high-value bridge
        - Person in BOTH FEC donations AND sanctions = critical bridge
        - These are the gems — cross-domain connections that reveal
          hidden relationships.

        Args:
            min_score: Minimum bridge score to include.
            limit: Maximum results.

        Returns:
            List of bridge entities sorted by bridge_score descending.
        """
        bridges: list[dict[str, Any]] = []

        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT canonical_id, canonical_name, entity_type,
                       aliases, sources, bridge_score
                FROM entity_resolution
                WHERE bridge_score >= :min_score
                ORDER BY bridge_score DESC
                LIMIT :lim
            """), {"min_score": min_score, "lim": limit}).fetchall()

            for row in rows:
                sources = row[4] if isinstance(row[4], dict) else json.loads(row[4] or "{}")
                aliases = row[3] if isinstance(row[3], list) else json.loads(row[3] or "[]")

                # Compute domain breakdown
                domains: dict[str, int] = defaultdict(int)
                for src_key, records in sources.items():
                    domain = SOURCE_DOMAINS.get(src_key, src_key)
                    domains[domain] += len(records) if isinstance(records, list) else 1

                bridges.append({
                    "canonical_id": row[0],
                    "canonical_name": row[1],
                    "entity_type": row[2],
                    "alias_count": len(aliases),
                    "bridge_score": float(row[5]),
                    "domains": dict(domains),
                    "domain_count": len(domains),
                })

        log.info("Found {n} bridge entities (min_score={s})",
                 n=len(bridges), s=min_score)
        return bridges

    # ── Statistics ────────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
        """Return resolution index statistics."""
        with self.engine.connect() as conn:
            total = conn.execute(text(
                "SELECT COUNT(*) FROM entity_resolution"
            )).scalar() or 0

            bridges = conn.execute(text(
                "SELECT COUNT(*) FROM entity_resolution WHERE bridge_score > 0"
            )).scalar() or 0

            high_bridges = conn.execute(text(
                "SELECT COUNT(*) FROM entity_resolution WHERE bridge_score >= 5"
            )).scalar() or 0

            avg_aliases = conn.execute(text(
                "SELECT AVG(jsonb_array_length(aliases)) FROM entity_resolution"
            )).scalar() or 0

            top_bridge = conn.execute(text("""
                SELECT canonical_name, bridge_score
                FROM entity_resolution
                ORDER BY bridge_score DESC
                LIMIT 1
            """)).fetchone()

            # Domain distribution
            type_counts = conn.execute(text("""
                SELECT entity_type, COUNT(*)
                FROM entity_resolution
                GROUP BY entity_type
            """)).fetchall()

        return {
            "total_resolved_entities": total,
            "entities_with_bridges": bridges,
            "high_value_bridges": high_bridges,
            "avg_aliases_per_entity": round(float(avg_aliases), 1),
            "top_bridge": {
                "name": top_bridge[0] if top_bridge else None,
                "score": float(top_bridge[1]) if top_bridge else 0,
            },
            "entity_types": {row[0]: row[1] for row in type_counts},
        }


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

def _guess_domain_from_source_id(source_id: str, category: str) -> str:
    """Infer the intelligence domain from a source_id or category string."""
    s = (source_id or "").lower() + " " + (category or "").lower()

    if any(kw in s for kw in ("senate", "house", "congress")):
        return "congressional"
    if any(kw in s for kw in ("form4", "insider", "sec_insider")):
        return "insider"
    if any(kw in s for kw in ("13f", "institutional")):
        return "institutional"
    if any(kw in s for kw in ("icij", "panama", "pandora", "offshore")):
        return "offshore"
    if any(kw in s for kw in ("fec", "campaign", "pac", "donation")):
        return "campaign_finance"
    if any(kw in s for kw in ("fara", "foreign_agent")):
        return "foreign_lobbying"
    if any(kw in s for kw in ("sanction", "ofac", "sdn")):
        return "sanctions"
    if any(kw in s for kw in ("dark_pool", "ats", "finra")):
        return "dark_pool"
    if any(kw in s for kw in ("gdelt", "geopolit")):
        return "geopolitical"
    if any(kw in s for kw in ("lobby", "lda")):
        return "lobbying"

    return category or "unknown"


# ══════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════

def _cli():
    """Command-line interface for entity resolution.

    Usage:
        python intelligence/entity_resolver.py resolve "David A Perdue, Jr"
        python intelligence/entity_resolver.py build-index
        python intelligence/entity_resolver.py find-bridges
        python intelligence/entity_resolver.py find-bridges --min-score 5.0
        python intelligence/entity_resolver.py stats
        python intelligence/entity_resolver.py normalize "PERDUE, DAVID A"
    """
    import sys
    # Add project root to path
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from db import get_engine

    args = sys.argv[1:]
    if not args:
        print(__doc__)
        print(_cli.__doc__)
        return

    cmd = args[0].lower()

    if cmd == "normalize":
        if len(args) < 2:
            print("Usage: entity_resolver.py normalize <name> [person|company]")
            return
        name = args[1]
        etype = args[2] if len(args) > 2 else "person"
        normalized = normalize_name(name, etype)
        key = canonical_key(normalized)
        pkey = phonetic_key(normalized)
        eid = entity_id(name, etype)
        print(f"  Raw:       {name}")
        print(f"  Canonical: {normalized}")
        print(f"  Key:       {key}")
        print(f"  Phonetic:  {pkey}")
        print(f"  Entity ID: {eid}")
        return

    # Commands that need the database
    engine = get_engine()
    resolver = EntityResolver(engine)

    if cmd == "resolve":
        if len(args) < 2:
            print("Usage: entity_resolver.py resolve <name> [person|company]")
            return
        name = args[1]
        etype = args[2] if len(args) > 2 else "person"
        entity = resolver.resolve(name, etype)
        print(f"\n  Canonical Name:  {entity.canonical_name}")
        print(f"  Canonical ID:    {entity.canonical_id}")
        print(f"  Entity Type:     {entity.entity_type}")
        print(f"  Bridge Score:    {entity.bridge_score}")
        print(f"  Domain Count:    {entity.domain_count}")
        print(f"  Aliases:         {', '.join(entity.aliases)}")
        print(f"  Sources:")
        for source, records in entity.sources.items():
            print(f"    {source}: {len(records)} record(s)")
            for rec in records[:3]:
                sim = rec.get("similarity", "?")
                raw = rec.get("raw_name", "?")
                print(f"      - {raw} (sim={sim})")
        print()

    elif cmd == "build-index":
        print("Building entity resolution index (this may take a while)...")
        stats = resolver.build_resolution_index()
        print(f"\n  Results:")
        for k, v in stats.items():
            print(f"    {k}: {v}")
        print()

    elif cmd == "find-bridges":
        min_score = 2.0
        for i, arg in enumerate(args[1:], 1):
            if arg == "--min-score" and i + 1 < len(args):
                min_score = float(args[i + 1])

        bridges = resolver.discover_bridges(min_score=min_score)
        print(f"\n  Bridge Entities (min_score={min_score}):")
        print(f"  {'─' * 70}")
        for b in bridges:
            domains = ", ".join(b["domains"].keys())
            print(f"  {b['canonical_name']:<35} score={b['bridge_score']:<8} "
                  f"domains={b['domain_count']}  [{domains}]")
        if not bridges:
            print("  No bridge entities found. Run build-index first.")
        print()

    elif cmd == "stats":
        s = resolver.stats()
        print(f"\n  Entity Resolution Statistics:")
        print(f"  {'─' * 40}")
        print(f"  Total resolved entities:    {s['total_resolved_entities']}")
        print(f"  Entities with bridges:      {s['entities_with_bridges']}")
        print(f"  High-value bridges (>=5):   {s['high_value_bridges']}")
        print(f"  Avg aliases per entity:     {s['avg_aliases_per_entity']}")
        if s["top_bridge"]["name"]:
            print(f"  Top bridge:                 {s['top_bridge']['name']} "
                  f"(score={s['top_bridge']['score']})")
        print(f"  Entity types:               {s['entity_types']}")
        print()

    else:
        print(f"Unknown command: {cmd}")
        print(_cli.__doc__)


if __name__ == "__main__":
    _cli()
