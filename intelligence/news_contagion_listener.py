"""News-driven contagion listener.

Scans ``news_articles`` headlines in a rolling window for shock-worthy
events (bankruptcies, supply halts, recalls, sanctions, commodity spikes,
strikes) and auto-fires ``intelligence.chain_contagion.simulate_contagion``
for every resolved entity, persisting the result to ``contagion_predictions``
with ``source='news_listener'`` and a back-pointer to the triggering row.

Zero hallucination rule
-----------------------
Every triggered shock MUST cite:
  - the exact ``news_articles.id`` that fired,
  - the exact regex pattern that matched,
  - a resolved shock node id (ticker OR supply_chain_node.id).

If any of these is missing the candidate is either skipped
(``reason='unresolved'``) or surfaced via the ``dry_run`` report.

Idempotency
-----------
A contagion_prediction is keyed by ``(trigger_news_id, shock_node,
shock_type)``. ``run_once`` refuses to re-fire a prediction for a
(news_id, shock_node, shock_type) triple that already exists, so the
listener is safe to schedule every 15 minutes.

Public API
----------
    run_once(engine, since_hours=24, dry_run=False, limit=500) -> dict
    scan_news(engine, since_hours=24, limit=500) -> list[Candidate]
    resolve_entity(engine, name) -> str | None
    shock_spec_for(pattern_name) -> tuple[str, float] | None

The module is PURE LIBRARY — no CLI code here. Use
``scripts/run_news_contagion_listener.py`` as the thin entrypoint.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import uuid4

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.chain_contagion import simulate_contagion


_PRODUCER_MODULE = "intelligence.news_contagion_listener"


def _emit_contagion_trigger(
    candidate: "Candidate",
    prediction_id: int | None,
) -> None:
    """Non-fatal SignalFired emit for a triggered news shock (SYNTH-38).

    ``strength`` is the shock magnitude (pattern-level confidence). The
    resolved_node is used as the ticker hint so oracle_signals routes
    the row into signal_sources under ``source_type='news_trigger'``.
    """
    try:
        from contracts.correlation import (
            get_current_correlation_id,
            new_correlation_id,
        )
        from contracts.emit import emit as _emit
        from contracts.schemas import SignalFired
    except Exception as exc:  # pragma: no cover — defensive import guard
        log.debug(
            "news_contagion: contracts import failed: {e}", e=str(exc)
        )
        return

    node = candidate.resolved_node
    if not node:
        return
    try:
        strength = float(candidate.magnitude)
    except (TypeError, ValueError):
        return
    if strength == 0.0:
        return
    strength = max(-1.0, min(1.0, strength))

    try:
        corr_id = get_current_correlation_id() or new_correlation_id()
    except Exception:
        return

    try:
        _emit(
            SignalFired(
                producer_module=_PRODUCER_MODULE,
                correlation_id=corr_id,
                signal_id=uuid4(),
                source=f"news_listener:{candidate.pattern}",
                signal_type="contagion_trigger",
                strength=strength,
                ticker=str(node),
                actor_hint=candidate.raw_entity or None,
                raw_row_ids=(
                    [int(candidate.news_id)] if candidate.news_id else []
                ),
            )
        )
    except Exception as exc:  # non-fatal per SYNTH-C contract
        log.debug(
            "news_contagion emit failed for news_id={n}: {e}",
            n=candidate.news_id, e=str(exc),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Pattern catalog
# ─────────────────────────────────────────────────────────────────────────────
#
# Each entry is (pattern_name, compiled_regex, shock_type, magnitude).
# ``shock_type`` must match chain_contagion.VALID_SHOCK_TYPES.
#
# ``{ENTITY}`` placeholder marks where the captured entity name lives so we
# can resolve it downstream. Patterns that just scan for event vocabulary
# without a named entity use group(0) of a companion entity extractor
# (see ``_extract_candidate_entities``).
#
# Magnitudes are deliberately conservative — we would rather run a
# simulation and have it rank nothing than overstate a news blip.

_WORD = r"[A-Z][A-Za-z0-9&.\-]{2,50}(?:\s+[A-Z][A-Za-z0-9&.\-]{0,50}){0,3}"

_RAW_PATTERNS: list[tuple[str, str, str, float]] = [
    # Supplier bankruptcy → heavy supply disruption
    ("bankruptcy", rf"({_WORD})\s+files for bankruptcy", "supply_disruption", 0.70),
    ("bankruptcy", rf"({_WORD})\s+chapter\s*11", "supply_disruption", 0.70),
    ("bankruptcy", rf"({_WORD})\s+insolvency", "supply_disruption", 0.70),

    # Supply disruption — halts, force majeure, strikes, fires
    ("halt_production", rf"({_WORD})\s+halts production", "supply_disruption", 0.40),
    ("halt_production", rf"({_WORD})\s+shuts plant", "supply_disruption", 0.40),
    ("force_majeure", rf"({_WORD}).{{0,30}}force majeure", "supply_disruption", 0.40),
    ("fire_at", rf"fire at ({_WORD})", "supply_disruption", 0.40),
    ("strike_at", rf"strike at ({_WORD})", "supply_disruption", 0.40),
    ("union_strike", rf"({_WORD}).{{0,20}}union strike", "supply_disruption", 0.40),
    ("walkout", rf"({_WORD}).{{0,20}}walkout", "supply_disruption", 0.40),
    ("workers_halt", rf"({_WORD}).{{0,20}}workers halt", "supply_disruption", 0.40),

    # Recalls — FDA, contamination — smaller ripple
    ("recall", rf"({_WORD})\s+recalls", "supply_disruption", 0.20),
    ("fda_warn", rf"FDA warns\s+({_WORD})", "supply_disruption", 0.20),
    ("contamination", rf"({_WORD}).{{0,30}}contamination", "supply_disruption", 0.20),

    # Sanctions / export controls
    ("sanctions", rf"sanctions on ({_WORD})", "supply_disruption", 0.50),
    ("export_ban", rf"({_WORD}).{{0,30}}export ban", "supply_disruption", 0.50),
    ("export_controls", rf"({_WORD}).{{0,30}}export controls", "supply_disruption", 0.50),
    ("entity_list", rf"({_WORD}).{{0,30}}entity list", "supply_disruption", 0.50),
]

# Commodity spike patterns use a fixed vocabulary so we don't ingest
# garbage. Everything in this list must have (or plausibly have) a
# matching supply_chain_nodes id we can seed a shock from.
_COMMODITIES: tuple[str, ...] = (
    "oil", "crude", "gasoline", "natural gas", "lng",
    "copper", "cobalt", "lithium", "nickel", "aluminum", "zinc", "tin",
    "gold", "silver", "platinum", "palladium", "uranium",
    "wheat", "corn", "soybean", "coffee", "sugar", "cocoa", "rice",
    "iron ore", "steel", "rubber", "cotton", "lumber",
    "neon", "helium", "rare earth",
)

_COMMODITY_ALIAS: dict[str, str] = {
    "oil": "oil_crude",
    "crude": "oil_crude",
    "natural gas": "natural_gas",
    "lng": "natural_gas",
    "cocoa": "cocoa_beans",
    "iron ore": "iron_ore",
    "rare earth": "rare_earths",
}

_SKIP_PATTERNS: tuple[tuple[str, str], ...] = (
    # M&A — handled by corporate_actions_parser, not a shock
    ("m_and_a", r"agrees to acquire|takeover bid|to break up"),
    # CEO departures — too diffuse
    ("ceo_departure", r"step down|resigns|ousted"),
)

_SPIKE_VERBS = r"(surges|plunges|soars|spikes|crashes|rockets|collapses)"


@dataclass(frozen=True)
class Candidate:
    """One shock candidate extracted from a news article."""

    news_id: int
    url: str | None
    title: str
    pattern: str
    shock_type: str
    magnitude: float
    raw_entity: str  # the text span matched by the pattern
    resolved_node: str | None = None
    reason: str = ""  # populated when skipped

    def as_dict(self) -> dict[str, Any]:
        return {
            "news_id": self.news_id,
            "url": self.url,
            "title": self.title,
            "pattern": self.pattern,
            "shock_type": self.shock_type,
            "magnitude": self.magnitude,
            "raw_entity": self.raw_entity,
            "resolved_node": self.resolved_node,
            "reason": self.reason,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Compiled pattern objects
# ─────────────────────────────────────────────────────────────────────────────

def _compile(patterns: Iterable[tuple[str, str, str, float]]) -> list[
    tuple[str, re.Pattern[str], str, float]
]:
    return [
        (name, re.compile(rx, re.IGNORECASE), stype, mag)
        for name, rx, stype, mag in patterns
    ]


COMPILED_PATTERNS: list[tuple[str, re.Pattern[str], str, float]] = _compile(_RAW_PATTERNS)

_SKIP_COMPILED: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (name, re.compile(rx, re.IGNORECASE)) for name, rx in _SKIP_PATTERNS
)

_COMMODITY_SPIKE_RE = re.compile(
    rf"({'|'.join(re.escape(c) for c in _COMMODITIES)})\s+{_SPIKE_VERBS}",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Pattern detection
# ─────────────────────────────────────────────────────────────────────────────

def detect_patterns(title: str) -> list[tuple[str, str, str, float]]:
    """Return ``(pattern_name, raw_entity, shock_type, magnitude)`` tuples
    for every pattern that matches ``title``.

    Commodity spike detection is layered on top. Skip-list patterns
    (M&A, CEO departures) suppress everything else — they mean "this is
    out of scope for the listener" so we don't want an entity scan to
    accidentally still fire a supply disruption.
    """
    if not title:
        return []

    # Kill-list first — if any skip pattern hits, drop the article entirely.
    for _name, rx in _SKIP_COMPILED:
        if rx.search(title):
            return []

    hits: list[tuple[str, str, str, float]] = []
    for name, rx, stype, mag in COMPILED_PATTERNS:
        match = rx.search(title)
        if not match:
            continue
        try:
            entity = match.group(1)
        except IndexError:
            entity = match.group(0)
        entity = (entity or "").strip()
        if not entity:
            continue
        hits.append((name, entity, stype, mag))

    # Commodity spike detection.
    cm = _COMMODITY_SPIKE_RE.search(title)
    if cm:
        raw = cm.group(1).lower().strip()
        node = _COMMODITY_ALIAS.get(raw, raw.replace(" ", "_"))
        hits.append(("commodity_spike", node, "price_increase", 0.20))

    return hits


# ─────────────────────────────────────────────────────────────────────────────
# Entity resolution
# ─────────────────────────────────────────────────────────────────────────────

# A slim local alias map that lets us resolve common "Foo, Inc." style
# wrappers without a round-trip to the DB every time.
_STRIP_SUFFIXES = (
    " inc", " inc.", " corp", " corp.", " corporation",
    " llc", " ltd", " ltd.", " plc", " sa", " s.a.", " ag",
    " holdings", " group", " nv", " n.v.", " co", " co.",
)


def _normalize(name: str) -> str:
    s = (name or "").strip().lower()
    for suf in _STRIP_SUFFIXES:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
    # Drop trailing commas / dots so "Foo," matches "Foo".
    return s.rstrip(" ,.")


_SECTOR_MAP_CACHE: dict[str, str] | None = None


def _load_sector_map_index() -> dict[str, str]:
    """Build a name→ticker index from ``analysis.sector_map.SECTOR_MAP``.

    Cached at module level. Returns an empty dict on any import failure
    so resolution gracefully degrades to the supply_chain_nodes lookup.
    """
    global _SECTOR_MAP_CACHE
    if _SECTOR_MAP_CACHE is not None:
        return _SECTOR_MAP_CACHE
    index: dict[str, str] = {}
    try:
        from analysis.sector_map import SECTOR_MAP
    except Exception as exc:
        log.debug("news_contagion: sector_map unavailable: {e}", e=str(exc))
        _SECTOR_MAP_CACHE = {}
        return _SECTOR_MAP_CACHE
    for sector in SECTOR_MAP.values():
        for sub in (sector.get("subsectors") or {}).values():
            for actor in sub.get("actors", []) or []:
                name = actor.get("name")
                ticker = actor.get("ticker")
                if not name or not ticker:
                    continue
                index[_normalize(name)] = ticker.lower()
    _SECTOR_MAP_CACHE = index
    return _SECTOR_MAP_CACHE


def resolve_entity(conn: Any, name: str) -> str | None:
    """Resolve a free-text entity mention to a canonical shock node id.

    Order of resolution:
      1. sector_map name → ticker
      2. supply_chain_nodes.name ILIKE
      3. supply_chain_nodes.id ILIKE (already-normalized mention)
      4. None — skip with ``reason='unresolved'``

    Never raises: DB errors return None.
    """
    norm = _normalize(name)
    if not norm:
        return None

    index = _load_sector_map_index()
    if norm in index:
        return index[norm]

    # Prefix match in sector_map (e.g. "TSMC Arizona" -> "tsmc")
    for key, ticker in index.items():
        if norm.startswith(key) and len(key) >= 3:
            return ticker

    # Supply chain nodes — name match first, then id match.
    try:
        row = conn.execute(
            text(
                """
                SELECT id FROM supply_chain_nodes
                WHERE LOWER(name) = :n
                   OR LOWER(name) LIKE :prefix
                ORDER BY LENGTH(name) ASC
                LIMIT 1
                """
            ),
            {"n": norm, "prefix": f"{norm}%"},
        ).fetchone()
        if row and row[0]:
            return str(row[0]).lower()
    except Exception as exc:
        log.debug("news_contagion: node name lookup failed: {e}", e=str(exc))

    try:
        row = conn.execute(
            text(
                "SELECT id FROM supply_chain_nodes WHERE id = :nid LIMIT 1"
            ),
            {"nid": norm.replace(" ", "_")},
        ).fetchone()
        if row and row[0]:
            return str(row[0]).lower()
    except Exception:
        pass

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Scan + run loop
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_recent_news(
    conn: Any, since_hours: int, limit: int
) -> list[tuple[int, str, str | None, datetime | None]]:
    """Return recent news rows as (id, title, url, published_at).

    Orders by ``COALESCE(published_at, created_at) DESC`` so fresh wires
    get priority when ``limit`` clamps the batch.
    """
    try:
        rows = conn.execute(
            text(
                """
                SELECT id, title, url, COALESCE(published_at, created_at) AS ts
                FROM news_articles
                WHERE COALESCE(published_at, created_at) >= NOW() - (:h || ' hours')::INTERVAL
                ORDER BY ts DESC
                LIMIT :lim
                """
            ),
            {"h": int(since_hours), "lim": int(limit)},
        ).fetchall()
    except Exception as exc:
        log.warning("news_contagion: news fetch failed: {e}", e=str(exc))
        return []
    return [(int(r[0]), r[1] or "", r[2], r[3]) for r in rows]


def scan_news(
    engine: Engine, since_hours: int = 24, limit: int = 500
) -> list[Candidate]:
    """Scan recent news and return every resolved/unresolved candidate.

    Never raises. DB errors return an empty list.
    """
    if since_hours <= 0 or limit <= 0:
        return []

    out: list[Candidate] = []
    try:
        with engine.connect() as conn:
            news = _fetch_recent_news(conn, since_hours, limit)
            for news_id, title, url, _ts in news:
                for pattern, raw_entity, stype, mag in detect_patterns(title):
                    resolved = resolve_entity(conn, raw_entity)
                    candidate = Candidate(
                        news_id=news_id,
                        url=url,
                        title=title,
                        pattern=pattern,
                        shock_type=stype,
                        magnitude=mag,
                        raw_entity=raw_entity,
                        resolved_node=resolved,
                        reason="" if resolved else "unresolved",
                    )
                    out.append(candidate)
    except Exception as exc:
        log.warning("news_contagion: scan failed: {e}", e=str(exc))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Dedup + persistence
# ─────────────────────────────────────────────────────────────────────────────

def _already_triggered(
    conn: Any, news_id: int, shock_node: str, shock_type: str
) -> bool:
    """True if a (news_id, shock_node, shock_type) prediction already exists.

    Uses the ``idx_contagion_predictions_trigger_news_id`` partial index so
    this is a cheap check even when the table grows large.
    """
    try:
        row = conn.execute(
            text(
                """
                SELECT 1 FROM contagion_predictions
                WHERE trigger_news_id = :nid
                  AND shock_node      = :sn
                  AND shock_type      = :st
                LIMIT 1
                """
            ),
            {"nid": int(news_id), "sn": shock_node, "st": shock_type},
        ).fetchone()
    except Exception:
        return False
    return bool(row)


def _persist(
    engine: Engine,
    candidate: Candidate,
    result: dict[str, Any],
) -> int | None:
    """Insert a contagion prediction row. Returns new id or None on failure."""
    summary = result.get("summary") or {}
    ranked = result.get("ranked_impact") or []
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    INSERT INTO contagion_predictions (
                        shock_node, shock_type, magnitude, max_depth,
                        summary, ranked_impact, source, caller_id,
                        trigger_news_id, trigger_url
                    ) VALUES (
                        :shock_node, :shock_type, :magnitude, :max_depth,
                        CAST(:summary AS JSONB), CAST(:ranked AS JSONB),
                        'news_listener', 'news_auto',
                        :trigger_news_id, :trigger_url
                    )
                    RETURNING id
                    """
                ),
                {
                    "shock_node": candidate.resolved_node,
                    "shock_type": candidate.shock_type,
                    "magnitude": float(candidate.magnitude),
                    "max_depth": 4,
                    "summary": json.dumps(summary),
                    "ranked": json.dumps(ranked),
                    "trigger_news_id": int(candidate.news_id),
                    "trigger_url": candidate.url,
                },
            ).fetchone()
        return int(row[0]) if row else None
    except Exception as exc:
        log.warning(
            "news_contagion: persist failed for news_id={n}: {e}",
            n=candidate.news_id,
            e=str(exc),
        )
        return None


def run_once(
    engine: Engine,
    since_hours: int = 24,
    dry_run: bool = False,
    limit: int = 500,
) -> dict[str, Any]:
    """Scan recent news, fire contagion for every resolved shock, persist.

    Returns a structured report suitable for the hermes operator dashboard:

        {
            "scanned_articles": int,
            "candidates_total": int,
            "resolved": int,
            "unresolved": int,
            "fired": int,
            "skipped_duplicate": int,
            "errors": int,
            "by_pattern": {pattern_name: count},
            "predictions": [{news_id, shock_node, pattern, prediction_id}, ...],
            "unresolved_samples": [{news_id, raw_entity, title}, ...],
            "dry_run": bool,
            "generated_at": iso8601,
        }
    """
    started = datetime.now(timezone.utc)
    candidates = scan_news(engine, since_hours=since_hours, limit=limit)

    by_pattern: dict[str, int] = {}
    predictions: list[dict[str, Any]] = []
    unresolved_samples: list[dict[str, Any]] = []
    fired = 0
    skipped_dup = 0
    errors = 0
    resolved_count = 0

    # Dedup within this batch — same (news_id, shock_node, shock_type).
    seen: set[tuple[int, str, str]] = set()

    for cand in candidates:
        by_pattern[cand.pattern] = by_pattern.get(cand.pattern, 0) + 1
        if cand.resolved_node is None:
            if len(unresolved_samples) < 20:
                unresolved_samples.append(
                    {
                        "news_id": cand.news_id,
                        "raw_entity": cand.raw_entity,
                        "title": cand.title[:160],
                        "pattern": cand.pattern,
                    }
                )
            continue
        resolved_count += 1

        key = (cand.news_id, cand.resolved_node, cand.shock_type)
        if key in seen:
            skipped_dup += 1
            continue
        seen.add(key)

        if dry_run:
            predictions.append(
                {
                    "news_id": cand.news_id,
                    "shock_node": cand.resolved_node,
                    "shock_type": cand.shock_type,
                    "magnitude": cand.magnitude,
                    "pattern": cand.pattern,
                    "title": cand.title[:160],
                    "prediction_id": None,
                    "dry_run": True,
                }
            )
            fired += 1
            continue

        # Check persistent dedup (across previous runs).
        try:
            with engine.connect() as conn:
                if _already_triggered(
                    conn, cand.news_id, cand.resolved_node, cand.shock_type
                ):
                    skipped_dup += 1
                    continue
        except Exception:
            pass

        # Fire the simulation.
        try:
            result = simulate_contagion(
                engine=engine,
                shock_node_id=cand.resolved_node,
                shock_type=cand.shock_type,
                shock_magnitude=cand.magnitude,
                max_depth=4,
            )
        except Exception as exc:
            log.warning(
                "news_contagion: simulate failed for {n}: {e}",
                n=cand.resolved_node,
                e=str(exc),
            )
            errors += 1
            continue

        prediction_id = _persist(engine, cand, result)
        if prediction_id is None:
            errors += 1
            continue

        # SYNTH-C / SYNTH-38 — non-fatal SignalFired fanout. Emits one
        # ``contagion_trigger`` per persisted shock so downstream
        # handlers (oracle_signals, journal) can pick it up.
        _emit_contagion_trigger(cand, prediction_id)

        fired += 1
        predictions.append(
            {
                "news_id": cand.news_id,
                "shock_node": cand.resolved_node,
                "shock_type": cand.shock_type,
                "magnitude": cand.magnitude,
                "pattern": cand.pattern,
                "title": cand.title[:160],
                "prediction_id": prediction_id,
                "dry_run": False,
            }
        )

    report = {
        "scanned_articles": len({c.news_id for c in candidates}),
        "candidates_total": len(candidates),
        "resolved": resolved_count,
        "unresolved": len(candidates) - resolved_count,
        "fired": fired,
        "skipped_duplicate": skipped_dup,
        "errors": errors,
        "by_pattern": by_pattern,
        "predictions": predictions[:50],
        "unresolved_samples": unresolved_samples,
        "dry_run": bool(dry_run),
        "since_hours": int(since_hours),
        "generated_at": started.isoformat(),
    }
    log.info(
        "news_contagion: scanned={s} resolved={r} fired={f} dup={d} err={e} dry_run={dr}",
        s=report["scanned_articles"],
        r=resolved_count,
        f=fired,
        d=skipped_dup,
        e=errors,
        dr=dry_run,
    )
    return report


__all__ = [
    "Candidate",
    "COMPILED_PATTERNS",
    "detect_patterns",
    "resolve_entity",
    "scan_news",
    "run_once",
]
