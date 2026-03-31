"""
GRID Intelligence — Actor Network & Power Structure Map.

The deepest intelligence layer. Maps the global financial power structure:
who controls money, where it flows, what motivates them, and how their
actions connect. Makes it actionable for trading.

Actor hierarchy:
    Sovereign     — central banks, treasuries, heads of state
    Regional      — ECB governors, BOJ/PBOC/BOE, key committee chairs
    Institutional — hedge funds, asset managers, SWFs, activist investors
    Individual    — traders, congressional members, corporate insiders

Data sources:
    - 13F filings (SEC EDGAR)
    - Congressional disclosures (House/Senate)
    - Form 4 insider filings
    - ICIJ Offshore Leaks (Panama/Pandora Papers)
    - Dark pool volume (FINRA ATS)
    - Federal Reserve speeches + dot plots
    - Public net worth / AUM estimates

Key entry points:
    build_actor_graph           — full graph for D3 force-directed viz
    track_wealth_migration      — follow the money over N days
    find_connected_actions      — who else in the network moved?
    assess_pocket_lining        — detect self-dealing, conflicts of interest
    get_actor_context_for_ticker — who cares about this stock?
    ingest_panama_pandora_data  — parse ICIJ offshore leaks

Implementation note:
    This file is a thin re-export facade. All logic lives in the
    intelligence/actors/ subpackage. Import from here or from the
    subpackage directly — both work identically.
"""

from __future__ import annotations

# ── Models ────────────────────────────────────────────────────────────────
from intelligence.actors.models import Actor, WealthFlow

# ── Seed data ─────────────────────────────────────────────────────────────
from intelligence.actors.seed_data import (
    _ACTOR_COUNT,
    _KNOWN_ACTORS,
    _SECTOR_COMMITTEE_MAP,
    _TICKER_SECTOR,
)

# Public alias for callers that use the unprotected name (deep_graph.py etc.)
KNOWN_ACTORS = _KNOWN_ACTORS

# ── Database layer ────────────────────────────────────────────────────────
from intelligence.actors.db import (
    _ensure_tables,
    _load_actors_from_db,
    _parse_jsonb,
    _seed_known_actors,
)

# ── Graph construction ────────────────────────────────────────────────────
from intelligence.actors.graph import (
    _compute_influence_propagation,
    _resolve_dynamic_insiders,
    build_actor_graph,
    find_connected_actions,
)

# ── Analysis ──────────────────────────────────────────────────────────────
from intelligence.actors.analysis import (
    enrich_lever_pullers_with_actors,
    generate_actor_report,
    get_actor_context_for_ticker,
)

# ── Ingestion ─────────────────────────────────────────────────────────────
from intelligence.actors.ingestion import ingest_panama_pandora_data

# ── Delegated functions (implemented in sibling modules) ──────────────────


def track_wealth_migration(engine, days: int = 90):
    """Track where money is moving over the last N days.

    Delegates to intelligence.wealth_tracker.
    Public API is unchanged.
    """
    from intelligence.wealth_tracker import track_wealth_migration as _impl
    return _impl(engine, days)


def _parse_signal_value(val):
    """Parse signal_value which may be JSON string, dict, or None.

    Delegates to intelligence.wealth_tracker.
    """
    from intelligence.wealth_tracker import _parse_signal_value as _impl
    return _impl(val)


def assess_pocket_lining(engine):
    """Detect self-dealing, conflicts of interest, and suspicious patterns.

    Delegates to intelligence.pocket_lining.
    Public API is unchanged.
    """
    from intelligence.pocket_lining import assess_pocket_lining as _impl
    return _impl(engine)


def persist_wealth_flows(engine, flows):
    """Persist WealthFlow objects to the wealth_flows table.

    Delegates to intelligence.wealth_tracker.
    Public API is unchanged.
    """
    from intelligence.wealth_tracker import persist_wealth_flows as _impl
    return _impl(engine, flows)


__all__ = [
    # Models
    "Actor",
    "WealthFlow",
    # Seed data
    "_KNOWN_ACTORS",
    "KNOWN_ACTORS",
    "_ACTOR_COUNT",
    "_SECTOR_COMMITTEE_MAP",
    "_TICKER_SECTOR",
    # DB
    "_ensure_tables",
    "_seed_known_actors",
    "_load_actors_from_db",
    "_parse_jsonb",
    # Graph
    "build_actor_graph",
    "find_connected_actions",
    "_resolve_dynamic_insiders",
    "_compute_influence_propagation",
    # Analysis
    "get_actor_context_for_ticker",
    "enrich_lever_pullers_with_actors",
    "generate_actor_report",
    # Ingestion
    "ingest_panama_pandora_data",
    # Delegated
    "track_wealth_migration",
    "_parse_signal_value",
    "assess_pocket_lining",
    "persist_wealth_flows",
]
