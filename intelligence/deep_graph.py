"""
GRID Intelligence — Deep Graph Traversal Engine.

Drills 10 layers deep from any ticker to find hidden connections between
seemingly unrelated networks.  Where the same people, money, or influence
appear across multiple paths, that overlap IS the alpha.

10 Layers of depth:
    1. Company (ticker)
    2. Board members + C-suite
    3. Their other board seats + fund affiliations
    4. Those companies' lobbyists
    5. The politicians those lobbyists work with (who received money?)
    6. Those politicians' committee assignments + votes
    7. Other companies affected by those committees' legislation
    8. Insiders of THOSE companies (who's trading?)
    9. Funds that hold positions in BOTH the original AND connected companies
   10. The ultimate beneficial owners / family offices behind those funds

At each layer: WHO, HOW MUCH money, WHEN, and CONNECTION TYPE.

Key entry points:
    deep_drill              — traverse outward layer by layer from a ticker
    find_overlaps           — drill from two tickers, find where graphs intersect
    find_all_overlaps       — pairwise overlap detection across all watchlist tickers
    generate_connection_map — D3-ready hierarchical graph
    discover_hidden_influence — sleuth: "these 3 events are connected"

Data table: graph_overlaps
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from itertools import combinations
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ══════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════


@dataclass
class GraphNode:
    """A node in the deep traversal graph."""

    id: str
    label: str
    node_type: str       # 'company', 'person', 'fund', 'lobbyist',
                         # 'politician', 'committee', 'legislation', 'insider'
    layer: int           # 1-10
    dollar_amount: float = 0.0
    timestamp: str = ""
    connection_type: str = ""   # how we reached this node
    metadata: dict = field(default_factory=dict)


@dataclass
class GraphEdge:
    """A directed edge between two graph nodes."""

    source: str
    target: str
    edge_type: str       # 'board_seat', 'lobbies_for', 'contributes_to',
                         # 'committee_member', 'legislates', 'insider_trade',
                         # 'holds_position', 'beneficial_owner', 'fund_affiliation'
    dollar_amount: float = 0.0
    timestamp: str = ""
    label: str = ""


@dataclass
class Overlap:
    """An intersection point between two apparently unrelated networks."""

    actor_a: str          # from one path
    actor_b: str          # from another path
    connection_point: str  # where they meet
    path_a: list[str] = field(default_factory=list)   # how we got to actor_a
    path_b: list[str] = field(default_factory=list)   # how we got to actor_b
    shared_tickers: list[str] = field(default_factory=list)
    shared_committees: list[str] = field(default_factory=list)
    shared_funds: list[str] = field(default_factory=list)
    total_dollar_flow: float = 0.0
    significance: float = 0.0   # 0-1
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LayerResult:
    """The result of drilling one layer."""

    depth: int
    layer_name: str
    actors: list[dict] = field(default_factory=list)
    connections: list[dict] = field(default_factory=list)
    dollar_flows: float = 0.0
    count: int = 0


# ══════════════════════════════════════════════════════════════════════════
# SCHEMA
# ══════════════════════════════════════════════════════════════════════════

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS graph_overlaps (
    id                SERIAL PRIMARY KEY,
    ticker_a          TEXT,
    ticker_b          TEXT,
    connection_point  TEXT,
    path_a            JSONB,
    path_b            JSONB,
    shared_entities   JSONB,
    dollar_flow       NUMERIC,
    significance      NUMERIC,
    description       TEXT,
    discovered_at     TIMESTAMPTZ DEFAULT NOW()
);
"""

_CREATE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_graph_overlaps_tickers ON graph_overlaps (ticker_a, ticker_b);",
    "CREATE INDEX IF NOT EXISTS idx_graph_overlaps_sig ON graph_overlaps (significance DESC);",
    "CREATE INDEX IF NOT EXISTS idx_graph_overlaps_discovered ON graph_overlaps (discovered_at DESC);",
]

LAYER_NAMES = {
    1: "Company",
    2: "Board & C-Suite",
    3: "Other Board Seats & Fund Affiliations",
    4: "Lobbyists",
    5: "Politicians (PAC Recipients)",
    6: "Committee Assignments & Votes",
    7: "Legislatively Affected Companies",
    8: "Insiders of Connected Companies",
    9: "Cross-Holding Funds",
    10: "Ultimate Beneficial Owners",
}

# Cap to prevent combinatorial explosion
# Raised for 250K-scale graph: deep_drill still caps per-layer output
# but the underlying actor pool is much larger now.
_MAX_ACTORS = 10_000
_MAX_PER_LAYER = 500


def ensure_table(engine: Engine) -> None:
    """Create the graph_overlaps table and indexes if they don't exist."""
    with engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE_SQL))
        for idx_sql in _CREATE_INDEX_SQL:
            conn.execute(text(idx_sql))
    log.debug("graph_overlaps table ensured")


# ══════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS — data retrieval per layer
# ══════════════════════════════════════════════════════════════════════════

def _parse_json(raw: Any) -> dict[str, Any] | None:
    """Parse a JSON string or dict safely."""
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return None
    return None


def _get_watchlist_tickers(engine: Engine) -> list[str]:
    """Fetch active watchlist tickers."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT ticker FROM watchlist WHERE active = TRUE LIMIT 100"
            )).fetchall()
            return [r[0] for r in rows] if rows else []
    except Exception:
        return []


def _layer1_company(engine: Engine, ticker: str) -> LayerResult:
    """Layer 1: The root company itself."""
    layer = LayerResult(depth=1, layer_name=LAYER_NAMES[1])

    # Try to find the company name from market_universe or signal_sources
    company_name = ticker
    try:
        from analysis.market_universe import MARKET_UNIVERSE
        for sector_data in MARKET_UNIVERSE.values():
            for ind_data in sector_data.get("industries", {}).values():
                for co in ind_data.get("companies", []):
                    if co.get("ticker") == ticker:
                        company_name = co.get("name", ticker)
                        break
    except ImportError:
        pass

    node = {
        "id": f"company:{ticker}",
        "label": company_name,
        "type": "company",
        "ticker": ticker,
        "layer": 1,
    }
    layer.actors.append(node)
    layer.count = 1
    return layer


def _layer2_board_csuite(engine: Engine, ticker: str) -> LayerResult:
    """Layer 2: Board members and C-suite executives.

    Sources: actors table, actor_network KNOWN_ACTORS, insider filings.
    """
    layer = LayerResult(depth=2, layer_name=LAYER_NAMES[2])
    seen: set[str] = set()

    # From actors table (auto-discovered actors)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT actor_id, name, tier, category, title, net_worth_estimate "
                "FROM actors "
                "WHERE category IN ('insider', 'corporation') "
                "AND (metadata->>'primary_ticker' = :ticker "
                "     OR metadata->>'company' ILIKE :pattern) "
                "LIMIT :lim"
            ), {"ticker": ticker, "pattern": f"%{ticker}%", "lim": _MAX_PER_LAYER}).fetchall()

            for row in rows:
                aid = row[0]
                if aid in seen:
                    continue
                seen.add(aid)
                layer.actors.append({
                    "id": f"person:{aid}",
                    "label": row[1],
                    "type": "person",
                    "title": row[4] or "",
                    "net_worth": float(row[5] or 0),
                    "layer": 2,
                    "connection_type": "board_or_csuite",
                })
    except Exception as exc:
        log.debug("Layer 2 actors table query failed: {e}", e=str(exc))

    # From insider filings (Form 4)
    try:
        cutoff = date.today() - timedelta(days=365)
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT source_id, signal_value "
                "FROM signal_sources "
                "WHERE source_type = 'insider' "
                "AND ticker = :ticker "
                "AND signal_date >= :cutoff "
                "LIMIT :lim"
            ), {"ticker": ticker, "cutoff": cutoff, "lim": _MAX_PER_LAYER}).fetchall()

            for row in rows:
                name = row[0] or ""
                val = _parse_json(row[1])
                title = val.get("owner_title", val.get("relationship", "")) if val else ""
                # Only include officers and directors (C-suite / board)
                if val and any(
                    kw in (title or "").lower()
                    for kw in ("officer", "director", "ceo", "cfo", "coo", "cto",
                               "president", "chairman", "chief", "evp", "svp", "vp")
                ):
                    slug = name.strip().lower().replace(" ", "_")
                    if slug and slug not in seen:
                        seen.add(slug)
                        amt = float(val.get("transaction_value", 0) or 0) if val else 0
                        layer.actors.append({
                            "id": f"person:{slug}",
                            "label": name,
                            "type": "person",
                            "title": title,
                            "layer": 2,
                            "connection_type": "insider_filing",
                            "dollar_amount": amt,
                        })
                        layer.dollar_flows += amt
    except Exception as exc:
        log.debug("Layer 2 insider query failed: {e}", e=str(exc))

    # From the hardcoded actor_network known actors
    try:
        from intelligence.actor_network import KNOWN_ACTORS
        for actor_def in KNOWN_ACTORS:
            board_seats = actor_def.get("board_seats", [])
            positions = actor_def.get("known_positions", [])
            name = actor_def.get("name", "")
            # Check if this actor is associated with the ticker
            ticker_match = False
            for seat in board_seats:
                if ticker.upper() in seat.upper():
                    ticker_match = True
                    break
            for pos in positions:
                if isinstance(pos, dict) and pos.get("ticker", "").upper() == ticker.upper():
                    ticker_match = True
                    break
            if ticker_match:
                slug = name.strip().lower().replace(" ", "_")
                if slug and slug not in seen:
                    seen.add(slug)
                    layer.actors.append({
                        "id": f"person:{slug}",
                        "label": name,
                        "type": "person",
                        "title": actor_def.get("title", ""),
                        "layer": 2,
                        "connection_type": "known_actor",
                    })
    except (ImportError, AttributeError):
        pass

    layer.count = len(layer.actors)
    return layer


def _layer3_other_affiliations(
    engine: Engine, layer2_actors: list[dict]
) -> LayerResult:
    """Layer 3: Other board seats and fund affiliations of Layer 2 actors.

    For each person from Layer 2, find what OTHER companies/funds they're
    connected to.
    """
    layer = LayerResult(depth=3, layer_name=LAYER_NAMES[3])
    seen: set[str] = set()

    for actor in layer2_actors[:_MAX_PER_LAYER]:
        actor_name = actor.get("label", "")
        if not actor_name:
            continue

        # Search actors table for their other affiliations
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT actor_id, metadata "
                    "FROM actors "
                    "WHERE name ILIKE :pattern "
                    "LIMIT 5"
                ), {"pattern": f"%{actor_name}%"}).fetchall()

                for row in rows:
                    meta = _parse_json(row[1])
                    if not meta:
                        continue
                    # Extract board seats, fund affiliations
                    for seat in meta.get("board_seats", []):
                        node_id = f"company:{seat.strip().lower().replace(' ', '_')}"
                        if node_id not in seen:
                            seen.add(node_id)
                            layer.actors.append({
                                "id": node_id,
                                "label": seat,
                                "type": "company",
                                "layer": 3,
                                "connection_type": "board_seat",
                                "connected_via": actor_name,
                            })
                            layer.connections.append({
                                "from": actor.get("id", ""),
                                "to": node_id,
                                "type": "board_seat",
                            })
                    for fund in meta.get("fund_affiliations", []):
                        node_id = f"fund:{fund.strip().lower().replace(' ', '_')}"
                        if node_id not in seen:
                            seen.add(node_id)
                            layer.actors.append({
                                "id": node_id,
                                "label": fund,
                                "type": "fund",
                                "layer": 3,
                                "connection_type": "fund_affiliation",
                                "connected_via": actor_name,
                            })
                            layer.connections.append({
                                "from": actor.get("id", ""),
                                "to": node_id,
                                "type": "fund_affiliation",
                            })
        except Exception as exc:
            log.debug("DeepGraph: fund affiliation query failed: {e}", e=str(exc))

        # Also check known actors for board_seats
        try:
            from intelligence.actor_network import KNOWN_ACTORS
            for actor_def in KNOWN_ACTORS:
                if actor_name.lower() in actor_def.get("name", "").lower():
                    for seat in actor_def.get("board_seats", []):
                        node_id = f"company:{seat.strip().lower().replace(' ', '_')}"
                        if node_id not in seen:
                            seen.add(node_id)
                            layer.actors.append({
                                "id": node_id,
                                "label": seat,
                                "type": "company",
                                "layer": 3,
                                "connection_type": "board_seat",
                                "connected_via": actor_name,
                            })
        except (ImportError, AttributeError):
            pass

    layer.count = len(layer.actors)
    return layer


def _layer4_lobbyists(engine: Engine, company_ids: list[str]) -> LayerResult:
    """Layer 4: Lobbyists working for companies found in Layers 1-3.

    Sources: lobbying data in signal_sources.
    """
    layer = LayerResult(depth=4, layer_name=LAYER_NAMES[4])
    seen: set[str] = set()
    cutoff = date.today() - timedelta(days=365)

    # Extract ticker-like identifiers from company node IDs
    tickers: set[str] = set()
    for cid in company_ids:
        # company:NVDA or company:some_name
        parts = cid.split(":", 1)
        if len(parts) == 2:
            tickers.add(parts[1].upper())

    for ticker in list(tickers)[:50]:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT signal_date, signal_value "
                    "FROM signal_sources "
                    "WHERE source_type = 'lobbying' "
                    "AND ticker = :ticker "
                    "AND signal_date >= :cutoff "
                    "ORDER BY signal_date DESC "
                    "LIMIT 50"
                ), {"ticker": ticker, "cutoff": cutoff}).fetchall()

                for row in rows:
                    val = _parse_json(row[1])
                    if not val:
                        continue
                    registrant = val.get("registrant_name", "")
                    if not registrant:
                        continue
                    slug = registrant.strip().lower().replace(" ", "_")
                    node_id = f"lobbyist:{slug}"
                    amt = float(val.get("amount", 0) or 0)
                    if node_id not in seen:
                        seen.add(node_id)
                        layer.actors.append({
                            "id": node_id,
                            "label": registrant,
                            "type": "lobbyist",
                            "layer": 4,
                            "connection_type": "lobbies_for",
                            "dollar_amount": amt,
                            "lobbies_for_ticker": ticker,
                            "issue_codes": val.get("issue_codes", []),
                        })
                    layer.dollar_flows += amt
                    layer.connections.append({
                        "from": f"company:{ticker}",
                        "to": node_id,
                        "type": "lobbies_for",
                        "amount": amt,
                        "date": str(row[0]),
                    })
        except Exception as exc:
            log.debug("DeepGraph layer4: lobbying query failed: {e}", e=str(exc))

    layer.count = len(layer.actors)
    return layer


def _layer5_politicians(engine: Engine, lobbyist_tickers: set[str]) -> LayerResult:
    """Layer 5: Politicians who received money from companies the lobbyists work for.

    Sources: campaign_finance / PAC contribution data in signal_sources.
    """
    layer = LayerResult(depth=5, layer_name=LAYER_NAMES[5])
    seen: set[str] = set()
    cutoff = date.today() - timedelta(days=730)

    for ticker in list(lobbyist_tickers)[:50]:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT signal_date, signal_value "
                    "FROM signal_sources "
                    "WHERE source_type = 'campaign_finance' "
                    "AND ticker = :ticker "
                    "AND signal_date >= :cutoff "
                    "ORDER BY signal_date DESC "
                    "LIMIT 100"
                ), {"ticker": ticker, "cutoff": cutoff}).fetchall()

                for row in rows:
                    val = _parse_json(row[1])
                    if not val:
                        continue
                    recipient = val.get("recipient_name", "")
                    if not recipient:
                        continue
                    slug = recipient.strip().lower().replace(" ", "_")
                    node_id = f"politician:{slug}"
                    amt = float(val.get("amount", 0) or 0)
                    if node_id not in seen:
                        seen.add(node_id)
                        layer.actors.append({
                            "id": node_id,
                            "label": recipient,
                            "type": "politician",
                            "layer": 5,
                            "connection_type": "pac_recipient",
                            "dollar_amount": amt,
                            "state": val.get("recipient_state", ""),
                            "party": val.get("party", ""),
                            "from_ticker": ticker,
                        })
                    else:
                        # Accumulate dollar amounts for already-seen politicians
                        for a in layer.actors:
                            if a["id"] == node_id:
                                a["dollar_amount"] = a.get("dollar_amount", 0) + amt
                                break
                    layer.dollar_flows += amt
                    layer.connections.append({
                        "from": f"company:{ticker}",
                        "to": node_id,
                        "type": "contributes_to",
                        "amount": amt,
                        "date": str(row[0]),
                    })
        except Exception as exc:
            log.debug("DeepGraph layer5: politician contributions query failed: {e}", e=str(exc))

    layer.count = len(layer.actors)
    return layer


def _layer6_committees(engine: Engine, politician_names: list[str]) -> LayerResult:
    """Layer 6: Committee assignments and votes of Layer 5 politicians.

    Sources: congressional trading disclosures (committee field),
             legislation data in raw_series.
    """
    layer = LayerResult(depth=6, layer_name=LAYER_NAMES[6])
    seen_committees: set[str] = set()
    seen_legislation: set[str] = set()

    # Get committee info from congressional trading disclosures
    for name in politician_names[:_MAX_PER_LAYER]:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT DISTINCT signal_value "
                    "FROM signal_sources "
                    "WHERE source_type = 'congressional' "
                    "AND source_id ILIKE :pattern "
                    "LIMIT 20"
                ), {"pattern": f"%{name}%"}).fetchall()

                for row in rows:
                    val = _parse_json(row[0])
                    if not val:
                        continue
                    committee = val.get("committee", "")
                    if committee and committee not in seen_committees:
                        seen_committees.add(committee)
                        node_id = f"committee:{committee.strip().lower().replace(' ', '_')}"
                        layer.actors.append({
                            "id": node_id,
                            "label": committee,
                            "type": "committee",
                            "layer": 6,
                            "connection_type": "committee_member",
                        })
                        layer.connections.append({
                            "from": f"politician:{name.strip().lower().replace(' ', '_')}",
                            "to": node_id,
                            "type": "committee_member",
                        })
        except Exception as exc:
            log.debug("DeepGraph layer6: committee member query failed: {e}", e=str(exc))

    # Search for legislation those committees touch
    cutoff = date.today() - timedelta(days=365)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT series_id, obs_date, raw_payload "
                "FROM raw_series "
                "WHERE series_id LIKE 'LEGISLATION:%' "
                "AND obs_date >= :cutoff "
                "AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC "
                "LIMIT 500"
            ), {"cutoff": cutoff}).fetchall()

            for row in rows:
                payload = _parse_json(row[2])
                if not payload:
                    continue
                bill_committee = (payload.get("committee", "") or "").lower()
                sponsors = (payload.get("sponsor", "") or "").lower()
                cosponsors_str = " ".join(payload.get("cosponsors", []) or []).lower()

                # Check if any of our politicians are sponsors
                politician_match = False
                for name in politician_names[:_MAX_PER_LAYER]:
                    if name.lower() in sponsors or name.lower() in cosponsors_str:
                        politician_match = True
                        break

                # Check if committee matches
                committee_match = any(
                    c.lower() in bill_committee for c in seen_committees
                )

                if politician_match or committee_match:
                    bill_id = payload.get("bill_id", row[0])
                    if bill_id not in seen_legislation:
                        seen_legislation.add(bill_id)
                        node_id = f"legislation:{bill_id.replace(' ', '_')}"
                        layer.actors.append({
                            "id": node_id,
                            "label": payload.get("title", bill_id),
                            "type": "legislation",
                            "layer": 6,
                            "connection_type": "legislates",
                            "affected_tickers": payload.get("affected_tickers", []),
                            "status": payload.get("status", ""),
                        })
    except Exception as exc:
        log.debug("DeepGraph layer6: legislation query failed: {e}", e=str(exc))

    layer.count = len(layer.actors)
    return layer


def _layer7_affected_companies(
    engine: Engine, layer6_actors: list[dict]
) -> LayerResult:
    """Layer 7: Companies affected by legislation from Layer 6.

    Extract affected_tickers from legislation metadata.
    """
    layer = LayerResult(depth=7, layer_name=LAYER_NAMES[7])
    seen: set[str] = set()

    for actor in layer6_actors:
        if actor.get("type") != "legislation":
            continue
        affected = actor.get("affected_tickers", [])
        for ticker in affected:
            if not ticker:
                continue
            node_id = f"company:{ticker.upper()}"
            if node_id not in seen:
                seen.add(node_id)
                layer.actors.append({
                    "id": node_id,
                    "label": ticker.upper(),
                    "type": "company",
                    "ticker": ticker.upper(),
                    "layer": 7,
                    "connection_type": "legislatively_affected",
                    "affected_by": actor.get("label", ""),
                })
                layer.connections.append({
                    "from": actor.get("id", ""),
                    "to": node_id,
                    "type": "legislates",
                })

    layer.count = len(layer.actors)
    return layer


def _layer8_insiders(engine: Engine, company_tickers: list[str]) -> LayerResult:
    """Layer 8: Insiders trading in the Layer 7 companies.

    Sources: insider filings + congressional trading disclosures.
    """
    layer = LayerResult(depth=8, layer_name=LAYER_NAMES[8])
    seen: set[str] = set()
    cutoff = date.today() - timedelta(days=180)

    for ticker in list(set(company_tickers))[:50]:
        # Insider filings
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT source_id, signal_date, signal_type, signal_value "
                    "FROM signal_sources "
                    "WHERE source_type IN ('insider', 'congressional') "
                    "AND ticker = :ticker "
                    "AND signal_date >= :cutoff "
                    "ORDER BY signal_date DESC "
                    "LIMIT 50"
                ), {"ticker": ticker, "cutoff": cutoff}).fetchall()

                for row in rows:
                    name = row[0] or ""
                    val = _parse_json(row[3])
                    if not name:
                        name = val.get("member_name", val.get("owner_name", "")) if val else ""
                    if not name:
                        continue
                    slug = name.strip().lower().replace(" ", "_")
                    node_id = f"insider:{slug}"
                    amt_raw = val.get("transaction_value", val.get("amount_range", 0)) if val else 0
                    amt = float(amt_raw) if isinstance(amt_raw, (int, float)) else 0
                    if node_id not in seen:
                        seen.add(node_id)
                        layer.actors.append({
                            "id": node_id,
                            "label": name,
                            "type": "insider",
                            "layer": 8,
                            "connection_type": "insider_trade",
                            "ticker": ticker,
                            "action": row[2] or "",
                            "dollar_amount": amt,
                            "date": str(row[1]),
                        })
                    layer.dollar_flows += amt
                    layer.connections.append({
                        "from": node_id,
                        "to": f"company:{ticker}",
                        "type": "insider_trade",
                        "action": row[2] or "",
                        "amount": amt,
                        "date": str(row[1]),
                    })
        except Exception as exc:
            log.debug("DeepGraph layer7: insider trades query failed: {e}", e=str(exc))

    layer.count = len(layer.actors)
    return layer


def _layer9_cross_holding_funds(
    engine: Engine, root_ticker: str, connected_tickers: list[str]
) -> LayerResult:
    """Layer 9: Funds holding positions in BOTH the original AND connected companies.

    Sources: 13F filings in signal_sources / raw_series.
    """
    layer = LayerResult(depth=9, layer_name=LAYER_NAMES[9])
    seen: set[str] = set()

    # Build a map: fund_name -> set of tickers they hold
    fund_holdings: dict[str, set[str]] = defaultdict(set)
    fund_amounts: dict[str, float] = defaultdict(float)

    all_tickers = [root_ticker] + list(set(connected_tickers))[:50]

    for ticker in all_tickers:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT source_id, signal_value "
                    "FROM signal_sources "
                    "WHERE source_type = '13f' "
                    "AND ticker = :ticker "
                    "ORDER BY signal_date DESC "
                    "LIMIT 100"
                ), {"ticker": ticker}).fetchall()

                for row in rows:
                    fund_name = row[0] or ""
                    val = _parse_json(row[1])
                    if not fund_name:
                        fund_name = val.get("manager_name", "") if val else ""
                    if fund_name:
                        fund_holdings[fund_name].add(ticker)
                        amt = float(val.get("value", val.get("market_value", 0)) or 0) if val else 0
                        fund_amounts[fund_name] += amt
        except Exception as exc:
            log.debug("DeepGraph: fund holdings enrichment query failed: {e}", e=str(exc))

    # Find funds that hold BOTH the root ticker and at least one connected ticker
    connected_set = set(connected_tickers)
    for fund_name, held_tickers in fund_holdings.items():
        if root_ticker in held_tickers and held_tickers & connected_set:
            slug = fund_name.strip().lower().replace(" ", "_")
            node_id = f"fund:{slug}"
            if node_id not in seen:
                seen.add(node_id)
                shared = sorted(held_tickers & (connected_set | {root_ticker}))
                layer.actors.append({
                    "id": node_id,
                    "label": fund_name,
                    "type": "fund",
                    "layer": 9,
                    "connection_type": "cross_holding",
                    "shared_tickers": shared,
                    "total_value": fund_amounts.get(fund_name, 0),
                })
                layer.dollar_flows += fund_amounts.get(fund_name, 0)
                # Add edges to each shared ticker
                for t in shared:
                    layer.connections.append({
                        "from": node_id,
                        "to": f"company:{t}",
                        "type": "holds_position",
                        "label": f"holds {t}",
                    })

    layer.count = len(layer.actors)
    return layer


def _layer10_beneficial_owners(
    engine: Engine, fund_ids: list[str]
) -> LayerResult:
    """Layer 10: Ultimate beneficial owners / family offices behind the funds.

    Sources: actors table (tier='institutional'), ICIJ offshore data if available,
             known actors database.
    """
    layer = LayerResult(depth=10, layer_name=LAYER_NAMES[10])
    seen: set[str] = set()

    # Extract fund names from IDs
    fund_names = []
    for fid in fund_ids:
        parts = fid.split(":", 1)
        if len(parts) == 2:
            fund_names.append(parts[1].replace("_", " "))

    for fund_name in fund_names[:_MAX_PER_LAYER]:
        # Search actors table for institutional owners
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT actor_id, name, tier, metadata, net_worth_estimate "
                    "FROM actors "
                    "WHERE (tier = 'institutional' OR category = 'fund') "
                    "AND name ILIKE :pattern "
                    "LIMIT 10"
                ), {"pattern": f"%{fund_name}%"}).fetchall()

                for row in rows:
                    meta = _parse_json(row[3])
                    # Look for beneficial_owner, founder, managing_partner
                    owners = []
                    if meta:
                        owners = meta.get("beneficial_owners", [])
                        if not owners:
                            founder = meta.get("founder", meta.get("managing_partner", ""))
                            if founder:
                                owners = [founder]

                    for owner in owners:
                        owner_name = owner if isinstance(owner, str) else owner.get("name", "")
                        if not owner_name:
                            continue
                        slug = owner_name.strip().lower().replace(" ", "_")
                        node_id = f"owner:{slug}"
                        if node_id not in seen:
                            seen.add(node_id)
                            layer.actors.append({
                                "id": node_id,
                                "label": owner_name,
                                "type": "beneficial_owner",
                                "layer": 10,
                                "connection_type": "beneficial_owner",
                                "fund": fund_name,
                                "net_worth": float(row[4] or 0),
                            })
                            layer.connections.append({
                                "from": node_id,
                                "to": f"fund:{fund_name.strip().lower().replace(' ', '_')}",
                                "type": "beneficial_owner",
                            })
        except Exception as exc:
            log.debug("DeepGraph: beneficial owner query failed: {e}", e=str(exc))

    # Also check hardcoded known actors for fund connections
    try:
        from intelligence.actor_network import KNOWN_ACTORS
        for actor_def in KNOWN_ACTORS:
            name = actor_def.get("name", "")
            category = actor_def.get("category", "")
            if category in ("fund", "activist", "swf"):
                for fund_name in fund_names:
                    if fund_name.lower() in name.lower() or any(
                        fund_name.lower() in (s or "").lower()
                        for s in actor_def.get("board_seats", [])
                    ):
                        slug = name.strip().lower().replace(" ", "_")
                        node_id = f"owner:{slug}"
                        if node_id not in seen:
                            seen.add(node_id)
                            layer.actors.append({
                                "id": node_id,
                                "label": name,
                                "type": "beneficial_owner",
                                "layer": 10,
                                "connection_type": "known_fund_principal",
                                "net_worth": actor_def.get("net_worth_estimate", 0) or 0,
                            })
    except (ImportError, AttributeError):
        pass

    layer.count = len(layer.actors)
    return layer


# ══════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════


def deep_drill(
    engine: Engine, ticker: str, max_depth: int = 10
) -> dict[str, Any]:
    """Start from a ticker, traverse outward layer by layer up to 10 deep.

    At each layer: find all connected actors, their connections, THEIR
    connections...  Returns a structured result with all layers, total
    actor count, and total connections.  Capped at 1000 actors to prevent
    combinatorial explosion.

    Args:
        engine: SQLAlchemy database engine.
        ticker: Root ticker symbol (e.g., "NVDA").
        max_depth: Maximum traversal depth (1-10, default 10).

    Returns:
        {
            "ticker": str,
            "layers": [{depth, layer_name, actors, connections, dollar_flows}],
            "total_actors": int,
            "total_connections": int,
            "total_dollar_flow": float,
            "capped": bool,
        }
    """
    ensure_table(engine)
    ticker = ticker.strip().upper()
    max_depth = min(max(max_depth, 1), 10)

    layers: list[dict] = []
    all_actors: list[dict] = []
    all_connections: list[dict] = []
    total_dollar_flow = 0.0
    capped = False

    log.info("Deep drill starting: ticker={t}, max_depth={d}", t=ticker, d=max_depth)

    # Layer 1: Company
    l1 = _layer1_company(engine, ticker)
    layers.append({"depth": 1, "layer_name": l1.layer_name,
                    "actors": l1.actors, "connections": l1.connections,
                    "dollar_flows": l1.dollar_flows, "count": l1.count})
    all_actors.extend(l1.actors)
    if max_depth < 2 or len(all_actors) >= _MAX_ACTORS:
        capped = len(all_actors) >= _MAX_ACTORS
        return _build_drill_result(ticker, layers, all_actors, all_connections,
                                   total_dollar_flow, capped)

    # Layer 2: Board & C-Suite
    l2 = _layer2_board_csuite(engine, ticker)
    layers.append({"depth": 2, "layer_name": l2.layer_name,
                    "actors": l2.actors, "connections": l2.connections,
                    "dollar_flows": l2.dollar_flows, "count": l2.count})
    all_actors.extend(l2.actors)
    total_dollar_flow += l2.dollar_flows
    if max_depth < 3 or len(all_actors) >= _MAX_ACTORS:
        capped = len(all_actors) >= _MAX_ACTORS
        return _build_drill_result(ticker, layers, all_actors, all_connections,
                                   total_dollar_flow, capped)

    # Layer 3: Other board seats + fund affiliations
    l3 = _layer3_other_affiliations(engine, l2.actors)
    layers.append({"depth": 3, "layer_name": l3.layer_name,
                    "actors": l3.actors, "connections": l3.connections,
                    "dollar_flows": l3.dollar_flows, "count": l3.count})
    all_actors.extend(l3.actors)
    all_connections.extend(l3.connections)
    if max_depth < 4 or len(all_actors) >= _MAX_ACTORS:
        capped = len(all_actors) >= _MAX_ACTORS
        return _build_drill_result(ticker, layers, all_actors, all_connections,
                                   total_dollar_flow, capped)

    # Collect all company IDs from layers 1-3 for lobbyist search
    company_ids = [
        a["id"] for a in all_actors
        if a.get("type") == "company" or a.get("id", "").startswith("company:")
    ]

    # Layer 4: Lobbyists
    l4 = _layer4_lobbyists(engine, company_ids)
    layers.append({"depth": 4, "layer_name": l4.layer_name,
                    "actors": l4.actors, "connections": l4.connections,
                    "dollar_flows": l4.dollar_flows, "count": l4.count})
    all_actors.extend(l4.actors)
    all_connections.extend(l4.connections)
    total_dollar_flow += l4.dollar_flows
    if max_depth < 5 or len(all_actors) >= _MAX_ACTORS:
        capped = len(all_actors) >= _MAX_ACTORS
        return _build_drill_result(ticker, layers, all_actors, all_connections,
                                   total_dollar_flow, capped)

    # Collect tickers that lobbyists are connected to
    lobbyist_tickers: set[str] = set()
    for a in l4.actors:
        t = a.get("lobbies_for_ticker", "")
        if t:
            lobbyist_tickers.add(t)
    # Also include all company tickers found so far
    for a in all_actors:
        t = a.get("ticker", "")
        if t:
            lobbyist_tickers.add(t)

    # Layer 5: Politicians
    l5 = _layer5_politicians(engine, lobbyist_tickers)
    layers.append({"depth": 5, "layer_name": l5.layer_name,
                    "actors": l5.actors, "connections": l5.connections,
                    "dollar_flows": l5.dollar_flows, "count": l5.count})
    all_actors.extend(l5.actors)
    all_connections.extend(l5.connections)
    total_dollar_flow += l5.dollar_flows
    if max_depth < 6 or len(all_actors) >= _MAX_ACTORS:
        capped = len(all_actors) >= _MAX_ACTORS
        return _build_drill_result(ticker, layers, all_actors, all_connections,
                                   total_dollar_flow, capped)

    # Layer 6: Committees & Legislation
    politician_names = [a.get("label", "") for a in l5.actors if a.get("label")]
    l6 = _layer6_committees(engine, politician_names)
    layers.append({"depth": 6, "layer_name": l6.layer_name,
                    "actors": l6.actors, "connections": l6.connections,
                    "dollar_flows": l6.dollar_flows, "count": l6.count})
    all_actors.extend(l6.actors)
    all_connections.extend(l6.connections)
    if max_depth < 7 or len(all_actors) >= _MAX_ACTORS:
        capped = len(all_actors) >= _MAX_ACTORS
        return _build_drill_result(ticker, layers, all_actors, all_connections,
                                   total_dollar_flow, capped)

    # Layer 7: Affected Companies
    l7 = _layer7_affected_companies(engine, l6.actors)
    layers.append({"depth": 7, "layer_name": l7.layer_name,
                    "actors": l7.actors, "connections": l7.connections,
                    "dollar_flows": l7.dollar_flows, "count": l7.count})
    all_actors.extend(l7.actors)
    all_connections.extend(l7.connections)
    if max_depth < 8 or len(all_actors) >= _MAX_ACTORS:
        capped = len(all_actors) >= _MAX_ACTORS
        return _build_drill_result(ticker, layers, all_actors, all_connections,
                                   total_dollar_flow, capped)

    # Layer 8: Insiders of connected companies
    connected_tickers = [a.get("ticker", "") for a in l7.actors if a.get("ticker")]
    l8 = _layer8_insiders(engine, connected_tickers)
    layers.append({"depth": 8, "layer_name": l8.layer_name,
                    "actors": l8.actors, "connections": l8.connections,
                    "dollar_flows": l8.dollar_flows, "count": l8.count})
    all_actors.extend(l8.actors)
    all_connections.extend(l8.connections)
    total_dollar_flow += l8.dollar_flows
    if max_depth < 9 or len(all_actors) >= _MAX_ACTORS:
        capped = len(all_actors) >= _MAX_ACTORS
        return _build_drill_result(ticker, layers, all_actors, all_connections,
                                   total_dollar_flow, capped)

    # Layer 9: Cross-holding funds
    all_connected_tickers = list({
        a.get("ticker", "") for a in all_actors
        if a.get("ticker") and a.get("ticker") != ticker
    })
    l9 = _layer9_cross_holding_funds(engine, ticker, all_connected_tickers)
    layers.append({"depth": 9, "layer_name": l9.layer_name,
                    "actors": l9.actors, "connections": l9.connections,
                    "dollar_flows": l9.dollar_flows, "count": l9.count})
    all_actors.extend(l9.actors)
    all_connections.extend(l9.connections)
    total_dollar_flow += l9.dollar_flows
    if max_depth < 10 or len(all_actors) >= _MAX_ACTORS:
        capped = len(all_actors) >= _MAX_ACTORS
        return _build_drill_result(ticker, layers, all_actors, all_connections,
                                   total_dollar_flow, capped)

    # Layer 10: Beneficial owners
    fund_ids = [a["id"] for a in l9.actors if a.get("type") == "fund"]
    l10 = _layer10_beneficial_owners(engine, fund_ids)
    layers.append({"depth": 10, "layer_name": l10.layer_name,
                    "actors": l10.actors, "connections": l10.connections,
                    "dollar_flows": l10.dollar_flows, "count": l10.count})
    all_actors.extend(l10.actors)
    all_connections.extend(l10.connections)
    capped = len(all_actors) >= _MAX_ACTORS

    log.info(
        "Deep drill complete: ticker={t}, actors={a}, connections={c}, dollars=${d:,.0f}",
        t=ticker, a=len(all_actors), c=len(all_connections), d=total_dollar_flow,
    )

    return _build_drill_result(ticker, layers, all_actors, all_connections,
                               total_dollar_flow, capped)


def _build_drill_result(
    ticker: str,
    layers: list[dict],
    all_actors: list[dict],
    all_connections: list[dict],
    total_dollar_flow: float,
    capped: bool,
) -> dict[str, Any]:
    """Assemble the final deep_drill result dict."""
    return {
        "ticker": ticker,
        "layers": layers,
        "total_actors": len(all_actors),
        "total_connections": len(all_connections),
        "total_dollar_flow": total_dollar_flow,
        "max_depth_reached": max((l["depth"] for l in layers), default=0),
        "capped": capped,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════════
# OVERLAP DETECTION
# ══════════════════════════════════════════════════════════════════════════


def _compute_significance(overlap: Overlap) -> float:
    """Score an overlap from 0 to 1 based on how meaningful it is.

    Factors:
        - Number of shared tickers          (0.20)
        - Number of shared committees        (0.20)
        - Number of shared funds             (0.20)
        - Dollar flow magnitude              (0.20)
        - Path length (shorter = more significant) (0.20)
    """
    score = 0.0

    # Shared entities
    if overlap.shared_tickers:
        score += min(0.20, 0.05 * len(overlap.shared_tickers))
    if overlap.shared_committees:
        score += min(0.20, 0.10 * len(overlap.shared_committees))
    if overlap.shared_funds:
        score += min(0.20, 0.07 * len(overlap.shared_funds))

    # Dollar flow
    if overlap.total_dollar_flow > 100_000_000:
        score += 0.20
    elif overlap.total_dollar_flow > 10_000_000:
        score += 0.15
    elif overlap.total_dollar_flow > 1_000_000:
        score += 0.10
    elif overlap.total_dollar_flow > 100_000:
        score += 0.05

    # Path brevity (shorter paths = more direct = more significant)
    avg_path_len = (len(overlap.path_a) + len(overlap.path_b)) / 2
    if avg_path_len <= 2:
        score += 0.20
    elif avg_path_len <= 4:
        score += 0.15
    elif avg_path_len <= 6:
        score += 0.10
    else:
        score += 0.05

    return round(min(score, 1.0), 3)


def _extract_actor_index(drill_result: dict) -> dict[str, dict]:
    """Build a {actor_id: actor_dict} index from a drill result."""
    index: dict[str, dict] = {}
    for layer in drill_result.get("layers", []):
        for actor in layer.get("actors", []):
            aid = actor.get("id", "")
            if aid:
                index[aid] = actor
    return index


def _extract_sets(drill_result: dict) -> tuple[set[str], set[str], set[str], set[str]]:
    """Extract sets of actor IDs, tickers, committees, and funds from a drill result."""
    actor_ids: set[str] = set()
    tickers: set[str] = set()
    committees: set[str] = set()
    funds: set[str] = set()

    for layer in drill_result.get("layers", []):
        for actor in layer.get("actors", []):
            aid = actor.get("id", "")
            if aid:
                actor_ids.add(aid)
            t = actor.get("ticker", "")
            if t:
                tickers.add(t)
            if actor.get("type") == "committee":
                committees.add(actor.get("label", ""))
            if actor.get("type") == "fund":
                funds.add(actor.get("label", ""))

    return actor_ids, tickers, committees, funds


def _build_path_to_actor(drill_result: dict, actor_id: str) -> list[str]:
    """Reconstruct the path from the root ticker to a specific actor."""
    path = [drill_result.get("ticker", "")]
    for layer in drill_result.get("layers", []):
        for actor in layer.get("actors", []):
            if actor.get("id") == actor_id:
                via = actor.get("connected_via", "")
                if via:
                    path.append(via)
                path.append(actor.get("label", actor_id))
                return path
    # If not found directly, return a minimal path
    return path


def find_overlaps(
    engine: Engine, ticker_a: str, ticker_b: str
) -> list[Overlap]:
    """Drill from two tickers independently, find where the graphs intersect.

    "NVDA and LMT seem unrelated, but Peter Thiel sits on both Palantir's
    board and funds defense tech, and 3 congress members who hold NVDA also
    sit on Armed Services which oversees LMT contracts."

    These hidden connections are the alpha.

    Args:
        engine: SQLAlchemy database engine.
        ticker_a: First ticker symbol.
        ticker_b: Second ticker symbol.

    Returns:
        List of Overlap dataclass instances, sorted by significance.
    """
    ensure_table(engine)
    ticker_a = ticker_a.strip().upper()
    ticker_b = ticker_b.strip().upper()

    log.info("Finding overlaps between {a} and {b}", a=ticker_a, b=ticker_b)

    # Drill both tickers
    drill_a = deep_drill(engine, ticker_a)
    drill_b = deep_drill(engine, ticker_b)

    # Extract actor sets
    ids_a, tickers_a, committees_a, funds_a = _extract_sets(drill_a)
    ids_b, tickers_b, committees_b, funds_b = _extract_sets(drill_b)
    index_a = _extract_actor_index(drill_a)
    index_b = _extract_actor_index(drill_b)

    overlaps: list[Overlap] = []

    # Find actors that appear in BOTH graphs
    shared_actor_ids = ids_a & ids_b
    shared_tickers = sorted(tickers_a & tickers_b - {ticker_a, ticker_b})
    shared_committees = sorted(committees_a & committees_b)
    shared_funds = sorted(funds_a & funds_b)

    for actor_id in shared_actor_ids:
        actor_a_data = index_a.get(actor_id, {})
        actor_b_data = index_b.get(actor_id, {})

        # Skip the root tickers themselves
        if actor_id in (f"company:{ticker_a}", f"company:{ticker_b}"):
            continue

        label = actor_a_data.get("label", actor_b_data.get("label", actor_id))
        path_a = _build_path_to_actor(drill_a, actor_id)
        path_b = _build_path_to_actor(drill_b, actor_id)

        dollar_a = float(actor_a_data.get("dollar_amount", 0) or 0)
        dollar_b = float(actor_b_data.get("dollar_amount", 0) or 0)

        overlap = Overlap(
            actor_a=ticker_a,
            actor_b=ticker_b,
            connection_point=label,
            path_a=path_a,
            path_b=path_b,
            shared_tickers=shared_tickers,
            shared_committees=shared_committees,
            shared_funds=shared_funds,
            total_dollar_flow=dollar_a + dollar_b,
            description=(
                f"{label} appears in both the {ticker_a} and {ticker_b} networks. "
                f"Reached via {' -> '.join(path_a)} from {ticker_a} and "
                f"{' -> '.join(path_b)} from {ticker_b}."
            ),
        )
        overlap.significance = _compute_significance(overlap)
        overlaps.append(overlap)

    # Also create overlaps for shared committees (even if specific actors differ)
    for committee in shared_committees:
        # Find which politicians from each graph sit on this committee
        politicians_a = [
            a.get("label", "") for layer in drill_a.get("layers", [])
            for a in layer.get("actors", [])
            if a.get("type") == "politician"
        ]
        politicians_b = [
            a.get("label", "") for layer in drill_b.get("layers", [])
            for a in layer.get("actors", [])
            if a.get("type") == "politician"
        ]

        overlap = Overlap(
            actor_a=ticker_a,
            actor_b=ticker_b,
            connection_point=f"Committee: {committee}",
            path_a=[ticker_a, "lobbyists", "PAC recipients", committee],
            path_b=[ticker_b, "lobbyists", "PAC recipients", committee],
            shared_tickers=shared_tickers,
            shared_committees=[committee],
            shared_funds=shared_funds,
            description=(
                f"Both {ticker_a} and {ticker_b} have influence paths through "
                f"the {committee} committee. {ticker_a} connects via "
                f"{', '.join(politicians_a[:3]) or 'unknown politicians'}; {ticker_b} via "
                f"{', '.join(politicians_b[:3]) or 'unknown politicians'}."
            ),
        )
        overlap.significance = _compute_significance(overlap)
        # Avoid duplicates — only add if this committee wasn't already covered
        existing_points = {o.connection_point for o in overlaps}
        if overlap.connection_point not in existing_points:
            overlaps.append(overlap)

    # Sort by significance descending
    overlaps.sort(key=lambda o: o.significance, reverse=True)

    # Persist to database
    _store_overlaps(engine, overlaps, ticker_a, ticker_b)

    log.info(
        "Overlap detection complete: {a} <-> {b}, {n} overlaps found, top significance={s}",
        a=ticker_a, b=ticker_b, n=len(overlaps),
        s=overlaps[0].significance if overlaps else 0,
    )

    return overlaps


def find_all_overlaps(
    engine: Engine, tickers: list[str] | None = None
) -> list[Overlap]:
    """Run pairwise overlap detection across all watchlist tickers.

    "Your watchlist has 15 hidden connections you didn't know about."

    Args:
        engine: SQLAlchemy database engine.
        tickers: Optional list of tickers.  If None, uses the active watchlist.

    Returns:
        List of all Overlap instances across all pairs, sorted by significance.
    """
    ensure_table(engine)

    if tickers is None:
        tickers = _get_watchlist_tickers(engine)

    if len(tickers) < 2:
        log.info("find_all_overlaps: need at least 2 tickers, got {n}", n=len(tickers))
        return []

    log.info("Running pairwise overlap detection across {n} tickers", n=len(tickers))

    # Pre-drill all tickers to avoid redundant computation
    drill_cache: dict[str, dict] = {}
    for ticker in tickers:
        ticker = ticker.strip().upper()
        if ticker not in drill_cache:
            drill_cache[ticker] = deep_drill(engine, ticker)

    all_overlaps: list[Overlap] = []

    for ticker_a, ticker_b in combinations(sorted(drill_cache.keys()), 2):
        drill_a = drill_cache[ticker_a]
        drill_b = drill_cache[ticker_b]

        ids_a, tickers_a, committees_a, funds_a = _extract_sets(drill_a)
        ids_b, tickers_b, committees_b, funds_b = _extract_sets(drill_b)
        index_a = _extract_actor_index(drill_a)
        index_b = _extract_actor_index(drill_b)

        shared_actor_ids = ids_a & ids_b
        shared_tickers_set = tickers_a & tickers_b - {ticker_a, ticker_b}
        shared_committees_set = committees_a & committees_b
        shared_funds_set = funds_a & funds_b

        # Skip pairs with zero intersection
        if not shared_actor_ids and not shared_committees_set and not shared_funds_set:
            continue

        for actor_id in shared_actor_ids:
            if actor_id in (f"company:{ticker_a}", f"company:{ticker_b}"):
                continue
            actor_data = index_a.get(actor_id, index_b.get(actor_id, {}))
            label = actor_data.get("label", actor_id)
            path_a = _build_path_to_actor(drill_a, actor_id)
            path_b = _build_path_to_actor(drill_b, actor_id)
            dollar_a = float(index_a.get(actor_id, {}).get("dollar_amount", 0) or 0)
            dollar_b = float(index_b.get(actor_id, {}).get("dollar_amount", 0) or 0)

            overlap = Overlap(
                actor_a=ticker_a,
                actor_b=ticker_b,
                connection_point=label,
                path_a=path_a,
                path_b=path_b,
                shared_tickers=sorted(shared_tickers_set),
                shared_committees=sorted(shared_committees_set),
                shared_funds=sorted(shared_funds_set),
                total_dollar_flow=dollar_a + dollar_b,
                description=(
                    f"{label} connects {ticker_a} and {ticker_b} networks."
                ),
            )
            overlap.significance = _compute_significance(overlap)
            all_overlaps.append(overlap)

    all_overlaps.sort(key=lambda o: o.significance, reverse=True)

    log.info(
        "All-overlaps scan complete: {n} overlaps across {t} tickers",
        n=len(all_overlaps), t=len(tickers),
    )

    return all_overlaps


def generate_connection_map(
    engine: Engine, ticker: str, depth: int = 5
) -> dict[str, Any]:
    """Generate a D3-ready hierarchical graph showing drill layers.

    Nodes are colored by layer depth; overlap nodes (appearing in multiple
    paths) are highlighted.

    Args:
        engine: SQLAlchemy database engine.
        ticker: Root ticker symbol.
        depth: How many layers deep (1-10, default 5).

    Returns:
        {nodes, links, metadata} suitable for D3 force-directed graph.
    """
    drill = deep_drill(engine, ticker, max_depth=depth)

    # D3 color palette by layer depth
    layer_colors = {
        1: "#1f77b4",   # blue — company
        2: "#ff7f0e",   # orange — board/csuite
        3: "#2ca02c",   # green — other affiliations
        4: "#d62728",   # red — lobbyists
        5: "#9467bd",   # purple — politicians
        6: "#8c564b",   # brown — committees
        7: "#e377c2",   # pink — affected companies
        8: "#7f7f7f",   # gray — insiders
        9: "#bcbd22",   # olive — funds
        10: "#17becf",  # cyan — beneficial owners
    }

    nodes: list[dict] = []
    links: list[dict] = []
    node_ids: set[str] = set()

    # Track which actor IDs appear in multiple layers (overlap nodes)
    actor_layer_count: dict[str, int] = defaultdict(int)
    for layer in drill.get("layers", []):
        for actor in layer.get("actors", []):
            aid = actor.get("id", "")
            if aid:
                actor_layer_count[aid] += 1

    for layer in drill.get("layers", []):
        layer_depth = layer.get("depth", 0)
        for actor in layer.get("actors", []):
            aid = actor.get("id", "")
            if not aid or aid in node_ids:
                continue
            node_ids.add(aid)
            is_overlap = actor_layer_count.get(aid, 0) > 1
            nodes.append({
                "id": aid,
                "label": actor.get("label", aid),
                "type": actor.get("type", "unknown"),
                "layer": layer_depth,
                "color": layer_colors.get(layer_depth, "#999999"),
                "size": 15 if layer_depth == 1 else max(5, 12 - layer_depth),
                "is_overlap": is_overlap,
                "dollar_amount": actor.get("dollar_amount", 0),
            })

        for conn in layer.get("connections", []):
            source = conn.get("from", "")
            target = conn.get("to", "")
            if source and target:
                links.append({
                    "source": source,
                    "target": target,
                    "type": conn.get("type", ""),
                    "label": conn.get("label", conn.get("type", "")),
                    "amount": conn.get("amount", 0),
                })

    # Add implicit links from layer 1 -> layer 2 actors
    root_id = f"company:{ticker}"
    for layer in drill.get("layers", []):
        if layer.get("depth") == 2:
            for actor in layer.get("actors", []):
                aid = actor.get("id", "")
                if aid:
                    links.append({
                        "source": root_id,
                        "target": aid,
                        "type": actor.get("connection_type", "associated"),
                        "label": actor.get("title", ""),
                    })

    return {
        "nodes": nodes,
        "links": links,
        "metadata": {
            "root_ticker": ticker,
            "depth": depth,
            "total_nodes": len(nodes),
            "total_links": len(links),
            "overlap_nodes": sum(1 for n in nodes if n.get("is_overlap")),
            "layer_colors": layer_colors,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    }


def discover_hidden_influence(engine: Engine) -> list[dict[str, Any]]:
    """The sleuth equivalent for the graph.

    "These 3 seemingly unrelated events are connected through a shared actor."

    Cross-references deep graph overlaps with causal chains to find hidden
    influence patterns.  Looks for:
        1. Actors who appear in multiple causal chains
        2. Funds that hold positions across connected companies
        3. Politicians on committees that affect multiple watchlist companies
        4. Insider trading clusters that span connected companies

    Returns:
        List of hidden influence discoveries, each with narrative and evidence.
    """
    ensure_table(engine)
    discoveries: list[dict] = []

    tickers = _get_watchlist_tickers(engine)
    if not tickers:
        log.info("discover_hidden_influence: no watchlist tickers")
        return []

    # Phase 1: Find all cross-graph overlaps
    all_overlaps = find_all_overlaps(engine, tickers)

    # Phase 2: Cross-reference with causal chains
    causal_actors: dict[str, list[str]] = defaultdict(list)  # actor -> [tickers]
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT actor, ticker "
                "FROM causal_links "
                "WHERE created_at >= NOW() - INTERVAL '90 days' "
                "ORDER BY created_at DESC "
                "LIMIT 1000"
            )).fetchall()
            for row in rows:
                if row[0] and row[1]:
                    causal_actors[row[0].lower()].append(row[1])
    except Exception as exc:
        log.debug("DeepGraph: causal actors query failed: {e}", e=str(exc))

    # Phase 3: Find actors in causal chains who also appear in overlaps
    for actor_name, affected_tickers in causal_actors.items():
        if len(set(affected_tickers)) < 2:
            continue
        # Check if this actor appears in any overlap
        related_overlaps = [
            o for o in all_overlaps
            if actor_name in o.connection_point.lower()
        ]
        if related_overlaps:
            discoveries.append({
                "type": "causal_overlap",
                "actor": actor_name,
                "affected_tickers": sorted(set(affected_tickers)),
                "overlap_count": len(related_overlaps),
                "total_dollar_flow": sum(o.total_dollar_flow for o in related_overlaps),
                "narrative": (
                    f"{actor_name.title()} appears in causal chains for "
                    f"{', '.join(sorted(set(affected_tickers)))} AND in "
                    f"{len(related_overlaps)} cross-network overlap(s). This actor "
                    f"may be a hidden connector between seemingly unrelated events."
                ),
                "significance": max(o.significance for o in related_overlaps),
                "evidence": [o.to_dict() for o in related_overlaps[:5]],
            })

    # Phase 4: Find committee-based influence
    committee_tickers: dict[str, set[str]] = defaultdict(set)
    for overlap in all_overlaps:
        for committee in overlap.shared_committees:
            committee_tickers[committee].add(overlap.actor_a)
            committee_tickers[committee].add(overlap.actor_b)

    for committee, affected in committee_tickers.items():
        if len(affected) >= 3:
            discoveries.append({
                "type": "committee_influence",
                "committee": committee,
                "affected_tickers": sorted(affected),
                "narrative": (
                    f"The {committee} committee has influence paths to "
                    f"{len(affected)} of your watchlist tickers: "
                    f"{', '.join(sorted(affected))}. Legislation or hearings in "
                    f"this committee could move multiple positions simultaneously."
                ),
                "significance": min(0.9, 0.2 * len(affected)),
            })

    # Phase 5: Find fund concentration risk
    fund_tickers: dict[str, set[str]] = defaultdict(set)
    for overlap in all_overlaps:
        for fund in overlap.shared_funds:
            fund_tickers[fund].add(overlap.actor_a)
            fund_tickers[fund].add(overlap.actor_b)

    for fund, held in fund_tickers.items():
        if len(held) >= 3:
            discoveries.append({
                "type": "fund_concentration",
                "fund": fund,
                "affected_tickers": sorted(held),
                "narrative": (
                    f"{fund} holds significant positions in {len(held)} of your "
                    f"watchlist tickers: {', '.join(sorted(held))}. A large "
                    f"rebalancing or redemption event at this fund could create "
                    f"correlated selling pressure."
                ),
                "significance": min(0.85, 0.15 * len(held)),
            })

    # Phase 6: Summarize the high-significance overlaps themselves
    for overlap in all_overlaps[:10]:
        if overlap.significance >= 0.5:
            discoveries.append({
                "type": "high_significance_overlap",
                "ticker_a": overlap.actor_a,
                "ticker_b": overlap.actor_b,
                "connection_point": overlap.connection_point,
                "significance": overlap.significance,
                "narrative": overlap.description,
                "shared_tickers": overlap.shared_tickers,
                "shared_committees": overlap.shared_committees,
                "shared_funds": overlap.shared_funds,
                "total_dollar_flow": overlap.total_dollar_flow,
            })

    # Sort by significance
    discoveries.sort(key=lambda d: d.get("significance", 0), reverse=True)

    log.info(
        "Hidden influence discovery complete: {n} findings across {t} tickers",
        n=len(discoveries), t=len(tickers),
    )

    return discoveries


# ══════════════════════════════════════════════════════════════════════════
# STORAGE
# ══════════════════════════════════════════════════════════════════════════


def _store_overlaps(
    engine: Engine, overlaps: list[Overlap], ticker_a: str, ticker_b: str
) -> int:
    """Persist detected overlaps to the database.

    Replaces existing overlaps for this ticker pair (snapshot pattern).

    Returns:
        Number of rows stored.
    """
    if not overlaps:
        return 0

    stored = 0
    try:
        with engine.begin() as conn:
            # Clear old data for this pair (in both orderings)
            conn.execute(
                text(
                    "DELETE FROM graph_overlaps "
                    "WHERE (ticker_a = :a AND ticker_b = :b) "
                    "OR (ticker_a = :b AND ticker_b = :a)"
                ),
                {"a": ticker_a, "b": ticker_b},
            )

            for overlap in overlaps:
                conn.execute(
                    text(
                        "INSERT INTO graph_overlaps "
                        "(ticker_a, ticker_b, connection_point, path_a, path_b, "
                        "shared_entities, dollar_flow, significance, description) "
                        "VALUES (:a, :b, :cp, :pa, :pb, :se, :df, :sig, :desc)"
                    ),
                    {
                        "a": ticker_a,
                        "b": ticker_b,
                        "cp": overlap.connection_point,
                        "pa": json.dumps(overlap.path_a),
                        "pb": json.dumps(overlap.path_b),
                        "se": json.dumps({
                            "tickers": overlap.shared_tickers,
                            "committees": overlap.shared_committees,
                            "funds": overlap.shared_funds,
                        }),
                        "df": overlap.total_dollar_flow,
                        "sig": overlap.significance,
                        "desc": overlap.description,
                    },
                )
                stored += 1
    except Exception as exc:
        log.warning("Failed to store overlaps: {e}", e=str(exc))

    log.debug("Stored {n} graph overlaps for {a} <-> {b}", n=stored, a=ticker_a, b=ticker_b)
    return stored
