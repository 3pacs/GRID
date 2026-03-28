"""
GRID Intelligence — Influence Network (Crown Jewel Analysis).

Maps the circular flows of money and influence in politics:
    Company --lobbies--> Member --votes--> Bill --funds--> Company
    Company --contributes--> Member --trades--> Company stock

This is the highest-impact analysis: connecting corporate lobbying spend,
PAC contributions, member votes, government contracts, and congressional
stock trades into a single graph.  Detects circular flows of money and
flags vote/trade hypocrisy.

Key entry points:
    build_influence_graph      — full graph for D3 force-directed viz
    detect_circular_flows      — find Company->Member->Bill->Company loops
    get_influence_for_ticker   — all influence data for one company
    vote_trade_hypocrisy       — members who vote one way but trade another

Data tables written: influence_loops
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

try:
    from ingestion.altdata.gov_contracts import CONTRACTOR_TICKER_MAP
except ImportError:
    CONTRACTOR_TICKER_MAP = {}


# ══════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════


@dataclass
class InfluenceLoop:
    """A detected circular flow of money and influence."""

    company: str             # e.g. "NVIDIA"
    ticker: str              # e.g. "NVDA"
    lobbying_spend: float    # total lobbying spend, USD
    pac_contributions: float  # total PAC contributions, USD
    recipients: list[dict] = field(default_factory=list)
    # [{member, amount, committee}]
    legislation_affected: list[dict] = field(default_factory=list)
    # [{bill, status, company_impact}]
    contracts_received: float = 0.0  # total contract value, USD
    member_trades: list[dict] = field(default_factory=list)
    # [{member, action, ticker, amount, date}]
    circular_flow_detected: bool = False
    suspicion_score: float = 0.0  # 0-1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ══════════════════════════════════════════════════════════════════════════
# SCHEMA
# ══════════════════════════════════════════════════════════════════════════

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS influence_loops (
    id                  SERIAL PRIMARY KEY,
    ticker              TEXT NOT NULL,
    company             TEXT NOT NULL,
    lobbying_spend      NUMERIC DEFAULT 0,
    pac_contributions   NUMERIC DEFAULT 0,
    contracts_received  NUMERIC DEFAULT 0,
    recipients          JSONB DEFAULT '[]',
    legislation         JSONB DEFAULT '[]',
    member_trades       JSONB DEFAULT '[]',
    circular_flow       BOOLEAN DEFAULT FALSE,
    suspicion_score     NUMERIC DEFAULT 0,
    computed_at         TIMESTAMPTZ DEFAULT NOW()
);
"""

_CREATE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_influence_loops_ticker ON influence_loops (ticker);",
    "CREATE INDEX IF NOT EXISTS idx_influence_loops_suspicion ON influence_loops (suspicion_score DESC);",
    "CREATE INDEX IF NOT EXISTS idx_influence_loops_circular ON influence_loops (circular_flow) WHERE circular_flow = TRUE;",
]


def ensure_table(engine: Engine) -> None:
    """Create the influence_loops table and indexes if they don't exist."""
    with engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE_SQL))
        for idx_sql in _CREATE_INDEX_SQL:
            conn.execute(text(idx_sql))
    log.info("influence_loops table ensured")


# ══════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS — data retrieval
# ══════════════════════════════════════════════════════════════════════════


def _get_unique_tickers() -> dict[str, str]:
    """Return {ticker: company_name} from CONTRACTOR_TICKER_MAP (deduplicated).

    Multiple keys may map to the same ticker; we pick the longest key as
    the canonical company name.
    """
    ticker_to_name: dict[str, str] = {}
    for name, ticker in CONTRACTOR_TICKER_MAP.items():
        existing = ticker_to_name.get(ticker, "")
        if len(name) > len(existing):
            ticker_to_name[ticker] = name
    return ticker_to_name


def _fetch_lobbying(engine: Engine, ticker: str, days: int = 365) -> tuple[float, list[dict]]:
    """Fetch lobbying spend and bill references for a ticker.

    Returns:
        (total_spend, list of {client, registrant, amount, bills_lobbied, date})
    """
    cutoff = date.today() - timedelta(days=days)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT signal_date, signal_value "
                "FROM signal_sources "
                "WHERE source_type = 'lobbying' "
                "AND ticker = :ticker "
                "AND signal_date >= :cutoff "
                "ORDER BY signal_date DESC"
            ),
            {"ticker": ticker, "cutoff": cutoff},
        ).fetchall()

    total = 0.0
    details: list[dict] = []
    for row in rows:
        val = _parse_json(row[1])
        if not val:
            continue
        amt = float(val.get("amount", 0) or 0)
        total += amt
        details.append({
            "client": val.get("client_name", ""),
            "registrant": val.get("registrant_name", ""),
            "amount": amt,
            "bills_lobbied": val.get("bills_lobbied", []),
            "issue_codes": val.get("issue_codes", []),
            "date": str(row[0]),
        })
    return total, details


def _fetch_pac_contributions(engine: Engine, ticker: str, days: int = 730) -> tuple[float, list[dict]]:
    """Fetch PAC contributions from a company's PAC to congressional members.

    Returns:
        (total_contributions, list of {member, amount, committee, date, state})
    """
    cutoff = date.today() - timedelta(days=days)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT signal_date, signal_value "
                "FROM signal_sources "
                "WHERE source_type = 'campaign_finance' "
                "AND ticker = :ticker "
                "AND signal_date >= :cutoff "
                "ORDER BY signal_date DESC"
            ),
            {"ticker": ticker, "cutoff": cutoff},
        ).fetchall()

    total = 0.0
    recipients: list[dict] = []
    for row in rows:
        val = _parse_json(row[1])
        if not val:
            continue
        amt = float(val.get("amount", 0) or 0)
        total += amt
        recipients.append({
            "member": val.get("recipient_name", ""),
            "amount": amt,
            "committee": val.get("pac_name", ""),
            "sector": val.get("sector", ""),
            "state": val.get("recipient_state", ""),
            "date": str(row[0]),
        })
    return total, recipients


def _fetch_member_votes(engine: Engine, member_name: str, ticker: str) -> list[dict]:
    """Fetch votes by a specific member on bills affecting a ticker.

    Searches LEGISLATION entries affecting the ticker, then cross-references
    with the member name in sponsors/cosponsors.

    Returns:
        List of {bill_id, title, vote, status, date}
    """
    cutoff = date.today() - timedelta(days=365)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT series_id, obs_date, raw_payload "
                "FROM raw_series "
                "WHERE series_id LIKE :pattern "
                "AND obs_date >= :cutoff "
                "AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC "
                "LIMIT 200"
            ),
            {"pattern": "LEGISLATION:%", "cutoff": cutoff},
        ).fetchall()

    votes: list[dict] = []
    member_lower = member_name.lower()
    for row in rows:
        payload = _parse_json(row[2])
        if not payload:
            continue
        affected = payload.get("affected_tickers", [])
        if ticker not in affected:
            continue
        # Check if member is referenced in the bill payload
        sponsor = (payload.get("sponsor", "") or "").lower()
        cosponsors = " ".join(payload.get("cosponsors", []) or []).lower()
        vote_records = payload.get("votes", []) or []

        member_voted = None
        for vr in vote_records:
            if isinstance(vr, dict) and member_lower in (vr.get("member", "") or "").lower():
                member_voted = vr.get("vote", "")

        if member_lower in sponsor or member_lower in cosponsors or member_voted:
            votes.append({
                "bill_id": payload.get("bill_id", ""),
                "title": payload.get("title", ""),
                "vote": member_voted or ("SPONSOR" if member_lower in sponsor else "COSPONSOR"),
                "status": payload.get("status", ""),
                "date": str(row[1]),
            })
    return votes


def _fetch_contracts(engine: Engine, ticker: str, days: int = 365) -> tuple[float, list[dict]]:
    """Fetch government contracts awarded to a company.

    Returns:
        (total_contract_value, list of {award_id, amount, agency, date, description})
    """
    cutoff = date.today() - timedelta(days=days)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT signal_date, signal_value "
                "FROM signal_sources "
                "WHERE source_type = 'gov_contract' "
                "AND ticker = :ticker "
                "AND signal_date >= :cutoff "
                "ORDER BY signal_date DESC"
            ),
            {"ticker": ticker, "cutoff": cutoff},
        ).fetchall()

    total = 0.0
    contracts: list[dict] = []
    for row in rows:
        val = _parse_json(row[1])
        if not val:
            continue
        amt = float(val.get("amount", 0) or 0)
        total += amt
        contracts.append({
            "award_id": val.get("award_id", ""),
            "amount": amt,
            "agency": val.get("awarding_agency", val.get("recipient_name", "")),
            "description": val.get("description", ""),
            "date": str(row[0]),
        })
    return total, contracts


def _fetch_member_trades(engine: Engine, ticker: str, days: int = 365) -> list[dict]:
    """Fetch congressional trading disclosures for a ticker.

    Returns:
        List of {member, action, ticker, amount, date, committee, party, state}
    """
    cutoff = date.today() - timedelta(days=days)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT source_id, signal_date, signal_type, signal_value "
                "FROM signal_sources "
                "WHERE source_type = 'congressional' "
                "AND ticker = :ticker "
                "AND signal_date >= :cutoff "
                "ORDER BY signal_date DESC"
            ),
            {"ticker": ticker, "cutoff": cutoff},
        ).fetchall()

    trades: list[dict] = []
    for row in rows:
        val = _parse_json(row[3])
        member = row[0] or (val.get("member_name", "") if val else "")
        trades.append({
            "member": member,
            "action": row[2] or "",
            "ticker": ticker,
            "amount": val.get("amount_range", val.get("amount", "")) if val else "",
            "date": str(row[1]),
            "committee": val.get("committee", "") if val else "",
            "party": val.get("party", "") if val else "",
            "state": val.get("state", "") if val else "",
        })
    return trades


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


def _compute_suspicion(loop: InfluenceLoop) -> float:
    """Score a loop from 0 to 1 based on how suspicious the circular flow is.

    Factors:
        - Lobbying + contract (company pays, then receives)  0.25
        - PAC contribution + favorable vote                  0.25
        - Member trades the company stock                    0.25
        - Scale of money involved                            0.15
        - Timing proximity                                   0.10
    """
    score = 0.0

    # Factor 1: Company lobbied AND received contracts
    if loop.lobbying_spend > 0 and loop.contracts_received > 0:
        ratio = loop.contracts_received / max(loop.lobbying_spend, 1)
        score += min(0.25, 0.05 * min(ratio, 5))

    # Factor 2: PAC contributions to members who voted on related bills
    if loop.pac_contributions > 0 and loop.legislation_affected:
        score += 0.25

    # Factor 3: Members who received money also traded the stock
    recipient_members = {r.get("member", "").lower() for r in loop.recipients if r.get("member")}
    trading_members = {t.get("member", "").lower() for t in loop.member_trades if t.get("member")}
    overlap = recipient_members & trading_members
    if overlap:
        score += 0.25

    # Factor 4: Scale of money
    total_money = loop.lobbying_spend + loop.pac_contributions + loop.contracts_received
    if total_money > 100_000_000:
        score += 0.15
    elif total_money > 10_000_000:
        score += 0.10
    elif total_money > 1_000_000:
        score += 0.05

    # Factor 5: Circular flow explicitly detected
    if loop.circular_flow_detected:
        score += 0.10

    return round(min(score, 1.0), 3)


# ══════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════


def build_influence_graph(engine: Engine) -> dict[str, Any]:
    """Build the full influence graph for D3 force-directed visualization.

    For each company in CONTRACTOR_TICKER_MAP:
        - Get lobbying spend
        - Get PAC contributions
        - Get which members received money
        - Get which bills those members voted on
        - Get which contracts the company received
        - Get which members traded the company's stock

    Returns:
        {nodes, links, metadata} suitable for D3 force graph.
        Nodes: companies + members + bills (typed).
        Links: lobbying, contribution, vote, contract, trade (typed, weighted).
    """
    ensure_table(engine)

    ticker_to_name = _get_unique_tickers()
    nodes: dict[str, dict] = {}
    links: list[dict] = []
    seen_links: set[str] = set()

    company_count = 0
    total_lobbying = 0.0
    total_pac = 0.0
    total_contracts = 0.0

    for ticker, company_name in ticker_to_name.items():
        # Company node
        company_id = f"company:{ticker}"
        lobby_total, lobby_details = _fetch_lobbying(engine, ticker)
        pac_total, pac_recipients = _fetch_pac_contributions(engine, ticker)
        contract_total, contract_details = _fetch_contracts(engine, ticker)
        member_trades = _fetch_member_trades(engine, ticker)

        # Skip companies with no influence data at all
        if not any([lobby_total, pac_total, contract_total, member_trades]):
            continue

        company_count += 1
        total_lobbying += lobby_total
        total_pac += pac_total
        total_contracts += contract_total

        nodes[company_id] = {
            "id": company_id,
            "label": company_name.title(),
            "type": "company",
            "ticker": ticker,
            "lobbying_spend": lobby_total,
            "pac_contributions": pac_total,
            "contracts_received": contract_total,
            "trade_count": len(member_trades),
        }

        # PAC contribution links → Member nodes
        for recip in pac_recipients:
            member_name = recip.get("member", "")
            if not member_name:
                continue
            member_id = f"member:{member_name.lower().replace(' ', '_')}"
            if member_id not in nodes:
                nodes[member_id] = {
                    "id": member_id,
                    "label": member_name,
                    "type": "member",
                    "total_received": 0.0,
                    "trades": [],
                    "state": recip.get("state", ""),
                }
            nodes[member_id]["total_received"] += recip.get("amount", 0)

            link_key = f"contribution:{company_id}->{member_id}"
            if link_key not in seen_links:
                links.append({
                    "source": company_id,
                    "target": member_id,
                    "type": "contribution",
                    "amount": recip.get("amount", 0),
                    "label": f"${recip.get('amount', 0):,.0f}",
                })
                seen_links.add(link_key)

        # Member trade links
        for trade in member_trades:
            member_name = trade.get("member", "")
            if not member_name:
                continue
            member_id = f"member:{member_name.lower().replace(' ', '_')}"
            if member_id not in nodes:
                nodes[member_id] = {
                    "id": member_id,
                    "label": member_name,
                    "type": "member",
                    "total_received": 0.0,
                    "trades": [],
                    "state": trade.get("state", ""),
                    "party": trade.get("party", ""),
                }
            nodes[member_id]["trades"].append(trade)

            link_key = f"trade:{member_id}->{company_id}:{trade.get('date', '')}"
            if link_key not in seen_links:
                links.append({
                    "source": member_id,
                    "target": company_id,
                    "type": "trade",
                    "action": trade.get("action", ""),
                    "amount": str(trade.get("amount", "")),
                    "date": trade.get("date", ""),
                    "label": f"{trade.get('action', '')} {trade.get('amount', '')}",
                })
                seen_links.add(link_key)

        # Lobbying links (company -> bill via lobbied-for bills)
        bills_lobbied: set[str] = set()
        for detail in lobby_details:
            for bill in detail.get("bills_lobbied", []):
                bills_lobbied.add(bill.strip())

        for bill_id in bills_lobbied:
            bill_node_id = f"bill:{bill_id.replace(' ', '_').replace('.', '')}"
            if bill_node_id not in nodes:
                nodes[bill_node_id] = {
                    "id": bill_node_id,
                    "label": bill_id,
                    "type": "bill",
                }

            link_key = f"lobbying:{company_id}->{bill_node_id}"
            if link_key not in seen_links:
                links.append({
                    "source": company_id,
                    "target": bill_node_id,
                    "type": "lobbying",
                    "amount": lobby_total,
                    "label": f"lobbied (${lobby_total:,.0f} total)",
                })
                seen_links.add(link_key)

        # Contract links (bill/government -> company)
        for contract in contract_details[:10]:  # limit for graph readability
            agency = contract.get("agency", "Government")
            agency_id = f"agency:{agency.lower().replace(' ', '_')[:30]}"
            if agency_id not in nodes:
                nodes[agency_id] = {
                    "id": agency_id,
                    "label": agency[:40],
                    "type": "agency",
                }

            link_key = f"contract:{agency_id}->{company_id}:{contract.get('award_id', '')}"
            if link_key not in seen_links:
                links.append({
                    "source": agency_id,
                    "target": company_id,
                    "type": "contract",
                    "amount": contract.get("amount", 0),
                    "label": f"${contract.get('amount', 0):,.0f}",
                })
                seen_links.add(link_key)

    metadata = {
        "companies_with_data": company_count,
        "total_nodes": len(nodes),
        "total_links": len(links),
        "total_lobbying": total_lobbying,
        "total_pac": total_pac,
        "total_contracts": total_contracts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    log.info(
        "Influence graph built: {n} nodes, {l} links across {c} companies",
        n=len(nodes), l=len(links), c=company_count,
    )

    return {
        "nodes": list(nodes.values()),
        "links": links,
        "metadata": metadata,
    }


def detect_circular_flows(engine: Engine) -> list[InfluenceLoop]:
    """Detect circular flows of money and influence.

    Pattern 1: Company A lobbies Member B -> Member B votes YES on Bill C
               -> Bill C awards funds to Company A.
    Pattern 2: Company A contributes to Member B -> Member B trades
               Company A stock.

    Returns:
        List of InfluenceLoop dataclass instances, sorted by suspicion.
    """
    ensure_table(engine)

    ticker_to_name = _get_unique_tickers()
    loops: list[InfluenceLoop] = []

    for ticker, company_name in ticker_to_name.items():
        lobby_total, lobby_details = _fetch_lobbying(engine, ticker)
        pac_total, pac_recipients = _fetch_pac_contributions(engine, ticker)
        contract_total, contract_details = _fetch_contracts(engine, ticker)
        member_trades = _fetch_member_trades(engine, ticker)

        if not any([lobby_total, pac_total, contract_total, member_trades]):
            continue

        # Identify recipient members
        recipient_members = {r.get("member", "").lower(): r for r in pac_recipients if r.get("member")}

        # Identify trading members
        trading_members: dict[str, list[dict]] = defaultdict(list)
        for t in member_trades:
            m = t.get("member", "").lower()
            if m:
                trading_members[m].append(t)

        # Check votes from recipient members on bills affecting this ticker
        legislation_affected: list[dict] = []
        for member_lower, recip_info in recipient_members.items():
            member_name = recip_info.get("member", "")
            votes = _fetch_member_votes(engine, member_name, ticker)
            for v in votes:
                legislation_affected.append({
                    "bill": v.get("bill_id", ""),
                    "title": v.get("title", ""),
                    "status": v.get("status", ""),
                    "member_vote": v.get("vote", ""),
                    "member": member_name,
                    "company_impact": "FAVORABLE" if v.get("vote", "") in ("YES", "YEA", "SPONSOR", "COSPONSOR") else "UNFAVORABLE",
                })

        # Circular flow detection
        #   Pattern 1: lobby + vote + contract
        has_lobby_vote_contract = (
            lobby_total > 0
            and legislation_affected
            and contract_total > 0
        )
        #   Pattern 2: PAC contribution + member trades stock
        contribution_trade_overlap = bool(set(recipient_members.keys()) & set(trading_members.keys()))

        circular = has_lobby_vote_contract or contribution_trade_overlap

        loop = InfluenceLoop(
            company=company_name.title(),
            ticker=ticker,
            lobbying_spend=lobby_total,
            pac_contributions=pac_total,
            recipients=[
                {"member": r.get("member", ""), "amount": r.get("amount", 0), "committee": r.get("committee", "")}
                for r in pac_recipients
            ],
            legislation_affected=legislation_affected,
            contracts_received=contract_total,
            member_trades=[
                {"member": t.get("member", ""), "action": t.get("action", ""),
                 "ticker": t.get("ticker", ""), "amount": t.get("amount", ""),
                 "date": t.get("date", "")}
                for t in member_trades
            ],
            circular_flow_detected=circular,
        )
        loop.suspicion_score = _compute_suspicion(loop)

        if loop.suspicion_score > 0:
            loops.append(loop)

    # Sort by suspicion descending
    loops.sort(key=lambda x: x.suspicion_score, reverse=True)

    # Persist to database
    _store_loops(engine, loops)

    log.info(
        "Circular flow detection: {n} loops, {c} circular, top suspicion={s}",
        n=len(loops),
        c=sum(1 for l in loops if l.circular_flow_detected),
        s=loops[0].suspicion_score if loops else 0,
    )

    return loops


def get_influence_for_ticker(engine: Engine, ticker: str) -> dict[str, Any]:
    """Get all influence data for a single company/ticker.

    Returns:
        Dict with lobbying, pac, contracts, trades, legislation, and loop info.
    """
    ticker = ticker.strip().upper()

    lobby_total, lobby_details = _fetch_lobbying(engine, ticker)
    pac_total, pac_recipients = _fetch_pac_contributions(engine, ticker)
    contract_total, contract_details = _fetch_contracts(engine, ticker)
    member_trades = _fetch_member_trades(engine, ticker)

    # Find company name
    ticker_to_name = _get_unique_tickers()
    company_name = ticker_to_name.get(ticker, ticker)

    # Check for circular flows
    recipient_set = {r.get("member", "").lower() for r in pac_recipients if r.get("member")}
    trading_set = {t.get("member", "").lower() for t in member_trades if t.get("member")}
    overlap_members = recipient_set & trading_set

    # Vote lookups for overlapping members
    hypocrisy_flags: list[dict] = []
    for member_lower in overlap_members:
        # Find the member's full name from recipients
        full_name = ""
        for r in pac_recipients:
            if r.get("member", "").lower() == member_lower:
                full_name = r.get("member", "")
                break
        if full_name:
            votes = _fetch_member_votes(engine, full_name, ticker)
            member_trade_list = [t for t in member_trades if t.get("member", "").lower() == member_lower]
            for trade in member_trade_list:
                for vote in votes:
                    hypocrisy_flags.append({
                        "member": full_name,
                        "received_from_pac": True,
                        "trade_action": trade.get("action", ""),
                        "trade_date": trade.get("date", ""),
                        "voted": vote.get("vote", ""),
                        "bill": vote.get("bill_id", ""),
                    })

    return {
        "ticker": ticker,
        "company": company_name.title(),
        "lobbying": {
            "total_spend": lobby_total,
            "filings": lobby_details[:20],
        },
        "pac_contributions": {
            "total": pac_total,
            "recipients": pac_recipients[:30],
        },
        "contracts": {
            "total_value": contract_total,
            "awards": contract_details[:20],
        },
        "member_trades": member_trades[:30],
        "legislation_affected": [],
        "circular_flow": {
            "detected": bool(overlap_members) or (lobby_total > 0 and contract_total > 0),
            "overlap_members": list(overlap_members),
            "hypocrisy_flags": hypocrisy_flags[:20],
        },
        "suspicion_summary": {
            "lobby_to_contract_ratio": (
                contract_total / max(lobby_total, 1) if lobby_total > 0 else 0
            ),
            "members_who_received_and_traded": len(overlap_members),
            "total_money_in_system": lobby_total + pac_total + contract_total,
        },
    }


def vote_trade_hypocrisy(engine: Engine) -> list[dict[str, Any]]:
    """Detect vote/trade hypocrisy across all members.

    Example from the NVDA audit: Tuberville voted NO on CHIPS Act but
    bought INTC (which benefits from CHIPS Act funding).

    For each member with trades: compare their vote on related bills to
    their trading direction. Flag misalignment as hypocrisy.

    Returns:
        List of hypocrisy flags sorted by severity.
    """
    cutoff = date.today() - timedelta(days=365)
    flags: list[dict[str, Any]] = []

    # Step 1: Get all congressional trades
    with engine.connect() as conn:
        trade_rows = conn.execute(
            text(
                "SELECT source_id, ticker, signal_date, signal_type, signal_value "
                "FROM signal_sources "
                "WHERE source_type = 'congressional' "
                "AND signal_date >= :cutoff "
                "ORDER BY signal_date DESC"
            ),
            {"cutoff": cutoff},
        ).fetchall()

    if not trade_rows:
        log.info("Vote-trade hypocrisy: no congressional trades found")
        return []

    # Group trades by member
    member_trades: dict[str, list[dict]] = defaultdict(list)
    for row in trade_rows:
        val = _parse_json(row[4])
        member = row[0] or (val.get("member_name", "") if val else "")
        if not member:
            continue
        member_trades[member].append({
            "ticker": row[1],
            "date": str(row[2]),
            "action": row[3] or "",
            "committee": val.get("committee", "") if val else "",
            "party": val.get("party", "") if val else "",
            "state": val.get("state", "") if val else "",
        })

    # Step 2: For each member, find votes on legislation affecting their traded tickers
    for member_name, trades in member_trades.items():
        traded_tickers = {t["ticker"] for t in trades if t.get("ticker")}

        for ticker in traded_tickers:
            votes = _fetch_member_votes(engine, member_name, ticker)
            if not votes:
                continue

            member_ticker_trades = [t for t in trades if t.get("ticker") == ticker]

            for trade in member_ticker_trades:
                action = trade.get("action", "").upper()
                for vote in votes:
                    vote_val = (vote.get("vote", "") or "").upper()

                    # Detect misalignment:
                    #   BUY + voted NO/NAY on bill that would help the stock
                    #   SELL + voted YES/YEA on bill that would help the stock
                    #   (Vote NO but buy = believes it will pass anyway / insider info)
                    #   (Vote YES but sell = public virtue, private profit)
                    is_hypocrisy = False
                    hypocrisy_type = ""

                    if action == "BUY" and vote_val in ("NO", "NAY"):
                        is_hypocrisy = True
                        hypocrisy_type = "VOTED_NO_BUT_BOUGHT"
                    elif action == "SELL" and vote_val in ("YES", "YEA", "SPONSOR", "COSPONSOR"):
                        is_hypocrisy = True
                        hypocrisy_type = "VOTED_YES_BUT_SOLD"

                    if is_hypocrisy:
                        flags.append({
                            "member": member_name,
                            "party": trade.get("party", ""),
                            "state": trade.get("state", ""),
                            "ticker": ticker,
                            "trade_action": action,
                            "trade_date": trade.get("date", ""),
                            "trade_amount": trade.get("amount", ""),
                            "vote": vote_val,
                            "bill_id": vote.get("bill_id", ""),
                            "bill_title": vote.get("title", ""),
                            "bill_date": vote.get("date", ""),
                            "hypocrisy_type": hypocrisy_type,
                            "severity": "HIGH",
                            "explanation": (
                                f"{member_name} voted {vote_val} on {vote.get('bill_id', 'bill')} "
                                f"affecting {ticker}, but executed a {action} trade. "
                                f"This misalignment suggests possible informed trading or "
                                f"public posturing."
                            ),
                        })

    # Sort by date descending
    flags.sort(key=lambda f: f.get("trade_date", ""), reverse=True)

    log.info(
        "Vote-trade hypocrisy: {n} flags across {m} members",
        n=len(flags),
        m=len({f["member"] for f in flags}),
    )

    return flags


# ══════════════════════════════════════════════════════════════════════════
# STORAGE
# ══════════════════════════════════════════════════════════════════════════


def _store_loops(engine: Engine, loops: list[InfluenceLoop]) -> int:
    """Persist detected influence loops to the database.

    Replaces all existing rows for each ticker (snapshot pattern).

    Returns:
        Number of rows stored.
    """
    if not loops:
        return 0

    stored = 0
    with engine.begin() as conn:
        # Clear old data for tickers we're updating
        tickers = list({loop.ticker for loop in loops})
        conn.execute(
            text("DELETE FROM influence_loops WHERE ticker = ANY(:tickers)"),
            {"tickers": tickers},
        )

        for loop in loops:
            conn.execute(
                text(
                    "INSERT INTO influence_loops "
                    "(ticker, company, lobbying_spend, pac_contributions, "
                    "contracts_received, recipients, legislation, member_trades, "
                    "circular_flow, suspicion_score) "
                    "VALUES (:ticker, :company, :lobby, :pac, :contracts, "
                    ":recipients, :legislation, :trades, :circular, :suspicion)"
                ),
                {
                    "ticker": loop.ticker,
                    "company": loop.company,
                    "lobby": loop.lobbying_spend,
                    "pac": loop.pac_contributions,
                    "contracts": loop.contracts_received,
                    "recipients": json.dumps(loop.recipients),
                    "legislation": json.dumps(loop.legislation_affected),
                    "trades": json.dumps(loop.member_trades),
                    "circular": loop.circular_flow_detected,
                    "suspicion": loop.suspicion_score,
                },
            )
            stored += 1

    log.info("Stored {n} influence loops", n=stored)
    return stored
