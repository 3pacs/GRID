"""
GRID — Enrich actor connections and wealth_flows from signal_data.

Creates connections between:
1. Insiders and the companies they trade (insider_trade)
2. Congress members and companies they trade (congressional_trade)
3. Companies that share the same insider/congress trader (co_traded)
4. Companies with government contracts (gov_contract)

Also populates wealth_flows with dollar amounts from insider/congressional trades.

Usage:
    python -m scripts.enrich_connections
"""
from __future__ import annotations

import sys
from collections import defaultdict

from loguru import logger as log
from sqlalchemy import text

from api.dependencies import get_db_engine


def _ensure_actor(conn, actor_id: str, name: str, category: str, tier: str = "individual"):
    """Upsert an actor into the actors table."""
    conn.execute(text("""
        INSERT INTO actors (id, name, tier, category, influence_score, trust_score, source, updated_at)
        VALUES (:id, :name, :tier, :cat, 0.5, 0.5, 'signal_enrichment', NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            updated_at = NOW()
    """), {"id": actor_id, "name": name, "tier": tier, "cat": category})


def _upsert_connection(conn, actor_a: str, actor_b: str, relationship: str, strength: float):
    """Upsert a connection."""
    conn.execute(text("""
        INSERT INTO actor_connections (actor_a, actor_b, relationship, strength, discovered_at)
        VALUES (:a, :b, :rel, :s, NOW())
        ON CONFLICT (actor_a, actor_b, relationship) DO UPDATE SET
            strength = GREATEST(actor_connections.strength, EXCLUDED.strength),
            discovered_at = NOW()
    """), {"a": actor_a, "b": actor_b, "rel": relationship, "s": strength})


def _upsert_flow(conn, from_actor: str, to_entity: str, amount: float,
                 confidence: str, flow_date, implication: str):
    """Insert a wealth flow."""
    conn.execute(text("""
        INSERT INTO wealth_flows (from_actor, to_entity, amount_estimate, confidence, flow_date, implication)
        VALUES (:fa, :te, :amt, :conf, :fd, :imp)
    """), {
        "fa": from_actor, "te": to_entity, "amt": amount,
        "conf": confidence, "fd": flow_date, "imp": implication,
    })


def enrich_insider_connections(engine) -> dict:
    """Create connections from insider trading signals."""
    stats = {"actors_created": 0, "connections": 0, "flows": 0}

    with engine.begin() as conn:
        # Get all insider signals with actor + ticker
        rows = conn.execute(text("""
            SELECT DISTINCT actor, ticker, direction, magnitude, signal_date, confidence
            FROM signal_data
            WHERE signal_type IN ('insider', 'quiverquant:insider')
            AND actor IS NOT NULL AND ticker IS NOT NULL
            AND actor != '' AND ticker != ''
            ORDER BY signal_date DESC
        """)).fetchall()

        log.info("Processing {} insider signals", len(rows))

        # Track which insiders trade which tickers
        insider_tickers: dict[str, set[str]] = defaultdict(set)
        ticker_insiders: dict[str, set[str]] = defaultdict(set)

        for row in rows:
            actor_name, ticker, direction, magnitude, signal_date, confidence = row
            actor_id = f"ins_{actor_name.lower().replace(' ', '_').replace('.', '')[:40]}"
            corp_id = f"corp_{ticker.upper()}"

            # Ensure both actors exist
            _ensure_actor(conn, actor_id, actor_name, "insider")
            _ensure_actor(conn, corp_id, f"{ticker.upper()} Corp", "corporation", "institutional")
            stats["actors_created"] += 2

            # Insider → Company connection
            strength = min(0.9, 0.5 + (float(magnitude or 0) / 10_000_000))
            _upsert_connection(conn, actor_id, corp_id, "insider_trade", strength)
            stats["connections"] += 1

            # Wealth flow
            if magnitude and float(magnitude) > 0:
                is_buy = "buy" in (direction or "").lower()
                imp = f"Insider {'bought' if is_buy else 'sold'} ${float(magnitude):,.0f} of {ticker}"
                _upsert_flow(conn, actor_id, corp_id, float(magnitude),
                             confidence or "derived", signal_date, imp)
                stats["flows"] += 1

            insider_tickers[actor_id].add(ticker)
            ticker_insiders[ticker].add(actor_id)

        # Co-traded: companies that share 2+ insider traders
        co_traded = 0
        tickers = list(ticker_insiders.keys())
        for i, t1 in enumerate(tickers):
            for t2 in tickers[i+1:]:
                shared = ticker_insiders[t1] & ticker_insiders[t2]
                if len(shared) >= 2:
                    c1 = f"corp_{t1.upper()}"
                    c2 = f"corp_{t2.upper()}"
                    _upsert_connection(conn, c1, c2, "co_traded_insider",
                                       min(0.9, 0.3 + 0.1 * len(shared)))
                    co_traded += 1
                    if co_traded >= 500:
                        break
            if co_traded >= 500:
                break

        stats["co_traded"] = co_traded
        log.info("Insider enrichment: {s}", s=stats)
    return stats


def enrich_congressional_connections(engine) -> dict:
    """Create connections from congressional trading signals."""
    stats = {"actors_created": 0, "connections": 0, "flows": 0}

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT actor, ticker, direction, magnitude, signal_date, confidence
            FROM signal_data
            WHERE signal_type IN ('congressional', 'quiverquant:house', 'quiverquant:senate')
            AND actor IS NOT NULL AND ticker IS NOT NULL
            AND actor != '' AND ticker != ''
            ORDER BY signal_date DESC
        """)).fetchall()

        log.info("Processing {} congressional signals", len(rows))

        congress_tickers: dict[str, set[str]] = defaultdict(set)
        ticker_congress: dict[str, set[str]] = defaultdict(set)

        for row in rows:
            actor_name, ticker, direction, magnitude, signal_date, confidence = row
            actor_id = f"pol_{actor_name.lower().replace(' ', '_').replace('.', '')[:40]}"
            corp_id = f"corp_{ticker.upper()}"

            _ensure_actor(conn, actor_id, actor_name, "politician")
            _ensure_actor(conn, corp_id, f"{ticker.upper()} Corp", "corporation", "institutional")
            stats["actors_created"] += 2

            strength = min(0.9, 0.5 + (float(magnitude or 0) / 1_000_000))
            _upsert_connection(conn, actor_id, corp_id, "congressional_trade", strength)
            stats["connections"] += 1

            if magnitude and float(magnitude) > 0:
                is_buy = "buy" in (direction or "").lower() or "purchase" in (direction or "").lower()
                imp = f"Congress member {'bought' if is_buy else 'sold'} ${float(magnitude):,.0f} of {ticker}"
                _upsert_flow(conn, actor_id, corp_id, float(magnitude),
                             confidence or "derived", signal_date, imp)
                stats["flows"] += 1

            congress_tickers[actor_id].add(ticker)
            ticker_congress[ticker].add(actor_id)

        # Co-traded by congress: companies traded by same congress member
        co_traded = 0
        for member_id, tickers in congress_tickers.items():
            tickers_list = list(tickers)
            for i, t1 in enumerate(tickers_list):
                for t2 in tickers_list[i+1:]:
                    c1 = f"corp_{t1.upper()}"
                    c2 = f"corp_{t2.upper()}"
                    _upsert_connection(conn, c1, c2, "co_traded_congress",
                                       min(0.8, 0.3 + 0.05 * len(tickers)))
                    co_traded += 1
                    if co_traded >= 1000:
                        break
                if co_traded >= 1000:
                    break
            if co_traded >= 1000:
                break

        stats["co_traded"] = co_traded
        log.info("Congressional enrichment: {s}", s=stats)
    return stats


def enrich_gov_contracts(engine) -> dict:
    """Create connections from government contract signals."""
    stats = {"connections": 0, "flows": 0}

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT ticker, data->>'Amount' as amount, data->>'Agency' as agency, signal_date
            FROM signal_data
            WHERE signal_type IN ('quiverquant:gov_contracts', 'gov_contract')
            AND ticker IS NOT NULL AND ticker != ''
            AND data->>'Amount' IS NOT NULL
            ORDER BY (data->>'Amount')::numeric DESC
            LIMIT 500
        """)).fetchall()

        log.info("Processing {} gov contract signals", len(rows))

        agency_companies: dict[str, set[str]] = defaultdict(set)

        for row in rows:
            ticker, amount_str, agency, signal_date = row
            corp_id = f"corp_{ticker.upper()}"
            amount = float(amount_str) if amount_str else 0

            _ensure_actor(conn, corp_id, f"{ticker.upper()} Corp", "corporation", "institutional")

            if agency:
                agency_id = f"gov_{agency.lower().replace(' ', '_')[:40]}"
                _ensure_actor(conn, agency_id, agency, "government", "sovereign")
                _upsert_connection(conn, agency_id, corp_id, "gov_contract",
                                   min(0.9, 0.3 + amount / 1e10))
                stats["connections"] += 1
                agency_companies[agency_id].add(corp_id)

            if amount > 0:
                _upsert_flow(conn, "us_government", corp_id, amount,
                             "confirmed", signal_date,
                             f"Gov contract ${amount:,.0f} to {ticker}")
                stats["flows"] += 1

        # Companies sharing the same agency → co_contractor
        co = 0
        for agency_id, corps in agency_companies.items():
            corps_list = list(corps)
            for i, c1 in enumerate(corps_list):
                for c2 in corps_list[i+1:]:
                    _upsert_connection(conn, c1, c2, "co_contractor", 0.4)
                    co += 1
                    if co >= 500:
                        break
                if co >= 500:
                    break
            if co >= 500:
                break

        stats["co_contractor"] = co
        log.info("Gov contract enrichment: {s}", s=stats)
    return stats


def enrich_lobbying(engine) -> dict:
    """Create connections from lobbying signals."""
    stats = {"connections": 0}

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT ticker, data->>'Client' as client, data->>'Amount' as amount
            FROM signal_data
            WHERE signal_type = 'quiverquant:lobbying'
            AND ticker IS NOT NULL AND ticker != ''
            LIMIT 2000
        """)).fetchall()

        log.info("Processing {} lobbying signals", len(rows))

        for row in rows:
            ticker, client, amount_str = row
            if not client:
                continue

            corp_id = f"corp_{ticker.upper()}"
            lobby_id = f"lobby_{client.lower().replace(' ', '_')[:40]}"

            _ensure_actor(conn, corp_id, f"{ticker.upper()} Corp", "corporation", "institutional")
            _ensure_actor(conn, lobby_id, client, "lobbying_firm", "institutional")

            amount = float(amount_str) if amount_str else 0
            strength = min(0.8, 0.3 + amount / 10_000_000)
            _upsert_connection(conn, corp_id, lobby_id, "lobbying", strength)
            stats["connections"] += 1

        log.info("Lobbying enrichment: {s}", s=stats)
    return stats


def main():
    engine = get_db_engine()

    log.info("=== Starting connection enrichment from signal_data ===")

    r1 = enrich_insider_connections(engine)
    r2 = enrich_congressional_connections(engine)
    r3 = enrich_gov_contracts(engine)
    r4 = enrich_lobbying(engine)

    total_connections = r1["connections"] + r2["connections"] + r3["connections"] + r4["connections"]
    total_flows = r1["flows"] + r2["flows"] + r3["flows"]

    log.info("=== Enrichment complete ===")
    log.info("Total connections created: {n}", n=total_connections)
    log.info("Total wealth flows created: {n}", n=total_flows)
    log.info("Co-traded insider pairs: {n}", n=r1.get("co_traded", 0))
    log.info("Co-traded congress pairs: {n}", n=r2.get("co_traded", 0))
    log.info("Co-contractor pairs: {n}", n=r3.get("co_contractor", 0))


if __name__ == "__main__":
    main()
