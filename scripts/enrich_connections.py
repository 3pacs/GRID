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

from collections import defaultdict

from loguru import logger as log
from sqlalchemy import text

from api.dependencies import get_db_engine


# These enrichment passes scan signal_data (152K+ rows) with DISTINCT
# sorts and self-JOINs on ABS(date - date) (insider/congress clusters,
# congress-insider overlap). On the live corpus those single statements
# blow past the default 120s per-statement timeout (db.get_engine) and
# get killed by postgres mid-job. This is exactly the case the db.py
# docstring calls out: "Override per-call with `SET LOCAL
# statement_timeout = 0` for jobs that legitimately need longer."
#
# `0` = no limit. This is a one-shot batch job (run from cron / the
# Hermes operator), not a request path, so it is safe to let these
# statements run to completion. SET LOCAL only affects the current
# transaction, so the global default is untouched for everyone else.
_ENRICH_STATEMENT_TIMEOUT_MS: int = 0  # 0 = unlimited (heavy batch job)


def _lift_statement_timeout(conn) -> None:
    """Disable the per-statement timeout for the current transaction.

    Must be called as the FIRST statement inside an ``engine.begin()``
    block so the ``SET LOCAL`` applies to the heavy scan that follows
    and is rolled back automatically when the transaction ends.
    """
    conn.execute(
        text(f"SET LOCAL statement_timeout = {_ENRICH_STATEMENT_TIMEOUT_MS}")
    )


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
        _lift_statement_timeout(conn)
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
        _lift_statement_timeout(conn)
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
    """Create connections from government contract signals.

    Parses agency from multiple data JSON fields since the 'Agency' key
    is often NULL. Falls back to 'agency', 'Department', 'department'.
    """
    stats = {"connections": 0, "flows": 0}

    with engine.begin() as conn:
        _lift_statement_timeout(conn)
        # Broader query — pull data JSON to parse agency from multiple fields
        rows = conn.execute(text("""
            SELECT ticker,
                   COALESCE(data->>'Amount', data->>'amount', '0') as amount,
                   COALESCE(
                       data->>'Agency', data->>'agency',
                       data->>'Department', data->>'department',
                       data->>'Contracting Agency', data->>'contracting_agency',
                       data->>'description'
                   ) as agency,
                   signal_date
            FROM signal_data
            WHERE signal_type IN ('quiverquant:gov_contracts', 'gov_contract')
            AND ticker IS NOT NULL AND ticker != ''
            ORDER BY signal_date DESC
            LIMIT 2000
        """)).fetchall()

        log.info("Processing {} gov contract signals", len(rows))

        agency_companies: dict[str, set[str]] = defaultdict(set)
        _ensure_actor(conn, "us_government", "US Federal Government", "government", "sovereign")

        for row in rows:
            ticker, amount_str, agency, signal_date = row
            corp_id = f"corp_{ticker.upper()}"
            try:
                amount = float(amount_str) if amount_str else 0
            except (ValueError, TypeError):
                amount = 0

            _ensure_actor(conn, corp_id, f"{ticker.upper()} Corp", "corporation", "institutional")

            if agency and agency.strip():
                agency_clean = agency.strip()[:60]
                agency_id = f"gov_{agency_clean.lower().replace(' ', '_').replace('.', '')[:40]}"
                _ensure_actor(conn, agency_id, agency_clean, "government", "sovereign")
                _upsert_connection(conn, agency_id, corp_id, "gov_contract",
                                   min(0.9, 0.3 + amount / 1e10))
                stats["connections"] += 1
                agency_companies[agency_id].add(corp_id)
            else:
                # No agency parsed — still link to US Government umbrella
                _upsert_connection(conn, "us_government", corp_id, "gov_contract",
                                   min(0.7, 0.2 + amount / 1e10))
                stats["connections"] += 1

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
                    if co >= 1000:
                        break
                if co >= 1000:
                    break
            if co >= 1000:
                break

        stats["co_contractor"] = co
        log.info("Gov contract enrichment: {s}", s=stats)
    return stats


def enrich_lobbying(engine) -> dict:
    """Create connections from lobbying signals."""
    stats = {"connections": 0}

    with engine.begin() as conn:
        _lift_statement_timeout(conn)
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


def enrich_insider_clusters(engine) -> dict:
    """Find insiders who trade the same ticker within ±7 days = insider cluster.

    These are potential "friends" — people who get the same information
    and act on it at the same time. This is the intelligence the SEC
    looks at for insider trading rings.
    """
    stats = {"clusters": 0, "cluster_connections": 0}

    with engine.begin() as conn:
        _lift_statement_timeout(conn)
        # Find pairs of different insiders trading the same ticker within 7 days
        rows = conn.execute(text("""
            WITH insider_trades AS (
                SELECT DISTINCT actor, ticker, signal_date, direction
                FROM signal_data
                WHERE signal_type IN ('insider', 'quiverquant:insider')
                AND actor IS NOT NULL AND ticker IS NOT NULL
                AND actor != '' AND ticker != ''
            )
            SELECT t1.actor AS actor1, t2.actor AS actor2, t1.ticker,
                   COUNT(*) AS co_trades,
                   MIN(ABS(t1.signal_date - t2.signal_date)) AS min_gap_days
            FROM insider_trades t1
            JOIN insider_trades t2
                ON t1.ticker = t2.ticker
                AND t1.actor < t2.actor
                AND ABS(t1.signal_date - t2.signal_date) <= 7
            GROUP BY t1.actor, t2.actor, t1.ticker
            HAVING COUNT(*) >= 2
            ORDER BY COUNT(*) DESC
            LIMIT 1000
        """)).fetchall()

        log.info("Found {} insider cluster pairs", len(rows))

        seen_pairs: set[str] = set()
        for row in rows:
            actor1, actor2, ticker, co_trades, min_gap = row
            a1_id = f"ins_{actor1.lower().replace(' ', '_').replace('.', '')[:40]}"
            a2_id = f"ins_{actor2.lower().replace(' ', '_').replace('.', '')[:40]}"
            pair_key = f"{a1_id}:{a2_id}"
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            strength = min(0.9, 0.4 + 0.1 * int(co_trades))
            _upsert_connection(conn, a1_id, a2_id, "insider_cluster", strength)
            stats["cluster_connections"] += 1

        stats["clusters"] = len(seen_pairs)
        log.info("Insider cluster enrichment: {s}", s=stats)
    return stats


def enrich_fara_foreign_lobbying(engine) -> dict:
    """Create connections from FARA foreign lobbying signals.

    Foreign governments pay lobbyists to influence US policy.
    This maps: foreign_entity → lobbying_firm → influenced_companies.
    """
    stats = {"connections": 0, "flows": 0}

    with engine.begin() as conn:
        _lift_statement_timeout(conn)
        rows = conn.execute(text("""
            SELECT actor, ticker,
                   COALESCE(data->>'Foreign Principal', data->>'foreign_principal') as principal,
                   COALESCE(data->>'Amount', data->>'amount', '0') as amount,
                   signal_date
            FROM signal_data
            WHERE signal_type IN ('fara', 'foreign_lobbying')
            AND actor IS NOT NULL AND actor != ''
            ORDER BY signal_date DESC
            LIMIT 2000
        """)).fetchall()

        log.info("Processing {} FARA signals", len(rows))

        for row in rows:
            actor_name, ticker, principal, amount_str, signal_date = row
            lobby_id = f"fara_{actor_name.lower().replace(' ', '_').replace('.', '')[:40]}"
            _ensure_actor(conn, lobby_id, actor_name, "foreign_agent", "institutional")

            if principal and principal.strip():
                principal_clean = principal.strip()[:60]
                principal_id = f"foreign_{principal_clean.lower().replace(' ', '_').replace('.', '')[:40]}"
                _ensure_actor(conn, principal_id, principal_clean, "foreign_government", "sovereign")
                _upsert_connection(conn, principal_id, lobby_id, "foreign_lobbying", 0.7)
                stats["connections"] += 1

            if ticker and ticker.strip():
                corp_id = f"corp_{ticker.upper()}"
                _ensure_actor(conn, corp_id, f"{ticker.upper()} Corp", "corporation", "institutional")
                _upsert_connection(conn, lobby_id, corp_id, "lobbying_influence", 0.5)
                stats["connections"] += 1

            try:
                amount = float(amount_str) if amount_str else 0
            except (ValueError, TypeError):
                amount = 0
            if amount > 0 and principal:
                _upsert_flow(conn, principal_id if principal else lobby_id, lobby_id,
                             amount, "confirmed", signal_date,
                             f"Foreign lobbying ${amount:,.0f} from {principal or 'unknown'}")
                stats["flows"] += 1

        log.info("FARA enrichment: {s}", s=stats)
    return stats


def enrich_darkpool_signals(engine) -> dict:
    """Create connections from dark pool trading signals.

    Large dark pool prints indicate institutional interest that isn't
    visible on lit exchanges. Connect companies with similar dark pool
    patterns → potential block trade coordination.
    """
    stats = {"connections": 0}

    with engine.begin() as conn:
        _lift_statement_timeout(conn)
        rows = conn.execute(text("""
            SELECT ticker, magnitude, signal_date, direction
            FROM signal_data
            WHERE signal_type IN ('darkpool', 'dark_pool', 'quiverquant:darkpool')
            AND ticker IS NOT NULL AND ticker != ''
            AND magnitude > 0
            ORDER BY magnitude DESC
            LIMIT 2000
        """)).fetchall()

        log.info("Processing {} dark pool signals", len(rows))

        # Group by ticker → find tickers with heavy dark pool activity
        ticker_activity: dict[str, float] = defaultdict(float)
        for row in rows:
            ticker, magnitude, _, _ = row
            ticker_activity[ticker] += float(magnitude or 0)

        # Top dark pool tickers → connect to "dark pool" actor
        dp_id = "darkpool_flow"
        _ensure_actor(conn, dp_id, "Dark Pool Flow", "institutional", "institutional")

        for ticker, total_volume in sorted(ticker_activity.items(), key=lambda x: -x[1])[:200]:
            corp_id = f"corp_{ticker.upper()}"
            _ensure_actor(conn, corp_id, f"{ticker.upper()} Corp", "corporation", "institutional")
            strength = min(0.8, 0.2 + total_volume / 1e10)
            _upsert_connection(conn, dp_id, corp_id, "darkpool_activity", strength)
            stats["connections"] += 1

        log.info("Dark pool enrichment: {s}", s=stats)
    return stats


def enrich_institutional_flows(engine) -> dict:
    """Create connections from institutional flow signals (13F, ETF flows).

    Maps funds to the companies they hold/trade. Also creates
    fund-to-fund connections when they hold similar portfolios.
    """
    stats = {"connections": 0, "flows": 0}

    with engine.begin() as conn:
        _lift_statement_timeout(conn)
        rows = conn.execute(text("""
            SELECT actor, ticker, direction, magnitude, signal_date, confidence
            FROM signal_data
            WHERE signal_type IN ('institutional', '13f', 'etf_flow',
                                  'quiverquant:institutional')
            AND actor IS NOT NULL AND ticker IS NOT NULL
            AND actor != '' AND ticker != ''
            ORDER BY signal_date DESC
            LIMIT 3000
        """)).fetchall()

        log.info("Processing {} institutional flow signals", len(rows))

        fund_tickers: dict[str, set[str]] = defaultdict(set)

        for row in rows:
            actor_name, ticker, direction, magnitude, signal_date, confidence = row
            fund_id = f"fund_{actor_name.lower().replace(' ', '_').replace('.', '')[:40]}"
            corp_id = f"corp_{ticker.upper()}"

            _ensure_actor(conn, fund_id, actor_name, "fund", "institutional")
            _ensure_actor(conn, corp_id, f"{ticker.upper()} Corp", "corporation", "institutional")

            amount = float(magnitude or 0)
            strength = min(0.8, 0.3 + amount / 1e9)
            _upsert_connection(conn, fund_id, corp_id, "institutional_holding", strength)
            stats["connections"] += 1

            if amount > 0:
                is_buy = "buy" in (direction or "").lower() or "increase" in (direction or "").lower()
                imp = f"Fund {'increased' if is_buy else 'decreased'} position ${amount:,.0f} in {ticker}"
                _upsert_flow(conn, fund_id, corp_id, amount,
                             confidence or "derived", signal_date, imp)
                stats["flows"] += 1

            fund_tickers[fund_id].add(ticker)

        # Funds with 5+ overlapping holdings → co_investment
        co = 0
        fund_ids = list(fund_tickers.keys())
        for i, f1 in enumerate(fund_ids):
            for f2 in fund_ids[i+1:]:
                overlap = fund_tickers[f1] & fund_tickers[f2]
                if len(overlap) >= 5:
                    _upsert_connection(conn, f1, f2, "co_investment",
                                       min(0.8, 0.3 + 0.05 * len(overlap)))
                    co += 1
                    if co >= 500:
                        break
            if co >= 500:
                break

        stats["co_investment"] = co
        log.info("Institutional flow enrichment: {s}", s=stats)
    return stats


def enrich_congress_insider_overlap(engine) -> dict:
    """Find when congress members and insiders trade the same ticker at the same time.

    This is the smoking gun: a politician buys AAPL the same week
    an AAPL insider sells? That's a connection worth surfacing.
    """
    stats = {"connections": 0}

    with engine.begin() as conn:
        _lift_statement_timeout(conn)
        rows = conn.execute(text("""
            WITH congress_trades AS (
                SELECT DISTINCT actor, ticker, signal_date
                FROM signal_data
                WHERE signal_type IN ('congressional', 'quiverquant:house', 'quiverquant:senate')
                AND actor IS NOT NULL AND ticker IS NOT NULL
            ),
            insider_trades AS (
                SELECT DISTINCT actor, ticker, signal_date
                FROM signal_data
                WHERE signal_type IN ('insider', 'quiverquant:insider')
                AND actor IS NOT NULL AND ticker IS NOT NULL
            )
            SELECT c.actor AS congress_member, i.actor AS insider_name, c.ticker,
                   COUNT(*) AS co_trades
            FROM congress_trades c
            JOIN insider_trades i
                ON c.ticker = i.ticker
                AND ABS(c.signal_date - i.signal_date) <= 14
            GROUP BY c.actor, i.actor, c.ticker
            HAVING COUNT(*) >= 1
            ORDER BY COUNT(*) DESC
            LIMIT 500
        """)).fetchall()

        log.info("Found {} congress-insider overlap pairs", len(rows))

        for row in rows:
            congress_member, insider_name, ticker, co_trades = row
            pol_id = f"pol_{congress_member.lower().replace(' ', '_').replace('.', '')[:40]}"
            ins_id = f"ins_{insider_name.lower().replace(' ', '_').replace('.', '')[:40]}"

            strength = min(0.9, 0.5 + 0.15 * int(co_trades))
            _upsert_connection(conn, pol_id, ins_id, "congress_insider_overlap", strength)
            stats["connections"] += 1

        log.info("Congress-insider overlap: {s}", s=stats)
    return stats


def main():
    engine = get_db_engine()

    log.info("=== Starting FULL connection enrichment from signal_data ===")

    results = {}
    results["insider"] = enrich_insider_connections(engine)
    results["congressional"] = enrich_congressional_connections(engine)
    results["gov_contracts"] = enrich_gov_contracts(engine)
    results["lobbying"] = enrich_lobbying(engine)
    results["insider_clusters"] = enrich_insider_clusters(engine)
    results["fara"] = enrich_fara_foreign_lobbying(engine)
    results["darkpool"] = enrich_darkpool_signals(engine)
    results["institutional"] = enrich_institutional_flows(engine)
    results["congress_insider"] = enrich_congress_insider_overlap(engine)

    total_connections = sum(r.get("connections", 0) + r.get("cluster_connections", 0) for r in results.values())
    total_flows = sum(r.get("flows", 0) for r in results.values())

    log.info("=== FULL Enrichment complete ===")
    log.info("Total connections created: {n}", n=total_connections)
    log.info("Total wealth flows created: {n}", n=total_flows)
    for name, r in results.items():
        log.info("  {name}: {r}", name=name, r=r)


if __name__ == "__main__":
    main()
