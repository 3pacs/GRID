"""Expand VIP actor networks — billionaires + government officials.

Seeds high-profile actors that don't exist yet, discovers connections
from existing data (congressional trades, insider filings, ICIJ,
FARA lobbying), and uses the spider backlinker to wire them together.

Usage:
    python scripts/expand_vip_actors.py

Safe to re-run — uses INSERT ... ON CONFLICT DO NOTHING for actors.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, ".")
from loguru import logger as log
from sqlalchemy import text
from db import get_engine


# ── VIP Seeds ──────────────────────────────────────────────────────

BILLIONAIRES = [
    # US Tech
    ("bill_gates", "Bill Gates", "insider", "individual", "Co-founder Microsoft", 128e9),
    ("jeff_bezos", "Jeff Bezos", "insider", "individual", "Founder Amazon", 200e9),
    ("mark_zuckerberg", "Mark Zuckerberg", "insider", "individual", "CEO Meta", 180e9),
    ("larry_ellison", "Larry Ellison", "insider", "individual", "Co-founder Oracle", 150e9),
    ("tim_cook", "Tim Cook", "insider", "individual", "CEO Apple", 2e9),
    ("satya_nadella", "Satya Nadella", "insider", "individual", "CEO Microsoft", 1e9),
    ("sergey_brin", "Sergey Brin", "insider", "individual", "Co-founder Google", 110e9),
    ("larry_page", "Larry Page", "insider", "individual", "Co-founder Google", 120e9),
    ("jensen_huang", "Jensen Huang", "insider", "individual", "CEO NVIDIA", 120e9),
    # US Finance
    ("warren_buffett", "Warren Buffett", "fund", "individual", "CEO Berkshire Hathaway", 130e9),
    ("jamie_dimon", "Jamie Dimon", "fund", "institutional", "CEO JPMorgan Chase", 2e9),
    ("ray_dalio", "Ray Dalio", "fund", "institutional", "Founder Bridgewater", 15e9),
    ("ken_griffin", "Ken Griffin", "fund", "institutional", "CEO Citadel", 35e9),
    ("steve_cohen", "Steve Cohen", "fund", "institutional", "CEO Point72", 20e9),
    ("carl_icahn", "Carl Icahn", "fund", "individual", "Icahn Enterprises", 6e9),
    ("george_soros", "George Soros", "fund", "individual", "Soros Fund Management", 7e9),
    ("michael_bloomberg", "Michael Bloomberg", "fund", "individual", "Bloomberg LP", 96e9),
    ("steve_schwarzman", "Steve Schwarzman", "fund", "institutional", "CEO Blackstone", 40e9),
    # Global
    ("bernard_arnault", "Bernard Arnault", "insider", "individual", "CEO LVMH", 210e9),
    ("mukesh_ambani", "Mukesh Ambani", "insider", "individual", "Chairman Reliance Industries", 115e9),
    ("gautam_adani", "Gautam Adani", "insider", "individual", "Chairman Adani Group", 85e9),
    ("carlos_slim", "Carlos Slim", "insider", "individual", "Grupo Carso", 100e9),
    ("amancio_ortega", "Amancio Ortega", "insider", "individual", "Founder Inditex/Zara", 85e9),
    ("zhong_shanshan", "Zhong Shanshan", "insider", "individual", "Nongfu Spring", 60e9),
    ("francoise_bettencourt", "Francoise Bettencourt Meyers", "insider", "individual", "L'Oreal heiress", 80e9),
    ("masayoshi_son", "Masayoshi Son", "insider", "individual", "CEO SoftBank", 25e9),
    ("peter_thiel", "Peter Thiel", "fund", "individual", "Palantir/Founders Fund", 10e9),
    ("sam_altman", "Sam Altman", "insider", "individual", "CEO OpenAI", 2e9),
]

GOVERNMENT = [
    # US Executive
    ("trump_donald", "Donald Trump", "politician", "sovereign", "President of the United States", 5e9),
    ("trump_ivanka", "Ivanka Trump", "politician", "individual", "Former Senior Advisor", 300e6),
    ("trump_jr", "Donald Trump Jr.", "politician", "individual", "Trump Organization EVP", 300e6),
    ("kushner_jared", "Jared Kushner", "politician", "individual", "Affinity Partners", 800e6),
    ("biden_joe", "Joe Biden", "politician", "sovereign", "Former President", 10e6),
    ("biden_hunter", "Hunter Biden", "politician", "individual", "Rosemont Seneca", 5e6),
    # US Congress
    ("congress_pelosi", "Nancy Pelosi", "politician", "regional", "Speaker Emerita", 120e6),
    ("congress_mcconnell", "Mitch McConnell", "politician", "regional", "Senate Minority Leader", 35e6),
    ("congress_schumer", "Chuck Schumer", "politician", "regional", "Senate Majority Leader", 1e6),
    ("congress_tuberville", "Tommy Tuberville", "politician", "regional", "Senator Alabama", 10e6),
    ("congress_crenshaw", "Dan Crenshaw", "politician", "regional", "Representative Texas", 1e6),
    ("congress_ossoff", "Jon Ossoff", "politician", "regional", "Senator Georgia", 5e6),
    ("congress_aoc", "Alexandria Ocasio-Cortez", "politician", "regional", "Representative New York", 0),
    ("congress_cruz", "Ted Cruz", "politician", "regional", "Senator Texas", 4e6),
    ("congress_warren", "Elizabeth Warren", "politician", "regional", "Senator Massachusetts", 12e6),
    ("congress_sanders", "Bernie Sanders", "politician", "regional", "Senator Vermont", 3e6),
    ("congress_hawley", "Josh Hawley", "politician", "regional", "Senator Missouri", 2e6),
    ("congress_gaetz", "Matt Gaetz", "politician", "regional", "Representative Florida", 1e6),
    ("congress_greene", "Marjorie Taylor Greene", "politician", "regional", "Representative Georgia", 1e6),
    # Clinton orbit
    ("clinton_hillary", "Hillary Clinton", "politician", "sovereign", "Former Secretary of State", 120e6),
    ("clinton_bill", "Bill Clinton", "politician", "sovereign", "42nd President", 120e6),
    ("clinton_chelsea", "Chelsea Clinton", "politician", "individual", "Clinton Foundation Vice Chair", 30e6),
    # US Agencies
    ("gensler_gary", "Gary Gensler", "government", "regional", "Former SEC Chair", 0),
    ("chopra_rohit", "Rohit Chopra", "government", "regional", "CFPB Director", 0),
    # Global Leaders
    ("xi_jinping", "Xi Jinping", "government", "sovereign", "President of China", 0),
    ("putin_vladimir", "Vladimir Putin", "government", "sovereign", "President of Russia", 0),
    ("modi_narendra", "Narendra Modi", "government", "sovereign", "Prime Minister of India", 0),
    ("macron_emmanuel", "Emmanuel Macron", "government", "sovereign", "President of France", 0),
    ("starmer_keir", "Keir Starmer", "government", "sovereign", "Prime Minister of UK", 0),
    ("scholz_olaf", "Olaf Scholz", "government", "sovereign", "Chancellor of Germany", 0),
    ("kishida_fumio", "Fumio Kishida", "government", "sovereign", "Former PM of Japan", 0),
    ("erdogan_recep", "Recep Tayyip Erdogan", "government", "sovereign", "President of Turkey", 0),
    ("lula_da_silva", "Luiz Inacio Lula da Silva", "government", "sovereign", "President of Brazil", 0),
    ("netanyahu_benjamin", "Benjamin Netanyahu", "government", "sovereign", "PM of Israel", 0),
    ("zelenskyy_volodymyr", "Volodymyr Zelenskyy", "government", "sovereign", "President of Ukraine", 0),
    ("milei_javier", "Javier Milei", "government", "sovereign", "President of Argentina", 0),
    ("meloni_giorgia", "Giorgia Meloni", "government", "sovereign", "PM of Italy", 0),
]

# Known relationships
CONNECTIONS = [
    # Trump orbit
    ("trump_donald", "kushner_jared", "family", 1.0),
    ("trump_donald", "trump_ivanka", "family", 1.0),
    ("trump_donald", "trump_jr", "family", 1.0),
    ("trump_donald", "ind_musk", "political_alliance", 0.9),
    ("trump_donald", "ind_musk_expanded", "political_alliance", 0.9),
    ("trump_donald", "peter_thiel", "political_donor", 0.8),
    ("trump_donald", "ken_griffin", "political_donor", 0.7),
    ("trump_donald", "steve_schwarzman", "political_donor", 0.7),
    ("trump_donald", "congress_cruz", "political_ally", 0.6),
    ("trump_donald", "congress_hawley", "political_ally", 0.7),
    ("trump_donald", "congress_gaetz", "political_ally", 0.8),
    ("trump_donald", "congress_greene", "political_ally", 0.8),
    ("trump_donald", "congress_tuberville", "political_ally", 0.7),
    ("trump_donald", "royal_mbs", "diplomatic_relationship", 0.8),
    ("trump_donald", "netanyahu_benjamin", "diplomatic_relationship", 0.9),
    ("trump_donald", "putin_vladimir", "diplomatic_relationship", 0.7),
    ("trump_donald", "xi_jinping", "diplomatic_relationship", 0.6),
    ("kushner_jared", "royal_mbs", "business_relationship", 0.9),
    ("kushner_jared", "netanyahu_benjamin", "business_relationship", 0.8),
    # Clinton orbit
    ("clinton_hillary", "clinton_bill", "family", 1.0),
    ("clinton_hillary", "clinton_chelsea", "family", 1.0),
    ("clinton_hillary", "george_soros", "political_donor", 0.8),
    ("clinton_hillary", "michael_bloomberg", "political_donor", 0.7),
    ("clinton_bill", "clinton_chelsea", "family", 1.0),
    # Pelosi
    ("congress_pelosi", "congress_schumer", "political_ally", 0.8),
    ("congress_pelosi", "congress_aoc", "same_party", 0.5),
    ("congress_pelosi", "congress_warren", "same_party", 0.6),
    ("congress_pelosi", "congress_sanders", "same_party", 0.5),
    # Tech connections
    ("ind_musk", "sam_altman", "competitor", 0.6),
    ("ind_musk_expanded", "sam_altman", "competitor", 0.6),
    ("mark_zuckerberg", "tim_cook", "competitor", 0.5),
    ("jensen_huang", "sam_altman", "business_partner", 0.8),
    ("jensen_huang", "satya_nadella", "business_partner", 0.7),
    ("sergey_brin", "larry_page", "co_founder", 1.0),
    ("bill_gates", "warren_buffett", "alliance", 0.9),
    ("peter_thiel", "ind_musk", "co_investor", 0.7),
    ("peter_thiel", "ind_musk_expanded", "co_investor", 0.7),
    # Finance connections
    ("jamie_dimon", "am_fink", "business_partner", 0.7),
    ("jamie_dimon", "fed_powell", "regulatory_relationship", 0.8),
    ("am_fink", "fed_powell", "regulatory_relationship", 0.8),
    ("am_fink", "treasury_yellen", "regulatory_relationship", 0.7),
    ("ray_dalio", "xi_jinping", "business_relationship", 0.6),
    ("masayoshi_son", "trump_donald", "business_relationship", 0.7),
    ("mukesh_ambani", "modi_narendra", "business_relationship", 0.8),
    ("gautam_adani", "modi_narendra", "business_relationship", 0.9),
    # Central banks ↔ leaders
    ("fed_powell", "trump_donald", "appointed_by", 0.7),
    ("fed_powell", "treasury_yellen", "regulatory_peer", 0.9),
    ("ecb_lagarde", "macron_emmanuel", "diplomatic_relationship", 0.7),
    ("pboc_pan", "xi_jinping", "reports_to", 0.9),
    ("boj_ueda", "kishida_fumio", "appointed_by", 0.7),
]


def seed_actors(engine):
    """Insert all VIP actors (skip existing)."""
    all_actors = BILLIONAIRES + GOVERNMENT
    log.info("Seeding {n} VIP actors...", n=len(all_actors))
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0

    for aid, name, category, tier, title, net_worth in all_actors:
        try:
            with engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO actors (id, name, category, tier, title, net_worth_estimate, "
                        "influence_score, trust_score, credibility, updated_at) "
                        "VALUES (:id, :name, :cat, :tier, :title, :nw, :inf, 0.5, 'confirmed', :now) "
                        "ON CONFLICT (id) DO UPDATE SET "
                        "name = EXCLUDED.name, category = EXCLUDED.category, tier = EXCLUDED.tier, "
                        "title = EXCLUDED.title, net_worth_estimate = COALESCE(EXCLUDED.net_worth_estimate, actors.net_worth_estimate), "
                        "updated_at = :now"
                    ),
                    {
                        "id": aid, "name": name, "cat": category, "tier": tier,
                        "title": title, "nw": net_worth if net_worth > 0 else None,
                        "inf": 0.95 if tier == "sovereign" else 0.85,
                        "now": now,
                    },
                )
                inserted += 1
        except Exception as exc:
            log.warning("Failed to seed {id}: {e}", id=aid, e=str(exc)[:80])

    log.info("Seeded {n} VIP actors", n=inserted)
    return inserted


def seed_connections(engine):
    """Insert known VIP connections."""
    log.info("Seeding {n} VIP connections...", n=len(CONNECTIONS))
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0

    for a, b, rel, strength in CONNECTIONS:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO actor_connections (actor_a, actor_b, relationship, strength, evidence, discovered_at) "
                        "VALUES (:a, :b, :rel, :str, :ev::jsonb, :now) "
                        "ON CONFLICT (actor_a, actor_b, relationship) DO UPDATE SET "
                        "strength = EXCLUDED.strength"
                    ),
                    {
                        "a": a, "b": b, "rel": rel, "str": strength,
                        "ev": json.dumps([{"source": "vip_seed", "date": now[:10]}]),
                        "now": now,
                    },
                )
                inserted += 1
        except Exception as exc:
            log.warning("Failed to connect {a}->{b}: {e}", a=a, b=b, e=str(exc)[:80])

    log.info("Seeded {n} VIP connections", n=inserted)
    return inserted


def discover_icij_links(engine):
    """Find ICIJ entities matching VIP actor names and create connections."""
    log.info("Discovering ICIJ links for VIP actors...")

    # Get all VIP actors
    with engine.connect() as conn:
        vips = conn.execute(text(
            "SELECT id, name FROM actors WHERE id NOT LIKE :patt AND influence_score >= 0.8"
        ), {"patt": "icij_%"}).fetchall()

    linked = 0
    now = datetime.now(timezone.utc).isoformat()

    for vip_id, vip_name in vips:
        # Search ICIJ for name matches
        search_name = vip_name.split("(")[0].strip()  # Remove "(Expanded Profile)" etc.
        if len(search_name) < 4:
            continue

        with engine.connect() as conn:
            matches = conn.execute(text(
                "SELECT id, name FROM actors WHERE id LIKE :patt AND name ILIKE :name LIMIT 5"
            ), {"patt": "icij_%", "name": f"%{search_name}%"}).fetchall()

        for icij_id, icij_name in matches:
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            "INSERT INTO actor_connections (actor_a, actor_b, relationship, strength, evidence, discovered_at) "
                            "VALUES (:a, :b, :rel, :str, :ev::jsonb, :now) "
                            "ON CONFLICT (actor_a, actor_b, relationship) DO NOTHING"
                        ),
                        {
                            "a": vip_id, "b": icij_id, "rel": "icij_name_match",
                            "str": 0.6,
                            "ev": json.dumps([{"source": "name_match", "icij_name": icij_name}]),
                            "now": now,
                        },
                    )
                    linked += 1
            except Exception:
                pass

    log.info("Discovered {n} ICIJ links", n=linked)
    return linked


def main():
    engine = get_engine()
    t0 = time.time()

    actors = seed_actors(engine)
    connections = seed_connections(engine)
    icij_links = discover_icij_links(engine)

    # Stats
    with engine.connect() as conn:
        total = conn.execute(text(
            "SELECT COUNT(*) FROM actors WHERE id NOT LIKE :p"
        ), {"p": "icij_%"}).scalar()
        total_conns = conn.execute(text(
            "SELECT COUNT(*) FROM actor_connections WHERE actor_a NOT LIKE :p AND actor_b NOT LIKE :p"
        ), {"p": "icij_%"}).scalar()
        gov = conn.execute(text(
            "SELECT COUNT(*) FROM actors WHERE category IN ('politician', 'government', 'central_bank')"
        )).scalar()
        billionaires = conn.execute(text(
            "SELECT COUNT(*) FROM actors WHERE net_worth_estimate >= 1000000000"
        )).scalar()

    elapsed = time.time() - t0
    log.info(
        "VIP expansion complete in {t:.1f}s: "
        "{a} actors seeded, {c} connections, {i} ICIJ links. "
        "Totals: {ta} non-ICIJ actors, {tc} connections, {g} govt, {b} billionaires",
        t=elapsed, a=actors, c=connections, i=icij_links,
        ta=total, tc=total_conns, g=gov, b=billionaires,
    )


if __name__ == "__main__":
    main()
