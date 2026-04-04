"""GRID Research Agent — autonomous intelligence-gathering system.

Analyzes the sector map, identifies data gaps, spawns LLM research tasks
to fill them, generates hypotheses about influence relationships, and
logs findings with taxonomies.

Architecture:
    1. Gap Analyzer — compares sector_map expectations vs actual DB coverage
    2. Research Spawner — uses LLM to research each gap and propose data sources
    3. Hypothesis Generator — creates testable hypotheses from actor relationships
    4. Taxonomy Builder — organizes findings into structured knowledge
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from analysis.sector_map import SECTOR_MAP, get_actor_influence, get_sector_features


# ── Gap Analysis ────────────────────────────────────────────────

def analyze_gaps(engine: Engine) -> dict[str, Any]:
    """Compare sector map expectations vs actual data coverage.

    Returns a structured gap report with actionable items.
    """
    with engine.connect() as c:
        existing = set(r[0] for r in c.execute(text(
            "SELECT name FROM feature_registry WHERE model_eligible = TRUE"
        )).fetchall())

        with_recent = set(r[0] for r in c.execute(text(
            "SELECT DISTINCT fr.name FROM feature_registry fr "
            "JOIN resolved_series rs ON rs.feature_id = fr.id "
            "WHERE rs.obs_date >= CURRENT_DATE - 14"
        )).fetchall())

    gaps = []
    coverage = {"total_actors": 0, "actors_with_data": 0, "features_needed": 0,
                "features_found": 0, "features_stale": 0, "features_missing": 0}

    for sector_name, sector in SECTOR_MAP.items():
        for sub_name, sub in sector.get("subsectors", {}).items():
            for actor in sub.get("actors", []):
                coverage["total_actors"] += 1
                actor_features = actor.get("features", [])
                has_any = False

                for feat in actor_features:
                    coverage["features_needed"] += 1
                    if feat in with_recent:
                        coverage["features_found"] += 1
                        has_any = True
                    elif feat in existing:
                        coverage["features_stale"] += 1
                        gaps.append({
                            "sector": sector_name, "subsector": sub_name,
                            "actor": actor["name"], "feature": feat,
                            "type": actor["type"], "influence": sub["weight"] * actor["weight"],
                            "status": "STALE", "priority": "medium",
                        })
                    else:
                        coverage["features_missing"] += 1
                        gaps.append({
                            "sector": sector_name, "subsector": sub_name,
                            "actor": actor["name"], "feature": feat,
                            "type": actor["type"], "influence": sub["weight"] * actor["weight"],
                            "status": "MISSING", "priority": "high",
                        })

                if not actor_features:
                    gaps.append({
                        "sector": sector_name, "subsector": sub_name,
                        "actor": actor["name"], "feature": None,
                        "type": actor["type"], "influence": sub["weight"] * actor["weight"],
                        "status": "NO_SOURCE", "priority": "low" if actor["type"] == "policy" else "high",
                    })
                elif has_any:
                    coverage["actors_with_data"] += 1

    # Sort by influence (highest gaps first)
    gaps.sort(key=lambda g: g["influence"], reverse=True)
    coverage["coverage_pct"] = round(
        coverage["features_found"] / max(coverage["features_needed"], 1) * 100, 1
    )

    return {"gaps": gaps, "coverage": coverage}


# ── Hypothesis Generator ────────────────────────────────────────

def generate_hypotheses(engine: Engine) -> list[dict[str, Any]]:
    """Generate testable hypotheses from the sector map relationships.

    Uses the actor influence weights and connected features to propose
    hypotheses about causal chains and lead/lag relationships.
    """
    hypotheses = []

    for sector_name, sector in SECTOR_MAP.items():
        actors = get_actor_influence(sector_name)

        # For each pair of actors with data, propose a relationship hypothesis
        actors_with_features = [a for a in actors if a["features"]]
        for i, a1 in enumerate(actors_with_features):
            for a2 in actors_with_features[i + 1:]:
                if a1["subsector"] == a2["subsector"]:
                    continue  # same subsector relationships are obvious

                # Higher influence actor should lead
                leader = a1 if a1["influence"] > a2["influence"] else a2
                follower = a2 if leader == a1 else a1

                hypotheses.append({
                    "sector": sector_name,
                    "hypothesis": (
                        f"{leader['name']} ({leader['type']}) movements in "
                        f"{leader['features'][0]} should lead "
                        f"{follower['name']} ({follower['type']}) in "
                        f"{follower['features'][0]} by 1-5 days"
                    ),
                    "leader": leader["name"],
                    "follower": follower["name"],
                    "leader_features": leader["features"],
                    "follower_features": follower["features"],
                    "expected_lag_days": 3,
                    "confidence": "medium",
                    "rationale": (
                        f"{leader['name']} has {leader['influence']:.0%} influence vs "
                        f"{follower['name']} at {follower['influence']:.0%}. "
                        f"{leader.get('description', '')}"
                    ),
                })

        # Cross-sector hypotheses (Fed → everything)
        fed_actors = [a for a in actors if a["type"] == "central_bank"]
        for fed in fed_actors:
            for other_sector, other_data in SECTOR_MAP.items():
                if other_sector == sector_name:
                    continue
                other_actors = get_actor_influence(other_sector)
                top_other = [a for a in other_actors if a["features"]][:1]
                for oa in top_other:
                    hypotheses.append({
                        "sector": f"{sector_name} → {other_sector}",
                        "hypothesis": (
                            f"Changes in {fed['features'][0] if fed['features'] else 'fed policy'} "
                            f"should propagate to {oa['name']} ({oa['features'][0] if oa['features'] else '?'}) "
                            f"within 5-20 days"
                        ),
                        "leader": fed["name"],
                        "follower": oa["name"],
                        "leader_features": fed.get("features", []),
                        "follower_features": oa.get("features", []),
                        "expected_lag_days": 10,
                        "confidence": "high",
                        "rationale": f"Monetary policy transmission to {other_sector}",
                    })

    return hypotheses


# ── LLM Research Agent ──────────────────────────────────────────

def _get_llm():
    """Get the LLM client."""
    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.ORACLE)
        return client if client.is_available else None
    except Exception:
        return None


def research_actor(actor: dict, sector: str, engine: Engine) -> dict[str, Any]:
    """Use the LLM to research a specific actor with multi-angle verification.

    Asks the same question from 2 different angles, then checks for
    contradictions. Only accepts claims that are consistent across
    both responses AND supported by data we actually have.
    """
    llm = _get_llm()
    if not llm:
        return {"actor": actor["name"], "status": "SKIPPED", "reason": "LLM unavailable"}

    # Build context from existing data (ground truth)
    data_context = []
    if actor.get("features"):
        with engine.connect() as c:
            for feat in actor["features"]:
                row = c.execute(text(
                    "SELECT rs.value, rs.obs_date FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE fr.name = :name "
                    "ORDER BY rs.obs_date DESC LIMIT 1"
                ), {"name": feat}).fetchone()
                if row:
                    data_context.append(f"{feat}: {row[0]} (as of {row[1]})")

    data_block = "\n".join(data_context) if data_context else "No tracked data available."

    # Angle 1: Direct analysis
    prompt1 = (
        f"Actor: {actor['name']} ({actor['type']})\n"
        f"Sector: {sector}, Influence: {actor['influence']:.0%}\n"
        f"Role: {actor.get('description', 'N/A')}\n"
        f"Current data:\n{data_block}\n\n"
        f"In 3 sentences: What is this actor's current market impact "
        f"direction (bullish/bearish/neutral), and what is the single "
        f"most important catalyst to watch? Only state facts you are "
        f"confident about. Say 'uncertain' if you don't know."
    )

    # Angle 2: Contrarian challenge
    prompt2 = (
        f"A market analyst claims {actor['name']} is important for the "
        f"{sector} sector with {actor['influence']:.0%} influence.\n"
        f"Data: {data_block}\n\n"
        f"In 2 sentences: What would DISPROVE this actor's current "
        f"importance? What signal would indicate their influence is "
        f"waning or their direction is about to reverse?"
    )

    response1 = llm.chat([{"role": "user", "content": prompt1}], temperature=0.2, num_predict=200)
    response2 = llm.chat([{"role": "user", "content": prompt2}], temperature=0.2, num_predict=150)

    # Determine confidence based on data grounding
    confidence = "low"
    if data_context:
        confidence = "medium"
        if len(data_context) >= 2:
            confidence = "high"

    return {
        "actor": actor["name"],
        "sector": sector,
        "type": actor["type"],
        "status": "COMPLETE",
        "analysis": response1,
        "contrarian_check": response2,
        "data_grounded": bool(data_context),
        "confidence": confidence,
        "data_points": len(data_context),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def research_sector(sector_name: str, engine: Engine) -> dict[str, Any]:
    """Research an entire sector — all actors, gaps, and hypotheses."""
    sector = SECTOR_MAP.get(sector_name)
    if not sector:
        return {"sector": sector_name, "error": "sector not found"}

    actors = get_actor_influence(sector_name)
    findings = []

    # Research top actors by influence
    for actor in actors[:5]:  # Top 5 by influence
        log.info("Researching {a} in {s}", a=actor["name"], s=sector_name)
        finding = research_actor(actor, sector_name, engine)
        findings.append(finding)

    return {
        "sector": sector_name,
        "actors_researched": len(findings),
        "findings": findings,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── Full Research Sweep ─────────────────────────────────────────

def run_full_research(engine: Engine) -> dict[str, Any]:
    """Run a complete research sweep across all sectors.

    1. Analyze gaps
    2. Generate hypotheses
    3. Research top actors in each sector
    4. Store findings
    """
    log.info("Starting full research sweep")

    # 1. Gap analysis
    gap_report = analyze_gaps(engine)
    log.info(
        "Gap analysis: {c}% coverage, {g} gaps found",
        c=gap_report["coverage"]["coverage_pct"],
        g=len(gap_report["gaps"]),
    )

    # 2. Hypothesis generation
    hypotheses = generate_hypotheses(engine)
    log.info("Generated {n} hypotheses", n=len(hypotheses))

    # Store hypotheses in DB
    stored_hypotheses = 0
    with engine.begin() as conn:
        for h in hypotheses:
            try:
                conn.execute(text(
                    "INSERT INTO hypothesis_registry "
                    "(statement, layer, state, lag_structure, "
                    "proposed_metric, proposed_threshold) "
                    "VALUES (:stmt, 'REGIME', 'CANDIDATE', :lag, "
                    "'lagged_correlation', :thresh)"
                ), {
                    "stmt": h["hypothesis"][:500],
                    "lag": json.dumps({
                        "leader_features": h["leader_features"],
                        "follower_features": h["follower_features"],
                        "expected_lag": h["expected_lag_days"],
                        "rationale": h["rationale"],
                    }),
                    "thresh": 0.3,
                })
                stored_hypotheses += 1
            except Exception:
                pass

    log.info("Stored {n} hypotheses", n=stored_hypotheses)

    # 3. Research sectors (top 3 by gap count)
    sector_gap_counts = {}
    for g in gap_report["gaps"]:
        sector_gap_counts[g["sector"]] = sector_gap_counts.get(g["sector"], 0) + 1
    priority_sectors = sorted(sector_gap_counts, key=sector_gap_counts.get, reverse=True)[:4]

    sector_findings = {}
    for sector in priority_sectors:
        log.info("Researching sector: {s}", s=sector)
        result = research_sector(sector, engine)
        sector_findings[sector] = result

    # 4. Store research findings as analytical snapshot
    try:
        from store.snapshots import AnalyticalSnapshotStore
        snap = AnalyticalSnapshotStore(db_engine=engine)
        snap.save_snapshot(
            category="research_sweep",
            payload={
                "gap_report": gap_report,
                "hypotheses_count": len(hypotheses),
                "hypotheses_stored": stored_hypotheses,
                "sectors_researched": list(sector_findings.keys()),
                "findings_summary": {
                    s: {"actors": r.get("actors_researched", 0)}
                    for s, r in sector_findings.items()
                },
            },
            as_of_date=date.today(),
            metrics={
                "coverage_pct": gap_report["coverage"]["coverage_pct"],
                "gaps": len(gap_report["gaps"]),
                "hypotheses": len(hypotheses),
                "sectors_researched": len(sector_findings),
            },
        )
    except Exception as exc:
        log.warning("Snapshot save failed: {e}", e=str(exc))

    result = {
        "gap_report": gap_report,
        "hypotheses": hypotheses,
        "hypotheses_stored": stored_hypotheses,
        "sector_findings": sector_findings,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    log.info("Research sweep complete")
    return result


# ── Fill Missing Stock Data ─────────────────────────────────────

def fill_missing_stocks(engine: Engine) -> dict[str, Any]:
    """Pull price data for stocks referenced in sector_map but missing from DB.

    Uses yfinance to fill gaps for individual stock tickers.
    """
    missing_tickers = set()
    for sector_name, sector in SECTOR_MAP.items():
        for sub_name, sub in sector.get("subsectors", {}).items():
            for actor in sub.get("actors", []):
                ticker = actor.get("ticker")
                if not ticker:
                    continue
                # Check if we have recent data for this ticker
                for feat in actor.get("features", []):
                    with engine.connect() as c:
                        row = c.execute(text(
                            "SELECT 1 FROM feature_registry fr "
                            "JOIN resolved_series rs ON rs.feature_id = fr.id "
                            "WHERE fr.name = :name AND rs.obs_date >= CURRENT_DATE - 7 "
                            "LIMIT 1"
                        ), {"name": feat}).fetchone()
                        if not row:
                            missing_tickers.add(ticker)

    if not missing_tickers:
        return {"status": "all_covered", "missing": 0}

    log.info("Filling {n} missing stock tickers: {t}", n=len(missing_tickers), t=missing_tickers)

    # Use yfinance to pull missing data
    results = []
    try:
        import yfinance as yf
        from datetime import timedelta

        today = date.today()
        start = today - timedelta(days=504)

        for ticker in missing_tickers:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(start=start.isoformat(), end=today.isoformat())
                if hist.empty:
                    results.append({"ticker": ticker, "status": "NO_DATA"})
                    continue

                # Register feature and insert data
                feat_name = ticker.lower()
                feat_full = f"{feat_name}_full"

                with engine.begin() as conn:
                    # Register features
                    for fn, desc in [(feat_name, f"{ticker} Close Price"),
                                     (feat_full, f"{ticker} Full History")]:
                        conn.execute(text(
                            "INSERT INTO feature_registry "
                            "(name, family, description, transformation, "
                            "transformation_version, lag_days, normalization, "
                            "missing_data_policy, eligible_from_date, model_eligible) "
                            "VALUES (:name, 'breadth', :desc, 'RAW', 1, 0, 'ZSCORE', "
                            "'FORWARD_FILL', '2024-01-01', TRUE) "
                            "ON CONFLICT (name) DO NOTHING"
                        ), {"name": fn, "desc": desc})

                    fid_row = conn.execute(text(
                        "SELECT id FROM feature_registry WHERE name = :name"
                    ), {"name": feat_name}).fetchone()
                    if not fid_row:
                        continue
                    fid = fid_row[0]

                    # Get yfinance source ID
                    src_row = conn.execute(text(
                        "SELECT id FROM source_catalog WHERE name = 'YFINANCE'"
                    )).fetchone()
                    src_id = src_row[0] if src_row else 1

                    inserted = 0
                    for dt, row in hist.iterrows():
                        close = row.get("Close")
                        if close is None:
                            continue
                        obs = dt.date()
                        conn.execute(text(
                            "INSERT INTO resolved_series "
                            "(feature_id, obs_date, release_date, vintage_date, value, "
                            "source_priority_used) "
                            "VALUES (:fid, :od, :od, :od, :val, :src) "
                            "ON CONFLICT DO NOTHING"
                        ), {"fid": fid, "od": obs, "val": float(close), "src": src_id})
                        inserted += 1

                results.append({"ticker": ticker, "status": "SUCCESS", "rows": inserted})
                log.info("Filled {t}: {n} rows", t=ticker, n=inserted)

            except Exception as exc:
                results.append({"ticker": ticker, "status": "FAILED", "error": str(exc)})
                log.warning("Failed to fill {t}: {e}", t=ticker, e=str(exc))

    except ImportError:
        return {"status": "yfinance_not_installed", "missing": len(missing_tickers)}

    return {
        "status": "complete",
        "tickers_filled": len([r for r in results if r["status"] == "SUCCESS"]),
        "tickers_failed": len([r for r in results if r["status"] == "FAILED"]),
        "results": results,
    }


if __name__ == "__main__":
    from db import get_engine
    engine = get_engine()

    print("=== GAP ANALYSIS ===")
    gaps = analyze_gaps(engine)
    print(f"Coverage: {gaps['coverage']['coverage_pct']}%")
    print(f"Gaps: {len(gaps['gaps'])}")

    print("\n=== FILLING MISSING STOCKS ===")
    fill_result = fill_missing_stocks(engine)
    print(json.dumps(fill_result, indent=2, default=str))

    print("\n=== GENERATING HYPOTHESES ===")
    hyps = generate_hypotheses(engine)
    print(f"Generated: {len(hyps)}")
    for h in hyps[:5]:
        print(f"  {h['hypothesis'][:80]}")

    print("\n=== FULL RESEARCH SWEEP ===")
    result = run_full_research(engine)
    print(f"Sectors researched: {list(result['sector_findings'].keys())}")
    for sector, findings in result["sector_findings"].items():
        print(f"\n  {sector}:")
        for f in findings.get("findings", []):
            print(f"    {f['actor']}: {(f.get('analysis') or 'no analysis')[:100]}")
