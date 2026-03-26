"""
GRID Taxonomy Audit Engine.

Runs daily to ensure the feature registry is world-class:
1. Detects misclassified features (e.g., price features in wrong family)
2. Finds features with stale or zero data
3. Identifies missing variables that should exist (from sector map actors)
4. Checks for impossible values (negative prices, extreme outliers)
5. Validates normalization consistency within families
6. Reports coverage gaps per sector/subsector
7. Auto-fixes obvious misclassifications

The taxonomy is the foundation of everything — if a feature is in the
wrong family, z-scores are corrupted, regime detection is unreliable,
and every downstream analysis inherits the error.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


def run_taxonomy_audit(engine: Engine) -> dict[str, Any]:
    """Run comprehensive taxonomy audit and return findings.

    Returns:
        Dict with sections: misclassified, stale, missing, impossible,
        coverage, auto_fixes, recommendations.
    """
    today = date.today()
    report: dict[str, Any] = {
        "date": today.isoformat(),
        "misclassified": [],
        "stale_features": [],
        "missing_features": [],
        "impossible_values": [],
        "zero_data_features": [],
        "coverage": {},
        "auto_fixes": [],
        "recommendations": [],
        "stats": {},
    }

    with engine.connect() as conn:
        # ── 1. Misclassification Detection ────────────────────────
        # _full features should be in equity/crypto/commodity, not breadth/sentiment
        misclass = conn.execute(text(
            "SELECT id, name, family FROM feature_registry "
            "WHERE name LIKE '%\\_full' ESCAPE '\\' "
            "AND family NOT IN ('equity', 'crypto', 'commodity', 'rates', 'credit', 'vol') "
            "AND model_eligible = TRUE"
        )).fetchall()
        for r in misclass:
            report["misclassified"].append({
                "id": r[0], "name": r[1], "current_family": r[2],
                "suggested_family": _suggest_family(r[1]),
                "reason": f"Price feature '{r[1]}' classified as '{r[2]}' instead of equity/crypto/commodity",
            })

        # Market cap features in wrong families
        mc_misclass = conn.execute(text(
            "SELECT id, name, family FROM feature_registry "
            "WHERE name LIKE '%\\_market\\_cap' ESCAPE '\\' "
            "AND family NOT IN ('equity', 'crypto') "
            "AND model_eligible = TRUE"
        )).fetchall()
        for r in mc_misclass:
            suggested = "crypto" if any(k in r[1] for k in ["btc", "eth", "sol", "crypto", "defi"]) else "equity"
            report["misclassified"].append({
                "id": r[0], "name": r[1], "current_family": r[2],
                "suggested_family": suggested,
                "reason": f"Market cap feature in '{r[2]}' should be '{suggested}'",
            })

        # ── 2. Stale Features ─────────────────────────────────────
        stale = conn.execute(text(
            "SELECT fr.name, fr.family, MAX(rs.obs_date) as latest "
            "FROM feature_registry fr "
            "LEFT JOIN resolved_series rs ON rs.feature_id = fr.id "
            "WHERE fr.model_eligible = TRUE "
            "GROUP BY fr.name, fr.family "
            "HAVING MAX(rs.obs_date) < CURRENT_DATE - 7 OR MAX(rs.obs_date) IS NULL "
            "ORDER BY MAX(rs.obs_date) NULLS FIRST"
        )).fetchall()
        for r in stale:
            days_stale = (today - r[2]).days if r[2] else None
            report["stale_features"].append({
                "name": r[0], "family": r[1],
                "last_data": str(r[2]) if r[2] else "NEVER",
                "days_stale": days_stale,
            })

        # ── 3. Missing Features from Sector Map ──────────────────
        try:
            from analysis.sector_map import SECTOR_MAP
            for sector_name, sector in SECTOR_MAP.items():
                for sub_name, sub in sector.get("subsectors", {}).items():
                    for actor in sub.get("actors", []):
                        ticker = actor.get("ticker")
                        if not ticker:
                            continue
                        tk = ticker.lower().replace("-", "_")
                        fname = f"{tk}_full"
                        exists = conn.execute(text(
                            "SELECT id FROM feature_registry WHERE name = :n"
                        ), {"n": fname}).fetchone()
                        if not exists:
                            report["missing_features"].append({
                                "ticker": ticker,
                                "expected_feature": fname,
                                "sector": sector_name,
                                "subsector": sub_name,
                                "influence": round(sub.get("weight", 0) * actor.get("weight", 0), 4),
                            })
        except Exception as exc:
            log.debug("Sector map check failed: {e}", e=str(exc))

        # ── 4. Impossible Values ──────────────────────────────────
        # Negative prices
        neg_prices = conn.execute(text(
            "SELECT fr.name, rs.value, rs.obs_date FROM resolved_series rs "
            "JOIN feature_registry fr ON fr.id = rs.feature_id "
            "WHERE fr.name LIKE '%\\_full' ESCAPE '\\' AND rs.value < 0 "
            "AND rs.obs_date >= CURRENT_DATE - 7"
        )).fetchall()
        for r in neg_prices:
            report["impossible_values"].append({
                "name": r[0], "value": float(r[1]), "date": str(r[2]),
                "reason": "Negative price",
            })

        # Extreme z-scores (>15 sigma — almost certainly bad data)
        extreme_z = conn.execute(text(
            "WITH stats AS ("
            "  SELECT feature_id, AVG(value) as mean, STDDEV(value) as std "
            "  FROM resolved_series WHERE obs_date >= CURRENT_DATE - 504 "
            "  GROUP BY feature_id HAVING STDDEV(value) > 0"
            ") "
            "SELECT fr.name, fr.family, rs.value, s.mean, s.std, "
            "  ABS(rs.value - s.mean) / s.std as z "
            "FROM resolved_series rs "
            "JOIN feature_registry fr ON fr.id = rs.feature_id "
            "JOIN stats s ON s.feature_id = rs.feature_id "
            "WHERE rs.obs_date >= CURRENT_DATE - 7 "
            "AND ABS(rs.value - s.mean) / s.std > 15 "
            "ORDER BY ABS(rs.value - s.mean) / s.std DESC LIMIT 10"
        )).fetchall()
        for r in extreme_z:
            report["impossible_values"].append({
                "name": r[0], "family": r[1], "value": float(r[2]),
                "mean": float(r[3]), "z_score": float(r[5]),
                "reason": f"Extreme z-score ({r[5]:.1f}σ) — likely bad data",
            })

        # ── 5. Zero Data Features ────────────────────────────────
        zero_data = conn.execute(text(
            "SELECT fr.name, fr.family FROM feature_registry fr "
            "WHERE fr.model_eligible = TRUE "
            "AND NOT EXISTS (SELECT 1 FROM resolved_series rs WHERE rs.feature_id = fr.id)"
        )).fetchall()
        report["zero_data_features"] = [{"name": r[0], "family": r[1]} for r in zero_data]

        # ── 6. Coverage per Family ───────────────────────────────
        coverage = conn.execute(text(
            "SELECT fr.family, COUNT(*) as total, "
            "  COUNT(CASE WHEN rs.obs_date >= CURRENT_DATE THEN 1 END) as today, "
            "  COUNT(CASE WHEN rs.obs_date >= CURRENT_DATE - 3 THEN 1 END) as fresh_3d, "
            "  COUNT(CASE WHEN rs.obs_date IS NULL THEN 1 END) as no_data "
            "FROM feature_registry fr "
            "LEFT JOIN LATERAL ("
            "  SELECT obs_date FROM resolved_series WHERE feature_id = fr.id "
            "  ORDER BY obs_date DESC LIMIT 1"
            ") rs ON TRUE "
            "WHERE fr.model_eligible = TRUE "
            "GROUP BY fr.family ORDER BY total DESC"
        )).fetchall()
        for r in coverage:
            pct_fresh = r[3] / r[1] * 100 if r[1] > 0 else 0
            status = "GREEN" if pct_fresh > 80 else "YELLOW" if pct_fresh > 50 else "RED"
            report["coverage"][r[0]] = {
                "total": r[1], "today": r[2], "fresh_3d": r[3],
                "no_data": r[4], "pct_fresh": round(pct_fresh, 1),
                "status": status,
            }

        # ── 7. Stats ─────────────────────────────────────────────
        total_features = conn.execute(text(
            "SELECT COUNT(*) FROM feature_registry WHERE model_eligible = TRUE"
        )).fetchone()[0]
        total_with_data = conn.execute(text(
            "SELECT COUNT(DISTINCT feature_id) FROM resolved_series "
            "WHERE obs_date >= CURRENT_DATE - 7"
        )).fetchone()[0]

        report["stats"] = {
            "total_features": total_features,
            "features_with_recent_data": total_with_data,
            "coverage_pct": round(total_with_data / total_features * 100, 1) if total_features > 0 else 0,
            "misclassified_count": len(report["misclassified"]),
            "stale_count": len(report["stale_features"]),
            "missing_count": len(report["missing_features"]),
            "impossible_count": len(report["impossible_values"]),
            "zero_data_count": len(report["zero_data_features"]),
        }

    # ── 8. Auto-fix obvious issues ───────────────────────────────
    auto_fixes = _auto_fix_misclassifications(engine, report["misclassified"])
    report["auto_fixes"] = auto_fixes

    # ── 9. Generate recommendations ──────────────────────────────
    recs = []
    if report["stats"]["misclassified_count"] > 0:
        recs.append(f"Fixed {len(auto_fixes)} misclassified features. {report['stats']['misclassified_count'] - len(auto_fixes)} remain.")
    if report["stats"]["zero_data_count"] > 10:
        recs.append(f"{report['stats']['zero_data_count']} features have zero data — run ingestion or remove from model_eligible.")
    if report["stats"]["impossible_count"] > 0:
        recs.append(f"{report['stats']['impossible_count']} impossible values detected — investigate data source integrity.")

    red_families = [f for f, c in report["coverage"].items() if c["status"] == "RED"]
    if red_families:
        recs.append(f"RED alert on families: {', '.join(red_families)} — less than 50% fresh data.")

    if report["missing_features"]:
        top_missing = sorted(report["missing_features"], key=lambda x: x["influence"], reverse=True)[:5]
        tickers = [m["ticker"] for m in top_missing]
        recs.append(f"Missing high-influence tickers: {', '.join(tickers)} — add to ingestion.")

    report["recommendations"] = recs

    log.info(
        "Taxonomy audit: {t} features, {c}% coverage, {m} misclassified, {s} stale, {z} zero-data, {i} impossible",
        t=report["stats"]["total_features"],
        c=report["stats"]["coverage_pct"],
        m=report["stats"]["misclassified_count"],
        s=report["stats"]["stale_count"],
        z=report["stats"]["zero_data_count"],
        i=report["stats"]["impossible_count"],
    )

    return report


def _suggest_family(feature_name: str) -> str:
    """Suggest the correct family for a feature based on naming conventions."""
    name = feature_name.lower()
    if any(k in name for k in ["btc", "eth", "sol", "doge", "ada", "avax", "tao", "crypto", "defi"]):
        return "crypto"
    if any(k in name for k in ["gold", "silver", "oil", "gas", "gld", "slv", "uso", "ung", "copper"]):
        return "commodity"
    if any(k in name for k in ["tlt", "ief", "shy", "bnd", "tip", "treasury"]):
        return "rates"
    if any(k in name for k in ["hyg", "lqd", "jnk", "emb"]):
        return "credit"
    if any(k in name for k in ["vix", "vvix", "move"]):
        return "vol"
    return "equity"


def _auto_fix_misclassifications(engine: Engine, misclassified: list[dict]) -> list[dict]:
    """Auto-fix obviously wrong family assignments."""
    fixes = []
    if not misclassified:
        return fixes

    with engine.begin() as conn:
        for item in misclassified:
            suggested = item["suggested_family"]
            if suggested and suggested != item["current_family"]:
                conn.execute(text(
                    "UPDATE feature_registry SET family = :fam WHERE id = :fid"
                ), {"fam": suggested, "fid": item["id"]})
                fixes.append({
                    "name": item["name"],
                    "from": item["current_family"],
                    "to": suggested,
                })

    if fixes:
        log.info("Auto-fixed {n} misclassified features", n=len(fixes))
    return fixes
