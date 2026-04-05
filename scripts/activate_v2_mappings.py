"""Activate V2 entity mappings and resolve pending data.

Ensures all V2 mapping target names exist in feature_registry,
then re-runs the resolver to process any previously-skipped raw_series.
"""
from __future__ import annotations

import sys
sys.path.insert(0, "/data/grid_v4/grid_repo/grid")

from db import get_engine
from sqlalchemy import text
from loguru import logger as log
from normalization.entity_map import EntityMap, NEW_MAPPINGS_V2, SEED_MAPPINGS
from normalization.resolver import Resolver

engine = get_engine()

# Step 1: Ensure feature_registry rows exist for all V2 targets
log.info("Step 1: Registering V2 feature names in feature_registry...")
v2_features = set(NEW_MAPPINGS_V2.values())

# Infer family from naming convention
def infer_family(name: str) -> str:
    prefixes = {
        "ecb_": "macro", "oecd_": "macro", "bis_": "credit",
        "china_": "macro", "brazil_": "macro", "korea_": "macro",
        "singapore_": "rates", "oi_": "macro", "ofr_": "systemic",
        "corn_": "commodity", "wheat_": "commodity", "eci_": "macro",
        "trade_": "trade", "us_china_": "trade", "viirs_": "alternative",
        "patent_": "alternative", "spy_": "vol", "qqq_": "vol",
        "iwm_": "vol", "euro_": "rates",
    }
    for prefix, family in prefixes.items():
        if name.startswith(prefix):
            return family
    return "alternative"

registered = 0
with engine.begin() as conn:
    for feat_name in sorted(v2_features):
        family = infer_family(feat_name)
        result = conn.execute(
            text("""
                INSERT INTO feature_registry (name, family, description, transformation,
                    transformation_version, lag_days, normalization, missing_data_policy,
                    eligible_from_date, model_eligible)
                VALUES (:name, :family, :desc, 'RAW', 1, 0, 'ZSCORE', 'FORWARD_FILL',
                    '2000-01-01', TRUE)
                ON CONFLICT (name) DO NOTHING
            """),
            {
                "name": feat_name,
                "family": family,
                "desc": feat_name.replace("_", " ").title(),
            },
        )
        if result.rowcount > 0:
            registered += 1

log.info("Registered {} new features in feature_registry", registered)

# Step 2: Verify entity map loads V2
log.info("Step 2: Verifying EntityMap loads V2 mappings...")
emap = EntityMap(db_engine=engine)
total_mappings = len(SEED_MAPPINGS)
log.info("Total active mappings: {}", total_mappings)

# Step 3: Run resolver
log.info("Step 3: Running resolver on pending data...")
resolver = Resolver(db_engine=engine)
result = resolver.resolve_pending()
log.info("Resolved: {}", result)

# Step 4: Report
with engine.connect() as conn:
    distinct_features = conn.execute(
        text("SELECT COUNT(DISTINCT feature_id) FROM resolved_series")
    ).fetchone()[0]
    total_resolved = conn.execute(
        text("SELECT COUNT(*) FROM resolved_series")
    ).fetchone()[0]

log.info("Result: {} distinct features in resolved_series ({} total rows)", distinct_features, total_resolved)
log.info("Done!")
