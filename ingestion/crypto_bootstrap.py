#!/usr/bin/env python3
"""
Bootstrap crypto-native data sources and features into GRID.

Adds:
- 'crypto' to the feature_registry family CHECK constraint
- DexScreener and PumpFun source_catalog entries
- Crypto feature_registry entries for autoresearch

Run once:  python ingestion/crypto_bootstrap.py
"""

from __future__ import annotations

import sys
sys.path.insert(0, "/home/grid/grid_v4/grid_repo/grid")

from db import get_engine, execute_sql
from loguru import logger as log


CRYPTO_FEATURES = [
    # DexScreener signals
    {
        "name": "dex_sol_volume_24h",
        "family": "crypto",
        "description": "Total 24h USD volume across top Solana DEX pairs (DexScreener)",
        "transformation": "Aggregate sum of h24 volume from top Solana pairs via DexScreener API",
        "normalization": "ZSCORE",
        "missing_data_policy": "FORWARD_FILL",
        "eligible_from_date": "2024-01-01",
    },
    {
        "name": "dex_sol_liquidity",
        "family": "crypto",
        "description": "Total USD liquidity depth across top Solana DEX pairs",
        "transformation": "Aggregate sum of USD liquidity from DexScreener",
        "normalization": "ZSCORE",
        "missing_data_policy": "FORWARD_FILL",
        "eligible_from_date": "2024-01-01",
    },
    {
        "name": "dex_sol_buy_sell_ratio",
        "family": "crypto",
        "description": "24h aggregate buy/sell transaction ratio on Solana DEXs (>1 = net buying pressure)",
        "transformation": "total_buys_24h / total_sells_24h from DexScreener",
        "normalization": "RAW",
        "missing_data_policy": "FORWARD_FILL",
        "eligible_from_date": "2024-01-01",
    },
    {
        "name": "dex_sol_momentum_24h",
        "family": "crypto",
        "description": "Average 24h price change across top Solana DEX pairs (momentum breadth)",
        "transformation": "Mean of h24 priceChange across tracked pairs",
        "normalization": "ZSCORE",
        "missing_data_policy": "FORWARD_FILL",
        "eligible_from_date": "2024-01-01",
    },
    {
        "name": "dex_sol_txn_count_24h",
        "family": "crypto",
        "description": "Total 24h transaction count across top Solana DEX pairs",
        "transformation": "Sum of buys+sells from DexScreener h24 txns",
        "normalization": "ZSCORE",
        "missing_data_policy": "FORWARD_FILL",
        "eligible_from_date": "2024-01-01",
    },
    {
        "name": "dex_sol_boosted_tokens",
        "family": "crypto",
        "description": "Count of actively boosted tokens on DexScreener (paid promotion = speculative interest)",
        "transformation": "Length of /token-boosts/top/v1 response",
        "normalization": "RAW",
        "missing_data_policy": "FORWARD_FILL",
        "eligible_from_date": "2024-01-01",
    },
    # Pump.fun signals
    {
        "name": "pump_new_tokens_count",
        "family": "crypto",
        "description": "Number of new tokens launched on Pump.fun (memecoin mania gauge)",
        "transformation": "Count from /coins/latest endpoint",
        "normalization": "ZSCORE",
        "missing_data_policy": "FORWARD_FILL",
        "eligible_from_date": "2024-01-01",
    },
    {
        "name": "pump_koth_mcap",
        "family": "crypto",
        "description": "Market cap of Pump.fun king-of-the-hill token (peak speculative sentiment)",
        "transformation": "usd_market_cap from /coins/king-of-the-hill",
        "normalization": "ZSCORE",
        "missing_data_policy": "FORWARD_FILL",
        "eligible_from_date": "2024-01-01",
    },
    {
        "name": "pump_graduated_count",
        "family": "crypto",
        "description": "Count of tokens that completed Pump.fun bonding curve (graduation rate)",
        "transformation": "Count from /coins?complete=true",
        "normalization": "ZSCORE",
        "missing_data_policy": "FORWARD_FILL",
        "eligible_from_date": "2024-01-01",
    },
    {
        "name": "pump_graduated_avg_mcap",
        "family": "crypto",
        "description": "Average market cap of recently graduated Pump.fun tokens",
        "transformation": "Mean usd_market_cap of graduated tokens",
        "normalization": "ZSCORE",
        "missing_data_policy": "FORWARD_FILL",
        "eligible_from_date": "2024-01-01",
    },
    {
        "name": "pump_latest_avg_mcap",
        "family": "crypto",
        "description": "Average market cap of newest Pump.fun launches (entry-level speculation)",
        "transformation": "Mean usd_market_cap of latest 50 tokens",
        "normalization": "ZSCORE",
        "missing_data_policy": "FORWARD_FILL",
        "eligible_from_date": "2024-01-01",
    },
]


def bootstrap():
    engine = get_engine()

    # Step 1: Add 'crypto' to the family CHECK constraint
    log.info("Adding 'crypto' family to feature_registry CHECK constraint...")
    try:
        execute_sql(
            "ALTER TABLE feature_registry DROP CONSTRAINT IF EXISTS feature_registry_family_check"
        )
        execute_sql("""
            ALTER TABLE feature_registry ADD CONSTRAINT feature_registry_family_check
            CHECK (family IN ('rates', 'credit', 'equity', 'vol', 'fx',
                              'commodity', 'sentiment', 'macro', 'crypto',
                              'alternative', 'flows', 'systemic', 'trade',
                              'breadth', 'earnings'))
        """)
        log.info("CHECK constraint updated — 'crypto' family now allowed")
    except Exception as exc:
        log.warning("Could not alter CHECK constraint (may already include crypto): {e}", e=str(exc))

    # Step 2: Register crypto features
    inserted = 0
    for feat in CRYPTO_FEATURES:
        try:
            execute_sql(
                "INSERT INTO feature_registry "
                "(name, family, description, transformation, transformation_version, "
                "lag_days, normalization, missing_data_policy, eligible_from_date, model_eligible) "
                "VALUES (%(name)s, %(family)s, %(description)s, %(transformation)s, 1, "
                "0, %(normalization)s, %(missing_data_policy)s, %(eligible_from_date)s, TRUE) "
                "ON CONFLICT (name) DO NOTHING",
                feat,
            )
            inserted += 1
            print(f"  + {feat['name']}")
        except Exception as exc:
            log.error("Failed to insert {n}: {e}", n=feat["name"], e=str(exc))

    print(f"\nRegistered {inserted} crypto features")

    # Step 3: Verify
    rows = execute_sql(
        "SELECT id, name, family FROM feature_registry WHERE family = 'crypto' ORDER BY id"
    )
    print(f"\nCrypto features in registry ({len(rows)}):")
    for r in rows:
        print(f"  ID={r['id']}  {r['name']}")


if __name__ == "__main__":
    bootstrap()
