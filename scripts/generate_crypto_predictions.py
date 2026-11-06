#!/usr/bin/env python3
"""Publish evidence-backed crypto insights; never manufacture a trading call."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.crypto_signals import CryptoSignalBridge
from oracle.astrogrid_insights import produce_insights
from oracle.astrogrid_order_flow import MODEL_VERSION as FLOW_MODEL
from oracle.astrogrid_order_flow import produce_forecasts
from store.astrogrid import AstroGridStore


def main() -> int:
    from db import get_engine

    engine = get_engine()
    store = AstroGridStore(engine)
    receipts = CryptoSignalBridge(engine).bridge_binance_order_flow()
    result = produce_insights(engine, store)
    result["spot_flow_receipts"] = receipts
    result["spot_flow_forecasts"] = produce_forecasts(engine, store)
    # The short research cohort matures between hourly learning passes. Scope
    # this cadence to that model; never bulk-rescore legacy predictions here.
    result["spot_flow_scores"] = store.score_predictions(model_versions=[FLOW_MODEL], limit=100)
    print(json.dumps(result, default=str))
    # Missing evidence is an explicit abstention, not a made-up forecast.
    # A completely stale feed or failed persistence is an operational failure.
    return int(bool(result["errors"] or receipts["errors"] or result["spot_flow_forecasts"]["errors"])
               or not any(o["available"] for o in result["observations"]))


if __name__ == "__main__":
    raise SystemExit(main())
