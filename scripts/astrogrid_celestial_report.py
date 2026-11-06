#!/usr/bin/env python3
"""Reproduce offline mystical ablations from provenance-bearing captures."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", type=Path, required=True)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    from evaluation.astrogrid_celestial import evaluate

    result = evaluate(args.panel, args.features)
    # Reference-period results are descriptive; do not publish another apparent
    # significance family alongside the predeclared final-test correction.
    for case in result["cases"]:
        case["reference"] = {k: v for k, v in case["reference"].items() if k in (
            "events", "first_decision", "last_decision", "mean_gross_bps", "mean_net_bps",
            "market_only_net_bps", "paired_advantage_bps", "direction_accuracy", "forecast_mse",
            "market_only_mse", "baselines_net_bps", "cost_sensitivity")}
    protocol = "docs/research/astrogrid-celestial-equity-v1-protocol.md"
    result.update(generated_at=datetime.now(timezone.utc).isoformat(), protocol_path=protocol,
                  protocol_sha256=hashlib.sha256((ROOT/protocol).read_bytes()).hexdigest(),
                  panel_sha256=hashlib.sha256(args.panel.read_bytes()).hexdigest(),
                  sky_capture_sha256=hashlib.sha256(args.features.read_bytes()).hexdigest(),
                  sun_formula_code_sha256=hashlib.sha256((ROOT/"analysis/ephemeris.py").read_bytes()).hexdigest(),
                  evaluator_sha256=hashlib.sha256((ROOT/"evaluation/astrogrid_celestial.py").read_bytes()).hexdigest(),
                  sky_feature_count=result["input_diagnostics"]["SP500"]["sky_features"],
                  forecast_model="Fixed ridge100, annual past-label-only refits; feature ablation, not historical LLM answers",
                  primary_cost={"commission_bps_per_side":1,"slippage_bps_per_side":1.5,
                                "basis":"Illustrative index paper exposure; no executable index fills, borrow/funding excluded"},
                  inference={"replicates":1999,"block_trading_sessions":20,"seed":20261106,
                             "family_tests":126,"correction":"Benjamini-Hochberg", "min_events_per_case":100})
    result["nominal_positive_tests"] = sum(r["eligible"] and r["bootstrap_p"]<.05 and r["paired_advantage_bps"]>0 for r in result["cases"])
    result["corrected_incremental_tests"] = sum(r["eligible"] and r["bh_q"]<=.05 and r["paired_95ci_bps"][0]>0 for r in result["cases"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False)+"\n")
    print(json.dumps({k:result[k] for k in ("declared_tests","eligible_tests","historical_candidates","nominal_positive_tests","corrected_incremental_tests")}))


if __name__ == "__main__":
    main()
