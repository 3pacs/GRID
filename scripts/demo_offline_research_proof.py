"""Run with python -m scripts.demo_offline_research_proof NEW_OUTPUT_DIRECTORY."""

import argparse
from datetime import datetime, timedelta, timezone

import numpy as np

from analysis.offline_research_proof import Protocol, run_proof


def fixture():
    rng = np.random.default_rng(20260926)
    features = ("fixture:signal", "fixture:noise", "fixture:constant", "snap:llm_task")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(200):
        when = start + timedelta(days=i)
        x, noise = rng.normal(size=2)
        target = float(x + rng.normal(scale=0.1))
        rows.append(
            {
                "origin": "synthetic_fixture",
                "decision_at": when.isoformat(),
                "label_end": (when + timedelta(hours=1)).isoformat(),
                "target_known_at": (when + timedelta(hours=2)).isoformat(),
                "target": target,
                "features": {
                    name: {"value": float(value), "known_at": when.isoformat()}
                    for name, value in zip(features, [x, noise, 1, target])
                },
            }
        )
    protocol = Protocol(
        "synthetic-demo-20260926",
        features,
        (start + timedelta(days=100)).isoformat(),
        (start + timedelta(days=200)).isoformat(),
    )
    return protocol, rows[:100], rows[100:]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    args = parser.parse_args()
    result = run_proof(*fixture(), args.output)
    print(
        f"SYNTHETIC ONLY: {len(result['candidates'])} pending candidates; "
        "0 forward observations; promotion disabled"
    )
