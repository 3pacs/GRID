"""CLI wrapper for the GRID source quality ablation report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from db import get_engine
from intelligence.source_quality_ablation import (
    DEFAULT_DAYS,
    DEFAULT_MIN_PREDICTION_SAMPLE,
    DEFAULT_PREDICTION_DAYS,
    DEFAULT_PREDICTION_LIMIT,
    DEFAULT_SOURCE_LIMIT,
    run_source_quality_ablation,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--prediction-days", type=int, default=DEFAULT_PREDICTION_DAYS)
    parser.add_argument("--source-limit", type=int, default=DEFAULT_SOURCE_LIMIT)
    parser.add_argument("--prediction-limit", type=int, default=DEFAULT_PREDICTION_LIMIT)
    parser.add_argument("--min-prediction-sample", type=int, default=DEFAULT_MIN_PREDICTION_SAMPLE)
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/source_quality"))
    parser.add_argument("--no-write", action="store_true", help="Compute only; do not write report files.")
    args = parser.parse_args()

    result = run_source_quality_ablation(
        get_engine(),
        days=max(1, args.days),
        prediction_days=max(1, args.prediction_days),
        source_limit=max(1, args.source_limit),
        prediction_limit=max(1, args.prediction_limit),
        min_prediction_sample=max(1, args.min_prediction_sample),
        output_dir=args.output_dir,
        write=not args.no_write,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
