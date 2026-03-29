"""Run the AstroGrid scoring/backtest/review loop in one shot."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.dependencies import get_astrogrid_store


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of-date", default=None, help="Evaluation date in YYYY-MM-DD")
    parser.add_argument("--score-limit", type=int, default=200)
    parser.add_argument("--backtest-limit", type=int, default=250)
    parser.add_argument("--backtest-window-days", type=int, default=180)
    parser.add_argument("--provider-mode", default="deterministic", choices=["deterministic", "llm", "hybrid"])
    parser.add_argument("--horizon", dest="horizon_label", default=None, choices=["macro", "swing"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    as_of_date = date.fromisoformat(args.as_of_date) if args.as_of_date else None
    result = get_astrogrid_store().run_learning_loop(
        as_of_date=as_of_date,
        score_limit=max(1, args.score_limit),
        backtest_limit=max(1, args.backtest_limit),
        backtest_window_days=max(7, args.backtest_window_days),
        provider_mode=args.provider_mode,
        horizon_label=args.horizon_label,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
