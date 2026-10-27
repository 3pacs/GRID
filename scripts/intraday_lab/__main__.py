"""Offline CLI: python -m scripts.intraday_lab packets.jsonl --output report.json."""
import argparse
import hashlib
import json
from pathlib import Path

from .evaluate import evaluate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packets", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--include-holdout", action="store_true", help="Explicitly examine reserved test outcomes; exploratory only")
    args = parser.parse_args()
    raw = args.packets.read_bytes()
    try:
        packets = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()]
        result = evaluate(packets, include_holdout=args.include_holdout)
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        # Do not serialize malformed values or leak arbitrary input text in errors.
        result = {"status": "INPUT_REJECTED", "input_sha256": hashlib.sha256(raw).hexdigest(),
                  "reason": "Invalid JSON, packet identity, timing or finite numeric data"}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(result["status"])


if __name__ == "__main__":
    main()
