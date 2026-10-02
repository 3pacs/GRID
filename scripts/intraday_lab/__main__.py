"""Offline CLI: python -m scripts.intraday_lab packets.jsonl --output report.json."""
import argparse
import json
from pathlib import Path

from .evaluate import evaluate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packets", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    packets = [json.loads(line) for line in args.packets.read_text(encoding="utf-8").splitlines() if line.strip()]
    result = evaluate(packets)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(result["status"])


if __name__ == "__main__":
    main()
