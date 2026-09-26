"""S09b: write a relabelled copy of a pre-S09b ``frozen-candidates.json``.

Run: python -m scripts.relabel_frozen_candidates PATH/TO/frozen-candidates.json

No DB, no scan. Reads the frozen candidates, marks each one ``SELF_LAG``
(target predicting itself or a declared near-copy, per
``analysis.research_real_panel.PROXY_GROUPS``) or ``CROSS_SERIES``, and writes
``frozen-candidates.relabelled.json`` beside the input. The original receipts
are never modified, and an existing relabelled file is never overwritten.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from analysis.offline_research_proof import write_once
from analysis.research_real_panel import relabel_frozen_candidates

OUTPUT_NAME = "frozen-candidates.relabelled.json"


def relabel(source: Path) -> Path:
    raw = source.read_bytes()
    result = relabel_frozen_candidates(json.loads(raw))
    result["source"] = {"file": source.name, "sha256": hashlib.sha256(raw).hexdigest()}
    output = source.with_name(OUTPUT_NAME)
    write_once(output, result)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("frozen_candidates")
    args = parser.parse_args()
    output = relabel(Path(args.frozen_candidates))
    counts = json.loads(output.read_text(encoding="utf-8"))["counts"]
    print(json.dumps({"written": str(output), **counts}, indent=2))


if __name__ == "__main__":
    main()
