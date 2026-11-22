"""Explicit-file, offline preview for the GRID tracker research lane."""
from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path

from .ercot_large_load import parse_monthly
from .moratorium_nation import adapt
from .nyfed_repo_pair import pair_rates
from .usda_barge_rates import normalize

SOURCES = frozenset(("ercot_monthly", "usda_barge", "moratorium_nation", "nyfed_repo_pair"))


def replay(manifest_path: Path) -> dict:
    """Replay the supplied manifest only. Does not access a service or a GRID database."""
    raw = manifest_path.read_bytes()
    config = json.loads(raw)
    if not isinstance(config, dict) or config.get("schema") != "grid-tracker-replay/1":
        raise ValueError("expected grid-tracker-replay/1")
    source = config.get("source")
    if not isinstance(source, str) or source not in SOURCES:
        raise ValueError("unsupported tracker")
    root = manifest_path.parent.resolve()

    def read(relative: str) -> bytes:
        if not isinstance(relative, str) or Path(relative).is_absolute():
            raise ValueError("payload files must be relative to the manifest")
        path = (root / relative).resolve()
        if root not in path.parents:
            raise ValueError("payload must stay inside the manifest directory")
        return path.read_bytes()

    opts = config.get("metadata")
    if not isinstance(opts, dict):
        raise ValueError("metadata must be an object")
    if source == "ercot_monthly":
        result = parse_monthly(read(config["payload"]), **opts)
    elif source == "usda_barge":
        result = normalize(read(config["payload"]), **opts)
    elif source == "moratorium_nation":
        result = adapt(read(config["payload"]), **opts)
    else:
        if opts.get("sofr_source_id") != "ny_fed_sofr":
            raise ValueError("repo preview must use an export from the existing SOFR capture source")
        result = pair_rates(
            read(config["sofr_payload"]), read(config["tgcr_payload"]),
            sofr_seen_at=opts["sofr_seen_at"], tgcr_seen_at=opts["tgcr_seen_at"],
        )

    return {
        "schema": "grid-tracker-preview/1",
        "lane": "research_only",
        "promotion_allowed": False,
        "eligible_for_scoring": False,
        "eligible_for_backtest": False,
        "source": source,
        "manifest_sha256": sha256(raw).hexdigest(),
        "result": result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()
    try:
        result = replay(args.manifest)
    except (KeyError, OSError, TypeError, ValueError) as exc:
        parser.exit(2, f"tracker preview refused: {exc}\n")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
