"""E0 v2 command line.

    python -m evals.e0v2 verify                       # v2 manifest + the e0-v1 it builds on (exit 1 on mismatch)
    python -m evals.e0v2 run --out DIR [--profile full|ci|smoke|ic01] [--jobs N] [--no-replication]
                                       [--no-crosscheck] [--scenarios NAME ...]
    python -m evals.e0v2 manifest --write --version e0-vN   # maintainers: a new benchmark version

``run`` verifies both manifests and the calibration receipt pin first and
writes ``DIR/scorecard.json`` once (refuses to overwrite). No database,
network or production write.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    for key in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS"):
        os.environ.setdefault(key, "1")
    parser = argparse.ArgumentParser(prog="python -m evals.e0v2", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("verify", help="check every pinned file against MANIFEST.sha256 (and e0-v1's)")
    run_p = sub.add_parser("run", help="run the benchmark and write DIR/scorecard.json")
    run_p.add_argument("--out", type=Path, required=True)
    run_p.add_argument("--profile", default="full", choices=("full", "ci", "smoke", "ic01"))
    run_p.add_argument("--no-replication", action="store_true")
    run_p.add_argument("--no-crosscheck", action="store_true")
    run_p.add_argument("--scenarios", nargs="+", default=None, help="override the profile's scenario list")
    run_p.add_argument("--jobs", type=int, default=1, help="jobs in parallel processes (result is identical)")
    man_p = sub.add_parser("manifest", help="(maintainers) regenerate MANIFEST.sha256 for a NEW version")
    man_p.add_argument("--write", action="store_true", required=True)
    man_p.add_argument("--version", required=True)
    args = parser.parse_args(argv)

    from evals.e0v2 import VERSION, manifest

    if args.command == "verify":
        try:
            out = manifest.verify()
            out["builds_on"] = manifest.verify_builds_on()
            print(json.dumps(out, indent=2, sort_keys=True))
        except manifest.ManifestError as exc:
            print(f"E0 v2 verify FAILED: {exc}", file=sys.stderr)
            return 1
        return 0
    if args.command == "manifest":
        if args.version != VERSION:
            print(f"refused: evals.e0v2.VERSION is {VERSION!r}; bump it (and config.json) to {args.version!r} first",
                  file=sys.stderr)
            return 2
        manifest.write(args.version)
        print(manifest.verify())
        return 0

    from evals.e0v2 import benchmark

    card = benchmark.run(args.profile, replicate=not args.no_replication,
                         crosscheck=False if args.no_crosscheck else None, jobs=args.jobs,
                         scenarios=args.scenarios, log=lambda msg: print(msg, file=sys.stderr, flush=True))
    path = benchmark.write_scorecard(card, args.out)
    print(json.dumps({"scorecard": str(path), "headline": card["headline"],
                      "vs1_design_crosscheck": card["vs1_design_crosscheck"].get("headline_power")},
                     indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
