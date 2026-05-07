#!/usr/bin/env python3
"""Verify the two GRID server deployment trees are in sync.

The grid-svr server hosts the GRID code at TWO paths:

  * `/data/grid_v4/astrogrid_dedup`  — `WorkingDirectory` for `grid-api`
  * `/home/grid/grid_v4/grid_repo`   — `WorkingDirectory` for everything else
                                       (`grid-hermes`, `grid-extractor`,
                                       `grid-intelligence`, `grid-realtime`,
                                       `grid-spider`, `grid-backlinker`,
                                       `grid-breaking-news`)

Because both trees are live, every deploy MUST copy to BOTH paths or
services will silently run stale code. This script SSHes to grid-svr and
compares file hashes between the two trees, flagging any drift in the
critical source directories (`api/`, `analysis/`, `intelligence/`,
`ingestion/`, `pwa_dist/`).

It deliberately ignores `__pycache__/`, `*.pyc`, `.git/`, and other
volatile artefacts so the report only shows real source-file drift.

Exit code 0 means in sync. Exit code 1 means at least one file differs.

Usage::

    python3 scripts/verify_deployment_sync.py
    python3 scripts/verify_deployment_sync.py --host grid@100.75.185.36
    python3 scripts/verify_deployment_sync.py --paths api analysis
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from collections import defaultdict
from typing import Iterable

DEFAULT_HOST = "grid@100.75.185.36"
TREE_A = "/home/grid/grid_v4/grid_repo"
TREE_B = "/data/grid_v4/astrogrid_dedup"

DEFAULT_PATHS = [
    "api",
    "analysis",
    "intelligence",
    "ingestion",
    "pwa_dist",
    "scripts",
]

IGNORE_GLOBS = [
    "*/__pycache__/*",
    "*.pyc",
    "*.pyo",
    "*/.git/*",
    "*.log",
    "*/.pytest_cache/*",
    "*/node_modules/*",
]


def ssh(host: str, cmd: str) -> str:
    """Run a remote command and return stdout (raises on non-zero exit)."""
    result = subprocess.run(
        ["ssh", host, cmd],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"ssh `{cmd}` failed with code {result.returncode}: {result.stderr}"
        )
    return result.stdout


def build_find_command(root: str, subpath: str) -> str:
    """Build a remote `find ... | xargs sha256sum` pipeline.

    `find` enumerates regular files under `<root>/<subpath>`, prunes the
    ignore globs, and pipes them to `sha256sum`. Output lines look like
    `<sha256>  <relative_path>`.
    """
    prune_clauses = " -o ".join(f"-path '{p}'" for p in IGNORE_GLOBS)
    full_root = f"{root}/{subpath}"
    return (
        f"if [ -d {full_root} ]; then "
        f"cd {root} && find {subpath} -type f \\( {prune_clauses} \\) -prune "
        f"-o -type f -print0 | xargs -0 sha256sum 2>/dev/null; "
        f"fi"
    )


def parse_hashes(stdout: str) -> dict[str, str]:
    """Parse `sha256sum` output into {relative_path: hash}."""
    out: dict[str, str] = {}
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        # `<hash><space><space><path>`; path may contain spaces.
        parts = line.split("  ", 1)
        if len(parts) != 2:
            continue
        h, rel = parts
        out[rel] = h
    return out


def diff_trees(
    host: str, subpath: str
) -> tuple[set[str], set[str], dict[str, tuple[str, str]]]:
    """Return (only_in_a, only_in_b, differing) for one subpath."""
    a_out = ssh(host, build_find_command(TREE_A, subpath))
    b_out = ssh(host, build_find_command(TREE_B, subpath))
    a = parse_hashes(a_out)
    b = parse_hashes(b_out)
    a_keys = set(a)
    b_keys = set(b)
    only_a = a_keys - b_keys
    only_b = b_keys - a_keys
    differing: dict[str, tuple[str, str]] = {
        k: (a[k], b[k]) for k in (a_keys & b_keys) if a[k] != b[k]
    }
    return only_a, only_b, differing


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_HOST, help="SSH target")
    parser.add_argument(
        "--paths",
        nargs="+",
        default=DEFAULT_PATHS,
        help="Subpaths under each tree to compare",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-subpath OK lines; only print drift",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    drift: dict[str, dict[str, list[str]]] = defaultdict(
        lambda: {"only_a": [], "only_b": [], "differs": []}
    )

    for sub in args.paths:
        only_a, only_b, differing = diff_trees(args.host, sub)
        if not (only_a or only_b or differing):
            if not args.quiet:
                print(f"OK   {sub} (in sync)")
            continue
        drift[sub]["only_a"] = sorted(only_a)
        drift[sub]["only_b"] = sorted(only_b)
        drift[sub]["differs"] = sorted(differing)
        print(
            f"DRIFT {sub}: "
            f"{len(only_a)} only in {TREE_A}, "
            f"{len(only_b)} only in {TREE_B}, "
            f"{len(differing)} differing"
        )
        for f in drift[sub]["only_a"][:20]:
            print(f"     [A only]   {f}")
        for f in drift[sub]["only_b"][:20]:
            print(f"     [B only]   {f}")
        for f in drift[sub]["differs"][:20]:
            print(f"     [differs]  {f}")
        truncated = max(
            0,
            len(drift[sub]["only_a"]) - 20,
            len(drift[sub]["only_b"]) - 20,
            len(drift[sub]["differs"]) - 20,
        )
        if truncated:
            print("     ... and more (truncated to first 20 per category)")

    if drift:
        print()
        print(
            f"verify_deployment_sync: drift detected in "
            f"{len(drift)}/{len(args.paths)} subpaths"
        )
        print(
            "  → Both trees are LIVE. The dedup tree feeds grid-api, the "
            "grid_repo tree feeds everything else.\n"
            "  → Run your deploy script with the dual-rsync block (see "
            "deploy_to_grid_svr.sh)."
        )
        return 1

    print()
    print("verify_deployment_sync: all checked subpaths are in sync")
    return 0


if __name__ == "__main__":
    sys.exit(main())
