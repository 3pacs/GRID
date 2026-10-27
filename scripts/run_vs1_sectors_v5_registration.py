"""Register sectors-v5 after v7 registration; no sector price opening here."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from analysis import panel_insider_density_sectors_v5 as s5
from analysis import panel_insider_density_sectors_v4 as s4
from analysis import panel_insider_density_v7 as v7


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("hash-prereg")
    register = sub.add_parser("register")
    register.add_argument("--log-dir", type=Path, required=True)
    register.add_argument("--v7-log-dir", type=Path, required=True)
    register.add_argument("--vault-repo", type=Path, required=True)
    register.add_argument("--code-sha", required=True)
    args = parser.parse_args(argv)
    if args.command == "hash-prereg":
        from analysis import panel_insider_density as v1
        print(json.dumps({"body_sha256": v1.prereg_body_sha256(s4.REPO / s5.PREREG_PATH),
                          "pinned": s5.PREREG_BODY_SHA256}, indent=2))
        return
    s5.check_prereg()
    witness = v7.check_offhost(args.vault_repo)
    records = s5.register(args.log_dir, datetime.now(timezone.utc), args.code_sha,
                          v7_log_dir=args.v7_log_dir, witness_repo=args.vault_repo,
                          census=witness.census)
    print(json.dumps({"appended": [r["kind"] for r in records],
                      "chain": s5.registry(args.log_dir).verify_chain(),
                      "anchor_path": str(s5.registry(args.log_dir).anchor_path),
                      "canonical_witness_path": s5.WITNESS_PATH}, indent=2))


if __name__ == "__main__":
    main()
