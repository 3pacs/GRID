"""Register sectors-v6 while v8 sits at its witnessed two-record registration; no sector opens here.

    hash-prereg
    register --log-dir NEW --v8-log-dir V8REG --vault-repo CLONE --code-sha SHA --run-at ISO
             [--execute --expected-head HEX]     (dry run unless --execute; HEX is the dry-run head)
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v4 as s4
from analysis import panel_insider_density_sectors_v6 as s6
from analysis import panel_insider_density_v8 as v8


def _run_at(raw: str) -> datetime:
    try:
        run_at = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
    except ValueError as exc:
        raise SystemExit("--run-at must be an ISO instant such as 2026-10-03T00:00:00+00:00") from exc
    if run_at.utcoffset() != timedelta(0) or run_at > datetime.now(timezone.utc):
        raise SystemExit("--run-at must be a UTC instant that is not in the future")
    return run_at


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("hash-prereg")
    reg = sub.add_parser("register")
    reg.add_argument("--log-dir", type=Path, required=True)
    reg.add_argument("--v8-log-dir", type=Path, required=True)
    reg.add_argument("--vault-repo", type=Path, required=True)
    reg.add_argument("--code-sha", required=True)
    reg.add_argument("--run-at", required=True)
    reg.add_argument("--execute", action="store_true")
    reg.add_argument("--expected-head")
    args = parser.parse_args(argv)
    if args.command == "hash-prereg":
        print(json.dumps({"body_sha256": v1.prereg_body_sha256(s4.REPO / s6.PREREG_PATH),
                          "pinned": s6.PREREG_BODY_SHA256}, indent=2))
        return
    s6.check_prereg()
    witness = v8.check_offhost(args.vault_repo)
    kwargs = {"v8_log_dir": args.v8_log_dir, "witness_repo": args.vault_repo, "census": witness.census}
    run_at = _run_at(args.run_at)
    preview = s6.register(args.log_dir, run_at, args.code_sha, dry_run=True, **kwargs)
    if not args.execute:
        print(json.dumps(preview, indent=2, sort_keys=True))
        return
    if preview["would_register_sha256"][-1] != args.expected_head:
        raise SystemExit("--expected-head differs from the dry-run head: nothing registered")
    out = s6.register(args.log_dir, run_at, args.code_sha, dry_run=False, **kwargs)
    print(json.dumps({**out, "chain": s6.registry(args.log_dir).verify_chain(),
                      "next": f"publish the anchor to {s6.WITNESS_PATH} before v8 discovery_opened"},
                     indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
