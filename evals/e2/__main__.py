"""E2 command line.

    python -m evals.e2 verify [--board-dir DIR] [--vault-repo CLONE]   # manifest (+ ledger, + off-host anchors)
    python -m evals.e2 run --board-dir DIR [--s10-log-dir D] [--gex-log-dir D] [--stream NAME=PATH ...]
                           [--prices raw_series:tiingo|raw_series:yfinance|spy_close_receipt]
                           [--witness-worktree VAULT_WORKTREE] [--now ISO]   # the daily job (the ONLY writer)
    python -m evals.e2 report --board-dir DIR [--format md|json]          # read-only view
    python -m evals.e2 manifest --write --version e2-vN                   # maintainers: a NEW version

``run`` verifies the manifest first and refuses to touch a ledger that was
started by different code. Stream logs are opened read-only. With
``--prices`` it opens ONE read-only database session (never inside the
03:30-10:30 UTC backup window); without it, generic price-resolved streams
simply stay pending. ``--now`` exists for offline dry runs over copied logs;
the ledger refuses a run instant earlier than its last record.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import subprocess
import sys
from datetime import datetime, time, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
HEX40 = re.compile(r"[0-9a-f]{40}")
BACKUP_WINDOW = (time(3, 30), time(10, 30))


def resolve_code_sha(repo_root: Path) -> str:
    """The installed archive's VERSION, else HEAD of a checkout with no modified tracked files."""
    version = Path(repo_root) / "VERSION"
    if version.exists():
        sha = version.read_text(encoding="utf-8").strip()
        if not HEX40.fullmatch(sha):
            raise RuntimeError(f"{version} does not hold a full commit sha")
        return sha
    head = subprocess.run(["git", "-C", str(repo_root), "rev-parse", "HEAD"], capture_output=True, text=True,
                          timeout=10, check=False)
    if head.returncode != 0 or not HEX40.fullmatch(head.stdout.strip()):
        raise RuntimeError(f"could not resolve code_sha under {repo_root}: no VERSION file and no git HEAD")
    dirty = subprocess.run(["git", "-C", str(repo_root), "status", "--porcelain", "--untracked-files=no"],
                           capture_output=True, text=True, timeout=10, check=False)
    if dirty.returncode != 0 or dirty.stdout.strip():
        raise RuntimeError(f"{repo_root} has modified tracked files: code_sha would not match the code")
    return head.stdout.strip()


def build_adapters(args, rules: dict, prices) -> list:
    from evals.e2.adapters.e2_stream import E2StreamAdapter
    from evals.e2.adapters.gex_levels import GexLevelsAdapter
    from evals.e2.adapters.s10 import S10Adapter

    adapters = []
    if args.s10_log_dir:
        adapters.append(S10Adapter(args.s10_log_dir, rules["streams"]["s10_hypothesis_forward_v1"],
                                   rules["rules"]["s10.ts_ic.v1"]))
    if args.gex_log_dir:
        adapters.append(GexLevelsAdapter(args.gex_log_dir, rules["streams"]["gex_levels_v1"]))
    for spec in args.stream or ():
        name, _, path = spec.partition("=")
        if name not in rules["streams"] or rules["streams"][name].get("adapter") != "e2_stream":
            raise SystemExit(f"stream {name!r} is not registered in rules.json as an e2_stream: adding a stream "
                             "is a new E2 version")
        adapters.append(E2StreamAdapter(Path(path), name, rules["streams"][name], rules, prices))
    return adapters


@contextlib.contextmanager
def price_source(spec: str | None, now: datetime):
    if not spec:
        yield None
        return
    if BACKUP_WINDOW[0] <= now.astimezone(timezone.utc).time() < BACKUP_WINDOW[1]:
        print("E2: inside the 03:30-10:30 UTC backup window: no database read this run", file=sys.stderr)
        yield None
        return
    from config import settings
    from evals.e2 import resolve

    with resolve.read_only_connection(settings.DB_URL) as conn:
        if spec == "spy_close_receipt":
            yield resolve.SpyCloseReceiptSource(conn)
        elif spec.startswith("raw_series:"):
            yield resolve.KnownAtCloseSource(conn, source=spec.split(":", 1)[1])
        else:
            raise SystemExit(f"unknown --prices {spec!r}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m evals.e2", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    ver = sub.add_parser("verify", help="manifest check (+ ledger chain, + off-host anchors)")
    ver.add_argument("--board-dir", type=Path)
    ver.add_argument("--vault-repo", type=Path, help="any local git clone; the pinned vault main is fetched into it")
    run_p = sub.add_parser("run", help="ingest, resolve, score, snapshot (the only ledger writer)")
    run_p.add_argument("--board-dir", type=Path, required=True)
    run_p.add_argument("--s10-log-dir", type=Path)
    run_p.add_argument("--gex-log-dir", type=Path)
    run_p.add_argument("--stream", action="append", help="NAME=PATH of a registered e2-stream-v1 log")
    run_p.add_argument("--prices", help="raw_series:tiingo | raw_series:yfinance | spy_close_receipt")
    run_p.add_argument("--witness-worktree", type=Path, help="vault worktree to append anchor lines to")
    run_p.add_argument("--now", help="ISO-8601 run instant (offline dry runs); default: the wall clock")
    rep = sub.add_parser("report", help="read-only view of the latest snapshot")
    rep.add_argument("--board-dir", type=Path, required=True)
    rep.add_argument("--format", choices=("md", "json"), default="md")
    man = sub.add_parser("manifest", help="(maintainers) regenerate MANIFEST.sha256 for a NEW version")
    man.add_argument("--write", action="store_true", required=True)
    man.add_argument("--version", required=True)
    args = parser.parse_args(argv)

    from evals.e2 import VERSION, manifest, report, scoring

    if args.command == "manifest":
        if args.version != VERSION:
            print(f"refused: evals.e2.VERSION is {VERSION!r}; bump it (and rules.json) to {args.version!r} first",
                  file=sys.stderr)
            return 2
        manifest.write(args.version)
        print(json.dumps(manifest.verify(), sort_keys=True))
        return 0
    if args.command == "report":
        board = report.load_board(args.board_dir, VERSION)
        if args.format == "json":
            print(json.dumps(board, indent=2, sort_keys=True))
        else:
            print(report.render_markdown(board["snapshot"]), end="")
        return 0 if board["status"] in ("ok", "not_initialized") else 1
    try:
        info = manifest.verify()
    except manifest.ManifestError as exc:
        print(f"E2 manifest FAILED: {exc}", file=sys.stderr)
        return 1
    if args.command == "verify":
        out: dict = {"manifest": info}
        if args.board_dir:
            from evals.e2.chain import Ledger

            out["ledger"] = Ledger(args.board_dir, VERSION).verify(manifest_sha256=info["manifest_sha256"])
            if args.vault_repo:
                from evals.e2 import witness

                out["offhost"] = witness.verify_against_offhost(args.board_dir, VERSION, args.vault_repo)
        print(json.dumps(out, indent=2, sort_keys=True, default=str))
        return 0 if (out.get("ledger") or {"ok": True})["ok"] else 1

    rules, cost_model = scoring.load_json("rules.json"), scoring.load_json("cost_model.json")
    if rules["version"] != VERSION:
        print("rules.json version differs from evals.e2.VERSION", file=sys.stderr)
        return 1
    now = datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now else datetime.now(timezone.utc)
    if now.tzinfo is None:
        print("--now must carry a timezone", file=sys.stderr)
        return 2
    for log_dir in (args.s10_log_dir, args.gex_log_dir):
        if log_dir is not None and Path(log_dir).resolve() == Path(args.board_dir).resolve():
            print("the board directory must not be a stream's log directory", file=sys.stderr)
            return 2
    from evals.e2 import board

    code_sha = resolve_code_sha(REPO)
    with price_source(args.prices, now) as prices:
        result = board.run(args.board_dir, build_adapters(args, rules, prices), now, rules=rules,
                           cost_model=cost_model, manifest_info=info, code_sha=code_sha)
    exported = []
    if args.witness_worktree:
        from evals.e2 import witness

        exported = witness.export_anchors(args.board_dir, VERSION, args.witness_worktree)
    snap = result["snapshot"]
    print(json.dumps({"run_at": snap["run_at"], "appended": result["appended"], "counts": snap["counts"],
                      "streams": {k: {"ok": v["ok"], **({"error": v["error"]} if not v["ok"] else {})}
                                  for k, v in snap["streams"].items()},
                      "anchor_lines_exported": len(exported)}, sort_keys=True))
    return 0 if all(v["ok"] for v in snap["streams"].values()) else 3


if __name__ == "__main__":
    sys.exit(main())
