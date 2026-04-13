#!/usr/bin/env python3
"""Dispatch agent prompt composer — enforces preamble, coverage check, return contract.

The main Claude Code session calls this tool to produce a fully-composed agent
prompt instead of hand-rolling Agent() calls. It:

  1. Reads the canonical preamble from docs/AGENT_PROMPT_TEMPLATE.md.
  2. Runs `scripts/pre_create_check.py` for the task's primary concept and
     embeds the output so the agent starts with the real coverage picture.
  3. Prepends the task body with the preamble + embedded coverage check.
  4. Optionally writes the composed prompt to a file for copy-paste (or
     prints to stdout for piping into the Agent() tool).
  5. Appends the task to docs/WAVE_LOG.md with a timestamp and expected
     return hash.

It also has a `verify` subcommand that reads an agent's return message and
parses the `<agent-return>` JSON block, validates required fields, and prints
a TaskUpdate recommendation.

## Usage

```bash
# Compose a prompt for a new task
python3 scripts/dispatch_agent.py compose \
  --task-id 91 \
  --concept "synthesis oracle wiring" \
  --body-file /tmp/task_91_body.md \
  --out /tmp/task_91_prompt.md

# Short form: pipe body from stdin
echo "fix the foo module" | python3 scripts/dispatch_agent.py compose \
  --task-id 68 --concept "news tickers"

# Verify an agent's return message
python3 scripts/dispatch_agent.py verify --task-id 91 --return-file /tmp/agent_out.txt

# List recent waves
python3 scripts/dispatch_agent.py log
```

## File claims (TAF-3 groundwork)

The dispatcher maintains `.grid_backups/file_claims.json` — a small registry
of which task_id currently has a write lock on which files. `compose` takes
an optional `--files` list and refuses to compose if any of the files are
already claimed by another in-progress task. Use `release --task-id N` when
a task completes to free its claims.

This is the first defense against the "3 parallel agents edit capital_flow.py
and last-writer-wins clobbers changes" failure mode (TAF-3).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
PREAMBLE_PATH = REPO_ROOT / "docs" / "AGENT_PROMPT_TEMPLATE.md"
WAVE_LOG = REPO_ROOT / "docs" / "WAVE_LOG.md"
CLAIMS_FILE = REPO_ROOT / ".grid_backups" / "file_claims.json"
PRE_CREATE_CHECK = REPO_ROOT / "scripts" / "pre_create_check.py"


# ── Preamble extraction ────────────────────────────────────────────────────────


def load_preamble() -> str:
    """Pull the PREAMBLE section out of AGENT_PROMPT_TEMPLATE.md.

    The preamble is everything between the two `## PREAMBLE` markers.
    """
    text = PREAMBLE_PATH.read_text()
    # Match the section from "## PREAMBLE" through "## PREAMBLE ends"
    m = re.search(
        r"## PREAMBLE.*?\n(.*?)## PREAMBLE ends",
        text,
        re.DOTALL,
    )
    if not m:
        raise SystemExit(
            f"error: preamble delimiters not found in {PREAMBLE_PATH}"
        )
    return m.group(1).strip()


# ── Coverage check ────────────────────────────────────────────────────────────


def run_pre_create_check(concept: str, synonyms: list[str] | None = None) -> dict:
    """Run scripts/pre_create_check.py --json for the concept and parse output."""
    cmd = [sys.executable, str(PRE_CREATE_CHECK), concept, "--json"]
    if synonyms:
        cmd.extend(["--synonyms", ",".join(synonyms)])
    res = subprocess.run(cmd, capture_output=True, text=True)
    coverage_exists = res.returncode == 0
    try:
        data = json.loads(res.stdout) if res.stdout.strip() else {}
    except json.JSONDecodeError:
        data = {"raw": res.stdout}
    return {
        "exit": res.returncode,
        "coverage_exists": coverage_exists,
        "data": data,
        "raw_text": res.stdout,
    }


def format_coverage_block(concept: str, check: dict) -> str:
    """Format the coverage check result for embedding in the agent prompt."""
    lines = [
        "### pre_create_check result (injected by dispatch_agent.py)",
        "",
        f"**Concept:** `{concept}`",
        f"**Exit code:** {check['exit']} — {'COVERAGE EXISTS (extend canonical)' if check['coverage_exists'] else 'NO COVERAGE (safe to create)'}",
        "",
    ]
    data = check.get("data", {})
    if data.get("files"):
        lines.append("**Existing coverage:**")
        for f in data["files"][:15]:
            if isinstance(f, dict):
                path = f.get("path", "?")
                loc = f.get("loc", "?")
                lines.append(f"- `{path}` ({loc} LOC)")
                if f.get("functions"):
                    for func in f["functions"][:5]:
                        lines.append(f"    - {func}")
            else:
                lines.append(f"- {f}")
        lines.append("")
    if data.get("decision"):
        lines.append(f"**Decision:** {data['decision']}")
        lines.append("")
    if not data.get("files") and check["coverage_exists"] is False:
        lines.append("No existing files match this concept. Safe to create a new module.")
        lines.append("")
    return "\n".join(lines)


# ── File claims (TAF-3) ───────────────────────────────────────────────────────


def load_claims() -> dict:
    if not CLAIMS_FILE.exists():
        return {}
    try:
        return json.loads(CLAIMS_FILE.read_text())
    except json.JSONDecodeError:
        return {}


def save_claims(claims: dict) -> None:
    CLAIMS_FILE.parent.mkdir(parents=True, exist_ok=True)
    CLAIMS_FILE.write_text(json.dumps(claims, indent=2, sort_keys=True))


def check_claims(files: list[str], task_id: int) -> list[tuple[str, int]]:
    """Return list of (file, claiming_task_id) conflicts."""
    claims = load_claims()
    conflicts = []
    for f in files:
        existing = claims.get(f)
        if existing is None:
            continue
        # Claims may be stored as an int (legacy) or a dict (new format).
        if isinstance(existing, dict):
            existing_id = existing.get("task_id")
        else:
            existing_id = existing
        if existing_id and existing_id != task_id:
            conflicts.append((f, existing_id))
    return conflicts


def claim_files(files: list[str], task_id: int) -> None:
    claims = load_claims()
    now = datetime.now(timezone.utc).isoformat()
    for f in files:
        claims[f] = {
            "task_id": task_id,
            "claimed_at": now,
        }
    save_claims(claims)


def release_files(task_id: int) -> int:
    claims = load_claims()
    released = 0
    to_delete = [
        f for f, c in claims.items()
        if isinstance(c, dict) and c.get("task_id") == task_id
    ]
    for f in to_delete:
        del claims[f]
        released += 1
    save_claims(claims)
    return released


# ── Wave log ──────────────────────────────────────────────────────────────────


def append_wave_log(entry: dict) -> None:
    WAVE_LOG.parent.mkdir(parents=True, exist_ok=True)
    # Wave log is append-only markdown, one row per entry.
    if not WAVE_LOG.exists():
        WAVE_LOG.write_text("# GRID Wave Log\n\nAppend-only record of dispatched agent tasks.\n\n")
    row = (
        f"- **{entry['timestamp']}** — task #{entry['task_id']} — "
        f"concept: `{entry['concept']}` — "
        f"coverage: {entry['coverage_status']} — "
        f"files: {len(entry.get('claimed_files') or [])}\n"
    )
    with WAVE_LOG.open("a") as f:
        f.write(row)


# ── Compose ───────────────────────────────────────────────────────────────────


def cmd_compose(args: argparse.Namespace) -> int:
    # Load body from file or stdin
    if args.body_file:
        body = Path(args.body_file).read_text()
    elif not sys.stdin.isatty():
        body = sys.stdin.read()
    else:
        print("error: --body-file or stdin required", file=sys.stderr)
        return 1

    # File claims
    files = args.files or []
    if files:
        conflicts = check_claims(files, args.task_id)
        if conflicts:
            print("error: file claims conflict detected:", file=sys.stderr)
            for f, claiming in conflicts:
                print(f"  {f} already claimed by task #{claiming}", file=sys.stderr)
            print("  → either wait for the other task, release their claims, or use a different file", file=sys.stderr)
            return 2
        claim_files(files, args.task_id)

    # Run coverage check
    check = run_pre_create_check(
        args.concept,
        synonyms=args.synonyms.split(",") if args.synonyms else None,
    )

    # Compose
    preamble = load_preamble()
    coverage_block = format_coverage_block(args.concept, check)

    composed = (
        f"{preamble}\n\n"
        f"---\n\n"
        f"{coverage_block}\n"
        f"---\n\n"
        f"## Task body — task #{args.task_id}\n\n"
        f"{body.strip()}\n\n"
        f"---\n\n"
        f"Remember to end your message with the `<agent-return>` JSON block "
        f"including `task_id: {args.task_id}`."
    )

    # Log
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "task_id": args.task_id,
        "concept": args.concept,
        "coverage_status": (
            "extend"
            if check["coverage_exists"]
            else "new"
        ),
        "claimed_files": files,
    }
    append_wave_log(entry)

    # Output
    if args.out:
        Path(args.out).write_text(composed)
        print(f"Composed prompt written to {args.out}")
        print(f"  length: {len(composed)} chars")
        print(f"  coverage: {entry['coverage_status']}")
        if files:
            print(f"  claimed files: {', '.join(files)}")
    else:
        print(composed)
    return 0


# ── Verify ────────────────────────────────────────────────────────────────────


REQUIRED_RETURN_FIELDS = [
    "task_id",
    "files_modified",
    "files_deleted",
    "files_created",
    "tests_passed",
    "endpoints_verified",
    "deploy_hash_verified",
    "smoke_passed",
    "pre_create_check_result",
]


def cmd_verify(args: argparse.Namespace) -> int:
    text = Path(args.return_file).read_text() if args.return_file else sys.stdin.read()
    m = re.search(r"<agent-return>(.*?)</agent-return>", text, re.DOTALL)
    if not m:
        print("FAIL: no <agent-return> block found", file=sys.stderr)
        return 1
    try:
        payload = json.loads(m.group(1))
    except json.JSONDecodeError as e:
        print(f"FAIL: invalid JSON in <agent-return>: {e}", file=sys.stderr)
        return 2

    missing = [f for f in REQUIRED_RETURN_FIELDS if f not in payload]
    if missing:
        print(f"FAIL: missing required fields: {', '.join(missing)}", file=sys.stderr)
        return 3

    if args.task_id and payload.get("task_id") != args.task_id:
        print(
            f"FAIL: task_id mismatch — expected {args.task_id}, got {payload.get('task_id')}",
            file=sys.stderr,
        )
        return 4

    # Hard requirements
    if not payload.get("deploy_hash_verified"):
        print("FAIL: deploy_hash_verified is false — agent did not use deploy.py", file=sys.stderr)
        return 5
    if payload.get("smoke_passed") is False:
        print("FAIL: smoke_passed is false — regression gate failed", file=sys.stderr)
        return 6

    # All good — print summary + recommended TaskUpdate
    print("PASS")
    print(f"  task: #{payload.get('task_id')}")
    print(f"  files_created: {len(payload.get('files_created') or [])}")
    print(f"  files_modified: {len(payload.get('files_modified') or [])}")
    print(f"  files_deleted: {len(payload.get('files_deleted') or [])}")
    print(f"  loc_delta: {payload.get('loc_delta', 0)}")
    print(f"  tests_passed: {payload.get('tests_passed')}")
    print(f"  endpoints_verified: {payload.get('endpoints_verified')}")
    print(f"  pre_create_check: {payload.get('pre_create_check_result')}")
    print(f"  notes: {payload.get('notes', '')}")
    print()
    print("Recommended main-session actions:")
    print(f"  1. TaskUpdate taskId={payload.get('task_id')} status=completed")
    print(f"  2. dispatch_agent.py release --task-id {payload.get('task_id')}")
    return 0


# ── Release ───────────────────────────────────────────────────────────────────


def cmd_release(args: argparse.Namespace) -> int:
    n = release_files(args.task_id)
    print(f"released {n} file claim(s) for task #{args.task_id}")
    return 0


# ── Log ───────────────────────────────────────────────────────────────────────


def cmd_log(args: argparse.Namespace) -> int:
    if WAVE_LOG.exists():
        lines = WAVE_LOG.read_text().splitlines()
        # Tail last 20 entries
        print("\n".join(lines[-args.lines :]))
    else:
        print("(wave log empty)")
    return 0


# ── Claims ────────────────────────────────────────────────────────────────────


def cmd_claims(args: argparse.Namespace) -> int:
    claims = load_claims()
    if not claims:
        print("(no active file claims)")
        return 0
    print("Active file claims:")
    for f, c in sorted(claims.items()):
        if isinstance(c, dict):
            print(f"  {f}  task=#{c.get('task_id')}  since={c.get('claimed_at')}")
        else:
            print(f"  {f}  {c}")
    return 0


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="GRID agent prompt composer + return verifier",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_compose = sub.add_parser("compose", help="compose an agent prompt")
    p_compose.add_argument("--task-id", type=int, required=True)
    p_compose.add_argument("--concept", required=True, help="primary concept for pre_create_check")
    p_compose.add_argument("--synonyms", default="", help="comma-separated synonym list")
    p_compose.add_argument("--body-file", help="path to task body markdown (or pipe to stdin)")
    p_compose.add_argument("--out", help="write composed prompt here (default: stdout)")
    p_compose.add_argument("--files", nargs="*", help="files to claim (conflict check)")

    p_verify = sub.add_parser("verify", help="verify an agent's <agent-return> block")
    p_verify.add_argument("--task-id", type=int, help="expected task_id")
    p_verify.add_argument("--return-file", help="path to agent's return message (or pipe to stdin)")

    p_release = sub.add_parser("release", help="release file claims for a task")
    p_release.add_argument("--task-id", type=int, required=True)

    p_log = sub.add_parser("log", help="show recent wave log entries")
    p_log.add_argument("--lines", type=int, default=20)

    sub.add_parser("claims", help="show active file claims")

    args = parser.parse_args()

    if args.cmd == "compose":
        return cmd_compose(args)
    if args.cmd == "verify":
        return cmd_verify(args)
    if args.cmd == "release":
        return cmd_release(args)
    if args.cmd == "log":
        return cmd_log(args)
    if args.cmd == "claims":
        return cmd_claims(args)
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
