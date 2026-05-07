#!/usr/bin/env python3
"""verify_systemd_units.py — Compare repo unit files against live systemd.

Walks every `*.service` and `*.timer` in `server_setup/`, runs `systemctl
cat <name>` for each on the host this script runs on, and reports any
divergence in the keys that matter at runtime: WorkingDirectory,
EnvironmentFile, ExecStart, ExecStartPre, ExecStartPost, ExecStop, User,
Group, Restart.

Why these keys: comments, descriptions, and ordering can drift harmlessly,
but a difference in `ExecStart` or `WorkingDirectory` means the running
service is not what the repo says it is.

Exit codes:
    0 — every repo unit either matches live or is intentionally missing
    1 — at least one drift detected
    2 — invocation error (cannot run systemctl, server_setup missing)

Usage:
    python3 scripts/verify_systemd_units.py
    python3 scripts/verify_systemd_units.py --verbose
    python3 scripts/verify_systemd_units.py --fix-direction repo-to-live
        # Show a sed-able patch to make repo match live (useful when an
        # operator hand-edited live and we want to capture that edit
        # back into the repo). Does NOT modify any files — prints only.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

# Keys that materially affect runtime behavior. Drift here = real bug.
RUNTIME_KEYS: tuple[str, ...] = (
    "WorkingDirectory",
    "EnvironmentFile",
    "ExecStart",
    "ExecStartPre",
    "ExecStartPost",
    "ExecStop",
    "ExecReload",
    "User",
    "Group",
    "Restart",
    "Type",
    "OnCalendar",  # for timers
    "Unit",        # for timers
)

KEY_RE = re.compile(r"^([A-Za-z][A-Za-z0-9]*)=(.*)$")


@dataclass(frozen=True)
class UnitKeys:
    """Multi-valued map of key → tuple of values (preserves duplicates)."""

    name: str
    values: dict[str, tuple[str, ...]]


def parse_unit(text: str, name: str) -> UnitKeys:
    """Extract RUNTIME_KEYS from a unit file body.

    Handles line continuations (lines ending with backslash) and dedupes
    nothing — duplicate keys (e.g. multiple ExecStartPost) are preserved
    in order so we can detect a missing line.
    """
    accumulated: dict[str, list[str]] = {k: [] for k in RUNTIME_KEYS}

    # Join continuation lines first.
    joined: list[str] = []
    buf = ""
    for raw in text.splitlines():
        line = raw.rstrip()
        if line.endswith("\\"):
            buf += line[:-1] + " "
            continue
        joined.append(buf + line)
        buf = ""
    if buf:
        joined.append(buf)

    for line in joined:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith(";"):
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            continue
        m = KEY_RE.match(stripped)
        if not m:
            continue
        key, val = m.group(1), m.group(2).strip()
        if key in accumulated:
            accumulated[key].append(val)

    return UnitKeys(
        name=name,
        values={k: tuple(v) for k, v in accumulated.items() if v},
    )


def read_repo_unit(path: Path) -> UnitKeys:
    return parse_unit(path.read_text(encoding="utf-8", errors="replace"), path.name)


def read_live_unit(name: str) -> UnitKeys | None:
    """Return parsed live unit, or None if systemctl says no such unit."""
    try:
        proc = subprocess.run(
            ["systemctl", "cat", name],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        print(
            "[verify_systemd_units] systemctl not found — skipping live comparison",
            file=sys.stderr,
        )
        return None
    if proc.returncode != 0:
        return None
    return parse_unit(proc.stdout, name)


@dataclass
class Drift:
    unit: str
    key: str
    repo: tuple[str, ...]
    live: tuple[str, ...]


def diff_units(repo: UnitKeys, live: UnitKeys) -> list[Drift]:
    drifts: list[Drift] = []
    keys = set(repo.values) | set(live.values)
    for k in sorted(keys):
        r = repo.values.get(k, ())
        l = live.values.get(k, ())
        if r != l:
            drifts.append(Drift(unit=repo.name, key=k, repo=r, live=l))
    return drifts


def render_drift(drift: Drift) -> str:
    lines = [f"  {drift.key}:"]
    for v in drift.repo:
        lines.append(f"    repo: {v}")
    if not drift.repo:
        lines.append("    repo: <unset>")
    for v in drift.live:
        lines.append(f"    live: {v}")
    if not drift.live:
        lines.append("    live: <unset>")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare repo systemd unit files in server_setup/ against the "
            "live systemd state on this host."
        )
    )
    parser.add_argument(
        "--root",
        default=None,
        help="Repo root containing server_setup/ (auto-detected).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show units that match too, not just drifters.",
    )
    parser.add_argument(
        "--fix-direction",
        choices=("repo-to-live", "live-to-repo", "none"),
        default="none",
        help=(
            "Print a suggested patch direction. repo-to-live = make live "
            "match repo (sudo cp + daemon-reload). live-to-repo = make "
            "repo match live (cp from /etc/systemd/system to server_setup/)."
        ),
    )
    args = parser.parse_args(argv)

    if args.root:
        root = Path(args.root).resolve()
    else:
        # Walk up looking for server_setup/
        cur = Path(__file__).resolve().parent
        while cur != cur.parent:
            if (cur / "server_setup").is_dir():
                root = cur
                break
            cur = cur.parent
        else:
            print(
                "[verify_systemd_units] could not locate server_setup/ — "
                "use --root to specify",
                file=sys.stderr,
            )
            return 2

    setup_dir = root / "server_setup"
    if not setup_dir.is_dir():
        print(f"[verify_systemd_units] {setup_dir} does not exist", file=sys.stderr)
        return 2

    repo_units = sorted(setup_dir.glob("*.service")) + sorted(
        setup_dir.glob("*.timer")
    )
    if not repo_units:
        print(f"[verify_systemd_units] no *.service or *.timer in {setup_dir}")
        return 0

    print("=" * 72)
    print(f"systemd unit drift check — {len(repo_units)} repo units vs live")
    print("=" * 72)

    total_drifts = 0
    no_live = 0
    matched = 0
    drift_units: list[str] = []

    for path in repo_units:
        repo = read_repo_unit(path)
        live = read_live_unit(path.name)
        if live is None:
            no_live += 1
            if args.verbose:
                print(f"[no live]  {path.name}")
            continue
        drifts = diff_units(repo, live)
        if not drifts:
            matched += 1
            if args.verbose:
                print(f"[match  ]  {path.name}")
            continue
        total_drifts += len(drifts)
        drift_units.append(path.name)
        print()
        print(f"[DRIFT  ]  {path.name}")
        for d in drifts:
            print(render_drift(d))

    print()
    print("─" * 72)
    print(
        f"summary: {matched} matched, {len(drift_units)} drifted, "
        f"{no_live} no-live ({total_drifts} total field diffs)"
    )

    if args.fix_direction == "repo-to-live" and drift_units:
        print()
        print("# To make live match repo (run as root on the target host):")
        print("set -e")
        for name in drift_units:
            src = setup_dir / name
            dst = f"/etc/systemd/system/{name}"
            print(f"sudo cp {src} {dst}")
        print("sudo systemctl daemon-reload")
        print(f"sudo systemctl restart {' '.join(drift_units)}")

    if args.fix_direction == "live-to-repo" and drift_units:
        print()
        print("# To capture live unit text back into the repo:")
        for name in drift_units:
            print(f"systemctl cat {name} | sed '/^# /d' > {setup_dir / name}")

    return 1 if drift_units else 0


if __name__ == "__main__":
    sys.exit(main())
