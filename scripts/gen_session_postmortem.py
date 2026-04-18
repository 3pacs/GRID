#!/usr/bin/env python3
"""TAF-OBS3 — Generate a SESSION_POSTMORTEM.md from session-local artifacts.

Reads three structured sources written automatically during the session:

  1. ``.grid_backups/deploy_log.jsonl``   — every successful deploy
  2. ``docs/WAVE_LOG.md``                 — dispatcher's wave-level rollup
  3. ``git log`` (current branch ahead of base) — commit history

…and emits ``docs/SESSION_POSTMORTEM_<UTC_DATE>.md`` summarising what shipped,
which tests + smoke checks passed, and any deploy failures so the next
session can pick up cleanly.

Usage
-----
    python3 scripts/gen_session_postmortem.py
    python3 scripts/gen_session_postmortem.py --since 24h
    python3 scripts/gen_session_postmortem.py --base origin/main --out custom.md

Designed to run as the last step of a wrap-up routine. Idempotent — re-running
overwrites the same dated file.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEPLOY_LOG = REPO_ROOT / ".grid_backups" / "deploy_log.jsonl"
WAVE_LOG = REPO_ROOT / "docs" / "WAVE_LOG.md"
DEFAULT_OUT = REPO_ROOT / "docs"


# ── Helpers ────────────────────────────────────────────────────────────────


def _parse_since(spec: str) -> timedelta:
    """Parse '24h', '7d', '1w' → timedelta. Raises on bad input."""
    m = re.fullmatch(r"(\d+)([hdw])", spec.strip().lower())
    if not m:
        raise SystemExit(f"error: --since must be like 24h / 7d / 1w, got {spec!r}")
    n = int(m.group(1))
    unit = m.group(2)
    if unit == "h":
        return timedelta(hours=n)
    if unit == "d":
        return timedelta(days=n)
    return timedelta(weeks=n)


def _read_deploy_log(since: datetime) -> list[dict]:
    """Return every deploy_log row with started_at >= since, parsed to dict."""
    if not DEPLOY_LOG.exists():
        return []
    rows: list[dict] = []
    with DEPLOY_LOG.open() as fh:
        for line in fh:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            try:
                ts = datetime.fromisoformat(row.get("started_at", ""))
            except ValueError:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= since:
                rows.append(row)
    return rows


def _read_wave_log(since: datetime) -> str:
    """Return the WAVE_LOG.md tail since the cutoff (or the whole file)."""
    if not WAVE_LOG.exists():
        return ""
    text = WAVE_LOG.read_text()
    # WAVE_LOG entries usually start with `## YYYY-MM-DD`. Best-effort filter.
    cutoff_str = since.strftime("%Y-%m-%d")
    lines = text.splitlines()
    keep: list[str] = []
    capturing = False
    for line in lines:
        m = re.match(r"^##\s+(\d{4}-\d{2}-\d{2})", line)
        if m:
            capturing = m.group(1) >= cutoff_str
        if capturing:
            keep.append(line)
    return "\n".join(keep) if keep else text


def _git_log(base: str, since: datetime) -> list[dict]:
    """Return commits on HEAD ahead of base, since the cutoff."""
    try:
        out = subprocess.run(
            [
                "git",
                "log",
                f"--since={since.isoformat()}",
                f"{base}..HEAD",
                "--pretty=format:%H%x09%ai%x09%s",
            ],
            capture_output=True,
            text=True,
            check=False,
            cwd=REPO_ROOT,
        )
    except FileNotFoundError:
        return []
    if out.returncode != 0:
        return []
    rows: list[dict] = []
    for line in out.stdout.splitlines():
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        rows.append({"sha": parts[0][:8], "when": parts[1], "subject": parts[2]})
    return rows


# ── Markdown rendering ─────────────────────────────────────────────────────


def _render(
    since: datetime,
    deploys: list[dict],
    wave_tail: str,
    commits: list[dict],
) -> str:
    now = datetime.now(timezone.utc)
    lines: list[str] = []
    lines.append(f"# Session postmortem — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append(f"_Window: since {since.isoformat()} (UTC)_")
    lines.append("")

    # ── Headline
    n_deploys = len(deploys)
    n_failed = sum(1 for d in deploys if d.get("exit_code", 0) != 0)
    n_smoke_ok = sum(1 for d in deploys if d.get("smoke_passed") is True)
    n_smoke_fail = sum(1 for d in deploys if d.get("smoke_passed") is False)
    n_files = sum(len(d.get("files", []) or []) for d in deploys)
    lines.append("## Headline")
    lines.append("")
    lines.append(f"- **{n_deploys}** deploys ({n_failed} failed)")
    lines.append(f"- **{n_files}** unique file writes (hash-verified)")
    lines.append(f"- **{len(commits)}** commits ahead of base branch")
    lines.append(
        f"- Smoke tests: {n_smoke_ok} green / {n_smoke_fail} red / "
        f"{n_deploys - n_smoke_ok - n_smoke_fail} not run"
    )
    lines.append("")

    # ── Failures (loud)
    failures = [d for d in deploys if d.get("exit_code", 0) != 0]
    if failures:
        lines.append("## Deploy failures (review before next session)")
        lines.append("")
        for d in failures:
            ts = d.get("started_at", "?")
            err = next(
                (f.get("error", "") for f in (d.get("files") or []) if f.get("error")),
                "(no error in log)",
            )
            lines.append(f"- `{ts}` exit_code={d.get('exit_code')}: {err}")
        lines.append("")

    # ── Successful deploys (compact)
    if deploys:
        lines.append("## Successful deploys")
        lines.append("")
        lines.append("| When (UTC) | Files | Smoke | Snapshot |")
        lines.append("|---|---|---|---|")
        for d in deploys:
            if d.get("exit_code", 0) != 0:
                continue
            ts = d.get("started_at", "?")[:19].replace("T", " ")
            files = len(d.get("files") or [])
            smoke = (
                "✓" if d.get("smoke_passed") is True
                else ("✗" if d.get("smoke_passed") is False else "—")
            )
            snap = d.get("snapshot_dir") or ""
            snap_short = snap.rsplit("/", 1)[-1] if snap else "—"
            lines.append(f"| {ts} | {files} | {smoke} | `{snap_short}` |")
        lines.append("")

    # ── Commits
    if commits:
        lines.append("## Commits")
        lines.append("")
        for c in commits:
            lines.append(f"- `{c['sha']}` {c['subject']}")
        lines.append("")

    # ── Wave log tail
    if wave_tail.strip():
        lines.append("## Wave log (recent)")
        lines.append("")
        lines.append(wave_tail.strip())
        lines.append("")

    # ── Next session checklist
    lines.append("## Next session — pickup checklist")
    lines.append("")
    lines.append("- [ ] Pull the latest branch and re-run smoke endpoints")
    lines.append("- [ ] Review any failures listed above")
    lines.append("- [ ] Check `TaskList` for pending items")
    lines.append("- [ ] Re-run `python3 scripts/lint_module_inventory.py` if any modules were added/removed")
    lines.append("")

    return "\n".join(lines)


# ── Main ───────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate SESSION_POSTMORTEM.md from session-local artifacts.",
    )
    parser.add_argument(
        "--since", default="24h",
        help="Lookback window: 24h, 7d, 1w (default 24h)",
    )
    parser.add_argument(
        "--base", default="origin/main",
        help="Git base branch for the commit diff (default origin/main)",
    )
    parser.add_argument(
        "--out", default=None,
        help="Output path. Default: docs/SESSION_POSTMORTEM_YYYY-MM-DD.md",
    )
    args = parser.parse_args()

    since_dt = datetime.now(timezone.utc) - _parse_since(args.since)

    deploys = _read_deploy_log(since_dt)
    wave_tail = _read_wave_log(since_dt)
    commits = _git_log(args.base, since_dt)

    md = _render(since_dt, deploys, wave_tail, commits)

    if args.out:
        out_path = Path(args.out)
    else:
        out_path = DEFAULT_OUT / f"SESSION_POSTMORTEM_{datetime.now(timezone.utc):%Y-%m-%d}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md)
    print(f"wrote {out_path} ({len(md)} chars, {len(deploys)} deploys, {len(commits)} commits)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
