#!/usr/bin/env python3
"""Audit whether the Hermes agent fleet is present and recently maintained."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_REGISTRY = Path("docs/hermes-agent-fleet.json")
DEFAULT_OUTPUT_JSON = Path("output/hermes_agent_fleet_audit.json")
DEFAULT_OUTPUT_MD = Path("output/hermes_agent_fleet_audit.md")
SECONDS_PER_DAY = 86400


@dataclass(frozen=True)
class WatchedPath:
    path: str
    exists: bool
    mtime: float | None = None
    age_days: float | None = None


@dataclass(frozen=True)
class AgentAudit:
    agent_id: str
    status: str
    max_age_days: int
    latest_age_days: float | None
    missing_paths: list[str]
    watched_paths: list[WatchedPath]
    recommended_action: str


def load_registry(path: Path) -> dict[str, Any]:
    registry = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(registry.get("agents"), list):
        raise ValueError("Registry must contain an agents list.")
    return registry


def audit_agent(agent: dict[str, Any], *, root: Path, now: float) -> AgentAudit:
    agent_id = str(agent.get("id") or "unknown")
    max_age_days = int(agent.get("max_age_days") or 14)
    watched: list[WatchedPath] = []
    missing: list[str] = []
    mtimes: list[float] = []

    for raw_path in agent.get("watch", []):
        rel_path = str(raw_path)
        path = root / rel_path
        if not path.exists():
            missing.append(rel_path)
            watched.append(WatchedPath(path=rel_path, exists=False))
            continue
        mtime = path.stat().st_mtime
        mtimes.append(mtime)
        watched.append(
            WatchedPath(
                path=rel_path,
                exists=True,
                mtime=mtime,
                age_days=round((now - mtime) / SECONDS_PER_DAY, 2),
            )
        )

    latest_age = round((now - max(mtimes)) / SECONDS_PER_DAY, 2) if mtimes else None
    if missing:
        status = "missing"
        action = "Restore or remove missing watched paths before trusting this agent lane."
    elif latest_age is not None and latest_age > max_age_days:
        status = "stale"
        action = "Refresh the agent prompt/runbook or verify it still reflects current fleet behavior."
    else:
        status = "current"
        action = "No action required; keep scheduled audit running."

    return AgentAudit(
        agent_id=agent_id,
        status=status,
        max_age_days=max_age_days,
        latest_age_days=latest_age,
        missing_paths=missing,
        watched_paths=watched,
        recommended_action=action,
    )


def build_audit_payload(registry: dict[str, Any], *, root: Path, now: float | None = None) -> dict[str, Any]:
    timestamp = time.time() if now is None else now
    agents = [audit_agent(agent, root=root, now=timestamp) for agent in registry["agents"]]
    counts: dict[str, int] = {"current": 0, "stale": 0, "missing": 0}
    for agent in agents:
        counts[agent.status] = counts.get(agent.status, 0) + 1
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp)),
        "schema_version": registry.get("schema_version", 1),
        "counts": counts,
        "agents": [asdict(agent) for agent in agents],
    }


def markdown_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Hermes Agent Fleet Audit",
        "",
        f"Generated: {payload['generated_at']}",
        "",
        "This audit is read-only. It checks whether registered agent prompts, runbooks, tests, and service units exist and have been maintained recently.",
        "",
        "## Summary",
        "",
    ]
    for status, count in sorted((payload.get("counts") or {}).items()):
        lines.append(f"- {status}: {count}")
    lines.extend(["", "## Agents", ""])
    for agent in payload.get("agents", []):
        lines.append(f"### {agent['agent_id']} - {agent['status']}")
        lines.append(f"- latest age days: {agent.get('latest_age_days')}")
        lines.append(f"- max age days: {agent.get('max_age_days')}")
        if agent.get("missing_paths"):
            lines.append(f"- missing paths: {', '.join(agent['missing_paths'])}")
        lines.append(f"- action: {agent['recommended_action']}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_outputs(payload: dict[str, Any], json_path: Path, md_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(markdown_report(payload), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit the registered Hermes agent fleet.")
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--fail-on-stale", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    registry = load_registry(args.registry)
    payload = build_audit_payload(registry, root=args.root.resolve())
    write_outputs(payload, args.output_json, args.output_md)
    print(json.dumps({"counts": payload["counts"], "output": str(args.output_json)}, sort_keys=True))
    if args.fail_on_stale and (payload["counts"].get("stale", 0) or payload["counts"].get("missing", 0)):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
