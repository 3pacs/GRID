#!/usr/bin/env python3
"""Build a privacy-scrubbed SFT dataset for GRID Hermes.

This is a data-prep step, not a trainer. It converts the Hermes command queue
and durable agent reports into small chat examples that teach Hermes how to
triage fleet/operator work safely. Large outputs should live off the Mac SSD.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Iterable


DEFAULT_QUEUE_PATH = Path.home() / "dev/obsidian-vault/Inbox/Hermes-Command-Queue.jsonl"
DEFAULT_REPORTS_DIR = Path.home() / "dev/obsidian-vault/00-Agent-Reports"
DEFAULT_OUTPUT_DIR = (
    Path("/data/agent-home/anikdang/hermes_finetune")
    if Path("/data/agent-home/anikdang").exists()
    else Path("output/hermes_finetune")
)
DEFAULT_OUTPUT_PATH = DEFAULT_OUTPUT_DIR / "hermes_sft.jsonl"

HERMES_SYSTEM_PROMPT = (
    "You are GRID Hermes, the operator for Anik's local research, render, and "
    "agent fleet. Be concise, evidence-first, and safety-bound. Prefer "
    "read-only inspection, durable Obsidian/GitHub-backed records, explicit "
    "approval for risky mutations, and concrete next actions."
)

SECRET_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b(sk-[A-Za-z0-9_-]{12,})\b"), "sk-[REDACTED]"),
    (re.compile(r"\b(hf_[A-Za-z0-9]{12,})\b"), "hf_[REDACTED]"),
    (re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{12,}"), r"\1 [REDACTED]"),
    (
        re.compile(
            r"(?i)\b([A-Z0-9_]*(?:TOKEN|PASSWORD|PASS|SECRET|API_KEY|ACCESS_KEY)[A-Z0-9_]*\s*=\s*)"
            r"(['\"]?)[^\s,'\"]+"
        ),
        r"\1\2[REDACTED]",
    ),
)


def redact_text(value: Any) -> str:
    text = str(value or "")
    for pattern, replacement in SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def chat_example(user: str, assistant: str, *, source: str, tags: list[str]) -> dict[str, Any]:
    return {
        "messages": [
            {"role": "system", "content": HERMES_SYSTEM_PROMPT},
            {"role": "user", "content": redact_text(user)},
            {"role": "assistant", "content": redact_text(assistant)},
        ],
        "source": source,
        "tags": tags,
    }


def seed_examples() -> list[dict[str, Any]]:
    return [
        chat_example(
            "/fleet panda render lane",
            (
                "I will run a read-only fleet audit scoped to panda, compare GPU/service state with "
                "queued render work, and write findings to the durable report path. I will not restart "
                "or rebind services without explicit approval."
            ),
            source="seed:fleet",
            tags=["seed", "fleet", "read_only"],
        ),
        chat_example(
            "/restart grid-hermes",
            (
                "That is a risky command. I need an approval token from the same allowlisted sender "
                "before queueing the restart, and I will only target the allowlisted grid-hermes unit."
            ),
            source="seed:approval",
            tags=["seed", "approval", "restart"],
        ),
        chat_example(
            "Several sessions crashed. What should Hermes do first?",
            (
                "Start from durable state: Obsidian agent reports, the central Agent-TODO file, current "
                "git worktrees, and live service health. Mark completed items off the list before "
                "opening new work, then leave a synced handoff report."
            ),
            source="seed:recovery",
            tags=["seed", "crash_recovery", "obsidian"],
        ),
    ]


def queue_record_to_example(record: dict[str, Any]) -> dict[str, Any] | None:
    command = str(record.get("command") or "").strip().lower()
    if not command:
        return None
    args = redact_text(record.get("args") or "").strip()
    approved = bool(record.get("approved"))
    user = f"Owner iMessage command: /{command} {args}".strip()

    if command == "fleet":
        assistant = (
            "Run a read-only fleet audit for the requested scope, compare host reachability, GPU "
            "utilization, service health, and queue pressure, then write a durable report with "
            "proposed actions. Do not mutate services from the audit pass."
        )
    elif command == "render":
        assistant = (
            "Queue the render request as production work, attach required references/provenance, "
            "write status to Obsidian, and leave heavyweight generated assets on grid-svr or scratch "
            "storage instead of the Mac internal SSD."
        )
    elif command == "todo":
        assistant = (
            "Add the task to the centralized Agent-TODO board, dedupe against completed items, and "
            "include the follow-up in the next durable handoff report."
        )
    elif command == "ask":
        assistant = (
            "Treat this as an operator question. Inspect current repo, report, or fleet state before "
            "answering, distinguish confirmed facts from stale memory, and capture any resulting task."
        )
    elif command == "restart":
        if approved:
            assistant = (
                "Approval is present. Before restarting, verify the service name is allowlisted and "
                "capture current status/log context. Queue only the approved unit action and audit the result."
            )
        else:
            assistant = (
                "Do not restart yet. Ask for explicit same-sender approval and keep the pending token "
                "audited until it expires or is confirmed."
            )
    else:
        assistant = (
            "Reject unknown commands politely, do not improvise shell execution, and offer the supported "
            "Hermes command set."
        )

    tags = ["queue", command]
    if approved:
        tags.append("approved")
    return chat_example(user, assistant, source=f"queue:{record.get('id', 'unknown')}", tags=tags)


def _section(text: str, names: Iterable[str], *, max_chars: int = 900) -> str:
    wanted = {name.lower() for name in names}
    lines = text.splitlines()
    capture = False
    chunks: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("## "):
            heading = stripped.lstrip("#").strip().lower()
            capture = heading in wanted
            continue
        if capture:
            chunks.append(line.rstrip())
    summary = "\n".join(chunks).strip()
    if not summary:
        return "Not recorded."
    return summary[:max_chars]


def report_file_to_example(path: Path) -> dict[str, Any] | None:
    if not path.exists() or path.suffix.lower() != ".md":
        return None
    text = redact_text(path.read_text(encoding="utf-8", errors="replace"))
    title = next((line.strip("# ").strip() for line in text.splitlines() if line.startswith("#")), path.name)
    changed = _section(text, ("what changed", "changes", "summary", "completed"))
    verification = _section(text, ("verification", "tests", "checks"))
    blockers = _section(text, ("unresolved blockers", "blockers", "manual asks", "follow ups", "follow-ups"))
    user = (
        f"Recover from this Hermes/agent report and update the active plan:\n\n"
        f"{text[:3500]}"
    )
    assistant = (
        f"Recovered report: {title}\n"
        f"- Changed: {changed}\n"
        f"- Verification: {verification}\n"
        f"- Blockers/follow-ups: {blockers}\n"
        "Next behavior: preserve completed work, avoid repeating stale tasks, and carry unresolved "
        "manual asks into the central queue."
    )
    return chat_example(user, assistant, source=f"report:{path.name}", tags=["report", "recovery"])


def newest_markdown_files(path: Path, limit: int) -> list[Path]:
    if not path.exists():
        return []
    files = [item for item in path.rglob("*.md") if item.is_file()]
    return sorted(files, key=lambda item: item.stat().st_mtime, reverse=True)[:limit]


def build_examples(
    *,
    queue_path: Path,
    reports_dir: Path,
    max_report_files: int,
    include_seed_examples: bool,
) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    if include_seed_examples:
        examples.extend(seed_examples())
    for record in read_jsonl(queue_path):
        example = queue_record_to_example(record)
        if example:
            examples.append(example)
    for report_path in newest_markdown_files(reports_dir, max_report_files):
        example = report_file_to_example(report_path)
        if example:
            examples.append(example)
    return examples


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build privacy-scrubbed Hermes SFT JSONL.")
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE_PATH)
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--max-report-files", type=int, default=25)
    parser.add_argument("--no-seed-examples", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    examples = build_examples(
        queue_path=args.queue,
        reports_dir=args.reports_dir,
        max_report_files=max(args.max_report_files, 0),
        include_seed_examples=not args.no_seed_examples,
    )
    write_jsonl(args.output, examples)
    print(json.dumps({"output": str(args.output), "examples": len(examples)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
