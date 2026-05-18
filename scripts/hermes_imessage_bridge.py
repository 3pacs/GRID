#!/usr/bin/env python3
"""iMessage control bridge for GRID Hermes.

This is deliberately a command router, not a shell bridge. Incoming iMessages
are accepted only from configured sender ids, parsed as a small slash-command
language, logged, and written to an Obsidian-backed Hermes queue.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_MESSAGES_DB = Path.home() / "Library/Messages/chat.db"
DEFAULT_QUEUE_PATH = Path.home() / "dev/obsidian-vault/Inbox/Hermes-Command-Queue.jsonl"
DEFAULT_AUDIT_PATH = Path.home() / "dev/obsidian-vault/00-Agent-Reports/hermes-imessage-bridge.jsonl"
DEFAULT_PENDING_PATH = Path.home() / ".grid/hermes-imessage-pending.json"
DEFAULT_STATE_PATH = Path.home() / ".grid/hermes-imessage-state.json"
RISKY_COMMANDS = {"restart"}
SAFE_QUEUE_COMMANDS = {"ask", "todo", "render", "fleet"}
DIRECT_COMMANDS = {"help", "status", "queue"}
ALLOWED_RESTART_SERVICES = {
    "grid-api",
    "grid-hermes",
    "grid-llamacpp",
    "storymill",
    "storymill-comfyui",
}


@dataclass(frozen=True)
class IMessage:
    rowid: int
    text: str
    sender: str
    chat_identifier: str | None = None
    room_name: str | None = None
    date_raw: int | None = None

    @property
    def direct_chat(self) -> bool:
        return not self.room_name and not (self.chat_identifier or "").startswith("chat")


@dataclass(frozen=True)
class CommandResult:
    accepted: bool
    reply: str | None
    reason: str
    queued_id: str | None = None
    approval_token: str | None = None
    command: str | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_sender(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if "@" in raw:
        return raw
    digits = re.sub(r"\D+", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits or raw


def sender_allowed(sender: str, allowed_senders: set[str]) -> bool:
    normalized = normalize_sender(sender)
    if normalized in allowed_senders:
        return True
    if normalized.isdigit():
        return any(item.isdigit() and normalized.endswith(item[-10:]) for item in allowed_senders)
    return False


def parse_sender_allowlist(values: list[str] | None = None) -> set[str]:
    senders: list[str] = []
    env_value = os.environ.get("HERMES_IMESSAGE_ALLOWED_SENDERS", "")
    if env_value:
        senders.extend(part.strip() for part in env_value.split(","))
    if values:
        senders.extend(values)
    return {normalize_sender(sender) for sender in senders if normalize_sender(sender)}


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def queue_length(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def tail_queue(path: Path, limit: int = 5) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines()[-limit:]:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


class HermesIMessageRouter:
    def __init__(
        self,
        *,
        allowed_senders: set[str],
        queue_path: Path = DEFAULT_QUEUE_PATH,
        audit_path: Path = DEFAULT_AUDIT_PATH,
        pending_path: Path = DEFAULT_PENDING_PATH,
        direct_only: bool = True,
        approval_ttl_seconds: int = 900,
    ) -> None:
        self.allowed_senders = allowed_senders
        self.queue_path = queue_path
        self.audit_path = audit_path
        self.pending_path = pending_path
        self.direct_only = direct_only
        self.approval_ttl_seconds = approval_ttl_seconds

    def handle_message(self, message: IMessage) -> CommandResult:
        if not sender_allowed(message.sender, self.allowed_senders):
            return self._audit(message, CommandResult(False, None, "sender_not_allowed"))
        if self.direct_only and not message.direct_chat:
            return self._audit(message, CommandResult(False, "Hermes ignores group chats.", "group_chat_rejected"))

        text = message.text.strip()
        if not text:
            return self._audit(message, CommandResult(False, None, "empty_message"))
        if text.lower().startswith("approve ") or text.lower().startswith("/approve "):
            return self._handle_approval(message, text.split(maxsplit=1)[1].strip())
        if not text.startswith("/"):
            return self._audit(
                message,
                CommandResult(False, "Hermes listens for slash commands. Text /help.", "not_a_command"),
            )

        parts = text[1:].split(maxsplit=1)
        command = parts[0].lower()
        args = parts[1].strip() if len(parts) > 1 else ""
        if command in DIRECT_COMMANDS:
            result = self._handle_direct(command)
        elif command in SAFE_QUEUE_COMMANDS:
            result = self._queue_command(message, command, args, approved=True)
        elif command in RISKY_COMMANDS:
            result = self._request_approval(message, command, args)
        else:
            result = CommandResult(False, f"Unknown Hermes command: /{command}. Text /help.", "unknown_command", command=command)
        return self._audit(message, result)

    def _handle_direct(self, command: str) -> CommandResult:
        if command == "help":
            reply = (
                "Hermes commands: /status, /queue, /ask <note>, /todo <task>, "
                "/fleet [scope], /render <job>, /restart <service>."
            )
            return CommandResult(True, reply, "help", command=command)
        if command == "status":
            reply = f"Hermes iMessage bridge online. Queue depth: {queue_length(self.queue_path)}."
            return CommandResult(True, reply, "status", command=command)
        recent = tail_queue(self.queue_path, limit=3)
        if not recent:
            return CommandResult(True, "Hermes queue is empty.", "queue_empty", command=command)
        lines = [f"{item.get('id', 'unknown')}: /{item.get('command')} {item.get('args', '')}".strip() for item in recent]
        return CommandResult(True, "Recent Hermes queue:\n" + "\n".join(lines), "queue_tail", command=command)

    def _queue_command(self, message: IMessage, command: str, args: str, *, approved: bool) -> CommandResult:
        if command in {"ask", "todo", "render"} and not args:
            return CommandResult(False, f"/{command} needs text after it.", "missing_args", command=command)
        item_id = f"imsg-{int(time.time())}-{random.randint(1000, 9999)}"
        record = {
            "id": item_id,
            "created_at": utc_now(),
            "source": "imessage",
            "sender": normalize_sender(message.sender),
            "command": command,
            "args": args,
            "approved": approved,
            "status": "pending",
            "message_rowid": message.rowid,
        }
        append_jsonl(self.queue_path, record)
        approval_note = "approved " if approved and command in RISKY_COMMANDS else ""
        return CommandResult(
            True,
            f"Queued {approval_note}Hermes /{command}: {item_id}",
            "queued",
            queued_id=item_id,
            command=command,
        )

    def _request_approval(self, message: IMessage, command: str, args: str) -> CommandResult:
        if command == "restart" and args not in ALLOWED_RESTART_SERVICES:
            services = ", ".join(sorted(ALLOWED_RESTART_SERVICES))
            return CommandResult(False, f"Restart target must be one of: {services}", "restart_target_rejected", command=command)
        token = f"{random.randint(100000, 999999)}"
        pending = load_json(self.pending_path, {})
        pending[token] = {
            "created_at": time.time(),
            "sender": normalize_sender(message.sender),
            "command": command,
            "args": args,
            "message_rowid": message.rowid,
        }
        write_json(self.pending_path, pending)
        return CommandResult(
            True,
            f"Confirm Hermes /{command} {args} with: approve {token}",
            "approval_required",
            approval_token=token,
            command=command,
        )

    def _handle_approval(self, message: IMessage, token: str) -> CommandResult:
        pending = load_json(self.pending_path, {})
        approval = pending.get(token)
        if not approval:
            return self._audit(message, CommandResult(False, "No pending Hermes approval for that token.", "approval_missing"))
        if approval.get("sender") != normalize_sender(message.sender):
            return self._audit(message, CommandResult(False, "Approval sender mismatch.", "approval_sender_mismatch"))
        if time.time() - float(approval.get("created_at", 0)) > self.approval_ttl_seconds:
            pending.pop(token, None)
            write_json(self.pending_path, pending)
            return self._audit(message, CommandResult(False, "Approval token expired.", "approval_expired"))
        pending.pop(token, None)
        write_json(self.pending_path, pending)
        result = self._queue_command(message, approval["command"], approval.get("args", ""), approved=True)
        return CommandResult(result.accepted, result.reply, "approval_queued", result.queued_id, token, approval["command"])

    def _audit(self, message: IMessage, result: CommandResult) -> CommandResult:
        append_jsonl(
            self.audit_path,
            {
                "created_at": utc_now(),
                "message_rowid": message.rowid,
                "sender": normalize_sender(message.sender),
                "chat_identifier": message.chat_identifier,
                "direct_chat": message.direct_chat,
                "accepted": result.accepted,
                "reason": result.reason,
                "command": result.command,
                "queued_id": result.queued_id,
                "approval_token": result.approval_token,
            },
        )
        return result


def read_messages(db_path: Path, after_rowid: int) -> list[IMessage]:
    query = """
        SELECT
          message.ROWID,
          message.date,
          message.text,
          handle.id AS sender,
          chat.chat_identifier,
          chat.room_name
        FROM message
        LEFT JOIN handle ON message.handle_id = handle.ROWID
        LEFT JOIN chat_message_join cmj ON cmj.message_id = message.ROWID
        LEFT JOIN chat ON chat.ROWID = cmj.chat_id
        WHERE message.ROWID > ?
          AND message.is_from_me = 0
          AND message.text IS NOT NULL
        ORDER BY message.ROWID ASC
    """
    uri = f"file:{db_path}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        rows = conn.execute(query, (after_rowid,)).fetchall()
    return [
        IMessage(
            rowid=int(row[0]),
            date_raw=row[1],
            text=str(row[2] or ""),
            sender=str(row[3] or ""),
            chat_identifier=row[4],
            room_name=row[5],
        )
        for row in rows
    ]


def send_imessage_reply(recipient: str, text: str) -> None:
    script = """
    on run argv
      set targetBuddyId to item 1 of argv
      set replyText to item 2 of argv
      tell application "Messages"
        set targetService to 1st service whose service type = iMessage
        set targetBuddy to buddy targetBuddyId of targetService
        send replyText to targetBuddy
      end tell
    end run
    """
    subprocess.run(["osascript", "-e", script, recipient, text], check=True)


def run_once(args: argparse.Namespace) -> int:
    allowed = parse_sender_allowlist(args.allow_sender)
    if not allowed:
        raise SystemExit("Set HERMES_IMESSAGE_ALLOWED_SENDERS or pass --allow-sender before polling Messages.")
    state = load_json(args.state_path, {"last_rowid": int(args.after_rowid or 0)})
    after_rowid = int(args.after_rowid if args.after_rowid is not None else state.get("last_rowid", 0))
    router = HermesIMessageRouter(
        allowed_senders=allowed,
        queue_path=args.queue_path,
        audit_path=args.audit_path,
        pending_path=args.pending_path,
        direct_only=not args.allow_group_chats,
    )
    messages = read_messages(args.messages_db, after_rowid)
    max_rowid = after_rowid
    for message in messages:
        max_rowid = max(max_rowid, message.rowid)
        result = router.handle_message(message)
        if result.reply:
            if args.send_replies:
                send_imessage_reply(message.sender, result.reply)
            if args.print_replies:
                print(f"{message.sender}: {result.reply}")
    write_json(args.state_path, {"last_rowid": max_rowid, "updated_at": utc_now()})
    return len(messages)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Route allowlisted iMessages into the GRID Hermes command queue.")
    parser.add_argument("--messages-db", type=Path, default=DEFAULT_MESSAGES_DB)
    parser.add_argument("--queue-path", type=Path, default=DEFAULT_QUEUE_PATH)
    parser.add_argument("--audit-path", type=Path, default=DEFAULT_AUDIT_PATH)
    parser.add_argument("--pending-path", type=Path, default=DEFAULT_PENDING_PATH)
    parser.add_argument("--state-path", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--allow-sender", action="append", default=[])
    parser.add_argument("--allow-group-chats", action="store_true")
    parser.add_argument("--after-rowid", type=int)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=15.0)
    parser.add_argument("--send-replies", action="store_true")
    parser.add_argument("--print-replies", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.once:
        run_once(args)
        return 0
    while True:
        run_once(args)
        time.sleep(args.poll_interval)


if __name__ == "__main__":
    raise SystemExit(main())
