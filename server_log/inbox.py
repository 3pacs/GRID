"""Operator inbox — two-way communication via git when no one is at the console.

The operator writes commands to ``.server-logs/inbox.jsonl`` and pushes.
The server periodically pulls and processes them, writing acknowledgements
to ``.server-logs/outbox.jsonl``.

Supported commands
------------------
- ``{"cmd": "set_log_level", "level": "DEBUG"}``
- ``{"cmd": "status"}`` — server writes current health to outbox
- ``{"cmd": "restart_scheduler"}``
- ``{"cmd": "pull_config"}`` — re-read .env without full restart
- ``{"cmd": "ping"}`` — server responds with pong + timestamp
- ``{"cmd": "run_ingestion", "source": "FRED"}``

Each processed command is acknowledged in outbox with ``{"ack": cmd_id, ...}``.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time as _time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from loguru import logger as log

from server_log.sanitizer import Sanitizer, build_sanitizer_from_settings

_LOGS_DIR_NAME = ".server-logs"
_INBOX_FILE = "inbox.jsonl"
_OUTBOX_FILE = "outbox.jsonl"
_DEFAULT_POLL_INTERVAL = 300  # 5 minutes


def _git(args: list[str], cwd: Path) -> tuple[int, str]:
    """Run a git command and return (returncode, output)."""
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode, (result.stdout + result.stderr).strip()
    except Exception as exc:
        return 1, str(exc)


class Inbox:
    """Poll git for operator commands and execute them.

    Parameters
    ----------
    repo_root:
        Git repo root path.
    poll_interval:
        Seconds between git pull checks.
    handlers:
        Dict mapping command names to handler callables.
        Each handler receives the full command dict and returns a response dict.
    """

    def __init__(
        self,
        repo_root: Path,
        poll_interval: int = _DEFAULT_POLL_INTERVAL,
        handlers: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] | None = None,
    ) -> None:
        self._repo = repo_root
        self._logs_dir = repo_root / _LOGS_DIR_NAME
        self._logs_dir.mkdir(exist_ok=True)
        self._inbox_path = self._logs_dir / _INBOX_FILE
        self._outbox_path = self._logs_dir / _OUTBOX_FILE
        self._poll_interval = poll_interval
        self._handlers = handlers or {}
        self._processed_lines: int = self._count_lines(self._inbox_path)
        self._sanitizer = build_sanitizer_from_settings()
        self._timer: threading.Timer | None = None
        self._stopped = False

        # Register built-in handlers
        self._handlers.setdefault("ping", self._handle_ping)
        self._handlers.setdefault("status", self._handle_status)
        self._handlers.setdefault("set_log_level", self._handle_set_log_level)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Begin polling for inbox commands."""
        self._stopped = False
        self._schedule_poll()

    def stop(self) -> None:
        """Stop polling."""
        self._stopped = True
        if self._timer:
            self._timer.cancel()
            self._timer = None

    def _schedule_poll(self) -> None:
        if self._stopped:
            return
        self._timer = threading.Timer(self._poll_interval, self._poll_cycle)
        self._timer.daemon = True
        self._timer.start()

    def _poll_cycle(self) -> None:
        try:
            self._pull_and_process()
        except Exception as exc:
            log.warning("Inbox poll failed: {err}", err=str(exc))
        finally:
            self._schedule_poll()

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    def _pull_and_process(self) -> None:
        """Pull latest from remote, read new inbox lines, execute commands."""
        # Pull only the .server-logs directory (sparse if possible, else full)
        branch = self._detect_branch()
        if branch:
            _git(["pull", "--rebase", "origin", branch], self._repo)

        if not self._inbox_path.exists():
            return

        # Read all lines, process only new ones
        with open(self._inbox_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        new_lines = lines[self._processed_lines:]
        if not new_lines:
            return

        responses: list[dict[str, Any]] = []
        for line in new_lines:
            line = line.strip()
            if not line:
                continue
            try:
                cmd = json.loads(line)
            except json.JSONDecodeError:
                responses.append({"error": "invalid JSON", "raw": line[:200]})
                continue

            response = self._dispatch(cmd)
            responses.append(response)

        self._processed_lines = len(lines)

        # Write responses to outbox
        if responses:
            self._write_outbox(responses)
            self._commit_and_push_outbox()

    def _dispatch(self, cmd: dict[str, Any]) -> dict[str, Any]:
        """Route a command to its handler."""
        cmd_name = cmd.get("cmd", "")
        handler = self._handlers.get(cmd_name)

        ts = datetime.now(timezone.utc).isoformat()
        if not handler:
            return {"ack": cmd_name, "ts": ts, "error": f"unknown command: {cmd_name}"}

        try:
            result = handler(cmd)
            result["ack"] = cmd_name
            result["ts"] = ts
            return result
        except Exception as exc:
            return {"ack": cmd_name, "ts": ts, "error": str(exc)}

    def _write_outbox(self, responses: list[dict[str, Any]]) -> None:
        """Append sanitized responses to the outbox file."""
        with open(self._outbox_path, "a", encoding="utf-8") as f:
            for resp in responses:
                sanitized = self._sanitizer.scrub(json.dumps(resp, default=str))
                f.write(sanitized + "\n")

    def _commit_and_push_outbox(self) -> None:
        """Commit and push outbox responses."""
        _git(["add", str(self._outbox_path)], self._repo)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        msg = f"server-log: outbox response at {ts}"
        rc, _ = _git(["commit", "-m", msg, "--", str(self._outbox_path)], self._repo)
        if rc != 0:
            return

        branch = self._detect_branch()
        if branch:
            _git(["push", "origin", branch], self._repo)

    # ------------------------------------------------------------------
    # Built-in command handlers
    # ------------------------------------------------------------------

    @staticmethod
    def _handle_ping(_cmd: dict[str, Any]) -> dict[str, Any]:
        return {"response": "pong", "server_time": datetime.now(timezone.utc).isoformat()}

    @staticmethod
    def _handle_status(_cmd: dict[str, Any]) -> dict[str, Any]:
        """Return basic server health without exposing secrets."""
        import platform

        try:
            from config import settings
            env = settings.ENVIRONMENT
            db_host = f"{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        except Exception:
            env = "unknown"
            db_host = "unknown"

        return {
            "response": "status",
            "environment": env,
            "db_host": db_host,
            "hostname": platform.node(),
            "python": platform.python_version(),
            "pid": os.getpid(),
        }

    @staticmethod
    def _handle_set_log_level(cmd: dict[str, Any]) -> dict[str, Any]:
        """Change the runtime log level."""
        level = cmd.get("level", "").upper()
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if level not in valid:
            return {"error": f"invalid level: {level}, must be one of {valid}"}

        from config import log as app_log
        # Re-add stderr handler with new level
        app_log.remove()
        import sys
        app_log.add(sys.stderr, level=level)
        return {"response": f"log level changed to {level}"}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _count_lines(path: Path) -> int:
        """Count existing lines in a file (0 if missing)."""
        if not path.exists():
            return 0
        with open(path, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)

    def _detect_branch(self) -> str | None:
        rc, out = _git(["rev-parse", "--abbrev-ref", "HEAD"], self._repo)
        return out if rc == 0 else None
