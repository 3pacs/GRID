"""Loguru sink that writes sanitized errors to a git-tracked JSONL file.

Flow:
  1. ERROR+ log messages are intercepted by ``GitSink.write()``.
  2. The sanitizer scrubs secrets from the message text.
  3. Entries are appended to ``.server-logs/errors.jsonl``.
  4. A background thread commits and pushes on a configurable interval
     (default: 5 minutes), batching all buffered entries into one commit.

The push runs in a daemon thread so it never blocks the main application.
If git operations fail (no remote, auth issues), failures are logged to
stderr and the system continues — this is best-effort telemetry.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger as _fallback_log

from server_log.sanitizer import Sanitizer, build_sanitizer_from_settings

if TYPE_CHECKING:
    from loguru import Message

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
_DEFAULT_PUSH_INTERVAL_SECONDS = 300  # 5 minutes
_LOGS_DIR_NAME = ".server-logs"
_ERRORS_FILE = "errors.jsonl"


def _repo_root() -> Path:
    """Find the git repository root above the grid/ package."""
    here = Path(__file__).resolve()
    # Walk up until we find .git
    for parent in here.parents:
        if (parent / ".git").exists():
            return parent
    # Fallback: two levels up from grid/server_log/
    return here.parent.parent.parent


def _git(args: list[str], cwd: Path) -> tuple[int, str]:
    """Run a git command and return (returncode, combined output)."""
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


class GitSink:
    """Loguru sink that appends sanitized error entries to a JSONL file
    and periodically commits + pushes to git.

    Parameters
    ----------
    repo_root:
        Path to the git repository root.  Defaults to auto-detected.
    push_interval:
        Seconds between git commit+push cycles.
    sanitizer:
        Pre-built Sanitizer instance.  If None, one is built from settings.
    branch:
        Git branch to push to.  Defaults to the current branch.
    """

    def __init__(
        self,
        repo_root: Path | None = None,
        push_interval: int = _DEFAULT_PUSH_INTERVAL_SECONDS,
        sanitizer: Sanitizer | None = None,
        branch: str | None = None,
    ) -> None:
        self._repo = repo_root or _repo_root()
        self._logs_dir = self._repo / _LOGS_DIR_NAME
        self._logs_dir.mkdir(exist_ok=True)
        self._errors_path = self._logs_dir / _ERRORS_FILE
        self._sanitizer = sanitizer or build_sanitizer_from_settings()
        self._push_interval = push_interval
        self._branch = branch
        self._buffer_lock = threading.Lock()
        self._pending_count = 0
        self._timer: threading.Timer | None = None
        self._stopped = False

        # Ensure .server-logs is tracked (create .gitkeep if empty)
        gitkeep = self._logs_dir / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()

    # ------------------------------------------------------------------
    # Loguru sink interface
    # ------------------------------------------------------------------

    def write(self, message: Message) -> None:
        """Called by loguru for each log record at the configured level."""
        record = message.record
        entry = self._format_entry(record)
        sanitized = self._sanitizer.scrub(json.dumps(entry, default=str))

        with self._buffer_lock:
            with open(self._errors_path, "a", encoding="utf-8") as f:
                f.write(sanitized + "\n")
            self._pending_count += 1

    def _format_entry(self, record: dict) -> dict:
        """Build a structured log entry from a loguru record."""
        exc_text = ""
        if record.get("exception"):
            exc_info = record["exception"]
            if exc_info.type and exc_info.value:
                exc_text = self._sanitizer.scrub(
                    "".join(traceback.format_exception(
                        exc_info.type, exc_info.value, exc_info.traceback
                    ))
                )

        return {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record["level"].name,
            "module": record.get("name", ""),
            "function": record.get("function", ""),
            "line": record.get("line", 0),
            "message": str(record["message"]),
            "exception": exc_text or None,
        }

    # ------------------------------------------------------------------
    # Git push cycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Begin the periodic commit+push timer."""
        self._stopped = False
        self._schedule_push()

    def stop(self) -> None:
        """Cancel the push timer (call on shutdown)."""
        self._stopped = True
        if self._timer:
            self._timer.cancel()
            self._timer = None
        # Final flush
        self._commit_and_push()

    def _schedule_push(self) -> None:
        if self._stopped:
            return
        self._timer = threading.Timer(self._push_interval, self._push_cycle)
        self._timer.daemon = True
        self._timer.start()

    def _push_cycle(self) -> None:
        """Run one commit+push, then reschedule."""
        try:
            self._commit_and_push()
        except Exception as exc:
            # Never let push failures crash the timer
            print(f"[server_log] git push failed: {exc}", flush=True)
        finally:
            self._schedule_push()

    def _commit_and_push(self) -> None:
        """Commit pending log entries and push to remote."""
        with self._buffer_lock:
            if self._pending_count == 0:
                return
            count = self._pending_count
            self._pending_count = 0

        # Stage the errors file
        rc, out = _git(["add", str(self._errors_path)], self._repo)
        if rc != 0:
            print(f"[server_log] git add failed: {out}", flush=True)
            return

        # Commit
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        msg = f"server-log: {count} error(s) at {ts}"
        rc, out = _git(["commit", "-m", msg, "--", str(self._errors_path)], self._repo)
        if rc != 0:
            # Nothing to commit (maybe file unchanged)
            if "nothing to commit" in out.lower():
                return
            print(f"[server_log] git commit failed: {out}", flush=True)
            return

        # Push — only if explicitly enabled (default off to prevent
        # accidental exfiltration of error context to remote git)
        import os
        if not os.getenv("GIT_SINK_PUSH_ENABLED", "").lower() in ("1", "true", "yes"):
            return

        branch = self._branch or self._detect_branch()
        if not branch:
            print("[server_log] could not detect git branch; skipping push", flush=True)
            return

        delays = [2, 4, 8, 16]
        for attempt, delay in enumerate(delays):
            rc, out = _git(["push", "origin", branch], self._repo)
            if rc == 0:
                return
            if attempt < len(delays) - 1:
                import time
                time.sleep(delay)

        print(f"[server_log] git push failed after retries: {out}", flush=True)

    def _detect_branch(self) -> str | None:
        """Return the current git branch name."""
        rc, out = _git(["rev-parse", "--abbrev-ref", "HEAD"], self._repo)
        return out if rc == 0 else None

    # ------------------------------------------------------------------
    # Manual flush (for critical errors)
    # ------------------------------------------------------------------

    def flush_now(self) -> None:
        """Force an immediate commit+push outside the timer cycle."""
        self._commit_and_push()
