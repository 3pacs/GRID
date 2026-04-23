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
import time as _time
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
# Dedup: collapse identical errors seen within this many seconds into a single
# emitted line plus a coalesced "(repeated N times)" summary at flush time.
# A single bug used to write 30+ identical lines per pull cycle.
_DEFAULT_DEDUP_WINDOW_SECONDS = 60
# Rotation: roll over errors.jsonl when it exceeds this size to prevent the
# git-tracked file from growing unbounded.
_DEFAULT_ROTATE_SIZE_MB = 50.0
_DEFAULT_ARCHIVE_RETENTION_DAYS = 90


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
        dedup_window_seconds: int = _DEFAULT_DEDUP_WINDOW_SECONDS,
        rotate_size_mb: float = _DEFAULT_ROTATE_SIZE_MB,
        archive_retention_days: int = _DEFAULT_ARCHIVE_RETENTION_DAYS,
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

        # Dedup state: signature -> {"first_ts", "count", "sample"}.
        # Suppressed repeats are emitted as a single coalesced summary line
        # once the window expires.
        self._dedup_window_s = max(0, int(dedup_window_seconds))
        self._dedup_state: dict[tuple[str, str, str], dict] = {}
        self._rotate_size_mb = rotate_size_mb
        self._archive_retention_days = archive_retention_days

        # Ensure .server-logs is tracked (create .gitkeep if empty)
        gitkeep = self._logs_dir / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()

    # ------------------------------------------------------------------
    # Loguru sink interface
    # ------------------------------------------------------------------

    def write(self, message: Message) -> None:
        """Called by loguru for each log record at the configured level.

        Identical errors (same module + function + message text) seen within
        ``_dedup_window_s`` are collapsed: the first occurrence is written
        immediately; subsequent duplicates increment a counter. The counter
        is flushed as a single ``(repeated N times)`` summary line when the
        window expires (during the next push cycle).
        """
        record = message.record
        entry = self._format_entry(record)

        with self._buffer_lock:
            sig = self._signature(entry)
            now = _time.monotonic()
            state = self._dedup_state.get(sig) if self._dedup_window_s > 0 else None

            if state is None or (now - state["first_ts"]) > self._dedup_window_s:
                # New or expired window — flush prior summary and emit fresh.
                if state is not None and state["count"] > 1:
                    self._append_summary(state)
                self._append_entry(entry)
                self._dedup_state[sig] = {
                    "first_ts": now,
                    "count": 1,
                    "sample": entry,
                }
                self._pending_count += 1
            else:
                # Within active window — suppress, just bump the counter.
                state["count"] += 1
                state["sample"] = entry  # keep latest copy in case it differs

    def _signature(self, entry: dict) -> tuple[str, str, str]:
        """Stable signature for dedup. Truncate message to bound cardinality."""
        return (
            str(entry.get("module", "")),
            str(entry.get("function", "")),
            str(entry.get("message", ""))[:200],
        )

    def _append_entry(self, entry: dict) -> None:
        """Sanitize and append a JSONL line. Caller must hold _buffer_lock."""
        sanitized = self._sanitizer.scrub(json.dumps(entry, default=str))
        with open(self._errors_path, "a", encoding="utf-8") as f:
            f.write(sanitized + "\n")

    def _append_summary(self, state: dict) -> None:
        """Write a coalesced summary for ``state["count"] - 1`` suppressed
        repeats. Caller must hold _buffer_lock."""
        sample = state["sample"]
        suppressed = max(0, state["count"] - 1)
        if suppressed == 0:
            return
        summary = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": sample.get("level", "ERROR"),
            "module": sample.get("module", ""),
            "function": sample.get("function", ""),
            "line": sample.get("line", 0),
            "message": (
                f"(repeated {suppressed}x in {self._dedup_window_s}s) "
                f"{sample.get('message', '')}"
            ),
            "exception": None,
            "dedup_count": suppressed,
        }
        self._append_entry(summary)
        self._pending_count += 1

    def _flush_expired_dedup(self) -> None:
        """Emit summaries for any signatures whose window has elapsed."""
        if self._dedup_window_s <= 0:
            return
        now = _time.monotonic()
        with self._buffer_lock:
            expired = [
                sig for sig, st in self._dedup_state.items()
                if (now - st["first_ts"]) > self._dedup_window_s
            ]
            for sig in expired:
                state = self._dedup_state.pop(sig)
                if state["count"] > 1:
                    self._append_summary(state)

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
            _fallback_log.error("[server_log] git push failed: {e}", e=exc)
        finally:
            self._schedule_push()

    def _commit_and_push(self) -> None:
        """Commit pending log entries and push to remote.

        Before staging, we flush any dedup summaries whose window has expired,
        rotate ``errors.jsonl`` if it exceeds the configured size, and prune
        old archives. This keeps the file bounded without operator action.
        """
        # Flush coalesced summaries first so they land in this commit.
        self._flush_expired_dedup()

        # Bound file size: rotate then prune. Best-effort — failures don't
        # block the commit.
        try:
            self.rotate_if_needed(max_size_mb=self._rotate_size_mb)
        except Exception as exc:
            _fallback_log.warning("[server_log] rotate failed: {e}", e=exc)
        try:
            self.cleanup_old_archives(max_age_days=self._archive_retention_days)
        except Exception as exc:
            _fallback_log.warning("[server_log] cleanup failed: {e}", e=exc)

        with self._buffer_lock:
            if self._pending_count == 0:
                return
            count = self._pending_count
            self._pending_count = 0

        # Stage the errors file
        rc, out = _git(["add", str(self._errors_path)], self._repo)
        if rc != 0:
            _fallback_log.error("[server_log] git add failed: {out}", out=out)
            return

        # Commit
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        msg = f"server-log: {count} error(s) at {ts}"
        rc, out = _git(["commit", "-m", msg, "--", str(self._errors_path)], self._repo)
        if rc != 0:
            # Nothing to commit (maybe file unchanged)
            if "nothing to commit" in out.lower():
                return
            _fallback_log.error("[server_log] git commit failed: {out}", out=out)
            return

        # Push — only if explicitly enabled (default off to prevent
        # accidental exfiltration of error context to remote git)
        import os
        if not os.getenv("GIT_SINK_PUSH_ENABLED", "").lower() in ("1", "true", "yes"):
            return

        branch = self._branch or self._detect_branch()
        if not branch:
            _fallback_log.warning("[server_log] could not detect git branch; skipping push")
            return

        delays = [2, 4, 8, 16]
        for attempt, delay in enumerate(delays):
            rc, out = _git(["push", "origin", branch], self._repo)
            if rc == 0:
                return
            if attempt < len(delays) - 1:
                import time
                time.sleep(delay)

        _fallback_log.error("[server_log] git push failed after retries: {out}", out=out)

    def _detect_branch(self) -> str | None:
        """Return the current git branch name."""
        rc, out = _git(["rev-parse", "--abbrev-ref", "HEAD"], self._repo)
        return out if rc == 0 else None

    # ------------------------------------------------------------------
    # Manual flush (for critical errors)
    # ------------------------------------------------------------------

    def flush_now(self) -> None:
        """Force an immediate commit+push outside the timer cycle.

        Also drains any in-window dedup state so the operator gets a complete
        picture (count > 1 windows that haven't expired yet are emitted now).
        """
        with self._buffer_lock:
            for sig in list(self._dedup_state.keys()):
                state = self._dedup_state.pop(sig)
                if state["count"] > 1:
                    self._append_summary(state)
        self._commit_and_push()

    def rotate_if_needed(self, max_size_mb: float = 50.0) -> bool:
        """Rotate errors.jsonl if it exceeds max_size_mb.

        Renames the current file with a timestamp suffix and starts a
        fresh file.  Returns True if rotation occurred.
        """
        if not self._errors_path.exists():
            return False

        size_mb = self._errors_path.stat().st_size / (1024 * 1024)
        if size_mb < max_size_mb:
            return False

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        archive_name = f"errors_{ts}.jsonl"
        archive_path = self._logs_dir / archive_name

        with self._buffer_lock:
            self._errors_path.rename(archive_path)

        _fallback_log.info(
            "[server_log] Rotated errors.jsonl ({sz:.1f}MB) -> {a}",
            sz=size_mb, a=archive_name,
        )
        return True

    def cleanup_old_archives(self, max_age_days: int = 90) -> int:
        """Delete archived error files older than max_age_days."""
        from datetime import timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        deleted = 0
        for f in self._logs_dir.glob("errors_*.jsonl"):
            parts = f.stem.replace("errors_", "")
            try:
                file_ts = datetime.strptime(parts, "%Y%m%d_%H%M%S").replace(
                    tzinfo=timezone.utc
                )
                if file_ts < cutoff:
                    f.unlink()
                    deleted += 1
            except ValueError:
                continue
        if deleted:
            _fallback_log.info(
                "[server_log] Cleaned up {n} old error archives", n=deleted,
            )
        return deleted
