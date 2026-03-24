"""
Tests for the server_log module — sanitizer, git sink, and inbox.

All tests run without git or network access (mocked).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from server_log.sanitizer import Sanitizer


# ===================================================================
# Sanitizer tests
# ===================================================================


class TestSanitizerExactValues:
    """Exact-match scrubbing of known secret values."""

    def test_scrubs_api_key(self):
        """Known API key value is replaced with [REDACTED]."""
        s = Sanitizer(secret_values=["my-secret-fred-key-12345"])
        result = s.scrub("Failed to fetch FRED with key my-secret-fred-key-12345")
        assert "my-secret-fred-key-12345" not in result
        assert "[REDACTED]" in result

    def test_scrubs_password(self):
        """Database password is redacted."""
        s = Sanitizer(secret_values=["SuperSecretPass"])
        result = s.scrub("Connection to postgresql://user:SuperSecretPass@host failed")
        assert "SuperSecretPass" not in result

    def test_ignores_short_values(self):
        """Values shorter than 4 chars are not registered (avoid false positives)."""
        s = Sanitizer(secret_values=["abc"])
        result = s.scrub("The abc module failed")
        assert "abc" in result  # NOT redacted — too short

    def test_empty_values_ignored(self):
        """Empty strings do not cause issues."""
        s = Sanitizer(secret_values=["", "valid-secret-1234"])
        result = s.scrub("Key is valid-secret-1234")
        assert "valid-secret-1234" not in result

    def test_multiple_secrets(self):
        """All secrets in a single message are scrubbed."""
        s = Sanitizer(secret_values=["secret-one-1234", "secret-two-5678"])
        result = s.scrub("Keys: secret-one-1234 and secret-two-5678")
        assert "secret-one-1234" not in result
        assert "secret-two-5678" not in result


class TestSanitizerPatterns:
    """Pattern-based scrubbing of common secret shapes."""

    def test_jwt_token_scrubbed(self):
        """JWT-shaped strings are replaced."""
        s = Sanitizer()
        jwt = "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyIjoiYWRtaW4ifQ.K7gNU3sdo_OL0wNhqoVWhr3g6s1xYv72ol_pe_Unkn0"
        result = s.scrub(f"Token: {jwt}")
        assert "eyJ" not in result
        assert "REDACTED" in result

    def test_bearer_token_scrubbed(self):
        """Bearer token in header-like text is redacted."""
        s = Sanitizer()
        result = s.scrub("Authorization: Bearer abc123def456ghi789jkl012mno345pqr678")
        assert "abc123def456" not in result
        assert "Bearer" in result
        assert "REDACTED" in result

    def test_pg_connection_string_scrubbed(self):
        """PostgreSQL connection string password is redacted."""
        s = Sanitizer()
        result = s.scrub("Connecting to postgresql://grid_user:MyP4ssw0rd@db.host:5432/grid")
        assert "MyP4ssw0rd" not in result
        assert "REDACTED" in result
        assert "grid_user" in result  # username preserved

    def test_password_equals_pattern(self):
        """password=value pattern is redacted."""
        s = Sanitizer()
        result = s.scrub("Config: password=hunter2 host=localhost")
        assert "hunter2" not in result
        assert "REDACTED" in result

    def test_clean_message_unchanged(self):
        """Messages with no secrets pass through unchanged."""
        s = Sanitizer()
        msg = "Database health check passed in 42ms"
        result = s.scrub(msg)
        assert result == msg


class TestSanitizerFromSettings:
    """Building sanitizer from config.settings."""

    @patch("config.settings")
    def test_build_from_settings(self, mock_settings):
        """build_sanitizer_from_settings picks up all sensitive fields."""
        mock_settings.DB_PASSWORD = "test-db-pass"
        mock_settings.FRED_API_KEY = "test-fred-key-1234"
        mock_settings.KOSIS_API_KEY = ""
        mock_settings.COMTRADE_API_KEY = ""
        mock_settings.JQUANTS_EMAIL = ""
        mock_settings.JQUANTS_PASSWORD = ""
        mock_settings.USDA_NASS_API_KEY = ""
        mock_settings.NOAA_TOKEN = ""
        mock_settings.EIA_API_KEY = ""
        mock_settings.GDELT_API_KEY = ""
        mock_settings.GRID_JWT_SECRET = "jwt-secret-long-value"
        mock_settings.GRID_MASTER_PASSWORD_HASH = "$2b$12$fakehashvalue1234567890"
        mock_settings.AGENTS_OPENAI_API_KEY = ""
        mock_settings.AGENTS_ANTHROPIC_API_KEY = ""

        from server_log.sanitizer import build_sanitizer_from_settings
        sanitizer = build_sanitizer_from_settings()

        result = sanitizer.scrub("DB pass: test-db-pass, FRED: test-fred-key-1234")
        assert "test-db-pass" not in result
        assert "test-fred-key-1234" not in result


# ===================================================================
# GitSink tests
# ===================================================================


class TestGitSinkWrite:
    """GitSink writes sanitized JSONL entries to the errors file."""

    def test_write_creates_entry(self, tmp_path):
        """A log record produces a JSONL line in the errors file."""
        # Set up a minimal git repo structure
        (tmp_path / ".git").mkdir()
        sanitizer = Sanitizer(secret_values=["my-secret-pass"])

        from server_log.git_sink import GitSink
        sink = GitSink(repo_root=tmp_path, sanitizer=sanitizer, push_interval=9999)

        # Simulate a loguru record
        record = {
            "level": MagicMock(name="ERROR"),
            "name": "test_module",
            "function": "test_func",
            "line": 42,
            "message": "Connection failed with password my-secret-pass",
            "exception": None,
        }
        record["level"].name = "ERROR"

        message = MagicMock()
        message.record = record
        message.__str__ = lambda self: record["message"]

        sink.write(message)

        errors_file = tmp_path / ".server-logs" / "errors.jsonl"
        assert errors_file.exists()
        lines = errors_file.read_text().strip().split("\n")
        assert len(lines) == 1

        entry = json.loads(lines[0])
        assert entry["level"] == "ERROR"
        assert entry["module"] == "test_module"
        assert "my-secret-pass" not in lines[0]
        assert "[REDACTED]" in entry["message"]

    def test_pending_count_increments(self, tmp_path):
        """Each write increments the pending count for batched commits."""
        (tmp_path / ".git").mkdir()
        from server_log.git_sink import GitSink
        sink = GitSink(repo_root=tmp_path, sanitizer=Sanitizer(), push_interval=9999)

        record = {
            "level": MagicMock(name="ERROR"),
            "name": "mod",
            "function": "fn",
            "line": 1,
            "message": "error 1",
            "exception": None,
        }
        record["level"].name = "ERROR"
        msg = MagicMock()
        msg.record = record

        sink.write(msg)
        sink.write(msg)
        assert sink._pending_count == 2


class TestGitSinkCommit:
    """GitSink commit+push cycle works correctly."""

    @patch.dict("os.environ", {"GIT_SINK_PUSH_ENABLED": "true"})
    @patch("server_log.git_sink._git")
    def test_commit_and_push(self, mock_git, tmp_path):
        """Commit+push is called with correct args when entries are pending."""
        (tmp_path / ".git").mkdir()
        from server_log.git_sink import GitSink
        sink = GitSink(repo_root=tmp_path, sanitizer=Sanitizer(), push_interval=9999)
        sink._pending_count = 3

        # Mock git commands to succeed
        mock_git.return_value = (0, "ok")

        sink._commit_and_push()

        # Should have called git add, git commit, git rev-parse, git push
        assert mock_git.call_count >= 3
        # Pending count should be reset
        assert sink._pending_count == 0

    @patch("server_log.git_sink._git")
    def test_no_commit_when_empty(self, mock_git, tmp_path):
        """No git operations when nothing is pending."""
        (tmp_path / ".git").mkdir()
        from server_log.git_sink import GitSink
        sink = GitSink(repo_root=tmp_path, sanitizer=Sanitizer(), push_interval=9999)
        sink._pending_count = 0

        sink._commit_and_push()
        mock_git.assert_not_called()


# ===================================================================
# Inbox tests
# ===================================================================


class TestInboxDispatch:
    """Inbox routes commands to handlers."""

    def test_ping_handler(self, tmp_path):
        """Built-in ping command returns pong."""
        (tmp_path / ".git").mkdir()
        (tmp_path / ".server-logs").mkdir()

        from server_log.inbox import Inbox
        inbox = Inbox(repo_root=tmp_path, poll_interval=9999)

        result = inbox._dispatch({"cmd": "ping"})
        assert result["response"] == "pong"
        assert result["ack"] == "ping"
        assert "server_time" in result

    def test_unknown_command(self, tmp_path):
        """Unknown commands return an error response."""
        (tmp_path / ".git").mkdir()
        (tmp_path / ".server-logs").mkdir()

        from server_log.inbox import Inbox
        inbox = Inbox(repo_root=tmp_path, poll_interval=9999)

        result = inbox._dispatch({"cmd": "self_destruct"})
        assert "error" in result
        assert "unknown" in result["error"]

    def test_custom_handler(self, tmp_path):
        """Custom handlers are dispatched correctly."""
        (tmp_path / ".git").mkdir()
        (tmp_path / ".server-logs").mkdir()

        def my_handler(cmd):
            return {"response": f"handled: {cmd.get('arg', '')}"}

        from server_log.inbox import Inbox
        inbox = Inbox(
            repo_root=tmp_path,
            poll_interval=9999,
            handlers={"my_cmd": my_handler},
        )

        result = inbox._dispatch({"cmd": "my_cmd", "arg": "hello"})
        assert result["response"] == "handled: hello"


class TestInboxProcessing:
    """Inbox reads and processes JSONL commands."""

    @patch("server_log.inbox._git")
    def test_processes_new_lines(self, mock_git, tmp_path):
        """New inbox lines are parsed and dispatched."""
        (tmp_path / ".git").mkdir()
        logs_dir = tmp_path / ".server-logs"
        logs_dir.mkdir()

        # Write a command to the inbox
        inbox_file = logs_dir / "inbox.jsonl"
        inbox_file.write_text(json.dumps({"cmd": "ping"}) + "\n")

        mock_git.return_value = (0, "ok")

        from server_log.inbox import Inbox
        inbox = Inbox(repo_root=tmp_path, poll_interval=9999)
        inbox._processed_lines = 0  # Reset so it reads the line

        inbox._pull_and_process()

        # Outbox should have a response
        outbox_file = logs_dir / "outbox.jsonl"
        assert outbox_file.exists()
        response = json.loads(outbox_file.read_text().strip())
        assert response["response"] == "pong"

    @patch("server_log.inbox._git")
    def test_skips_already_processed(self, mock_git, tmp_path):
        """Lines already processed are not re-executed."""
        (tmp_path / ".git").mkdir()
        logs_dir = tmp_path / ".server-logs"
        logs_dir.mkdir()

        inbox_file = logs_dir / "inbox.jsonl"
        inbox_file.write_text(json.dumps({"cmd": "ping"}) + "\n")

        mock_git.return_value = (0, "ok")

        from server_log.inbox import Inbox
        inbox = Inbox(repo_root=tmp_path, poll_interval=9999)
        inbox._processed_lines = 1  # Already seen this line

        inbox._pull_and_process()

        # No outbox written
        outbox_file = logs_dir / "outbox.jsonl"
        assert not outbox_file.exists()


class TestInboxSanitization:
    """Outbox responses are sanitized before writing."""

    @patch("server_log.inbox._git")
    def test_outbox_scrubs_secrets(self, mock_git, tmp_path):
        """Secret values from handlers are scrubbed in outbox output."""
        (tmp_path / ".git").mkdir()
        logs_dir = tmp_path / ".server-logs"
        logs_dir.mkdir()

        def leaky_handler(_cmd):
            return {"response": "password=SuperSecret1234"}

        mock_git.return_value = (0, "ok")

        from server_log.inbox import Inbox
        inbox = Inbox(
            repo_root=tmp_path,
            poll_interval=9999,
            handlers={"leak": leaky_handler},
        )

        inbox_file = logs_dir / "inbox.jsonl"
        inbox_file.write_text(json.dumps({"cmd": "leak"}) + "\n")
        inbox._processed_lines = 0

        inbox._pull_and_process()

        outbox = (logs_dir / "outbox.jsonl").read_text()
        assert "SuperSecret1234" not in outbox
        assert "REDACTED" in outbox
