"""Tests for scripts/dropin_backup_and_write.sh.

This exercises the actual backup-before-overwrite mechanism deploy.yml's
grid-intelligence (and, by the same pre-existing pattern, grid-scheduler)
restart steps rely on -- previously only reviewable as inline bash, never
executed by a test. Runs against a real temporary directory; no sudo, no
systemd involved.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "dropin_backup_and_write.sh"


def _run(dropin: Path, service: str, content: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(SCRIPT), str(dropin), service],
        input=content,
        capture_output=True,
        text=True,
        timeout=10,
    )


class TestDropinBackupAndWrite:
    def test_first_activation_no_existing_file(self, tmp_path):
        dropin = tmp_path / "zz-release-worktree.conf"
        result = _run(dropin, "grid-intelligence", "[Service]\nWorkingDirectory=/data/grid_v4/grid_release\n")

        assert result.returncode == 0
        assert "first activation" in result.stdout
        assert dropin.read_text() == "[Service]\nWorkingDirectory=/data/grid_v4/grid_release\n"
        # No backup file should exist -- nothing to back up.
        assert list(tmp_path.glob("*.bak-*")) == []

    def test_first_activation_rollback_command_is_a_plain_delete(self, tmp_path):
        dropin = tmp_path / "zz-release-worktree.conf"
        result = _run(dropin, "grid-intelligence", "[Service]\n")

        assert f"rm {dropin}" in result.stdout
        assert "daemon-reload" in result.stdout
        assert "restart grid-intelligence" in result.stdout

    def test_existing_file_is_backed_up_not_overwritten_blind(self, tmp_path):
        dropin = tmp_path / "zz-release-worktree.conf"
        original_content = "[Service]\nWorkingDirectory=/some/prior/path\n"
        dropin.write_text(original_content)

        result = _run(dropin, "grid-intelligence", "[Service]\nWorkingDirectory=/data/grid_v4/grid_release\n")

        assert result.returncode == 0
        # The new content landed...
        assert dropin.read_text() == "[Service]\nWorkingDirectory=/data/grid_v4/grid_release\n"
        # ...but the ORIGINAL content is recoverable from a backup file.
        backups = list(tmp_path.glob("*.bak-*"))
        assert len(backups) == 1
        assert backups[0].read_text() == original_content

    def test_existing_file_rollback_command_restores_the_actual_backup(self, tmp_path):
        dropin = tmp_path / "zz-release-worktree.conf"
        dropin.write_text("[Service]\nWorkingDirectory=/some/prior/path\n")

        result = _run(dropin, "grid-intelligence", "[Service]\n")
        backups = list(tmp_path.glob("*.bak-*"))
        assert len(backups) == 1

        assert f"cp -a {backups[0]} {dropin}" in result.stdout
        assert "daemon-reload" in result.stdout
        assert "restart grid-intelligence" in result.stdout

    def test_rollback_command_is_actually_executable_and_restores_original(self, tmp_path):
        """Not just that the printed command *looks* right -- run it (minus
        the systemctl calls, which need root/a real unit) and confirm the
        file is byte-for-byte the original again.
        """
        dropin = tmp_path / "zz-release-worktree.conf"
        original_content = "[Service]\nWorkingDirectory=/some/prior/path\n"
        dropin.write_text(original_content)

        _run(dropin, "grid-intelligence", "[Service]\nWorkingDirectory=/data/grid_v4/grid_release\n")
        assert dropin.read_text() != original_content  # overwritten

        backups = list(tmp_path.glob("*.bak-*"))
        subprocess.run(["cp", "-a", str(backups[0]), str(dropin)], check=True)

        assert dropin.read_text() == original_content

    def test_missing_arguments_fail_loudly_not_silently(self):
        result = subprocess.run(
            ["bash", str(SCRIPT)],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode != 0
