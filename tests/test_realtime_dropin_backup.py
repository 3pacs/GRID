"""Tests for scripts/realtime_dropin_backup.sh.

Proves the backup-then-rollback round trip actually works -- restoring from
the printed ROLLBACK command must reproduce the original drop-in
byte-for-byte -- rather than only asserting that some backup file exists.
Runs entirely against a temp directory; no sudo, no real systemd unit.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "realtime_dropin_backup.sh"


def _run(dropin_dir: Path, dropin_file: Path, working_directory: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(SCRIPT), str(dropin_dir), str(dropin_file), working_directory],
        capture_output=True,
        text=True,
        timeout=10,
    )


def _parse(stdout: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in stdout.strip().splitlines():
        key, _, value = line.partition(":")
        fields[key] = value
    return fields


class TestRealtimeDropinBackup:
    def test_first_activation_has_no_backup(self, tmp_path):
        dropin_dir = tmp_path / "grid-realtime.service.d"
        dropin_file = dropin_dir / "zz-release-worktree.conf"

        result = _run(dropin_dir, dropin_file, "/data/grid_v4/grid_release")

        assert result.returncode == 0, result.stderr
        fields = _parse(result.stdout)
        assert "NO_PRIOR_DROPIN" in fields or "NO_PRIOR_DROPIN" in result.stdout
        assert dropin_file.exists()
        assert "WorkingDirectory=/data/grid_v4/grid_release" in dropin_file.read_text()

    def test_second_activation_backs_up_the_first(self, tmp_path):
        dropin_dir = tmp_path / "grid-realtime.service.d"
        dropin_file = dropin_dir / "zz-release-worktree.conf"

        _run(dropin_dir, dropin_file, "/data/grid_v4/grid_release")
        first_content = dropin_file.read_text()

        result = _run(dropin_dir, dropin_file, "/data/grid_v4/grid_release_v2")
        fields = _parse(result.stdout)

        assert "BACKED_UP" in fields, result.stdout
        backup_path = Path(fields["BACKED_UP"])
        assert backup_path.exists()
        assert backup_path.read_text() == first_content, (
            "the backup must be an exact copy of what was there before the overwrite"
        )
        assert "WorkingDirectory=/data/grid_v4/grid_release_v2" in dropin_file.read_text()

    def test_rollback_command_actually_restores_the_original_content(self, tmp_path):
        """Not just 'a backup exists' -- executes the printed ROLLBACK command
        (minus the systemd calls, which need a real unit) and confirms the
        live file is restored byte-for-byte.
        """
        dropin_dir = tmp_path / "grid-realtime.service.d"
        dropin_file = dropin_dir / "zz-release-worktree.conf"

        _run(dropin_dir, dropin_file, "/data/grid_v4/grid_release")
        original_content = dropin_file.read_text()

        result = _run(dropin_dir, dropin_file, "/data/grid_v4/grid_release_v2")
        fields = _parse(result.stdout)
        assert dropin_file.read_text() != original_content  # sanity: it really did change

        rollback_cmd = fields["ROLLBACK"]
        # The real command also runs `systemctl daemon-reload && systemctl
        # restart grid-realtime`, which needs a live systemd -- strip that
        # part and execute only the file-restore half, which is the part
        # this script is actually responsible for getting right.
        file_restore_cmd = rollback_cmd.split("&&")[0].strip()
        assert file_restore_cmd.startswith("cp -a ")

        subprocess.run(file_restore_cmd, shell=True, check=True, cwd=str(tmp_path))

        assert dropin_file.read_text() == original_content, (
            "executing the printed ROLLBACK command must restore the exact "
            "original content, not just leave *a* file in place"
        )

    def test_rollback_command_on_first_activation_removes_the_file(self, tmp_path):
        dropin_dir = tmp_path / "grid-realtime.service.d"
        dropin_file = dropin_dir / "zz-release-worktree.conf"

        result = _run(dropin_dir, dropin_file, "/data/grid_v4/grid_release")
        fields = _parse(result.stdout)
        assert dropin_file.exists()

        rollback_cmd = fields["ROLLBACK"]
        file_restore_cmd = rollback_cmd.split("&&")[0].strip()
        assert file_restore_cmd == f"rm {dropin_file}"

        subprocess.run(file_restore_cmd, shell=True, check=True)
        assert not dropin_file.exists(), (
            "rolling back a first-ever activation must remove the drop-in "
            "entirely, restoring the unit's un-overridden WorkingDirectory"
        )

    def test_rollback_line_names_the_correct_service(self, tmp_path):
        dropin_dir = tmp_path / "grid-realtime.service.d"
        dropin_file = dropin_dir / "zz-release-worktree.conf"

        result = _run(dropin_dir, dropin_file, "/data/grid_v4/grid_release")
        assert "restart grid-realtime" in result.stdout

    def test_does_not_error_under_set_euo_pipefail(self, tmp_path):
        # Exercises the script's own `set -euo pipefail` with a nested,
        # non-trivial directory structure to catch an unquoted-variable
        # regression early.
        dropin_dir = tmp_path / "nested" / "grid-realtime.service.d"
        dropin_file = dropin_dir / "zz-release-worktree.conf"
        result = _run(dropin_dir, dropin_file, "/data/grid_v4/grid_release")
        assert result.returncode == 0, result.stderr
