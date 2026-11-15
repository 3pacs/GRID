"""Offline deploy-tree diagnostics; no production, network or file mutations."""
from subprocess import CompletedProcess

import pytest

from scripts import smoke_dad_path as smoke


@pytest.mark.parametrize("failed_command", ["log", "status"])
def test_git_failure_cannot_report_clean(tmp_path, monkeypatch, failed_command):
    (tmp_path / "pwa_dist").mkdir()
    (tmp_path / "pwa_dist/index.html").write_text("fixture")

    def run(args, **kwargs):
        return CompletedProcess(args, 128 if failed_command in args else 0, "", "denied")

    monkeypatch.setattr(smoke.subprocess, "run", run)
    result = smoke.step_deploy_tree(str(tmp_path))
    assert result.status == "blocked"
    assert f"git {failed_command} exited 128" in result.note


def test_dirty_paths_preserved_bounded(tmp_path, monkeypatch):
    (tmp_path / "pwa_dist").mkdir()
    (tmp_path / "pwa_dist/index.html").write_text("fixture")

    def run(args, **kwargs):
        output = "\n".join(f"?? fixture-{i}.json" for i in range(55)) if "status" in args else "abc fixture"
        return CompletedProcess(args, 0, output, "")

    monkeypatch.setattr(smoke.subprocess, "run", run)
    result = smoke.step_deploy_tree(str(tmp_path))
    assert result.status == "degraded"
    assert result.data["dirty_files"] == 55
    assert len(result.data["dirty_entries"]) == 50
    assert result.data["dirty_entries"][0] == "?? fixture-0.json"
    assert result.data["dirty_entries_truncated"] is True
