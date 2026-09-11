from __future__ import annotations

from agent_hub import materialize


def test_default_destination_is_canonical_mini24_vault():
    assert materialize.DEFAULT_DEST == (
        "mini24@100.125.97.81:/Users/mini24/dev/obsidian-vault/00-Agent-Reports"
    )


def test_rsync_to_obsidian_precreates_remote_path_without_delete(monkeypatch, tmp_path):
    calls: list[list[str]] = []
    day_dir = tmp_path / "2026-05-07"
    day_dir.mkdir()

    def fake_run(cmd, check):
        calls.append(cmd)

    monkeypatch.setattr(materialize.subprocess, "run", fake_run)

    materialize.rsync_to_obsidian(
        day_dir,
        "anikdang@100.120.20.120:/Users/anikdang/Documents/Obsidian Vault/00-Agent-Reports",
    )

    assert calls[0] == [
        "ssh",
        "anikdang@100.120.20.120",
        "mkdir -p '/Users/anikdang/Documents/Obsidian Vault/00-Agent-Reports/2026-05-07/'",
    ]
    assert calls[1] == [
        "rsync",
        "-az",
        "--ignore-existing",
        f"{day_dir}/",
        "anikdang@100.120.20.120:/Users/anikdang/Documents/Obsidian Vault/00-Agent-Reports/2026-05-07/",
    ]
