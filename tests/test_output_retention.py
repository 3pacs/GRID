"""discovery.output_retention.prune_outputs (2026-11-12)."""

from __future__ import annotations

import os
from datetime import datetime

import pytest

from discovery.output_retention import prune_outputs

NOW = datetime(2026, 11, 12, 12, 0, 0)


def _touch(d, name: str) -> None:
    (d / name).write_text("x")


@pytest.mark.unit
def test_removes_only_stamped_files_older_than_keep_days(tmp_path) -> None:
    _touch(tmp_path, "corr_full_20260329_033512.png")      # old: removed
    _touch(tmp_path, "factor_loadings_20261020_104650.csv")  # 23d: removed
    _touch(tmp_path, "pca_scree_20261105_104650.png")       # 7d: kept
    _touch(tmp_path, "README.md")                           # unstamped: kept
    _touch(tmp_path, "notes_20260101.png")                  # wrong pattern: kept

    assert prune_outputs(tmp_path, keep_days=14, now=NOW) == 2
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "README.md", "notes_20260101.png", "pca_scree_20261105_104650.png",
    ]


@pytest.mark.unit
def test_never_follows_or_deletes_symlinks(tmp_path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    target = outside / "keep_20260101_000000.csv"
    target.write_text("x")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    os.symlink(target, run_dir / "link_20260101_000000.csv")

    assert prune_outputs(run_dir, keep_days=14, now=NOW) == 0
    assert target.exists()


@pytest.mark.unit
def test_missing_directory_is_not_an_error(tmp_path) -> None:
    assert prune_outputs(tmp_path / "absent", now=NOW) == 0


@pytest.mark.unit
def test_keep_days_must_be_positive(tmp_path) -> None:
    with pytest.raises(ValueError):
        prune_outputs(tmp_path, keep_days=0)
