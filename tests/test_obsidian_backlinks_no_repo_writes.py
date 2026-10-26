"""GRID W4d — the Hermes automated Obsidian backlink step must never
rewrite tracked docs in the repository checkout it runs from.

Before this fix: scripts/hermes_operator.py::_run_obsidian_cycle's step 5
called scripts.obsidian_backlinks.add_wikilinks() on every file from
collect_markdown_files() (README.md, CLAUDE.md, ATTENTION.md, every
docs/**/*.md) and wrote the result straight back onto that SAME tracked
file, on nearly every Hermes cycle. See
docs/handoffs/2026-09-18/fable-w4d-hermes-docs-rewrite.md.

After: scripts/obsidian_backlinks.py::resolve_backlinks_output_dir() picks
a directory outside the repo checkout (env override, else a subdirectory
of the existing Obsidian vault path, else None/skip), and
write_annotated_copy() writes there — never to the source file.

This file tests scripts/obsidian_backlinks.py's two new functions
directly against a FAKE repo root (a tmp_path with its own tracked-looking
markdown files) — no real Postgres, no real vault, no real GRID_ROOT
touched. Run with:

    DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest tests/test_obsidian_backlinks_no_repo_writes.py -q
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.obsidian_backlinks as backlinks  # noqa: E402


@pytest.fixture
def fake_repo(tmp_path, monkeypatch):
    """A fake repo root with tracked-looking markdown, wired in place of
    the real GRID_ROOT/SCAN_DIRS for collect_markdown_files().
    """
    root = tmp_path / "fake_repo"
    docs_dir = root / "docs"
    docs_dir.mkdir(parents=True)

    readme = root / "README.md"
    readme.write_text("See PIT store for background.\n", encoding="utf-8")
    arch = docs_dir / "ARCHITECTURE.md"
    arch.write_text("Uses conflict resolution and PIT store.\n", encoding="utf-8")

    monkeypatch.setattr(backlinks, "GRID_ROOT", root)
    monkeypatch.setattr(backlinks, "SCAN_DIRS", [docs_dir, root])
    # A fresh pattern cache per test — it's keyed by id(all_entities), but
    # different tests build different dict instances anyway; reset for
    # clarity/safety.
    monkeypatch.setattr(backlinks, "_PATTERN_CACHE", [])
    monkeypatch.setattr(backlinks, "_PATTERN_CACHE_KEY", 0)

    return {"root": root, "docs_dir": docs_dir, "readme": readme, "arch": arch}


def _run_automated_backlink_step(fake_repo, output_dir):
    """Reproduce exactly what scripts/hermes_operator.py::_run_obsidian_cycle's
    step 5 does, against the fake repo, and return the list of paths written.
    """
    files = backlinks.collect_markdown_files()
    doc_registry = backlinks.build_doc_registry(files)
    all_entities = {**backlinks.CONCEPT_LINKS}
    skip_stems = {"README", "CLAUDE", "index", "plan", "config"}
    for stem, target in doc_registry.items():
        if stem not in skip_stems and len(stem) > 3:
            all_entities[stem] = target

    written = []
    for f in files:
        content = f.read_text(encoding="utf-8")
        new_content, changes = backlinks.add_wikilinks(content, f, all_entities)
        if changes:
            written.append(backlinks.write_annotated_copy(output_dir, f, new_content))
    return written


class TestNeverWritesInsideTheRepoCheckout:
    def test_source_docs_are_byte_identical_after_the_step_runs(self, fake_repo, tmp_path, monkeypatch):
        output_dir = tmp_path / "vault_backlinks"
        monkeypatch.setenv(backlinks.BACKLINKS_OUTPUT_DIR_ENV, str(output_dir))

        original_readme = fake_repo["readme"].read_bytes()
        original_arch = fake_repo["arch"].read_bytes()

        resolved = backlinks.resolve_backlinks_output_dir()
        assert resolved is not None
        written = _run_automated_backlink_step(fake_repo, resolved)

        # The whole point: nothing under the fake repo root changed.
        assert fake_repo["readme"].read_bytes() == original_readme
        assert fake_repo["arch"].read_bytes() == original_arch

        # But the step DID do real work — links were found and written
        # somewhere, just not onto the tracked files.
        assert written
        for p in written:
            assert fake_repo["root"] not in p.parents
            assert output_dir.resolve() in p.parents or p.parent == output_dir.resolve()

    def test_writes_only_to_the_configured_directory_mirroring_relative_paths(self, fake_repo, tmp_path, monkeypatch):
        output_dir = tmp_path / "vault_backlinks"
        monkeypatch.setenv(backlinks.BACKLINKS_OUTPUT_DIR_ENV, str(output_dir))

        resolved = backlinks.resolve_backlinks_output_dir()
        written = _run_automated_backlink_step(fake_repo, resolved)

        written_rel = {p.relative_to(resolved) for p in written}
        assert Path("README.md") in written_rel
        assert Path("docs/ARCHITECTURE.md") in written_rel

        # The mirrored copy actually contains the added wikilink.
        readme_copy = resolved / "README.md"
        assert "[[PIT Store" in readme_copy.read_text(encoding="utf-8")


class TestOutputDirResolution:
    def test_explicit_env_var_wins(self, fake_repo, tmp_path, monkeypatch):
        chosen = tmp_path / "explicit-dir"
        monkeypatch.setenv(backlinks.BACKLINKS_OUTPUT_DIR_ENV, str(chosen))

        resolved = backlinks.resolve_backlinks_output_dir()

        assert resolved == chosen.resolve()
        assert resolved.is_dir()  # created

    def test_empty_env_var_disables_the_step(self, fake_repo, monkeypatch):
        monkeypatch.setenv(backlinks.BACKLINKS_OUTPUT_DIR_ENV, "")

        assert backlinks.resolve_backlinks_output_dir() is None

    def test_defaults_under_the_existing_vault_path_when_unset(self, fake_repo, tmp_path, monkeypatch):
        monkeypatch.delenv(backlinks.BACKLINKS_OUTPUT_DIR_ENV, raising=False)
        vault = tmp_path / "my-vault"
        vault.mkdir()

        import config

        monkeypatch.setattr(config.settings, "OBSIDIAN_VAULT_PATH", str(vault))

        resolved = backlinks.resolve_backlinks_output_dir()

        assert resolved == (vault / "grid-backlinks").resolve()

    def test_refuses_and_returns_none_if_resolved_dir_is_inside_the_repo_checkout(self, fake_repo, monkeypatch):
        """Defensive check: even if some future config points the output
        directory back inside GRID_ROOT, this must skip rather than write
        there — never silently reintroduce the bug."""
        inside = fake_repo["root"] / "docs" / "wiki"
        monkeypatch.setenv(backlinks.BACKLINKS_OUTPUT_DIR_ENV, str(inside))

        assert backlinks.resolve_backlinks_output_dir() is None
        assert not inside.exists()  # never even created

    def test_returns_none_and_does_not_raise_when_directory_cannot_be_created(self, fake_repo, tmp_path, monkeypatch):
        # A file (not a directory) in the path makes mkdir(parents=True) fail.
        blocker = tmp_path / "blocker"
        blocker.write_text("not a directory", encoding="utf-8")
        unwritable = blocker / "subdir"
        monkeypatch.setenv(backlinks.BACKLINKS_OUTPUT_DIR_ENV, str(unwritable))

        assert backlinks.resolve_backlinks_output_dir() is None
