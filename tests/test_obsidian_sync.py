"""Tests for Obsidian vault <-> Postgres sync engine."""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

import pytest


def _make_note(tmp: Path, rel_path: str, content: str) -> Path:
    fp = tmp / rel_path
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_text(content)
    return fp


SAMPLE_NOTE = """\
---
title: Test Tool
domain: tools
status: evaluating
tags: [testing, ci]
confidence: confirmed
priority: medium
---

# Test Tool

This is a test tool evaluation.
"""

SAMPLE_NOTE_NO_FM = """\
# Raw Note

No frontmatter here, just markdown.
"""


class TestParseFrontmatter:
    def test_parses_yaml_frontmatter(self):
        from ingestion.altdata.obsidian_sync import parse_frontmatter
        fm, body = parse_frontmatter(SAMPLE_NOTE)
        assert fm["title"] == "Test Tool"
        assert fm["domain"] == "tools"
        assert fm["status"] == "evaluating"
        assert fm["tags"] == ["testing", "ci"]
        assert "# Test Tool" in body

    def test_no_frontmatter_returns_empty_dict(self):
        from ingestion.altdata.obsidian_sync import parse_frontmatter
        fm, body = parse_frontmatter(SAMPLE_NOTE_NO_FM)
        assert fm == {}
        assert "# Raw Note" in body

    def test_empty_string(self):
        from ingestion.altdata.obsidian_sync import parse_frontmatter
        fm, body = parse_frontmatter("")
        assert fm == {}
        assert body == ""


class TestDomainFromPath:
    def test_tools_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path
        assert domain_from_path("02-Tools/Firecrawl.md") == "tools"

    def test_pipeline_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path
        assert domain_from_path("01-Pipeline/Inbox.md") == "pipeline"

    def test_alpha_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path
        assert domain_from_path("03-Alpha/nvda-signal.md") == "alpha"

    def test_intel_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path
        assert domain_from_path("04-Intel/fed-analysis.md") == "intel"

    def test_grid_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path
        assert domain_from_path("05-GRID/notes.md") == "grid"

    def test_dashboard_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path
        assert domain_from_path("00-DASHBOARD.md") == "dashboard"

    def test_unknown_defaults_to_grid(self):
        from ingestion.altdata.obsidian_sync import domain_from_path
        assert domain_from_path("random/file.md") == "grid"


class TestContentHash:
    def test_sha256_of_content(self):
        from ingestion.altdata.obsidian_sync import content_hash
        expected = hashlib.sha256(SAMPLE_NOTE.encode()).hexdigest()
        assert content_hash(SAMPLE_NOTE) == expected


class TestScanVault:
    def test_finds_markdown_files(self):
        from ingestion.altdata.obsidian_sync import scan_vault
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            _make_note(vault, "02-Tools/Firecrawl.md", SAMPLE_NOTE)
            _make_note(vault, "03-Alpha/signal.md", SAMPLE_NOTE_NO_FM)
            (vault / ".obsidian").mkdir()
            (vault / ".obsidian" / "config.json").write_text("{}")
            results = scan_vault(vault)
            paths = [r["vault_path"] for r in results]
            assert "02-Tools/Firecrawl.md" in paths
            assert "03-Alpha/signal.md" in paths
            assert not any(".obsidian" in p for p in paths)

    def test_parses_frontmatter_from_files(self):
        from ingestion.altdata.obsidian_sync import scan_vault
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            _make_note(vault, "02-Tools/Test.md", SAMPLE_NOTE)
            results = scan_vault(vault)
            assert len(results) == 1
            assert results[0]["frontmatter"]["title"] == "Test Tool"
            assert results[0]["domain"] == "tools"


class TestBuildFrontmatter:
    def test_renders_yaml_block(self):
        from ingestion.altdata.obsidian_sync import build_frontmatter
        fm = {"title": "Test", "domain": "tools", "status": "approved", "tags": ["a", "b"]}
        result = build_frontmatter(fm)
        assert result.startswith("---\n")
        assert result.endswith("---\n")
        assert "title: Test" in result
        assert "domain: tools" in result

    def test_empty_frontmatter(self):
        from ingestion.altdata.obsidian_sync import build_frontmatter
        result = build_frontmatter({})
        assert result.startswith("---\n")
        assert result.endswith("---\n")


class TestBuildNoteFile:
    def test_combines_frontmatter_and_body(self):
        from ingestion.altdata.obsidian_sync import build_note_file
        fm = {"title": "Test", "domain": "tools"}
        body = "# Test\n\nSome content."
        result = build_note_file(fm, body)
        assert result.startswith("---\n")
        assert "# Test" in result
        assert "Some content." in result


class TestDomainToFolder:
    def test_tools_folder(self):
        from ingestion.altdata.obsidian_sync import domain_to_folder
        assert domain_to_folder("tools") == "02-Tools"

    def test_alpha_folder(self):
        from ingestion.altdata.obsidian_sync import domain_to_folder
        assert domain_to_folder("alpha") == "03-Alpha"

    def test_unknown_defaults_to_grid(self):
        from ingestion.altdata.obsidian_sync import domain_to_folder
        assert domain_to_folder("unknown") == "05-GRID"


class TestGenerateDashboard:
    def test_generates_markdown_with_stats(self):
        from ingestion.altdata.obsidian_sync import generate_dashboard
        notes = [
            {"domain": "tools", "status": "inbox", "title": "T1", "agent_flags": {"needs_human_review": True, "priority": "urgent"}},
            {"domain": "tools", "status": "approved", "title": "T2", "agent_flags": {}},
            {"domain": "alpha", "status": "evaluating", "title": "A1", "agent_flags": {"needs_human_review": True, "priority": "high"}},
        ]
        actions = [
            {"action": "created", "detail": {"vault_path": "03-Alpha/signal.md"}, "actor": "hermes", "created_at": "2026-04-04T12:00:00Z"},
        ]
        md = generate_dashboard(notes, actions)
        assert "# GRID Intelligence Vault" in md
        assert "Needs Your Review" in md
        assert "Pipeline Stats" in md
        assert "tools" in md.lower()

    def test_urgent_sorted_before_high(self):
        from ingestion.altdata.obsidian_sync import generate_dashboard
        notes = [
            {"domain": "alpha", "status": "inbox", "title": "High Item", "agent_flags": {"needs_human_review": True, "priority": "high"}},
            {"domain": "tools", "status": "inbox", "title": "Urgent Item", "agent_flags": {"needs_human_review": True, "priority": "urgent"}},
        ]
        md = generate_dashboard(notes, [])
        urgent_pos = md.index("URGENT")
        high_pos = md.index("HIGH")
        assert urgent_pos < high_pos

    def test_no_review_items_skips_section(self):
        from ingestion.altdata.obsidian_sync import generate_dashboard
        notes = [{"domain": "tools", "status": "approved", "title": "T1", "agent_flags": {}}]
        md = generate_dashboard(notes, [])
        assert "Needs Your Review" not in md
        assert "Pipeline Stats" in md
