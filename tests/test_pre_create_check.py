"""Tests for scripts/pre_create_check.py.

Validates the coverage probe tool that future agents call before
creating new modules.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "pre_create_check.py"


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )


def test_known_keyword_chokepoint_returns_coverage():
    """A concept we know is implemented (chokepoint) returns exit 0 and lists files."""
    result = _run("chokepoint")
    assert result.returncode == 0, f"expected coverage, got stderr={result.stderr}"
    out = result.stdout
    assert "Existing coverage" in out
    assert "intelligence/supply_chokepoints.py" in out
    assert "DECISION: Coverage EXISTS" in out


def test_unknown_keyword_returns_exit_one():
    """An invented concept returns exit 1 and declares safe-to-create."""
    result = _run("zzz_totally_novel_concept_qwerty_xyz")
    assert result.returncode == 1
    assert "No existing coverage" in result.stdout
    assert "Safe to create" in result.stdout


def test_synonyms_flag_expands_search():
    """Passing --synonyms broadens the search so a synonym-only term finds coverage."""
    # Search for an unlikely word, but provide chokepoint as a synonym: must hit.
    result = _run("zzz_totally_novel_concept_qwerty_xyz", "--synonyms", "chokepoint")
    assert result.returncode == 0
    assert "intelligence/supply_chokepoints.py" in result.stdout
    # And it should mention we also searched for chokepoint.
    assert "chokepoint" in result.stdout.lower()


def test_json_output_parseable():
    """--json produces a parseable document with the expected shape."""
    result = _run("chokepoint", "--json")
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["keyword"] == "chokepoint"
    assert payload["coverage_exists"] is True
    assert isinstance(payload["files"], list)
    assert any("supply_chokepoints" in f["path"] for f in payload["files"])
    assert isinstance(payload["migrations"], list)
    assert "decision" in payload


def test_skip_list_respected():
    """__pycache__ directories must not appear in any reported file path."""
    result = _run("chokepoint", "--json")
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    for f in payload["files"]:
        assert "__pycache__" not in f["path"], f
        # Tests are excluded by default.
        assert not f["path"].startswith("tests/"), f
