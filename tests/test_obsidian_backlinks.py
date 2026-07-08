"""Regression tests for the Obsidian wikilink backlinker."""

from __future__ import annotations

from pathlib import Path

from scripts.obsidian_backlinks import add_wikilinks, find_malformed_wikilinks


def test_backlinker_does_not_wrap_plural_suffixes() -> None:
    content = "promotion gates, actor networks, dark pools, and entity mappings"
    entities = {
        "promotion gate": "Walk-Forward Backtesting",
        "actor network": "Actor Network",
        "dark pool": "Dark Pool",
        "entity map": "Entity Map",
    }

    output, changes = add_wikilinks(content, Path("docs/Test.md"), entities)

    assert output == content
    assert changes == []


def test_backlinker_does_not_wrap_module_basename_inside_path() -> None:
    content = "intelligence/postmortem.py handles postmortem."
    entities = {
        "postmortem.py": "Postmortem",
        "postmortem": "Postmortem",
    }

    output, changes = add_wikilinks(content, Path("docs/Test.md"), entities)

    assert "intelligence/postmortem.py" in output
    assert "intelligence/[[Postmortem|postmortem.py]]" not in output
    assert output.endswith("[[Postmortem|postmortem]].")
    assert changes == ["  L1: 'postmortem' → [[Postmortem]]"]


def test_backlinker_counts_existing_wikilink_as_linked_target() -> None:
    content = "Use [[Decision Journal|decision journal]] before decision journal."
    entities = {"decision journal": "Decision Journal"}

    output, changes = add_wikilinks(content, Path("docs/Test.md"), entities)

    assert output == content
    assert changes == []


def test_malformed_wikilink_checker_flags_visualization_breakers() -> None:
    content = "\n".join(
        [
            "Use [[Decision Journal|[[Decision Journal|decision journal]]]].",
            "Review [[Entity Map|entity map]]pings.",
            "Broken [[Entity",
            "Map|entity map]] across lines.",
            "```",
            "Callable[[str], str]",
            "```",
        ]
    )

    findings = find_malformed_wikilinks(content)
    reasons = [reason for _, reason, _ in findings]

    assert "nested wikilink" in reasons
    assert "wikilink has attached suffix" in reasons
    assert "wikilink spans multiple lines" in reasons
    assert not any("Callable" in line for _, _, line in findings)
