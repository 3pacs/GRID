"""
GRID workflow loader.

Parses declarative workflow files (Markdown + YAML frontmatter) from the
workflows/available/ and workflows/enabled/ directories.  Provides
enable/disable via symlinks, validation, and schedule parsing.

Workflow file format:
    ---
    name: pull-ecb
    group: ingestion
    schedule: "daily 20:00 weekdays"
    secrets: ["ECB_API_KEY"]
    depends_on: []
    description: Pull ECB Statistical Data Warehouse series
    ---
    ## Steps
    1. ...

    ## Output
    - ...

    ## Notes
    - ...
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from loguru import logger as log

# Resolve workflow directories relative to this file
_WORKFLOWS_DIR = Path(__file__).parent
_AVAILABLE_DIR = _WORKFLOWS_DIR / "available"
_ENABLED_DIR = _WORKFLOWS_DIR / "enabled"


def _parse_frontmatter(content: str) -> tuple[dict[str, Any], str]:
    """Extract YAML frontmatter and markdown body from a workflow file.

    Parameters:
        content: Raw file content.

    Returns:
        tuple: (frontmatter_dict, markdown_body)
    """
    match = re.match(r"^---\s*\n(.*?)\n---\s*\n(.*)", content, re.DOTALL)
    if not match:
        return {}, content

    try:
        frontmatter = yaml.safe_load(match.group(1)) or {}
    except yaml.YAMLError as exc:
        log.warning("Failed to parse YAML frontmatter: {e}", e=str(exc))
        frontmatter = {}

    body = match.group(2)
    return frontmatter, body


def _parse_sections(body: str) -> dict[str, str]:
    """Parse markdown sections (## heading) into a dict.

    Parameters:
        body: Markdown body after frontmatter.

    Returns:
        dict: {section_name: section_content}
    """
    sections: dict[str, str] = {}
    current_section = "_preamble"
    current_lines: list[str] = []

    for line in body.split("\n"):
        if line.startswith("## "):
            if current_lines:
                sections[current_section] = "\n".join(current_lines).strip()
            current_section = line[3:].strip().lower()
            current_lines = []
        else:
            current_lines.append(line)

    if current_lines:
        sections[current_section] = "\n".join(current_lines).strip()

    return sections


def load_workflow(path: Path) -> dict[str, Any]:
    """Load and parse a single workflow file.

    Parameters:
        path: Path to the .md workflow file.

    Returns:
        dict: Parsed workflow with keys:
            name, group, schedule, secrets, depends_on, description,
            steps, output, notes, file_path, enabled
    """
    content = path.read_text(encoding="utf-8")
    frontmatter, body = _parse_frontmatter(content)
    sections = _parse_sections(body)

    workflow: dict[str, Any] = {
        "name": frontmatter.get("name", path.stem),
        "group": frontmatter.get("group", "misc"),
        "schedule": frontmatter.get("schedule", ""),
        "secrets": frontmatter.get("secrets", []),
        "depends_on": frontmatter.get("depends_on", []),
        "description": frontmatter.get("description", ""),
        "steps": sections.get("steps", ""),
        "output": sections.get("output", ""),
        "notes": sections.get("notes", ""),
        "file_path": str(path),
        "enabled": False,
    }

    # Check if enabled (symlink exists in enabled/)
    enabled_path = _ENABLED_DIR / path.name
    workflow["enabled"] = enabled_path.is_symlink() or enabled_path.exists()

    return workflow


def load_all_available() -> list[dict[str, Any]]:
    """Load all workflows from the available/ directory.

    Returns:
        list[dict]: Parsed workflows sorted by name.
    """
    _AVAILABLE_DIR.mkdir(parents=True, exist_ok=True)
    workflows = []

    for path in sorted(_AVAILABLE_DIR.glob("*.md")):
        try:
            wf = load_workflow(path)
            workflows.append(wf)
        except Exception as exc:
            log.warning("Failed to load workflow {p}: {e}", p=path, e=str(exc))

    log.info("Loaded {n} available workflows", n=len(workflows))
    return workflows


def load_enabled() -> list[dict[str, Any]]:
    """Load only enabled workflows (those symlinked in enabled/).

    Returns:
        list[dict]: Enabled workflows sorted by name.
    """
    _ENABLED_DIR.mkdir(parents=True, exist_ok=True)
    workflows = []

    for path in sorted(_ENABLED_DIR.glob("*.md")):
        try:
            # Resolve symlink to get the actual file
            actual_path = path.resolve()
            wf = load_workflow(actual_path)
            wf["enabled"] = True
            workflows.append(wf)
        except Exception as exc:
            log.warning("Failed to load enabled workflow {p}: {e}", p=path, e=str(exc))

    log.info("Loaded {n} enabled workflows", n=len(workflows))
    return workflows


def enable_workflow(name: str) -> bool:
    """Enable a workflow by creating a symlink in enabled/.

    Parameters:
        name: Workflow name (without .md extension).

    Returns:
        bool: True if successfully enabled.
    """
    source = _AVAILABLE_DIR / f"{name}.md"
    target = _ENABLED_DIR / f"{name}.md"

    if not source.exists():
        log.error("Workflow '{n}' not found in available/", n=name)
        return False

    _ENABLED_DIR.mkdir(parents=True, exist_ok=True)

    if target.exists() or target.is_symlink():
        log.info("Workflow '{n}' already enabled", n=name)
        return True

    try:
        target.symlink_to(source.resolve())
        log.info("Enabled workflow '{n}'", n=name)
        return True
    except OSError as exc:
        log.error("Failed to enable '{n}': {e}", n=name, e=str(exc))
        return False


def disable_workflow(name: str) -> bool:
    """Disable a workflow by removing its symlink from enabled/.

    Parameters:
        name: Workflow name (without .md extension).

    Returns:
        bool: True if successfully disabled.
    """
    target = _ENABLED_DIR / f"{name}.md"

    if not target.exists() and not target.is_symlink():
        log.info("Workflow '{n}' not currently enabled", n=name)
        return True

    try:
        target.unlink()
        log.info("Disabled workflow '{n}'", n=name)
        return True
    except OSError as exc:
        log.error("Failed to disable '{n}': {e}", n=name, e=str(exc))
        return False


def validate_workflow(path: Path) -> list[str]:
    """Validate a workflow file for required fields and structure.

    Parameters:
        path: Path to the .md workflow file.

    Returns:
        list[str]: Validation errors.  Empty if valid.
    """
    errors: list[str] = []

    try:
        content = path.read_text(encoding="utf-8")
    except Exception as exc:
        return [f"Cannot read file: {exc}"]

    frontmatter, body = _parse_frontmatter(content)

    if not frontmatter:
        errors.append("Missing or invalid YAML frontmatter")
        return errors

    required_fields = ["name", "group", "schedule", "description"]
    for field in required_fields:
        if field not in frontmatter:
            errors.append(f"Missing required field: {field}")

    valid_groups = {
        "ingestion", "resolution", "features", "discovery",
        "validation", "governance", "physics", "misc",
    }
    group = frontmatter.get("group", "")
    if group and group not in valid_groups:
        errors.append(f"Invalid group '{group}'. Valid: {sorted(valid_groups)}")

    sections = _parse_sections(body)
    if "steps" not in sections:
        errors.append("Missing '## Steps' section")

    # Check depends_on references
    depends = frontmatter.get("depends_on", [])
    if depends:
        available_names = {p.stem for p in _AVAILABLE_DIR.glob("*.md")}
        for dep in depends:
            if dep not in available_names:
                errors.append(f"depends_on '{dep}' not found in available/")

    return errors


def resolve_secrets(workflow: dict[str, Any], env: dict[str, str] | None = None) -> dict[str, str]:
    """Resolve secret references for a workflow.

    Looks up {{SECRET:KEY_NAME}} references from environment variables.

    Parameters:
        workflow: Parsed workflow dict.
        env: Environment dict (default: os.environ).

    Returns:
        dict: {secret_name: value} for all declared secrets.
    """
    if env is None:
        env = dict(os.environ)

    resolved: dict[str, str] = {}
    for secret_name in workflow.get("secrets", []):
        value = env.get(secret_name, "")
        if not value:
            log.warning("Secret '{s}' not found in environment", s=secret_name)
        resolved[secret_name] = value

    return resolved


def parse_schedule(schedule_str: str) -> dict[str, Any]:
    """Parse a human-readable schedule string into structured form.

    Formats supported:
        "daily 20:00 weekdays"
        "weekly sunday 03:00"
        "monthly 2 04:00"
        "annual january 15 04:30"
        "manual"

    Parameters:
        schedule_str: Human-readable schedule.

    Returns:
        dict: {frequency, time, days, day_of_month, month}
    """
    parts = schedule_str.strip().lower().split()
    result: dict[str, Any] = {"frequency": "manual", "raw": schedule_str}

    if not parts:
        return result

    freq = parts[0]
    result["frequency"] = freq

    if freq == "daily" and len(parts) >= 2:
        result["time"] = parts[1]
        if len(parts) >= 3 and parts[2] == "weekdays":
            result["days"] = ["monday", "tuesday", "wednesday", "thursday", "friday"]
        else:
            result["days"] = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    elif freq == "weekly" and len(parts) >= 3:
        result["days"] = [parts[1]]
        result["time"] = parts[2]

    elif freq == "monthly" and len(parts) >= 3:
        result["day_of_month"] = int(parts[1])
        result["time"] = parts[2]

    elif freq == "annual" and len(parts) >= 4:
        result["month"] = parts[1]
        result["day_of_month"] = int(parts[2])
        result["time"] = parts[3]

    return result
