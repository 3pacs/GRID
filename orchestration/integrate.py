#!/usr/bin/env python3
"""
GRID Multi-Model Integration Tool.

Validates external model contributions and slots them into the codebase.

Usage:
    # Print a brief for a target model (ready to paste)
    python orchestration/integrate.py brief ux
    python orchestration/integrate.py brief algo
    python orchestration/integrate.py brief research

    # Validate a contribution in the inbox
    python orchestration/integrate.py check inbox/MyComponent.jsx

    # Slot a contribution into the codebase
    python orchestration/integrate.py slot inbox/MyComponent.jsx

    # List all pending inbox items
    python orchestration/integrate.py inbox
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

GRID_ROOT = Path(__file__).parent.parent
INBOX = GRID_ROOT / "orchestration" / "inbox"
BRIEFS = GRID_ROOT / "orchestration" / "briefs"

# Maps file extensions to target directories and validation rules
SLOT_RULES: dict[str, dict] = {
    ".jsx": {
        "targets": {
            "views": GRID_ROOT / "pwa" / "src" / "views",
            "components": GRID_ROOT / "pwa" / "src" / "components",
        },
        "default": "views",
        "checks": ["no_css_import", "has_default_export", "uses_inline_styles"],
    },
    ".py": {
        "targets": {
            "ingestion": GRID_ROOT / "ingestion",
            "features": GRID_ROOT / "features",
            "discovery": GRID_ROOT / "discovery",
            "validation": GRID_ROOT / "validation",
            "inference": GRID_ROOT / "inference",
            "normalization": GRID_ROOT / "normalization",
            "scripts": GRID_ROOT / "scripts",
            "tests": GRID_ROOT / "tests",
        },
        "default": "scripts",
        "checks": ["no_fstring_sql", "has_type_hints", "no_star_import"],
    },
    ".sql": {
        "targets": {
            "migrations": GRID_ROOT / "migrations",
        },
        "default": "migrations",
        "checks": ["no_drop_table"],
    },
}


# ── Validation checks ────────────────────────────────────────

def check_no_fstring_sql(content: str) -> list[str]:
    """Detect f-string or .format() SQL injection risks."""
    issues = []
    for i, line in enumerate(content.splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'''"):
            continue
        # f-string with SQL keywords
        if re.search(r'f["\'].*\b(SELECT|INSERT|UPDATE|DELETE|WHERE|FROM)\b', line, re.IGNORECASE):
            issues.append(f"  Line {i}: f-string SQL detected — use text() with bindparams")
        # .format() with SQL keywords
        if re.search(r'\.format\(.*\).*\b(SELECT|INSERT|UPDATE|DELETE|WHERE|FROM)\b', line, re.IGNORECASE):
            issues.append(f"  Line {i}: .format() SQL detected — use text() with bindparams")
        if re.search(r'\b(SELECT|INSERT|UPDATE|DELETE)\b.*\.format\(', line, re.IGNORECASE):
            issues.append(f"  Line {i}: .format() SQL detected — use text() with bindparams")
    return issues


def check_has_type_hints(content: str) -> list[str]:
    """Check that function definitions have return type hints."""
    issues = []
    for i, line in enumerate(content.splitlines(), 1):
        if re.match(r'\s*def \w+\(', line) and '->' not in line:
            fn_name = re.search(r'def (\w+)', line)
            name = fn_name.group(1) if fn_name else "unknown"
            if name.startswith("_") and name != "__init__":
                continue  # skip private helpers
            issues.append(f"  Line {i}: function '{name}' missing return type hint")
    return issues


def check_no_star_import(content: str) -> list[str]:
    """Detect wildcard imports."""
    issues = []
    for i, line in enumerate(content.splitlines(), 1):
        if re.match(r'\s*from\s+\S+\s+import\s+\*', line):
            issues.append(f"  Line {i}: wildcard import — use explicit imports")
    return issues


def check_no_css_import(content: str) -> list[str]:
    """Detect CSS file imports in JSX."""
    issues = []
    for i, line in enumerate(content.splitlines(), 1):
        if re.search(r"import\s+.*\.css", line):
            issues.append(f"  Line {i}: CSS import detected — use inline styles object")
    return issues


def check_has_default_export(content: str) -> list[str]:
    """Verify JSX has a default export."""
    if "export default" not in content:
        return ["  Missing 'export default' — component must be the default export"]
    return []


def check_uses_inline_styles(content: str) -> list[str]:
    """Check JSX uses inline styles pattern."""
    if "className=" in content and "const styles" not in content:
        return ["  Uses className without styles object — prefer inline styles (const styles = {...})"]
    return []


def check_no_drop_table(content: str) -> list[str]:
    """Detect DROP TABLE in SQL."""
    issues = []
    for i, line in enumerate(content.splitlines(), 1):
        if re.search(r'\bDROP\s+TABLE\b', line, re.IGNORECASE):
            issues.append(f"  Line {i}: DROP TABLE detected — dangerous operation")
    return issues


CHECKS = {
    "no_fstring_sql": check_no_fstring_sql,
    "has_type_hints": check_has_type_hints,
    "no_star_import": check_no_star_import,
    "no_css_import": check_no_css_import,
    "has_default_export": check_has_default_export,
    "uses_inline_styles": check_uses_inline_styles,
    "no_drop_table": check_no_drop_table,
}


# ── Commands ──────────────────────────────────────────────────

def cmd_brief(target: str) -> None:
    """Print a brief for the target model."""
    brief_file = BRIEFS / f"{target}.md"
    if not brief_file.exists():
        available = [f.stem for f in BRIEFS.glob("*.md") if f.stem != "TEMPLATE"]
        print(f"Unknown brief target '{target}'. Available: {', '.join(available)}")
        sys.exit(1)

    content = brief_file.read_text()
    print(content)
    print("\n" + "=" * 60)
    print("Copy everything above and paste into the model's chat UI.")
    print("Fill in the 'Your Task' section with your specific request.")
    print("=" * 60)


def cmd_check(filepath: str) -> None:
    """Validate a contribution file."""
    path = Path(filepath)
    if not path.exists():
        path = INBOX / filepath
    if not path.exists():
        print(f"File not found: {filepath}")
        sys.exit(1)

    ext = path.suffix.lower()
    rules = SLOT_RULES.get(ext)
    if not rules:
        print(f"Unknown file type: {ext}")
        print(f"Supported: {', '.join(SLOT_RULES.keys())}")
        return

    content = path.read_text()
    all_issues: list[str] = []

    for check_name in rules["checks"]:
        check_fn = CHECKS.get(check_name)
        if check_fn:
            issues = check_fn(content)
            if issues:
                all_issues.extend([f"[{check_name}]"] + issues)

    if all_issues:
        print(f"Issues found in {path.name}:")
        for issue in all_issues:
            print(f"  {issue}")
        print(f"\n{len(all_issues)} issue(s) — fix before slotting.")
    else:
        print(f"{path.name}: all checks passed")

    # File stats
    lines = len(content.splitlines())
    print(f"  {lines} lines, {len(content)} bytes, type={ext}")


def cmd_slot(filepath: str) -> None:
    """Validate and slot a contribution into the codebase."""
    path = Path(filepath)
    if not path.exists():
        path = INBOX / filepath
    if not path.exists():
        print(f"File not found: {filepath}")
        sys.exit(1)

    ext = path.suffix.lower()
    rules = SLOT_RULES.get(ext)
    if not rules:
        print(f"Unknown file type: {ext}")
        return

    content = path.read_text()

    # Run checks first
    all_issues: list[str] = []
    for check_name in rules["checks"]:
        check_fn = CHECKS.get(check_name)
        if check_fn:
            issues = check_fn(content)
            all_issues.extend(issues)

    if all_issues:
        print(f"Cannot slot — {len(all_issues)} issue(s) found:")
        for issue in all_issues:
            print(f"  {issue}")
        print("\nFix issues first, then retry.")
        sys.exit(1)

    # Determine target directory
    targets = rules["targets"]
    if len(targets) == 1:
        target_key = list(targets.keys())[0]
    else:
        # Try to auto-detect from filename or content
        target_key = _detect_target(path.name, content, targets, rules["default"])

    target_dir = targets[target_key]
    dest = target_dir / path.name

    if dest.exists():
        print(f"Warning: {dest} already exists — will be overwritten")
        resp = input("Continue? [y/N] ").strip().lower()
        if resp != "y":
            print("Aborted.")
            return

    target_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, dest)
    print(f"Slotted: {path.name} -> {dest.relative_to(GRID_ROOT)}")

    # Run tests if Python
    if ext == ".py":
        print("\nRunning tests...")
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short", "-q"],
            cwd=str(GRID_ROOT),
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            print("All tests passed.")
        else:
            print("Test failures detected:")
            print(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
            print("\nContribution slotted but tests are failing — review needed.")

    # Remove from inbox
    path.unlink()
    print(f"Removed from inbox: {path.name}")


def _detect_target(filename: str, content: str, targets: dict, default: str) -> str:
    """Auto-detect target directory from filename patterns."""
    name_lower = filename.lower()

    # Python file detection
    if name_lower.startswith("test_"):
        return "tests" if "tests" in targets else default
    if "puller" in name_lower or "pull" in name_lower:
        return "ingestion" if "ingestion" in targets else default
    if "feature" in name_lower or "lab" in name_lower:
        return "features" if "features" in targets else default
    if "cluster" in name_lower or "regime" in name_lower or "orthog" in name_lower:
        return "discovery" if "discovery" in targets else default
    if "gate" in name_lower or "backtest" in name_lower:
        return "validation" if "validation" in targets else default
    if "resolver" in name_lower or "entity" in name_lower:
        return "normalization" if "normalization" in targets else default

    # JSX detection
    if "useStore" in content or "useState" in content:
        # Check if it looks like a view (full page) or component (reusable widget)
        if "api.get" in content or "useEffect" in content:
            return "views" if "views" in targets else default
        return "components" if "components" in targets else default

    return default


def cmd_inbox() -> None:
    """List pending inbox items."""
    items = [f for f in INBOX.iterdir() if f.name != ".gitkeep" and not f.name.startswith(".")]
    if not items:
        print("Inbox is empty.")
        print(f"Drop files into: {INBOX.relative_to(GRID_ROOT)}/")
        return

    print(f"Inbox ({len(items)} items):")
    for item in sorted(items):
        size = item.stat().st_size
        lines = len(item.read_text().splitlines()) if item.suffix in (".py", ".jsx", ".sql", ".md") else 0
        print(f"  {item.name}  ({lines} lines, {size} bytes)")


# ── Main ──────────────────────────────────────────────────────

def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "brief":
        target = sys.argv[2] if len(sys.argv) > 2 else "TEMPLATE"
        cmd_brief(target)
    elif cmd == "check":
        if len(sys.argv) < 3:
            print("Usage: integrate.py check <file>")
            sys.exit(1)
        cmd_check(sys.argv[2])
    elif cmd == "slot":
        if len(sys.argv) < 3:
            print("Usage: integrate.py slot <file>")
            sys.exit(1)
        cmd_slot(sys.argv[2])
    elif cmd == "inbox":
        cmd_inbox()
    else:
        print(f"Unknown command: {cmd}")
        print("Commands: brief, check, slot, inbox")
        sys.exit(1)


if __name__ == "__main__":
    main()
