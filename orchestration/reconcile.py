#!/usr/bin/env python3
"""
GRID Contribution Reconciler.

Reviews recently added files for style drift, security issues, and
PIT correctness violations. Run after slotting external contributions.

Usage:
    python orchestration/reconcile.py                  # check all recent changes
    python orchestration/reconcile.py --file path.py   # check specific file
    python orchestration/reconcile.py --since HEAD~3   # check last 3 commits
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

GRID_ROOT = Path(__file__).parent.parent


def get_changed_files(since: str = "HEAD~1") -> list[Path]:
    """Get files changed since a git ref."""
    result = subprocess.run(
        ["git", "diff", "--name-only", since, "HEAD"],
        cwd=str(GRID_ROOT),
        capture_output=True,
        text=True,
    )
    files = []
    for line in result.stdout.strip().splitlines():
        p = GRID_ROOT / line
        if p.exists() and p.suffix in (".py", ".jsx", ".sql"):
            files.append(p)
    return files


def audit_python(path: Path) -> list[str]:
    """Audit a Python file for GRID compliance."""
    content = path.read_text()
    issues: list[str] = []

    for i, line in enumerate(content.splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue

        # SQL injection
        if re.search(r'f["\'].*\b(SELECT|INSERT|UPDATE|DELETE)\b', line, re.IGNORECASE):
            issues.append(f"  L{i} [SECURITY] f-string SQL — use text() with bindparams")

        # PIT violation: raw table access without as_of
        if re.search(r'FROM\s+(raw_series|resolved_series)\b', line, re.IGNORECASE):
            # Check if as_of or obs_date filter exists nearby
            context = "\n".join(content.splitlines()[max(0, i-5):i+5])
            if "as_of" not in context and "obs_date" not in context:
                issues.append(f"  L{i} [PIT] querying raw/resolved_series without as_of filter")

        # Future data access
        if re.search(r'\bnow\(\)\b.*\+', line) or re.search(r'timedelta.*days.*\+', line):
            issues.append(f"  L{i} [PIT] possible future date construction — verify no lookahead")

        # NaN silencing
        if "errors='coerce'" in line or 'errors="coerce"' in line:
            if "log.warning" not in content.splitlines()[min(i, len(content.splitlines())-1)]:
                issues.append(f"  L{i} [DATA] pd.to_numeric(errors='coerce') without logging — silent NaN")

        # Bare except
        if re.match(r'\s*except\s*:', line):
            issues.append(f"  L{i} [STYLE] bare except — catch specific exceptions")

        # Missing type hints on public functions
        if re.match(r'\s*def [a-z]\w+\(', line) and '->' not in line:
            fn = re.search(r'def (\w+)', line)
            name = fn.group(1) if fn else ""
            if not name.startswith("_"):
                issues.append(f"  L{i} [STYLE] function '{name}' missing return type hint")

    return issues


def audit_jsx(path: Path) -> list[str]:
    """Audit a JSX file for GRID compliance."""
    content = path.read_text()
    issues: list[str] = []

    for i, line in enumerate(content.splitlines(), 1):
        # CSS imports
        if re.search(r"import\s+.*\.css", line):
            issues.append(f"  L{i} [STYLE] CSS import — use inline styles")

        # className without styles object
        if "className=" in line:
            if "const styles" not in content:
                issues.append(f"  L{i} [STYLE] className without styles object — use inline styles")
                break  # only flag once

        # Redux or Context API
        if "useContext" in line or "useDispatch" in line or "useSelector" in line:
            issues.append(f"  L{i} [STYLE] Redux/Context detected — use Zustand (useStore)")

        # Direct fetch instead of api.js
        if re.search(r'\bfetch\(', line) and "api.js" not in content[:500]:
            issues.append(f"  L{i} [STYLE] direct fetch() — use api.get/post from '../api.js'")
            break

    # Check for default export
    if "export default" not in content:
        issues.append("  [STYLE] no default export — components must use 'export default'")

    return issues


def audit_sql(path: Path) -> list[str]:
    """Audit a SQL file for safety."""
    content = path.read_text()
    issues: list[str] = []

    for i, line in enumerate(content.splitlines(), 1):
        if re.search(r'\bDROP\s+(TABLE|DATABASE|SCHEMA)\b', line, re.IGNORECASE):
            issues.append(f"  L{i} [SECURITY] DROP statement — requires manual review")
        if re.search(r'\bTRUNCATE\b', line, re.IGNORECASE):
            issues.append(f"  L{i} [SECURITY] TRUNCATE — requires manual review")
        if re.search(r'\bDELETE\s+FROM\b.*(?!WHERE)', line, re.IGNORECASE):
            if "WHERE" not in line.upper():
                issues.append(f"  L{i} [SECURITY] DELETE without WHERE — deletes all rows")

    return issues


AUDITORS = {
    ".py": audit_python,
    ".jsx": audit_jsx,
    ".sql": audit_sql,
}


def main() -> None:
    since = "HEAD~1"
    specific_file = None

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--since" and i + 1 < len(args):
            since = args[i + 1]
            i += 2
        elif args[i] == "--file" and i + 1 < len(args):
            specific_file = Path(args[i + 1])
            i += 2
        else:
            print(f"Unknown arg: {args[i]}")
            sys.exit(1)

    if specific_file:
        files = [specific_file] if specific_file.exists() else []
    else:
        files = get_changed_files(since)

    if not files:
        print("No files to reconcile.")
        return

    total_issues = 0
    for f in files:
        auditor = AUDITORS.get(f.suffix)
        if not auditor:
            continue

        issues = auditor(f)
        rel = f.relative_to(GRID_ROOT) if str(f).startswith(str(GRID_ROOT)) else f
        if issues:
            print(f"\n{rel} ({len(issues)} issues):")
            for issue in issues:
                print(issue)
            total_issues += len(issues)
        else:
            print(f"{rel}: clean")

    print(f"\n{'=' * 40}")
    print(f"Files checked: {len(files)}")
    print(f"Issues found:  {total_issues}")
    if total_issues == 0:
        print("All contributions pass reconciliation.")
    else:
        print("Review and fix issues before merging.")


if __name__ == "__main__":
    main()
