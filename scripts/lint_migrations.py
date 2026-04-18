#!/usr/bin/env python3
"""Lint GRID SQL migrations for the GRANT footer.

Every migration in `migrations/*.sql` that creates a new table must include
a matching `GRANT ALL ON <table> TO grid;` (and ideally
`GRANT USAGE, SELECT ON SEQUENCE <table>_<col>_seq TO grid;` for SERIAL
primary keys). Without grants, the unprivileged `grid` role used by the API
and ingestors will see `permission denied for table X` the first time it
tries to read the table.

This script:
  * Scans every `migrations/*.sql` file (skipping `_TEMPLATE.sql`).
  * Extracts every `CREATE TABLE [IF NOT EXISTS] <name>` statement.
  * Checks each table has a matching `GRANT ALL ON <name> TO grid` somewhere
    in the same file.
  * Also flags SERIAL / BIGSERIAL primary keys whose sequence is not granted
    (warning, not failure, because some migrations don't use SERIAL).

Exit code 0 means clean. Exit code 1 means at least one violation.

Usage::

    python3 scripts/lint_migrations.py
    python3 scripts/lint_migrations.py --migrations migrations/

Wire it into pre-commit / CI to keep new migrations from regressing.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MIGRATIONS = REPO_ROOT / "migrations"

# Match `CREATE TABLE [IF NOT EXISTS] [schema.]name (`
# Captures schema (optional, group 1) and table name (group 2).
CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s*\(",
    re.IGNORECASE,
)

# Match `GRANT ALL ON [schema.]name TO grid` (also accepts ALL PRIVILEGES).
GRANT_ALL_RE = re.compile(
    r"GRANT\s+(?:ALL(?:\s+PRIVILEGES)?)\s+ON\s+(?:TABLE\s+)?(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s+TO\s+grid\b",
    re.IGNORECASE,
)

# Match SERIAL / BIGSERIAL primary key declarations (used to detect implicit sequences).
SERIAL_PK_RE = re.compile(
    r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:BIG)?SERIAL\b",
    re.IGNORECASE,
)

# Match `GRANT USAGE, SELECT ON SEQUENCE <name> TO grid`.
GRANT_SEQ_RE = re.compile(
    r"GRANT\s+(?:USAGE|SELECT|USAGE\s*,\s*SELECT|SELECT\s*,\s*USAGE)\s+ON\s+SEQUENCE\s+(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s+TO\s+grid\b",
    re.IGNORECASE,
)


def strip_sql_comments(sql: str) -> str:
    """Remove `--` line comments and `/* */` block comments so the regexes
    don't match commented-out CREATE TABLEs in the docstring footer."""
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", "", sql)
    return sql


def find_create_tables(sql: str) -> list[tuple[str | None, str]]:
    """Return list of (schema, table) tuples for CREATE TABLE statements."""
    return [(m.group(1), m.group(2)) for m in CREATE_TABLE_RE.finditer(sql)]


def find_granted_tables(sql: str) -> set[tuple[str | None, str]]:
    return {(m.group(1), m.group(2)) for m in GRANT_ALL_RE.finditer(sql)}


def find_granted_sequences(sql: str) -> set[str]:
    return {m.group(2).lower() for m in GRANT_SEQ_RE.finditer(sql)}


def find_serial_columns(sql: str) -> list[tuple[str, str]]:
    """Return (table, column) for every SERIAL/BIGSERIAL column.

    This is approximate: it finds the nearest preceding CREATE TABLE name
    for each SERIAL declaration so we can predict the auto-created sequence
    name `<table>_<column>_seq`.
    """
    results: list[tuple[str, str]] = []
    for create_match in CREATE_TABLE_RE.finditer(sql):
        table = create_match.group(2)
        # Find the matching closing paren for this CREATE TABLE.
        start = create_match.end() - 1  # The "(" itself.
        depth = 0
        end = start
        for i in range(start, len(sql)):
            ch = sql[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        body = sql[start + 1 : end]
        for serial_match in SERIAL_PK_RE.finditer(body):
            col = serial_match.group(1)
            results.append((table, col))
    return results


def lint_file(path: Path) -> list[str]:
    """Return a list of violation messages for one migration file."""
    raw = path.read_text(encoding="utf-8", errors="replace")
    sql = strip_sql_comments(raw)
    violations: list[str] = []

    creates = find_create_tables(sql)
    grants = find_granted_tables(sql)
    granted_lower = {(s.lower() if s else None, t.lower()) for (s, t) in grants}

    for schema, table in creates:
        key = (schema.lower() if schema else None, table.lower())
        # Also accept a grant that omits the schema qualifier (or vice versa).
        unqualified_key = (None, table.lower())
        if key not in granted_lower and unqualified_key not in granted_lower:
            qualified = f"{schema}.{table}" if schema else table
            violations.append(
                f"missing `GRANT ALL ON {qualified} TO grid;` for table `{qualified}`"
            )

    granted_sequences = find_granted_sequences(sql)
    for table, col in find_serial_columns(sql):
        seq_name = f"{table}_{col}_seq".lower()
        if seq_name not in granted_sequences:
            violations.append(
                f"missing `GRANT USAGE, SELECT ON SEQUENCE {seq_name} TO grid;` "
                f"for SERIAL column `{table}.{col}`"
            )

    return violations


def iter_migration_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    return sorted(p for p in root.glob("*.sql") if p.name != "_TEMPLATE.sql")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--migrations",
        type=Path,
        default=DEFAULT_MIGRATIONS,
        help="Path to the migrations directory (default: %(default)s)",
    )
    args = parser.parse_args(argv)

    files = list(iter_migration_files(args.migrations))
    if not files:
        print(f"lint_migrations: no .sql files found under {args.migrations}")
        return 0

    total_violations = 0
    clean_files = 0
    for path in files:
        violations = lint_file(path)
        if violations:
            total_violations += len(violations)
            print(f"FAIL {path.relative_to(REPO_ROOT)}")
            for v in violations:
                print(f"     - {v}")
        else:
            clean_files += 1
            print(f"OK   {path.relative_to(REPO_ROOT)}")

    print()
    print(
        f"lint_migrations: {clean_files}/{len(files)} files clean, "
        f"{total_violations} violation(s)"
    )
    return 1 if total_violations else 0


if __name__ == "__main__":
    sys.exit(main())
