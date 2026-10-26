"""Fail if a tracked file hardcodes a database password.

Scans `git ls-files` for inline ``PGPASSWORD=<literal>`` assignments and
``postgres(ql)://user:<literal>@host`` connection strings. Allowed password
values are shell/env references (``$VAR``, ``"$VAR``, ``'$VAR``) and
placeholders enclosed in <>, {} or ${}.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

PGPASSWORD_RE = re.compile(r"""PGPASSWORD=(?!\$|"\$|'\$|\\"\$|\\\$|\.\.\.)\S""")
URL_RE = re.compile(r"postgres(?:ql)?(?:\+\w+)?://[\w.-]+:([^$@\s{<\[(][^@\s]*)@")

# Obvious dummy / redacted values used in tests, CI service containers and docs.
# Anything else in a connection string or PGPASSWORD assignment is treated as a
# real secret. Add to this set only for values that are verifiably not secrets.
PLACEHOLDER_PASSWORDS = {
    "testpass", "changeme", "p", "pass", "pw", "secret", "password", "postgres",
    "x", "grid", "abc123", "SuperSecretPass", "MyP4ssw0rd", "***",
}

SELF = "tests/test_no_hardcoded_db_credentials.py"
BINARY_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf", ".woff", ".woff2",
                   ".zip", ".gz", ".deb", ".parquet", ".pkl", ".db", ".sqlite"}


def _tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO, capture_output=True, check=True
    ).stdout.decode()
    return [f for f in out.split("\0") if f]


def _has_real_url_password(line: str) -> bool:
    return any(m.group(1) not in PLACEHOLDER_PASSWORDS for m in URL_RE.finditer(line))


def _scan() -> list[str]:
    hits = []
    for rel in _tracked_files():
        if rel == SELF or Path(rel).suffix.lower() in BINARY_SUFFIXES:
            continue
        path = REPO / rel
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            if PGPASSWORD_RE.search(line) or _has_real_url_password(line):
                hits.append(f"{rel}:{lineno}")  # location only: never echo the line
    return hits


def test_no_hardcoded_db_passwords_in_tracked_files():
    hits = _scan()
    assert not hits, "hardcoded DB credential patterns at: " + ", ".join(hits)


def test_scanner_flags_and_allows_expected_patterns():
    assert PGPASSWORD_RE.search("PGPASSWORD=Zq81xKd psql")
    assert PGPASSWORD_RE.search("export PGPASSWORD='Zq81xKd'")
    assert not PGPASSWORD_RE.search('PGPASSWORD="$GRID_DB_PASSWORD" psql')
    assert not PGPASSWORD_RE.search("PGPASSWORD=$GRID_DB_PASSWORD psql")
    assert not PGPASSWORD_RE.search("PGPASSWORD='$X' psql")
    assert _has_real_url_password("postgresql://grid:Zq81xKd@localhost:5432/db")
    assert _has_real_url_password("postgresql+psycopg2://grid:Zq81xKd@h/db")
    assert not _has_real_url_password("postgresql://grid:testpass@localhost/db")
    assert not _has_real_url_password("postgresql://grid:${GRID_DB_PASSWORD}@localhost/db")
    assert not _has_real_url_password("postgresql://grid@localhost/db")
    assert not _has_real_url_password("postgresql://user:$PW@host/db")
