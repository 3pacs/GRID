#!/usr/bin/env python3
"""audit_literal_secrets.py — find API keys/tokens pasted straight into source.

The secrets rule (``.claude/rules/security.md``) says every secret comes from
the environment via ``config.py``. This script guards the failure mode that
rule cannot see on its own: a real key typed into a script, a Dockerfile
``ENV`` line, a prose ``Key: ...`` line in a prompt, or an
``os.getenv(name, "<real key>")`` default. Five such keys (Alpha Vantage,
NewsAPI, EIA, NOAA, FRED) sat in ``scripts/`` and ``tasks/`` from the first
commit until the 2026-09 cleanup.

Usage::

    python -m scripts.audit_literal_secrets              # scan every tracked file
    python -m scripts.audit_literal_secrets scripts/     # scan a subtree or files
    python -m scripts.audit_literal_secrets --quiet      # exit code only

Exit code 1 when anything is found. ``tests/test_no_literal_secrets.py`` runs
the same scan, so CI fails on a newly committed literal.

The matcher is deliberately narrow so it stays quiet on ordinary code:

* the value must be 16+ alphanumerics mixing at least two of lower / upper /
  digit (placeholders like ``your_key_here``, hyphenated test values, and
  snake_case constants never qualify);
* it must sit in a key-shaped slot — a ``*KEY / *TOKEN / *SECRET / *PASSWORD``
  assignment (Python, shell, Dockerfile ``ENV``, YAML, prose ``Key:``), an
  ``apikey= / api_key= / token=`` query-string parameter, or the default
  argument of ``os.getenv`` / ``os.environ.get``.

Findings never print the full value — only a 4-character prefix and the
length — so running the audit does not itself copy a secret into a log.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import NamedTuple

REPO_ROOT = Path(__file__).resolve().parent.parent

# Directories that are generated, vendored, or data — never source we own.
_SKIP_DIR_PARTS = frozenset(
    {
        ".git",
        "node_modules",
        "pwa_dist",
        "dist",
        "build",
        "outputs",
        "output",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "venv",
    }
)
_MAX_FILE_BYTES = 2_000_000

_VALUE = r"([A-Za-z0-9]{16,})"
_PATTERNS: tuple[re.Pattern[str], ...] = (
    # NAME_KEY = 'literal' | export X_TOKEN="literal" | ENV X_KEY=literal | Key: literal
    re.compile(
        r"(?:^|[\s(,])(?:export\s+|ENV\s+)?[A-Za-z0-9_]*(?:KEY|TOKEN|SECRET|PASSWORD)"
        r"\s*[=:]\s*[\"']?" + _VALUE + r"[\"']?(?=[\s,;)]|$)",
        re.IGNORECASE,
    ),
    # ?apikey=literal | &api_key=literal | token=literal inside a URL
    re.compile(r"(?:apikey|api_key|token)=" + _VALUE + r"(?=[&\"'\s]|$)", re.IGNORECASE),
    # os.getenv("X", "literal") | os.environ.get("X", "literal") — a real key as the default
    re.compile(
        r"(?:getenv|environ\.get)\(\s*[\"'][A-Za-z0-9_]+[\"']\s*,\s*[\"']" + _VALUE + r"[\"']\s*\)"
    ),
)
_CLASS_PROBES = (re.compile(r"[a-z]"), re.compile(r"[A-Z]"), re.compile(r"[0-9]"))


class Finding(NamedTuple):
    """One suspected literal secret; ``preview`` is redacted on purpose."""

    path: str
    line_no: int
    preview: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line_no}: literal secret {self.preview}"


def looks_like_secret(value: str) -> bool:
    """Return True when ``value`` mixes at least two of lower / upper / digit."""
    return sum(bool(probe.search(value)) for probe in _CLASS_PROBES) >= 2


def _redact(value: str) -> str:
    return f"{value[:4]}…({len(value)} chars)"


def find_literal_secrets(text: str, path: str = "<text>") -> list[Finding]:
    """Scan ``text`` line by line and return every key-shaped literal."""
    findings: list[Finding] = []
    for line_no, line in enumerate(text.splitlines(), 1):
        seen: set[str] = set()
        for pattern in _PATTERNS:
            for match in pattern.finditer(line):
                value = match.group(1)
                if value in seen or not looks_like_secret(value):
                    continue
                seen.add(value)
                findings.append(Finding(path, line_no, _redact(value)))
    return findings


def _tracked_files(root: Path) -> list[Path] | None:
    try:
        out = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"],
            capture_output=True,
            check=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return None
    return [root / p for p in out.split("\0") if p]


def iter_scan_files(root: Path = REPO_ROOT) -> Iterator[Path]:
    """Yield the files worth scanning: git-tracked when possible, else a walk."""
    files = _tracked_files(root)
    if files is None:
        files = [p for p in root.rglob("*") if p.is_file()]
    for path in files:
        rel = path.relative_to(root)
        if _SKIP_DIR_PARTS.intersection(rel.parts[:-1]) or not path.is_file():
            continue
        try:
            if path.stat().st_size > _MAX_FILE_BYTES:
                continue
        except OSError:
            continue
        yield path


def scan_paths(paths: Iterable[Path], root: Path = REPO_ROOT) -> list[Finding]:
    """Scan the given files and return findings with repo-relative paths."""
    findings: list[Finding] = []
    for path in paths:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        try:
            rel = str(path.relative_to(root))
        except ValueError:
            rel = str(path)
        findings.extend(find_literal_secrets(text, rel))
    return findings


def scan_repo(root: Path = REPO_ROOT) -> list[Finding]:
    """Scan every tracked, non-generated file under ``root``."""
    return scan_paths(iter_scan_files(root), root)


def _expand(args: list[str], root: Path) -> list[Path]:
    out: list[Path] = []
    for arg in args:
        target = Path(arg)
        if not target.is_absolute():
            target = root / target
        if target.is_dir():
            out.extend(f for f in iter_scan_files(root) if f.is_relative_to(target))
        else:
            out.append(target)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Find API keys/tokens pasted into source.")
    parser.add_argument(
        "paths", nargs="*", help="files or directories to scan (default: all tracked files)"
    )
    parser.add_argument("--quiet", action="store_true", help="no output, exit code only")
    ns = parser.parse_args(argv)

    files = _expand(ns.paths, REPO_ROOT) if ns.paths else iter_scan_files(REPO_ROOT)
    findings = scan_paths(files, REPO_ROOT)
    if not ns.quiet:
        for finding in findings:
            print(finding)
        print(f"{len(findings)} literal secret(s) found" if findings else "no literal secrets found")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
