"""Guard: no runtime module may claim production uses TimescaleDB/hypertables.

Production grid-svr is PostgreSQL 14.23 with **no TimescaleDB extension** — it is
not installed and not even listed in ``pg_available_extensions`` (verified on
griddb, 2026-09-14). ``raw_series`` is a plain, non-partitioned table of ~1.93
billion rows.

Eight source sites nonetheless justified their bounded ``obs_date`` predicates
with "so the planner prunes to the window's chunks". The bounds are correct and
must stay — they bound an index range, and in two cases they are a *staleness
rule*, not an optimisation — but the stated mechanism does not exist, and anyone
reasoning from it is reasoning about a database that isn't there. This test is
the thing that would have caught that.

Scope, deliberately narrow:

* It does not ban "hypertable"/"TimescaleDB" repo-wide. CI (the ``alien`` runner)
  and the docker-compose dev stack really do run TimescaleDB, and
  ``migrations/0037_options_v2_schema.sql`` calls ``create_hypertable()`` inside a
  DO block that degrades gracefully. Only *runtime* modules are scanned.
* A line that **denies** the claim is fine — that is the correction, not the bug.
* It does not police the word "chunk". It is overloaded here:
  ``normalization/resolver.py`` uses "chunk" for an application-level backfill
  batch, and its comments about chunk width are already correct about a plain
  table. Banning the word flagged that correct code (and this file's own
  corrections) as violations.

See CLAUDE.md, "Database environments (they are NOT the same)".
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

#: Runtime trees whose comments describe the production database.
RUNTIME_DIRS = (
    "analysis", "api", "discovery", "features", "governance", "inference",
    "ingestion", "intelligence", "journal", "llm", "normalization", "oracle",
    "physics", "scripts", "store", "trading", "validation",
)

#: A runtime mention of TimescaleDB semantics at all. The regex requires the
#: "DB" so the ordinary English word "timescale" is not a claim -- physics and
#: backtest modules legitimately discuss "the characteristic timescale" and
#: "lift at each timescale".
CLAIM = re.compile(r"\bhypertables?\b|\btimescaledb\b", re.I)

#: Files that may name the product without asserting anything about production.
ALLOWED = {
    # A vocabulary map for Obsidian backlinks; the vault has a TimescaleDB note.
    "scripts/obsidian_backlinks.py",
}

#: ...unless the line denies it. These are corrections, not regressions.
DENIAL = re.compile(
    r"\bno\b|\bnot\b|\bnever\b|\bwithout\b|\bn't\b|\babsent\b|\bisn\b|"
    r"\bnone\b|\bonly\b|\brather than\b|\binstead\b",
    re.I,
)


def _runtime_sources() -> list[Path]:
    files: list[Path] = []
    for d in RUNTIME_DIRS:
        root = REPO / d
        if root.is_dir():
            files.extend(p for p in root.rglob("*.py") if "__pycache__" not in p.parts)
    assert files, "no runtime sources found — check RUNTIME_DIRS"
    return files


@pytest.mark.parametrize("path", _runtime_sources(), ids=lambda p: str(p.relative_to(REPO)))
def test_no_production_hypertable_claim(path: Path) -> None:
    rel_path = str(path.relative_to(REPO))
    if rel_path in ALLOWED:
        pytest.skip(f"{rel_path} is an allowed non-claim mention")
    text = path.read_text(encoding="utf-8", errors="replace")
    for line_no, line in enumerate(text.splitlines(), start=1):
        if CLAIM.search(line) and not DENIAL.search(line):
            rel = path.relative_to(REPO)
            pytest.fail(
                f"{rel}:{line_no} asserts TimescaleDB/hypertable behaviour that "
                f"production does not have (PG 14.23, extension not installed and "
                f"not available; raw_series is a plain ~1.93e9-row table):\n"
                f"    {line.strip()}\n"
                f"Keep any bounded predicate — fix the stated reason for it. "
                f"See CLAUDE.md 'Database environments'."
            )


def test_claude_md_documents_the_real_production_database() -> None:
    """CLAUDE.md must not re-advertise PG15 + TimescaleDB as the backend."""
    text = (REPO / "CLAUDE.md").read_text(encoding="utf-8")
    assert "Database environments (they are NOT the same)" in text, (
        "CLAUDE.md lost the section distinguishing production / CI / dev databases"
    )
    assert "PostgreSQL 14.23" in text, "CLAUDE.md no longer states the production PG version"
    stack = text.split("## Tech Stack", 1)[1].split("\n- **Frontend:", 1)[0]
    assert "15 + [[TimescaleDB]]" not in stack, (
        "Tech Stack again claims PostgreSQL 15 + TimescaleDB; production is PG 14.23 "
        "with no TimescaleDB"
    )
