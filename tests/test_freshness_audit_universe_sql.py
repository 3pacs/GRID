"""scripts/freshness_audit_universe.sql ran on grid-svr for months as an
untracked, unreviewed file (deployed from
/data/grid_v4/astrogrid_dedup/scripts/), doing DROP TABLE + CREATE TABLE
on every run with no explicit transaction. That caused two separate,
confirmed incidents on 2026-09-17: an earlier manual GRANT to the `grid`
role on data_freshness_audit was silently lost the next time this job
ran (DROP recreates the table with no grants), and the DROP itself
queued for 2h52m behind a concurrent pg_dump backup's ACCESS SHARE lock
— blocking every other reader of the table, including
scripts/td_backfill_universe.py's daily run — until the stuck session
was cancelled directly (pg_cancel_backend, not this file).

These are static checks on the committed SQL text — there is no local
Postgres to execute it against from this dev box. They exist so the
specific structural properties the incident depended on (an explicit
transaction, bounded lock waits, durable grants, no DROP) are visible
in review and can't silently regress, not to prove runtime correctness.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "scripts" / "freshness_audit_universe.sql"


def _text() -> str:
    return SQL.read_text(encoding="utf-8")


def _sql_only() -> str:
    """The file's text with `-- ...` line comments stripped, so a check
    against actual executable SQL can't accidentally match prose in a
    comment instead (this file's own header comments narrate the exact
    "DROP TABLE" and "GRANT ... TO grid" phrases these checks look for,
    describing what the *old*, broken version did).
    """
    return "\n".join(
        line for line in _text().splitlines() if not line.strip().startswith("--")
    )


def test_file_exists_and_is_tracked() -> None:
    assert SQL.is_file()


def test_no_drop_table() -> None:
    """The root cause of both incidents: DROP TABLE needs an ACCESS
    EXCLUSIVE lock (hangs behind any concurrent reader, including a
    multi-hour backup) and recreates the object with no grants.
    """
    assert not re.search(r"\bDROP\s+TABLE\b", _sql_only(), re.IGNORECASE)


def test_refresh_uses_delete_not_drop() -> None:
    src = _sql_only()
    assert "DELETE FROM data_freshness_audit" in src
    assert "DELETE FROM data_freshness_universe" in src


def test_table_creation_is_idempotent_not_destructive() -> None:
    src = _sql_only()
    assert "CREATE TABLE IF NOT EXISTS data_freshness_audit" in src
    assert "CREATE TABLE IF NOT EXISTS data_freshness_universe" in src


def test_whole_refresh_is_one_transaction() -> None:
    """A reader must never see a committed-empty intermediate table, and
    a failed INSERT must not leave the DELETE committed on its own —
    both require the DELETE and the replacement INSERT to be in the same
    transaction as each other.
    """
    src = _sql_only()
    begin_idx = src.index("BEGIN;")
    commit_idx = src.index("COMMIT;")
    assert begin_idx < commit_idx

    for needle in (
        "DELETE FROM data_freshness_universe",
        "INSERT INTO data_freshness_universe",
        "DELETE FROM data_freshness_audit",
        "INSERT INTO data_freshness_audit",
    ):
        pos = src.index(needle)
        assert begin_idx < pos < commit_idx, f"{needle!r} must be inside the BEGIN/COMMIT block"


def test_error_stop_is_set_in_the_file_itself() -> None:
    """Must not rely solely on the caller remembering -v ON_ERROR_STOP=1
    — a failed statement without it would just print an error and keep
    going, reaching GRANT/COMMIT/the summary SELECT against a dataset
    that never actually finished refreshing.
    """
    assert "\\set ON_ERROR_STOP on" in _sql_only()


def test_lock_and_statement_timeouts_are_bounded() -> None:
    """The incident this fixes was an unbounded wait — the old script
    had `SET statement_timeout = 0` (no bound at all) and nothing
    governing lock-acquisition wait time specifically.
    """
    src = _sql_only()
    assert "SET statement_timeout = 0" not in src
    assert re.search(r"SET\s+lock_timeout\s*=", src, re.IGNORECASE)
    assert re.search(r"SET\s+statement_timeout\s*=\s*'?\d", src, re.IGNORECASE)


def test_grants_are_present_inside_the_transaction_after_create() -> None:
    """Grants must be re-applied every run (not assumed to survive from
    a table that no longer gets recreated) and must be inside the same
    transaction as everything else, so a failed refresh can't leave a
    table that exists with grants but no data, or vice versa.
    """
    src = _sql_only()
    begin_idx = src.index("BEGIN;")
    commit_idx = src.index("COMMIT;")

    for table in ("data_freshness_audit", "data_freshness_universe"):
        create_idx = src.index(f"CREATE TABLE IF NOT EXISTS {table}")
        grant_match = re.search(
            rf"GRANT\s+[^;]*ON\s+{table}\s+TO\s+grid", src, re.IGNORECASE
        )
        assert grant_match, f"missing GRANT for {table}"
        grant_idx = grant_match.start()
        assert begin_idx < create_idx < grant_idx < commit_idx
