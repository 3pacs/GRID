"""The only database access in this package: read-only, bounded, outside the backup window.

Guards, each enforced in code (not by convention):

* ``assert_db_window_open`` refuses to run between 03:30 and 10:30 UTC
  (the nightly ``pg_dump`` and its tail; see the GRID migration/backup note).
* Every session sets ``default_transaction_read_only=on``,
  ``statement_timeout=20s`` and ``lock_timeout=2s`` and the connection is
  checked to really be read-only before the first query.
* ``guard_sql`` rejects any statement that is not a single ``SELECT``/``WITH``
  or that names ``raw_series`` or a never-a-channel table.
* Queries are keyed on indexed columns: ``signal_sources`` by
  ``source_type`` (leading column of its unique index) plus a
  ``signal_date`` floor and a row ``LIMIT``.
"""

from __future__ import annotations

import re
from datetime import datetime, time, timezone
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from intelligence.people_events_pipeline.adapters import NEVER_A_CHANNEL, SIGNAL_SOURCE_COLUMNS, SOURCE_SPECS

BACKUP_WINDOW_UTC = (time(3, 30), time(10, 30))
STATEMENT_TIMEOUT_MS = 20_000
LOCK_TIMEOUT_MS = 2_000

_FORBIDDEN_TABLES = frozenset({"raw_series"}) | NEVER_A_CHANNEL
_WRITE_WORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|ALTER|CREATE|DROP|TRUNCATE|GRANT|REVOKE|COPY|VACUUM|ANALYZE|REFRESH|CALL|DO|LOCK)\b",
    re.IGNORECASE,
)


class WindowClosed(RuntimeError):
    pass


def assert_db_window_open(now: datetime | None = None) -> None:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    lo, hi = BACKUP_WINDOW_UTC
    if lo <= now.time() < hi:
        raise WindowClosed(
            f"refusing database reads at {now:%H:%M}Z: 03:30-10:30Z is the backup window"
        )


def guard_sql(sql: str) -> str:
    body = sql.strip().rstrip(";")
    if ";" in body:
        raise ValueError("one statement only")
    if not re.match(r"^\s*(SELECT|WITH)\b", body, re.IGNORECASE):
        raise ValueError("read-only module: only SELECT/WITH statements")
    if _WRITE_WORDS.search(body):
        raise ValueError("read-only module: statement contains a write keyword")
    words = {w.lower() for w in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", body)}
    hit = words & _FORBIDDEN_TABLES
    if hit:
        raise ValueError(f"read-only module: forbidden table(s) {sorted(hit)}")
    return body


def readonly_engine(url: str) -> Engine:
    options = (
        f"-c default_transaction_read_only=on -c statement_timeout={STATEMENT_TIMEOUT_MS} "
        f"-c lock_timeout={LOCK_TIMEOUT_MS} -c idle_in_transaction_session_timeout=60000 "
        "-c application_name=people_events_dry_run"
    )
    return create_engine(url, connect_args={"options": options}, pool_pre_ping=True, pool_size=1, max_overflow=0)


def _query(conn: Connection, sql: str, params: dict[str, Any]) -> pd.DataFrame:
    result = conn.execute(text(guard_sql(sql)), params)
    return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def _assert_read_only(conn: Connection) -> None:
    value = conn.execute(text("SHOW transaction_read_only")).scalar()
    if str(value).lower() != "on":
        raise RuntimeError("connection is not read-only; refusing to continue")


_SIGNAL_SOURCES_SQL = """
    SELECT id, source_type, source_id, ticker, signal_date, signal_type, signal_value, created_at
    FROM signal_sources
    WHERE source_type = :source_type AND signal_date >= :since
    ORDER BY id
    LIMIT :limit
"""
_SIGNAL_SOURCES_COUNT_SQL = """
    SELECT count(*) AS n, min(signal_date) AS min_date, max(signal_date) AS max_date
    FROM signal_sources WHERE source_type = :source_type
"""
_HOLDINGS_SQL = """
    SELECT id, cik, holder_name, ticker, cusip, shares_held, value_usd, report_date, filed_date, source, created_at
    FROM institutional_holdings
    WHERE report_date >= :since
    ORDER BY id
    LIMIT :limit
"""
_IDENTIFIERS_SQL = """
    SELECT entity_id, id_scheme, id_value, valid_from, valid_to, is_primary, conflict_flag
    FROM security_identifiers
    WHERE id_scheme IN ('ticker', 'cik')
"""
_MASTER_COUNT_SQL = "SELECT count(*) AS entities, count(cik) AS with_cik FROM security_master"
_PEOPLE_EVENTS_COUNT_SQL = """
    SELECT channel, count(*) AS n, count(security_id) AS with_security_id
    FROM people_events GROUP BY channel ORDER BY channel
"""
_PEOPLE_EVENTS_ROWS_SQL = """
    SELECT channel, dedup_key, known_at, known_at_basis, source_refs
    FROM people_events
    ORDER BY id
    LIMIT :limit
"""


def read_inputs(url: str, *, since: Any, limit_per_source: int, source_types: list[str] | None = None,
                holdings_since: Any = None, now: datetime | None = None) -> dict[str, Any]:
    """Everything the dry run needs from the database, in one read-only session."""
    assert_db_window_open(now)
    engine = readonly_engine(url)
    out: dict[str, Any] = {"source_counts": {}, "frames": {}}
    try:
        with engine.connect() as conn:
            _assert_read_only(conn)
            types = source_types or sorted(SOURCE_SPECS)
            frames = []
            for st in types:
                out["source_counts"][st] = _query(conn, _SIGNAL_SOURCES_COUNT_SQL, {"source_type": st}).iloc[0].to_dict()
                frames.append(_query(conn, _SIGNAL_SOURCES_SQL,
                                     {"source_type": st, "since": since, "limit": limit_per_source}))
            frames = [f for f in frames if not f.empty]
            out["frames"]["signal_sources"] = (pd.concat(frames, ignore_index=True) if frames
                                               else pd.DataFrame(columns=SIGNAL_SOURCE_COLUMNS))
            out["frames"]["institutional_holdings"] = _query(
                conn, _HOLDINGS_SQL, {"since": holdings_since or since, "limit": limit_per_source})
            out["frames"]["security_identifiers"] = _query(conn, _IDENTIFIERS_SQL, {})
            out["security_master"] = _query(conn, _MASTER_COUNT_SQL, {}).iloc[0].to_dict()
            out["people_events_counts"] = _query(conn, _PEOPLE_EVENTS_COUNT_SQL, {}).to_dict("records")
            out["frames"]["people_events"] = _query(conn, _PEOPLE_EVENTS_ROWS_SQL, {"limit": 500_000})
    finally:
        engine.dispose()
    return out
