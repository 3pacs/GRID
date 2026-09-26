"""Pure-Python fake store + SQLAlchemy-shaped engine for source-puller tests.

GRID pullers (see ``ingestion/base.py::BasePuller``) talk to the database
through three shapes only:

  * ``engine.connect()`` / ``engine.begin()`` -- context managers yielding a
    connection with ``.execute(text(sql), params)``.
  * ``conn.execute(...)`` returning something with ``.fetchone()`` /
    ``.fetchall()``.
  * The four SQL statements ``BasePuller`` and the altdata pullers issue:
    resolve/auto-create ``source_catalog``, dedup-check and bulk-fetch
    ``raw_series`` dates, and insert into ``raw_series``.

This module fakes exactly that surface against an in-memory
:class:`FakeRawSeriesStore`, so puller tests can assert on what would have
been written without a real Postgres connection (per the workstream's "no
database connections" boundary) and without hand-rolling a fresh
``unittest.mock`` object graph per test.

Not a general SQL engine: it recognizes the literal statement shapes the
pullers under test issue and nothing else. If a puller's SQL changes, this
fake's ``_dispatch`` needs a matching branch -- it will raise
``NotImplementedError`` rather than silently no-op, so a drifted assumption
fails loudly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any


@dataclass
class FakeRawSeriesStore:
    """In-memory stand-in for the ``source_catalog`` + ``raw_series`` tables.

    Attributes:
        rows: Every row a puller has inserted into ``raw_series``, in
            insertion order, as plain dicts with keys ``series_id``,
            ``source_id``, ``obs_date``, ``value``, ``raw_payload``,
            ``pull_status``, ``pull_timestamp``.
        source_catalog: name (lower-cased) -> source_catalog.id.
        execute_log: every SQL statement text seen, for assertions like
            "no metadata writes happened" (i.e. no INSERT INTO source_catalog
            beyond the initial resolve).
    """

    rows: list[dict[str, Any]] = field(default_factory=list)
    source_catalog: dict[str, int] = field(default_factory=dict)
    execute_log: list[str] = field(default_factory=list)
    _next_source_id: int = 1

    # -- source_catalog -----------------------------------------------------

    def resolve_source_id(self, name: str) -> int:
        key = name.lower()
        if key not in self.source_catalog:
            self.source_catalog[key] = self._next_source_id
            self._next_source_id += 1
        return self.source_catalog[key]

    # -- raw_series -----------------------------------------------------------

    def insert_raw(
        self,
        *,
        series_id: str,
        source_id: int,
        obs_date: date,
        value: float | None,
        raw_payload: str | None,
        pull_status: str,
    ) -> None:
        self.rows.append(
            {
                "series_id": series_id,
                "source_id": source_id,
                "obs_date": obs_date,
                "value": value,
                "raw_payload": raw_payload,
                "pull_status": pull_status,
                "pull_timestamp": datetime.now(timezone.utc),
            }
        )

    def row_exists(
        self,
        series_id: str,
        source_id: int,
        obs_date: date,
        cutoff: datetime,
    ) -> bool:
        return any(
            r["series_id"] == series_id
            and r["source_id"] == source_id
            and r["obs_date"] == obs_date
            and r["pull_timestamp"] >= cutoff
            for r in self.rows
        )

    def existing_dates(
        self,
        series_id: str,
        source_id: int,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> set[date]:
        out: set[date] = set()
        for r in self.rows:
            if r["series_id"] != series_id or r["source_id"] != source_id:
                continue
            if r["pull_status"] != "SUCCESS":
                continue
            od = r["obs_date"]
            if start_date is not None and od < start_date:
                continue
            if end_date is not None and od > end_date:
                continue
            out.add(od)
        return out

    def rows_for(self, series_id: str) -> list[dict[str, Any]]:
        return [r for r in self.rows if r["series_id"] == series_id]


class _FakeResult:
    def __init__(self, one: Any = None, many: list[Any] | None = None) -> None:
        self._one = one
        self._many = many if many is not None else []

    def fetchone(self) -> Any:
        return self._one

    def fetchall(self) -> list[Any]:
        return self._many


class FakeConnection:
    """Fakes a SQLAlchemy ``Connection`` against a :class:`FakeRawSeriesStore`."""

    def __init__(self, store: FakeRawSeriesStore) -> None:
        self.store = store

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        sql = str(stmt)
        self.store.execute_log.append(sql)
        params = params or {}

        if "SELECT id FROM source_catalog" in sql:
            sid = self.store.source_catalog.get(str(params["name"]).lower())
            return _FakeResult(one=(sid,) if sid is not None else None)

        if "INSERT INTO source_catalog" in sql:
            sid = self.store.resolve_source_id(params["name"])
            return _FakeResult(one=(sid,))

        if "SELECT DISTINCT obs_date FROM raw_series" in sql:
            dates = self.store.existing_dates(
                params["sid"],
                params["src"],
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
            )
            return _FakeResult(many=[(d,) for d in sorted(dates)])

        if "SELECT 1 FROM raw_series" in sql:
            exists = self.store.row_exists(
                params["sid"], params["src"], params["od"], params["ts"]
            )
            return _FakeResult(one=(1,) if exists else None)

        if "SELECT MAX(obs_date) FROM raw_series" in sql:
            dates = self.store.existing_dates(params["sid"], params["src"])
            return _FakeResult(one=(max(dates),) if dates else (None,))

        if "INSERT INTO raw_series" in sql:
            # Two call shapes hit this: BasePuller._insert_raw (status bound
            # as :status) and LMEWarehousePuller.save_to_db (literal
            # 'SUCCESS' baked into the SQL text, no :status param).
            status = params.get("status", "SUCCESS")
            self.store.insert_raw(
                series_id=params["sid"],
                source_id=params["src"],
                obs_date=params["od"],
                value=params.get("val"),
                raw_payload=params.get("payload"),
                pull_status=status,
            )
            return _FakeResult()

        raise NotImplementedError(f"FakeConnection cannot handle statement: {sql!r}")


class FakeEngine:
    """Fakes a SQLAlchemy ``Engine`` against a :class:`FakeRawSeriesStore`.

    Both ``connect()`` and ``begin()`` hand back the same
    :class:`FakeConnection` type -- the pullers under test never rely on
    ``begin()``'s commit/rollback semantics beyond "run these statements",
    and the fake store has no transaction concept to roll back.
    """

    def __init__(self, store: FakeRawSeriesStore | None = None) -> None:
        self.store = store if store is not None else FakeRawSeriesStore()

    def connect(self) -> FakeConnection:
        return FakeConnection(self.store)

    def begin(self) -> FakeConnection:
        return FakeConnection(self.store)
