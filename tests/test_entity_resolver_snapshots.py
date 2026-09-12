"""``intelligence/entity_resolver.py`` must read the real analytical_snapshots.

The DB-backed :class:`EntityResolver` queried ``analytical_snapshots`` for
``actor``, ``title`` and ``source_id`` as columns. The table has exactly
``id, snapshot_date, category, subcategory, as_of_date, payload, metrics,
created_at`` (plus ``search_vector``, added by the FTS migration) and has
never had any of those three — they came from a rival ``CREATE TABLE`` in
``scripts/parse_datasets.py`` that #477 deleted.

So both call sites raised ``psycopg2.errors.UndefinedColumn`` on *every*
call, and both handlers logged it at ``log.warning`` ("Could not scan
snapshots"). Actor resolution skipped the entire 556k-row snapshot corpus and
``build_resolution_index`` reported zero snapshot names, with nothing in
``.server-logs/errors.jsonl`` to say why.

Two things are pinned here:

* the SQL runs against the canonical columns and extracts the descriptive
  fields from the jsonb ``payload`` — exercised by *executing* it, not by
  pattern-matching the string, so a reference to a column the table lacks
  fails the test the way it fails production; and
* a query naming a missing column is logged at ``log.error``. CLAUDE.md
  reserves that level for unhandled application bugs, and SQL that cannot
  succeed on any retry is one. Downgrading it to a warning is what hid this
  fault; an undefined *table* stays a warning, because an optional corpus
  that was never loaded is a legitimate runtime state.

SQLite is used as the executable harness. GRID itself is PostgreSQL-only
(``store/pit.py``'s ``DISTINCT ON`` sees to that) — SQLite appears here
solely because it shares the ``->>`` operator and rejects unknown column
names, which is exactly the property under test.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pytest
from loguru import logger
from sqlalchemy import create_engine, text

from intelligence.entity_resolver import (
    SNAPSHOT_NAME_SCAN_SQL,
    SNAPSHOT_SEARCH_SQL,
    EntityResolver,
    _is_schema_fault,
    _log_query_failure,
    canonical_key,
    normalize_name,
)
from store.snapshots import ANALYTICAL_SNAPSHOTS_DDL

REPO_ROOT = Path(__file__).resolve().parents[1]

# Columns the phantom DDL declared and the real table has never had. If any of
# these is ever added for real, the DDL-derived check below stops flagging it.
PHANTOM_COLUMNS = (
    "actor", "ticker", "title", "summary", "data", "confidence", "source_id",
)


# ---------------------------------------------------------------------------
# Canonical schema, mirrored into SQLite
# ---------------------------------------------------------------------------

def _declared_columns(ddl: str) -> list[str]:
    """Column names declared by a ``CREATE TABLE`` statement, in order."""
    body = ddl.split("(", 1)[1].rsplit(")", 1)[0]
    columns = []
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if line:
            columns.append(line.split()[0].lower())
    return columns


def _sqlite_ddl() -> str:
    """Translate the canonical DDL to SQLite, keeping every column name.

    Only the *types* are rewritten. ``test_sqlite_mirror_matches_canonical_ddl``
    fails if this ever drops or renames a column, so the harness cannot drift
    away from the table the queries actually run against.
    """
    return (
        ANALYTICAL_SNAPSHOTS_DDL
        .replace("BIGSERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
        .replace("JSONB", "TEXT")
        .replace("TIMESTAMPTZ NOT NULL DEFAULT NOW()", "TEXT NOT NULL DEFAULT ''")
    )


def _load_parse_datasets() -> ModuleType:
    """Import ``scripts/parse_datasets.py``, stubbing ``db`` if psycopg2 is absent.

    The fixtures below build their rows with that script's own
    ``_snapshot_row``, so the reader under test is pinned to the writer: if
    the payload keys are renamed on one side, these tests fail.
    """
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    try:
        import db  # noqa: F401
    except Exception:  # pragma: no cover - environment-dependent
        stub = ModuleType("db")
        stub.get_engine = lambda *a, **k: None  # type: ignore[attr-defined]
        sys.modules["db"] = stub

    spec = importlib.util.spec_from_file_location(
        "parse_datasets_for_resolver_test", REPO_ROOT / "scripts" / "parse_datasets.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# The records parse_datasets builds for a Senate trade and a Fed speech, in
# the flat shape its source datasets have. _snapshot_row folds them into the
# canonical columns.
SENATE_TRADE = {
    "category": "congressional_trade",
    "snapshot_date": date(2021, 2, 10),
    "actor": "David A Perdue , Jr",
    "ticker": "GOOGL",
    "title": "David A Perdue , Jr — Purchase",
    "summary": "Purchase GOOGL ($1,001 - $15,000) by self",
    "data": json.dumps({"chamber": "senate", "type": "Purchase"}),
    "confidence": "confirmed",
    "source_id": "senate_efds",
}

HOUSE_TRADE = {
    "category": "congressional_trade",
    "snapshot_date": date(2021, 3, 2),
    "actor": "Thomas R Carper",
    "ticker": "MSFT",
    "title": "Thomas R Carper — Sale",
    "summary": "Sale MSFT ($15,001 - $50,000)",
    "data": json.dumps({"chamber": "house"}),
    "confidence": "confirmed",
    "source_id": "house_disclosures",
}

FED_SPEECH = {
    "category": "fed_speech",
    "snapshot_date": date(2022, 8, 26),
    "actor": "Jerome H Powell",
    "ticker": None,
    "title": "Monetary Policy and Price Stability",
    "summary": "Jerome H Powell at Jackson Hole",
    "data": json.dumps({"location": "Jackson Hole"}),
    "confidence": "confirmed",
    "source_id": "fed_speeches",
}

# A speech whose scraped speaker field was empty — parse_datasets writes
# speech.get("s", ""), so this reaches the table as an empty actor string.
UNNAMED_SPEECH = {
    "category": "fed_speech",
    "snapshot_date": date(2022, 9, 1),
    "actor": "",
    "ticker": None,
    "title": "Opening Remarks",
    "summary": "",
    "data": json.dumps({}),
    "confidence": "confirmed",
    "source_id": "fed_speeches",
}

# A congressional_trade row in the shape griddb actually holds: the raw Senate
# EFD record, written before #477 fixed parse_datasets, with the actor under
# `senator` and the name repeated in subcategory. 5,000 of these are live.
LEGACY_SENATE_ROW = {
    "snapshot_date": date(2021, 2, 16),
    "category": "congressional_trade",
    "subcategory": "Thomas H Tuberville",
    "as_of_date": date(2021, 2, 16),
    "payload": json.dumps({
        "senator": "Thomas H Tuberville",
        "ticker": "AAPL",
        "type": "Purchase",
        "amount": "$15,001 - $50,000",
        "transaction_date": "02/16/2021",
        "owner": "Self",
        "asset_description": "Apple Inc",
        "asset_type": "Stock",
        "comment": "--",
        "ptr_link": "https://efdsearch.senate.gov/search/view/ptr/",
    }),
}

# An OpenSanctions FollowTheMoney entity, the shape of the 12,282
# category='opensanctions' rows. Its `name` key is deliberately NOT read —
# see SNAPSHOT_SEARCH_SQL.
OPENSANCTIONS_ROW = {
    "snapshot_date": date(2024, 1, 5),
    "category": "opensanctions",
    "subcategory": "person",
    "as_of_date": date(2024, 1, 5),
    "payload": json.dumps({
        "schema": "Person",
        "id": "NK-abc123",
        "name": "Perdue, David Alfred",
        "topics": ["role.pep"],
        "countries": ["us"],
    }),
}

# An analytical snapshot with no actor at all — the overwhelming majority of
# the live table (clustering, llm_task_*, sector_flows).
CLUSTERING_SNAPSHOT = {
    "snapshot_date": date(2026, 9, 11),
    "category": "clustering",
    "subcategory": None,
    "as_of_date": date(2026, 9, 11),
    "payload": json.dumps({"best_k": 4, "n_observations": 5000}),
}


@pytest.fixture
def snapshot_engine():
    """In-memory database holding the canonical table, seeded via parse_datasets."""
    parse_datasets = _load_parse_datasets()
    row_of = parse_datasets.DatasetParser._snapshot_row

    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text(_sqlite_ddl()))
        for record in (SENATE_TRADE, HOUSE_TRADE, FED_SPEECH, UNNAMED_SPEECH):
            row = dict(row_of(record))
            row["created_at"] = "2026-09-12T00:00:00Z"
            conn.execute(
                text(
                    "INSERT INTO analytical_snapshots "
                    "(snapshot_date, category, subcategory, as_of_date, "
                    " payload, created_at) "
                    "VALUES (:snapshot_date, :category, :subcategory, "
                    "        :as_of_date, :payload, :created_at)"
                ),
                row,
            )
        for raw in (CLUSTERING_SNAPSHOT, LEGACY_SENATE_ROW, OPENSANCTIONS_ROW):
            conn.execute(
                text(
                    "INSERT INTO analytical_snapshots "
                    "(snapshot_date, category, subcategory, as_of_date, "
                    " payload, created_at) "
                    "VALUES (:snapshot_date, :category, :subcategory, "
                    "        :as_of_date, :payload, :created_at)"
                ),
                {**raw, "created_at": "2026-09-12T00:00:00Z"},
            )
    return engine


@pytest.fixture
def resolver(snapshot_engine):
    """An EntityResolver bound to the seeded database.

    Built without ``__init__`` on purpose: ``_ensure_tables`` creates
    ``entity_resolution`` with PostgreSQL-only DDL (``TIMESTAMPTZ DEFAULT
    NOW()``), which is irrelevant to the queries under test and unsupported by
    the harness.
    """
    instance = EntityResolver.__new__(EntityResolver)
    instance.engine = snapshot_engine
    return instance


def _search(resolver, name: str):
    """Call ``_search_snapshots`` the way ``resolve()`` does."""
    normalized = normalize_name(name)
    return resolver._search_snapshots(normalized, canonical_key(normalized), "person")


# ---------------------------------------------------------------------------
# The harness itself
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_sqlite_mirror_matches_canonical_ddl():
    """The test table declares exactly the columns the real one does."""
    assert _declared_columns(_sqlite_ddl()) == _declared_columns(
        ANALYTICAL_SNAPSHOTS_DDL
    )


@pytest.mark.unit
def test_canonical_table_has_none_of_the_phantom_columns():
    """Guard the premise: if a phantom name ever becomes real, revisit this file."""
    declared = set(_declared_columns(ANALYTICAL_SNAPSHOTS_DDL))
    assert declared.isdisjoint(PHANTOM_COLUMNS)


# ---------------------------------------------------------------------------
# The statements run against the real columns
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_search_sql_executes_against_the_canonical_table(snapshot_engine):
    """The exact statement production runs must not name a missing column.

    This is the test that would have caught the bug: the pre-fix SELECT
    (``id, category, actor, snapshot_date, title, source_id, created_at``)
    fails here with the same "no such column: actor" it raised on griddb.
    """
    with snapshot_engine.connect() as conn:
        rows = conn.execute(
            text(SNAPSHOT_SEARCH_SQL),
            {"name": "David Perdue", "pattern": "%david%"},
        ).fetchall()
    assert rows, "the seeded Perdue trade should match"


@pytest.mark.unit
def test_name_scan_sql_executes_against_the_canonical_table(snapshot_engine):
    """The index-build scan must run, and must find the payload actors."""
    with snapshot_engine.connect() as conn:
        names = {row[0] for row in conn.execute(text(SNAPSHOT_NAME_SCAN_SQL))}
    assert names == {
        "David A Perdue , Jr",      # payload ->> 'actor', post-#477 writer
        "Thomas R Carper",
        "Jerome H Powell",
        "Thomas H Tuberville",      # payload ->> 'senator', the live shape
    }
    # The OpenSanctions `name` key is deliberately not an actor source.
    assert "Perdue, David Alfred" not in names


@pytest.mark.unit
@pytest.mark.parametrize("sql", [SNAPSHOT_SEARCH_SQL, SNAPSHOT_NAME_SCAN_SQL])
def test_statements_reference_no_phantom_column(sql):
    """No phantom identifier appears outside a JSON key literal.

    Complements the execution tests: SQLite would also accept
    ``payload ->> 'actor' AS actor``, and an alias that shadows a phantom
    column name is exactly the kind of thing that makes the next reader
    believe the column exists.
    """
    import re

    without_literals = re.sub(r"'[^']*'", "''", sql)
    for column in PHANTOM_COLUMNS:
        assert not re.search(rf"\b{column}\b", without_literals), (
            f"{column!r} appears as an identifier in:\n{without_literals}"
        )


@pytest.mark.unit
@pytest.mark.parametrize("sql", [SNAPSHOT_SEARCH_SQL, SNAPSHOT_NAME_SCAN_SQL])
def test_statements_read_the_payload(sql):
    """Positive counterpart: the actor must come from the jsonb payload."""
    assert "payload ->> 'actor'" in sql


@pytest.mark.unit
def test_search_sql_binds_every_value():
    """`.claude/rules/security.md`: no f-strings or .format() in SQL."""
    assert ":name" in SNAPSHOT_SEARCH_SQL
    assert ":pattern" in SNAPSHOT_SEARCH_SQL
    assert "{" not in SNAPSHOT_SEARCH_SQL
    assert "%s" not in SNAPSHOT_SEARCH_SQL


# ---------------------------------------------------------------------------
# Payload extraction
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_search_returns_the_actor_from_the_payload(resolver):
    hits = _search(resolver, "David Perdue")
    assert len(hits) == 1
    assert hits[0]["raw_name"] == "David A Perdue , Jr"


@pytest.mark.unit
def test_search_returns_title_and_category_from_the_row(resolver):
    hit = _search(resolver, "David Perdue")[0]
    assert hit["title"] == "David A Perdue , Jr — Purchase"
    assert hit["category"] == "congressional_trade"
    assert hit["snapshot_date"] == "2021-02-10"


@pytest.mark.unit
def test_search_maps_source_id_to_an_intelligence_domain(resolver):
    """``senate_efds`` from the payload must resolve to the congressional domain."""
    hit = _search(resolver, "David Perdue")[0]
    assert hit["source"] == "congressional"


@pytest.mark.unit
def test_search_carries_similarity_and_snapshot_id(resolver):
    hit = _search(resolver, "David Perdue")[0]
    assert hit["snapshot_id"] is not None
    assert 0.85 <= hit["similarity"] <= 1.0


@pytest.mark.unit
def test_search_finds_a_house_actor_by_surname(resolver):
    """The LIKE prefilter is built from the canonical key's first token."""
    hits = _search(resolver, "Thomas Carper")
    assert [h["raw_name"] for h in hits] == ["Thomas R Carper"]


@pytest.mark.unit
def test_search_finds_a_fed_speaker(resolver):
    hits = _search(resolver, "Jerome Powell")
    assert [h["raw_name"] for h in hits] == ["Jerome H Powell"]
    assert hits[0]["category"] == "fed_speech"


@pytest.mark.unit
def test_search_ignores_snapshots_with_no_actor(resolver):
    """A clustering payload must not surface as an entity."""
    assert _search(resolver, "clustering") == []


@pytest.mark.unit
def test_name_scan_skips_an_empty_actor(snapshot_engine):
    """A Fed speech with no scraped speaker is not a nameless entity."""
    with snapshot_engine.connect() as conn:
        names = {row[0] for row in conn.execute(text(SNAPSHOT_NAME_SCAN_SQL))}
    assert "" not in names
    assert None not in names


@pytest.mark.unit
def test_search_finds_the_legacy_senator_shape(resolver):
    """The 5,000 live congressional rows keep the actor under `senator`.

    Written before #477 fixed parse_datasets, so they never got
    ``payload ->> 'actor'``. Reading only the canonical key would leave the
    resolver blind to the whole congressional corpus on griddb today.
    """
    hits = _search(resolver, "Thomas Tuberville")
    assert [h["raw_name"] for h in hits] == ["Thomas H Tuberville"]


@pytest.mark.unit
def test_legacy_senator_row_still_resolves_its_domain(resolver):
    """Its subcategory is the senator's name, not a source id.

    ``_guess_domain_from_source_id`` also reads ``category``, which spells
    ``congressional_trade``, so the domain is right regardless.
    """
    hit = _search(resolver, "Thomas Tuberville")[0]
    assert hit["source"] == "congressional"
    assert hit["category"] == "congressional_trade"


@pytest.mark.unit
def test_opensanctions_name_key_is_not_read_as_an_actor(resolver):
    """`payload ->> 'name'` is deliberately excluded — see SNAPSHOT_SEARCH_SQL.

    That corpus is already loaded into ``actors`` by ``parse_opensanctions``,
    so reading it here too would let one source count as two domains in
    ``_compute_bridge_score``.
    """
    assert _search(resolver, "David Alfred Perdue") == []


@pytest.mark.unit
def test_unknown_name_returns_no_hits(resolver):
    assert _search(resolver, "Completely Unknown Person") == []


# ---------------------------------------------------------------------------
# Log level: a schema fault is an error, not a warning
# ---------------------------------------------------------------------------

class _FakeOrig(Exception):
    """Stand-in for a psycopg2 error carrying a SQLSTATE."""

    def __init__(self, pgcode: str, message: str = "") -> None:
        super().__init__(message)
        self.pgcode = pgcode


class _FakeDBAPIError(Exception):
    """Stand-in for a SQLAlchemy DBAPIError wrapping a driver error."""

    def __init__(self, message: str, orig: Exception | None = None) -> None:
        super().__init__(message)
        self.orig = orig


@pytest.mark.unit
@pytest.mark.parametrize("sqlstate", ["42703", "42883", "42P10"])
def test_sqlstate_schema_faults_are_recognised(sqlstate):
    exc = _FakeDBAPIError("boom", _FakeOrig(sqlstate))
    assert _is_schema_fault(exc) is True


@pytest.mark.unit
@pytest.mark.parametrize(
    "message",
    [
        'column "actor" does not exist',
        'column "actor" of relation "analytical_snapshots" does not exist',
        "no such column: actor",
        'record "new" has no field "title"',
    ],
)
def test_missing_column_messages_are_schema_faults(message):
    """Covers drivers that expose no SQLSTATE, and the PL/pgSQL trigger form."""
    assert _is_schema_fault(_FakeDBAPIError(message)) is True


@pytest.mark.unit
@pytest.mark.parametrize(
    "message",
    [
        'relation "wealth_flows" does not exist',
        "could not connect to server: Connection refused",
        "canceling statement due to statement timeout",
        "remaining connection slots are reserved",
    ],
)
def test_transient_and_missing_table_are_not_schema_faults(message):
    """An absent optional corpus is a runtime state, not a bug in the SQL."""
    assert _is_schema_fault(_FakeDBAPIError(message)) is False


@pytest.fixture
def loguru_records():
    """Capture (level, message) pairs emitted during a test.

    ``caplog`` cannot be used: loguru does not route through stdlib logging.
    """
    records: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda msg: records.append(
            (msg.record["level"].name, msg.record["message"])
        ),
        level="WARNING",
    )
    try:
        yield records
    finally:
        logger.remove(sink_id)


@pytest.mark.unit
def test_schema_fault_logs_at_error(loguru_records):
    """The fault that hid for months must reach errors.jsonl."""
    _log_query_failure(
        "analytical_snapshots name scan",
        _FakeDBAPIError('column "actor" does not exist'),
    )
    assert [level for level, _ in loguru_records] == ["ERROR"], loguru_records
    assert "analytical_snapshots name scan" in loguru_records[0][1]


@pytest.mark.unit
def test_transient_failure_stays_a_warning(loguru_records):
    """errors.jsonl keeps its signal (CLAUDE.md, "Log levels")."""
    _log_query_failure(
        "analytical_snapshots name scan",
        _FakeDBAPIError("could not connect to server: Connection refused"),
    )
    assert [level for level, _ in loguru_records] == ["WARNING"], loguru_records


@pytest.mark.unit
def test_missing_optional_table_stays_a_warning(loguru_records):
    """A corpus that was never loaded must not read as a code bug."""
    _log_query_failure(
        "wealth_flows entity search",
        _FakeDBAPIError('relation "wealth_flows" does not exist'),
    )
    assert [level for level, _ in loguru_records] == ["WARNING"], loguru_records


@pytest.mark.unit
def test_a_broken_snapshot_query_is_classified_as_a_schema_fault(
    resolver, loguru_records
):
    """End to end, against a real failing query rather than a crafted message.

    ``resolve()`` deliberately keeps going when one source fails — five other
    sources still carry signal — so the guarantee is visibility, not raising.
    This reproduces the production failure (a table without the column the
    statement names), then runs it through the handler ``resolve()`` uses.
    """
    with resolver.engine.begin() as conn:
        conn.execute(text("DROP TABLE analytical_snapshots"))
        conn.execute(text("CREATE TABLE analytical_snapshots (id INTEGER)"))

    with pytest.raises(Exception) as caught:
        _search(resolver, "David Perdue")

    assert _is_schema_fault(caught.value) is True
    _log_query_failure("analytical_snapshots entity search", caught.value)
    assert [level for level, _ in loguru_records] == ["ERROR"], loguru_records
