"""Tests for store/people_events.py.

The `PeopleEvent` validation tests need no database. The upsert/read tests
run against real PostgreSQL in a throwaway schema built from this PR's own
migration (migrations/versions/people_events_20260927.py) and skip cleanly
when no PostgreSQL is reachable, following
tests/test_raw_series_quarantined_migration_pg.py's `pg_engine` pattern.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from store.people_events import PeopleEvent, read_events, upsert_event

_MIGRATION_MODULE = "migrations.versions.people_events_20260927"


# ---------------------------------------------------------------------------
# Pure validation tests -- no database.
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestPeopleEventValidation:
    def _base_kwargs(self, **overrides):
        kwargs = dict(
            channel="form4",
            dedup_key="AAPL|X|2026-09-01|P|100",
            event_time=datetime(2026, 9, 1, tzinfo=timezone.utc),
            known_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
            known_at_basis="filing",
            actor_id="X",
            actor_id_basis="normalized_name",
            actor_type="insider",
            source="quiverquant",
        )
        kwargs.update(overrides)
        return kwargs

    def test_valid_event_constructs(self):
        PeopleEvent(**self._base_kwargs())

    def test_rejects_unknown_channel(self):
        with pytest.raises(ValueError, match="channel"):
            PeopleEvent(**self._base_kwargs(channel="bogus"))

    def test_rejects_unknown_known_at_basis(self):
        with pytest.raises(ValueError, match="known_at_basis"):
            PeopleEvent(**self._base_kwargs(known_at_basis="vibes"))

    def test_rejects_unknown_actor_id_basis(self):
        with pytest.raises(ValueError, match="actor_id_basis"):
            PeopleEvent(**self._base_kwargs(actor_id_basis="vibes"))

    def test_rejects_unknown_direction(self):
        with pytest.raises(ValueError, match="direction"):
            PeopleEvent(**self._base_kwargs(direction="sideways"))

    def test_allows_none_direction(self):
        PeopleEvent(**self._base_kwargs(direction=None))

    def test_rejects_negative_size_usd(self):
        with pytest.raises(ValueError, match="size_usd"):
            PeopleEvent(**self._base_kwargs(size_usd=-1.0))

    def test_rejects_n_sources_below_one(self):
        with pytest.raises(ValueError, match="n_sources"):
            PeopleEvent(**self._base_kwargs(n_sources=0))

    def test_rejects_empty_dedup_key(self):
        with pytest.raises(ValueError, match="dedup_key"):
            PeopleEvent(**self._base_kwargs(dedup_key=""))

    def test_rejects_empty_actor_id(self):
        with pytest.raises(ValueError, match="actor_id"):
            PeopleEvent(**self._base_kwargs(actor_id=""))


# ---------------------------------------------------------------------------
# PostgreSQL contract: upsert merges, read enforces known_at <= as_of.
# ---------------------------------------------------------------------------
@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"people_events_store_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        migration = importlib.import_module(_MIGRATION_MODULE)
        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        with engine.connect() as conn:
            trans = conn.begin()
            real_op = migration.op
            migration.op = Operations(MigrationContext.configure(conn))
            try:
                migration.upgrade()
            finally:
                migration.op = real_op
            trans.commit()
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _event(**overrides) -> PeopleEvent:
    kwargs = dict(
        channel="form4",
        dedup_key="AAPL|TIMOTHY D COOK|2026-09-01|P|1000",
        event_time=datetime(2026, 9, 1, tzinfo=timezone.utc),
        known_at=datetime(2026, 9, 2, 14, 30, tzinfo=timezone.utc),
        known_at_basis="filing",
        actor_id="TIMOTHY D COOK",
        actor_id_basis="normalized_name",
        actor_type="insider",
        entity_ticker="AAPL",
        direction="buy",
        source="quiverquant",
        source_refs=({"source_type": "quiverquant:insider", "signal_sources_id": 1},),
    )
    kwargs.update(overrides)
    return PeopleEvent(**kwargs)


def test_upsert_then_read_round_trips(scratch):
    upsert_event(scratch, _event())
    events = read_events(scratch, as_of=datetime(2026, 9, 3, tzinfo=timezone.utc), entity_ticker="AAPL")
    assert len(events) == 1
    got = events[0]
    assert got.channel == "form4"
    assert got.actor_id == "TIMOTHY D COOK"
    assert got.n_sources == 1
    assert got.source_refs[0]["signal_sources_id"] == 1


def test_read_enforces_known_at_pit_bound(scratch):
    upsert_event(scratch, _event())
    # known_at is 2026-09-02T14:30Z; as_of one second earlier must exclude it.
    before = datetime(2026, 9, 2, 14, 29, 59, tzinfo=timezone.utc)
    after = datetime(2026, 9, 2, 14, 30, 0, tzinfo=timezone.utc)
    assert read_events(scratch, as_of=before) == []
    assert len(read_events(scratch, as_of=after)) == 1


def test_second_source_for_the_same_act_merges_instead_of_duplicating(scratch):
    upsert_event(scratch, _event(source_refs=({"source_type": "quiverquant:insider", "signal_sources_id": 1},)))
    upsert_event(scratch, _event(source_refs=({"source_type": "insider", "signal_sources_id": 2},)))

    events = read_events(scratch, as_of=datetime(2026, 9, 3, tzinfo=timezone.utc))
    assert len(events) == 1, "same (channel, dedup_key) must merge, not duplicate"
    assert events[0].n_sources == 2
    seen_ids = {ref["signal_sources_id"] for ref in events[0].source_refs}
    assert seen_ids == {1, 2}


def test_transaction_code_round_trips(scratch):
    upsert_event(scratch, _event(transaction_code="P"))
    events = read_events(scratch, as_of=datetime(2026, 9, 3, tzinfo=timezone.utc))
    assert len(events) == 1
    assert events[0].transaction_code == "P"


def test_transaction_code_defaults_to_none(scratch):
    upsert_event(scratch, _event())
    events = read_events(scratch, as_of=datetime(2026, 9, 3, tzinfo=timezone.utc))
    assert events[0].transaction_code is None


def test_second_source_with_an_earlier_known_at_pulls_known_at_earlier(scratch):
    # First source: a same-day "first_seen" fallback known_at.
    upsert_event(scratch, _event(
        known_at=datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc),
        known_at_basis="first_seen",
        source_refs=({"source_type": "quiverquant:insider", "signal_sources_id": 1},),
    ))
    # Second source: the real, earlier SEC filing timestamp for the same act.
    upsert_event(scratch, _event(
        known_at=datetime(2026, 9, 2, 14, 30, tzinfo=timezone.utc),
        known_at_basis="filing",
        source_refs=({"source_type": "insider", "signal_sources_id": 2},),
    ))

    events = read_events(scratch, as_of=datetime(2026, 9, 3, tzinfo=timezone.utc))
    assert len(events) == 1
    got = events[0]
    assert got.known_at == datetime(2026, 9, 2, 14, 30, tzinfo=timezone.utc)
    assert got.known_at_basis == "filing"
    assert got.n_sources == 2


def test_second_source_with_a_later_known_at_does_not_move_known_at_later(scratch):
    # First source already has the earlier, real filing timestamp.
    upsert_event(scratch, _event(
        known_at=datetime(2026, 9, 2, 14, 30, tzinfo=timezone.utc),
        known_at_basis="filing",
        source_refs=({"source_type": "insider", "signal_sources_id": 1},),
    ))
    # A second, later source (e.g. a slower ingestion path) must not push
    # known_at later -- that would silently un-know something the public
    # could already see per the first source.
    upsert_event(scratch, _event(
        known_at=datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc),
        known_at_basis="first_seen",
        source_refs=({"source_type": "quiverquant:insider", "signal_sources_id": 2},),
    ))

    events = read_events(scratch, as_of=datetime(2026, 9, 3, tzinfo=timezone.utc))
    assert len(events) == 1
    got = events[0]
    assert got.known_at == datetime(2026, 9, 2, 14, 30, tzinfo=timezone.utc)
    assert got.known_at_basis == "filing"


def test_different_dedup_key_does_not_merge(scratch):
    upsert_event(scratch, _event(dedup_key="AAPL|TIMOTHY D COOK|2026-09-01|P|1000"))
    upsert_event(scratch, _event(dedup_key="AAPL|TIMOTHY D COOK|2026-09-02|P|1000", event_time=datetime(2026, 9, 2, tzinfo=timezone.utc)))
    events = read_events(scratch, as_of=datetime(2026, 9, 3, tzinfo=timezone.utc))
    assert len(events) == 2


def test_channel_filter(scratch):
    upsert_event(scratch, _event())
    assert len(read_events(scratch, as_of=datetime(2026, 9, 3, tzinfo=timezone.utc), channel="form4")) == 1
    assert len(read_events(scratch, as_of=datetime(2026, 9, 3, tzinfo=timezone.utc), channel="congress")) == 0


def test_echo_of_self_references_and_can_be_excluded(scratch):
    original_id = upsert_event(scratch, _event())
    echo = _event(
        channel="news",
        dedup_key="news-dedup-1",
        source="news",
        actor_id="TIMOTHY D COOK",
        echo_of=original_id,
        known_at=datetime(2026, 9, 2, 15, 0, tzinfo=timezone.utc),
    )
    upsert_event(scratch, echo)

    all_events = read_events(scratch, as_of=datetime(2026, 9, 3, tzinfo=timezone.utc))
    assert len(all_events) == 2

    non_echo = read_events(scratch, as_of=datetime(2026, 9, 3, tzinfo=timezone.utc), exclude_echoes=True)
    assert len(non_echo) == 1
    assert non_echo[0].echo_of is None


def test_database_rejects_a_null_known_at_even_if_python_validation_is_bypassed(scratch):
    """The table itself, not just the dataclass, must refuse a fabricated/NULL known_at."""
    from sqlalchemy.exc import DBAPIError

    with pytest.raises(DBAPIError):
        with scratch.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO people_events "
                    "(channel, dedup_key, event_time, known_at, known_at_basis, "
                    " actor_id, actor_id_basis, actor_type, source) "
                    "VALUES ('form4', 'x', NOW(), NULL, 'filing', 'x', 'normalized_name', 'insider', 'quiverquant')"
                )
            )


def test_database_rejects_an_unknown_known_at_basis(scratch):
    from sqlalchemy.exc import DBAPIError

    with pytest.raises(DBAPIError):
        with scratch.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO people_events "
                    "(channel, dedup_key, event_time, known_at, known_at_basis, "
                    " actor_id, actor_id_basis, actor_type, source) "
                    "VALUES ('form4', 'x', NOW(), NOW(), 'made_up_basis', 'x', 'normalized_name', 'insider', 'quiverquant')"
                )
            )
